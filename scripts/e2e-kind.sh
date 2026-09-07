#!/usr/bin/env bash
set -Eeuo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TAMS_VERSION="${TAMSIN_E2E_VERSION:-8.2}"
case "$TAMS_VERSION" in
  8.1)
    TAMOSS_COMMIT=cda9611e31ff427d34afb2c06c45c78c91262ee9
    TAMS_COMMIT=98d307b09b5ebf79278aa7d3aad53295154e2c17
    ;;
  8.2)
    TAMOSS_COMMIT=c17e20fe4aa732e6c0b30f904ca25e3dd2cf2c9c
    TAMS_COMMIT=34fb31b80cb8afb3194f28c8b787301379caacf8
    ;;
  *) echo "Unsupported TAMS E2E version: $TAMS_VERSION" >&2; exit 2 ;;
esac
PROFILE=local-kind

require() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Required command not found: $1" >&2
    exit 2
  }
}

AQUA_VERSION="v2.60.1"
PROJECT_NAME="tamsin-e2e"
IMAGE="${IMAGE:-tamsin:dev}"
prune_builder_cache="${TAMSIN_E2E_PRUNE_BUILDER_CACHE:-${CI:-false}}"
case "$prune_builder_cache" in
  true|false) ;;
  *) echo "TAMSIN_E2E_PRUNE_BUILDER_CACHE must be true or false" >&2; exit 2 ;;
esac
# Use a dedicated loopback port so unrelated local ingress controllers cannot capture the matrix traffic.
HTTPS_PORT="${TAMSIN_E2E_HTTPS_PORT:-18443}"
API_URL="https://api.tamoss.localtest.me:$HTTPS_PORT"
S3_URL="https://s3.tamoss.localtest.me:$HTTPS_PORT"
AUTH_URL="https://auth.tamoss.localtest.me:$HTTPS_PORT"
docker_host_args=(
  --add-host api.tamoss.localtest.me:127.0.0.1
  --add-host s3.tamoss.localtest.me:127.0.0.1
  --add-host auth.tamoss.localtest.me:127.0.0.1
)

cache="$root/.cache"
tamoss="$cache/tamoss-$TAMOSS_COMMIT"
kubeconfig="$cache/$PROJECT_NAME.kubeconfig"
fixtures="$root/.tmp/e2e"
http_pid=""

# Preserve redacted diagnostics before tearing down a failed cluster.
capture_diagnostics() {
  # Keep diagnostics outside the disposable fixtures directory.
  local bundle="$root/.tmp/e2e-support"
  [ -f "$kubeconfig" ] || return 0
  [ -f "$tamoss/scripts/support_bundle.py" ] || return 0
  mkdir -p "$bundle"
  # The upstream collector needs a tams namespace; also cover earlier failures.
  {
    echo "== pods =="
    kubectl --kubeconfig "$kubeconfig" get pods -A -o wide
    echo
    echo "== recent events =="
    kubectl --kubeconfig "$kubeconfig" get events -A --sort-by=.lastTimestamp | tail -60
    echo
    echo "== nodes =="
    kubectl --kubeconfig "$kubeconfig" describe nodes
    echo
    echo "== host Docker storage =="
    docker system df
    echo
    echo "== Kind node filesystems =="
    docker exec "$PROJECT_NAME-control-plane" df -h / /var/lib/containerd
    echo
    echo "== operator =="
    kubectl --kubeconfig "$kubeconfig" -n tamoss-system describe deploy,pods
  } >"$bundle/cluster-state.txt" 2>&1 || true

  echo "Collecting TAMOSS diagnostics into $bundle" >&2
  python3 "$tamoss/scripts/support_bundle.py" \
    --kubeconfig "$kubeconfig" \
    --namespace tams \
    --operator-namespace tamoss-system \
    --additional-namespace auth \
    --additional-namespace cert-manager \
    --additional-namespace cnpg-system \
    --additional-namespace rustfs-system \
    --additional-namespace traefik \
    --output-root "$bundle" >/dev/null 2>&1 || true
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if [ "$status" -ne 0 ]; then
    capture_diagnostics
  fi
  if [ -n "$http_pid" ]; then
    kill "$http_pid" >/dev/null 2>&1 || true
    wait "$http_pid" >/dev/null 2>&1 || true
  fi
  if [ "$status" -ne 0 ] && [ "${TAMSIN_E2E_KEEP_CLUSTER:-false}" = true ]; then
    echo "Preserving failed E2E cluster and fixtures at $fixtures" >&2
    exit "$status"
  fi
  if [ -d "$tamoss" ] && command -v task >/dev/null 2>&1; then
    (
      cd "$tamoss"
      task --yes kind:down PROFILE="$PROFILE" PROJECT_NAME="$PROJECT_NAME" KUBECONFIG="$kubeconfig"
    ) >/dev/null 2>&1 || true
  fi
  rm -rf "$fixtures"
  exit "$status"
}
trap cleanup EXIT INT TERM
trap 'status=$?; printf "E2E failed at line %s (exit %s): %s\n" "$LINENO" "$status" "$BASH_COMMAND" >&2; exit "$status"' ERR

if ! command -v aqua >/dev/null 2>&1; then
  require go
  mkdir -p "$cache/aqua-bin"
  GOBIN="$cache/aqua-bin" go install "github.com/aquaproj/aqua/v2/cmd/aqua@$AQUA_VERSION"
  export PATH="$cache/aqua-bin:$PATH"
fi

for command in aqua base64 curl docker git go jq kubectl python3 tar; do
  require "$command"
done

# Clear stale fixtures, including any previous HTTP server's readiness file.
rm -rf "$fixtures"
mkdir -p "$cache" "$fixtures"
if [ ! -f "$tamoss/Taskfile.yaml" ]; then
  rm -rf "$tamoss"
  curl -fsSL "https://github.com/livewyer-ops/tamoss/archive/$TAMOSS_COMMIT.tar.gz" \
    | tar -xz -C "$cache"
fi
if [ ! -f "$tamoss/src/vendor/bbc-tams/api/TimeAddressableMediaStore.yaml" ]; then
  mkdir -p "$tamoss/src/vendor/bbc-tams"
  curl -fsSL "https://github.com/bbc/tams/archive/$TAMS_COMMIT.tar.gz" \
    | tar -xz --strip-components=1 -C "$tamoss/src/vendor/bbc-tams"
fi
# The pinned source is an archive inside this worktree. Give it its own Git
# boundary so TAMOSS content-derived image tags cannot resolve the parent
# TAMSin repository by accident.
if [ ! -d "$tamoss/.git" ]; then
  git -C "$tamoss" init --quiet
fi

(
  cd "$tamoss"
  aqua install
)
aqua_root="$(cd "$tamoss" && aqua root-dir)"
export PATH="$aqua_root/bin:$PATH"
require kind
require task

kind_config="$fixtures/kind.yaml"
cat >"$kind_config" <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 30443
        hostPort: $HTTPS_PORT
        listenAddress: "127.0.0.1"
        protocol: TCP
EOF
(
  cd "$tamoss"
  task --yes kind:down PROFILE="$PROFILE" PROJECT_NAME="$PROJECT_NAME" KUBECONFIG="$kubeconfig"
) >/dev/null 2>&1 || true


# Replace the operator in place: small CI runners cannot fit a surge replica.
python3 "$root/scripts/e2e-prepare.py" "$tamoss" "$prune_builder_cache"

# Apply the test configuration once, with the unused UI disabled.
cat >"$tamoss/deploy/environments/$PROFILE/tamsin-e2e-instance.yaml" <<EOF
apiVersion: tamoss.livewyer.io/v1alpha1
kind: Tamoss
metadata:
  name: tamoss-kind
  namespace: tams
spec:
  ui:
    enabled: false
  auth:
    providedBy: authentik-blueprints
    external: null
    authentikBlueprints:
      platformNamespace: auth
      issuerURL: $AUTH_URL
      internalURL: http://authentik-server.auth.svc.cluster.local
  backends:
    s3:
      rustfsOperator:
        publicEndpoint:
          url: $S3_URL
EOF
# Overwrite to avoid duplicate patch entries when reusing a cached checkout.
cat >"$tamoss/deploy/environments/$PROFILE/kustomization.yaml" <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - ../../instances/$PROFILE

patches:
  - path: tamsin-e2e-instance.yaml
EOF

(
  cd "$tamoss"
  KIND_DEMO_INGEST=false KIND_SUMMARY=false task kind:up \
    PROFILE="$PROFILE" PROJECT_NAME="$PROJECT_NAME" KUBECONFIG="$kubeconfig" KIND_CONFIG="$kind_config"
)
# kind:up has already waited for the instance to report Ready, and it was
# applied with the configuration above rather than reconciled into it.
backend_reconciled=false
for _ in {1..90}; do
  configured_public_endpoint="$(
    kubectl --kubeconfig "$kubeconfig" -n tams get secret tams-backends \
      -o 'jsonpath={.data.TAMOSS_S3_PUBLIC_ENDPOINT}' 2>/dev/null \
      | base64 --decode 2>/dev/null || true
  )"
  resolved_public_endpoint="$(
    kubectl --kubeconfig "$kubeconfig" -n tams get storagebackend tams-storage-default \
      -o 'jsonpath={.status.resolved.publicEndpointURL}' 2>/dev/null || true
  )"
  backend_ready="$(
    kubectl --kubeconfig "$kubeconfig" -n tams get storagebackend tams-storage-default \
      -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true
  )"
  if [ "$configured_public_endpoint" = "$S3_URL" ] \
    && [ "$resolved_public_endpoint" = "$S3_URL" ] \
    && [ "$backend_ready" = "True" ]; then
    backend_reconciled=true
    break
  fi
  sleep 2
done
if [ "$backend_reconciled" != true ]; then
  echo "Timed out waiting for TAMOSS storage configuration to use $S3_URL" >&2
  exit 1
fi

# The API reads Secret-backed storage settings only at process startup.
kubectl --kubeconfig "$kubeconfig" -n tams delete pod \
  -l app.kubernetes.io/name=tamoss,app.kubernetes.io/component=api \
  --wait=true >/dev/null
kubectl --kubeconfig "$kubeconfig" -n tams rollout status deployment/tams-api \
  --timeout=180s >/dev/null
active_public_endpoint="$(
  kubectl --kubeconfig "$kubeconfig" -n tams exec deployment/tams-api \
    -- printenv TAMOSS_S3_PUBLIC_ENDPOINT
)"
if [ "$active_public_endpoint" != "$S3_URL" ]; then
  echo "TAMOSS API did not reload the E2E public S3 endpoint" >&2
  exit 1
fi
active_oauth_issuer="$(
  kubectl --kubeconfig "$kubeconfig" -n tams exec deployment/tams-api \
    -- printenv TAMOSS_OAUTH2_ISSUER
)"
expected_oauth_issuer="$AUTH_URL/application/o/tamoss-tams-tamoss-kind/"
if [ "$active_oauth_issuer" != "$expected_oauth_issuer" ]; then
  echo "TAMOSS API did not reload the E2E OAuth issuer" >&2
  exit 1
fi


secret_value() {
  local namespace="$1"
  local secret="$2"
  shift 2
  local key value
  for key in "$@"; do
    value="$(
      kubectl --kubeconfig "$kubeconfig" -n "$namespace" get secret "$secret" \
        -o "jsonpath={.data.${key}}" 2>/dev/null \
        | base64 --decode 2>/dev/null || true
    )"
    if [ -n "$value" ]; then
      printf '%s' "$value"
      return 0
    fi
  done
  return 1
}

api_secret="$(kubectl --kubeconfig "$kubeconfig" -n tams get tamoss tamoss-kind -o 'jsonpath={.status.resolved.generatedSecrets.apiToken}')"
discovered_api_url="$(kubectl --kubeconfig "$kubeconfig" -n tams get tamoss tamoss-kind -o 'jsonpath={.status.endpoints.api}')"
if [ -z "$discovered_api_url" ]; then
  echo "TAMOSS did not publish an API endpoint" >&2
  exit 1
fi
api_secret="${api_secret:-tams-api-token}"
TAMSIN_AUTH_TOKEN="$(secret_value tams "$api_secret" TAMOSS_API_TOKEN)"
export TAMSIN_AUTH_TOKEN

if [ -z "$TAMSIN_AUTH_TOKEN" ]; then
  echo "TAMOSS bearer token is empty" >&2
  exit 1
fi

export TAMSIN_ENDPOINT="$API_URL"
export TAMSIN_AUTH_MODE="bearer"
export TAMSIN_HTTP_INSECURE_SKIP_VERIFY="true"
export TAMSIN_LOG_LEVEL="warn"
export TAMSIN_FORMAT="json"
if ! curl --ipv4 --resolve "api.tamoss.localtest.me:$HTTPS_PORT:127.0.0.1" \
  --insecure --fail --silent --retry 89 --retry-delay 2 --retry-all-errors \
  --retry-max-time 180 --max-time 5 "${API_URL%/}/readyz" >/dev/null 2>&1; then
  echo "Timed out waiting for TAMOSS API readiness at ${API_URL%/}/readyz" >&2
  exit 1
fi

mkdir -p "$fixtures/single" "$fixtures/directory" "$fixtures/manifest-media" "$fixtures/list" "$fixtures/http" "$fixtures/s3"
cp "$tamoss/deploy/demo/tamoss-demo.ts" "$fixtures/single/demo.ts"
cp "$tamoss/deploy/demo/tamoss-demo.ts" "$fixtures/directory/one.ts"
printf '\0' >> "$fixtures/directory/one.ts"
cp "$tamoss/deploy/demo/tamoss-demo.ts" "$fixtures/directory/two.ts"
printf '\0\0' >> "$fixtures/directory/two.ts"
cp "$tamoss/deploy/demo/tamoss-demo.ts" "$fixtures/manifest-media/one.ts"
printf '\1' >> "$fixtures/manifest-media/one.ts"
cp "$tamoss/deploy/demo/tamoss-demo.ts" "$fixtures/manifest-media/two.ts"
printf '\1\1' >> "$fixtures/manifest-media/two.ts"
printf '../manifest-media/one.ts\n../manifest-media/two.ts\n' > "$fixtures/list/sources.txt"
cp "$tamoss/deploy/demo/tamoss-demo.ts" "$fixtures/http/http.ts"
printf '\2' >> "$fixtures/http/http.ts"
cp "$tamoss/deploy/demo/tamoss-demo.ts" "$fixtures/s3/s3.ts"
# Add audio to the video-only demo to exercise both essence-storage arrangements.
# Use FFmpeg from the image under test.
mkdir -p "$fixtures/muxed"
docker run --rm --entrypoint ffmpeg -v "$fixtures:/fixtures" -u "$(id -u):$(id -g)" \
  "$IMAGE" -hide_banner -loglevel error -y \
  -i /fixtures/single/demo.ts -f lavfi -i "sine=frequency=440:sample_rate=48000" \
  -map 0:v -map 1:a -c:v copy -c:a aac -shortest /fixtures/muxed/muxed.ts
docker run --rm --entrypoint ffprobe -v "$fixtures:/fixtures:ro" -u "$(id -u):$(id -g)" \
  "$IMAGE" -v error -show_entries stream=codec_type -of csv=p=0 /fixtures/muxed/muxed.ts \
  | sort -u | tr '\n' ' ' | grep -q 'audio video' || {
  printf 'e2e: muxed fixture does not contain both a video and an audio track\n' >&2
  exit 1
}
printf '\3' >> "$fixtures/s3/s3.ts"

port_file="$fixtures/http.port"
go build -o "$fixtures/e2e-http" "$root/scripts/e2e-http.go"
"$fixtures/e2e-http" "$fixtures/http" "$port_file" >"$fixtures/http.log" 2>&1 &
http_pid=$!
for _ in {1..30}; do
  if [ -s "$port_file" ]; then
    break
  fi
  sleep 1
done
if [ ! -s "$port_file" ]; then
  echo "HTTP fixture server did not publish its port" >&2
  cat "$fixtures/http.log" >&2 || true
  exit 1
fi
http_url="http://127.0.0.1:$(<"$port_file")/http.ts"
curl -fsS "$http_url" >/dev/null

run_ingest() {
  local name="$1"
  local expected="$2"
  shift 2
  rm -f "$fixtures/$name.events.jsonl"
  docker run --rm --network host "${docker_host_args[@]}" \
    -e TAMSIN_ENDPOINT -e TAMSIN_AUTH_MODE -e TAMSIN_AUTH_TOKEN \
    -e TAMSIN_HTTP_INSECURE_SKIP_VERIFY -e TAMSIN_LOG_LEVEL -e TAMSIN_FORMAT \
    -v "$fixtures:/fixtures:ro" \
    "$IMAGE" --profile essence-segments "$@" >"$fixtures/$name.events.jsonl" || {
      status=$?
      cat "$fixtures/$name.events.jsonl" >&2
      return "$status"
  }
  assert_ingest_artifacts "$name" "$expected"
}

# Check terminal outcomes here; CLI tests cover protocol sequencing and framing.
assert_ingest_artifacts() {
  local name="$1" expected="$2"
  jq -s -e --argjson expected "$expected" '
    [.[] | select(.type == "input.finished") | .payload] as $inputs |
    last.type == "run.finished" and
    (last.payload | .outcome == "succeeded" and .exit_code == 0 and
      .total == $expected and .succeeded == $expected and .failed == 0) and
    ($inputs | length) == $expected and
    all($inputs[];
      (.status == "ingested" or .status == "resumed") and
      .verification == "verified" and .object_count > 0) and
    all(.[] | select(.type == "flow.result") | .payload;
      .disposition == "written" or .disposition == "unchanged") and
    ([.[] | select(.type == "object.result")] | length) > 0 and
    all(.[] | select(.type == "object.result") | .payload;
      (.disposition == "ingested" or .disposition == "resumed") and
      .verification_status == "verified")
  ' "$fixtures/$name.events.jsonl" >/dev/null || {
    cat "$fixtures/$name.events.jsonl" >&2
    return 1
  }
}
api_component() {
  jq -rn --arg value "$1" '$value | @uri'
}

# Read the API directly to check persisted resources.
api_request() {
  local method="$1" path="$2" body="${3:-}"
  local options=(
    --ipv4
    --resolve "api.tamoss.localtest.me:$HTTPS_PORT:127.0.0.1"
    --insecure --fail --silent --show-error
    --request "$method"
    --header "Authorization: Bearer $TAMSIN_AUTH_TOKEN"
    --header 'Accept: application/json'
  )
  if [ -n "$body" ]; then
    options+=(--header 'Content-Type: application/json' --data-binary "$body")
  fi
  curl "${options[@]}" "${API_URL%/}/$path"
}

get_flow() {
  api_request GET "flows/$(api_component "$1")"
}

get_segments() {
  api_request GET "flows/$(api_component "$1")/segments?limit=1000&accept_get_urls=&presigned=false"
}

result_flow_ids() {
  jq -r 'select(.type == "flow.result") | .payload.flow_id' "$1"
}

# Pair each reported Object with its owning Flow.
result_flow_objects() {
  jq -r 'select(.type == "object.result")
         | [.scope.flow_id, .payload.object_id] | @tsv' "$1"
}

# Check persisted Flows against the named specification rules.
assert_flow_conformance() {
  local name="$1" flow_id="$2"
  local flow
  flow="$(get_flow "$flow_id")" || {
    printf 'e2e: could not read back Flow %s for %s\n' "$flow_id" "$name" >&2
    return 1
  }

  # AppNote 0003: unprefixed tag names are reserved for interoperable tags, so
  # implementation-specific ones carry an underscore and the service name.
  jq -e 'if (.tags // {} | keys | length) == 0 then false
         else (.tags | keys | all(startswith("_tamsin_"))) end' <<<"$flow" >/dev/null || {
    printf 'e2e: %s Flow tags are not _tamsin_ prefixed (AppNote 0003): %s\n' \
      "$name" "$(jq -c '.tags' <<<"$flow")" >&2
    return 1
  }

  # flow-core: generation records source lineage, which a local stream copy
  # cannot infer. The matrix does not supply it, so Tamsin must leave it unset
  # rather than claiming the input came directly from an originating device.
  jq -e 'has("generation") | not' <<<"$flow" >/dev/null || {
    printf 'e2e: %s Flow should leave operator-owned generation unset, got %s\n' \
      "$name" "$(jq -c '.generation' <<<"$flow")" >&2
    return 1
  }

  # AppNote 0006: the presence of container flags that a Flow references Media
  # Objects directly. Which way round that applies depends on the Flow, so it is
  # asked rather than assumed: an essence and a muxed multi-essence Flow own
  # Segments and declare a container, while a collector and a collected
  # mono-essence Flow own none and must not. Assuming every Flow in a result
  # owns Segments held only until independent storage began writing a collector.
  local own_segments
  own_segments="$(get_segments "$flow_id")" || return 1
  if [ "$(jq -r 'length' <<<"$own_segments")" -gt 0 ]; then
    jq -e 'has("container")' <<<"$flow" >/dev/null || {
      printf 'e2e: %s Flow %s owns Segments but declares no container (AppNote 0006)\n' "$name" "$flow_id" >&2
      return 1
    }
  else
    jq -e 'has("container") | not' <<<"$flow" >/dev/null || {
      printf 'e2e: %s Flow %s owns no Segments but declares a container (AppNote 0006)\n' "$name" "$flow_id" >&2
      return 1
    }
  fi

  # AppNote 0006: a multi-essence Flow collects one mono-essence Flow per
  # elementary stream, and those collected Flows are reached through this Flow's
  # Segments rather than referencing Media Objects themselves.
  if jq -e '.format == "urn:x-nmos:format:multi"' <<<"$flow" >/dev/null; then
    jq -e '(.flow_collection // []) | length > 0' <<<"$flow" >/dev/null || {
      printf 'e2e: %s multi-essence Flow has no flow_collection (AppNote 0006)\n' "$name" >&2
      return 1
    }
    # Collection Items require both id and role.
    jq -e '.flow_collection | all(has("id") and has("role") and (.role | length > 0))' <<<"$flow" >/dev/null || {
      printf 'e2e: %s Collection Items need id and role: %s\n' \
        "$name" "$(jq -c '.flow_collection' <<<"$flow")" >&2
      return 1
    }
    local collected_id collected collection_item collection_index collection_count
    collection_count="$(jq -r '.flow_collection | length' <<<"$flow")"
    for ((collection_index = 0; collection_index < collection_count; collection_index++)); do
      collection_item="$(jq -c --argjson index "$collection_index" '.flow_collection[$index]' <<<"$flow")"
      collected_id="$(jq -r '.id' <<<"$collection_item")"
      collected="$(get_flow "$collected_id")" || {
        printf 'e2e: %s collected Flow %s is not registered\n' "$name" "$collected_id" >&2
        return 1
      }
      # Which rule applies depends on where the media actually is. A collector
      # that owns the Segments holds a multiplex, so the Flows it collects
      # describe tracks inside it; a collector that owns none is recording that
      # its essences were demultiplexed and own their Objects themselves.
      if [ "$(jq -r 'length' <<<"$own_segments")" -gt 0 ]; then
        if ! jq -e '.container_mapping | has("track_index") and has("format_track_index")' \
          <<<"$collection_item" >/dev/null \
          || ! jq -e '(has("container") | not) and (has("container_mapping") | not)' <<<"$collected" >/dev/null; then
          printf 'e2e: %s Collection Item for %s needs container_mapping, while the collected Flow needs no container or mapping (AppNote 0006)\n' \
            "$name" "$collected_id" >&2
          return 1
        fi
      else
        if ! jq -e 'has("container_mapping") | not' <<<"$collection_item" >/dev/null \
          || ! jq -e 'has("container") and (has("container_mapping") | not)' <<<"$collected" >/dev/null; then
          printf 'e2e: %s collected Flow %s owns its Objects, so it needs a container and no container_mapping (AppNote 0006)\n' \
            "$name" "$collected_id" >&2
          return 1
        fi
      fi
    done
  fi

  # Every Object tamsin reported must be registered as a Flow Segment, which is
  # what makes it reachable at all: TAMS returns 404 for an Object assigned via
  # /flows/{flowId}/storage but not yet registered against a Segment.
  local segments
  segments="$own_segments"
  local object_id
  for object_id in $(result_flow_objects "$fixtures/$name.events.jsonl" | awk -F'\t' -v flow="$flow_id" '$1 == flow {print $2}'); do
    jq -e --arg id "$object_id" 'any(.[]; .object_id == $id)' <<<"$segments" >/dev/null || {
      printf 'e2e: %s Object %s is owned by Flow %s but is not registered as one of its Segments\n' \
        "$name" "$object_id" "$flow_id" >&2
      return 1
    }
  done
}


run_ingest local-file 1 -i /fixtures/single/demo.ts
run_ingest local-file-resume 1 -i /fixtures/single/demo.ts
jq -s -e 'all(.[] | select(.type == "input.finished") | .payload;
         .status == "resumed" and .verification == "verified") and
       all(.[] | select(.type == "object.result") | .payload; .disposition == "resumed")' \
  "$fixtures/local-file-resume.events.jsonl" >/dev/null
for flow_id in $(result_flow_ids "$fixtures/local-file.events.jsonl"); do
  assert_flow_conformance local-file "$flow_id"
done

if [ "$TAMS_VERSION" = "8.2" ]; then
  profile_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
  source_flow_id="$(jq -ser '.[] | select(.type == "input.finished") | .payload.root_flow_id' "$fixtures/local-file.events.jsonl")"
  profile_body="$(get_flow "$source_flow_id" | jq -c --arg id "$profile_id" '
    {
      id: $id,
      label: "TAMSin numeric Profile E2E",
      flow_metadata: (
        {format, codec, container, avg_bit_rate, segment_duration, container_mapping, essence_parameters}
        | with_entries(select(.value != null))
        # TAMOSS materialises the TAMS default on a normal Flow read. Profile
        # matching remains presence-strict, so retain the generated form:
        # fixed frame_rate with vfr omitted.
        | if .essence_parameters.vfr == false then del(.essence_parameters.vfr) else . end
      )
    }
  ')"
  api_request POST "service/profiles/$profile_id" "$profile_body" >/dev/null
  api_request GET "service/profiles/$profile_id" \
    | jq -e '.flow_metadata.segment_duration as $duration |
        ($duration.numerator | type) == "number" and ($duration.denominator | type) == "number"' >/dev/null

  run_ingest profile-backed 1 --tams-flow-profile "video=$profile_id" -i /fixtures/single/demo.ts
  profiled_flow="$(jq -ser --arg id "$profile_id" '
    .[] | select(.type == "flow.result" and .payload.tams_flow_profile_id == $id) | .payload.flow_id
  ' "$fixtures/profile-backed.events.jsonl")"
  get_flow "$profiled_flow" | jq -e --arg id "$profile_id" '.profile_id == $id' >/dev/null
fi

run_ingest directory 2 -i /fixtures/directory
for flow_id in $(result_flow_ids "$fixtures/directory.events.jsonl"); do
  assert_flow_conformance directory "$flow_id"
done
run_ingest manifest 2 -i /fixtures/list/sources.txt
run_ingest http 1 --input-mode stream -i "$http_url"
run_ingest http-resume 1 --input-mode stream -i "$http_url"
jq -s -e 'all(.[] | select(.type == "input.finished") | .payload; .status == "resumed" and (has("sha256") | not))' \
  "$fixtures/http-resume.events.jsonl" >/dev/null
run_ingest http-fallback 1 -i "${http_url%/http.ts}/stage/http.ts"
jq -s -e 'all(.[] | select(.type == "input.finished") | .payload; (.sha256 | length) == 64)' \
  "$fixtures/http-fallback.events.jsonl" >/dev/null
if [ "$TAMS_VERSION" = "8.2" ]; then
  run_ingest http-profile 1 --input-mode stream --tams-flow-profile "video=$profile_id" -i "$http_url"
fi
for flow_id in $(result_flow_ids "$fixtures/http.events.jsonl"); do
  assert_flow_conformance http "$flow_id"
done

# Segments are cut at a 10s target by default, so the matrix also covers an
# explicit whole-file Object and an explicit container, which take different
# paths through Flow metadata.
run_ingest whole-file 1 -d 0 -i /fixtures/single/demo.ts
for flow_id in $(result_flow_ids "$fixtures/whole-file.events.jsonl"); do
  assert_flow_conformance whole-file "$flow_id"
done
jq -s -e '([.[] | select(.type == "input.finished")][0].payload.root_flow_id) as $root |
  [.[] | select(.type == "object.result" and .scope.flow_id == $root)] | length == 1' \
  "$fixtures/whole-file.events.jsonl" >/dev/null

run_ingest mpegts-format 1 --segment-format mpegts -i /fixtures/single/demo.ts
mpegts_flow="$(jq -ser '.[] | select(.type == "input.finished") | .payload.root_flow_id' "$fixtures/mpegts-format.events.jsonl")"
for flow_id in $(result_flow_ids "$fixtures/mpegts-format.events.jsonl"); do
  assert_flow_conformance mpegts-format "$flow_id"
done
# The Flow must declare the container that was written, not the input's.
get_flow "$mpegts_flow" | jq -e '.container == "video/mp2t"' >/dev/null || {
  printf 'e2e: --segment-format mpegts did not set container video/mp2t\n' >&2
  exit 1
}

# The two essence-storage arrangements are described by opposite halves of
# AppNote 0006, so both are exercised against the live service. The demo fixture
# is a multiplex, which is what makes the distinction meaningful.
run_ingest independent-essences 1 --essence-storage independent -i /fixtures/muxed/muxed.ts
# AppNote 0001: a demultiplexed ingest produces a Flow per essence plus the
# Multi-Flow recording that they came from one input. The collector is the Flow
# that stands for the input, so it is what the result names.
independent_collector="$(jq -ser '.[] | select(.type == "input.finished") | .payload.root_flow_id' "$fixtures/independent-essences.events.jsonl")"
jq -s -e '[.[] | select(.type == "flow.result") | .payload.flow_id] |
  length >= 3 and length == (unique | length)' \
  "$fixtures/independent-essences.events.jsonl" >/dev/null || {
  printf 'e2e: independent storage should report a Flow per essence plus the collector\n' >&2
  exit 1
}
for flow_id in $(result_flow_ids "$fixtures/independent-essences.events.jsonl"); do
  assert_flow_conformance independent-essences "$flow_id"
  if [ "$flow_id" = "$independent_collector" ]; then
    # The collector records the association and owns no media of its own: its
    # content is reached through the essences it collects.
    get_flow "$flow_id" \
      | jq -e '.format == "urn:x-nmos:format:multi" and ((.flow_collection // []) | length >= 2) and ((has("container")) | not)' >/dev/null || {
      printf 'e2e: collector %s must collect the essences and declare no container\n' "$flow_id" >&2
      exit 1
    }
    continue
  fi
  # An independently stored essence owns its Objects, so it declares a container
  # and has no multiplex to map into.
  get_flow "$flow_id" | jq -e 'has("container") and (has("container_mapping") | not) and (has("flow_collection") | not)' >/dev/null || {
    printf 'e2e: independently stored Flow %s must declare a container and carry no container_mapping\n' "$flow_id" >&2
    exit 1
  }
done

run_ingest muxed-essences 1 --essence-storage muxed -i /fixtures/muxed/muxed.ts
muxed_flow="$(jq -ser '.[] | select(.type == "input.finished") | .payload.root_flow_id' "$fixtures/muxed-essences.events.jsonl")"
assert_flow_conformance muxed-essences "$muxed_flow"
# The collector owns the Objects; each parent Collection Item maps one child to
# a track inside them, while the collected Flow has no container of its own.
get_flow "$muxed_flow" | jq -e '.format == "urn:x-nmos:format:multi" and ((.flow_collection // []) | length >= 2)' >/dev/null || {
  printf 'e2e: muxed storage should produce a multi-essence Flow collecting its essences\n' >&2
  exit 1
}

rm -f "$fixtures/stdin.events.jsonl"
cat "$fixtures/single/demo.ts" \
  | docker run --rm -i --network host "${docker_host_args[@]}" \
      -e TAMSIN_ENDPOINT -e TAMSIN_AUTH_MODE -e TAMSIN_AUTH_TOKEN \
      -e TAMSIN_HTTP_INSECURE_SKIP_VERIFY -e TAMSIN_LOG_LEVEL -e TAMSIN_FORMAT \
      "$IMAGE" --profile essence-segments -i - --stdin-name event.ts >"$fixtures/stdin.events.jsonl"
assert_ingest_artifacts stdin 1
# The same media and treatment must resume the local-file graph through stdin.
jq -s -e 'all(.[] | select(.type == "input.finished") | .payload;
  .status == "resumed" and .verification == "verified")' "$fixtures/stdin.events.jsonl" >/dev/null || {
  cat "$fixtures/stdin.events.jsonl" >&2
  exit 1
}

s3_secret="$(kubectl --kubeconfig "$kubeconfig" -n tams get storagebackend tams-storage-default -o 'jsonpath={.spec.credentials.existingSecret}')"
s3_secret="${s3_secret:-tams-s3-creds}"
AWS_ACCESS_KEY_ID="$(secret_value tams "$s3_secret" RUSTFS_ACCESS_KEY accesskey)"
AWS_SECRET_ACCESS_KEY="$(secret_value tams "$s3_secret" RUSTFS_SECRET_KEY secretkey)"
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION=us-east-1
if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
  echo "TAMOSS S3 credentials are empty" >&2
  exit 1
fi
aws_dist="$(dirname "$(cd "$tamoss" && aqua which aws)")"
docker run --rm --network host "${docker_host_args[@]}" \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION \
  -v "$aws_dist:/aws:ro" --entrypoint /aws/aws "$IMAGE" \
  --endpoint-url "$S3_URL" --no-verify-ssl s3 mb s3://tamsin-inputs 2>/dev/null || true
docker run --rm --network host "${docker_host_args[@]}" \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION \
  -v "$aws_dist:/aws:ro" -v "$fixtures:/fixtures:ro" --entrypoint /aws/aws "$IMAGE" \
  --endpoint-url "$S3_URL" --no-verify-ssl s3 cp /fixtures/s3/s3.ts s3://tamsin-inputs/s3.ts >/dev/null
for name in s3 s3-resume; do
  docker run --rm --network host "${docker_host_args[@]}" \
    -e TAMSIN_ENDPOINT -e TAMSIN_AUTH_MODE -e TAMSIN_AUTH_TOKEN \
    -e TAMSIN_HTTP_INSECURE_SKIP_VERIFY -e TAMSIN_LOG_LEVEL -e TAMSIN_FORMAT \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION \
    "$IMAGE" --profile essence-segments --input-mode stream -i s3://tamsin-inputs/ --s3-endpoint "$S3_URL" --s3-path-style >"$fixtures/$name.events.jsonl"
  assert_ingest_artifacts "$name" 1
  jq -s -e 'all(.[] | select(.type == "input.finished") | .payload; has("sha256") | not)' "$fixtures/$name.events.jsonl" >/dev/null
done
jq -s -e 'all(.[] | select(.type == "input.finished") | .payload; .status == "ingested")' "$fixtures/s3.events.jsonl" >/dev/null
jq -s -e 'all(.[] | select(.type == "input.finished") | .payload; .status == "resumed")' "$fixtures/s3-resume.events.jsonl" >/dev/null

# Exercise URL-token acquisition exactly as specified by TAMS 8.1.
TAMSIN_ENDPOINT="$API_URL?access_token=$TAMSIN_AUTH_TOKEN" TAMSIN_AUTH_MODE=url-token \
  docker run --rm --network host "${docker_host_args[@]}" \
    -e TAMSIN_ENDPOINT -e TAMSIN_AUTH_MODE -e TAMSIN_HTTP_INSECURE_SKIP_VERIFY -e TAMSIN_FORMAT \
    "$IMAGE" doctor --online >/dev/null

# Exercise OAuth2 client credentials against TAMOSS's managed Authentik endpoint.
oauth_secret="$(kubectl --kubeconfig "$kubeconfig" -n tams get tamoss tamoss-kind -o 'jsonpath={.status.resolved.generatedSecrets.oauth2Credentials}')"
oauth_secret="${oauth_secret:-tams-oauth2-creds}"
TAMSIN_AUTH_CLIENT_ID="$(secret_value tams "$oauth_secret" TAMOSS_OAUTH_CLIENT_ID client_id)"
TAMSIN_AUTH_CLIENT_SECRET="$(secret_value tams "$oauth_secret" TAMOSS_OAUTH_CLIENT_SECRET client_secret)"
TAMSIN_AUTH_TOKEN_URL="$AUTH_URL/application/o/token/"
export TAMSIN_AUTH_CLIENT_ID TAMSIN_AUTH_CLIENT_SECRET TAMSIN_AUTH_TOKEN_URL
TAMSIN_AUTH_MODE=oauth-client docker run --rm --network host "${docker_host_args[@]}" \
  -e TAMSIN_ENDPOINT -e TAMSIN_AUTH_MODE -e TAMSIN_AUTH_CLIENT_ID -e TAMSIN_AUTH_CLIENT_SECRET -e TAMSIN_AUTH_TOKEN_URL \
  -e TAMSIN_HTTP_INSECURE_SKIP_VERIFY -e TAMSIN_FORMAT \
  "$IMAGE" doctor --online >/dev/null

printf 'TAMS %s end-to-end matrix passed with profile %s: local, deterministic resume, directory, manifest, HTTP, stdin, S3, whole-file, MPEG-TS segments, bearer, URL token, and OAuth client credentials.\nFlows read back from the live service satisfied AppNote 0003 tag naming, AppNote 0006 container and collection rules, and operator-owned generation handling.\n' \
  "$TAMS_VERSION" "$PROFILE"
