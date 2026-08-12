#!/usr/bin/env bash
set -Eeuo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
contract_name="${TAMSIN_E2E_CONTRACT:-tams-v8.2.json}"
case "$contract_name" in
  tams-v8.1.json|tams-v8.2.json) ;;
  *) echo "Unsupported TAMS E2E contract: $contract_name" >&2; exit 2 ;;
esac
contract="$root/contracts/$contract_name"

require() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Required command not found: $1" >&2
    exit 2
  }
}

# The conformance matrix is the single source of truth for the live service
# exercised here. jq is needed before the rest of the E2E toolchain is
# bootstrapped, so check it before attempting to read any value through it.
require jq
contract_value() {
  local selector="$1"
  jq -er "$selector | strings | select(length > 0)" "$contract"
}
TAMOSS_COMMIT="$(contract_value '.tamoss.commit')"
TAMS_COMMIT="$(contract_value '.tams.commit')"
TAMOSS_RELEASE="$(contract_value '.tamoss.release')"
PROFILE="$(contract_value '.tamoss.profile')"

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
journal_dir="$fixtures/journals"
http_pid=""

# capture_diagnostics records what the cluster thought was happening when a run
# failed. Every CI failure so far has been diagnosed by inference, because the
# cluster is torn down before anyone can look at it -- which cannot distinguish
# a workload that was stuck from one that was merely slow. TAMOSS ships the
# collector its own CI uses, and it is redacted.
capture_diagnostics() {
  # Deliberately outside the fixtures directory, which cleanup removes on a
  # failure that is not being preserved -- which is every CI failure.
  local bundle="$root/.tmp/e2e-support"
  [ -f "$kubeconfig" ] || return 0
  [ -f "$tamoss/scripts/support_bundle.py" ] || return 0
  mkdir -p "$bundle"
  # Collected first and unconditionally, because the support bundle stops when
  # its primary namespace is absent -- and a run that fails before the instance
  # is applied has no tams namespace at all, which is precisely the case that
  # produced an empty bundle and taught us this.
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

for command in aqua base64 curl docker jq kubectl python3 tar; do
  require "$command"
done

# A failed run is preserved for inspection when TAMSIN_E2E_KEEP_CLUSTER is set,
# so the next run starts by clearing it. Inheriting it is worse than useless:
# the HTTP fixture server publishes its port to a file, and the wait loop takes
# a non-empty file as proof the server is up. A port left behind by a previous
# run satisfies that immediately, and the run then fails against a port nothing
# is listening on -- five hours after the process that opened it exited.
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


# kind:up rebuilds the TAMOSS operator image, side-loads it into the node, and
# then rolls the Deployment so the running pod adopts an image whose tag has not
# changed. On a fresh cluster that restart is a no-op -- the pod was created
# after the load and is already running exactly that image -- but it is not free.
# The Deployment sets no strategy, so it takes the Kubernetes default, and 25%
# maxSurge rounds up to one for a single replica: the rollout wants a second
# 500m/256Mi pod scheduled before the first goes away.
#
# On a two-core runner already holding Postgres, Authentik, rustfs, Traefik,
# cert-manager and the TAMS workloads, there is no room for it. The new pod
# stays Pending, the old one waits for a replacement that never arrives, and the
# rollout times out on "1 old replicas are pending termination" -- which is
# exactly how this failed twice in CI while passing locally.
#
# Replacing in place removes the need for that headroom. The restart still
# happens and still has to succeed; it just stops requiring room for two.
python3 - "$tamoss/operator/config/manager/manager.yaml" <<'EOF'
import sys

path = sys.argv[1]
with open(path) as handle:
    documents = handle.read().split("\n---\n")

# Written as a text edit rather than through a YAML library, because the file is
# TAMOSS's and round-tripping it would reformat everything around the change.
for index, document in enumerate(documents):
    if "\nkind: Deployment\n" not in document:
        continue
    if "\n  strategy:\n" in document:
        break
    documents[index] = document.replace(
        "\n  replicas: 1\n",
        "\n  replicas: 1\n"
        "  strategy:\n"
        "    type: RollingUpdate\n"
        "    rollingUpdate:\n"
        "      maxSurge: 0\n"
        "      maxUnavailable: 1\n",
        1,
    )
    break
else:
    raise SystemExit("no Deployment found in the operator manifest")

with open(path, "w") as handle:
    handle.write("\n---\n".join(documents))
EOF

# The local-kind task always builds and loads the UI image even when the
# instance disables the UI. On GitHub's runner that dead image, plus the build
# cache for all three TAMOSS images and Tamsin itself, left too little space in
# the Kind node to unpack PostgreSQL. Patch the pinned harness narrowly: omit
# only the two UI image commands. On CI, also release unreferenced builder cache
# after the API/operator images have been side-loaded; local retries retain it
# unless TAMSIN_E2E_PRUNE_BUILDER_CACHE explicitly requests pruning. Retained
# images are never pruned; the matrix uses Tamsin from the host and TAMOSS from
# Kind.
python3 - "$tamoss/.tasks/kind.yaml" "$prune_builder_cache" <<'EOF'
import sys

path = sys.argv[1]
prune_builder_cache = sys.argv[2] == "true"
with open(path) as handle:
    contents = handle.read()

ui_commands = (
    '        task_kind_build_image "TAMOSS UI" "{{.UI_IMAGE}}" "" "src/app/frontend"\n',
    '        task_kind_load_image "{{.PROJECT_NAME}}" "TAMOSS UI" "{{.UI_IMAGE}}"\n',
)
create_end = '          OPERATOR_IMAGE: "{{.OPERATOR_IMAGE}}"\n\n  delete:'
patched_end = (
    '          OPERATOR_IMAGE: "{{.OPERATOR_IMAGE}}"\n'
    '      - docker builder prune --all --force\n\n'
    '  delete:'
)
command_counts = [contents.count(command) for command in ui_commands]
if command_counts == [1, 1]:
    for command in ui_commands:
        contents = contents.replace(command, "", 1)
elif command_counts != [0, 0]:
    raise SystemExit(f"pinned TAMOSS kind task has partial UI commands: counts={command_counts}")

original_end_count = contents.count(create_end)
patched_end_count = contents.count(patched_end)
if original_end_count + patched_end_count != 1:
    raise SystemExit(
        "pinned TAMOSS kind task has an unexpected create-task ending: "
        f"original={original_end_count}, patched={patched_end_count}"
    )
if prune_builder_cache and original_end_count == 1:
    contents = contents.replace(create_end, patched_end, 1)
elif not prune_builder_cache and patched_end_count == 1:
    contents = contents.replace(patched_end, create_end, 1)

with open(path, "w") as handle:
    handle.write(contents)
EOF

# The instance is configured before it is first applied, rather than reconciled
# once with defaults and then patched into shape. Two reconciles is twice the
# work for a result that was known before the cluster existed, and on a small
# runner that is not free.
#
# The UI is switched off with it. Tamsin talks to the TAMS API and never to the
# UI, so deploying it spends a Deployment, an ingress route and their share of a
# constrained node on something no test touches.
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
# Written whole rather than appended to, so a cached checkout reused across runs
# does not accumulate the patch entry.
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
api_ready=false
for _ in {1..90}; do
  if curl --ipv4 --resolve "api.tamoss.localtest.me:$HTTPS_PORT:127.0.0.1" \
    --insecure --fail --silent "${API_URL%/}/readyz" >/dev/null 2>&1; then
    api_ready=true
    break
  fi
  sleep 2
done
if [ "$api_ready" != true ]; then
  echo "Timed out waiting for TAMOSS API readiness at ${API_URL%/}/readyz" >&2
  exit 1
fi

mkdir -p "$fixtures/single" "$fixtures/directory" "$fixtures/manifest-media" "$fixtures/list" "$fixtures/http" "$fixtures/s3" "$journal_dir"
# The image runs as its non-root TAMSin user. Give that user a dedicated test
# directory for exclusively-created, mode-0600 journals while all media stays
# mounted read-only.
chmod 0777 "$journal_dir"
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
# The TAMOSS demo clip carries video only, so on its own it exercises neither
# essence-storage arrangement: a single-essence input produces one Flow either
# way. Add a synthetic audio track to the real video to make a genuine
# multiplex. FFmpeg comes from the image under test rather than the host.
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
python3 -u - "$fixtures/http" "$port_file" >"$fixtures/http.log" 2>&1 <<'PY' &
import functools
import http.server
import pathlib
import sys

handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=sys.argv[1])
with http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler) as server:
    pathlib.Path(sys.argv[2]).write_text(str(server.server_port), encoding="utf-8")
    server.serve_forever()
PY
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
  rm -f "$fixtures/$name.events.jsonl" "$journal_dir/$name.jsonl" "$fixtures/$name.json"
  docker run --rm --network host "${docker_host_args[@]}" \
    -e TAMSIN_ENDPOINT -e TAMSIN_AUTH_MODE -e TAMSIN_AUTH_TOKEN \
    -e TAMSIN_HTTP_INSECURE_SKIP_VERIFY -e TAMSIN_LOG_LEVEL -e TAMSIN_FORMAT \
    -v "$fixtures:/fixtures:ro" -v "$journal_dir:/journals" \
    "$IMAGE" --profile essence-segments --journal "/journals/$name.jsonl" "$@" >"$fixtures/$name.events.jsonl" || {
      status=$?
      cat "$fixtures/$name.events.jsonl" >&2
      return "$status"
  }
  assert_ingest_artifacts "$name" "$expected"
}

# Machine ingest stdout is the live NDJSON process protocol. The independently
# synced journal retains the complete terminal Results used by the rest of this
# live-service conformance matrix. Validate both projections, then build a
# finite result document from the journal for the existing graph assertions.
assert_ingest_artifacts() {
  local name="$1"
  local expected="$2"
  jq -s -e --argjson expected "$expected" '
    length > 0 and
    .[0].type == "hello" and .[-1].type == "run.finished" and
    ([.[].seq] == [range(0; length)]) and
    ([.[].run_id] | unique | length) == 1 and
    all(.[]; .protocol == "tamsin.ingest.events" and .protocol_version == "2.1") and
    ([.[] | select(.type == "input.finished")] | length) == $expected and
    (.[-1].payload.outcome == "succeeded" and
     .[-1].payload.exit_code == 0 and
     .[-1].payload.total == $expected and
     .[-1].payload.succeeded == $expected and
     .[-1].payload.failed == 0)
  ' "$fixtures/$name.events.jsonl" >/dev/null || {
    cat "$fixtures/$name.events.jsonl" >&2
    return 1
  }
  if LC_ALL=C grep -q $'\r\|\033' "$fixtures/$name.events.jsonl"; then
    printf 'e2e: %s event stream contains a carriage return or terminal escape\n' "$name" >&2
    return 1
  fi
  docker run --rm -v "$journal_dir:/journals:ro" --entrypoint cat "$IMAGE" "/journals/$name.jsonl" \
    | jq -s -e '
    . as $records |
    (first($records[] | select(.record_type == "start"))) as $start |
    (first($records[] | select(.record_type == "summary"))) as $terminal |
    {
      schema_version: $start.schema_version,
      tool_version: $start.tool_version,
      tool_commit: $start.tool_commit,
      tool_build_date: $start.tool_build_date,
      profile_version: $start.profile_version,
      run_id: $start.run_id,
      total: $terminal.summary.total,
      succeeded: $terminal.summary.succeeded,
      failed: $terminal.summary.failed,
      results: (
        [$records[] | select(.record_type == "input")]
        | sort_by(.index)
        | map(
            . as $input
            | .result
            | .flows = [
                .flows[] as $flow
                | $flow + {
                    objects: [
                      $records[]
                      | select(
                          .record_type == "object" and
                          .index == $input.index and
                          .flow_id == $flow.flow_id
                        )
                      | .object
                    ]
                  }
              ]
          )
      )
    }
  ' >"$fixtures/$name.json" || {
    docker run --rm -v "$journal_dir:/journals:ro" --entrypoint cat "$IMAGE" "/journals/$name.jsonl" >&2
    return 1
  }
  jq -e --argjson expected "$expected" \
	'.schema_version == "2.1" and ((.tool_version | length) > 0) and ((.tool_commit | length) > 0) and
	 ((.profile_version | length) > 0) and ((.run_id | length) > 0) and
	 .failed == 0 and .succeeded == $expected and (.results | length) == $expected and
	 all(.results[]; ((.profile | length) > 0) and ((.profile_version | length) > 0) and
	     (.status == "ingested" or .status == "resumed") and .verification == "verified" and
	     all(.flows[];
	       (.disposition == "written" or .disposition == "unchanged") and
	       all((.objects // [])[];
	         (.disposition == "ingested" or .disposition == "resumed") and
	         .verification_status == "verified"
	       )
	     ) and
	     ((((.flows | map((.objects // []) | length) | add) // 0) > 0)))' \
    "$fixtures/$name.json" >/dev/null || {
      cat "$fixtures/$name.json" >&2
      return 1
    }
}
api_component() {
  jq -rn --arg value "$1" '$value | @uri'
}

# Live conformance assertions use the API directly. They verify what the
# service retained without making TAMSin carry a general control-plane CLI.
api_request() {
  local method="$1" path="$2"
  curl --ipv4 \
    --resolve "api.tamoss.localtest.me:$HTTPS_PORT:127.0.0.1" \
    --insecure --fail --silent --show-error \
    --request "$method" \
    --header "Authorization: Bearer $TAMSIN_AUTH_TOKEN" \
    --header 'Accept: application/json' \
    "${API_URL%/}/$path"
}

# capture_api preserves the concise resource-oriented calls used throughout
# this test while translating them to read-only HTTP requests.
capture_api() {
  local resource="$1" action="$2" id="$3"
  shift 3
  case "$resource $action" in
    'flow get')
      api_request GET "flows/$(api_component "$id")"
      ;;
    'segment list')
      local query='limit=1000&accept_get_urls=&presigned=false' object_id=''
      while [ "$#" -gt 0 ]; do
        case "$1" in
          --object-id)
            object_id="$2"
            shift 2
            ;;
          *)
            printf 'e2e: unsupported Segment-list argument %s\n' "$1" >&2
            return 2
            ;;
        esac
      done
      if [ -n "$object_id" ]; then
        query="$query&object_id=$(api_component "$object_id")"
      fi
      api_request GET "flows/$(api_component "$id")/segments?$query"
      ;;
    *)
      printf 'e2e: unsupported direct API operation %s %s\n' "$resource" "$action" >&2
      return 2
      ;;
  esac
}

retract_segment() {
  local flow_id="$1" timerange="$2" object_id="$3"
  api_request DELETE \
    "flows/$(api_component "$flow_id")/segments?timerange=$(api_component "$timerange")&object_id=$(api_component "$object_id")" \
    >/dev/null

  local segments
  for _ in {1..120}; do
    segments="$(capture_api segment list "$flow_id" --object-id "$object_id")" || return 1
    if jq -e --arg timerange "$timerange" --arg object_id "$object_id" \
      'all(.[]; .timerange != $timerange or .object_id != $object_id)' <<<"$segments" >/dev/null; then
      return 0
    fi
    sleep 0.25
  done
  printf 'e2e: Segment %s remained on Flow %s after retraction\n' "$object_id" "$flow_id" >&2
  return 1
}

# result_flow_ids yields every Flow an ingest produced. Both storage
# arrangements use the same result shape and list each Flow once.
result_flow_ids() {
  jq -er '.results[].flows[].flow_id' "$1"
}

# result_flow_objects pairs every reported Media Object with the Flow that owns
# it. Pairing matters: a batch ingest produces several Flows, and an Object
# only belongs to one of them.
result_flow_objects() {
  jq -r '.results[]
         | .flows[]
         | .flow_id as $flow
         | (.objects // [])[]
         | "\($flow)\t\(.object_id)"' "$1"
}

# assert_flow_conformance reads a Flow back out of the live service and checks
# it against the pinned TAMS contract. Asserting tamsin's own output only proves
# it is self-consistent; these read what TAMOSS actually stored.
#
# Each check names the specification text it enforces so a failure points at the
# rule rather than at a bare jq expression.
assert_flow_conformance() {
  local name="$1" flow_id="$2"
  local flow
  flow="$(capture_api flow get "$flow_id")" || {
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
  own_segments="$(capture_api segment list "$flow_id")" || return 1
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
      collected="$(capture_api flow get "$collected_id")" || {
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
  for object_id in $(result_flow_objects "$fixtures/$name.json" | awk -F'\t' -v flow="$flow_id" '$1 == flow {print $2}'); do
    jq -e --arg id "$object_id" 'any(.[]; .object_id == $id)' <<<"$segments" >/dev/null || {
      printf 'e2e: %s Object %s is owned by Flow %s but is not registered as one of its Segments\n' \
        "$name" "$object_id" "$flow_id" >&2
      return 1
    }
  done
}


run_ingest local-file 1 -i /fixtures/single/demo.ts
run_ingest local-file-resume 1 -i /fixtures/single/demo.ts
jq -e '.results[0].status == "resumed" and .results[0].verification == "verified" and
       all(.results[0].flows[]; all((.objects // [])[]; .disposition == "resumed"))' \
  "$fixtures/local-file-resume.json" >/dev/null
for flow_id in $(result_flow_ids "$fixtures/local-file.json"); do
  assert_flow_conformance local-file "$flow_id"
done
run_ingest directory 2 -i /fixtures/directory
for flow_id in $(result_flow_ids "$fixtures/directory.json"); do
  assert_flow_conformance directory "$flow_id"
done
run_ingest manifest 2 -i /fixtures/list/sources.txt
run_ingest http 1 -i "$http_url"
for flow_id in $(result_flow_ids "$fixtures/http.json"); do
  assert_flow_conformance http "$flow_id"
done

# Segments are cut at a 10s target by default, so the matrix also covers an
# explicit whole-file Object and an explicit container, which take different
# paths through Flow metadata.
run_ingest whole-file 1 -d 0 -i /fixtures/single/demo.ts
for flow_id in $(result_flow_ids "$fixtures/whole-file.json"); do
  assert_flow_conformance whole-file "$flow_id"
done
jq -e '.results[0] as $result | $result.flows[] | select(.flow_id == $result.root_flow_id) | .objects | length == 1' \
  "$fixtures/whole-file.json" >/dev/null

run_ingest mpegts-format 1 --segment-format mpegts -i /fixtures/single/demo.ts
mpegts_flow="$(jq -er '.results[0].root_flow_id' "$fixtures/mpegts-format.json")"
for flow_id in $(result_flow_ids "$fixtures/mpegts-format.json"); do
  assert_flow_conformance mpegts-format "$flow_id"
done
# The Flow must declare the container that was written, not the input's.
capture_api flow get "$mpegts_flow" | jq -e '.container == "video/mp2t"' >/dev/null || {
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
independent_collector="$(jq -er '.results[0].root_flow_id' "$fixtures/independent-essences.json")"
jq -e '(.results[0].flows | length) >= 3 and ((.results[0].root_flow_id // "") | length) > 0 and
       ([.results[0].flows[].flow_id] | length == (unique | length))' \
  "$fixtures/independent-essences.json" >/dev/null || {
  printf 'e2e: independent storage should report a Flow per essence plus the collector\n' >&2
  exit 1
}
for flow_id in $(result_flow_ids "$fixtures/independent-essences.json"); do
  assert_flow_conformance independent-essences "$flow_id"
  if [ "$flow_id" = "$independent_collector" ]; then
    # The collector records the association and owns no media of its own: its
    # content is reached through the essences it collects.
    capture_api flow get "$flow_id" \
      | jq -e '.format == "urn:x-nmos:format:multi" and ((.flow_collection // []) | length >= 2) and ((has("container")) | not)' >/dev/null || {
      printf 'e2e: collector %s must collect the essences and declare no container\n' "$flow_id" >&2
      exit 1
    }
    continue
  fi
  # An independently stored essence owns its Objects, so it declares a container
  # and has no multiplex to map into.
  capture_api flow get "$flow_id" | jq -e 'has("container") and (has("container_mapping") | not) and (has("flow_collection") | not)' >/dev/null || {
    printf 'e2e: independently stored Flow %s must declare a container and carry no container_mapping\n' "$flow_id" >&2
    exit 1
  }
done

run_ingest muxed-essences 1 --essence-storage muxed -i /fixtures/muxed/muxed.ts
muxed_flow="$(jq -er '.results[0].root_flow_id' "$fixtures/muxed-essences.json")"
assert_flow_conformance muxed-essences "$muxed_flow"
# The collector owns the Objects; each parent Collection Item maps one child to
# a track inside them, while the collected Flow has no container of its own.
capture_api flow get "$muxed_flow" | jq -e '.format == "urn:x-nmos:format:multi" and ((.flow_collection // []) | length >= 2)' >/dev/null || {
  printf 'e2e: muxed storage should produce a multi-essence Flow collecting its essences\n' >&2
  exit 1
}

# Retraction is how a Segment that fails verification is withdrawn, so the
# delete path is exercised against the live service rather than only in unit
# tests. TAMS also removes any Media Object left unreferenced.
retract_flow="$(jq -er '.results[0].root_flow_id' "$fixtures/whole-file.json")"
retract_timerange="$(jq -er '.results[0] as $result | $result.flows[] | select(.flow_id == $result.root_flow_id) | .objects[0].timerange' "$fixtures/whole-file.json")"
retract_object="$(jq -er '.results[0] as $result | $result.flows[] | select(.flow_id == $result.root_flow_id) | .objects[0].object_id' "$fixtures/whole-file.json")"
retract_segment "$retract_flow" "$retract_timerange" "$retract_object"
# Retraction is terminal only once the exact Object/timerange tuple is absent.
capture_api segment list "$retract_flow" --object-id "$retract_object" | jq -e 'length == 0' >/dev/null || {
  printf 'e2e: Segment %s is still present after terminal retraction from Flow %s\n' \
    "$retract_object" "$retract_flow" >&2
  exit 1
}
rm -f "$fixtures/stdin.events.jsonl" "$journal_dir/stdin.jsonl" "$fixtures/stdin.json"
cat "$fixtures/single/demo.ts" \
  | docker run --rm -i --network host "${docker_host_args[@]}" \
      -e TAMSIN_ENDPOINT -e TAMSIN_AUTH_MODE -e TAMSIN_AUTH_TOKEN \
      -e TAMSIN_HTTP_INSECURE_SKIP_VERIFY -e TAMSIN_LOG_LEVEL -e TAMSIN_FORMAT \
      -v "$journal_dir:/journals" \
      "$IMAGE" --profile essence-segments --journal /journals/stdin.jsonl -i - --stdin-name event.ts >"$fixtures/stdin.events.jsonl"
assert_ingest_artifacts stdin 1
# This is deliberately the same media and treatment as local-file above.
# Locator-independent identity must therefore resume the existing graph even
# though a non-reopenable stdin stream supplied the bytes this time.
jq -e '.failed == 0 and .succeeded == 1 and .results[0].status == "resumed" and
       .results[0].verification == "verified"' "$fixtures/stdin.json" >/dev/null || {
  cat "$fixtures/stdin.json" >&2
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
rm -f "$fixtures/s3.events.jsonl" "$journal_dir/s3.jsonl" "$fixtures/s3.json"
docker run --rm --network host "${docker_host_args[@]}" \
  -e TAMSIN_ENDPOINT -e TAMSIN_AUTH_MODE -e TAMSIN_AUTH_TOKEN \
  -e TAMSIN_HTTP_INSECURE_SKIP_VERIFY -e TAMSIN_LOG_LEVEL -e TAMSIN_FORMAT \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION \
  -v "$journal_dir:/journals" \
  "$IMAGE" --profile essence-segments --journal /journals/s3.jsonl -i s3://tamsin-inputs/ --s3-endpoint "$S3_URL" --s3-path-style >"$fixtures/s3.events.jsonl"
assert_ingest_artifacts s3 1
jq -e '.failed == 0 and .succeeded == 1 and .results[0].status == "ingested"' "$fixtures/s3.json" >/dev/null

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

printf 'TAMOSS %s end-to-end matrix passed with profile %s: local, deterministic resume, directory, manifest, HTTP, stdin, S3, whole-file, MPEG-TS segments, Segment retraction, bearer, URL token, and OAuth client credentials.\nFlows read back from the live service satisfied AppNote 0003 tag naming, AppNote 0006 container and collection rules, and operator-owned generation handling.\n' \
  "$TAMOSS_RELEASE" "$PROFILE"
