# Configuration

TAMSin resolves settings in this order:

1. command-line flags;
2. non-empty `TAMSIN_*` environment variables;
3. YAML configuration;
4. built-in defaults.

The default file is `$XDG_CONFIG_HOME/tamsin/config.yaml`. If
`XDG_CONFIG_HOME` is unset, TAMSin uses the platform configuration directory.
Use `--config PATH` or `TAMSIN_CONFIG` to select another file. An explicitly
selected missing file is an error; an absent default file is not.

Unknown keys, duplicate keys, wrong value types and multiple YAML documents are
rejected before source resolution or TAMS mutation. `doctor` checks the same
effective configuration as ingest.

## Example

```yaml
endpoint: https://tams.example.com
format: human
progress: auto
color: auto
quiet: false
verbose: false

ingest:
  profile: essence-segments
  concurrency: 4
  transfers: 4
  probe_concurrency: 2
  segment_duration: 10s
  segment_format: source
  essence_storage: independent
  verify: auto
  max_inputs: 10000
  staging_byte_budget: auto
  start: "0:0"
  temp_directory: ""
  storage_id: ""
  source_id: ""
  flow_id: ""
  flow_metadata: ""
  tams_flow_profiles: []
  dry_run: "off"

input: []

media:
  ffprobe: ffprobe
  ffmpeg: ffmpeg
  ffmpeg_args: []

source:
  stdin_name: stdin.bin
  http_headers: []
  s3_endpoint: ""
  s3_region: ""
  s3_path_style: false

http:
  retries: 3
  timeout: 30s
  transfer_idle_timeout: 1m
  transfer_timeout: 0
  insecure_skip_verify: false

log:
  level: info
  format: text

auth:
  mode: auto
  allow_insecure_loopback: false
  username: ""
  client_id: ""
  token_url: ""
  redirect_url: http://127.0.0.1:53682/callback
  scopes: []
```

Durations require units, except that integer `0` disables an optional timeout
or segmentation. Lists must be YAML lists of strings.

Every key has an environment form made by uppercasing it, replacing dots with
underscores, and adding `TAMSIN_`. Examples include:

| YAML key | Environment variable | Flag |
| --- | --- | --- |
| `endpoint` | `TAMSIN_ENDPOINT` | `--endpoint` |
| `ingest.profile` | `TAMSIN_INGEST_PROFILE` | `--profile` |
| `ingest.segment_duration` | `TAMSIN_INGEST_SEGMENT_DURATION` | `--segment-duration` |
| `media.ffmpeg_args` | `TAMSIN_MEDIA_FFMPEG_ARGS` | `--ffmpeg-arg` |
| `source.s3_endpoint` | `TAMSIN_SOURCE_S3_ENDPOINT` | `--s3-endpoint` |
| `http.timeout` | `TAMSIN_HTTP_TIMEOUT` | `--timeout` |
| `auth.mode` | `TAMSIN_AUTH_MODE` | `--auth` |

List-valued environment variables use whitespace-separated values or a JSON
array when an item contains spaces. Repeat the corresponding flag where the CLI
permits it.

## Secrets

Prefer environment variables or workload secret injection for credentials:

- `TAMSIN_AUTH_TOKEN`
- `TAMSIN_AUTH_URL_TOKEN`
- `TAMSIN_AUTH_USERNAME` and `TAMSIN_AUTH_PASSWORD`
- `TAMSIN_AUTH_CLIENT_ID` and `TAMSIN_AUTH_CLIENT_SECRET`
- `TAMSIN_AUTH_CODE` and `TAMSIN_AUTH_PKCE_VERIFIER`
- standard AWS credential-chain variables for S3

`source.http_headers` and `media.ffmpeg_args` may also contain credentials.
Keep configuration files private and do not log their contents. TAMSin redacts
known credentials and URL queries from its own diagnostics, but cannot make an
unsafe external FFmpeg argument harmless.
