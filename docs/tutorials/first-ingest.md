# Your first ingest

By the end of this tutorial you will have put a piece of media into a
Time-addressable Media Store, verified its stored bytes, and reviewed the
ingest receipt using a local file and a terminal.

You will need TAMSin on your `PATH`, FFmpeg 5.1 or newer installed, and a TAMS
endpoint you can write to.

## 1. Check your tools

TAMSin drives FFmpeg to inspect and cut media, so start by confirming both are present:

```sh
tamsin doctor --profile essence-segments
```

You should see a check-oriented report naming the TAMSin/Go/platform and the
resolved profile, followed by `configuration`, `profile`, `staging`, `ffprobe`,
and `ffmpeg` checks. If `ffprobe` fails, install FFmpeg before going further —
TAMSin cannot ingest anything without it. The `essence-segments@1` treatment requires
`ffmpeg`; a whole-file `preserve@1` treatment reports that check
as skipped because it does not render media.

## 2. Point TAMSin at your store

TAMSin can read credentials from the environment, avoiding secret command-line
arguments. Typing a literal export can still record the secret in shell
history. In Bash, prompt for the token without echoing it:

```bash
export TAMSIN_ENDPOINT='https://tams.example.com'
export TAMSIN_AUTH_MODE='bearer'
read -r -s -p 'TAMS token: ' TAMSIN_AUTH_TOKEN
printf '\n'
export TAMSIN_AUTH_TOKEN
```

Confirm TAMSin can reach the store and that your credentials work:

```sh
tamsin doctor --profile essence-segments --online
```

The redacted `endpoint` and `auth` fields tell you which destination and
authentication mode were selected. Passing `authentication`, `service`,
`api_compatibility`, `service_lifetimes`, `storage_backends`, and
`storage_selection` checks mean the same read-only startup preflight used by
ingest succeeded. It reads the service and paginated storage backends without
creating a TAMS resource. If this
fails, fix it now — every later step depends on it.

## 3. Make a piece of media

So the tutorial is self-contained, generate a short clip rather than hunting for one. This makes four seconds of colour bars with a tone:

```sh
ffmpeg -f lavfi -i "testsrc=size=320x240:rate=25" \
       -f lavfi -i "sine=frequency=440" \
       -t 4 -c:v libx264 -g 25 -c:a aac first-ingest.ts
```

You now have `first-ingest.ts`, containing a video track and an audio track together in one file.

## 4. Look before you leap

Before writing anything to the store, ask TAMSin what it *would* do:

```sh
tamsin --profile essence-segments --dry-run=exact -i first-ingest.ts
```

The mode value is required. `fast` stages and probes the source and validates
the Flow graph, but skips FFmpeg rendering and therefore cannot report exact
Objects. This tutorial uses `exact` because the next steps inspect those
Objects.

This performs the complete local render and prints a human-readable plan —
without contacting TAMS at all. It still stages, separates, segments, and
hashes the media, so budget the same temporary space and FFmpeg time as a real
run. You should see **three** Flows: `video`, `audio`, and a containerless
`collection` representing the input as a whole.

If you are building a wrapper or want to inspect the exact Object plan, capture
the live event stream instead:

```sh
tamsin --profile essence-segments --dry-run=exact --format json -i first-ingest.ts >plan.ndjson
jq -c 'select(.type == "flow.planned" or .type == "object.result" or .type == "input.finished")' plan.ndjson
```

`plan.ndjson` is a sequence of independently valid JSON objects, not an array
or one final document. Across those bounded records, the Flow plan has
**three** entries: two `flow.planned` payloads carry `"role": "video"` and
`"role": "audio"`, while `input.finished` names the containerless collector in
`root_flow_id`. Each `flow.planned` marks `root` or names `parent_flow_id`
without forcing TAMSin to render the whole input before publishing the graph.
Each terminal planned `object.result` carries its expected byte count and
checksum, and each `flow.result` closes the Flow with bounded Object counters.
The final `run.finished` event records the dry-run outcome and intended process
exit code.

That is worth pausing on. You gave TAMSin one file and it is planning three
Flows: two media-owning essence Flows plus the containerless Multi-Flow that records
their association. The selected `essence-segments` profile separates each
essence so a later consumer can fetch the audio without downloading the video.
See [profiles and supported media](../reference/profiles.md) for the storage
trade-offs.

This segmented treatment is not a byte-for-byte archive of the
container you supplied: separating and segmenting the essences rewrites their
containers even though the coded samples are copied. If preserving the exact
input file is the requirement, select the whole-file muxed profile instead.
Keep the explicit profile for this tutorial; in a preservation workflow this
would replace the ingest command in the next step:

```sh
tamsin --profile preserve -i first-ingest.ts
```

The equivalent explicit settings are
`--essence-storage muxed --segment-duration 0`. Both parts matter: muxed
storage alone may still segment and rewrite the container, while duration zero
with independent storage still has to demultiplex the essences.

## 5. Ingest it

Now do it for real:

```sh
tamsin --profile essence-segments --verbose -i first-ingest.ts
```

TAMSin separates the two essences, cuts each into Flow Segments, uploads them,
and registers them. Its default `auto` integrity policy accepts trustworthy
storage SHA-256 evidence when the upload provides it and otherwise downloads
the registered Object for a byte-for-byte check. Live progress shows `storing`
and `verifying` separately; the permanent receipt begins
`INGESTED AND VERIFIED` when that has all succeeded.

## 6. Review the result

The verbose receipt names the root `collection` Flow and its `video` and
`audio` children, with their identifiers and Object totals. The collection owns no
Objects in independent mode. Successful per-Object records are streamed rather
than retained for the human receipt. To keep every Object record, use
`--format json` and redirect stdout to an NDJSON file.

Each Object record includes a `timerange` such as `[0:0_4:0)` — from zero
seconds, up to but not including four — together with its byte count and
SHA-256 digest. The NDJSON stream therefore records exactly what this ingest
committed and verified without turning TAMSin into a general TAMS inspection
client. Later service-side inspection and administration use the tooling
provided for that service and are outside TAMSin's command interface.

## 7. Prove the retry is safe

Run exactly the same ingest again:

```sh
tamsin --profile essence-segments -i first-ingest.ts
```

The receipt now begins `RESUMED AND VERIFIED` rather than `INGESTED AND
VERIFIED`, and nothing was uploaded a second time. TAMSin derives its
identifiers from the content itself, so a repeated run recognises what is
already present and verifies it instead of duplicating it. This is what makes
TAMSin safe to put in a job that might be retried.

## What you did

You checked your tooling, planned an ingest without touching the store,
ingested a multiplexed file as two independently addressable essence Flows plus
their collector, reviewed the verified Objects in the permanent receipt, and
saw that re-running is safe.

From here:

- [Profiles and supported media](../reference/profiles.md) to choose a byte-packaging policy
- [Inputs](../reference/inputs.md) for directories, manifests, HTTP, S3 and standard input
- [CLI reference](../reference/cli.md) for every command and flag
