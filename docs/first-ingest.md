# Your first ingest

Create a short audio file, store it in TAMS and repeat the ingest to verify
that the stored Object is reused. This tutorial uses `preserve@1`, which stores
the original file unchanged. Its commands work with `8.2.0-in2` and this
development branch; complete the run with the same binary and settings.

## Prepare your environment

You need:

- The [TAMSin binary](../README.md#install) on your `PATH`.
- FFmpeg and FFprobe 5.1 or newer from the same maintained build. FFmpeg
  creates the sample; `preserve` itself needs only FFprobe.
- An evaluation TAMS 8.2 service, or a compatible 8.1 service, with storage
  configured and credentials allowed to create Flows and upload media.
- A Bash shell and a writable directory with space for the sample and
  temporary files.

If you need a service, complete the
[TAMOSS local evaluation guide](https://github.com/livewyer-ops/tamoss/blob/main/docs/getting-started/local-kind.md)
first. Obtain the API endpoint and credentials from that installation. A local
TAMOSS deployment requires its own tools; the TAMSin binary works with any
supported service reachable from your machine.

Use the API URL, including any path prefix, rather than a web console URL.
For a private certificate authority, install its CA certificate in your
client's trust store before continuing. Keep certificate verification enabled.

Set the endpoint and prompt for the bearer token without putting it in shell
history. Replace the example URL with your service's API URL:

```bash
export TAMSIN_ENDPOINT=https://tams.example.com
export TAMSIN_AUTH_MODE=bearer
read -r -s -p 'TAMS token: ' TAMSIN_AUTH_TOKEN
printf '\n'
export TAMSIN_AUTH_TOKEN
```

For another authentication scheme, use its
[credential variables](configuration.md#authentication). Remove unrelated
TAMSin environment settings and use an empty configuration file for this
lesson so that a previous job's settings do not affect it:

```sh
mkdir tamsin-first-ingest
cd tamsin-first-ingest
printf '{}\n' > config.yaml
export TAMSIN_CONFIG="$PWD/config.yaml"
tamsin --version
tamsin doctor --online --profile preserve@1
```

Doctor should finish successfully. It checks the service and local tools
without writing media. Resolve any failed check before continuing; see
[readiness checks](operations.md#check-readiness).

## Create and plan the sample

Generate twelve seconds of mono PCM audio:

```sh
ffmpeg -hide_banner -loglevel error \
  -f lavfi -i 'sine=frequency=440:sample_rate=48000:duration=12' \
  -c:a pcm_s16le -fflags +bitexact -flags:a +bitexact sample.wav
tamsin ingest --profile preserve@1 --dry-run=exact --input ./sample.wav
```

The receipt should say `PLANNED - NO CHANGES MADE` and report one Object.
No media has been uploaded. Keep this file unchanged for the remaining steps;
overwriting it may produce a different identity.

## Ingest and inspect the result

```sh
tamsin ingest --profile preserve@1 --verbose --input ./sample.wav
```

The receipt should say `INGESTED AND VERIFIED`. Record the Flow UUID from the
verbose receipt. In your store's console or API client, open that Flow and
inspect its Segments: this sample has one Object covering twelve seconds,
starting at `0:0`. Its container is `audio/wav`. The downloaded Object contains
the original WAV file; playback should produce a steady tone.

## Verify a retry

Run exactly the same ingest again:

```sh
tamsin ingest --profile preserve@1 --verbose --input ./sample.wav
```

The receipt should say `RESUMED AND VERIFIED`, with the same Flow and Object
identifiers. TAMSin reads the existing Object to verify its bytes and uploads
no duplicate. Each successful command exits `0`.

The media remains in the store after this lesson. Keep the recorded Flow UUID
if you want to inspect it later. To remove the test media, use your store's
documented deletion procedure for that Flow and its Segments; deleting the
local WAV does not remove stored Objects. Clear the tutorial environment when
finished:

```sh
unset TAMSIN_AUTH_TOKEN TAMSIN_AUTH_MODE TAMSIN_ENDPOINT TAMSIN_CONFIG
```

Continue with [packaging choices](profiles.md#named-profiles) for segmented
media, or [remote input](configuration.md#ingest-an-http-or-s3-input) for HTTP
and S3 sources. [Media, identity and verification](concepts.md) explains the
entities created by an ingest.
