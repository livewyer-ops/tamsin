# Choose how essences are stored

A file carrying video and audio together can be stored whole, demultiplexed, or
segmented in either arrangement. Pick by what consumers need to read afterwards.

## Keep complete essences separately addressable

Select `demux` when consumers need individual essences but do not need short
time-range reads:

```sh
tamsin --profile demux -i programme.ts -o https://tams.example.com
```

Each essence becomes its own Flow owning a complete Media Object. An empty
collector Flow records their association. This suits a transcription service
fetching audio or proxy generation fetching video without the other tracks.
It normally creates about one Object per essence and invokes FFmpeg only when
there is a multiplex to separate.

## Keep segmented essences separately addressable

Select `essence-segments` when consumers need both individual essences and
time-range access:

```sh
tamsin --profile essence-segments -i programme.ts -o https://tams.example.com
```

This creates the same Flow arrangement as `demux`, but targets ten-second
Objects. That reduces bytes fetched for a time-range read at the cost of roughly
360 Objects per essence-hour, with corresponding API and verification work.

Both independent profiles produce a receipt grouped under the empty collector:

```text
INGESTED AND VERIFIED  programme.ts

  collection 821256e5-9820-447e-b62e-66fac962ea1b
  video 9a69b060-143b-4d85-b3a8-a55343e9ed16 1 media object
  audio 34201935-4b67-4785-b24f-caf2889b25ca 1 media object
```

The Object count shown depends on the selected profile and the media duration.

## Keep the original multiplex byte-for-byte

```sh
tamsin --profile preserve -i programme.ts -o https://tams.example.com
```

The bytes are stored as they arrived, described by a root multi-essence Flow
that owns the Object and collects one Flow per track. The receipt lists each
Flow once; the root is labelled `flow` because it owns the multiplexed media:

```text
INGESTED AND VERIFIED  programme.ts

  flow c8f9cf7e-9ce2-4f74-aef5-5a90ac3665fd 1 media object
  video 9a69b060-143b-4d85-b3a8-a55343e9ed16
  audio 34201935-4b67-4785-b24f-caf2889b25ca
```

Choose this when you must demonstrate that what you hold is what you received.
It is the only profile that does not rewrite the container and normally creates
one Object per input. A reader wanting ten seconds must fetch the whole Object.

## Segment while keeping essences muxed

Select `muxed-segments` when consumers need time-range access but normally use
the complete multiplex:

```sh
tamsin --profile muxed-segments -i programme.ts -o https://tams.example.com
```

This targets ten-second Objects and avoids multiplying Object count by essence,
but an audio-only consumer still has to fetch the video. Segmentation rewrites
container bytes even though TAMSin stream-copies the coded essence.

## Set the choice permanently

In `config.yaml`:

```yaml
ingest:
  profile: preserve
```

Or in the environment, which suits a Kubernetes Job:

```sh
export TAMSIN_INGEST_PROFILE=preserve
```

Use the named profile alone unless you deliberately want a `custom@1` policy.
Run `tamsin profiles` to compare the built-ins and their resource impact.

## Check what you got

A muxed ingest produces a media-owning Flow that collects other Flows. An
independent ingest produces media-owning essence Flows and an empty collector:

```sh
tamsctl flow get "$FLOW_ID" --output json | jq '{format, container, flow_collection}'
```

For muxed storage, each parent `flow_collection` item carries the
`container_mapping`; the collected child carries neither a mapping nor a
`container`. An independently stored essence carries `container` and no
`container_mapping`.

## See also

- [Profiles and supported media](../reference/profiles.md) — the complete policy and resource matrix
- [Essence storage](../explanation/essence-storage.md) — why the two graph arrangements differ
