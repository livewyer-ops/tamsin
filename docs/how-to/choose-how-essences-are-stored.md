# Choose how essences are stored

A file carrying video and audio together can go into TAMS two ways. Pick by what you need to do with the media afterwards.

## Keep essences separately addressable

Select the editorial profile:

```sh
tamsin --profile editorial -i programme.ts -o https://tams.example.com
```

Each essence becomes its own Flow owning its own Media Objects. The permanent
human receipt groups them under the empty collector:

```text
INGESTED AND VERIFIED  programme.ts

  collection 821256e5-9820-447e-b62e-66fac962ea1b
  video 9a69b060-143b-4d85-b3a8-a55343e9ed16 1 media object
  audio 34201935-4b67-4785-b24f-caf2889b25ca 1 media object
```

Do this when downstream consumers need one essence without the other — a transcription service fetching audio, or proxy generation fetching video.

## Keep the original multiplex

```sh
tamsin --profile preserve -i programme.ts -o https://tams.example.com
```

The bytes are stored as they arrived, described by a root multi-essence Flow
that owns the Objects and collects one Flow per track. The receipt lists each
Flow once; the root is labelled `flow` because it owns the multiplexed media:

```text
INGESTED AND VERIFIED  programme.ts

  flow c8f9cf7e-9ce2-4f74-aef5-5a90ac3665fd 1 media object
  video 9a69b060-143b-4d85-b3a8-a55343e9ed16
  audio 34201935-4b67-4785-b24f-caf2889b25ca
```

Do this when you must be able to demonstrate that what you hold is what you received. This is the only mode that does not rewrite the container.

## Store the input byte-for-byte

Muxed storage still cuts the media into Segments, which rewrites containers. To store exactly the bytes you were given, also disable segmentation:

```sh
tamsin --profile preserve -i programme.ts -o https://tams.example.com
```

The cost is that the Flow holds one whole-file Media Object, so a reader wanting ten seconds must fetch all of it.

## Set it permanently

In `config.yaml`:

```yaml
ingest:
  profile: preserve
  essence_storage: muxed
```

Or in the environment, which suits a Kubernetes Job:

```sh
export TAMSIN_INGEST_ESSENCE_STORAGE=muxed
export TAMSIN_INGEST_PROFILE=preserve
```

## Check what you got

A muxed ingest produces a Flow that collects others:

```sh
tamsin api flow get "$FLOW_ID" --format json | jq '{format, container, flow_collection}'
```

An independent ingest produces essence Flows that own their media; its empty
multi-essence collector has the collection:

```sh
tamsin api flow get "$FLOW_ID" --format json | jq '{format, container, flow_collection}'
```

For muxed storage, each parent `flow_collection` item carries the
`container_mapping`; the collected child carries neither a mapping nor a
`container`. An independently stored essence carries `container` and no
`container_mapping`.

## See also

- [Essence storage](../explanation/essence-storage.md) — why the two arrangements differ and which the specification prefers
