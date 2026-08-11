# Essence storage

A file that carries video and audio together poses a question TAMS does not answer for you: should the store hold that multiplex as it arrived, or hold each essence separately? Both are valid, they are described by different halves of [AppNote 0006](https://github.com/bbc/tams/blob/98d307b09b5ebf79278aa7d3aad53295154e2c17/docs/appnotes/0006-containers-and-mappings.md), and TAMSin implements both. This page explains what is actually different between them, so the choice is yours to make on purpose.

## The two arrangements

Under **independent** storage, TAMSin demultiplexes the input. Each elementary stream becomes a Flow in its own right, owning its own Media Objects. There is no multiplex left to describe, so no `container_mapping` is written.

A third Flow is created alongside them: a multi-essence Flow whose `flow_collection` records that these essences came from one input. It owns no Media Objects and declares no container — its media is reached through the essences it collects — and it is registered after its children, because a Collection Item may only reference a Flow the service already holds. The complete empty graph is created before any Media Object is allocated, so an essence cannot acquire Segments while it still appears unrelated. It is the Flow that stands for the input, so `--flow-id` and `--source-id` name it.

Each essence keeps the offset at which its stream begins relative to the container, so a source whose audio starts after its video stays in sync once the two are separate Flows.

Under **muxed** storage, the bytes stay as they arrived. A multi-essence Flow owns the Media Objects, and it collects one mono-essence Flow per elementary stream through `flow_collection`. Those collected Flows own no media themselves — they describe tracks *inside* somebody else's Objects.

That difference drives everything else, including a pair of rules that invert between the modes and are easy to get backwards:

| | independent | muxed |
| --- | --- | --- |
| Who owns the Media Objects | each essence Flow | the multi-essence Flow |
| `container` on an essence Flow | **set** — it owns Objects, so it says what they are | **absent** — it owns none |
| `container_mapping` | **absent** — no multiplex to point into | **set on each parent Collection Item** — locates that child's track |
| `flow_collection` | on the empty multi-essence collector | on the media-owning multi-essence Flow |

The logic is consistent once you see it: `container` describes media a Flow *has*, and its presence is what signals that a Flow references Media Objects directly. `container_mapping` describes where a child Flow's essence sits inside the particular parent container collecting it, so AppNote 0006 puts the mapping on that parent's Collection Item rather than on the child globally. A collected child therefore has neither property: it is reachable through the Collection Item that names and maps it.

## Why editorial ingest uses independent storage

[AppNote 0001](https://github.com/bbc/tams/blob/98d307b09b5ebf79278aa7d3aad53295154e2c17/docs/appnotes/0001-multi-mono-essence-flows-sources.md) leads with independent storage, and its argument is about what happens after ingest:

> Storing the media elements independently affords more flexibility in cases where media elements regularly need to be manipulated separately, as it avoids the overhead of repeated unpacking and repacking. For example, a transcription service would need access to audio, but not video. And generation of video proxies would not need access to audio.

Under muxed storage a transcription service must download the video it will discard, because the audio is welded to it. Under independent storage it fetches audio and nothing else. The same appnote notes a second saving: you avoid materialising every combination of audio and video qualities as its own multiplex.

## Why muxed still exists

The same appnote is equally clear about when to keep the multiplex:

> As an alternative, multi-essence streams can be stored directly in muxed form if the flexibility of elemental media is not required. This may be of particular use where the original stream must be retained for compliance reasons.

This is the point that decides it for archival work. Demultiplexing rewrites containers: the coded pictures and samples survive untouched, but the framing around them does not, so what you get back is not the file you put in. Muxed storage is the only mode that keeps the arriving multiplex intact. If you may one day have to demonstrate that what you hold is what you received, that is not a detail you can trade for convenience.

## What both modes share

Neither mode transcodes. TAMSin stream-copies throughout, so the coded essence is carried through unchanged and both arrangements record `generation: 0` to say so. Demultiplexing changes which container the essence sits in; it does not decode and re-encode it.

Both also give each essence its own Source. A video track and an audio track are not editorially equivalent — they are different content, not alternative representations of the same content — so they do not belong under one Source. TAMS derives the counterpart `source_collection` from the Flow collection itself.

A Source is derived from the content it stands for rather than from where that content was found. The same essence therefore keeps one Source across both arrangements, because being inside a multiplex or beside it are two representations of the same thing; and reusing a path for an unrelated programme does not hand the new content the old Source. Use `--source-id` to assert an equivalence TAMSin cannot see, such as between a master and its transcode.

## Single-essence inputs

If an input carries one elementary stream, the two modes are identical: there is nothing to separate, and you get one Flow either way. The setting only bites on a multiplex.

## See also

- [Choose how essences are stored](../how-to/choose-how-essences-are-stored.md) — the task
- [Segmentation](segmentation.md) — how each Flow's media is cut up on the timeline
