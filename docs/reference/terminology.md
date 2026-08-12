# Terminology and product model

This page defines the names TAMSin uses in commands, receipts, event streams,
journals, and documentation. Start here when two nearby concepts appear to mean
the same thing.

## Product and project names

| Name | Meaning |
| --- | --- |
| **TAMSin** | This ingest product and project |
| `tamsin` | The command-line executable, Go module name, and lowercase prefix used by machine protocols |
| **TAMS** | The BBC Time-addressable Media Store specification and API model |
| **TAMOSS** | The open-source Kubernetes-oriented TAMS implementation used by TAMSin's end-to-end tests |

Literal identifiers are not branding prose. Keep `tamsin` in shell commands,
`github.com/livewyer-ops/tamsin` in imports, `tamsin.ingest.events` on the wire,
and `TAMSIN_*` for environment variables.

## Ingest choices

### Input

One user-supplied selector such as a file, directory, manifest, HTTP URL, S3
object, or `-` for standard input. Resolution can expand one selector into
multiple ordered inputs. Each resolved item receives its own terminal result.

### Ingest profile

A named, versioned packaging contract. It fixes the essence-storage model,
Segment target duration, stored container policy, and compatibility boundary.
TAMSin currently provides `preserve@1`, `demux@1`, `muxed-segments@1`,
`essence-segments@1`, and `mpegts-segments@1`.
See [profiles and supported media](profiles.md).

Profiles describe byte packaging and access characteristics. Workflow labels
such as archive, editorial, or streaming may eventually select several steps
as recipes, but they are not aliases for one media profile.

### Essence storage

The relationship between an input's elementary streams and its TAMS Flow
graph:

- **Independent** storage creates one object-owning Flow per essence and groups
  them with an empty collector Multi-Flow.
- **Muxed** storage creates one object-owning Flow for the multiplexed
  representation.

### Run

One `tamsin ingest` invocation. A run contains an ordered input manifest and
ends in a human receipt or an exact terminal `run.finished` event. A partial
batch is still one run with complete per-input results.

## TAMS graph and storage entities

### Source

The stable TAMS identity for source material. A Source can be associated with
one or more Flows that describe stored representations of that material.

### Flow

A time-addressable media timeline. TAMSin generates stable Flow identifiers
from the input identity and the resolved storage/profile/toolchain contract.

### Essence Flow

An object-owning Flow for one video, audio, data, or muxed representation. In
independent storage, each elementary stream has its own essence Flow.

### Collector Flow

An empty Multi-Flow that groups independent essence Flows into one copyable
collection identity. It owns no Media Objects and has no Segments of its own.

### Media Object

Immutable bytes stored at a URL obtained from a TAMS storage allocation. An
Object can be referenced by one or more Flow Segments when the stored bytes
contain multiple independently described essences.

### Flow Segment

A mapping from a timerange in a Flow to a Media Object and, where applicable,
the corresponding timerange within that Object. Segments describe timeline
membership; they are not the stored bytes themselves.

## Outputs and durable records

### Human receipt

The permanent stdout summary for a human-formatted run. It reports the same
terminal object, Flow, input, and run facts represented by machine events.
Transient progress and diagnostics remain on stderr.

### Event stream

The versioned NDJSON stream selected by `--format json`. It begins with
`hello`, publishes lifecycle and progress records while the run is active, and
ends with exactly one `run.finished`. EOF before that terminal record is an
incomplete stream, not a result.

### Journal

An optional independently synced, redacted record selected with `--journal`.
It persists the resolved input manifest and terminal input results for support
and recovery; it is not a substitute for consuming the live event stream.

See the [output protocol and durable journal](result-contract.md) for their
schemas, ordering rules, and failure semantics.
