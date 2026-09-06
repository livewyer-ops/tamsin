# TAMSin documentation

Use this page to start from your goal. The documentation follows
[Diátaxis](https://diataxis.fr), keeping learning material, task recipes,
authoritative contracts, and design discussion distinct.

## Start here

| Goal | Start | Continue with |
| --- | --- | --- |
| Complete a first ingest | [Your first ingest](tutorials/first-ingest.md) | [Inputs](reference/inputs.md) |
| Learn the product vocabulary | [Terminology and product model](reference/terminology.md) | [Essence storage](explanation/essence-storage.md) |
| Choose an ingest policy | [Profiles and supported media](reference/profiles.md) | [Choose how essences are stored](how-to/choose-how-essences-are-stored.md) |
| Prepare MPEG-TS Objects | [Prepare MPEG-TS Segments](how-to/prepare-mpegts-segments.md) | [Segmentation](explanation/segmentation.md) |
| Run in Kubernetes | [Run as a Kubernetes Job](how-to/run-as-a-kubernetes-job.md) | [Configuration](reference/configuration.md) |
| Cut a release | [Release procedure](how-to/cut-a-release.md) | [TAMS conformance](explanation/conformance.md) |
| Integrate a service or UI | [Output protocol](reference/result-contract.md) | [Exit codes](reference/exit-codes.md) |
| Diagnose an environment | [Doctor report](reference/doctor.md) | [CLI reference](reference/cli.md) |
| Understand recovery guarantees | [Integrity](explanation/integrity.md) | [Design decisions](explanation/decisions) |
| Check standards behaviour | [TAMS conformance](explanation/conformance.md) | [Media metadata](reference/media-metadata.md) |

## Tutorials

Tutorials teach by taking you through a complete learning experience.

- [Your first ingest](tutorials/first-ingest.md)

## How-to guides

How-to guides assume you know the basics and help you complete a particular
operational task.

- [Authenticate against a TAMS store](how-to/authenticate.md)
- [Choose how essences are stored](how-to/choose-how-essences-are-stored.md)
- [Cut a release](how-to/cut-a-release.md)
- [Run as a Kubernetes Job](how-to/run-as-a-kubernetes-job.md)
- [Prepare MPEG-TS Segments](how-to/prepare-mpegts-segments.md)

## Reference

Reference pages describe the stable interface, accepted values, and observable
behaviour.

- [Terminology and product model](reference/terminology.md)
- [Profiles and supported media](reference/profiles.md)
- [CLI](reference/cli.md)
- [Configuration](reference/configuration.md)
- [Inputs](reference/inputs.md)
- [Media metadata](reference/media-metadata.md)
- [Doctor report](reference/doctor.md)
- [Exit codes](reference/exit-codes.md)
- [Output protocol and durable journal](reference/result-contract.md)

## Explanation

Explanation pages develop the reasoning, constraints, and trade-offs behind
TAMSin's behaviour.

- [Essence storage](explanation/essence-storage.md)
- [Segmentation](explanation/segmentation.md)
- [Integrity](explanation/integrity.md)
- [TAMS conformance](explanation/conformance.md)
- [Architecture and product decisions](explanation/decisions)

## Documentation conventions

- Product prose uses **TAMSin**. The executable, module path, protocol names,
  and configuration keys retain their literal forms: `tamsin`,
  `github.com/livewyer-ops/tamsin`, `tamsin.ingest.events`, and `TAMSIN_*`.
- A page should answer one dominant kind of question. Put runnable learning in
  tutorials, goal-directed procedures in how-to guides, facts and contracts in
  reference, and rationale in explanation.
- Link to the canonical reference rather than copying tables of flags, profile
  values, exit codes, or wire fields into several pages.

The [repository README](../README.md) is the concise product entry point. This
page is the complete reader-facing documentation map.
