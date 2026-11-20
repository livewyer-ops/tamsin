# TAMSin documentation

Start with [installation](../README.md#install) and check `tamsin --version`.
These pages describe TAMSin `8.2.0-in3`. For another release, open the
documentation at the Git tag matching your binary or image.

| Goal | Guide |
| --- | --- |
| Learn by ingesting a known sample | [Your first ingest](first-ingest.md) |
| Understand Sources, Flows, Objects and Segments | [Media, identity and verification](concepts.md) |
| Supply credentials | [Authentication](configuration.md#authentication) |
| Ingest HTTP or S3 media | [Remote input procedure](configuration.md#ingest-an-http-or-s3-input) |
| Supply labels or technical metadata | [Metadata procedure](profiles.md#supply-flow-metadata) |
| Recover a partial or interrupted run | [Resume procedure](operations.md#resume-an-interrupted-ingest) |
| Check a worker and size its temporary storage | [Operations](operations.md) |
| Look up commands and exit codes | [CLI reference](cli.md) |
| Look up flag defaults, YAML keys and environment variables | [Configuration reference](configuration.md#settings) |
| Check supported packaging, codecs and metadata | [Profiles reference](profiles.md) |
| Consume process output | [Event protocol reference](events.md) |
| Check service support and upgrade boundaries | [Compatibility](compatibility.md) |

For maintainers: [Contributing](../CONTRIBUTING.md),
[release procedure](releasing.md) and [security reporting](../SECURITY.md).
