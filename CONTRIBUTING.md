# Contributing

TAMSin uses Go 1.26+, FFmpeg as a process dependency, and the pinned TAMS and
TAMOSS revisions in `contracts/tams-v8.1.json`.

## Before opening a change

Open an issue before investing in a large feature or a change to a public
contract. Keep pull requests focused on one observable outcome; separate
mechanical cleanup from behavioural changes.

Design changes that add native dependencies, replace established libraries,
alter Flow or Object identity, introduce transcoding defaults, or change the
pinned API contract require a decision record under
`docs/explanation/decisions`.

## Develop and verify

Run the local gate before requesting review:

```sh
make clean verify
make dist
```

Changes to source resolution, authentication, upload or registration state,
output JSON, or exit codes must include an observable behaviour test. Use
`make e2e` for changes that affect TAMS requests, presigned storage, S3, FFmpeg
packaging, TLS, or the OCI runtime.

Tests must remain deterministic and safe to run with the complete suite. Do not
commit media fixtures, credentials, generated binaries, Kind state, or TAMOSS
source caches. Keep stdout machine-readable, diagnostics on stderr, secrets
redacted, retries bounded, and POST recovery read-based rather than blind
replay.

## Pull requests

Use a conventional, user-meaningful title; the title becomes the squash commit
on `main`. In the description, explain the problem and decision, user-visible
impact, security and resource impact, and the evidence used to verify the
change. Mark checklist items only after performing them.

Work-in-progress commits are welcome on the topic branch. The repository
squashes an approved pull request into one coherent mainline commit and deletes
the merged head branch.

## AI-assisted development

AI-assisted changes are welcome when the submitting contributor understands
and takes responsibility for the result.

- Disclose material AI assistance in the pull request, including the tool's
  role and how its output was independently checked.
- Never provide credentials, customer media, private source, or confidential
  operational data to an external model.
- Apply the same tests, security review, performance analysis, readability
  standard, and licence review as for manually written code.
- Do not submit broad generated rewrites or speculative edge-case handling
  without a concrete, reviewable reason.
- Do not name an AI tool as a commit author or co-author.

## Project history

TAMSin was incubated and reviewed in a private engineering repository before
its first public release. The maintainers retain that audit history privately;
the public history starts from the reviewed release tree and records all public
development from that point onward.
