# Inputs

Repeat `--input` to ingest a batch. TAMSin accepts:

- a local file;
- a directory, expanded recursively in lexical order;
- a line-oriented manifest file;
- an `http://` or `https://` URL;
- an `s3://bucket/key` object or prefix;
- `-` for standard input.

A positional input is equivalent to one `--input`. `--stdin-name` explicitly
selects standard input only when no other input was supplied.

## Manifests and directories

Manifest lines are trimmed; empty lines and lines beginning with `#` are
ignored. Relative local paths resolve from the manifest directory. Nested
manifests are expanded recursively; cycles and nesting beyond 32 levels fail.

Directories include regular files only. Symlinks are not followed. Duplicate
local paths and canonical remote locators are removed while preserving the
first occurrence. `--max-inputs` limits the final expanded set.

Expansion is atomic. If any requested directory, manifest, HTTP source or S3
prefix cannot be resolved, TAMSin reports no partial manifest and performs no
TAMS mutation.

## HTTP

Use `--input-header 'Name: value'` for required request headers. Headers are
sent only to the original origin and are removed on cross-origin redirects.
Redirects to non-HTTP schemes are rejected. URL user information, queries and
fragments are removed from receipts, events and diagnostics.

## S3

S3 credentials use the standard AWS credential chain. `--s3-region`,
`--s3-endpoint` and `--s3-path-style` support compatible object stores. A URI
ending in `/` is treated as a prefix; a concrete key is treated as one object.

## Standard input

Standard input is staged once because FFprobe and FFmpeg may need to reopen the
bytes. It may appear once alongside other inputs. `--stdin-name` supplies a
filename hint; it does not override content-derived container detection:

```sh
producer | tamsin ingest --profile preserve --input - --stdin-name programme.ts
```
