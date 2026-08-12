#!/usr/bin/env bash
#
# Refuse to cut a release while a known specification gap is open.
#
# The conformance inventory records requirements that bear on ingest but are
# not met yet. Some of those are the kind you can ship around and some are not,
# so each says which it is. This turns that record into something enforced:
# without it, "recorded, not implemented" is a note nobody reads at the moment
# it matters, which is when somebody pushes a tag.
set -Eeuo pipefail

contract="${1:-contracts/tams-v8.2.json}"

python3 - "$contract" <<'PY'
import json, sys
from pathlib import Path

path = Path(sys.argv[1])
findings = []
seen = set()
while path:
    resolved = path.resolve()
    if resolved in seen:
        print(f"conformance inventory inheritance cycle at {path}", file=sys.stderr)
        raise SystemExit(2)
    seen.add(resolved)
    try:
        with path.open() as handle:
            document = json.load(handle)
    except (OSError, ValueError) as error:
        print(f"cannot read the conformance inventory at {path}: {error}", file=sys.stderr)
        raise SystemExit(2)
    findings.extend(document.get("open_findings", []))
    parent = document.get("extends")
    path = path.parent / parent if parent else None

blocking = [entry for entry in findings if entry.get("blocks_release")]
if not blocking:
    print(f"Release gate: {len(findings)} open finding(s), none blocking.")
    raise SystemExit(0)

print(f"Refusing to release: {len(blocking)} specification finding(s) block it.\n", file=sys.stderr)
for entry in blocking:
    print(f"  {entry.get('id')}  [{entry.get('source')}]", file=sys.stderr)
    print(f"    {entry.get('requirement')}", file=sys.stderr)
    print(f"    exposure: {entry.get('exposure')}\n", file=sys.stderr)
print(
    "Close these, or drop blocks_release from the ones you are accepting.\n"
    "Accepting a gap is a decision worth making on purpose.",
    file=sys.stderr,
)
raise SystemExit(1)
PY
