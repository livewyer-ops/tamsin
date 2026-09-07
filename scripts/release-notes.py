#!/usr/bin/env python3
"""Extract curated release notes for a TAMSin release tag from CHANGELOG.md.

A release tag is MAJOR.MINOR.PATCH-inN: MAJOR.MINOR.PATCH is the BBC TAMS API
version targeted and N counts TAMSin releases for that API version. A trailing
-rcM marks a release candidate. Notes come from the changelog section headed
"## [MAJOR.MINOR.PATCH-inN] - YYYY-MM-DD"; a candidate shares the section of
the release it precedes and gains a banner naming the candidate.
"""

import pathlib
import re
import sys


NUMBER = r"[0-9]+"
TAG = re.compile(
    rf"^v?(?P<major>{NUMBER})\.(?P<minor>{NUMBER})\.(?P<patch>{NUMBER})"
    rf"-in(?P<release>{NUMBER})(?:-rc(?P<candidate>{NUMBER}))?$"
)


def release_notes(tag: str, changelog: str) -> str:
    match = TAG.fullmatch(tag)
    if match is None:
        raise ValueError(
            f"tag {tag!r} is not a TAMSin release tag: expected MAJOR.MINOR.PATCH-inN "
            "or MAJOR.MINOR.PATCH-inN-rcM, for example 8.2.0-in1 or 8.2.0-in1-rc1"
        )

    version = f"{match['major']}.{match['minor']}.{match['patch']}-in{match['release']}"
    heading = re.compile(
        rf"(?m)^## \[{re.escape(version)}\] - [0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}\s*$"
    ).search(changelog)
    if heading is None:
        raise ValueError(f"CHANGELOG.md has no dated [{version}] release section")

    following = changelog[heading.end() :]
    next_heading = re.search(r"(?m)^## ", following)
    body = following[: next_heading.start() if next_heading else None].strip()
    if not body:
        raise ValueError(f"CHANGELOG.md [{version}] release section is empty")

    if match["candidate"] is not None:
        body = f"> Release candidate `{tag}` for `{version}`.\n\n{body}"
    return body + "\n"


def main(arguments: list[str]) -> None:
    if len(arguments) != 2:
        raise ValueError("usage: release-notes.py TAG CHANGELOG")
    tag, path = arguments
    try:
        changelog = pathlib.Path(path).read_text(encoding="utf-8")
    except OSError as error:
        raise ValueError(f"cannot read {path}: {error}") from error
    sys.stdout.write(release_notes(tag, changelog))


try:
    main(sys.argv[1:])
except ValueError as error:
    print(error, file=sys.stderr)
    raise SystemExit(2)
