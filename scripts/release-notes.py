#!/usr/bin/env python3
"""Extract curated release notes for a SemVer tag from CHANGELOG.md."""

import pathlib
import re
import sys


CORE = r"(?:0|[1-9][0-9]*)"
PRERELEASE_ID = r"(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
TAG = re.compile(
    rf"^v(?P<major>{CORE})\.(?P<minor>{CORE})\.(?P<patch>{CORE})"
    rf"(?:-(?P<prerelease>{PRERELEASE_ID}(?:\.{PRERELEASE_ID})*))?"
    rf"$"
)


def release_notes(tag: str, changelog: str) -> str:
    match = TAG.fullmatch(tag)
    if match is None:
        raise ValueError(f"tag {tag!r} is not a supported SemVer release tag")

    version = f"{match.group('major')}.{match.group('minor')}.{match.group('patch')}"
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

    prerelease = match.group("prerelease")
    if prerelease:
        body = (
            f"> Release candidate `{tag}` for `v{version}`.\n\n"
            f"{body}"
        )
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
