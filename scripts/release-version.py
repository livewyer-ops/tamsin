#!/usr/bin/env python3
"""Validate release tags and choose the newest stable release."""

import re
import sys


CORE = r"(?:0|[1-9][0-9]*)"
PRERELEASE_ID = r"(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
VERSION_TAG = re.compile(
    rf"^v(?P<major>{CORE})\.(?P<minor>{CORE})\.(?P<patch>{CORE})"
    rf"(?:-(?P<prerelease>{PRERELEASE_ID}(?:\.{PRERELEASE_ID})*))?"
    rf"$"
)
REPOSITORY = re.compile(r"^[0-9A-Za-z_.-]+/[0-9A-Za-z_.-]+$")


def parse(tag):
    match = VERSION_TAG.fullmatch(tag)
    if match is None:
        return None
    return (
        int(match.group("major")),
        int(match.group("minor")),
        int(match.group("patch")),
        match.group("prerelease"),
    )


def describe(tag, repository):
    parsed = parse(tag)
    if parsed is None:
        raise ValueError(f"tag {tag!r} is not a SemVer version tag")
    if REPOSITORY.fullmatch(repository) is None:
        raise ValueError(f"repository {repository!r} is not an owner/name pair")
    version = tag[1:]
    stable = parsed[3] is None
    print(f"version={version}")
    print(f"stable={'true' if stable else 'false'}")
    print(f"image_ref=ghcr.io/{repository.lower()}:{version}")


def newest(lines):
    candidates = []
    for line in lines:
        tag = line.rstrip("\r\n")
        parsed = parse(tag)
        if parsed is None or parsed[3] is not None:
            continue
        candidates.append(((parsed[0], parsed[1], parsed[2]), tag))
    if candidates:
        print(max(candidates)[1])


def main(arguments):
    if not arguments:
        raise ValueError("usage: release-version.py describe TAG OWNER/REPO | newest")
    if arguments[0] == "describe" and len(arguments) == 3:
        describe(arguments[1], arguments[2])
        return
    if arguments[0] == "newest" and len(arguments) == 1:
        newest(sys.stdin)
        return
    raise ValueError("usage: release-version.py describe TAG OWNER/REPO | newest")


try:
    main(sys.argv[1:])
except ValueError as error:
    print(error, file=sys.stderr)
    raise SystemExit(2)
