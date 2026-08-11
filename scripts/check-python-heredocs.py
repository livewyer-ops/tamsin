#!/usr/bin/env python3
"""Compile quoted Python heredocs embedded in shell scripts.

`bash -n` treats a heredoc body as opaque data. That means it accepts a Python
heredoc whose intended terminator was accidentally deleted and whose body now
contains the following shell commands. Compile the bodies separately so that
class of edit fails the ordinary format gate.
"""

from __future__ import annotations

import pathlib
import re
import sys


START = re.compile(
    r"^\s*python3(?:\s|$).*<<-?\s*(?P<quote>['\"])(?P<marker>[A-Za-z_][A-Za-z0-9_]*)"
    r"(?P=quote)(?:\s*&)?\s*$"
)


def python_heredocs(path: pathlib.Path) -> list[tuple[int, str]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    bodies: list[tuple[int, str]] = []
    index = 0
    while index < len(lines):
        match = START.match(lines[index])
        if match is None:
            index += 1
            continue

        marker = match.group("marker")
        start = index + 1
        index = start
        while index < len(lines) and lines[index].lstrip("\t") != marker:
            index += 1
        if index == len(lines):
            raise ValueError(f"{path}:{start}: unterminated Python heredoc {marker!r}")
        bodies.append((start + 1, "\n".join(lines[start:index]) + "\n"))
        index += 1
    return bodies


def main(arguments: list[str]) -> int:
    failed = False
    for filename in arguments:
        path = pathlib.Path(filename)
        try:
            bodies = python_heredocs(path)
        except (OSError, UnicodeError, ValueError) as error:
            print(error, file=sys.stderr)
            failed = True
            continue
        for line, body in bodies:
            try:
                compile(body, f"{path}:Python-heredoc-at-{line}", "exec")
            except SyntaxError as error:
                print(error, file=sys.stderr)
                failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
