#!/usr/bin/env python3
"""Checks that every `.rs` file git tracks starts with the license header in `.resources/license_header`.

Tracked files that are deleted from the working tree are not checked. On Linux, the copyright range
in that header must also end in the current year.

Usage: scripts/check_sources.py [ROOT]

ROOT is a git working tree and defaults to this repository. The exit status is 1 if a check fails.
"""

import datetime
import os
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
LICENSE_HEADER = (REPO_ROOT / ".resources" / "license_header").read_bytes()


def rust_files(root):
    """Yields the tracked `.rs` files under `root` that exist in the working tree."""
    output = subprocess.run(["git", "-C", root, "ls-files", "-z", "--", "*.rs"], check=True, capture_output=True).stdout
    for name in output.split(b"\0"):
        path = root / os.fsdecode(name)
        if name and os.path.lexists(path):
            yield path


def check_license_year():
    """Returns an error message if the license header does not end in the current year."""
    match = re.search(rb"Copyright \(c\) \d{4}-(\d{4})", LICENSE_HEADER)
    if match is None:
        return ".resources/license_header has no copyright year range"
    header_year = int(match.group(1))
    current_year = datetime.date.today().year
    if header_year != current_year:
        return f".resources/license_header ends its copyright range in {header_year}, but the current year is {current_year}"
    return None


def main():
    root = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else REPO_ROOT

    errors = []

    if sys.platform.startswith("linux"):
        year_error = check_license_year()
        if year_error is not None:
            errors.append(year_error)

    for path in rust_files(root):
        name = path.relative_to(root)
        try:
            contents = path.read_bytes()
        except OSError as error:
            errors.append(f"{name}: cannot be read: {error}")
            continue
        if not contents.startswith(LICENSE_HEADER):
            errors.append(f"{name}: the license header is missing or does not match .resources/license_header")

    for error in errors:
        print(f"error: {error}", file=sys.stderr)
    if errors:
        print(f"{len(errors)} source check(s) failed.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
