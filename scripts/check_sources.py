#!/usr/bin/env python3
"""Checks the `.rs` files that git tracks.

Tracked files that are deleted from the working tree are not checked.

- Every file starts with the license header in `.resources/license_header`.
  On Linux, the copyright range in that header must end in the current year.
- In every file, each `Mutex` or `RwLock` imported from `parking_lot` or
  `tokio` has a matching import from `locktick`. The `locktick` feature relies
  on this to swap in its instrumented lock types.

Usage: scripts/check_sources.py [ROOT]

ROOT is a git working tree and defaults to this repository. The exit status is 1 if a check fails.
"""

import datetime
import os
import re
import subprocess
import sys
from itertools import dropwhile, takewhile
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


def is_import_line(line):
    """Returns whether `line` can belong to the import block at the top of a file."""
    return line.startswith(("use", "#[cfg", "//")) or line == "};" or line[:1] in " \t\n\r\f"


def lock_import_imbalance(text):
    """Returns the number of `parking_lot` and `tokio` lock imports minus the number of `locktick` lock imports.

    Only the first import block of `text` is scanned. The scan is line-based: an import
    statement starts at a `use` line and ends at the first line that ends with `;`.
    """
    # This matches Rust's `str::lines`, which splits on `\n` and strips one trailing `\r`.
    lines = (line.removesuffix("\r") for line in text.split("\n"))
    lines = (line for line in lines if line)
    import_block = takewhile(is_import_line, dropwhile(lambda line: not line.startswith("use"), lines))

    imbalance = 0
    # The crate of the import statement being scanned, or `None` outside a relevant statement.
    crate = None
    for line in import_block:
        if crate is None:
            if line.startswith("use locktick::"):
                crate = "locktick"
            elif line.startswith(("use parking_lot::", "use tokio::")):
                crate = "lock"

        if crate is not None:
            locks = line.count("Mutex") + line.count("RwLock")
            # `Mutex as TMutex` names `Mutex` twice for one lock.
            if "TMutex" in line:
                locks -= 1
            if crate == "lock":
                # Lock guards have no `locktick` counterpart.
                locks -= sum(guard in line for guard in ("MutexGuard", "RwLockReadGuard", "RwLockWriteGuard"))
                imbalance += locks
            else:
                imbalance -= locks

        if line.endswith(";"):
            crate = None

    return imbalance


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
        try:
            text = contents.decode("utf-8")
        except UnicodeDecodeError as error:
            errors.append(f"{name}: is not valid UTF-8: {error}")
            continue
        imbalance = lock_import_imbalance(text)
        if imbalance > 0:
            errors.append(f"{name}: {imbalance} lock import(s) from parking_lot or tokio have no locktick counterpart")
        elif imbalance < 0:
            errors.append(f"{name}: {-imbalance} locktick import(s) have no parking_lot or tokio counterpart")

    for error in errors:
        print(f"error: {error}", file=sys.stderr)
    if errors:
        print(f"{len(errors)} source check(s) failed.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
