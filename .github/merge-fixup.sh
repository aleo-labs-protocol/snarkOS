#!/usr/bin/env bash
# The post-merge hook of merge-into-staging.yml. It runs with the merge in
# progress, and its arguments are the unmerged paths. See "Merging branches" in
# aleo-labs-protocol/gh-actions for the contract.
#
# Release branches pin snarkVM by version or by a release revision, and staging
# pins it by a staging revision. So:
#   - The root manifest keeps the target's [workspace.dependencies.snarkvm]
#     table, whichever side changed it. The rest of the manifest merges as usual.
#   - Cargo.lock is the target's when every manifest is the target's.
# Other paths stay as git merged them.
set -euo pipefail

manifest=Cargo.toml
lockfile=Cargo.lock
header='[workspace.dependencies.snarkvm]'

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# Prints the table: its header and the lines up to the next header.
table() {
  awk -v h="$header" '$0 == h { on = 1; print; next } on && /^\[/ { exit } on'
}

# Prints the input with its table replaced by the target's. An input without
# the table is printed unchanged.
with_target_table() {
  awk -v h="$header" -v t="$tmp/table" '
    $0 == h { while ((getline l < t) > 0) print l; skip = 1; next }
    skip && /^\[/ { skip = 0 }
    !skip'
}

unmerged() {
  [ -n "$(git ls-files -u -- "$@")" ]
}

git show "HEAD:$manifest" | table > "$tmp/table"
if [ ! -s "$tmp/table" ]; then
  echo "::error::$manifest on $TARGET has no $header table" >&2
  exit 1
fi

if unmerged "$manifest"; then
  # Merges again with the table made equal on all three sides. A side without
  # the manifest leaves the conflict to people.
  if git show ":1:$manifest" > "$tmp/base" 2> /dev/null && git show ":3:$manifest" > "$tmp/theirs" 2> /dev/null; then
    git show ":2:$manifest" > "$tmp/ours"
    with_target_table < "$tmp/base" > "$tmp/base.fixed"
    with_target_table < "$tmp/theirs" > "$tmp/theirs.fixed"
    if git merge-file -L "$TARGET" -L "merge base" -L "$SOURCE" "$tmp/ours" "$tmp/base.fixed" "$tmp/theirs.fixed"; then
      cp "$tmp/ours" "$manifest"
      git add "$manifest"
      echo "$manifest: resolved, keeping the snarkVM pin of $TARGET"
    else
      # Fewer markers for people to resolve. The path stays unmerged.
      cp "$tmp/ours" "$manifest"
      echo "$manifest: conflicts outside the snarkVM table remain"
    fi
  fi
elif [ -f "$manifest" ]; then
  with_target_table < "$manifest" > "$tmp/merged"
  if ! cmp -s "$tmp/merged" "$manifest"; then
    cp "$tmp/merged" "$manifest"
    git add "$manifest"
    echo "$manifest: restored the snarkVM pin of $TARGET"
  fi
fi

# The target's lockfile matches the target's manifests.
manifests=':(glob)**/Cargo.toml'
if ! unmerged "$manifests" && git diff --cached --quiet HEAD -- "$manifests" \
    && git cat-file -e "HEAD:$lockfile" 2> /dev/null \
    && { unmerged "$lockfile" || ! git diff --cached --quiet HEAD -- "$lockfile"; }; then
  git checkout HEAD -- "$lockfile"
  echo "$lockfile: took the one on $TARGET, whose manifests this merge keeps"
fi
