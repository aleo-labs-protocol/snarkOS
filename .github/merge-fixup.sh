#!/usr/bin/env bash
# The post-merge hook of merge-into-staging.yml. It runs with the merge in
# progress, and its arguments are the unmerged paths. See "Merging branches" in
# aleo-labs-protocol/gh-actions for the contract.
#
# Release branches pin snarkVM by version or by a release revision, and staging
# pins it by a staging revision. So:
#   - The root manifest keeps the [workspace.dependencies.snarkvm] table of the
#     target as HEAD forked from it, whichever side changed it. HEAD is the open
#     pull request's branch when there is one, and the target can move on after
#     that branch forks. The rest of the manifest merges as usual.
#   - Cargo.lock keeps git's merge without the source's snarkVM entries. When
#     the source also changed the versions of workspace packages and nothing
#     else, it is HEAD's lockfile with those versions. It is left unmerged for
#     people when the source changed snarkVM and anything more than that, or
#     when HEAD's pin differs from the target's.
# Other paths stay as git merged them.
#
# The hook must not run cargo: the merged tree's cargo and rustup configuration
# would pick the programs it runs, on the runner the push step shares.
#
# The hook reads blobs that the workflow's partial clone may not hold yet, and it
# runs without a token, so it relies on the repository being public.
set -euo pipefail

manifest=Cargo.toml
lockfile=Cargo.lock
header='[workspace.dependencies.snarkvm]'

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# Prints the table: its header and the lines up to the next header. Blank and
# comment lines just before the next header belong to the next header.
table() {
  awk -v h="$header" '
    $0 == h { on = 1; print; next }
    on && /^\[/ { exit }
    on && /^[[:space:]]*(#|$)/ { held = held $0 "\n"; next }
    on { printf "%s", held; held = ""; print }'
}

# Prints the input with its table replaced by the target's. An input without
# the table is printed unchanged.
with_target_table() {
  awk -v h="$header" -v t="$tmp/table" '
    $0 == h { while ((getline l < t) > 0) print l; skip = 1; next }
    skip && /^\[/ { printf "%s", held; held = ""; skip = 0 }
    skip && /^[[:space:]]*(#|$)/ { held = held $0 "\n"; next }
    skip { held = ""; next }
    { print }
    END { printf "%s", held }'
}

unmerged() {
  [ -n "$(git ls-files -u -- "$@")" ]
}

# Prints one line per package of the lockfile: its name, a tab, and the entry to
# compare. A snarkVM pin change also rewrites the dependency lists of packages
# that share dependencies with snarkVM, so a package from a registry or git is
# compared by its name, version and source alone. A workspace package is
# compared whole, so a dependency added to it shows.
packages() {
  awk '
    function flush() { if (n != "") print n "\t" (src ? key : all); n = key = all = ""; src = 0 }
    /^\[/ { flush(); on = $0 == "[[package]]"; next }
    on && /^name = / { n = $3; gsub(/"/, "", n) }
    on && /^source = / { src = 1 }
    on && /^(name|version|source) = / { key = key " " $0 }
    on && NF { all = all " " $0 }
    END { flush() }'
}

# Prints one line per workspace package of the lockfile: its name, its version
# and the rest of its entry, separated by tabs.
members() {
  awk '
    function flush() { if (n != "" && !src) print n "\t" v "\t" rest; n = v = rest = ""; src = 0 }
    /^\[/ { flush(); on = $0 == "[[package]]"; next }
    on && /^name = / { n = $3; gsub(/"/, "", n); next }
    on && /^version = / { v = $3; gsub(/"/, "", v); next }
    on && /^source = / { src = 1 }
    on && NF { rest = rest " " $0 }
    END { flush() }'
}

# Reads package names, and prints each with the source's version of it after a
# tab. Fails unless each is a workspace package on all three sides, the source
# changed nothing but its version, and HEAD has the merge base's version. The
# rest of the entry covers the dependency lists, so it also covers a lockfile
# that names a workspace package by version in one of them.
version_bumps() {
  awk -F '\t' '
    FILENAME == ARGV[1] { base[$1] = $2; base_rest[$1] = $3; next }
    FILENAME == ARGV[2] { source[$1] = $2; source_rest[$1] = $3; next }
    FILENAME == ARGV[3] { head[$1] = $2; next }
    !($1 in base && $1 in source && $1 in head) { exit 1 }
    base_rest[$1] != source_rest[$1] || head[$1] != base[$1] { exit 1 }
    { print $1 "\t" source[$1] }' "$tmp/base.members" "$tmp/source.members" "$tmp/head.members" -
}

# Prints the input lockfile with the versions of the packages in the given file.
with_versions() {
  awk -v f="$1" '
    BEGIN { while ((getline l < f) > 0) { split(l, a, "\t"); to[a[1]] = a[2] } }
    /^\[/ { n = "" }
    /^name = / { n = $3; gsub(/"/, "", n) }
    /^version = / && n in to { print "version = \"" to[n] "\""; next }
    { print }'
}

has_lockfile() {
  [ -n "$(git ls-tree "$1" -- "$lockfile")" ]
}

# Prints ls-tree lines as update-index lines at the given stage.
at_stage() {
  awk -v n="$1" -F '\t' '{ split($1, a, " "); print a[1] " " a[3] " " n "\t" $2 }'
}

# Leaves the path unmerged, with the merge base, HEAD and the source as its
# stages. The working tree keeps git's merge.
leave_unmerged() {
  local base
  unmerged "$1" && return
  base=$(git merge-base HEAD MERGE_HEAD | head -n 1)
  {
    printf '0 %s\t%s\n' "$(git rev-parse HEAD | tr 0-9a-f 0)" "$1"
    git ls-tree "$base" -- "$1" | at_stage 1
    git ls-tree HEAD -- "$1" | at_stage 2
    git ls-tree MERGE_HEAD -- "$1" | at_stage 3
  } | git update-index --index-info
}

fork=$(git merge-base HEAD "origin/$TARGET")
git show "$fork:$manifest" | table > "$tmp/table"
if [ ! -s "$tmp/table" ]; then
  echo "::error::$manifest on $TARGET has no $header table" >&2
  exit 1
fi

if unmerged "$manifest"; then
  # Merges again with the table made equal on all three sides. A side without
  # the manifest leaves the conflict to people.
  if git show ":1:$manifest" > "$tmp/base" 2> /dev/null && git show ":3:$manifest" > "$tmp/theirs" 2> /dev/null; then
    git show ":2:$manifest" | with_target_table > "$tmp/ours"
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

if has_lockfile HEAD; then
  if [ "$(git show "HEAD:$manifest" | table)" != "$(cat "$tmp/table")" ]; then
    leave_unmerged "$lockfile"
    echo "$lockfile: left unmerged, since HEAD pins snarkVM unlike $TARGET"
  else
    # The packages whose entries the source changed. A side without the
    # lockfile leaves git's merge, conflicted or not, to stand.
    changed=
    base=$(git merge-base HEAD MERGE_HEAD | head -n 1)
    if has_lockfile "$base" && has_lockfile MERGE_HEAD; then
      # Outside a command substitution, so a failed fetch stops the hook.
      git show "$base:$lockfile" | packages | LC_ALL=C sort > "$tmp/base.packages"
      git show "MERGE_HEAD:$lockfile" | packages | LC_ALL=C sort > "$tmp/source.packages"
      changed=$(LC_ALL=C comm -3 "$tmp/base.packages" "$tmp/source.packages" \
        | awk -F '\t' '{ print $1 == "" ? $2 : $1 }' | LC_ALL=C sort -u)
    fi
    if grep -qE '^snarkvm(-|$)' <<< "$changed"; then
      if grep -qvE '^snarkvm(-|$)' <<< "$changed"; then
        git show "$base:$lockfile" | members > "$tmp/base.members"
        git show "MERGE_HEAD:$lockfile" | members > "$tmp/source.members"
        git show "HEAD:$lockfile" | members > "$tmp/head.members"
        if grep -vE '^snarkvm(-|$)' <<< "$changed" | version_bumps > "$tmp/bumps"; then
          git show "HEAD:$lockfile" | with_versions "$tmp/bumps" > "$tmp/lockfile"
          cp "$tmp/lockfile" "$lockfile"
          git add "$lockfile"
          echo "$lockfile: kept that of $TARGET, with the workspace versions of $SOURCE"
        else
          leave_unmerged "$lockfile"
          echo "$lockfile: left unmerged, since $SOURCE changed snarkVM and other packages"
        fi
      else
        git checkout HEAD -- "$lockfile"
        echo "$lockfile: dropped the snarkVM entries of $SOURCE"
      fi
    fi
  fi
fi
