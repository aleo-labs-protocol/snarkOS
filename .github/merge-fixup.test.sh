#!/usr/bin/env bash
# Tests merge-fixup.sh against throwaway repositories.
set -euo pipefail

hook=$(realpath "$(dirname "$0")/merge-fixup.sh")
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
pass=0
fail=0

ck() {
  local name=$1
  shift
  if "$@"; then
    pass=$((pass + 1))
  else
    fail=$((fail + 1))
    echo "FAIL: $name" >&2
  fi
}

manifest() { # pin, anyhow version, line above the anyhow table
  cat <<EOF
[workspace]
members = [ "a" ]

[workspace.dependencies.snarkvm]
#path = "../snarkVM"
${1}
default-features = false
${3:-}
[workspace.dependencies.anyhow]
version = "${2:-1.0}"
EOF
}

lockfile() { # pin, anyhow version, foo version
  cat <<EOF
version = 4

[[package]]
name = "anyhow"
version = "${2:-1.0}"

[[package]]
name = "foo"
version = "${3:-1.0.0}"

[[package]]
name = "snarkvm"
version = "4.12.0"
source = "$(cksum <<< "$1" | cut -d' ' -f1)"
EOF
}

staging_pin='git = "https://github.com/ProvableHQ/snarkVM.git"
rev = "1111111" #staging'
staging_bump='git = "https://github.com/ProvableHQ/snarkVM.git"
rev = "3333333" #staging'
release_pin='#git = "https://github.com/ProvableHQ/snarkVM.git"
version = "=4.12.0"'
release_rev='git = "https://github.com/ProvableHQ/snarkVM.git"
rev = "2222222" #test_net_v4120'

# Makes a repository at $repo whose branches staging and release start at a
# common base with the given pin.
setup() {
  repo=$(mktemp -d -p "$work")
  git -C "$repo" init -q -b staging
  git -C "$repo" config user.name t
  git -C "$repo" config user.email t@t
  mkdir "$repo/a"
  echo '[package]' > "$repo/a/Cargo.toml"
  manifest "$1" > "$repo/Cargo.toml"
  lockfile "$1" > "$repo/Cargo.lock"
  git -C "$repo" add -A
  git -C "$repo" commit -qm base
  git -C "$repo" branch release
}

edit() { # branch, pin, anyhow version, foo version, line above the anyhow table
  git -C "$repo" checkout -q "$1"
  manifest "$2" "${3:-}" "${5:-}" > "$repo/Cargo.toml"
  lockfile "$2" "${3:-}" "${4:-}" > "$repo/Cargo.lock"
  git -C "$repo" commit -qam "$1"
}

# Starts merging release into the branch, by default staging, as the workflow
# does: origin/staging is the target, and the branch is HEAD.
merge_into() {
  git -C "$repo" update-ref refs/remotes/origin/staging staging
  git -C "$repo" checkout -q "${1:-staging}"
  git -C "$repo" merge -q --no-ff --no-commit release > /dev/null 2>&1 || true
}

# Runs the hook as the workflow does, with the unmerged paths as arguments.
hook() {
  local paths
  mapfile -t paths < <(git -C "$repo" -c core.quotePath=false diff --name-only --diff-filter=U)
  (cd "$repo" && TARGET=staging SOURCE=release "$hook" "${paths[@]}") > "$repo.out" 2> "$repo.err"
}

run_hook() {
  if ! hook; then
    fail=$((fail + 1))
    echo "FAIL: the hook failed:" >&2
    cat "$repo.err" >&2
  fi
}

fails() { ! "$@"; }

# What the workflow's push step reads as unresolved.
unmerged_paths() { git -C "$repo" -c core.quotePath=false diff --name-only --diff-filter=U; }

unmerged_are() { [ "$(unmerged_paths)" = "$1" ]; }

is_unmerged() { unmerged_paths | grep -qxF "$1"; }

same_as_head() { # path
  git -C "$repo" diff --cached --quiet HEAD -- "$1" && fails is_unmerged "$1" && git -C "$repo" diff --quiet -- "$1"
}

staged_has() { git -C "$repo" show ":$1" | grep -qxF "$2"; }

after_marker() { grep -A1 '^<<<<<<<' "$repo/$1" | grep -qxF "$2"; }

near_line_no_markers() { grep -B1 -A2 -xF "$2" "$repo/$1" | fails grep -q '^[<=>]\{7\}'; }

silent() { ! [ -s "$repo.out" ]; }

index_is() { [ "$(git -C "$repo" ls-files -s)" = "$1" ]; }

echo "both sides moved the pin"
setup "$release_rev"
edit staging "$staging_pin"
edit release "$release_pin"
merge_into
ck "the manifest conflicts before the hook" is_unmerged Cargo.toml
run_hook
ck "the manifest is the target's" same_as_head Cargo.toml
ck "the lockfile is the target's" same_as_head Cargo.lock
ck "nothing is left for people" unmerged_are ''

echo "only the source moved the pin"
setup "$staging_pin"
edit release "$release_pin"
merge_into
ck "git merges everything cleanly" unmerged_are ''
run_hook
ck "the pin is restored" same_as_head Cargo.toml
ck "the lockfile is restored" same_as_head Cargo.lock

echo "the source also changed another table"
setup "$release_rev"
edit staging "$staging_pin"
edit release "$release_pin" 1.1
merge_into
run_hook
ck "the manifest is resolved" fails is_unmerged Cargo.toml
ck "the pin is the target's" staged_has Cargo.toml 'rev = "1111111" #staging'
ck "the other change is kept" staged_has Cargo.toml 'version = "1.1"'
ck "the lockfile is left for people" unmerged_are Cargo.lock

echo "the source moved the pin and changed another table, and git merges both"
setup "$staging_pin"
edit release "$release_pin" 1.1
merge_into
ck "git merges everything cleanly" unmerged_are ''
run_hook
ck "the pin is restored" staged_has Cargo.toml 'rev = "1111111" #staging'
ck "the other change is kept" staged_has Cargo.toml 'version = "1.1"'
ck "the lockfile is left for people" unmerged_are Cargo.lock

echo "a conflict outside the table"
setup "$release_rev"
edit staging "$staging_pin" 1.2
edit release "$release_pin" 1.1
merge_into
run_hook
ck "the manifest stays unmerged" is_unmerged Cargo.toml
ck "the remaining markers are in the anyhow table" after_marker Cargo.toml 'version = "1.2"'
ck "the pin has no markers" near_line_no_markers Cargo.toml 'rev = "1111111" #staging'
ck "the lockfile is left for people" is_unmerged Cargo.lock

echo "only the target moved the pin"
setup "$release_rev"
edit staging "$staging_pin"
edit release "$release_rev" 1.0 1.0.0 '# A comment.'
merge_into
run_hook
ck "the manifest is git's merge" staged_has Cargo.toml '# A comment.'
ck "the pin is the target's" staged_has Cargo.toml 'rev = "1111111" #staging'
ck "the lockfile is the target's" same_as_head Cargo.lock
ck "the hook changes nothing" silent

echo "the source bumped a package in the lockfile only"
setup "$staging_pin"
edit release "$staging_pin" 1.0 1.0.1
merge_into
run_hook
ck "the bump is kept" staged_has Cargo.lock 'version = "1.0.1"'
ck "the hook changes nothing" silent
ck "nothing is left for people" unmerged_are ''

echo "the target bumped a package, and the source moved the pin"
setup "$release_rev"
edit staging "$staging_pin" 1.0 1.0.2
edit release "$release_pin"
merge_into
run_hook
ck "the lockfile is the target's" same_as_head Cargo.lock
ck "nothing is left for people" unmerged_are ''

echo "the source added a comment above the next table and moved the pin"
setup "$staging_pin"
edit release "$release_pin" 1.0 1.0.0 '# Pinned for a reason.'
merge_into
run_hook
ck "the pin is restored" staged_has Cargo.toml 'rev = "1111111" #staging'
ck "the comment is kept" staged_has Cargo.toml '# Pinned for a reason.'

echo "the target moved its pin after the pull request's branch forked"
setup "$staging_pin"
git -C "$repo" branch pr staging
edit staging "$staging_bump"
edit release "$release_pin"
merge_into pr
run_hook
ck "the manifest is the branch's" same_as_head Cargo.toml
ck "the lockfile is the branch's" same_as_head Cargo.lock
ck "nothing is left for people" unmerged_are ''

echo "the pull request's branch took the source's pin"
setup "$staging_pin"
git -C "$repo" branch pr staging
edit pr "$release_pin"
edit release "$release_pin" 1.0 1.0.1
merge_into pr
run_hook
ck "the pin is the target's" staged_has Cargo.toml 'rev = "1111111" #staging'
ck "the lockfile is left for people" unmerged_are Cargo.lock

echo "the target has no snarkVM table"
setup "$release_rev"
sed -i '/^\[workspace.dependencies.snarkvm\]/d' "$repo/Cargo.toml"
git -C "$repo" commit -qam "drop the table"
edit release "$release_pin"
merge_into
ck "the hook fails" fails hook
ck "the hook names the table" grep -q 'has no \[workspace.dependencies.snarkvm\] table' "$repo.err"

# The merges merge-into-staging.yml made first, when run from a snarkOS clone.
replay() { # target commit, source commit
  repo=$(mktemp -d -p "$work")
  git clone -q --shared --no-checkout "$top" "$repo"
  git -C "$repo" config user.name t
  git -C "$repo" config user.email t@t
  git -C "$repo" update-ref refs/remotes/origin/staging "$1"
  git -C "$repo" checkout -q --detach "$1"
  git -C "$repo" merge -q --no-ff --no-commit "$2" > /dev/null 2>&1 || true
}

if top=$(git -C "$(dirname "$0")" rev-parse --show-toplevel) && git -C "$top" cat-file -e f2eea52d9 2> /dev/null \
    && git -C "$top" cat-file -e b80d7794d 2> /dev/null; then
  echo "mainnet f2eea52d9 into staging b63526c54"
  replay b63526c54 f2eea52d9
  ck "three paths conflict before the hook" unmerged_are $'.circleci/config.yml\nCargo.lock\nCargo.toml'
  run_hook
  ck "the manifest is staging's" same_as_head Cargo.toml
  ck "the lockfile is staging's" same_as_head Cargo.lock
  ck "only the CircleCI config is left" unmerged_are .circleci/config.yml

  echo "testnet b80d7794d into staging b63526c54"
  replay b63526c54 b80d7794d
  before=$(git -C "$repo" ls-files -s)
  run_hook
  ck "the hook changes nothing" index_is "$before"
  ck "the hook prints nothing" silent
else
  echo "skipped the replays: this clone lacks mainnet f2eea52d9 or testnet b80d7794d"
fi

echo "$pass passed, $fail failed"
[ "$fail" = 0 ]
