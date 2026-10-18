#!/usr/bin/env bash
# Tests merge-fixup.sh against throwaway repositories.
set -euo pipefail

hook=$(realpath "$(dirname "$0")/merge-fixup.sh")
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

manifest() { # rev, extra line in the anyhow table
  cat <<EOF
[workspace]
members = [ "a" ]

[workspace.dependencies.snarkvm]
#path = "../snarkVM"
${1}
default-features = false

[workspace.dependencies.anyhow]
version = "${2:-1.0}"
EOF
}

staging_pin='git = "https://github.com/ProvableHQ/snarkVM.git"
rev = "1111111" #staging'
release_pin='#git = "https://github.com/ProvableHQ/snarkVM.git"
version = "=4.12.0"'
release_rev='git = "https://github.com/ProvableHQ/snarkVM.git"
rev = "2222222" #test_net_v4120'

# Makes a repository at $repo with branches staging and release from a common
# base, then starts merging release into staging.
#   $1: base pin   $2: staging pin   $3: release pin
#   $4: staging anyhow version   $5: release anyhow version
setup() {
  repo=$(mktemp -d)
  git -C "$repo" init -q -b staging
  git -C "$repo" config user.name t
  git -C "$repo" config user.email t@t
  mkdir "$repo/a"
  manifest "$1" > "$repo/Cargo.toml"
  echo '[package]' > "$repo/a/Cargo.toml"
  echo "lock $1" > "$repo/Cargo.lock"
  git -C "$repo" add -A
  git -C "$repo" commit -qm base
  git -C "$repo" branch release
  edit staging "$2" "${4:-1.0}"
  edit release "$3" "${5:-1.0}"
  git -C "$repo" checkout -q staging
  git -C "$repo" merge -q --no-ff --no-commit release > /dev/null 2>&1 || true
}

edit() { # branch, pin, anyhow version
  git -C "$repo" checkout -q "$1"
  manifest "$2" "$3" > "$repo/Cargo.toml"
  echo "lock $2" > "$repo/Cargo.lock"
  git -C "$repo" commit -qam "$1" > /dev/null || true
}

run_hook() {
  # shellcheck disable=SC2046
  (cd "$repo" && TARGET=staging SOURCE=release "$hook" $(git ls-files -u | cut -f2 | sort -u)) > "$repo.out" 2> "$repo.err"
}

fails() { ! "$@"; }

same_as_head() { # path
  git -C "$repo" diff --cached --quiet HEAD -- "$1" && ! is_unmerged "$1" && git -C "$repo" diff --quiet -- "$1"
}

is_unmerged() { [ -n "$(git -C "$repo" ls-files -u -- "$1")" ]; }

has_markers() { grep -q '^<<<<<<<' "$repo/$1"; }

echo "both sides moved the pin"
setup "$release_rev" "$staging_pin" "$release_pin"
ck "the manifest conflicts before the hook" is_unmerged Cargo.toml
run_hook
ck "the manifest is the target's" same_as_head Cargo.toml
ck "the lockfile is the target's" same_as_head Cargo.lock

echo "only the source moved the pin"
setup "$staging_pin" "$staging_pin" "$release_pin"
ck "git merges the manifest cleanly" bash -c "[ -z \"\$(git -C $repo ls-files -u)\" ]"
run_hook
ck "the pin is restored" same_as_head Cargo.toml
ck "the lockfile is restored" same_as_head Cargo.lock

echo "the source also changed another table"
setup "$release_rev" "$staging_pin" "$release_pin" 1.0 1.1
run_hook
ck "the manifest is resolved" bash -c "! git -C $repo ls-files -u | grep -q Cargo.toml"
ck "the pin is the target's" bash -c "git -C $repo show :Cargo.toml | grep -qx 'rev = \"1111111\" #staging'"
ck "the other change is kept" bash -c "git -C $repo show :Cargo.toml | grep -qx 'version = \"1.1\"'"
ck "the lockfile is left for people" is_unmerged Cargo.lock

echo "a conflict outside the table"
setup "$release_rev" "$staging_pin" "$release_pin" 1.2 1.1
run_hook
ck "the manifest stays unmerged" is_unmerged Cargo.toml
ck "the remaining markers are in the anyhow table" bash -c "grep -A1 '^<<<<<<<' $repo/Cargo.toml | grep -qx 'version = \"1.2\"'"
ck "the pin has no markers" bash -c "grep -B1 -A2 '^rev = \"1111111\" #staging' $repo/Cargo.toml | grep -vq '^[<=>]\{7\}'"
ck "the lockfile is left for people" is_unmerged Cargo.lock

echo "only the target moved the pin"
setup "$release_rev" "$staging_pin" "$release_rev"
run_hook
ck "the manifest is the target's" same_as_head Cargo.toml
ck "the hook changes nothing" bash -c "! [ -s $repo.out ]"

echo "the target has no snarkVM table"
setup "$release_rev" "$release_rev" "$release_pin"
git -C "$repo" merge --abort
sed -i '/^\[workspace.dependencies.snarkvm\]/d' "$repo/Cargo.toml"
git -C "$repo" commit -qam "drop the table"
git -C "$repo" merge -q --no-ff --no-commit release > /dev/null 2>&1 || true
ck "the hook fails" fails run_hook
ck "the hook names the table" grep -q 'has no \[workspace.dependencies.snarkvm\] table' "$repo.err"

# The merges merge-into-staging.yml made first, when run from a snarkOS clone.
if top=$(git -C "$(dirname "$0")" rev-parse --show-toplevel) && git -C "$top" cat-file -e f2eea52d9 2> /dev/null; then
  echo "mainnet f2eea52d9 into staging b63526c54"
  repo=$(mktemp -d)
  git -C "$top" worktree add -q --detach "$repo" b63526c54
  git -C "$repo" merge -q --no-ff --no-commit f2eea52d9 > /dev/null 2>&1 || true
  ck "three paths conflict before the hook" bash -c "[ \"\$(git -C $repo ls-files -u | cut -f2 | sort -u | wc -l)\" = 3 ]"
  run_hook
  ck "the manifest is staging's" same_as_head Cargo.toml
  ck "the lockfile is staging's" same_as_head Cargo.lock
  ck "only the CircleCI config is left" bash -c "[ \"\$(git -C $repo ls-files -u | cut -f2 | sort -u)\" = .circleci/config.yml ]"
  git -C "$top" worktree remove --force "$repo"

  echo "testnet into staging b63526c54"
  repo=$(mktemp -d)
  git -C "$top" worktree add -q --detach "$repo" b63526c54
  git -C "$repo" merge -q --no-ff --no-commit "$(git -C "$top" rev-parse origin/testnet)" > /dev/null 2>&1
  before=$(git -C "$repo" ls-files -s)
  run_hook
  ck "the hook changes nothing" bash -c "[ \"\$(git -C $repo ls-files -s)\" = '$before' ] && ! [ -s $repo.out ]"
  git -C "$top" worktree remove --force "$repo"
else
  echo "skipped the replays: this clone lacks mainnet f2eea52d9"
fi

echo "$pass passed, $fail failed"
[ "$fail" = 0 ]
