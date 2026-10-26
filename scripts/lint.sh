#!/usr/bin/env bash
#
# Runs the same lints as CI: clippy, rustfmt, cargo-deny, and shellcheck.
# Invoked by the rusty-hook pre-commit hook (see .rusty-hook.toml).

set -e

echo "Running clippy..."
cargo clippy --workspace --all-targets --all-features -- -D warnings

echo "Checking rustfmt..."
cargo +nightly fmt --all -- --check

if command -v cargo-deny >/dev/null; then
  echo "Running cargo deny..."
  cargo deny check bans licenses sources
else
  echo "warning: cargo-deny not installed, skipping license/bans/sources check (CI will still run it)"
fi

if command -v shellcheck >/dev/null; then
  echo "Running shellcheck..."
  shellcheck -x .ci/*.sh scripts/*.sh
else
  echo "warning: shellcheck not installed, skipping script lint (CI will still run it)"
fi
