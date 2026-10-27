# Agent Guidelines

## Formatting and Lints

Always run the following after any code modification:

```bash
cargo +nightly-2026-04-02 fmt
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Formatting requires the nightly `.github/workflows/ci.yml` pins as `FMT_TOOLCHAIN` --
`.rustfmt.toml` uses nightly-only options, and plain `cargo fmt` drops them
silently rather than refusing.

## Code comments

Comments must describe the state of the code today -- not a previous state or an
alternative state.

Exception: Comments may describe an alternative possible state of the code,
*if* they are documenting a hazard that a future developer may otherwise walk into.

Comments must not describe the rationale for a change.

**Rationale may appear in: the commit message and the pull request.**
You may self-comment on your own github pull request at select positions in the code to aide reviewers.

Comments must help someone who has never heard of this change you are making.

Comments must not state the obvious. Comments that explain what attributes
do or how language constructs work are unhelpful.

## Pull requests
- Follow `.github/PULL_REQUEST_TEMPLATE.md`.
- Write the PR README in Simplified Technical English. Use short sentences, the active voice, and one idea per sentence.
- Keep the pull request description succinct and DRY.
- Explain each commit in one line.

## Modifying BFT Code

When making changes to BFT-related code (anything under `node/bft/`), run the following checks in order:

### 1. Unit tests

```bash
cargo test -p snarkos-node-bft --lib
```

### 2. Build with test features

The CI scripts invoke `snarkos` by name, so the binary must be on PATH. Build it with the
`test_network` feature and prepend the output directory to PATH:

```bash
cargo build --features test_network
export PATH="$PWD/target/debug:$PATH"
```

### 3. Devnet test

```bash
.ci/test_devnet.sh
```

### 4. Additional CI tests

Ask the user whether they want to run the following tests before merging. These cannot run
concurrently and take significant time in total, so they are not always run on every change.

```bash
.ci/test_partial_upgrade.sh
.ci/test_full_upgrade.sh
.ci/test_restart_majority.sh
.ci/test_reset_minority.sh
.ci/test_restart_all.sh
```
