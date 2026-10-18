# CI overview

CI runs on GitHub Actions, in [`ci.yml`](.github/workflows/ci.yml). Benchmarks
run from [`benchmarks.yml`](.github/workflows/benchmarks.yml).

Jobs run in one of two places:

- **GitHub-hosted runners** for cheap jobs: formatting, `cargo deny`,
  shellcheck, `cargo machete`, the Windows checks and the lighter crates'
  tests. These restore `~/.cargo` and `target/` with `Swatinem/rust-cache`,
  saved only from `staging`.
- **The self-hosted `cpu` fleet** for heavy jobs: clippy, the heavier
  crates' tests, the `snarkos` build and the devnets. `start-cpu-runners`
  starts the boxes the run needs; they stop themselves when idle. Each box
  keeps a persistent `target/` and sccache, so builds there are incremental.

## Triggers

| Event | Runs |
|---|---|
| Pull request | `fmt`, `deny`, `shellcheck`, `machete`, `lint`, `test`, `build-snarkos`, `devnet`, `ci-ok` |
| Push to `staging`, `canary`, `testnet`, `mainnet`, or manual dispatch | the above, plus `devnet-merge`, `windows` |

## Jobs

A job marked "merge" runs only on the second row of the table above. The
other jobs run on every event.

| Job | Runner | Trigger | Timeout (min) | What it runs |
|---|---|---|---|---|
| `start-cpu-runners` | hosted | all | | starts fleet boxes |
| `check-shared-refs` | hosted | all | | checks that each `aleo-labs-protocol/gh-actions` ref is a release tag |
| `fmt` | hosted | all | | `cargo fmt --check` on `FMT_TOOLCHAIN` |
| `deny` | hosted | all | | `cargo deny check bans licenses sources`; advisories are checked nightly by [`bump-advisories.yml`](.github/workflows/bump-advisories.yml) |
| `shellcheck` | hosted | all | | `.ci/*.sh`, `scripts/*.sh` |
| `lint` | `cpu` | all | 90 | clippy, default and `--all-features`; `cargo check --all-features` of `snarkos-node-metrics` and `snarkos-display` |
| `test (light)` | hosted | all | 90 | `snarkos-account`, `-utilities`, `-node-tcp`, `-node-sync-locators`, `-node-network` |
| `test (cdn)` | hosted | all | 90 | `snarkos-node-cdn` |
| `test (messages)` | hosted | all | 90 | `snarkos-node-bft-events`, `-node-router-messages`, `-node-bft-ledger-service` |
| `test (services)` | `cpu` | all | 90 | `snarkos-node-consensus`, `-node-rest`, `-cli` |
| `test (node)` | `cpu` | all | 90 | `snarkos-node` with `snarkos-node-bft-storage-service` (features `memory,persistent`) |
| `test (bft)` | `cpu` | all | 90 | `snarkos-node-bft` |
| `test (router)` | `cpu` | all | 90 | `snarkos-node-router` |
| `test (sync)` | `cpu` | all | 90 | `snarkos-node-sync` |
| `build-snarkos` | `cpu` | all | 60 | release `snarkos` with `test_network`, as an artifact |
| `devnet (devnet)` | `cpu` | all | 40 | `test_db_backup.sh`, `test_devnet.sh` |
| `devnet (dev-on-prod-restart)` | `cpu` | merge | 45 | `test_dev_on_prod_restart.sh` |
| `devnet (chaotic-minority-reset)` | `cpu` | merge | 45 | `test_reset_minority.sh`, with delays |
| `devnet (chaotic-majority-restart)` | `cpu` | merge | 45 | `test_restart_majority.sh`, with delays |
| `devnet (upgrade)` | `cpu` | merge | 90 | `test_full_upgrade.sh`, `test_partial_upgrade.sh` |
| `machete` | hosted | all | | `cargo machete` |
| `windows` | hosted (Windows) | merge | | `cargo check --examples --benches --tests` in `account`, `cli`, `display`, `node` |
| `ci-ok` | hosted | all | | fails unless every job a pull request runs succeeded |

A job with no timeout gets the GitHub default of six hours. Only hosted jobs
have none, and a hosted job holds no fleet box.

Protected branches require `ci-ok` alone. GitHub counts a skipped job as
passed for a required check, and a fleet job is skipped when
`start-cpu-runners` fails or the pull request comes from a fork; `ci-ok`
fails in both cases.

## Test groups

Each `test` entry is one cargo build. `cargo test -p a -p b` unifies the
features of `a` and `b`, and several crates enable features on their
dependencies that change what those dependencies do, such as
`snarkos-node-router/test` and `snarkvm-ledger-block/test`. So crates share an
entry only where that unification enables no such feature on any crate.
Helper features such as `test-helpers` and `mock` can still be unified, so a
test may compile in its entry that would not under `cargo test -p <crate>`
alone. Check this before moving a crate between entries, for example by
comparing `cargo tree -e features` for the entry against each crate alone.

[`.config/nextest.toml`](.config/nextest.toml) sets two test threads. Each
entry uploads nextest's JUnit report as the artifact `junit-<entry>`, with the
time of each test.

## Devnets

`build-snarkos` builds the node once with `--features test_network`. Each
`devnet` and `devnet-merge` entry downloads it and runs scripts from `.ci` on
one box. A box has one runner slot, so the nodes' fixed ports never collide
with another job's.

Each `test` and devnet job on the fleet gets its own home directory and
`TMPDIR`, so ledgers and node data from one run never reach the next.
Downloaded parameters and dev genesis blocks persist on the box; the genesis
blocks are keyed by `Cargo.lock` and `cli/src/commands/start.rs`. The
`upgrade` entry also runs the latest release, so its genesis blocks have a key
of their own. It builds that release the first time a box sees its tag, in a
target dir of its own, and keeps it. The chaotic entries delay loopback
traffic through the boxes' `runner-netem` helper, which the runner user may
run with `sudo`. Every devnet entry stops leftover `snarkos` processes and
clears the delays before it starts and after it ends.

To run the `.ci` scripts on macOS you may want to `brew install bash coreutils
&& echo 'export PATH="/opt/homebrew/bin:$PATH"' >> ~/.bash_profile`.

## Forks

A fork's pull request runs nothing on the fleet. Every self-hosted job checks
that the head repository is this one. The fleet's `target/` and sccache
persist between jobs, so anyone who can push a branch here can affect later
builds there. That is the same trust as push access; do not widen the guard.

The guard is not what holds. A pull request runs its own copy of `ci.yml`, so
a fork can delete the guard and target the fleet directly. What holds is the
repository setting that makes every external contributor's workflow run wait
for approval. Read a fork's workflow changes before approving its run.
