# CI overview

CI runs on GitHub Actions, in [`ci.yml`](.github/workflows/ci.yml). Benchmarks
run from [`benchmarks.yml`](.github/workflows/benchmarks.yml).

Jobs run in one of two places:

- **GitHub-hosted runners** for cheap jobs: formatting, `cargo deny`,
  shellcheck, the source checks, `cargo machete`, the Windows checks and the lighter crates'
  tests. The hosted test jobs restore `~/.cargo` and `target/` with
  `Swatinem/rust-cache`, saved only from `staging`. The Windows checks build
  cold. Their caches would take about half of the repository's 10 GB cache
  limit and evict the test jobs' entries.
- **The self-hosted `cpu` fleet** for heavy jobs: clippy, the heavier
  crates' tests, the `snarkos` build and the devnets. `start-cpu-runners`
  starts the boxes the run needs; they stop themselves when idle. Each box
  keeps a persistent `target/` and sccache, so builds there are incremental.

## Triggers

| Event | Runs |
|---|---|
| Pull request into `staging` | `fmt`, `deny`, `shellcheck`, `source-checks`, `machete`, `lint`, `test`, `build-snarkos`, `devnet`, `ci-ok` |
| Push to `staging` | the above, plus `devnet-merge`, `windows` |
| Pull request into `release/**`, push to `release/**`, or manual dispatch | the above, plus `devnet-release` |

## Jobs

A job marked "merge" runs on the second and third rows of the table above, and
a job marked "release" only on the third. The other jobs run on every event.

| Job | Runner | Trigger | Timeout (min) | What it runs |
|---|---|---|---|---|
| `start-cpu-runners` | hosted | all | | starts fleet boxes |
| `check-shared-refs` | hosted | all | | checks that each `aleo-labs-protocol/gh-actions` ref is a release tag |
| `fmt` | hosted | all | | `cargo fmt --check` on `FMT_TOOLCHAIN` |
| `deny` | hosted | all | | `cargo deny --workspace check bans licenses sources`, plus `advisories` everywhere but `staging`, whose advisories [`bump-advisories.yml`](.github/workflows/bump-advisories.yml) bumps nightly |
| `shellcheck` | hosted | all | | `.ci/*.sh`, `scripts/*.sh` |
| `source-checks` | hosted | all | | `scripts/check_sources.py`: license headers and `locktick` imports in `.rs` files |
| `lint` | `cpu` | all | 90 | clippy, default and `--all-features`; `cargo check --all-features` of `snarkos-node-metrics` and `snarkos-display` |
| `test (light)` | hosted | all | 90 | `snarkos-account`, `-utilities`, `-node-tcp`, `-node-sync-locators`, `-node-network` |
| `test (cdn)` | hosted | all | 90 | `snarkos-node-cdn` |
| `test (messages)` | hosted | all | 90 | `snarkos-node-bft-events`, `-node-router-messages`, `-node-bft-ledger-service` |
| `test (services)` | `cpu` | all | 90 | `snarkos-node-consensus`, `-cli`; then `-node-rest` |
| `test (node)` | `cpu` | all | 90 | `snarkos-node`, `-node-bft-storage-service` |
| `test (bft)` | `cpu` | all | 90 | `snarkos-node-bft` with `metrics` |
| `test (router-sync)` | `cpu` | all | 90 | `snarkos-node-router`; then `-node-sync` |
| `build-snarkos` | `cpu` | all | 60 | release `snarkos` with `test_network`, as an artifact |
| `devnet (devnet)` | `cpu` | all | 40 | `test_db_backup.sh`, `test_devnet.sh` |
| `devnet (simple-node)` | `cpu` | all | 60 | `test_simple_node.sh` |
| `devnet (dev-on-prod-restart)` | `cpu` | merge | 45 | `test_dev_on_prod_restart.sh` |
| `devnet (chaotic-minority-reset)` | `cpu` | merge | 45 | `test_reset_minority.sh`, with delays |
| `devnet (chaotic-majority-restart)` | `cpu` | merge | 45 | `test_restart_majority.sh`, with delays |
| `devnet (upgrade)` | `cpu` | merge | 90 | `test_full_upgrade.sh`, `test_partial_upgrade.sh` |
| `devnet (upgrade-required)` | `cpu` | release | 30 | `test_upgrade_required.sh` |
| `devnet (chaotic-restart-all)` | `cpu` | release | 45 | `test_restart_all.sh`, with delays |
| `machete` | hosted | all | | `cargo machete` |
| `windows` | hosted (Windows) | merge | | `cargo check --examples --benches --tests` in `account`, `cli`, `display`, `node` |
| `ci-ok` | hosted | all | | fails unless every job succeeded; a merge or release job may be skipped where it is not meant to run |

A job with no timeout gets the GitHub default of six hours. Only hosted jobs
have none, and a hosted job holds no fleet box.

Protected branches require `ci-ok` alone. GitHub counts a skipped job as
passed for a required check, and a fleet job is skipped when
`start-cpu-runners` fails; `ci-ok` fails then.

A pull request that changes only Markdown files starts no run, so it gets no
`ci-ok` check. Where a branch rule requires `ci-ok`,
such a pull request cannot merge until a workflow reports that check for it.

## Test groups

Each line of a `test` entry is one cargo build. `cargo test -p a -p b` unifies
the features of `a` and `b`, and several crates enable features on their
dependencies that change what those dependencies do, such as
`snarkos-node-router/test-helpers` and `snarkvm-ledger-block/test`. So crates
share a line only where that unification enables no such feature on any crate.
Other helper features, such as `mock` and the other crates' `test-helpers`, can
still be unified, so a test may
compile in its line that would not under `cargo test -p <crate>` alone. Check
this before moving a crate between lines, for example by comparing
`cargo tree -e features` for the line against each crate alone.

`test (bft)` runs with `metrics`, which the `snarkos` binary enables by
default, so the tests in `helpers::telemetry` compile. The `each-feature` job
checks the crate without `metrics`. The `simple_node` line passes
`--no-tests=fail`, because the step otherwise lets a command match no tests.

[`.config/nextest.toml`](.config/nextest.toml) sets two test threads, which
the hosted entries use. The fleet entries run 16 tests at a time through
`NEXTEST_TEST_THREADS`: most of their slow tests wait on timers rather than
the CPU, and a box runs one job. `gateway::prop_tests::gateway_start` runs
alone, because it asserts a fixed port that other tests may take. Each entry
uploads nextest's JUnit reports as the artifact `junit-<entry>`, one file per
line, with the time of each test.

## Devnets

`build-snarkos` builds the node once with `--features test_network`. Each
`devnet`, `devnet-merge` and `devnet-release` entry downloads it and runs
scripts from `.ci` on one box. A box has one runner slot, so the nodes' fixed
ports never collide with another job's.

Each `test` and devnet job on the fleet gets its own home directory and
`TMPDIR`, so ledgers and node data from one run never reach the next.
Downloaded parameters and dev genesis blocks persist on the box; the genesis
blocks are keyed by `Cargo.lock` and `cli/src/commands/start.rs`. The
`upgrade` entry also runs the latest release, so its genesis blocks have a key
of their own. It builds that release the first time a box sees its tag, in a
target dir of its own, and keeps it. The chaotic entries delay loopback
traffic through the boxes' `runner-netem` helper, which the runner user may
run with `sudo`. Every devnet entry stops leftover `snarkos` and `simple_node`
processes and clears the delays before it starts and after it ends.

To run the `.ci` scripts on macOS you may want to `brew install bash coreutils
&& echo 'export PATH="/opt/homebrew/bin:$PATH"' >> ~/.bash_profile`.

## Forks

A fork's pull request runs on the fleet once a team member approves its run.
The run gets no secrets, so `start-cpu-runners` is skipped;
`fork-pr-runners.yml` starts the boxes instead, and the fleet jobs accept the
skipped start.

The fleet's `target/` and sccache persist between jobs, so an approved run can
affect later builds there. A pull request runs its own copy of `ci.yml`, so no
guard in that file holds. What holds is the repository setting that makes every
external contributor's workflow run wait for approval. Read a fork's changes
before approving its run.
