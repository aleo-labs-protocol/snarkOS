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
  crates' tests, the `snarkos` build and the devnets. The org's fleet starter
  starts the boxes when GitHub queues their jobs; they stop themselves when
  idle. Each box keeps a persistent `target/` and sccache, so builds there are
  incremental.

## Triggers

| Event | Runs |
|---|---|
| Pull request into `staging` | `fmt`, `deny`, `shellcheck`, `source-checks`, `machete`, `lint`, `lint-locktick`, `test`, `build-snarkos`, `devnet`, `ci-ok` |
| Schedule, on `staging`, every six hours at 47 minutes past | the above, plus `devnet-merge`, `windows`, `each-feature` |
| Pull request into `release/**` | the first row, plus `devnet-merge`, `windows`, `devnet-release` |
| Push to `release/**`, or manual dispatch | the first row, plus `devnet-merge`, `windows`, `each-feature`, `devnet-release` |

A push to `staging` starts no run. The schedule checks `staging`'s head
instead, so `staging` is checked after each merge and not before it.

## The schedule

The `schedule-due` job runs first in a scheduled run. It decides whether the
head of `staging` is due a check:

- The head is not due if an earlier scheduled run on it gave a verdict: the
  run passed or failed, and its `fmt` job ran. Every other job in the run is
  then skipped, and so is `ci-ok`. The job's summary names that run.
- A cancelled run, a run that was not due, and a run where `schedule-due`
  failed give no verdict. A head with only such runs is due.
- A failed head is not checked again on the next tick. To retry it, re-run
  the scheduled run's jobs. A re-run is always due.
- If the GitHub API fails, the head is due.

Scheduled runs have concurrency groups of their own. Two on one commit run
one after the other, and no push or pull request run cancels one.

A day with no merge into `staging` costs only the hosted minutes of
`schedule-due` and `notify`.

The `notify` job posts to Slack when the scheduled run turns red, and once when
it is green again. A run that was not due changes nothing. The channel is the
repository variable `CI_SLACK_CHANNEL`, `#ci` by default. The job runs in the
`ci-notify` environment, which admits only `staging` and holds the
`CI_SLACK_TOKEN` secret. The nightly run of
[`bump-advisories.yml`](.github/workflows/bump-advisories.yml) has a `notify`
job of its own, which posts when that run fails, and once when it passes again.
Each workflow keeps its own state.

A schedule fires only in the default branch's copy of `ci.yml`.
`snarkOS-internal` mirrors `staging` with this file, so its schedule fires too.
There `schedule-due` is skipped, and so is every job after it.

## Jobs

A job marked "merge" runs on the schedule, on pull requests into and pushes to
`release/**`, and on manual dispatch. A job marked "release" runs on the last
two rows of the table above. `each-feature` runs on no pull request. The other
jobs run on every event.

| Job | Runner | Trigger | Timeout (min) | What it runs |
|---|---|---|---|---|
| `schedule-due` | hosted | schedule | 10 | decides whether `staging`'s head is due a check |
| `check-shared-refs` | hosted | all | | checks that each `aleo-labs-protocol/gh-actions` ref is a release tag, and that each matrix `name` axis lists the same names as its `include` |
| `fmt` | hosted | all | | `cargo fmt --check` on `FMT_TOOLCHAIN` |
| `deny` | hosted | all | | `cargo deny --workspace check bans licenses sources`, plus `advisories` everywhere but `staging`, whose advisories and yanked crates [`bump-advisories.yml`](.github/workflows/bump-advisories.yml) bumps nightly. A yanked crate fails `advisories` |
| `shellcheck` | hosted | all | | `.ci/*.sh`, `scripts/*.sh` |
| `source-checks` | hosted | all | | `scripts/check_sources.py`: license headers in `.rs` files |
| `lint` | `cpu` | all | 90 | clippy, default and `--all-features`; `cargo check --all-features` of `snarkos-node-metrics` and `snarkos-display` |
| `lint-locktick` | `cpu` | all | 90 | clippy `--all-features` under `--cfg locktick` (`locktick-clippy` alias) |
| `test (light)` | hosted | all | 90 | `snarkos-account`, `-utilities`, `-node-tcp`, `-node-sync-locators`, `-node-network` |
| `test (cdn)` | hosted | all | 90 | `snarkos-node-cdn` |
| `test (messages)` | hosted | all | 90 | `snarkos-node-bft-events`, `-node-router-messages`, `-node-bft-ledger-service` |
| `test (services)` | `cpu` | all | 90 | `snarkos-node-consensus`, `-cli` |
| `test (node-rest-distribution)` | `cpu` | all | 90 | `snarkos-node`, `-node-bft-storage-service`; then `-node-rest`, `snarkos-ledger-distribution`; one build |
| `test (bft)` | `cpu` | all | 90 | `snarkos-node-bft` with `metrics` |
| `test (router-sync)` | `cpu` | all | 90 | `snarkos-node-router`; then `-node-sync` |
| `build-snarkos` | `cpu` | all | 60 | release `snarkos` with `dev-network`, as an artifact |
| `devnet (devnet)` | `cpu` | all | 40 | `test_db_backup.sh`, `test_devnet.sh` |
| `devnet (simple-node)` | `cpu` | all | 60 | `test_simple_node.sh` |
| `devnet-merge (dev-on-prod-restart)` | `cpu` | merge | 90 | release `snarkos` with `dev-committee-override`, then `test_dev_on_prod_restart.sh` |
| `devnet-merge (chaotic-minority-reset)` | `cpu` | merge | 45 | `test_reset_minority.sh`, with delays |
| `devnet-merge (chaotic-majority-restart)` | `cpu` | merge | 45 | `test_restart_majority.sh`, with delays |
| `devnet-merge (upgrade)` | `cpu` | merge | 90 | `test_full_upgrade.sh`, `test_partial_upgrade.sh`, on testnet |
| `devnet-release (upgrade-required)` | `cpu` | release | 30 | `test_upgrade_required.sh` |
| `devnet-release (chaotic-restart-all)` | `cpu` | release | 45 | `test_restart_all.sh`, with delays |
| `machete` | hosted | all | | `cargo machete` |
| `windows` | hosted (Windows) | merge | | `cargo check --examples --benches --tests` in `account`, `cli`, `display`, `node` |
| `each-feature` | `cpu` | merge, no pull request | 120 | `cargo hack clippy --each-feature` over the workspace, without `cuda` |
| `ci-ok` | hosted | all | | fails unless every job succeeded; a job may be skipped where it is not meant to run |
| `notify` | hosted | schedule | 10 | posts to Slack when the scheduled run turns red or green |

A job with no timeout gets the GitHub default of six hours. Only hosted jobs
have none, and a hosted job holds no fleet box.

Protected branches require `ci-ok` alone. GitHub counts a skipped job as
passed for a required check, so `ci-ok` checks each job's result itself.

A pull request that changes only Markdown files starts no run, so it gets no
`ci-ok` check. Where a branch rule requires `ci-ok`,
such a pull request cannot merge until a workflow reports that check for it.

## Test groups

Each line of a `test` entry is one cargo command. Lines with the same `-p` set
and cargo flags share one build, as the two lines of
`test (node-rest-distribution)` do: nextest's `-E` filters split that build's
tests between them. `cargo test -p a -p b` unifies the features of `a` and
`b`, and several crates enable features on their
dependencies that change what those dependencies do, such as
`snarkos-node-router/test-helpers`. So crates share a line only where that
unification enables no such feature on any crate.
Other helper features, such as `mock` and the other crates' `test-helpers`, can
still be unified, so a test may
compile in its line that would not under `cargo test -p <crate>` alone. Check
this before moving a crate between lines, for example by comparing
`cargo tree -e features` for the line against each crate alone.

`test (bft)` runs with `metrics`, which the `snarkos` binary enables by
default, so the tests in `helpers::telemetry` compile. The `each-feature` job
checks the crate without `metrics`. The `simple_node` line and the two
`node-rest-distribution` lines pass `--no-tests=fail`, because the step
otherwise lets a command match no tests.

[`.config/nextest.toml`](.config/nextest.toml) sets two test threads, which
the hosted entries use. The fleet entries run 16 tests at a time through
`NEXTEST_TEST_THREADS`: most of their slow tests wait on timers rather than
the CPU, and a box runs one job. `gateway::prop_tests::gateway_start` runs
alone, because it asserts a fixed port that other tests may take. Each entry
uploads nextest's JUnit reports as the artifact `junit-<entry>`, one file per
line, with the time of each test.

## Devnets

`build-snarkos` builds the node once with `--features dev-network`. Each
`devnet`, `devnet-merge` and `devnet-release` entry downloads it and runs
scripts from `.ci` on one box. The `dev-on-prod-restart` entry instead builds
the node with `--features dev-committee-override`, because `--dev-on-prod`
needs production network parameters. A box has one runner slot, so the nodes' fixed
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
GitHub then queues its fleet jobs, and the fleet starter boots boxes for them
as for any other run.

The fleet's `target/` and sccache persist between jobs, so an approved run can
affect later builds there. A pull request runs its own copy of `ci.yml`, so no
guard in that file holds. What holds is the repository setting that makes every
external contributor's workflow run wait for approval. Read a fork's changes
before approving its run.
