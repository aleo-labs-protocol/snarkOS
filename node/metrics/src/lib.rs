// Copyright (c) 2019-2026 Provable Inc.
// This file is part of the snarkOS library.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod names;

// Expose the names at the crate level for easy access.
pub use names::*;

// Re-export the snarkVM metrics. This includes snarkVM's re-export of the `metrics` crate, which
// this crate uses as `metrics::`.
pub use snarkvm::metrics::*;

use rayon::prelude::*;

use snarkos_utilities::locks::parking_lot::Mutex;
use snarkvm::{
    ledger::puzzle::SolutionID,
    prelude::{Block, Network, Transaction, cfg_iter},
};
use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    sync::{
        OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};
use tokio::task::JoinHandle;

/// The handle to the task running the metrics exporter.
static EXPORTER_TASK: OnceLock<JoinHandle<()>> = OnceLock::new();

/// Initializes the metrics and starts the metrics exporter.
///
/// Note: the exporter runs until [`shut_down_metrics`] is called, or until the process exits.
pub fn initialize_metrics(ip: Option<SocketAddr>) {
    // Build the Prometheus recorder and exporter.
    //
    // Note: this is `PrometheusBuilder::install` spelled out, so that the handle to the exporter
    // task can be retained. `install` spawns the task and immediately drops its handle, which
    // leaves the exporter running after the node has shut down; see `shut_down_metrics`.
    // Seconds-scale latency buckets, shared by all `*_secs` histograms (including `BLOCK_LAG`) plus
    // `TRANSMISSION_LATENCY`.
    // Unitless count buckets, for the `snarkos_bft_subdag_*` histograms.
    //
    // Note: these are first-pass boundaries, not yet tuned against production data.
    const SECS_BUCKETS: &[f64] = &[0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 3.0, 5.0, 10.0];
    const COUNT_BUCKETS: &[f64] = &[1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, 34.0, 55.0, 89.0];
    // Verification buckets, for `verification::TRANSACTION_CHECK_SECS`. They span checks that skip
    // proof verification, up to full checks of deployments, which take seconds.
    const VERIFICATION_SECS_BUCKETS: &[f64] =
        &[0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0];

    let builder = metrics_exporter_prometheus::PrometheusBuilder::new()
        .set_buckets_for_metric(metrics_exporter_prometheus::Matcher::Suffix("_secs".to_string()), SECS_BUCKETS)
        .expect("can't set the seconds-scale histogram buckets")
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Full(consensus::TRANSMISSION_LATENCY.to_string()),
            SECS_BUCKETS,
        )
        .expect("can't set the transmission latency histogram buckets")
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Full(verification::TRANSACTION_CHECK_SECS.to_string()),
            VERIFICATION_SECS_BUCKETS,
        )
        .expect("can't set the verification histogram buckets")
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Prefix("snarkos_bft_subdag_".to_string()),
            COUNT_BUCKETS,
        )
        .expect("can't set the subdag count histogram buckets");
    let (recorder, exporter) = if let Some(ip) = ip { builder.with_http_listener(ip) } else { builder }
        .build()
        .expect("can't build the prometheus exporter");
    metrics::set_global_recorder(recorder).expect("can't install the prometheus recorder");

    // Spawn the exporter, retaining its handle so that it can be stopped on shutdown.
    let task = tokio::spawn(async move {
        if let Err(error) = exporter.await {
            tracing::error!("The metrics exporter has failed: {error:?}");
        }
    });
    if let Err(task) = EXPORTER_TASK.set(task) {
        // Note: unreachable in practice, as installing the recorder twice fails above.
        task.abort();
        tracing::warn!("The metrics exporter was already initialized");
    }

    // Register the snarkVM metrics.
    snarkvm::metrics::register_metrics();

    // Register the metrics so they exist on init.
    for name in crate::names::GAUGE_NAMES {
        register_gauge(name);
    }
    for name in crate::names::COUNTER_NAMES {
        register_counter(name);
    }
    for name in crate::names::HISTOGRAM_NAMES {
        register_histogram(name);
    }

    // Set the build information metric
    set_build_info();
}

/// Stops the task running the metrics exporter, if one was started.
pub fn shut_down_metrics() {
    if let Some(task) = EXPORTER_TASK.get() {
        tracing::info!("Shutting down the metrics exporter...");
        task.abort();
    }
}

pub fn update_block_metrics<N: Network>(block: &Block<N>) {
    use snarkvm::ledger::ConfirmedTransaction;

    let accepted_deploy = AtomicUsize::new(0);
    let accepted_execute = AtomicUsize::new(0);
    let rejected_deploy = AtomicUsize::new(0);
    let rejected_execute = AtomicUsize::new(0);

    // Add transaction to atomic counter based on enum type match.
    cfg_iter!(block.transactions()).for_each(|tx| match tx {
        ConfirmedTransaction::AcceptedDeploy(_, _, _) => {
            accepted_deploy.fetch_add(1, Ordering::Relaxed);
        }
        ConfirmedTransaction::AcceptedExecute(_, _, _) => {
            accepted_execute.fetch_add(1, Ordering::Relaxed);
        }
        ConfirmedTransaction::RejectedDeploy(_, _, _, _) => {
            rejected_deploy.fetch_add(1, Ordering::Relaxed);
        }
        ConfirmedTransaction::RejectedExecute(_, _, _, _) => {
            rejected_execute.fetch_add(1, Ordering::Relaxed);
        }
    });

    increment_gauge(blocks::ACCEPTED_DEPLOY, accepted_deploy.load(Ordering::Relaxed) as f64);
    increment_gauge(blocks::ACCEPTED_EXECUTE, accepted_execute.load(Ordering::Relaxed) as f64);
    increment_gauge(blocks::REJECTED_DEPLOY, rejected_deploy.load(Ordering::Relaxed) as f64);
    increment_gauge(blocks::REJECTED_EXECUTE, rejected_execute.load(Ordering::Relaxed) as f64);

    // Update aborted transactions and solutions.
    increment_gauge(blocks::ABORTED_TRANSACTIONS, block.aborted_transaction_ids().len() as f64);
    increment_gauge(blocks::ABORTED_SOLUTIONS, block.aborted_solution_ids().len() as f64);

    // Update the active consensus version.
    gauge(consensus::VERSION, N::CONSENSUS_VERSION(block.height()).map_or(0.0, |version| version as u16 as f64));
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum TrackedId<N: Network> {
    Solution(SolutionID<N>),
    Transaction(N::TransactionID),
}

pub struct TransmissionTracker<N: Network> {
    capacity: usize,
    sequence: u64,
    entries: HashMap<TrackedId<N>, (Instant, u64)>,
    expiry: BTreeMap<(Instant, u64), TrackedId<N>>,
}

const EXPIRY_BUDGET: usize = 32;
const MAX_AGE: std::time::Duration = std::time::Duration::from_secs(30 * 60);

impl<N: Network> TransmissionTracker<N> {
    pub fn new(capacity: usize) -> Self {
        Self { capacity, sequence: 0, entries: HashMap::new(), expiry: BTreeMap::new() }
    }

    fn remove(&mut self, id: TrackedId<N>) -> Option<Instant> {
        let key = self.entries.remove(&id)?;
        self.expiry.remove(&key);
        Some(key.0)
    }

    fn retire(&mut self, now: Instant) -> [u64; 2] {
        let mut counts = [0; 2];
        for _ in 0..EXPIRY_BUDGET {
            let Some((&(start, _), &id)) = self.expiry.first_key_value() else { break };
            let expired = now.duration_since(start) > MAX_AGE;
            if self.entries.len() <= self.capacity && !expired {
                break;
            }
            self.remove(id);
            // Only age evictions count as stale; capacity evictions drop the entry silently.
            if expired {
                counts[usize::from(matches!(id, TrackedId::Transaction(_)))] += 1;
            }
        }
        counts
    }

    fn finish(
        &mut self,
        ids: impl IntoIterator<Item = TrackedId<N>>,
        now: Instant,
    ) -> (Vec<(&'static str, f64)>, [u64; 2]) {
        let samples = ids
            .into_iter()
            .filter_map(|id| {
                let start = self.remove(id)?;
                let kind = match id {
                    TrackedId::Solution(_) => "solution",
                    TrackedId::Transaction(_) => "transaction",
                };
                Some((kind, now.duration_since(start).as_secs_f64()))
            })
            .collect::<Vec<_>>();
        (samples, self.retire(now))
    }

    fn untrack_solutions(&mut self) {
        self.entries.retain(|id, _| !matches!(id, TrackedId::Solution(_)));
        self.expiry.retain(|_, id| !matches!(id, TrackedId::Solution(_)));
    }

    fn insert(&mut self, id: TrackedId<N>, now: Instant) -> [u64; 2] {
        self.remove(id);
        let key = (now, self.sequence);
        self.sequence += 1;
        self.entries.insert(id, key);
        self.expiry.insert(key, id);
        self.retire(now)
    }
}

fn record_stale(counts: [u64; 2]) {
    for (name, count) in
        [consensus::STALE_UNCONFIRMED_SOLUTIONS, consensus::STALE_UNCONFIRMED_TRANSACTIONS].into_iter().zip(counts)
    {
        if count > 0 {
            metrics::counter!(name).increment(count);
        }
    }
}

pub fn track_transmission<N: Network>(tracker: &Mutex<TransmissionTracker<N>>, id: TrackedId<N>) {
    let counts = tracker.lock().insert(id, Instant::now());
    record_stale(counts);
}

/// Stops tracking transmissions that left the memory pool without reaching a block (e.g. evicted).
pub fn untrack_transmissions<N: Network>(
    tracker: &Mutex<TransmissionTracker<N>>,
    ids: impl IntoIterator<Item = TrackedId<N>>,
) {
    let mut tracker = tracker.lock();
    for id in ids {
        tracker.remove(id);
    }
}

/// Stops tracking every solution, e.g. when an epoch boundary clears all unconfirmed solutions.
pub fn untrack_solutions<N: Network>(tracker: &Mutex<TransmissionTracker<N>>) {
    tracker.lock().untrack_solutions();
}

pub fn add_transmission_latency_metric<N: Network>(tracker: &Mutex<TransmissionTracker<N>>, block: &Block<N>) {
    let solutions =
        block.solutions().solution_ids().chain(block.aborted_solution_ids()).copied().map(TrackedId::Solution);
    let transactions =
        block.transaction_ids().chain(block.aborted_transaction_ids()).copied().map(TrackedId::Transaction);
    let (samples, counts) = {
        let mut tracker = tracker.lock();
        let now = Instant::now();
        tracker.finish(solutions.chain(transactions), now)
    };
    for (kind, seconds) in samples {
        histogram_label(consensus::TRANSMISSION_LATENCY, "transmission_type", kind.to_owned(), seconds);
    }
    record_stale(counts);
}

pub fn gauge_label<V: Into<f64>>(name: &'static str, label_key: &'static str, label_value: String, value: V) {
    metrics::gauge!(name, label_key => label_value).set(value.into());
}

pub fn increment_counter_1(name: &'static str, key: &'static str, value: impl Into<metrics::SharedString>) {
    metrics::counter!(name, key => value).increment(1);
}

pub fn counter_1(name: &'static str, key: &'static str, value: impl Into<metrics::SharedString>, amount: u64) {
    metrics::counter!(name, key => value).increment(amount);
}

pub fn increment_counter_2(
    name: &'static str,
    key_a: &'static str,
    value_a: impl Into<metrics::SharedString>,
    key_b: &'static str,
    value_b: impl Into<metrics::SharedString>,
) {
    metrics::counter!(name, key_a => value_a, key_b => value_b).increment(1);
}

pub fn increment_counter_3(
    name: &'static str,
    key_a: &'static str,
    value_a: impl Into<metrics::SharedString>,
    key_b: &'static str,
    value_b: impl Into<metrics::SharedString>,
    key_c: &'static str,
    value_c: impl Into<metrics::SharedString>,
) {
    metrics::counter!(name, key_a => value_a, key_b => value_b, key_c => value_c).increment(1);
}

pub fn histogram_2<V: Into<f64>>(
    name: &'static str,
    key_a: &'static str,
    value_a: impl Into<metrics::SharedString>,
    key_b: &'static str,
    value_b: impl Into<metrics::SharedString>,
    value: V,
) {
    metrics::histogram!(name, key_a => value_a, key_b => value_b).record(value.into());
}

/// Returns the `kind` label value for the given transaction.
pub fn transaction_kind<N: Network>(transaction: &Transaction<N>) -> &'static str {
    match transaction {
        Transaction::Deploy(..) => "deploy",
        Transaction::Execute(..) => "execute",
        Transaction::Fee(..) => "fee",
    }
}

/// Times a full transaction check at one site; `finish` records its outcome and duration.
pub struct TransactionCheckTimer {
    site: &'static str,
    kind: &'static str,
    start: Instant,
}

impl TransactionCheckTimer {
    /// Starts timing a check of `transaction` at `site`.
    pub fn start<N: Network>(site: &'static str, transaction: &Transaction<N>) -> Self {
        Self { site, kind: transaction_kind(transaction), start: Instant::now() }
    }

    /// Records the check's outcome and the time since `start`.
    pub fn finish<T, E>(self, result: &Result<T, E>) {
        let Self { site, kind, start } = self;
        let outcome = if result.is_ok() { "ok" } else { "err" };
        increment_counter_3(verification::TRANSACTION_CHECKS, "site", site, "kind", kind, "outcome", outcome);
        histogram_2(verification::TRANSACTION_CHECK_SECS, "site", site, "kind", kind, start.elapsed().as_secs_f64());
    }
}

/// Records the outcome of a solution check at the given site.
pub fn record_solution_check(site: &'static str, is_ok: bool) {
    let outcome = if is_ok { "ok" } else { "err" };
    increment_counter_2(verification::SOLUTION_CHECKS, "site", site, "outcome", outcome);
}

/// Records the outcome of offering a transaction to the memory pool.
pub fn record_mempool_transaction(kind: &'static str, outcome: &'static str) {
    increment_counter_2(consensus::MEMPOOL_TRANSACTIONS, "kind", kind, "outcome", outcome);
}

// Include the generated build information
mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

/// Sets the build information metric with version details as labels.
/// The resulting metric will show as:
/// snarkos_build_info{version="4.2.1",git_commit="abc123",git_branch="main",features="cuda,metrics"} 1
pub fn set_build_info() {
    let version = env!("SNARKOS_VERSION");
    let git_commit = built_info::GIT_COMMIT_HASH.unwrap_or("unknown");
    let git_branch = built_info::GIT_HEAD_REF.unwrap_or("unknown");
    let features = built_info::FEATURES_LOWERCASE_STR.replace(' ', "");

    metrics::gauge!(build::BUILD_INFO, "version" => version.to_string()).set(1.0);
    metrics::gauge!(build::BUILD_INFO, "git_commit" => git_commit.to_string()).set(1.0);
    metrics::gauge!(build::BUILD_INFO, "git_branch" => git_branch.to_string()).set(1.0);
    metrics::gauge!(build::BUILD_INFO, "features" => features).set(1.0);
}

#[cfg(test)]
mod transmission_tracker {
    use super::*;
    use snarkvm::prelude::{Field, MainnetV0};
    use std::time::Duration;

    #[test]
    fn bounds_and_retires_each_admission_once() {
        let capacity = EXPIRY_BUDGET + 5;
        let mut tracker = TransmissionTracker::<MainnetV0>::new(capacity);
        let solution = |n| TrackedId::Solution(SolutionID::from(n));
        let transaction = |n| TrackedId::Transaction(Field::<MainnetV0>::from_u64(n).into());
        let start = Instant::now();
        for n in 0..capacity as u64 {
            tracker.insert(solution(n), start);
        }
        for n in 0..3 {
            assert_eq!(tracker.insert(transaction(n), start), [0, 0]);
        }
        let refreshed = start + Duration::from_millis(250);
        for _ in 0..3 {
            tracker.insert(transaction(0), refreshed);
        }
        assert_eq!(tracker.entries.len(), capacity);
        assert_eq!(tracker.expiry.len(), capacity);
        let now = refreshed + MAX_AGE + Duration::from_millis(500);
        let (samples, stale) =
            tracker.finish([solution(0), solution(3), solution(4), transaction(0), transaction(1)], now);
        assert_eq!(samples, [
            ("solution", 1800.75),
            ("solution", 1800.75),
            ("transaction", 1800.5),
            ("transaction", 1800.75)
        ]);
        assert_eq!(stale, [EXPIRY_BUDGET as u64, 0]);
        assert_eq!(tracker.retire(now), [0, 1]);
        assert_eq!(tracker.finish([transaction(2)], now), (vec![], [0, 0]));
    }

    #[test]
    fn untracked_transmissions_are_neither_sampled_nor_stale() {
        let tracker = Mutex::new(TransmissionTracker::<MainnetV0>::new(16));
        let solution = |n| TrackedId::Solution(SolutionID::from(n));
        let transaction = |n| TrackedId::Transaction(Field::<MainnetV0>::from_u64(n).into());
        for n in 0..3 {
            track_transmission(&tracker, solution(n));
            track_transmission(&tracker, transaction(n));
        }

        // An evicted transaction is dropped; untracking an unknown ID is a no-op.
        untrack_transmissions(&tracker, [transaction(1), transaction(7)]);
        // An epoch boundary drops every solution.
        untrack_solutions(&tracker);

        let mut tracker = tracker.lock();
        assert_eq!(tracker.entries.len(), 2);
        assert_eq!(tracker.expiry.len(), 2);
        let now = Instant::now() + MAX_AGE + Duration::from_secs(1);
        let (samples, stale) = tracker.finish([solution(0), transaction(1)], now);
        assert!(samples.is_empty());
        // Only the two transactions that were still tracked age out as stale.
        assert_eq!(stale, [0, 2]);
        assert!(tracker.entries.is_empty() && tracker.expiry.is_empty());
    }
}
