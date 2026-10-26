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

// Re-export the snarkVM metrics.
pub use snarkvm::metrics::*;

#[cfg(not(feature = "serial"))]
use rayon::prelude::*;

#[cfg(feature = "locktick")]
use locktick::parking_lot::Mutex;
#[cfg(not(feature = "locktick"))]
use parking_lot::Mutex;
use snarkvm::{
    ledger::narwhal::TransmissionID,
    prelude::{Block, Network, cfg_iter},
};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc,
        OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
};
use time::OffsetDateTime;
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

    let builder = metrics_exporter_prometheus::PrometheusBuilder::new()
        .set_buckets_for_metric(metrics_exporter_prometheus::Matcher::Suffix("_secs".to_string()), SECS_BUCKETS)
        .expect("can't set the seconds-scale histogram buckets")
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Full(consensus::TRANSMISSION_LATENCY.to_string()),
            SECS_BUCKETS,
        )
        .expect("can't set the transmission latency histogram buckets")
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
            tracing::error!("The metrics exporter has failed: {error}");
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

pub fn add_transmission_latency_metric<N: Network>(
    transmissions_tracker: &Arc<Mutex<HashMap<TransmissionID<N>, i64>>>,
    block: &Block<N>,
) {
    // The age at which we consider a transmission "stale".
    const AGE_THRESHOLD_SECONDS: i32 = 30 * 60; // 30 minutes

    // Retrieve all solution IDs (including aborted).
    let solution_ids: std::collections::HashSet<_> =
        block.solutions().solution_ids().chain(block.aborted_solution_ids()).collect();

    // Retrieve all transaction IDs (including aborted).
    let transaction_ids: std::collections::HashSet<_> =
        block.transaction_ids().chain(block.aborted_transaction_ids()).collect();

    let mut transmissions_tracker = transmissions_tracker.lock();
    let ts_now = OffsetDateTime::now_utc().unix_timestamp();

    // Determine which keys to remove.
    // Note: this is intentionally serial, as it is done while holding the lock above. A parallel
    // iterator that blocks on a lock can occupy every worker of the rayon thread pool, which in
    // turn prevents the lock holder's own parallel iterator from ever being scheduled.
    let keys_to_remove = transmissions_tracker
        .iter()
        .flat_map(|(transmission_id, timestamp)| {
            let elapsed_time = std::time::Duration::from_secs((ts_now - *timestamp) as u64);
            let transmission_type = match transmission_id {
                TransmissionID::Solution(solution_id, _) if solution_ids.contains(solution_id) => Some("solution"),
                TransmissionID::Transaction(transaction_id, _) if transaction_ids.contains(transaction_id) => {
                    Some("transaction")
                }
                // Either a ratification, or the transmission was not included in the block
                _ => None,
            };

            if let Some(transmission_type_string) = transmission_type {
                histogram_label(
                    consensus::TRANSMISSION_LATENCY,
                    "transmission_type",
                    transmission_type_string.to_owned(),
                    elapsed_time.as_secs_f64(),
                );
                Some(*transmission_id)
            } else if elapsed_time.as_secs() > AGE_THRESHOLD_SECONDS as u64 {
                // A transmission included in this block is confirmed, so record its latency even if it
                // predates the staleness threshold — confirmation takes precedence over staleness.
                match transmission_id {
                    TransmissionID::Solution(..) => increment_counter(consensus::STALE_UNCONFIRMED_SOLUTIONS),
                    TransmissionID::Transaction(..) => increment_counter(consensus::STALE_UNCONFIRMED_TRANSACTIONS),
                    TransmissionID::Ratification => {}
                }
                Some(*transmission_id)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    // Remove keys of stale or seen transmissions.
    for key in keys_to_remove {
        transmissions_tracker.remove(&key);
    }
}

pub fn gauge_label<V: Into<f64>>(name: &'static str, label_key: &'static str, label_value: String, value: V) {
    ::metrics::gauge!(name, label_key => label_value).set(value.into());
}

pub fn increment_counter_2(
    name: &'static str,
    key_a: &'static str,
    value_a: impl Into<::metrics::SharedString>,
    key_b: &'static str,
    value_b: impl Into<::metrics::SharedString>,
) {
    ::metrics::counter!(name, key_a => value_a, key_b => value_b).increment(1);
}

pub fn increment_counter_3(
    name: &'static str,
    key_a: &'static str,
    value_a: impl Into<::metrics::SharedString>,
    key_b: &'static str,
    value_b: impl Into<::metrics::SharedString>,
    key_c: &'static str,
    value_c: impl Into<::metrics::SharedString>,
) {
    ::metrics::counter!(name, key_a => value_a, key_b => value_b, key_c => value_c).increment(1);
}

pub fn histogram_2<V: Into<f64>>(
    name: &'static str,
    key_a: &'static str,
    value_a: impl Into<::metrics::SharedString>,
    key_b: &'static str,
    value_b: impl Into<::metrics::SharedString>,
    value: V,
) {
    ::metrics::histogram!(name, key_a => value_a, key_b => value_b).record(value.into());
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

    ::metrics::gauge!(build::BUILD_INFO, "version" => version.to_string()).set(1.0);
    ::metrics::gauge!(build::BUILD_INFO, "git_commit" => git_commit.to_string()).set(1.0);
    ::metrics::gauge!(build::BUILD_INFO, "git_branch" => git_branch.to_string()).set(1.0);
    ::metrics::gauge!(build::BUILD_INFO, "features" => features).set(1.0);
}
