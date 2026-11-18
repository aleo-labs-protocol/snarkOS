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

use snarkos_node_bft_ledger_service::CommitteeNotFound;

#[cfg(feature = "locktick")]
use locktick::parking_lot::Mutex;
#[cfg(not(feature = "locktick"))]
use parking_lot::Mutex;
use std::{
    fmt,
    time::{Duration, Instant},
};

/// The minimum time between two summaries of the peer messages skipped while the ledger is behind.
const SKIP_SUMMARY_INTERVAL: Duration = Duration::from_secs(30);

/// Returns `true` if `error` is the failure every live batch proposal and batch certificate runs
/// into while this node is catching up: the ledger has not reached the round whose committee
/// validates the message.
///
/// A missing committee on a synced node, or for a round the ledger has already reached, is an
/// anomaly and returns `false`.
pub fn is_awaiting_ledger_sync(error: &anyhow::Error, is_synced: bool, latest_ledger_round: u64) -> bool {
    !is_synced && error.downcast_ref::<CommitteeNotFound>().is_some_and(|error| error.round > latest_ledger_round)
}

/// A kind of peer message that cannot be validated while the ledger is behind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SkippedMessage {
    BatchProposal,
    BatchCertificate,
}

/// The peer messages skipped over one summary interval.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SkipSummary {
    pub proposals: u64,
    pub certificates: u64,
    pub elapsed: Duration,
}

impl fmt::Display for SkipSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { proposals, certificates, elapsed } = self;
        write!(
            f,
            "Skipped {proposals} batch proposals and {certificates} batch certificates from peers in the last {}s",
            elapsed.as_secs()
        )
    }
}

/// The counts accumulated since the first skipped message that no summary has covered yet.
struct SkipWindow {
    start: Instant,
    proposals: u64,
    certificates: u64,
}

/// Counts the peer messages skipped while the ledger is behind, so that they are reported as a
/// periodic summary instead of one log line each.
#[derive(Default)]
pub struct SkippedWhileSyncing {
    window: Mutex<Option<SkipWindow>>,
}

impl SkippedWhileSyncing {
    /// Counts a skipped message observed at `now`.
    ///
    /// Returns the summary to log once [`SKIP_SUMMARY_INTERVAL`] has passed since the first
    /// message it covers. Messages are only counted here, so a summary is not emitted until the
    /// next skipped message arrives, and the counts of a final, shorter window are never reported.
    pub fn record(&self, message: SkippedMessage, now: Instant) -> Option<SkipSummary> {
        let mut window = self.window.lock();
        let current = window.get_or_insert(SkipWindow { start: now, proposals: 0, certificates: 0 });
        match message {
            SkippedMessage::BatchProposal => current.proposals += 1,
            SkippedMessage::BatchCertificate => current.certificates += 1,
        }

        let elapsed = now.saturating_duration_since(current.start);
        if elapsed < SKIP_SUMMARY_INTERVAL {
            return None;
        }
        window.take().map(|SkipWindow { proposals, certificates, .. }| SkipSummary { proposals, certificates, elapsed })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::anyhow;

    #[test]
    fn missing_committee_ahead_of_the_ledger_is_expected_while_syncing() {
        let error = anyhow::Error::from(CommitteeNotFound { round: 101 });
        assert!(is_awaiting_ledger_sync(&error, false, 100));
    }

    #[test]
    fn missing_committee_is_recognised_through_added_context() {
        let error = anyhow::Error::from(CommitteeNotFound { round: 101 })
            .context("Failed to fetch missing transmissions and previous certificates")
            .context("Cannot sign a batch at round 203 from '127.0.0.1:5000'");
        assert!(is_awaiting_ledger_sync(&error, false, 100));
    }

    #[test]
    fn missing_committee_on_a_synced_node_is_an_anomaly() {
        let error = anyhow::Error::from(CommitteeNotFound { round: 101 });
        assert!(!is_awaiting_ledger_sync(&error, true, 100));
    }

    #[test]
    fn missing_committee_for_a_round_the_ledger_reached_is_an_anomaly() {
        let error = anyhow::Error::from(CommitteeNotFound { round: 100 });
        assert!(!is_awaiting_ledger_sync(&error, false, 100));
        assert!(!is_awaiting_ledger_sync(&error, false, 250));
    }

    #[test]
    fn other_errors_are_anomalies_while_syncing() {
        // The classification goes by error type: the same text in an untyped error does not match.
        let error = anyhow!("No committee found for round 101 in the ledger");
        assert!(!is_awaiting_ledger_sync(&error, false, 100));
        let error = anyhow!("Malicious peer - proposed batch from a different validator");
        assert!(!is_awaiting_ledger_sync(&error, false, 100));
    }

    #[test]
    fn summary_is_withheld_until_the_interval_has_passed() {
        let skipped = SkippedWhileSyncing::default();
        let start = Instant::now();

        assert_eq!(skipped.record(SkippedMessage::BatchProposal, start), None);
        assert_eq!(skipped.record(SkippedMessage::BatchCertificate, start + Duration::from_secs(1)), None);
        let just_before = start + SKIP_SUMMARY_INTERVAL - Duration::from_millis(1);
        assert_eq!(skipped.record(SkippedMessage::BatchCertificate, just_before), None);

        let summary = skipped.record(SkippedMessage::BatchProposal, start + SKIP_SUMMARY_INTERVAL).unwrap();
        assert_eq!(summary, SkipSummary { proposals: 2, certificates: 2, elapsed: SKIP_SUMMARY_INTERVAL });
    }

    #[test]
    fn a_new_window_starts_at_the_first_message_after_a_summary() {
        let skipped = SkippedWhileSyncing::default();
        let start = Instant::now();

        assert_eq!(skipped.record(SkippedMessage::BatchProposal, start), None);
        assert!(skipped.record(SkippedMessage::BatchProposal, start + SKIP_SUMMARY_INTERVAL).is_some());

        // The next message arrives long after the summary; the quiet time is not part of its window.
        let resumed = start + SKIP_SUMMARY_INTERVAL * 10;
        assert_eq!(skipped.record(SkippedMessage::BatchCertificate, resumed), None);
        let summary = skipped.record(SkippedMessage::BatchCertificate, resumed + SKIP_SUMMARY_INTERVAL).unwrap();
        assert_eq!(summary, SkipSummary { proposals: 0, certificates: 2, elapsed: SKIP_SUMMARY_INTERVAL });
    }

    #[test]
    fn summary_is_formatted_for_the_log() {
        let summary = SkipSummary { proposals: 337, certificates: 338, elapsed: Duration::from_secs(30) };
        assert_eq!(
            summary.to_string(),
            "Skipped 337 batch proposals and 338 batch certificates from peers in the last 30s"
        );
    }
}
