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

use crate::{CREATE_BATCH_INTERVAL, MAX_BATCH_DELAY, MIN_BATCH_DELAY};

use anyhow::Result;
use snarkos_utilities::time::{Instant, sleep, sleep_until};
use snarkvm::{prelude::Network, utilities::flatten_error};
use std::{marker::PhantomData, sync::Arc};
use tokio::sync::watch;
use tracing::{debug, warn};

/// Abstracts over batch-proposal operations, allowing the proposal loop to be tested without a
/// real primary.
#[async_trait::async_trait]
pub(super) trait BatchPropose: Send + Sync {
    /// Returns the current consensus round.
    fn current_round(&self) -> u64;

    /// Attempts to propose a batch for the current round, which can be a later round than the
    /// caller last saw.
    ///
    /// Returns `Ok(Some(round))` when a batch was successfully proposed for `round`, `Ok(None)`
    /// to retry, and `Err` on an unexpected error.
    async fn propose_batch(&self) -> Result<Option<u64>>;
}

/// Manages batch proposal readiness and drives the batch proposal loop.
///
/// Holds the readiness state and the logic for the proposal task. The actual task is started by
/// calling [`Self::run`] inside a spawned future (see [`Primary::start_handlers`]).
pub struct ProposalTask<N: Network> {
    inner: Arc<ProposalTaskInner>,
    _phantom: PhantomData<N>,
}

/// Manual `Clone` impl so that `N: Clone` is not required.
impl<N: Network> Clone for ProposalTask<N> {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner), _phantom: PhantomData }
    }
}

/// The inner state of a [`ProposalTask`], shared via `Arc`.
struct ProposalTaskInner {
    /// The highest round that [`ProposalTask::signal`] named as ready for a batch proposal.
    ///
    /// The task is ready when this is above the round of its last proposal, so a proposal uses
    /// up the signals of its round and of the rounds before it, and no later one. Hazard: a flag
    /// that the task clears when it proposes cannot tell the two apart. It loses the signal of a
    /// round that starts while the task proposes, or keeps one for a round that has its batch.
    ready_round: watch::Sender<u64>,
    /// Whether the task proposes its first batch without a signal, so that the primary can
    /// propose in the round that it starts in.
    is_ready_at_start: bool,
}

impl<N: Network> Default for ProposalTask<N> {
    fn default() -> Self {
        Self::new(true)
    }
}

impl<N: Network> ProposalTask<N> {
    fn new(is_ready_at_start: bool) -> Self {
        let (ready_round, _) = watch::channel(0);
        Self { inner: Arc::new(ProposalTaskInner { ready_round, is_ready_at_start }), _phantom: PhantomData }
    }

    /// Signals that the primary is ready to propose a new batch for `round`, the round that it
    /// is in.
    ///
    /// Should be called from [`Primary::try_increment_to_the_next_round`] whenever the primary
    /// successfully advances to a new round.
    pub fn signal(&self, round: u64) {
        self.inner.ready_round.send_if_modified(|ready_round| {
            let is_higher = round > *ready_round;
            if is_higher {
                *ready_round = round;
            }
            is_higher
        });
    }

    /// Runs the batch proposal loop. This is intended to be spawned as a long-running task.
    ///
    /// Each iteration waits for a round to be ready, proposes a batch, and waits for its
    /// signatures. It ends when the round of the batch ends. If the call that sends the batch
    /// again proposes a batch for a later round, the iteration goes on with that round.
    /// The three stages are implemented as separate methods; see their doc-comments for details.
    pub(super) async fn run<P: BatchPropose + 'static>(self, primary: P) {
        let mut ready_rx = self.inner.ready_round.subscribe();
        let mut earliest_proposal = Instant::now() + MIN_BATCH_DELAY;
        // The lowest signalled round that makes the task ready. It is the one after the round of
        // the last proposal, or any round before the first proposal. A signal that the task has
        // not used thus stays good for later rounds.
        let mut min_ready_round = if self.inner.is_ready_at_start { 0 } else { 1 };

        loop {
            let round = primary.current_round();
            let round_start = Instant::now();

            if !Self::wait_until_proposal_ready(
                &primary,
                &mut ready_rx,
                round,
                min_ready_round,
                round_start,
                earliest_proposal,
            )
            .await
            {
                continue; // round changed; restart
            }

            let Some(mut proposed_round) = Self::propose(&primary, round).await else {
                continue; // round changed; restart
            };

            loop {
                earliest_proposal = Instant::now() + MIN_BATCH_DELAY;
                min_ready_round = proposed_round.saturating_add(1);

                match Self::wait_for_signatures(&primary, &mut ready_rx, proposed_round).await {
                    Some(new_round) => proposed_round = new_round, // proposed for a later round
                    None => break,
                }
            }
        }
    }

    /// Stage 1: Wait until conditions are met to propose a batch.
    ///
    /// Blocks until the minimum spacing from the last successful proposal has elapsed, and either
    /// `signal()` names `min_ready_round` or a later round (leader cert arrived), or
    /// MAX_BATCH_DELAY expires since `round_start`.
    ///
    /// Returns `true` if ready to propose, `false` if the round changed (caller should restart).
    async fn wait_until_proposal_ready<P: BatchPropose>(
        primary: &P,
        ready_rx: &mut watch::Receiver<u64>,
        round: u64,
        min_ready_round: u64,
        round_start: Instant,
        earliest_proposal: Instant,
    ) -> bool {
        loop {
            if primary.current_round() != round {
                return false;
            }

            // Enforce the minimum inter-proposal delay.
            // This is a no-op once the deadline has already passed.
            sleep_until(earliest_proposal).await;

            // Wait for a readiness signal, the MAX_BATCH_DELAY deadline, or a short heartbeat
            // that lets the round-change check at the top of the loop fire regularly.
            tokio::select! {
                _ = sleep_until(round_start + MAX_BATCH_DELAY) => {
                    debug!("Did not receive leader certificate within MAX_BATCH_DELAY");
                    return true;
                },
                _ = Self::wait_until_ready(ready_rx, min_ready_round) => {
                    return true;
                },
                _ = sleep(CREATE_BATCH_INTERVAL) => {
                    debug!(dim = true, "Skipping batch proposal for round {round} (not ready yet)");
                }
            };
        }
    }

    /// Stage 2: Propose a batch.
    ///
    /// Calls `propose_batch()` until it submits a batch to the network, retrying from
    /// CREATE_BATCH_INTERVAL and backing off to at most MAX_BATCH_DELAY.
    ///
    /// Returns the round of the batch if it was submitted, which can be a later round than
    /// `round`. Returns `None` if the round changed before that (caller should restart).
    async fn propose<P: BatchPropose>(primary: &P, round: u64) -> Option<u64> {
        let mut attempt = 1u32;
        let mut backoff = CREATE_BATCH_INTERVAL;

        loop {
            // The round advances both when this primary certifies a batch and when block sync
            // applies a block (`Storage::sync_round_with_block`), so this doubles as the local
            // "we were behind and have since caught up" check: a syncing node restarts here
            // rather than retrying against a round it has already moved past.
            if primary.current_round() != round {
                return None;
            }

            if attempt > 1 {
                // The round only advances on an external event (a certificate or an applied
                // block), so a round that is not progressing would otherwise hold the 250ms
                // retry cadence indefinitely, each attempt taking the proposal lock and reading
                // the committee lookback from the ledger.
                sleep(backoff).await;
                backoff = (backoff.saturating_mul(2)).min(MAX_BATCH_DELAY);
                if primary.current_round() != round {
                    return None;
                }
            }

            // Note: Do NOT spawn a task around this function call.  Proposing a batch is a
            // critical path, and only one batch needs to be proposed at a time.
            debug!("Trying batch proposal for round {round} (attempt #{attempt})");
            match primary.propose_batch().await {
                Ok(Some(proposed_round)) => return Some(proposed_round), // batch submitted; proceed to Stage 3
                Ok(None) => {}                                           // not ready yet; retry
                Err(err) => {
                    warn!("{}", flatten_error(err.context("Cannot propose a batch")));
                }
            }

            attempt += 1;
        }
    }

    /// Stage 3: Wait for the proposed batch of `round` to collect enough signatures.
    ///
    /// Periodically rebroadcasts the batch to non-signers (via `propose_batch`) at most once per
    /// MAX_BATCH_DELAY until the round advances. Returns `None` when the round changes.
    ///
    /// The round can change just before a rebroadcast, and `propose_batch` then proposes a
    /// batch for the new round. Returns that round in this case.
    async fn wait_for_signatures<P: BatchPropose>(
        primary: &P,
        ready_rx: &mut watch::Receiver<u64>,
        round: u64,
    ) -> Option<u64> {
        loop {
            if primary.current_round() != round {
                return None;
            }

            // Wait for the rebroadcast interval or an explicit round-advance signal,
            // whichever comes first.
            tokio::select! {
                _ = Self::wait_until_ready(ready_rx, round.saturating_add(1)) => return None, // round advanced
                _ = sleep(MAX_BATCH_DELAY) => {}
            }

            if primary.current_round() != round {
                return None;
            }

            // Rebroadcast to non-signers (`propose_batch` handles this internally).
            match primary.propose_batch().await {
                Ok(Some(new_round)) if new_round != round => return Some(new_round),
                Ok(_) => {}
                Err(err) => {
                    warn!("{}", flatten_error(err.context("Cannot rebroadcast a batch")));
                }
            }
        }
    }

    /// Waits until `round` or a later round is signalled as ready. Returns immediately if one
    /// already is.
    async fn wait_until_ready(receiver: &mut watch::Receiver<u64>, round: u64) {
        loop {
            if *receiver.borrow_and_update() >= round {
                return;
            }

            // Block until the signalled round changed, or the channel is closed.
            if receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use snarkvm::prelude::MainnetV0;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU32, Ordering},
        },
        time::Duration,
    };
    use tokio::sync::{Notify, mpsc};

    /// A minimal [`BatchPropose`] implementation for testing.
    ///
    /// Always reports round 1. Records how many times [`propose_batch`] is called and
    /// fires a [`Notify`] on each call.
    struct DummyProposer {
        propose_count: Arc<AtomicU32>,
        proposed_notify: Arc<Notify>,
    }

    #[async_trait::async_trait]
    impl BatchPropose for DummyProposer {
        fn current_round(&self) -> u64 {
            1
        }

        async fn propose_batch(&self) -> Result<Option<u64>> {
            self.propose_count.fetch_add(1, Ordering::SeqCst);
            self.proposed_notify.notify_one();

            Ok(Some(1))
        }
    }

    /// A [`BatchPropose`] implementation that returns round 1 on the very first call to
    /// `current_round`, then round 2 for all subsequent calls.
    ///
    /// This simulates the round advancing between the outer-loop capture and the inner-loop
    /// condition check, without any real-time waiting or time mocking.
    struct RoundAdvancingProposer {
        current_round_calls: Arc<AtomicU32>,
        propose_count: Arc<AtomicU32>,
    }

    #[async_trait::async_trait]
    impl BatchPropose for RoundAdvancingProposer {
        fn current_round(&self) -> u64 {
            let n = self.current_round_calls.fetch_add(1, Ordering::SeqCst);
            if n == 0 { 1 } else { 2 }
        }

        async fn propose_batch(&self) -> Result<Option<u64>> {
            self.propose_count.fetch_add(1, Ordering::SeqCst);
            Ok(Some(2))
        }
    }

    /// A [`BatchPropose`] implementation that returns `Ok(None)` a fixed number of times before
    /// succeeding.
    struct RetryProposer {
        retries_before_success: u32,
        propose_count: Arc<AtomicU32>,
        proposed_notify: Arc<Notify>,
    }

    #[async_trait::async_trait]
    impl BatchPropose for RetryProposer {
        fn current_round(&self) -> u64 {
            1
        }

        async fn propose_batch(&self) -> Result<Option<u64>> {
            let count = self.propose_count.fetch_add(1, Ordering::SeqCst) + 1;
            if count <= self.retries_before_success {
                Ok(None)
            } else {
                self.proposed_notify.notify_one();
                Ok(Some(1))
            }
        }
    }

    /// Signals the proposal task and verifies that `propose_batch` is called on the dummy.
    #[cfg_attr(not(shuttle), tokio::test)]
    async fn test_proposal_task_calls_propose_batch_on_signal() {
        // Start with the task not ready so the initial signal is the trigger.
        let task = ProposalTask::<MainnetV0>::new(false);

        let proposed_notify = Arc::new(Notify::new());
        let propose_count = Arc::new(AtomicU32::new(0));

        let proposer = DummyProposer { propose_count: propose_count.clone(), proposed_notify: proposed_notify.clone() };

        let task_for_spawn = task.clone();
        tokio::spawn(task_for_spawn.run(proposer));

        // Before signalling, propose_batch should not have been called.
        sleep(Duration::from_millis(50)).await;
        assert_eq!(propose_count.load(Ordering::SeqCst), 0, "propose_batch called before signal");

        // Signal readiness — the proposal loop should wake up and call propose_batch.
        task.signal(1);

        snarkos_utilities::time::timeout(std::time::Duration::from_secs(5), proposed_notify.notified())
            .await
            .expect("propose_batch was not called within 5 seconds after signal");

        assert!(propose_count.load(Ordering::SeqCst) >= 1, "propose_batch was not called");
    }

    /// When the round advances between iterations, `propose_batch` is not called for the old round.
    ///
    /// `RoundAdvancingProposer` returns round 1 on the first `current_round()` call (outer-loop
    /// capture) and round 2 on every subsequent call. The inner-loop condition therefore fails
    /// immediately — no time mocking needed.
    #[cfg_attr(not(shuttle), tokio::test)]
    async fn test_proposal_task_exits_on_round_advancement() {
        let propose_count = Arc::new(AtomicU32::new(0));
        let proposer = RoundAdvancingProposer {
            current_round_calls: Arc::new(AtomicU32::new(0)),
            propose_count: propose_count.clone(),
        };

        // Start not-ready so the task parks in round 2's inner loop without proposing round 1.
        let task = ProposalTask::<MainnetV0>::new(false);

        tokio::spawn(task.run(proposer));

        // Yield once: the task runs through round 1 (inner loop exits immediately because
        // current_round() already returns 2) and then parks in round 2's inner loop.
        tokio::task::yield_now().await;

        assert_eq!(propose_count.load(Ordering::SeqCst), 0, "propose_batch called despite round advancement");
    }

    /// Tests the following scenario
    ///
    ///   1. A batch was already certified for the current round, so the task needs a signal.
    ///   2. `signal()` is **never** called externally — the BFT cannot advance the round until
    ///      `propose_batch()` is called (which internally checks the leader-certificate timer).
    #[cfg_attr(not(shuttle), test_log::test(tokio::test))]
    async fn test_proposal_task_advances_without_leader_cert() {
        // Start NOT ready: simulates a batch that was already certified for the round but the
        // round has not yet advanced (the even-round leader cert was missing — e.g. the elected
        // leader was one of the freshly-reset minority validators).
        let task = ProposalTask::<MainnetV0>::new(false);

        let proposed_notify = Arc::new(Notify::new());
        let propose_count = Arc::new(AtomicU32::new(0));

        // A proposer that stays on round 1 and returns `Ok(Some(1))` on every call to
        // propose_batch(), simulating try_advance_to_next_round finding the leader-certificate
        // timer expired and advancing the round without an external signal().
        struct NoSignalProposer {
            propose_count: Arc<AtomicU32>,
            proposed_notify: Arc<Notify>,
        }

        #[async_trait::async_trait]
        impl BatchPropose for NoSignalProposer {
            fn current_round(&self) -> u64 {
                1
            }

            async fn propose_batch(&self) -> Result<Option<u64>> {
                self.propose_count.fetch_add(1, Ordering::SeqCst);
                self.proposed_notify.notify_one();
                Ok(Some(1))
            }
        }

        let proposer =
            NoSignalProposer { propose_count: propose_count.clone(), proposed_notify: proposed_notify.clone() };

        // signal() is intentionally never called — the task must retry on its own.
        tokio::spawn(task.run(proposer));

        // Allow enough time for MAX_BATCH_DELAY (2.5 s) to elapse plus the CREATE_BATCH_INTERVAL
        // (250 ms) retry window. Use 10 s to give generous headroom on slow CI machines.
        snarkos_utilities::time::timeout(std::time::Duration::from_secs(10), proposed_notify.notified())
            .await
            .expect("propose_batch was not called");

        assert!(propose_count.load(Ordering::SeqCst) >= 1, "propose_batch should have been called at least once");
    }

    /// After the leader-certificate timer fires (MAX_BATCH_DELAY elapses without an explicit
    /// `signal()`), the task should still retry `propose_batch` when it returns `Ok(None)` and
    /// eventually succeed once it returns the round.
    ///
    /// This models the real primary: when a round is already certified but the round has not yet
    /// advanced (e.g. the elected leader was a freshly-reset minority validator), `propose_batch`
    /// returns `Ok(None)` until `try_advance_to_next_round` can make progress.
    #[cfg_attr(not(shuttle), test_log::test(tokio::test))]
    async fn test_proposal_task_retries_after_leader_timeout() {
        const RETRIES: u32 = 2;

        // Start NOT ready — no external signal will be sent. The task must wait for
        // MAX_BATCH_DELAY to fire, then retry until propose_batch succeeds.
        let task = ProposalTask::<MainnetV0>::new(false);

        let proposed_notify = Arc::new(Notify::new());
        let propose_count = Arc::new(AtomicU32::new(0));
        let proposer = RetryProposer {
            retries_before_success: RETRIES,
            propose_count: propose_count.clone(),
            proposed_notify: proposed_notify.clone(),
        };

        // signal() is intentionally never called — the leader timeout arm must trigger.
        tokio::spawn(task.run(proposer));

        // Allow enough time for MAX_BATCH_DELAY (2.5 s) plus RETRIES × CREATE_BATCH_INTERVAL (250 ms each).
        // Use 10 s to give generous headroom on slow CI machines.
        snarkos_utilities::time::timeout(std::time::Duration::from_secs(10), proposed_notify.notified())
            .await
            .expect("propose_batch did not succeed within 10 seconds after leader timeout");

        // Stage 3 may make additional rebroadcast calls after success, so use >.
        assert!(propose_count.load(Ordering::SeqCst) > RETRIES, "expected at least {} total attempts", RETRIES + 1);
    }

    /// When `propose_batch` returns `Ok(None)`, the task retries within the same round until
    /// it succeeds.
    #[cfg_attr(not(shuttle), tokio::test)]
    async fn test_proposal_task_retries_on_none() {
        const RETRIES: u32 = 2;

        // Default starts ready, so no signal needed.
        let task = ProposalTask::<MainnetV0>::default();

        let proposed_notify = Arc::new(Notify::new());
        let propose_count = Arc::new(AtomicU32::new(0));
        let proposer = RetryProposer {
            retries_before_success: RETRIES,
            propose_count: propose_count.clone(),
            proposed_notify: proposed_notify.clone(),
        };

        tokio::spawn(task.run(proposer));

        // The task internally waits MIN_BATCH_DELAY before the first attempt; allow up to 10s.
        snarkos_utilities::time::timeout(std::time::Duration::from_secs(10), proposed_notify.notified())
            .await
            .expect("propose_batch did not succeed within 10 seconds");

        // Stage 3 may make additional rebroadcast calls after success, so use >.
        assert!(propose_count.load(Ordering::SeqCst) > RETRIES, "expected at least {} total attempts", RETRIES + 1);
    }

    /// A round that ends while `propose_batch` runs signals the task during that call. The task
    /// keeps the signal: it proposes its next batch `MIN_BATCH_DELAY` after its last proposal,
    /// also if the primary is in a later round than the signal names by then. That proposal
    /// uses up the signal, so the round after it, which has no signal, waits for
    /// `MAX_BATCH_DELAY`.
    #[cfg_attr(not(shuttle), tokio::test)]
    async fn test_proposal_task_keeps_signal_from_during_proposal() {
        /// A proposer whose round ends while it proposes for it. The first proposal signals
        /// round 2 and ends in round 3, and the second one ends in round 4 with no signal.
        struct RoundEndingProposer {
            task: ProposalTask<MainnetV0>,
            round: AtomicU32,
            proposal_times: mpsc::UnboundedSender<Instant>,
        }

        #[async_trait::async_trait]
        impl BatchPropose for RoundEndingProposer {
            fn current_round(&self) -> u64 {
                self.round.load(Ordering::SeqCst).into()
            }

            async fn propose_batch(&self) -> Result<Option<u64>> {
                let round = self.round.load(Ordering::SeqCst);
                match round {
                    1 => {
                        self.task.signal(2);
                        self.round.store(3, Ordering::SeqCst);
                    }
                    3 => self.round.store(4, Ordering::SeqCst),
                    _ => {}
                }
                self.proposal_times.send(Instant::now()).ok();
                Ok(Some(round.into()))
            }
        }

        let task = ProposalTask::<MainnetV0>::default();
        let (proposal_times, mut proposal_times_receiver) = mpsc::unbounded_channel();
        tokio::spawn(task.clone().run(RoundEndingProposer { task, round: AtomicU32::new(1), proposal_times }));

        let first_proposal = proposal_times_receiver.recv().await.unwrap();
        let second_proposal = proposal_times_receiver.recv().await.unwrap();
        let delay = second_proposal - first_proposal;
        assert!(delay >= MIN_BATCH_DELAY, "the second proposal came after {delay:?}");

        // The clock of a shuttle test is exact. A real timer can end late by any amount, so
        // only that build can tell the two delays apart.
        #[cfg(shuttle)]
        {
            assert_eq!(delay, MIN_BATCH_DELAY);
            let third_proposal = proposal_times_receiver.recv().await.unwrap();
            assert_eq!(third_proposal - second_proposal, MAX_BATCH_DELAY);
        }
    }

    /// The round ends between the round check of the task and `propose_batch`, which thus
    /// proposes for the next round. The task counts the batch for that round: it does not
    /// propose for it again, and it sends the batch again `MAX_BATCH_DELAY` later.
    #[cfg_attr(not(shuttle), tokio::test)]
    async fn test_proposal_task_follows_round_of_proposal() {
        /// A proposer whose round 1 ends just before its first proposal, which is thus for
        /// round 2. Its later calls send that batch again.
        struct LateRoundProposer {
            task: ProposalTask<MainnetV0>,
            round: AtomicU32,
            call_times: mpsc::UnboundedSender<Instant>,
        }

        #[async_trait::async_trait]
        impl BatchPropose for LateRoundProposer {
            fn current_round(&self) -> u64 {
                self.round.load(Ordering::SeqCst).into()
            }

            async fn propose_batch(&self) -> Result<Option<u64>> {
                self.call_times.send(Instant::now()).ok();
                if self.round.swap(2, Ordering::SeqCst) == 1 {
                    self.task.signal(2);
                    return Ok(Some(2));
                }
                Ok(None)
            }
        }

        let task = ProposalTask::<MainnetV0>::default();
        let (call_times, mut call_times_receiver) = mpsc::unbounded_channel();
        tokio::spawn(task.clone().run(LateRoundProposer { task, round: AtomicU32::new(1), call_times }));

        let proposal = call_times_receiver.recv().await.unwrap();
        let rebroadcast = call_times_receiver.recv().await.unwrap();
        let delay = rebroadcast - proposal;
        assert!(delay >= MAX_BATCH_DELAY, "the task called `propose_batch` again after {delay:?}");
    }

    /// The async tests of this module under the `shuttle` scheduler; see `snarkos_utilities::locks`.
    /// Run them with `cargo shuttle-test -p snarkos-node-bft --lib shuttle_tests`.
    ///
    /// Each one runs in many orders of the steps of its tasks; see
    /// `snarkos_utilities::shuttle_test::check_async`.
    #[cfg(shuttle)]
    mod shuttle_tests {
        use super::*;
        use snarkos_utilities::shuttle_test::check_async;

        /// The number of task orders that each test runs with each scheduler.
        const ITERATIONS: usize = 1000;

        #[test]
        fn test_proposal_task_calls_propose_batch_on_signal() {
            check_async(ITERATIONS, super::test_proposal_task_calls_propose_batch_on_signal);
        }

        #[test]
        fn test_proposal_task_exits_on_round_advancement() {
            check_async(ITERATIONS, super::test_proposal_task_exits_on_round_advancement);
        }

        #[test]
        fn test_proposal_task_advances_without_leader_cert() {
            check_async(ITERATIONS, super::test_proposal_task_advances_without_leader_cert);
        }

        #[test]
        fn test_proposal_task_retries_after_leader_timeout() {
            check_async(ITERATIONS, super::test_proposal_task_retries_after_leader_timeout);
        }

        #[test]
        fn test_proposal_task_retries_on_none() {
            check_async(ITERATIONS, super::test_proposal_task_retries_on_none);
        }

        #[test]
        fn test_proposal_task_keeps_signal_from_during_proposal() {
            check_async(ITERATIONS, super::test_proposal_task_keeps_signal_from_during_proposal);
        }

        #[test]
        fn test_proposal_task_follows_round_of_proposal() {
            check_async(ITERATIONS, super::test_proposal_task_follows_round_of_proposal);
        }

        /// The round ends just before the task sends its batch again, so that call proposes a
        /// batch for the next round. The task counts that batch: it sends it again
        /// `MAX_BATCH_DELAY` later, and does not propose for that round a second time.
        #[test]
        fn rebroadcast_proposes_for_the_next_round() {
            /// A proposer that proposes for round 1, and for round 2 in its second call.
            struct Proposer {
                task: ProposalTask<MainnetV0>,
                num_calls: AtomicU32,
                call_times: mpsc::UnboundedSender<Instant>,
            }

            #[async_trait::async_trait]
            impl BatchPropose for Proposer {
                fn current_round(&self) -> u64 {
                    if self.num_calls.load(Ordering::SeqCst) < 2 { 1 } else { 2 }
                }

                async fn propose_batch(&self) -> Result<Option<u64>> {
                    self.call_times.send(Instant::now()).ok();
                    match self.num_calls.fetch_add(1, Ordering::SeqCst) {
                        0 => Ok(Some(1)),
                        1 => {
                            self.task.signal(2);
                            Ok(Some(2))
                        }
                        _ => Ok(None),
                    }
                }
            }

            check_async(ITERATIONS, || async {
                let task = ProposalTask::<MainnetV0>::default();
                let (call_times, mut call_times_receiver) = mpsc::unbounded_channel();
                tokio::spawn(task.clone().run(Proposer { task, num_calls: AtomicU32::new(0), call_times }));

                let mut call_times = Vec::new();
                for _ in 0..3 {
                    call_times.push(call_times_receiver.recv().await.unwrap());
                }
                assert_eq!(call_times[1] - call_times[0], MAX_BATCH_DELAY);
                assert_eq!(call_times[2] - call_times[1], MAX_BATCH_DELAY);
            });
        }
    }
}
