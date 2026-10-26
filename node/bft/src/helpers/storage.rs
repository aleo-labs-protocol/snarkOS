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

use crate::helpers::{check_timestamp_for_liveness, fmt_id};
use snarkos_node_bft_ledger_service::LedgerService;
use snarkos_node_bft_storage_service::StorageService;
use snarkvm::{
    ledger::{
        block::{Block, Transaction},
        narwhal::{BatchCertificate, BatchHeader, Transmission, TransmissionID},
    },
    prelude::{Address, Field, Network, Result, anyhow, bail, ensure},
    utilities::{cfg_into_iter, cfg_iter, cfg_sorted_by},
};

use anyhow::Context;
use indexmap::{IndexMap, IndexSet, map::Entry};
#[cfg(feature = "locktick")]
use locktick::parking_lot::{Mutex, RwLock};
use lru::LruCache;
#[cfg(not(feature = "locktick"))]
use parking_lot::{Mutex, RwLock};
#[cfg(not(feature = "serial"))]
use rayon::prelude::*;
use std::{
    cmp,
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
};
use tokio_util::sync::CancellationToken;

/// Errors returned by [`Storage::check_certificate`] (and therefore [`Storage::insert_certificate`]).
///
/// The `SameCertificate` and `SameAuthorAndRound` variants describe benign races: concurrent sync
/// paths regularly try to insert the same certificate, and the loser of that race should treat its
/// failure as a no-op (the certificate is in fact present in storage) rather than a hard error.
#[derive(Debug, thiserror::Error)]
pub enum CheckCertificateError {
    #[error("Certificate round {round} already exists in storage (gc_round = {gc_round})")]
    SameCertificate { round: u64, gc_round: u64 },
    #[error("Certificate with this author in round {round} is already in storage (gc_round = {gc_round})")]
    SameAuthorAndRound { round: u64, gc_round: u64 },
    #[error("Certificate round {round} is at or below the GC round {gc_round}")]
    RoundTooLow { round: u64, gc_round: u64 },
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl CheckCertificateError {
    /// Whether the error indicates the certificate is already in storage (a benign sync race
    /// rather than a hard failure). Callers should not log benign errors at ERROR.
    pub fn is_benign(&self) -> bool {
        matches!(self, Self::SameCertificate { .. } | Self::SameAuthorAndRound { .. } | Self::RoundTooLow { .. })
    }
}

#[derive(Clone, Debug)]
pub struct Storage<N: Network>(Arc<StorageInner<N>>);

impl<N: Network> std::ops::Deref for Storage<N> {
    type Target = Arc<StorageInner<N>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// The storage for the memory pool.
///
/// The storage is used to store the following:
/// - `current_height` tracker.
/// - `current_round` tracker.
/// - `round` to `(certificate ID, batch ID, author)` entries.
/// - `certificate ID` to `certificate` entries.
/// - `batch ID` to `round` entries.
/// - `transmission ID` to `(transmission, certificate IDs)` entries.
///
/// The chain of events is as follows:
/// 1. A `transmission` is received.
/// 2. After a `batch` is ready to be stored:
///   - The `certificate` is inserted, triggering updates to the
///     `rounds`, `certificates`, `batch_ids`, and `transmissions` maps.
///   - The missing `transmissions` from storage are inserted into the `transmissions` map.
///   - The certificate ID is inserted into the `transmissions` map.
/// 3. After a `round` reaches quorum threshold:
///  - The next round is inserted into the `current_round`.
#[derive(Debug)]
pub struct StorageInner<N: Network> {
    /// The ledger service.
    ledger: Arc<dyn LedgerService<N>>,
    /* Once per block */
    /// The current height.
    current_height: AtomicU32,
    /* Once per round */
    /// The current round.
    ///
    /// Invariant: current_round > 0.
    /// This is established in [`Storage::new`], which sets it to at least 1 via [`Storage::update_current_round`].
    /// The only callers of [`Storage::update_current_round`] are
    /// [`Storage::increment_to_next_round`] and [`Storage::sync_round_with_block`],
    /// both of which set it to at least 1.
    current_round: AtomicU64,
    /// The cancellation token for the current round, along with the round it belongs to.
    ///
    /// The token is cancelled as soon as the round advances, so tasks spawned on behalf of a
    /// round (e.g., sending its batch proposal) can stop once that round is over.
    /// See [`Storage::round_cancellation_token`].
    round_token: Mutex<(u64, CancellationToken)>,
    /// The `round` for which garbage collection has occurred **up to** (inclusive).
    gc_round: AtomicU64,
    /// The maximum number of rounds to keep in storage.
    max_gc_rounds: u64,
    /* Once per batch */
    /// The map of `round` to a list of `(certificate ID, author)` entries.
    rounds: RwLock<IndexMap<u64, IndexSet<(Field<N>, Address<N>)>>>,
    /// A cache of `certificate ID` to unprocessed `certificate`.
    unprocessed_certificates: RwLock<LruCache<Field<N>, BatchCertificate<N>>>,
    /// The map of `certificate ID` to `certificate`.
    certificates: RwLock<IndexMap<Field<N>, BatchCertificate<N>>>,
    /// The map of `certificate ID` to `round`.
    batch_ids: RwLock<IndexMap<Field<N>, u64>>,
    /// The map of `transmission ID` to `(transmission, certificate IDs)` entries.
    transmissions: Arc<dyn StorageService<N>>,
}

impl<N: Network> Storage<N> {
    /// Initializes a new instance of storage.
    pub fn new(
        ledger: Arc<dyn LedgerService<N>>,
        transmissions: Arc<dyn StorageService<N>>,
        max_gc_rounds: u64,
    ) -> Result<Self> {
        // Retrieve the latest committee bonded in the ledger
        // (genesis committee if the ledger contains only the genesis block).
        let committee = ledger.current_committee().expect("Ledger is missing a committee.");
        // Retrieve the round at which that committee was created, or 1 if it is the genesis committee.
        let current_round = committee.starting_round().max(1);
        // Set the unprocessed certificates cache size.
        let unprocessed_cache_size = NonZeroUsize::new((N::LATEST_MAX_CERTIFICATES() * 2) as usize).unwrap();

        // Create the storage.
        let storage = Self(Arc::new(StorageInner {
            ledger,
            current_height: Default::default(),
            current_round: AtomicU64::new(current_round),
            round_token: Mutex::new((current_round, CancellationToken::new())),
            gc_round: Default::default(),
            max_gc_rounds,
            rounds: Default::default(),
            unprocessed_certificates: RwLock::new(LruCache::new(unprocessed_cache_size)),
            certificates: Default::default(),
            batch_ids: Default::default(),
            transmissions,
        }));

        // Perform GC on the current round.
        // Since there are no certificates yet, this only sets `gc_round`.
        storage.garbage_collect_certificates(current_round)?;

        // Return the storage.
        Ok(storage)
    }
}

impl<N: Network> Storage<N> {
    /// Returns the current height.
    pub fn current_height(&self) -> u32 {
        // Get the current height.
        self.current_height.load(Ordering::SeqCst)
    }
}

impl<N: Network> Storage<N> {
    /// Returns the current round.
    pub fn current_round(&self) -> u64 {
        // Get the current round.
        self.current_round.load(Ordering::SeqCst)
    }

    /// Returns the `round` that garbage collection has occurred **up to** (inclusive).
    ///
    /// # Invariants
    /// The value returned is greater or equal to return values of prior calls to this method.
    pub fn gc_round(&self) -> u64 {
        // Get the GC round.
        self.gc_round.load(Ordering::SeqCst)
    }

    /// Returns the maximum number of rounds to keep in storage.
    pub fn max_gc_rounds(&self) -> u64 {
        self.max_gc_rounds
    }

    /// Increments storage to the next round, updating the current round.
    /// Note: This method is only called once per round, upon certification of the primary's batch.
    pub fn increment_to_next_round(&self, current_round: u64) -> Result<u64> {
        // Determine the next round.
        let next_round = current_round + 1;

        // Check if the next round is less than the current round in storage.
        {
            // Retrieve the storage round.
            let storage_round = self.current_round();
            // If the next round is less than the current round in storage, return early with the storage round.
            if next_round < storage_round {
                return Ok(storage_round);
            }

            trace!("Incrementing storage from round {storage_round} to {next_round}");
        }

        // Retrieve the current committee.
        let current_committee = self.ledger.current_committee()?;
        // Retrieve the current committee's starting round.
        let starting_round = current_committee.starting_round();
        // If the primary is behind the current committee's starting round, sync with the latest block.
        if next_round < starting_round {
            // Retrieve the latest block round.
            let latest_block_round = self.ledger.latest_round();
            // Log the round sync.
            info!(
                "Syncing primary round ({next_round}) with the current committee's starting round ({starting_round}). Syncing with the latest block round {latest_block_round}..."
            );
            // Sync the round with the latest block.
            self.sync_round_with_block(latest_block_round);
            // Return the latest block round.
            return Ok(latest_block_round);
        }

        // Update the storage to the next round.
        let storage_round = self.update_current_round(next_round);

        // Retrieve the GC round.
        let gc_round = self.gc_round();
        // Storage is guaranteed to have advanced to at least the next round, since
        // `update_current_round` only ever moves it forward via `fetch_max`.
        debug_assert!(
            storage_round >= next_round,
            "Storage round {storage_round} is behind the expected round {next_round}"
        );
        // Ensure the next round is greater than or equal to the GC round. This can legitimately
        // fail under normal operation: a concurrent sync may have advanced the GC round past this
        // (now stale) round update.
        ensure!(
            next_round >= gc_round,
            "The next round {next_round} is behind the current GC round {gc_round}, likely because a concurrent sync advanced past it"
        );

        #[cfg(feature = "metrics")]
        metrics::gauge(metrics::bft::LAST_STORED_ROUND, storage_round as f64);

        // Storage may already be ahead of `next_round` if a concurrent sync-applied round update
        // landed in between; return the true storage round rather than the stale `next_round`.
        if storage_round > next_round {
            return Ok(storage_round);
        }

        // Log the updated round.
        info!("Starting round {next_round}...");
        Ok(next_round)
    }

    /// Updates the storage to the next round.
    ///
    /// This is called concurrently from two independent paths: the BFT round-certification path
    /// (`increment_to_next_round`) and the sync-applied-block path (`sync_round_with_block`).
    /// `fetch_max` ensures the round only ever advances, regardless of interleaving, instead of a
    /// plain store letting a stale writer regress it.
    fn update_current_round(&self, next_round: u64) -> u64 {
        let previous_value = self.current_round.fetch_max(next_round, Ordering::SeqCst);
        let current_round = cmp::max(previous_value, next_round);
        // Cancel the token of any round that is now over.
        self.round_cancellation_token(current_round);
        current_round
    }

    /// Returns the cancellation token for the given round.
    ///
    /// The token is cancelled once the round is over, i.e., when storage advances past it.
    /// Tasks doing work that is only useful during `round` should stop when it fires.
    ///
    /// - If `round` is the latest round seen, this returns that round's (live) token.
    /// - If `round` is newer, the previous round's token is cancelled and a new one is created.
    ///   This keeps the token in step with `current_round`, even if called between the
    ///   `fetch_max` in [`Storage::update_current_round`] and its own call to this function.
    /// - If `round` is older, this returns an already-cancelled token.
    ///
    /// Callers must not pass a round ahead of `current_round`, as that would end the current round early.
    pub(crate) fn round_cancellation_token(&self, round: u64) -> CancellationToken {
        let mut round_token = self.round_token.lock();
        let (token_round, token) = &mut *round_token;
        match round.cmp(token_round) {
            cmp::Ordering::Greater => {
                debug_assert!(round <= self.current_round(), "Round {round} is ahead of the storage round");
                token.cancel();
                *token_round = round;
                *token = CancellationToken::new();
                token.clone()
            }
            cmp::Ordering::Equal => token.clone(),
            cmp::Ordering::Less => {
                let token = CancellationToken::new();
                token.cancel();
                token
            }
        }
    }

    /// Update the storage by performing garbage collection based on the next round.
    ///
    /// This is called concurrently from two independent paths: the BFT commit path
    /// (`commit_leader_certificate`) and the sync-bootup path (`sync_storage_with_ledger_at_bootup`).
    /// `fetch_max` ensures `gc_round` only ever advances, regardless of interleaving, instead of a
    /// compare-exchange erroring out when a stale/losing caller observes a smaller target round.
    pub(crate) fn garbage_collect_certificates(&self, next_round: u64) -> Result<()> {
        // Compute the next GC round.
        let next_gc_round = next_round.saturating_sub(self.max_gc_rounds);
        // Advance the GC round, recording the previous value.
        let previous_gc_round = self.gc_round.fetch_max(next_gc_round, Ordering::SeqCst);
        // Only the call that actually advanced the GC round performs the removal sweep.
        if next_gc_round > previous_gc_round {
            // Remove the GC round(s) from storage.
            for gc_round in previous_gc_round..=next_gc_round {
                // Iterate over the certificates for the GC round.
                for id in self.get_certificate_ids_for_round(gc_round).into_iter() {
                    trace!(
                        "Garbage collecting certificate {id} at round {gc_round} (cut-off is round {next_gc_round})"
                    );
                    self.remove_certificate(id);
                }
            }
        }

        Ok(())
    }
}

impl<N: Network> Storage<N> {
    /// Returns `true` if the storage contains the specified `round`.
    pub fn contains_certificates_for_round(&self, round: u64) -> bool {
        // Check if the round exists in storage.
        self.rounds.read().contains_key(&round)
    }

    /// Returns `true` if the storage contains the specified `certificate ID`.
    pub fn contains_certificate(&self, certificate_id: Field<N>) -> bool {
        // Check if the certificate ID exists in storage.
        self.certificates.read().contains_key(&certificate_id)
    }

    /// Returns `true` if the storage contains a certificate from the specified `author` in the given `round`.
    pub fn contains_certificate_in_round_from(&self, round: u64, author: Address<N>) -> bool {
        self.rounds.read().get(&round).is_some_and(|set| set.iter().any(|(_, a)| a == &author))
    }

    /// Returns `true` if the storage contains the specified `certificate ID`.
    pub fn contains_unprocessed_certificate(&self, certificate_id: Field<N>) -> bool {
        // Check if the certificate ID exists in storage.
        self.unprocessed_certificates.read().contains(&certificate_id)
    }

    /// Returns `true` if the storage contains the specified `batch ID`.
    pub fn contains_batch(&self, batch_id: Field<N>) -> bool {
        // Check if the batch ID exists in storage.
        self.batch_ids.read().contains_key(&batch_id)
    }

    /// Returns `true` if the storage holds the specified transmission, i.e. exactly when
    /// [`Self::get_transmission`] would return `Some`.
    ///
    /// This is `false` for a transmission ID that storage knows only as aborted, which holds no
    /// transmission; see [`Self::contains_aborted_transmission`]. There is deliberately no single
    /// query for "known either way": conflating the two is what let a batch be certified while
    /// committing to a transmission that storage cannot hand back.
    pub fn contains_retrievable_transmission(&self, transmission_id: impl Into<TransmissionID<N>>) -> bool {
        self.transmissions.contains_retrievable_transmission(transmission_id.into())
    }

    /// Returns `true` if the specified transmission ID was recorded as aborted.
    ///
    /// This and [`Self::contains_retrievable_transmission`] are **not** mutually exclusive: one
    /// certificate can record an ID as aborted while another provides the bytes for it - as
    /// `sync_certificate_with_block` does when the block carries them - in which case both
    /// queries answer `true`. Use `contains_aborted_transmission(id) &&
    /// !contains_retrievable_transmission(id)` to ask whether an ID is aborted *and* has nothing to
    /// hand back.
    pub fn contains_aborted_transmission(&self, transmission_id: impl Into<TransmissionID<N>>) -> bool {
        self.transmissions.contains_aborted_transmission(transmission_id.into())
    }

    /// Returns the transmission for the given `transmission ID`.
    /// If the transmission ID does not exist in storage or was aborted, `None` is returned.
    pub fn get_transmission(&self, transmission_id: impl Into<TransmissionID<N>>) -> Option<Transmission<N>> {
        self.transmissions.get_transmission(transmission_id.into())
    }

    /// Returns the certificate for the given `certificate ID`.
    /// If the certificate ID does not exist in storage, `None` is returned.
    pub fn get_certificate(&self, certificate_id: Field<N>) -> Option<BatchCertificate<N>> {
        // Get the batch certificate.
        self.certificates.read().get(&certificate_id).cloned()
    }

    /// Returns the unprocessed certificate for the given `certificate ID`.
    /// If the certificate ID does not exist in storage, `None` is returned.
    pub fn get_unprocessed_certificate(&self, certificate_id: Field<N>) -> Option<BatchCertificate<N>> {
        // Get the unprocessed certificate.
        self.unprocessed_certificates.read().peek(&certificate_id).cloned()
    }

    /// Returns the certificate for the given `round` and `author`.
    /// If the round does not exist in storage, `None` is returned.
    /// If the author for the round does not exist in storage, `None` is returned.
    pub fn get_certificate_for_round_with_author(&self, round: u64, author: Address<N>) -> Option<BatchCertificate<N>> {
        // Retrieve the certificates.
        if let Some(entries) = self.rounds.read().get(&round) {
            let certificates = self.certificates.read();
            entries.iter().find_map(
                |(certificate_id, a)| if a == &author { certificates.get(certificate_id).cloned() } else { None },
            )
        } else {
            Default::default()
        }
    }

    /// Returns the certificates for the given `round`.
    /// If the round does not exist in storage, an empty set is returned.
    pub fn get_certificates_for_round(&self, round: u64) -> IndexSet<BatchCertificate<N>> {
        // The genesis round does not have batch certificates.
        if round == 0 {
            return Default::default();
        }
        // Retrieve the certificates.
        if let Some(entries) = self.rounds.read().get(&round) {
            let certificates = self.certificates.read();
            entries.iter().flat_map(|(certificate_id, _)| certificates.get(certificate_id).cloned()).collect()
        } else {
            Default::default()
        }
    }

    /// Returns the certificate IDs for the given `round`.
    /// If the round does not exist in storage, an empty set is returned.
    pub fn get_certificate_ids_for_round(&self, round: u64) -> IndexSet<Field<N>> {
        // The genesis round does not have batch certificates.
        if round == 0 {
            return Default::default();
        }
        // Retrieve the certificates.
        if let Some(entries) = self.rounds.read().get(&round) {
            entries.iter().map(|(certificate_id, _)| *certificate_id).collect()
        } else {
            Default::default()
        }
    }

    /// Returns the certificate authors for the given `round`.
    /// If the round does not exist in storage, an empty set is returned.
    pub fn get_certificate_authors_for_round(&self, round: u64) -> HashSet<Address<N>> {
        // The genesis round does not have batch certificates.
        if round == 0 {
            return Default::default();
        }
        // Retrieve the certificates.
        if let Some(entries) = self.rounds.read().get(&round) {
            entries.iter().map(|(_, author)| *author).collect()
        } else {
            Default::default()
        }
    }

    /// Returns the certificates that have not yet been included in the ledger.
    /// Note that the order of this set is by round and then insertion.
    pub(crate) fn get_pending_certificates(&self) -> IndexSet<BatchCertificate<N>> {
        // Obtain the read locks.
        let rounds = self.rounds.read();
        let certificates = self.certificates.read();

        // Iterate over the rounds.
        cfg_sorted_by!(rounds.clone(), |a, _, b, _| a.cmp(b))
            .flat_map(|(_, certificates_for_round)| {
                // Iterate over the certificates for the round.
                cfg_into_iter!(certificates_for_round).filter_map(|(certificate_id, _)| {
                    // Skip the certificate if it already exists in the ledger.
                    if self.ledger.contains_certificate(&certificate_id).unwrap_or(false) {
                        None
                    } else {
                        // Add the certificate to the pending certificates.
                        certificates.get(&certificate_id).cloned()
                    }
                })
            })
            .collect()
    }

    /// Checks the given `batch_header` for validity, returning the missing transmissions from storage.
    ///
    /// # Arguments
    /// - `batch_header`: The batch header to check.
    /// - `transmissions`: All transmissions referenced by the certificate.
    /// - `aborted_transmissions`: The set of aborted transmissions in this certificate.
    ///
    /// # Invariants
    /// This method ensures the following invariants:
    /// - The batch ID does not already exist in storage.
    /// - The author is a member of the committee for the batch round.
    /// - The timestamp is within the allowed time range.
    /// - None of the transmissions are from any past rounds (up to GC).
    /// - All transmissions declared in the batch header are provided or exist in storage (up to GC).
    /// - All previous certificates declared in the certificate exist in storage (up to GC).
    /// - All previous certificates are for the previous round (i.e. round - 1).
    /// - All previous certificates contain a unique author.
    /// - The previous certificates reached the quorum threshold (N - f).
    ///
    /// # Returns
    /// - `Ok(Some(txns))` for a valid new batch, where `txns` is the set of missing transactions in the batch
    ///   that need to be fetched from peers.
    /// - `Ok(None)` if the batch already exists in storage
    pub fn check_batch_header(
        &self,
        batch_header: &BatchHeader<N>,
        transmissions: HashMap<TransmissionID<N>, Transmission<N>>,
        aborted_transmissions: HashSet<TransmissionID<N>>,
    ) -> Result<Option<HashMap<TransmissionID<N>, Transmission<N>>>> {
        // Retrieve the round.
        let round = batch_header.round();
        // Retrieve the GC round.
        let gc_round = self.gc_round();
        // Construct a GC log message.
        let gc_log = format!("(gc = {gc_round})");

        // Ensure the batch ID does not already exist in storage.
        if self.contains_batch(batch_header.batch_id()) {
            debug!("Batch for round {round} already exists in storage {gc_log}");
            return Ok(None);
        }

        // Retrieve the committee lookback for the batch round.
        let Ok(committee_lookback) = self.ledger.get_committee_lookback_for_round(round) else {
            bail!("Storage failed to retrieve the committee lookback for round {round} {gc_log}")
        };
        // Ensure the author is in the committee.
        if !committee_lookback.is_committee_member(batch_header.author()) {
            bail!("Author {} is not in the committee for round {round} {gc_log}", batch_header.author())
        }

        // Check the timestamp for liveness.
        check_timestamp_for_liveness(batch_header.timestamp())?;

        // Determine which transmissions in this batch are missing from storage.
        let missing_transmissions = self
            .transmissions
            .find_missing_transmissions(batch_header, transmissions, aborted_transmissions)
            .map_err(|e| anyhow!("{e} for round {round} {gc_log}"))?;

        // Compute the previous round.
        let previous_round = round.saturating_sub(1);
        // Check if the previous round is within range of the GC round.
        if previous_round > gc_round {
            // Retrieve the committee lookback for the previous round.
            let Ok(previous_committee_lookback) = self.ledger.get_committee_lookback_for_round(previous_round) else {
                bail!("Missing committee for the previous round {previous_round} in storage {gc_log}")
            };
            // Ensure the previous round certificates exists in storage.
            if !self.contains_certificates_for_round(previous_round) {
                bail!("Missing certificates for the previous round {previous_round} in storage {gc_log}")
            }
            // Ensure the number of previous certificate IDs is at or below the number of committee members.
            // As for the `test_network` clause, a more robust approach would be to set the
            // previous_committee_lookback appropriately when dev-on-prod is activated, or to only skip this
            // check right at the hotswap height.
            if !cfg!(feature = "test_network")
                && batch_header.previous_certificate_ids().len() > previous_committee_lookback.num_members()
            {
                bail!("Too many previous certificates for round {round} {gc_log}")
            }
            // Initialize a set of the previous authors.
            let mut previous_authors = HashSet::with_capacity(batch_header.previous_certificate_ids().len());
            // Ensure storage contains all declared previous certificates (up to GC).
            for previous_certificate_id in batch_header.previous_certificate_ids() {
                // Retrieve the previous certificate.
                let Some(previous_certificate) = self.get_certificate(*previous_certificate_id) else {
                    bail!(
                        "Missing previous certificate '{}' for certificate in round {round} {gc_log}",
                        fmt_id(previous_certificate_id)
                    )
                };
                // Ensure the previous certificate is for the previous round.
                if previous_certificate.round() != previous_round {
                    bail!("Round {round} certificate contains a round {previous_round} certificate {gc_log}")
                }
                // Ensure the previous author is new.
                if previous_authors.contains(&previous_certificate.author()) {
                    bail!("Round {round} certificate contains a duplicate author {gc_log}")
                }
                // Insert the author of the previous certificate.
                previous_authors.insert(previous_certificate.author());
            }
            // Ensure the previous certificates have reached the quorum threshold.
            if !previous_committee_lookback.is_quorum_threshold_reached(&previous_authors) {
                bail!("Previous certificates for a batch in round {round} did not reach quorum threshold {gc_log}")
            }
        }

        Ok(Some(missing_transmissions))
    }

    /// Check the validity of a certificate coming from another validator.
    ///
    /// It suffices to check that the signers (author and endorsers) are members of the applicable committee
    /// and that they form a quorum in the committee.
    /// Under the fundamental fault tolerance assumption of at most `f` (stake of) faulty validators,
    /// the quorum check on signers guarantees that at least one correct validator
    /// has ensured the validity of the proposal contained in the certificate,
    /// either by construction (by the author) or by checking (by an endorser):
    /// given `N > 0` total stake, and `f` the largest integer `< N/3` (where `/` is exact rational division),
    /// we have `N >= 3f + 1`, which implies `N - f >= 2f + 1`, which is always `> f`;
    /// `N - f` is the quorum stake.
    pub fn check_incoming_certificate(&self, certificate: &BatchCertificate<N>) -> Result<()> {
        // Retrieve the certificate author and round.
        let certificate_author = certificate.author();
        let certificate_round = certificate.round();

        // Retrieve the committee lookback.
        let committee_lookback = self.ledger.get_committee_lookback_for_round(certificate_round)?;

        // Ensure that the signers of the certificate reach the quorum threshold.
        // Note that certificate.signers() only returns the endorsing signers, not the author.
        let mut signers: HashSet<Address<N>> = certificate.signers().iter().copied().collect();
        signers.insert(certificate_author);
        ensure!(
            committee_lookback.is_quorum_threshold_reached(&signers),
            "Certificate '{}' for round {certificate_round} does not meet quorum requirements",
            certificate.id()
        );

        // Ensure that the signers of the certificate are in the committee.
        cfg_iter!(&signers).try_for_each(|signer| {
            ensure!(
                committee_lookback.is_committee_member(*signer),
                "Signer '{signer}' of certificate '{}' for round {certificate_round} is not in the committee",
                certificate.id()
            );
            Ok(())
        })?;

        Ok(())
    }

    /// Checks the given `certificate` for validity, returning the missing transmissions from storage.
    ///
    /// # Arguments
    /// - `certificate`: The certificate to check.
    /// - `transmissions`: The transmissions contained in the certificate.
    /// - `aborted_transmissions`: The aborted transmission contained in the certificate.
    ///
    /// # Invariants
    /// This method ensures the following invariants:
    /// - The certificate ID does not already exist in storage.
    /// - The batch ID does not already exist in storage.
    /// - The author is a member of the committee for the batch round.
    /// - The author has not already created a certificate for the batch round.
    /// - The timestamp is within the allowed time range.
    /// - None of the transmissions are from any past rounds (up to GC).
    /// - All transmissions declared in the batch header are provided or exist in storage (up to GC).
    /// - All previous certificates declared in the certificate exist in storage (up to GC).
    /// - All previous certificates are for the previous round (i.e. round - 1).
    /// - The previous certificates reached the quorum threshold (N - f).
    /// - The timestamps from the signers are all within the allowed time range.
    /// - The signers have reached the quorum threshold (N - f).
    ///
    /// # Errors
    /// Returns [`CheckCertificateError::SameCertificate`] or [`CheckCertificateError::SameAuthorAndRound`]
    /// if the certificate (or one from the same author for the same round) is already in storage.
    /// These are benign during concurrent sync; callers should not log them at ERROR.
    pub fn check_certificate(
        &self,
        certificate: &BatchCertificate<N>,
        transmissions: HashMap<TransmissionID<N>, Transmission<N>>,
        aborted_transmissions: HashSet<TransmissionID<N>>,
    ) -> Result<HashMap<TransmissionID<N>, Transmission<N>>, CheckCertificateError> {
        // Retrieve the round.
        let round = certificate.round();
        // Retrieve the GC round.
        let gc_round = self.gc_round();
        // Construct a GC log message.
        let gc_log = format!("(gc = {gc_round})");

        // Ensure the certificate ID does not already exist in storage.
        if self.contains_certificate(certificate.id()) {
            return Err(CheckCertificateError::SameCertificate { round, gc_round });
        }

        // Ensure the storage does not already contain a certificate for this author in this round.
        if self.contains_certificate_in_round_from(round, certificate.author()) {
            return Err(CheckCertificateError::SameAuthorAndRound { round, gc_round });
        }

        // Ensure the batch header is well-formed.
        let Some(missing_transmissions) =
            self.check_batch_header(certificate.batch_header(), transmissions, aborted_transmissions)?
        else {
            return Err(CheckCertificateError::SameCertificate { round, gc_round });
        };

        // Check the timestamp for liveness.
        check_timestamp_for_liveness(certificate.timestamp())?;

        // Retrieve the committee lookback for the batch round.
        let Ok(committee_lookback) = self.ledger.get_committee_lookback_for_round(round) else {
            return Err(anyhow!("Storage failed to retrieve the committee for round {round} {gc_log}").into());
        };

        // Initialize a set of the signers.
        let mut signers = HashSet::with_capacity(certificate.signers().len() + 1);
        // Append the batch author.
        signers.insert(certificate.author());

        // Iterate over the signers.
        for signer in certificate.signers().iter().copied() {
            // Ensure the signer is in the committee.
            if !committee_lookback.is_committee_member(signer) {
                return Err(anyhow!("Signer {signer} is not in the committee for round {round} {gc_log}").into());
            }
            // Append the signer.
            signers.insert(signer);
        }

        // Ensure the signatures have reached the quorum threshold.
        if !committee_lookback.is_quorum_threshold_reached(&signers) {
            return Err(
                anyhow!("Signatures for a batch in round {round} did not reach quorum threshold {gc_log}").into()
            );
        }

        Ok(missing_transmissions)
    }

    /// Inserts the given `certificate` into storage.
    ///
    /// This method triggers updates to the `rounds`, `certificates`, `batch_ids`, and `transmissions` maps.
    ///
    /// # Arguments
    /// - `certificate`: The certificate to insert.
    /// - `transmissions`: The transmissions contained in the certificate, or the subset of the transmissions that in the certificate that do not yet exist in storage.
    /// - `aborted_transmissions`: The aborted transmission contained in the certificate.
    ///
    /// # Invariants
    /// This method ensures the following invariants:
    /// - The certificate ID does not already exist in storage.
    /// - The batch ID does not already exist in storage.
    /// - All transmissions declared in the certificate are provided or exist in storage (up to GC).
    /// - All previous certificates declared in the certificate exist in storage (up to GC).
    /// - All previous certificates are for the previous round (i.e. round - 1).
    /// - The previous certificates reached the quorum threshold (N - f).
    pub fn insert_certificate(
        &self,
        certificate: BatchCertificate<N>,
        transmissions: HashMap<TransmissionID<N>, Transmission<N>>,
        aborted_transmissions: HashSet<TransmissionID<N>>,
    ) -> Result<(), CheckCertificateError> {
        // Ensure the certificate round is above the GC round.
        if certificate.round() <= self.gc_round() {
            return Err(CheckCertificateError::RoundTooLow { round: certificate.round(), gc_round: self.gc_round() });
        }
        // Ensure the certificate and its transmissions are valid.
        let missing_transmissions =
            self.check_certificate(&certificate, transmissions, aborted_transmissions.clone())?;
        // Note: a referenced ID that storage only knows as aborted, and that nobody provided, is
        // counted against the certificate by `insert_transmissions` itself - callers outside the
        // block sync always pass an empty set of aborted IDs.
        // Insert the certificate into storage.
        self.insert_certificate_atomic(certificate, aborted_transmissions, missing_transmissions);
        Ok(())
    }

    /// Inserts the given `certificate` into storage.
    ///
    /// This method assumes **all missing** transmissions are provided in the `missing_transmissions` map.
    ///
    /// This method triggers updates to the `rounds`, `certificates`, `batch_ids`, and `transmissions` maps.
    fn insert_certificate_atomic(
        &self,
        certificate: BatchCertificate<N>,
        aborted_transmission_ids: HashSet<TransmissionID<N>>,
        missing_transmissions: HashMap<TransmissionID<N>, Transmission<N>>,
    ) {
        // Retrieve the round.
        let round = certificate.round();
        // Retrieve the certificate ID.
        let certificate_id = certificate.id();
        // Retrieve the author of the batch.
        let author = certificate.author();

        // Insert the round to certificate ID entry.
        self.rounds.write().entry(round).or_default().insert((certificate_id, author));
        // Obtain the certificate's transmission ids.
        let transmission_ids = certificate.transmission_ids().clone();
        // Insert the certificate.
        self.certificates.write().insert(certificate_id, certificate);
        // Remove the unprocessed certificate.
        self.unprocessed_certificates.write().pop(&certificate_id);
        // Insert the batch ID.
        self.batch_ids.write().insert(certificate_id, round);
        // Insert the certificate ID for each of the transmissions into storage.
        self.transmissions.insert_transmissions(
            certificate_id,
            transmission_ids,
            aborted_transmission_ids,
            missing_transmissions,
        );
    }

    /// Inserts the given unprocessed `certificate` into storage.
    ///
    /// This is a temporary storage, which is cleared again when calling `insert_certificate_atomic`.
    pub fn insert_unprocessed_certificate(&self, certificate: BatchCertificate<N>) -> Result<()> {
        // Ensure the certificate round is above the GC round.
        ensure!(certificate.round() > self.gc_round(), "Certificate round is at or below the GC round");
        // Insert the certificate.
        self.unprocessed_certificates.write().put(certificate.id(), certificate);

        Ok(())
    }

    /// Removes the given `certificate ID` from storage. This method is used to garbage collect individual certificates once blocks are committed.
    ///
    /// This method triggers updates to the `rounds`, `certificates`, `batch_ids`, and `transmissions` maps.
    ///
    /// If the certificate was successfully removed, `true` is returned.
    /// If the certificate did not exist in storage, `false` is returned.
    fn remove_certificate(&self, certificate_id: Field<N>) -> bool {
        // Retrieve the certificate.
        let Some(certificate) = self.get_certificate(certificate_id) else {
            warn!("Certificate {certificate_id} does not exist in storage");
            return false;
        };
        // Retrieve the round.
        let round = certificate.round();
        // Compute the author of the batch.
        let author = certificate.author();

        // TODO (howardwu): We may want to use `shift_remove` below, in order to align compatibility
        //  with tests written to for `remove_certificate`. However, this will come with performance hits.
        //  It will be better to write tests that compare the union of the sets.

        // Update the round.
        match self.rounds.write().entry(round) {
            Entry::Occupied(mut entry) => {
                // Remove the round to certificate ID entry.
                entry.get_mut().swap_remove(&(certificate_id, author));
                // If the round is empty, remove it.
                if entry.get().is_empty() {
                    entry.swap_remove();
                }
            }
            Entry::Vacant(_) => {}
        }
        // Remove the certificate.
        self.certificates.write().swap_remove(&certificate_id);
        // Remove the unprocessed certificate.
        self.unprocessed_certificates.write().pop(&certificate_id);
        // Remove the batch ID.
        self.batch_ids.write().swap_remove(&certificate_id);
        // Remove the transmission entries in the certificate from storage.
        self.transmissions.remove_transmissions(&certificate_id, certificate.transmission_ids());
        // Return successfully.
        true
    }
}

impl<N: Network> Storage<N> {
    /// Syncs the current height with the block.
    pub(crate) fn sync_height_with_block(&self, next_height: u32) {
        // Update the current height in storage. `fetch_max` ensures the height only ever
        // advances, even if a concurrent writer already stored a higher value in between.
        self.current_height.fetch_max(next_height, Ordering::SeqCst);
    }

    /// Syncs the current round with the block.
    pub(crate) fn sync_round_with_block(&self, next_round: u64) {
        // Ensure we sync to at least round 1.
        let next_round = next_round.max(1);
        // If the round in the block is greater than the current round in storage, sync the round.
        if next_round > self.current_round() {
            // Update the current round in storage.
            self.update_current_round(next_round);
            // Log the updated round.
            info!("Synced to round {next_round}...");
        } else {
            trace!(
                "Skipping sync to round {next_round} as it is less than or equal to the current round ({})",
                self.current_round()
            );
        }
    }

    /// Syncs the batch certificate with the block.
    pub(crate) fn sync_certificate_with_block(
        &self,
        block: &Block<N>,
        certificate: BatchCertificate<N>,
        unconfirmed_transactions: &HashMap<N::TransactionID, Transaction<N>>,
        trusted_ledger_certificate: bool,
    ) -> Result<()> {
        // Skip if the certificate round is below the GC round.
        let gc_round = self.gc_round();
        if certificate.round() <= gc_round {
            trace!("Got certificate for round {} below GC round ({gc_round}). Will not store it.", certificate.round());
            return Ok(());
        }

        // If the certificate ID already exists in storage, skip it.
        if self.contains_certificate(certificate.id()) {
            trace!("Got certificate {} for round {} more than once.", certificate.id(), certificate.round());
            return Ok(());
        }
        // Retrieve the transmissions for the certificate.
        let mut missing_transmissions = HashMap::new();

        // Retrieve the aborted transmissions for the certificate.
        let mut aborted_transmissions = HashSet::new();

        // Track the block's aborted solutions and transactions.
        let aborted_solutions: IndexSet<_> = block.aborted_solution_ids().iter().collect();
        let aborted_transactions: IndexSet<_> = block.aborted_transaction_ids().iter().collect();

        // Iterate over the transmission IDs.
        for transmission_id in certificate.transmission_ids() {
            // If the transmission ID already exists in the map, skip it.
            if missing_transmissions.contains_key(transmission_id) {
                continue;
            }
            // If the transmission can be retrieved from storage, skip it.
            //
            // Note that an ID recorded as aborted by an earlier certificate is deliberately not
            // skipped here: storage holds no transmission for it, so it has to go through the
            // classification below - which either recovers the transmission from the block, or
            // declares the ID as aborted for this certificate as well.
            if self.contains_retrievable_transmission(*transmission_id) {
                continue;
            }
            // Retrieve the transmission.
            match transmission_id {
                TransmissionID::Ratification => (),
                TransmissionID::Solution(solution_id, _) => {
                    // The solution may exist either in the block itself, or stored in the ledger.
                    // Note that the ledger does *not* store aborted transmissions, so we need to check if the solution
                    // was aborted before querying the ledger.
                    //
                    // Aborted transmissions only appear in the aborted set of the first block that contains them,
                    // for subsequent blocks, `contains_aborted_transmission` is what tells us the transmission was aborted.
                    //
                    // Note that the ledger only answers for the blocks it already holds: the block that
                    // aborted the ID may still be awaiting its availability check in `Sync::pending_blocks`,
                    // so storage is consulted as well - it recorded the abort when that block was processed.
                    if let Some(solution) = block.get_solution(solution_id) {
                        missing_transmissions.insert(*transmission_id, (*solution).into());
                    } else if let Ok(Some(solution)) = self.ledger.get_solution(solution_id) {
                        missing_transmissions.insert(*transmission_id, solution.into());
                    } else if aborted_solutions.contains(solution_id)
                        || self.ledger.contains_transmission(transmission_id).unwrap_or(false)
                        || self.contains_aborted_transmission(*transmission_id)
                    {
                        aborted_transmissions.insert(*transmission_id);
                    } else {
                        bail!("Missing solution {solution_id} for block {}", block.height());
                    }
                }
                TransmissionID::Transaction(transaction_id, _) => {
                    // The transaction may exists either in the block itself, or stored in the ledger.
                    // Note that the ledger does *not* store aborted transmissions, so we need to check if the transaction
                    // was aborted before querying the ledger.
                    //
                    // Aborted solutions only appear in the aborted set of the first block that contains them,
                    // for subsequent blocks, `contains_aborted_transmission` is what tells us the transaction was aborted.
                    //
                    // Note that the ledger only answers for the blocks it already holds: the block that
                    // aborted the ID may still be awaiting its availability check in `Sync::pending_blocks`,
                    // so storage is consulted as well - it recorded the abort when that block was processed.
                    if let Some(transaction) = unconfirmed_transactions.get(transaction_id) {
                        missing_transmissions.insert(*transmission_id, transaction.clone().into());
                    } else if let Ok(Some(transaction)) = self.ledger.get_unconfirmed_transaction(*transaction_id) {
                        missing_transmissions.insert(*transmission_id, transaction.into());
                    } else if aborted_transactions.contains(transaction_id)
                        || self.ledger.contains_transmission(transmission_id).unwrap_or(false)
                        || self.contains_aborted_transmission(*transmission_id)
                    {
                        aborted_transmissions.insert(*transmission_id);
                    } else {
                        bail!("Missing transaction {transaction_id} for block {}", block.height());
                    }
                }
            }
        }
        // Insert the batch certificate into storage.
        let certificate_id = fmt_id(certificate.id());
        debug!(
            "Syncing certificate '{certificate_id}' for round {} with {} transmissions",
            certificate.round(),
            certificate.transmission_ids().len()
        );

        if trusted_ledger_certificate {
            self.insert_certificate_atomic(certificate, aborted_transmissions, missing_transmissions);
            Ok(())
        } else {
            self.insert_certificate(certificate, missing_transmissions, aborted_transmissions).with_context(|| {
                format!("Failed to insert certificate '{certificate_id}' from block {}", block.height())
            })
        }
    }
}

#[cfg(test)]
impl<N: Network> Storage<N> {
    /// Returns the ledger service.
    pub fn ledger(&self) -> &Arc<dyn LedgerService<N>> {
        &self.ledger
    }

    /// Returns an iterator over the `(round, (certificate ID, batch ID, author))` entries.
    pub fn rounds_iter(&self) -> impl Iterator<Item = (u64, IndexSet<(Field<N>, Address<N>)>)> {
        self.rounds.read().clone().into_iter()
    }

    /// Returns an iterator over the `(certificate ID, certificate)` entries.
    pub fn certificates_iter(&self) -> impl Iterator<Item = (Field<N>, BatchCertificate<N>)> {
        self.certificates.read().clone().into_iter()
    }

    /// Returns an iterator over the `(batch ID, round)` entries.
    pub fn batch_ids_iter(&self) -> impl Iterator<Item = (Field<N>, u64)> {
        self.batch_ids.read().clone().into_iter()
    }

    /// Returns an iterator over the `(transmission ID, (transmission, certificate IDs))` entries.
    pub fn transmissions_iter(
        &self,
    ) -> impl Iterator<Item = (TransmissionID<N>, (Transmission<N>, IndexSet<Field<N>>))> {
        self.transmissions.as_hashmap().into_iter()
    }

    /// Inserts the given `certificate` into storage.
    ///
    /// Note: Do NOT use this in production. This is for **testing only**.
    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) fn testing_only_insert_certificate_testing_only(&self, certificate: BatchCertificate<N>) {
        // Retrieve the round.
        let round = certificate.round();
        // Retrieve the certificate ID.
        let certificate_id = certificate.id();
        // Retrieve the author of the batch.
        let author = certificate.author();

        // Insert the round to certificate ID entry.
        self.rounds.write().entry(round).or_default().insert((certificate_id, author));
        // Obtain the certificate's transmission ids.
        let transmission_ids = certificate.transmission_ids().clone();
        // Insert the certificate.
        self.certificates.write().insert(certificate_id, certificate);
        // Insert the batch ID.
        self.batch_ids.write().insert(certificate_id, round);

        // Construct the dummy missing transmissions (for testing purposes).
        let missing_transmissions = transmission_ids
            .iter()
            .map(|id| (*id, Transmission::Transaction(snarkvm::ledger::narwhal::Data::Buffer(bytes::Bytes::new()))))
            .collect::<HashMap<_, _>>();
        // Insert the certificate ID for each of the transmissions into storage.
        self.transmissions.insert_transmissions(
            certificate_id,
            transmission_ids,
            Default::default(),
            missing_transmissions,
        );
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use snarkos_node_bft_ledger_service::MockLedgerService;
    use snarkos_node_bft_storage_service::BFTMemoryService;
    use snarkvm::{
        ledger::narwhal::{Data, batch_certificate::test_helpers::sample_batch_certificate_for_round_with_committee},
        prelude::{Rng, TestRng},
    };

    use ::bytes::Bytes;
    use indexmap::indexset;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    /// Asserts that the storage matches the expected layout.
    pub fn assert_storage<N: Network>(
        storage: &Storage<N>,
        rounds: &[(u64, IndexSet<(Field<N>, Address<N>)>)],
        certificates: &[(Field<N>, BatchCertificate<N>)],
        batch_ids: &[(Field<N>, u64)],
        transmissions: &HashMap<TransmissionID<N>, (Transmission<N>, IndexSet<Field<N>>)>,
    ) {
        // Ensure the rounds are well-formed.
        assert_eq!(storage.rounds_iter().collect::<Vec<_>>(), *rounds);
        // Ensure the certificates are well-formed.
        assert_eq!(storage.certificates_iter().collect::<Vec<_>>(), *certificates);
        // Ensure the batch IDs are well-formed.
        assert_eq!(storage.batch_ids_iter().collect::<Vec<_>>(), *batch_ids);
        // Ensure the transmissions are well-formed.
        assert_eq!(storage.transmissions_iter().collect::<HashMap<_, _>>(), *transmissions);
    }

    /// Samples a random transmission.
    fn sample_transmission(rng: &mut TestRng) -> Transmission<CurrentNetwork> {
        // Sample random fake solution bytes.
        let s = |rng: &mut TestRng| Data::Buffer(Bytes::from((0..512).map(|_| rng.random::<u8>()).collect::<Vec<_>>()));
        // Sample random fake transaction bytes.
        let t =
            |rng: &mut TestRng| Data::Buffer(Bytes::from((0..2048).map(|_| rng.random::<u8>()).collect::<Vec<_>>()));
        // Sample a random transmission.
        match rng.random::<bool>() {
            true => Transmission::Solution(s(rng)),
            false => Transmission::Transaction(t(rng)),
        }
    }

    /// Samples the random transmissions, returning the missing transmissions and the transmissions.
    pub(crate) fn sample_transmissions(
        certificate: &BatchCertificate<CurrentNetwork>,
        rng: &mut TestRng,
    ) -> (
        HashMap<TransmissionID<CurrentNetwork>, Transmission<CurrentNetwork>>,
        HashMap<TransmissionID<CurrentNetwork>, (Transmission<CurrentNetwork>, IndexSet<Field<CurrentNetwork>>)>,
    ) {
        // Retrieve the certificate ID.
        let certificate_id = certificate.id();

        let mut missing_transmissions = HashMap::new();
        let mut transmissions = HashMap::<_, (_, IndexSet<Field<CurrentNetwork>>)>::new();
        for transmission_id in certificate.transmission_ids() {
            // Initialize the transmission.
            let transmission = sample_transmission(rng);
            // Update the missing transmissions.
            missing_transmissions.insert(*transmission_id, transmission.clone());
            // Update the transmissions map.
            transmissions
                .entry(*transmission_id)
                .or_insert((transmission, Default::default()))
                .1
                .insert(certificate_id);
        }
        (missing_transmissions, transmissions)
    }

    // TODO (howardwu): Testing with 'max_gc_rounds' set to '0' should ensure everything is cleared after insertion.

    #[test]
    fn test_round_cancellation_token() {
        let rng = &mut TestRng::default();

        // Initialize the storage.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        let ledger = Arc::new(MockLedgerService::new(committee));
        let storage = Storage::<CurrentNetwork>::new(ledger, Arc::new(BFTMemoryService::new()), 1).unwrap();
        let round = storage.current_round();

        // The token for the current round is live, and shared across calls.
        let token = storage.round_cancellation_token(round);
        assert!(!token.is_cancelled());
        let same_token = storage.round_cancellation_token(round);

        // Advancing the round cancels the previous round's tokens.
        assert_eq!(storage.increment_to_next_round(round).unwrap(), round + 1);
        assert!(token.is_cancelled());
        assert!(same_token.is_cancelled());

        // A token for a round that is over is already cancelled.
        assert!(storage.round_cancellation_token(round).is_cancelled());

        // The token for the new round is live, until a sync moves storage past it.
        let token = storage.round_cancellation_token(round + 1);
        assert!(!token.is_cancelled());
        storage.sync_round_with_block(round + 5);
        assert!(token.is_cancelled());
        assert!(!storage.round_cancellation_token(round + 5).is_cancelled());

        // A sync that does not advance the round leaves the current token live.
        let token = storage.round_cancellation_token(round + 5);
        storage.sync_round_with_block(round + 2);
        assert!(!token.is_cancelled());
    }

    #[test]
    fn test_certificate_insert_remove() {
        let rng = &mut TestRng::default();

        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        // Initialize the ledger.
        let ledger = Arc::new(MockLedgerService::new(committee));
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger, Arc::new(BFTMemoryService::new()), 1).unwrap();

        // Ensure the storage is empty.
        assert_storage(&storage, &[], &[], &[], &Default::default());

        // Create a new certificate.
        let certificate = snarkvm::ledger::narwhal::batch_certificate::test_helpers::sample_batch_certificate(rng);
        // Retrieve the certificate ID.
        let certificate_id = certificate.id();
        // Retrieve the round.
        let round = certificate.round();
        // Retrieve the author of the batch.
        let author = certificate.author();

        // Construct the sample 'transmissions'.
        let (missing_transmissions, transmissions) = sample_transmissions(&certificate, rng);

        // Insert the certificate.
        storage.insert_certificate_atomic(certificate.clone(), Default::default(), missing_transmissions);
        // Ensure the certificate exists in storage.
        assert!(storage.contains_certificate(certificate_id));
        // Ensure the certificate is stored in the correct round.
        assert_eq!(storage.get_certificates_for_round(round), indexset! { certificate.clone() });
        // Ensure the certificate is stored for the correct round and author.
        assert_eq!(storage.get_certificate_for_round_with_author(round, author), Some(certificate.clone()));

        // Check that the underlying storage representation is correct.
        {
            // Construct the expected layout for 'rounds'.
            let rounds = [(round, indexset! { (certificate_id, author) })];
            // Construct the expected layout for 'certificates'.
            let certificates = [(certificate_id, certificate.clone())];
            // Construct the expected layout for 'batch_ids'.
            let batch_ids = [(certificate_id, round)];
            // Assert the storage is well-formed.
            assert_storage(&storage, &rounds, &certificates, &batch_ids, &transmissions);
        }

        // Retrieve the certificate.
        let candidate_certificate = storage.get_certificate(certificate_id).unwrap();
        // Ensure the retrieved certificate is the same as the inserted certificate.
        assert_eq!(certificate, candidate_certificate);

        // Remove the certificate.
        assert!(storage.remove_certificate(certificate_id));
        // Ensure the certificate does not exist in storage.
        assert!(!storage.contains_certificate(certificate_id));
        // Ensure the certificate is no longer stored in the round.
        assert!(storage.get_certificates_for_round(round).is_empty());
        // Ensure the certificate is no longer stored for the round and author.
        assert_eq!(storage.get_certificate_for_round_with_author(round, author), None);
        // Ensure the storage is empty.
        assert_storage(&storage, &[], &[], &[], &Default::default());
    }

    #[test]
    fn test_certificate_duplicate() {
        let rng = &mut TestRng::default();

        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        // Initialize the ledger.
        let ledger = Arc::new(MockLedgerService::new(committee));
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger, Arc::new(BFTMemoryService::new()), 1).unwrap();

        // Ensure the storage is empty.
        assert_storage(&storage, &[], &[], &[], &Default::default());

        // Create a new certificate.
        let certificate = snarkvm::ledger::narwhal::batch_certificate::test_helpers::sample_batch_certificate(rng);
        // Retrieve the certificate ID.
        let certificate_id = certificate.id();
        // Retrieve the round.
        let round = certificate.round();
        // Retrieve the author of the batch.
        let author = certificate.author();

        // Construct the expected layout for 'rounds'.
        let rounds = [(round, indexset! { (certificate_id, author) })];
        // Construct the expected layout for 'certificates'.
        let certificates = [(certificate_id, certificate.clone())];
        // Construct the expected layout for 'batch_ids'.
        let batch_ids = [(certificate_id, round)];
        // Construct the sample 'transmissions'.
        let (missing_transmissions, transmissions) = sample_transmissions(&certificate, rng);

        // Insert the certificate.
        storage.insert_certificate_atomic(certificate.clone(), Default::default(), missing_transmissions.clone());
        // Ensure the certificate exists in storage.
        assert!(storage.contains_certificate(certificate_id));
        // Check that the underlying storage representation is correct.
        assert_storage(&storage, &rounds, &certificates, &batch_ids, &transmissions);

        // Insert the certificate again - without any missing transmissions.
        storage.insert_certificate_atomic(certificate.clone(), Default::default(), Default::default());
        // Ensure the certificate exists in storage.
        assert!(storage.contains_certificate(certificate_id));
        // Check that the underlying storage representation remains unchanged.
        assert_storage(&storage, &rounds, &certificates, &batch_ids, &transmissions);

        // Insert the certificate again - with all of the original missing transmissions.
        storage.insert_certificate_atomic(certificate, Default::default(), missing_transmissions);
        // Ensure the certificate exists in storage.
        assert!(storage.contains_certificate(certificate_id));
        // Check that the underlying storage representation remains unchanged.
        assert_storage(&storage, &rounds, &certificates, &batch_ids, &transmissions);
    }

    /// Verify that when inserting a certificate with a mix of provided transmissions and aborted
    /// transmission IDs, storage correctly records both: every ID is recorded as either stored or
    /// aborted, and aborted IDs are kept so sync can resolve certificate references.
    #[test]
    fn test_certificate_insert_with_aborted_transmissions() {
        use std::collections::HashSet;

        let rng = &mut TestRng::default();

        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        let ledger = Arc::new(MockLedgerService::new(committee));
        let storage = Storage::<CurrentNetwork>::new(ledger, Arc::new(BFTMemoryService::new()), 1).unwrap();

        let certificate = snarkvm::ledger::narwhal::batch_certificate::test_helpers::sample_batch_certificate(rng);
        let certificate_id = certificate.id();
        let round = certificate.round();
        let transmission_ids: Vec<_> = certificate.transmission_ids().iter().copied().collect();

        if transmission_ids.len() < 2 {
            // Certificate has 0 or 1 transmission; just verify insert without aborted works.
            let (missing_transmissions, _) = sample_transmissions(&certificate, rng);
            storage.insert_certificate_atomic(certificate.clone(), HashSet::new(), missing_transmissions);
            for id in certificate.transmission_ids() {
                assert!(storage.contains_retrievable_transmission(*id));
            }
            return;
        }

        let (all_missing, _) = sample_transmissions(&certificate, rng);
        let aborted_id = transmission_ids[0];
        let aborted_transmission_ids: HashSet<_> = [aborted_id].into_iter().collect();
        let mut missing_transmissions = all_missing;
        missing_transmissions.remove(&aborted_id);

        storage.insert_certificate_atomic(certificate.clone(), aborted_transmission_ids, missing_transmissions);

        assert!(storage.contains_certificate(certificate_id));
        assert_eq!(storage.get_certificates_for_round(round), indexset! { certificate.clone() });

        // Every transmission ID in the certificate must be recorded, one way or the other.
        for id in certificate.transmission_ids() {
            assert!(
                storage.contains_retrievable_transmission(*id) || storage.contains_aborted_transmission(*id),
                "every transmission ID must be recorded as stored or aborted, including {id:?}"
            );
        }

        // Aborted transmission has no content in storage; others do.
        assert!(
            storage.get_transmission(aborted_id).is_none(),
            "Aborted transmission should not have content in storage"
        );
        for id in transmission_ids.iter().skip(1) {
            assert!(
                storage.get_transmission(*id).is_some(),
                "Non-aborted transmission {id:?} should have content in storage"
            );
        }
    }

    /// Builds a storage instance, plus a closure producing quorum-signed round-1 certificates that
    /// all reference the given transmission ID, each authored by a different committee member.
    ///
    /// The ledger holds no transaction for the ID either way - as is the case for one it recorded as
    /// aborted; `ledger_knows_the_id` only decides whether it reports the ID as contained.
    fn sample_storage_and_certificates_for_transmission(
        transmission_id: TransmissionID<CurrentNetwork>,
        ledger_knows_the_id: bool,
        rng: &mut TestRng,
    ) -> (Storage<CurrentNetwork>, impl Fn(usize, &mut TestRng) -> BatchCertificate<CurrentNetwork> + use<>) {
        let (committee, private_keys) =
            snarkvm::ledger::committee::test_helpers::sample_committee_and_keys_for_round(0, 5, rng);
        let known_transmission_ids =
            if ledger_knows_the_id { [transmission_id].into_iter().collect() } else { Default::default() };
        let ledger =
            Arc::new(MockLedgerService::new_with_known_transmission_ids(committee.clone(), known_transmission_ids));
        let storage = Storage::<CurrentNetwork>::new(ledger, Arc::new(BFTMemoryService::new()), 1).unwrap();

        let make_certificate = move |author_index: usize, rng: &mut TestRng| {
            let batch_header = BatchHeader::new(
                &private_keys[author_index],
                1,
                crate::helpers::now(),
                committee.id(),
                indexset![transmission_id],
                Default::default(),
                rng,
            )
            .unwrap();
            let signatures = private_keys
                .iter()
                .enumerate()
                .filter(|(index, _)| *index != author_index)
                .map(|(_, private_key)| private_key.sign(&[batch_header.batch_id()], rng).unwrap())
                .collect();
            BatchCertificate::from(batch_header, signatures).unwrap()
        };

        (storage, make_certificate)
    }

    /// A certificate referencing a transmission that an earlier certificate recorded as aborted must
    /// still be syncable from a block.
    ///
    /// Storage holds no transmission for such an ID, so it cannot be skipped as already known: it
    /// has to go through the classification, which recovers the transmission from the block if it
    /// carries one, and otherwise records the ID as aborted for this certificate as well. Skipping
    /// it would leave the ID out of both sets, and it would not be reference counted against the
    /// certificate that declares it.
    ///
    /// `ledger_knows_the_id` separates the two ways the classification can conclude that the ID was
    /// aborted: the block that aborted it has reached the ledger, or it has not - as while it awaits
    /// its availability check in `Sync::pending_blocks` - and only storage remembers.
    fn check_sync_certificate_with_a_previously_aborted_transmission(ledger_knows_the_id: bool) {
        use snarkvm::prelude::Uniform;

        let rng = &mut TestRng::default();

        // A transaction transmission ID, referenced by both certificates.
        let transmission_id = TransmissionID::Transaction(
            <CurrentNetwork as Network>::TransactionID::from(Field::rand(rng)),
            <CurrentNetwork as Network>::TransmissionChecksum::from(rng.random::<u128>()),
        );
        let (storage, make_certificate) =
            sample_storage_and_certificates_for_transmission(transmission_id, ledger_knows_the_id, rng);

        // Insert the first certificate, recording the transmission as aborted.
        let certificate_1 = make_certificate(0, rng);
        storage
            .insert_certificate(certificate_1.clone(), Default::default(), [transmission_id].into_iter().collect())
            .unwrap();
        assert!(storage.contains_aborted_transmission(transmission_id));
        assert!(!storage.contains_retrievable_transmission(transmission_id));

        // Sync a later certificate that references the previously-aborted transmission. The block
        // carries neither the transmission nor its ID, so only the ledger and storage know it; any
        // block will do here, hence the sample genesis block.
        let certificate_2 = make_certificate(1, rng);
        let block = snarkvm::ledger::test_helpers::sample_genesis_block(rng);
        storage
            .sync_certificate_with_block(&block, certificate_2.clone(), &Default::default(), false)
            .expect("syncing a certificate referencing a previously-aborted transmission must succeed");
        assert!(storage.contains_certificate(certificate_2.id()));

        // The ID must have been recorded as aborted for the second certificate too, so dropping the
        // first one must not take the aborted entry with it.
        assert!(storage.remove_certificate(certificate_1.id()));
        assert!(storage.contains_aborted_transmission(transmission_id));
        assert!(!storage.contains_retrievable_transmission(transmission_id));
    }

    #[test]
    fn test_sync_certificate_with_a_previously_aborted_transmission() {
        check_sync_certificate_with_a_previously_aborted_transmission(true);
    }

    #[test]
    fn test_sync_certificate_with_a_previously_aborted_transmission_the_ledger_does_not_know() {
        check_sync_certificate_with_a_previously_aborted_transmission(false);
    }

    /// A certificate inserted outside the block sync - as the primary does for a peer's batch or
    /// certificate - arrives with an empty set of aborted transmission IDs.
    ///
    /// A referenced ID that storage only knows as aborted still has to be counted against such a
    /// certificate, or removing the certificate that first recorded the abort drops the entry while
    /// this one is still referring to it.
    #[test]
    fn test_insert_certificate_counts_a_previously_aborted_transmission() {
        use snarkvm::prelude::Uniform;

        let rng = &mut TestRng::default();

        let transmission_id = TransmissionID::Transaction(
            <CurrentNetwork as Network>::TransactionID::from(Field::rand(rng)),
            <CurrentNetwork as Network>::TransmissionChecksum::from(rng.random::<u128>()),
        );
        let (storage, make_certificate) = sample_storage_and_certificates_for_transmission(transmission_id, false, rng);

        // Insert the first certificate, recording the transmission as aborted.
        let certificate_1 = make_certificate(0, rng);
        storage
            .insert_certificate(certificate_1.clone(), Default::default(), [transmission_id].into_iter().collect())
            .unwrap();

        // Insert a second certificate the way the primary does: no transmissions, and nothing
        // declared as aborted.
        let certificate_2 = make_certificate(1, rng);
        storage.insert_certificate(certificate_2.clone(), Default::default(), Default::default()).unwrap();
        assert!(storage.contains_certificate(certificate_2.id()));

        // Dropping the first certificate must leave the aborted entry in place for the second.
        assert!(storage.remove_certificate(certificate_1.id()));
        assert!(storage.contains_aborted_transmission(transmission_id));
        assert!(!storage.contains_retrievable_transmission(transmission_id));

        // Dropping the second one as well must then remove it.
        assert!(storage.remove_certificate(certificate_2.id()));
        assert!(!storage.contains_aborted_transmission(transmission_id));
    }

    /// Test that `check_incoming_certificate` does not reject a valid cert.
    #[test]
    fn test_valid_incoming_certificate() {
        let rng = &mut TestRng::default();

        // Sample a committee.
        let (committee, private_keys) =
            snarkvm::ledger::committee::test_helpers::sample_committee_and_keys_for_round(0, 5, rng);
        // Initialize the ledger.
        let ledger = Arc::new(MockLedgerService::new(committee));
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger, Arc::new(BFTMemoryService::new()), 1).unwrap();

        // Go through many rounds of valid certificates and ensure they're accepted.
        let mut previous_certs = IndexSet::default();

        for round in 1..=100 {
            let mut new_certs = IndexSet::default();

            // Generate one cert per validator
            for private_key in private_keys.iter() {
                let other_keys: Vec<_> = private_keys.iter().cloned().filter(|k| k != private_key).collect();

                let certificate = sample_batch_certificate_for_round_with_committee(
                    round,
                    previous_certs.clone(),
                    private_key,
                    &other_keys,
                    rng,
                );
                storage.check_incoming_certificate(&certificate).expect("Valid certificate rejected");
                new_certs.insert(certificate.id());

                // Construct the sample 'transmissions'.
                let (missing_transmissions, _transmissions) = sample_transmissions(&certificate, rng);
                storage.insert_certificate_atomic(certificate, Default::default(), missing_transmissions);
            }

            previous_certs = new_certs;
        }
    }

    /// Make sure that we reject all certificates without sufficient signatures early.
    #[test]
    fn test_invalid_incoming_certificate_missing_signature() {
        let rng = &mut TestRng::default();

        // Sample a committee.
        let (committee, private_keys) =
            snarkvm::ledger::committee::test_helpers::sample_committee_and_keys_for_round(0, 10, rng);
        // Initialize the ledger.
        let ledger = Arc::new(MockLedgerService::new(committee));
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger, Arc::new(BFTMemoryService::new()), 1).unwrap();

        // Go through many rounds of valid certificates and ensure they're accepted.
        let mut previous_certs = IndexSet::default();

        for round in 1..=5 {
            let mut new_certs = IndexSet::default();

            // Generate one cert per validator
            for private_key in private_keys.iter() {
                if round < 5 {
                    let other_keys: Vec<_> = private_keys.iter().cloned().filter(|k| k != private_key).collect();

                    let certificate = sample_batch_certificate_for_round_with_committee(
                        round,
                        previous_certs.clone(),
                        private_key,
                        &other_keys,
                        rng,
                    );
                    storage.check_incoming_certificate(&certificate).expect("Valid certificate rejected");
                    new_certs.insert(certificate.id());

                    // Construct the sample 'transmissions'.
                    let (missing_transmissions, _transmissions) = sample_transmissions(&certificate, rng);
                    storage.insert_certificate_atomic(certificate, Default::default(), missing_transmissions);
                } else {
                    // Pick a few signers, but not enough to form a quorum.
                    let other_keys: Vec<_> = private_keys[0..=3].iter().cloned().filter(|k| k != private_key).collect();

                    let certificate = sample_batch_certificate_for_round_with_committee(
                        round,
                        previous_certs.clone(),
                        private_key,
                        &other_keys,
                        rng,
                    );
                    assert!(storage.check_incoming_certificate(&certificate).is_err());
                }
            }

            previous_certs = new_certs;
        }
    }

    /// Verify that `insert_certificate` rejects certs with less edges than required.
    #[test]
    fn test_invalid_certificate_insufficient_previous_certs() {
        let rng = &mut TestRng::default();

        // Sample a committee.
        let (committee, private_keys) =
            snarkvm::ledger::committee::test_helpers::sample_committee_and_keys_for_round(0, 10, rng);
        // Initialize the ledger.
        let ledger = Arc::new(MockLedgerService::new(committee));
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger, Arc::new(BFTMemoryService::new()), 1).unwrap();

        // Go through many rounds of valid certificates and ensure they're accepted.
        let mut previous_certs = IndexSet::default();

        for round in 1..=6 {
            let mut new_certs = IndexSet::default();

            // Generate one cert per validator
            for private_key in private_keys.iter() {
                let other_keys: Vec<_> = private_keys.iter().cloned().filter(|k| k != private_key).collect();

                let certificate = sample_batch_certificate_for_round_with_committee(
                    round,
                    previous_certs.clone(),
                    private_key,
                    &other_keys,
                    rng,
                );

                // Construct the sample 'transmissions'.
                let (_missing_transmissions, transmissions) = sample_transmissions(&certificate, rng);
                let transmissions = transmissions.into_iter().map(|(k, (t, _))| (k, t)).collect();

                if round <= 5 {
                    new_certs.insert(certificate.id());
                    storage
                        .insert_certificate(certificate, transmissions, Default::default())
                        .expect("Valid certificate rejected");
                } else {
                    let err = storage
                        .insert_certificate(certificate, transmissions, Default::default())
                        .expect_err("Certificate with insufficient previous certs was accepted");
                    assert!(matches!(&err, CheckCertificateError::Other(_)));
                    assert!(!err.is_benign());
                }
            }

            if round < 5 {
                previous_certs = new_certs;
            } else {
                // Remove more than half of the previous certs.
                previous_certs = new_certs.into_iter().skip(6).collect();
            }
        }
    }

    /// Verify that inserting the exact same certificate twice returns `CheckCertificateError::SameCertificate`.
    #[test]
    fn test_check_certificate_error_same_certificate() {
        let rng = &mut TestRng::default();

        // Sample a committee.
        let (committee, private_keys) =
            snarkvm::ledger::committee::test_helpers::sample_committee_and_keys_for_round(0, 10, rng);
        // Initialize the ledger.
        let ledger = Arc::new(MockLedgerService::new(committee));
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger, Arc::new(BFTMemoryService::new()), 1).unwrap();

        // Construct a certificate with a full quorum of signers.
        let author = &private_keys[0];
        let other_keys: Vec<_> = private_keys.iter().cloned().filter(|k| k != author).collect();
        let certificate =
            sample_batch_certificate_for_round_with_committee(1, Default::default(), author, &other_keys, rng);

        // Construct the sample 'transmissions'.
        let (_missing_transmissions, transmissions) = sample_transmissions(&certificate, rng);
        let transmissions: HashMap<_, _> = transmissions.into_iter().map(|(k, (t, _))| (k, t)).collect();

        // Insert the certificate.
        storage
            .insert_certificate(certificate.clone(), transmissions.clone(), Default::default())
            .expect("Valid certificate rejected");

        // Inserting the exact same certificate again must fail with `SameCertificate`.
        let result = storage.insert_certificate(certificate, transmissions, Default::default());
        assert!(matches!(result, Err(CheckCertificateError::SameCertificate { .. })));
    }

    /// Verify that inserting two distinct certificates from the same author in the same round
    /// returns `CheckCertificateError::SameAuthorAndRound`.
    #[test]
    fn test_check_certificate_error_same_author_and_round() {
        let rng = &mut TestRng::default();

        // Sample a committee.
        let (committee, private_keys) =
            snarkvm::ledger::committee::test_helpers::sample_committee_and_keys_for_round(0, 10, rng);
        // Initialize the ledger.
        let ledger = Arc::new(MockLedgerService::new(committee));
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger, Arc::new(BFTMemoryService::new()), 1).unwrap();

        // Sample two distinct certificates from the same author for the same round.
        let author = &private_keys[0];
        let other_keys: Vec<_> = private_keys.iter().cloned().filter(|k| k != author).collect();
        let certificate_1 =
            sample_batch_certificate_for_round_with_committee(1, Default::default(), author, &other_keys, rng);
        let certificate_2 =
            sample_batch_certificate_for_round_with_committee(1, Default::default(), author, &other_keys, rng);
        assert_ne!(certificate_1.id(), certificate_2.id());

        // Insert the first certificate.
        let (_missing_transmissions, transmissions_1) = sample_transmissions(&certificate_1, rng);
        let transmissions_1: HashMap<_, _> = transmissions_1.into_iter().map(|(k, (t, _))| (k, t)).collect();
        storage
            .insert_certificate(certificate_1, transmissions_1, Default::default())
            .expect("Valid certificate rejected");

        // Inserting a different certificate from the same author and round must fail with `SameAuthorAndRound`.
        let (_missing_transmissions, transmissions_2) = sample_transmissions(&certificate_2, rng);
        let transmissions_2: HashMap<_, _> = transmissions_2.into_iter().map(|(k, (t, _))| (k, t)).collect();
        let result = storage.insert_certificate(certificate_2, transmissions_2, Default::default());
        assert!(matches!(result, Err(CheckCertificateError::SameAuthorAndRound { .. })));
    }

    /// Verify that `insert_certificate` rejects a certificate at or below the GC round with
    /// `CheckCertificateError::RoundTooLow`.
    #[test]
    fn test_check_certificate_error_round_too_low() {
        let rng = &mut TestRng::default();

        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        // Initialize the ledger.
        let ledger = Arc::new(MockLedgerService::new(committee));
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger, Arc::new(BFTMemoryService::new()), 1).unwrap();

        // Advance the GC round past round 2.
        storage.garbage_collect_certificates(3).unwrap();
        assert_eq!(storage.gc_round(), 2);

        // A certificate at or below the GC round must be rejected as `RoundTooLow`, regardless of its content.
        // Note: round 1 is reserved for the genesis committee and cannot have previous certificates, so round 2
        // is used here to keep certificate sampling generic (its round is otherwise irrelevant to this check).
        let certificate =
            snarkvm::ledger::narwhal::batch_certificate::test_helpers::sample_batch_certificate_for_round(2, rng);
        let result = storage.insert_certificate(certificate, Default::default(), Default::default());
        assert!(matches!(result, Err(CheckCertificateError::RoundTooLow { .. })));
    }

    /// Verify that a genuine validity failure (insufficient signatures to reach quorum) is
    /// reported as `CheckCertificateError::Other` and is not misclassified as benign.
    #[test]
    fn test_check_certificate_error_other() {
        let rng = &mut TestRng::default();

        // Sample a committee.
        let (committee, private_keys) =
            snarkvm::ledger::committee::test_helpers::sample_committee_and_keys_for_round(0, 10, rng);
        // Initialize the ledger.
        let ledger = Arc::new(MockLedgerService::new(committee));
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger, Arc::new(BFTMemoryService::new()), 1).unwrap();

        // Sign with too few endorsers to reach the committee's quorum threshold.
        let author = &private_keys[0];
        let other_keys: Vec<_> = private_keys[0..=3].iter().cloned().filter(|k| k != author).collect();
        let certificate =
            sample_batch_certificate_for_round_with_committee(1, Default::default(), author, &other_keys, rng);

        // Construct the sample 'transmissions'.
        let (_missing_transmissions, transmissions) = sample_transmissions(&certificate, rng);
        let transmissions: HashMap<_, _> = transmissions.into_iter().map(|(k, (t, _))| (k, t)).collect();

        let err = storage
            .insert_certificate(certificate, transmissions, Default::default())
            .expect_err("Certificate without quorum was accepted");
        assert!(matches!(&err, CheckCertificateError::Other(_)));
        assert!(!err.is_benign());
    }

    /// Verify that `insert_certificate` rejects certs that do not increment the round number.
    #[test]
    fn test_invalid_certificate_wrong_round_number() {
        let rng = &mut TestRng::default();

        // Sample a committee.
        let (committee, private_keys) =
            snarkvm::ledger::committee::test_helpers::sample_committee_and_keys_for_round(0, 10, rng);
        // Initialize the ledger.
        let ledger = Arc::new(MockLedgerService::new(committee));
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger, Arc::new(BFTMemoryService::new()), 1).unwrap();

        // Go through many rounds of valid certificates and ensure they're accepted.
        let mut previous_certs = IndexSet::default();

        for round in 1..=6 {
            let mut new_certs = IndexSet::default();

            // Generate one cert per validator
            for private_key in private_keys.iter() {
                let cert_round = round.min(5); // In the sixth round, do not increment
                let other_keys: Vec<_> = private_keys.iter().cloned().filter(|k| k != private_key).collect();

                let certificate = sample_batch_certificate_for_round_with_committee(
                    cert_round,
                    previous_certs.clone(),
                    private_key,
                    &other_keys,
                    rng,
                );

                // Construct the sample 'transmissions'.
                let (_missing_transmissions, transmissions) = sample_transmissions(&certificate, rng);
                let transmissions = transmissions.into_iter().map(|(k, (t, _))| (k, t)).collect();

                if round <= 5 {
                    new_certs.insert(certificate.id());
                    storage
                        .insert_certificate(certificate, transmissions, Default::default())
                        .expect("Valid certificate rejected");
                } else {
                    assert!(storage.insert_certificate(certificate, transmissions, Default::default()).is_err());
                }
            }

            if round < 5 {
                previous_certs = new_certs;
            } else {
                // Remove more than half of the previous certs.
                previous_certs = new_certs.into_iter().skip(6).collect();
            }
        }
    }

    /// `current_round`/`current_height` are written concurrently by two independent paths: the
    /// BFT round-certification path (`increment_to_next_round`) and the sync-applied-block path
    /// (`sync_round_with_block`/`sync_height_with_block`). A third, observing thread continuously
    /// samples both values while the writers race; neither value may ever be seen to regress, and
    /// the final values must converge to the max of what each writer proposed.
    ///
    /// The sync writer deliberately syncs in descending order, so a "stale" (lower) write can
    /// land after a fresher (higher) one — exactly the interleaving the old check-then-act code
    /// (load, compare, then a separate `store`) could get wrong. A shared barrier keeps all three
    /// threads in lockstep, one round per iteration, so the observer is reading concurrently with
    /// the writers on every iteration instead of racing ahead and finishing before they do.
    #[test]
    fn test_concurrent_round_and_height_updates_never_regress() {
        let rng = &mut TestRng::default();

        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        // Initialize the ledger.
        let ledger = Arc::new(MockLedgerService::new(committee));
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger, Arc::new(BFTMemoryService::new()), 10_000).unwrap();

        let start_round = storage.current_round();
        let start_height = storage.current_height();
        const ITERATIONS: u64 = 2_000;

        let barrier = Arc::new(std::sync::Barrier::new(3));

        // Thread A mimics the normal BFT path, incrementing one round at a time, from a fixed,
        // known sequence of rounds (rather than re-reading live storage) so its maximum proposed
        // round is known ahead of time.
        let storage_a = storage.clone();
        let barrier_a = barrier.clone();
        let increment_handle = std::thread::spawn(move || {
            for round in start_round..start_round + ITERATIONS {
                barrier_a.wait();
                storage_a.increment_to_next_round(round).expect("increment_to_next_round should not fail");
            }
        });

        // Thread B mimics a sync-applied block, syncing rounds/heights in descending order.
        let storage_b = storage.clone();
        let barrier_b = barrier.clone();
        let sync_handle = std::thread::spawn(move || {
            for i in (0..ITERATIONS).rev() {
                barrier_b.wait();
                storage_b.sync_round_with_block(start_round + i);
                storage_b.sync_height_with_block(i as u32);
            }
        });

        // Thread C repeatedly samples both values and asserts they never go backwards.
        let storage_c = storage.clone();
        let barrier_c = barrier.clone();
        let observer_handle = std::thread::spawn(move || {
            let mut last_round = storage_c.current_round();
            let mut last_height = storage_c.current_height();
            for _ in 0..ITERATIONS {
                barrier_c.wait();
                let round = storage_c.current_round();
                let height = storage_c.current_height();
                assert!(round >= last_round, "current_round regressed: {round} < {last_round}");
                assert!(height >= last_height, "current_height regressed: {height} < {last_height}");
                last_round = round;
                last_height = height;
            }
        });

        increment_handle.join().unwrap();
        sync_handle.join().unwrap();
        observer_handle.join().unwrap();

        // The final values must converge to the max of what each writer ever proposed.
        assert_eq!(storage.current_round(), start_round + ITERATIONS);
        assert_eq!(storage.current_height(), start_height.max(ITERATIONS as u32 - 1));
    }

    #[test]
    fn test_concurrent_gc_round_updates_never_regress() {
        let rng = &mut TestRng::default();

        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        // Initialize the ledger.
        let ledger = Arc::new(MockLedgerService::new(committee));
        // Initialize the storage with a small GC window so the GC round actually advances
        // as rounds are garbage collected.
        let storage = Storage::<CurrentNetwork>::new(ledger, Arc::new(BFTMemoryService::new()), 10).unwrap();

        let start_round = storage.current_round();
        const ITERATIONS: u64 = 2_000;

        let barrier = Arc::new(std::sync::Barrier::new(3));

        // Thread A mimics the BFT commit path, garbage collecting an increasing sequence of rounds.
        let storage_a = storage.clone();
        let barrier_a = barrier.clone();
        let commit_handle = std::thread::spawn(move || {
            for round in start_round..start_round + ITERATIONS {
                barrier_a.wait();
                storage_a.garbage_collect_certificates(round).expect("garbage_collect_certificates should not fail");
            }
        });

        // Thread B mimics the sync-bootup path racing against it with an out-of-order sequence.
        let storage_b = storage.clone();
        let barrier_b = barrier.clone();
        let sync_handle = std::thread::spawn(move || {
            for i in (0..ITERATIONS).rev() {
                barrier_b.wait();
                storage_b
                    .garbage_collect_certificates(start_round + i)
                    .expect("garbage_collect_certificates should not fail");
            }
        });

        // Thread C repeatedly samples the GC round and asserts it never goes backwards.
        let storage_c = storage.clone();
        let barrier_c = barrier.clone();
        let observer_handle = std::thread::spawn(move || {
            let mut last_gc_round = storage_c.gc_round();
            for _ in 0..ITERATIONS {
                barrier_c.wait();
                let gc_round = storage_c.gc_round();
                assert!(gc_round >= last_gc_round, "gc_round regressed: {gc_round} < {last_gc_round}");
                last_gc_round = gc_round;
            }
        });

        commit_handle.join().unwrap();
        sync_handle.join().unwrap();
        observer_handle.join().unwrap();

        // The final value must converge to the max of what each writer ever proposed.
        let max_round_proposed = start_round + ITERATIONS - 1;
        assert_eq!(storage.gc_round(), max_round_proposed.saturating_sub(storage.max_gc_rounds()));
    }
}

#[cfg(test)]
pub mod prop_tests {
    use super::*;
    use crate::helpers::{now, storage::tests::assert_storage};
    use snarkos_node_bft_events::committee_prop_tests::{CommitteeContext, ValidatorSet};
    use snarkos_node_bft_ledger_service::MockLedgerService;
    use snarkos_node_bft_storage_service::BFTMemoryService;
    use snarkvm::{
        ledger::{
            narwhal::{BatchHeader, Data},
            puzzle::SolutionID,
        },
        prelude::{Signature, Uniform},
    };

    use ::bytes::Bytes;
    use indexmap::indexset;
    use proptest::{
        collection,
        prelude::{Arbitrary, BoxedStrategy, Just, Strategy, any},
        prop_oneof,
        sample::{Selector, size_range},
    };
    use rand::{CryptoRng, SeedableRng, TryCryptoRng, TryRng};
    use rand_chacha::ChaChaRng;
    use std::fmt::Debug;
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    impl Arbitrary for Storage<CurrentNetwork> {
        type Parameters = CommitteeContext;
        type Strategy = BoxedStrategy<Storage<CurrentNetwork>>;

        fn arbitrary() -> Self::Strategy {
            (any::<CommitteeContext>(), 0..BatchHeader::<CurrentNetwork>::MAX_GC_ROUNDS as u64)
                .prop_map(|(CommitteeContext(committee, _), gc_rounds)| {
                    let ledger = Arc::new(MockLedgerService::new(committee));
                    Storage::<CurrentNetwork>::new(ledger, Arc::new(BFTMemoryService::new()), gc_rounds).unwrap()
                })
                .boxed()
        }

        fn arbitrary_with(context: Self::Parameters) -> Self::Strategy {
            (Just(context), 0..BatchHeader::<CurrentNetwork>::MAX_GC_ROUNDS as u64)
                .prop_map(|(CommitteeContext(committee, _), gc_rounds)| {
                    let ledger = Arc::new(MockLedgerService::new(committee));
                    Storage::<CurrentNetwork>::new(ledger, Arc::new(BFTMemoryService::new()), gc_rounds).unwrap()
                })
                .boxed()
        }
    }

    // The `proptest::TestRng` doesn't implement `rand_core::CryptoRng` trait which is required in snarkVM, so we use a wrapper
    // We wrap a `ChaChaRng` (rand 0.10 compatible) seeded from the proptest RNG.
    #[derive(Debug)]
    pub struct CryptoTestRng(ChaChaRng);

    impl Arbitrary for CryptoTestRng {
        type Parameters = ();
        type Strategy = BoxedStrategy<CryptoTestRng>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            use proptest::prelude::RngCore as ProptestRngCore;
            Just(0).prop_perturb(|_, mut rng| CryptoTestRng(ChaChaRng::seed_from_u64(rng.next_u64()))).boxed()
        }
    }

    impl TryRng for CryptoTestRng {
        type Error = core::convert::Infallible;

        fn try_next_u32(&mut self) -> Result<u32, Self::Error> {
            TryRng::try_next_u32(&mut self.0)
        }

        fn try_next_u64(&mut self) -> Result<u64, Self::Error> {
            TryRng::try_next_u64(&mut self.0)
        }

        fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), Self::Error> {
            TryRng::try_fill_bytes(&mut self.0, dest)
        }
    }

    impl TryCryptoRng for CryptoTestRng {}

    #[derive(Debug, Clone)]
    pub struct AnyTransmission(pub Transmission<CurrentNetwork>);

    impl Arbitrary for AnyTransmission {
        type Parameters = ();
        type Strategy = BoxedStrategy<AnyTransmission>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            any_transmission().prop_map(AnyTransmission).boxed()
        }
    }

    #[derive(Debug, Clone)]
    pub struct AnyTransmissionID(pub TransmissionID<CurrentNetwork>);

    impl Arbitrary for AnyTransmissionID {
        type Parameters = ();
        type Strategy = BoxedStrategy<AnyTransmissionID>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            any_transmission_id().prop_map(AnyTransmissionID).boxed()
        }
    }

    fn any_transmission() -> BoxedStrategy<Transmission<CurrentNetwork>> {
        prop_oneof![
            (collection::vec(any::<u8>(), 512..=512))
                .prop_map(|bytes| Transmission::Solution(Data::Buffer(Bytes::from(bytes)))),
            (collection::vec(any::<u8>(), 2048..=2048))
                .prop_map(|bytes| Transmission::Transaction(Data::Buffer(Bytes::from(bytes)))),
        ]
        .boxed()
    }

    pub fn any_solution_id() -> BoxedStrategy<SolutionID<CurrentNetwork>> {
        any::<u64>().prop_map(|x| x.into()).boxed()
    }

    pub fn any_transaction_id() -> BoxedStrategy<<CurrentNetwork as Network>::TransactionID> {
        any::<u64>()
            .prop_map(|seed| {
                let rng = &mut ChaChaRng::seed_from_u64(seed);
                <CurrentNetwork as Network>::TransactionID::from(Field::rand(rng))
            })
            .boxed()
    }

    pub fn any_transmission_id() -> BoxedStrategy<TransmissionID<CurrentNetwork>> {
        prop_oneof![
            (any_transaction_id(), any::<<CurrentNetwork as Network>::TransmissionChecksum>())
                .prop_map(|(id, cs)| TransmissionID::Transaction(id, cs)),
            (any_solution_id(), any::<<CurrentNetwork as Network>::TransmissionChecksum>())
                .prop_map(|(id, cs)| TransmissionID::Solution(id, cs)),
        ]
        .boxed()
    }

    pub fn sign_batch_header<R: CryptoRng>(
        validator_set: &ValidatorSet,
        batch_header: &BatchHeader<CurrentNetwork>,
        rng: &mut R,
    ) -> IndexSet<Signature<CurrentNetwork>> {
        let mut signatures = IndexSet::with_capacity(validator_set.0.len());
        for validator in validator_set.0.iter() {
            let private_key = validator.private_key;
            signatures.insert(private_key.sign(&[batch_header.batch_id()], rng).unwrap());
        }
        signatures
    }

    #[proptest]
    fn test_certificate_duplicate(
        context: CommitteeContext,
        #[any(size_range(1..16).lift())] transmissions: Vec<(AnyTransmissionID, AnyTransmission)>,
        mut rng: CryptoTestRng,
        selector: Selector,
    ) {
        let CommitteeContext(committee, ValidatorSet(validators)) = context;
        let committee_id = committee.id();

        // Initialize the storage.
        let ledger = Arc::new(MockLedgerService::new(committee));
        let storage = Storage::<CurrentNetwork>::new(ledger, Arc::new(BFTMemoryService::new()), 1).unwrap();

        // Ensure the storage is empty.
        assert_storage(&storage, &[], &[], &[], &Default::default());

        // Create a new certificate.
        let signer = selector.select(&validators);

        let mut transmission_map = IndexMap::new();

        for (AnyTransmissionID(id), AnyTransmission(t)) in transmissions.iter() {
            transmission_map.insert(*id, t.clone());
        }

        let batch_header = BatchHeader::new(
            &signer.private_key,
            0,
            now(),
            committee_id,
            transmission_map.keys().cloned().collect(),
            Default::default(),
            &mut rng,
        )
        .unwrap();

        // Remove the author from the validator set passed to create the batch
        // certificate, the author should not sign their own batch.
        let mut validators = validators.clone();
        validators.remove(signer);

        let certificate = BatchCertificate::from(
            batch_header.clone(),
            sign_batch_header(&ValidatorSet(validators), &batch_header, &mut rng),
        )
        .unwrap();

        // Retrieve the certificate ID.
        let certificate_id = certificate.id();
        let mut internal_transmissions = HashMap::<_, (_, IndexSet<Field<CurrentNetwork>>)>::new();
        for (AnyTransmissionID(id), AnyTransmission(t)) in transmissions.iter().cloned() {
            internal_transmissions.entry(id).or_insert((t, Default::default())).1.insert(certificate_id);
        }

        // Retrieve the round.
        let round = certificate.round();
        // Retrieve the author of the batch.
        let author = certificate.author();

        // Construct the expected layout for 'rounds'.
        let rounds = [(round, indexset! { (certificate_id, author) })];
        // Construct the expected layout for 'certificates'.
        let certificates = [(certificate_id, certificate.clone())];
        // Construct the expected layout for 'batch_ids'.
        let batch_ids = [(certificate_id, round)];

        // Insert the certificate.
        let missing_transmissions: HashMap<TransmissionID<CurrentNetwork>, Transmission<CurrentNetwork>> =
            transmission_map.into_iter().collect();
        storage.insert_certificate_atomic(certificate.clone(), Default::default(), missing_transmissions.clone());
        // Ensure the certificate exists in storage.
        assert!(storage.contains_certificate(certificate_id));
        // Check that the underlying storage representation is correct.
        assert_storage(&storage, &rounds, &certificates, &batch_ids, &internal_transmissions);

        // Insert the certificate again - without any missing transmissions.
        storage.insert_certificate_atomic(certificate.clone(), Default::default(), Default::default());
        // Ensure the certificate exists in storage.
        assert!(storage.contains_certificate(certificate_id));
        // Check that the underlying storage representation remains unchanged.
        assert_storage(&storage, &rounds, &certificates, &batch_ids, &internal_transmissions);

        // Insert the certificate again - with all of the original missing transmissions.
        storage.insert_certificate_atomic(certificate, Default::default(), missing_transmissions);
        // Ensure the certificate exists in storage.
        assert!(storage.contains_certificate(certificate_id));
        // Check that the underlying storage representation remains unchanged.
        assert_storage(&storage, &rounds, &certificates, &batch_ids, &internal_transmissions);
    }
}
