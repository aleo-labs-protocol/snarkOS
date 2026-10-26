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

use crate::{BeginLedgerUpdateError, LedgerService, LedgerUpdateService, fmt_id};
use snarkvm::{
    ledger::{
        Block,
        CheckBlockError,
        PendingBlock,
        Transaction,
        committee::Committee,
        narwhal::{BatchCertificate, Data, Transmission, TransmissionID},
        puzzle::{Solution, SolutionID},
    },
    prelude::{Address, ConsensusVersion, Field, Network, Result, Zero, bail, consensus_config_value},
};

#[cfg(feature = "locktick")]
use locktick::parking_lot::Mutex;
#[cfg(not(feature = "locktick"))]
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, HashSet},
    ops::Range,
};
use tracing::*;

/// A mock ledger service that always returns `false`, unless configured otherwise.
#[derive(Debug)]
pub struct MockLedgerService<N: Network> {
    committee: Committee<N>,
    height_to_round_and_hash: Mutex<BTreeMap<u32, (u64, N::BlockHash)>>,
    /// The transmission IDs the ledger knows without being able to produce a transmission for them,
    /// as is the case for the ones it recorded as aborted.
    known_transmission_ids: HashSet<TransmissionID<N>>,
}

impl<N: Network> MockLedgerService<N> {
    /// Initializes a new mock ledger service.
    pub fn new(committee: Committee<N>) -> Self {
        Self { committee, height_to_round_and_hash: Default::default(), known_transmission_ids: Default::default() }
    }

    /// Initializes a new mock ledger service at the specified height.
    pub fn new_at_height(committee: Committee<N>, height: u32) -> Self {
        let mut height_to_hash = BTreeMap::new();
        for i in 0..=height {
            height_to_hash.insert(i, (i as u64 * 2, Field::<N>::from_u32(i).into()));
        }
        Self {
            committee,
            height_to_round_and_hash: Mutex::new(height_to_hash),
            known_transmission_ids: Default::default(),
        }
    }

    /// Initializes a new mock ledger service that knows the given transmission IDs.
    ///
    /// The real ledger reports an aborted transmission ID as contained while holding no transmission
    /// for it; this models that, which the default mock cannot do.
    pub fn new_with_known_transmission_ids(
        committee: Committee<N>,
        known_transmission_ids: HashSet<TransmissionID<N>>,
    ) -> Self {
        Self { committee, height_to_round_and_hash: Default::default(), known_transmission_ids }
    }
}

#[async_trait]
impl<N: Network> LedgerService<N> for MockLedgerService<N> {
    /// Returns the latest round in the ledger.
    fn latest_round(&self) -> u64 {
        *self.height_to_round_and_hash.lock().keys().last().unwrap_or(&0) as u64
    }

    /// Returns the latest block height in the canonical ledger.
    fn latest_block_height(&self) -> u32 {
        self.height_to_round_and_hash.lock().last_key_value().map(|(height, _)| *height).unwrap_or(0)
    }

    /// Returns the latest block in the ledger.
    fn latest_block(&self) -> Block<N> {
        unreachable!("MockLedgerService does not support latest_block")
    }

    /// Returns the latest restrictions ID in the ledger.
    fn latest_restrictions_id(&self) -> Field<N> {
        Field::zero()
    }

    /// Returns the latest cached leader and its associated round.
    fn latest_leader(&self) -> Option<(u64, Address<N>)> {
        None
    }

    /// Updates the latest cached leader and its associated round.
    fn update_latest_leader(&self, _round: u64, _leader: Address<N>) {}

    /// Returns `true` if the given block height exists in the canonical ledger.
    fn contains_block_height(&self, height: u32) -> bool {
        self.height_to_round_and_hash.lock().contains_key(&height)
    }

    /// Returns the canonical block height for the given block hash, if it exists.
    fn get_block_height(&self, hash: &N::BlockHash) -> Result<u32> {
        match self
            .height_to_round_and_hash
            .lock()
            .iter()
            .find_map(|(height, (_, h))| if h == hash { Some(*height) } else { None })
        {
            Some(height) => Ok(height),
            None => bail!("Missing block {hash}"),
        }
    }

    /// Returns the canonical block hash for the given block height, if it exists.
    fn get_block_hash(&self, height: u32) -> Result<N::BlockHash> {
        match self.height_to_round_and_hash.lock().get(&height).cloned() {
            Some((_, hash)) => Ok(hash),
            None => bail!("Missing block {height}"),
        }
    }

    /// Returns the block round for the given block height, if it exists.
    fn get_block_round(&self, height: u32) -> Result<u64> {
        match self
            .height_to_round_and_hash
            .lock()
            .iter()
            .find_map(|(h, (round, _))| if *h == height { Some(*round) } else { None })
        {
            Some(round) => Ok(round),
            None => bail!("Missing block {height}"),
        }
    }

    /// Returns the block for the given block height.
    fn get_block(&self, _height: u32) -> Result<Block<N>> {
        unreachable!("MockLedgerService does not support get_block")
    }

    /// Returns the blocks in the given block range.
    /// The range is inclusive of the start and exclusive of the end.
    fn get_blocks(&self, _heights: Range<u32>) -> Result<Vec<Block<N>>> {
        unreachable!("MockLedgerService does not support get_blocks")
    }

    /// Returns the solution for the given solution ID.
    fn get_solution(&self, _solution_id: &SolutionID<N>) -> Result<Option<Solution<N>>> {
        unreachable!("MockLedgerService does not support get_solution")
    }

    /// Returns the unconfirmed transaction for the given transaction ID.
    fn get_unconfirmed_transaction(&self, _transaction_id: N::TransactionID) -> Result<Option<Transaction<N>>> {
        // Note: this deliberately does not panic, unlike the other getters. A caller that finds a
        // transmission ID contained but unretrievable reaches this on its way to establishing that
        // the ID was aborted, and the mock holds no transactions to hand back.
        Ok(None)
    }

    /// Returns the batch certificate for the given batch certificate ID.
    fn get_batch_certificate(&self, _certificate_id: &Field<N>) -> Result<BatchCertificate<N>> {
        unreachable!("MockLedgerService does not support get_batch_certificate")
    }

    /// Returns the current committee.
    fn current_committee(&self) -> Result<Committee<N>> {
        Ok(self.committee.clone())
    }

    /// Returns the committee for the given round.
    fn get_committee_for_round(&self, _round: u64) -> Result<Committee<N>> {
        Ok(self.committee.clone())
    }

    /// Returns the committee lookback for the given round.
    fn get_committee_lookback_for_round(&self, _round: u64) -> Result<Committee<N>> {
        Ok(self.committee.clone())
    }

    /// Returns `false` for all queries.
    fn contains_certificate(&self, certificate_id: &Field<N>) -> Result<bool> {
        trace!("[MockLedgerService] Contains certificate ID {} - false", fmt_id(certificate_id));
        Ok(false)
    }

    /// Returns `false`, unless the mock was configured to know the transmission ID.
    fn contains_transmission(&self, transmission_id: &TransmissionID<N>) -> Result<bool> {
        let contains = self.known_transmission_ids.contains(transmission_id);
        trace!(
            "[MockLedgerService] Contains transmission ID {}.{} - {contains}",
            fmt_id(transmission_id),
            fmt_id(transmission_id.checksum().unwrap_or_default())
        );
        Ok(contains)
    }

    /// Ensures that the given transmission is not a fee and matches the given transmission ID.
    fn ensure_transmission_is_well_formed(
        &self,
        transmission_id: TransmissionID<N>,
        _transmission: &mut Transmission<N>,
    ) -> Result<()> {
        trace!(
            "[MockLedgerService] Ensure transmission ID matches {}.{} - Ok",
            fmt_id(transmission_id),
            fmt_id(transmission_id.checksum().unwrap_or_default())
        );
        Ok(())
    }

    /// Checks the given solution is well-formed.
    async fn check_solution_basic(&self, solution_id: SolutionID<N>, _solution: Data<Solution<N>>) -> Result<()> {
        trace!("[MockLedgerService] Check solution basic {:?} - Ok", fmt_id(solution_id));
        Ok(())
    }

    /// Checks the given transaction is well-formed and unique.
    async fn check_transaction_basic(
        &self,
        transaction_id: N::TransactionID,
        _transaction: Transaction<N>,
    ) -> Result<()> {
        trace!("[MockLedgerService] Check transaction basic {:?} - Ok", fmt_id(transaction_id));
        Ok(())
    }

    fn check_block_subdag(
        &self,
        _block: Block<N>,
        _prefix: &[PendingBlock<N>],
    ) -> Result<PendingBlock<N>, CheckBlockError<N>> {
        unimplemented!();
    }

    /// Begins a ledger update.
    fn begin_ledger_update<'a>(&'a self) -> Result<Box<dyn LedgerUpdateService<N> + 'a>, BeginLedgerUpdateError> {
        unimplemented!();
    }

    /// Returns the spent cost for a transaction in microcredits.
    fn transaction_spend_in_microcredits(
        &self,
        _transaction: &Transaction<N>,
        consensus_version: ConsensusVersion,
    ) -> Result<u64> {
        let height = N::CONSENSUS_HEIGHT(consensus_version).unwrap();
        Ok(consensus_config_value!(N, TRANSACTION_SPEND_LIMIT, height).unwrap())
    }

    fn is_stopped(&self) -> bool {
        // No ledger to check against.
        false
    }
}
