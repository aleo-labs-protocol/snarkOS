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

use crate::{BeginLedgerUpdateError, CoreLedgerService, LedgerService, LedgerUpdateService};

use snarkos_utilities::Stoppable;

use snarkvm::{
    ledger::{
        Block,
        CheckBlockError,
        Ledger,
        PendingBlock,
        Transaction,
        committee::Committee,
        narwhal::{Data, Transmission, TransmissionID},
        puzzle::{Solution, SolutionID},
        store::ConsensusStorage,
    },
    prelude::{Address, ConsensusVersion, Field, Network, Result, narwhal::BatchCertificate},
    synthesizer::process::deployment_freeze::FreezeWindow,
};

use async_trait::async_trait;
use std::{fmt, ops::Range, sync::Arc};

pub struct TranslucentLedgerService<N: Network, C: ConsensusStorage<N>> {
    inner: CoreLedgerService<N, C>,
}

impl<N: Network, C: ConsensusStorage<N>> fmt::Debug for TranslucentLedgerService<N, C> {
    /// Implements a custom `fmt::Debug` for `TranslucentLedgerService`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TranslucentLedgerService").field("inner", &self.inner).finish()
    }
}

impl<N: Network, C: ConsensusStorage<N>> TranslucentLedgerService<N, C> {
    /// Initializes a new ledger service wrapper.
    pub fn new(ledger: Ledger<N, C>, stoppable: Arc<dyn Stoppable>) -> Self {
        Self { inner: CoreLedgerService::new(ledger, stoppable) }
    }
}

#[async_trait]
impl<N: Network, C: ConsensusStorage<N>> LedgerService<N> for TranslucentLedgerService<N, C> {
    /// Returns the latest round in the ledger.
    fn latest_round(&self) -> u64 {
        self.inner.latest_round()
    }

    /// Returns the latest block height in the ledger.
    fn latest_block_height(&self) -> u32 {
        self.inner.latest_block_height()
    }

    /// Returns the latest block in the ledger.
    fn latest_block(&self) -> Block<N> {
        self.inner.latest_block()
    }

    /// Returns the latest restrictions ID in the ledger.
    fn latest_restrictions_id(&self) -> Field<N> {
        self.inner.latest_restrictions_id()
    }

    /// Returns the latest cached leader and its associated round.
    fn latest_leader(&self) -> Option<(u64, Address<N>)> {
        self.inner.latest_leader()
    }

    /// Updates the latest cached leader and its associated round.
    fn update_latest_leader(&self, round: u64, leader: Address<N>) {
        self.inner.update_latest_leader(round, leader);
    }

    /// Returns `true` if the given block height exists in the ledger.
    fn contains_block_height(&self, height: u32) -> bool {
        self.inner.contains_block_height(height)
    }

    /// Returns the block height for the given block hash, if it exists.
    fn get_block_height(&self, hash: &N::BlockHash) -> Result<u32> {
        self.inner.get_block_height(hash)
    }

    /// Returns the block hash for the given block height, if it exists.
    fn get_block_hash(&self, height: u32) -> Result<N::BlockHash> {
        self.inner.get_block_hash(height)
    }

    /// Returns the block round for the given block height, if it exists.
    fn get_block_round(&self, height: u32) -> Result<u64> {
        self.inner.get_block_round(height)
    }

    /// Returns the block for the given block height.
    fn get_block(&self, height: u32) -> Result<Block<N>> {
        self.inner.get_block(height)
    }

    /// Returns the blocks in the given block range.
    /// The range is inclusive of the start and exclusive of the end.
    fn get_blocks(&self, heights: Range<u32>) -> Result<Vec<Block<N>>> {
        self.inner.get_blocks(heights)
    }

    /// Returns the solution for the given solution ID.
    fn get_solution(&self, solution_id: &SolutionID<N>) -> Result<Option<Solution<N>>> {
        self.inner.get_solution(solution_id)
    }

    /// Returns the unconfirmed transaction for the given transaction ID.
    fn get_unconfirmed_transaction(&self, transaction_id: N::TransactionID) -> Result<Option<Transaction<N>>> {
        self.inner.get_unconfirmed_transaction(transaction_id)
    }

    /// Returns the batch certificate for the given batch certificate ID.
    fn get_batch_certificate(&self, certificate_id: &Field<N>) -> Result<BatchCertificate<N>> {
        self.inner.get_batch_certificate(certificate_id)
    }

    /// Returns the current committee.
    fn current_committee(&self) -> Result<Committee<N>> {
        self.inner.current_committee()
    }

    /// Returns the committee for the given round.
    fn get_committee_for_round(&self, round: u64) -> Result<Committee<N>> {
        self.inner.get_committee_for_round(round)
    }

    /// Returns the committee lookback for the given round.
    fn get_committee_lookback_for_round(&self, round: u64) -> Result<Committee<N>> {
        self.inner.get_committee_lookback_for_round(round)
    }

    /// Returns `true` if the ledger contains the given certificate ID in block history.
    fn contains_certificate(&self, certificate_id: &Field<N>) -> Result<bool> {
        self.inner.contains_certificate(certificate_id)
    }

    /// Returns `true` if the transmission exists in the ledger.
    fn contains_transmission(&self, transmission_id: &TransmissionID<N>) -> Result<bool> {
        self.inner.contains_transmission(transmission_id)
    }

    /// Always succeeds.
    fn ensure_transmission_is_well_formed(
        &self,
        _transmission_id: TransmissionID<N>,
        _transmission: &mut Transmission<N>,
    ) -> Result<()> {
        Ok(())
    }

    /// Always succeeds.
    async fn check_solution_basic(&self, _solution_id: SolutionID<N>, _solution: Data<Solution<N>>) -> Result<()> {
        Ok(())
    }

    /// Returns the deployment freeze policy of the inner ledger.
    fn deployment_freeze_policy(&self) -> Option<FreezeWindow> {
        self.inner.deployment_freeze_policy()
    }

    /// Always succeeds.
    async fn check_transaction_basic(
        &self,
        _transaction_id: N::TransactionID,
        _transaction: Transaction<N>,
    ) -> Result<()> {
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
        self.inner.begin_ledger_update()
    }

    /// Returns the spent cost for a transaction in microcredits.
    fn transaction_spend_in_microcredits(
        &self,
        transaction: &Transaction<N>,
        consensus_version: ConsensusVersion,
    ) -> Result<u64> {
        self.inner.transaction_spend_in_microcredits(transaction, consensus_version)
    }

    fn is_stopped(&self) -> bool {
        self.inner.is_stopped()
    }
}
