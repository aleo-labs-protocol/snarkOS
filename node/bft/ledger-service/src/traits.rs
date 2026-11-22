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

use snarkvm::{
    ledger::{
        Block,
        CheckBlockError,
        PendingBlock,
        Transaction,
        committee::Committee,
        narwhal::{BatchCertificate, Data, Subdag, Transmission, TransmissionID},
        puzzle::{Solution, SolutionID},
    },
    prelude::{Address, ConsensusVersion, Field, Network, Result},
};

use indexmap::IndexMap;
use std::ops::Range;

/// Errors that can occur when attempting to start a ledger update.
#[derive(Debug, thiserror::Error)]
pub enum BeginLedgerUpdateError {
    #[error("Node is shutting down.")]
    ShuttingDown,
    #[error("Not supported for prover.")]
    NotSupportedForProver,
}

/// The ledger holds no committee for the given round.
///
/// A ledger that has not yet advanced to `round` returns this error for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("No committee found for round {round} in the ledger")]
pub struct CommitteeNotFound {
    pub round: u64,
}

/// Allows performing updates to the ledger and prevents concurrent updates while the object is alive.
pub trait LedgerUpdateService<N: Network> {
    /// Checks the given block is valid subdag.
    fn check_block_subdag(
        &self,
        block: Block<N>,
        prefix: &[PendingBlock<N>],
    ) -> Result<PendingBlock<N>, CheckBlockError<N>>;
    /// Checks the given pending block has valid content and can be appended to the ledger.
    fn check_block_content(&self, block: PendingBlock<N>) -> Result<Block<N>, CheckBlockError<N>>;
    /// Checks the given block is a valid next block for the ledger.
    fn check_next_block(&self, block: Block<N>) -> Result<Block<N>, CheckBlockError<N>>;
    /// Prepares the next block in the ledger, using a committed subdag and its transmissions.
    fn prepare_advance_to_next_quorum_block(
        &self,
        subdag: Subdag<N>,
        transmissions: IndexMap<TransmissionID<N>, Transmission<N>>,
    ) -> Result<Block<N>, CheckBlockError<N>>;
    /// Advances the ledger to the next block.
    fn advance_to_next_block(&self, block: &Block<N>) -> Result<()>;
}

#[async_trait]
pub trait LedgerService<N: Network>: std::fmt::Debug + Send + Sync {
    /// Returns the latest round in the ledger.
    fn latest_round(&self) -> u64;

    /// Returns the latest block height in the ledger.
    fn latest_block_height(&self) -> u32;

    /// Returns the latest block in the ledger.
    fn latest_block(&self) -> Block<N>;

    /// Returns the latest restrictions ID in the ledger.
    fn latest_restrictions_id(&self) -> Field<N>;

    /// Returns the latest cached leader and its associated round.
    fn latest_leader(&self) -> Option<(u64, Address<N>)>;

    /// Updates the latest cached leader and its associated round.
    fn update_latest_leader(&self, round: u64, leader: Address<N>);

    /// Returns `true` if the given block height exists in the ledger.
    fn contains_block_height(&self, height: u32) -> bool;

    /// Returns the block height for the given block hash, if it exists.
    fn get_block_height(&self, hash: &N::BlockHash) -> Result<u32>;

    /// Returns the block hash for the given block height, if it exists.
    fn get_block_hash(&self, height: u32) -> Result<N::BlockHash>;

    /// Returns the block round for the given block height, if it exists.
    fn get_block_round(&self, height: u32) -> Result<u64>;

    /// Returns the block for the given block height.
    fn get_block(&self, height: u32) -> Result<Block<N>>;

    /// Returns the blocks in the given block range.
    /// The range is inclusive of the start and exclusive of the end.
    fn get_blocks(&self, heights: Range<u32>) -> Result<Vec<Block<N>>>;

    /// Returns the solution for the given solution ID, or `None` if no such solution exists.
    fn get_solution(&self, solution_id: &SolutionID<N>) -> Result<Option<Solution<N>>>;

    /// Returns the unconfirmed transaction for the given transaction ID, or `None` if no such transaction exists.
    fn get_unconfirmed_transaction(&self, transaction_id: N::TransactionID) -> Result<Option<Transaction<N>>>;

    /// Returns the batch certificate for the given batch certificate ID.
    fn get_batch_certificate(&self, certificate_id: &Field<N>) -> Result<BatchCertificate<N>>;

    /// Returns the current committee.
    fn current_committee(&self) -> Result<Committee<N>>;

    /// Returns the committee for the given round.
    fn get_committee_for_round(&self, round: u64) -> Result<Committee<N>>;

    /// Returns the committee lookback for the given round.
    fn get_committee_lookback_for_round(&self, round: u64) -> Result<Committee<N>>;

    /// Returns the deterministic hotswapped dev committee for the given round, if active.
    #[cfg(feature = "dev-network")]
    fn dev_committee_for_round(&self, _round: u64) -> Result<Option<Committee<N>>> {
        Ok(None)
    }

    /// Returns `true` if the ledger contains the given certificate ID.
    fn contains_certificate(&self, certificate_id: &Field<N>) -> Result<bool>;

    /// Returns `true` if the ledger contains the given transmission ID.
    fn contains_transmission(&self, transmission_id: &TransmissionID<N>) -> Result<bool>;

    /// Ensures that the given transmission is not a fee and matches the given transmission ID.
    fn ensure_transmission_is_well_formed(
        &self,
        transmission_id: TransmissionID<N>,
        transmission: &mut Transmission<N>,
    ) -> Result<()>;

    /// Checks the given solution is well-formed.
    async fn check_solution_basic(&self, solution_id: SolutionID<N>, solution: Data<Solution<N>>) -> Result<()>;

    /// Checks the given transaction is well-formed and unique.
    async fn check_transaction_basic(
        &self,
        transaction_id: N::TransactionID,
        transaction: Transaction<N>,
    ) -> Result<()>;

    /// Start a transactional update to the ledger.
    ///
    /// Note: This may block and should only be called from a blocking task.
    fn begin_ledger_update<'a>(&'a self) -> Result<Box<dyn LedgerUpdateService<N> + 'a>, BeginLedgerUpdateError>;

    /// Checks that the subDAG in a given block is valid, but does not fully verify the block.
    fn check_block_subdag(
        &self,
        block: Block<N>,
        prefix: &[PendingBlock<N>],
    ) -> std::result::Result<PendingBlock<N>, CheckBlockError<N>>;

    /// Returns the spent cost for a transaction in microcredits.
    fn transaction_spend_in_microcredits(
        &self,
        transaction: &Transaction<N>,
        consensus_version: ConsensusVersion,
    ) -> Result<u64>;

    fn is_stopped(&self) -> bool;
}
