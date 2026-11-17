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

use crate::{
    BeginLedgerUpdateError,
    LedgerService,
    LedgerUpdateService,
    deserialize_solution_strict,
    deserialize_transaction_strict,
    fmt_id,
    spawn_blocking,
};

use snarkos_utilities::Stoppable;
#[cfg(feature = "test_network")]
use snarkos_utilities::{DevHotswapConfig, NodeDataDir};

#[cfg(feature = "test_network")]
use snarkvm::ledger::DevCommitteeOptions;
use snarkvm::{
    ledger::{
        Block,
        CheckBlockError,
        Ledger,
        PendingBlock,
        Transaction,
        committee::Committee,
        narwhal::{BatchCertificate, Data, Subdag, Transmission, TransmissionID},
        puzzle::{Solution, SolutionID},
        store::ConsensusStorage,
    },
    prelude::{
        Address,
        ConsensusVersion,
        Field,
        Network,
        Result,
        bail,
        cfg_into_iter,
        consensus_config_value_by_version,
        transaction_compute_spend_in_microcredits,
    },
};

use anyhow::ensure;
use indexmap::IndexMap;
#[cfg(feature = "locktick")]
use locktick::{
    LockGuard,
    parking_lot::{Mutex, RwLock},
};
use parking_lot::MutexGuard;
#[cfg(not(feature = "locktick"))]
use parking_lot::{Mutex, RwLock};
#[cfg(not(feature = "serial"))]
use rayon::prelude::*;

use std::{fmt, ops::Range, sync::Arc};

/// A core ledger service.
#[allow(clippy::type_complexity)]
pub struct CoreLedgerService<N: Network, C: ConsensusStorage<N>> {
    ledger: Ledger<N, C>,
    latest_leader: Arc<RwLock<Option<(u64, Address<N>)>>>,
    stoppable: Arc<dyn Stoppable>,
    update_lock: Arc<Mutex<()>>,
}

/// A transactional update to the ledger.
#[cfg(feature = "ledger-write")]
pub struct LedgerUpdate<'a, N: Network, C: ConsensusStorage<N>> {
    ledger: Ledger<N, C>,
    #[cfg(feature = "locktick")]
    _lock: LockGuard<MutexGuard<'a, ()>>,
    #[cfg(not(feature = "locktick"))]
    _lock: MutexGuard<'a, ()>,
}

#[cfg(feature = "ledger-write")]
impl<'a, N: Network, C: ConsensusStorage<N>> LedgerUpdateService<N> for LedgerUpdate<'a, N, C> {
    fn check_block_subdag(
        &self,
        block: Block<N>,
        prefix: &[PendingBlock<N>],
    ) -> Result<PendingBlock<N>, CheckBlockError<N>> {
        self.ledger.check_block_subdag(block, prefix)
    }

    fn check_block_content(&self, block: PendingBlock<N>) -> Result<Block<N>, CheckBlockError<N>> {
        self.ledger.check_block_content(block, &mut rand::rng())
    }

    /// Checks the given block is valid next block.
    fn check_next_block(&self, block: Block<N>) -> Result<Block<N>, CheckBlockError<N>> {
        let pending = self.ledger.check_block_subdag(block, &[])?;
        self.check_block_content(pending)
    }

    /// Returns a candidate for the next block in the ledger, using a committed subdag and its transmissions.
    fn prepare_advance_to_next_quorum_block(
        &self,
        subdag: Subdag<N>,
        transmissions: IndexMap<TransmissionID<N>, Transmission<N>>,
    ) -> Result<Block<N>, CheckBlockError<N>> {
        self.ledger.prepare_advance_to_next_quorum_block(subdag, transmissions, &mut rand::rng())
    }

    /// Adds the given block as the next block in the ledger.
    fn advance_to_next_block(&self, block: &Block<N>) -> Result<()> {
        // Advance to the next block.
        self.ledger.advance_to_next_block(block)?;
        // Update BFT metrics.
        #[cfg(feature = "metrics")]
        {
            let num_sol = block.solutions().len();
            let num_tx = block.transactions().len();

            metrics::gauge(metrics::bft::HEIGHT, block.height() as f64);
            metrics::gauge(metrics::bft::LAST_COMMITTED_ROUND, block.round() as f64);
            metrics::increment_gauge(metrics::blocks::SOLUTIONS, num_sol as f64);
            metrics::increment_gauge(metrics::blocks::TRANSACTIONS, num_tx as f64);
            metrics::update_block_metrics(block);
        }

        tracing::info!("Advanced to block {} at round {} - {}\n", block.height(), block.round(), block.hash());
        Ok(())
    }
}

impl<N: Network, C: ConsensusStorage<N>> CoreLedgerService<N, C> {
    /// Initializes a new core ledger service.
    pub fn new(ledger: Ledger<N, C>, stoppable: Arc<dyn Stoppable>) -> Self {
        // Initialize the block height and consensus version metrics.
        #[cfg(feature = "metrics")]
        {
            let height = ledger.latest_block().height();
            metrics::gauge(metrics::bft::HEIGHT, height as f64);
            metrics::gauge(
                metrics::consensus::VERSION,
                N::CONSENSUS_VERSION(height).map_or(0.0, |version| version as u16 as f64),
            );
        }

        Self { ledger, latest_leader: Default::default(), stoppable, update_lock: Default::default() }
    }

    /// Returns the deterministic dev committee for rounds at or after the hotswap start,
    /// reading from the snarkVM `Ledger` (which owns the committee under `--dev-on-prod`).
    #[cfg(feature = "test_network")]
    fn dev_committee_for_round(&self, round: u64) -> Result<Option<Committee<N>>> {
        let Some(dev_committee) = self.ledger.dev_committee() else {
            return Ok(None);
        };
        if round < dev_committee.starting_round() {
            return Ok(None);
        }
        Ok(Some(dev_committee.clone()))
    }

    /// Returns the committee for the given round, or `None` if the ledger has no committee for it.
    ///
    /// A round above the starting round of `Ledger::latest_committee` yields `None` without reading storage.
    /// The committee store indexes rounds only up to the starting round of the last committee it stored,
    /// and `Ledger::latest_committee` is a copy of that committee, taken after each block is stored.
    ///
    /// Hazard: between a block being written to storage and `Ledger::advance_to_next_block` refreshing
    /// that copy, this returns `None` for the rounds the block added, as it would have just before the write.
    /// A caller that requires those rounds as soon as `Ledger::latest_round` reports the new block
    /// must use `Ledger::get_committee_for_round` instead.
    fn try_get_committee_for_round(&self, round: u64) -> Result<Option<Committee<N>>> {
        // The dev committee applies to rounds the ledger has not reached.
        #[cfg(feature = "test_network")]
        if self.ledger.dev_committee().is_some() {
            return self.ledger.get_committee_for_round(round);
        }

        if self.ledger.latest_committee().is_ok_and(|committee| round > committee.starting_round()) {
            return Ok(None);
        }
        self.ledger.get_committee_for_round(round)
    }
}

/// Resolves the dev committee's starting round prior to loading the ledger.
///
/// Resolution order (highest priority first):
/// 1. The round persisted to `<node_data_dir>/dev-committee-state` from a previous run.
/// 2. `None` — defer to snarkVM, which falls back to the latest block's round at load time
///    and is then persisted by [`persist_dev_committee_start_round_if_unwritten`].
#[cfg(feature = "test_network")]
pub fn prepare_dev_committee_options(
    node_data_dir: &NodeDataDir,
    config: DevHotswapConfig,
) -> Result<DevCommitteeOptions> {
    let path = node_data_dir.dev_committee_state_path();
    let path_str = path.display();

    let start_round = if path.exists() {
        let contents = std::fs::read_to_string(&path)
            .map_err(|err| anyhow::anyhow!("Failed to read dev committee state from {path_str} - {err}"))?;
        let r = contents.trim().parse::<u64>().map_err(|err| {
            anyhow::anyhow!("Failed to parse dev committee state at {path_str} ({contents}) - {err}",)
        })?;
        tracing::info!("Loaded the dev committee starting round {r} from {path_str}");
        Some(r)
    } else {
        None
    };

    Ok(DevCommitteeOptions {
        start_round,
        dev_num_validators: config.dev_num_validators,
        seed: snarkos_utilities::DEVELOPMENT_MODE_RNG_SEED,
    })
}

/// Persists the given `start_round` to `<node_data_dir>/dev-committee-state` if the file
/// doesn't already exist. Called after [`Ledger::load_with_dev_committee`] so that the
/// round chosen by snarkVM (its `latest_round()` default) survives restarts.
#[cfg(feature = "test_network")]
pub fn persist_dev_committee_start_round_if_unwritten(node_data_dir: &NodeDataDir, start_round: u64) -> Result<()> {
    let path = node_data_dir.dev_committee_state_path();
    if !path.exists() {
        write_dev_committee_start_round(&path, start_round)?;
        tracing::info!("Persisted the dev committee starting round {start_round} to {}", path.display());
    }
    Ok(())
}

/// Writes the given `start_round` to the dev committee state file at `path`, creating the
/// parent directory if needed.
#[cfg(feature = "test_network")]
fn write_dev_committee_start_round(path: &std::path::Path, start_round: u64) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.exists()
    {
        std::fs::create_dir_all(parent).map_err(|err| {
            anyhow::anyhow!("Failed to create dev committee state directory {} - {err}", parent.display())
        })?;
    }
    std::fs::write(path, start_round.to_string())
        .map_err(|err| anyhow::anyhow!("Failed to write dev committee state to {} - {err}", path.display()))?;
    Ok(())
}

impl<N: Network, C: ConsensusStorage<N>> fmt::Debug for CoreLedgerService<N, C> {
    /// Implements a custom `fmt::Debug` for `CoreLedgerService`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoreLedgerService").field("current_committee", &self.current_committee()).finish()
    }
}

#[async_trait]
impl<N: Network, C: ConsensusStorage<N>> LedgerService<N> for CoreLedgerService<N, C> {
    /// Returns the latest round in the ledger.
    fn latest_round(&self) -> u64 {
        self.ledger.latest_round()
    }

    /// Returns the latest block height in the ledger.
    fn latest_block_height(&self) -> u32 {
        self.ledger.latest_height()
    }

    /// Returns the latest block in the ledger.
    fn latest_block(&self) -> Block<N> {
        self.ledger.latest_block()
    }

    /// Returns the latest restrictions ID in the ledger.
    fn latest_restrictions_id(&self) -> Field<N> {
        self.ledger.vm().restrictions().restrictions_id()
    }

    /// Returns the latest cached leader and its associated round.
    fn latest_leader(&self) -> Option<(u64, Address<N>)> {
        *self.latest_leader.read()
    }

    /// Updates the latest cached leader and its associated round.
    fn update_latest_leader(&self, round: u64, leader: Address<N>) {
        *self.latest_leader.write() = Some((round, leader));
    }

    /// Returns `true` if the given block height exists in the ledger.
    fn contains_block_height(&self, height: u32) -> bool {
        self.ledger.contains_block_height(height).unwrap_or(false)
    }

    /// Returns the block height for the given block hash, if it exists.
    fn get_block_height(&self, hash: &N::BlockHash) -> Result<u32> {
        self.ledger.get_height(hash)
    }

    /// Returns the block hash for the given block height, if it exists.
    fn get_block_hash(&self, height: u32) -> Result<N::BlockHash> {
        self.ledger.get_hash(height)
    }

    /// Returns the block round for the given block height, if it exists.
    fn get_block_round(&self, height: u32) -> Result<u64> {
        self.ledger.get_block(height).map(|block| block.round())
    }

    /// Returns the block for the given block height.
    fn get_block(&self, height: u32) -> Result<Block<N>> {
        self.ledger.get_block(height)
    }

    /// Returns the blocks in the given block range.
    /// The range is inclusive of the start and exclusive of the end.
    fn get_blocks(&self, heights: Range<u32>) -> Result<Vec<Block<N>>> {
        cfg_into_iter!(heights).map(|height| self.get_block(height)).collect()
    }

    /// Returns the solution for the given solution ID.
    fn get_solution(&self, solution_id: &SolutionID<N>) -> Result<Option<Solution<N>>> {
        self.ledger.try_get_solution(solution_id)
    }

    /// Returns the unconfirmed transaction for the given transaction ID.
    fn get_unconfirmed_transaction(&self, transaction_id: N::TransactionID) -> Result<Option<Transaction<N>>> {
        self.ledger.try_get_unconfirmed_transaction(&transaction_id)
    }

    /// Returns the batch certificate for the given batch certificate ID.
    fn get_batch_certificate(&self, certificate_id: &Field<N>) -> Result<BatchCertificate<N>> {
        match self.ledger.get_batch_certificate(certificate_id) {
            Ok(Some(certificate)) => Ok(certificate),
            Ok(None) => bail!("No batch certificate found for certificate ID {certificate_id} in the ledger"),
            Err(error) => Err(error),
        }
    }

    /// Returns the current committee.
    fn current_committee(&self) -> Result<Committee<N>> {
        #[cfg(feature = "test_network")]
        {
            if let Some(dev_committee) = self.ledger.dev_committee() {
                return Ok(dev_committee.clone());
            }
        }

        self.ledger.latest_committee()
    }

    /// Returns the committee for the given round.
    fn get_committee_for_round(&self, round: u64) -> Result<Committee<N>> {
        match self.try_get_committee_for_round(round)? {
            Some(committee) => Ok(committee),
            None => bail!("No committee found for round {round} in the ledger"),
        }
    }

    /// Returns the committee lookback for the given round.
    fn get_committee_lookback_for_round(&self, round: u64) -> Result<Committee<N>> {
        #[cfg(feature = "test_network")]
        {
            if let Some(dev_committee) = self.dev_committee_for_round(round)? {
                return Ok(dev_committee);
            }
        }

        // Get the round number for the previous committee. Note, we subtract 2 from odd rounds,
        // because committees are updated in even rounds.
        let previous_round = match round.is_multiple_of(2) {
            true => round.saturating_sub(1),
            false => round.saturating_sub(2),
        };

        // Get the committee lookback round.
        let committee_lookback_round = previous_round.saturating_sub(Committee::<N>::COMMITTEE_LOOKBACK_RANGE);

        // Retrieve the committee for the committee lookback round.
        self.get_committee_for_round(committee_lookback_round)
    }

    /// Returns the deterministic hotswapped dev committee for the given round, if active.
    #[cfg(feature = "test_network")]
    fn dev_committee_for_round(&self, round: u64) -> Result<Option<Committee<N>>> {
        CoreLedgerService::dev_committee_for_round(self, round)
    }

    /// Returns `true` if the ledger contains the given certificate ID in block history.
    fn contains_certificate(&self, certificate_id: &Field<N>) -> Result<bool> {
        self.ledger.contains_certificate(certificate_id)
    }

    /// Returns `true` if the transmission exists in the ledger.
    fn contains_transmission(&self, transmission_id: &TransmissionID<N>) -> Result<bool> {
        match transmission_id {
            TransmissionID::Ratification => Ok(false),
            TransmissionID::Solution(solution_id, _) => self.ledger.contains_solution_id(solution_id),
            TransmissionID::Transaction(transaction_id, _) => self.ledger.contains_transaction_id(transaction_id),
        }
    }

    /// Ensures that the given transmission is not a fee and matches the given transmission ID.
    fn ensure_transmission_is_well_formed(
        &self,
        transmission_id: TransmissionID<N>,
        transmission: &mut Transmission<N>,
    ) -> Result<()> {
        match (transmission_id, transmission) {
            (TransmissionID::Ratification, Transmission::Ratification) => {
                bail!("Ratification transmissions are currently not supported.")
            }
            (
                TransmissionID::Transaction(expected_transaction_id, expected_checksum),
                Transmission::Transaction(transaction_data),
            ) => {
                // Deserialize the transaction. If the transaction exceeds the maximum size, then return an error.
                let transaction = deserialize_transaction_strict(transaction_data.clone())?;
                // Ensure the transaction ID matches the expected transaction ID.
                if transaction.id() != expected_transaction_id {
                    bail!(
                        "Received mismatching transaction ID - expected {}, found {}",
                        fmt_id(expected_transaction_id),
                        fmt_id(transaction.id()),
                    );
                }

                // Ensure the transmission checksum matches the expected checksum.
                let checksum = transaction_data.to_checksum::<N>()?;
                if checksum != expected_checksum {
                    bail!(
                        "Received mismatching checksum for transaction {} - expected {expected_checksum} but found {checksum}",
                        fmt_id(expected_transaction_id)
                    );
                }

                // Ensure the transaction is not a fee transaction.
                if transaction.is_fee() {
                    bail!("Received a fee transaction in a transmission");
                }

                // Update the transmission with the deserialized transaction.
                *transaction_data = Data::Object(transaction);
            }
            (
                TransmissionID::Solution(expected_solution_id, expected_checksum),
                Transmission::Solution(solution_data),
            ) => {
                match deserialize_solution_strict(solution_data.clone()) {
                    Ok(solution) => {
                        if solution.id() != expected_solution_id {
                            bail!(
                                "Received mismatching solution ID - expected {}, found {}",
                                fmt_id(expected_solution_id),
                                fmt_id(solution.id()),
                            );
                        }

                        // Ensure the transmission checksum matches the expected checksum.
                        let checksum = solution_data.to_checksum::<N>()?;
                        if checksum != expected_checksum {
                            bail!(
                                "Received mismatching checksum for solution {} - expected {expected_checksum} but found {checksum}",
                                fmt_id(expected_solution_id)
                            );
                        }

                        // Update the transmission with the deserialized solution.
                        *solution_data = Data::Object(solution);
                    }
                    Err(err) => {
                        bail!("Failed to deserialize solution: {err}");
                    }
                }
            }
            _ => {
                bail!("Mismatching `(transmission_id, transmission)` pair");
            }
        }

        Ok(())
    }

    /// Checks the given solution is well-formed.
    async fn check_solution_basic(&self, solution_id: SolutionID<N>, solution: Data<Solution<N>>) -> Result<()> {
        // Deserialize the solution.
        let solution = spawn_blocking!(deserialize_solution_strict(solution))?;
        // Ensure the solution ID matches in the solution.
        if solution_id != solution.id() {
            bail!("Invalid solution - expected {solution_id}, found {}", solution.id());
        }

        // Check if the prover has reached their solution limit.
        // While snarkVM will ultimately abort any excess solutions for safety, performing this check
        // here prevents the to-be aborted solutions from propagating through the network.
        let prover_address = solution.address();
        if self.ledger.is_solution_limit_reached(&prover_address, 0) {
            bail!(
                "Invalid Solution '{}' - Prover '{prover_address}' has reached their solution limit for the current epoch",
                fmt_id(solution.id())
            );
        }
        // Compute the current epoch hash.
        let epoch_hash = self.ledger.latest_epoch_hash()?;
        // Retrieve the current proof target.
        let proof_target = self.ledger.latest_proof_target();

        // Ensure that the solution is valid for the given epoch.
        let puzzle = self.ledger.puzzle().clone();
        match spawn_blocking!(puzzle.check_solution(&solution, epoch_hash, proof_target)) {
            Ok(()) => Ok(()),
            Err(e) => bail!("Invalid solution '{}' for the current epoch - {e}", fmt_id(solution_id)),
        }
    }

    /// Checks the given transaction is well-formed and unique.
    async fn check_transaction_basic(
        &self,
        transaction_id: N::TransactionID,
        transaction: Transaction<N>,
    ) -> Result<()> {
        // Ensure the transaction ID matches in the transaction.
        if transaction_id != transaction.id() {
            bail!("Invalid transaction - expected {transaction_id}, found {}", transaction.id());
        }
        // Check if the transmission is a fee transaction.
        if transaction.is_fee() {
            bail!("Invalid transaction - 'Transaction::fee' type is not valid at this stage ({})", transaction.id());
        }
        // Check the transaction is well-formed.
        let ledger = self.ledger.clone();
        spawn_blocking!(ledger.check_transaction_basic(&transaction, None, &mut rand::rng()))
    }

    fn check_block_subdag(
        &self,
        block: Block<N>,
        prefix: &[PendingBlock<N>],
    ) -> std::result::Result<PendingBlock<N>, CheckBlockError<N>> {
        self.ledger.check_block_subdag(block, prefix)
    }

    /// Begins a ledger update.
    ///
    /// # Returns
    /// - `Ok(Some(LedgerUpdate<N, C>))` if the ledger update was successfully started.
    /// - `Ok(None)` if the node is stopped.
    /// - `Err(anyhow::Error)` if we failed to acquire the update lock.
    fn begin_ledger_update<'a>(&'a self) -> Result<Box<dyn LedgerUpdateService<N> + 'a>, BeginLedgerUpdateError> {
        if self.stoppable.is_stopped() {
            return Err(BeginLedgerUpdateError::ShuttingDown);
        }

        Ok(Box::new(LedgerUpdate { ledger: self.ledger.clone(), _lock: self.update_lock.lock() }))
    }

    /// Returns the spend for a transaction in microcredits.
    /// This is used to limit the amount of compute in the block generation hot
    /// path. This does NOT represent the full costs which a user has to pay.
    fn transaction_spend_in_microcredits(
        &self,
        transaction: &Transaction<N>,
        consensus_version: ConsensusVersion,
    ) -> Result<u64> {
        let transaction_spend_limit =
            consensus_config_value_by_version!(N, TRANSACTION_SPEND_LIMIT, consensus_version).unwrap();
        let id = transaction.id();

        let compute_spend =
            transaction_compute_spend_in_microcredits(self.ledger.vm().process(), transaction, consensus_version)?;
        ensure!(
            compute_spend <= transaction_spend_limit,
            "Transaction '{id}' exceeds the transaction spend limit with compute_spend: '{compute_spend}'"
        );

        Ok(compute_spend)
    }

    fn is_stopped(&self) -> bool {
        self.stoppable.is_stopped()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use snarkos_utilities::SimpleStoppable;
    use snarkvm::{
        console::{account::PrivateKey, network::MainnetV0},
        ledger::store::{ConsensusStore, StorageMode, helpers::memory::ConsensusMemory},
        prelude::{TestRng, VM},
    };

    type CurrentNetwork = MainnetV0;
    type CurrentLedger = Ledger<CurrentNetwork, ConsensusMemory<CurrentNetwork>>;

    /// Returns a ledger holding the genesis block and `num_blocks` empty beacon blocks.
    fn sample_ledger(num_blocks: u32, rng: &mut TestRng) -> CurrentLedger {
        let private_key = PrivateKey::<CurrentNetwork>::new(rng).unwrap();
        let store = ConsensusStore::<_, ConsensusMemory<_>>::open(StorageMode::new_test(None)).unwrap();
        let genesis = VM::from(store).unwrap().genesis_beacon(&private_key, rng).unwrap();
        let ledger = CurrentLedger::load(genesis, StorageMode::new_test(None)).unwrap();
        for _ in 0..num_blocks {
            let block = ledger.prepare_advance_to_next_beacon_block(&private_key, vec![], vec![], vec![], rng).unwrap();
            ledger.advance_to_next_block(&block).unwrap();
        }
        ledger
    }

    #[test]
    fn test_get_committee_for_round_up_to_latest_committee() {
        let rng = &mut TestRng::default();
        let ledger = sample_ledger(2, rng);
        let service = CoreLedgerService::new(ledger.clone(), SimpleStoppable::new());

        let latest_round = ledger.latest_round();
        assert!(latest_round > 0);
        assert_eq!(ledger.latest_committee().unwrap().starting_round(), latest_round);

        for round in 0..=latest_round {
            let expected = ledger.get_committee_for_round(round).unwrap().unwrap();
            assert_eq!(service.get_committee_for_round(round).unwrap(), expected);
        }
        assert_eq!(service.get_committee_for_round(latest_round).unwrap(), ledger.latest_committee().unwrap());

        let above = latest_round + 1;
        assert!(ledger.get_committee_for_round(above).unwrap().is_none());
        let error = service.get_committee_for_round(above).unwrap_err();
        assert_eq!(error.to_string(), format!("No committee found for round {above} in the ledger"));
    }

    #[test]
    fn test_get_committee_lookback_for_round() {
        let rng = &mut TestRng::default();
        let ledger = sample_ledger(1, rng);
        let service = CoreLedgerService::new(ledger.clone(), SimpleStoppable::new());

        let lookback_range = Committee::<CurrentNetwork>::COMMITTEE_LOOKBACK_RANGE;
        // Lookback rounds are odd, so the latest round must be odd to be one.
        let latest_round = ledger.latest_round();
        assert_eq!(latest_round % 2, 1);

        // The highest round whose lookback round is the latest round.
        let at_bound = latest_round + lookback_range + 2;
        assert_eq!(service.get_committee_lookback_for_round(at_bound).unwrap(), ledger.latest_committee().unwrap());

        let error = service.get_committee_lookback_for_round(at_bound + 1).unwrap_err();
        assert_eq!(error.to_string(), format!("No committee found for round {} in the ledger", latest_round + 2));
    }

    #[test]
    fn test_get_committee_for_round_above_latest_committee_skips_storage() {
        let rng = &mut TestRng::default();
        let ledger = sample_ledger(1, rng);
        let service = CoreLedgerService::new(ledger.clone(), SimpleStoppable::new());

        // Store a committee that `Ledger::latest_committee` does not reflect.
        let latest_committee = ledger.latest_committee().unwrap();
        let stored_round = latest_committee.starting_round() + 2;
        let stored_committee = Committee::new(stored_round, latest_committee.members().clone()).unwrap();
        let committee_store = ledger.vm().finalize_store().committee_store();
        committee_store.insert(ledger.latest_height() + 1, stored_committee.clone()).unwrap();

        // The stored committee is reachable through storage, so a lookup that read storage would return it.
        assert_eq!(committee_store.get_committee_for_round(stored_round).unwrap(), Some(stored_committee.clone()));
        assert_eq!(ledger.get_committee_for_round(stored_round).unwrap(), Some(stored_committee));

        for round in [stored_round - 1, stored_round] {
            let error = service.get_committee_for_round(round).unwrap_err();
            assert_eq!(error.to_string(), format!("No committee found for round {round} in the ledger"));
        }
    }
}
