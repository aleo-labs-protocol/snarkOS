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

//! Node policy for the deployment freeze (`aleo_deployment_freeze.aleo`).
//!
//! From the block after a freeze passes until its `end`, a node turns every deployment away before it
//! checks the fee or the proofs, before it admits the deployment to its mempool, and before it
//! forwards it. Consensus rejects (and charges) a frozen deployment that still reaches a block; this
//! policy keeps honest deployments from getting that far.

use snarkvm::{
    ledger::{Transaction, narwhal::Data},
    prelude::{Network, Result, anyhow},
    synthesizer::process::deployment_freeze::FreezeWindow,
};

use std::sync::RwLock;

/// Returns the error for a deployment that arrives while deployments are frozen.
fn deployment_frozen_error(freeze: FreezeWindow) -> anyhow::Error {
    anyhow!(
        "Deployments are frozen by validator vote (aleo_deployment_freeze.aleo) until block {}; send the deployment again from that block",
        freeze.end
    )
}

/// Returns an error if a deployment arrives while `policy`, the freeze window that applies to the
/// next block, is set. Callers that may see other transactions check that it is a deployment first.
pub fn check_deployment_not_frozen(policy: Option<FreezeWindow>) -> Result<()> {
    match policy {
        Some(freeze) => Err(deployment_frozen_error(freeze)),
        None => Ok(()),
    }
}

/// Returns `true` if the transaction is a deployment, without deserializing a buffered transaction.
///
/// A serialized transaction starts with its version byte and then its variant byte, which is `0`
/// for a deployment.
pub fn is_deployment<N: Network>(transaction: &Data<Transaction<N>>) -> bool {
    match transaction {
        Data::Object(transaction) => transaction.is_deploy(),
        Data::Buffer(bytes) => bytes.get(1) == Some(&0),
    }
}

/// Caches the deployment freeze policy for the current tip, so it is read from the ledger at most once per block.
#[derive(Debug)]
pub struct DeploymentFreezeCache<N: Network> {
    /// The tip the cached value was read at, and the value.
    cached: RwLock<Option<(N::BlockHash, Option<FreezeWindow>)>>,
}

impl<N: Network> Default for DeploymentFreezeCache<N> {
    fn default() -> Self {
        Self { cached: RwLock::new(None) }
    }
}

impl<N: Network> DeploymentFreezeCache<N> {
    /// Returns the freeze window if deployments should be turned away for the block after `tip`.
    ///
    /// `read` is called only when the tip has changed since the last call.
    pub fn policy_at(&self, tip: N::BlockHash, read: impl FnOnce() -> Option<FreezeWindow>) -> Option<FreezeWindow> {
        if let Some((cached_tip, freeze)) = *self.cached.read().unwrap_or_else(|e| e.into_inner())
            && cached_tip == tip
        {
            return freeze;
        }
        let freeze = read();
        *self.cached.write().unwrap_or_else(|e| e.into_inner()) = Some((tip, freeze));
        freeze
    }

    /// Returns the freeze window if the ledger's committed freeze applies to the next block.
    ///
    /// A failed read is logged and treated as no freeze: this is node policy, and consensus still
    /// enforces the freeze.
    pub fn policy<C: snarkvm::ledger::store::ConsensusStorage<N>>(
        &self,
        ledger: &snarkvm::ledger::Ledger<N, C>,
    ) -> Option<FreezeWindow> {
        // Read the tip's hash and height together, so the policy is for the block after that tip.
        let (tip, height) = ledger.latest_hash_and_height();
        self.policy_at(tip, || {
            ledger.vm().deployment_freeze_policy_at(height.saturating_add(1)).unwrap_or_else(|error| {
                tracing::warn!("Failed to read the deployment freeze - {error}");
                None
            })
        })
    }

    /// Records `freeze` as the policy for the block after `tip`, as a ledger read at `tip` would.
    /// Tests use this to exercise the receive paths without voting a freeze.
    #[cfg(any(test, feature = "mock"))]
    pub fn set_policy_at(&self, tip: N::BlockHash, freeze: Option<FreezeWindow>) {
        *self.cached.write().unwrap_or_else(|e| e.into_inner()) = Some((tip, freeze));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use snarkvm::prelude::{MainnetV0, TestRng, ToBytes};

    type CurrentNetwork = MainnetV0;

    #[test]
    fn test_policy_reads_once_per_tip() {
        let cache = DeploymentFreezeCache::<CurrentNetwork>::default();
        let tip = Default::default();
        let freeze = FreezeWindow { start: 20, end: 30 };
        let mut reads = 0;
        assert_eq!(
            cache.policy_at(tip, || {
                reads += 1;
                Some(freeze)
            }),
            Some(freeze)
        );
        assert_eq!(cache.policy_at(tip, || panic!("the cached value must be used")), Some(freeze));
        assert_eq!(reads, 1);
        // A recorded value replaces the ledger read for its tip.
        cache.set_policy_at(tip, None);
        assert_eq!(cache.policy_at(tip, || panic!("the recorded value must be used")), None);
    }

    #[test]
    fn test_is_deployment() {
        let rng = &mut TestRng::default();
        let deployment = snarkvm::ledger::test_helpers::sample_deployment_transaction(2, 0, false, true, rng);
        let execution = snarkvm::ledger::test_helpers::sample_execution_transaction_with_fee(true, rng, 0);
        for (transaction, expected) in [(deployment, true), (execution, false)] {
            let buffer = Data::<Transaction<CurrentNetwork>>::Buffer(transaction.to_bytes_le().unwrap().into());
            assert_eq!(is_deployment(&buffer), expected);
            assert_eq!(is_deployment(&Data::Object(transaction)), expected);
        }
    }

    #[test]
    fn test_check_deployment_not_frozen() {
        let freeze = FreezeWindow { start: 20, end: 30 };
        // A deployment during a freeze is refused, and the error names the end height.
        let message = check_deployment_not_frozen(Some(freeze)).unwrap_err().to_string();
        assert!(message.contains("frozen") && message.contains("30"), "{message}");
        // A deployment outside a freeze is allowed.
        assert!(check_deployment_not_frozen(None).is_ok());
    }
}
