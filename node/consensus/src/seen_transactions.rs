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

use lru::LruCache;
use snarkvm::{prelude::Network, synthesizer::process::deployment_freeze::FreezeWindow};
use std::num::NonZeroUsize;

/// The recently-seen unconfirmed transactions, and whether each one is a deployment.
pub struct SeenTransactions<N: Network> {
    seen: LruCache<N::TransactionID, bool>,
    /// The freeze for which the deployments were last forgotten.
    forgotten_for: Option<FreezeWindow>,
}

impl<N: Network> SeenTransactions<N> {
    /// Initializes an empty cache with the given capacity.
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self { seen: LruCache::new(capacity), forgotten_for: None }
    }

    /// Records the transaction as seen, and returns `true` if it was already seen.
    pub fn insert(&mut self, transaction_id: N::TransactionID, is_deploy: bool) -> bool {
        self.seen.put(transaction_id, is_deploy).is_some()
    }

    /// Forgets every deployment seen so far, once per freeze.
    ///
    /// A freeze drops the queued deployments, both here and in the BFT's ready queue. Forgetting them
    /// lets their owners send the same transactions again after the freeze, instead of having them
    /// ignored as recently seen. A freeze refuses new deployments before they are recorded, so after
    /// the first call for a freeze there is nothing new to forget.
    pub fn forget_deployments(&mut self, freeze: FreezeWindow) {
        if self.forgotten_for == Some(freeze) {
            return;
        }
        self.forgotten_for = Some(freeze);
        let deployments: Vec<_> =
            self.seen.iter().filter(|(_, is_deploy)| **is_deploy).map(|(transaction_id, _)| *transaction_id).collect();
        for transaction_id in deployments {
            self.seen.pop(&transaction_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transactions_queue::{QueueInsertion, TransactionsQueue};
    use snarkvm::{
        ledger::test_helpers::{sample_deployment_transaction, sample_execution_transaction_with_fee},
        prelude::{MainnetV0, TestRng, U64, Zero},
    };

    type CurrentNetwork = MainnetV0;

    /// A deployment queued before a freeze and dropped by it is queued again when resent after the
    /// thaw. An execution seen before the freeze is still recognized as seen.
    #[test]
    fn test_resend_after_freeze_drops_a_deployment() {
        let rng = &mut TestRng::default();
        let deployment = sample_deployment_transaction(2, 0, false, true, rng);
        let execution = sample_execution_transaction_with_fee(false, rng, 0);
        let mut seen = SeenTransactions::<CurrentNetwork>::new(NonZeroUsize::new(16).unwrap());
        let mut queue = TransactionsQueue::<CurrentNetwork>::default();

        // Before the freeze: both are seen and queued.
        assert!(!seen.insert(deployment.id(), true));
        assert!(!seen.insert(execution.id(), false));
        assert_eq!(queue.insert(deployment.id(), deployment.clone(), U64::zero()).unwrap(), QueueInsertion::Inserted);

        // A freeze passes: the queued deployment is dropped and forgotten.
        let freeze = FreezeWindow { start: 20, end: 30 };
        assert_eq!(queue.deployments.clear(), 1);
        seen.forget_deployments(freeze);

        // After the thaw, the same deployment is not recently seen, and is queued again.
        assert!(!seen.insert(deployment.id(), true));
        assert_eq!(queue.insert(deployment.id(), deployment.clone(), U64::zero()).unwrap(), QueueInsertion::Inserted);
        // The execution is still recently seen.
        assert!(seen.insert(execution.id(), false));

        // The same freeze does not forget again; a new freeze does.
        seen.forget_deployments(freeze);
        assert!(seen.insert(deployment.id(), true));
        seen.forget_deployments(FreezeWindow { start: 40, end: 50 });
        assert!(!seen.insert(deployment.id(), true));
    }
}
