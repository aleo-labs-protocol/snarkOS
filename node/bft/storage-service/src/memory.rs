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

use crate::StorageService;
use snarkvm::{
    ledger::narwhal::{Transmission, TransmissionID},
    prelude::{Field, Network},
};

use indexmap::{IndexMap, IndexSet, indexset, map::Entry};
#[cfg(feature = "locktick")]
use locktick::parking_lot::RwLock;
#[cfg(not(feature = "locktick"))]
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use tracing::error;

/// A BFT in-memory storage service.
#[derive(Debug)]
pub struct BFTMemoryService<N: Network> {
    /// The map of `transmission ID` to `(transmission, certificate IDs)` entries.
    transmissions: RwLock<IndexMap<TransmissionID<N>, (Transmission<N>, IndexSet<Field<N>>)>>,
    /// The map of `aborted transmission ID` to `certificate IDs` entries.
    aborted_transmission_ids: RwLock<IndexMap<TransmissionID<N>, IndexSet<Field<N>>>>,
}

impl<N: Network> Default for BFTMemoryService<N> {
    /// Initializes a new BFT in-memory storage service.
    fn default() -> Self {
        Self::new()
    }
}

impl<N: Network> BFTMemoryService<N> {
    /// Initializes a new BFT in-memory storage service.
    pub fn new() -> Self {
        Self { transmissions: Default::default(), aborted_transmission_ids: Default::default() }
    }
}

impl<N: Network> StorageService<N> for BFTMemoryService<N> {
    /// Returns `true` if the storage holds the transmission for the specified `transmission ID`.
    fn contains_retrievable_transmission(&self, transmission_id: TransmissionID<N>) -> bool {
        self.transmissions.read().contains_key(&transmission_id)
    }

    /// Returns `true` if the specified `transmission ID` is recorded as aborted.
    fn contains_aborted_transmission(&self, transmission_id: TransmissionID<N>) -> bool {
        self.aborted_transmission_ids.read().contains_key(&transmission_id)
    }

    /// Returns the transmission for the given `transmission ID`.
    /// If the transmission does not exist in storage, `None` is returned.
    fn get_transmission(&self, transmission_id: TransmissionID<N>) -> Option<Transmission<N>> {
        // Get the transmission.
        self.transmissions.read().get(&transmission_id).map(|(transmission, _)| transmission).cloned()
    }

    /// Inserts the given certificate ID for each of the transmission IDs, using the missing transmissions map, into storage.
    fn insert_transmissions(
        &self,
        certificate_id: Field<N>,
        transmission_ids: IndexSet<TransmissionID<N>>,
        mut aborted_transmission_ids: HashSet<TransmissionID<N>>,
        mut missing_transmissions: HashMap<TransmissionID<N>, Transmission<N>>,
    ) {
        // Acquire the transmissions write lock.
        let mut transmissions = self.transmissions.write();
        // Acquire the aborted transmission IDs write lock.
        let mut aborted_transmission_ids_lock = self.aborted_transmission_ids.write();
        // Inserts the following:
        //   - Inserts **only the missing** transmissions from storage.
        //   - Inserts the certificate ID into the corresponding set for **all** transmissions.
        'outer: for transmission_id in transmission_ids {
            // Retrieve the transmission entry.
            match transmissions.entry(transmission_id) {
                Entry::Occupied(mut occupied_entry) => {
                    let (_, certificate_ids) = occupied_entry.get_mut();
                    // Insert the certificate ID into the set.
                    certificate_ids.insert(certificate_id);
                }
                Entry::Vacant(vacant_entry) => {
                    // Retrieve the missing transmission.
                    let Some(transmission) = missing_transmissions.remove(&transmission_id) else {
                        // note: `self.contains_retrievable_transmission` would deadlock here; it
                        // read-locks `self.transmissions`, which is write-locked above.
                        if !aborted_transmission_ids.contains(&transmission_id) {
                            // An ID that storage already knows as aborted needs no bytes to be
                            // inserted, but this certificate still has to be counted against the
                            // aborted entry: `remove_transmissions` decrements it either way.
                            if aborted_transmission_ids_lock.contains_key(&transmission_id) {
                                aborted_transmission_ids.insert(transmission_id);
                            } else {
                                error!("Failed to provide a missing transmission {transmission_id}");
                            }
                        }
                        continue 'outer;
                    };
                    // Prepare the set of certificate IDs.
                    let certificate_ids = indexset! { certificate_id };
                    // Insert the transmission and a new set with the certificate ID.
                    vacant_entry.insert((transmission, certificate_ids));
                }
            }
        }
        // Inserts the aborted transmission IDs.
        for aborted_transmission_id in aborted_transmission_ids {
            // Retrieve the transmission entry.
            match aborted_transmission_ids_lock.entry(aborted_transmission_id) {
                Entry::Occupied(mut occupied_entry) => {
                    let certificate_ids = occupied_entry.get_mut();
                    // Insert the certificate ID into the set.
                    certificate_ids.insert(certificate_id);
                }
                Entry::Vacant(vacant_entry) => {
                    // Prepare the set of certificate IDs.
                    let certificate_ids = indexset! { certificate_id };
                    // Insert the transmission and a new set with the certificate ID.
                    vacant_entry.insert(certificate_ids);
                }
            }
        }
    }

    /// Removes the certificate ID for the transmissions from storage.
    ///
    /// If the transmission no longer references any certificate IDs, the entry is removed from storage.
    fn remove_transmissions(&self, certificate_id: &Field<N>, transmission_ids: &IndexSet<TransmissionID<N>>) {
        // Acquire the transmissions write lock.
        let mut transmissions = self.transmissions.write();
        // Acquire the aborted transmission IDs write lock.
        let mut aborted_transmission_ids = self.aborted_transmission_ids.write();
        // If this is the last certificate ID for the transmission ID, remove the transmission.
        for transmission_id in transmission_ids {
            // Remove the certificate ID for the transmission ID, and determine if there are any more certificate IDs.
            match transmissions.entry(*transmission_id) {
                Entry::Occupied(mut occupied_entry) => {
                    let (_, certificate_ids) = occupied_entry.get_mut();
                    // Remove the certificate ID for the transmission ID.
                    certificate_ids.swap_remove(certificate_id);
                    // If there are no more certificate IDs for the transmission ID, remove the transmission.
                    if certificate_ids.is_empty() {
                        // Remove the entry for the transmission ID.
                        occupied_entry.shift_remove();
                    }
                }
                Entry::Vacant(_) => {}
            }
            // Remove the certificate ID for the aborted transmission ID, and determine if there are any more certificate IDs.
            match aborted_transmission_ids.entry(*transmission_id) {
                Entry::Occupied(mut occupied_entry) => {
                    let certificate_ids = occupied_entry.get_mut();
                    // Remove the certificate ID for the transmission ID.
                    certificate_ids.swap_remove(certificate_id);
                    // If there are no more certificate IDs for the transmission ID, remove the transmission.
                    if certificate_ids.is_empty() {
                        // Remove the entry for the transmission ID.
                        occupied_entry.shift_remove();
                    }
                }
                Entry::Vacant(_) => {}
            }
        }
    }

    /// Returns a HashMap over the `(transmission ID, (transmission, certificate IDs))` entries.
    #[cfg(any(test, feature = "test"))]
    fn as_hashmap(&self) -> HashMap<TransmissionID<N>, (Transmission<N>, IndexSet<Field<N>>)> {
        self.transmissions.read().clone().into_iter().collect()
    }
}

/// The expectations shared with every other service are asserted in `crate::tests`; what remains
/// here is specific to this one's locking.
#[cfg(test)]
mod tests {
    use super::*;

    use snarkvm::{
        console::network::MainnetV0,
        prelude::{Rng, TestRng, Uniform},
    };

    use std::{sync::mpsc, time::Duration};

    type CurrentNetwork = MainnetV0;

    fn sample_transmission_id(rng: &mut TestRng) -> TransmissionID<CurrentNetwork> {
        TransmissionID::Transaction(
            <CurrentNetwork as Network>::TransactionID::from(Field::rand(rng)),
            <CurrentNetwork as Network>::TransmissionChecksum::from(rng.random::<u128>()),
        )
    }

    /// `insert_transmissions` must return when a declared transmission is neither provided nor
    /// aborted, rather than deadlock on a re-entrant lock acquisition.
    ///
    /// That branch is reached through the gap between `find_missing_transmissions` and
    /// `insert_transmissions`, which take separate locks: a concurrent `remove_transmissions` can
    /// evict an entry the first call saw and therefore left out of `missing_transmissions`.
    ///
    /// The call runs on its own thread so a regression fails on the timeout rather than hanging
    /// the test binary.
    #[test]
    fn insert_transmissions_returns_when_a_transmission_is_neither_provided_nor_aborted() {
        let (sender, receiver) = mpsc::channel();

        std::thread::spawn(move || {
            let rng = &mut TestRng::default();
            let service = BFTMemoryService::<CurrentNetwork>::new();
            let transmission_id = sample_transmission_id(rng);

            // Declare a transmission that is neither provided nor aborted.
            service.insert_transmissions(
                Field::rand(rng),
                indexset![transmission_id],
                Default::default(),
                Default::default(),
            );

            let _ = sender.send(service.contains_retrievable_transmission(transmission_id));
        });

        let contains = receiver
            .recv_timeout(Duration::from_secs(10))
            .expect("insert_transmissions did not return: it re-entered its own lock");

        // The undeliverable transmission is skipped, not recorded as a phantom entry.
        assert!(!contains);
    }
}
