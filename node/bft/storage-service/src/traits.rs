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
    ledger::narwhal::{BatchHeader, Transmission, TransmissionID},
    prelude::{Field, Network, Result, bail},
};

use indexmap::IndexSet;
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
};

pub trait StorageService<N: Network>: Debug + Send + Sync {
    /// Returns `true` if the storage holds the transmission for the specified `transmission ID`,
    /// i.e. exactly when [`Self::get_transmission`] would return `Some`.
    ///
    /// This is `false` for an ID that storage knows only as aborted, which holds no transmission to
    /// hand back; see [`Self::contains_aborted_transmission`]. Prefer this over
    /// `get_transmission(id).is_some()`, which clones the transmission.
    ///
    /// Note: there is deliberately no single query for "storage knows this ID, either way". The two
    /// states answer different questions, and conflating them is what let a batch be certified
    /// while committing to a transmission that storage cannot hand back, so a caller that genuinely
    /// wants both has to name both.
    fn contains_retrievable_transmission(&self, transmission_id: TransmissionID<N>) -> bool;

    /// Returns `true` if the specified `transmission ID` is recorded as aborted.
    ///
    /// This and [`Self::contains_retrievable_transmission`] are **not** mutually exclusive: one
    /// certificate can record an ID as aborted while another provides the bytes for it, in which
    /// case storage holds both entries and both queries answer `true`. Each entry is reference
    /// counted against its own certificates, so removing one leaves the other in place. Use
    /// `contains_aborted_transmission(id) && !contains_retrievable_transmission(id)` to ask the
    /// narrower question of whether an ID is aborted *and* has nothing to hand back.
    fn contains_aborted_transmission(&self, transmission_id: TransmissionID<N>) -> bool;

    /// Returns the transmission for the given `transmission ID`.
    /// If the transmission ID does not exist in storage, `None` is returned.
    fn get_transmission(&self, transmission_id: TransmissionID<N>) -> Option<Transmission<N>>;

    /// Takes a batch header and the transmissions provided for it, and returns the subset of those
    /// transmissions that the storage does not already hold.
    ///
    /// A transmission that cannot be retrieved from this storage must either be provided by the
    /// caller or be classified as aborted, whether declared by the caller or already recorded in
    /// storage. Anything else is an error: a peer does not get to have a certificate accepted while
    /// withholding a transmission that it commits to.
    ///
    /// An aborted marker only means that this storage has no payload for that entry; it does not
    /// imply that a peer cannot provide the bytes.
    fn find_missing_transmissions(
        &self,
        batch_header: &BatchHeader<N>,
        mut transmissions: HashMap<TransmissionID<N>, Transmission<N>>,
        aborted_transmissions: HashSet<TransmissionID<N>>,
    ) -> Result<HashMap<TransmissionID<N>, Transmission<N>>> {
        // Initialize a list for the missing transmissions from storage.
        let mut missing_transmissions = HashMap::new();
        // Ensure the declared transmission IDs are all present in storage or the given transmissions map.
        for transmission_id in batch_header.transmission_ids() {
            // If the transmission cannot be retrieved from storage, ensure it was provided by the
            // caller. Note that an ID recorded as aborted holds no transmission, so bytes provided
            // for it must still be collected here in order to become retrievable.
            if !self.contains_retrievable_transmission(*transmission_id) {
                // Retrieve the transmission.
                match transmissions.remove(transmission_id) {
                    // Append the transmission if it exists.
                    Some(transmission) => {
                        missing_transmissions.insert(*transmission_id, transmission);
                    }
                    // If the transmission does not exist, it must be aborted: either the caller
                    // declares it as such, or storage already recorded it as aborted for an earlier
                    // certificate. Note that this accepts the ID without bytes; obtaining them, if
                    // a peer has them, is the caller's business - see `fetch_missing_transmissions`.
                    None => {
                        if !aborted_transmissions.contains(transmission_id)
                            && !self.contains_aborted_transmission(*transmission_id)
                        {
                            bail!("Failed to provide a transmission");
                        }
                    }
                }
            }
        }
        Ok(missing_transmissions)
    }

    /// Inserts the given certificate ID for each of the transmission IDs, using the missing transmissions map, into storage.
    ///
    /// The certificate has to be counted against every ID it declares: against the transmission
    /// entry for the IDs whose bytes storage holds or the caller provides, and against the aborted
    /// entry for the rest - both the IDs the caller declares as aborted, and the ones storage
    /// already recorded as aborted for an earlier certificate. [`Self::remove_transmissions`]
    /// decrements both maps for every declared ID, so an ID that is credited to neither is dropped
    /// from storage while this certificate still refers to it.
    ///
    /// Note that this makes the implementation, rather than the caller, responsible for recognizing
    /// an already-aborted ID: only the implementation can decide it under the locks that
    /// [`Self::remove_transmissions`] takes as well.
    fn insert_transmissions(
        &self,
        certificate_id: Field<N>,
        transmission_ids: IndexSet<TransmissionID<N>>,
        aborted_transmission_ids: HashSet<TransmissionID<N>>,
        missing_transmissions: HashMap<TransmissionID<N>, Transmission<N>>,
    );

    /// Removes the certificate ID for the transmissions from storage.
    ///
    /// If the transmission no longer references any certificate IDs, the entry is removed from storage.
    fn remove_transmissions(&self, certificate_id: &Field<N>, transmission_ids: &IndexSet<TransmissionID<N>>);

    /// Returns a HashMap over the `(transmission ID, (transmission, certificate IDs))` entries.
    #[cfg(any(test, feature = "test"))]
    fn as_hashmap(&self) -> HashMap<TransmissionID<N>, (Transmission<N>, IndexSet<Field<N>>)>;
}
