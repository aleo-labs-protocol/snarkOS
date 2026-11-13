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
    console::{
        account::{Address, Signature},
        network::Network,
        types::Field,
    },
    ledger::{
        committee::Committee,
        narwhal::{BatchCertificate, BatchHeader, Transmission, TransmissionID},
    },
    prelude::{FromBytes, IoResult, Itertools, Read, Result, ToBytes, Write, bail, ensure, error},
};

use indexmap::{IndexMap, IndexSet};
use std::collections::HashSet;

/// A pending proposal by a validator.
///
/// When a validator creates and broadcasts a proposal, it collects endorsing signatures from other validators.
/// When the validator has enough endorsements, it turns the proposal into a certificate.
/// This struct holds the information about the pending proposal, in the proposing validator's state,
/// between the creation of the proposal and its turning into a certificate.
///
/// Invariant: `batch_header.transmission_ids()` contains the same elements as `transmissions.keys()`.
/// [`Proposal::new`] and [`Proposal::read_le`] establish the invariant, by checking the condition;
/// none of the other functions modifies the batch header or the transmissions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Proposal<N: Network> {
    /// The proposed batch header.
    batch_header: BatchHeader<N>,
    /// The proposed transmissions.
    transmissions: IndexMap<TransmissionID<N>, Transmission<N>>,
    /// The set of endorsing signatures accumulated so far from other validators
    /// (excludes the signature of the proposal author, which is in the `batch_header` component).
    signatures: IndexSet<Signature<N>>,
}

impl<N: Network> Proposal<N> {
    /// Initializes a new instance of the proposal.
    ///
    /// The `committee` input is the active (i.e. lookback) committee for the batch round.
    /// A crucial protocol safety check made in this constructor is that the proposal author (the current validator),
    /// is a member of that committee, because only members of the committee can propose batches.
    pub fn new(
        committee: Committee<N>,
        batch_header: BatchHeader<N>,
        transmissions: IndexMap<TransmissionID<N>, Transmission<N>>,
    ) -> Result<Self> {
        // Ensure the committee is for the batch round.
        ensure!(batch_header.round() >= committee.starting_round(), "Batch round must be >= the committee round");
        // Ensure the batch author is a member of the committee.
        ensure!(committee.is_committee_member(batch_header.author()), "The batch author is not a committee member");
        // Ensure the transmission IDs match in the batch header and transmissions.
        ensure!(
            batch_header.transmission_ids().len() == transmissions.len(),
            "The transmission IDs do not match in the batch header and transmissions"
        );
        for (a, b) in batch_header.transmission_ids().iter().zip_eq(transmissions.keys()) {
            ensure!(a == b, "The transmission IDs do not match in the batch header and transmissions");
        }
        // Return the proposal.
        Ok(Self { batch_header, transmissions, signatures: Default::default() })
    }

    /// Returns the proposed batch header.
    pub const fn batch_header(&self) -> &BatchHeader<N> {
        &self.batch_header
    }

    /// Returns the proposed batch ID.
    pub const fn batch_id(&self) -> Field<N> {
        self.batch_header.batch_id()
    }

    /// Returns the round of the batch header.
    pub const fn round(&self) -> u64 {
        self.batch_header.round()
    }

    /// Returns the timestamp of the batch header.
    pub const fn timestamp(&self) -> i64 {
        self.batch_header.timestamp()
    }

    /// Returns the transmissions.
    pub const fn transmissions(&self) -> &IndexMap<TransmissionID<N>, Transmission<N>> {
        &self.transmissions
    }

    /// Returns the transmissions.
    pub fn into_transmissions(self) -> IndexMap<TransmissionID<N>, Transmission<N>> {
        self.transmissions
    }

    /// Returns all the signers, including author and endorsers.
    pub fn signers(&self) -> HashSet<Address<N>> {
        self.signatures.iter().chain(Some(self.batch_header.signature())).map(Signature::to_address).collect()
    }

    /// Returns the non-signers, i.e. the committee members that have not signed the proposal.
    pub fn nonsigners(&self, committee: &Committee<N>) -> HashSet<Address<N>> {
        // Retrieve the current signers.
        let signers = self.signers();
        // Initialize a set for the non-signers.
        let mut nonsigners = HashSet::new();
        // Iterate through the committee members.
        for address in committee.members().keys() {
            // Insert the address if it is not a signer.
            if !signers.contains(address) {
                nonsigners.insert(*address);
            }
        }
        // Return the non-signers.
        nonsigners
    }

    /// Returns `true` if the signers of the batch (author and endorsers)
    /// have reached the quorum threshold for the committee.
    pub fn is_quorum_threshold_reached(&self, committee: &Committee<N>) -> bool {
        // Check if the batch has reached the quorum threshold.
        committee.is_quorum_threshold_reached(&self.signers())
    }

    /// Returns `true` if the proposal contains the given transmission ID.
    pub fn contains_transmission(&self, transmission_id: impl Into<TransmissionID<N>>) -> bool {
        self.transmissions.contains_key(&transmission_id.into())
    }

    /// Returns the `transmission` for the given `transmission ID`.
    pub fn get_transmission(&self, transmission_id: impl Into<TransmissionID<N>>) -> Option<&Transmission<N>> {
        self.transmissions.get(&transmission_id.into())
    }

    /// Adds an endorsing signature to the proposal, if the signature is valid
    /// and the signer is a committee member that has not already signed the proposal
    /// (this implicitly checks that the signature is not from the author).
    ///
    /// # Returns
    ///  - `Ok(true)` for a new and valid signature.
    ///  - `Ok(false)` for a valid but already existing signature.
    ///  - `Err(err)` for invalid signatures and other errors.
    pub fn add_signature(
        &mut self,
        signer: Address<N>,
        signature: Signature<N>,
        committee: &Committee<N>,
    ) -> Result<bool> {
        // Ensure the signer is in the committee.
        if !committee.is_committee_member(signer) {
            bail!("Signature from a non-committee member - '{signer}'")
        }
        // Ensure the signer is new.
        if self.signers().contains(&signer) {
            return Ok(false);
        }
        // Verify the signature. If the signature is not valid, return an error.
        // Note: This check ensures the peer's address matches the address of the signature.
        if !signature.verify(&signer, &[self.batch_id()]) {
            bail!("Signature verification failed")
        }

        // Insert the new signature and return success.
        self.signatures.insert(signature);
        Ok(true)
    }

    /// Returns the batch certificate and transmissions.
    pub fn to_certificate(
        &self,
        committee: &Committee<N>,
    ) -> Result<(BatchCertificate<N>, IndexMap<TransmissionID<N>, Transmission<N>>)> {
        // Ensure the quorum threshold has been reached.
        ensure!(self.is_quorum_threshold_reached(committee), "The quorum threshold has not been reached");
        // Create the batch certificate.
        let certificate = BatchCertificate::from(self.batch_header.clone(), self.signatures.clone())?;
        // Return the certificate and transmissions.
        Ok((certificate, self.transmissions.clone()))
    }
}

impl<N: Network> ToBytes for Proposal<N> {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        // Write the batch header.
        self.batch_header.write_le(&mut writer)?;
        // Write the number of transmissions.
        u32::try_from(self.transmissions.len()).map_err(error)?.write_le(&mut writer)?;
        // Write the transmissions.
        for (transmission_id, transmission) in &self.transmissions {
            transmission_id.write_le(&mut writer)?;
            transmission.write_le(&mut writer)?;
        }
        // Write the number of signatures.
        u32::try_from(self.signatures.len()).map_err(error)?.write_le(&mut writer)?;
        // Write the signatures.
        for signature in &self.signatures {
            signature.write_le(&mut writer)?;
        }
        Ok(())
    }
}

impl<N: Network> FromBytes for Proposal<N> {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        // Read the batch header.
        let batch_header: BatchHeader<N> = FromBytes::read_le(&mut reader)?;
        // Read the number of transmissions.
        let num_transmissions = u32::read_le(&mut reader)?;
        // Ensure the number of transmissions is within bounds (this is an early safety check).
        if num_transmissions as usize > BatchHeader::<N>::MAX_TRANSMISSIONS_PER_BATCH {
            return Err(error("Invalid number of transmissions in the proposal"));
        }
        // Read the transmissions.
        let mut transmissions = IndexMap::default();
        for _ in 0..num_transmissions {
            let transmission_id = FromBytes::read_le(&mut reader)?;
            let transmission = FromBytes::read_le(&mut reader)?;
            transmissions.insert(transmission_id, transmission);
        }
        // Read the number of signatures.
        let num_signatures = u32::read_le(&mut reader)?;
        // Ensure the number of signatures is within bounds (this is an early safety check).
        if num_signatures as usize > Committee::<N>::max_committee_size() as usize {
            return Err(error("Invalid number of signatures in the proposal"));
        }
        // Read the signatures.
        let mut signatures = IndexSet::default();
        for _ in 0..num_signatures {
            signatures.insert(FromBytes::read_le(&mut reader)?);
        }

        // Ensure the transmission IDs match in the batch header and transmissions.
        if batch_header.transmission_ids().len() != transmissions.len() {
            return Err(error("The transmission IDs do not match in the batch header and transmissions"));
        }
        for (a, b) in batch_header.transmission_ids().iter().zip_eq(transmissions.keys()) {
            if a != b {
                return Err(error("The transmission IDs do not match in the batch header and transmissions"));
            }
        }

        Ok(Self { batch_header, transmissions, signatures })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::helpers::storage::tests::sample_transmissions;
    use snarkvm::{console::network::MainnetV0, utilities::TestRng};

    type CurrentNetwork = MainnetV0;

    const ITERATIONS: usize = 100;

    pub(crate) fn sample_proposal(rng: &mut TestRng) -> Proposal<CurrentNetwork> {
        let certificate = snarkvm::ledger::narwhal::batch_certificate::test_helpers::sample_batch_certificate(rng);
        let (_, transmissions) = sample_transmissions(&certificate, rng);

        let transmissions =
            certificate.transmission_ids().iter().map(|id| (*id, transmissions.get(id).unwrap().0.clone())).collect();

        let batch_header = certificate.batch_header().clone();
        let signatures = certificate.signatures().copied().collect();

        Proposal { batch_header, transmissions, signatures }
    }

    #[test]
    fn test_bytes() {
        let rng = &mut TestRng::default();

        for _ in 0..ITERATIONS {
            let expected = sample_proposal(rng);
            // Check the byte representation.
            let expected_bytes = expected.to_bytes_le().unwrap();
            assert_eq!(expected, Proposal::read_le(&expected_bytes[..]).unwrap());
        }
    }
}

#[cfg(test)]
mod prop_tests {
    use crate::helpers::{
        Proposal,
        now,
        storage::prop_tests::{AnyTransmission, AnyTransmissionID, CryptoTestRng},
    };
    use snarkos_node_bft_events::committee_prop_tests::{CommitteeContext, ValidatorSet};
    use snarkvm::ledger::narwhal::BatchHeader;

    use indexmap::IndexMap;
    use proptest::sample::{Selector, size_range};
    use test_strategy::proptest;

    #[proptest]
    fn initialize_proposal(
        context: CommitteeContext,
        #[any(size_range(1..16).lift())] transmissions: Vec<(AnyTransmissionID, AnyTransmission)>,
        selector: Selector,
        mut rng: CryptoTestRng,
    ) {
        let CommitteeContext(committee, ValidatorSet(validators)) = context;

        let signer = selector.select(&validators);
        let mut transmission_map = IndexMap::new();

        for (AnyTransmissionID(id), AnyTransmission(t)) in transmissions.iter() {
            transmission_map.insert(*id, t.clone());
        }

        let header = BatchHeader::new(
            &signer.private_key,
            committee.starting_round(),
            now(),
            committee.id(),
            transmission_map.keys().cloned().collect(),
            Default::default(),
            &mut rng,
        )
        .unwrap();
        let proposal = Proposal::new(committee, header.clone(), transmission_map.clone()).unwrap();
        assert_eq!(proposal.batch_id(), header.batch_id());
    }
}
