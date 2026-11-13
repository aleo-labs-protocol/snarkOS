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

use snarkos_node_sync::locators::NUM_RECENT_BLOCKS;
use snarkvm::{
    console::{
        account::{Address, Signature},
        network::Network,
        types::Field,
    },
    prelude::{FromBytes, IoResult, Read, ToBytes, Write, error},
};

use std::{collections::HashMap, ops::Deref};

/// The recently-signed batch proposals.
/// A map of `address` to (`round`, `batch ID`, `signature`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedProposals<N: Network>(pub HashMap<Address<N>, (u64, Field<N>, Signature<N>)>);

impl<N: Network> SignedProposals<N> {
    /// Ensure that every signed proposal is associated with the `expected_signer`.
    pub fn is_valid(&self, expected_signer: Address<N>) -> bool {
        self.0.iter().all(|(_, (_, _, signature))| signature.to_address() == expected_signer)
    }
}

impl<N: Network> ToBytes for SignedProposals<N> {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        // Write the number of signed proposals.
        u32::try_from(self.0.len()).map_err(error)?.write_le(&mut writer)?;
        // Serialize the signed proposals.
        for (address, (round, batch_id, signature)) in &self.0 {
            // Write the address.
            address.write_le(&mut writer)?;
            // Write the round.
            round.write_le(&mut writer)?;
            // Write the batch id.
            batch_id.write_le(&mut writer)?;
            // Write the signature.
            signature.write_le(&mut writer)?;
        }

        Ok(())
    }
}

impl<N: Network> FromBytes for SignedProposals<N> {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        // Read the number of signed proposals.
        let num_signed_proposals = u32::read_le(&mut reader)?;

        let max_certificates = N::LATEST_MAX_CERTIFICATES();
        // Ensure the number of signed proposals is within bounds
        if num_signed_proposals as usize > max_certificates as usize * NUM_RECENT_BLOCKS {
            return Err(error(format!(
                "Number of signed proposals ({num_signed_proposals}) is greater than the maximum ({max_certificates} * {NUM_RECENT_BLOCKS})",
            )));
        }
        // Deserialize the signed proposals.
        let mut signed_proposals = HashMap::with_capacity(num_signed_proposals as usize);
        for _ in 0..num_signed_proposals {
            // Read the address.
            let address = FromBytes::read_le(&mut reader)?;
            // Read the round.
            let round = FromBytes::read_le(&mut reader)?;
            // Read the batch id.
            let batch_id = FromBytes::read_le(&mut reader)?;
            // Read the signature.
            let signature = FromBytes::read_le(&mut reader)?;
            // Insert the signed proposal.
            signed_proposals.insert(address, (round, batch_id, signature));
        }

        Ok(Self(signed_proposals))
    }
}

impl<N: Network> Deref for SignedProposals<N> {
    type Target = HashMap<Address<N>, (u64, Field<N>, Signature<N>)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<N: Network> Default for SignedProposals<N> {
    /// Initializes a new instance of the signed proposals.
    fn default() -> Self {
        Self(Default::default())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use rand::RngExt;
    use snarkvm::{
        console::{account::PrivateKey, network::MainnetV0},
        utilities::{TestRng, Uniform},
    };

    type CurrentNetwork = MainnetV0;

    const ITERATIONS: usize = 10;

    pub(crate) fn sample_signed_proposals(
        signer: &PrivateKey<CurrentNetwork>,
        rng: &mut TestRng,
    ) -> SignedProposals<CurrentNetwork> {
        let mut signed_proposals: HashMap<_, _> = Default::default();
        for _ in 0..CurrentNetwork::LATEST_MAX_CERTIFICATES() {
            let private_key = PrivateKey::<CurrentNetwork>::new(rng).unwrap();
            let address = Address::try_from(&private_key).unwrap();

            // Add the signed proposal to the map.
            let round = rng.random();
            let batch_id = Field::rand(rng);
            let signature = signer.sign(&[batch_id], rng).unwrap();
            signed_proposals.insert(address, (round, batch_id, signature));
        }

        SignedProposals(signed_proposals)
    }

    #[test]
    fn test_bytes() {
        let rng = &mut TestRng::default();
        let singer_private_key = PrivateKey::<CurrentNetwork>::new(rng).unwrap();

        for _ in 0..ITERATIONS {
            let expected = sample_signed_proposals(&singer_private_key, rng);
            // Check the byte representation.
            let expected_bytes = expected.to_bytes_le().unwrap();
            assert_eq!(expected, SignedProposals::read_le(&expected_bytes[..]).unwrap());
        }
    }

    #[test]
    fn test_is_valid() {
        let rng = &mut TestRng::default();

        for _ in 0..ITERATIONS {
            let singer_private_key = PrivateKey::<CurrentNetwork>::new(rng).unwrap();
            let singer_address = Address::try_from(&singer_private_key).unwrap();
            let signed_proposals = sample_signed_proposals(&singer_private_key, rng);
            // Ensure that the signed proposals are valid.
            assert!(signed_proposals.is_valid(singer_address));
        }
    }
}
