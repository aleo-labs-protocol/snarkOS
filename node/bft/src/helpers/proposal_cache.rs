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

use crate::helpers::{Proposal, SignedProposals};
use snarkos_utilities::NodeDataDir;

use snarkvm::{
    console::{account::Address, network::Network, program::SUBDAG_CERTIFICATES_DEPTH},
    ledger::narwhal::BatchCertificate,
    prelude::{FromBytes, IoResult, Read, Result, ToBytes, Write, anyhow, bail, error},
};

use indexmap::IndexSet;
use std::{fs, path::Path};

/// A helper type for the cache of proposal and signed proposals.
#[derive(Debug, PartialEq, Eq)]
pub struct ProposalCache<N: Network> {
    /// The latest round this node was on prior to the reboot.
    latest_round: u64,
    /// The latest proposal this node has created.
    proposal: Option<Proposal<N>>,
    /// The signed proposals this node has received.
    signed_proposals: SignedProposals<N>,
    /// The pending certificates in storage that have not been included in the ledger.
    pending_certificates: IndexSet<BatchCertificate<N>>,
}

impl<N: Network> ProposalCache<N> {
    /// Initializes a new instance of the proposal cache.
    pub fn new(
        latest_round: u64,
        proposal: Option<Proposal<N>>,
        signed_proposals: SignedProposals<N>,
        pending_certificates: IndexSet<BatchCertificate<N>>,
    ) -> Self {
        Self { latest_round, proposal, signed_proposals, pending_certificates }
    }

    /// Ensure that the proposal and every signed proposal is associated with the `expected_signer`.
    pub fn is_valid(&self, expected_signer: Address<N>) -> bool {
        self.proposal
            .as_ref()
            .map(|proposal| {
                proposal.batch_header().author() == expected_signer && self.latest_round == proposal.round()
            })
            .unwrap_or(true)
            && self.signed_proposals.is_valid(expected_signer)
    }

    /// Returns `true` if a proposal cache exists for the given network and `dev`.
    pub fn exists(node_data_dir: &NodeDataDir) -> bool {
        node_data_dir.current_proposal_cache_path().exists()
    }

    /// Load the proposal cache from the file system and ensure that the proposal cache is valid.
    pub fn load(expected_signer: Address<N>, node_data_dir: &NodeDataDir) -> Result<Self> {
        // Construct the proposal cache file system path.
        let path = node_data_dir.current_proposal_cache_path();

        // Deserialize the proposal cache from the file system.
        let proposal_cache = match fs::read(&path) {
            Ok(bytes) => match Self::from_bytes_le(&bytes) {
                Ok(proposal_cache) => proposal_cache,
                Err(_) => bail!("Couldn't deserialize the proposal stored at {}", path.display()),
            },
            Err(_) => bail!("Couldn't read the proposal stored at {}", path.display()),
        };

        // Ensure the proposal cache is valid.
        if !proposal_cache.is_valid(expected_signer) {
            bail!("The proposal cache at {} is invalid for the given address {expected_signer}", path.display());
        }

        info!("Loaded the proposal cache from {} at round {}", path.display(), proposal_cache.latest_round);

        Ok(proposal_cache)
    }

    /// Store the proposal cache to the file system.
    pub fn store(&self, node_data_dir: &NodeDataDir) -> Result<()> {
        let path = node_data_dir.current_proposal_cache_path();
        debug!("Storing the proposal cache to {}...", path.display());

        // Serialize the proposal cache.
        let bytes = self.to_bytes_le()?;
        // Store the proposal cache to a temporary file, sync it, and rename it over the cache path,
        // so that a crash mid-write never leaves a torn cache file.
        let tmp_path = path.with_extension("tmp");
        let write_tmp = || -> std::io::Result<()> {
            let mut file = fs::File::create(&tmp_path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&tmp_path, &path)?;
            // Sync the parent directory, so that the rename itself survives a crash.
            sync_parent_dir(&path)
        };
        write_tmp().map_err(|err| anyhow!("Couldn't write the proposal cache to {} - {err}", path.display()))?;

        Ok(())
    }

    /// Returns the latest round, proposal, signed proposals, and pending certificates.
    pub fn into(self) -> (u64, Option<Proposal<N>>, SignedProposals<N>, IndexSet<BatchCertificate<N>>) {
        (self.latest_round, self.proposal, self.signed_proposals, self.pending_certificates)
    }
}

/// Syncs the directory containing `path`, making a rename into it durable.
fn sync_parent_dir(path: &Path) -> std::io::Result<()> {
    #[cfg(test)]
    tests::DIR_SYNCS.with(|count| count.set(count.get() + 1));
    // Directories cannot be opened for syncing on Windows, where the rename is already durable.
    #[cfg(unix)]
    {
        let parent = match path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent,
            _ => Path::new("."),
        };
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

impl<N: Network> ToBytes for ProposalCache<N> {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        // Serialize the `latest_round`.
        self.latest_round.write_le(&mut writer)?;
        // Serialize the `proposal`.
        self.proposal.is_some().write_le(&mut writer)?;
        if let Some(proposal) = &self.proposal {
            proposal.write_le(&mut writer)?;
        }
        // Serialize the `signed_proposals`.
        self.signed_proposals.write_le(&mut writer)?;
        // Write the number of pending certificates.
        u32::try_from(self.pending_certificates.len()).map_err(error)?.write_le(&mut writer)?;
        // Serialize the pending certificates.
        for certificate in &self.pending_certificates {
            certificate.write_le(&mut writer)?;
        }

        Ok(())
    }
}

impl<N: Network> FromBytes for ProposalCache<N> {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        // Deserialize `latest_round`.
        let latest_round = u64::read_le(&mut reader)?;
        // Deserialize `proposal`.
        let has_proposal: bool = FromBytes::read_le(&mut reader)?;
        let proposal = match has_proposal {
            true => Some(Proposal::read_le(&mut reader)?),
            false => None,
        };
        // Deserialize `signed_proposals`.
        let signed_proposals = SignedProposals::read_le(&mut reader)?;
        // Read the number of pending certificates.
        let num_certificates = u32::read_le(&mut reader)?;
        // Ensure the number of certificates is within bounds.
        if num_certificates > 2u32.saturating_pow(SUBDAG_CERTIFICATES_DEPTH as u32) {
            return Err(error(format!(
                "Number of certificates ({num_certificates}) exceeds the maximum ({})",
                2u32.saturating_pow(SUBDAG_CERTIFICATES_DEPTH as u32)
            )));
        };
        // Deserialize the pending certificates.
        let pending_certificates =
            (0..num_certificates).map(|_| BatchCertificate::read_le(&mut reader)).collect::<IoResult<IndexSet<_>>>()?;

        Ok(Self::new(latest_round, proposal, signed_proposals, pending_certificates))
    }
}

impl<N: Network> Default for ProposalCache<N> {
    /// Initializes a new instance of the proposal cache.
    fn default() -> Self {
        Self::new(0, None, Default::default(), Default::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::{proposal::tests::sample_proposal, signed_proposals::tests::sample_signed_proposals};
    use snarkvm::{
        console::{account::PrivateKey, network::MainnetV0},
        ledger::narwhal::batch_certificate::test_helpers::sample_batch_certificates,
        utilities::TestRng,
    };

    type CurrentNetwork = MainnetV0;

    const ITERATIONS: usize = 100;

    thread_local! {
        /// The number of parent directory syncs on this thread.
        pub(super) static DIR_SYNCS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    pub(crate) fn sample_proposal_cache(
        signer: &PrivateKey<CurrentNetwork>,
        rng: &mut TestRng,
    ) -> ProposalCache<CurrentNetwork> {
        let proposal = sample_proposal(rng);
        let signed_proposals = sample_signed_proposals(signer, rng);
        let round = proposal.round();
        let pending_certificates = sample_batch_certificates(rng);

        ProposalCache::new(round, Some(proposal), signed_proposals, pending_certificates)
    }

    #[test]
    fn test_bytes() {
        let rng = &mut TestRng::default();
        let singer_private_key = PrivateKey::<CurrentNetwork>::new(rng).unwrap();

        for _ in 0..ITERATIONS {
            let expected = sample_proposal_cache(&singer_private_key, rng);
            // Check the byte representation.
            let expected_bytes = expected.to_bytes_le().unwrap();
            assert_eq!(expected, ProposalCache::read_le(&expected_bytes[..]).unwrap());
        }
    }

    #[test]
    fn test_store_is_complete_and_loads() {
        let rng = &mut TestRng::default();
        let signer = PrivateKey::<CurrentNetwork>::new(rng).unwrap();
        let dir = std::env::temp_dir().join(format!("snarkos-proposal-cache-store-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let node_data_dir = NodeDataDir::new(dir.clone());
        let path = node_data_dir.current_proposal_cache_path();

        // After a store, the cache path holds the complete serialized cache and no temporary file remains.
        let expected = sample_proposal_cache(&signer, rng);
        expected.store(&node_data_dir).unwrap();
        assert_eq!(fs::read(&path).unwrap(), expected.to_bytes_le().unwrap());
        assert!(!path.with_extension("tmp").exists());

        // A load after a store returns the same latest round, proposal, and signed proposals.
        let expected = ProposalCache::new(7, None, sample_signed_proposals(&signer, rng), Default::default());
        expected.store(&node_data_dir).unwrap();
        let address = Address::try_from(&signer).unwrap();
        assert_eq!(ProposalCache::load(address, &node_data_dir).unwrap(), expected);

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_store_syncs_parent_dir_after_replace() {
        let rng = &mut TestRng::default();
        let signer = PrivateKey::<CurrentNetwork>::new(rng).unwrap();
        let dir = std::env::temp_dir().join(format!("snarkos-proposal-cache-dir-sync-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let node_data_dir = NodeDataDir::new(dir.clone());

        // Replace an existing cache file, and check that the parent directory is synced once per store.
        let cache = sample_proposal_cache(&signer, rng);
        cache.store(&node_data_dir).unwrap();
        let before = DIR_SYNCS.with(|count| count.get());
        cache.store(&node_data_dir).unwrap();
        assert_eq!(DIR_SYNCS.with(|count| count.get()), before + 1);

        fs::remove_dir_all(&dir).unwrap();
    }
}
