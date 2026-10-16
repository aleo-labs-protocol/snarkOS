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

use super::*;

use snarkvm::{
    console::network::ConsensusVersion,
    ledger::narwhal::Data,
    prelude::{FromBytes, ToBytes},
    utilities::io_error,
};

use std::borrow::Cow;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockResponse<N: Network> {
    /// The original block request.
    pub request: BlockRequest,
    /// The blocks.
    pub blocks: Data<DataBlocks<N>>,
    /// The consensus version at the height of the *last* block in this response.
    /// This enables detecting if the current node, or the peer, missed an upgrade. Its value is `None` for messages with version < 2.
    pub latest_consensus_version: Option<ConsensusVersion>,
}

impl<N: Network> EventTrait for BlockResponse<N> {
    /// Returns the event name.
    #[inline]
    fn name(&self) -> Cow<'static, str> {
        let start = self.request.start_height;
        let end = self.request.end_height;
        match start + 1 == end {
            true => format!("BlockResponse {start}"),
            false => format!("BlockResponse {start}..{end}"),
        }
        .into()
    }
}

impl<N: Network> BlockResponse<N> {
    // Constructs a new block response.
    pub fn new(request: BlockRequest, blocks: DataBlocks<N>, latest_consensus_version: ConsensusVersion) -> Self {
        Self { request, blocks: Data::Object(blocks), latest_consensus_version: Some(latest_consensus_version) }
    }
}

impl<N: Network> ToBytes for BlockResponse<N> {
    fn write_le<W: io::Write>(&self, mut writer: W) -> io::Result<()> {
        // Block responses without a consesnsus version have message version `1`, other have to `2` (or greater in the future).
        let Some(latest_consensus_version) = self.latest_consensus_version else {
            return Err(io_error("Can only serialize block responses of version 2 or greater"));
        };

        // Send the consensus version starting with V12.
        if latest_consensus_version >= ConsensusVersion::V12 {
            // Currently, we simply write four zero bytes as the version number,
            // because we know a valid request start height is always non-zero.
            // In the future we can encode the real version here.
            0u32.write_le(&mut writer)?;
            self.request.write_le(&mut writer)?;
            self.blocks.write_le(&mut writer)?;
            latest_consensus_version.write_le(&mut writer)
        } else {
            self.request.write_le(&mut writer)?;
            self.blocks.write_le(&mut writer)
        }
    }
}

impl<N: Network> FromBytes for BlockResponse<N> {
    fn read_le<R: io::Read>(mut reader: R) -> io::Result<Self> {
        let start_height = u32::read_le(&mut reader)?;

        // An invalid start height as the first four bytes indicates that this message
        // contains the consensus version of the last block.
        let contains_consensus_version = start_height == 0;

        // If this message type does not contain the consensus version, use the first four bytes as the start height.
        // Otherwise, read the full request.
        let request = if contains_consensus_version {
            BlockRequest::read_le(&mut reader)?
        } else {
            let end_height = u32::read_le(&mut reader)?;
            BlockRequest::new(start_height, end_height)?
        };

        let blocks = Data::read_le(&mut reader)?;

        let latest_consensus_version =
            if contains_consensus_version { Some(FromBytes::read_le(&mut reader)?) } else { None };

        Ok(Self { request, blocks, latest_consensus_version })
    }
}

/// A wrapper for a list of blocks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DataBlocks<N: Network>(pub Vec<Block<N>>);

impl<N: Network> DataBlocks<N> {
    /// The maximum number of blocks that can be sent in a single message.
    pub const MAXIMUM_NUMBER_OF_BLOCKS: u8 = 5;

    /// Ensures that the blocks are well-formed in a block response.
    pub fn ensure_response_is_well_formed(
        &self,
        peer_ip: SocketAddr,
        start_height: u32,
        end_height: u32,
    ) -> Result<()> {
        // Ensure the blocks are not empty.
        ensure!(!self.0.is_empty(), "Peer '{peer_ip}' sent an empty block response ({start_height}..{end_height})");
        // Check that the blocks are sequentially ordered.
        if !self.0.windows(2).all(|w| w[0].height() + 1 == w[1].height()) {
            bail!("Peer '{peer_ip}' sent an invalid block response (blocks are not sequentially ordered)")
        }

        // Retrieve the start (inclusive) and end (exclusive) block height.
        let candidate_start_height = self.first().map(|b| b.height()).unwrap_or(0);
        let candidate_end_height = 1 + self.last().map(|b| b.height()).unwrap_or(0);
        // Check that the range matches the block request.
        if start_height != candidate_start_height || end_height != candidate_end_height {
            bail!("Peer '{peer_ip}' sent an invalid block response (range does not match block request)")
        }
        Ok(())
    }
}

impl<N: Network> std::ops::Deref for DataBlocks<N> {
    type Target = Vec<Block<N>>;

    /// Returns the list of blocks.
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<N: Network> ToBytes for DataBlocks<N> {
    /// Writes the blocks to the given writer.
    #[inline]
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        // Prepare the number of blocks.
        let num_blocks = self.0.len() as u8;
        // Ensure that the number of blocks is within the allowed range.
        if num_blocks > Self::MAXIMUM_NUMBER_OF_BLOCKS {
            return Err(error("Block response exceeds maximum number of blocks"));
        }
        // Write the number of blocks.
        num_blocks.write_le(&mut writer)?;
        // Write the blocks.
        self.0.iter().take(num_blocks as usize).try_for_each(|block| block.write_le(&mut writer))
    }
}

impl<N: Network> FromBytes for DataBlocks<N> {
    /// Reads the message from the given reader.
    #[inline]
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        // Read the number of blocks.
        let num_blocks = u8::read_le(&mut reader)?;
        // Ensure that the number of blocks is within the allowed range.
        if num_blocks > Self::MAXIMUM_NUMBER_OF_BLOCKS {
            return Err(error("Block response exceeds maximum number of blocks"));
        }
        // Read the blocks.
        let blocks = (0..num_blocks).map(|_| Block::read_le(&mut reader)).collect::<Result<Vec<_>, _>>()?;
        Ok(Self(blocks))
    }
}

#[cfg(test)]
pub mod prop_tests {
    use crate::{BlockRequest, BlockResponse, DataBlocks, block_request::prop_tests::any_block_request};

    use snarkvm::{
        console::network::ConsensusVersion,
        ledger::{narwhal::Data, test_helpers::sample_genesis_block},
        prelude::{FromBytes, TestRng, ToBytes},
    };

    use bytes::{Buf, BufMut, BytesMut};
    use proptest::prelude::{BoxedStrategy, ProptestConfig, Strategy, any};
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    pub fn any_block_response() -> BoxedStrategy<BlockResponse<CurrentNetwork>> {
        (any_block_request(), any::<u64>())
            .prop_map(|(request, seed)| {
                // Generate blocks that match the requests range.
                let mut rng = TestRng::from_seed(seed);
                let blocks: Vec<_> =
                    (request.start_height..request.end_height).map(|_| sample_genesis_block(&mut rng)).collect();

                // V12 is the current wire format: the version is written and survives a roundtrip.
                // V11 is the legacy encoding (no version on the wire); after decode it cannot be
                // re-serialized, so it is not a valid input for generic Event codec properties.
                // That path is covered by `deserialize_version1`.
                BlockResponse::new(request, DataBlocks(blocks), ConsensusVersion::V12)
            })
            .boxed()
    }

    #[proptest(ProptestConfig { cases: 10, ..ProptestConfig::default() })]
    fn block_response_roundtrip(#[strategy(any_block_response())] block_response: BlockResponse<CurrentNetwork>) {
        let mut bytes = BytesMut::default().writer();
        block_response.write_le(&mut bytes).unwrap();
        let decoded = BlockResponse::<CurrentNetwork>::read_le(&mut bytes.into_inner().reader()).unwrap();

        assert_eq!(block_response.request, decoded.request);

        // A block response will never contain a version below 12.
        if let Some(vno) = block_response.latest_consensus_version
            && vno < ConsensusVersion::V12
        {
            assert_eq!(decoded.latest_consensus_version, None)
        } else {
            assert_eq!(decoded.latest_consensus_version, block_response.latest_consensus_version);
        }

        assert_eq!(
            block_response.blocks.deserialize_blocking().unwrap(),
            decoded.blocks.deserialize_blocking().unwrap(),
        );
    }

    /// Generates a block response encoded in the old format, and ensures it is still deserializable.
    #[proptest(ProptestConfig { cases: 10, ..ProptestConfig::default() })]
    fn deserialize_version1(
        #[strategy(any_block_request())] request: BlockRequest,
        #[strategy(any::<u64>())] seed: u64,
    ) {
        let mut rng = TestRng::from_seed(seed);

        let blocks = DataBlocks(
            (request.start_height..request.end_height).map(|_| sample_genesis_block(&mut rng)).collect::<Vec<_>>(),
        );

        // Write the response without message or consesnsus version.
        let mut data = Vec::new();
        request.write_le(&mut data).unwrap();
        Data::Object(blocks.clone()).write_le(&mut data).unwrap();

        // Deserialize it.
        let response = BlockResponse::read_le(data.reader()).unwrap();

        assert_eq!(response.request, request);
        assert_eq!(response.latest_consensus_version, None);
        assert_eq!(response.blocks.deserialize_blocking().unwrap(), blocks);
    }
}
