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
    console::{account::ComputeKeyCache, network::ConsensusVersion},
    ledger::narwhal::Data,
    prelude::{FromBytes, ToBytes},
    utilities::io_error,
};

use bytes::Bytes;
use std::borrow::Cow;

/// Whether a block response carries the requested blocks, and if not, why.
///
/// A status other than `Blocks` is only sent to peers whose protocol version supports it.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum BlockResponseStatus {
    /// The response carries the requested blocks.
    Blocks,
    /// The sender no longer serves the start of the range; it serves heights from `oldest_retained`.
    Pruned { oldest_retained: u32 },
    /// The range reaches past the sender's latest block, at `latest_height`.
    NotYet { latest_height: u32 },
}

impl BlockResponseStatus {
    const NOT_YET_TAG: u8 = 1;
    const PRUNED_TAG: u8 = 0;

    /// Ensures that a reply of this status does not contradict the request it answers.
    pub fn ensure_consistent_with(&self, peer_ip: SocketAddr, request: &BlockRequest) -> Result<()> {
        let BlockRequest { start_height, end_height } = *request;
        match *self {
            Self::Blocks => Ok(()),
            Self::Pruned { oldest_retained } if start_height >= oldest_retained => bail!(
                "Peer '{peer_ip}' sent a contradictory block response ({start_height}..{end_height} pruned below {oldest_retained})"
            ),
            Self::NotYet { latest_height } if end_height - 1 <= latest_height => bail!(
                "Peer '{peer_ip}' sent a contradictory block response ({start_height}..{end_height} not yet at {latest_height})"
            ),
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockResponse<N: Network> {
    /// The original block request.
    pub request: BlockRequest,
    /// Whether the response carries the blocks.
    pub status: BlockResponseStatus,
    /// The blocks, empty unless the status is `Blocks`.
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
        Self {
            request,
            status: BlockResponseStatus::Blocks,
            blocks: Data::Object(blocks),
            latest_consensus_version: Some(latest_consensus_version),
        }
    }

    /// Constructs a block response without blocks, which explains why with `status`.
    pub fn unavailable(request: BlockRequest, status: BlockResponseStatus) -> Self {
        // The serialized empty `DataBlocks`, which is a block count of 0.
        let blocks = Data::Buffer(Bytes::from_static(&[0]));
        Self { request, status, blocks, latest_consensus_version: None }
    }
}

impl<N: Network> ToBytes for BlockResponse<N> {
    fn write_le<W: io::Write>(&self, mut writer: W) -> io::Result<()> {
        // A status reply starts with `u32::MAX`, which is never a valid start height.
        let status = match self.status {
            BlockResponseStatus::Blocks => None,
            BlockResponseStatus::Pruned { oldest_retained } => Some((BlockResponseStatus::PRUNED_TAG, oldest_retained)),
            BlockResponseStatus::NotYet { latest_height } => Some((BlockResponseStatus::NOT_YET_TAG, latest_height)),
        };
        if let Some((tag, value)) = status {
            u32::MAX.write_le(&mut writer)?;
            self.request.write_le(&mut writer)?;
            tag.write_le(&mut writer)?;
            return value.write_le(&mut writer);
        }

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

        if start_height == u32::MAX {
            let request = BlockRequest::read_le(&mut reader)?;
            let tag = u8::read_le(&mut reader)?;
            let value = u32::read_le(&mut reader)?;
            let status = match tag {
                BlockResponseStatus::PRUNED_TAG => BlockResponseStatus::Pruned { oldest_retained: value },
                BlockResponseStatus::NOT_YET_TAG => BlockResponseStatus::NotYet { latest_height: value },
                _ => return Err(io_error(format!("Invalid block response status {tag}"))),
            };
            return Ok(Self::unavailable(request, status));
        }

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

        Ok(Self { request, status: BlockResponseStatus::Blocks, blocks, latest_consensus_version })
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
        // Read the blocks through one compute-key cache.
        let mut cache = ComputeKeyCache::default();
        let blocks = (0..num_blocks)
            .map(|_| Block::read_le_with_cache(&mut reader, false, &mut cache))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self(blocks))
    }
}

#[cfg(any(test, feature = "test-helpers"))]
pub mod prop_tests {
    #![cfg_attr(not(test), allow(unused_imports))]
    use crate::{
        BlockRequest,
        BlockResponse,
        BlockResponseStatus,
        DataBlocks,
        block_request::prop_tests::any_block_request,
    };

    use snarkos_utilities::test_fixture::cached_fixture;
    use snarkvm::{
        console::network::ConsensusVersion,
        ledger::narwhal::Data,
        prelude::{FromBytes, TestRng, ToBytes, block::Block},
    };

    use bytes::{Buf, BufMut, BytesMut};
    use proptest::prelude::{BoxedStrategy, ProptestConfig, Strategy, any, prop_oneof};
    use std::sync::OnceLock;
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    /// Returns a sample genesis block, which costs two execution proofs to build.
    ///
    /// Every call returns the same block, whatever `rng` is, and so does every test process of this
    /// binary.
    pub fn sample_genesis_block(rng: &mut TestRng) -> Block<CurrentNetwork> {
        static BLOCK: OnceLock<Block<CurrentNetwork>> = OnceLock::new();
        BLOCK
            .get_or_init(|| {
                cached_fixture(
                    "sample-genesis-block",
                    &[],
                    |block: &Block<CurrentNetwork>| block.to_bytes_le().unwrap(),
                    |bytes| Block::from_bytes_le(bytes).ok(),
                    || snarkvm::ledger::test_helpers::sample_genesis_block(rng),
                )
            })
            .clone()
    }

    pub fn any_block_response() -> BoxedStrategy<BlockResponse<CurrentNetwork>> {
        prop_oneof![any_blocks_response(), any_status_response()].boxed()
    }

    /// A response without blocks, of any status other than `Blocks`.
    pub fn any_status_response() -> BoxedStrategy<BlockResponse<CurrentNetwork>> {
        (any_block_request(), any::<bool>(), any::<u32>())
            .prop_map(|(request, pruned, value)| {
                let status = match pruned {
                    true => BlockResponseStatus::Pruned { oldest_retained: value },
                    false => BlockResponseStatus::NotYet { latest_height: value },
                };
                BlockResponse::unavailable(request, status)
            })
            .boxed()
    }

    pub fn any_blocks_response() -> BoxedStrategy<BlockResponse<CurrentNetwork>> {
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
    fn block_response_roundtrip(#[strategy(any_blocks_response())] block_response: BlockResponse<CurrentNetwork>) {
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

    #[proptest]
    fn status_response_roundtrip(#[strategy(any_status_response())] block_response: BlockResponse<CurrentNetwork>) {
        let bytes = block_response.to_bytes_le().unwrap();
        assert_eq!(bytes.len(), 17);
        assert_eq!(BlockResponse::<CurrentNetwork>::read_le(&bytes[..]).unwrap(), block_response);
    }

    /// A `Blocks` response keeps the legacy and V12 layouts.
    #[test]
    fn blocks_response_bytes_are_unchanged() {
        let request = BlockRequest::new(1, 3).unwrap();
        let block = sample_genesis_block(&mut TestRng::default());
        let blocks = DataBlocks(vec![block.clone(), block]);
        let blocks_bytes = Data::Object(blocks.clone()).to_bytes_le().unwrap();
        let request_bytes = request.to_bytes_le().unwrap();

        let legacy = BlockResponse::new(request, blocks.clone(), ConsensusVersion::V11).to_bytes_le().unwrap();
        assert_eq!(legacy, [&request_bytes[..], &blocks_bytes].concat());

        let v12 = BlockResponse::new(request, blocks, ConsensusVersion::V12).to_bytes_le().unwrap();
        let version_bytes = ConsensusVersion::V12.to_bytes_le().unwrap();
        assert_eq!(v12, [&0u32.to_le_bytes()[..], &request_bytes, &blocks_bytes, &version_bytes].concat());
    }

    #[test]
    fn status_response_layout() {
        let request = BlockRequest::new(7, 9).unwrap();
        let cases = [
            (BlockResponseStatus::Pruned { oldest_retained: 20 }, 0u8),
            (BlockResponseStatus::NotYet { latest_height: 7 }, 1),
        ];
        for (status, tag) in cases {
            let bytes = BlockResponse::<CurrentNetwork>::unavailable(request, status).to_bytes_le().unwrap();
            let value = if tag == 0 { 20u32 } else { 7 };
            let expected =
                [&u32::MAX.to_le_bytes()[..], &7u32.to_le_bytes(), &9u32.to_le_bytes(), &[tag], &value.to_le_bytes()]
                    .concat();
            assert_eq!(bytes, expected);
        }

        // A status reply has no payload left to serialize.
        let response = BlockResponse::<CurrentNetwork>::unavailable(request, cases[0].0);
        assert!(!crate::Event::BlockResponse(response).has_unserialized_payload());

        // An unknown tag is rejected.
        let mut bytes = BlockResponse::<CurrentNetwork>::unavailable(request, cases[0].0).to_bytes_le().unwrap();
        bytes[12] = 2;
        assert!(BlockResponse::<CurrentNetwork>::read_le(&bytes[..]).is_err());
    }

    /// A status reply that contradicts its own request is rejected; any other is accepted.
    #[test]
    fn status_contradictions() {
        let peer_ip = "127.0.0.1:1234".parse().unwrap();
        let request = BlockRequest::new(10, 15).unwrap();
        let cases = [
            (BlockResponseStatus::Blocks, true),
            (BlockResponseStatus::Pruned { oldest_retained: 11 }, true),
            (BlockResponseStatus::Pruned { oldest_retained: u32::MAX }, true),
            (BlockResponseStatus::Pruned { oldest_retained: 10 }, false),
            (BlockResponseStatus::Pruned { oldest_retained: 0 }, false),
            (BlockResponseStatus::NotYet { latest_height: 13 }, true),
            (BlockResponseStatus::NotYet { latest_height: 0 }, true),
            (BlockResponseStatus::NotYet { latest_height: 14 }, false),
            (BlockResponseStatus::NotYet { latest_height: u32::MAX }, false),
        ];
        for (status, consistent) in cases {
            assert_eq!(status.ensure_consistent_with(peer_ip, &request).is_ok(), consistent, "{status:?}");
        }
    }

    #[test]
    fn read_rejects_a_bad_compute_key_after_a_good_one() {
        let block = sample_genesis_block(&mut TestRng::default());
        let block_bytes = block.to_bytes_le().unwrap();
        // The authority follows the version, the block hash, the previous hash and the header.
        let authority_at = 1 + 32 + 32 + block.header().to_bytes_le().unwrap().len();
        assert!(block_bytes[authority_at..].starts_with(&block.authority().to_bytes_le().unwrap()));
        // The beacon signature's `pk_sig` follows the authority variant, the challenge and the response.
        let pk_sig_at = authority_at + 1 + 64;
        let mut bad_block_bytes = block_bytes.clone();
        bad_block_bytes[pk_sig_at..pk_sig_at + 32].fill(0xFF);
        let expected = Block::<CurrentNetwork>::read_le(&bad_block_bytes[..]).unwrap_err().to_string();

        let bytes = [&[2u8][..], &block_bytes, &bad_block_bytes].concat();
        let error = DataBlocks::<CurrentNetwork>::read_le(&bytes[..]).unwrap_err().to_string();
        assert_eq!(error, expected);
    }
}
