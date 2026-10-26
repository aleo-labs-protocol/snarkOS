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

use snarkos_node_network::NodeType;
use snarkos_node_sync_locators::{CHECKPOINT_INTERVAL, NUM_RECENT_BLOCKS};
use snarkvm::prelude::{FromBytes, ToBytes};

use std::borrow::Cow;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ping<N: Network> {
    pub version: u32,
    pub node_type: NodeType,
    pub block_locators: Option<BlockLocators<N>>,
}

/// The chain height [`MAX_PING_MESSAGE_SIZE`] is sized for: mainnet is at ~19.9M blocks and grows
/// ~10.4M a year, so 500M is 40+ years of headroom. Whoever gets near it raises the cap, or - the
/// real fix - makes the locator scheme logarithmic in height rather than linear.
pub const MAX_PING_LOCATOR_HEIGHT: u32 = 500_000_000;

/// The maximum size of a `Ping` frame, kept next to `write_le` below since that is what actually
/// defines the fixed part of this layout (4 + 1 + 1 bytes, plus the message ID that
/// `Message::write_le` prepends). Unlike every other message type, `Ping` has no fixed shape:
/// `block_locators` grows with the height of the chain, carrying up to `NUM_RECENT_BLOCKS` recent
/// entries plus one checkpoint per `CHECKPOINT_INTERVAL` blocks (see
/// `snarkos_node_sync_locators::block_locators`), each a `(u32, BlockHash)` pair - 4 bytes plus a
/// 32-byte field element.
///
/// This is a policy cap rather than the wire format's own ceiling: the format can express
/// `MAX_CHECKPOINTS` checkpoints, a chain of 4.29 billion blocks and ~14.75 MiB, but locator size
/// is linear in chain height, so what an honest peer actually sends is set by the height the
/// chain has reached. Since this cap is what bounds the memory an untrusted router peer can pin
/// (see `MessageId::max_size` and `MessageCodec::for_allowed_ids`), it is sized from
/// `MAX_PING_LOCATOR_HEIGHT` instead, which the assert below holds it to.
pub const MAX_PING_MESSAGE_SIZE: usize = 2 * 1024 * 1024; // 2 MiB

const _: () = {
    let locator_entry = size_of::<u32>() + 32; // a block height, plus a `BlockHash` field element
    let checkpoints = (MAX_PING_LOCATOR_HEIGHT / CHECKPOINT_INTERVAL) as usize + 1; // one at height 0
    let ping = 2 // the message ID
        + 4 // `version`
        + 1 // `node_type`
        + 1 // the `Some`/`None` marker on `block_locators`
        + 4 // the `recents` map's length prefix
        + 4 // the `checkpoints` map's length prefix
        + (NUM_RECENT_BLOCKS + checkpoints) * locator_entry;
    assert!(
        ping <= MAX_PING_MESSAGE_SIZE,
        "MAX_PING_MESSAGE_SIZE is below the Ping a chain at MAX_PING_LOCATOR_HEIGHT sends"
    );
};

impl<N: Network> MessageTrait for Ping<N> {
    /// Returns the message name.
    #[inline]
    fn name(&self) -> Cow<'static, str> {
        "Ping".into()
    }
}

impl<N: Network> ToBytes for Ping<N> {
    fn write_le<W: io::Write>(&self, mut writer: W) -> io::Result<()> {
        self.version.write_le(&mut writer)?;
        self.node_type.write_le(&mut writer)?;
        if let Some(locators) = &self.block_locators {
            1u8.write_le(&mut writer)?;
            locators.write_le(&mut writer)?;
        } else {
            0u8.write_le(&mut writer)?;
        }

        Ok(())
    }
}

impl<N: Network> FromBytes for Ping<N> {
    fn read_le<R: io::Read>(mut reader: R) -> io::Result<Self> {
        let version = u32::read_le(&mut reader)?;
        let node_type = NodeType::read_le(&mut reader)?;

        let selector = u8::read_le(&mut reader)?;
        let block_locators = match selector {
            0 => None,
            1 => Some(BlockLocators::read_le(&mut reader)?),
            _ => return Err(error("Invalid block locators marker")),
        };

        Ok(Self { version, node_type, block_locators })
    }
}

impl<N: Network> Ping<N> {
    pub fn new(node_type: NodeType, block_locators: Option<BlockLocators<N>>) -> Self {
        Self { version: <Message<N>>::latest_message_version(), node_type, block_locators }
    }
}

#[cfg(test)]
pub mod prop_tests {
    use crate::{MAX_PING_LOCATOR_HEIGHT, Ping, challenge_request::prop_tests::any_node_type};
    use snarkos_node_network::NodeType;
    use snarkos_node_sync_locators::{
        BlockLocators,
        CHECKPOINT_INTERVAL,
        NUM_RECENT_BLOCKS,
        test_helpers::sample_block_locators,
    };
    use snarkvm::utilities::{FromBytes, ToBytes};

    use bytes::{Buf, BufMut, BytesMut};
    use proptest::{
        option,
        prelude::{BoxedStrategy, Strategy, any},
    };
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    /// Locator size is linear in height, so the random heights stay below 2^25 (at most 3,356
    /// checkpoints). Half of them are at least 2^24, so their recent heights use all four bytes.
    /// `ping_roundtrip_at_locator_boundaries` covers the heights a uniform draw is unlikely to hit.
    ///
    /// Heights of `MAX_CHECKPOINTS * CHECKPOINT_INTERVAL` and above do not round-trip: their
    /// locators carry more than `MAX_CHECKPOINTS` checkpoints, which `BlockLocators::read_le`
    /// rejects. `BlockLocators::new` already rejects a height of `u32::MAX`, so
    /// `sample_block_locators` panics there.
    pub fn any_block_locators() -> BoxedStrategy<BlockLocators<CurrentNetwork>> {
        (0u32..1 << 25).prop_map(sample_block_locators).boxed()
    }

    pub fn any_ping() -> BoxedStrategy<Ping<CurrentNetwork>> {
        (any::<u32>(), option::of(any_block_locators()), any_node_type())
            .prop_map(|(version, block_locators, node_type)| Ping { version, block_locators, node_type })
            .boxed()
    }

    fn assert_roundtrip(ping: &Ping<CurrentNetwork>) {
        let mut bytes = BytesMut::default().writer();
        ping.write_le(&mut bytes).unwrap();
        let decoded = Ping::<CurrentNetwork>::read_le(&mut bytes.into_inner().reader()).unwrap();
        assert_eq!(ping, &decoded);
    }

    #[proptest]
    fn ping_roundtrip(#[strategy(any_ping())] ping: Ping<CurrentNetwork>) {
        assert_roundtrip(&ping);
    }

    /// The heights where the locator layout changes shape:
    /// - below `NUM_RECENT_BLOCKS`, the recents start at height 0 and share it with the first
    ///   checkpoint;
    /// - within `NUM_RECENT_BLOCKS` past a checkpoint, the recents share that checkpoint's height;
    /// - `MAX_PING_LOCATOR_HEIGHT` is the chain height `MAX_PING_MESSAGE_SIZE` is sized for.
    #[test]
    fn ping_roundtrip_at_locator_boundaries() {
        let recent = NUM_RECENT_BLOCKS as u32;
        let heights = [
            0,
            1,
            recent - 1,
            recent,
            CHECKPOINT_INTERVAL - 1,
            CHECKPOINT_INTERVAL,
            CHECKPOINT_INTERVAL + recent - 1,
            CHECKPOINT_INTERVAL + recent,
            MAX_PING_LOCATOR_HEIGHT,
        ];
        for height in heights {
            assert_roundtrip(&Ping {
                version: u32::MAX,
                node_type: NodeType::Client,
                block_locators: Some(sample_block_locators(height)),
            });
        }
    }

    /// The largest `Ping` the cap is sized to accept: the locators of a chain at
    /// `MAX_PING_LOCATOR_HEIGHT`. The failure mode a too-tight cap causes is disconnecting honest
    /// peers, which is worse than the DoS it exists to prevent, so this has to keep decoding.
    pub fn largest_supported_ping() -> Ping<CurrentNetwork> {
        Ping {
            version: 0,
            node_type: snarkos_node_network::NodeType::Client,
            block_locators: Some(sample_block_locators(MAX_PING_LOCATOR_HEIGHT)),
        }
    }
}
