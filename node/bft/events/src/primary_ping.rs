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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrimaryPing<N: Network> {
    pub version: u32,
    pub block_locators: BlockLocators<N>,
    pub primary_certificate: Data<BatchCertificate<N>>,
}

impl<N: Network> PrimaryPing<N> {
    /// Initializes a new ping event.
    pub const fn new(
        version: u32,
        block_locators: BlockLocators<N>,
        primary_certificate: Data<BatchCertificate<N>>,
    ) -> Self {
        Self { version, block_locators, primary_certificate }
    }
}

impl<N: Network> From<(u32, BlockLocators<N>, BatchCertificate<N>)> for PrimaryPing<N> {
    /// Initializes a new ping event.
    fn from((version, block_locators, primary_certificate): (u32, BlockLocators<N>, BatchCertificate<N>)) -> Self {
        Self::new(version, block_locators, Data::Object(primary_certificate))
    }
}

impl<N: Network> EventTrait for PrimaryPing<N> {
    /// Returns the event name.
    #[inline]
    fn name(&self) -> Cow<'static, str> {
        "PrimaryPing".into()
    }
}

impl<N: Network> ToBytes for PrimaryPing<N> {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        // Write the version.
        self.version.write_le(&mut writer)?;
        // Write the block locators.
        self.block_locators.write_le(&mut writer)?;
        // Write the primary certificate.
        self.primary_certificate.write_le(&mut writer)?;

        Ok(())
    }
}

impl<N: Network> FromBytes for PrimaryPing<N> {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        // Read the version.
        let version = u32::read_le(&mut reader)?;
        // Read the block locators.
        let block_locators = BlockLocators::read_le(&mut reader)?;
        // Read the primary certificate.
        let primary_certificate = Data::read_le(&mut reader)?;

        // Return the ping event.
        Ok(Self::new(version, block_locators, primary_certificate))
    }
}

#[cfg(test)]
pub mod prop_tests {
    use crate::{PrimaryPing, certificate_response::prop_tests::any_batch_certificate};
    use snarkos_node_sync_locators::{BlockLocators, test_helpers::sample_block_locators};
    use snarkvm::utilities::{FromBytes, ToBytes};

    use bytes::{Buf, BufMut, BytesMut};
    use proptest::prelude::{BoxedStrategy, Strategy, any};
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    pub fn any_block_locators() -> BoxedStrategy<BlockLocators<CurrentNetwork>> {
        // `sample_block_locators` inserts a checkpoint every 10_000 heights. An unconstrained
        // `u32` can therefore allocate hundreds of thousands of checkpoints and blow up codec tests.
        (0u32..50_000).prop_map(sample_block_locators).boxed()
    }

    pub fn any_primary_ping() -> BoxedStrategy<PrimaryPing<CurrentNetwork>> {
        (any::<u32>(), any_block_locators(), any_batch_certificate())
            .prop_map(|(version, block_locators, batch_certificate)| {
                PrimaryPing::from((version, block_locators, batch_certificate.clone()))
            })
            .boxed()
    }

    #[proptest]
    fn primary_ping_roundtrip(#[strategy(any_primary_ping())] primary_ping: PrimaryPing<CurrentNetwork>) {
        let mut bytes = BytesMut::default().writer();
        primary_ping.write_le(&mut bytes).unwrap();
        let decoded = PrimaryPing::<CurrentNetwork>::read_le(&mut bytes.into_inner().reader()).unwrap();
        assert_eq!(primary_ping.version, decoded.version);
        assert_eq!(primary_ping.block_locators, decoded.block_locators);
        assert_eq!(
            primary_ping.primary_certificate.deserialize_blocking().unwrap(),
            decoded.primary_certificate.deserialize_blocking().unwrap(),
        );
    }
}
