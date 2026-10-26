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
use snarkvm::prelude::{FromBytes, ToBytes};

use std::borrow::Cow;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChallengeRequest<N: Network> {
    pub version: u32,
    pub listener_port: u16,
    pub node_type: NodeType,
    pub address: Address<N>,
    pub nonce: u64,
    pub snarkos_sha: Option<[u8; 40]>,
}

impl<N: Network> MessageTrait for ChallengeRequest<N> {
    /// Returns the message name.
    #[inline]
    fn name(&self) -> Cow<'static, str> {
        "ChallengeRequest".into()
    }
}

impl<N: Network> ToBytes for ChallengeRequest<N> {
    fn write_le<W: io::Write>(&self, mut writer: W) -> io::Result<()> {
        self.version.write_le(&mut writer)?;
        self.listener_port.write_le(&mut writer)?;
        self.node_type.write_le(&mut writer)?;
        self.address.write_le(&mut writer)?;
        self.nonce.write_le(&mut writer)?;
        self.snarkos_sha.unwrap_or(Self::UNKNOWN_COMMIT_HASH).write_le(&mut writer)?;

        Ok(())
    }
}

impl<N: Network> FromBytes for ChallengeRequest<N> {
    fn read_le<R: io::Read>(mut reader: R) -> io::Result<Self> {
        let version = u32::read_le(&mut reader)?;
        let listener_port = u16::read_le(&mut reader)?;
        let node_type = NodeType::read_le(&mut reader)?;
        let address = Address::<N>::read_le(&mut reader)?;
        let nonce = u64::read_le(&mut reader)?;

        let snarkos_sha = match <[u8; 40]>::read_le(&mut reader) {
            Ok(bytes) => {
                if bytes == Self::UNKNOWN_COMMIT_HASH {
                    None
                } else {
                    Some(bytes)
                }
            }
            Err(err) => {
                tracing::warn!("Invalid snarkOS SHA - {err}");
                None
            }
        };

        Ok(Self { version, listener_port, node_type, address, nonce, snarkos_sha })
    }
}

impl<N: Network> ChallengeRequest<N> {
    /// Constant for an unknown commit hash.
    const UNKNOWN_COMMIT_HASH: [u8; 40] = [b'?'; 40];

    pub fn new(
        listener_port: u16,
        node_type: NodeType,
        address: Address<N>,
        nonce: u64,
        snarkos_sha: Option<[u8; 40]>,
    ) -> Self {
        Self { version: Message::<N>::latest_message_version(), listener_port, node_type, address, nonce, snarkos_sha }
    }
}

#[cfg(test)]
pub mod prop_tests {
    use super::*;

    use snarkvm::{
        console::prelude::{FromBytes, ToBytes},
        prelude::{Address, TestRng, Uniform},
    };

    use bytes::{Buf, BufMut, BytesMut};
    use proptest::{
        collection,
        prelude::{BoxedStrategy, Strategy, any},
    };
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    pub fn any_valid_address() -> BoxedStrategy<Address<CurrentNetwork>> {
        any::<u64>().prop_map(|seed| Address::rand(&mut TestRng::fixed(seed))).boxed()
    }

    pub fn any_node_type() -> BoxedStrategy<NodeType> {
        // Must cover every variant: this strategy is what round-trips `NodeType` through a real
        // `ChallengeRequest` and `Ping`, and a variant missing here is a variant never encoded.
        (0..=3)
            .prop_map(|id| match id {
                0 => NodeType::Client,
                1 => NodeType::Prover,
                2 => NodeType::Validator,
                3 => NodeType::BootstrapClient,
                _ => unreachable!(),
            })
            .boxed()
    }

    pub fn any_challenge_request() -> BoxedStrategy<ChallengeRequest<CurrentNetwork>> {
        (any_valid_address(), any::<u64>(), any::<u32>(), any::<u16>(), any_node_type(), collection::vec(0u8..=127, 40))
            .prop_map(|(address, nonce, version, listener_port, node_type, sha)| {
                let sha: [u8; 40] = sha.try_into().unwrap();
                let snarkos_sha =
                    if sha == ChallengeRequest::<CurrentNetwork>::UNKNOWN_COMMIT_HASH { None } else { Some(sha) };
                ChallengeRequest { address, nonce, version, listener_port, node_type, snarkos_sha }
            })
            .boxed()
    }

    #[proptest]
    fn challenge_request_roundtrip(#[strategy(any_challenge_request())] original: ChallengeRequest<CurrentNetwork>) {
        let mut buf = BytesMut::default().writer();
        ChallengeRequest::write_le(&original, &mut buf).unwrap();

        let deserialized: ChallengeRequest<CurrentNetwork> =
            ChallengeRequest::read_le(buf.into_inner().reader()).unwrap();
        assert_eq!(original, deserialized);
    }
}
