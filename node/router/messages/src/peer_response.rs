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

use snarkvm::prelude::{FromBytes, ToBytes};

use std::borrow::Cow;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerResponse {
    pub peers: Vec<(SocketAddr, Option<u32>)>,
}

impl MessageTrait for PeerResponse {
    /// Returns the message name.
    #[inline]
    fn name(&self) -> Cow<'static, str> {
        "PeerResponse".into()
    }
}

impl ToBytes for PeerResponse {
    fn write_le<W: io::Write>(&self, mut writer: W) -> io::Result<()> {
        // Return error if the number of peers exceeds the maximum.
        if self.peers.len() > u8::MAX as usize {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("Too many peers: {}", self.peers.len())));
        }

        // A version indicator; we don't expect empty peer responses, so a zero value can serve
        // as an indicator that this message is to be processed differently. The version value
        // can be changed to a 2 in the future, once everyone expects it there.
        0u8.write_le(&mut writer)?;

        (self.peers.len() as u8).write_le(&mut writer)?;
        for (addr, height) in self.peers.iter() {
            addr.write_le(&mut writer)?;
            if let Some(h) = height {
                1u8.write_le(&mut writer)?;
                h.write_le(&mut writer)?;
            } else {
                0u8.write_le(&mut writer)?;
            }
        }
        Ok(())
    }
}

impl FromBytes for PeerResponse {
    fn read_le<R: io::Read>(mut reader: R) -> io::Result<Self> {
        // Read the peer count if their heights aren't present; otherwise, interpret this value
        // as the message version. It is a workaround for a currently missing version value.
        // The worst-case scenario is if a node hasn't updated, and it gets a `PeerRequest` from
        // its only peer who has; this would cause it to return a message that appears as if it
        // contains heights (due to a leading `0`), but it would end up failing to deserialize.
        // TODO: after a release or two, we should always be expecting the version to be present,
        // simplifying the deserialization; also, remove the `empty_old_peerlist_handling` test.
        let mut contains_heights = false;
        let count_or_version = u8::read_le(&mut reader)?;
        let count = if count_or_version == 0 {
            // Version indicator found; this message will contain optional heights.
            contains_heights = true;
            // If the first value is a zero, the next u8 is the peer count.
            u8::read_le(&mut reader)?
        } else {
            // A non-zero value indicates that this is the "old" PeerResponse without heights.
            count_or_version
        };

        let mut peers = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let addr = SocketAddr::read_le(&mut reader)?;
            let height = if contains_heights {
                match u8::read_le(&mut reader)? {
                    1 => Some(u32::read_le(&mut reader)?),
                    0 => None,
                    _ => return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid peer height".to_string())),
                }
            } else {
                None
            };
            peers.push((addr, height));
        }

        Ok(Self { peers })
    }
}

#[cfg(any(test, feature = "fuzz-helpers"))]
pub mod prop_tests {
    #![cfg_attr(not(test), allow(unused_imports))]
    use crate::PeerResponse;
    use snarkvm::utilities::{FromBytes, ToBytes};

    use bytes::{Buf, BufMut, BytesMut};
    use proptest::{
        collection::vec,
        prelude::{BoxedStrategy, Strategy, any},
    };
    use std::{
        io,
        net::{IpAddr, SocketAddr},
    };
    use test_strategy::proptest;

    pub fn any_valid_socket_addr() -> BoxedStrategy<(SocketAddr, Option<u32>)> {
        any::<(IpAddr, u16, Option<u32>)>()
            .prop_map(|(ip_addr, port, height)| (SocketAddr::new(ip_addr, port), height))
            .boxed()
    }

    pub fn any_vec() -> BoxedStrategy<Vec<(SocketAddr, Option<u32>)>> {
        vec(any_valid_socket_addr(), 0..50).prop_map(|v| v).boxed()
    }

    pub fn any_peer_response() -> BoxedStrategy<PeerResponse> {
        any_vec().prop_map(|peers| PeerResponse { peers }).boxed()
    }

    #[proptest]
    fn peer_response_roundtrip(#[strategy(any_peer_response())] peer_response: PeerResponse) {
        let mut bytes = BytesMut::default().writer();
        peer_response.write_le(&mut bytes).unwrap();
        let decoded = PeerResponse::read_le(&mut bytes.into_inner().reader()).unwrap();
        assert_eq!(decoded, peer_response);
    }

    // The following test will be obsolete once all the nodes handle heights in the `PeerResponse`.
    #[test]
    fn empty_old_peerlist_handling() {
        // An empty `PeerResponse` without heights contains a single 0u8.
        let serialized = &[0u8];
        let deserialized = PeerResponse::read_le(&serialized[..]).unwrap_err();
        // Check for the expected error.
        assert_eq!(deserialized.kind(), io::ErrorKind::UnexpectedEof);
    }
}
