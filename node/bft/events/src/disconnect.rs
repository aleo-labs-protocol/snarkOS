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

use snarkos_node_tcp::ConnectError;

use tracing::warn;

/// The reason behind the node disconnecting from a peer.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[repr(u8)]
pub enum DisconnectReason {
    /// The peer's challenge response is invalid.
    InvalidChallengeResponse = 0,
    /// No reason given.
    NoReasonGiven = 1,
    /// The peer is not following the protocol.
    ProtocolViolation = 2,
    /// The peer's client is outdated, judging by its version.
    OutdatedClientVersion = 3,
    /// The two validators are the same node.
    SelfConnect = 4,
    /// No untrusted external peers are allowed.
    NoExternalPeersAllowed = 5,
    /// Already connecting to the same node (through another TCP channel).
    AlreadyConnecting = 6,
    /// Already connected to the same node (through another TCP channel).
    AlreadyConnected = 7,
    /// Already connected to the given Aleo address.
    AlreadyConnectedToAleoAddress = 8,
    /// The sent challenge request is invalid.
    InvalidChallengeRequest = 9,
    /// The peer is not an authorized validator.
    UnauthorizedValidator = 10,
    /// The peer is on a network with a different genesis block.
    GenesisMismatch = 11,
    /// The disconnect reason is not known. This is used for when the peers sends a disconnect reason that is not known to us.
    UnknownReason = u8::MAX,
}

impl snarkos_node_tcp::ApplicationError for DisconnectReason {}

impl std::fmt::Display for DisconnectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidChallengeResponse => write!(f, "invalid challenge response"),
            Self::NoReasonGiven => write!(f, "no reason given"),
            Self::ProtocolViolation => write!(f, "protocol violation"),
            Self::OutdatedClientVersion => write!(f, "outdated client version"),
            Self::SelfConnect => write!(f, "self connect"),
            Self::NoExternalPeersAllowed => write!(f, "no external peers allowed"),
            Self::AlreadyConnecting => write!(f, "already connecting"),
            Self::AlreadyConnected => write!(f, "already connected"),
            Self::AlreadyConnectedToAleoAddress => write!(f, "already connected to the given Aleo address"),
            Self::InvalidChallengeRequest => write!(f, "invalid challenge request"),
            Self::UnauthorizedValidator => write!(f, "unauthorized validator"),
            Self::GenesisMismatch => write!(f, "genesis mismatch"),
            Self::UnknownReason => write!(f, "unknown"),
        }
    }
}

impl DisconnectReason {
    pub fn into_connect_error(self, address: SocketAddr) -> ConnectError {
        match self {
            DisconnectReason::SelfConnect => ConnectError::SelfConnect { address },
            DisconnectReason::AlreadyConnected => ConnectError::AlreadyConnected { address },
            DisconnectReason::AlreadyConnecting => ConnectError::AlreadyConnecting { address },
            _ => ConnectError::application(self),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Disconnect {
    pub reason: DisconnectReason,
}

impl From<DisconnectReason> for Disconnect {
    fn from(reason: DisconnectReason) -> Self {
        Self { reason }
    }
}

impl EventTrait for Disconnect {
    /// Returns the event name.
    #[inline]
    fn name(&self) -> Cow<'static, str> {
        "Disconnect".into()
    }
}

impl ToBytes for Disconnect {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        if self.reason == DisconnectReason::UnknownReason {
            return Err(io_error("Cannot serialize unknown disconnect reason"));
        }

        (self.reason as u8).write_le(&mut writer)
    }
}

impl FromBytes for Disconnect {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        let index = match u8::read_le(&mut reader) {
            Ok(index) => index,
            Err(err) => return Err(io_error(format!("Failed to deserialize disconnect reason: {err}"))),
        };

        let reason = match index {
            0 => DisconnectReason::InvalidChallengeResponse,
            1 => DisconnectReason::NoReasonGiven,
            2 => DisconnectReason::ProtocolViolation,
            3 => DisconnectReason::OutdatedClientVersion,
            4 => DisconnectReason::SelfConnect,
            5 => DisconnectReason::NoExternalPeersAllowed,
            6 => DisconnectReason::AlreadyConnecting,
            7 => DisconnectReason::AlreadyConnected,
            8 => DisconnectReason::AlreadyConnectedToAleoAddress,
            9 => DisconnectReason::InvalidChallengeRequest,
            10 => DisconnectReason::UnauthorizedValidator,
            11 => DisconnectReason::GenesisMismatch,
            val => {
                warn!("received unknown disconnect reason (id={val})");
                DisconnectReason::UnknownReason
            }
        };

        Ok(Self { reason })
    }
}

#[cfg(test)]
mod tests {
    use crate::{Disconnect, DisconnectReason};
    use snarkvm::console::prelude::{FromBytes, ToBytes};

    use bytes::{Buf, BufMut, BytesMut};

    #[test]
    fn serialize_deserialize() {
        // TODO switch to an iteration method that doesn't require manually updating this vec if enums are added
        // Note, do not include `UnknownReason` here, as it is not a valid disconnect reason to send over the wire.
        let all_reasons = [
            DisconnectReason::ProtocolViolation,
            DisconnectReason::NoReasonGiven,
            DisconnectReason::InvalidChallengeResponse,
            DisconnectReason::OutdatedClientVersion,
            DisconnectReason::SelfConnect,
            DisconnectReason::NoExternalPeersAllowed,
            DisconnectReason::AlreadyConnecting,
            DisconnectReason::AlreadyConnected,
            DisconnectReason::AlreadyConnectedToAleoAddress,
            DisconnectReason::InvalidChallengeRequest,
            DisconnectReason::UnauthorizedValidator,
            DisconnectReason::GenesisMismatch,
        ];

        for reason in all_reasons.iter() {
            let disconnect = Disconnect::from(*reason);
            let mut buf = BytesMut::default().writer();
            Disconnect::write_le(&disconnect, &mut buf).unwrap();

            let disconnect = Disconnect::read_le(buf.into_inner().reader()).unwrap();
            assert_eq!(reason, &disconnect.reason);
        }
    }

    #[test]
    fn deserialize_unknown_reason() {
        let mut buf = BytesMut::default().writer();
        51u8.to_le_bytes().write_le(&mut buf).unwrap();
        let disconnect = Disconnect::read_le(buf.into_inner().reader()).unwrap();
        assert_eq!(disconnect.reason, DisconnectReason::UnknownReason);
    }
}
