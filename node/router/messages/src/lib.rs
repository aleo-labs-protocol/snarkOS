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

#![forbid(unsafe_code)]

#[macro_use]
extern crate tracing;

pub mod helpers;
pub use helpers::*;

mod block_request;
pub use block_request::BlockRequest;

mod block_response;
pub use block_response::BlockResponse;

mod challenge_request;
pub use challenge_request::ChallengeRequest;

mod challenge_response;
pub use challenge_response::ChallengeResponse;

mod disconnect;
pub use disconnect::Disconnect;

mod message_id;
pub use message_id::{MAX_SMALL_MESSAGE_SIZE, MessageId};

mod peer_request;
pub use peer_request::PeerRequest;

mod peer_response;
pub use peer_response::PeerResponse;

mod ping;
pub use ping::{MAX_PING_LOCATOR_HEIGHT, MAX_PING_MESSAGE_SIZE, Ping};

mod pong;
pub use pong::Pong;

mod puzzle_request;
pub use puzzle_request::PuzzleRequest;

mod puzzle_response;
pub use puzzle_response::PuzzleResponse;

mod unconfirmed_solution;
pub use unconfirmed_solution::UnconfirmedSolution;

mod unconfirmed_transaction;
pub use unconfirmed_transaction::UnconfirmedTransaction;

pub use snarkos_node_bft_events::DataBlocks;

use snarkos_node_sync_locators::BlockLocators;
use snarkvm::prelude::{
    Address,
    ConsensusVersion,
    FromBytes,
    MainnetV0,
    Network,
    Signature,
    ToBytes,
    block::{Header, Transaction},
    error,
    puzzle::{Solution, SolutionID},
};

use std::{borrow::Cow, io, net::SocketAddr};

// A compile-time compatibility check for the Message.
const _: () = {
    let msg_versions = Message::<MainnetV0>::VERSIONS;
    let net_versions = MainnetV0::_CONSENSUS_VERSION_HEIGHTS;

    let message_version = msg_versions[msg_versions.len() - 1].0;
    let expected_version = net_versions[net_versions.len() - 1].0;

    if message_version as u16 != expected_version as u16 {
        panic!("Message <> ConsensusVersion mismatch!");
    }
};

pub trait MessageTrait: ToBytes + FromBytes {
    /// Returns the message name.
    fn name(&self) -> Cow<'static, str>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message<N: Network> {
    BlockRequest(BlockRequest),
    BlockResponse(BlockResponse<N>),
    ChallengeRequest(ChallengeRequest<N>),
    ChallengeResponse(ChallengeResponse<N>),
    Disconnect(Disconnect),
    PeerRequest(PeerRequest),
    PeerResponse(PeerResponse),
    Ping(Ping<N>),
    Pong(Pong),
    PuzzleRequest(PuzzleRequest),
    PuzzleResponse(PuzzleResponse<N>),
    UnconfirmedSolution(UnconfirmedSolution<N>),
    UnconfirmedTransaction(UnconfirmedTransaction<N>),
}

impl<N: Network> From<DisconnectReason> for Message<N> {
    fn from(reason: DisconnectReason) -> Self {
        Self::Disconnect(Disconnect { reason })
    }
}

impl<N: Network> Message<N> {
    /// The version of the network protocol; this is incremented for breaking changes between migration versions.
    // Note. This should be incremented for each new `ConsensusVersion` that is added.
    pub const VERSIONS: [(ConsensusVersion, u32); 17] = [
        (ConsensusVersion::V5, 17),
        (ConsensusVersion::V7, 18),
        (ConsensusVersion::V8, 19),
        (ConsensusVersion::V9, 20),
        (ConsensusVersion::V10, 21),
        (ConsensusVersion::V11, 22),
        (ConsensusVersion::V12, 23),
        (ConsensusVersion::V13, 24),
        (ConsensusVersion::V14, 25),
        (ConsensusVersion::V15, 26),
        (ConsensusVersion::V16, 27),
        (ConsensusVersion::V17, 28),
        (ConsensusVersion::V18, 29),
        (ConsensusVersion::V19, 30),
        (ConsensusVersion::V20, 31),
        (ConsensusVersion::V21, 32),
        (ConsensusVersion::V22, 33),
    ];

    /// Returns the latest message version whose consensus upgrade has a scheduled block height.
    pub fn latest_message_version() -> u32 {
        Self::VERSIONS
            .iter()
            .rev()
            .find(|(consensus_version, _)| {
                N::CONSENSUS_HEIGHT(*consensus_version).is_ok_and(|height| height != u32::MAX)
            })
            .map(|(_, version)| *version)
            .unwrap_or(0)
    }

    /// Returns the lowest acceptable message version for the given block height.
    /// Example scenario:
    ///     At block height `X`, the protocol upgrades to message version from `Y-1` to `Y`.
    ///     Client A upgrades and starts using message version `Y`.
    ///     Client B has not upgraded and still uses message version `Y-1`.
    ///     Until block `X`, they stay connected and can communicate.
    ///     After block `X`, Client A will reject messages from Client B.
    pub fn lowest_accepted_message_version(current_block_height: u32) -> u32 {
        // Fetch the latest message version.
        let latest_message_version = Self::latest_message_version();

        // Fetch the versions.
        let versions = Self::VERSIONS;

        // Determine the minimum accepted message version.
        N::CONSENSUS_VERSION(current_block_height).map_or(latest_message_version, |seek_version| {
            // Search the consensus value for the specified version.
            match versions.binary_search_by(|(version, _)| version.cmp(&seek_version)) {
                // If a value was found for this consensus version, return it.
                Ok(index) => versions[index].1,
                // If the specified version was not found exactly, return the appropriate value belonging to the consensus version *lower* than the sought version.
                // If the constant is not yet in effect at this consensus version, use the earliest version.
                Err(index) => versions[index.saturating_sub(1)].1,
            }
        })
    }

    /// Returns the message type. Instance fields, such as a block height range, are omitted.
    #[inline]
    pub fn variant_name(&self) -> &'static str {
        match self {
            Self::BlockRequest(..) => "BlockRequest",
            Self::BlockResponse(..) => "BlockResponse",
            Self::ChallengeRequest(..) => "ChallengeRequest",
            Self::ChallengeResponse(..) => "ChallengeResponse",
            Self::Disconnect(..) => "Disconnect",
            Self::PeerRequest(..) => "PeerRequest",
            Self::PeerResponse(..) => "PeerResponse",
            Self::Ping(..) => "Ping",
            Self::Pong(..) => "Pong",
            Self::PuzzleRequest(..) => "PuzzleRequest",
            Self::PuzzleResponse(..) => "PuzzleResponse",
            Self::UnconfirmedSolution(..) => "UnconfirmedSolution",
            Self::UnconfirmedTransaction(..) => "UnconfirmedTransaction",
        }
    }

    /// Returns the message name.
    #[inline]
    pub fn name(&self) -> Cow<'static, str> {
        match self {
            Self::BlockRequest(message) => message.name(),
            Self::BlockResponse(message) => message.name(),
            Self::ChallengeRequest(message) => message.name(),
            Self::ChallengeResponse(message) => message.name(),
            Self::Disconnect(message) => message.name(),
            Self::PeerRequest(message) => message.name(),
            Self::PeerResponse(message) => message.name(),
            Self::Ping(message) => message.name(),
            Self::Pong(message) => message.name(),
            Self::PuzzleRequest(message) => message.name(),
            Self::PuzzleResponse(message) => message.name(),
            Self::UnconfirmedSolution(message) => message.name(),
            Self::UnconfirmedTransaction(message) => message.name(),
        }
    }

    /// Returns the message ID.
    ///
    /// This match is exhaustive over `Message`'s variants with no fallback arm, so a new variant
    /// forces an ID here - and, since [`MessageId::max_size`] has no fallback arm either, a cap -
    /// before the crate compiles. See the type-level docs on [`MessageId`] for the one part of
    /// the ID mapping this still can't enforce.
    #[inline]
    pub fn id(&self) -> MessageId {
        match self {
            Self::BlockRequest(..) => MessageId::BlockRequest,
            Self::BlockResponse(..) => MessageId::BlockResponse,
            Self::ChallengeRequest(..) => MessageId::ChallengeRequest,
            Self::ChallengeResponse(..) => MessageId::ChallengeResponse,
            Self::Disconnect(..) => MessageId::Disconnect,
            Self::PeerRequest(..) => MessageId::PeerRequest,
            Self::PeerResponse(..) => MessageId::PeerResponse,
            Self::Ping(..) => MessageId::Ping,
            Self::Pong(..) => MessageId::Pong,
            Self::PuzzleRequest(..) => MessageId::PuzzleRequest,
            Self::PuzzleResponse(..) => MessageId::PuzzleResponse,
            Self::UnconfirmedSolution(..) => MessageId::UnconfirmedSolution,
            Self::UnconfirmedTransaction(..) => MessageId::UnconfirmedTransaction,
        }
    }

    /// Checks the message byte length against the cap for its type. To be used before
    /// deserialization.
    ///
    /// An ID this node doesn't recognize at all is not rejected here - `Message::read_le` already
    /// rejects it, and reports it as an unknown message rather than a size violation.
    pub fn check_size(bytes: &[u8]) -> io::Result<()> {
        if bytes.len() < MessageId::SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid message"));
        }

        if let Some(id) = MessageId::peek(bytes)
            && bytes.len() > id.max_size::<N>()
        {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "message is too large for its type"));
        }

        Ok(())
    }
}

impl<N: Network> ToBytes for Message<N> {
    fn write_le<W: io::Write>(&self, mut writer: W) -> io::Result<()> {
        self.id().as_u16().write_le(&mut writer)?;

        match self {
            Self::BlockRequest(message) => message.write_le(writer),
            Self::BlockResponse(message) => message.write_le(writer),
            Self::ChallengeRequest(message) => message.write_le(writer),
            Self::ChallengeResponse(message) => message.write_le(writer),
            Self::Disconnect(message) => message.write_le(writer),
            Self::PeerRequest(message) => message.write_le(writer),
            Self::PeerResponse(message) => message.write_le(writer),
            Self::Ping(message) => message.write_le(writer),
            Self::Pong(message) => message.write_le(writer),
            Self::PuzzleRequest(message) => message.write_le(writer),
            Self::PuzzleResponse(message) => message.write_le(writer),
            Self::UnconfirmedSolution(message) => message.write_le(writer),
            Self::UnconfirmedTransaction(message) => message.write_le(writer),
        }
    }
}

impl<N: Network> FromBytes for Message<N> {
    fn read_le<R: io::Read>(mut reader: R) -> io::Result<Self> {
        // Read the message ID.
        let mut id_bytes = [0u8; MessageId::SIZE];
        reader.read_exact(&mut id_bytes)?;
        let id = u16::from_le_bytes(id_bytes);
        let Some(id) = MessageId::from_u16(id) else {
            return Err(error(format!("Unknown message ID {id}")));
        };

        // Deserialize the data field.
        let message = match id {
            MessageId::BlockRequest => Self::BlockRequest(BlockRequest::read_le(&mut reader)?),
            MessageId::BlockResponse => Self::BlockResponse(BlockResponse::read_le(&mut reader)?),
            MessageId::ChallengeRequest => Self::ChallengeRequest(ChallengeRequest::read_le(&mut reader)?),
            MessageId::ChallengeResponse => Self::ChallengeResponse(ChallengeResponse::read_le(&mut reader)?),
            MessageId::Disconnect => Self::Disconnect(Disconnect::read_le(&mut reader)?),
            MessageId::PeerRequest => Self::PeerRequest(PeerRequest::read_le(&mut reader)?),
            MessageId::PeerResponse => Self::PeerResponse(PeerResponse::read_le(&mut reader)?),
            MessageId::Ping => Self::Ping(Ping::read_le(&mut reader)?),
            MessageId::Pong => Self::Pong(Pong::read_le(&mut reader)?),
            MessageId::PuzzleRequest => Self::PuzzleRequest(PuzzleRequest::read_le(&mut reader)?),
            MessageId::PuzzleResponse => Self::PuzzleResponse(PuzzleResponse::read_le(&mut reader)?),
            MessageId::UnconfirmedSolution => Self::UnconfirmedSolution(UnconfirmedSolution::read_le(&mut reader)?),
            MessageId::UnconfirmedTransaction => {
                Self::UnconfirmedTransaction(UnconfirmedTransaction::read_le(&mut reader)?)
            }
        };

        // Ensure that there are no "dangling" bytes.
        #[allow(clippy::unbuffered_bytes)]
        if reader.bytes().next().is_some() {
            return Err(error("Leftover bytes in a Message"));
        }

        Ok(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use snarkvm::prelude::{CanaryV0, MainnetV0, TestnetV0};

    /// Ensure that the message versions used at genesis is correct.
    fn consensus_constants_at_genesis<N: Network>() {
        let height = 0;
        let consensus_version = Message::<N>::lowest_accepted_message_version(height);
        assert_eq!(consensus_version as usize, Message::<N>::VERSIONS.first().unwrap().1 as usize);
    }

    /// Ensure that the consensus *versions* are unique and incrementing.
    fn consensus_versions<N: Network>() {
        let mut previous_version = Message::<N>::VERSIONS.first().unwrap().0;
        for (version, _) in Message::<N>::VERSIONS.iter().skip(1) {
            assert!(*version as usize > previous_version as usize);
            previous_version = *version;
        }
    }

    /// Ensure that *message versions* are unique and incrementing by 1.
    fn consensus_constants_increasing_heights<N: Network>() {
        let mut previous_message_version = Message::<N>::VERSIONS.first().unwrap().1;
        for (_consensus_version, message_version) in Message::<N>::VERSIONS.iter().skip(1) {
            assert_eq!(*message_version, previous_message_version + 1);
            previous_message_version = *message_version;
        }
    }

    fn latest_message_version_has_scheduled_height<N: Network>() {
        let latest_message_version = Message::<N>::latest_message_version();
        let (consensus_version, _) =
            Message::<N>::VERSIONS.iter().find(|(_, version)| *version == latest_message_version).unwrap();
        assert_ne!(N::CONSENSUS_HEIGHT(*consensus_version).unwrap(), u32::MAX);

        for (consensus_version, message_version) in Message::<N>::VERSIONS {
            if message_version > latest_message_version {
                assert_eq!(N::CONSENSUS_HEIGHT(consensus_version).unwrap(), u32::MAX);
            }
        }
    }

    #[test]
    fn test_latest_message_version_has_scheduled_height() {
        latest_message_version_has_scheduled_height::<MainnetV0>();
        latest_message_version_has_scheduled_height::<TestnetV0>();
        latest_message_version_has_scheduled_height::<CanaryV0>();
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_consensus_constants() {
        consensus_constants_at_genesis::<MainnetV0>();
        consensus_constants_at_genesis::<TestnetV0>();
        consensus_constants_at_genesis::<CanaryV0>();

        consensus_versions::<MainnetV0>();
        consensus_versions::<TestnetV0>();
        consensus_versions::<CanaryV0>();

        consensus_constants_increasing_heights::<MainnetV0>();
        consensus_constants_increasing_heights::<TestnetV0>();
        consensus_constants_increasing_heights::<CanaryV0>();
    }
}
