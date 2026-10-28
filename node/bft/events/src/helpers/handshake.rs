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

//! The payloads carried by the messages of the gateway's Noise handshake.
//!
//! These payloads are not part of the [`Event`] enum. They are only ever exchanged inside a Noise
//! message, never over an established connection.
//!
//! The four messages are:
//!
//! 1. `->` [`HandshakeHint`] - cleartext, so the responder can bail out before doing any work.
//! 2. `<-` [`ResponderInfo`] - the responder's metadata, deliberately without a signature.
//! 3. `->` [`InitiatorInfo`] - the initiator's metadata *and* the proof of its identity.
//! 4. `<-` [`ResponderProof`] - the responder's verdict, and its own proof if the initiator checked
//!    out. This is the first transport message, the pattern having ended with the third.
//!
//! # Important: how the initiator's metadata is bound to its signature
//!
//! The initiator signs the handshake hash as it stands after message 2. Its metadata is tied to that
//! signature by one of two mechanisms, depending on which message carries it:
//!
//! - Messages 1 and 2 are folded into the hash - Noise mixes a message's payload into `h` whether or
//!   not a key has been established yet, so even the cleartext [`HandshakeHint`] is covered by the
//!   signature. Tampering with it in flight makes the two sides derive different hashes, and the
//!   handshake fails.
//! - Message 3 is *not* covered by the hash the initiator signed, and is bound instead by its AEAD
//!   tag: only the two endpoints can derive the key, so the party that produced the signature is
//!   necessarily the party that sent [`InitiatorInfo`] alongside it.
//!
//! Either placement is sound, so metadata may be moved into the hint if there is a reason to act on
//! it earlier. What must not change is that anything the responder acts on ends up covered by one of
//! the two: metadata reaching it unencrypted *after* message 2 would be bound by neither.
//!
//! # Fields after the original layout
//!
//! Peers that predate a field ignore whatever follows the fields they know, as handshake payloads
//! are decoded without a check for leftover bytes. Fields added since are therefore appended to the
//! end of messages 2 and 3, behind a length prefix, as the [`HandshakeTrailer`]. A field must not go
//! into [`PeerInfo`] itself: in message 3 it is followed by the signature, which an older peer would
//! then misread.
//!
//! The separate reason the responder re-checks the hint against [`InitiatorInfo`] is a
//! self-inconsistent initiator, not tampering. Nothing stops a peer from claiming one value in the
//! hint, having the responder run its early checks against it, and then authenticating a different
//! one - so every field duplicated between the two must be compared, and the connection rejected
//! when they disagree.

use crate::{Disconnect, DisconnectReason, Event};
use snarkos_node_network::ConnectionMode;
use snarkos_node_tcp::ConnectError;
use snarkvm::{
    console::prelude::{FromBytes, Network, Read, ToBytes, Write, io_error},
    ledger::narwhal::Data,
    prelude::{Address, Field, Signature},
};

use tracing::warn;

use std::{io::Result as IoResult, net::SocketAddr};

/// The domain separator for the signatures binding an Aleo address to a gateway Noise session.
pub const HANDSHAKE_DOMAIN: &[u8] = b"snarkos-bft-handshake-v1";

/// Serializes a handshake payload for transmission inside a Noise message.
pub fn encode_payload<T: ToBytes>(payload: &T) -> Result<Vec<u8>, ConnectError> {
    payload.to_bytes_le().map_err(ConnectError::other)
}

/// Deserializes a handshake payload received inside a Noise message.
pub fn decode_payload<T: FromBytes>(peer_addr: SocketAddr, bytes: &[u8]) -> Result<T, ConnectError> {
    T::from_bytes_le(bytes)
        .map_err(|err| ConnectError::other(format!("'{peer_addr}' sent a malformed handshake payload: {err}")))
}

/// Constant for an unknown commit hash.
const UNKNOWN_COMMIT_HASH: [u8; 40] = [b'?'; 40];

/// The cleartext payload of the first handshake message.
///
/// It is neither encrypted nor authenticated, so it must be treated as a claim rather than a fact.
/// It exists so that the responder can turn away peers it would never accept - self-connects,
/// outdated versions, untrusted or non-committee addresses, peers it is already connected to -
/// before performing a single Diffie-Hellman operation. Every field reappears in [`InitiatorInfo`],
/// where it is authenticated, and the responder must reject the connection if the two disagree;
/// otherwise this message would be a way to be checked as one peer and admitted as another.
///
/// The payload opens with the connection mode, so that a bootstrap client - which accepts both the
/// gateway's and the router's connections on one listener - knows which hint it is reading before it
/// parses the rest; see [`ConnectionMode`]. A hint for a different mode is rejected by name
/// rather than misread as this one.
///
/// Disclosing the Aleo address here costs little: committee membership is public on-chain, and the
/// responder discloses its own address to an unauthenticated peer in the second message anyway.
/// What it buys is the committee check, which is the one cheap test that a peer cannot satisfy by
/// guessing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HandshakeHint<N: Network> {
    pub version: u32,
    pub listener_port: u16,
    pub address: Address<N>,
}

impl<N: Network> HandshakeHint<N> {
    /// The connection mode this hint describes; the gateway's handshake only ever serves one.
    const MODE: ConnectionMode = ConnectionMode::Gateway;
}

impl<N: Network> ToBytes for HandshakeHint<N> {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        Self::MODE.write_le(&mut writer)?;
        self.version.write_le(&mut writer)?;
        self.listener_port.write_le(&mut writer)?;
        self.address.write_le(&mut writer)
    }
}

impl<N: Network> FromBytes for HandshakeHint<N> {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        let mode = ConnectionMode::read_le(&mut reader)?;
        if mode != Self::MODE {
            return Err(io_error(format!("expected a {}-mode handshake, got {mode}", Self::MODE)));
        }

        let version = u32::read_le(&mut reader)?;
        let listener_port = u16::read_le(&mut reader)?;
        let address = Address::<N>::read_le(&mut reader)?;

        Ok(Self { version, listener_port, address })
    }
}

/// What a party discloses about itself during the handshake.
///
/// This is the payload of the second message on its own, and part of the payload of the third; it
/// is also what a completed handshake hands back to the gateway.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeerInfo<N: Network> {
    pub version: u32,
    pub listener_port: u16,
    pub address: Address<N>,
    pub restrictions_id: Field<N>,
    pub snarkos_sha: Option<[u8; 40]>,
}

impl<N: Network> PeerInfo<N> {
    pub fn new(
        listener_port: u16,
        address: Address<N>,
        restrictions_id: Field<N>,
        snarkos_sha: Option<[u8; 40]>,
    ) -> Self {
        Self { version: Event::<N>::VERSION, listener_port, address, restrictions_id, snarkos_sha }
    }
}

impl<N: Network> ToBytes for PeerInfo<N> {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        self.version.write_le(&mut writer)?;
        self.listener_port.write_le(&mut writer)?;
        self.address.write_le(&mut writer)?;
        self.restrictions_id.write_le(&mut writer)?;
        // Serialize `None` as a constant.
        self.snarkos_sha.unwrap_or(UNKNOWN_COMMIT_HASH).write_le(&mut writer)
    }
}

impl<N: Network> FromBytes for PeerInfo<N> {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        let version = u32::read_le(&mut reader)?;
        let listener_port = u16::read_le(&mut reader)?;
        let address = Address::<N>::read_le(&mut reader)?;
        let restrictions_id = Field::read_le(&mut reader)?;
        // A missing SHA is an error rather than a `None`: the payload is delimited by the Noise
        // message, so a short read is a protocol violation.
        let snarkos_sha = <[u8; 40]>::read_le(&mut reader)?;
        let snarkos_sha = if snarkos_sha == UNKNOWN_COMMIT_HASH { None } else { Some(snarkos_sha) };

        Ok(Self { version, listener_port, address, restrictions_id, snarkos_sha })
    }
}

/// The activation height of each consensus version that a build schedules, in version order,
/// starting with `ConsensusVersion::V1`. A version that is not scheduled has the height `u32::MAX`.
///
/// Versions are positions rather than `ConsensusVersion`s, so that a schedule that names a version
/// this build does not define still decodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsensusSchedule {
    heights: Vec<u32>,
}

impl ConsensusSchedule {
    /// Initializes a schedule from the activation heights of versions 1, 2, and so on.
    ///
    /// The number of versions is encoded as a `u8`. In practice, the 1024-byte limit on handshake
    /// messages caps a schedule at about 150 versions.
    pub fn new(heights: Vec<u32>) -> IoResult<Self> {
        u8::try_from(heights.len()).map_err(io_error)?;
        Ok(Self { heights })
    }

    /// Returns the schedule of this build.
    pub fn of<N: Network>() -> Self {
        // `N::CONSENSUS_HEIGHT` indexes the table by version, so it is in version order.
        Self { heights: N::CONSENSUS_VERSION_HEIGHTS().iter().map(|(_, height)| *height).collect() }
    }

    /// Returns the activation heights of versions 1, 2, and so on.
    pub fn heights(&self) -> &[u32] {
        &self.heights
    }
}

impl ToBytes for ConsensusSchedule {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        let num_versions = u8::try_from(self.heights.len()).map_err(io_error)?;
        num_versions.write_le(&mut writer)?;
        self.heights.iter().try_for_each(|height| height.write_le(&mut writer))
    }
}

impl FromBytes for ConsensusSchedule {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        let num_versions = u8::read_le(&mut reader)?;
        let heights = (0..num_versions).map(|_| u32::read_le(&mut reader)).collect::<IoResult<_>>()?;
        Ok(Self { heights })
    }
}

/// The fields appended to messages 2 and 3 after their original layout.
///
/// It is written as a `u16` length followed by the fields, so that fields added later are skipped by
/// peers that only know the earlier ones. A missing or malformed trailer decodes as one without
/// fields. That is only acceptable from a peer below [`Self::FIRST_VERSION`]; the gateway rejects a
/// peer at or above it that sends no schedule.
///
/// The responder's trailer, in message 2, is covered by the responder's signature. The initiator's
/// trailer, in message 3, is not covered by the initiator's signature, which is made before that
/// message; the AEAD of the Noise session binds it, like the rest of message 3.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HandshakeTrailer {
    /// The consensus schedule of the sender's build.
    pub schedule: Option<ConsensusSchedule>,
}

impl HandshakeTrailer {
    /// The first event version whose handshake messages 2 and 3 must carry a trailer with a schedule.
    pub const FIRST_VERSION: u32 = 11;

    /// Returns the trailer that this build sends.
    pub fn of<N: Network>() -> Self {
        Self { schedule: Some(ConsensusSchedule::of::<N>()) }
    }

    /// Parses a trailer, or returns `None` if it is malformed.
    fn parse(bytes: &[u8]) -> Option<Self> {
        let (length, rest) = bytes.split_first_chunk::<2>()?;
        let fields = rest.get(..usize::from(u16::from_le_bytes(*length)))?;
        let schedule = ConsensusSchedule::read_le(fields).ok()?;
        Some(Self { schedule: Some(schedule) })
    }
}

impl ToBytes for HandshakeTrailer {
    /// Writes the trailer, unless it holds no field.
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        let Some(schedule) = &self.schedule else {
            return Ok(());
        };
        let fields = schedule.to_bytes_le().map_err(io_error)?;
        u16::try_from(fields.len()).map_err(io_error)?.write_le(&mut writer)?;
        writer.write_all(&fields)
    }
}

impl FromBytes for HandshakeTrailer {
    /// Reads the trailer from the remainder of a payload.
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        let mut remainder = Vec::new();
        reader.read_to_end(&mut remainder)?;
        if remainder.is_empty() {
            return Ok(Self::default());
        }
        match Self::parse(&remainder) {
            Some(trailer) => Ok(trailer),
            None => {
                warn!("Ignoring a malformed handshake trailer of {} bytes", remainder.len());
                Ok(Self::default())
            }
        }
    }
}

/// The payload of the second handshake message: the responder's metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResponderInfo<N: Network> {
    pub info: PeerInfo<N>,
    pub trailer: HandshakeTrailer,
}

impl<N: Network> ToBytes for ResponderInfo<N> {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        self.info.write_le(&mut writer)?;
        self.trailer.write_le(&mut writer)
    }
}

impl<N: Network> FromBytes for ResponderInfo<N> {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        let info = PeerInfo::read_le(&mut reader)?;
        let trailer = HandshakeTrailer::read_le(&mut reader)?;

        Ok(Self { info, trailer })
    }
}

/// The payload of the third handshake message: the initiator's metadata, plus the signature that
/// binds its Aleo address to this session.
///
/// The initiator signs first by design. Verifying the signature is the most expensive thing the
/// responder does, and it only reaches that point once every cheap check against `info` has passed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitiatorInfo<N: Network> {
    pub info: PeerInfo<N>,
    pub signature: Data<Signature<N>>,
    pub trailer: HandshakeTrailer,
}

impl<N: Network> ToBytes for InitiatorInfo<N> {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        self.info.write_le(&mut writer)?;
        self.signature.write_le(&mut writer)?;
        self.trailer.write_le(&mut writer)
    }
}

impl<N: Network> FromBytes for InitiatorInfo<N> {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        let info = PeerInfo::read_le(&mut reader)?;
        let signature = Data::read_le(&mut reader)?;
        let trailer = HandshakeTrailer::read_le(&mut reader)?;

        Ok(Self { info, signature, trailer })
    }
}

/// The payload of the fourth handshake message: the responder's verdict.
///
/// The responder's proof is sent separately from its [`PeerInfo`], and last, so that it never signs
/// anything for a peer that has not already authenticated itself. If the initiator was turned away
/// instead, this message carries the reason: the initiator dialed in and has no other way to find
/// out, whereas a responder that gets rejected is owed nothing and is simply dropped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResponderProof<N: Network> {
    /// The initiator was accepted; the signature binds the responder's Aleo address to the session.
    Accepted { signature: Data<Signature<N>> },
    /// The initiator was rejected.
    Rejected { reason: DisconnectReason },
}

impl<N: Network> ToBytes for ResponderProof<N> {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        match self {
            Self::Accepted { signature } => {
                0u8.write_le(&mut writer)?;
                signature.write_le(&mut writer)
            }
            Self::Rejected { reason } => {
                1u8.write_le(&mut writer)?;
                // Reuse the event's encoding of the reason, which rejects the unknown variant.
                Disconnect::from(*reason).write_le(&mut writer)
            }
        }
    }
}

impl<N: Network> FromBytes for ResponderProof<N> {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        match u8::read_le(&mut reader)? {
            0 => Ok(Self::Accepted { signature: Data::read_le(&mut reader)? }),
            1 => Ok(Self::Rejected { reason: Disconnect::read_le(&mut reader)?.reason }),
            variant => Err(io_error(format!("Invalid responder proof variant ({variant})"))),
        }
    }
}

#[cfg(test)]
pub mod prop_tests {
    use super::*;
    use crate::{challenge_request::prop_tests::any_valid_address, challenge_response::prop_tests::any_signature};

    use snarkvm::{
        console::prelude::{FromBytes, ToBytes},
        prelude::{Address, Field, TestRng, Uniform},
    };

    use bytes::{Buf, BufMut, BytesMut};
    use proptest::{
        collection,
        prelude::{BoxedStrategy, Strategy, any},
    };
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    pub fn any_schedule() -> BoxedStrategy<ConsensusSchedule> {
        collection::vec(any::<u32>(), 1..=40).prop_map(|heights| ConsensusSchedule::new(heights).unwrap()).boxed()
    }

    pub fn any_trailer() -> BoxedStrategy<HandshakeTrailer> {
        proptest::option::of(any_schedule()).prop_map(|schedule| HandshakeTrailer { schedule }).boxed()
    }

    pub fn any_peer_info() -> BoxedStrategy<PeerInfo<CurrentNetwork>> {
        (any_valid_address(), any::<u32>(), any::<u16>(), any::<u64>(), collection::vec(0u8..=127, 40))
            .prop_map(|(address, version, listener_port, seed, sha)| {
                let sha: [u8; 40] = sha.try_into().unwrap();
                let snarkos_sha = if sha == UNKNOWN_COMMIT_HASH { None } else { Some(sha) };
                PeerInfo {
                    version,
                    listener_port,
                    address,
                    restrictions_id: Field::rand(&mut TestRng::fixed(seed)),
                    snarkos_sha,
                }
            })
            .boxed()
    }

    #[proptest]
    fn handshake_hint_serialize_deserialize(
        #[strategy(any::<(u32, u16)>())] fields: (u32, u16),
        #[strategy(any_valid_address())] address: Address<CurrentNetwork>,
    ) {
        let original = HandshakeHint { version: fields.0, listener_port: fields.1, address };

        let mut buf = BytesMut::default().writer();
        original.write_le(&mut buf).unwrap();

        let deserialized = HandshakeHint::<CurrentNetwork>::read_le(buf.into_inner().reader()).unwrap();
        assert_eq!(original, deserialized);
    }

    #[proptest]
    fn peer_info_serialize_deserialize(#[strategy(any_peer_info())] original: PeerInfo<CurrentNetwork>) {
        let mut buf = BytesMut::default().writer();
        original.write_le(&mut buf).unwrap();

        let deserialized = PeerInfo::read_le(buf.into_inner().reader()).unwrap();
        assert_eq!(original, deserialized);
    }

    #[proptest]
    fn initiator_info_serialize_deserialize(
        #[strategy(any_peer_info())] info: PeerInfo<CurrentNetwork>,
        #[strategy(any_signature())] signature: snarkvm::prelude::Signature<CurrentNetwork>,
        #[strategy(any_trailer())] trailer: HandshakeTrailer,
    ) {
        let original = InitiatorInfo { info, signature: Data::Object(signature), trailer };

        let mut buf = BytesMut::default().writer();
        original.write_le(&mut buf).unwrap();

        let deserialized = InitiatorInfo::<CurrentNetwork>::read_le(buf.into_inner().reader()).unwrap();
        assert_eq!(original.info, deserialized.info);
        assert_eq!(original.trailer, deserialized.trailer);
        assert_eq!(
            original.signature.deserialize_blocking().unwrap(),
            deserialized.signature.deserialize_blocking().unwrap()
        );
    }

    #[proptest]
    fn responder_info_serialize_deserialize(
        #[strategy(any_peer_info())] info: PeerInfo<CurrentNetwork>,
        #[strategy(any_trailer())] trailer: HandshakeTrailer,
    ) {
        let original = ResponderInfo { info, trailer };
        let deserialized = ResponderInfo::<CurrentNetwork>::from_bytes_le(&original.to_bytes_le().unwrap()).unwrap();
        assert_eq!(original, deserialized);
    }

    #[proptest]
    fn a_legacy_peer_reads_the_responder_info(
        #[strategy(any_peer_info())] info: PeerInfo<CurrentNetwork>,
        #[strategy(any_trailer())] trailer: HandshakeTrailer,
    ) {
        let bytes = ResponderInfo { info, trailer }.to_bytes_le().unwrap();
        assert_eq!(PeerInfo::<CurrentNetwork>::from_bytes_le(&bytes).unwrap(), info);
    }

    #[proptest]
    fn a_legacy_peer_reads_the_initiator_info(
        #[strategy(any_peer_info())] info: PeerInfo<CurrentNetwork>,
        #[strategy(any_signature())] signature: snarkvm::prelude::Signature<CurrentNetwork>,
        #[strategy(any_trailer())] trailer: HandshakeTrailer,
    ) {
        let bytes = InitiatorInfo { info, signature: Data::Object(signature), trailer }.to_bytes_le().unwrap();

        // A peer that predates the trailer reads the original layout, and ignores what follows it.
        let mut reader = &bytes[..];
        assert_eq!(PeerInfo::<CurrentNetwork>::read_le(&mut reader).unwrap(), info);
        let legacy_signature = Data::<snarkvm::prelude::Signature<CurrentNetwork>>::read_le(&mut reader).unwrap();
        assert_eq!(legacy_signature.deserialize_blocking().unwrap(), signature);
    }

    #[proptest]
    fn a_payload_from_a_legacy_peer_has_no_trailer(
        #[strategy(any_peer_info())] info: PeerInfo<CurrentNetwork>,
        #[strategy(any_signature())] signature: snarkvm::prelude::Signature<CurrentNetwork>,
    ) {
        let responder = ResponderInfo::<CurrentNetwork>::from_bytes_le(&info.to_bytes_le().unwrap()).unwrap();
        assert_eq!(responder.trailer, HandshakeTrailer::default());

        let mut legacy = info.to_bytes_le().unwrap();
        legacy.extend(Data::Object(signature).to_bytes_le().unwrap());
        let initiator = InitiatorInfo::<CurrentNetwork>::from_bytes_le(&legacy).unwrap();
        assert_eq!(initiator.trailer, HandshakeTrailer::default());
    }

    #[proptest]
    fn a_malformed_trailer_reads_as_none(
        #[strategy(any_peer_info())] info: PeerInfo<CurrentNetwork>,
        #[strategy(any_schedule())] schedule: ConsensusSchedule,
    ) {
        let fields = schedule.to_bytes_le().unwrap();
        let length = u16::try_from(fields.len()).unwrap();
        let malformed = [
            // A length without fields.
            length.to_le_bytes().to_vec(),
            // Fewer fields than the length announces.
            [&(length + 1).to_le_bytes()[..], &fields].concat(),
            // A schedule with fewer heights than it announces.
            [&(length - 1).to_le_bytes()[..], &fields[..fields.len() - 1]].concat(),
            // A single byte.
            vec![7],
        ];
        for trailer in malformed {
            let bytes = [info.to_bytes_le().unwrap(), trailer].concat();
            let decoded = ResponderInfo::<CurrentNetwork>::from_bytes_le(&bytes).unwrap();
            assert_eq!(decoded, ResponderInfo { info, trailer: HandshakeTrailer::default() });
        }
    }

    #[proptest]
    fn fields_appended_to_the_trailer_are_skipped(
        #[strategy(any_peer_info())] info: PeerInfo<CurrentNetwork>,
        #[strategy(any_schedule())] schedule: ConsensusSchedule,
        #[strategy(collection::vec(any::<u8>(), 1..16))] later_fields: Vec<u8>,
    ) {
        let fields = [schedule.to_bytes_le().unwrap(), later_fields].concat();
        let length = u16::try_from(fields.len()).unwrap();
        let bytes = [info.to_bytes_le().unwrap(), length.to_le_bytes().to_vec(), fields].concat();
        let decoded = ResponderInfo::<CurrentNetwork>::from_bytes_le(&bytes).unwrap();
        assert_eq!(decoded.trailer.schedule, Some(schedule));
    }

    #[test]
    fn the_schedule_of_this_build_lists_every_version_in_order() {
        let schedule = ConsensusSchedule::of::<CurrentNetwork>();
        let table = CurrentNetwork::CONSENSUS_VERSION_HEIGHTS();
        assert_eq!(schedule.heights().len(), table.len());
        for (index, (version, height)) in table.iter().enumerate() {
            assert_eq!(*version as usize, index + 1);
            assert_eq!(schedule.heights()[index], *height);
        }
    }

    #[test]
    fn a_schedule_holds_at_most_as_many_versions_as_a_u8_counts() {
        assert!(ConsensusSchedule::new(vec![0; u8::MAX as usize]).is_ok());
        assert!(ConsensusSchedule::new(vec![0; u8::MAX as usize + 1]).is_err());
    }

    #[proptest]
    fn accepted_responder_proof_serialize_deserialize(
        #[strategy(any_signature())] signature: snarkvm::prelude::Signature<CurrentNetwork>,
    ) {
        let original = ResponderProof::<CurrentNetwork>::Accepted { signature: Data::Object(signature) };

        let mut buf = BytesMut::default().writer();
        original.write_le(&mut buf).unwrap();

        let deserialized = ResponderProof::<CurrentNetwork>::read_le(buf.into_inner().reader()).unwrap();
        let (ResponderProof::Accepted { signature: original }, ResponderProof::Accepted { signature: deserialized }) =
            (original, deserialized)
        else {
            panic!("expected an accepted proof");
        };
        assert_eq!(original.deserialize_blocking().unwrap(), deserialized.deserialize_blocking().unwrap());
    }

    #[test]
    fn rejected_responder_proof_serialize_deserialize() {
        for reason in [
            DisconnectReason::OutdatedClientVersion,
            DisconnectReason::UnauthorizedValidator,
            DisconnectReason::InvalidChallengeResponse,
        ] {
            let original = ResponderProof::<CurrentNetwork>::Rejected { reason };

            let mut buf = BytesMut::default().writer();
            original.write_le(&mut buf).unwrap();

            let deserialized = ResponderProof::<CurrentNetwork>::read_le(buf.into_inner().reader()).unwrap();
            assert_eq!(original, deserialized);
        }
    }
}
