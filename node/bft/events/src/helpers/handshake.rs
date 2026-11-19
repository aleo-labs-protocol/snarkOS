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
//! 2. `<-` [`CborResponderInfo`] or [`ResponderInfo`] - the responder's metadata, deliberately
//!    without a signature.
//! 3. `->` [`CborInitiatorInfo`] or [`InitiatorInfo`] - the initiator's metadata *and* the proof of
//!    its identity.
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
//! # Encodings of messages 2 and 3
//!
//! Messages 2 and 3 open with the sender's event version as a raw little-endian `u32`. The receiver
//! reads it, and decodes the rest of the payload in one of two encodings accordingly; see
//! [`decode_responder_info`] and [`decode_initiator_info`]. The sender knows which encoding the
//! receiver reads: the responder learns the initiator's version from the hint, and the initiator
//! learns the responder's from message 2.
//!
//! - A peer at [`HandshakeTrailer::CBOR_VERSION`] or above is sent the version followed by a single
//!   CBOR map: [`CborResponderInfo`] or [`CborInitiatorInfo`].
//! - An older peer is sent the original layout: [`ResponderInfo`] or [`InitiatorInfo`], whose first
//!   field is the version. That version is still this build's, so the older peer holds the payload to
//!   every requirement it knows for this version, all of which the trailer below meets.
//!
//! Messages 1 and 4 have a single encoding each. The hint must stay readable by every peer at
//! [`Event::MINIMUM_VERSION`] or above, as the initiator sends it before it knows anything about the
//! responder.
//!
//! ## The CBOR map
//!
//! The map only stays readable across versions under these rules:
//!
//! - Unknown keys are skipped, which is how a peer reads a map from a later version. The map types
//!   must never deny unknown fields.
//! - A field added after [`HandshakeTrailer::CBOR_VERSION`] must be optional or defaulted, as a peer
//!   that predates it does not send it. A field that becomes mandatory from some version on is
//!   required by the gateway's `verify_peer_info`, against the peer's version, the way it requires
//!   the fields of the trailer.
//! - Every key is pinned with a `rename`, so that renaming a Rust field does not change the format.
//! - Every key is a text string. A key sent as a byte string reads as the text of the same bytes, and
//!   a tag on a key is ignored. A key of any other type, such as an integer, fails the whole map, so a
//!   later version cannot add one.
//! - A key must also have a definite length of at most 4096 bytes, and the value of a key must not
//!   hold a simple value other than `false`, `true`, `null` and `undefined`. A peer cannot skip a key
//!   that breaks either rule, so it fails the whole map.
//! - The map holds no enum whose variants a peer may not know, as an unknown variant fails the whole
//!   map.
//!
//! The fields that every peer at [`HandshakeTrailer::CBOR_VERSION`] sends are required; a map that
//! lacks one is malformed. So are bytes after the map.
//!
//! ## The original layout
//!
//! Peers that predate a field ignore whatever follows the fields they know, as this layout is decoded
//! without a check for leftover bytes. Fields added since are therefore appended to the end of
//! messages 2 and 3, behind a length prefix, as the [`HandshakeTrailer`]. A field must not go into
//! [`PeerInfo`] itself: in message 3 it is followed by the signature, which an older peer would then
//! misread.
//!
//! # Re-checking the hint
//!
//! The separate reason the responder re-checks the hint against message 3 is a self-inconsistent
//! initiator, not tampering. Nothing stops a peer from claiming one value in the hint, having the
//! responder run its early checks against it, and then authenticating a different one - so every
//! field duplicated between the two must be compared, and the connection rejected when they
//! disagree.

use crate::{Disconnect, DisconnectReason, Event};
use snarkos_node_network::ConnectionMode;
use snarkos_node_tcp::ConnectError;
use snarkvm::{
    console::prelude::{FromBytes, Network, Read, SizeInBytes, ToBytes, ToBytesSerializer, Write, io_error},
    ledger::narwhal::Data,
    prelude::{Address, Field, Signature},
};

use bytes::Bytes;
use serde::{
    Deserialize,
    Deserializer,
    Serialize,
    Serializer,
    de::{self, DeserializeOwned, SeqAccess, Visitor},
};
use tracing::warn;

use std::{fmt, io::Result as IoResult, net::SocketAddr};

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

/// Returns `true` if messages 2 and 3 from a sender at `sender_version` to a receiver at
/// `receiver_version` are a CBOR map.
fn reads_cbor(sender_version: u32, receiver_version: u32) -> bool {
    sender_version.min(receiver_version) >= HandshakeTrailer::CBOR_VERSION
}

/// Returns the sender's event version at the head of message 2 or 3, or 0 if the message is too
/// short to hold one.
fn payload_version(bytes: &[u8]) -> u32 {
    bytes.first_chunk::<4>().map_or(0, |version| u32::from_le_bytes(*version))
}

/// Encodes message 2 in the encoding that a peer at `peer_version` reads.
pub fn encode_responder_info<N: Network>(
    peer_version: u32,
    info: PeerInfo<N>,
    schedule: ConsensusSchedule,
    genesis_hash: [u8; 32],
) -> Result<Vec<u8>, ConnectError> {
    if reads_cbor(info.version, peer_version) {
        encode_payload(&Versioned(info.version, CborResponderInfo::from_parts(info, schedule, genesis_hash)))
    } else {
        encode_payload(&ResponderInfo { info, trailer: HandshakeTrailer::new(schedule, genesis_hash) })
    }
}

/// Decodes message 2, as received by a party at `our_version`.
pub fn decode_responder_info<N: Network>(
    our_version: u32,
    peer_addr: SocketAddr,
    bytes: &[u8],
) -> Result<ResponderInfo<N>, ConnectError> {
    if reads_cbor(payload_version(bytes), our_version) {
        let Versioned(version, payload) = decode_payload::<Versioned<CborResponderInfo<N>>>(peer_addr, bytes)?;
        Ok(payload.into_parts(version))
    } else {
        decode_payload(peer_addr, bytes)
    }
}

/// Encodes message 3 in the encoding that a peer at `peer_version` reads.
pub fn encode_initiator_info<N: Network>(
    peer_version: u32,
    info: PeerInfo<N>,
    schedule: ConsensusSchedule,
    genesis_hash: [u8; 32],
    signature: Data<Signature<N>>,
) -> Result<Vec<u8>, ConnectError> {
    if reads_cbor(info.version, peer_version) {
        let mut signature_bytes = Vec::new();
        signature.serialize_blocking_into(&mut signature_bytes).map_err(ConnectError::other)?;
        let payload = CborInitiatorInfo::from_parts(info, schedule, genesis_hash, signature_bytes);
        encode_payload(&Versioned(info.version, payload))
    } else {
        encode_payload(&InitiatorInfo { info, signature, trailer: HandshakeTrailer::new(schedule, genesis_hash) })
    }
}

/// Decodes message 3, as received by a party at `our_version`.
pub fn decode_initiator_info<N: Network>(
    our_version: u32,
    peer_addr: SocketAddr,
    bytes: &[u8],
) -> Result<InitiatorInfo<N>, ConnectError> {
    if reads_cbor(payload_version(bytes), our_version) {
        let Versioned(version, payload) = decode_payload::<Versioned<CborInitiatorInfo<N>>>(peer_addr, bytes)?;
        // The signature is parsed later, by a parser that ignores bytes left over after it.
        let RawBytes(signature) = &payload.signature;
        if signature.len() != Signature::<N>::size_in_bytes() {
            return Err(ConnectError::other(format!(
                "'{peer_addr}' sent a malformed handshake payload: a signature of {} bytes",
                signature.len()
            )));
        }
        Ok(payload.into_parts(version))
    } else {
        decode_payload(peer_addr, bytes)
    }
}

/// Constant for an unknown commit hash.
const UNKNOWN_COMMIT_HASH: [u8; 40] = [b'?'; 40];

/// A value that serde encodes as a byte string of its little-endian bytes.
///
/// The serde impls of snarkVM types write a sequence of single bytes in a binary format, which CBOR
/// encodes as an array of integers. Decoding goes through the checked `read_le`, and rejects bytes
/// left over after the value.
#[derive(Clone, Debug, PartialEq, Eq)]
struct AsBytes<T>(T);

impl<T: ToBytes> Serialize for AsBytes<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        ToBytesSerializer::serialize_with_size_encoding(&self.0, serializer)
    }
}

impl<'de, T: FromBytes> Deserialize<'de> for AsBytes<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let RawBytes(bytes) = RawBytes::deserialize(deserializer)?;
        let mut reader = &bytes[..];
        let value = T::read_le(&mut reader).map_err(de::Error::custom)?;
        if !reader.is_empty() {
            return Err(de::Error::custom(format!("{} bytes left over after the value", reader.len())));
        }
        Ok(Self(value))
    }
}

/// Bytes that serde encodes as a byte string, and leaves unparsed.
#[derive(Clone, Debug, PartialEq, Eq)]
struct RawBytes(Vec<u8>);

impl Serialize for RawBytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for RawBytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_byte_buf(RawBytesVisitor).map(Self)
    }
}

/// Accepts a byte string, and nothing else: in particular not an array of integers.
struct RawBytesVisitor;

impl Visitor<'_> for RawBytesVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a byte string")
    }

    fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
        Ok(bytes.to_vec())
    }

    fn visit_byte_buf<E: de::Error>(self, bytes: Vec<u8>) -> Result<Self::Value, E> {
        Ok(bytes)
    }
}

/// A CBOR map, preceded by the sender's event version as a raw little-endian `u32`.
struct Versioned<T>(u32, T);

impl<T: Serialize> ToBytes for Versioned<T> {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        self.0.write_le(&mut writer)?;
        ciborium::into_writer(&self.1, writer).map_err(io_error)
    }
}

impl<T: DeserializeOwned> FromBytes for Versioned<T> {
    /// Reads the version and the map, and rejects any bytes after the map.
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        let version = u32::read_le(&mut reader)?;
        let map = ciborium::from_reader(&mut reader).map_err(io_error)?;
        if reader.read(&mut [0u8])? != 0 {
            return Err(io_error("bytes left over after the CBOR map"));
        }
        Ok(Self(version, map))
    }
}

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
    /// The most versions a schedule holds, which is the most the original layout can count. A peer's
    /// schedule with more is malformed, and this build fails to send one.
    ///
    /// The size of a handshake message is the tighter limit in practice: a message 3 whose other
    /// fields are at their longest has room for about 120 versions whose heights are all `u32::MAX`.
    /// See the test `this_build_leaves_room_for_more_versions_in_a_handshake_message`.
    pub const MAX_VERSIONS: usize = u8::MAX as usize;

    /// Initializes a schedule from the activation heights of versions 1, 2, and so on, of which
    /// there are at most [`Self::MAX_VERSIONS`].
    pub fn new(heights: Vec<u32>) -> IoResult<Self> {
        if heights.len() > Self::MAX_VERSIONS {
            return Err(io_error(format!("a schedule of {} versions exceeds {}", heights.len(), Self::MAX_VERSIONS)));
        }
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
        Self::new((0..num_versions).map(|_| u32::read_le(&mut reader)).collect::<IoResult<_>>()?)
    }
}

impl Serialize for ConsensusSchedule {
    /// Writes the heights as an array, failing if there are more than [`Self::MAX_VERSIONS`].
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.heights.len() > Self::MAX_VERSIONS {
            let message = format!("a schedule of {} versions exceeds {}", self.heights.len(), Self::MAX_VERSIONS);
            return Err(serde::ser::Error::custom(message));
        }
        self.heights.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ConsensusSchedule {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_seq(ScheduleVisitor)
    }
}

/// Reads a schedule, failing as soon as it holds more than [`ConsensusSchedule::MAX_VERSIONS`]
/// versions. A peer can announce any length, so the length is never trusted for an allocation.
struct ScheduleVisitor;

impl<'de> Visitor<'de> for ScheduleVisitor {
    type Value = ConsensusSchedule;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "an array of at most {} activation heights", ConsensusSchedule::MAX_VERSIONS)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let max = ConsensusSchedule::MAX_VERSIONS;
        let mut heights = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(max));
        while let Some(height) = seq.next_element()? {
            if heights.len() == max {
                return Err(de::Error::invalid_length(max + 1, &self));
            }
            heights.push(height);
        }
        Ok(ConsensusSchedule { heights })
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
    /// The little-endian bytes of the hash of the sender's genesis block, if it sent one. Bytes of
    /// the wrong length are kept as sent, so that they fail the genesis check instead of reading as none.
    pub genesis_hash: Option<Vec<u8>>,
}

impl HandshakeTrailer {
    /// The first event version that reads messages 2 and 3 as a CBOR map, rather than in the
    /// original layout and this trailer.
    pub const CBOR_VERSION: u32 = 13;
    /// The first event version whose handshake messages 2 and 3 must carry a trailer with a schedule.
    pub const FIRST_VERSION: u32 = 11;
    /// The first event version whose handshake trailer must carry the genesis hash.
    pub const GENESIS_VERSION: u32 = 12;

    /// Returns a trailer with every field.
    pub fn new(schedule: ConsensusSchedule, genesis_hash: [u8; 32]) -> Self {
        Self { schedule: Some(schedule), genesis_hash: Some(genesis_hash.to_vec()) }
    }

    /// Parses a trailer, or returns `None` if it is malformed.
    fn parse(bytes: &[u8]) -> Option<Self> {
        let (length, rest) = bytes.split_first_chunk::<2>()?;
        let mut fields = rest.get(..usize::from(u16::from_le_bytes(*length)))?;
        let schedule = ConsensusSchedule::read_le(&mut fields).ok()?;
        let genesis_hash = (!fields.is_empty()).then(|| fields[..fields.len().min(32)].to_vec());
        Some(Self { schedule: Some(schedule), genesis_hash })
    }
}

impl ToBytes for HandshakeTrailer {
    /// Writes the trailer, unless it holds no field.
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        let Some(schedule) = &self.schedule else {
            return Ok(());
        };
        let mut fields = schedule.to_bytes_le().map_err(io_error)?;
        if let Some(genesis_hash) = &self.genesis_hash {
            fields.extend_from_slice(genesis_hash);
        }
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

/// The contents of the second handshake message: the responder's metadata.
///
/// Its byte encoding is the original layout, which a peer below [`HandshakeTrailer::CBOR_VERSION`] is
/// sent; see [`encode_responder_info`].
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

/// The contents of the third handshake message: the initiator's metadata, plus the signature that
/// binds its Aleo address to this session.
///
/// Its byte encoding is the original layout, which a peer below [`HandshakeTrailer::CBOR_VERSION`] is
/// sent; see [`encode_initiator_info`].
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

/// The payload of the second handshake message, toward a peer at [`HandshakeTrailer::CBOR_VERSION`]
/// or above: a CBOR map of [`ResponderInfo`]'s fields, sent after the version.
///
/// See the module documentation for the rules that keep the map readable across versions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct CborResponderInfo<N: Network> {
    #[serde(rename = "port")]
    listener_port: u16,
    #[serde(rename = "addr")]
    address: AsBytes<Address<N>>,
    #[serde(rename = "rid")]
    restrictions_id: AsBytes<Field<N>>,
    #[serde(rename = "sha", default, skip_serializing_if = "Option::is_none")]
    snarkos_sha: Option<AsBytes<[u8; 40]>>,
    #[serde(rename = "sched")]
    schedule: ConsensusSchedule,
    #[serde(rename = "genesis")]
    genesis_hash: AsBytes<[u8; 32]>,
}

impl<N: Network> CborResponderInfo<N> {
    /// Initializes the payload. The version of `info` is sent ahead of the map, not in it.
    fn from_parts(info: PeerInfo<N>, schedule: ConsensusSchedule, genesis_hash: [u8; 32]) -> Self {
        let PeerInfo { version: _, listener_port, address, restrictions_id, snarkos_sha } = info;
        Self {
            listener_port,
            address: AsBytes(address),
            restrictions_id: AsBytes(restrictions_id),
            snarkos_sha: snarkos_sha.map(AsBytes),
            schedule,
            genesis_hash: AsBytes(genesis_hash),
        }
    }

    /// Returns the payload as the [`ResponderInfo`] it encodes, given the version sent ahead of it.
    fn into_parts(self, version: u32) -> ResponderInfo<N> {
        let Self {
            listener_port,
            address: AsBytes(address),
            restrictions_id: AsBytes(restrictions_id),
            snarkos_sha,
            schedule,
            genesis_hash: AsBytes(genesis_hash),
        } = self;
        let snarkos_sha = snarkos_sha.map(|AsBytes(sha)| sha).filter(|sha| *sha != UNKNOWN_COMMIT_HASH);
        let info = PeerInfo { version, listener_port, address, restrictions_id, snarkos_sha };
        ResponderInfo { info, trailer: HandshakeTrailer::new(schedule, genesis_hash) }
    }
}

/// The payload of the third handshake message, toward a peer at [`HandshakeTrailer::CBOR_VERSION`]
/// or above: a CBOR map of [`InitiatorInfo`]'s fields, sent after the version.
///
/// It holds the fields of [`CborResponderInfo`] and the signature. The signature is carried as raw
/// bytes and left unparsed, so that the responder only spends time on it once every cheap check has
/// passed. See the module documentation for the rules that keep the map readable across versions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct CborInitiatorInfo<N: Network> {
    #[serde(rename = "port")]
    listener_port: u16,
    #[serde(rename = "addr")]
    address: AsBytes<Address<N>>,
    #[serde(rename = "rid")]
    restrictions_id: AsBytes<Field<N>>,
    #[serde(rename = "sha", default, skip_serializing_if = "Option::is_none")]
    snarkos_sha: Option<AsBytes<[u8; 40]>>,
    #[serde(rename = "sched")]
    schedule: ConsensusSchedule,
    #[serde(rename = "genesis")]
    genesis_hash: AsBytes<[u8; 32]>,
    #[serde(rename = "sig")]
    signature: RawBytes,
}

impl<N: Network> CborInitiatorInfo<N> {
    /// Initializes the payload from the little-endian bytes of the signature. The version of `info`
    /// is sent ahead of the map, not in it.
    fn from_parts(info: PeerInfo<N>, schedule: ConsensusSchedule, genesis_hash: [u8; 32], signature: Vec<u8>) -> Self {
        let CborResponderInfo { listener_port, address, restrictions_id, snarkos_sha, schedule, genesis_hash } =
            CborResponderInfo::from_parts(info, schedule, genesis_hash);
        Self {
            listener_port,
            address,
            restrictions_id,
            snarkos_sha,
            schedule,
            genesis_hash,
            signature: RawBytes(signature),
        }
    }

    /// Returns the payload as the [`InitiatorInfo`] it encodes, given the version sent ahead of it.
    /// The signature is still unparsed.
    fn into_parts(self, version: u32) -> InitiatorInfo<N> {
        let Self {
            listener_port,
            address,
            restrictions_id,
            snarkos_sha,
            schedule,
            genesis_hash,
            signature: RawBytes(signature),
        } = self;
        let responder =
            CborResponderInfo { listener_port, address, restrictions_id, snarkos_sha, schedule, genesis_hash };
        let ResponderInfo { info, trailer } = responder.into_parts(version);
        InitiatorInfo { info, signature: Data::Buffer(Bytes::from(signature)), trailer }
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

#[cfg(any(test, feature = "fuzz-helpers"))]
pub mod prop_tests {
    #![cfg_attr(not(test), allow(unused_imports))]
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
        // A genesis hash is only sent after a schedule, and may be cut short.
        (proptest::option::of(any_schedule()), proptest::option::of(collection::vec(any::<u8>(), 1..=32)))
            .prop_map(|(schedule, genesis_hash)| {
                let genesis_hash = genesis_hash.filter(|_| schedule.is_some());
                HandshakeTrailer { schedule, genesis_hash }
            })
            .boxed()
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

    /// The metadata of a peer that reads the CBOR encoding.
    pub fn any_cbor_peer_info() -> BoxedStrategy<PeerInfo<CurrentNetwork>> {
        (any_peer_info(), any::<bool>())
            .prop_map(|(mut info, has_sha)| {
                info.version = info.version.max(HandshakeTrailer::CBOR_VERSION);
                info.snarkos_sha = info.snarkos_sha.filter(|_| has_sha);
                info
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
        fn check<N: Network>() {
            let schedule = ConsensusSchedule::of::<N>();
            let table = N::CONSENSUS_VERSION_HEIGHTS();
            assert_eq!(schedule.heights().len(), table.len());
            assert!(schedule.heights().len() <= ConsensusSchedule::MAX_VERSIONS, "{} has too many versions", N::NAME);
            for (index, (version, height)) in table.iter().enumerate() {
                assert_eq!(*version as usize, index + 1);
                assert_eq!(schedule.heights()[index], *height);
            }
        }
        check::<snarkvm::prelude::MainnetV0>();
        check::<snarkvm::prelude::TestnetV0>();
        check::<snarkvm::prelude::CanaryV0>();
    }

    #[test]
    fn a_schedule_holds_at_most_max_versions() {
        let max = ConsensusSchedule::MAX_VERSIONS;
        assert!(ConsensusSchedule::new(vec![0; max]).is_ok());
        assert!(ConsensusSchedule::new(vec![0; max + 1]).is_err());
        assert!(ConsensusSchedule { heights: vec![0; max + 1] }.to_bytes_le().is_err());
    }

    #[cfg(test)]
    mod cbor {
        use super::*;

        use snarkos_node_network::noise::MAX_HANDSHAKE_PAYLOAD_LEN;
        use std::str::FromStr;

        use ciborium::Value;

        /// The version of a receiver that reads the CBOR encoding.
        const CBOR_RECEIVER: u32 = HandshakeTrailer::CBOR_VERSION;

        fn peer_addr() -> SocketAddr {
            SocketAddr::from(([127, 0, 0, 1], 4130))
        }

        fn to_cbor(value: &impl Serialize) -> Vec<u8> {
            let mut bytes = Vec::new();
            ciborium::into_writer(value, &mut bytes).unwrap();
            bytes
        }

        fn hex(string: &str) -> Vec<u8> {
            (0..string.len()).step_by(2).map(|i| u8::from_str_radix(&string[i..i + 2], 16).unwrap()).collect()
        }

        /// Splits an encoded CBOR payload into its version and its map.
        fn split_cbor(bytes: &[u8]) -> (&[u8], Vec<(Value, Value)>) {
            let (version, map) = bytes.split_at(4);
            let Value::Map(map) = ciborium::from_reader(map).unwrap() else { panic!("expected a CBOR map") };
            (version, map)
        }

        /// Joins what [`split_cbor`] splits.
        fn join_cbor(version: &[u8], map: Vec<(Value, Value)>) -> Vec<u8> {
            [version.to_vec(), to_cbor(&Value::Map(map))].concat()
        }

        /// Bytes of the length of a signature, which are never parsed.
        fn any_raw_signature() -> BoxedStrategy<Data<Signature<CurrentNetwork>>> {
            collection::vec(any::<u8>(), Signature::<CurrentNetwork>::size_in_bytes())
                .prop_map(|bytes| Data::Buffer(bytes.into()))
                .boxed()
        }

        /// The CBOR encoding of a fixed message 3, spelled out item by item.
        #[test]
        fn cbor_initiator_info_matches_its_golden_bytes() {
            let address =
                Address::<CurrentNetwork>::from_str("aleo1qy4qufq03wcph05fdf5aj09ez67vcmmlrzqf0zza352qwaq43gyqt3wdf6")
                    .unwrap();
            let info = PeerInfo {
                version: 13,
                listener_port: 4130,
                address,
                restrictions_id: Field::from_u64(7),
                snarkos_sha: Some([b'a'; 40]),
            };
            let schedule = ConsensusSchedule::new(vec![0, 24, 70_000, u32::MAX]).unwrap();
            let signature = Data::Buffer(Bytes::from_static(&[1, 2, 3, 4]));
            let bytes = encode_initiator_info(CBOR_RECEIVER, info, schedule, [0x11; 32], signature).unwrap();

            let address_bytes = "012a0e240f8bb01bbe896a69d93cb916bccc6f7f188097885d8d140774158a08";
            let mut restrictions_id = [0u8; 32];
            restrictions_id[0] = 7;
            let expected = [
                // The version, ahead of the map.
                &13u32.to_le_bytes()[..],
                // A map of 7 entries.
                &[0xa7],
                // "port": 4130, as a 2-byte unsigned integer.
                &[0x64],
                b"port",
                &[0x19, 0x10, 0x22],
                // "addr": a 32-byte string.
                &[0x64],
                b"addr",
                &[0x58, 0x20],
                &hex(address_bytes),
                // "rid": a 32-byte string.
                &[0x63],
                b"rid",
                &[0x58, 0x20],
                &restrictions_id,
                // "sha": a 40-byte string.
                &[0x63],
                b"sha",
                &[0x58, 0x28],
                &[b'a'; 40],
                // "sched": an array of 4 unsigned integers.
                &[0x65],
                b"sched",
                &[0x84, 0x00, 0x18, 0x18, 0x1a, 0x00, 0x01, 0x11, 0x70, 0x1a, 0xff, 0xff, 0xff, 0xff],
                // "genesis": a 32-byte string.
                &[0x67],
                b"genesis",
                &[0x58, 0x20],
                &[0x11; 32],
                // "sig": a 4-byte string.
                &[0x63],
                b"sig",
                &[0x44, 1, 2, 3, 4],
            ]
            .concat();
            assert_eq!(bytes, expected);
        }

        #[proptest]
        fn a_signature_of_the_wrong_length_is_malformed(
            #[strategy(any_cbor_peer_info())] info: PeerInfo<CurrentNetwork>,
            #[strategy(any_schedule())] schedule: ConsensusSchedule,
            genesis_hash: [u8; 32],
            #[strategy(any_raw_signature())] signature: Data<Signature<CurrentNetwork>>,
            extra_byte: bool,
        ) {
            let Data::Buffer(bytes) = signature else { unreachable!() };
            let length = if extra_byte { bytes.len() + 1 } else { bytes.len() - 1 };
            let signature = Data::Buffer(bytes.iter().copied().cycle().take(length).collect());
            let message_3 = encode_initiator_info(CBOR_RECEIVER, info, schedule, genesis_hash, signature).unwrap();
            assert!(decode_initiator_info::<CurrentNetwork>(CBOR_RECEIVER, peer_addr(), &message_3).is_err());
        }

        #[proptest]
        fn an_unknown_key_in_the_map_is_skipped(
            #[strategy(any_cbor_peer_info())] info: PeerInfo<CurrentNetwork>,
            #[strategy(any_schedule())] schedule: ConsensusSchedule,
            genesis_hash: [u8; 32],
            #[strategy(any_raw_signature())] signature: Data<Signature<CurrentNetwork>>,
        ) {
            let bytes = encode_initiator_info(CBOR_RECEIVER, info, schedule.clone(), genesis_hash, signature).unwrap();
            let (version, mut map) = split_cbor(&bytes);
            let nested = Value::Map(vec![(Value::Integer(1.into()), Value::Bytes(vec![2]))]);
            map.insert(0, (Value::Text("later".into()), nested));
            map.push((Value::Text("latest".into()), Value::Array(vec![Value::Null, Value::Bool(true)])));

            let decoded = decode_initiator_info(CBOR_RECEIVER, peer_addr(), &join_cbor(version, map)).unwrap();
            assert_eq!((decoded.info, decoded.trailer), (info, HandshakeTrailer::new(schedule, genesis_hash)));
        }

        #[proptest]
        fn a_key_that_is_neither_text_nor_bytes_fails_the_map(
            #[strategy(any_cbor_peer_info())] info: PeerInfo<CurrentNetwork>,
            #[strategy(any_schedule())] schedule: ConsensusSchedule,
            genesis_hash: [u8; 32],
        ) {
            let bytes = encode_responder_info(CBOR_RECEIVER, info, schedule, genesis_hash).unwrap();
            let (version, mut map) = split_cbor(&bytes);
            map.push((Value::Integer(100.into()), Value::Null));
            let result = decode_responder_info::<CurrentNetwork>(CBOR_RECEIVER, peer_addr(), &join_cbor(version, map));
            assert!(result.is_err());
        }

        #[proptest]
        fn a_byte_string_key_reads_as_text(
            #[strategy(any_cbor_peer_info())] info: PeerInfo<CurrentNetwork>,
            #[strategy(any_schedule())] schedule: ConsensusSchedule,
            genesis_hash: [u8; 32],
        ) {
            let bytes = encode_responder_info(CBOR_RECEIVER, info, schedule.clone(), genesis_hash).unwrap();
            let (version, map) = split_cbor(&bytes);
            let mut map: Vec<_> = map
                .into_iter()
                .map(|(key, value)| (Value::Bytes(key.into_text().unwrap().into_bytes()), value))
                .collect();
            map.push((Value::Bytes(b"later".to_vec()), Value::Null));

            let decoded = decode_responder_info(CBOR_RECEIVER, peer_addr(), &join_cbor(version, map)).unwrap();
            assert_eq!(decoded, ResponderInfo { info, trailer: HandshakeTrailer::new(schedule, genesis_hash) });
        }

        #[proptest]
        fn an_unknown_commit_hash_reads_as_none(
            #[strategy(any_cbor_peer_info())] info: PeerInfo<CurrentNetwork>,
            #[strategy(any_schedule())] schedule: ConsensusSchedule,
            genesis_hash: [u8; 32],
        ) {
            let bytes = encode_responder_info(CBOR_RECEIVER, info, schedule, genesis_hash).unwrap();
            let (version, mut map) = split_cbor(&bytes);
            map.retain(|(key, _)| key.as_text() != Some("sha"));
            map.push((Value::Text("sha".into()), Value::Bytes(UNKNOWN_COMMIT_HASH.to_vec())));

            let decoded = decode_responder_info::<CurrentNetwork>(CBOR_RECEIVER, peer_addr(), &join_cbor(version, map));
            assert_eq!(decoded.unwrap().info.snarkos_sha, None);
        }

        #[proptest]
        fn a_missing_required_key_fails_the_map(
            #[strategy(any_cbor_peer_info())] info: PeerInfo<CurrentNetwork>,
            #[strategy(any_schedule())] schedule: ConsensusSchedule,
            genesis_hash: [u8; 32],
            #[strategy(any_raw_signature())] signature: Data<Signature<CurrentNetwork>>,
        ) {
            let bytes = encode_initiator_info(CBOR_RECEIVER, info, schedule, genesis_hash, signature).unwrap();
            let (version, map) = split_cbor(&bytes);
            let without = |key: &str| {
                let map = map.iter().filter(|(k, _)| k.as_text() != Some(key)).cloned().collect();
                decode_initiator_info::<CurrentNetwork>(CBOR_RECEIVER, peer_addr(), &join_cbor(version, map))
            };
            for required in ["port", "addr", "rid", "sched", "genesis", "sig"] {
                assert!(without(required).is_err(), "a map without \"{required}\" should be rejected");
            }
            // The commit hash is optional.
            assert_eq!(without("sha").unwrap().info.snarkos_sha, None);
        }

        #[proptest]
        fn bytes_after_the_map_are_rejected(
            #[strategy(any_cbor_peer_info())] info: PeerInfo<CurrentNetwork>,
            #[strategy(any_schedule())] schedule: ConsensusSchedule,
            genesis_hash: [u8; 32],
            #[strategy(collection::vec(any::<u8>(), 1..8))] leftover: Vec<u8>,
        ) {
            let bytes = encode_responder_info(CBOR_RECEIVER, info, schedule, genesis_hash).unwrap();
            assert!(decode_responder_info::<CurrentNetwork>(CBOR_RECEIVER, peer_addr(), &bytes).is_ok());
            let extended = [bytes, leftover].concat();
            assert!(decode_responder_info::<CurrentNetwork>(CBOR_RECEIVER, peer_addr(), &extended).is_err());
        }

        #[test]
        fn as_bytes_reads_exactly_one_value() {
            let decode = |bytes: &[u8]| {
                ciborium::from_reader::<AsBytes<[u8; 32]>, _>(&to_cbor(&Value::Bytes(bytes.to_vec()))[..])
            };
            assert_eq!(decode(&[7; 32]).unwrap(), AsBytes([7; 32]));
            assert!(decode(&[7; 31]).is_err());
            assert!(decode(&[7; 33]).is_err());
        }

        #[test]
        fn as_bytes_reads_a_byte_string_and_nothing_else() {
            // A byte string sent in chunks.
            let chunked = [&[0x5f, 0x50][..], &[7; 16], &[0x50], &[7; 16], &[0xff]].concat();
            assert_eq!(ciborium::from_reader::<AsBytes<[u8; 32]>, _>(&chunked[..]).unwrap(), AsBytes([7; 32]));

            // The same bytes as an array of integers.
            let array = to_cbor(&Value::Array(vec![Value::Integer(7.into()); 32]));
            assert!(ciborium::from_reader::<AsBytes<[u8; 32]>, _>(&array[..]).is_err());
        }

        #[test]
        fn as_bytes_rejects_an_invalid_value() {
            // Not the x-coordinate of a point on the curve.
            let encoded = to_cbor(&Value::Bytes(vec![0xff; 32]));
            assert!(ciborium::from_reader::<AsBytes<Address<CurrentNetwork>>, _>(&encoded[..]).is_err());
        }

        #[test]
        fn a_cbor_schedule_holds_at_most_max_versions() {
            let max = ConsensusSchedule::MAX_VERSIONS;
            let decode = |bytes: &[u8]| ciborium::from_reader::<ConsensusSchedule, _>(bytes);
            assert!(decode(&to_cbor(&vec![0u32; max])).is_ok());
            assert!(decode(&to_cbor(&vec![0u32; max + 1])).is_err());
            let mut bytes = Vec::new();
            assert!(ciborium::into_writer(&ConsensusSchedule { heights: vec![0; max + 1] }, &mut bytes).is_err());
            // An array that announces 2^32 - 1 heights and holds none.
            assert!(decode(&[0x9a, 0xff, 0xff, 0xff, 0xff]).is_err());
        }

        #[proptest]
        fn a_peer_below_the_cbor_version_is_sent_the_original_layout(
            #[strategy(any_cbor_peer_info())] info: PeerInfo<CurrentNetwork>,
            #[strategy(any_schedule())] schedule: ConsensusSchedule,
            genesis_hash: [u8; 32],
            #[strategy(any_raw_signature())] signature: Data<Signature<CurrentNetwork>>,
        ) {
            let peer_version = HandshakeTrailer::CBOR_VERSION - 1;
            let trailer = HandshakeTrailer::new(schedule.clone(), genesis_hash);

            let message_2 = encode_responder_info(peer_version, info, schedule.clone(), genesis_hash).unwrap();
            assert_eq!(message_2, ResponderInfo { info, trailer: trailer.clone() }.to_bytes_le().unwrap());
            let decoded = decode_responder_info(peer_version, peer_addr(), &message_2).unwrap();
            assert_eq!(decoded, ResponderInfo { info, trailer: trailer.clone() });

            let message_3 =
                encode_initiator_info(peer_version, info, schedule, genesis_hash, signature.clone()).unwrap();
            assert_eq!(message_3, InitiatorInfo { info, signature, trailer }.to_bytes_le().unwrap());
        }

        #[proptest]
        fn a_peer_at_the_cbor_version_is_sent_a_cbor_map(
            #[strategy(any_cbor_peer_info())] info: PeerInfo<CurrentNetwork>,
            #[strategy(any_schedule())] schedule: ConsensusSchedule,
            genesis_hash: [u8; 32],
            #[strategy(any_signature())] signature: Signature<CurrentNetwork>,
        ) {
            let trailer = HandshakeTrailer::new(schedule.clone(), genesis_hash);
            let is_cbor_map = |bytes: &[u8]| {
                bytes[..4] == info.version.to_le_bytes()
                    && ciborium::from_reader::<Value, _>(&bytes[4..]).unwrap().is_map()
            };

            let message_2 = encode_responder_info(CBOR_RECEIVER, info, schedule.clone(), genesis_hash).unwrap();
            assert!(is_cbor_map(&message_2));
            let decoded = decode_responder_info(CBOR_RECEIVER, peer_addr(), &message_2).unwrap();
            assert_eq!(decoded, ResponderInfo { info, trailer: trailer.clone() });

            let message_3 =
                encode_initiator_info(CBOR_RECEIVER, info, schedule, genesis_hash, Data::Object(signature)).unwrap();
            assert!(is_cbor_map(&message_3));
            let InitiatorInfo { info: decoded_info, signature: decoded_signature, trailer: decoded_trailer } =
                decode_initiator_info(CBOR_RECEIVER, peer_addr(), &message_3).unwrap();
            assert_eq!((decoded_info, decoded_trailer), (info, trailer));
            // The signature stays unparsed until the gateway gets to it.
            assert!(matches!(decoded_signature, Data::Buffer(_)));
            assert_eq!(decoded_signature.deserialize_blocking().unwrap(), signature);
        }

        #[proptest]
        fn a_sender_below_the_cbor_version_sends_the_original_layout(
            #[strategy(any_peer_info())] mut info: PeerInfo<CurrentNetwork>,
            #[strategy(any_schedule())] schedule: ConsensusSchedule,
            genesis_hash: [u8; 32],
        ) {
            info.version %= HandshakeTrailer::CBOR_VERSION;
            let message_2 = encode_responder_info(CBOR_RECEIVER, info, schedule.clone(), genesis_hash).unwrap();
            let trailer = HandshakeTrailer::new(schedule, genesis_hash);
            assert_eq!(message_2, ResponderInfo { info, trailer }.to_bytes_le().unwrap());
        }

        #[proptest]
        fn the_original_layout_from_an_older_peer_still_decodes(
            #[strategy(any_peer_info())] mut info: PeerInfo<CurrentNetwork>,
            #[strategy(any_trailer())] trailer: HandshakeTrailer,
            #[strategy(any_signature())] signature: Signature<CurrentNetwork>,
        ) {
            info.version %= HandshakeTrailer::CBOR_VERSION;

            let message_2 = ResponderInfo { info, trailer: trailer.clone() }.to_bytes_le().unwrap();
            let decoded = decode_responder_info(CBOR_RECEIVER, peer_addr(), &message_2).unwrap();
            assert_eq!(decoded, ResponderInfo { info, trailer: trailer.clone() });

            let message_3 = InitiatorInfo { info, signature: Data::Object(signature), trailer: trailer.clone() }
                .to_bytes_le()
                .unwrap();
            let InitiatorInfo { info: decoded_info, signature: decoded_signature, trailer: decoded_trailer } =
                decode_initiator_info(CBOR_RECEIVER, peer_addr(), &message_3).unwrap();
            assert_eq!((decoded_info, decoded_trailer), (info, trailer));
            assert_eq!(decoded_signature.deserialize_blocking().unwrap(), signature);
        }

        #[proptest]
        fn the_original_layout_at_the_cbor_version_is_rejected(
            #[strategy(any_cbor_peer_info())] info: PeerInfo<CurrentNetwork>,
            #[strategy(any_trailer())] trailer: HandshakeTrailer,
        ) {
            let message_2 = ResponderInfo { info, trailer }.to_bytes_le().unwrap();
            assert!(decode_responder_info::<CurrentNetwork>(CBOR_RECEIVER, peer_addr(), &message_2).is_err());
        }

        /// A message 3 whose every field is at its longest has room for at least twice the versions
        /// that this build schedules.
        #[test]
        fn this_build_leaves_room_for_more_versions_in_a_handshake_message() {
            let rng = &mut TestRng::default();
            let private_key = snarkvm::prelude::PrivateKey::new(rng).unwrap();
            let signature = Signature::sign(&private_key, &[Field::rand(rng)], rng).unwrap();
            let info = PeerInfo::<CurrentNetwork> {
                version: u32::MAX,
                listener_port: u16::MAX,
                address: Address::rand(rng),
                restrictions_id: Field::rand(rng),
                snarkos_sha: Some([0xff; 40]),
            };
            let fits = |num_versions: usize| {
                let schedule = ConsensusSchedule::new(vec![u32::MAX; num_versions]).unwrap();
                let signature = Data::Object(signature);
                let message_3 = encode_initiator_info(CBOR_RECEIVER, info, schedule, [0xff; 32], signature).unwrap();
                message_3.len() <= MAX_HANDSHAKE_PAYLOAD_LEN
            };
            let room = (0..=ConsensusSchedule::MAX_VERSIONS).take_while(|num_versions| fits(*num_versions)).count() - 1;
            println!("a worst-case message 3 has room for {room} versions");

            for (name, num_versions) in [
                ("MainnetV0", ConsensusSchedule::of::<snarkvm::prelude::MainnetV0>().heights().len()),
                ("TestnetV0", ConsensusSchedule::of::<snarkvm::prelude::TestnetV0>().heights().len()),
                ("CanaryV0", ConsensusSchedule::of::<snarkvm::prelude::CanaryV0>().heights().len()),
            ] {
                assert!(2 * num_versions <= room, "{name} schedules {num_versions} of the {room} versions that fit");
            }
        }
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
