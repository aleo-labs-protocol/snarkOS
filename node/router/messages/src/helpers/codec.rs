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

use crate::{MAX_SMALL_MESSAGE_SIZE, Message, MessageId};
use snarkvm::prelude::{FromBytes, Network, ToBytes};

use ::bytes::{Buf, BufMut, BytesMut};
use core::marker::PhantomData;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

/// The maximum size of a message that can be transmitted during the handshake.
const MAXIMUM_HANDSHAKE_MESSAGE_SIZE: usize = 1024 * 1024; // 1 MiB

/// The codec used to decode and encode network `Message`s.
pub struct MessageCodec<N: Network> {
    codec: LengthDelimitedCodec,
    /// Message IDs this codec refuses to decode, rejected the moment the ID is visible - before
    /// deserializing the message, regardless of whether it is otherwise well-formed and within
    /// its own size cap. Empty by default; see [`Self::excluding`].
    excluded_ids: &'static [MessageId],
    _phantom: PhantomData<N>,
}

impl<N: Network> MessageCodec<N> {
    pub fn handshake() -> Self {
        let mut codec = Self::default();
        codec.codec.set_max_frame_length(MAXIMUM_HANDSHAKE_MESSAGE_SIZE);
        codec
    }

    /// Builds a codec whose frame-length ceiling is the maximum over the sizes of the message
    /// types in `allowed_ids`.
    fn for_allowed_ids(allowed_ids: impl IntoIterator<Item = MessageId>) -> Self {
        let max_frame_length =
            allowed_ids.into_iter().map(|id| id.max_size::<N>()).max().unwrap_or(MAX_SMALL_MESSAGE_SIZE);
        Self {
            codec: LengthDelimitedCodec::builder().max_frame_length(max_frame_length).little_endian().new_codec(),
            excluded_ids: &[],
            _phantom: PhantomData,
        }
    }

    /// Builds a codec for a connection that structurally has no legitimate reason to ever receive
    /// any of `excluded_ids` - for instance, a validator's router connections, which never expect
    /// a `BlockResponse` since validators sync blocks over the committee-gated BFT gateway
    /// instead.
    ///
    /// This does two things: it rejects any frame with an excluded ID outright (see `decode`
    /// below), and it shrinks the connection's frame-length ceiling to the largest type that
    /// remains allowed - so an excluded type can no longer be used to force as large an
    /// allocation as its own (possibly much bigger) cap would have permitted.
    pub fn excluding(excluded_ids: &'static [MessageId]) -> Self {
        let allowed_ids = MessageId::all().filter(|id| !excluded_ids.contains(id));
        Self { excluded_ids, ..Self::for_allowed_ids(allowed_ids) }
    }
}

impl<N: Network> Default for MessageCodec<N> {
    fn default() -> Self {
        Self::for_allowed_ids(MessageId::all())
    }
}

impl<N: Network> Encoder<Message<N>> for MessageCodec<N> {
    type Error = std::io::Error;

    fn encode(&mut self, message: Message<N>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Serialize the payload directly into dst.
        message
            .write_le(&mut dst.writer())
            // This error should never happen, the conversion is for greater compatibility.
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "serialization error"))?;

        let serialized_message = dst.split_to(dst.len()).freeze();

        self.codec.encode(serialized_message, dst)
    }
}

impl<N: Network> Decoder for MessageCodec<N> {
    type Error = std::io::Error;
    type Item = Message<N>;

    fn decode(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Decode a frame containing bytes belonging to a message.
        let bytes = match self.codec.decode(source)? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };

        // Reject a message type this connection has no legitimate reason to ever receive - before
        // checking its size or deserializing it. An ID this node doesn't recognize at all isn't
        // rejected here; `Message::read_le` below rejects it instead, and reports it as unknown.
        if let Some(id) = MessageId::peek(&bytes)
            && self.excluded_ids.contains(&id)
        {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "message type is not permitted"));
        }

        Self::Item::check_size(&bytes)?;

        // Convert the bytes to a message, or fail if it is not valid.
        let reader = bytes.reader();
        match Message::read_le(reader) {
            Ok(message) => Ok(Some(message)),
            Err(error) => {
                warn!("Failed to deserialize a message - {}", error);
                Err(std::io::ErrorKind::InvalidData.into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        BlockRequest,
        BlockResponse,
        DataBlocks,
        MAX_SMALL_MESSAGE_SIZE,
        MessageId,
        PeerResponse,
        Pong,
        PuzzleResponse,
        Transaction,
        UnconfirmedSolution,
        UnconfirmedTransaction,
        ping::prop_tests::largest_supported_ping,
        puzzle_response::prop_tests::{any_large_puzzle_response, any_puzzle_response},
        unconfirmed_solution::prop_tests::{any_large_unconfirmed_solution, any_unconfirmed_solution},
        unconfirmed_transaction::prop_tests::{
            any_large_unconfirmed_transaction,
            any_max_size_unconfirmed_transaction,
            any_transaction,
            any_unconfirmed_transaction,
        },
    };

    use proptest::prelude::ProptestConfig;
    use snarkvm::console::network::ConsensusVersion;
    use std::net::{Ipv6Addr, SocketAddr};
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    #[proptest]
    fn unconfirmed_transaction(#[strategy(any_unconfirmed_transaction())] tx: UnconfirmedTransaction<CurrentNetwork>) {
        let mut bytes = BytesMut::new();
        let mut codec = MessageCodec::<CurrentNetwork>::default();
        assert!(codec.encode(Message::UnconfirmedTransaction(tx), &mut bytes).is_ok());
        // `Ok(None)` ("frame incomplete") would also pass a bare `.is_ok()` check, so this must
        // pin down that a message was actually decoded, not merely that decoding didn't error.
        assert!(matches!(codec.decode(&mut bytes), Ok(Some(Message::UnconfirmedTransaction(_)))));
    }

    /// Pins `UnconfirmedTransaction::OVERHEAD` against what `Message::write_le` actually emits,
    /// rather than trusting the arithmetic in its doc comment: encodes a real transaction inside
    /// an `UnconfirmedTransaction` frame and checks the frame is exactly that much larger than
    /// the transaction's own serialized size, for every size a transaction actually takes.
    #[proptest]
    fn unconfirmed_transaction_overhead_matches_the_real_encoding(
        #[strategy(any_transaction())] transaction: Transaction<CurrentNetwork>,
    ) {
        let mut transaction_bytes = BytesMut::default().writer();
        transaction.write_le(&mut transaction_bytes).unwrap();
        let transaction_len = transaction_bytes.into_inner().len();

        let mut frame = BytesMut::default().writer();
        Message::UnconfirmedTransaction(transaction.into()).write_le(&mut frame).unwrap();
        let frame_len = frame.into_inner().len();

        assert_eq!(frame_len, transaction_len + UnconfirmedTransaction::<CurrentNetwork>::OVERHEAD);
    }

    #[proptest(ProptestConfig { cases : 10, ..ProptestConfig::default() })]
    fn overly_large_unconfirmed_transaction(
        #[strategy(any_large_unconfirmed_transaction())] tx: UnconfirmedTransaction<CurrentNetwork>,
    ) {
        let mut bytes = BytesMut::new();
        let mut codec = MessageCodec::<CurrentNetwork>::default();
        assert!(codec.encode(Message::UnconfirmedTransaction(tx), &mut bytes).is_ok());
        assert!(matches!(codec.decode(&mut bytes), Err(err) if err.kind() == std::io::ErrorKind::InvalidData));
    }

    /// A transaction of exactly `LATEST_MAX_TRANSACTION_SIZE` is valid - both the ledger service
    /// and the REST endpoint accept one - so the message carrying it must be accepted too, even
    /// though the frame is larger than the transaction itself by its envelope.
    #[proptest(ProptestConfig { cases : 10, ..ProptestConfig::default() })]
    fn max_size_unconfirmed_transaction_is_accepted(
        #[strategy(any_max_size_unconfirmed_transaction())] tx: UnconfirmedTransaction<CurrentNetwork>,
    ) {
        let mut bytes = BytesMut::new();
        let mut codec = MessageCodec::<CurrentNetwork>::default();
        assert!(codec.encode(Message::UnconfirmedTransaction(tx), &mut bytes).is_ok());
        assert!(matches!(codec.decode(&mut bytes), Ok(Some(Message::UnconfirmedTransaction(_)))));
    }

    /// The largest `Ping` the cap is sized for must still be accepted - the failure mode a too
    /// tight cap causes is disconnecting honest peers, which is worse than the DoS this exists to
    /// prevent.
    #[test]
    fn largest_supported_ping_is_accepted() {
        let mut bytes = BytesMut::new();
        let mut codec = MessageCodec::<CurrentNetwork>::default();
        assert!(codec.encode(Message::Ping(largest_supported_ping()), &mut bytes).is_ok());
        assert!(matches!(codec.decode(&mut bytes), Ok(Some(Message::Ping(_)))));
    }

    /// The bug this whole cap exists to fix: before it, nothing stopped a `PuzzleResponse` or
    /// `UnconfirmedSolution` from carrying an arbitrarily large payload, up to the general frame
    /// ceiling. Both are now bounded like every other small, fixed-shape message.
    #[proptest(ProptestConfig { cases : 5, ..ProptestConfig::default() })]
    fn overly_large_puzzle_response(#[strategy(any_large_puzzle_response())] message: PuzzleResponse<CurrentNetwork>) {
        let mut bytes = BytesMut::new();
        let mut codec = MessageCodec::<CurrentNetwork>::default();
        assert!(codec.encode(Message::PuzzleResponse(message), &mut bytes).is_ok());
        assert!(matches!(codec.decode(&mut bytes), Err(err) if err.kind() == std::io::ErrorKind::InvalidData));
    }

    #[proptest(ProptestConfig { cases : 5, ..ProptestConfig::default() })]
    fn overly_large_unconfirmed_solution(
        #[strategy(any_large_unconfirmed_solution())] solution: UnconfirmedSolution<CurrentNetwork>,
    ) {
        let mut bytes = BytesMut::new();
        let mut codec = MessageCodec::<CurrentNetwork>::default();
        assert!(codec.encode(Message::UnconfirmedSolution(solution), &mut bytes).is_ok());
        assert!(matches!(codec.decode(&mut bytes), Err(err) if err.kind() == std::io::ErrorKind::InvalidData));
    }

    #[proptest(ProptestConfig { cases: 10, ..ProptestConfig::default() })]
    fn puzzle_response_is_accepted(#[strategy(any_puzzle_response())] message: PuzzleResponse<CurrentNetwork>) {
        let mut bytes = BytesMut::new();
        let mut codec = MessageCodec::<CurrentNetwork>::default();
        assert!(codec.encode(Message::PuzzleResponse(message), &mut bytes).is_ok());
        assert!(matches!(codec.decode(&mut bytes), Ok(Some(Message::PuzzleResponse(_)))));
    }

    #[proptest]
    fn unconfirmed_solution_is_accepted(
        #[strategy(any_unconfirmed_solution())] solution: UnconfirmedSolution<CurrentNetwork>,
    ) {
        let mut bytes = BytesMut::new();
        let mut codec = MessageCodec::<CurrentNetwork>::default();
        assert!(codec.encode(Message::UnconfirmedSolution(solution), &mut bytes).is_ok());
        assert!(matches!(codec.decode(&mut bytes), Ok(Some(Message::UnconfirmedSolution(_)))));
    }

    /// The caps have to admit the largest message an honest node will actually send, or peers
    /// start disconnecting each other. `PeerResponse` is the tightest of the small messages: a
    /// full peer list of IPv6 addresses, every one of them carrying a height - the encoding's
    /// actual worst case, not just a plausible-looking sample of one.
    #[test]
    fn largest_honest_peer_response_is_accepted() {
        let peers = (0..u8::MAX as u16)
            .map(|i| (SocketAddr::new(Ipv6Addr::new(0x2001, 0xdb8, i, i, i, i, i, i).into(), 4130), Some(u32::MAX)))
            .collect::<Vec<_>>();

        let mut bytes = BytesMut::new();
        let mut codec = MessageCodec::<CurrentNetwork>::default();
        assert!(codec.encode(Message::PeerResponse(PeerResponse { peers }), &mut bytes).is_ok());
        assert!(matches!(codec.decode(&mut bytes), Ok(Some(Message::PeerResponse(_)))));
    }

    /// `check_size` bounds every message type it recognizes, not just `Ping` and
    /// `UnconfirmedTransaction` - this sweeps every `MessageId` and pins its boundary exactly: a
    /// frame declaring precisely the type's maximum is accepted, one byte more is rejected.
    #[test]
    fn size_gate_boundary_is_exact() {
        for id in MessageId::all() {
            let max_size = id.max_size::<CurrentNetwork>();

            let mut bytes = vec![0u8; max_size];
            bytes[..MessageId::SIZE].copy_from_slice(&id.as_u16().to_le_bytes());
            assert!(
                Message::<CurrentNetwork>::check_size(&bytes).is_ok(),
                "{id:?} rejected a frame of exactly its maximum size ({max_size})"
            );

            let mut bytes = vec![0u8; max_size + 1];
            bytes[..MessageId::SIZE].copy_from_slice(&id.as_u16().to_le_bytes());
            assert!(
                Message::<CurrentNetwork>::check_size(&bytes).is_err(),
                "{id:?} admitted a frame one byte over its cap"
            );
        }
    }

    /// An ID this node doesn't recognize is left uncapped by `check_size` - `Message::read_le`
    /// rejects it instead, and reports it as an unknown message rather than a size violation.
    #[test]
    fn unrecognized_message_id_is_not_size_capped() {
        let unrecognized_id: u16 = MessageId::all().count() as u16;
        let mut bytes = vec![0u8; MAX_SMALL_MESSAGE_SIZE + 1];
        bytes[..MessageId::SIZE].copy_from_slice(&unrecognized_id.to_le_bytes());
        assert!(Message::<CurrentNetwork>::check_size(&bytes).is_ok());
    }

    /// Builds a small, well-formed `BlockResponse` - an empty block list is a legitimate wire
    /// encoding (nothing in `write_le`/`read_le` requires a non-empty one), so this never has to
    /// construct a real block just to exercise the codec. The request range just has to be
    /// well-formed by `BlockRequest`'s own rules (non-zero start, start < end) - it does not have
    /// to match the (empty) block list for the codec to accept the frame.
    fn small_block_response() -> Message<CurrentNetwork> {
        let request = BlockRequest { start_height: 1, end_height: 2 };
        Message::BlockResponse(BlockResponse::new(request, DataBlocks(vec![]), ConsensusVersion::V12))
    }

    /// The default codec's frame ceiling is `BlockResponse`'s own cap: it is the largest of every
    /// message type this node might decode on a connection with no further restriction.
    #[test]
    fn default_codec_ceiling_is_the_largest_allowed_type() {
        let codec = MessageCodec::<CurrentNetwork>::default();
        let expected = MessageId::all().map(|id| id.max_size::<CurrentNetwork>()).max().unwrap();
        assert_eq!(expected, MessageId::BlockResponse.max_size::<CurrentNetwork>());
        assert_eq!(codec.codec.max_frame_length(), expected);
    }

    /// Excluding `BlockResponse` shrinks the ceiling to `UnconfirmedTransaction`'s - the largest
    /// of what remains - rather than to some hardcoded number, so tightening any one type's cap in
    /// the future automatically tightens this ceiling too. A validator must accept those, so this
    /// is also the floor the router ceiling can reach by capping other message types alone.
    #[test]
    fn excluding_block_response_shrinks_the_ceiling_to_the_next_largest_type() {
        let codec = MessageCodec::<CurrentNetwork>::excluding(&[MessageId::BlockResponse]);
        assert_eq!(codec.codec.max_frame_length(), MessageId::UnconfirmedTransaction.max_size::<CurrentNetwork>());
    }

    /// Excluding a type rejects it outright, even a small, well-formed, correctly-sized instance -
    /// not just an oversized one. A connection that structurally never expects a type has no
    /// legitimate reason to accept any instance of it, and rejecting it here, by ID, gives a
    /// clearer signal for debugging than silently decoding it and relying on a much later,
    /// semantic-level handler (e.g. `Validator::block_response`) to reject it instead.
    #[test]
    fn excluding_a_type_rejects_even_a_small_well_formed_instance_of_it() {
        let mut bytes = BytesMut::new();
        let mut encoder = MessageCodec::<CurrentNetwork>::default();
        encoder.encode(small_block_response(), &mut bytes).unwrap();

        let mut restricted = MessageCodec::<CurrentNetwork>::excluding(&[MessageId::BlockResponse]);
        assert!(matches!(restricted.decode(&mut bytes), Err(err) if err.kind() == std::io::ErrorKind::InvalidData));
    }

    /// An exclusion is specific to the excluded ID - an unrelated, permitted message still decodes
    /// normally on the same codec.
    #[test]
    fn excluding_a_type_still_allows_a_permitted_message() {
        let mut bytes = BytesMut::new();
        let mut encoder = MessageCodec::<CurrentNetwork>::default();
        encoder.encode(Message::Pong(Pong { is_fork: Some(false) }), &mut bytes).unwrap();

        let mut restricted = MessageCodec::<CurrentNetwork>::excluding(&[MessageId::BlockResponse]);
        assert!(matches!(restricted.decode(&mut bytes), Ok(Some(Message::Pong(_)))));
    }

    /// The property that actually matters: a connection that excludes `BlockResponse` cannot be
    /// forced to buffer anywhere near the general 128 MiB ceiling by a peer merely claiming to
    /// send one - the frame is rejected the moment its length prefix is visible, before the rest
    /// of a peer-declared, possibly enormous, body has to arrive or be held.
    #[test]
    fn excluding_block_response_rejects_an_oversized_frame_far_below_the_general_ceiling() {
        let declared_len = MessageId::UnconfirmedTransaction.max_size::<CurrentNetwork>() + 1;
        let mut bytes = BytesMut::new();
        bytes.extend_from_slice(&(declared_len as u32).to_le_bytes());
        bytes.extend_from_slice(&MessageId::BlockResponse.as_u16().to_le_bytes());
        // Deliberately no further bytes: a real peer sending this much data would need to
        // actually transmit `declared_len` bytes, which we never provide here.

        let mut restricted = MessageCodec::<CurrentNetwork>::excluding(&[MessageId::BlockResponse]);
        assert!(matches!(restricted.decode(&mut bytes), Err(err) if err.kind() == std::io::ErrorKind::InvalidData));
    }
}
