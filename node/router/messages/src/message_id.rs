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

use crate::{MAX_PING_MESSAGE_SIZE, UnconfirmedTransaction};
use snarkvm::prelude::Network;

/// The wire ID of a [`crate::Message`] variant: the `u16` that leads every message payload.
///
/// This exists to let a size cap be looked up for a message from its ID alone - before, or
/// instead of, deserializing the message itself.
///
/// # Adding a message
///
/// [`crate::Message::id`] and [`Self::max_size`] are both exhaustive matches with no fallback arm,
/// so a new `Message` variant forces a `MessageId` variant, which in turn forces a cap, before the
/// crate compiles.
///
/// [`Self::from_u16`] is the one part of this **not** checked by the compiler, and cannot be on
/// stable Rust - enumerating an enum's variants needs a macro, a derive, or the unstable
/// `variant_count`. A variant missing there still compiles and every existing test still passes,
/// but the node will happily *encode* the new message while every decoder - its own included -
/// rejects the frame as an unrecognized ID and drops the connection. So: **a variant added to this
/// enum must also be added to `from_u16` by hand.**
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u16)]
pub enum MessageId {
    BlockRequest = 0,
    BlockResponse = 1,
    ChallengeRequest = 2,
    ChallengeResponse = 3,
    Disconnect = 4,
    PeerRequest = 5,
    PeerResponse = 6,
    Ping = 7,
    Pong = 8,
    PuzzleRequest = 9,
    PuzzleResponse = 10,
    UnconfirmedSolution = 11,
    UnconfirmedTransaction = 12,
}

/// The maximum size of the small, fixed-shape messages - everything except `Ping`,
/// `BlockResponse`, and `UnconfirmedTransaction`. None of these carry more than a handful of
/// fixed-size fields, or (for `PeerResponse`) a peer list bounded by the `u8` count that
/// `PeerResponse::write_le` enforces - the compile-time assertion below pins the largest of them
/// against this value, rather than trusting the "handful of fields" claim alone.
pub const MAX_SMALL_MESSAGE_SIZE: usize = 8 * 1024; // 8 KiB

/// The general frame ceiling: the maximum size of a message that can be transmitted in the
/// network. Only `BlockResponse` is left at this ceiling - see [`MessageId::max_size`].
pub(crate) const MAXIMUM_MESSAGE_SIZE: usize = 128 * 1024 * 1024; // 128 MiB

// `MAX_SMALL_MESSAGE_SIZE` is only useful if it is actually above what an honest node sends.
// `PeerResponse` is the one "small" message whose worst case isn't obviously tiny: a full peer
// list of `u8::MAX` entries, every one an IPv6 address carrying a height.
// `largest_honest_peer_response_is_accepted` (in the codec's tests) checks this same bound
// against what `PeerResponse::write_le` really produces.
const _: () = {
    let peer_response = MessageId::SIZE
        + 1 // the version indicator
        + 1 // the peer count
        + (u8::MAX as usize)
            * (1     // the IPv6 tag
                + 16 // the address
                + 2  // the port
                + 1  // the `Some` tag on the height
                + 4); // the height
    assert!(
        peer_response <= MAX_SMALL_MESSAGE_SIZE,
        "MAX_SMALL_MESSAGE_SIZE is below the largest PeerResponse an honest node will send"
    );
};

impl MessageId {
    /// The width, in bytes, of the message ID that leads every message payload.
    pub const SIZE: usize = size_of::<u16>();

    /// Every message ID, in wire order.
    ///
    /// Derived from `from_u16` rather than restated, so there is one hand-maintained list here
    /// instead of two.
    pub fn all() -> impl Iterator<Item = Self> {
        (0..).map_while(Self::from_u16)
    }

    /// Returns the ID as it appears on the wire.
    pub const fn as_u16(self) -> u16 {
        self as u16
    }

    /// Reads the ID leading `bytes` - the first two bytes of a message frame - or `None` if
    /// `bytes` is too short to hold one, or the ID isn't one this node recognizes.
    pub fn peek(bytes: &[u8]) -> Option<Self> {
        let id_bytes: [u8; Self::SIZE] = bytes.get(..Self::SIZE)?.try_into().ok()?;
        Self::from_u16(u16::from_le_bytes(id_bytes))
    }

    /// Returns the ID for the given wire value, or `None` if no message this node understands
    /// uses it.
    ///
    /// Must be kept in step with the enum by hand - see the type-level docs.
    pub const fn from_u16(id: u16) -> Option<Self> {
        Some(match id {
            0 => Self::BlockRequest,
            1 => Self::BlockResponse,
            2 => Self::ChallengeRequest,
            3 => Self::ChallengeResponse,
            4 => Self::Disconnect,
            5 => Self::PeerRequest,
            6 => Self::PeerResponse,
            7 => Self::Ping,
            8 => Self::Pong,
            9 => Self::PuzzleRequest,
            10 => Self::PuzzleResponse,
            11 => Self::UnconfirmedSolution,
            12 => Self::UnconfirmedTransaction,
            _ => return None,
        })
    }

    /// Returns the largest frame, in bytes, that a message with this ID may legitimately be. The
    /// size is of the whole frame - the message ID included - because that is what the length
    /// prefix on the wire counts.
    ///
    /// A connection's frame-length ceiling is the maximum of this over whatever subset of IDs
    /// that connection actually expects to receive - see `MessageCodec::for_allowed_ids`. Letting
    /// a smaller-capped type ride along under a bigger type's ceiling cannot hand an attacker any
    /// capability they don't already have: they could send the bigger type instead, and this
    /// check still rejects a smaller type's frame once it is over its own cap.
    pub fn max_size<N: Network>(self) -> usize {
        match self {
            // The one message that is legitimately large - up to a handful of full blocks,
            // bounded by block count rather than by byte size - so it is left at the general
            // frame ceiling rather than a type-specific one.
            Self::BlockResponse => MAXIMUM_MESSAGE_SIZE,
            Self::Ping => MAX_PING_MESSAGE_SIZE,
            // The one message with a network-defined, consensus-version-dependent cap. The
            // envelope has to be added to it: a transaction of exactly `LATEST_MAX_TRANSACTION_SIZE`
            // is valid (both the ledger service and the REST endpoint accept one), and the message
            // carrying it is necessarily larger than the transaction itself.
            Self::UnconfirmedTransaction => {
                N::LATEST_MAX_TRANSACTION_SIZE().saturating_add(UnconfirmedTransaction::<N>::OVERHEAD)
            }
            Self::BlockRequest
            | Self::ChallengeRequest
            | Self::ChallengeResponse
            | Self::Disconnect
            | Self::PeerRequest
            | Self::PeerResponse
            | Self::Pong
            | Self::PuzzleRequest
            | Self::PuzzleResponse
            | Self::UnconfirmedSolution => MAX_SMALL_MESSAGE_SIZE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `from_u16` is hand-maintained, so check that each arm maps an ID to the variant whose
    /// discriminant actually is that ID - a transposed or copy-pasted arm would otherwise treat
    /// one message type's frames as if they were another's.
    #[test]
    fn from_u16_agrees_with_discriminants() {
        for (index, id) in MessageId::all().enumerate() {
            assert_eq!(
                id.as_u16() as usize,
                index,
                "from_u16({index}) returned {id:?}, whose wire value is not {index}"
            );
        }
    }

    /// The IDs `from_u16` accepts have to be a contiguous run from zero, which is what `all()`
    /// assumes when it stops at the first gap.
    #[test]
    fn message_ids_are_contiguous() {
        let count = MessageId::all().count();
        assert!(count > 0, "no message IDs are reachable from the wire");
        assert_eq!(MessageId::from_u16(count as u16), None, "an ID past the end of the run was accepted");
    }

    /// Every ID has a size that is at least large enough to hold the ID itself, and no capped
    /// message may exceed the general frame ceiling.
    #[test]
    fn max_sizes_are_sane() {
        type CurrentNetwork = snarkvm::prelude::MainnetV0;

        for id in MessageId::all() {
            let max_size = id.max_size::<CurrentNetwork>();
            assert!(max_size >= MessageId::SIZE, "{id:?} cannot hold its own message ID");
            assert!(max_size <= MAXIMUM_MESSAGE_SIZE, "{id:?} is allowed to exceed the general frame ceiling");
        }
    }
}
