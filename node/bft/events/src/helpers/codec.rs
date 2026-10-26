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

use crate::Event;
use snarkvm::prelude::{FromBytes, Network, ToBytes};

use bytes::{Buf, BufMut, BytesMut};
use core::marker::PhantomData;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};
use tracing::*;

/// The maximum size of an event that can be transmitted in the network.
const MAX_EVENT_SIZE: usize = 256 * 1024 * 1024; // 256 MiB
/// The integer type of a frame's length prefix.
///
/// The decoder is configured with this via `length_field_type`, and the encoder writes the prefix
/// as one of these, so the two cannot disagree about the framing.
type FrameLength = u32;
/// The width of a frame's length prefix, in bytes.
const LENGTH_FIELD_LEN: usize = size_of::<FrameLength>();

/// The codec used to decode and encode network `Event`s.
pub struct EventCodec<N: Network> {
    codec: LengthDelimitedCodec,
    _phantom: PhantomData<N>,
}

impl<N: Network> Default for EventCodec<N> {
    fn default() -> Self {
        Self {
            codec: LengthDelimitedCodec::builder()
                .max_frame_length(MAX_EVENT_SIZE)
                .length_field_type::<FrameLength>()
                .little_endian()
                .new_codec(),
            _phantom: Default::default(),
        }
    }
}

impl<N: Network> Encoder<Event<N>> for EventCodec<N> {
    type Error = std::io::Error;

    fn encode(&mut self, event: Event<N>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Reserve the frame's length prefix, remembering where it has to be written.
        //
        // The length is not known until the event has been serialized, so the alternative is to
        // serialize the event elsewhere and copy the result in once its size is known. Writing the
        // event straight into `dst` and filling in the length afterwards avoids that copy.
        //
        // Note that `dst` is the connection's write buffer and is not necessarily empty: it still
        // holds any frames that have not been flushed yet. Everything below is therefore relative
        // to where this frame starts, not to the start of the buffer.
        let frame_offset = dst.len();
        dst.extend_from_slice(&[0u8; LENGTH_FIELD_LEN]);

        // Serialize the event directly into the destination buffer.
        if let Err(error) = event.write_le(&mut dst.writer()) {
            // Leave the buffer as it was found, so a failed encode cannot corrupt the stream.
            dst.truncate(frame_offset);
            error!("Failed to serialize an event: {error}");
            // This error should never happen, the conversion is for greater compatibility.
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "serialization error"));
        }

        // Determine the length of what was just written, and ensure it is a permitted frame size.
        let frame_len = dst.len() - frame_offset - LENGTH_FIELD_LEN;
        if frame_len > self.codec.max_frame_length() {
            dst.truncate(frame_offset);
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "frame size too big"));
        }

        // Fill in the length prefix, in the same little-endian `FrameLength` framing that
        // `self.codec` is built to decode. The `event_roundtrip` test covers that agreement.
        let frame_len = FrameLength::try_from(frame_len)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "frame size too big"))?;
        dst[frame_offset..frame_offset + LENGTH_FIELD_LEN].copy_from_slice(&frame_len.to_le_bytes());

        Ok(())
    }
}

impl<N: Network> Decoder for EventCodec<N> {
    type Error = std::io::Error;
    type Item = Event<N>;

    fn decode(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Decode a frame containing bytes belonging to an event.
        let bytes = match self.codec.decode(source)? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };

        // Convert the bytes to an event, or fail if it is not valid.
        let reader = bytes.reader();
        match Event::read_le(reader) {
            Ok(event) => Ok(Some(event)),
            Err(error) => {
                error!("Failed to deserialize an event: {}", error);
                Err(std::io::ErrorKind::InvalidData.into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prop_tests::any_event;
    use proptest::prelude::ProptestConfig;
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    fn assert_roundtrip(msg: Event<CurrentNetwork>) {
        let mut codec: EventCodec<CurrentNetwork> = Default::default();
        let mut encoded_event = BytesMut::new();

        assert!(codec.encode(msg.clone(), &mut encoded_event).is_ok());
        let decoded = codec.decode(&mut encoded_event).unwrap().unwrap();
        assert_eq!(decoded.to_bytes_le().unwrap(), msg.to_bytes_le().unwrap());
    }

    #[proptest(ProptestConfig { cases: 128, ..ProptestConfig::default() })]
    fn event_roundtrip(#[strategy(any_event())] event: Event<CurrentNetwork>) {
        assert_roundtrip(event)
    }

    #[proptest(ProptestConfig { cases: 128, ..ProptestConfig::default() })]
    fn events_encoded_into_a_shared_buffer_stay_separate(
        #[strategy(any_event())] first: Event<CurrentNetwork>,
        #[strategy(any_event())] second: Event<CurrentNetwork>,
    ) {
        // Encoding into a buffer that already holds a frame is what a sink does when it is fed
        // several events before being flushed; each one must remain a frame of its own.
        let mut codec: EventCodec<CurrentNetwork> = Default::default();
        let mut buffer = BytesMut::new();

        codec.encode(first.clone(), &mut buffer).unwrap();
        codec.encode(second.clone(), &mut buffer).unwrap();

        for expected in [first, second] {
            let decoded = codec.decode(&mut buffer).unwrap().unwrap();
            assert_eq!(decoded.to_bytes_le().unwrap(), expected.to_bytes_le().unwrap());
        }
        assert!(buffer.is_empty());
    }
}
