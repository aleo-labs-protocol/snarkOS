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

use crate::{
    bft::events::Event,
    bootstrap_client::network::MessageOrEvent,
    network::ConnectionMode,
    router::messages::Message,
};
use snarkvm::prelude::{FromBytes, Network, ToBytes};

use bytes::{BufMut, BytesMut};
use core::marker::PhantomData;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

/// The maximum size of a message that can be transmitted during the handshake.
const MAX_HANDSHAKE_SIZE: usize = 1024 * 1024; // 1 MiB
/// The maximum size of a post-handshake message that can be obtained from the network.
const MAX_POST_HANDSHAKE_SIZE: usize = 2 * 1024 * 1024; // 2 MiB

/// The codec used to decode and encode network messages.
///
/// Router messages and gateway events reuse the same IDs, so a frame can only be read by a codec
/// that knows which of the two protocols the peer speaks. The codec decodes router messages from a
/// peer connected in [`ConnectionMode::Router`], and gateway events from one connected in
/// [`ConnectionMode::Gateway`]. Encoding is the same in either mode.
pub struct BootstrapClientCodec<N: Network> {
    codec: LengthDelimitedCodec,
    mode: ConnectionMode,
    _phantom: PhantomData<N>,
}

impl<N: Network> BootstrapClientCodec<N> {
    /// Returns a codec for a peer that completed its handshake in the given mode.
    pub fn new(mode: ConnectionMode) -> Self {
        Self {
            codec: LengthDelimitedCodec::builder()
                .max_frame_length(MAX_POST_HANDSHAKE_SIZE)
                .little_endian()
                .new_codec(),
            mode,
            _phantom: Default::default(),
        }
    }

    /// Returns a codec for the router's challenge-response handshake. Gateway peers handshake over
    /// Noise, which does not use this codec.
    pub fn handshake() -> Self {
        let mut codec = Self::new(ConnectionMode::Router);
        codec.codec.set_max_frame_length(MAX_HANDSHAKE_SIZE);
        codec
    }
}

impl<N: Network> Encoder<Message<N>> for BootstrapClientCodec<N> {
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

impl<N: Network> Encoder<Event<N>> for BootstrapClientCodec<N> {
    type Error = std::io::Error;

    fn encode(&mut self, event: Event<N>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Serialize the payload directly into dst.
        event
            .write_le(&mut dst.writer())
            // This error should never happen, the conversion is for greater compatibility.
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "serialization error"))?;

        let serialized_event = dst.split_to(dst.len()).freeze();

        self.codec.encode(serialized_event, dst)
    }
}

impl<N: Network> Encoder<MessageOrEvent<N>> for BootstrapClientCodec<N> {
    type Error = std::io::Error;

    fn encode(&mut self, item: MessageOrEvent<N>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Serialize the payload directly into dst.
        match item {
            MessageOrEvent::Message(message) => self.encode(message, dst),
            MessageOrEvent::Event(event) => self.encode(event, dst),
        }
    }
}

impl<N: Network> Decoder for BootstrapClientCodec<N> {
    type Error = std::io::Error;
    type Item = MessageOrEvent<N>;

    fn decode(&mut self, source: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // An ignored frame moves on to the next one. Returning `Ok(None)` for it instead would leave
        // any frames already buffered behind it unread until more bytes arrive from the peer.
        loop {
            // Decode a frame containing bytes belonging to a message.
            let bytes = match self.codec.decode(source)? {
                Some(bytes) => bytes,
                None => return Ok(None),
            };

            // Reject invalid/truncated messages.
            if bytes.len() < 2 {
                warn!("Failed to deserialize a message: too short");
                return Err(std::io::ErrorKind::InvalidData.into());
            }

            // Check the ID of the serialized Message or Event.
            let message_id = u16::from_le_bytes(bytes[..2].try_into().unwrap());

            // Discard messages that aren't of interest to a bootstrapper node.
            match (self.mode, message_id) {
                (ConnectionMode::Router, 2..=5) => {
                    return match Message::read_le(&bytes[..]) {
                        Ok(message) => Ok(Some(MessageOrEvent::Message(message))),
                        Err(error) => {
                            warn!("Failed to deserialize a message: {error}");
                            Err(std::io::ErrorKind::InvalidData.into())
                        }
                    };
                }
                (ConnectionMode::Gateway, 7..=9 | 13) => {
                    return match Event::read_le(&bytes[..]) {
                        Ok(event) => Ok(Some(MessageOrEvent::Event(event))),
                        Err(error) => {
                            warn!("Failed to deserialize an event: {error}");
                            Err(std::io::ErrorKind::InvalidData.into())
                        }
                    };
                }
                (mode, id) => trace!("Ignoring an unhandled message ({mode:?} mode, ID {id})"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        bft::events,
        router::messages::{self, MessageId},
    };
    use snarkos_node_network::NodeType;
    use snarkvm::prelude::{Field, MainnetV0};

    type CurrentNetwork = MainnetV0;

    fn gateway_codec() -> BootstrapClientCodec<CurrentNetwork> {
        BootstrapClientCodec::new(ConnectionMode::Gateway)
    }

    fn router_codec() -> BootstrapClientCodec<CurrentNetwork> {
        BootstrapClientCodec::new(ConnectionMode::Router)
    }

    /// Frames each item on its own, as `encode` consumes all of `dst`.
    fn frames(items: Vec<MessageOrEvent<CurrentNetwork>>) -> BytesMut {
        let mut out = BytesMut::new();
        for item in items {
            let mut frame = BytesMut::new();
            router_codec().encode(item, &mut frame).unwrap();
            out.extend_from_slice(&frame);
        }
        out
    }

    #[test]
    fn gateway_events_sharing_router_ids_are_skipped() {
        let certificate_request = Event::CertificateRequest(events::CertificateRequest::new(Field::from_u64(1)));
        assert_eq!(certificate_request.id(), MessageId::PeerRequest as u16);
        let mut buf = frames(vec![
            MessageOrEvent::Event(certificate_request),
            MessageOrEvent::Event(Event::ValidatorsRequest(events::ValidatorsRequest)),
        ]);

        let decoded = gateway_codec().decode(&mut buf).unwrap();
        assert!(matches!(decoded, Some(MessageOrEvent::Event(Event::ValidatorsRequest(_)))), "{decoded:?}");
        assert!(buf.is_empty());
    }

    #[test]
    fn router_messages_sharing_gateway_ids_are_skipped() {
        let ping = Message::Ping(messages::Ping::new(NodeType::Client, None));
        // The ID of the gateway's ChallengeRequest event.
        assert_eq!(ping.id() as u16, 7);
        let mut buf = frames(vec![
            MessageOrEvent::Message(ping),
            MessageOrEvent::Message(Message::PeerRequest(messages::PeerRequest)),
        ]);

        let decoded = router_codec().decode(&mut buf).unwrap();
        assert!(matches!(decoded, Some(MessageOrEvent::Message(Message::PeerRequest(_)))), "{decoded:?}");
        assert!(buf.is_empty());
    }
}
