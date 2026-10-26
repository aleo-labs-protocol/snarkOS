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

use snarkos_node_sync_locators::BlockLocators;
use snarkos_node_tcp::protocols::Writing;

use std::io;
use tokio::sync::oneshot;

impl<N: Network> Router<N> {
    /// Sends a "Ping" message to the given peer.
    ///
    /// Returns false if the peer does not exist or disconnected.
    #[must_use]
    pub fn send_ping(&self, peer_ip: SocketAddr, block_locators: Option<BlockLocators<N>>) -> bool {
        let result = self.send(peer_ip, Message::Ping(messages::Ping::new(self.node_type(), block_locators)));
        result.is_some()
    }

    /// Sends the given message to specified peer.
    ///
    /// This function returns as soon as the message is queued to be sent,
    /// without waiting for the actual delivery; instead, the caller is provided with a [`oneshot::Receiver`]
    /// which can be used to determine when and whether the message has been delivered.
    ///
    /// This returns None, if the peer does not exist or disconnected.
    pub fn send(&self, peer_ip: SocketAddr, message: Message<N>) -> Option<oneshot::Receiver<io::Result<()>>> {
        // Determine whether to send the message.
        if !self.can_send(peer_ip, &message) {
            return None;
        }
        // Resolve the listener IP to the (ambiguous) peer address.
        let peer_addr = match self.resolve_to_ambiguous(peer_ip) {
            Some(peer_addr) => peer_addr,
            None => {
                warn!("Unable to resolve the listener IP address '{peer_ip}'");
                return None;
            }
        };
        // If the message type is a block request, add it to the cache.
        if let Message::BlockRequest(request) = message {
            self.cache.insert_outbound_block_request(peer_ip, request);
        }
        // If the message type is a puzzle request, increment the cache.
        if matches!(message, Message::PuzzleRequest(_)) {
            self.cache.increment_outbound_puzzle_requests(peer_ip);
        }
        // If the message type is a peer request, increment the cache.
        if matches!(message, Message::PeerRequest(_)) {
            self.cache.increment_outbound_peer_requests(peer_ip);
        }
        // Retrieve the message name.
        let name = message.name();
        #[cfg(feature = "metrics")]
        metrics::increment_counter_2(metrics::router::EVENTS, "direction", "outbound", "type", message.variant_name());
        // Send the message to the peer.
        trace!("Sending '{name}' to '{peer_ip}'");
        let result = self.unicast(peer_addr, message);
        // If the message was unable to be sent, disconnect.
        if let Err(e) = &result {
            warn!("Failed to send '{name}' to '{peer_ip}': {e}");
            debug!("Disconnecting from '{peer_ip}' (unable to send)");
            self.disconnect(peer_ip);
        }
        result.ok()
    }

    /// Returns `true` if the message can be sent.
    fn can_send(&self, peer_ip: SocketAddr, message: &Message<N>) -> bool {
        // Ensure the peer is connected before sending.
        if !self.is_connected(peer_ip) {
            warn!("Attempted to send to a non-connected peer {peer_ip}");
            return false;
        }
        // Determine whether to send the message.
        match message {
            Message::UnconfirmedSolution(message) => {
                // Update the timestamp for the unconfirmed solution.
                let seen_before = self.cache.insert_outbound_solution(peer_ip, message.solution_id).is_some();
                // Determine whether to send the solution.
                !seen_before
            }
            Message::UnconfirmedTransaction(message) => {
                // Update the timestamp for the unconfirmed transaction.
                let seen_before = self.cache.insert_outbound_transaction(peer_ip, message.transaction_id).is_some();
                // Determine whether to send the transaction.
                !seen_before
            }
            // For all other message types, return `true`.
            _ => true,
        }
    }
}
#[async_trait]
impl<N: Network> Writing for Router<N> {
    type Codec = MessageCodec<N>;
    type Message = Message<N>;

    /// Creates an [`Encoder`] used to write the outbound messages to the target stream.
    /// The `side` parameter indicates the connection side **from the node's perspective**.
    fn codec(&self, _addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        Default::default()
    }
}
