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
    ConnectionMode,
    NodeType,
    PeerPoolHandling,
    Router,
    messages::{ChallengeRequest, ChallengeResponse, DisconnectReason, Message, MessageCodec, MessageTrait},
};
use snarkos_node_network::{ConnectingPeerGuard, get_repo_commit_hash, log_repo_sha_comparison};
use snarkos_node_tcp::{ConnectError, ConnectionSide, P2P, Tcp};
use snarkvm::{
    ledger::narwhal::Data,
    prelude::{Address, ConsensusVersion, Field, Network, block::Header},
};

use anyhow::{Result, anyhow};
use futures::SinkExt;

use std::{io, net::SocketAddr};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

impl<N: Network> P2P for Router<N> {
    /// Returns a reference to the TCP instance.
    fn tcp(&self) -> &Tcp {
        &self.tcp
    }
}

/// A macro unwrapping the expected handshake message or returning an error for unexpected messages.
#[macro_export]
macro_rules! expect_message {
    ($msg_ty:path, $framed:expr, $peer_addr:expr) => {{
        match $framed.try_next().await? {
            // Received the expected message, proceed.
            Some($msg_ty(data)) => {
                trace!("Received '{}' from '{}'", data.name(), $peer_addr);
                data
            }
            // Received a disconnect message, abort.
            Some(Message::Disconnect($crate::messages::Disconnect { reason })) => {
                return Err(ConnectError::other(format!("'{}' disconnected with reason \"{reason}\"", $peer_addr)));
            }
            // Received an unexpected message, abort.
            Some(ty) => {
                return Err(ConnectError::other(format!(
                    "'{}' did not follow the handshake protocol: received {:?} instead of {}",
                    $peer_addr,
                    ty.name(),
                    stringify!($msg_ty),
                )));
            }
            // Received nothing.
            None => return Err(ConnectError::IoError(io::ErrorKind::BrokenPipe.into())),
        }
    }};
}

/// Send the given message to the peer.
async fn send<N: Network>(
    framed: &mut Framed<&mut TcpStream, MessageCodec<N>>,
    peer_addr: SocketAddr,
    message: Message<N>,
) -> io::Result<()> {
    trace!("Sending '{}' to '{peer_addr}'", message.name());
    framed.send(message).await
}

impl<N: Network> Router<N> {
    /// Executes the handshake protocol.
    pub async fn handshake<'a>(
        &'a self,
        peer_addr: SocketAddr,
        stream: &'a mut TcpStream,
        peer_side: ConnectionSide,
        genesis_header: Header<N>,
        restrictions_id: Field<N>,
    ) -> Result<ChallengeRequest<N>, ConnectError> {
        if peer_side == ConnectionSide::Initiator {
            debug!("Received a connection request from '{peer_addr}'");
        } else {
            debug!("Shaking hands with '{peer_addr}'...");
        }

        // Check (or impose) IP-level bans.
        #[cfg(not(feature = "test-helpers"))]
        if !self.is_dev() && peer_side == ConnectionSide::Initiator {
            // If the IP is already banned reject the connection.
            if self.is_ip_banned(peer_addr.ip()) {
                trace!("Rejected a connection request from banned IP '{}'", peer_addr.ip());
                return Err(ConnectError::other(anyhow!("'{}' is a banned IP address", peer_addr.ip())));
            }

            let num_attempts =
                self.cache.insert_inbound_connection(peer_addr.ip(), Router::<N>::CONNECTION_ATTEMPTS_SINCE_SECS);

            debug!("Number of connection attempts from '{}': {}", peer_addr.ip(), num_attempts);
            if num_attempts > Router::<N>::MAX_CONNECTION_ATTEMPTS {
                self.update_ip_ban(peer_addr.ip());
                trace!("Rejected a consecutive connection request from IP '{}'", peer_addr.ip());
                return Err(ConnectError::other(anyhow!("'{}' appears to be spamming connections", peer_addr.ip())));
            }
        }

        let handshake_result = match peer_side {
            ConnectionSide::Responder => {
                self.handshake_inner_initiator(peer_addr, stream, genesis_header, restrictions_id).await
            }
            ConnectionSide::Initiator => {
                self.handshake_inner_responder(peer_addr, stream, genesis_header, restrictions_id).await
            }
        };

        // On failure, the handshake has already returned any peer it marked as connecting to the
        // candidates.
        let (peer_request, connecting) = handshake_result?;
        if let Some(peer) = self.peer_pool.write().get_mut(&connecting.listener_addr()) {
            self.resolver.write().insert_peer(peer.listener_addr(), peer_addr, Some(peer_request.address));
            peer.upgrade_to_connected(
                peer_addr,
                peer_request.listener_port,
                peer_request.address,
                peer_request.node_type,
                peer_request.version,
                peer_request.snarkos_sha,
                ConnectionMode::Router,
            );
        }
        // The connecting peer takes the peer pool's lock when dropped.
        drop(connecting);

        #[cfg(feature = "metrics")]
        self.update_metrics();

        Ok(peer_request)
    }

    /// The connection initiator side of the handshake.
    async fn handshake_inner_initiator<'a>(
        &'a self,
        peer_addr: SocketAddr,
        stream: &'a mut TcpStream,
        genesis_header: Header<N>,
        restrictions_id: Field<N>,
    ) -> Result<(ChallengeRequest<N>, ConnectingPeerGuard<'a, N>), ConnectError> {
        // Introduce the peer into the peer pool.
        // If we are connecting, the peer and listener address are identical.
        let connecting = self.add_connecting_peer(peer_addr)?;

        // Construct the stream.
        let mut framed = Framed::new(stream, MessageCodec::<N>::handshake());

        // Determine the snarkOS SHA to send to the peer.
        let current_block_height = self.ledger.latest_block_height();
        let consensus_version = N::CONSENSUS_VERSION(current_block_height).unwrap();
        let snarkos_sha = match (consensus_version >= ConsensusVersion::V12, get_repo_commit_hash()) {
            (true, Some(sha)) => Some(sha),
            _ => None,
        };

        /* Step 1: Send the challenge request. */

        // Sample a random nonce.
        let our_nonce: u64 = rand::random();
        // Send a challenge request to the peer.
        let our_request =
            ChallengeRequest::new(self.local_ip().port(), self.node_type, self.address(), our_nonce, snarkos_sha);
        send(&mut framed, peer_addr, Message::ChallengeRequest(our_request)).await?;

        /* Step 2: Receive the peer's challenge response followed by the challenge request. */

        // Listen for the challenge response message.
        let peer_response = expect_message!(Message::ChallengeResponse, framed, peer_addr);
        // Listen for the challenge request message.
        let peer_request = expect_message!(Message::ChallengeRequest, framed, peer_addr);

        // Verify the challenge response. If a disconnect reason was returned, send the disconnect message and abort.
        if let Some(reason) = self
            .verify_challenge_response(
                peer_addr,
                peer_request.address,
                peer_request.node_type,
                peer_response,
                genesis_header,
                restrictions_id,
                our_nonce,
            )
            .await
        {
            send(&mut framed, peer_addr, reason.into()).await?;
            return Err(reason.into_connect_error(peer_addr));
        }

        // Verify the challenge request. If a disconnect reason was returned, send the disconnect message and abort.
        if let Some(reason) = self.verify_challenge_request(peer_addr, &peer_request) {
            send(&mut framed, peer_addr, reason.into()).await?;
            return Err(reason.into_connect_error(peer_addr));
        }

        /* Step 3: Send the challenge response. */

        let response_nonce: u64 = rand::random();
        let data = [peer_request.nonce.to_le_bytes(), response_nonce.to_le_bytes()].concat();
        // Sign the counterparty nonce.
        let Ok(our_signature) = self.account.sign_bytes(&data, &mut rand::rng()) else {
            return Err(ConnectError::other(anyhow!("Failed to sign the challenge request nonce")));
        };
        // Send the challenge response.
        let our_response = ChallengeResponse {
            genesis_header,
            restrictions_id,
            signature: Data::Object(our_signature),
            nonce: response_nonce,
        };
        send(&mut framed, peer_addr, Message::ChallengeResponse(our_response)).await?;

        Ok((peer_request, connecting))
    }

    /// The connection responder side of the handshake.
    async fn handshake_inner_responder<'a>(
        &'a self,
        peer_addr: SocketAddr,
        stream: &'a mut TcpStream,
        genesis_header: Header<N>,
        restrictions_id: Field<N>,
    ) -> Result<(ChallengeRequest<N>, ConnectingPeerGuard<'a, N>), ConnectError> {
        // Construct the stream.
        let mut framed = Framed::new(stream, MessageCodec::<N>::handshake());

        /* Step 1: Receive the challenge request. */

        // Wait for the challenge request message.
        let peer_request = expect_message!(Message::ChallengeRequest, framed, peer_addr);

        // Determine the snarkOS SHA to send to the peer.
        let current_block_height = self.ledger.latest_block_height();
        let consensus_version = N::CONSENSUS_VERSION(current_block_height).unwrap();
        let snarkos_sha = match (consensus_version >= ConsensusVersion::V12, get_repo_commit_hash()) {
            (true, Some(sha)) => Some(sha),
            _ => None,
        };

        // Obtain the peer's listening address.
        let listener_addr = SocketAddr::new(peer_addr.ip(), peer_request.listener_port);

        // Knowing the peer's listening address, ensure it is allowed to connect.
        if let Err(reason) = self.ensure_peer_is_allowed(listener_addr) {
            send(&mut framed, peer_addr, reason.into()).await?;
            return Err(reason.into_connect_error(listener_addr));
        }

        // Introduce the peer into the peer pool.
        let connecting = self.add_connecting_peer(listener_addr)?;

        // Verify the challenge request. If a disconnect reason was returned, send the disconnect message and abort.
        if let Some(reason) = self.verify_challenge_request(peer_addr, &peer_request) {
            send(&mut framed, peer_addr, reason.into()).await?;
            return Err(reason.into_connect_error(peer_addr));
        }

        /* Step 2: Send the challenge response followed by own challenge request. */

        // Sign the counterparty nonce.
        let response_nonce: u64 = rand::random();
        let data = [peer_request.nonce.to_le_bytes(), response_nonce.to_le_bytes()].concat();
        let Ok(our_signature) = self.account.sign_bytes(&data, &mut rand::rng()) else {
            return Err(ConnectError::Other(
                anyhow!("Failed to sign the challenge request nonce from '{peer_addr}'").into(),
            ));
        };
        // Send the challenge response.
        let our_response = ChallengeResponse {
            genesis_header,
            restrictions_id,
            signature: Data::Object(our_signature),
            nonce: response_nonce,
        };
        send(&mut framed, peer_addr, Message::ChallengeResponse(our_response)).await?;

        // Sample a random nonce.
        let our_nonce: u64 = rand::random();
        // Send the challenge request.
        let our_request =
            ChallengeRequest::new(self.local_ip().port(), self.node_type, self.address(), our_nonce, snarkos_sha);
        send(&mut framed, peer_addr, Message::ChallengeRequest(our_request)).await?;

        /* Step 3: Receive the challenge response. */

        // Wait for the challenge response message.
        let peer_response = expect_message!(Message::ChallengeResponse, framed, peer_addr);

        // Verify the challenge response. If a disconnect reason was returned, send the disconnect message and abort.
        if let Some(reason) = self
            .verify_challenge_response(
                peer_addr,
                peer_request.address,
                peer_request.node_type,
                peer_response,
                genesis_header,
                restrictions_id,
                our_nonce,
            )
            .await
        {
            send(&mut framed, peer_addr, reason.into()).await?;
            Err(reason.into_connect_error(peer_addr))
        } else {
            Ok((peer_request, connecting))
        }
    }

    /// Ensure the peer is allowed to connect.
    fn ensure_peer_is_allowed(&self, listener_addr: SocketAddr) -> Result<(), DisconnectReason> {
        // Ensure that it's not a self-connect attempt.
        if self.is_local_ip(listener_addr) {
            return Err(DisconnectReason::SelfConnect);
        }
        // As a validator, only accept connections from trusted peers and bootstrap nodes.
        if self.node_type() == NodeType::Validator
            && !self.is_trusted(listener_addr)
            && !crate::bootstrap_peers::<N>(self.is_dev()).contains(&listener_addr)
        {
            return Err(DisconnectReason::NoExternalPeersAllowed);
        }
        // If the node is in trusted peers only mode, ensure the peer is explicitly trusted.
        if self.trusted_peers_only() && !self.is_trusted(listener_addr) {
            return Err(DisconnectReason::NoExternalPeersAllowed);
        }

        Ok(())
    }

    /// Verifies the given challenge request. Returns a disconnect reason if the request is invalid.
    fn verify_challenge_request(
        &self,
        peer_addr: SocketAddr,
        message: &ChallengeRequest<N>,
    ) -> Option<DisconnectReason> {
        // Retrieve the components of the challenge request.
        let &ChallengeRequest { version, listener_port: _, node_type, address, nonce: _, ref snarkos_sha } = message;
        log_repo_sha_comparison(peer_addr, snarkos_sha, Self::OWNER);

        // Ensure the message protocol version is not outdated.
        if !self.is_valid_message_version(version) {
            warn!("Dropping '{peer_addr}' on version {version} (outdated)");
            return Some(DisconnectReason::OutdatedClientVersion);
        }

        // Ensure there are no validators connected with the given Aleo address.
        if self.node_type() == NodeType::Validator
            && node_type == NodeType::Validator
            && self.is_connected_address(address)
        {
            warn!("Dropping '{peer_addr}' for being already connected ({address})");
            return Some(DisconnectReason::NoReasonGiven);
        }

        None
    }

    /// Verifies the given challenge response. Returns a disconnect reason if the response is invalid.
    #[allow(clippy::too_many_arguments)]
    async fn verify_challenge_response(
        &self,
        peer_addr: SocketAddr,
        peer_address: Address<N>,
        peer_node_type: NodeType,
        response: ChallengeResponse<N>,
        expected_genesis_header: Header<N>,
        expected_restrictions_id: Field<N>,
        expected_nonce: u64,
    ) -> Option<DisconnectReason> {
        // Retrieve the components of the challenge response.
        let ChallengeResponse { genesis_header, restrictions_id, signature, nonce } = response;

        // Verify the challenge response, by checking that the block header matches.
        if genesis_header != expected_genesis_header {
            warn!("Handshake with '{peer_addr}' failed (incorrect block header)");
            return Some(DisconnectReason::InvalidChallengeResponse);
        }
        // Verify the restrictions ID.
        if !peer_node_type.is_prover() && !self.node_type.is_prover() && restrictions_id != expected_restrictions_id {
            warn!("Handshake with '{peer_addr}' failed (incorrect restrictions ID)");
            return Some(DisconnectReason::InvalidChallengeResponse);
        }
        // Perform the deferred non-blocking deserialization of the signature.
        let Ok(signature) = signature.deserialize().await else {
            warn!("Handshake with '{peer_addr}' failed (cannot deserialize the signature)");
            return Some(DisconnectReason::InvalidChallengeResponse);
        };
        // Verify the signature.
        if !signature.verify_bytes(&peer_address, &[expected_nonce.to_le_bytes(), nonce.to_le_bytes()].concat()) {
            warn!("Handshake with '{peer_addr}' failed (invalid signature)");
            return Some(DisconnectReason::InvalidChallengeResponse);
        }
        None
    }
}
