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
    BootstrapClient,
    bft::events::{
        DisconnectReason,
        Event,
        HANDSHAKE_DOMAIN,
        HandshakeHint,
        InitiatorInfo,
        PeerInfo,
        ResponderProof,
        decode_payload,
        encode_payload,
    },
    bootstrap_client::{codec::BootstrapClientCodec, network::MessageOrEvent},
    network::{ConnectionMode, NodeType, PeerPoolHandling, log_repo_sha_comparison},
    router::messages::{self, Message},
    tcp::{ConnectError, Connection, ConnectionSide, protocols::*},
};
use snarkos_node_network::{
    harden_socket,
    noise::{
        HandshakeProtocol,
        NoiseSession,
        PendingSession,
        Role,
        binding_message,
        detect_handshake_protocol,
        prepare_framed,
    },
};
use snarkvm::{
    ledger::narwhal::Data,
    prelude::{Address, Network, io_error},
};

use futures_util::sink::SinkExt;

use std::{io, net::SocketAddr};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

#[derive(Debug)]
enum HandshakeMessageKind {
    ChallengeRequest,
    ChallengeResponse,
}

macro_rules! send_msg {
    ($msg:expr, $framed:expr, $peer_addr:expr) => {{
        trace!("Sending '{}' to '{}'", $msg.name(), $peer_addr);
        $framed.send($msg).await
    }};
}

/// A macro handling incoming handshake messages, rejecting unexpected ones.
macro_rules! expect_handshake_msg {
    ($msg_ty:expr, $framed:expr, $peer_addr:expr) => {{
        // Read the message as bytes.
        let Some(message) = $framed.try_next().await? else {
            return Err(ConnectError::other(format!(
                "the peer disconnected before sending {:?}, likely due to peer saturation or shutdown",
                stringify!($msg_ty),
            )));
        };

        // Match the expected message type with its expected size or peer type indicator.
        match $msg_ty {
            HandshakeMessageKind::ChallengeRequest
                if matches!(message, MessageOrEvent::Message(Message::ChallengeRequest(_))) =>
            {
                trace!("Received a '{}' from '{}'", stringify!($msg_ty), $peer_addr);
                message
            }
            HandshakeMessageKind::ChallengeResponse
                if matches!(message, MessageOrEvent::Message(Message::ChallengeResponse(_))) =>
            {
                trace!("Received a '{}' from '{}'", stringify!($msg_ty), $peer_addr);
                message
            }
            _ => {
                let msg_name = match message {
                    MessageOrEvent::Message(message) => format!("message {}", message.name()),
                    MessageOrEvent::Event(event) => format!("event {}", event.name()),
                };
                return Err(ConnectError::other(format!(
                    "'{}' did not follow the handshake protocol: expected {}, got {msg_name}",
                    $peer_addr,
                    stringify!($msg_ty),
                )));
            }
        }
    }};
}

#[async_trait]
impl<N: Network> Handshake for BootstrapClient<N> {
    async fn perform_handshake(&self, mut connection: Connection) -> Result<Connection, ConnectError> {
        let peer_addr = connection.addr();
        let peer_side = connection.side();
        let stream = self.borrow_stream(&mut connection);
        // Make the socket more robust.
        harden_socket(stream)?;

        // We don't know the listening address yet, as we don't initiate connections.
        let mut listener_addr = if peer_side == ConnectionSide::Initiator {
            debug!("Received a connection request from '{peer_addr}'");
            None
        } else {
            unreachable!("The boostrapper clients don't initiate connections");
        };

        // Perform the handshake; we pass on a mutable reference to listener_addr in case the process is broken at any point in time.
        let handshake_result = if peer_side == ConnectionSide::Responder {
            unreachable!("The boostrapper clients don't initiate connections");
        } else {
            match detect_handshake_protocol(stream).await? {
                // The gateway speaks Noise and the router speaks the challenge-response handshake,
                // so the marker settles the connection mode. When the router's handshake is
                // converted the two will need telling apart - most likely by giving each
                // subprotocol its own marker - and this is the dispatch that has to change.
                (HandshakeProtocol::Noise, _) => {
                    self.handshake_inner_responder_noise(peer_addr, &mut listener_addr, stream).await
                }
                (HandshakeProtocol::Legacy, prefix) => {
                    self.handshake_inner_responder(peer_addr, &mut listener_addr, stream, &prefix).await
                }
            }
        };

        if let Some(addr) = listener_addr {
            match handshake_result {
                Ok((peer_port, peer_aleo_addr, peer_node_type, peer_version, peer_snarkos_sha, connection_mode)) => {
                    if let Some(peer) = self.peer_pool.write().get_mut(&addr) {
                        // Due to only having a single Resolver, the BootstrapClient only adds an Aleo
                        // address mapping for Gateway-mode connections, as it is only used there, and
                        // it could otherwise clash with the Router-mode mapping for validators, which
                        // may connect in both modes at the same time.
                        let aleo_addr =
                            if connection_mode == ConnectionMode::Gateway { Some(peer_aleo_addr) } else { None };
                        self.resolver.write().insert_peer(peer.listener_addr(), peer_addr, aleo_addr);
                        peer.upgrade_to_connected(
                            peer_addr,
                            peer_port,
                            peer_aleo_addr,
                            peer_node_type,
                            peer_version,
                            peer_snarkos_sha,
                            connection_mode,
                        );
                    }
                    debug!("Completed the handshake with '{peer_addr}'");
                }
                Err(_) => {
                    if let Some(peer) = self.peer_pool.write().get_mut(&addr) {
                        // The peer may only be downgraded if it's a ConnectingPeer.
                        if peer.is_connecting() {
                            peer.downgrade_to_candidate(addr);
                        }
                    }
                }
            }
        }

        handshake_result.map(|_| connection)
    }
}

impl<N: Network> BootstrapClient<N> {
    /// The connection responder side of the Noise handshake, which validators use to connect in
    /// Gateway mode.
    ///
    /// As on the gateway itself, the expensive work is deferred for as long as the protocol allows:
    /// the peer's cleartext hint is checked before any key is derived, its signature is verified only
    /// once the authenticated payload has passed every cheap check, and one is produced only once
    /// that verification has succeeded.
    async fn handshake_inner_responder_noise<'a>(
        &'a self,
        peer_addr: SocketAddr,
        listener_addr: &mut Option<SocketAddr>,
        stream: &'a mut TcpStream,
    ) -> Result<(u16, Address<N>, NodeType, u32, Option<[u8; 40]>, ConnectionMode), ConnectError> {
        /* Message 1: the peer's cleartext hint, read without deriving anything. */

        let pending = PendingSession::accept(stream).await?;
        // The hint opens with its connection mode, so a payload meant for another subprotocol is
        // rejected by name instead of misread as this one.
        //
        // The router's handshake is the challenge-response protocol below, not a Noise hint. When
        // it is converted, dispatch on the mode here and parse the router's hint on that arm.
        let hint: HandshakeHint<N> = decode_payload(peer_addr, pending.first_payload()?)?;

        // Nothing here is trustworthy yet - message 3 runs it again against the authenticated copy -
        // but a peer that fails one of these would fail it there too, so it is not worth a
        // Diffie-Hellman.
        if let Some(reason) = self.verify_peer_claims(peer_addr, hint.version, hint.address).await? {
            return Err(reason.into_connect_error(peer_addr));
        }

        // Introduce the peer into the peer pool; this is the only thing mutated here, so it goes
        // after the checks that might have turned the peer away.
        *listener_addr = Some(SocketAddr::new(peer_addr.ip(), hint.listener_port));
        self.add_connecting_peer(listener_addr.unwrap())?;

        /* Message 2: disclose ourselves, without signing anything yet. */

        let mut noise = pending.into_session()?;
        // The bootstrap client does not track the block height, so it discloses no commit hash.
        let our_info = PeerInfo::new(self.local_ip().port(), self.account.address(), self.restrictions_id, None);
        noise.send(&encode_payload(&our_info)?).await?;

        let peer_binding = binding_message(HANDSHAKE_DOMAIN, Role::Initiator, &noise.handshake_hash()?);

        /* Message 3: the peer's authenticated metadata and its proof of identity. */

        let InitiatorInfo { info: peer_info, signature: peer_signature } =
            decode_payload::<InitiatorInfo<N>>(peer_addr, &noise.recv().await?)?;

        let binding = binding_message(HANDSHAKE_DOMAIN, Role::Responder, &noise.handshake_hash()?);
        let mut noise = noise.into_transport_mode()?;

        // The hint is a claim: reject the peer if it contradicts what it has now authenticated,
        // as otherwise it could be checked as one validator and admitted as another.
        if (hint.version, hint.listener_port, hint.address)
            != (peer_info.version, peer_info.listener_port, peer_info.address)
        {
            warn!("{} Handshake with '{peer_addr}' failed (the handshake hint was contradicted)", Self::OWNER);
            return self.reject_noise_handshake(peer_addr, noise, DisconnectReason::ProtocolViolation).await;
        }
        log_repo_sha_comparison(peer_addr, &peer_info.snarkos_sha, Self::OWNER);

        if peer_info.restrictions_id != self.restrictions_id {
            warn!("{} Handshake with '{peer_addr}' failed (incorrect restrictions ID)", Self::OWNER);
            return self.reject_noise_handshake(peer_addr, noise, DisconnectReason::InvalidChallengeResponse).await;
        }
        if let Some(reason) = self.verify_peer_claims(peer_addr, peer_info.version, peer_info.address).await? {
            return self.reject_noise_handshake(peer_addr, noise, reason).await;
        }

        /* Message 4: verify the peer's proof, then produce our own. */

        // Perform the deferred non-blocking deserialization of the signature.
        let Ok(signature) = peer_signature.deserialize().await else {
            warn!("{} Handshake with '{peer_addr}' failed (cannot deserialize the signature)", Self::OWNER);
            return self.reject_noise_handshake(peer_addr, noise, DisconnectReason::InvalidChallengeResponse).await;
        };
        if !signature.verify_bytes(&peer_info.address, &peer_binding) {
            warn!("{} Handshake with '{peer_addr}' failed (invalid signature)", Self::OWNER);
            return self.reject_noise_handshake(peer_addr, noise, DisconnectReason::InvalidChallengeResponse).await;
        }

        let Ok(our_signature) = self.account.sign_bytes(&binding, &mut rand::rng()) else {
            return Err(ConnectError::other(format!("Failed to sign the handshake binding for '{peer_addr}'")));
        };
        noise.send(&encode_payload(&ResponderProof::Accepted { signature: Data::Object(our_signature) })?).await?;

        Ok((
            peer_info.listener_port,
            peer_info.address,
            NodeType::Validator,
            peer_info.version,
            peer_info.snarkos_sha,
            ConnectionMode::Gateway,
        ))
    }

    /// Tells the initiator why it was turned away, and fails the handshake with that reason.
    async fn reject_noise_handshake(
        &self,
        peer_addr: SocketAddr,
        mut noise: NoiseSession<&mut TcpStream>,
        reason: DisconnectReason,
    ) -> Result<(u16, Address<N>, NodeType, u32, Option<[u8; 40]>, ConnectionMode), ConnectError> {
        noise.send(&encode_payload(&ResponderProof::<N>::Rejected { reason })?).await?;

        Err(reason.into_connect_error(peer_addr))
    }

    /// The checks a peer's claimed identity can be held to without any cryptography, run first
    /// against the cleartext hint and then against the authenticated payload.
    async fn verify_peer_claims(
        &self,
        peer_addr: SocketAddr,
        version: u32,
        address: Address<N>,
    ) -> Result<Option<DisconnectReason>, ConnectError> {
        if version < Event::<N>::VERSION {
            warn!("{} Dropping '{peer_addr}' on version {version} (outdated)", Self::OWNER);
            return Ok(Some(DisconnectReason::OutdatedClientVersion));
        }

        // Reject validators that aren't members of the committee.
        let committee =
            self.get_or_update_committee().await.map_err(|_| ConnectError::other("Couldn't load the committee"))?;
        if let Some(committee) = committee
            && !committee.contains(&address)
        {
            warn!("{} Dropping '{peer_addr}' for not being a committee member ({address})", Self::OWNER);
            return Ok(Some(DisconnectReason::UnauthorizedValidator));
        }

        Ok(None)
    }

    /// The connection responder side of the router's handshake.
    ///
    /// `prefix` holds the bytes consumed while determining which handshake the peer speaks; they are
    /// the beginning of its first frame.
    async fn handshake_inner_responder<'a>(
        &'a self,
        peer_addr: SocketAddr,
        listener_addr: &mut Option<SocketAddr>,
        stream: &'a mut TcpStream,
        prefix: &[u8],
    ) -> Result<(u16, Address<N>, NodeType, u32, Option<[u8; 40]>, ConnectionMode), ConnectError> {
        // Construct the stream.
        let mut framed = prepare_framed(stream, BootstrapClientCodec::<N>::handshake(), prefix);

        /* Step 1: Receive the challenge request. */

        let peer_request = expect_handshake_msg!(HandshakeMessageKind::ChallengeRequest, framed, peer_addr);
        let MessageOrEvent::Message(Message::ChallengeRequest(peer_request)) = peer_request else {
            unreachable!("expect_handshake_msg only returns a router challenge request");
        };

        // Obtain the peer's listening address.
        *listener_addr = Some(SocketAddr::new(peer_addr.ip(), peer_request.listener_port));

        // Introduce the peer into the peer pool.
        self.add_connecting_peer(listener_addr.unwrap())?;

        // Verify the challenge request.
        if !self.verify_challenge_request(peer_addr, &mut framed, &peer_request).await? {
            return Err(ConnectError::application(DisconnectReason::InvalidChallengeRequest));
        };

        /* Step 2: Send the challenge response followed by own challenge request. */

        // Sign the counterparty nonce.
        let response_nonce: u64 = rand::random();
        let data = [peer_request.nonce.to_le_bytes(), response_nonce.to_le_bytes()].concat();
        let Ok(our_signature) = self.account.sign_bytes(&data, &mut rand::rng()) else {
            return Err(ConnectError::other(format!("Failed to sign the challenge request nonce from '{peer_addr}'")));
        };

        let our_response = messages::ChallengeResponse {
            genesis_header: self.genesis_header,
            restrictions_id: self.restrictions_id,
            signature: Data::Object(our_signature),
            nonce: response_nonce,
        };
        let msg = Message::ChallengeResponse::<N>(our_response);
        send_msg!(msg, framed, peer_addr)?;

        // Sample a random nonce.
        let our_nonce: u64 = rand::random();
        // The bootstrap client does not track the block height, so it discloses no commit hash.
        let our_request = messages::ChallengeRequest::new(
            self.local_ip().port(),
            NodeType::BootstrapClient,
            self.account.address(),
            our_nonce,
            None,
        );
        let msg = Message::ChallengeRequest(our_request);
        send_msg!(msg, framed, peer_addr)?;

        /* Step 3: Receive the challenge response. */

        let peer_response = expect_handshake_msg!(HandshakeMessageKind::ChallengeResponse, framed, peer_addr);
        let MessageOrEvent::Message(Message::ChallengeResponse(peer_response)) = peer_response else {
            unreachable!("expect_handshake_msg only returns a router challenge response");
        };
        if !self.verify_challenge_response(peer_addr, peer_request.address, our_nonce, &peer_response).await {
            let msg = Message::Disconnect::<N>(messages::DisconnectReason::InvalidChallengeResponse.into());
            send_msg!(msg, framed, peer_addr)?;
            return Err(ConnectError::application(DisconnectReason::InvalidChallengeResponse));
        }

        Ok((
            peer_request.listener_port,
            peer_request.address,
            peer_request.node_type,
            peer_request.version,
            peer_request.snarkos_sha,
            ConnectionMode::Router,
        ))
    }

    async fn verify_challenge_request(
        &self,
        peer_addr: SocketAddr,
        framed: &mut Framed<&mut TcpStream, BootstrapClientCodec<N>>,
        msg: &messages::ChallengeRequest<N>,
    ) -> io::Result<bool> {
        log_repo_sha_comparison(peer_addr, &msg.snarkos_sha, Self::OWNER);

        if msg.version < Message::<N>::latest_message_version() {
            let msg = Message::Disconnect::<N>(messages::DisconnectReason::OutdatedClientVersion.into());
            send_msg!(msg, framed, peer_addr)?;
            return Ok(false);
        }

        // Reject validators that aren't members of the committee.
        if msg.node_type == NodeType::Validator
            && let Some(current_committee) =
                self.get_or_update_committee().await.map_err(|_| io_error("Couldn't load the committee"))?
            && !current_committee.contains(&msg.address)
        {
            let msg = Message::Disconnect::<N>(messages::DisconnectReason::ProtocolViolation.into());
            send_msg!(msg, framed, peer_addr)?;
            return Ok(false);
        }

        Ok(true)
    }

    async fn verify_challenge_response(
        &self,
        peer_addr: SocketAddr,
        peer_aleo_addr: Address<N>,
        our_nonce: u64,
        response: &messages::ChallengeResponse<N>,
    ) -> bool {
        // Verify the restrictions ID.
        if response.restrictions_id != self.restrictions_id {
            warn!("{} Handshake with '{peer_addr}' failed (incorrect restrictions ID)", Self::OWNER);
            return false;
        }
        // Perform the deferred non-blocking deserialization of the signature.
        let Ok(signature) = response.signature.clone().deserialize().await else {
            warn!("{} Handshake with '{peer_addr}' failed (cannot deserialize the signature)", Self::OWNER);
            return false;
        };
        // Verify the signature.
        if !signature.verify_bytes(&peer_aleo_addr, &[our_nonce.to_le_bytes(), response.nonce.to_le_bytes()].concat()) {
            warn!("{} Handshake with '{peer_addr}' failed (invalid signature)", Self::OWNER);
            return false;
        }

        true
    }
}
