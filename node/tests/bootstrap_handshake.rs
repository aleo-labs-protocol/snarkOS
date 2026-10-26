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

//! The bootstrap client's side of the gateway's Noise handshake.

#[allow(dead_code)]
mod common;

use crate::common::{sample_account, sample_genesis_block};
use snarkos_account::Account;
use snarkos_node::{
    BootstrapClient,
    bft::events::{DisconnectReason, Event, HANDSHAKE_DOMAIN, HandshakeHint, InitiatorInfo, PeerInfo, ResponderProof},
    network::{
        PeerPoolHandling,
        noise::{NoiseSession, Role, binding_message, write_noise_magic},
    },
    tcp::P2P,
};
use snarkvm::{
    ledger::narwhal::Data,
    prelude::{FromBytes, MainnetV0 as CurrentNetwork, TestRng, ToBytes},
};

use std::{net::SocketAddr, time::Duration};

use deadline::deadline;
use tokio::net::TcpStream;

/// Spawns a bootstrap client in development mode, so that its committee lookup stays local.
async fn new_test_bootstrap_client() -> BootstrapClient<CurrentNetwork> {
    let rng = &mut TestRng::default();
    let listener_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let genesis_header = *sample_genesis_block().header();

    BootstrapClient::new(listener_addr, sample_account(rng), genesis_header, Some(0)).await.unwrap()
}

fn dial_addr(client: &BootstrapClient<CurrentNetwork>) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], client.tcp().listening_addr().unwrap().port()))
}

/// Drives the gateway's Noise handshake against a bootstrap client, up to the verdict.
async fn handshake_with(
    client: &BootstrapClient<CurrentNetwork>,
    account: &Account<CurrentNetwork>,
    version: u32,
) -> std::io::Result<Option<(ResponderProof<CurrentNetwork>, NoiseSession<TcpStream>)>> {
    let mut stream = TcpStream::connect(dial_addr(client)).await?;
    write_noise_magic(&mut stream).await?;
    let mut noise = NoiseSession::new(stream, Role::Initiator)?;

    // Message 1: the cleartext hint.
    let hint = HandshakeHint { version, listener_port: 5000, address: account.address() };
    noise.send(&hint.to_bytes_le().unwrap()).await?;

    // Message 2: the client's metadata, if it is still talking to us at all.
    let Ok(peer_info) = noise.recv().await else {
        return Ok(None);
    };
    let peer_info = PeerInfo::<CurrentNetwork>::from_bytes_le(&peer_info).unwrap();

    // Message 3: our metadata and the proof of our identity.
    let binding = binding_message(HANDSHAKE_DOMAIN, Role::Initiator, &noise.handshake_hash()?);
    let signature = account.sign_bytes(&binding, &mut rand::rng()).unwrap();
    let our_info = PeerInfo::new(5000, account.address(), peer_info.restrictions_id, None);
    let our_message = InitiatorInfo { info: our_info, signature: Data::Object(signature) };
    noise.send(&our_message.to_bytes_le().unwrap()).await?;

    // Message 4: the verdict.
    let mut noise = noise.into_transport_mode()?;
    let verdict = ResponderProof::from_bytes_le(&noise.recv().await?).unwrap();

    // The session is handed back so the caller can hold the connection open; dropping it here would
    // disconnect us again before the client has finished registering the peer.
    Ok(Some((verdict, noise)))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_validator_completes_a_noise_handshake_with_a_bootstrap_client() {
    let client = new_test_bootstrap_client().await;
    let rng = &mut TestRng::default();
    let validator = Account::<CurrentNetwork>::new(rng).unwrap();

    let version = Event::<CurrentNetwork>::VERSION;
    let verdict = handshake_with(&client, &validator, version).await.unwrap();

    // The client must have accepted, and proved its own identity in doing so.
    let Some((ResponderProof::Accepted { signature }, _session)) = verdict else {
        panic!("expected the handshake to be accepted");
    };
    assert!(signature.deserialize_blocking().is_ok());

    // And it must have registered the peer in Gateway mode.
    let client_ = client.clone();
    let address = validator.address();
    deadline!(Duration::from_secs(5), move || {
        client_.get_connected_peers().iter().any(|peer| peer.aleo_addr == address)
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn an_outdated_validator_is_dropped_before_the_bootstrap_client_replies() {
    let client = new_test_bootstrap_client().await;
    let rng = &mut TestRng::default();
    let validator = Account::<CurrentNetwork>::new(rng).unwrap();

    // The version is checked against the cleartext hint, so the client should hang up rather than
    // answer. Never receiving a second message is what proves it derived no keys for this peer.
    let verdict = handshake_with(&client, &validator, 0).await.unwrap();

    assert!(verdict.is_none(), "the bootstrap client should not have replied to an outdated peer");
    assert_eq!(client.tcp().num_connected(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bootstrap_client_rejects_an_unprovable_identity() {
    let client = new_test_bootstrap_client().await;
    let rng = &mut TestRng::default();
    let validator = Account::<CurrentNetwork>::new(rng).unwrap();
    let impostor = Account::<CurrentNetwork>::new(rng).unwrap();

    let mut stream = TcpStream::connect(dial_addr(&client)).await.unwrap();
    write_noise_magic(&mut stream).await.unwrap();
    let mut noise = NoiseSession::new(stream, Role::Initiator).unwrap();

    let version = Event::<CurrentNetwork>::VERSION;
    let hint = HandshakeHint { version, listener_port: 5001, address: validator.address() };
    noise.send(&hint.to_bytes_le().unwrap()).await.unwrap();
    let peer_info = PeerInfo::<CurrentNetwork>::from_bytes_le(&noise.recv().await.unwrap()).unwrap();

    // Claim the validator's address, but sign with a key we actually hold.
    let binding = binding_message(HANDSHAKE_DOMAIN, Role::Initiator, &noise.handshake_hash().unwrap());
    let signature = impostor.sign_bytes(&binding, &mut rand::rng()).unwrap();
    let our_info = PeerInfo::new(5001, validator.address(), peer_info.restrictions_id, None);
    let our_message = InitiatorInfo { info: our_info, signature: Data::Object(signature) };
    noise.send(&our_message.to_bytes_le().unwrap()).await.unwrap();

    let mut noise = noise.into_transport_mode().unwrap();
    let verdict = ResponderProof::<CurrentNetwork>::from_bytes_le(&noise.recv().await.unwrap()).unwrap();

    assert_eq!(verdict, ResponderProof::Rejected { reason: DisconnectReason::InvalidChallengeResponse });
}
