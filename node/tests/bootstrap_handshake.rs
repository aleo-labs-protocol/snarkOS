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
    bft::events::{
        ConsensusSchedule,
        DisconnectReason,
        Event,
        HANDSHAKE_DOMAIN,
        HandshakeHint,
        HandshakeTrailer,
        PeerInfo,
        ResponderInfo,
        ResponderProof,
        decode_responder_info,
        encode_initiator_info,
    },
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

use std::{net::SocketAddr, sync::LazyLock, time::Duration};

use deadline::deadline;
use tokio::net::TcpStream;

/// Spawns a bootstrap client in development mode, so that its committee lookup stays local.
async fn new_test_bootstrap_client() -> BootstrapClient<CurrentNetwork> {
    let rng = &mut TestRng::default();
    let listener_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let genesis = sample_genesis_block();

    BootstrapClient::new(listener_addr, sample_account(rng), *genesis.header(), genesis.hash(), Some(0)).await.unwrap()
}

fn dial_addr(client: &BootstrapClient<CurrentNetwork>) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], client.tcp().listening_addr().unwrap().port()))
}

/// The little-endian bytes of the hash of the bootstrap client's genesis block.
static GENESIS_HASH: LazyLock<[u8; 32]> =
    LazyLock::new(|| sample_genesis_block().hash().to_bytes_le().unwrap().try_into().unwrap());

/// Drives the gateway's Noise handshake against a bootstrap client, up to the verdict, in the
/// encoding a validator at `version` speaks.
async fn handshake_with(
    client: &BootstrapClient<CurrentNetwork>,
    account: &Account<CurrentNetwork>,
    version: u32,
    listener_port: u16,
) -> std::io::Result<Option<(ResponderProof<CurrentNetwork>, NoiseSession<TcpStream>)>> {
    let mut stream = TcpStream::connect(dial_addr(client)).await?;
    write_noise_magic(&mut stream).await?;
    let mut noise = NoiseSession::new(stream, Role::Initiator)?;

    // Message 1: the cleartext hint.
    let hint = HandshakeHint { version, listener_port, address: account.address() };
    noise.send(&hint.to_bytes_le().unwrap()).await?;

    // Message 2: the client's metadata, if it is still talking to us at all.
    let Ok(peer_info) = noise.recv().await else {
        return Ok(None);
    };
    let ResponderInfo { info: peer_info, trailer: peer_trailer } =
        decode_responder_info(version, dial_addr(client), &peer_info).unwrap();
    // The client states its own version in either encoding, and discloses what a validator at that
    // version requires.
    assert_eq!(peer_info.version, Event::<CurrentNetwork>::VERSION);
    assert_eq!(peer_trailer, HandshakeTrailer::new(ConsensusSchedule::of::<CurrentNetwork>(), *GENESIS_HASH));

    // Message 3: our metadata and the proof of our identity.
    let binding = binding_message(HANDSHAKE_DOMAIN, Role::Initiator, &noise.handshake_hash()?);
    let signature = Data::Object(account.sign_bytes(&binding, &mut rand::rng()).unwrap());
    let mut our_info = PeerInfo::new(listener_port, account.address(), peer_info.restrictions_id, None);
    our_info.version = version;
    let schedule = ConsensusSchedule::of::<CurrentNetwork>();
    noise
        .send(&encode_initiator_info(peer_info.version, our_info, schedule, *GENESIS_HASH, signature).unwrap())
        .await?;

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

    // A validator at the current version, and one below the CBOR version that speaks the original
    // layout.
    for (version, listener_port) in
        [(Event::<CurrentNetwork>::VERSION, 5000), (HandshakeTrailer::CBOR_VERSION - 1, 5002)]
    {
        let validator = Account::<CurrentNetwork>::new(rng).unwrap();

        let verdict = handshake_with(&client, &validator, version, listener_port).await.unwrap();

        // The client must have accepted, and proved its own identity in doing so.
        let Some((ResponderProof::Accepted { signature }, _session)) = verdict else {
            panic!("expected the handshake at version {version} to be accepted");
        };
        assert!(signature.deserialize_blocking().is_ok());

        // And it must have registered the peer in Gateway mode.
        let client_ = client.clone();
        let address = validator.address();
        deadline!(Duration::from_secs(5), move || {
            client_.get_connected_peers().iter().any(|peer| peer.aleo_addr == address)
        });
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_outdated_validator_is_dropped_before_the_bootstrap_client_replies() {
    let client = new_test_bootstrap_client().await;
    let rng = &mut TestRng::default();
    let validator = Account::<CurrentNetwork>::new(rng).unwrap();

    // The version is checked against the cleartext hint, so the client should hang up rather than
    // answer. Never receiving a second message is what proves it derived no keys for this peer.
    let verdict = handshake_with(&client, &validator, 0, 5000).await.unwrap();

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
    let ResponderInfo { info: peer_info, .. } =
        decode_responder_info(version, dial_addr(&client), &noise.recv().await.unwrap()).unwrap();

    // Claim the validator's address, but sign with a key we actually hold.
    let binding = binding_message(HANDSHAKE_DOMAIN, Role::Initiator, &noise.handshake_hash().unwrap());
    let signature = Data::Object(impostor.sign_bytes(&binding, &mut rand::rng()).unwrap());
    let our_info = PeerInfo::new(5001, validator.address(), peer_info.restrictions_id, None);
    let schedule = ConsensusSchedule::of::<CurrentNetwork>();
    let our_message = encode_initiator_info(peer_info.version, our_info, schedule, *GENESIS_HASH, signature).unwrap();
    noise.send(&our_message).await.unwrap();

    let mut noise = noise.into_transport_mode().unwrap();
    let verdict = ResponderProof::<CurrentNetwork>::from_bytes_le(&noise.recv().await.unwrap()).unwrap();

    assert_eq!(verdict, ResponderProof::Rejected { reason: DisconnectReason::InvalidChallengeResponse });
}
