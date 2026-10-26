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

//! End-to-end tests for the gateway's Noise handshake.

#[allow(dead_code)]
mod common;

use crate::common::{
    CurrentNetwork,
    primary::new_test_committee,
    utils::{sample_gateway, sample_ledger, sample_storage},
};
use snarkos_account::Account;
use snarkos_node_bft::{Gateway, helpers::init_primary_channels};
use snarkos_node_bft_events::{
    DisconnectReason,
    Event,
    EventCodec,
    HANDSHAKE_DOMAIN,
    HandshakeHint,
    InitiatorInfo,
    PeerInfo,
    ResponderProof,
    ValidatorsRequest,
};
use snarkos_node_network::{
    PeerPoolHandling,
    noise::{HandshakeProtocol, NoiseSession, Role, binding_message, detect_handshake_protocol, write_noise_magic},
};
use snarkos_node_tcp::P2P;
use snarkvm::{
    ledger::narwhal::Data,
    prelude::{FromBytes, TestRng, ToBytes},
};

use std::{io, net::SocketAddr, time::Duration};

use deadline::deadline;
use futures::{SinkExt, TryStreamExt};
use rand::RngExt;
use tokio::{
    net::{TcpListener, TcpStream},
    task,
    time::timeout,
};
use tokio_util::codec::Framed;

/// The address to dial a test gateway on.
///
/// The gateways listen on `0.0.0.0` here, which `Tcp::connect` treats as a self-connect when the
/// target's IP matches its own, so the loopback address has to be spelled out.
fn dial_addr(gateway: &Gateway<CurrentNetwork>) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], gateway.local_ip().port()))
}

/// The size of the committee the test gateways belong to; a committee needs at least three members.
const COMMITTEE_SIZE: u16 = 4;

/// Builds `n` running gateways drawn from a single committee, along with all of its accounts.
async fn new_test_gateways(
    n: usize,
    rng: &mut TestRng,
) -> (Vec<Account<CurrentNetwork>>, Vec<Gateway<CurrentNetwork>>) {
    let (accounts, committee) = new_test_committee(COMMITTEE_SIZE, rng);
    let accounts_ = accounts.clone();
    let mut rng_ = TestRng::fixed(rng.random());
    let ledger = task::spawn_blocking(move || sample_ledger(&accounts_, &committee, &mut rng_)).await.unwrap();

    let mut gateways = Vec::with_capacity(n);
    for account in accounts.iter().take(n) {
        let storage = sample_storage(ledger.clone());
        let gateway = sample_gateway(account.clone(), storage, ledger.clone());

        // Set up primary channels; the rx is discarded as these tests exercise the gateway alone.
        let (primary_tx, _primary_rx) = init_primary_channels();
        gateway.run(primary_tx, [].into(), None).await;

        gateways.push(gateway);
    }

    (accounts, gateways)
}

#[tokio::test(flavor = "multi_thread")]
async fn two_gateways_complete_a_noise_handshake() {
    let mut rng = TestRng::default();
    let (accounts, gateways) = new_test_gateways(2, &mut rng).await;
    let (gateway_a, gateway_b) = (gateways[0].clone(), gateways[1].clone());

    gateway_a.tcp().connect(dial_addr(&gateway_b)).await.unwrap();

    // Both sides must have authenticated the other's Aleo address, not merely completed a TCP
    // connection.
    let (a, b) = (gateway_a.clone(), gateway_b.clone());
    let (addr_a, addr_b) = (accounts[0].address(), accounts[1].address());
    deadline!(Duration::from_secs(5), move || {
        a.connected_addresses().contains(&addr_b) && b.connected_addresses().contains(&addr_a)
    });
}

/// The handshake hands a bare stream back to the connection, where [`Reading`] builds an event codec
/// of its own. A `ValidatorsRequest` is always answered, so a response proves the handover left the
/// stream exactly where that codec expects to start.
#[tokio::test(flavor = "multi_thread")]
async fn a_noise_handshake_leaves_the_connection_usable() {
    let mut rng = TestRng::default();
    // One gateway, so that the committee member we authenticate as has no node of its own running.
    let (accounts, gateways) = new_test_gateways(1, &mut rng).await;
    let gateway = gateways[0].clone();
    let peer = accounts[1].clone();

    let signer = peer.clone();
    let sign = move |binding: &[u8]| signer.sign_bytes(binding, &mut rand::rng()).unwrap().to_bytes_le().unwrap();
    let (verdict, mut stream) = handshake_with_gateway(dial_addr(&gateway), &peer, 4140, sign).await.unwrap();
    assert!(matches!(verdict, ResponderProof::Accepted { .. }), "the handshake should have been accepted");

    // Now speak events over the same stream, through a codec built from scratch as `Reading` does.
    let mut framed = Framed::new(&mut stream, EventCodec::<CurrentNetwork>::default());
    framed.send(Event::ValidatorsRequest(ValidatorsRequest)).await.unwrap();

    // The gateway solicits validators of its own accord too, so anything arriving ahead of the answer
    // is skipped.
    let answered = timeout(Duration::from_secs(5), async {
        while let Some(event) = framed.try_next().await.unwrap() {
            if matches!(event, Event::ValidatorsResponse(_)) {
                return true;
            }
        }
        false
    })
    .await
    .expect("the gateway did not answer in time");

    assert!(answered, "the gateway closed the connection instead of answering");
    assert!(gateway.connected_addresses().contains(&peer.address()));
}

/// Relays a Noise handshake between a victim that dials `listener` and a `target` it believes it is
/// talking to.
///
/// The attacker terminates a Noise session on each side and forwards the decrypted payloads
/// verbatim. Each side signs its own session's handshake hash, and the two sessions cannot have
/// the same one, so the handshake is rejected.
async fn relay_noise_handshake(listener: TcpListener, target: SocketAddr) -> io::Result<()> {
    let (mut victim_stream, _) = listener.accept().await?;

    // The victim announced the Noise handshake; consume the marker and answer as the responder.
    let (protocol, _) = detect_handshake_protocol(&mut victim_stream).await?;
    assert_eq!(protocol, HandshakeProtocol::Noise);
    let mut from_victim = NoiseSession::new(victim_stream, Role::Responder)?;

    // Open a second, entirely independent session to the target.
    let mut target_stream = TcpStream::connect(target).await?;
    write_noise_magic(&mut target_stream).await?;
    let mut to_target = NoiseSession::new(target_stream, Role::Initiator)?;

    // Forward each payload from one session into the other.
    let hint = from_victim.recv().await?;
    to_target.send(&hint).await?;

    let responder_info = to_target.recv().await?;
    from_victim.send(&responder_info).await?;

    let initiator_info = from_victim.recv().await?;
    to_target.send(&initiator_info).await?;

    let mut to_target = to_target.into_transport_mode()?;
    let mut from_victim = from_victim.into_transport_mode()?;

    let verdict = to_target.recv().await?;
    from_victim.send(&verdict).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_relayed_noise_handshake_is_rejected() {
    let mut rng = TestRng::default();
    let (accounts, gateways) = new_test_gateways(2, &mut rng).await;
    let (gateway_a, gateway_b) = (gateways[0].clone(), gateways[1].clone());

    // Put the attacker between the two gateways.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_addr = listener.local_addr().unwrap();
    let target = dial_addr(&gateway_b);
    let relay = task::spawn(async move { relay_noise_handshake(listener, target).await });

    // The victim dials the attacker, believing it to be a validator.
    assert!(gateway_a.tcp().connect(relay_addr).await.is_err(), "the relayed handshake should have been rejected");

    // Neither side may end up considering the other a peer.
    assert!(!gateway_a.connected_addresses().contains(&accounts[1].address()));
    assert!(!gateway_b.connected_addresses().contains(&accounts[0].address()));

    relay.abort();
}

/// Drives a Noise handshake against a gateway by hand, up to and including the verdict, and hands the
/// stream back so that a test can carry on speaking events over it.
///
/// The signature over the binding is produced by `sign`, so that a test can decide whether to
/// authenticate honestly or not.
async fn handshake_with_gateway(
    gateway_addr: SocketAddr,
    account: &Account<CurrentNetwork>,
    listener_port: u16,
    sign: impl Fn(&[u8]) -> Vec<u8>,
) -> io::Result<(ResponderProof<CurrentNetwork>, TcpStream)> {
    let mut stream = TcpStream::connect(gateway_addr).await?;
    write_noise_magic(&mut stream).await?;
    let mut noise = NoiseSession::new(stream, Role::Initiator)?;

    let version = snarkos_node_bft_events::Event::<CurrentNetwork>::VERSION;

    // Message 1: the cleartext hint.
    let hint = HandshakeHint { version, listener_port, address: account.address() };
    noise.send(&hint.to_bytes_le().unwrap()).await?;

    // Message 2: the gateway's metadata, which tells us the restrictions ID it expects back.
    let peer_info = PeerInfo::<CurrentNetwork>::from_bytes_le(&noise.recv().await?).unwrap();

    // Message 3: our metadata, and whatever `sign` decides to offer as proof.
    let binding = binding_message(HANDSHAKE_DOMAIN, Role::Initiator, &noise.handshake_hash()?);
    let our_info = PeerInfo::new(listener_port, account.address(), peer_info.restrictions_id, None);
    let our_message = InitiatorInfo { info: our_info, signature: Data::Buffer(sign(&binding).into()) };
    noise.send(&our_message.to_bytes_le().unwrap()).await?;

    // Message 4: the verdict.
    let mut noise = noise.into_transport_mode()?;
    let verdict = ResponderProof::from_bytes_le(&noise.recv().await?).unwrap();

    Ok((verdict, noise.into_inner()))
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unauthorized_validator_is_dropped_before_the_responder_replies() {
    let mut rng = TestRng::default();
    let (_accounts, gateways) = new_test_gateways(1, &mut rng).await;
    let gateway = gateways[0].clone();

    // An account that is not a member of the gateway's committee.
    let outsider = Account::<CurrentNetwork>::new(&mut rng).unwrap();

    let mut stream = TcpStream::connect(dial_addr(&gateway)).await.unwrap();
    write_noise_magic(&mut stream).await.unwrap();
    let mut noise = NoiseSession::new(stream, Role::Initiator).unwrap();

    let version = snarkos_node_bft_events::Event::<CurrentNetwork>::VERSION;
    let hint = HandshakeHint { version, listener_port: 4130, address: outsider.address() };
    noise.send(&hint.to_bytes_le().unwrap()).await.unwrap();

    // The committee check runs against the hint, so the gateway hangs up rather than answering.
    // Never receiving a second message is what proves it spent no Diffie-Hellman on this peer.
    assert!(noise.recv().await.is_err(), "the gateway should not have replied to a non-committee peer");
    assert!(!gateway.connected_addresses().contains(&outsider.address()));

    // And the failure must actually abort the connection. A peer rejected this early never reaches
    // the peer pool, so the handshake result is the only thing standing between it and a live
    // connection with a reader attached.
    let gateway_clone = gateway.clone();
    deadline!(Duration::from_secs(5), move || gateway_clone.tcp().num_connected() == 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_committee_member_that_cannot_prove_its_identity_is_rejected() {
    let mut rng = TestRng::default();
    let (accounts, gateways) = new_test_gateways(2, &mut rng).await;
    let gateway = gateways[0].clone();

    // Claim the Aleo address of a genuine committee member, which is public knowledge, but sign
    // with a key we actually own.
    let impostor = Account::<CurrentNetwork>::new(&mut rng).unwrap();
    let sign = move |binding: &[u8]| impostor.sign_bytes(binding, &mut rand::rng()).unwrap().to_bytes_le().unwrap();

    let (verdict, _stream) = handshake_with_gateway(dial_addr(&gateway), &accounts[1], 4131, sign).await.unwrap();

    assert_eq!(verdict, ResponderProof::Rejected { reason: DisconnectReason::InvalidChallengeResponse });
    assert!(!gateway.connected_addresses().contains(&accounts[1].address()));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_contradicted_handshake_hint_is_rejected() {
    let mut rng = TestRng::default();
    let (accounts, gateways) = new_test_gateways(2, &mut rng).await;
    let gateway = gateways[0].clone();
    let peer = accounts[1].clone();

    let mut stream = TcpStream::connect(dial_addr(&gateway)).await.unwrap();
    write_noise_magic(&mut stream).await.unwrap();
    let mut noise = NoiseSession::new(stream, Role::Initiator).unwrap();

    let version = snarkos_node_bft_events::Event::<CurrentNetwork>::VERSION;

    // Claim one listening port in the cleartext hint...
    let hint = HandshakeHint { version, listener_port: 4132, address: peer.address() };
    noise.send(&hint.to_bytes_le().unwrap()).await.unwrap();
    let peer_info = PeerInfo::<CurrentNetwork>::from_bytes_le(&noise.recv().await.unwrap()).unwrap();

    // ...and a different one in the authenticated payload. The hint is what the gateway ran its
    // early checks against, so disagreeing with it must not be a way to slip past them.
    let binding = binding_message(HANDSHAKE_DOMAIN, Role::Initiator, &noise.handshake_hash().unwrap());
    let signature = peer.sign_bytes(&binding, &mut rand::rng()).unwrap();
    let our_info = PeerInfo::new(4133, peer.address(), peer_info.restrictions_id, None);
    let our_message = InitiatorInfo { info: our_info, signature: Data::Object(signature) };
    noise.send(&our_message.to_bytes_le().unwrap()).await.unwrap();

    let mut noise = noise.into_transport_mode().unwrap();
    let verdict = ResponderProof::<CurrentNetwork>::from_bytes_le(&noise.recv().await.unwrap()).unwrap();

    assert_eq!(verdict, ResponderProof::Rejected { reason: DisconnectReason::ProtocolViolation });
}

#[tokio::test(flavor = "multi_thread")]
async fn an_initiator_cannot_be_checked_as_one_validator_and_admitted_as_another() {
    let mut rng = TestRng::default();
    let (accounts, gateways) = new_test_gateways(2, &mut rng).await;
    let gateway = gateways[0].clone();

    // Pass the early checks as one committee member...
    let claimed = accounts[1].clone();
    // ...then authenticate, correctly and verifiably, as a different one.
    let actual = accounts[2].clone();

    let mut stream = TcpStream::connect(dial_addr(&gateway)).await.unwrap();
    write_noise_magic(&mut stream).await.unwrap();
    let mut noise = NoiseSession::new(stream, Role::Initiator).unwrap();

    let version = snarkos_node_bft_events::Event::<CurrentNetwork>::VERSION;
    let hint = HandshakeHint { version, listener_port: 4134, address: claimed.address() };
    noise.send(&hint.to_bytes_le().unwrap()).await.unwrap();
    let peer_info = PeerInfo::<CurrentNetwork>::from_bytes_le(&noise.recv().await.unwrap()).unwrap();

    let binding = binding_message(HANDSHAKE_DOMAIN, Role::Initiator, &noise.handshake_hash().unwrap());
    let signature = actual.sign_bytes(&binding, &mut rand::rng()).unwrap();
    let our_info = PeerInfo::new(4134, actual.address(), peer_info.restrictions_id, None);
    let our_message = InitiatorInfo { info: our_info, signature: Data::Object(signature) };
    noise.send(&our_message.to_bytes_le().unwrap()).await.unwrap();

    // The signature is perfectly valid for `actual`; what must fail is that the gateway ran its
    // early checks against `claimed`.
    let mut noise = noise.into_transport_mode().unwrap();
    let verdict = ResponderProof::<CurrentNetwork>::from_bytes_le(&noise.recv().await.unwrap()).unwrap();

    assert_eq!(verdict, ResponderProof::Rejected { reason: DisconnectReason::ProtocolViolation });
    assert!(!gateway.connected_addresses().contains(&actual.address()));
}
