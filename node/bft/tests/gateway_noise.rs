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
    TranslucentLedgerService,
    primary::shared_test_committee,
    utils::{sample_gateway, sample_ledger, sample_storage},
};
use snarkos_account::Account;
use snarkos_node_bft::{Gateway, helpers::init_primary_channels};
use snarkos_node_bft_events::{
    ConsensusSchedule,
    DisconnectReason,
    Event,
    EventCodec,
    HANDSHAKE_DOMAIN,
    HandshakeHint,
    HandshakeTrailer,
    InitiatorInfo,
    PeerInfo,
    ResponderInfo,
    ResponderProof,
    ValidatorsRequest,
    decode_responder_info,
    encode_initiator_info,
};
use snarkos_node_network::{
    PeerPoolHandling,
    noise::{
        HandshakeProtocol,
        NoiseSession,
        PendingSession,
        Role,
        binding_message,
        detect_handshake_protocol,
        write_noise_magic,
    },
};
use snarkos_node_tcp::P2P;
use snarkvm::{
    ledger::{narwhal::Data, store::helpers::memory::ConsensusMemory},
    prelude::{FromBytes, TestRng, ToBytes},
};

use std::{io, net::SocketAddr, sync::Arc, time::Duration};

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
    let (accounts, committee) = shared_test_committee(COMMITTEE_SIZE, rng);
    let accounts_ = accounts.clone();
    let mut rng_ = TestRng::fixed(rng.random());
    let ledger = task::spawn_blocking(move || sample_ledger(&accounts_, &committee, &mut rng_)).await.unwrap();

    let mut gateways = Vec::with_capacity(n);
    for account in accounts.iter().take(n) {
        gateways.push(run_test_gateway(account.clone(), ledger.clone()).await);
    }

    (accounts, gateways)
}

/// Builds and runs a gateway for `account` on the given ledger.
async fn run_test_gateway(
    account: Account<CurrentNetwork>,
    ledger: Arc<TranslucentLedgerService<CurrentNetwork, ConsensusMemory<CurrentNetwork>>>,
) -> Gateway<CurrentNetwork> {
    let storage = sample_storage(ledger.clone());
    let gateway = sample_gateway(account, storage, ledger);

    // Set up primary channels; the rx is discarded as these tests exercise the gateway alone.
    let (primary_tx, _primary_rx) = init_primary_channels();
    gateway.run(primary_tx, [].into(), None).await;

    gateway
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

    let sign = honest_signer(peer.clone());
    let (verdict, mut stream) = handshake_with_gateway(&gateway, &peer, 4140, sign).await.unwrap();
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

/// Returns a signature producer that proves the ownership of `account` honestly.
fn honest_signer(account: Account<CurrentNetwork>) -> impl Fn(&[u8]) -> Vec<u8> {
    move |binding| account.sign_bytes(binding, &mut rand::rng()).unwrap().to_bytes_le().unwrap()
}

/// Returns the little-endian bytes of the hash of the gateway's genesis block.
fn genesis_hash(gateway: &Gateway<CurrentNetwork>) -> [u8; 32] {
    gateway.ledger().get_block_hash(0).unwrap().to_bytes_le().unwrap().try_into().unwrap()
}

/// Returns the trailer of a peer that shares the gateway's genesis block.
fn trailer_for(gateway: &Gateway<CurrentNetwork>) -> HandshakeTrailer {
    HandshakeTrailer::new(ConsensusSchedule::of::<CurrentNetwork>(), genesis_hash(gateway))
}

/// Drives a Noise handshake against a gateway by hand, up to and including the verdict, and hands the
/// stream back so that a test can carry on speaking events over it.
///
/// The signature over the binding is produced by `sign`, so that a test can decide whether to
/// authenticate honestly or not. It announces the current event version, this build's schedule and
/// the gateway's genesis hash.
async fn handshake_with_gateway(
    gateway: &Gateway<CurrentNetwork>,
    account: &Account<CurrentNetwork>,
    listener_port: u16,
    sign: impl Fn(&[u8]) -> Vec<u8>,
) -> io::Result<(ResponderProof<CurrentNetwork>, TcpStream)> {
    let version = Event::<CurrentNetwork>::VERSION;
    handshake_with_gateway_sending(dial_addr(gateway), account, listener_port, version, trailer_for(gateway), sign)
        .await
}

/// Drives a Noise handshake like [`handshake_with_gateway`], announcing the event `version` and
/// sending `trailer` in the third message, in the encoding a peer at that version sends. The gateway's
/// second message must state its own version and carry what a peer at `version` requires.
///
/// A peer at [`HandshakeTrailer::CBOR_VERSION`] or above must be given a trailer with every field.
async fn handshake_with_gateway_sending(
    gateway_addr: SocketAddr,
    account: &Account<CurrentNetwork>,
    listener_port: u16,
    version: u32,
    trailer: HandshakeTrailer,
    sign: impl Fn(&[u8]) -> Vec<u8>,
) -> io::Result<(ResponderProof<CurrentNetwork>, TcpStream)> {
    let mut stream = TcpStream::connect(gateway_addr).await?;
    write_noise_magic(&mut stream).await?;
    let mut noise = NoiseSession::new(stream, Role::Initiator)?;

    // Message 1: the cleartext hint.
    let hint = HandshakeHint { version, listener_port, address: account.address() };
    noise.send(&hint.to_bytes_le().unwrap()).await?;

    // Message 2: the gateway's metadata, which tells us the restrictions ID it expects back.
    let ResponderInfo { info: peer_info, trailer: peer_trailer } =
        decode_responder_info(version, gateway_addr, &noise.recv().await?).unwrap();
    assert_eq!(peer_info.version, Event::<CurrentNetwork>::VERSION);
    assert_eq!(peer_trailer.schedule, Some(ConsensusSchedule::of::<CurrentNetwork>()));
    assert_eq!(peer_trailer.genesis_hash.map(|hash| hash.len()), Some(32));

    // Message 3: our metadata, and whatever `sign` decides to offer as proof.
    let binding = binding_message(HANDSHAKE_DOMAIN, Role::Initiator, &noise.handshake_hash()?);
    let mut our_info = PeerInfo::new(listener_port, account.address(), peer_info.restrictions_id, None);
    our_info.version = version;
    let signature = Data::Buffer(sign(&binding).into());
    let genesis_hash = trailer.genesis_hash.as_deref().and_then(|hash| <[u8; 32]>::try_from(hash).ok());
    let our_message = match trailer.schedule.clone().zip(genesis_hash) {
        Some((schedule, genesis_hash)) => {
            encode_initiator_info(peer_info.version, our_info, schedule, genesis_hash, signature).unwrap()
        }
        // Only the original layout can carry a trailer that lacks a field or holds a malformed one.
        None => InitiatorInfo { info: our_info, signature, trailer }.to_bytes_le().unwrap(),
    };
    noise.send(&our_message).await?;

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

    let (verdict, _stream) = handshake_with_gateway(&gateway, &accounts[1], 4131, sign).await.unwrap();

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
    let ResponderInfo { info: peer_info, .. } =
        decode_responder_info(version, dial_addr(&gateway), &noise.recv().await.unwrap()).unwrap();

    // ...and a different one in the authenticated payload. The hint is what the gateway ran its
    // early checks against, so disagreeing with it must not be a way to slip past them.
    let binding = binding_message(HANDSHAKE_DOMAIN, Role::Initiator, &noise.handshake_hash().unwrap());
    let signature = Data::Object(peer.sign_bytes(&binding, &mut rand::rng()).unwrap());
    let our_info = PeerInfo::new(4133, peer.address(), peer_info.restrictions_id, None);
    let schedule = ConsensusSchedule::of::<CurrentNetwork>();
    let our_message =
        encode_initiator_info(peer_info.version, our_info, schedule, genesis_hash(&gateway), signature).unwrap();
    noise.send(&our_message).await.unwrap();

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
    let ResponderInfo { info: peer_info, .. } =
        decode_responder_info(version, dial_addr(&gateway), &noise.recv().await.unwrap()).unwrap();

    let binding = binding_message(HANDSHAKE_DOMAIN, Role::Initiator, &noise.handshake_hash().unwrap());
    let signature = Data::Object(actual.sign_bytes(&binding, &mut rand::rng()).unwrap());
    let our_info = PeerInfo::new(4134, actual.address(), peer_info.restrictions_id, None);
    let schedule = ConsensusSchedule::of::<CurrentNetwork>();
    let our_message =
        encode_initiator_info(peer_info.version, our_info, schedule, genesis_hash(&gateway), signature).unwrap();
    noise.send(&our_message).await.unwrap();

    // The signature is perfectly valid for `actual`; what must fail is that the gateway ran its
    // early checks against `claimed`.
    let mut noise = noise.into_transport_mode().unwrap();
    let verdict = ResponderProof::<CurrentNetwork>::from_bytes_le(&noise.recv().await.unwrap()).unwrap();

    assert_eq!(verdict, ResponderProof::Rejected { reason: DisconnectReason::ProtocolViolation });
    assert!(!gateway.connected_addresses().contains(&actual.address()));
}

/// The gateway discloses its consensus schedule in the second message.
#[tokio::test(flavor = "multi_thread")]
async fn the_responder_discloses_its_consensus_schedule() {
    let mut rng = TestRng::default();
    let (accounts, gateways) = new_test_gateways(1, &mut rng).await;
    let peer = &accounts[1];

    let mut stream = TcpStream::connect(dial_addr(&gateways[0])).await.unwrap();
    write_noise_magic(&mut stream).await.unwrap();
    let mut noise = NoiseSession::new(stream, Role::Initiator).unwrap();
    let version = Event::<CurrentNetwork>::VERSION;
    let hint = HandshakeHint { version, listener_port: 4141, address: peer.address() };
    noise.send(&hint.to_bytes_le().unwrap()).await.unwrap();

    let ResponderInfo { trailer, .. } =
        decode_responder_info::<CurrentNetwork>(version, dial_addr(&gateways[0]), &noise.recv().await.unwrap())
            .unwrap();
    assert_eq!(trailer.schedule, Some(ConsensusSchedule::of::<CurrentNetwork>()));
}

/// A peer at the current event version that sends its consensus schedule is accepted.
#[tokio::test(flavor = "multi_thread")]
async fn an_initiator_that_discloses_its_consensus_schedule_is_accepted() {
    let mut rng = TestRng::default();
    let (accounts, gateways) = new_test_gateways(1, &mut rng).await;
    let peer = accounts[1].clone();

    let sign = honest_signer(peer.clone());
    let trailer = trailer_for(&gateways[0]);
    let (verdict, _stream) = handshake_with_gateway_sending(
        dial_addr(&gateways[0]),
        &peer,
        4142,
        Event::<CurrentNetwork>::VERSION,
        trailer,
        sign,
    )
    .await
    .unwrap();
    assert!(matches!(verdict, ResponderProof::Accepted { .. }), "the handshake should have been accepted");

    let (gateway, address) = (gateways[0].clone(), peer.address());
    deadline!(Duration::from_secs(5), move || gateway.connected_addresses().contains(&address));
}

/// The gateway weighs the schedule a peer disclosed in the handshake: a committee member ahead of
/// this build holds back the block at the height it is ahead, while too little stake is connected.
#[tokio::test(flavor = "multi_thread")]
async fn the_gateway_weighs_the_schedule_of_an_initiator() {
    let mut rng = TestRng::default();
    let (accounts, gateways) = new_test_gateways(1, &mut rng).await;
    let (gateway, peer) = (gateways[0].clone(), accounts[1].clone());

    let height = u32::MAX - 1_000;
    let mut heights = ConsensusSchedule::of::<CurrentNetwork>().heights().to_vec();
    heights.push(height);
    let trailer = HandshakeTrailer::new(ConsensusSchedule::new(heights).unwrap(), genesis_hash(&gateway));
    assert!(gateway.ensure_block_may_be_built(height).is_ok());

    let sign = honest_signer(peer.clone());
    let (verdict, _stream) = handshake_with_gateway_sending(
        dial_addr(&gateway),
        &peer,
        4143,
        Event::<CurrentNetwork>::VERSION,
        trailer,
        sign,
    )
    .await
    .unwrap();
    assert!(matches!(verdict, ResponderProof::Accepted { .. }), "the handshake should have been accepted");
    let (gateway_, address) = (gateway.clone(), peer.address());
    deadline!(Duration::from_secs(5), move || gateway_.connected_addresses().contains(&address));

    assert!(gateway.ensure_block_may_be_built(height - 1).is_ok());
    assert!(gateway.ensure_block_may_be_built(height).is_err());
}

/// A validator counts as connected only once its schedule is recorded, so that no block check
/// counts its stake as not ahead before its schedule is known.
///
/// The handshake and the heartbeat block their workers while the lock is held, so the test keeps
/// workers to spare.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_validator_counts_as_connected_only_once_its_schedule_is_recorded() {
    let mut rng = TestRng::default();
    let (accounts, gateways) = new_test_gateways(1, &mut rng).await;
    let (gateway, peer) = (gateways[0].clone(), accounts[1].clone());

    // The gateway cannot record the schedule while the lock is held.
    let guard = gateway.upgrade_monitor().lock_for_test();
    let sign = honest_signer(peer.clone());
    let (verdict, _stream) = handshake_with_gateway(&gateway, &peer, 4143, sign).await.unwrap();
    assert!(matches!(verdict, ResponderProof::Accepted { .. }), "the handshake should have been accepted");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!gateway.connected_addresses().contains(&peer.address()));

    drop(guard);
    let address = peer.address();
    deadline!(Duration::from_secs(5), move || gateway.connected_addresses().contains(&address));
}

/// A peer at an event version that predates the schedule is accepted without one.
#[tokio::test(flavor = "multi_thread")]
async fn an_initiator_that_predates_the_consensus_schedule_is_accepted_without_one() {
    let mut rng = TestRng::default();
    let (accounts, gateways) = new_test_gateways(1, &mut rng).await;
    let peer = accounts[1].clone();

    let sign = honest_signer(peer.clone());
    let version = HandshakeTrailer::FIRST_VERSION - 1;
    let (verdict, _stream) = handshake_with_gateway_sending(
        dial_addr(&gateways[0]),
        &peer,
        4144,
        version,
        HandshakeTrailer::default(),
        sign,
    )
    .await
    .unwrap();
    assert!(matches!(verdict, ResponderProof::Accepted { .. }), "the handshake should have been accepted");

    let (gateway, address) = (gateways[0].clone(), peer.address());
    deadline!(Duration::from_secs(5), move || gateway.connected_addresses().contains(&address));
}

/// A peer at an event version that requires the schedule is rejected without one, rather than read
/// as a peer that predates it.
#[tokio::test(flavor = "multi_thread")]
async fn an_initiator_that_owes_a_consensus_schedule_is_rejected_without_one() {
    let mut rng = TestRng::default();
    let (accounts, gateways) = new_test_gateways(1, &mut rng).await;
    let peer = accounts[1].clone();

    let sign = honest_signer(peer.clone());
    let version = HandshakeTrailer::FIRST_VERSION;
    let (verdict, _stream) = handshake_with_gateway_sending(
        dial_addr(&gateways[0]),
        &peer,
        4145,
        version,
        HandshakeTrailer::default(),
        sign,
    )
    .await
    .unwrap();
    assert_eq!(verdict, ResponderProof::Rejected { reason: DisconnectReason::ProtocolViolation });
    assert!(!gateways[0].connected_addresses().contains(&peer.address()));
}

/// A peer that discloses the gateway's own genesis hash passes the genesis check.
#[tokio::test(flavor = "multi_thread")]
async fn an_initiator_that_shares_the_genesis_is_accepted() {
    let mut rng = TestRng::default();
    let (accounts, gateways) = new_test_gateways(1, &mut rng).await;
    let peer = accounts[1].clone();

    let trailer = trailer_for(&gateways[0]);
    let sign = honest_signer(peer.clone());
    let (verdict, _stream) = handshake_with_gateway_sending(
        dial_addr(&gateways[0]),
        &peer,
        4146,
        Event::<CurrentNetwork>::VERSION,
        trailer,
        sign,
    )
    .await
    .unwrap();
    assert!(matches!(verdict, ResponderProof::Accepted { .. }), "the handshake should have been accepted");

    let (gateway, address) = (gateways[0].clone(), peer.address());
    deadline!(Duration::from_secs(5), move || gateway.connected_addresses().contains(&address));
}

/// A peer that discloses a different genesis hash is on another network, and is rejected.
#[tokio::test(flavor = "multi_thread")]
async fn an_initiator_with_a_different_genesis_is_rejected() {
    let mut rng = TestRng::default();
    let (accounts, gateways) = new_test_gateways(1, &mut rng).await;
    let peer = accounts[1].clone();

    let trailer = HandshakeTrailer::new(ConsensusSchedule::of::<CurrentNetwork>(), [0; 32]);
    let sign = honest_signer(peer.clone());
    let (verdict, _stream) = handshake_with_gateway_sending(
        dial_addr(&gateways[0]),
        &peer,
        4147,
        Event::<CurrentNetwork>::VERSION,
        trailer,
        sign,
    )
    .await
    .unwrap();
    assert_eq!(verdict, ResponderProof::Rejected { reason: DisconnectReason::GenesisMismatch });
    assert!(!gateways[0].connected_addresses().contains(&peer.address()));
}

/// A peer at an event version that requires the genesis hash is rejected without one, rather than
/// read as a peer that predates it.
#[tokio::test(flavor = "multi_thread")]
async fn an_initiator_that_owes_a_genesis_hash_is_rejected_without_one() {
    let mut rng = TestRng::default();
    let (accounts, gateways) = new_test_gateways(1, &mut rng).await;
    let peer = accounts[1].clone();

    let sign = honest_signer(peer.clone());
    let (verdict, _stream) = handshake_with_gateway_sending(
        dial_addr(&gateways[0]),
        &peer,
        4148,
        HandshakeTrailer::GENESIS_VERSION,
        HandshakeTrailer { schedule: Some(ConsensusSchedule::of::<CurrentNetwork>()), genesis_hash: None },
        sign,
    )
    .await
    .unwrap();
    assert_eq!(verdict, ResponderProof::Rejected { reason: DisconnectReason::ProtocolViolation });
    assert!(!gateways[0].connected_addresses().contains(&peer.address()));
}

/// A peer at an event version that predates the genesis hash is accepted without one.
#[tokio::test(flavor = "multi_thread")]
async fn an_initiator_that_predates_the_genesis_hash_is_accepted_without_one() {
    let mut rng = TestRng::default();
    let (accounts, gateways) = new_test_gateways(1, &mut rng).await;
    let peer = accounts[1].clone();

    let sign = honest_signer(peer.clone());
    let (verdict, _stream) = handshake_with_gateway_sending(
        dial_addr(&gateways[0]),
        &peer,
        4149,
        HandshakeTrailer::GENESIS_VERSION - 1,
        HandshakeTrailer { schedule: Some(ConsensusSchedule::of::<CurrentNetwork>()), genesis_hash: None },
        sign,
    )
    .await
    .unwrap();
    assert!(matches!(verdict, ResponderProof::Accepted { .. }), "the handshake should have been accepted");

    let (gateway, address) = (gateways[0].clone(), peer.address());
    deadline!(Duration::from_secs(5), move || gateway.connected_addresses().contains(&address));
}

/// A genesis hash cut short in the trailer is malformed, not absent, so it is rejected even from a
/// peer at an event version that predates the genesis hash.
#[tokio::test(flavor = "multi_thread")]
async fn an_initiator_with_a_truncated_genesis_hash_is_rejected() {
    let mut rng = TestRng::default();
    let (accounts, gateways) = new_test_gateways(1, &mut rng).await;
    let peer = accounts[1].clone();

    for (version, listener_port) in
        [(HandshakeTrailer::GENESIS_VERSION - 1, 4150), (HandshakeTrailer::GENESIS_VERSION, 4151)]
    {
        let truncated = genesis_hash(&gateways[0])[..31].to_vec();
        let trailer = HandshakeTrailer {
            schedule: Some(ConsensusSchedule::of::<CurrentNetwork>()),
            genesis_hash: Some(truncated),
        };
        let sign = honest_signer(peer.clone());
        let (verdict, _stream) =
            handshake_with_gateway_sending(dial_addr(&gateways[0]), &peer, listener_port, version, trailer, sign)
                .await
                .unwrap();
        assert_eq!(verdict, ResponderProof::Rejected { reason: DisconnectReason::GenesisMismatch });
        assert!(!gateways[0].connected_addresses().contains(&peer.address()));
    }
}

/// A genesis hash cut short in the CBOR map of a peer at [`HandshakeTrailer::CBOR_VERSION`] makes the
/// whole message malformed, which the gateway rejects as a protocol violation.
#[tokio::test(flavor = "multi_thread")]
async fn an_initiator_with_a_truncated_genesis_hash_in_cbor_is_rejected() {
    let mut rng = TestRng::default();
    let (accounts, gateways) = new_test_gateways(1, &mut rng).await;
    let (gateway, peer) = (gateways[0].clone(), accounts[1].clone());

    let mut stream = TcpStream::connect(dial_addr(&gateway)).await.unwrap();
    write_noise_magic(&mut stream).await.unwrap();
    let mut noise = NoiseSession::new(stream, Role::Initiator).unwrap();

    let version = Event::<CurrentNetwork>::VERSION;
    let hint = HandshakeHint { version, listener_port: 4153, address: peer.address() };
    noise.send(&hint.to_bytes_le().unwrap()).await.unwrap();
    let ResponderInfo { info: peer_info, .. } =
        decode_responder_info(version, dial_addr(&gateway), &noise.recv().await.unwrap()).unwrap();

    let binding = binding_message(HANDSHAKE_DOMAIN, Role::Initiator, &noise.handshake_hash().unwrap());
    let signature = Data::Object(peer.sign_bytes(&binding, &mut rand::rng()).unwrap());
    let our_info = PeerInfo::new(4153, peer.address(), peer_info.restrictions_id, None);
    let schedule = ConsensusSchedule::of::<CurrentNetwork>();
    let genesis_hash = genesis_hash(&gateway);
    let message_3 = encode_initiator_info(peer_info.version, our_info, schedule, genesis_hash, signature).unwrap();

    // Re-encode the value of "genesis", a 32-byte string, as its first 31 bytes.
    let genesis_entry = [&b"\x67genesis\x58\x20"[..], &genesis_hash].concat();
    let start = message_3.windows(genesis_entry.len()).position(|window| window == genesis_entry).unwrap();
    let truncated_entry = [&b"\x67genesis\x58\x1f"[..], &genesis_hash[..31]].concat();
    let message_3 = [&message_3[..start], &truncated_entry[..], &message_3[start + genesis_entry.len()..]].concat();
    noise.send(&message_3).await.unwrap();

    let mut noise = noise.into_transport_mode().unwrap();
    let verdict = ResponderProof::<CurrentNetwork>::from_bytes_le(&noise.recv().await.unwrap()).unwrap();

    assert_eq!(verdict, ResponderProof::Rejected { reason: DisconnectReason::ProtocolViolation });
    assert!(!gateway.connected_addresses().contains(&peer.address()));
}

/// A gateway that dials a validator on a network with a different genesis block turns it away on its
/// metadata, before signing anything for it.
#[tokio::test(flavor = "multi_thread")]
async fn an_initiator_rejects_a_responder_with_a_different_genesis() {
    let mut rng = TestRng::default();
    let (accounts, committee) = shared_test_committee(COMMITTEE_SIZE, &mut rng);

    // Two chains with the same committee. The test genesis cache is keyed by the generator key, which
    // `sample_ledger` takes from the first account, so the second chain starts from another member.
    let mut other_accounts = accounts.clone();
    other_accounts.rotate_left(1);
    let mut ledgers = Vec::with_capacity(2);
    for chain_accounts in [accounts.clone(), other_accounts] {
        let committee = committee.clone();
        let mut rng_ = TestRng::fixed(rng.random());
        ledgers
            .push(task::spawn_blocking(move || sample_ledger(&chain_accounts, &committee, &mut rng_)).await.unwrap());
    }

    let gateway_a = run_test_gateway(accounts[0].clone(), ledgers[0].clone()).await;
    let gateway_b = run_test_gateway(accounts[1].clone(), ledgers[1].clone()).await;
    assert_ne!(genesis_hash(&gateway_a), genesis_hash(&gateway_b));

    // The reason reaches the caller as text, having passed through an I/O error on the way out of the
    // handshake.
    let error = gateway_a.tcp().connect(dial_addr(&gateway_b)).await.unwrap_err();
    assert_eq!(error.to_string(), DisconnectReason::GenesisMismatch.to_string());

    // Neither side may end up considering the other a peer.
    assert!(!gateway_a.connected_addresses().contains(&accounts[1].address()));
    assert!(!gateway_b.connected_addresses().contains(&accounts[0].address()));
}

/// A gateway answers an initiator below [`HandshakeTrailer::CBOR_VERSION`] in the original layout,
/// which still states the gateway's own version and meets what that peer requires of it.
#[tokio::test(flavor = "multi_thread")]
async fn a_gateway_answers_an_initiator_below_the_cbor_version_in_the_original_layout() {
    let mut rng = TestRng::default();
    let (accounts, gateways) = new_test_gateways(1, &mut rng).await;
    let (gateway, peer) = (gateways[0].clone(), accounts[1].clone());

    let version = HandshakeTrailer::CBOR_VERSION - 1;
    let sign = honest_signer(peer.clone());
    let (verdict, _stream) =
        handshake_with_gateway_sending(dial_addr(&gateway), &peer, 4152, version, trailer_for(&gateway), sign)
            .await
            .unwrap();
    assert!(matches!(verdict, ResponderProof::Accepted { .. }), "the handshake should have been accepted");

    let address = peer.address();
    deadline!(Duration::from_secs(5), move || gateway.connected_addresses().contains(&address));
}

/// Answers a gateway's dial as a responder below [`HandshakeTrailer::CBOR_VERSION`], which speaks the
/// original layout, and returns the gateway's third message as such a peer reads it, along with the
/// binding that message's signature is over.
///
/// The stream is handed back, so that the connection stays open.
async fn respond_in_the_original_layout(
    listener: TcpListener,
    gateway: Gateway<CurrentNetwork>,
    account: Account<CurrentNetwork>,
) -> io::Result<(InitiatorInfo<CurrentNetwork>, Vec<u8>, TcpStream)> {
    let listener_port = listener.local_addr()?.port();
    let (mut stream, _) = listener.accept().await?;
    let (protocol, _) = detect_handshake_protocol(&mut stream).await?;
    assert_eq!(protocol, HandshakeProtocol::Noise);

    let pending = PendingSession::accept(stream).await?;
    let hint = HandshakeHint::<CurrentNetwork>::from_bytes_le(pending.first_payload()?).unwrap();
    assert_eq!(hint.version, Event::<CurrentNetwork>::VERSION);
    let mut noise = pending.into_session()?;

    let restrictions_id = gateway.ledger().latest_restrictions_id();
    let mut our_info = PeerInfo::new(listener_port, account.address(), restrictions_id, None);
    our_info.version = HandshakeTrailer::CBOR_VERSION - 1;
    noise.send(&ResponderInfo { info: our_info, trailer: trailer_for(&gateway) }.to_bytes_le().unwrap()).await?;
    let peer_binding = binding_message(HANDSHAKE_DOMAIN, Role::Initiator, &noise.handshake_hash()?);

    let message_3 = InitiatorInfo::<CurrentNetwork>::from_bytes_le(&noise.recv().await?).unwrap();
    let binding = binding_message(HANDSHAKE_DOMAIN, Role::Responder, &noise.handshake_hash()?);
    let mut noise = noise.into_transport_mode()?;
    let signature = account.sign_bytes(&binding, &mut rand::rng()).unwrap();
    noise.send(&ResponderProof::Accepted { signature: Data::Object(signature) }.to_bytes_le().unwrap()).await?;

    Ok((message_3, peer_binding, noise.into_inner()))
}

/// A gateway that dials a responder below [`HandshakeTrailer::CBOR_VERSION`] sends it the original
/// layout, which still states the gateway's own version and meets what that peer requires of it.
#[tokio::test(flavor = "multi_thread")]
async fn a_gateway_initiates_with_a_responder_below_the_cbor_version_in_the_original_layout() {
    let mut rng = TestRng::default();
    let (accounts, gateways) = new_test_gateways(1, &mut rng).await;
    let (gateway, peer) = (gateways[0].clone(), accounts[1].clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener_addr = listener.local_addr().unwrap();
    let responder = task::spawn(respond_in_the_original_layout(listener, gateway.clone(), peer.clone()));

    gateway.tcp().connect(listener_addr).await.unwrap();
    let (message_3, binding, _stream) = responder.await.unwrap().unwrap();

    assert_eq!(message_3.info.version, Event::<CurrentNetwork>::VERSION);
    assert_eq!(message_3.info.address, accounts[0].address());
    assert_eq!(message_3.trailer, trailer_for(&gateway));
    assert!(message_3.signature.deserialize_blocking().unwrap().verify_bytes(&accounts[0].address(), &binding));

    let address = peer.address();
    deadline!(Duration::from_secs(5), move || gateway.connected_addresses().contains(&address));
}
