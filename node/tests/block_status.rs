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

#![recursion_limit = "256"]

#[allow(dead_code)]
mod common;
use common::{
    node,
    sample_genesis_block,
    test_peer::{TestPeer, connect_peer},
};

use snarkos_node::Client;
use snarkos_node_network::{NodeType, PeerPoolHandling};
use snarkos_node_router::{
    Router,
    messages::{BlockRequest, BlockResponse, BlockResponseStatus, Message, Ping},
};
use snarkos_node_sync::locators::{BlockLocators, CHECKPOINT_INTERVAL};
use snarkvm::prelude::{Field, MainnetV0 as CurrentNetwork, TestRng, store::helpers::memory::ConsensusMemory};

use deadline::deadline;
use pea2pea::protocols::Writing;
use std::{net::SocketAddr, time::Duration};

/// The first message version that receives status replies and `Ping` floors.
const STATUS_VERSION: u32 = Message::<CurrentNetwork>::STATUS_MESSAGE_VERSION;

/// Returns a test peer that advertises message version `version`, with an account from `seed`.
async fn peer_at(version: u32, seed: u64) -> TestPeer {
    let account = common::sample_account(&mut TestRng::from_seed(seed));
    TestPeer::with_version(NodeType::Client, account, version).await
}

/// Returns the `Ping`s the peer received.
fn pings(peer: &TestPeer) -> Vec<Ping<CurrentNetwork>> {
    peer.received_messages()
        .into_iter()
        .filter_map(|message| match message {
            Message::Ping(ping) => Some(ping),
            _ => None,
        })
        .collect()
}

/// Has the node's router send the peer a `Ping` with block locators, and returns it as received.
///
/// The `Ping` the node sends on connect can arrive within the test peer's handshake reads and be
/// dropped with them, so the test sends its own.
async fn ping_peer(router: &Router<CurrentNetwork>, peer: &TestPeer) -> Ping<CurrentNetwork> {
    let num_pings = pings(peer).len();
    assert!(router.send_ping(peer.listening_addr().await, Some(locators(0)), router.oldest_retained_height()));
    let peer_clone = peer.clone();
    deadline!(Duration::from_secs(5), move || pings(&peer_clone).len() > num_pings);
    pings(peer).pop().unwrap()
}

/// Returns the first `BlockResponse` the peer received.
fn first_block_response(peer: &TestPeer) -> Option<BlockResponse<CurrentNetwork>> {
    peer.received_messages().into_iter().find_map(|message| match message {
        Message::BlockResponse(response) => Some(response),
        _ => None,
    })
}

/// Returns how often the peer received `request`.
fn times_requested(peer: &TestPeer, request: BlockRequest) -> usize {
    peer.received_messages().iter().filter(|m| matches!(m, Message::BlockRequest(r) if *r == request)).count()
}

/// Returns block locators up to `tip` that agree with the node's genesis block.
fn locators(tip: u32) -> BlockLocators<CurrentNetwork> {
    assert!(tip < CHECKPOINT_INTERVAL);
    let genesis_hash = sample_genesis_block().hash();
    let hash = |height: u32| match height {
        0 => (0, genesis_hash),
        _ => (height, Field::<CurrentNetwork>::from_u32(height).into()),
    };
    BlockLocators::new((0..=tip).map(hash).collect(), [hash(0)].into_iter().collect()).unwrap()
}

macro_rules! test_block_status_gating {
    ($($node_type:ident),*) => {
        $(
            paste::paste! {
                /// Tests that a peer below the status version gets `Ping` marker 1, and is
                /// disconnected when it asks for blocks past the tip.
                #[tokio::test]
                async fn [<$node_type _sends_legacy_replies_to_old_peer>]() {
                    let node = node::$node_type().await;
                    let peer = peer_at(STATUS_VERSION - 1, 1).await;
                    let node_addr = connect_peer(node.router(), &peer).await;

                    let ping = ping_peer(node.router(), &peer).await;
                    assert!(ping.block_locators.is_some());
                    assert_eq!(ping.oldest_retained, None);

                    let request = BlockRequest { start_height: 1, end_height: 3 };
                    peer.unicast(node_addr, Message::BlockRequest(request)).unwrap();
                    let node_clone = node.clone();
                    deadline!(Duration::from_secs(5), move || node_clone.router().number_of_connected_peers() == 0);
                    assert!(first_block_response(&peer).is_none());
                }

                /// Tests that a peer at the status version gets `Ping` marker 2 with floor 0, and a
                /// `NotYet` reply when it asks for blocks past the tip.
                #[tokio::test]
                async fn [<$node_type _sends_status_replies_to_new_peer>]() {
                    let node = node::$node_type().await;
                    let peer = peer_at(STATUS_VERSION, 1).await;
                    let node_addr = connect_peer(node.router(), &peer).await;

                    assert_eq!(ping_peer(node.router(), &peer).await.oldest_retained, Some(0));

                    let request = BlockRequest { start_height: 1, end_height: 3 };
                    peer.unicast(node_addr, Message::BlockRequest(request)).unwrap();
                    let peer_clone = peer.clone();
                    deadline!(Duration::from_secs(5), move || first_block_response(&peer_clone).is_some());
                    let response = first_block_response(&peer).unwrap();
                    assert_eq!(response.request, request);
                    assert_eq!(response.status, BlockResponseStatus::NotYet { latest_height: 0 });
                    assert_eq!(node.router().number_of_connected_peers(), 1);
                }
            }
        )*
    };
}

test_block_status_gating!(client, validator);

/// The tip the scripted peers advertise.
const TIP: u32 = 12;

/// Connects a peer at the status version to the client and sends it a `Ping` with locators up to
/// `TIP` and floor 0.
async fn connect_serving_peer(
    node: &Client<CurrentNetwork, ConsensusMemory<CurrentNetwork>>,
    seed: u64,
) -> (TestPeer, SocketAddr) {
    let peer = peer_at(STATUS_VERSION, seed).await;
    let node_addr = connect_peer(node.router(), &peer).await;
    let ping = Ping::new(NodeType::Client, Some(locators(TIP)), Some(0));
    peer.unicast(node_addr, Message::Ping(ping)).unwrap();
    (peer, node_addr)
}

/// Tests that a `Pruned` reply re-routes the request to a peer that serves it, and never back to
/// the peer that sent it.
#[tokio::test]
async fn pruned_reply_reroutes_request() {
    let node = node::client().await;
    let chunk = BlockRequest { start_height: 1, end_height: 6 };

    // Peer A is the only peer, so the node sends it the first chunk.
    let (peer_a, node_addr_a) = connect_serving_peer(&node, 1).await;
    let peer_clone = peer_a.clone();
    deadline!(Duration::from_secs(10), move || times_requested(&peer_clone, chunk) == 1);

    // Peer B connects, then peer A replies that it serves heights from 6 only.
    let (peer_b, _) = connect_serving_peer(&node, 2).await;
    let reply = BlockResponse::unavailable(chunk, BlockResponseStatus::Pruned { oldest_retained: 6 });
    peer_a.unicast(node_addr_a, Message::BlockResponse(reply)).unwrap();

    let peer_clone = peer_b.clone();
    deadline!(Duration::from_secs(10), move || times_requested(&peer_clone, chunk) == 1);
    // Peer B holds the chunk now. A `Ping` sent to peer A after that arrives after any request the
    // node sent peer A before it.
    ping_peer(node.router(), &peer_a).await;
    assert_eq!(times_requested(&peer_a, chunk), 1);
    assert_eq!(node.router().number_of_connected_peers(), 2);
}

/// Tests that a reply that contradicts its own request disconnects the peer.
#[tokio::test]
async fn contradictory_reply_disconnects() {
    let node = node::client().await;
    let chunk = BlockRequest { start_height: 1, end_height: 6 };

    let (peer, node_addr) = connect_serving_peer(&node, 1).await;
    let peer_clone = peer.clone();
    deadline!(Duration::from_secs(10), move || times_requested(&peer_clone, chunk) == 1);

    let reply = BlockResponse::unavailable(chunk, BlockResponseStatus::Pruned { oldest_retained: 1 });
    peer.unicast(node_addr, Message::BlockResponse(reply)).unwrap();
    let node_clone = node.clone();
    deadline!(Duration::from_secs(5), move || node_clone.router().number_of_connected_peers() == 0);
}
