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
    node::client_with,
    test_peer::{TestPeer, connect_peer},
};

use snarkos_node_network::{ConnectionMode, NodeType, PeerPoolHandling};
use snarkos_node_router::messages::{BlockRequest, BlockResponse, BlockResponseStatus, Message, Ping};
use snarkos_node_sync::BlockSync;
use snarkvm::{
    ledger::{LoadOptions, store::min_authority_retention_blocks},
    prelude::{
        Ledger,
        MainnetV0 as CurrentNetwork,
        PrivateKey,
        TestRng,
        VM,
        block::Block,
        store::{
            ConsensusStore,
            StorageMode,
            helpers::{memory::ConsensusMemory, rocksdb::ConsensusDB},
        },
    },
};

use deadline::deadline;
use pea2pea::protocols::Writing;
use std::time::Duration;

/// The tip of the pruned ledger.
const TIP: u32 = 110;

/// Builds a ledger at `storage_mode` that keeps the authority data of only snarkVM's smallest window,
/// and advances it to `TIP` with beacon blocks. Returns its genesis block and its watermark.
fn sample_pruned_ledger(storage_mode: StorageMode) -> (Block<CurrentNetwork>, u32) {
    let rng = &mut TestRng::default();
    let private_key = PrivateKey::<CurrentNetwork>::new(rng).unwrap();
    let store = ConsensusStore::<CurrentNetwork, ConsensusMemory<_>>::open(StorageMode::new_test(None)).unwrap();
    let genesis = VM::from(store).unwrap().genesis_beacon(&private_key, rng).unwrap();
    let options =
        LoadOptions::default().with_authority_retention(Some(min_authority_retention_blocks::<CurrentNetwork>()));
    let ledger =
        Ledger::<CurrentNetwork, ConsensusDB<_>>::load_with_options(genesis.clone(), storage_mode, options).unwrap();
    while ledger.latest_height() < TIP {
        let block = ledger.prepare_advance_to_next_beacon_block(&private_key, vec![], vec![], vec![], rng).unwrap();
        ledger.advance_to_next_block(&block).unwrap();
    }
    (genesis, ledger.vm().block_store().authority_pruned_below().unwrap().unwrap())
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

/// Returns the first `BlockResponse` the peer received for `request`.
fn response_to(peer: &TestPeer, request: BlockRequest) -> Option<BlockResponse<CurrentNetwork>> {
    peer.received_messages().into_iter().find_map(|message| match message {
        Message::BlockResponse(response) if response.request == request => Some(response),
        _ => None,
    })
}

/// Tests that a client whose ledger was pruned, and that now runs without
/// `--prune-authority-data`, advertises its watermark `W` as its floor, answers a request below `W`
/// with `Pruned { W }`, and serves the blocks from `W` on.
#[tokio::test]
async fn pruned_client_serves_from_its_watermark() {
    let storage_mode = StorageMode::new_test(None);
    let storage_mode_ = storage_mode.clone();
    let (genesis, w) = tokio::task::spawn_blocking(move || sample_pruned_ledger(storage_mode_)).await.unwrap();
    assert_eq!(w, TIP - (min_authority_retention_blocks::<CurrentNetwork>() - 1));

    let node = client_with::<ConsensusDB<CurrentNetwork>>(genesis.clone(), storage_mode).await;
    assert_eq!(node.ledger().latest_height(), TIP);
    let account = common::sample_account(&mut TestRng::default());
    let version = Message::<CurrentNetwork>::STATUS_MESSAGE_VERSION;
    let peer = TestPeer::with_genesis(NodeType::Client, account, version, *genesis.header()).await;
    let node_addr = connect_peer(node.router(), &peer).await;

    // The `Ping` carries the watermark as the floor.
    let locators = BlockSync::new(node.router().ledger().clone(), ConnectionMode::Router).get_block_locators().unwrap();
    assert!(node.router().send_ping(peer.listening_addr().await, Some(locators)));
    let peer_clone = peer.clone();
    deadline!(Duration::from_secs(10), move || !pings(&peer_clone).is_empty());
    assert_eq!(pings(&peer).last().unwrap().oldest_retained, Some(w));

    // A request below the watermark, and one that crosses it, are `Pruned { W }`.
    for request in
        [BlockRequest { start_height: 1, end_height: 6 }, BlockRequest { start_height: w - 1, end_height: w + 1 }]
    {
        peer.unicast(node_addr, Message::BlockRequest(request)).unwrap();
        let peer_clone = peer.clone();
        deadline!(Duration::from_secs(10), move || response_to(&peer_clone, request).is_some());
        assert_eq!(response_to(&peer, request).unwrap().status, BlockResponseStatus::Pruned { oldest_retained: w });
    }

    // The blocks from the watermark on are served.
    let request = BlockRequest { start_height: w, end_height: w + 5 };
    peer.unicast(node_addr, Message::BlockRequest(request)).unwrap();
    let peer_clone = peer.clone();
    deadline!(Duration::from_secs(10), move || response_to(&peer_clone, request).is_some());
    let response = response_to(&peer, request).unwrap();
    assert_eq!(response.status, BlockResponseStatus::Blocks);
    assert_eq!(response.blocks.deserialize_blocking().unwrap().len(), 5);
    assert_eq!(node.router().number_of_connected_peers(), 1);
}
