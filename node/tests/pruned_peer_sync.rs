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
    node::client_with_genesis,
    test_peer::{TestPeer, connect_peer, sample_account},
};

use snarkos_node_network::{NodeType, PeerPoolHandling};
use snarkos_node_router::messages::{BlockRequest, BlockResponse, DataBlocks, Message, Ping};
use snarkos_node_sync::locators::{BlockLocators, CHECKPOINT_INTERVAL, NUM_RECENT_BLOCKS};
use snarkvm::{
    ledger::Ledger,
    prelude::{
        MainnetV0 as CurrentNetwork,
        Network,
        PrivateKey,
        TestRng,
        VM,
        block::Block,
        store::{ConsensusStore, helpers::memory::ConsensusMemory},
    },
};

use aleo_std::StorageMode;
use deadline::deadline;
use pea2pea::{Pea2Pea, protocols::Writing};
use std::{sync::Arc, time::Duration};

type Chain = Arc<Vec<Block<CurrentNetwork>>>;

/// The tip of the serving peer.
const TIP: u32 = 12;
/// The peer's retention floor: it cannot serve a request that starts below it.
const FLOOR: u32 = 4;
/// The blocks in one block request.
const CHUNK: u32 = DataBlocks::<CurrentNetwork>::MAXIMUM_NUMBER_OF_BLOCKS as u32;

/// Builds `TIP` empty beacon blocks on a dev genesis. snarkVM refuses to build blocks from an
/// async context, so this runs on a blocking thread.
async fn sample_chain() -> Chain {
    tokio::task::spawn_blocking(|| {
        let rng = &mut TestRng::default();
        let private_key = PrivateKey::<CurrentNetwork>::new(rng).unwrap();
        let store = ConsensusStore::<CurrentNetwork, ConsensusMemory<_>>::open(StorageMode::new_test(None)).unwrap();
        let genesis = VM::from(store).unwrap().genesis_beacon(&private_key, rng).unwrap();
        let ledger =
            Ledger::<CurrentNetwork, ConsensusMemory<_>>::load(genesis.clone(), StorageMode::new_test(None)).unwrap();
        let mut blocks = vec![genesis];
        for _ in 0..TIP {
            let block = ledger.prepare_advance_to_next_beacon_block(&private_key, vec![], vec![], vec![], rng).unwrap();
            ledger.advance_to_next_block(&block).unwrap();
            blocks.push(block);
        }
        Arc::new(blocks)
    })
    .await
    .unwrap()
}

/// Returns the block locators of a peer that holds `blocks`.
fn locators(blocks: &Chain) -> BlockLocators<CurrentNetwork> {
    let tip = blocks.len() as u32 - 1;
    let hash = |height: u32| (height, blocks[height as usize].hash());
    let recents = (tip.saturating_sub(NUM_RECENT_BLOCKS as u32 - 1)..=tip).map(hash).collect();
    let checkpoints = (0..=tip).step_by(CHECKPOINT_INTERVAL as usize).map(hash).collect();
    BlockLocators::new(recents, checkpoints).unwrap()
}

/// Returns the response of a node that has pruned the authority data below `FLOOR` (see
/// `blocks_or_empty`): empty for a range that starts below it, the blocks otherwise. A request
/// past the tip is reported and left unanswered, since a panic on the peer's reading task would
/// not reach the test.
fn response(blocks: &Chain, request: BlockRequest) -> Option<Message<CurrentNetwork>> {
    let range = request.start_height as usize..request.end_height as usize;
    let Some(blocks) = blocks.get(range) else {
        eprintln!("{request:?} reaches past the peer's tip");
        return None;
    };
    let served = if request.start_height < FLOOR { vec![] } else { blocks.to_vec() };
    let version = CurrentNetwork::CONSENSUS_VERSION(request.end_height - 1).unwrap();
    Some(Message::BlockResponse(BlockResponse::new(request, DataBlocks(served), version)))
}

/// Returns how often `peer` received `request`.
fn times_requested(peer: &TestPeer, request: BlockRequest) -> usize {
    peer.received_messages().iter().filter(|m| matches!(m, Message::BlockRequest(r) if *r == request)).count()
}

/// Tests the requesting side of sync against a peer that has pruned the lowest blocks the node
/// needs. The empty response to the request straddling the peer's floor is benign: the node keeps
/// the connection, releases that range without re-sending it from the same stale tip, and keeps
/// the requests the peer did serve. Completing sync would need beacon blocks, which snarkVM only
/// accepts with its `test` feature, so the servable part is checked at the request level.
#[tokio::test]
async fn client_keeps_served_requests_after_empty_response() {
    let blocks = sample_chain().await;
    let node = client_with_genesis(blocks[0].clone()).await;

    // The peer serves every request above its floor at once, and holds the reply to the request
    // straddling it until the node's later requests are in flight too.
    let peer = TestPeer::with_genesis(NodeType::Client, sample_account(), *blocks[0].header()).await;
    let blocks_clone = blocks.clone();
    peer.set_responder(move |message| match message {
        Message::BlockRequest(request) if request.start_height >= FLOOR => response(&blocks_clone, *request),
        _ => None,
    });
    let (peer, node_addr) = connect_peer(node.router(), peer).await;
    peer.unicast(node_addr, Message::Ping(Ping::new(NodeType::Client, Some(locators(&blocks))))).unwrap();

    // The node requests `1..=TIP` in chunks.
    let chunk = |start| BlockRequest { start_height: start, end_height: (start + CHUNK).min(TIP + 1) };
    let chunks = [chunk(1), chunk(1 + CHUNK), chunk(1 + 2 * CHUNK)];
    let peer_clone = peer.clone();
    deadline!(Duration::from_secs(10), move || chunks.iter().all(|c| times_requested(&peer_clone, *c) == 1));

    // The first chunk straddles the floor and is answered empty. Its range is released, but not
    // re-sent to the peer from the same tip; the later chunks are left in flight.
    peer.unicast(node_addr, response(&blocks, chunk(1)).unwrap()).unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    for chunk in chunks {
        assert_eq!(times_requested(&peer, chunk), 1, "{chunk:?}");
    }
    assert_eq!(node.router().number_of_connected_peers(), 1, "the empty response did not cost the connection");
    assert_eq!(peer.node().num_connected(), 1);
    assert_eq!(node.ledger().latest_height(), 0);
}
