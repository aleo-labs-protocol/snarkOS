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
use common::test_peer::connect_test_peer;

use snarkos_node_network::PeerPoolHandling;
use snarkos_node_router::messages::{BlockRequest, Message};

use deadline::deadline;
use paste::paste;
use pea2pea::protocols::Writing;
use std::time::Duration;

macro_rules! test_block_request_handling {
    ($($node_type:ident),*) => {
        $(
            paste! {
                /// Tests that a node replies with an empty `BlockResponse`, the explicit
                /// "cannot serve this range" signal, when asked for blocks it does not have,
                /// instead of disconnecting the requester.
                #[tokio::test]
                async fn [<$node_type _sends_empty_response_when_blocks_are_unavailable>]() {
                    // Spin up a full node with a genesis-only ledger, connected to a test peer.
                    let node = $crate::common::node::$node_type().await;
                    let (peer, node_addr) = connect_test_peer(node.router()).await;

                    // Request blocks the node does not have (the ledger only contains genesis).
                    let request = BlockRequest { start_height: 1, end_height: 3 };
                    assert!(peer.unicast(node_addr, Message::BlockRequest(request)).is_ok());

                    // The node replies with an empty block response instead of disconnecting.
                    let peer_clone = peer.clone();
                    deadline!(Duration::from_secs(5), move || {
                        peer_clone.received_messages().into_iter().any(|message| match message {
                            Message::BlockResponse(response) => {
                                assert_eq!(response.request, request);
                                let blocks = response.blocks.deserialize_blocking().unwrap();
                                assert!(blocks.is_empty(), "expected an empty block response");
                                true
                            }
                            _ => false,
                        })
                    });

                    // The node remains connected to the peer.
                    assert_eq!(node.router().number_of_connected_peers(), 1);
                }
            }
        )*
    };
}

test_block_request_handling!(client, validator);
