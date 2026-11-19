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

mod common;
use common::*;

use snarkos_node_network::{ConnectionMode, NodeType, Peer, PeerPoolHandling};
use snarkos_node_router::messages::Message;
use snarkvm::{prelude::MainnetV0 as CurrentNetwork, utilities::TestRng};

use std::net::SocketAddr;

/// Tests that only peers connected at `STATUS_MESSAGE_VERSION` or above receive status replies
/// and `Ping` floors.
#[tokio::test]
async fn status_replies_need_the_status_message_version() {
    let rng = &mut TestRng::default();
    let router = client(0, 10, rng).await;
    let status_version = Message::<CurrentNetwork>::STATUS_MESSAGE_VERSION;

    let connect = |port: u16, version: u32| {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let mut peer = Peer::new_connecting(addr, false);
        peer.upgrade_to_connected(
            addr,
            port,
            router.address(),
            NodeType::Client,
            version,
            None,
            ConnectionMode::Router,
        );
        router.peer_pool().write().insert(addr, peer);
        addr
    };
    let old_peer = connect(4001, status_version - 1);
    let new_peer = connect(4002, status_version);

    assert!(!router.supports_status_replies(old_peer));
    assert!(router.supports_status_replies(new_peer));
    assert!(!router.supports_status_replies(SocketAddr::from(([127, 0, 0, 1], 4003))));
}
