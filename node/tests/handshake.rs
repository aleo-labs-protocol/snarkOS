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
use common::{node::*, test_peer::TestPeer};

use snarkos_node::{Client, Prover, Validator};
use snarkos_node_network::PeerPoolHandling;
use snarkos_node_tcp::{ConnectError, P2P, protocols::Handshake};
use snarkvm::prelude::{MainnetV0 as CurrentNetwork, store::helpers::memory::ConsensusMemory};

use pea2pea::Pea2Pea;

use deadline::deadline;
use std::{io, net::SocketAddr, time::Duration};
use tokio::time::{sleep, timeout};

// Trait to unify Pea2Pea and P2P traits.
#[async_trait::async_trait]
trait Connect {
    async fn listening_addr(&self) -> SocketAddr;

    async fn connect(&self, target: SocketAddr) -> Result<(), ConnectError>;
}

// Implement the `Connect` trait for each node type.
macro_rules! impl_connect {
    ($($node_type:ident),*) => {
        $(
            #[async_trait::async_trait]
            impl Connect for $node_type<CurrentNetwork, ConsensusMemory<CurrentNetwork>> {
                async fn listening_addr(&self) -> SocketAddr {
                    self.tcp().listening_addr().expect("node listener should exist")
                }

                async fn connect(&self, target: SocketAddr) -> Result<(), ConnectError>
                where
                    Self: P2P,
                {
                    self.tcp().connect(target).await
                }
            }
        )*
    };
}

impl_connect!(Client, Prover, Validator);

// Implement the `Connect` trait for the test peer.
#[async_trait::async_trait]
impl Connect for TestPeer
where
    Self: Pea2Pea,
{
    async fn listening_addr(&self) -> SocketAddr {
        self.node().listening_addr().await.expect("node listener should exist")
    }

    async fn connect(&self, target: SocketAddr) -> Result<(), ConnectError> {
        self.node().connect(target).await.map_err(|err| err.into())
    }
}

/* Test case */

// Asserts a successful connection was created from initiator to responder.
async fn assert_connect<T, U>(initiator: T, responder: U)
where
    T: Connect,
    U: Connect,
{
    initiator.connect(responder.listening_addr().await).await.unwrap()
}

// Macro to simply construct handshake cases.
// Syntax:
// - (full_node -> test_peer): full node initiates a handshake to the test peer (synthetic node).
// - (full_node <- test_peer): full node receives a handshake initiated by the test peer.
//
// Test naming: full_node::handshake_<initiator or responder>_side::test_peer.
macro_rules! test_handshake {
    ($node_type:ident, $peer_type:ident, $is_initiator:expr, $($attr:meta)?) => {
        #[tokio::test]
        $(#[$attr])?
        async fn $peer_type() {
            // $crate::common::initialise_logger(2);

            // Spin up a full node.
            let node = $crate::$node_type().await;

            // Spin up a test peer (synthetic node).
            let peer = $crate::common::test_peer::TestPeer::$peer_type().await;

            // Sets up the connection direction as described above.
            if $is_initiator {
                $crate::assert_connect(node, peer).await;
            } else {
                $crate::assert_connect(peer, node).await;
            };
        }
    };

    // Initiator side.
    ($($node_type:ident -> $peer_type:ident $(= $attr:meta)?),*) => {
        mod handshake_initiator_side {
            $(
                test_handshake!($node_type, $peer_type, true, $($attr)?);
            )*
        }

    };

    // Responder side.
    ($($node_type:ident <- $peer_type:ident $(= $attr:meta)?),*) => {
        mod handshake_responder_side {
            $(
                test_handshake!($node_type, $peer_type, false, $($attr)?);
            )*
        }

    };
}

mod client {
    // Initiator side (full node connects to synthetic peer).
    test_handshake! {
        client -> client,
        // client -> validator, // router connections to untrusted peers are not allowed
        client -> prover
    }

    // Responder side (synthetic peer connects to full node).
    test_handshake! {
        client <- client,
        // client <- validator, // router connections to untrusted peers are not allowed
        client <- prover
    }
}

mod prover {
    // Initiator side (full node connects to synthetic peer).
    test_handshake! {
        prover -> client,
        // prover -> validator, // router connections to untrusted peers are not allowed
        prover -> prover
    }

    // Responder side (synthetic peer connects to full node).
    test_handshake! {
        prover <- client,
        // prover <- validator, // router connections to untrusted peers are not allowed
        prover <- prover
    }
}

// router connections to untrusted peers are not allowed
// mod validator {
//     // Initiator side (full node connects to synthetic peer).
//     test_handshake! {
//         validator -> client,
//         validator -> validator,
//         validator -> prover
//     }

//     // Responder side (synthetic peer connects to full node).
//     test_handshake! {
//         validator <- client,
//         validator <- validator,
//         validator <- prover
//     }
// }

#[tokio::test(flavor = "multi_thread")]
async fn simultaneous_connection_attempt() {
    // common::initialise_logger(3);

    // Spin up 2 full nodes.
    let node1 = validator().await;
    let addr1 = node1.listening_addr().await;
    let node2 = validator().await;
    let addr2 = node2.listening_addr().await;

    // Prepare connection attempts.
    let node1_clone = node1.clone();
    let conn1 = tokio::spawn(async move {
        if let Ok(conn_task) = node1_clone.router().connect(addr2) { conn_task.await.unwrap().is_ok() } else { false }
    });
    let node2_clone = node2.clone();
    let conn2 = tokio::spawn(async move {
        if let Ok(conn_task) = node2_clone.router().connect(addr1) { conn_task.await.unwrap().is_ok() } else { false }
    });

    // Attempt to connect both nodes to one another at the same time.
    let (result1, result2) = tokio::join!(conn1, conn2);
    // A small anti-flakiness buffer.
    sleep(Duration::from_millis(200)).await;

    // Count connection successes.
    let mut successes = 0;
    if result1.unwrap() {
        successes += 1;
    }
    if result2.unwrap() {
        successes += 1;
    }

    // Record the number of connected peers for both nodes.
    let tcp_connected1 = node1.tcp().num_connected();
    let tcp_connected2 = node2.tcp().num_connected();
    let router_connected1 = node1.router().number_of_connected_peers();
    let router_connected2 = node2.router().number_of_connected_peers();

    // It's possible for both attempts to fail and that's ok; the important
    // thing is that at most a single connection is established in the end.
    assert!(successes <= 1);

    // If both attempts failed, all the counters should be 0; otherwise,
    // all should be 1.
    if successes == 0 {
        assert_eq!(tcp_connected1, 0);
        assert_eq!(tcp_connected2, 0);
        assert_eq!(router_connected1, 0);
        assert_eq!(router_connected2, 0);
    } else {
        assert_eq!(tcp_connected1, 1);
        assert_eq!(tcp_connected2, 1);
        assert_eq!(router_connected1, 1);
        assert_eq!(router_connected2, 1);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_connection_attempts() {
    // common::initialise_logger(3);

    // Spin up 2 full nodes.
    let node1 = client().await;
    let node2 = client().await;
    let addr2 = node2.listening_addr().await;

    // Prepare connection attempts.
    let node1_clone = node1.clone();
    let conn1 = tokio::spawn(async move {
        if let Ok(conn_task) = node1_clone.router().connect(addr2) { conn_task.await.unwrap().is_ok() } else { false }
    });
    let node1_clone = node1.clone();
    let conn2 = tokio::spawn(async move {
        if let Ok(conn_task) = node1_clone.router().connect(addr2) { conn_task.await.unwrap().is_ok() } else { false }
    });
    let node1_clone = node1.clone();
    let conn3 = tokio::spawn(async move {
        if let Ok(conn_task) = node1_clone.router().connect(addr2) { conn_task.await.unwrap().is_ok() } else { false }
    });

    // Attempt to connect the 1st node to the other one several times at once.
    let (result1, result2, result3) = tokio::join!(conn1, conn2, conn3);
    // A small anti-flakiness buffer.
    sleep(Duration::from_millis(200)).await;

    // Count the successes.
    let mut successes = 0;
    if result1.unwrap() {
        successes += 1;
    }
    if result2.unwrap() {
        successes += 1;
    }
    if result3.unwrap() {
        successes += 1;
    }

    // Connection checks.
    assert_eq!(successes, 1);
    assert_eq!(node1.router().number_of_connected_peers(), 1);
    assert_eq!(node2.router().number_of_connected_peers(), 1);
}

/// How long to wait for a stalled handshake to time out.
fn handshake_timeout_with_margin<H: Handshake>(_node: &H) -> Duration {
    Duration::from_millis(H::TIMEOUT_MS + 3_000)
}

/// A peer that accepts the connection and then never answers holds the router's handshake until it
/// times out. Once it has, the peer must be dialable again; a peer stuck as connecting is never
/// dialed again and turns away every inbound handshake.
#[tokio::test(flavor = "multi_thread")]
async fn a_timed_out_handshake_does_not_leave_the_peer_connecting() {
    let node = client().await;

    // Accept connections and keep them open without ever writing to them.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let silent_addr = listener.local_addr().unwrap();
    let _silent = tokio::spawn(async move {
        let mut streams = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            streams.push(stream);
        }
    });

    let dial = node.router().connect(silent_addr).unwrap();
    let router = node.router().clone();
    deadline!(Duration::from_secs(5), move || router.is_connecting(silent_addr));

    let result =
        timeout(handshake_timeout_with_margin(&node), dial).await.expect("the handshake never timed out").unwrap();
    assert!(
        matches!(&result, Err(ConnectError::IoError(e)) if e.kind() == io::ErrorKind::TimedOut),
        "the handshake should have timed out: {result:?}"
    );

    assert!(!node.router().is_connecting(silent_addr), "the timed-out peer is still marked as connecting");
    assert!(!matches!(node.router().connect(silent_addr), Err(ConnectError::AlreadyConnecting { .. })));
}
