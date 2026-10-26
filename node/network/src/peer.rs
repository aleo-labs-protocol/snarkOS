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

use crate::NodeType;
use snarkvm::prelude::{Address, FromBytes, Network, ToBytes, error};
use tracing::*;

use std::{fmt, io, net::SocketAddr, time::Instant};

/// A peer of any connection status.
#[derive(Clone, Debug)]
pub enum Peer<N: Network> {
    /// A candidate peer that's currently not connected to.
    Candidate(CandidatePeer<N>),
    /// A peer that's currently being connected to (the handshake is in progress).
    Connecting(ConnectingPeer),
    /// A fully connected (post-handshake) peer.
    Connected(ConnectedPeer<N>),
}

/// A connecting peer.
#[derive(Clone, Debug)]
pub struct ConnectingPeer {
    /// The listening address of a connecting peer.
    pub listener_addr: SocketAddr,
    /// Indicates whether the peer is considered trusted.
    pub trusted: bool,
    /// The last time we attempted to connect to the peer.
    pub last_connection_attempt: Option<Instant>,
    /// The total number of connection attempts, since the peer was last connected.
    pub total_connection_attempts: u32,
}

/// A candidate peer.
#[derive(Clone, Debug)]
pub struct CandidatePeer<N: Network> {
    /// The listening address of a candidate peer.
    pub listener_addr: SocketAddr,
    /// Indicates whether the peer is considered trusted.
    pub trusted: bool,
    /// The latest block height known to be associated with the peer.
    pub last_height_seen: Option<u32>,
    /// The last time we attempted to connect to the peer.
    /// `None` if there was no attempt to connect since the peer was last connected, or no attempt at all.
    pub last_connection_attempt: Option<Instant>,
    /// The total number of connection attempts, since the peer was last connected.
    pub total_connection_attempts: u32,
    /// The last known Aleo address of this peer, carried over from a prior connection.
    /// Used to detect when a validator reconnects from a different IP address.
    pub last_known_aleo_addr: Option<Address<N>>,
}

/// A fully connected peer.
#[derive(Clone, Debug)]
pub struct ConnectedPeer<N: Network> {
    /// The listener address of the peer.
    pub listener_addr: SocketAddr,
    /// The connected address of the peer.
    pub connected_addr: SocketAddr,
    /// Indicates whether this is a Router or a Gateway connection for the peer.
    pub connection_mode: ConnectionMode,
    /// Indicates whether the peer is considered trusted.
    pub trusted: bool,
    /// The Aleo address of the peer.
    pub aleo_addr: Address<N>,
    /// The node type of the peer.
    pub node_type: NodeType,
    /// The message version of the peer.
    pub version: u32,
    /// The snarkOS commit hash of the peer.
    pub snarkos_sha: Option<[u8; 40]>,
    /// The latest block height known to be associated with the peer.
    pub last_height_seen: Option<u32>,
    /// The timestamp of the first message received from the peer.
    pub first_seen: Instant,
    /// The timestamp of the last message received from this peer.
    pub last_seen: Instant,
}

/// Indicates whether a peer is connected via the Gateway or the Router.
///
/// The mode is serialized as the first thing a Noise handshake's first message carries, because a
/// node accepting more than one subprotocol on a single listener - the bootstrap clients take both
/// the gateway's and the router's connections - has to know which payload it is looking at before it
/// can parse it. The marker preceding the pattern cannot say, being shared, and neither can the node
/// type, since a validator connects in both modes at once.
///
/// It is unauthenticated there, but does not need to be trusted: each subprotocol signs under its
/// own binding domain, so a peer that claims one mode and then speaks another fails signature
/// verification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ConnectionMode {
    Gateway = 0,
    Router = 1,
}

impl ToBytes for ConnectionMode {
    fn write_le<W: io::Write>(&self, writer: W) -> io::Result<()> {
        (*self as u8).write_le(writer)
    }
}

impl FromBytes for ConnectionMode {
    fn read_le<R: io::Read>(reader: R) -> io::Result<Self> {
        match u8::read_le(reader)? {
            0 => Ok(Self::Gateway),
            1 => Ok(Self::Router),
            x => Err(error(format!("Invalid connection mode: expected 0..=1, got {x}."))),
        }
    }
}

impl fmt::Display for ConnectionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectionMode::Gateway => write!(f, "Gateway"),
            ConnectionMode::Router => write!(f, "Router"),
        }
    }
}

impl<N: Network> Peer<N> {
    /// Create a candidate peer.
    pub fn new_candidate(listener_addr: SocketAddr, trusted: bool) -> Self {
        Self::Candidate(CandidatePeer {
            listener_addr,
            trusted,
            last_height_seen: None,
            last_connection_attempt: None,
            total_connection_attempts: 0,
            last_known_aleo_addr: None,
        })
    }

    /// Create a connecting peer, with no prior connection attempts on record.
    pub const fn new_connecting(listener_addr: SocketAddr, trusted: bool) -> Self {
        Self::Connecting(ConnectingPeer {
            listener_addr,
            trusted,
            last_connection_attempt: None,
            total_connection_attempts: 0,
        })
    }

    /// Promote a candidate peer to connecting status, preserving its connection-attempt metadata.
    pub fn promote_to_connecting(&mut self) {
        let listener_addr = self.listener_addr();
        let trusted = self.is_trusted();

        let (last_connection_attempt, total_connection_attempts) = match self {
            Self::Candidate(peer) => (peer.last_connection_attempt, peer.total_connection_attempts),
            Self::Connecting(_) | Self::Connected(_) => {
                warn!(
                    "Peer '{listener_addr}' is being promoted to Connecting, but is {}",
                    if self.is_connected() { "already Connected" } else { "already Connecting" }
                );
                (None, 0)
            }
        };

        *self = Self::Connecting(ConnectingPeer {
            listener_addr,
            trusted,
            last_connection_attempt,
            total_connection_attempts,
        });
    }

    /// Promote a connecting peer to a fully connected one.
    #[allow(clippy::too_many_arguments)]
    pub fn upgrade_to_connected(
        &mut self,
        connected_addr: SocketAddr,
        listener_port: u16,
        aleo_address: Address<N>,
        node_type: NodeType,
        node_version: u32,
        snarkos_sha: Option<[u8; 40]>,
        connection_mode: ConnectionMode,
    ) {
        let timestamp = Instant::now();
        let listener_addr = SocketAddr::from((connected_addr.ip(), listener_port));

        // Logic check: this can only happen during the handshake. This isn't a fatal
        // error, but should not be triggered.
        if !matches!(self, Self::Connecting(_)) {
            warn!(
                "Peer '{listener_addr}' is being upgraded to Connected, but isn't Connecting \
                - it is {}",
                if self.is_connected() { "already Connected" } else { "only a Candidate" }
            );
        }

        *self = Self::Connected(ConnectedPeer {
            listener_addr,
            connected_addr,
            connection_mode,
            aleo_addr: aleo_address,
            node_type,
            trusted: self.is_trusted(),
            version: node_version,
            snarkos_sha,
            last_height_seen: None,
            first_seen: timestamp,
            last_seen: timestamp,
        });
    }

    /// Demote a peer to candidate status, marking it as disconnected.
    pub fn downgrade_to_candidate(&mut self, listener_addr: SocketAddr) {
        let last_known_aleo_addr = match self {
            Self::Connected(p) => Some(p.aleo_addr),
            _ => None,
        };

        // Preserve the connection attempt info if the handshake didn't succeed.
        let (last_connection_attempt, total_connection_attempts) = match self {
            Self::Candidate(p) => (p.last_connection_attempt, p.total_connection_attempts),
            Self::Connecting(p) => (p.last_connection_attempt, p.total_connection_attempts),
            Self::Connected(_) => (None, 0),
        };

        *self = Self::Candidate(CandidatePeer {
            listener_addr,
            trusted: self.is_trusted(),
            last_height_seen: self.last_height_seen(),
            last_connection_attempt,
            total_connection_attempts,
            last_known_aleo_addr,
        });
    }

    /// Returns the type of the node (only applicable to connected peers).
    pub fn node_type(&self) -> Option<NodeType> {
        match self {
            Self::Candidate(_) => None,
            Self::Connecting(_) => None,
            Self::Connected(peer) => Some(peer.node_type),
        }
    }

    /// The listener (public) address of this peer.
    pub fn listener_addr(&self) -> SocketAddr {
        match self {
            Self::Candidate(p) => p.listener_addr,
            Self::Connecting(p) => p.listener_addr,
            Self::Connected(p) => p.listener_addr,
        }
    }

    /// The listener (public) address of this peer.
    pub fn last_height_seen(&self) -> Option<u32> {
        match self {
            Self::Candidate(_) => None,
            Self::Connecting(_) => None,
            Self::Connected(peer) => peer.last_height_seen,
        }
    }

    /// The number of connection attempts made since this peer was last connected.
    pub fn failed_connection_attempts(&self) -> u32 {
        match self {
            Self::Candidate(peer) => peer.total_connection_attempts,
            Self::Connecting(peer) => peer.total_connection_attempts,
            Self::Connected(_) => 0,
        }
    }

    /// Returns `true` if the peer is not connected or connecting.
    pub fn is_candidate(&self) -> bool {
        matches!(self, Peer::Candidate(_))
    }

    /// Returns `true` if the peer is currently undergoing the network handshake.
    pub fn is_connecting(&self) -> bool {
        matches!(self, Peer::Connecting(_))
    }

    /// Returns `true` if the peer has concluded the network handshake.
    pub fn is_connected(&self) -> bool {
        matches!(self, Peer::Connected(_))
    }

    /// Returns `true` if the peer is considered trusted.
    pub fn is_trusted(&self) -> bool {
        match self {
            Self::Candidate(peer) => peer.trusted,
            Self::Connecting(peer) => peer.trusted,
            Self::Connected(peer) => peer.trusted,
        }
    }

    /// Updates the peer's `last_seen` timestamp.
    pub fn update_last_seen(&mut self) {
        if let Self::Connected(ConnectedPeer { last_seen, .. }) = self {
            *last_seen = Instant::now();
        }
    }

    /// Returns a reference to the underlying `ConnectedPeer` if it is connedcted,
    /// otherwise `None`.
    pub fn as_connected(&self) -> Option<&ConnectedPeer<N>> {
        match self {
            Self::Connected(peer) => Some(peer),
            _ => None,
        }
    }
}

impl<N: Network> ConnectedPeer<N> {
    /// Returns `true` if this peer is validator.
    pub fn is_validator(&self) -> bool {
        self.node_type == NodeType::Validator
    }
}
