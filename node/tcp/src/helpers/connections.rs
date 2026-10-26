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

//! Objects associated with connection handling.

use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    ops::Not,
    sync::{Arc, atomic::AtomicBool},
    time::Instant,
};

#[cfg(feature = "locktick")]
use locktick::parking_lot::RwLock;
#[cfg(not(feature = "locktick"))]
use parking_lot::RwLock;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    sync::oneshot,
    task::JoinHandle,
};
use tracing::*;

use crate::Stats;
#[cfg(doc)]
use crate::{
    Tcp,
    protocols::{Disconnect, Handshake, OnConnect, Reading, Writing},
};

/// A map of all currently connected addresses to their associated connection.
#[derive(Default)]
pub(crate) struct Connections(pub(crate) RwLock<HashMap<SocketAddr, Connection>>);

impl Connections {
    /// Adds the given connection to the list of active connections.
    pub(crate) fn add(&self, conn: Connection) {
        self.0.write().insert(conn.addr, conn);
    }

    /// Returns `true` if the given address is connected.
    pub(crate) fn is_connected(&self, addr: SocketAddr) -> bool {
        self.0.read().contains_key(&addr)
    }

    /// Removes the connection associated with the given address.
    pub(crate) fn remove(&self, addr: SocketAddr) -> Option<Connection> {
        self.0.write().remove(&addr)
    }

    /// Returns the number of connected addresses.
    pub(crate) fn num_connected(&self) -> usize {
        self.0.read().len()
    }

    /// Returns the list of connected addresses.
    pub(crate) fn addrs(&self) -> Vec<SocketAddr> {
        self.0.read().keys().copied().collect()
    }

    /// Returns the stats of the connection with the given address, if it is still active.
    pub(crate) fn stats(&self, addr: SocketAddr) -> Option<Arc<Stats>> {
        self.0.read().get(&addr).map(|conn| Arc::clone(conn.stats()))
    }

    /// Returns the stats of every active connection.
    pub(crate) fn stats_snapshot(&self) -> HashMap<SocketAddr, Arc<Stats>> {
        self.0.read().iter().map(|(addr, conn)| (*addr, Arc::clone(conn.stats()))).collect()
    }

    /// Returns the number of active connections charged to the given address' IP.
    ///
    /// note: This is a scan rather than a lookup, as connections are keyed by their full address;
    /// it is only performed once per connection setup, and is bounded by `Config::max_connections`.
    pub(crate) fn num_with_ip(&self, addr: SocketAddr) -> usize {
        let ip = canonical_ip(addr);
        self.0.read().keys().filter(|addr| canonical_ip(**addr) == ip).count()
    }
}

/// The IP address a connection is charged to, for the purposes of per-IP limits.
///
/// Canonicalizing is what keeps the two spellings of an IPv4 host - the native form and the
/// IPv4-mapped IPv6 form a dual-stack listener reports - in a single bucket. Without it, a peer
/// reaching a node bound to `::` could claim the per-IP allowance twice over by alternating
/// between them. Deriving the key here, rather than accepting one from the caller, is what
/// guarantees every comparison uses the same bucket.
pub(crate) const fn canonical_ip(addr: SocketAddr) -> IpAddr {
    addr.ip().to_canonical()
}

/// A helper trait to facilitate trait-objectification of connection readers.
pub(crate) trait AR: AsyncRead + Unpin + Send + Sync {}
impl<T: AsyncRead + Unpin + Send + Sync> AR for T {}

/// A helper trait to facilitate trait-objectification of connection writers.
pub(crate) trait AW: AsyncWrite + Unpin + Send + Sync {}
impl<T: AsyncWrite + Unpin + Send + Sync> AW for T {}

/// Created for each active connection; used by the protocols to obtain a handle for
/// reading and writing, and keeps track of tasks that have been spawned for the connection.
pub struct Connection {
    /// The address of the connection.
    addr: SocketAddr,
    /// The connection's side in relation to Tcp.
    side: ConnectionSide,
    /// Statistics covering this connection alone, for as long as it is active.
    stats: Arc<Stats>,
    /// Available and used only in the [`Handshake`] protocol.
    pub(crate) stream: Option<TcpStream>,
    /// Available and used only in the [`Reading`] protocol.
    pub(crate) reader: Option<Box<dyn AR>>,
    /// Available and used only in the [`Writing`] protocol.
    pub(crate) writer: Option<Box<dyn AW>>,
    /// Used to notify the [`Reading`] protocol that the connection is fully ready.
    pub(crate) readiness_notifier: Option<oneshot::Sender<()>>,
    /// Prevents the OnDisconnect hook from being triggered multiple times.
    pub(crate) disconnecting: AtomicBool,
    /// Handles to tasks spawned for the connection.
    pub(crate) tasks: Vec<JoinHandle<()>>,
    /// The tracing span.
    pub(crate) span: Span,
}

impl Connection {
    /// Creates a [`Connection`] with placeholders for protocol-related objects.
    pub(crate) fn new(addr: SocketAddr, stream: TcpStream, side: ConnectionSide, span: Span) -> Self {
        Self {
            addr,
            stream: Some(stream),
            reader: None,
            writer: None,
            readiness_notifier: None,
            disconnecting: Default::default(),
            side,
            stats: Arc::new(Stats::new(Instant::now())),
            tasks: Default::default(),
            span,
        }
    }

    /// Returns the address associated with the connection.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Returns the statistics of this connection.
    #[inline]
    pub fn stats(&self) -> &Arc<Stats> {
        &self.stats
    }

    /// Returns `ConnectionSide::Initiator` if the associated peer initiated the connection
    /// and `ConnectionSide::Responder` if the connection request was initiated by Tcp.
    pub fn side(&self) -> ConnectionSide {
        self.side
    }

    /// Returns the tracing [`Span`] associated with the connection.
    #[inline]
    pub const fn span(&self) -> &Span {
        &self.span
    }
}

/// Indicates who was the initiator and who was the responder when the connection was established.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionSide {
    /// The side that initiated the connection.
    Initiator,
    /// The side that accepted the connection.
    Responder,
}

impl Not for ConnectionSide {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Self::Initiator => Self::Responder,
            Self::Responder => Self::Initiator,
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        for task in self.tasks.iter().rev() {
            task.abort();
        }
    }
}

pub(crate) fn create_connection_span(addr: SocketAddr, parent: &Span) -> Span {
    macro_rules! try_span {
        ($lvl:expr) => {
            let s = span!(parent: parent, $lvl, "conn", addr = %addr);
            if !s.is_disabled() {
                return s;
            }
        };
    }
    try_span!(Level::TRACE);
    try_span!(Level::DEBUG);
    try_span!(Level::INFO);
    try_span!(Level::WARN);
    error_span!(parent: parent, "conn", addr = %addr)
}

/// Describes what triggered a disconnect, as delivered to [`Disconnect::handle_disconnect`].
///
/// note: Handshake failures do not appear here. A failed handshake prevents the connection
/// from ever being registered, so there is no connection to disconnect.
///
/// note: When several events would race to trigger a disconnect on the same connection,
/// only the first to claim it is delivered to [`Disconnect::handle_disconnect`]; subsequent
/// claims are silently dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DisconnectOrigin {
    /// The [`OnConnect`] task terminated abnormally before defusing its connection cleanup.
    /// In practice this almost always means the user's [`OnConnect::on_connect`] implementation
    /// panicked, and the disconnect is a side effect of that panic unwinding past the cleanup
    /// guard.
    OnConnectAbort,
    /// The reader task for this connection terminated. Typical causes are the peer closing
    /// its end of the socket, a decode error from the user-supplied [`Reading::Codec`], or
    /// no message arriving within [`Reading::IDLE_TIMEOUT_MS`]. Often (but not always)
    /// indicates a peer-side issue.
    Reading,
    /// The disconnect was initiated by [`Tcp::shut_down`], which tears down every active
    /// connection as part of stopping the node. Unlike [`DisconnectOrigin::User`], this
    /// signals that the entire node is going away - reconnection is not meaningful.
    Shutdown,
    /// The disconnect was explicitly requested via [`Tcp::disconnect`]. This is the only
    /// origin produced directly by user code; the others all reflect events the library
    /// detected internally.
    User,
    /// The writer task for this connection terminated. Typical causes are a [`Writing::TIMEOUT_MS`]
    /// timeout while flushing, an underlying socket write error, or the message channel being
    /// closed. Often correlates with the peer disappearing, but can also reflect local-side
    /// pipeline problems (slow consumer, broken pipe).
    Writing,
}
