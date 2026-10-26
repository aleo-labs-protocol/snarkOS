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

use std::{
    collections::{HashMap, HashSet},
    fmt,
    io,
    net::{IpAddr, SocketAddr},
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::*},
    },
    time::{Duration, Instant},
};

use anyhow::anyhow;
#[cfg(feature = "locktick")]
use locktick::parking_lot::Mutex;
use once_cell::sync::OnceCell;
#[cfg(not(feature = "locktick"))]
use parking_lot::Mutex;
use tokio::{
    io::split,
    net::{TcpListener, TcpSocket, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore, oneshot},
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use tracing::*;

use crate::{
    BannedPeers,
    Config,
    Stats,
    connections::{Connection, ConnectionSide, Connections, DisconnectOrigin, canonical_ip, create_connection_span},
    protocols::{Protocol, Protocols},
};

// A sequential numeric identifier assigned to `Tcp`s that were not provided with a name.
static SEQUENTIAL_NODE_ID: AtomicUsize = AtomicUsize::new(0);

/// The central object responsible for handling connections.
#[derive(Clone)]
pub struct Tcp(Arc<InnerTcp>);

impl Deref for Tcp {
    type Target = Arc<InnerTcp>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A custom application error that can be returned by the `Tcp` stack.
pub trait ApplicationError: Send + Sync + std::fmt::Debug + std::fmt::Display + 'static {}

/// Error types for the `Tcp::connect` function.
#[allow(missing_docs)]
#[derive(thiserror::Error, Debug)]
pub enum ConnectError {
    #[error("already reached the maximum number of {limit} connections")]
    MaximumConnectionsReached { limit: u16 },
    #[error("already reached the maximum number of {limit} connections with IP '{ip}'")]
    MaximumConnectionsPerIpReached { ip: IpAddr, limit: u16 },
    #[error("already connecting to node at {address:?}")]
    AlreadyConnecting { address: SocketAddr },
    #[error("already connected to node at {address:?}")]
    AlreadyConnected { address: SocketAddr },
    #[error("attempt to self-connect (at address {address:?}")]
    SelfConnect { address: SocketAddr },
    #[error("rejected a connection attempt from a banned IP '{ip}'")]
    BannedIp { ip: IpAddr },
    // Socket errors, such as "connection refused".
    #[error(transparent)]
    IoError(std::io::Error),
    // An application-specific reason to reject the connection or abort the handshake.
    // For snarkOS, this is either a `DisconnectReason` or a `PeeringError`, which do not fully implement `std::error::Error`.
    #[error("{0}")]
    ApplicationError(Box<dyn ApplicationError>),
    /// An unexpected error at the application layer and certain deserialization errors.
    /// TODO(kaimast): (some of) these should be treated with higher severity, as they indicate a bug or corrupted state,
    ///                and deserialization errors should not be included in this "other" category.
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl ConnectError {
    /// Pass an application-level error to the `Tcp` stack.
    pub fn application<E: ApplicationError>(err: E) -> Self {
        Self::ApplicationError(Box::new(err))
    }

    /// A generic error that can be returned by the `Tcp` stack.
    pub fn other<E: Into<Box<dyn std::error::Error + Send + Sync>>>(err: E) -> Self {
        Self::Other(err.into())
    }
}

impl From<ConnectError> for std::io::Error {
    fn from(err: ConnectError) -> Self {
        match err {
            ConnectError::IoError(err) => err,
            ConnectError::Other(err) => std::io::Error::other(err),
            err => std::io::Error::other(err.to_string()),
        }
    }
}

impl From<std::io::Error> for ConnectError {
    fn from(err: std::io::Error) -> Self {
        // Other error are usually checks that fail when snarkVM deserializes a message.
        if err.kind() == std::io::ErrorKind::Other {
            // This unwrap should always succeed.
            let inner = err.into_inner().unwrap_or_else(|| anyhow!("Unknown error").into());
            ConnectError::other(inner)
        } else {
            ConnectError::IoError(err)
        }
    }
}

#[doc(hidden)]
pub struct InnerTcp {
    /// The tracing span.
    span: Span,
    /// The node's configuration.
    config: Config,
    /// The node's listening address.
    listening_addr: OnceCell<SocketAddr>,
    /// Contains objects used by the protocols implemented by the node.
    pub(crate) protocols: Protocols,
    /// A set of connections that have not been finalized yet.
    connecting: Mutex<HashSet<SocketAddr>>,
    /// Contains objects related to the node's active connections.
    pub(crate) connections: Connections,
    /// Contains the set of currently banned peers.
    banned_peers: BannedPeers,
    /// Collects statistics related to the node itself.
    stats: Stats,
    /// The node's tasks.
    pub(crate) tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl Tcp {
    /// Creates a new [`Tcp`] using the given [`Config`].
    pub fn new(mut config: Config) -> Self {
        // If there is no pre-configured name, assign a sequential numeric identifier.
        if config.name.is_none() {
            config.name = Some(SEQUENTIAL_NODE_ID.fetch_add(1, Relaxed).to_string());
        }

        // Create a tracing span containing the node's name.
        let span = crate::helpers::create_span(config.name.as_deref().unwrap());

        // A zero here would render the node inoperable, and would do so obscurely (at the first
        // connection) rather than up front.
        assert!(config.max_connections_per_ip != 0, "Config::max_connections_per_ip must not be 0");

        // Initialize the Tcp stack.
        let tcp = Tcp(Arc::new(InnerTcp {
            span,
            config,
            listening_addr: Default::default(),
            protocols: Default::default(),
            connecting: Default::default(),
            connections: Default::default(),
            banned_peers: Default::default(),
            stats: Stats::new(Instant::now()),
            tasks: Default::default(),
        }));

        debug!(parent: tcp.span(), "The node is ready");

        tcp
    }

    /// How long has this node accepting connections?
    pub fn uptime(&self) -> Duration {
        self.stats.created().elapsed()
    }

    /// Returns the name assigned.
    #[inline]
    pub fn name(&self) -> &str {
        // safe; can be set as None in Config, but receives a default value on Tcp creation
        self.config.name.as_deref().unwrap()
    }

    /// Returns a reference to the configuration.
    #[inline]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns the listening address; returns an error if Tcp was not configured
    /// to listen for inbound connections.
    pub fn listening_addr(&self) -> io::Result<SocketAddr> {
        self.listening_addr.get().copied().ok_or_else(|| io::ErrorKind::AddrNotAvailable.into())
    }

    /// Checks whether the provided address is connected.
    pub fn is_connected(&self, addr: SocketAddr) -> bool {
        self.connections.is_connected(addr)
    }

    /// Checks if Tcp is currently setting up a connection with the provided address.
    pub fn is_connecting(&self, addr: SocketAddr) -> bool {
        self.connecting.lock().contains(&addr)
    }

    /// Returns the number of active connections.
    pub fn num_connected(&self) -> usize {
        self.connections.num_connected()
    }

    /// Returns the number of connections that are currently being set up.
    pub fn num_connecting(&self) -> usize {
        self.connecting.lock().len()
    }

    /// Returns a list containing addresses of active connections.
    pub fn connected_addrs(&self) -> Vec<SocketAddr> {
        self.connections.addrs()
    }

    /// Returns a list containing addresses of pending connections.
    pub fn connecting_addrs(&self) -> Vec<SocketAddr> {
        self.connecting.lock().iter().copied().collect()
    }

    /// Returns the statistics of the active connection with the given address, if any.
    pub fn connection_stats(&self, addr: SocketAddr) -> Option<Arc<Stats>> {
        self.connections.stats(addr)
    }

    /// Returns the statistics of every active connection.
    pub fn connection_stats_snapshot(&self) -> HashMap<SocketAddr, Arc<Stats>> {
        self.connections.stats_snapshot()
    }

    /// Returns a reference to the set of currently banned peers.
    #[inline]
    pub fn banned_peers(&self) -> &BannedPeers {
        &self.banned_peers
    }

    /// Returns a reference to the statistics.
    #[inline]
    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// Returns the tracing [`Span`] associated with Tcp.
    #[inline]
    pub fn span(&self) -> &Span {
        &self.span
    }

    /// Gracefully shuts down the stack.
    pub async fn shut_down(&self) {
        debug!(parent: self.span(), "Shutting down the TCP stack");

        // Retrieve all tasks.
        let mut tasks = std::mem::take(&mut *self.tasks.lock()).into_iter();

        // Abort the listening task first.
        if let Some(listening_task) = tasks.next() {
            listening_task.abort(); // abort the listening task first
        }

        // Disconnect from all connected peers.
        let mut disconnect_tasks = JoinSet::new();
        for addr in self.connected_addrs() {
            let node = self.clone();
            disconnect_tasks.spawn(async move {
                node.disconnect_w_origin(addr, DisconnectOrigin::Shutdown).await;
            });
        }
        while disconnect_tasks.join_next().await.is_some() {}

        // Abort all remaining tasks.
        for handle in tasks {
            handle.abort();
        }
    }
}

impl Tcp {
    /// Connects to the provided `SocketAddr`.
    pub async fn connect(&self, addr: SocketAddr) -> Result<(), ConnectError> {
        if let Ok(listening_addr) = self.listening_addr() {
            // TODO(nkls): maybe this first check can be dropped; though it might be best to keep just in case.
            if addr == listening_addr || self.is_self_connect(addr) {
                error!(parent: self.span(), "Attempted to self-connect ({addr})");
                return Err(ConnectError::SelfConnect { address: addr });
            }
        }

        self.can_add_connection(addr)?;

        if self.is_connected(addr) {
            trace!(parent: self.span(), "Already connected to {addr}");
            return Err(ConnectError::AlreadyConnected { address: addr });
        }

        if !self.connecting.lock().insert(addr) {
            debug!(parent: self.span(), "Already connecting to {addr}");
            return Err(ConnectError::AlreadyConnecting { address: addr });
        }

        let timeout_duration = Duration::from_millis(self.config().connection_timeout_ms.into());

        // Bind the tcp socket to the configured listener ip if it's set.
        // Otherwise default to the system's default interface.
        let res = if let Some(listen_ip) = self.config().listener_ip {
            timeout(timeout_duration, self.connect_with_specific_interface(listen_ip, addr)).await
        } else {
            timeout(timeout_duration, TcpStream::connect(addr)).await
        };

        let stream = match res {
            Ok(Ok(stream)) => Ok(stream),
            Ok(err) => {
                self.connecting.lock().remove(&addr);
                err
            }
            Err(err) => {
                self.connecting.lock().remove(&addr);
                error!("connection timeout error: {}", err);
                Err(io::ErrorKind::TimedOut.into())
            }
        }?;

        let ret = self.adapt_stream(stream, addr, ConnectionSide::Initiator).await;

        if let Err(ref e) = ret {
            self.connecting.lock().remove(&addr);
            error!(parent: self.span(), "Unable to initiate a connection with {addr}: {e}");
        }

        ret.map_err(|err| err.into())
    }

    async fn connect_with_specific_interface(&self, listen_ip: IpAddr, addr: SocketAddr) -> io::Result<TcpStream> {
        let sock = if listen_ip.is_ipv4() { TcpSocket::new_v4()? } else { TcpSocket::new_v6()? };
        // Lock the socket to a specific interface.
        sock.bind(SocketAddr::new(listen_ip, 0))?;
        sock.connect(addr).await
    }

    /// Disconnects from the provided `SocketAddr`.
    ///
    /// Returns true if the we were connected to the given address.
    pub async fn disconnect(&self, addr: SocketAddr) -> bool {
        self.disconnect_w_origin(addr, DisconnectOrigin::User).await
    }

    pub(crate) async fn disconnect_w_origin(&self, addr: SocketAddr, origin: DisconnectOrigin) -> bool {
        // claim the disconnect to avoid duplicate executions, or return early if already claimed
        if let Some(conn) = self.connections.0.read().get(&addr) {
            if conn.disconnecting.swap(true, AcqRel) {
                // valid connection, but someone else is already disconnecting it
                return false;
            }
        } else {
            // not connected
            return false;
        };

        if let Some(handler) = self.protocols.disconnect.get() {
            let (sender, receiver) = oneshot::channel();
            handler.trigger(((addr, origin), sender)).await;
            if let Ok((handle, waiter)) = receiver.await {
                // register the associated task with the connection, in case
                // it gets terminated before its completion
                if let Some(conn) = self.connections.0.write().get_mut(&addr) {
                    conn.tasks.push(handle);
                }
                // wait for the OnDisconnect protocol to perform its specified actions
                let _ = waiter.await;
            }
        }

        let conn = self.connections.remove(addr);
        let disconnected = conn.is_some();

        if let Some(conn) = conn {
            debug!(parent: self.span(), "Disconnecting from {addr}");

            // Shut down the associated tasks of the peer.
            drop(conn);

            debug!(parent: self.span(), "Disconnected from {addr}");
        } else {
            warn!(parent: self.span(), "Failed to disconnect, was not connected to {addr}");
        }

        disconnected
    }
}

impl Tcp {
    /// Spawns a task that listens for incoming connections.
    pub async fn enable_listener(&self) -> io::Result<SocketAddr> {
        // Retrieve the listening IP address, which must be set.
        let listener_ip =
            self.config().listener_ip.expect("Tcp::enable_listener was called, but Config::listener_ip is not set");

        // Initialize the TCP listener.
        let listener = self.create_listener(listener_ip).await?;

        // Discover the port, if it was unspecified.
        let port = listener.local_addr()?.port();

        // Set the listening IP address.
        let listening_addr = (listener_ip, port).into();
        self.listening_addr.set(listening_addr).expect("The node's listener was started more than once");

        // Use a channel to know when the listening task is ready.
        let (tx, rx) = oneshot::channel();

        // Cap the number of in-flight inbound connection handlers; the hard
        // connection limits are still enforced inside `can_add_connection`;
        // this bound exists separately to prevent per-SYN task-creation overhead
        // from being unbounded under flood.
        let inbound_permits = Arc::new(Semaphore::new(self.config.max_connections as usize));

        let tcp = self.clone();
        let listening_task = tokio::spawn(async move {
            trace!(parent: tcp.span(), "Spawned the listening task");
            tx.send(()).unwrap(); // safe; the channel was just opened

            loop {
                // Wait for capacity before accepting.
                let permit = match inbound_permits.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => {
                        // semaphore is never closed in practice; bail defensively
                        error!(parent: tcp.span(), "Inbound permit semaphore closed unexpectedly");
                        return;
                    }
                };

                // Await connection requests from peers.
                match listener.accept().await {
                    Ok((stream, addr)) => tcp.handle_connection(stream, addr, permit),
                    Err(e) => {
                        // Free the permit immediately.
                        drop(permit);

                        match e.kind() {
                            // A peer aborted/reset before accept completed; no backoff - the listener is healthy.
                            io::ErrorKind::ConnectionAborted | io::ErrorKind::ConnectionReset => {
                                debug!(parent: tcp.span(), "Transient accept error: {e}");
                            }
                            // Otherwise, assume fd / memory exhaustion (EMFILE, ENFILE, ENOBUFS, ...)
                            // and back off so we don't spin at 100% CPU waiting for a slot to free.
                            _ => {
                                error!(parent: tcp.span(), "Couldn't accept a connection: {e}");
                                tokio::time::sleep(Duration::from_millis(500)).await;
                            }
                        }
                    }
                }
            }
        });
        self.tasks.lock().push(listening_task);
        let _ = rx.await;
        debug!(parent: self.span(), "Listening on {listening_addr}");

        Ok(listening_addr)
    }

    /// Creates an instance of `TcpListener` based on the node's configuration.
    async fn create_listener(&self, listener_ip: IpAddr) -> io::Result<TcpListener> {
        debug!("Creating a TCP listener on {listener_ip}...");
        let listener = if let Some(port) = self.config().desired_listening_port {
            // Construct the desired listening IP address.
            let desired_listening_addr = SocketAddr::new(listener_ip, port);
            // If a desired listening port is set, try to bind to it.
            match TcpListener::bind(desired_listening_addr).await {
                Ok(listener) => listener,
                Err(e) => {
                    if self.config().allow_random_port {
                        warn!(
                            parent: self.span(),
                            "Trying any listening port, as the desired port is unavailable: {e}"
                        );
                        let random_available_addr = SocketAddr::new(listener_ip, 0);
                        TcpListener::bind(random_available_addr).await?
                    } else {
                        error!(parent: self.span(), "The desired listening port is unavailable: {e}");
                        return Err(e);
                    }
                }
            }
        } else if self.config().allow_random_port {
            let random_available_addr = SocketAddr::new(listener_ip, 0);
            TcpListener::bind(random_available_addr).await?
        } else {
            panic!("As 'listener_ip' is set, either 'desired_listening_port' or 'allow_random_port' must be set");
        };

        Ok(listener)
    }

    /// Handles a new inbound connection.
    fn handle_connection(&self, stream: TcpStream, addr: SocketAddr, permit: OwnedSemaphorePermit) {
        debug!(parent: self.span(), "Received a connection from {addr}");

        if self.can_add_connection(addr).is_err() || self.is_self_connect(addr) {
            debug!(parent: self.span(), "Rejecting the connection from {addr}");
            return;
        }

        self.connecting.lock().insert(addr);

        let tcp = self.clone();
        tokio::spawn(async move {
            // The permit is released when the connection is accepted or fails.
            let _permit = permit;

            if let Err(e) = tcp.adapt_stream(stream, addr, ConnectionSide::Responder).await {
                tcp.connecting.lock().remove(&addr);
                error!(parent: tcp.span(), "Failed to connect with {addr}: {e}");
            }
        });
    }

    /// Checks if the given IP address is the same as the listening address of this `Tcp`.
    fn is_self_connect(&self, addr: SocketAddr) -> bool {
        // SAFETY: if we're opening connections, this should never fail.
        let listening_addr = self.listening_addr().unwrap();

        match listening_addr.ip().is_loopback() {
            // If localhost, check the ports, this only works on outbound connections, since we
            // don't know the ephemeral port a peer might be using if they initiate the connection.
            true => listening_addr.port() == addr.port(),
            // If it's not localhost, matching IPs indicate a self-connect in both directions.
            false => listening_addr.ip() == addr.ip(),
        }
    }

    /// Checks whether the `Tcp` can handle an additional connection with the given address.
    ///
    /// Pending connections count towards both the global and the per-IP limit, as they are all
    /// expected to conclude successfully.
    fn can_add_connection(&self, addr: SocketAddr) -> Result<(), ConnectError> {
        // Retrieve the number of connected peers.
        let num_connected = self.num_connected();
        // Retrieve the maximum number of connected peers.
        let limit = self.config.max_connections as usize;

        if num_connected >= limit {
            warn!(parent: self.span(), "Maximum number of active connections ({limit}) reached");
            return Err(ConnectError::MaximumConnectionsReached { limit: self.config.max_connections });
        } else if num_connected + self.num_connecting() >= limit {
            warn!(parent: self.span(), "Maximum number of active & pending connections ({limit}) reached");
            return Err(ConnectError::MaximumConnectionsReached { limit: self.config.max_connections });
        }

        // Retrieve the number of connections already charged to the address' IP.
        let num_with_ip = self.num_connections_with_ip(addr);
        // Retrieve the maximum number of connections permitted per IP.
        let ip_limit = self.config.max_connections_per_ip as usize;

        if num_with_ip >= ip_limit {
            warn!(
                parent: self.span(),
                "Maximum number of connections ({ip_limit}) with IP '{}' reached", addr.ip(),
            );
            return Err(ConnectError::MaximumConnectionsPerIpReached {
                ip: addr.ip(),
                limit: self.config.max_connections_per_ip,
            });
        }

        Ok(())
    }

    /// Returns the number of active and pending connections charged to the given address' IP.
    ///
    /// Both are counted, as a pending connection is expected to conclude successfully. IPs are
    /// compared canonically (see [`canonical_ip`]), so the native and IPv4-mapped spellings of a
    /// host share one allowance.
    fn num_connections_with_ip(&self, addr: SocketAddr) -> usize {
        let ip = canonical_ip(addr);

        // The two collections are counted under separate locks, in the same order as elsewhere, so
        // that no lock is held while the other is acquired.
        let num_connected = self.connections.num_with_ip(addr);
        let num_connecting = self.connecting.lock().iter().filter(|addr| canonical_ip(**addr) == ip).count();

        num_connected.saturating_add(num_connecting)
    }

    /// Prepares the freshly acquired connection to handle the protocols the Tcp implements.
    async fn adapt_stream(&self, stream: TcpStream, peer_addr: SocketAddr, own_side: ConnectionSide) -> io::Result<()> {
        // Register the port seen by the peer.
        if own_side == ConnectionSide::Initiator {
            if let Ok(addr) = stream.local_addr() {
                debug!(
                    parent: self.span(), "establishing connection with {}; the peer is connected on port {}",
                    peer_addr, addr.port()
                );
            } else {
                warn!(parent: self.span(), "couldn't determine the peer's port");
            }
        }

        let conn_span = create_connection_span(peer_addr, self.span());
        let connection = Connection::new(peer_addr, stream, !own_side, conn_span);

        // Enact the enabled protocols.
        let mut connection = self.enable_protocols(connection).await?;

        // if Reading is enabled, we'll notify the related task when the connection is fully ready.
        let conn_ready_tx = connection.readiness_notifier.take();

        self.connections.add(connection);
        self.connecting.lock().remove(&peer_addr);

        // Send the aforementioned notification so that reading from the socket can commence.
        if let Some(tx) = conn_ready_tx {
            let _ = tx.send(());
        }

        // If enabled, enact OnConnect.
        if let Some(handler) = self.protocols.on_connect.get() {
            let (sender, receiver) = oneshot::channel();
            handler.trigger((peer_addr, sender)).await;
            // Receive the handle for the running task.
            if let Ok(handle) = receiver.await {
                // Add the task to the connection so it gets aborted on disconnect.
                if let Some(conn) = self.connections.0.write().get_mut(&peer_addr) {
                    conn.tasks.push(handle);
                } else {
                    // The connection has just been terminated; abort the OnConnect work.
                    handle.abort();
                }
            }
        }

        Ok(())
    }

    /// Enacts the enabled protocols on the provided connection.
    async fn enable_protocols(&self, conn: Connection) -> io::Result<Connection> {
        /// A helper macro to enable a protocol on a connection.
        macro_rules! enable_protocol {
            ($handler_type: ident, $node:expr, $conn: expr) => {
                if let Some(handler) = $node.protocols.$handler_type.get() {
                    let (conn_returner, conn_retriever) = oneshot::channel();

                    handler.trigger(($conn, conn_returner)).await;

                    match conn_retriever.await {
                        Ok(Ok(conn)) => conn,
                        Err(_) => return Err(io::ErrorKind::BrokenPipe.into()),
                        Ok(e) => return e,
                    }
                } else {
                    $conn
                }
            };
        }

        let mut conn = enable_protocol!(handshake, self, conn);

        // Split the stream after the handshake (if not done before).
        if let Some(stream) = conn.stream.take() {
            let (reader, writer) = split(stream);
            conn.reader = Some(Box::new(reader));
            conn.writer = Some(Box::new(writer));
        }

        let conn = enable_protocol!(reading, self, conn);
        let conn = enable_protocol!(writing, self, conn);

        Ok(conn)
    }
}

impl fmt::Debug for Tcp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "The TCP stack config: {:?}", self.config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        net::{IpAddr, Ipv4Addr},
        str::FromStr,
    };

    #[tokio::test]
    async fn test_new() {
        let tcp = Tcp::new(Config {
            listener_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            max_connections: 200,
            ..Default::default()
        });

        assert_eq!(tcp.config.max_connections, 200);
        assert_eq!(tcp.config.listener_ip, Some(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert_eq!(tcp.enable_listener().await.unwrap().ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));

        assert_eq!(tcp.num_connected(), 0);
        assert_eq!(tcp.num_connecting(), 0);
    }

    #[tokio::test]
    async fn test_connect() {
        let tcp = Tcp::new(Config::default());
        let node_ip = tcp.enable_listener().await.unwrap();

        // Ensure self-connecting is not possible.
        let result = tcp.connect(node_ip).await;
        assert!(matches!(result, Err(ConnectError::SelfConnect { .. })));

        assert_eq!(tcp.num_connected(), 0);
        assert_eq!(tcp.num_connecting(), 0);
        assert!(!tcp.is_connected(node_ip));
        assert!(!tcp.is_connecting(node_ip));

        // Initialize the peer.
        let peer = Tcp::new(Config {
            listener_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            desired_listening_port: Some(0),
            max_connections: 1,
            ..Default::default()
        });
        let peer_ip = peer.enable_listener().await.unwrap();

        // Connect to the peer.
        tcp.connect(peer_ip).await.unwrap();
        assert_eq!(tcp.num_connected(), 1);
        assert_eq!(tcp.num_connecting(), 0);
        assert!(tcp.is_connected(peer_ip));
        assert!(!tcp.is_connecting(peer_ip));
    }

    #[tokio::test]
    async fn test_disconnect() {
        let tcp = Tcp::new(Config::default());
        let _node_ip = tcp.enable_listener().await.unwrap();

        // Initialize the peer.
        let peer = Tcp::new(Config {
            listener_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            desired_listening_port: Some(0),
            max_connections: 1,
            ..Default::default()
        });
        let peer_ip = peer.enable_listener().await.unwrap();

        // Connect to the peer.
        tcp.connect(peer_ip).await.unwrap();
        assert_eq!(tcp.num_connected(), 1);
        assert_eq!(tcp.num_connecting(), 0);
        assert!(tcp.is_connected(peer_ip));
        assert!(!tcp.is_connecting(peer_ip));

        // Disconnect from the peer.
        let has_disconnected = tcp.disconnect(peer_ip).await;
        assert!(has_disconnected);
        assert_eq!(tcp.num_connected(), 0);
        assert_eq!(tcp.num_connecting(), 0);
        assert!(!tcp.is_connected(peer_ip));
        assert!(!tcp.is_connecting(peer_ip));

        // Ensure disconnecting from the peer a second time is okay.
        let has_disconnected = tcp.disconnect(peer_ip).await;
        assert!(!has_disconnected);
        assert_eq!(tcp.num_connected(), 0);
        assert_eq!(tcp.num_connecting(), 0);
        assert!(!tcp.is_connected(peer_ip));
        assert!(!tcp.is_connecting(peer_ip));
    }

    #[tokio::test]
    async fn test_can_add_connection() {
        let tcp = Tcp::new(Config { max_connections: 1, ..Default::default() });

        // Initialize the peer.
        let peer = Tcp::new(Config {
            listener_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            desired_listening_port: Some(0),
            max_connections: 1,
            ..Default::default()
        });
        let peer_ip = peer.enable_listener().await.unwrap();

        assert!(tcp.can_add_connection(peer_ip).is_ok());

        // Simulate an active connection.
        let stream = TcpStream::connect(peer_ip).await.unwrap();
        tcp.connections.add(Connection::new(peer_ip, stream, ConnectionSide::Initiator, Span::none()));
        assert!(tcp.can_add_connection(peer_ip).is_err());

        // Ensure that we cannot invoke connect() successfully in this case.
        // Use a non-local IP, to ensure it is never qual to peer IP.
        let another_ip = SocketAddr::from_str("1.2.3.4:4242").unwrap();
        let result = tcp.connect(another_ip).await;
        assert!(matches!(result, Err(ConnectError::MaximumConnectionsReached { .. })));

        // Remove the active connection.
        tcp.connections.remove(peer_ip);
        assert!(tcp.can_add_connection(peer_ip).is_ok());

        // Simulate a pending connection.
        tcp.connecting.lock().insert(peer_ip);
        assert!(tcp.can_add_connection(peer_ip).is_err());

        // Ensure that we cannot invoke connect() successfully in this case either.
        let another_ip = SocketAddr::from_str("1.2.3.4:4242").unwrap();
        let result = tcp.connect(another_ip).await;
        assert!(matches!(result, Err(ConnectError::MaximumConnectionsReached { .. })));

        // Remove the pending connection.
        tcp.connecting.lock().remove(&peer_ip);
        assert!(tcp.can_add_connection(peer_ip).is_ok());

        // Simulate an active and a pending connection (this case should never occur).
        let stream = TcpStream::connect(peer_ip).await.unwrap();
        tcp.connections.add(Connection::new(peer_ip, stream, ConnectionSide::Responder, Span::none()));
        tcp.connecting.lock().insert(peer_ip);
        assert!(tcp.can_add_connection(peer_ip).is_err());

        // Remove the active and pending connection.
        tcp.connections.remove(peer_ip);
        tcp.connecting.lock().remove(&peer_ip);
        assert!(tcp.can_add_connection(peer_ip).is_ok());
    }

    #[tokio::test]
    async fn test_max_connections_per_ip() {
        let tcp = Tcp::new(Config { max_connections: 10, max_connections_per_ip: 2, ..Default::default() });

        // Initialize a listener to source real streams from; the addresses the connections are
        // registered under are independent of it.
        let peer = Tcp::new(Config {
            listener_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            desired_listening_port: Some(0),
            ..Default::default()
        });
        let peer_ip = peer.enable_listener().await.unwrap();

        // Two addresses sharing an IP, and a third on a different IP.
        let first = SocketAddr::from_str("1.2.3.4:1111").unwrap();
        let second = SocketAddr::from_str("1.2.3.4:2222").unwrap();
        let third = SocketAddr::from_str("1.2.3.4:3333").unwrap();
        let other_ip = SocketAddr::from_str("5.6.7.8:1111").unwrap();

        // The per-IP limit counts active connections...
        for addr in [first, second] {
            assert!(tcp.can_add_connection(addr).is_ok());
            let stream = TcpStream::connect(peer_ip).await.unwrap();
            tcp.connections.add(Connection::new(addr, stream, ConnectionSide::Initiator, Span::none()));
        }
        assert_eq!(tcp.num_connections_with_ip(first), 2);
        assert!(matches!(
            tcp.can_add_connection(third),
            Err(ConnectError::MaximumConnectionsPerIpReached { limit: 2, .. })
        ));

        // ...while leaving the global limit, and therefore other IPs, unaffected.
        assert!(tcp.can_add_connection(other_ip).is_ok());

        // Pending connections count towards the limit too.
        tcp.connections.remove(second);
        assert!(tcp.can_add_connection(third).is_ok());
        tcp.connecting.lock().insert(second);
        assert_eq!(tcp.num_connections_with_ip(first), 2);
        assert!(matches!(
            tcp.can_add_connection(third),
            Err(ConnectError::MaximumConnectionsPerIpReached { limit: 2, .. })
        ));
    }

    #[tokio::test]
    async fn test_max_connections_per_ip_canonicalizes_mapped_addresses() {
        let tcp = Tcp::new(Config { max_connections: 10, max_connections_per_ip: 2, ..Default::default() });

        let peer = Tcp::new(Config {
            listener_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            desired_listening_port: Some(0),
            ..Default::default()
        });
        let peer_ip = peer.enable_listener().await.unwrap();

        // The same host in its native form and as a dual-stack listener reports it.
        let native = SocketAddr::from_str("1.2.3.4:1111").unwrap();
        let mapped = SocketAddr::from_str("[::ffff:1.2.3.4]:2222").unwrap();
        let also_mapped = SocketAddr::from_str("[::ffff:1.2.3.4]:3333").unwrap();

        // Sanity: as raw IP addresses these are two distinct keys, which is the bypass being closed.
        assert_ne!(native.ip(), mapped.ip());

        for addr in [native, mapped] {
            assert!(tcp.can_add_connection(addr).is_ok());
            let stream = TcpStream::connect(peer_ip).await.unwrap();
            tcp.connections.add(Connection::new(addr, stream, ConnectionSide::Initiator, Span::none()));
        }

        // Both spellings are charged to one bucket, so the allowance cannot be claimed twice.
        assert_eq!(tcp.num_connections_with_ip(native), 2);
        assert_eq!(tcp.num_connections_with_ip(mapped), 2);
        for addr in [native, also_mapped] {
            assert!(matches!(
                tcp.can_add_connection(addr),
                Err(ConnectError::MaximumConnectionsPerIpReached { limit: 2, .. })
            ));
        }
    }

    #[tokio::test]
    async fn test_handle_connection() {
        let tcp = Tcp::new(Config {
            listener_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            max_connections: 1,
            ..Default::default()
        });

        // Initialize peer 1.
        let peer1 = Tcp::new(Config {
            listener_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            desired_listening_port: Some(0),
            max_connections: 1,
            ..Default::default()
        });
        let peer1_ip = peer1.enable_listener().await.unwrap();

        // Simulate an active connection.
        let stream = TcpStream::connect(peer1_ip).await.unwrap();
        tcp.connections.add(Connection::new(peer1_ip, stream, ConnectionSide::Responder, Span::none()));
        assert!(tcp.can_add_connection(peer1_ip).is_err());
        assert_eq!(tcp.num_connected(), 1);
        assert_eq!(tcp.num_connecting(), 0);
        assert!(tcp.is_connected(peer1_ip));
        assert!(!tcp.is_connecting(peer1_ip));

        // Initialize peer 2.
        let peer2 = Tcp::new(Config {
            listener_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            desired_listening_port: Some(0),
            max_connections: 1,
            ..Default::default()
        });
        let peer2_ip = peer2.enable_listener().await.unwrap();

        // Handle the connection.
        let stream = TcpStream::connect(peer2_ip).await.unwrap();
        let inbound_permits = Arc::new(Semaphore::new(1));
        let permit = inbound_permits.clone().acquire_owned().await.unwrap();
        tcp.handle_connection(stream, peer2_ip, permit);
        assert!(tcp.can_add_connection(peer1_ip).is_err());
        assert_eq!(tcp.num_connected(), 1);
        assert_eq!(tcp.num_connecting(), 0);
        assert!(tcp.is_connected(peer1_ip));
        assert!(!tcp.is_connected(peer2_ip));
        assert!(!tcp.is_connecting(peer1_ip));
        assert!(!tcp.is_connecting(peer2_ip));
    }

    #[tokio::test]
    async fn test_adapt_stream() {
        let tcp = Tcp::new(Config { max_connections: 1, ..Default::default() });

        // Initialize the peer.
        let peer = Tcp::new(Config {
            listener_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            desired_listening_port: Some(0),
            max_connections: 1,
            ..Default::default()
        });
        let peer_ip = peer.enable_listener().await.unwrap();

        // Simulate a pending connection.
        tcp.connecting.lock().insert(peer_ip);
        assert_eq!(tcp.num_connected(), 0);
        assert_eq!(tcp.num_connecting(), 1);
        assert!(!tcp.is_connected(peer_ip));
        assert!(tcp.is_connecting(peer_ip));

        // Simulate a new connection.
        let stream = TcpStream::connect(peer_ip).await.unwrap();
        tcp.adapt_stream(stream, peer_ip, ConnectionSide::Responder).await.unwrap();
        assert_eq!(tcp.num_connected(), 1);
        assert_eq!(tcp.num_connecting(), 0);
        assert!(tcp.is_connected(peer_ip));
        assert!(!tcp.is_connecting(peer_ip));
    }
}
