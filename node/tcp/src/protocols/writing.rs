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

use std::{any::Any, collections::HashMap, io, net::SocketAddr, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures_util::sink::SinkExt;
#[cfg(feature = "locktick")]
use locktick::parking_lot::RwLock;
#[cfg(not(feature = "locktick"))]
use parking_lot::RwLock;
use tokio::{
    io::AsyncWrite,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::codec::{Encoder, FramedWrite};
use tracing::*;

#[cfg(doc)]
use crate::{Config, Tcp, protocols::Handshake};
use crate::{
    Connection,
    ConnectionSide,
    P2P,
    connections::{DisconnectOrigin, create_connection_span},
    protocols::{DisconnectOnDrop, Protocol, ProtocolHandler, ReturnableConnection},
};

type WritingSenders = Arc<RwLock<HashMap<SocketAddr, mpsc::Sender<WrappedMessage>>>>;

/// Can be used to specify and enable writing, i.e. sending outbound messages. If the [`Handshake`]
/// protocol is enabled too, it goes into force only after the handshake has been concluded.
#[async_trait]
pub trait Writing: P2P
where
    Self: Clone + Send + Sync + 'static,
{
    /// The depth of per-connection queues used to send outbound messages; the greater it is, the more outbound
    /// messages the node can enqueue. Setting it to a large value is not recommended, as doing it might
    /// obscure potential issues with your implementation (like slow serialization) or network.
    ///
    /// The default value is 1024.
    fn message_queue_depth(&self) -> usize {
        1024
    }

    /// The maximum time allowed for a single message write, both encoding it and flushing it to
    /// the underlying stream, before the connection is considered dead.
    ///
    /// This has to leave room for encoding the largest message we can produce, as blocks can
    /// currently be quite large.
    const TIMEOUT: Duration = Duration::from_secs(10);

    /// The type of the outbound messages; unless their serialization is expensive and the message
    /// is broadcasted (in which case it would get serialized multiple times), serialization should
    /// be done in the implementation of [`Self::Codec`].
    type Message: Send;

    /// The user-supplied [`Encoder`] used to write outbound messages to the target stream.
    type Codec: Encoder<Self::Message, Error = io::Error> + Send;

    /// Prepares the node to send messages.
    async fn enable_writing(&self) {
        let (conn_sender, mut conn_receiver) = mpsc::channel(self.tcp().config().max_connections as usize);

        // the conn_senders are used to send messages from the Tcp to individual connections
        let conn_senders: WritingSenders = Default::default();
        // procure a clone to create the WritingHandler with
        let senders = conn_senders.clone();

        // use a channel to know when the writing task is ready
        let (tx_writing, rx_writing) = oneshot::channel();

        // the task spawning tasks sending messages to all the streams
        let self_clone = self.clone();
        let writing_task = tokio::spawn(async move {
            trace!(parent: self_clone.tcp().span(), "spawned the Writing handler task");
            tx_writing.send(()).unwrap(); // safe; the channel was just opened

            // these objects are sent from `Tcp::adapt_stream`
            while let Some(returnable_conn) = conn_receiver.recv().await {
                self_clone.handle_new_connection(returnable_conn, &conn_senders).await;
            }
        });
        let _ = rx_writing.await;
        self.tcp().tasks.lock().push(writing_task);

        // register the WritingHandler with the Tcp
        let hdl = Box::new(WritingHandler { handler: ProtocolHandler(conn_sender), senders });
        assert!(self.tcp().protocols.writing.set(hdl).is_ok(), "the Writing protocol was enabled more than once!");
    }

    /// Creates an [`Encoder`] used to write the outbound messages to the target stream.
    /// The `side` param indicates the connection side **from the node's perspective**.
    fn codec(&self, addr: SocketAddr, side: ConnectionSide) -> Self::Codec;

    /// Sends the provided message to the specified [`SocketAddr`]. Returns as soon as the message is queued to
    /// be sent, without waiting for the actual delivery; instead, the caller is provided with a [`oneshot::Receiver`]
    /// which can be used to determine when and whether the message has been delivered.
    ///
    /// # Errors
    ///
    /// The following errors can be returned:
    /// - [`io::ErrorKind::NotConnected`] if the node is not connected to the provided address
    /// - [`io::ErrorKind::Other`] if the outbound message queue for this address is full
    /// - [`io::ErrorKind::Unsupported`] if [`Writing::enable_writing`] hadn't been called yet
    fn unicast(&self, addr: SocketAddr, message: Self::Message) -> io::Result<oneshot::Receiver<io::Result<()>>> {
        // access the protocol handler
        if let Some(handler) = self.tcp().protocols.writing.get() {
            // find the message sender for the given address
            if let Some(sender) = handler.senders.read().get(&addr).cloned() {
                let (msg, delivery) = WrappedMessage::new(Box::new(message));
                sender
                    .try_send(msg)
                    .map_err(|e| {
                        let conn_span = create_connection_span(addr, self.tcp().span());
                        error!(parent: conn_span, "can't send a message: {e}");
                        io::ErrorKind::Other.into()
                    })
                    .map(|_| delivery)
            } else {
                Err(io::ErrorKind::NotConnected.into())
            }
        } else {
            Err(io::ErrorKind::Unsupported.into())
        }
    }

    /// Broadcasts the provided message to all connected peers. Returns as soon as the message is queued to
    /// be sent to all the peers, without waiting for the actual delivery. This method doesn't provide the
    /// means to check when and if the messages actually get delivered; you can achieve that by calling
    /// [`Writing::unicast`] for each address returned by [`Tcp::connected_addrs`].
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::Unsupported`] if [`Writing::enable_writing`] hadn't been called yet.
    fn broadcast(&self, message: Self::Message) -> io::Result<()>
    where
        Self::Message: Clone,
    {
        // access the protocol handler
        if let Some(handler) = self.tcp().protocols.writing.get() {
            let senders = handler.senders.read().clone();
            for (addr, message_sender) in senders {
                let (msg, _delivery) = WrappedMessage::new(Box::new(message.clone()));
                let _ = message_sender.try_send(msg).map_err(|e| {
                    let conn_span = create_connection_span(addr, self.tcp().span());
                    error!(parent: conn_span, "can't send a message: {e}");
                });
            }

            Ok(())
        } else {
            Err(io::ErrorKind::Unsupported.into())
        }
    }
}

/// This trait is used to restrict access to methods that would otherwise be public in [`Writing`].
#[async_trait]
trait WritingInternal: Writing {
    /// Writes the given message to the network stream and returns the number of written bytes.
    async fn write_to_stream<W: AsyncWrite + Unpin + Send>(
        &self,
        message: Self::Message,
        writer: &mut FramedWrite<W, Self::Codec>,
    ) -> Result<usize, <Self::Codec as Encoder<Self::Message>>::Error>;

    /// Applies the [`Writing`] protocol to a single connection.
    async fn handle_new_connection(&self, (conn, conn_returner): ReturnableConnection, conn_senders: &WritingSenders);
}

#[async_trait]
impl<W: Writing> WritingInternal for W {
    async fn write_to_stream<A: AsyncWrite + Unpin + Send>(
        &self,
        message: Self::Message,
        writer: &mut FramedWrite<A, Self::Codec>,
    ) -> Result<usize, <Self::Codec as Encoder<Self::Message>>::Error> {
        // Guard against write starvation. `feed` is covered as well as `flush`, as `FramedWrite`
        // flushes from within `feed` once its buffer is over the backpressure boundary, so a peer
        // that has stopped reading blocks `feed` too.
        let write = async {
            writer.feed(message).await?;
            let len = writer.write_buffer().len();
            writer.flush().await?;
            Ok(len)
        };
        match timeout(W::TIMEOUT, write).await {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(io::ErrorKind::TimedOut, "write timed out")),
        }
    }

    async fn handle_new_connection(
        &self,
        (mut conn, conn_returner): ReturnableConnection,
        conn_senders: &WritingSenders,
    ) {
        let addr = conn.addr();
        let codec = self.codec(addr, !conn.side());
        let writer = conn.writer.take().expect("missing connection writer!");
        let mut framed = FramedWrite::new(writer, codec);

        let (outbound_message_sender, mut outbound_message_receiver) = mpsc::channel(self.message_queue_depth());

        // register the connection's message sender with the Writing protocol handler
        conn_senders.write().insert(addr, outbound_message_sender);

        // this will automatically drop the sender upon a disconnect
        let sender_cleanup = SenderCleanup { addr, senders: Arc::clone(conn_senders) };

        // use a channel to know when the writer task is ready
        let (tx_writer, rx_writer) = oneshot::channel();

        // the task for writing outbound messages
        let self_clone = self.clone();
        let conn_stats = Arc::clone(conn.stats());
        let conn_span = conn.span().clone();
        let writer_task = tokio::spawn(Box::pin(async move {
            let node = self_clone.tcp();
            trace!(parent: &conn_span, "spawned a task for writing messages");
            tx_writer.send(()).unwrap(); // safe; the channel was just opened

            // move the cleanup into the task that gets aborted on disconnect
            let _sender_cleanup = sender_cleanup;

            // disconnect automatically regardless of how this task concludes
            let _conn_cleanup = DisconnectOnDrop::new(node.clone(), addr, DisconnectOrigin::Writing);

            while let Some(wrapped_msg) = outbound_message_receiver.recv().await {
                let msg = wrapped_msg.msg.downcast().unwrap();

                match self_clone.write_to_stream(*msg, &mut framed).await {
                    Ok(len) => {
                        let _ = wrapped_msg.delivery_notification.send(Ok(()));
                        conn_stats.register_sent_message(len);
                        node.stats().register_sent_message(len);
                        trace!(parent: &conn_span, "sent {len}B");
                    }
                    Err(e) => {
                        error!(parent: &conn_span, "couldn't send a message: {e}");
                        let _ = wrapped_msg.delivery_notification.send(Err(e));
                        break;
                    }
                }
            }
        }));
        let _ = rx_writer.await;
        conn.tasks.push(writer_task);

        // return the Connection to the Tcp, resuming Tcp::adapt_stream
        if conn_returner.send(Ok(conn)).is_err() {
            unreachable!("couldn't return a Connection to the Tcp");
        }
    }
}

/// Used to queue messages for delivery.
struct WrappedMessage {
    msg: Box<dyn Any + Send>,
    delivery_notification: oneshot::Sender<io::Result<()>>,
}

impl WrappedMessage {
    fn new(msg: Box<dyn Any + Send>) -> (Self, oneshot::Receiver<io::Result<()>>) {
        let (tx, rx) = oneshot::channel();
        let wrapped_msg = Self { msg, delivery_notification: tx };

        (wrapped_msg, rx)
    }
}

/// The handler object dedicated to the [`Writing`] protocol.
pub(crate) struct WritingHandler {
    handler: ProtocolHandler<Connection, io::Result<Connection>>,
    senders: WritingSenders,
}

impl Protocol<Connection, io::Result<Connection>> for WritingHandler {
    async fn trigger(&self, item: ReturnableConnection) {
        self.handler.trigger(item).await;
    }
}

struct SenderCleanup {
    addr: SocketAddr,
    senders: WritingSenders,
}

impl Drop for SenderCleanup {
    fn drop(&mut self) {
        self.senders.write().remove(&self.addr);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, ConnectionSide, Tcp, protocols::Reading};
    use bytes::Bytes;
    use std::net::{IpAddr, Ipv4Addr};
    use tokio::time::Instant;
    use tokio_util::codec::BytesCodec;

    fn test_config() -> Config {
        Config { listener_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)), ..Default::default() }
    }

    #[derive(Clone)]
    struct TestNode(Tcp);
    impl P2P for TestNode {
        fn tcp(&self) -> &Tcp {
            &self.0
        }
    }
    #[async_trait]
    impl Writing for TestNode {
        type Codec = BytesCodec;
        type Message = Bytes;

        const TIMEOUT: Duration = Duration::from_millis(200);

        fn codec(&self, _a: SocketAddr, _s: ConnectionSide) -> Self::Codec {
            Default::default()
        }
    }
    #[async_trait]
    impl Reading for TestNode {
        type Codec = BytesCodec;
        type Message = bytes::BytesMut;

        fn codec(&self, _a: SocketAddr, _s: ConnectionSide) -> Self::Codec {
            Default::default()
        }

        async fn process_message(&self, _s: SocketAddr, _m: Self::Message) -> io::Result<()> {
            Ok(())
        }
    }

    fn flood(sender: &TestNode, peer: SocketAddr) {
        let msg = Bytes::from(vec![0u8; 1024 * 1024]);
        for _ in 0..64 {
            if sender.unicast(peer, msg.clone()).is_err() {
                break;
            }
        }
    }
    async fn disconnected(sender: &TestNode, peer: SocketAddr, within: Duration) -> bool {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if !sender.tcp().is_connected(peer) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }

    #[tokio::test]
    async fn stalled_peer_is_disconnected() {
        let sender = TestNode(Tcp::new(test_config()));
        sender.tcp().enable_listener().await.unwrap();
        sender.enable_writing().await;
        let receiver = Tcp::new(test_config());
        let ip = receiver.enable_listener().await.unwrap();
        sender.tcp().connect(ip).await.unwrap();
        let peer = *sender.tcp().connected_addrs().first().unwrap();
        flood(&sender, peer);
        assert!(disconnected(&sender, peer, Duration::from_secs(10)).await, "stalled peer not disconnected");
    }

    #[tokio::test]
    async fn reading_peer_is_kept() {
        let sender = TestNode(Tcp::new(test_config()));
        sender.tcp().enable_listener().await.unwrap();
        sender.enable_writing().await;
        let receiver = TestNode(Tcp::new(test_config()));
        let ip = receiver.tcp().enable_listener().await.unwrap();
        receiver.enable_reading().await;
        sender.tcp().connect(ip).await.unwrap();
        let peer = *sender.tcp().connected_addrs().first().unwrap();
        flood(&sender, peer);
        assert!(!disconnected(&sender, peer, Duration::from_secs(3)).await, "reading peer was disconnected");
    }
}
