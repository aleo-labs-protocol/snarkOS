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

#[cfg(doc)]
use crate::{Config, protocols::Handshake};
use crate::{
    Connection,
    ConnectionSide,
    P2P,
    Stats,
    Tcp,
    connections::DisconnectOrigin,
    protocols::{DisconnectOnDrop, ProtocolHandler, ReturnableConnection},
};

use async_trait::async_trait;
use bytes::BytesMut;
use futures_util::StreamExt;
use std::{
    io,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::AsyncRead,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::codec::{Decoder, FramedRead};
use tracing::*;

/// Can be used to specify and enable reading, i.e. receiving inbound messages. If the [`Handshake`]
/// protocol is enabled too, it goes into force only after the handshake has been concluded.
///
/// Each inbound message is isolated by the user-supplied [`Reading::Codec`], creating a [`Reading::Message`],
/// which is immediately queued (with a [`Reading::MESSAGE_QUEUE_DEPTH`] limit) to be processed by
/// [`Reading::process_message`]. Errors result in an immediate disconnect (in order to e.g. avoid
/// accidentally reading "borked" messages).
#[async_trait]
pub trait Reading: P2P
where
    Self: Clone + Send + Sync + 'static,
{
    /// The depth of per-connection queues used to process inbound messages; the greater it is, the more inbound
    /// messages the node can enqueue, but setting it to a large value can make the node more susceptible to DoS
    /// attacks.
    ///
    /// The default value is 1024.
    fn message_queue_depth(&self) -> usize {
        1024
    }

    /// The initial size of a per-connection buffer for reading inbound messages. Can be set to the maximum expected size
    /// of the inbound message in order to only allocate it once.
    ///
    /// The default value is 1024KiB.
    const INITIAL_BUFFER_SIZE: usize = 1024 * 1024;

    /// The maximum time the node will wait for a new message before considering the connection dead.
    const IDLE_TIMEOUT: Duration = Duration::from_secs(150);

    /// The final (deserialized) type of inbound messages.
    type Message: Send;

    /// The user-supplied [`Decoder`] used to interpret inbound messages.
    type Codec: Decoder<Item = Self::Message, Error = io::Error> + Send;

    /// Prepares the node to receive messages.
    async fn enable_reading(&self) {
        let (conn_sender, mut conn_receiver) = mpsc::channel(self.tcp().config().max_connections as usize);

        // use a channel to know when the reading task is ready
        let (tx_reading, rx_reading) = oneshot::channel();

        // the main task spawning per-connection tasks reading messages from their streams
        let self_clone = self.clone();
        let reading_task = tokio::spawn(async move {
            trace!(parent: self_clone.tcp().span(), "spawned the Reading handler task");
            tx_reading.send(()).unwrap(); // safe; the channel was just opened

            // these objects are sent from `Tcp::adapt_stream`
            while let Some(returnable_conn) = conn_receiver.recv().await {
                self_clone.handle_new_connection(returnable_conn).await;
            }
        });
        let _ = rx_reading.await;
        self.tcp().tasks.lock().push(reading_task);

        // register the Reading handler with the Tcp
        let hdl = Box::new(ProtocolHandler(conn_sender));
        assert!(self.tcp().protocols.reading.set(hdl).is_ok(), "the Reading protocol was enabled more than once!");
    }

    /// Creates a [`Decoder`] used to interpret messages from the network.
    /// The `side` param indicates the connection side **from the node's perspective**.
    fn codec(&self, addr: SocketAddr, side: ConnectionSide) -> Self::Codec;

    /// Processes an inbound message. Can be used to update state, send replies etc.
    async fn process_message(&self, source: SocketAddr, message: Self::Message) -> io::Result<()>;
}

/// This trait is used to restrict access to methods that would otherwise be public in [`Reading`].
#[async_trait]
trait ReadingInternal: Reading {
    /// Applies the [`Reading`] protocol to a single connection.
    async fn handle_new_connection(&self, (conn, conn_returner): ReturnableConnection);

    /// Wraps the user-supplied [`Decoder`] ([`Reading::Codec`]) in another one used for message accounting.
    fn map_codec<T: AsyncRead>(
        &self,
        framed: FramedRead<T, Self::Codec>,
        conn: &Connection,
    ) -> FramedRead<T, CountingCodec<Self::Codec>>;
}

#[async_trait]
impl<R: Reading> ReadingInternal for R {
    async fn handle_new_connection(&self, (mut conn, conn_returner): ReturnableConnection) {
        let addr = conn.addr();
        let codec = self.codec(addr, !conn.side());
        let reader = conn.reader.take().expect("missing connection reader!");
        let framed = FramedRead::new(reader, codec);
        let mut framed = self.map_codec(framed, &conn);

        // the connection will notify the reading task once it's fully ready
        let (tx_conn_ready, rx_conn_ready) = oneshot::channel();
        conn.readiness_notifier = Some(tx_conn_ready);

        if Self::INITIAL_BUFFER_SIZE != 0 {
            framed.read_buffer_mut().reserve(Self::INITIAL_BUFFER_SIZE);
        }

        let (inbound_message_sender, mut inbound_message_receiver) =
            mpsc::channel::<(R::Message, QueuedMessageGuard)>(self.message_queue_depth());

        // use a channel to know when the processing task is ready
        let (tx_processing, rx_processing) = oneshot::channel::<()>();

        // the task for processing parsed messages
        let self_clone = self.clone();
        let conn_span = conn.span().clone();
        let inbound_processing_task = tokio::spawn(Box::pin(async move {
            let node = self_clone.tcp();
            trace!(parent: &conn_span, "spawned a task for processing messages");
            tx_processing.send(()).unwrap(); // safe; the channel was just opened

            // disconnect automatically regardless of how this task concludes
            let _conn_cleanup = DisconnectOnDrop::new(node.clone(), addr, DisconnectOrigin::Reading);

            while let Some((msg, _guard)) = inbound_message_receiver.recv().await {
                if let Err(e) = self_clone.process_message(addr, msg).await {
                    error!(parent: &conn_span, "can't process a message: {e}");
                }
                // _guard drops here, after process_message completes
            }
        }));
        let _ = rx_processing.await;
        conn.tasks.push(inbound_processing_task);

        // use a channel to know when the reader task is ready
        let (tx_reader, rx_reader) = oneshot::channel::<()>();

        // the task for reading messages from a stream
        let node = self.tcp().clone();
        let conn_span = conn.span().clone();
        let reader_task = tokio::spawn(Box::pin(async move {
            trace!(parent: &conn_span, "spawned a task for reading messages");
            tx_reader.send(()).unwrap(); // safe; the channel was just opened

            // postpone reads until the connection is fully established; if the process fails,
            // this task gets aborted, so there is no need for a dedicated timeout
            let _ = rx_conn_ready.await;

            // disconnect automatically regardless of how this task concludes
            let _conn_cleanup = DisconnectOnDrop::new(node.clone(), addr, DisconnectOrigin::Reading);

            // dropped message log suppression helpers
            let mut dropped_count: usize = 0;
            let mut last_drop_log = Instant::now();

            loop {
                let next_frame_future = framed.next();
                let read_result = match timeout(Self::IDLE_TIMEOUT, next_frame_future).await {
                    Ok(res) => res, // IO completed (success or error)
                    Err(_) => {
                        debug!(parent: &conn_span, "connection timed out due to inactivity");
                        break;
                    }
                };
                match read_result {
                    Some(Ok(msg)) => {
                        // send the message for further processing
                        if let Err(e) = inbound_message_sender.try_send((msg, QueuedMessageGuard::new())) {
                            match e {
                                mpsc::error::TrySendError::Full(_) => {
                                    // avoid log flooding
                                    dropped_count += 1;
                                    if last_drop_log.elapsed() >= Duration::from_secs(1) {
                                        warn_about_dropped_messages(&conn_span, &mut dropped_count, &mut last_drop_log);
                                    }
                                }
                                mpsc::error::TrySendError::Closed(_) => {
                                    error!(parent: &conn_span, "inbound channel closed");
                                    break;
                                }
                            }
                        } else if dropped_count != 0 {
                            warn_about_dropped_messages(&conn_span, &mut dropped_count, &mut last_drop_log);
                            debug!(parent: &conn_span, "the inbound queue is no longer saturated");
                        }
                    }
                    Some(Err(e)) => {
                        error!(parent: &conn_span, "can't read: {e}");
                        break;
                    }
                    None => break, // end of stream
                }
            }
        }));
        let _ = rx_reader.await;
        conn.tasks.push(reader_task);

        // return the Connection to the Tcp, resuming Tcp::adapt_stream
        if conn_returner.send(Ok(conn)).is_err() {
            unreachable!("couldn't return a Connection to the Tcp");
        }
    }

    fn map_codec<T: AsyncRead>(
        &self,
        framed: FramedRead<T, Self::Codec>,
        conn: &Connection,
    ) -> FramedRead<T, CountingCodec<Self::Codec>> {
        framed.map_decoder(|codec| CountingCodec {
            codec,
            node: self.tcp().clone(),
            stats: Arc::clone(conn.stats()),
            acc: 0,
            span: conn.span().clone(),
        })
    }
}

/// A wrapper [`Decoder`] that also counts the inbound messages.
struct CountingCodec<D: Decoder> {
    codec: D,
    node: Tcp,
    stats: Arc<Stats>,
    acc: usize,
    span: Span,
}

impl<D: Decoder> Decoder for CountingCodec<D> {
    type Error = D::Error;
    type Item = D::Item;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let initial_buf_len = src.len();
        let ret = self.codec.decode(src)?;
        let final_buf_len = src.len();
        // defensive: the Decoder trait does not strictly forbid an inner codec from
        // growing `src`; use saturating_sub to guard against such a possibility
        let consumed = initial_buf_len.saturating_sub(final_buf_len);
        let read_len = consumed + self.acc;

        if read_len != 0 {
            trace!(parent: &self.span, "read {read_len}B");

            if ret.is_some() {
                self.acc = 0;
                self.stats.register_received_message(read_len);
                self.node.stats().register_received_message(read_len);
            } else {
                self.acc = read_len;
            }
        }

        Ok(ret)
    }
}

/// Maintains the [`metrics::tcp::QUEUED_INBOUND_MESSAGES`] gauge. Paired with each queued message
/// so the gauge stays balanced whether the message is processed normally or discarded when the
/// inbound channel is dropped (e.g. on connection abort). The caller must hold this guard until
/// processing is complete; dropping it earlier will decrement the gauge prematurely.
///
/// This is the only thing that may touch the gauge: a second increment anywhere else makes it
/// drift upward forever, since nothing else decrements it.
struct QueuedMessageGuard;

impl QueuedMessageGuard {
    fn new() -> Self {
        #[cfg(feature = "metrics")]
        metrics::increment_gauge(metrics::tcp::QUEUED_INBOUND_MESSAGES, 1f64);
        Self
    }
}

impl Drop for QueuedMessageGuard {
    fn drop(&mut self) {
        #[cfg(feature = "metrics")]
        metrics::decrement_gauge(metrics::tcp::QUEUED_INBOUND_MESSAGES, 1f64);
    }
}

/// Warns that some messages were dropped and resets the related counters.
fn warn_about_dropped_messages(span: &Span, dropped_count: &mut usize, last_drop_log: &mut Instant) {
    warn!(
        parent: span,
        "dropped {dropped_count} messages due\
        to inbound queue saturation",
    );
    // reset counters
    *dropped_count = 0;
    *last_drop_log = Instant::now();
}
