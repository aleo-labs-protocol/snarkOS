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

#![forbid(unsafe_code)]

#[macro_use]
extern crate async_trait;

use std::{io, net::SocketAddr};
use tokio::sync::oneshot;

/// Abstract communication service.
///
/// Implemented by `Gateway` and `Client`.
#[async_trait]
pub trait CommunicationService: Send + Sync {
    /// The message type used by this communication service.
    type Message: Clone;

    /// Generates the service-specific message for a block request.
    fn prepare_block_request(start: u32, end: u32) -> Self::Message;

    /// Sends the given message to specified peer.
    ///
    /// This function returns as soon as the message is queued to be sent,
    /// without waiting for the actual delivery; instead, the caller is provided with a [`oneshot::Receiver`]
    /// which can be used to determine when and whether the message has been delivered.
    /// If no peer with the given IP exists, this function returns None.
    async fn send(&self, peer_ip: SocketAddr, message: Self::Message) -> Option<oneshot::Receiver<io::Result<()>>>;
}
