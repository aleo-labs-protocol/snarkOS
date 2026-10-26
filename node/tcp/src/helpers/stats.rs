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
    sync::atomic::{AtomicU64, Ordering::Relaxed},
    time::Instant,
};

/// Contains statistics related to Tcp or one of its connections.
pub struct Stats {
    /// The timestamp of the creation (for the node) or connection (to a peer).
    created: Instant,
    /// The number of all messages sent.
    msgs_sent: AtomicU64,
    /// The number of all messages received.
    msgs_received: AtomicU64,
    /// The number of all bytes sent.
    bytes_sent: AtomicU64,
    /// The number of all bytes received.
    bytes_received: AtomicU64,
}

impl Stats {
    /// Creates a new instance of the object.
    pub fn new(created: Instant) -> Self {
        Self {
            created,
            msgs_sent: Default::default(),
            msgs_received: Default::default(),
            bytes_sent: Default::default(),
            bytes_received: Default::default(),
        }
    }

    /// Returns the creation or connection timestamp.
    pub fn created(&self) -> Instant {
        self.created
    }

    /// Returns the number of sent messages and their collective size in bytes.
    pub fn sent(&self) -> (u64, u64) {
        let msgs = self.msgs_sent.load(Relaxed);
        let bytes = self.bytes_sent.load(Relaxed);

        (msgs, bytes)
    }

    /// Returns the number of received messages and their collective size in bytes.
    pub fn received(&self) -> (u64, u64) {
        let msgs = self.msgs_received.load(Relaxed);
        let bytes = self.bytes_received.load(Relaxed);

        (msgs, bytes)
    }

    /// Registers a sent message of the provided `size` in bytes.
    pub fn register_sent_message(&self, size: usize) {
        self.msgs_sent.fetch_add(1, Relaxed);
        self.bytes_sent.fetch_add(size as u64, Relaxed);
    }

    /// Registers a received message of the provided `size` in bytes.
    pub fn register_received_message(&self, size: usize) {
        self.msgs_received.fetch_add(1, Relaxed);
        self.bytes_received.fetch_add(size as u64, Relaxed);
    }
}
