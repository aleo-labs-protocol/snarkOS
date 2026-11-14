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
#![allow(clippy::blocks_in_conditions)]
#![allow(clippy::type_complexity)]

#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate tracing;

#[cfg(feature = "metrics")]
extern crate snarkos_node_metrics as metrics;

pub use snarkos_node_bft_events as events;
pub use snarkos_node_bft_ledger_service as ledger_service;
pub use snarkos_node_bft_storage_service as storage_service;

use std::time::Duration;

pub mod helpers;

mod bft;
pub use bft::*;

mod gateway;
pub use gateway::*;

mod primary;
pub use primary::*;

mod sync;
pub use sync::*;

#[cfg(test)]
mod test_helpers;

mod worker;
pub use worker::*;

pub const CONTEXT: &str = "[MemoryPool]";

/// The port on which the memory pool listens for incoming connections.
pub const MEMORY_POOL_PORT: u16 = 5000; // port

/// The maximum time to wait before proposing a batch.
pub const MAX_BATCH_DELAY: Duration = Duration::from_millis(2500);

/// The minimum time that needs to elapse between two consecutive batch proposals.
/// This creates a lower bound on the block interval, and ensures the network will not be overwhelmed with too many blocks/certificates.
pub const MIN_BATCH_DELAY: Duration = Duration::from_secs(1);

/// The time a primary waits between attempts to create a new batch (only relevant after `MIN_BATCH_DELAY` has passed).
/// This only serves as a failsafe in case the task does not get woken up through other means.
/// Lowering it too much would be wasteful.
pub const CREATE_BATCH_INTERVAL: Duration = Duration::from_millis(250);

/// The maximum time to wait before timing out on a fetch.
/// TODO(kaimast): directy multiply by constant once the `const_trait_impl` feature is stable.
pub const MAX_FETCH_TIMEOUT: Duration = Duration::from_millis(3 * (MAX_BATCH_DELAY.as_millis() as u64));

/// The maximum time allowed for the leader to send their certificate.
/// After this time, the node will consider the leader as failed and try to advance the round without it.
pub const MAX_LEADER_CERTIFICATE_DELAY: Duration = Duration::from_millis(2 * (MAX_BATCH_DELAY.as_millis() as u64));

/// The maximum difference allowed between our local time and a certificate's timestamp, for the node to sign the certificate.
/// This prevents malicious actors from proposing certificates with timestamps that are too log or too far in the future)
/// w
pub const MAX_TIMESTAMP_DELTA: Duration = Duration::from_secs(10);

/// The maximum number of workers that can be spawned.
pub const MAX_WORKERS: u8 = 1; // worker(s)

/// The interval at which each primary broadcasts a ping to every other node.
/// Note: If this is updated, be sure to update `MAX_BLOCKS_BEHIND` to correspond properly.
pub const PRIMARY_PING_INTERVAL: Duration = Duration::from_millis(2 * (MAX_BATCH_DELAY.as_millis() as u64));

/// The interval at which each worker broadcasts a ping to every other node.
pub const WORKER_PING_INTERVAL: Duration = Duration::from_millis(4 * (MAX_BATCH_DELAY.as_millis() as u64));

/// A helper macro to spawn a blocking task.
#[macro_export]
macro_rules! spawn_blocking {
    ($expr:expr) => {
        match tokio::task::spawn_blocking(move || $expr).await {
            Ok(value) => value,
            Err(error) => Err(anyhow::anyhow!("[tokio::spawn_blocking] {error}")),
        }
    };
}
