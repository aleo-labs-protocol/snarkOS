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

use snarkos_node_network::{NodeType, PeerPoolHandling};
use snarkos_node_router::Routing;
use snarkos_utilities::SignalHandler;
use snarkvm::prelude::{Address, Network, PrivateKey, ViewKey};

use std::time::Duration;
use tokio::time::sleep;

#[async_trait]
pub trait NodeInterface<N: Network>: Routing<N> {
    /// Returns the node type.
    fn node_type(&self) -> NodeType {
        self.router().node_type()
    }

    /// Returns the account private key of the node.
    fn private_key(&self) -> &PrivateKey<N> {
        self.router().private_key()
    }

    /// Returns the account view key of the node.
    fn view_key(&self) -> &ViewKey<N> {
        self.router().view_key()
    }

    /// Returns the account address of the node.
    fn address(&self) -> Address<N> {
        self.router().address()
    }

    /// Returns `true` if the node is in development mode.
    fn is_dev(&self) -> bool {
        self.router().is_dev()
    }

    /// Blocks until a shutdown signal was received or manual shutdown was triggered.
    async fn wait_for_signals(&self, handler: &SignalHandler) {
        handler.wait_for_signals().await;

        // If the node is already initialized, then shut it down.
        self.shut_down().await;

        // Stop the metrics exporter. Note: it is started before the node is, so it is not one
        // of the node's own tasks, and it would be reported as a straggler below.
        #[cfg(feature = "metrics")]
        snarkos_node_metrics::shut_down_metrics();

        // Allow a bit of time for the tasks to wind down.
        sleep(Duration::from_secs(1)).await;

        // Check if there are any stragglers left.
        if let Some(handle) = &handler.handle {
            let live_tasks = handle.metrics().num_alive_tasks();
            let expected_leftover = if cfg!(unix) {
                // The 1 "extra" task comes from the FD monitor, which is aborted once
                // wait_for_signals concludes its work.
                1
            } else {
                0
            };
            if live_tasks > expected_leftover {
                error!("There are {} surplus live tasks", live_tasks - expected_leftover);
            }
        }
    }

    /// Shuts down the node.
    async fn shut_down(&self);
}
