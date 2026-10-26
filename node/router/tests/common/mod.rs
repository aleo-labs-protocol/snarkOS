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

//! Shared helpers for the router's integration tests.
//!
//! `TestRouter` and the router constructors live in `snarkos_node_router::test_helpers` so that
//! other crates can use them too; this module only re-exports them.

pub use snarkos_node_router::test_helpers::*;

/// A helper macro to print the TCP listening address, along with the connected and connecting peers.
#[macro_export]
macro_rules! print_tcp {
    ($node:expr) => {
        println!(
            "{}: Active - {:?}, Pending - {:?}",
            $node.local_ip(),
            $node.tcp().connected_addrs(),
            $node.tcp().connecting_addrs()
        );
    };
}
