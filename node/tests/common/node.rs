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

use crate::common::test_peer::sample_genesis_block;

use snarkos_account::Account;
use snarkos_node::{Client, Prover, Validator, rest::RestVerificationLimits};
use snarkos_utilities::{NodeDataDir, SignalHandler};

use snarkvm::prelude::{MainnetV0 as CurrentNetwork, store::helpers::memory::ConsensusMemory};

use aleo_std::StorageMode;
use std::str::FromStr;

pub async fn client() -> Client<CurrentNetwork, ConsensusMemory<CurrentNetwork>> {
    Client::new(
        "127.0.0.1:0".parse().unwrap(),
        None,
        10,
        RestVerificationLimits::max::<CurrentNetwork, ConsensusMemory<CurrentNetwork>>(),
        None,  // No history compatibility mode.
        false, // Do not write JSON history.
        Account::<CurrentNetwork>::from_str("APrivateKey1zkp2oVPTci9kKcUprnbzMwq95Di1MQERpYBhEeqvkrDirK1").unwrap(),
        &[],
        sample_genesis_block(),
        None, // No CDN.
        StorageMode::new_test(None),
        NodeDataDir::new_test(None),
        false, // Connect to untrusted peers.
        None,
        SignalHandler::new(None),
    )
    .await
    .expect("couldn't create client instance")
}

pub async fn prover() -> Prover<CurrentNetwork, ConsensusMemory<CurrentNetwork>> {
    Prover::new(
        "127.0.0.1:0".parse().unwrap(),
        Account::<CurrentNetwork>::from_str("APrivateKey1zkp2oVPTci9kKcUprnbzMwq95Di1MQERpYBhEeqvkrDirK1").unwrap(),
        &[],
        sample_genesis_block(),
        NodeDataDir::new_test(None),
        false,
        None,
        SignalHandler::new(None),
    )
    .await
    .expect("couldn't create prover instance")
}

pub async fn validator() -> Validator<CurrentNetwork, ConsensusMemory<CurrentNetwork>> {
    Validator::new(
        "127.0.0.1:0".parse().unwrap(),
        None,
        None,
        10,
        RestVerificationLimits::max::<CurrentNetwork, ConsensusMemory<CurrentNetwork>>(),
        None,  // No history compatibility mode.
        false, // Do not write JSON history.
        Account::<CurrentNetwork>::from_str("APrivateKey1zkp2oVPTci9kKcUprnbzMwq95Di1MQERpYBhEeqvkrDirK1").unwrap(),
        &[],
        &[],
        sample_genesis_block(), // Should load the current network's genesis block.
        None,                   // No CDN.
        StorageMode::new_test(None),
        NodeDataDir::new_test(None),
        false, // This test requires validators to connect to peers.
        false, // No dev traffic in production mode.
        None,
        None, // No dev committee hotswap in production mode.
        1,    // Leave the batch spend limit unchanged.
        SignalHandler::new(None),
    )
    .await
    .expect("couldn't create validator instance")
}
