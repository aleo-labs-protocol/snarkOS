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

//! Samples that cost proofs to build, shared by the test processes of a binary.

use snarkos_utilities::test_fixture::cached_fixture;
use snarkvm::{
    ledger::{Block, Transaction, test_helpers},
    prelude::{FromBytes, TestRng, ToBytes},
};

use std::sync::OnceLock;

type CurrentNetwork = snarkvm::prelude::MainnetV0;

/// The samples are one fixture: snarkVM builds the fee transaction from its sample genesis block,
/// which costs two of the three proofs.
fn samples(rng: &mut TestRng) -> &'static (Block<CurrentNetwork>, Transaction<CurrentNetwork>) {
    static SAMPLES: OnceLock<(Block<CurrentNetwork>, Transaction<CurrentNetwork>)> = OnceLock::new();
    SAMPLES.get_or_init(|| {
        cached_fixture(
            "samples",
            &[],
            |(block, transaction): &(Block<CurrentNetwork>, Transaction<CurrentNetwork>)| {
                let mut bytes = block.to_bytes_le().unwrap();
                transaction.write_le(&mut bytes).unwrap();
                bytes
            },
            |mut bytes| Some((Block::read_le(&mut bytes).ok()?, Transaction::read_le(&mut bytes).ok()?)),
            || (test_helpers::sample_genesis_block(rng), test_helpers::sample_fee_public_transaction(rng)),
        )
    })
}

/// Returns a sample genesis block. Every call returns the same block, whatever `rng` is.
pub fn sample_genesis_block(rng: &mut TestRng) -> Block<CurrentNetwork> {
    samples(rng).0.clone()
}

/// Returns a sample fee transaction. Every call returns the same transaction, whatever `rng` is.
pub fn sample_fee_public_transaction(rng: &mut TestRng) -> Transaction<CurrentNetwork> {
    samples(rng).1.clone()
}
