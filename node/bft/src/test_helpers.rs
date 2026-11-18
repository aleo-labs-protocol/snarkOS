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

//! Fixtures that cost proofs to build, shared by the test processes of this binary.

use snarkos_utilities::test_fixture::cached_fixture;
use snarkvm::{
    ledger::{
        Block,
        Transaction,
        block::{Fee, Transition},
        test_helpers,
    },
    prelude::{Field, FromBytes, TestRng, ToBytes, Uniform},
};

use std::sync::OnceLock;

type CurrentNetwork = snarkvm::prelude::MainnetV0;

/// The samples are one fixture: snarkVM builds the transaction from its sample genesis block, which
/// costs two of the three proofs.
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
            || {
                (
                    test_helpers::sample_genesis_block(rng),
                    test_helpers::sample_execution_transaction_with_fee(false, rng, 0),
                )
            },
        )
    })
}

/// Returns a sample genesis block. Every call returns the same block, whatever `rng` is.
pub(crate) fn sample_genesis_block(rng: &mut TestRng) -> Block<CurrentNetwork> {
    samples(rng).0.clone()
}

/// Samples an execution transaction with a public fee and an ID of its own.
///
/// Only the commitment of the fee transition is sampled; the rest is the same for every call. The
/// fee proof is not made for that commitment and does not verify, so this is only for tests in which
/// nothing verifies the transaction.
pub(crate) fn sample_transaction(rng: &mut TestRng) -> Transaction<CurrentNetwork> {
    let Transaction::Execute(_, _, execution, Some(fee)) = &samples(rng).1 else {
        unreachable!("the sample is an execution with a fee")
    };

    let transition = fee.transition();
    let transition = Transition::new(
        *transition.program_id(),
        *transition.function_name(),
        transition.inputs().to_vec(),
        transition.outputs().to_vec(),
        *transition.tpk(),
        Field::rand(rng),
        *transition.scm(),
    )
    .unwrap();
    let fee = Fee::from_unchecked(transition, fee.global_state_root(), fee.proof().cloned());

    Transaction::from_execution(execution.as_ref().clone(), Some(fee)).unwrap()
}
