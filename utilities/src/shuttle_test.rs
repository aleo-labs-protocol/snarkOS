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

//! The runner of the tests of a `--cfg shuttle` build; see [`crate::locks`].

use crate::time::virtual_clock::IdleClock;

use ::shuttle::{
    Config,
    Runner,
    scheduler::{PctScheduler, RandomScheduler, Scheduler},
};
use std::{future::Future, sync::Arc};

/// The depth of the PCT scheduler, which changes the priorities of the threads at
/// `PCT_DEPTH - 1` points.
const PCT_DEPTH: usize = 3;

/// The stack size of each thread and task of a test, which is the one of the threads of the node.
/// Shuttle's default of 2 MiB is too small for the test fixtures that build a transaction.
const STACK_SIZE: usize = 8 * 1024 * 1024;

/// Runs `test` under two schedulers, each with new seeds and for `iterations` orders of the
/// steps of its threads and tasks. A failure prints the schedule that replays it, which
/// `shuttle::replay` takes. The random scheduler also prints its seed, which
/// `SHUTTLE_RANDOM_SEED` takes.
///
/// The PCT scheduler runs one thread until a random point, and then prefers another one. It
/// finds the orders in which one thread stops between two particular steps while another one
/// runs to its end. The random scheduler picks a thread at every step, so its threads advance
/// together, and it almost never produces those orders.
///
/// Both run the clock of [`crate::time`] only when no thread can run.
///
/// A test in which no two threads can run at the same time has one order. It runs once.
pub fn check(iterations: usize, test: impl Fn() + Send + Sync + 'static) {
    let test = Arc::new(test);
    let run = |scheduler: IdleClock<Box<dyn Scheduler + Send>>| {
        let (test, had_choice) = (test.clone(), scheduler.had_choice());
        let mut config = Config::new();
        config.stack_size = STACK_SIZE;
        Runner::new(scheduler, config).run(move || test());
        had_choice.load(std::sync::atomic::Ordering::Relaxed)
    };

    // The PCT scheduler panics on a test that gives it nothing to decide, so find that out first.
    if !run(IdleClock::new(Box::new(RandomScheduler::new(1)))) {
        return;
    }
    run(IdleClock::new(Box::new(PctScheduler::new(PCT_DEPTH, iterations))));
    run(IdleClock::new(Box::new(RandomScheduler::new(iterations))));
}

/// Runs the future that `test` makes; see [`check`].
pub fn check_async<F: Future<Output = ()>>(iterations: usize, test: impl Fn() -> F + Send + Sync + 'static) {
    check(iterations, move || ::shuttle::future::block_on(test()));
}
