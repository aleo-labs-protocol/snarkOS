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
    scheduler::{PctScheduler, RandomScheduler, ReplayScheduler, Scheduler},
};
use std::{
    future::Future,
    sync::{Arc, atomic::Ordering},
};

/// The depth of the PCT scheduler, which changes the priorities of the threads at
/// `PCT_DEPTH - 1` points.
const PCT_DEPTH: usize = 3;

/// The stack size of each thread and task of a test, which is the one of the threads of the node.
/// Shuttle's default of 60 KiB is too small for the test fixtures that build a transaction.
const STACK_SIZE: usize = 8 * 1024 * 1024;

/// The environment variable that names a file with the schedule of a failure. [`check`] then
/// runs that schedule only.
const REPLAY_FILE: &str = "SHUTTLE_REPLAY_FILE";

/// Runs `test` under two schedulers, each with new seeds and for `iterations` orders of the
/// steps of its threads and tasks.
///
/// A failure prints the schedule that replays it. To replay it, save it in a file, name the file
/// in `SHUTTLE_REPLAY_FILE`, and run that test only. The random scheduler also prints its seed,
/// which `SHUTTLE_RANDOM_SEED` takes.
///
/// The PCT scheduler runs one thread until a random point, and then prefers another one. It
/// finds the orders in which one thread stops between two particular steps while another one
/// runs to its end. The random scheduler picks a thread at every step, so its threads advance
/// together, and it almost never produces those orders.
///
/// Both run the clock of [`crate::time`] only when no thread can run.
///
/// A test in which no two threads can run at the same time, and that takes no random data from
/// shuttle, has one order. It runs once. A `tokio::select!` takes random data: it starts at a
/// random branch.
pub fn check(iterations: usize, test: impl Fn() + Send + Sync + 'static) {
    let test = Arc::new(test);
    let run = |scheduler: IdleClock<Box<dyn Scheduler + Send>>| {
        let (test, had_choice, took_random_data) = (test.clone(), scheduler.had_choice(), scheduler.took_random_data());
        let mut config = Config::new();
        config.stack_size = STACK_SIZE;
        // The one warning of shuttle is for a `lazy_static` that an execution drops at its end.
        // The clock is one.
        config.silence_warnings = true;
        Runner::new(scheduler, config).run(move || test());
        (had_choice.load(Ordering::Relaxed), took_random_data.load(Ordering::Relaxed))
    };

    if let Ok(path) = std::env::var(REPLAY_FILE) {
        let scheduler = ReplayScheduler::new_from_file(&path)
            .unwrap_or_else(|error| panic!("could not read the schedule in '{path}': {error}"));
        run(IdleClock::new(Box::new(scheduler)));
        return;
    }

    let (had_choice, took_random_data) = run(IdleClock::new(Box::new(RandomScheduler::new(1))));
    // The PCT scheduler panics on a test that gives it no threads to choose from.
    if had_choice {
        run(IdleClock::new(Box::new(PctScheduler::new(PCT_DEPTH, iterations))));
    }
    if had_choice || took_random_data {
        run(IdleClock::new(Box::new(RandomScheduler::new(iterations))));
    }
}

/// Runs the future that `test` makes; see [`check`].
pub fn check_async<F: Future<Output = ()>>(iterations: usize, test: impl Fn() -> F + Send + Sync + 'static) {
    check(iterations, move || ::shuttle::future::block_on(test()));
}

#[cfg(test)]
mod shuttle_tests {
    use super::*;
    use crate::time::sleep;

    use std::{sync::atomic::AtomicUsize, time::Duration};

    const ITERATIONS: usize = 100;

    /// One task waits in a `select!` for two timers with the same deadline. No two tasks can run
    /// at the same time, and each branch must still occur.
    #[test]
    fn select_in_one_task_takes_each_branch() {
        let counts = Arc::new((AtomicUsize::new(0), AtomicUsize::new(0)));

        let test_counts = counts.clone();
        check_async(ITERATIONS, move || {
            let counts = test_counts.clone();
            async move {
                let (num_runs, num_first) = &*counts;
                num_runs.fetch_add(1, Ordering::Relaxed);
                tokio::select! {
                    _ = sleep(Duration::from_secs(1)) => { num_first.fetch_add(1, Ordering::Relaxed); }
                    _ = sleep(Duration::from_secs(1)) => {}
                }
            }
        });

        let (num_runs, num_first) = (counts.0.load(Ordering::Relaxed), counts.1.load(Ordering::Relaxed));
        assert!(0 < num_first && num_first < num_runs, "the first branch won {num_first} of {num_runs} runs");
    }

    /// A task waits for an event that never comes, while another task keeps a timer.
    #[test]
    #[should_panic(expected = "the clock passed")]
    fn clock_ends_an_execution_that_waits_forever() {
        check_async(ITERATIONS, || async {
            tokio::spawn(async {
                loop {
                    sleep(Duration::from_secs(1)).await;
                }
            });
            std::future::pending::<()>().await;
        });
    }
}
