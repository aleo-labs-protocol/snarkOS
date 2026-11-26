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

//! The lock types that snarkOS crates use.
//!
//! Building with `--cfg locktick` swaps every lock for its instrumented counterpart from the
//! `locktick` crate, which records where each lock and guard is taken. `.cargo/config.toml`
//! defines the cargo aliases that set the cfg.
//!
//! `locktick` comes from snarkVM's re-export, `snarkvm::utilities::locktick`, so these locks and
//! snarkVM's register with one copy of the crate, and [`spawn_lock_reporter`] logs both. Hazard: a
//! direct dependency on `locktick` can build a second copy, whose `lock_snapshots` misses
//! snarkVM's locks.
//!
//! Building with `--cfg shuttle` swaps the `parking_lot` locks for the ones of the `shuttle`
//! concurrency testing tool, and the types of [`crate::atomic`] with them. A shuttle test runs
//! its threads and tasks one at a time, and picks which one runs next at each operation on these
//! types.
//!
//! In that build, the `tokio` dependency of a workspace crate is the package
//! `shuttle-tokio-impl`: its `Cargo.toml` names one package for `cfg(shuttle)` and the other
//! one for `cfg(not(shuttle))`. The [`tokio`] locks are thus shuttle's as well, and so are the
//! tasks, the channels and the timers. The exceptions are `snarkos-cli` and
//! `snarkos-ledger-distribution`, which build a `tokio` runtime with options that shuttle's
//! runtime does not have. `snarkos-cli` and the `snarkos` binary do not compile in that build.
//!
//! Hazards of that build:
//! - These types panic outside a shuttle test, so the build can run no test that takes a lock
//!   without one. A `#[tokio::test]` is a shuttle test there.
//! - A `tokio` type of a crate that has the two entries is not the type of the same name in a
//!   crate that does not. A new crate that exchanges such a type with them needs the entries too.
//! - Time is not real. `tokio::time::sleep` ends as soon as shuttle runs the task again, and
//!   `tokio::time::timeout` ends only when the test expires it with `tokio::time::trigger_timeouts`.
//! - snarkVM keeps its own locks and the real `tokio`. Shuttle does not see them: no switch
//!   happens at them, and a test deadlocks if one thread waits for such a lock while its holder
//!   is switched out.
//! - `tokio::io`, `tokio::net`, `tokio::fs` and `tokio::signal` stay the real ones, and so do the
//!   types of `tokio-util` and `tokio-stream`. Shuttle does not see them either, and a socket or
//!   a signal handler has no `tokio` runtime to register with.
//! - The two cfgs are mutually exclusive.
//!
//! The submodules mirror the layout of `locktick`. A lock's methods return the guard alias of
//! its submodule in every build. Under `--cfg locktick` that guard is a `locktick::LockGuard`
//! that dereferences to the plain guard, and under `--cfg shuttle` it is the guard of shuttle's
//! lock. Code that passes a guard to an associated function of the plain guard type, such as
//! `parking_lot::MutexGuard::map`, compiles only without the cfgs.
//!
//! `clippy.toml` disallows the lock types of `parking_lot`, `tokio` and `locktick` everywhere
//! else. Turning these type aliases into `pub use` re-exports would make that lint flag every
//! user of them.

#![allow(clippy::disallowed_types)]

#[cfg(all(locktick, shuttle))]
compile_error!("`--cfg locktick` and `--cfg shuttle` both replace the locks of `snarkos_utilities::locks`: set one");

#[cfg(not(locktick))]
type Guard<G> = G;
#[cfg(locktick)]
type Guard<G> = ::snarkvm::utilities::locktick::LockGuard<G>;

/// Synchronous locks from `parking_lot`.
pub mod parking_lot {
    use super::Guard;

    /// The crate whose guards the locks of `backend` hand out, wrapped or not.
    #[cfg(not(shuttle))]
    use ::parking_lot as guards;
    #[cfg(shuttle)]
    use ::shuttle_parking_lot as guards;

    #[cfg(locktick)]
    use ::snarkvm::utilities::locktick::parking_lot as backend;
    #[cfg(not(locktick))]
    use guards as backend;

    pub type Mutex<T> = backend::Mutex<T>;
    pub type RwLock<T> = backend::RwLock<T>;
    pub type MutexGuard<'a, T> = Guard<guards::MutexGuard<'a, T>>;
    pub type RwLockReadGuard<'a, T> = Guard<guards::RwLockReadGuard<'a, T>>;
    pub type RwLockWriteGuard<'a, T> = Guard<guards::RwLockWriteGuard<'a, T>>;
}

/// Asynchronous locks from `tokio::sync`.
pub mod tokio {
    use super::Guard;

    #[cfg(locktick)]
    use ::snarkvm::utilities::locktick::tokio as backend;
    #[cfg(not(locktick))]
    use ::tokio::sync as backend;

    pub type Mutex<T> = backend::Mutex<T>;
    pub type RwLock<T> = backend::RwLock<T>;
    pub type MutexGuard<'a, T> = Guard<::tokio::sync::MutexGuard<'a, T>>;
    pub type RwLockReadGuard<'a, T> = Guard<::tokio::sync::RwLockReadGuard<'a, T>>;
    pub type RwLockWriteGuard<'a, T> = Guard<::tokio::sync::RwLockWriteGuard<'a, T>>;
}

/// Under `--cfg locktick`, spawns a thread that logs every lock guard that is held or waited
/// for, every three seconds. Otherwise, does nothing.
pub fn spawn_lock_reporter() {
    #[cfg(locktick)]
    std::thread::spawn(|| {
        loop {
            tracing::info!("[locktick] checking for active lock guards");
            let ts = std::time::Instant::now();
            let mut infos = ::snarkvm::utilities::locktick::lock_snapshots();
            infos.sort_unstable_by(|l1, l2| l1.location.cmp(&l2.location));

            for lock in infos {
                let mut guards = lock.known_guards.values().filter(|g| g.is_in_use()).collect::<Vec<_>>();
                guards.sort_unstable_by(|g1, g2| g1.location.cmp(&g2.location));

                for guard in guards {
                    let location = &guard.location;
                    let kind = guard.kind;
                    let num_uses = guard.num_uses;
                    let active_users = guard.num_active_uses();
                    let avg_duration = guard.avg_duration();
                    let avg_wait_time = guard.avg_wait_time();
                    let num_waiting = guard.num_waiting();
                    tracing::info!(
                        "[locktick] {location} ({:?}): {num_uses}; {active_users} active; {num_waiting} waiting; avg d: {:?}; avg w: {:?}",
                        kind,
                        avg_duration,
                        avg_wait_time
                    );
                }
            }
            tracing::debug!("[locktick] finished the check in {:?}", ts.elapsed());
            std::thread::sleep(std::time::Duration::from_secs(3));
        }
    });
}
