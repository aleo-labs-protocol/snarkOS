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
//! The submodules mirror the layout of `locktick`. A lock's methods return the guard alias of
//! its submodule in both builds. Under `--cfg locktick` that guard is a `locktick::LockGuard`
//! that dereferences to the plain guard. Code that passes a guard to an associated function of
//! the plain guard type, such as `parking_lot::MutexGuard::map`, compiles only without the cfg.
//!
//! `clippy.toml` disallows the lock types of `parking_lot`, `tokio` and `locktick` everywhere
//! else. Turning these type aliases into `pub use` re-exports would make that lint flag every
//! user of them.

#![allow(clippy::disallowed_types)]

#[cfg(not(locktick))]
type Guard<G> = G;
#[cfg(locktick)]
type Guard<G> = ::snarkvm::utilities::locktick::LockGuard<G>;

/// Synchronous locks from `parking_lot`.
pub mod parking_lot {
    use super::Guard;

    #[cfg(not(locktick))]
    use ::parking_lot as backend;
    #[cfg(locktick)]
    use ::snarkvm::utilities::locktick::parking_lot as backend;

    pub type Mutex<T> = backend::Mutex<T>;
    pub type RwLock<T> = backend::RwLock<T>;
    pub type MutexGuard<'a, T> = Guard<::parking_lot::MutexGuard<'a, T>>;
    pub type RwLockReadGuard<'a, T> = Guard<::parking_lot::RwLockReadGuard<'a, T>>;
    pub type RwLockWriteGuard<'a, T> = Guard<::parking_lot::RwLockWriteGuard<'a, T>>;
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
