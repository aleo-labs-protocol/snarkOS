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
//! The submodules mirror the layout of `locktick`. A lock's methods return the guard alias of
//! its submodule in both builds. Under `--cfg locktick` that guard is a `locktick::LockGuard`
//! that dereferences to the plain guard. Code that passes a guard to an associated function of
//! the plain guard type, such as `parking_lot::MutexGuard::map`, compiles only without the cfg.
//!
//! `clippy.toml` disallows the lock types of `parking_lot`, `tokio` and `locktick` everywhere
//! else. Turning these type aliases into `pub use` re-exports would make that lint flag every
//! user of them.

/// Synchronous locks from `parking_lot`.
pub mod parking_lot {
    #[cfg(not(locktick))]
    pub type Mutex<T> = ::parking_lot::Mutex<T>;
    #[cfg(locktick)]
    pub type Mutex<T> = ::locktick::parking_lot::Mutex<T>;

    #[cfg(not(locktick))]
    pub type RwLock<T> = ::parking_lot::RwLock<T>;
    #[cfg(locktick)]
    pub type RwLock<T> = ::locktick::parking_lot::RwLock<T>;

    #[cfg(not(locktick))]
    pub type MutexGuard<'a, T> = ::parking_lot::MutexGuard<'a, T>;
    #[cfg(locktick)]
    pub type MutexGuard<'a, T> = ::locktick::LockGuard<::parking_lot::MutexGuard<'a, T>>;

    #[cfg(not(locktick))]
    pub type RwLockReadGuard<'a, T> = ::parking_lot::RwLockReadGuard<'a, T>;
    #[cfg(locktick)]
    pub type RwLockReadGuard<'a, T> = ::locktick::LockGuard<::parking_lot::RwLockReadGuard<'a, T>>;

    #[cfg(not(locktick))]
    pub type RwLockWriteGuard<'a, T> = ::parking_lot::RwLockWriteGuard<'a, T>;
    #[cfg(locktick)]
    pub type RwLockWriteGuard<'a, T> = ::locktick::LockGuard<::parking_lot::RwLockWriteGuard<'a, T>>;
}

/// Asynchronous locks from `tokio::sync`.
pub mod tokio {
    #[cfg(not(locktick))]
    pub type Mutex<T> = ::tokio::sync::Mutex<T>;
    #[cfg(locktick)]
    pub type Mutex<T> = ::locktick::tokio::Mutex<T>;

    #[cfg(not(locktick))]
    pub type RwLock<T> = ::tokio::sync::RwLock<T>;
    #[cfg(locktick)]
    pub type RwLock<T> = ::locktick::tokio::RwLock<T>;

    #[cfg(not(locktick))]
    pub type MutexGuard<'a, T> = ::tokio::sync::MutexGuard<'a, T>;
    #[cfg(locktick)]
    pub type MutexGuard<'a, T> = ::locktick::LockGuard<::tokio::sync::MutexGuard<'a, T>>;

    #[cfg(not(locktick))]
    pub type RwLockReadGuard<'a, T> = ::tokio::sync::RwLockReadGuard<'a, T>;
    #[cfg(locktick)]
    pub type RwLockReadGuard<'a, T> = ::locktick::LockGuard<::tokio::sync::RwLockReadGuard<'a, T>>;

    #[cfg(not(locktick))]
    pub type RwLockWriteGuard<'a, T> = ::tokio::sync::RwLockWriteGuard<'a, T>;
    #[cfg(locktick)]
    pub type RwLockWriteGuard<'a, T> = ::locktick::LockGuard<::tokio::sync::RwLockWriteGuard<'a, T>>;
}

#[cfg(locktick)]
pub use ::locktick::lock_snapshots;
