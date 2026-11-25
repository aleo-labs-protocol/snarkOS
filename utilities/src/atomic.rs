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

//! Atomic types for state that a shuttle test covers.
//!
//! These are the types of `std::sync::atomic`. Building with `--cfg shuttle` swaps them for the
//! ones of the `shuttle` concurrency testing tool, which can switch threads at each operation on
//! them; see [`crate::locks`] for that build.
//!
//! Hazard: shuttle does not see an atomic that comes from `std` directly. A test then never runs
//! another thread between a read of it and the code that acts on the value.

#[cfg(shuttle)]
pub use ::shuttle::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering};
#[cfg(not(shuttle))]
pub use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering};
