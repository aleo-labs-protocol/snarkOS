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

//! The clock and the timers of code that a shuttle test covers.
//!
//! These are the items of `tokio::time`. Building with `--cfg shuttle` swaps them for a clock
//! that a test controls; see the `virtual_clock` module for how time moves there, and
//! [`crate::locks`] for that build.
//!
//! Hazard: `tokio::time` itself has no clock in that build. Its `sleep` ends as soon as shuttle
//! runs the task again, its `timeout` ends only when the test expires it, and its `Instant` is
//! the real one. Code that uses it directly does not keep the order of deadlines in a test.

#[cfg(shuttle)]
pub mod virtual_clock;

#[cfg(not(shuttle))]
pub use ::tokio::time::{Instant, Sleep, Timeout, error, sleep, sleep_until, timeout, timeout_at};
#[cfg(shuttle)]
pub use virtual_clock::{Instant, Sleep, Timeout, error, sleep, sleep_until, timeout, timeout_at};
