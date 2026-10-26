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

#![forbid(unsafe_code)]
#![allow(clippy::type_complexity)]

#[cfg(feature = "memory")]
pub mod memory;
#[cfg(feature = "memory")]
pub use memory::*;

#[cfg(feature = "persistent")]
pub mod persistent;
#[cfg(feature = "persistent")]
pub use persistent::*;

pub mod traits;
pub use traits::*;

// The expectations shared by every service, which need every service to assert them against.
#[cfg(all(test, any(feature = "memory", feature = "persistent")))]
mod tests;
