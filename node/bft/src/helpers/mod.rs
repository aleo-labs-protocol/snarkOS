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

pub mod cache;
pub use cache::*;

pub mod channels;
pub use channels::*;

pub mod dag;
pub use dag::*;

pub mod partition;
pub use partition::*;

pub mod pending;
pub use pending::*;

pub mod proposal;
pub use proposal::*;

pub mod proposal_cache;
pub use proposal_cache::*;

pub mod ready;
pub use ready::*;

pub mod signed_proposals;
pub use signed_proposals::*;

pub mod storage;
pub use storage::*;

#[cfg(feature = "metrics")]
pub mod telemetry;
#[cfg(feature = "metrics")]
pub use telemetry::*;

pub mod timestamp;
pub use timestamp::*;

/// Formats an ID into a truncated identifier (for logging purposes).
pub fn fmt_id(id: impl ToString) -> String {
    let id = id.to_string();
    let mut formatted_id = id.chars().take(16).collect::<String>();
    if id.chars().count() > 16 {
        formatted_id.push_str("..");
    }
    formatted_id
}
