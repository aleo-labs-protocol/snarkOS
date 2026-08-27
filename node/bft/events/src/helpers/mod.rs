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

mod codec;
pub use codec::*;

mod handshake;
pub use handshake::*;

/// The handshake strategies, which are otherwise unreachable from outside this
/// crate: `handshake` is a private module. See `crate::prop_tests` for the rest.
#[cfg(any(test, feature = "fuzz-helpers"))]
pub use handshake::prop_tests as handshake_generators;
