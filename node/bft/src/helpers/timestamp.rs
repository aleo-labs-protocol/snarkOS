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

use crate::MAX_TIMESTAMP_DELTA;
use snarkvm::prelude::{Result, bail};

use time::OffsetDateTime;

/// Returns the current UTC epoch timestamp.
pub fn now() -> i64 {
    now_utc().unix_timestamp()
}

/// Returns the current UTC epoch time.
pub fn now_utc() -> OffsetDateTime {
    // In a shuttle test, the timers of `snarkos_utilities::time` end with no real time in between.
    #[cfg(shuttle)]
    return OffsetDateTime::now_utc() + snarkos_utilities::time::virtual_clock::elapsed();
    #[cfg(not(shuttle))]
    OffsetDateTime::now_utc()
}

/// Converts the given timestamp to an `OffsetDateTime`, defaulting to the current UTC time if invalid.
pub fn to_utc_datetime(timestamp: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(timestamp).unwrap_or_else(|_| OffsetDateTime::now_utc())
}

/// Sanity checks the timestamp for liveness.
pub fn check_timestamp_for_liveness(timestamp: i64) -> Result<()> {
    // Ensure the timestamp is within range.
    if timestamp > (now() + MAX_TIMESTAMP_DELTA.as_secs() as i64) {
        bail!("Timestamp {timestamp} is too far in the future")
    }
    Ok(())
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use crate::MAX_TIMESTAMP_DELTA;

    use proptest::prelude::*;
    use test_strategy::proptest;

    fn any_valid_timestamp() -> BoxedStrategy<i64> {
        (Just(now()), 0..(MAX_TIMESTAMP_DELTA.as_secs() as i64)).prop_map(|(now, delta)| now + delta).boxed()
    }

    fn any_invalid_timestamp() -> BoxedStrategy<i64> {
        (Just(now()), (MAX_TIMESTAMP_DELTA.as_secs() as i64)..).prop_map(|(now, delta)| now + delta).boxed()
    }

    #[proptest]
    fn test_check_timestamp_for_liveness(#[strategy(any_valid_timestamp())] timestamp: i64) {
        check_timestamp_for_liveness(timestamp).unwrap();
    }

    #[proptest]
    fn test_check_timestamp_for_liveness_too_far_in_future(#[strategy(any_invalid_timestamp())] timestamp: i64) {
        assert!(check_timestamp_for_liveness(timestamp).is_err());
    }
}
