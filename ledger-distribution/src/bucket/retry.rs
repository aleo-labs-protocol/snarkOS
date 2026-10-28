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

use std::time::Duration;

use backoff::{ExponentialBackoff, future::retry};
use futures::Future;
use tracing::*;

use rusoto_core::*;

pub async fn handle_dispatch_error<'a, T, E, F>(func: impl Fn() -> F + 'a) -> RusotoResult<T, E>
where
    F: Future<Output = RusotoResult<T, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    retry(default_backoff(), || async { func().await.map_err(from_rusoto_err::<E>) }).await
}

fn from_rusoto_err<E>(err: RusotoError<E>) -> backoff::Error<RusotoError<E>> {
    use backoff::Error;

    match err {
        RusotoError::HttpDispatch(_) => {
            debug!("Retrying S3 dispatch error");
            Error::Transient { err, retry_after: None }
        }
        RusotoError::Unknown(ref res) if res.status.is_server_error() => {
            debug!("Retrying S3 server error: {}", res.body_as_str());
            Error::Transient { err, retry_after: None }
        }
        _ => Error::Permanent(err),
    }
}

fn default_backoff() -> ExponentialBackoff {
    ExponentialBackoff {
        max_interval: Duration::from_secs(10),
        max_elapsed_time: Some(Duration::from_secs(45)),
        ..Default::default()
    }
}
