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

mod cache;
pub use cache::Cache;

use snarkvm::{
    ledger::narwhal::Data,
    prelude::{FromBytes, ToBytes},
};

use anyhow::Result;

/// Returns the object in `data`, deserializing a buffer on the blocking thread pool.
pub async fn deserialize_data<T: FromBytes + ToBytes + Send + 'static>(data: Data<T>) -> Result<T> {
    match data {
        Data::Object(object) => Ok(object),
        Data::Buffer(bytes) => tokio::task::spawn_blocking(move || T::from_bytes_le(&bytes)).await?,
    }
}
