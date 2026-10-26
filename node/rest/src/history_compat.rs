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

//! History compatibility mode (`--history-compat-mode`).
//!
//! A note on the comments in this module and in its handlers in `routes.rs`: they are deliberately
//! far heavier than elsewhere in snarkOS, and they record rationale and the literal requests and
//! responses observed from the upstream API, against the usual guideline. This is a temporary
//! shim that proxies and rewrites another service's responses, it was written under time pressure,
//! and getting it right depends on facts about that service that are not visible in the code --
//! so the facts are written down next to the code that relies on them.
//!
//! # What this is, and why it exists
//!
//! snarkOS used to have a `history` cargo feature that recorded every mapping update in RocksDB
//! and served "the value of mapping `m` at key `k` as of block `h`" from that record, along with a
//! `history-staking-rewards` feature that recorded each block's staking rewards. Those tables were
//! unsound -- they never recorded deletions, so a key removed at height `h` was still served at
//! its last value for every height above `h`, and they mixed two incompatible height encodings --
//! and both features were removed (snarkVM #3418 and #3408 describe the defects).
//!
//! Some operators had exposed those routes to their own customers, and this mode keeps the routes
//! up for them: the node registers the same paths and produces the same response bodies, but the
//! data comes from the Provable historical staking API, which is backed by a fork of snarkOS that
//! writes a *full* JSON snapshot of each `credits.aleo` staking mapping at every block. Because a
//! snapshot is complete, a key that is absent from it was not in the mapping at that height,
//! which is exactly the case the tables got wrong. This mode is a temporary shim until a proper
//! archival mode exists; expect it to be removed then.
//!
//! # The upstream API
//!
//! One endpoint, `GET {base_url}/{network}/block/{height}/history/{name}`, where `name` is one of
//! `bonded`, `delegated`, `metadata`, `unbonding`, `withdraw` or `stakingrewards`. The five
//! mapping snapshots are a JSON array of `[key, value]` pairs, both Aleo plaintext strings exactly
//! as `Plaintext::to_string` / `Value::to_string` render them (so the node can hand them back
//! verbatim). Observed on mainnet:
//!
//! ```text
//! GET https://mainnet.historical-staking.provable.com/mainnet/block/1000000/history/unbonding
//! -> 200
//! [
//!   [
//!     "aleo1qgtvgvzkxqyh0jc7wxv3zjzjcd5epll38uv4wmmxjt8hexjluygqu4ukl2",
//!     "{\n  microcredits: 31712836548u64,\n  height: 883089u32\n}"
//!   ],
//!   ...
//! ]
//!
//! GET https://mainnet.historical-staking.provable.com/mainnet/block/1000000/history/bonded
//! -> 200
//! [
//!   [
//!     "aleo1qy4qufq03wcph05fdf5aj09ez67vcmmlrzqf0zza352qwaq43gyqt3wdf6",
//!     "{\n  validator: aleo1vfukg8ky2mhfprw63s0k0hl4vvd8573s6fkn8cv9y0ca6q27eq8qwdnxls,\n  microcredits: 141347021440u64\n}"
//!   ],
//!   ...
//! ]
//!
//! GET https://mainnet.historical-staking.provable.com/mainnet/block/1000000/history/metadata
//! -> 200
//! [
//!   ["aleo1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq3ljyzc", "16u32"],
//!   ["aleo1qgqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqanmpl0", "138u32"]
//! ]
//! ```
//!
//! `stakingrewards` is shaped differently: a JSON object keyed by staker address, whose value is
//! `[validator address, reward in microcredits]` for the reward paid at that block:
//!
//! ```text
//! GET https://mainnet.historical-staking.provable.com/mainnet/block/1000000/history/stakingrewards
//! -> 200
//! {
//!   "aleo1qy4qufq03wcph05fdf5aj09ez67vcmmlrzqf0zza352qwaq43gyqt3wdf6": [
//!     "aleo1vfukg8ky2mhfprw63s0k0hl4vvd8573s6fkn8cv9y0ca6q27eq8qwdnxls",
//!     6477
//!   ],
//!   ...
//! }
//! ```
//!
//! A block the upstream has no snapshot of is **not** a 404: the upstream's file read fails and
//! it answers 500 with a plain-text body naming the missing file. Observed for block 0 (the
//! genesis state; recording starts at block 1) and for a height far above the tip:
//!
//! ```text
//! GET https://mainnet.historical-staking.provable.com/mainnet/block/0/history/unbonding
//! -> 500
//! Could not load mapping 'unbonding' from block '0' — No such file or directory (os error 2)
//! ```
//!
//! An unknown mapping name is also a 500, with a body listing the valid names. The root path and
//! unfilled placeholders are an empty 404. There is one instance per network
//! (`https://{network}.historical-staking.provable.com`); mainnet and testnet resolve, canary does
//! not.
//!
//! Sizes at mainnet block 1,000,000: `unbonding` 437 B, `stakingrewards` 24 KB, `bonded` 31 KB,
//! answered in 0.1-0.3 s. The whole mapping is fetched to answer a single key, so snapshots are
//! cached by height and every concurrent request for one height shares one fetch.
//!
//! # What this mode serves
//!
//! - `GET /program/credits.aleo/mapping/{name}/{key}/history/{height}` and the `?keys=` batch form,
//!   for the five mappings above. The response body is the value string from the snapshot, or
//!   `null` if the key is absent -- the same body the removed feature produced. Any other program
//!   or mapping is a 404 saying what is supported, and so is a height the upstream has no
//!   snapshot of. This node's own height and sync state play no part; the upstream is the source
//!   of truth.
//! - `GET /staking/rewards/{address}/{height}`: `[validator, reward, new_stake]`, as the removed
//!   feature produced it, joined from `stakingrewards` and `bonded` at that height.
//! - `POST /program/{id}/view/{function}/{height}`: a 404 explaining that it cannot be served.
//!
//! The handlers are in `routes.rs`; this module is the upstream client and its cache.

use crate::{Mutex, RestError};

use anyhow::anyhow;
use lru::LruCache;
use serde::de::DeserializeOwned;
use std::{
    collections::{HashMap, hash_map::Entry},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;

/// The number of snapshots of each kind kept in memory. Clients tend to ask for several keys at
/// one height, and to walk consecutive heights.
const SNAPSHOT_CACHE_SIZE: usize = 256;

/// How long to wait for the upstream API.
const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(20);

/// The number of upstream requests in flight at once; further requests wait their turn.
const MAX_CONCURRENT_UPSTREAM_REQUESTS: usize = 8;

/// The largest snapshot accepted from the upstream. The largest mapping, `bonded`, is well under
/// 1 MiB at mainnet's current size.
const MAX_SNAPSHOT_BYTES: usize = 64 << 20;

/// The program whose mappings the upstream API records.
pub(crate) const SUPPORTED_PROGRAM: &str = "credits.aleo";

/// A `credits.aleo` mapping the upstream API records a snapshot of at every block.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SnapshotMapping {
    Bonded,
    Delegated,
    Metadata,
    Unbonding,
    Withdraw,
}

impl SnapshotMapping {
    /// Every mapping the upstream records, which is what the `/history/` routes can serve.
    pub(crate) const ALL: [Self; 5] = [Self::Bonded, Self::Delegated, Self::Metadata, Self::Unbonding, Self::Withdraw];

    /// The mapping's name in `credits.aleo`, which is also its name in the upstream API's path.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Bonded => "bonded",
            Self::Delegated => "delegated",
            Self::Metadata => "metadata",
            Self::Unbonding => "unbonding",
            Self::Withdraw => "withdraw",
        }
    }

    /// Returns the snapshot for a `credits.aleo` mapping name, if the upstream records it.
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|mapping| mapping.name() == name)
    }
}

/// A snapshot of one mapping at one block height: each key and value is an Aleo plaintext string,
/// as `Plaintext::to_string` and `Value::to_string` render them. A key is looked up by its
/// canonical string form, so a parsed-and-reprinted key normalizes whatever spelling the client
/// used.
pub(crate) type MappingSnapshot = HashMap<String, String>;

/// The rewards paid out at one block height: staker address to `(validator address, reward)`.
pub(crate) type StakingRewardsSnapshot = HashMap<String, (String, u64)>;

/// Why a snapshot could not be fetched. Cloneable, so that one failed fetch can be reported to
/// every request that was waiting on it.
#[derive(Clone, Debug)]
enum FetchError {
    /// The upstream has no snapshot for the requested height.
    NotFound(String),
    /// The upstream could not be reached, or answered with something other than a snapshot.
    Unavailable(String),
}

impl From<FetchError> for RestError {
    fn from(error: FetchError) -> Self {
        match error {
            FetchError::NotFound(message) => RestError::not_found(anyhow!("{message}")),
            FetchError::Unavailable(message) => {
                RestError::service_unavailable(anyhow!("The historical data upstream is unavailable: {message}"))
            }
        }
    }
}

/// One fetch in progress, which every request for its key awaits. The outcome is set exactly
/// once, by the request that performed the fetch.
type Flight<T> = tokio::sync::Mutex<Option<Result<Arc<T>, FetchError>>>;

/// A cache of snapshots with single-flight fetching: concurrent requests for one key share one
/// upstream request, and its outcome -- success or failure.
struct SnapshotCache<K, T> {
    snapshots: Mutex<LruCache<K, Arc<T>>>,
    in_flight: Mutex<HashMap<K, Arc<Flight<T>>>>,
}

/// Removes a flight from the cache's in-flight map when the request performing its fetch is done
/// with it -- also when that request is cancelled, so that a later request starts a fresh fetch.
struct FlightGuard<'a, K: Copy + Eq + std::hash::Hash, T> {
    cache: &'a SnapshotCache<K, T>,
    key: K,
    flight: Arc<Flight<T>>,
}

impl<K: Copy + Eq + std::hash::Hash, T> Drop for FlightGuard<'_, K, T> {
    fn drop(&mut self) {
        if let Entry::Occupied(entry) = self.cache.in_flight.lock().entry(self.key)
            && Arc::ptr_eq(entry.get(), &self.flight)
        {
            entry.remove();
        }
    }
}

impl<K: Copy + Eq + std::hash::Hash, T> SnapshotCache<K, T> {
    fn new() -> Self {
        Self {
            snapshots: Mutex::new(LruCache::new(NonZeroUsize::new(SNAPSHOT_CACHE_SIZE).expect("nonzero"))),
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the cached snapshot for `key`, or fetches, caches and returns it.
    async fn get_or_fetch<F, Fut>(&self, key: K, fetch: F) -> Result<Arc<T>, FetchError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, FetchError>>,
    {
        if let Some(snapshot) = self.snapshots.lock().get(&key) {
            return Ok(snapshot.clone());
        }
        let flight = self.in_flight.lock().entry(key).or_default().clone();
        let mut outcome = flight.lock().await;
        if let Some(result) = &*outcome {
            return result.clone();
        }
        // This request performs the fetch; the others for this key are waiting on `outcome`.
        let _guard = FlightGuard { cache: self, key, flight: flight.clone() };
        let result = fetch().await.map(Arc::new);
        if let Ok(snapshot) = &result {
            self.snapshots.lock().put(key, snapshot.clone());
        }
        *outcome = Some(result.clone());
        result
    }
}

/// The client for the upstream API, with a small cache of recent snapshots.
pub(crate) struct HistoryCompat {
    /// The HTTP client.
    client: reqwest::Client,
    /// The upstream base URL, without a trailing slash.
    base_url: String,
    /// The network path segment, e.g. `mainnet`.
    network: &'static str,
    /// Bounds the upstream requests in flight.
    upstream_requests: Semaphore,
    /// Recently fetched mapping snapshots.
    mappings: SnapshotCache<(u32, SnapshotMapping), MappingSnapshot>,
    /// Recently fetched staking rewards.
    staking_rewards: SnapshotCache<u32, StakingRewardsSnapshot>,
}

impl HistoryCompat {
    /// Initializes a client for the upstream at `base_url`, serving the given network.
    pub(crate) fn new(base_url: &str, network: &'static str) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(UPSTREAM_TIMEOUT)
            .user_agent(concat!("snarkos/", env!("SNARKOS_VERSION")))
            .build()?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            network,
            upstream_requests: Semaphore::new(MAX_CONCURRENT_UPSTREAM_REQUESTS),
            mappings: SnapshotCache::new(),
            staking_rewards: SnapshotCache::new(),
        })
    }

    /// Returns the URL of a snapshot.
    fn snapshot_url(&self, height: u32, name: &str) -> String {
        format!("{}/{}/block/{height}/history/{name}", self.base_url, self.network)
    }

    /// Returns the snapshot of a mapping at a block height.
    pub(crate) async fn mapping(
        &self,
        height: u32,
        mapping: SnapshotMapping,
    ) -> Result<Arc<MappingSnapshot>, RestError> {
        let snapshot = self
            .mappings
            .get_or_fetch((height, mapping), || async move {
                let entries: Vec<(String, String)> = self.fetch(height, mapping.name()).await?;
                Ok(entries.into_iter().collect())
            })
            .await?;
        Ok(snapshot)
    }

    /// Returns the staking rewards paid out at a block height.
    pub(crate) async fn staking_rewards(&self, height: u32) -> Result<Arc<StakingRewardsSnapshot>, RestError> {
        Ok(self.staking_rewards.get_or_fetch(height, || self.fetch(height, "stakingrewards")).await?)
    }

    /// Fetches and parses a snapshot from the upstream.
    async fn fetch<T: DeserializeOwned>(&self, height: u32, name: &str) -> Result<T, FetchError> {
        let _permit = self.upstream_requests.acquire().await.expect("the semaphore is never closed");
        let url = self.snapshot_url(height, name);
        let unavailable = |message: String| FetchError::Unavailable(format!("{url}: {message}"));
        let response = self.client.get(&url).send().await.map_err(|error| unavailable(error.to_string()))?;
        let status = response.status();
        if response.content_length().is_some_and(|length| length > MAX_SNAPSHOT_BYTES as u64) {
            return Err(unavailable(format!("the snapshot exceeds {MAX_SNAPSHOT_BYTES} bytes")));
        }
        let body = response.bytes().await.map_err(|error| unavailable(error.to_string()))?;
        if body.len() > MAX_SNAPSHOT_BYTES {
            return Err(unavailable(format!("the snapshot exceeds {MAX_SNAPSHOT_BYTES} bytes")));
        }
        // The upstream answers a height it has no snapshot of with a 500 whose body reports the
        // missing file, e.g. "Could not load mapping 'unbonding' from block '0' — No such file or
        // directory (os error 2)".
        let missing = status == reqwest::StatusCode::NOT_FOUND
            || (status.is_server_error() && body.starts_with(b"Could not load mapping"));
        if missing {
            return Err(FetchError::NotFound(format!("No snapshot of '{name}' is recorded for block {height}")));
        }
        if !status.is_success() {
            return Err(unavailable(format!("answered {status}")));
        }
        serde_json::from_slice(&body).map_err(|error| unavailable(error.to_string()))
    }
}

/// Responses of the upstream API at block 1,000,000 of mainnet, trimmed to a few entries.
#[cfg(test)]
pub(crate) mod fixtures {
    /// `GET /mainnet/block/1000000/history/unbonding`.
    pub(crate) const UNBONDING: &str = r#"[
  [
    "aleo1qgtvgvzkxqyh0jc7wxv3zjzjcd5epll38uv4wmmxjt8hexjluygqu4ukl2",
    "{\n  microcredits: 31712836548u64,\n  height: 883089u32\n}"
  ],
  [
    "aleo1sdjqhlcm9qltpu74ek0vxewt52zsdmn6swmpjn6m0tp9xf57dvpq740r8j",
    "{\n  microcredits: 10113730488u64,\n  height: 621255u32\n}"
  ]
]"#;

    /// `GET /mainnet/block/1000000/history/bonded`.
    pub(crate) const BONDED: &str = r#"[
  [
    "aleo1qy4qufq03wcph05fdf5aj09ez67vcmmlrzqf0zza352qwaq43gyqt3wdf6",
    "{\n  validator: aleo1vfukg8ky2mhfprw63s0k0hl4vvd8573s6fkn8cv9y0ca6q27eq8qwdnxls,\n  microcredits: 141347021440u64\n}"
  ]
]"#;

    /// `GET /mainnet/block/1000000/history/stakingrewards`.
    pub(crate) const STAKING_REWARDS: &str = r#"{
  "aleo1qy4qufq03wcph05fdf5aj09ez67vcmmlrzqf0zza352qwaq43gyqt3wdf6": [
    "aleo1vfukg8ky2mhfprw63s0k0hl4vvd8573s6fkn8cv9y0ca6q27eq8qwdnxls",
    6477
  ]
}"#;

    /// `GET /mainnet/block/1000000/history/metadata`.
    pub(crate) const METADATA: &str = r#"[
  ["aleo1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq3ljyzc", "16u32"],
  ["aleo1qgqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqanmpl0", "138u32"]
]"#;

    /// The upstream's answer for a block it has no snapshot of: a 500 with this body.
    pub(crate) const MISSING: &str =
        "Could not load mapping 'withdraw' from block '0' — No such file or directory (os error 2)";
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    #[test]
    fn test_mapping_names() {
        for mapping in SnapshotMapping::ALL {
            assert_eq!(SnapshotMapping::from_name(mapping.name()), Some(mapping));
        }
        assert!(SnapshotMapping::from_name("committee").is_none());
        assert!(SnapshotMapping::from_name("account").is_none());
        // The rewards are not addressable as a mapping.
        assert!(SnapshotMapping::from_name("stakingrewards").is_none());
    }

    #[test]
    fn test_snapshot_url() {
        let compat = HistoryCompat::new("https://example.com/", "mainnet").unwrap();
        assert_eq!(
            compat.snapshot_url(1_000_000, "stakingrewards"),
            "https://example.com/mainnet/block/1000000/history/stakingrewards"
        );
        assert_eq!(compat.snapshot_url(7, "unbonding"), "https://example.com/mainnet/block/7/history/unbonding");
    }

    /// A fetch that reports when it starts and completes only when released, so that a test can
    /// hold it in flight while other requests for its key arrive.
    struct HeldFetch {
        started: Notify,
        release: Notify,
        fetches: AtomicUsize,
    }

    impl HeldFetch {
        fn new() -> Arc<Self> {
            Arc::new(Self { started: Notify::new(), release: Notify::new(), fetches: AtomicUsize::new(0) })
        }

        async fn fetch(self: Arc<Self>, result: Result<u32, FetchError>) -> Result<u32, FetchError> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            self.release.notified().await;
            result
        }
    }

    #[tokio::test]
    async fn test_concurrent_requests_share_one_fetch() {
        let cache = Arc::new(SnapshotCache::<u32, u32>::new());
        let held = HeldFetch::new();

        // The first request starts a fetch and is held in flight.
        let first = tokio::spawn({
            let (cache, held) = (cache.clone(), held.clone());
            async move { cache.get_or_fetch(1, || held.fetch(Ok(10))).await }
        });
        held.started.notified().await;
        // Two more requests for the key arrive while it is in flight, and one for another key.
        let second = tokio::spawn({
            let (cache, held) = (cache.clone(), held.clone());
            async move { cache.get_or_fetch(1, || held.fetch(Ok(10))).await }
        });
        let third = tokio::spawn({
            let (cache, held) = (cache.clone(), held.clone());
            async move { cache.get_or_fetch(1, || held.fetch(Ok(10))).await }
        });
        let other = cache.get_or_fetch(2, || async { Ok(20) }).await.unwrap();
        assert_eq!(*other, 20);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(held.fetches.load(Ordering::SeqCst), 1);

        held.release.notify_one();
        let (first, second, third) =
            (first.await.unwrap().unwrap(), second.await.unwrap().unwrap(), third.await.unwrap().unwrap());
        assert!(Arc::ptr_eq(&first, &second) && Arc::ptr_eq(&second, &third));
        assert_eq!(*first, 10);
        assert_eq!(held.fetches.load(Ordering::SeqCst), 1);
        // Later requests hit the cache, and no flight lingers.
        assert_eq!(*cache.get_or_fetch(1, || async { Ok(99) }).await.unwrap(), 10);
        assert!(cache.in_flight.lock().is_empty());
    }

    #[tokio::test]
    async fn test_concurrent_requests_share_one_failure() {
        let cache = Arc::new(SnapshotCache::<u32, u32>::new());
        let held = HeldFetch::new();

        let first = tokio::spawn({
            let (cache, held) = (cache.clone(), held.clone());
            async move { cache.get_or_fetch(1, || held.fetch(Err(FetchError::NotFound("missing".into())))).await }
        });
        held.started.notified().await;
        let second = tokio::spawn({
            let (cache, held) = (cache.clone(), held.clone());
            async move { cache.get_or_fetch(1, || held.fetch(Ok(10))).await }
        });
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        held.release.notify_one();
        // Both requests fail with the one fetch's error, without a second fetch.
        assert!(matches!(first.await.unwrap(), Err(FetchError::NotFound(_))));
        assert!(matches!(second.await.unwrap(), Err(FetchError::NotFound(_))));
        assert_eq!(held.fetches.load(Ordering::SeqCst), 1);
        // A failure is not cached: the next request fetches again.
        assert_eq!(*cache.get_or_fetch(1, || async { Ok(30) }).await.unwrap(), 30);
        assert!(cache.in_flight.lock().is_empty());
    }

    #[tokio::test]
    async fn test_cancelled_fetch_leaves_no_flight_behind() {
        let cache = Arc::new(SnapshotCache::<u32, u32>::new());
        let held = HeldFetch::new();
        let request = tokio::spawn({
            let (cache, held) = (cache.clone(), held.clone());
            async move { cache.get_or_fetch(1, || held.fetch(Ok(10))).await }
        });
        held.started.notified().await;
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        assert!(cache.in_flight.lock().is_empty());
        // The next request fetches afresh.
        assert_eq!(*cache.get_or_fetch(1, || async { Ok(11) }).await.unwrap(), 11);
    }
}

/// A sweep of the *live* upstream API, checking every assumption the compat routes make about its
/// responses across many heights: that every key and value is a canonical plaintext string, that
/// each mapping's values have the shape the handlers rely on, that the rewards snapshot joins with
/// `bonded` the way `get_staking_reward_compat` assumes, and that a height above the tip is
/// reported as missing. The unit tests above only see four fixtures from one block; this is what
/// catches the upstream changing shape, or a height range that is shaped differently.
///
/// It is ignored by default because it needs the network and makes ~1,000 requests. Run it with:
///
/// ```text
/// cargo test -p snarkos-node-rest live_upstream_sweep -- --ignored --nocapture
/// ```
///
/// Environment: `HISTORY_API_URL` (default: the mainnet instance), `HISTORY_SWEEP_RANDOM` (number
/// of random heights, default 100), `HISTORY_SWEEP_SEED` (default 0), `HISTORY_SWEEP_TIP` (skip
/// asking the explorer for the tip).
#[cfg(test)]
mod live_upstream_sweep {
    use super::*;
    use snarkvm::prelude::{Identifier, Literal, MainnetV0, Plaintext, Value};

    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use std::{collections::HashSet, str::FromStr};

    type N = MainnetV0;

    /// The two keys of `credits.aleo/metadata`: the number of validators, and of delegators.
    const METADATA_KEYS: [&str; 2] = [
        "aleo1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq3ljyzc",
        "aleo1qgqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqanmpl0",
    ];

    /// The upstream is a few blocks behind the tip (observed: 5); heights nearer than this are
    /// not expected to exist yet.
    const TIP_LAG: u32 = 10;

    fn env_or<T: FromStr>(name: &str, default: T) -> T {
        std::env::var(name).ok().and_then(|value| value.parse().ok()).unwrap_or(default)
    }

    /// Checks that a snapshot's keys and values are canonical plaintext strings -- the lookup by
    /// `Plaintext::to_string` depends on it -- and returns each value parsed.
    fn check_canonical(
        height: u32,
        mapping: SnapshotMapping,
        snapshot: &MappingSnapshot,
        failures: &mut Vec<String>,
    ) -> Vec<(String, Plaintext<N>)> {
        let mut parsed = Vec::with_capacity(snapshot.len());
        for (key, value) in snapshot {
            match Plaintext::<N>::from_str(key) {
                Ok(plaintext) if plaintext.to_string() == *key => {}
                Ok(plaintext) => failures.push(format!(
                    "{height}/{}: key {key:?} is not canonical (reprints as {:?})",
                    mapping.name(),
                    plaintext.to_string()
                )),
                Err(error) => {
                    failures.push(format!("{height}/{}: key {key:?} does not parse: {error}", mapping.name()))
                }
            }
            match Value::<N>::from_str(value) {
                Ok(Value::Plaintext(plaintext)) if plaintext.to_string() == *value => {
                    parsed.push((key.clone(), plaintext))
                }
                Ok(other) => failures.push(format!(
                    "{height}/{}: value for {key} is not a canonical plaintext: {:?}",
                    mapping.name(),
                    other.to_string()
                )),
                Err(error) => {
                    failures.push(format!("{height}/{}: value for {key} does not parse: {error}", mapping.name()))
                }
            }
        }
        parsed
    }

    /// Returns the `u64` member of a struct plaintext, if it is one.
    fn u64_member(plaintext: &Plaintext<N>, member: &str) -> Option<u64> {
        match plaintext.find(&[Identifier::from_str(member).unwrap()]).ok()? {
            Plaintext::Literal(Literal::U64(value), _) => Some(*value),
            _ => None,
        }
    }

    fn address_member(plaintext: &Plaintext<N>, member: &str) -> Option<String> {
        match plaintext.find(&[Identifier::from_str(member).unwrap()]).ok()? {
            Plaintext::Literal(Literal::Address(address), _) => Some(address.to_string()),
            _ => None,
        }
    }

    /// Checks every snapshot at one height. With `check_join`, also fetches `bonded` at the
    /// previous height to check the reward arithmetic.
    async fn check_height(compat: &HistoryCompat, height: u32, check_join: bool) -> Vec<String> {
        let mut failures = Vec::new();
        let mut fetch = |name: &str, result: Result<Arc<MappingSnapshot>, RestError>| match result {
            Ok(snapshot) => Some(snapshot),
            Err(error) => {
                failures.push(format!("{height}/{name}: {error}"));
                None
            }
        };
        let bonded = fetch("bonded", compat.mapping(height, SnapshotMapping::Bonded).await);
        let delegated = fetch("delegated", compat.mapping(height, SnapshotMapping::Delegated).await);
        let metadata = fetch("metadata", compat.mapping(height, SnapshotMapping::Metadata).await);
        let unbonding = fetch("unbonding", compat.mapping(height, SnapshotMapping::Unbonding).await);
        let withdraw = fetch("withdraw", compat.mapping(height, SnapshotMapping::Withdraw).await);
        let rewards = match compat.staking_rewards(height).await {
            Ok(rewards) => Some(rewards),
            Err(error) => {
                failures.push(format!("{height}/stakingrewards: {error}"));
                None
            }
        };

        // bonded: { validator: address, microcredits: u64 }, and every validator it names is a
        // key of `delegated`.
        let mut bonded_parsed = HashMap::new();
        if let Some(bonded) = &bonded {
            for (key, plaintext) in check_canonical(height, SnapshotMapping::Bonded, bonded, &mut failures) {
                match (address_member(&plaintext, "validator"), u64_member(&plaintext, "microcredits")) {
                    (Some(validator), Some(microcredits)) => {
                        if let Some(delegated) = &delegated
                            && !delegated.contains_key(&validator)
                        {
                            failures.push(format!(
                                "{height}/bonded: {key} is bonded to {validator}, which `delegated` lacks"
                            ));
                        }
                        bonded_parsed.insert(key, (validator, microcredits));
                    }
                    _ => failures.push(format!("{height}/bonded: unexpected value shape for {key}: {plaintext}")),
                }
            }
            if bonded.is_empty() {
                failures.push(format!("{height}/bonded: empty"));
            }
        }
        // delegated: u64.
        if let Some(delegated) = &delegated {
            for (key, plaintext) in check_canonical(height, SnapshotMapping::Delegated, delegated, &mut failures) {
                if !matches!(plaintext, Plaintext::Literal(Literal::U64(_), _)) {
                    failures.push(format!("{height}/delegated: unexpected value shape for {key}: {plaintext}"));
                }
            }
        }
        // metadata: exactly the two known keys, u32 values.
        if let Some(metadata) = &metadata {
            let keys: HashSet<&str> = metadata.keys().map(String::as_str).collect();
            if keys != HashSet::from(METADATA_KEYS) {
                failures.push(format!("{height}/metadata: unexpected keys {keys:?}"));
            }
            for (key, plaintext) in check_canonical(height, SnapshotMapping::Metadata, metadata, &mut failures) {
                if !matches!(plaintext, Plaintext::Literal(Literal::U32(_), _)) {
                    failures.push(format!("{height}/metadata: unexpected value shape for {key}: {plaintext}"));
                }
            }
        }
        // unbonding: { microcredits: u64, height: u32 }, with a withdrawal address on file.
        if let Some(unbonding) = &unbonding {
            for (key, plaintext) in check_canonical(height, SnapshotMapping::Unbonding, unbonding, &mut failures) {
                let height_member = plaintext.find(&[Identifier::<N>::from_str("height").unwrap()]).ok();
                if u64_member(&plaintext, "microcredits").is_none()
                    || !matches!(height_member, Some(Plaintext::Literal(Literal::U32(_), _)))
                {
                    failures.push(format!("{height}/unbonding: unexpected value shape for {key}: {plaintext}"));
                }
                if let Some(withdraw) = &withdraw
                    && !withdraw.contains_key(&key)
                {
                    failures.push(format!("{height}/unbonding: {key} is unbonding but `withdraw` lacks it"));
                }
            }
        }
        // withdraw: address.
        if let Some(withdraw) = &withdraw {
            for (key, plaintext) in check_canonical(height, SnapshotMapping::Withdraw, withdraw, &mut failures) {
                if !matches!(plaintext, Plaintext::Literal(Literal::Address(_), _)) {
                    failures.push(format!("{height}/withdraw: unexpected value shape for {key}: {plaintext}"));
                }
            }
        }
        // stakingrewards: one row per bonded staker, naming the validator it is bonded to, and
        // (with the previous height) bonded@h == bonded@(h-1) + reward for stakers who did not
        // bond or unbond in block h -- which is all but a handful at most.
        if let (Some(rewards), Some(_)) = (&rewards, &bonded) {
            let reward_stakers: HashSet<&String> = rewards.keys().collect();
            let bonded_stakers: HashSet<&String> = bonded_parsed.keys().collect();
            if reward_stakers != bonded_stakers {
                let only_rewards = reward_stakers.difference(&bonded_stakers).count();
                let only_bonded = bonded_stakers.difference(&reward_stakers).count();
                failures.push(format!(
                    "{height}/stakingrewards: stakers differ from bonded ({only_rewards} only in rewards, {only_bonded} only in bonded)"
                ));
            }
            for (staker, (validator, _)) in rewards.iter() {
                if let Some((bonded_validator, _)) = bonded_parsed.get(staker)
                    && bonded_validator != validator
                {
                    failures.push(format!(
                        "{height}/stakingrewards: {staker} rewarded via {validator} but bonded to {bonded_validator}"
                    ));
                }
            }
            // The upstream has no snapshot of block 0 (the genesis state), so block 1 cannot be
            // joined with its predecessor.
            if check_join && height > 1 {
                match compat.mapping(height - 1, SnapshotMapping::Bonded).await {
                    Ok(previous) => {
                        let mut mismatches = Vec::new();
                        for (staker, (_, reward)) in rewards.iter() {
                            let Some((_, now)) = bonded_parsed.get(staker) else { continue };
                            let Some(before) = previous
                                .get(staker)
                                .and_then(|value| u64_member(&Plaintext::<N>::from_str(value).ok()?, "microcredits"))
                            else {
                                continue;
                            };
                            if before + reward != *now {
                                mismatches.push(format!("{staker}: {before} + {reward} != {now}"));
                            }
                        }
                        // A staker who bonded or unbonded in this block legitimately breaks the
                        // identity; more than a few is a shape change.
                        if mismatches.len() > rewards.len() / 100 + 3 {
                            failures.push(format!(
                                "{height}/stakingrewards: bonded@h != bonded@(h-1) + reward for {} of {} stakers, e.g. {:?}",
                                mismatches.len(),
                                rewards.len(),
                                &mismatches[..mismatches.len().min(3)]
                            ));
                        }
                    }
                    Err(error) => failures.push(format!("{}/bonded (for the join check): {error}", height - 1)),
                }
            }
        }
        failures
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "makes ~1,000 requests to the live upstream; run explicitly"]
    async fn sweep() {
        let base_url = std::env::var("HISTORY_API_URL")
            .unwrap_or_else(|_| "https://mainnet.historical-staking.provable.com".to_string());
        let compat = Arc::new(HistoryCompat::new(&base_url, "mainnet").unwrap());
        let tip: u32 = match std::env::var("HISTORY_SWEEP_TIP") {
            Ok(tip) => tip.parse().unwrap(),
            Err(_) => reqwest::get("https://api.explorer.provable.com/v1/mainnet/latest/height")
                .await
                .unwrap()
                .text()
                .await
                .unwrap()
                .trim()
                .parse()
                .unwrap(),
        };
        let seed: u64 = env_or("HISTORY_SWEEP_SEED", 0);
        let random: usize = env_or("HISTORY_SWEEP_RANDOM", 100);
        println!("upstream {base_url}, tip {tip}, {random} random heights, seed {seed}");

        // Deliberate heights: the earliest blocks, the old height-encoding boundaries, every
        // consensus version boundary and its neighbours, and the blocks just behind the tip.
        let mut deliberate: Vec<u32> = vec![1, 2, 3, 10, 100, 1_000, 65_535, 65_536, 65_537];
        for (_, boundary) in snarkvm::console::network::MAINNET_V0_CONSENSUS_VERSION_HEIGHTS {
            if boundary > 0 && boundary <= tip - TIP_LAG {
                deliberate.extend([boundary - 1, boundary, boundary + 1]);
            }
        }
        deliberate.extend([tip - TIP_LAG, tip - TIP_LAG - 1, tip - 50, tip - 1_000]);
        let mut rng = StdRng::seed_from_u64(seed);
        let random_heights: Vec<u32> = (0..random).map(|_| rng.random_range(1..tip - TIP_LAG)).collect();

        // Every deliberate height and a fifth of the random ones get the (h-1) join check.
        let mut tasks = tokio::task::JoinSet::new();
        for (index, height) in deliberate.iter().chain(&random_heights).copied().enumerate() {
            let compat = compat.clone();
            let check_join = index < deliberate.len() || index % 5 == 0;
            tasks.spawn(async move { (height, check_height(&compat, height, check_join).await) });
        }
        let mut failures = Vec::new();
        let mut checked = 0;
        while let Some(result) = tasks.join_next().await {
            let (height, height_failures) = result.unwrap();
            checked += 1;
            if height_failures.is_empty() {
                println!("ok   {height}");
            } else {
                println!("FAIL {height}: {}", height_failures.join("; "));
            }
            failures.extend(height_failures);
        }

        // A height the upstream cannot have yet is reported as missing, not as an outage.
        match compat.mapping(tip + 1_000_000, SnapshotMapping::Unbonding).await {
            Err(RestError::NotFound(_)) => println!("ok   {} (missing, as expected)", tip + 1_000_000),
            other => failures.push(format!("{}: expected not-found, got {other:?}", tip + 1_000_000)),
        }

        println!("checked {checked} heights, {} failures", failures.len());
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }
}
