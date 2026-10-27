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

use super::*;
use snarkos_node_network::PeerPoolHandling;
use snarkos_node_router::messages::UnconfirmedSolution;
use snarkos_node_sync::BftSyncMode;
use snarkvm::{
    ledger::puzzle::Solution,
    prelude::{
        Address,
        ConsensusVersion,
        Identifier,
        LimitedWriter,
        Literal,
        Plaintext,
        Program,
        ToBytes,
        Value,
        block::Transaction,
    },
    synthesizer::{
        program::{FinalizeGlobalState, StackTrait},
        vm::{History, MappingName},
    },
};

use axum::{Json, extract::rejection::JsonRejection};

use aleo_std::aleo_ledger_dir;
use anyhow::{Context, anyhow};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_with::skip_serializing_none;
use std::{collections::HashMap, fs, str::FromStr};

#[cfg(not(feature = "serial"))]
use rayon::prelude::*;
use version::VersionInfo;

const MAX_KEYS_PER_REQUEST: usize = 1 << 7;
type HistoricalMappingKey<N> = (ProgramID<N>, Identifier<N>, Plaintext<N>, u32);
type HistoricalMappingRoute<N> = (ProgramID<N>, Identifier<N>, u32);
type ViewFunctionRoute<N> = (ProgramID<N>, Identifier<N>, u32);

fn parse_historical_mapping_keys<N: Network>(keys: &[String]) -> Result<Vec<Plaintext<N>>, RestError> {
    // Retrieve the number of keys.
    let num_keys = keys.len();
    // Return an error if no keys are provided.
    if num_keys == 0 {
        return Err(RestError::unprocessable_entity(anyhow!("No keys provided")));
    }
    // Return an error if the number of keys exceeds the maximum allowed.
    if num_keys > MAX_KEYS_PER_REQUEST {
        return Err(RestError::unprocessable_entity(anyhow!(
            "Too many keys provided (max: {MAX_KEYS_PER_REQUEST}, got: {num_keys})"
        )));
    }

    // Deserialize the keys from the query.
    keys.iter()
        .enumerate()
        .map(|(index, key)| {
            key.parse::<Plaintext<N>>().map_err(|err| {
                RestError::unprocessable_entity(err.context(format!("Invalid key at index {index}: {key}")))
            })
        })
        .collect::<Result<Vec<_>, _>>()
}

/// Parses a list of strings into a `Vec<Value<N>>` for use as view function inputs.
fn parse_view_inputs<N: Network>(inputs: &[String]) -> Result<Vec<Value<N>>, RestError> {
    inputs
        .iter()
        .enumerate()
        .map(|(index, input)| {
            input.parse::<Value<N>>().map_err(|err| {
                RestError::unprocessable_entity(err.context(format!("Invalid input at index {index}: {input}")))
            })
        })
        .collect::<Result<Vec<_>, _>>()
}

fn map_missing_resource_error(err: anyhow::Error) -> RestError {
    /// The markers that identify an absent resource rather than a fault.
    const MISSING_RESOURCE_MARKERS: [&str; 3] = ["Missing", "does not exist in storage", "Failed to find"];

    // Inspect the whole chain rather than just the outermost message. Callers attach context with
    // `with_context`, which is what `to_string` then reports, so a marker added by the ledger ends
    // up buried one level down. `/block/{height}` was the visible case: `get_block` wraps the
    // ledger's "Missing block hash for block {h}" in "Failed to get a block's hash", so a height
    // the node did not have was reported as a 500 instead of a 404.
    let is_missing_resource = err.chain().any(|cause| {
        let message = cause.to_string();
        MISSING_RESOURCE_MARKERS.iter().any(|marker| message.contains(marker))
    });

    if is_missing_resource { RestError::not_found(err) } else { RestError::from(err) }
}

/// Deserialize a CSV string into a vector of strings.
fn de_csv<'de, D>(de: D) -> std::result::Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(de)?;
    Ok(if s.trim().is_empty() { Vec::new() } else { s.split(',').map(|x| x.trim().to_string()).collect() })
}

/// The query object for the routes serving a range of block heights.
#[derive(Copy, Clone, Deserialize, Serialize)]
pub(crate) struct BlockRange {
    /// The starting block height (inclusive).
    start: u32,
    /// The ending block height (exclusive).
    end: u32,
    /// Whether to clamp a range that reaches past the tip, rather than fail the request.
    allow_partial: Option<bool>,
}

/// The maximum number of blocks that `get_blocks` serves in one request.
const MAX_BLOCK_RANGE: u32 = 50;

/// The maximum number of block hashes that `get_block_hashes` serves in one request.
const MAX_BLOCK_HASH_RANGE: u32 = 5_000;

/// The maximum number of block headers that `get_block_headers` serves in one request.
const MAX_BLOCK_HEADER_RANGE: u32 = 320;

/// The maximum number of state roots that `get_block_state_roots` serves in one request.
const MAX_STATE_ROOT_RANGE: u32 = 5_000;

/// The maximum number of blocks whose transactions `get_block_transactions_range` serves in one
/// request.
///
/// Unlike a hash, a state root or a header, a block's transactions have no fixed size, so this
/// cannot be sized to fit a response inside one block the way the others are. What bounds it is
/// the largest response the node already produces over the same data: a block's bytes are
/// dominated by its authority, so this many blocks' transactions weigh less than the
/// `MAX_BLOCK_RANGE` whole blocks `get_blocks` serves, and the route asks nothing of the node it
/// could not already be asked for.
///
/// That is a bound on bytes rather than on blocks, and it does not survive an arbitrary raise:
/// mainnet has stretches where transactions are most of a block, so setting this to
/// `MAX_BLOCK_HEADER_RANGE` would let the route return more in one response than `get_blocks`
/// can. `the_transactions_maximum_stays_within_a_get_blocks_response` holds this against measured
/// figures and rejects a maximum nobody has measured.
const MAX_BLOCK_TRANSACTIONS_RANGE: u32 = 160;

/// Validates a block range against the given maximum, and returns `(start, end)`.
///
/// With `allow_partial`, the range is then clamped to the heights up to `latest_height`, so a
/// caller following the tip can ask for the next `N` heights and receive as many as exist: element
/// `i` of the response is height `start + i`, and a `start` past the tip yields an empty range. The
/// maximum applies to the range as requested. A height missing at or below the tip is left in the
/// range for the lookup to report.
///
/// Each route serving a range picks its own maximum, sized so that the largest response it can
/// produce stays on the order of a single block. A block hash and a header are several orders of
/// magnitude smaller than the block they belong to, so applying the `get_blocks` maximum to them
/// would bound those responses far below what the node already serves in one request.
///
/// `item` names what the route serves, so that a caller who exceeds the maximum is told the limit
/// in the unit it applies to. On the default and `/v1` prefixes this text is the only diagnostic
/// the caller receives, since `v1_error_middleware` replaces the status code.
fn check_block_range(
    block_range: BlockRange,
    max_block_range: u32,
    item: &str,
    latest_height: u32,
) -> Result<(u32, u32), RestError> {
    let (start_height, end_height) = (block_range.start, block_range.end);

    // Ensure the end height is greater than the start height.
    if start_height > end_height {
        return Err(RestError::bad_request(anyhow!("Invalid block range")));
    }

    // Ensure the block range is bounded.
    if end_height - start_height > max_block_range {
        return Err(RestError::bad_request(anyhow!(
            "Cannot request more than {max_block_range} {item} per call (requested {})",
            end_height - start_height
        )));
    }

    if !block_range.allow_partial.unwrap_or(false) {
        return Ok((start_height, end_height));
    }

    // Clamp the range to the tip.
    let end_height = end_height.min(latest_height.saturating_add(1));
    let start_height = start_height.min(end_height);

    Ok((start_height, end_height))
}

#[derive(Deserialize, Serialize)]
pub(crate) struct BackupPath {
    path: std::path::PathBuf,
}

/// The query object for `get_mapping_value` and `get_mapping_values`.
#[derive(Copy, Clone, Deserialize, Serialize)]
pub(crate) struct Metadata {
    metadata: Option<bool>,
    all: Option<bool>,
}

/// The query object for `transaction_broadcast`.
#[derive(Copy, Clone, Deserialize, Serialize)]
pub(crate) struct CheckTransaction {
    check_transaction: Option<bool>,
}

/// The query object for `solution_broadcast`.
#[derive(Copy, Clone, Deserialize, Serialize)]
pub(crate) struct CheckSolution {
    check_solution: Option<bool>,
}

/// The query object for `get_state_paths_for_commitments`.
#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct Commitments {
    #[serde(deserialize_with = "de_csv")]
    commitments: Vec<String>,
}

/// Resolves a `/history/` route's program and mapping to the upstream snapshot that serves it in
/// history compatibility mode.
///
/// The upstream only records five `credits.aleo` mappings. The removed `history` feature served
/// every mapping of every program, so a client of it may well ask for `credits.aleo/committee`
/// or `credits.aleo/account` or another program entirely; those are answered with a 404 that
/// names what is available rather than being forwarded (the upstream would answer 500).
fn history_compat_mapping<N: Network>(
    program_id: &ProgramID<N>,
    mapping_name: &Identifier<N>,
) -> Result<SnapshotMapping, RestError> {
    let mapping = match program_id.to_string() == SUPPORTED_PROGRAM {
        true => SnapshotMapping::from_name(&mapping_name.to_string()),
        false => None,
    };
    mapping.ok_or_else(|| {
        RestError::not_found(anyhow!(
            "History compatibility mode serves only the mappings {:?} of '{SUPPORTED_PROGRAM}'; \
             '{program_id}/{mapping_name}' is not available",
            SnapshotMapping::ALL.map(SnapshotMapping::name),
        ))
    })
}

/// The query object for `get_history_batch_compat`.
#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct HistoricalKeys {
    #[serde(deserialize_with = "de_csv")]
    keys: Vec<String>,
}

/// The return value for a transaction metadata query.
#[skip_serializing_none]
#[derive(Serialize)]
pub(crate) struct TransactionWithMetadata<T: Serialize, N: Network> {
    transaction: T,
    block_hash: Option<N::BlockHash>,
    block_height: Option<u32>,
}

/// The return value for a `sync_status` query.
#[skip_serializing_none]
#[derive(Copy, Clone, Serialize)]
struct SyncStatus<'a> {
    /// Is this node fully synced with the network?
    is_synced: bool,
    /// The block height of this node.
    ledger_height: u32,
    /// Which way are we sync'ing (either "cdn" or "p2p")
    sync_mode: &'a str,
    /// Validators can either sync in "fast" mode, by fetching blocks similar to how clients do,
    /// or the can sync certificates in "dag" mode.
    bft_sync_mode: Option<&'a str>,
    /// The block height of the CDN (if connected to a CDN).
    cdn_height: Option<u32>,
    /// The greatest known block height of a peer.
    /// None, if no peers are connected yet.
    p2p_height: Option<u32>,
    /// The number of outstanding p2p sync requests.
    outstanding_block_requests: usize,
    /// The current sync speed in blocks per second.
    sync_speed_bps: f64,
}

impl<N: Network, C: ConsensusStorage<N>, R: Routing<N>> Rest<N, C, R> {
    /// GET /<network>/version
    pub(crate) async fn get_version() -> ErasedJson {
        ErasedJson::new(VersionInfo::get::<N>())
    }

    /// Get /<network>/consensus_version
    pub(crate) async fn get_consensus_version(State(rest): State<Self>) -> Result<ErasedJson, RestError> {
        Ok(ErasedJson::new(N::CONSENSUS_VERSION(rest.ledger.latest_height())? as u16))
    }

    /// GET /<network>/block/height/latest
    pub(crate) async fn get_block_height_latest(State(rest): State<Self>) -> ErasedJson {
        ErasedJson::new(rest.ledger.latest_height())
    }

    /// GET /<network>/block/hash/latest
    pub(crate) async fn get_block_hash_latest(State(rest): State<Self>) -> ErasedJson {
        ErasedJson::new(rest.ledger.latest_hash())
    }

    /// GET /<network>/block/latest
    pub(crate) async fn get_block_latest(State(rest): State<Self>) -> ErasedJson {
        let block = rest.ledger.latest_block();
        let hash = block.hash();
        // When present, this is 3x faster than serializing the block from the ledger.
        rest.block_cache.lock().get_or_insert(hash, || ErasedJson::new(block)).clone()
    }

    /// GET /<network>/block/{height}
    /// GET /<network>/block/{blockHash}
    pub(crate) async fn get_block(
        State(rest): State<Self>,
        Path(height_or_hash): Path<String>,
    ) -> Result<ErasedJson, RestError> {
        // Manually parse the height or the height of the hash, axum doesn't support different types
        // for the same path param.
        let hash = if let Ok(height) = height_or_hash.parse::<u32>() {
            rest.ledger
                .get_hash(height)
                .with_context(|| "Failed to get a block's hash")
                .map_err(map_missing_resource_error)?
        } else if let Ok(hash) = height_or_hash.parse::<N::BlockHash>() {
            hash
        } else {
            return Err(RestError::bad_request(anyhow!("invalid input: neither a block height nor a block hash")));
        };

        // Attempt to find a serialized block in the cache.
        if let Some(json_block) = rest.block_cache.lock().get(&hash) {
            return Ok(json_block.clone());
        }

        // Retrieve the block from the database.
        let json_block = match tokio::task::spawn_blocking(move || match rest.ledger.try_get_block_by_hash(&hash) {
            Ok(Some(block)) => Some(ErasedJson::new(block)),
            Ok(None) => None,
            Err(e) => {
                error!("Couldn't find a block: {e}");
                None
            }
        })
        .await
        {
            Ok(Some(block)) => Ok(block),
            Ok(None) => Err(RestError::not_found(anyhow!("Couldn't find block {height_or_hash}"))),
            Err(e) => Err(RestError::internal_server_error(anyhow!("tokio error: {e}"))),
        }?;

        Ok(json_block)
    }

    /// GET /<network>/blocks?start={start_height}&end={end_height}
    /// GET /<network>/blocks?start={start_height}&end={end_height}&allow_partial=true
    ///
    /// `start` is inclusive and `end` is exclusive. A height the node does not have is a 404. With
    /// `allow_partial=true`, a range that reaches past the tip is clamped to it instead, so the
    /// array is shorter than the range, and empty when `start` is past the tip.
    pub(crate) async fn get_blocks(
        State(rest): State<Self>,
        Query(block_range): Query<BlockRange>,
    ) -> Result<ErasedJson, RestError> {
        let (start_height, end_height) =
            check_block_range(block_range, MAX_BLOCK_RANGE, "blocks", rest.ledger.latest_height())?;

        // Prepare a closure for the blocking work.
        let get_json_blocks = move || -> Result<ErasedJson, RestError> {
            let blocks = cfg_into_iter!(start_height..end_height)
                .map(|height| rest.ledger.get_block(height).map_err(map_missing_resource_error))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(ErasedJson::new(blocks))
        };

        // Fetch the blocks from ledger and serialize to json.
        match tokio::task::spawn_blocking(get_json_blocks).await {
            Ok(json) => json,
            Err(err) => {
                let err: anyhow::Error = err.into();

                Err(RestError::internal_server_error(
                    err.context(format!("Failed to get blocks '{start_height}..{end_height}'")),
                ))
            }
        }
    }

    /// GET /<network>/blocks/hashes?start={start_height}&end={end_height}
    ///
    /// `start` is inclusive and `end` is exclusive, as in `get_blocks`, so `start == end` returns
    /// an empty array rather than the hash at that height.
    ///
    /// A height the node does not have is a 404, and the whole request fails rather than returning
    /// a short array. Note that this is only visible on `/v2`: on the default and `/v1` prefixes
    /// the v1 error middleware replaces the status with a 500, so a caller that needs to tell a
    /// not-yet-synced height apart from a fault has to use `/v2`, or pass `allow_partial=true` to
    /// clamp the range to the tip as in `get_blocks`.
    pub(crate) async fn get_block_hashes(
        State(rest): State<Self>,
        Query(block_range): Query<BlockRange>,
    ) -> Result<ErasedJson, RestError> {
        let (start_height, end_height) =
            check_block_range(block_range, MAX_BLOCK_HASH_RANGE, "block hashes", rest.ledger.latest_height())?;

        // Prepare a closure for the blocking work.
        //
        // Unlike `get_blocks`, this stays sequential: each height is a single point lookup in the
        // block ID map, which is cheaper than the work of handing it to another thread, and this
        // range is large enough that saturating the rayon pool would contend with consensus.
        let get_json_hashes = move || -> Result<ErasedJson, RestError> {
            let hashes = (start_height..end_height)
                .map(|height| rest.ledger.get_hash(height).map_err(map_missing_resource_error))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(ErasedJson::new(hashes))
        };

        // Fetch the block hashes from the ledger and serialize to json.
        match tokio::task::spawn_blocking(get_json_hashes).await {
            Ok(json) => json,
            Err(err) => {
                let err: anyhow::Error = err.into();

                Err(RestError::internal_server_error(
                    err.context(format!("Failed to get block hashes '{start_height}..{end_height}'")),
                ))
            }
        }
    }

    /// GET /<network>/blocks/headers?start={start_height}&end={end_height}
    ///
    /// `start` is inclusive and `end` is exclusive, as in `get_blocks`.
    ///
    /// A height the node does not have is a 404, and the whole request fails rather than returning
    /// a short array. Note that this is only visible on `/v2`: on the default and `/v1` prefixes
    /// the v1 error middleware replaces the status with a 500, so a caller that needs to tell a
    /// not-yet-synced height apart from a fault has to use `/v2`, or pass `allow_partial=true` to
    /// clamp the range to the tip as in `get_blocks`.
    pub(crate) async fn get_block_headers(
        State(rest): State<Self>,
        Query(block_range): Query<BlockRange>,
    ) -> Result<ErasedJson, RestError> {
        let (start_height, end_height) =
            check_block_range(block_range, MAX_BLOCK_HEADER_RANGE, "block headers", rest.ledger.latest_height())?;

        // Prepare a closure for the blocking work. Each height is two point lookups: the block ID
        // map, then the header map. See `get_block_hashes` for why this is sequential.
        let get_json_headers = move || -> Result<ErasedJson, RestError> {
            let headers = (start_height..end_height)
                .map(|height| rest.ledger.get_header(height).map_err(map_missing_resource_error))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(ErasedJson::new(headers))
        };

        // Fetch the block headers from the ledger and serialize to json.
        match tokio::task::spawn_blocking(get_json_headers).await {
            Ok(json) => json,
            Err(err) => {
                let err: anyhow::Error = err.into();

                Err(RestError::internal_server_error(
                    err.context(format!("Failed to get block headers '{start_height}..{end_height}'")),
                ))
            }
        }
    }

    /// GET /<network>/blocks/transactions?start={start_height}&end={end_height}
    ///
    /// `start` is inclusive and `end` is exclusive, as in `get_blocks`. Each element is the
    /// confirmed transactions of one block, in block order, so a block that confirmed none is an
    /// empty array rather than an omission.
    ///
    /// This is the range form of `/block/{height}/transactions`. It carries the part of a block a
    /// consumer reconstructing transaction trees needs -- the confirmed transaction ids and their
    /// contents, including the program a deployment carries -- without `authority`, which is 97% of
    /// mainnet's block bytes in aggregate and which no transaction tree touches.
    ///
    /// A height the node does not have is a 404, and the whole request fails rather than returning
    /// a short array. Note that this is only visible on `/v2`: on the default and `/v1` prefixes
    /// the v1 error middleware replaces the status with a 500, so a caller that needs to tell a
    /// not-yet-synced height apart from a fault has to use `/v2`, or pass `allow_partial=true` to
    /// clamp the range to the tip as in `get_blocks`.
    pub(crate) async fn get_block_transactions_range(
        State(rest): State<Self>,
        Query(block_range): Query<BlockRange>,
    ) -> Result<ErasedJson, RestError> {
        let (start_height, end_height) = check_block_range(
            block_range,
            MAX_BLOCK_TRANSACTIONS_RANGE,
            "blocks' transactions",
            rest.ledger.latest_height(),
        )?;

        // Prepare a closure for the blocking work. Each height is two point lookups, the block ID
        // map then the transactions map, and deserializing the transactions themselves. That last
        // part is unbounded per block, unlike the other range routes, so this keeps `get_blocks`'
        // maximum and its rayon fan-out rather than the sequential loop the cheaper routes use.
        let get_json_transactions = move || -> Result<ErasedJson, RestError> {
            let transactions = cfg_into_iter!(start_height..end_height)
                .map(|height| rest.ledger.get_transactions(height).map_err(map_missing_resource_error))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(ErasedJson::new(transactions))
        };

        // Fetch the transactions from the ledger and serialize to json.
        match tokio::task::spawn_blocking(get_json_transactions).await {
            Ok(json) => json,
            Err(err) => {
                let err: anyhow::Error = err.into();

                Err(RestError::internal_server_error(
                    err.context(format!("Failed to get transactions for blocks '{start_height}..{end_height}'")),
                ))
            }
        }
    }

    /// GET /<network>/blocks/stateRoots?start={start_height}&end={end_height}
    ///
    /// `start` is inclusive and `end` is exclusive, as in `get_blocks`.
    ///
    /// Each entry is the state root *after* the block at that height, the same root the singular
    /// `/stateRoot/{height}` route returns. Note that this is not the root a header carries: a
    /// header holds `previous_state_root`, so the root for height `h` appears in the header of
    /// height `h + 1`.
    ///
    /// A height with no stored root is deliberately a 404 here, whereas the singular route serves
    /// `null` for it. Returning `null` inside an array would make a gap indistinguishable from a
    /// root that is genuinely absent, so this matches the other range routes and fails the whole
    /// request instead. As above, that 404 is only visible on `/v2`; the default and `/v1`
    /// prefixes replace it with a 500. `allow_partial=true` clamps the range to the tip as in
    /// `get_blocks`.
    pub(crate) async fn get_block_state_roots(
        State(rest): State<Self>,
        Query(block_range): Query<BlockRange>,
    ) -> Result<ErasedJson, RestError> {
        let (start_height, end_height) =
            check_block_range(block_range, MAX_STATE_ROOT_RANGE, "state roots", rest.ledger.latest_height())?;

        // Prepare a closure for the blocking work. The state root map is keyed by height directly,
        // so each height is a single point lookup. See `get_block_hashes` for why this is
        // sequential.
        let get_json_state_roots = move || -> Result<ErasedJson, RestError> {
            let state_roots = (start_height..end_height)
                .map(|height| {
                    // Unlike the other getters, this one reports a missing height as `None` rather
                    // than as an error, so translate it to stay consistent with the other routes.
                    rest.ledger
                        .get_state_root(height)
                        .map_err(RestError::from)?
                        .ok_or_else(|| RestError::not_found(anyhow!("Missing state root for block {height}")))
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(ErasedJson::new(state_roots))
        };

        // Fetch the state roots from the ledger and serialize to json.
        match tokio::task::spawn_blocking(get_json_state_roots).await {
            Ok(json) => json,
            Err(err) => {
                let err: anyhow::Error = err.into();

                Err(RestError::internal_server_error(
                    err.context(format!("Failed to get state roots '{start_height}..{end_height}'")),
                ))
            }
        }
    }

    /// GET /<network>/sync/status
    pub(crate) async fn get_sync_status(State(rest): State<Self>) -> Result<ErasedJson, RestError> {
        // Get the CDN height (if we are syncing from a CDN)
        let (cdn_sync, cdn_height) = if let Some(cdn_sync) = &rest.cdn_sync {
            let done = cdn_sync.is_done();

            // Do not show CDN height if we are already done syncing from the CDN.
            let cdn_height = if done { None } else { Some(cdn_sync.get_cdn_height().await?) };

            // Report CDN sync until it is finished.
            (!done, cdn_height)
        } else {
            (false, None)
        };

        // Generate a string representing the current sync mode.
        let sync_mode = if cdn_sync { "cdn" } else { "p2p" };

        let bft_sync_mode = rest.block_sync.get_bft_sync_mode().map(|mode| match mode {
            BftSyncMode::Fast => "fast",
            BftSyncMode::Dag => "dag",
        });

        Ok(ErasedJson::new(SyncStatus {
            sync_mode,
            bft_sync_mode,
            cdn_height,
            is_synced: !cdn_sync && rest.routing.is_block_synced(),
            ledger_height: rest.ledger.latest_height(),
            p2p_height: rest.block_sync.greatest_peer_block_height(),
            outstanding_block_requests: rest.block_sync.num_outstanding_block_requests(),
            sync_speed_bps: rest.block_sync.get_sync_speed(),
        }))
    }

    /// GET /<network>/sync/peers
    pub(crate) async fn get_sync_peers(State(rest): State<Self>) -> Result<ErasedJson, RestError> {
        let peers: HashMap<String, u32> =
            rest.block_sync.get_peer_heights().into_iter().map(|(addr, height)| (addr.to_string(), height)).collect();
        Ok(ErasedJson::new(peers))
    }

    /// GET /<network>/sync/requests
    pub(crate) async fn get_sync_requests_summary(State(rest): State<Self>) -> Result<ErasedJson, RestError> {
        let summary = rest.block_sync.get_block_requests_summary();
        Ok(ErasedJson::new(summary))
    }

    /// GET /<network>/sync/requests/list
    pub(crate) async fn get_sync_requests_list(State(rest): State<Self>) -> Result<ErasedJson, RestError> {
        let requests = rest.block_sync.get_block_requests_info();
        Ok(ErasedJson::new(requests))
    }

    /// GET /<network>/height/{blockHash}
    pub(crate) async fn get_height(
        State(rest): State<Self>,
        Path(hash): Path<N::BlockHash>,
    ) -> Result<ErasedJson, RestError> {
        Ok(ErasedJson::new(rest.ledger.get_height(&hash).map_err(map_missing_resource_error)?))
    }

    /// GET /<network>/block/{height}/header
    pub(crate) async fn get_block_header(
        State(rest): State<Self>,
        Path(height): Path<u32>,
    ) -> Result<ErasedJson, RestError> {
        Ok(ErasedJson::new(rest.ledger.get_header(height)?))
    }

    /// GET /<network>/block/{height}/transactions
    pub(crate) async fn get_block_transactions(
        State(rest): State<Self>,
        Path(height): Path<u32>,
    ) -> Result<ErasedJson, RestError> {
        Ok(ErasedJson::new(rest.ledger.get_transactions(height)?))
    }

    /// GET /<network>/transaction/{transactionID}
    /// GET /<network>/transaction/{transactionID}?metadata={true}
    pub(crate) async fn get_transaction(
        State(rest): State<Self>,
        Path(tx_id): Path<N::TransactionID>,
        metadata: Query<Metadata>,
    ) -> Result<ErasedJson, RestError> {
        // Ledger returns a generic anyhow::Error, so checking the message is the only way to parse it.
        let transaction = rest.ledger.get_transaction(tx_id).map_err(|err| {
            if err.to_string().contains("Missing") { RestError::not_found(err) } else { RestError::from(err) }
        })?;
        // Check if metadata is requested and return the transaction with metadata if so.
        if metadata.metadata.unwrap_or(false) {
            // Get the block hash and height for the transaction, if it exists.
            let block_hash = rest.ledger.find_block_hash(&tx_id).ok().flatten();
            let block_height = block_hash.and_then(|hash| rest.ledger.get_height(&hash).ok());
            Ok(ErasedJson::new(TransactionWithMetadata::<_, N> { transaction, block_hash, block_height }))
        } else {
            Ok(ErasedJson::new(transaction))
        }
    }

    /// GET /<network>/transaction/confirmed/{transactionID}
    /// GET /<network>/transaction/confirmed/{transactionID}?metadata={true}
    pub(crate) async fn get_confirmed_transaction(
        State(rest): State<Self>,
        Path(tx_id): Path<N::TransactionID>,
        metadata: Query<Metadata>,
    ) -> Result<ErasedJson, RestError> {
        // Ledger returns a generic anyhow::Error, so checking the message is the only way to parse it.
        let transaction = rest.ledger.get_confirmed_transaction(tx_id).map_err(|err| {
            if err.to_string().contains("Missing") { RestError::not_found(err) } else { RestError::from(err) }
        })?;
        // Check if metadata is requested and return the transaction with metadata if so.
        if metadata.metadata.unwrap_or(false) {
            // Get the block hash and height for the confirmed transaction.
            let block_hash = rest
                .ledger
                .find_block_hash(&tx_id)?
                .ok_or_else(|| anyhow!("Block hash not found for transaction {tx_id}"))?;
            let block_height = rest.ledger.get_height(&block_hash)?;
            Ok(ErasedJson::new(TransactionWithMetadata::<_, N> {
                transaction,
                block_hash: Some(block_hash),
                block_height: Some(block_height),
            }))
        } else {
            Ok(ErasedJson::new(transaction))
        }
    }

    /// GET /<network>/transaction/unconfirmed/{transactionID}
    pub(crate) async fn get_unconfirmed_transaction(
        State(rest): State<Self>,
        Path(tx_id): Path<N::TransactionID>,
    ) -> Result<ErasedJson, RestError> {
        // Ledger returns a generic anyhow::Error, so checking the message is the only way to parse it.
        Ok(ErasedJson::new(rest.ledger.get_unconfirmed_transaction(&tx_id).map_err(|err| {
            if err.to_string().contains("Missing") { RestError::not_found(err) } else { RestError::from(err) }
        })?))
    }

    /// GET /<network>/transaction/rejected/{transactionID}/reason
    pub(crate) async fn get_transaction_rejection_reason(
        State(rest): State<Self>,
        Path(tx_id): Path<N::TransactionID>,
    ) -> Result<ErasedJson, RestError> {
        let rejection_reason = Self::lookup_transaction_rejection_reason(&rest, &tx_id)?;
        match rejection_reason {
            Some(reason) => Ok(ErasedJson::new(reason)),
            None => Err(RestError::not_found(anyhow!("Rejection reason not found for transaction {tx_id}"))),
        }
    }

    /// Looks up the rejection reason for a transaction ID.
    ///
    /// Rejection reasons are stored under the confirmed (fee) transaction ID. Callers may provide
    /// either the unconfirmed transaction ID or the confirmed rejected transaction ID.
    fn lookup_transaction_rejection_reason(
        rest: &Self,
        tx_id: &N::TransactionID,
    ) -> Result<Option<snarkvm::prelude::block::transactions::RejectedReason<N>>, RestError> {
        let store = rest.ledger.vm().finalize_store();

        if let Some(reason) = store.get_rejected_reason(tx_id)? {
            return Ok(Some(reason));
        }

        // Fall back to the unconfirmed transaction ID.
        if let Some(unconfirmed) = rest.ledger.try_get_unconfirmed_transaction(tx_id)?
            && let Some(reason) = store.get_rejected_reason(&*unconfirmed.id())?
        {
            return Ok(Some(reason));
        }

        // Fall back to the confirmed (fee) transaction ID.
        if let Some(confirmed) = rest.ledger.try_get_confirmed_transaction(tx_id)?
            && let Some(reason) = store.get_rejected_reason(&*confirmed.id())?
        {
            return Ok(Some(reason));
        }

        Ok(None)
    }

    /// GET /<network>/memoryPool/transmissions
    pub(crate) async fn get_memory_pool_transmissions(State(rest): State<Self>) -> Result<ErasedJson, RestError> {
        match rest.consensus {
            Some(consensus) => Ok(ErasedJson::new(consensus.unconfirmed_transmissions().collect::<IndexMap<_, _>>())),
            None => Err(RestError::service_unavailable(anyhow!("Route isn't available for this node type"))),
        }
    }

    /// GET /<network>/memoryPool/solutions
    pub(crate) async fn get_memory_pool_solutions(State(rest): State<Self>) -> Result<ErasedJson, RestError> {
        match rest.consensus {
            Some(consensus) => Ok(ErasedJson::new(consensus.unconfirmed_solutions().collect::<IndexMap<_, _>>())),
            None => Err(RestError::service_unavailable(anyhow!("Route isn't available for this node type"))),
        }
    }

    /// GET /<network>/memoryPool/transactions
    pub(crate) async fn get_memory_pool_transactions(State(rest): State<Self>) -> Result<ErasedJson, RestError> {
        match rest.consensus {
            Some(consensus) => Ok(ErasedJson::new(consensus.unconfirmed_transactions().collect::<IndexMap<_, _>>())),
            None => Err(RestError::service_unavailable(anyhow!("Route isn't available for this node type"))),
        }
    }

    /// GET /<network>/program/{programID}
    /// GET /<network>/program/{programID}?metadata={true}
    pub(crate) async fn get_program(
        State(rest): State<Self>,
        Path(id): Path<ProgramID<N>>,
        metadata: Query<Metadata>,
    ) -> Result<ErasedJson, RestError> {
        // Get the program from the ledger.
        let program = rest
            .ledger
            .get_program(id)
            .with_context(|| format!("Failed to find program `{id}`"))
            .map_err(map_missing_resource_error)?;
        // Check if metadata is requested and return the program with metadata if so.
        if metadata.metadata.unwrap_or(false) {
            // Get the edition of the program.
            let edition = rest.ledger.get_latest_edition_for_program(&id)?;
            return rest.return_program_with_metadata(program, edition);
        }
        // Return the program without metadata.
        Ok(ErasedJson::new(program))
    }

    /// GET /<network>/program/{programID}/{edition}
    /// GET /<network>/program/{programID}/{edition}?metadata={true}
    pub(crate) async fn get_program_for_edition(
        State(rest): State<Self>,
        Path((id, edition)): Path<(ProgramID<N>, u16)>,
        metadata: Query<Metadata>,
    ) -> Result<ErasedJson, RestError> {
        // Get the program from the ledger.
        match rest
            .ledger
            .try_get_program_for_edition(&id, edition)
            .with_context(|| format!("Failed get program `{id}` for edition {edition}"))?
        {
            Some(program) => {
                // Check if metadata is requested and return the program with metadata if so.
                if metadata.metadata.unwrap_or(false) {
                    rest.return_program_with_metadata(program, edition)
                } else {
                    Ok(ErasedJson::new(program))
                }
            }
            None => Err(RestError::not_found(anyhow!("No program `{id}` exists for edition {edition}"))),
        }
    }

    /// A helper function to return the program and its metadata.
    /// This function is used in the `get_program` and `get_program_for_edition` functions.
    fn return_program_with_metadata(&self, program: Program<N>, edition: u16) -> Result<ErasedJson, RestError> {
        let id = program.id();
        // Get the transaction ID associated with the program and edition.
        let tx_id = self.ledger.find_latest_transaction_id_from_program_id_and_edition(id, edition)?;
        // Get the optional program owner associated with the program.
        // Note: The owner is only available after `ConsensusVersion::V9`.
        let program_owner = match &tx_id {
            Some(tid) => self
                .ledger
                .vm()
                .block_store()
                .transaction_store()
                .deployment_store()
                .get_deployment(tid)?
                .and_then(|deployment| deployment.program_owner()),
            None => None,
        };
        // Get the amendment count for this program and edition.
        let amendment_count =
            self.ledger.vm().block_store().transaction_store().get_amendment_count(id, edition)?.unwrap_or(0);
        Ok(ErasedJson::new(json!({
            "program": program,
            "edition": edition,
            "transaction_id": tx_id,
            "program_owner": program_owner,
            "amendment_count": amendment_count,
        })))
    }

    /// GET /<network>/program/{programID}/latest_edition
    pub(crate) async fn get_latest_program_edition(
        State(rest): State<Self>,
        Path(id): Path<ProgramID<N>>,
    ) -> Result<ErasedJson, RestError> {
        Ok(ErasedJson::new(rest.ledger.get_latest_edition_for_program(&id)?))
    }

    /// GET /<network>/program/{programID}/mappings
    pub(crate) async fn get_mapping_names(
        State(rest): State<Self>,
        Path(id): Path<ProgramID<N>>,
    ) -> Result<ErasedJson, RestError> {
        Ok(ErasedJson::new(rest.ledger.vm().finalize_store().get_mapping_names_confirmed(&id)?))
    }

    /// GET /<network>/program/{programID}/mapping/{mappingName}/{mappingKey}
    /// GET /<network>/program/{programID}/mapping/{mappingName}/{mappingKey}?metadata={true}
    pub(crate) async fn get_mapping_value(
        State(rest): State<Self>,
        Path((id, name, key)): Path<(ProgramID<N>, Identifier<N>, Plaintext<N>)>,
        metadata: Query<Metadata>,
    ) -> Result<ErasedJson, RestError> {
        // Retrieve the mapping value.
        let mapping_value = rest.ledger.vm().finalize_store().get_value_confirmed(id, name, &key)?;

        // Check if metadata is requested and return the value with metadata if so.
        if metadata.metadata.unwrap_or(false) {
            return Ok(ErasedJson::new(json!({
                "data": mapping_value,
                "height": rest.ledger.latest_height(),
            })));
        }

        // Return the value without metadata.
        Ok(ErasedJson::new(mapping_value))
    }

    /// GET /<network>/program/{programID}/mapping/{mappingName}?all={true}&metadata={true}
    pub(crate) async fn get_mapping_values(
        State(rest): State<Self>,
        Path((id, name)): Path<(ProgramID<N>, Identifier<N>)>,
        metadata: Query<Metadata>,
    ) -> Result<ErasedJson, RestError> {
        // Return an error if the `all` query parameter is not set to `true`.
        if metadata.all != Some(true) {
            return Err(RestError::bad_request(anyhow!(
                "Invalid query parameter. At this time, 'all=true' must be included"
            )));
        }

        // Retrieve the latest height.
        let height = rest.ledger.latest_height();

        // Retrieve all the mapping values from the mapping.
        match tokio::task::spawn_blocking(move || rest.ledger.vm().finalize_store().get_mapping_confirmed(id, name))
            .await
        {
            Ok(Ok(mapping_values)) => {
                // Check if metadata is requested and return the mapping with metadata if so.
                if metadata.metadata.unwrap_or(false) {
                    return Ok(ErasedJson::new(json!({
                        "data": mapping_values,
                        "height": height,
                    })));
                }

                // Return the full mapping without metadata.
                Ok(ErasedJson::new(mapping_values))
            }
            Ok(Err(err)) => Err(RestError::internal_server_error(err.context("Unable to read mapping"))),
            Err(err) => Err(RestError::internal_server_error(anyhow!("Tokio error: {err}"))),
        }
    }

    /// GET /<network>/program/{programID}/amendment_count
    pub(crate) async fn get_program_amendment_count(
        State(rest): State<Self>,
        Path(id): Path<ProgramID<N>>,
    ) -> Result<ErasedJson, RestError> {
        // Get the latest edition.
        let edition = rest.ledger.get_latest_edition_for_program(&id)?;
        // Get the amendment count for this program and edition.
        let amendment_count =
            rest.ledger.vm().block_store().transaction_store().get_amendment_count(&id, edition)?.unwrap_or(0);

        Ok(ErasedJson::new(json!({
            "program_id": id,
            "edition": edition,
            "amendment_count": amendment_count,
        })))
    }

    /// GET /<network>/program/{programID}/{edition}/amendment_count
    pub(crate) async fn get_program_amendment_count_for_edition(
        State(rest): State<Self>,
        Path((id, edition)): Path<(ProgramID<N>, u16)>,
    ) -> Result<ErasedJson, RestError> {
        // Get the amendment count for this program and edition.
        let amendment_count =
            rest.ledger.vm().block_store().transaction_store().get_amendment_count(&id, edition)?.unwrap_or(0);

        Ok(ErasedJson::new(json!({
            "program_id": id,
            "edition": edition,
            "amendment_count": amendment_count,
        })))
    }

    /// GET /<network>/statePath/{commitment}
    pub(crate) async fn get_state_path_for_commitment(
        State(rest): State<Self>,
        Path(commitment): Path<Field<N>>,
    ) -> Result<ErasedJson, RestError> {
        Ok(ErasedJson::new(rest.ledger.get_state_path_for_commitment(&commitment)?))
    }

    /// GET /<network>/statePaths?commitments=cm1,cm2,...
    pub(crate) async fn get_state_paths_for_commitments(
        State(rest): State<Self>,
        Query(commitments): Query<Commitments>,
    ) -> Result<ErasedJson, RestError> {
        // Retrieve the number of commitments.
        let num_commitments = commitments.commitments.len();
        // Return an error if no commitments are provided.
        if num_commitments == 0 {
            return Err(RestError::unprocessable_entity(anyhow!("No commitments provided")));
        }
        // Return an error if the number of commitments exceeds the maximum allowed.
        if num_commitments > N::MAX_INPUTS {
            return Err(RestError::unprocessable_entity(anyhow!(format!(
                "Too many commitments provided (max: {}, got: {num_commitments})",
                N::MAX_INPUTS
            ))));
        }

        // Parse the commitments and retrieve their state paths in a blocking task.
        let get_json_state_paths = move || -> Result<ErasedJson, RestError> {
            let commitments = commitments
                .commitments
                .iter()
                .map(|s| {
                    s.parse::<Field<N>>()
                        .map_err(|err| RestError::unprocessable_entity(err.context(format!("Invalid commitment: {s}"))))
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(ErasedJson::new(rest.ledger.get_state_paths_for_commitments(&commitments)?))
        };

        match tokio::task::spawn_blocking(get_json_state_paths).await {
            Ok(json) => json,
            Err(err) => {
                let err: anyhow::Error = err.into();
                Err(RestError::internal_server_error(err.context("Failed to get state paths for commitments")))
            }
        }
    }

    /// GET /<network>/stateRoot/latest
    pub(crate) async fn get_state_root_latest(State(rest): State<Self>) -> ErasedJson {
        ErasedJson::new(rest.ledger.latest_state_root())
    }

    /// GET /<network>/stateRoot/{height}
    pub(crate) async fn get_state_root(
        State(rest): State<Self>,
        Path(height): Path<u32>,
    ) -> Result<ErasedJson, RestError> {
        Ok(ErasedJson::new(rest.ledger.get_state_root(height)?))
    }

    /// GET /<network>/committee/latest
    pub(crate) async fn get_committee_latest(State(rest): State<Self>) -> Result<ErasedJson, RestError> {
        Ok(ErasedJson::new(rest.ledger.latest_committee()?))
    }

    /// GET /<network>/committee/{height}
    pub(crate) async fn get_committee(
        State(rest): State<Self>,
        Path(height): Path<u32>,
    ) -> Result<ErasedJson, RestError> {
        Ok(ErasedJson::new(rest.ledger.get_committee(height)?))
    }

    /// GET /<network>/delegators/{validator}
    pub(crate) async fn get_delegators_for_validator(
        State(rest): State<Self>,
        Path(validator): Path<Address<N>>,
    ) -> Result<ErasedJson, RestError> {
        // Do not process the request if the node is too far behind to avoid sending outdated data.
        if !rest.routing.is_within_sync_leniency() {
            return Err(RestError::service_unavailable(anyhow!("Unable to request delegators (node is syncing)")));
        }

        // Return the delegators for the given validator.
        match tokio::task::spawn_blocking(move || rest.ledger.get_delegators_for_validator(&validator)).await {
            Ok(Ok(delegators)) => Ok(ErasedJson::new(delegators)),
            Ok(Err(err)) => Err(RestError::internal_server_error(err.context("Unable to request delegators"))),
            Err(err) => Err(RestError::internal_server_error(anyhow!(err).context("Tokio error"))),
        }
    }

    /// GET /<network>/peers/count (alias: /connections/p2p/count)
    pub(crate) async fn get_peers_count(State(rest): State<Self>) -> ErasedJson {
        ErasedJson::new(rest.routing.router().number_of_connected_peers())
    }

    /// GET /<network>/peers/all (alias: /connections/p2p/all)
    pub(crate) async fn get_peers_all(State(rest): State<Self>) -> ErasedJson {
        ErasedJson::new(rest.routing.router().connected_peers())
    }

    /// GET /<network>/peers/all/metrics (alias: /connections/p2p/all/metrics)
    pub(crate) async fn get_peers_all_metrics(State(rest): State<Self>) -> ErasedJson {
        ErasedJson::new(rest.routing.router().connected_metrics())
    }

    /// GET /<network>/connections/bft/count
    pub(crate) async fn get_bft_connections_count(State(rest): State<Self>) -> Result<ErasedJson, RestError> {
        match rest.consensus {
            Some(consensus) => Ok(ErasedJson::new(consensus.bft().primary().gateway().number_of_connected_peers())),
            None => Err(RestError::service_unavailable(anyhow!("Route isn't available for this node type"))),
        }
    }

    /// GET /<network>/connections/bft/all
    pub(crate) async fn get_bft_connections_all(State(rest): State<Self>) -> Result<ErasedJson, RestError> {
        match rest.consensus {
            Some(consensus) => Ok(ErasedJson::new(consensus.bft().primary().gateway().connected_peers())),
            None => Err(RestError::service_unavailable(anyhow!("Route isn't available for this node type"))),
        }
    }

    /// GET /<network>/node/address
    pub(crate) async fn get_node_address(State(rest): State<Self>) -> ErasedJson {
        ErasedJson::new(rest.routing.router().address())
    }

    /// GET /<network>/find/blockHash/{transactionID}
    pub(crate) async fn find_block_hash(
        State(rest): State<Self>,
        Path(tx_id): Path<N::TransactionID>,
    ) -> Result<ErasedJson, RestError> {
        Ok(ErasedJson::new(rest.ledger.find_block_hash(&tx_id)?))
    }

    /// GET /<network>/block/{height}/history/{mapping}
    ///
    /// The JSON file written by `snarkos start --history-json`.
    pub(crate) async fn get_block_history(
        State(rest): State<Self>,
        Path((height, mapping)): Path<(u32, String)>,
    ) -> Result<Response, RestError> {
        let mapping = mapping.parse::<MappingName>().map_err(RestError::bad_request)?;
        let storage_mode = rest.ledger.vm().finalize_store().storage_mode().clone();
        let json =
            tokio::task::spawn_blocking(move || History::new(N::ID, &storage_mode).load_mapping(height, mapping))
                .await
                .map_err(|err| RestError::internal_server_error(anyhow!("Tokio error: {err}")))?
                .map_err(|err| RestError::not_found(err.context(format!("No JSON history for block {height}"))))?;
        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(json))
            .map_err(|err| RestError::internal_server_error(anyhow!("Failed to build the history response: {err}")))
    }

    /// GET /<network>/find/blockHeight/{stateRoot}
    pub(crate) async fn find_block_height_from_state_root(
        State(rest): State<Self>,
        Path(state_root): Path<N::StateRoot>,
    ) -> Result<ErasedJson, RestError> {
        Ok(ErasedJson::new(rest.ledger.find_block_height_from_state_root(state_root)?))
    }

    /// GET /<network>/find/transactionID/deployment/{programID}
    pub(crate) async fn find_latest_transaction_id_from_program_id(
        State(rest): State<Self>,
        Path(program_id): Path<ProgramID<N>>,
    ) -> Result<ErasedJson, RestError> {
        Ok(ErasedJson::new(rest.ledger.find_latest_transaction_id_from_program_id(&program_id)?))
    }

    /// GET /<network>/find/transactionID/deployment/{programID}/{edition}
    pub(crate) async fn find_latest_transaction_id_from_program_id_and_edition(
        State(rest): State<Self>,
        Path((program_id, edition)): Path<(ProgramID<N>, u16)>,
    ) -> Result<ErasedJson, RestError> {
        Ok(ErasedJson::new(rest.ledger.find_latest_transaction_id_from_program_id_and_edition(&program_id, edition)?))
    }

    /// GET /<network>/find/transactionID/deployment/{programID}/{edition}/original
    /// Finds the transaction ID for the original deployment (not an amendment).
    pub(crate) async fn find_original_deployment_transaction_id(
        State(rest): State<Self>,
        Path((program_id, edition)): Path<(ProgramID<N>, u16)>,
    ) -> Result<ErasedJson, RestError> {
        Ok(ErasedJson::new(rest.ledger.find_original_transaction_id_from_program_id_and_edition(&program_id, edition)?))
    }

    /// GET /<network>/find/transactionID/deployment/{programID}/{edition}/{amendment}
    /// Finds the transaction ID for an amendment deployment at the specified index.
    pub(crate) async fn find_transaction_id_from_program_id_edition_and_amendment(
        State(rest): State<Self>,
        Path((program_id, edition, amendment)): Path<(ProgramID<N>, u16, u64)>,
    ) -> Result<ErasedJson, RestError> {
        Ok(ErasedJson::new(rest.ledger.find_transaction_id_from_program_id_edition_and_amendment(
            &program_id,
            edition,
            amendment,
        )?))
    }

    /// GET /<network>/find/transactionID/{transitionID}
    pub(crate) async fn find_transaction_id_from_transition_id(
        State(rest): State<Self>,
        Path(transition_id): Path<N::TransitionID>,
    ) -> Result<ErasedJson, RestError> {
        Ok(ErasedJson::new(rest.ledger.find_transaction_id_from_transition_id(&transition_id)?))
    }

    /// GET /<network>/find/transitionID/{inputOrOutputID}
    pub(crate) async fn find_transition_id(
        State(rest): State<Self>,
        Path(input_or_output_id): Path<Field<N>>,
    ) -> Result<ErasedJson, RestError> {
        Ok(ErasedJson::new(rest.ledger.find_transition_id(&input_or_output_id).map_err(map_missing_resource_error)?))
    }

    /// POST /<network>/transaction/broadcast
    /// POST /<network>/transaction/broadcast?check_transaction={true}
    ///
    /// Transaction Broadcast Flow
    ///
    /// /transaction/broadcast
    ///         |
    ///    +----+---------------------------+
    ///    |                               |
    ///    v                               v
    /// Without Query Params        With Query Param
    ///                                check_transaction=true
    ///    |                               |
    ///    +---------+                     +---------+
    ///    |         |                     |         |
    ///    v         v                     v         v
    /// Synced   Not Synced            Synced   Not Synced
    ///    |         |                     |         |
    ///    v         v                     v         v
    ///   200       200        check_transaction  check_transaction
    ///                           +---------+        +---------+
    ///                           |         |        |         |
    ///                           v         v        v         v
    ///                          200       422      203       503
    pub(crate) async fn transaction_broadcast(
        State(rest): State<Self>,
        check_transaction: Query<CheckTransaction>,
        json_result: Result<Json<Transaction<N>>, JsonRejection>,
    ) -> Result<impl axum::response::IntoResponse, RestError> {
        let Json(tx) = match json_result {
            Ok(json) => json,
            Err(JsonRejection::JsonDataError(err)) => {
                // For JsonDataError, return 422 to let transaction validation handle it
                return Err(RestError::unprocessable_entity(anyhow!("Invalid transaction data: {err}")));
            }
            Err(other_rejection) => return Err(other_rejection.into()),
        };

        // If the transaction exceeds the transaction size limit, return an error.
        // The buffer is initially roughly sized to hold a `transfer_public`,
        // most transactions will be smaller and this reduces unnecessary allocations.
        // TODO: Should this be a blocking task?
        let buffer = Vec::with_capacity(3000);
        if tx.write_le(LimitedWriter::new(buffer, N::LATEST_MAX_TRANSACTION_SIZE())).is_err() {
            return Err(RestError::bad_request(anyhow!("Transaction size exceeds the byte limit")));
        }

        // Prepare the unconfirmed transaction message.
        let tx_id = tx.id();
        let message = Message::UnconfirmedTransaction(UnconfirmedTransaction {
            transaction_id: tx_id,
            transaction: Data::Object(tx.clone()),
        });

        // Check if the node is within sync leniency.
        let is_within_sync_leniency = rest.routing.is_within_sync_leniency();

        // Determine if we need to check the transaction.
        let check_transaction = check_transaction.check_transaction.unwrap_or(false);

        let tx = if check_transaction {
            let verification_slot = if tx.is_execute() {
                rest.verification_slots.executions.acquire().await?
            } else {
                rest.verification_slots.deploys.acquire().await?
            };

            // Perform the check in a blocking task, handing the transaction back on success.
            let ledger = rest.ledger.clone();
            match tokio::task::spawn_blocking(move || {
                // Hold the slot until verification finishes, even if the client disconnects.
                let _verification_slot = verification_slot;
                #[cfg(feature = "metrics")]
                let timer = metrics::TransactionCheckTimer::start("rest", &tx);
                let result = ledger.check_transaction_basic(&tx, None, &mut rand::rng());
                #[cfg(feature = "metrics")]
                timer.finish(&result);
                result.map(|()| tx)
            })
            .await
            {
                Ok(Ok(tx)) => tx,
                Ok(Err(err)) => {
                    return match is_within_sync_leniency {
                        // The transaction failed to verify.
                        true => Err(RestError::unprocessable_entity(err.context("Invalid transaction"))),
                        // The node is out of sync and may not be able to properly validate the transaction.
                        false => Err(RestError::service_unavailable(
                            err.context("Unable to validate transaction (node is syncing)"),
                        )),
                    };
                }
                Err(err) => return Err(RestError::internal_server_error(anyhow!(err).context("Tokio error"))),
            }
        } else {
            tx
        };

        // If the consensus module is enabled, add the unconfirmed transaction to the memory pool.
        if let Some(consensus) = rest.consensus {
            // Add the unconfirmed transaction to the memory pool.
            consensus.add_unconfirmed_transaction(tx.clone()).await?;
        }

        // Broadcast the transaction.
        rest.routing.propagate(message, &[]);
        #[cfg(feature = "metrics")]
        metrics::increment_counter_1(metrics::router::UNCONFIRMED_TRANSACTIONS_PROPAGATED, "site", "rest");

        // Determine if the node is synced and if the transaction was checked.
        match !is_within_sync_leniency && check_transaction {
            // If the node is not synced and we validated the transaction, return a 203.
            true => Ok((StatusCode::NON_AUTHORITATIVE_INFORMATION, ErasedJson::new(tx_id))),
            // Otherwise, return a 200.
            false => Ok((StatusCode::OK, ErasedJson::new(tx_id))),
        }
    }

    /// POST /<network>/solution/broadcast
    /// POST /<network>/solution/broadcast?check_solution={true}
    ///
    /// Solution Broadcast Flow
    ///
    /// /solution/broadcast
    ///         |
    ///    +----+---------------------------+
    ///    |                               |
    ///    v                               v
    /// Without Query Params        With Query Param
    ///                                check_solution=true
    ///    |                               |
    ///    +---------+                     +---------+
    ///    |         |                     |         |
    ///    v         v                     v         v
    /// Synced   Not Synced            Synced   Not Synced
    ///    |         |                     |         |
    ///    v         v                     v         v
    ///   200       200        check_solution        check_solution
    ///                           +---------+        +---------+
    ///                           |         |        |         |
    ///                           v         v        v         v
    ///                          200       422      203       503
    pub(crate) async fn solution_broadcast(
        State(rest): State<Self>,
        check_solution: Query<CheckSolution>,
        Json(solution): Json<Solution<N>>,
    ) -> Result<impl axum::response::IntoResponse, RestError> {
        // Check if the node is within sync leniency.
        let is_within_sync_leniency = rest.routing.is_within_sync_leniency();
        // Determine if we need to check the solution.
        let check_solution = check_solution.check_solution.unwrap_or(false);
        // Check if the prover has reached their solution limit.
        // While snarkVM will ultimately abort any excess solutions for safety, performing this check
        // here prevents the to-be aborted solutions from propagating through the network.
        let prover_address = solution.address();
        if rest.ledger.is_solution_limit_reached(&prover_address, 0) {
            return Err(RestError::unprocessable_entity(anyhow!(
                "Invalid solution '{}' - Prover '{prover_address}' has reached their solution limit for the current epoch",
                fmt_id(solution.id())
            )));
        }

        if check_solution {
            let verification_slot = rest.verification_slots.solutions.acquire().await?;

            // Compute the current epoch hash.
            let epoch_hash = rest.ledger.latest_epoch_hash()?;
            // Retrieve the current proof target.
            let proof_target = rest.ledger.latest_proof_target();
            // Ensure that the solution is valid for the given epoch.
            let puzzle = rest.ledger.puzzle().clone();
            // Verify the solution in a blocking task.
            let res: Result<(), anyhow::Error> = match tokio::task::spawn_blocking(move || {
                // Hold the slot until verification finishes, even if the client disconnects.
                let _verification_slot = verification_slot;
                let result = puzzle.check_solution(&solution, epoch_hash, proof_target);
                #[cfg(feature = "metrics")]
                metrics::record_solution_check("rest", result.is_ok());
                result
            })
            .await
            {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => {
                    return match is_within_sync_leniency {
                        // The solution failed to verify.
                        true => Err(RestError::unprocessable_entity(
                            err.context(format!("Invalid solution '{}'", fmt_id(solution.id()))),
                        )),
                        // The node is out of sync and may not be able to properly validate the solution.
                        false => Err(RestError::service_unavailable(anyhow!(
                            "Unable to validate solution '{}' (node is syncing)",
                            fmt_id(solution.id())
                        ))),
                    };
                }
                Err(err) => {
                    return Err(RestError::internal_server_error(anyhow!("Tokio error: {err}")));
                }
            };
            // Propagate error if any.
            res?;
        }

        // If the consensus module is enabled, add the unconfirmed solution to the memory pool.
        if let Some(consensus) = rest.consensus {
            // Add the unconfirmed solution to the memory pool.
            let _ = consensus.add_unconfirmed_solution(solution).await;
        }

        let solution_id = solution.id();
        // Prepare the unconfirmed solution message.
        let message =
            Message::UnconfirmedSolution(UnconfirmedSolution { solution_id, solution: Data::Object(solution) });

        // Broadcast the unconfirmed solution message.
        rest.routing.propagate(message, &[]);
        #[cfg(feature = "metrics")]
        metrics::increment_counter_1(metrics::router::UNCONFIRMED_SOLUTIONS_PROPAGATED, "site", "rest");

        // Determine if the node is synced and if the solution was checked.
        match !is_within_sync_leniency && check_solution {
            // If the node is not synced and we validated the solution, return a 203.
            true => Ok((StatusCode::NON_AUTHORITATIVE_INFORMATION, ErasedJson::new(solution_id))),
            // Otherwise, return a 200.
            false => Ok((StatusCode::OK, ErasedJson::new(solution_id))),
        }
    }

    /// POST /{network}/db_backup?path=new_fs_path
    pub(crate) async fn db_backup(
        State(rest): State<Self>,
        backup_path: Query<BackupPath>,
    ) -> Result<ErasedJson, RestError> {
        // Create a checkpoint at the given location.
        let mut backup_path = backup_path.path.clone();
        rest.ledger.backup_database(&backup_path)?;

        // Dump the block tree.
        let ret = ErasedJson::new(());
        if let Err(e) = rest.ledger.cache_block_tree() {
            warn!("Couldn't cache the block tree for a ledger checkpoint: {e}");
            return Ok(ret);
        }

        // Copy the block tree file to the new checkpoint.
        let mut block_tree_path = aleo_ledger_dir(N::ID, rest.ledger.vm().block_store().storage_mode());
        block_tree_path.push("block_tree");
        backup_path.push("block_tree");
        if let Err(e) = fs::copy(block_tree_path, backup_path) {
            warn!("Couldn't copy the block tree file to a ledger checkpoint: {e}");
        }

        Ok(ret)
    }

    /// GET /<network>/solution/limits/{prover_address}
    pub(crate) async fn get_solution_limits_for_prover(
        State(rest): State<Self>,
        Path(prover_address): Path<Address<N>>,
    ) -> Result<ErasedJson, RestError> {
        Ok(ErasedJson::new(json!({
            "is_limit_reached": rest.ledger.is_solution_limit_reached(&prover_address, 0),
            "num_remaining_solutions": rest.ledger.num_remaining_solutions(&prover_address, 0),
            "latest_epoch_hash": rest.ledger.latest_epoch_hash()?,
            "blocks_until_next_epoch": N::NUM_BLOCKS_PER_EPOCH.saturating_sub(rest.ledger.latest_height() % N::NUM_BLOCKS_PER_EPOCH),
        })))
    }

    /// POST /{network}/program/{id}/view/{functionName}
    ///
    /// Evaluates a view function against the ledger state at the latest block height.
    /// The request body must be a JSON array of string-encoded inputs, e.g.:
    ///
    /// ```json
    /// ["aleo1...", "10u64"]
    /// ```
    ///
    /// Returns the outputs as a JSON array of string-encoded values.
    /// Optionally, append `?metadata=true` to also return the block height at which the
    /// view was evaluated (same semantics as the mapping-read endpoints).
    pub(crate) async fn evaluate_view_latest(
        State(rest): State<Self>,
        Path((program_id, view_name)): Path<(ProgramID<N>, Identifier<N>)>,
        metadata: Query<Metadata>,
        json_result: Result<Json<Vec<String>>, JsonRejection>,
    ) -> Result<ErasedJson, RestError> {
        // Parse the inputs from the request body.
        let Json(raw_inputs) = match json_result {
            Ok(json) => json,
            Err(err) => return Err(RestError::unprocessable_entity(anyhow!("Invalid request body: {err}"))),
        };

        // Parse the inputs into `Value<N>`.
        let inputs = parse_view_inputs::<N>(&raw_inputs)?;

        // Evaluate the view function in a blocking task.
        // The latest block's state is captured inside the task to minimise the window
        // between state sampling and evaluation.
        let (outputs, height) = match tokio::task::spawn_blocking(move || {
            // Capture the latest block to build a consistent `FinalizeGlobalState`.
            let block = rest.ledger.latest_block();
            let height = block.height();

            // Reconstruct the `FinalizeGlobalState` for the latest block. The block timestamp
            // is only included from `ConsensusVersion::V12` onward, matching the consensus path.
            let block_timestamp =
                (height >= N::CONSENSUS_HEIGHT(ConsensusVersion::V12).unwrap_or_default()).then_some(block.timestamp());
            let state = FinalizeGlobalState::new::<N>(
                block.round(),
                height,
                block_timestamp,
                block.cumulative_weight(),
                block.cumulative_proof_target(),
                block.previous_hash(),
                None,
                None,
            )?;

            // Get the current (latest-edition) stack for the program.
            let stack = rest.ledger.vm().process().get_stack(program_id)?;

            // Evaluate the view against the current finalize store.
            let outputs = stack.evaluate_view(state, rest.ledger.vm().finalize_store(), &view_name, inputs)?;

            Ok::<_, anyhow::Error>((outputs, height))
        })
        .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(err)) => {
                return Err(RestError::bad_request(err.context(format!(
                    "Failed to evaluate view '{view_name}' for '{program_id}' at the latest height"
                ))));
            }
            Err(err) => return Err(RestError::internal_server_error(anyhow!("Tokio error: {err}"))),
        };

        // Encode each output as a string.
        let output_strings: Vec<String> = outputs.iter().map(|v| v.to_string()).collect();

        // Check if metadata is requested and return the outputs with the sampled height if so.
        if metadata.metadata.unwrap_or(false) {
            return Ok(ErasedJson::new(json!({
                "data": output_strings,
                "height": height,
            })));
        }

        Ok(ErasedJson::new(output_strings))
    }

    /// Returns the history compatibility upstream, which is present whenever these routes are registered.
    fn history_compat(&self) -> &HistoryCompat {
        self.history_compat.as_deref().expect("the history routes are registered only in compatibility mode")
    }

    /// GET /{network}/program/{id}/mapping/{name}/{key}/history/{height}
    ///
    /// History compatibility mode. The value is read from the upstream snapshot of the mapping at
    /// `height`. The response is the removed feature's: the value's plaintext string, or `null`
    /// if the key is absent at that height. For example:
    ///
    /// ```text
    /// GET /mainnet/program/credits.aleo/mapping/unbonding/aleo1qgtv...4ukl2/history/1000000
    /// -> 200
    /// "{\n  microcredits: 31712836548u64,\n  height: 883089u32\n}"
    ///
    /// GET /mainnet/program/credits.aleo/mapping/unbonding/aleo1qy4q...wdf6/history/1000000
    /// -> 200
    /// null
    /// ```
    ///
    /// This node's own height plays no part: the upstream is the source of truth, so a node that
    /// is itself still syncing answers correctly, and a height the upstream has no snapshot of --
    /// above its tip, which trails the network's by a few blocks, or block 0 -- is a 404 saying
    /// so. (The removed feature answered `null` for a height above its own. `null` here means
    /// "the key was not in the mapping at that height", which is not known for such a height, so
    /// it is not claimed.)
    pub(crate) async fn get_history_compat(
        State(rest): State<Self>,
        Path((program_id, mapping_name, mapping_key, height)): Path<HistoricalMappingKey<N>>,
    ) -> Result<impl axum::response::IntoResponse, RestError> {
        let mapping = history_compat_mapping(&program_id, &mapping_name)?;
        let snapshot = rest.history_compat().mapping(height, mapping).await?;
        // The key is reprinted with `to_string` so that it matches the upstream's canonical spelling
        // (the upstream keys are `Plaintext::to_string` output; a client may have spelled the same
        // plaintext differently).
        let value = snapshot.get(&mapping_key.to_string());
        Ok((StatusCode::OK, ErasedJson::new(value)))
    }

    /// GET /{network}/program/{id}/mapping/{name}/history/{height}?keys=key1,key2,...
    ///
    /// History compatibility mode. Every key is read from the one upstream snapshot of the mapping
    /// at `height`; the response is the removed feature's, one `{key, value}` object per key in
    /// the order given, with `value` as in the single-key route:
    ///
    /// ```text
    /// GET /mainnet/program/credits.aleo/mapping/unbonding/history/1000000?keys=aleo1qgtv...,aleo1qy4q...
    /// -> 200
    /// [
    ///   { "key": "aleo1qgtv...4ukl2", "value": "{\n  microcredits: 31712836548u64,\n  height: 883089u32\n}" },
    ///   { "key": "aleo1qy4q...wdf6", "value": null }
    /// ]
    /// ```
    pub(crate) async fn get_history_batch_compat(
        State(rest): State<Self>,
        Path((program_id, mapping_name, height)): Path<HistoricalMappingRoute<N>>,
        Query(historical_keys): Query<HistoricalKeys>,
    ) -> Result<impl axum::response::IntoResponse, RestError> {
        let mapping = history_compat_mapping(&program_id, &mapping_name)?;
        let mapping_keys = parse_historical_mapping_keys::<N>(&historical_keys.keys)?;
        let snapshot = rest.history_compat().mapping(height, mapping).await?;
        let values = historical_keys
            .keys
            .iter()
            .zip(&mapping_keys)
            .map(|(key, mapping_key)| json!({ "key": key, "value": snapshot.get(&mapping_key.to_string()) }))
            .collect::<Vec<_>>();
        Ok((StatusCode::OK, ErasedJson::new(values)))
    }

    /// POST /{network}/program/{id}/view/{functionName}/{height}
    ///
    /// History compatibility mode: this route cannot be served, and always answers 404. The
    /// removed `history` feature evaluated the view's body against its own per-height record of
    /// *every* mapping of every program; the upstream historical API only holds snapshots of five
    /// `credits.aleo` staking mappings, which is not enough state to evaluate an arbitrary view
    /// at a past height, and it cannot evaluate Aleo instructions anyway. The route is registered
    /// regardless so that a client of the old feature gets an explanation rather than a routing
    /// miss, and is pointed at `POST /program/{id}/view/{function}`, which evaluates against the
    /// latest state and is always available.
    pub(crate) async fn evaluate_view_at_height_compat(
        Path((program_id, view_name, height)): Path<ViewFunctionRoute<N>>,
    ) -> RestError {
        RestError::not_found(anyhow!(
            "History compatibility mode cannot evaluate '{program_id}/{view_name}' at height {height}: views at a \
             past height are not available; use POST /program/{program_id}/view/{view_name} for the latest height"
        ))
    }

    /// GET /{network}/staking/rewards/{address}/{height}
    ///
    /// History compatibility mode. The response is the `history-staking-rewards` feature's:
    /// `[validator, reward, new_stake]` for the reward paid to `address` at block `height`, or
    /// `null` if it received none (it was not bonded). As for the mapping routes, this node's own
    /// height plays no part, and a height the upstream has no snapshot of is a 404:
    ///
    /// ```text
    /// GET /mainnet/staking/rewards/aleo1qy4q...wdf6/1000000
    /// -> 200
    /// [
    ///   "aleo1vfukg8ky2mhfprw63s0k0hl4vvd8573s6fkn8cv9y0ca6q27eq8qwdnxls",
    ///   6477,
    ///   141347021440
    /// ]
    /// ```
    ///
    /// The upstream's `stakingrewards` snapshot holds only `[validator, reward]`. The third
    /// element, the stake after the reward, is the staker's `bonded` entry at the same height:
    /// the upstream writes `bonded` after applying the block's rewards, so
    /// `bonded[staker]@h == bonded[staker]@(h-1) + reward@h` (checked against the live API at
    /// block 1,000,000).
    pub(crate) async fn get_staking_reward_compat(
        State(rest): State<Self>,
        Path((address, height)): Path<(Address<N>, u32)>,
    ) -> Result<impl axum::response::IntoResponse, RestError> {
        let staker = address.to_string();
        let rewards = rest.history_compat().staking_rewards(height).await?;
        let Some((validator, reward)) = rewards.get(&staker) else {
            return Ok((StatusCode::OK, ErasedJson::new(None::<()>)));
        };
        let bonded = rest.history_compat().mapping(height, SnapshotMapping::Bonded).await?;
        let Some(bonded_value) = bonded.get(&staker) else {
            return Err(RestError::not_found(anyhow!(
                "The upstream records a reward for {staker} at block {height} but no bonded stake"
            )));
        };
        // The bonded value is the plaintext `{\n  validator: aleo1...,\n  microcredits: 141347021440u64\n}`;
        // parsing it as a `Plaintext` and taking the member is what `bonded_map_into_stakers` does.
        let new_stake = match Plaintext::<N>::from_str(bonded_value)?.find(&[Identifier::from_str("microcredits")?])? {
            Plaintext::Literal(Literal::U64(microcredits), _) => *microcredits,
            other => return Err(RestError::internal_server_error(anyhow!("Unexpected bonded stake: {other}"))),
        };
        Ok((StatusCode::OK, ErasedJson::new((validator, reward, new_stake))))
    }

    /// GET /{network}/validators/participation
    /// GET /{network}/validators/participation?metadata={true}
    #[cfg(feature = "metrics")]
    pub(crate) async fn get_validator_participation_scores(
        State(rest): State<Self>,
        metadata: Query<Metadata>,
    ) -> Result<impl axum::response::IntoResponse, RestError> {
        match rest.consensus {
            Some(consensus) => {
                // Retrieve the committee lookback for the latest round.
                let latest_round = rest.ledger.latest_round();
                let committee_lookback = rest
                    .ledger
                    .get_committee_lookback_for_round(latest_round)?
                    .ok_or_else(|| RestError::not_found(anyhow!("No committee found for round {latest_round}")))?;
                // Retrieve the latest participation scores, combining certificate and signature scores.
                let participation_scores: IndexMap<_, _> = consensus
                    .bft()
                    .primary()
                    .gateway()
                    .validator_telemetry()
                    .get_participation_scores(&committee_lookback)
                    .into_iter()
                    .map(|(address, (cert_score, sig_score))| {
                        let combined = ((0.9 * cert_score + 0.1 * sig_score) * 100.0).round() / 100.0;
                        (address, combined)
                    })
                    .collect();

                // Check if metadata is requested and return the participation scores with metadata if so.
                if metadata.metadata.unwrap_or(false) {
                    return Ok(ErasedJson::new(json!({
                        "participation_scores": participation_scores,
                        "height": rest.ledger.latest_height(),
                    })));
                }

                Ok(ErasedJson::new(participation_scores))
            }
            None => Err(RestError::service_unavailable(anyhow!("Route isn't available for this node type"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use snarkvm::prelude::MainnetV0;

    #[test]
    fn history_compat_mapping_accepts_only_the_upstream_snapshots() {
        let credits = ProgramID::<MainnetV0>::from_str("credits.aleo").unwrap();
        for mapping in SnapshotMapping::ALL {
            let name = Identifier::from_str(mapping.name()).unwrap();
            assert_eq!(history_compat_mapping(&credits, &name).unwrap(), mapping);
        }
        let committee = Identifier::from_str("committee").unwrap();
        let err = history_compat_mapping(&credits, &committee).unwrap_err();
        assert_eq!(err, StatusCode::NOT_FOUND);
        assert!(err.to_string().contains("credits.aleo/committee"), "{err}");

        let other = ProgramID::<MainnetV0>::from_str("other.aleo").unwrap();
        let bonded = Identifier::from_str("bonded").unwrap();
        let err = history_compat_mapping(&other, &bonded).unwrap_err();
        assert_eq!(err, StatusCode::NOT_FOUND);
        assert!(err.to_string().contains("other.aleo/bonded"), "{err}");
    }

    #[test]
    fn parse_historical_mapping_keys_rejects_empty() {
        let err = parse_historical_mapping_keys::<MainnetV0>(&[]).unwrap_err();
        assert_eq!(err, StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn parse_historical_mapping_keys_rejects_too_many() {
        let keys = vec![String::from("1field"); MAX_KEYS_PER_REQUEST + 1];
        let err = parse_historical_mapping_keys::<MainnetV0>(&keys).unwrap_err();
        assert_eq!(err, StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn parse_historical_mapping_keys_rejects_invalid_key_with_index() {
        let keys = vec![String::from("1field"), String::from("not_a_plaintext")];
        let err = parse_historical_mapping_keys::<MainnetV0>(&keys).unwrap_err();
        assert_eq!(err, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(err.to_string().contains("Invalid key at index 1"));
    }
}

#[cfg(test)]
mod route_error_tests {
    use super::*;
    use axum::http::StatusCode;

    #[test]
    fn missing_resource_errors_map_to_not_found() {
        let message = "Missing block hash for block 10";
        let err = map_missing_resource_error(anyhow::anyhow!(message));
        assert_eq!(err, StatusCode::NOT_FOUND);
        assert_eq!(err.to_string(), message);

        let message = "Block 10 does not exist in storage";
        let err = map_missing_resource_error(anyhow::anyhow!(message));
        assert_eq!(err, StatusCode::NOT_FOUND);
        assert_eq!(err.to_string(), message);

        let message = "Failed to find the transition ID for the given input or output ID '123field'";
        let err = map_missing_resource_error(anyhow::anyhow!(message));
        assert_eq!(err, StatusCode::NOT_FOUND);
        assert_eq!(err.to_string(), message);
    }

    #[test]
    fn non_missing_resource_errors_remain_internal_server_error() {
        let message = "disk I/O failed";
        let err = map_missing_resource_error(anyhow::anyhow!(message));
        assert_eq!(err, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(err.to_string(), message);
    }
}

#[cfg(test)]
mod range_tests {
    use super::*;

    const MAX: u32 = 50;

    /// A tip above every height the validation tests ask for, so that a clamp would leave them
    /// alone.
    const TIP: u32 = 1_000;

    fn range(start: u32, end: u32) -> BlockRange {
        BlockRange { start, end, allow_partial: None }
    }

    fn partial_range(start: u32, end: u32) -> BlockRange {
        BlockRange { start, end, allow_partial: Some(true) }
    }

    #[test]
    fn accepts_a_range_within_the_maximum() {
        assert_eq!(check_block_range(range(10, 20), MAX, "blocks", TIP).unwrap(), (10, 20));
    }

    #[test]
    fn accepts_a_range_of_exactly_the_maximum() {
        assert_eq!(check_block_range(range(10, 10 + MAX), MAX, "blocks", TIP).unwrap(), (10, 10 + MAX));
    }

    #[test]
    fn accepts_an_empty_range() {
        assert_eq!(check_block_range(range(10, 10), MAX, "blocks", TIP).unwrap(), (10, 10));
    }

    #[test]
    fn rejects_an_inverted_range() {
        // This must be rejected before the width check, which would otherwise underflow.
        let err = check_block_range(range(20, 10), MAX, "blocks", TIP).unwrap_err();
        assert_eq!(err, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn rejects_a_range_over_the_maximum() {
        let err = check_block_range(range(10, 11 + MAX), MAX, "blocks", TIP).unwrap_err();
        assert_eq!(err, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn leaves_a_range_past_the_tip_alone_by_default() {
        assert_eq!(check_block_range(range(10, 30), MAX, "blocks", 19).unwrap(), (10, 30));
    }

    #[test]
    fn clamps_a_partial_range_that_reaches_past_the_tip() {
        // The tip itself is served, so the exclusive end lands one past it.
        assert_eq!(check_block_range(partial_range(10, 30), MAX, "blocks", 19).unwrap(), (10, 20));
        assert_eq!(check_block_range(partial_range(10, 30), MAX, "blocks", 10).unwrap(), (10, 11));
    }

    #[test]
    fn clamps_a_partial_range_that_starts_past_the_tip_to_an_empty_range() {
        let (start, end) = check_block_range(partial_range(10, 30), MAX, "blocks", 9).unwrap();
        assert_eq!(start, end);

        let (start, end) = check_block_range(partial_range(500, 530), MAX, "blocks", 9).unwrap();
        assert_eq!(start, end);
    }

    #[test]
    fn clamps_at_the_top_of_the_height_space_without_overflowing() {
        assert_eq!(
            check_block_range(partial_range(u32::MAX - 5, u32::MAX), MAX, "blocks", u32::MAX).unwrap(),
            (u32::MAX - 5, u32::MAX)
        );
    }

    #[test]
    fn applies_the_maximum_to_the_requested_range_rather_than_the_clamped_one() {
        let err = check_block_range(partial_range(0, 1 + MAX), MAX, "blocks", 0).unwrap_err();
        assert_eq!(err, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn rejects_a_range_spanning_the_whole_height_space() {
        let err = check_block_range(range(0, u32::MAX), MAX, "blocks", TIP).unwrap_err();
        assert_eq!(err, StatusCode::BAD_REQUEST);
    }

    /// The json size of one *array element* of each kind, measured on mainnet block 21,815,000 as
    /// the routes serve it: pretty-printed, so each element carries its own indent, comma and
    /// newline. These are deliberately not the sizes of the bare values (a block hash is 63 bytes
    /// as a quoted string but 67 as an element), because it is the response that has to fit.
    ///
    /// The header figure is the worst case rather than the measured one: the sampled block had an
    /// empty `solutions_root` of `"0field"`, and a populated root brings the element from 965 to
    /// 1,040 bytes.
    const BLOCK_HASH_BYTES: u32 = 67;
    const STATE_ROOT_BYTES: u32 = 67;
    const BLOCK_HEADER_BYTES: u32 = 1_040;
    const BLOCK_BYTES: u32 = 337_511;

    #[test]
    fn each_maximum_bounds_its_response_to_at_most_one_block() {
        // Every route added here serves a projection of a block, so none of them should be able to
        // produce a response larger than the single block `get_blocks` already serves. If a
        // maximum is raised past that point, it is no longer free from the node's perspective.
        let largest_get_blocks_response = MAX_BLOCK_RANGE * BLOCK_BYTES;

        for (name, max, item_bytes) in [
            ("hashes", MAX_BLOCK_HASH_RANGE, BLOCK_HASH_BYTES),
            ("headers", MAX_BLOCK_HEADER_RANGE, BLOCK_HEADER_BYTES),
            ("stateRoots", MAX_STATE_ROOT_RANGE, STATE_ROOT_BYTES),
        ] {
            let largest_response = max * item_bytes;
            assert!(
                largest_response <= BLOCK_BYTES,
                "the {name} maximum can serve {largest_response} bytes, more than the {BLOCK_BYTES} bytes of one block"
            );
            assert!(largest_response < largest_get_blocks_response);

            // The headroom above is thin by design, so confirm the guard is load-bearing: the
            // next maximum that would round up to another whole block must fail it.
            let too_wide = BLOCK_BYTES / item_bytes + 1;
            assert!(too_wide * item_bytes > BLOCK_BYTES, "the {name} guard would not catch a raised maximum");
            assert!(max <= BLOCK_BYTES / item_bytes, "the {name} maximum is already over the limit");
        }
    }

    /// The heaviest `get_blocks` response `MAX_BLOCK_RANGE` can produce, and the heaviest response
    /// this route can produce at each maximum that has been measured, in json bytes.
    ///
    /// Both are maxima over every contiguous run of heights in the sample, not averages, because a
    /// caller picks the heights. Sampled from mainnet on 2026-09-20 over 209,683 heights in 670
    /// runs of 320 contiguous blocks, spanning 7,771 to 22,094,742 and deliberately over-weighting
    /// the 2024-2025 stretch where transactions are the largest share of a block. The heaviest
    /// `get_blocks` response in that sample is heights 11,076,870 to 11,076,919; the heaviest
    /// transactions responses are all around height 3,325,700.
    const HEAVIEST_GET_BLOCKS_RESPONSE_BYTES: u32 = 28_068_594;
    const HEAVIEST_TRANSACTIONS_RESPONSE_BYTES: [(u32, u32); 4] =
        [(50, 10_247_221), (160, 25_736_904), (176, 27_927_850), (320, 47_465_514)];

    #[test]
    fn the_transactions_maximum_stays_within_a_get_blocks_response() {
        // `each_maximum_bounds_its_response_to_at_most_one_block` deliberately does not cover this
        // route: a block's transactions have no fixed size, so no per-item figure bounds it. The
        // property that holds instead is that the heaviest response this maximum can produce is no
        // larger than the heaviest `get_blocks` already produces, so the route introduces no
        // response the node could not already be asked for.
        let heaviest = HEAVIEST_TRANSACTIONS_RESPONSE_BYTES
            .iter()
            .find(|(max, _)| *max == MAX_BLOCK_TRANSACTIONS_RANGE)
            .map(|(_, bytes)| *bytes)
            .expect("this maximum has not been measured against mainnet; measure it before using it");

        assert!(
            heaviest <= HEAVIEST_GET_BLOCKS_RESPONSE_BYTES,
            "{MAX_BLOCK_TRANSACTIONS_RANGE} blocks' transactions reach {heaviest} bytes, more than \
             the {HEAVIEST_GET_BLOCKS_RESPONSE_BYTES} bytes of the heaviest {MAX_BLOCK_RANGE} whole blocks"
        );
    }

    #[test]
    fn each_maximum_improves_on_fetching_whole_blocks() {
        // The point of these routes is that syncing a projection over the whole chain costs far
        // fewer requests than syncing the blocks that contain it. Guard that they are worth having.
        for (name, max) in [
            ("hashes", MAX_BLOCK_HASH_RANGE),
            ("headers", MAX_BLOCK_HEADER_RANGE),
            ("stateRoots", MAX_STATE_ROOT_RANGE),
            ("transactions", MAX_BLOCK_TRANSACTIONS_RANGE),
        ] {
            assert!(max > MAX_BLOCK_RANGE, "the {name} maximum is no better than fetching whole blocks");
        }
    }
}
