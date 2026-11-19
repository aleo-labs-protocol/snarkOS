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

use snarkos_node_router::messages::{BlockRequest, BlockResponse, BlockResponseStatus, DataBlocks};

use snarkvm::{console::network::Network, ledger::Block, utilities::flatten_error};

use anyhow::Result;
use std::{cell::RefCell, ops::Range};

/// What to do with a block request (see [`serve_block_request`]).
#[derive(Debug)]
pub enum BlockRequestOutcome<N: Network> {
    /// Send this response.
    Reply(BlockResponse<N>),
    /// Send nothing, so that the request times out at the peer.
    Drop,
    /// Disconnect the peer: it predates status replies and asked for blocks this node does not serve.
    OutdatedPeer(BlockResponseStatus),
}

/// Returns what to do with `request`: reply with the blocks, `Pruned` if the range starts below the
/// store's authority-pruning watermark (`pruned_below`), or `NotYet` if it reaches past
/// `latest_height`.
///
/// A peer that does not support status replies (`supports_status`) gets `OutdatedPeer` instead of
/// a status reply. A failed read is an error, unless the range is then pruned or past the tip. A
/// watermark that cannot be read counts as none, and is logged once; if the blocks cannot be read
/// either, the request is dropped.
pub fn serve_block_request<N: Network>(
    request: BlockRequest,
    supports_status: bool,
    latest_height: impl Fn() -> u32,
    pruned_below: impl Fn() -> Result<Option<u32>>,
    get_blocks: impl FnOnce(Range<u32>) -> Result<Vec<Block<N>>>,
) -> Result<BlockRequestOutcome<N>> {
    let BlockRequest { start_height, end_height } = request;
    let watermark_error = RefCell::new(None);
    let unavailable = || {
        let pruned_below = pruned_below().unwrap_or_else(|error| {
            watermark_error.borrow_mut().get_or_insert(error);
            None
        });
        if let Some(oldest_retained) = pruned_below.filter(|pruned_below| start_height < *pruned_below) {
            return Some(BlockResponseStatus::Pruned { oldest_retained });
        }
        let latest_height = latest_height();
        (end_height - 1 > latest_height).then_some(BlockResponseStatus::NotYet { latest_height })
    };

    let status = match unavailable() {
        Some(status) => status,
        None => match get_blocks(start_height..end_height) {
            Ok(blocks) => {
                if let Some(error) = watermark_error.take() {
                    warn!("{}", flatten_error(error.context("Failed to read the authority-pruning watermark")));
                }
                let latest_consensus_version = N::CONSENSUS_VERSION(end_height - 1)?;
                let response = BlockResponse::new(request, DataBlocks(blocks), latest_consensus_version);
                return Ok(BlockRequestOutcome::Reply(response));
            }
            Err(error) => match unavailable() {
                Some(status) => status,
                None => match watermark_error.take() {
                    Some(watermark_error) => {
                        warn!(
                            "Dropping the request for blocks {start_height} to {end_height}: reading them failed ({error:#}), \
                            and so did reading the authority-pruning watermark ({watermark_error:#})"
                        );
                        return Ok(BlockRequestOutcome::Drop);
                    }
                    None => {
                        return Err(error.context(format!("Failed to retrieve blocks {start_height} to {end_height}")));
                    }
                },
            },
        },
    };

    if let Some(error) = watermark_error.take() {
        warn!("{}", flatten_error(error.context("Failed to read the authority-pruning watermark")));
    }
    match supports_status {
        true => Ok(BlockRequestOutcome::Reply(BlockResponse::unavailable(request, status))),
        false => Ok(BlockRequestOutcome::OutdatedPeer(status)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::anyhow;
    use snarkvm::prelude::FromBytes;
    use std::{cell::Cell, sync::OnceLock};

    type CurrentNetwork = snarkvm::prelude::MainnetV0;
    type Outcome = BlockRequestOutcome<CurrentNetwork>;

    /// Returns `heights.len()` copies of the genesis block, in place of the blocks of `heights`.
    fn sample_blocks(heights: Range<u32>) -> Vec<Block<CurrentNetwork>> {
        static GENESIS: OnceLock<Block<CurrentNetwork>> = OnceLock::new();
        let genesis = GENESIS.get_or_init(|| Block::from_bytes_le(CurrentNetwork::genesis_bytes()).unwrap());
        vec![genesis.clone(); heights.len()]
    }

    /// Serves `start..end` for a peer that supports status replies if `supports_status`.
    fn serve(
        start: u32,
        end: u32,
        supports_status: bool,
        latest: impl Fn() -> u32,
        pruned_below: impl Fn() -> Result<Option<u32>>,
        get_blocks: impl FnOnce(Range<u32>) -> Result<Vec<Block<CurrentNetwork>>>,
    ) -> Result<Outcome> {
        serve_block_request(BlockRequest::new(start, end).unwrap(), supports_status, latest, pruned_below, get_blocks)
    }

    /// Returns the status of a reply, or `None` for any other outcome.
    fn status(outcome: Outcome) -> Option<BlockResponseStatus> {
        match outcome {
            Outcome::Reply(response) => Some(response.status),
            _ => None,
        }
    }

    #[test]
    fn test_serve_block_request() {
        const LATEST: u32 = 100;
        let read = Cell::new(false);
        let get_blocks = |heights| {
            read.set(true);
            Ok(sample_blocks(heights))
        };
        let none = || Ok(None);

        // Up to the tip, the blocks are read.
        let Outcome::Reply(response) = serve(96, 101, true, || LATEST, none, get_blocks).unwrap() else { panic!() };
        assert_eq!(response.status, BlockResponseStatus::Blocks);
        assert_eq!(response.blocks.deserialize_blocking().unwrap().len(), 5);
        assert!(read.replace(false));

        // Past the tip, the reply is `NotYet` without a read; an older peer is disconnected instead.
        let not_yet = BlockResponseStatus::NotYet { latest_height: LATEST };
        assert_eq!(status(serve(96, 102, true, || LATEST, none, get_blocks).unwrap()), Some(not_yet));
        assert!(!read.get());
        assert!(
            matches!(serve(96, 102, false, || LATEST, none, get_blocks).unwrap(), Outcome::OutdatedPeer(s) if s == not_yet)
        );

        // A read error below the tip is returned.
        assert!(serve(96, 101, true, || LATEST, none, |_| Err(anyhow!("storage error"))).is_err());

        // A read error after the tip moved below the range is `NotYet`.
        let latest = Cell::new(LATEST);
        let get_blocks = |_| {
            latest.set(98);
            Err(anyhow!("missing block"))
        };
        let outcome = serve(96, 101, true, || latest.get(), none, get_blocks).unwrap();
        assert_eq!(status(outcome), Some(BlockResponseStatus::NotYet { latest_height: 98 }));
    }

    /// Tests the serving table with a watermark `W`: a range that starts below `W`, also one that
    /// crosses it or reaches past the tip, is `Pruned { W }` without a read.
    #[test]
    fn test_serve_pruned_block_request() {
        const LATEST: u32 = 1_000;
        const W: u32 = 500;
        let read = Cell::new(false);
        let get_blocks = |heights| {
            read.set(true);
            Ok(sample_blocks(heights))
        };
        let serve_at =
            |start, end, w: u32| status(serve(start, end, true, || LATEST, || Ok(Some(w)), get_blocks).unwrap());
        let pruned = |w| Some(BlockResponseStatus::Pruned { oldest_retained: w });

        assert_eq!(serve_at(1, 6, W), pruned(W));
        assert_eq!(serve_at(W - 2, W + 3, W), pruned(W));
        assert_eq!(serve_at(LATEST - 1, LATEST + 3, LATEST), pruned(LATEST));
        assert!(!read.get());
        assert_eq!(serve_at(W, W + 5, W), Some(BlockResponseStatus::Blocks));
        assert!(read.replace(false));
        assert_eq!(serve_at(LATEST, LATEST + 2, W), Some(BlockResponseStatus::NotYet { latest_height: LATEST }));

        // An older peer is disconnected instead.
        let outcome = serve(1, 6, false, || LATEST, || Ok(Some(W)), get_blocks).unwrap();
        assert!(matches!(outcome, Outcome::OutdatedPeer(BlockResponseStatus::Pruned { oldest_retained: W })));

        // A range pruned between the check and the read is `Pruned`.
        let watermark = Cell::new(None);
        let get_blocks = |_| {
            watermark.set(Some(W + 10));
            Err(anyhow!("pruned"))
        };
        let outcome = serve(W, W + 5, true, || LATEST, || Ok(watermark.get()), get_blocks).unwrap();
        assert_eq!(status(outcome), pruned(W + 10));
    }

    /// Tests that a watermark that cannot be read counts as none: the blocks are served if they can
    /// be read, and otherwise the request is dropped rather than failed.
    #[test]
    fn test_serve_with_unreadable_watermark() {
        const LATEST: u32 = 1_000;
        let unreadable = || Err(anyhow!("watermark error"));

        let outcome = serve(1, 6, true, || LATEST, unreadable, |heights| Ok(sample_blocks(heights))).unwrap();
        assert_eq!(status(outcome), Some(BlockResponseStatus::Blocks));

        let outcome = serve(1, 6, true, || LATEST, unreadable, |_| Err(anyhow!("pruned"))).unwrap();
        assert!(matches!(outcome, Outcome::Drop));
        let outcome = serve(1, 6, false, || LATEST, unreadable, |_| Err(anyhow!("pruned"))).unwrap();
        assert!(matches!(outcome, Outcome::Drop));

        let outcome = serve(LATEST, LATEST + 2, true, || LATEST, unreadable, |_| Err(anyhow!("missing"))).unwrap();
        assert_eq!(status(outcome), Some(BlockResponseStatus::NotYet { latest_height: LATEST }));
    }
}
