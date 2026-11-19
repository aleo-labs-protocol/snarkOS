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

use anyhow::{Result, bail};
use std::ops::Range;

/// Returns the response to `request`: the blocks, `Pruned` if the range starts below the store's
/// authority-pruning watermark (`pruned_below`), or `NotYet` if it reaches past `latest_height`.
///
/// A peer that does not support status replies (`supports_status`) gets an error instead of a
/// status reply. A failed read is an error, unless the range is then pruned or past the tip. A
/// watermark that cannot be read counts as none.
pub fn serve_block_request<N: Network>(
    request: BlockRequest,
    supports_status: bool,
    latest_height: impl Fn() -> u32,
    pruned_below: impl Fn() -> Result<Option<u32>>,
    get_blocks: impl FnOnce(Range<u32>) -> Result<Vec<Block<N>>>,
) -> Result<BlockResponse<N>> {
    let BlockRequest { start_height, end_height } = request;
    let unavailable = || {
        let pruned_below = pruned_below().unwrap_or_else(|error| {
            warn!("{}", flatten_error(error.context("Failed to read the authority-pruning watermark")));
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
                let latest_consensus_version = N::CONSENSUS_VERSION(end_height - 1)?;
                return Ok(BlockResponse::new(request, DataBlocks(blocks), latest_consensus_version));
            }
            Err(error) => unavailable()
                .ok_or_else(|| error.context(format!("Failed to retrieve blocks {start_height} to {end_height}")))?,
        },
    };

    if !supports_status {
        bail!("Cannot serve blocks {start_height} to {end_height} ({status:?}) to a peer without status replies");
    }
    Ok(BlockResponse::unavailable(request, status))
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::anyhow;
    use std::cell::Cell;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    fn serve(
        start_height: u32,
        end_height: u32,
        supports_status: bool,
        latest: impl Fn() -> u32,
        get_blocks: impl FnOnce(Range<u32>) -> Result<Vec<Block<CurrentNetwork>>>,
    ) -> Result<BlockResponse<CurrentNetwork>> {
        serve_pruned(start_height, end_height, supports_status, latest, || Ok(None), get_blocks)
    }

    fn serve_pruned(
        start_height: u32,
        end_height: u32,
        supports_status: bool,
        latest: impl Fn() -> u32,
        pruned_below: impl Fn() -> Result<Option<u32>>,
        get_blocks: impl FnOnce(Range<u32>) -> Result<Vec<Block<CurrentNetwork>>>,
    ) -> Result<BlockResponse<CurrentNetwork>> {
        let request = BlockRequest::new(start_height, end_height).unwrap();
        serve_block_request(request, supports_status, latest, pruned_below, get_blocks)
    }
    #[test]
    fn test_serve_block_request() {
        const LATEST: u32 = 100;
        let read = Cell::new(false);
        let get_blocks = |_| {
            read.set(true);
            Ok(vec![])
        };

        // Up to the tip, the blocks are read.
        let response = serve(96, 101, true, || LATEST, get_blocks).unwrap();
        assert_eq!(response.status, BlockResponseStatus::Blocks);
        assert!(read.replace(false));

        // Past the tip, the reply is `NotYet` without a read.
        let response = serve(96, 102, true, || LATEST, get_blocks).unwrap();
        assert_eq!(response.status, BlockResponseStatus::NotYet { latest_height: LATEST });
        assert!(!read.get());

        // A peer without status replies gets an error instead.
        assert!(serve(96, 102, false, || LATEST, get_blocks).is_err());

        // A read error below the tip is returned.
        assert!(serve(96, 101, true, || LATEST, |_| Err(anyhow!("storage error"))).is_err());

        // A read error after the tip moved below the range is `NotYet`.
        let latest = Cell::new(LATEST);
        let response = serve(
            96,
            101,
            true,
            || latest.get(),
            |_| {
                latest.set(98);
                Err(anyhow!("missing block"))
            },
        )
        .unwrap();
        assert_eq!(response.status, BlockResponseStatus::NotYet { latest_height: 98 });
    }

    /// Tests the serving table with a watermark `W`: a range that starts below `W`, also one that
    /// crosses it, is `Pruned { W }` without a read, before `NotYet` applies.
    #[test]
    fn test_serve_pruned_block_request() {
        const LATEST: u32 = 1_000;
        const W: u32 = 500;
        let read = Cell::new(false);
        let get_blocks = |_| {
            read.set(true);
            Ok(vec![])
        };
        let status = |start, end| serve_pruned(start, end, true, || LATEST, || Ok(Some(W)), get_blocks).unwrap().status;

        assert_eq!(status(1, 6), BlockResponseStatus::Pruned { oldest_retained: W });
        assert_eq!(status(W - 2, W + 3), BlockResponseStatus::Pruned { oldest_retained: W });
        assert!(!read.get());
        assert_eq!(status(W, W + 5), BlockResponseStatus::Blocks);
        assert!(read.replace(false));
        assert_eq!(status(LATEST, LATEST + 2), BlockResponseStatus::NotYet { latest_height: LATEST });

        // A peer without status replies gets an error.
        assert!(serve_pruned(1, 6, false, || LATEST, || Ok(Some(W)), get_blocks).is_err());

        // A range pruned between the check and the read is `Pruned`.
        let watermark = Cell::new(None);
        let pruned_below = || Ok(watermark.get());
        let response = serve_pruned(
            W,
            W + 5,
            true,
            || LATEST,
            pruned_below,
            |_| {
                watermark.set(Some(W + 10));
                Err(anyhow!("pruned"))
            },
        );
        assert_eq!(response.unwrap().status, BlockResponseStatus::Pruned { oldest_retained: W + 10 });

        // A watermark that cannot be read counts as none.
        let status =
            serve_pruned(1, 6, true, || LATEST, || Err(anyhow!("watermark error")), get_blocks).unwrap().status;
        assert_eq!(status, BlockResponseStatus::Blocks);
    }
}
