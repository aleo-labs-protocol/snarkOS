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

use snarkvm::{console::network::Network, ledger::Block};

use anyhow::{Result, bail};
use std::ops::Range;

/// Returns the response to `request`: the blocks, or `NotYet` if the range reaches past
/// `latest_height`.
///
/// A peer that does not support status replies (`supports_status`) gets an error instead of a
/// status reply. A failed read is an error, unless the range then reaches past the tip.
pub fn serve_block_request<N: Network>(
    request: BlockRequest,
    supports_status: bool,
    latest_height: impl Fn() -> u32,
    get_blocks: impl FnOnce(Range<u32>) -> Result<Vec<Block<N>>>,
) -> Result<BlockResponse<N>> {
    let BlockRequest { start_height, end_height } = request;
    let not_yet = || {
        let latest_height = latest_height();
        (end_height - 1 > latest_height).then_some(BlockResponseStatus::NotYet { latest_height })
    };

    let status = match not_yet() {
        Some(status) => status,
        None => match get_blocks(start_height..end_height) {
            Ok(blocks) => {
                let latest_consensus_version = N::CONSENSUS_VERSION(end_height - 1)?;
                return Ok(BlockResponse::new(request, DataBlocks(blocks), latest_consensus_version));
            }
            Err(error) => not_yet()
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
        serve_block_request(BlockRequest::new(start_height, end_height).unwrap(), supports_status, latest, get_blocks)
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
}
