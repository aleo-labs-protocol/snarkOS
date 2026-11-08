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

use snarkvm::utilities::io_error;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct BlockRequest {
    /// The starting block height (inclusive).
    pub start_height: u32,
    /// The ending block height (exclusive).
    pub end_height: u32,
}

impl BlockRequest {
    /// Creates a new block request and return an error if the range is not  well-formed.
    pub fn new(start_height: u32, end_height: u32) -> IoResult<Self> {
        if start_height == 0 {
            Err(io_error("BlockRequest cannot include the genesis block"))
        } else if start_height >= end_height {
            Err(io_error(format!("BlockRequest must contain a valid range, but was {start_height}..{end_height}")))
        } else {
            Ok(Self { start_height, end_height })
        }
    }
}

impl EventTrait for BlockRequest {
    /// Returns the event name.
    #[inline]
    fn name(&self) -> Cow<'static, str> {
        let start = self.start_height;
        let end = self.end_height;
        match start + 1 == end {
            true => format!("BlockRequest {start}"),
            false => format!("BlockRequest {start}..{end}"),
        }
        .into()
    }
}

impl ToBytes for BlockRequest {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        self.start_height.write_le(&mut writer)?;
        self.end_height.write_le(&mut writer)
    }
}

impl FromBytes for BlockRequest {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        let start_height = u32::read_le(&mut reader)?;
        let end_height = u32::read_le(&mut reader)?;

        Self::new(start_height, end_height)
    }
}

#[cfg(any(test, feature = "fuzz-helpers"))]
pub mod prop_tests {
    #![cfg_attr(not(test), allow(unused_imports))]
    use crate::{BlockRequest, DataBlocks};

    use snarkvm::{
        console::network::MainnetV0,
        utilities::{FromBytes, ToBytes},
    };

    use bytes::{Buf, BufMut, BytesMut};
    use proptest::prelude::prop_compose;
    use test_strategy::proptest;

    const MAX_RANGE: u32 = DataBlocks::<MainnetV0>::MAXIMUM_NUMBER_OF_BLOCKS as u32;

    prop_compose! {
        /// Creates a block request with a start point and a valid range.
        pub fn any_block_request()(start_height in 1..u32::MAX-MAX_RANGE, num_blocks in 1..MAX_RANGE) -> BlockRequest {
            BlockRequest { start_height, end_height: start_height + num_blocks }
        }
    }

    #[proptest]
    fn block_request_roundtrip(#[strategy(any_block_request())] block_request: BlockRequest) {
        let mut bytes = BytesMut::default().writer();
        block_request.write_le(&mut bytes).unwrap();
        let decoded = BlockRequest::read_le(&mut bytes.into_inner().reader()).unwrap();
        assert_eq![decoded, block_request];
    }
}
