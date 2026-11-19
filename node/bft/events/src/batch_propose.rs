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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchPropose<N: Network> {
    pub round: u64,
    pub batch_header: Data<BatchHeader<N>>,
}

impl<N: Network> BatchPropose<N> {
    /// Initializes a new batch propose event.
    pub fn new(round: u64, batch_header: Data<BatchHeader<N>>) -> Self {
        Self { round, batch_header }
    }
}

impl<N: Network> From<BatchHeader<N>> for BatchPropose<N> {
    /// Initializes a new batch propose event.
    fn from(batch_header: BatchHeader<N>) -> Self {
        Self::new(batch_header.round(), Data::Object(batch_header))
    }
}

impl<N: Network> EventTrait for BatchPropose<N> {
    /// Returns the event name.
    #[inline]
    fn name(&self) -> Cow<'static, str> {
        "BatchPropose".into()
    }
}

impl<N: Network> ToBytes for BatchPropose<N> {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        self.round.write_le(&mut writer)?;
        self.batch_header.write_le(&mut writer)?;
        Ok(())
    }
}

impl<N: Network> FromBytes for BatchPropose<N> {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        let round = u64::read_le(&mut reader)?;
        let batch_header = Data::read_le(&mut reader)?;

        Ok(Self { round, batch_header })
    }
}

#[cfg(any(test, feature = "fuzz-helpers"))]
pub mod prop_tests {
    #![cfg_attr(not(test), allow(unused_imports))]
    use crate::{
        BatchPropose,
        certificate_response::prop_tests::any_batch_header,
        committee_prop_tests::CommitteeContext,
    };
    use snarkvm::{
        console::prelude::{FromBytes, ToBytes},
        prelude::narwhal::Data,
    };

    use bytes::{Buf, BufMut, BytesMut};
    use proptest::prelude::{BoxedStrategy, Strategy, any};
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    pub fn any_batch_propose() -> BoxedStrategy<BatchPropose<CurrentNetwork>> {
        any::<CommitteeContext>()
            .prop_flat_map(|committee| (any::<u64>(), any_batch_header(&committee)))
            .prop_map(|(round, batch_header)| BatchPropose::new(round, Data::Object(batch_header)))
            .boxed()
    }

    #[proptest]
    fn serialize_deserialize(#[strategy(any_batch_propose())] original: BatchPropose<CurrentNetwork>) {
        let mut buf = BytesMut::default().writer();
        BatchPropose::write_le(&original, &mut buf).unwrap();

        let deserialized: BatchPropose<CurrentNetwork> = BatchPropose::read_le(buf.into_inner().reader()).unwrap();
        // because of the Data enum, we cannot compare the structs directly even though it derives PartialEq
        assert_eq!(original.round, deserialized.round);
        assert_eq!(
            original.batch_header.deserialize_blocking().unwrap(),
            deserialized.batch_header.deserialize_blocking().unwrap()
        );
    }
}
