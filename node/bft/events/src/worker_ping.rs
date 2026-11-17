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
pub struct WorkerPing<N: Network> {
    pub transmission_ids: IndexSet<TransmissionID<N>>,
}

impl<N: Network> WorkerPing<N> {
    /// Initializes a new ping event.
    pub fn new(transmission_ids: IndexSet<TransmissionID<N>>) -> Self {
        Self { transmission_ids }
    }
}

impl<N: Network> From<IndexSet<TransmissionID<N>>> for WorkerPing<N> {
    /// Initializes a new ping event.
    fn from(transmission_ids: IndexSet<TransmissionID<N>>) -> Self {
        Self::new(transmission_ids)
    }
}

impl<N: Network> EventTrait for WorkerPing<N> {
    /// Returns the event name.
    #[inline]
    fn name(&self) -> Cow<'static, str> {
        "WorkerPing".into()
    }
}

impl<N: Network> ToBytes for WorkerPing<N> {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        u16::try_from(self.transmission_ids.len()).map_err(error)?.write_le(&mut writer)?;
        for transmission_id in &self.transmission_ids {
            transmission_id.write_le(&mut writer)?;
        }
        Ok(())
    }
}

impl<N: Network> FromBytes for WorkerPing<N> {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        let num_transmissions = u16::read_le(&mut reader)?;
        let mut transmission_ids = IndexSet::new();
        for _ in 0..num_transmissions {
            transmission_ids.insert(TransmissionID::read_le(&mut reader)?);
        }
        Ok(Self { transmission_ids })
    }
}

#[cfg(any(test, feature = "fuzz-helpers"))]
pub mod prop_tests {
    #![cfg_attr(not(test), allow(unused_imports))]
    use crate::{WorkerPing, prop_tests::any_transmission_id};
    use snarkvm::console::prelude::{FromBytes, ToBytes};

    use bytes::{Buf, BufMut, BytesMut};
    use proptest::{
        collection::hash_set,
        prelude::{BoxedStrategy, Strategy},
    };
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    pub fn any_worker_ping() -> BoxedStrategy<WorkerPing<CurrentNetwork>> {
        hash_set(any_transmission_id(), 1..16).prop_map(|ids| WorkerPing::new(ids.into_iter().collect())).boxed()
    }

    #[proptest]
    fn serialize_deserialize(#[strategy(any_worker_ping())] original: WorkerPing<CurrentNetwork>) {
        let mut buf = BytesMut::default().writer();
        WorkerPing::write_le(&original, &mut buf).unwrap();

        let deserialized = WorkerPing::read_le(buf.into_inner().reader()).unwrap();
        assert_eq!(original, deserialized);
    }
}
