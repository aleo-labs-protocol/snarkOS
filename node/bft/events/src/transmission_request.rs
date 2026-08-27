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
pub struct TransmissionRequest<N: Network> {
    pub transmission_id: TransmissionID<N>,
}

impl<N: Network> TransmissionRequest<N> {
    /// Initializes a new transmission request event.
    pub const fn new(transmission_id: TransmissionID<N>) -> Self {
        Self { transmission_id }
    }
}

impl<N: Network> From<TransmissionID<N>> for TransmissionRequest<N> {
    /// Initializes a new transmission request event.
    fn from(transmission_id: TransmissionID<N>) -> Self {
        Self::new(transmission_id)
    }
}

impl<N: Network> EventTrait for TransmissionRequest<N> {
    /// Returns the event name.
    #[inline]
    fn name(&self) -> Cow<'static, str> {
        "TransmissionRequest".into()
    }
}

impl<N: Network> ToBytes for TransmissionRequest<N> {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        self.transmission_id.write_le(&mut writer)?;
        Ok(())
    }
}

impl<N: Network> FromBytes for TransmissionRequest<N> {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        let transmission_id = TransmissionID::read_le(&mut reader)?;

        Ok(Self { transmission_id })
    }
}

#[cfg(any(test, feature = "fuzz-helpers"))]
pub mod prop_tests {
    #![cfg_attr(not(test), allow(unused_imports))]
    use crate::{TransmissionRequest, prop_tests::any_transmission_id};
    use snarkvm::console::prelude::{FromBytes, ToBytes};

    use bytes::{Buf, BufMut, BytesMut};
    use proptest::prelude::{BoxedStrategy, Strategy};
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    pub fn any_transmission_request() -> BoxedStrategy<TransmissionRequest<CurrentNetwork>> {
        any_transmission_id().prop_map(TransmissionRequest::new).boxed()
    }

    #[proptest]
    fn serialize_deserialize(#[strategy(any_transmission_request())] original: TransmissionRequest<CurrentNetwork>) {
        let mut buf = BytesMut::default().writer();
        TransmissionRequest::write_le(&original, &mut buf).unwrap();

        let deserialized = TransmissionRequest::read_le(buf.into_inner().reader()).unwrap();
        assert_eq!(original, deserialized);
    }
}
