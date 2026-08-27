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
pub struct TransmissionResponse<N: Network> {
    pub transmission_id: TransmissionID<N>,
    pub transmission: Transmission<N>,
}

impl<N: Network> TransmissionResponse<N> {
    /// Initializes a new transmission response event.
    pub fn new(transmission_id: TransmissionID<N>, transmission: Transmission<N>) -> Self {
        Self { transmission_id, transmission }
    }
}

impl<N: Network> From<(TransmissionID<N>, Transmission<N>)> for TransmissionResponse<N> {
    /// Initializes a new transmission response event.
    fn from((transmission_id, transmission): (TransmissionID<N>, Transmission<N>)) -> Self {
        Self::new(transmission_id, transmission)
    }
}

impl<N: Network> EventTrait for TransmissionResponse<N> {
    /// Returns the event name.
    #[inline]
    fn name(&self) -> Cow<'static, str> {
        "TransmissionResponse".into()
    }
}

impl<N: Network> ToBytes for TransmissionResponse<N> {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        self.transmission_id.write_le(&mut writer)?;
        self.transmission.write_le(&mut writer)?;
        Ok(())
    }
}

impl<N: Network> FromBytes for TransmissionResponse<N> {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        let transmission_id = TransmissionID::read_le(&mut reader)?;
        let transmission = Transmission::read_le(&mut reader)?;

        Ok(Self { transmission_id, transmission })
    }
}

#[cfg(any(test, feature = "fuzz-helpers"))]
pub mod prop_tests {
    #![cfg_attr(not(test), allow(unused_imports))]
    use crate::{
        TransmissionResponse,
        prop_tests::{any_solution_id, any_transaction_id, any_transmission_checksum},
    };
    use snarkvm::{
        console::prelude::{FromBytes, ToBytes},
        ledger::narwhal::{Data, Transmission, TransmissionID},
    };

    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use proptest::{
        collection,
        prelude::{BoxedStrategy, Strategy, any},
        prop_oneof,
    };
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    pub fn any_transmission() -> BoxedStrategy<(TransmissionID<CurrentNetwork>, Transmission<CurrentNetwork>)> {
        prop_oneof![
            (any_solution_id(), any_transmission_checksum(), collection::vec(any::<u8>(), 256..=256)).prop_map(
                |(pc, cs, bytes)| (
                    TransmissionID::Solution(pc, cs),
                    Transmission::Solution(Data::Buffer(Bytes::from(bytes)))
                )
            ),
            (any_transaction_id(), any_transmission_checksum(), collection::vec(any::<u8>(), 512..=512)).prop_map(
                |(tid, cs, bytes)| (
                    TransmissionID::Transaction(tid, cs),
                    Transmission::Transaction(Data::Buffer(Bytes::from(bytes)))
                )
            ),
        ]
        .boxed()
    }

    pub fn any_transmission_response() -> BoxedStrategy<TransmissionResponse<CurrentNetwork>> {
        any_transmission().prop_map(|(id, t)| TransmissionResponse::new(id, t)).boxed()
    }

    #[proptest]
    fn serialize_deserialize(#[strategy(any_transmission_response())] original: TransmissionResponse<CurrentNetwork>) {
        let mut buf = BytesMut::default().writer();
        TransmissionResponse::write_le(&original, &mut buf).unwrap();

        let deserialized = TransmissionResponse::read_le(buf.into_inner().reader()).unwrap();
        assert_eq!(original, deserialized);
    }
}
