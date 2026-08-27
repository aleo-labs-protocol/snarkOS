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

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct BatchSignature<N: Network> {
    pub batch_id: Field<N>,
    pub signature: Signature<N>,
}

impl<N: Network> BatchSignature<N> {
    /// Initializes a new batch signature event.
    pub fn new(batch_id: Field<N>, signature: Signature<N>) -> Self {
        Self { batch_id, signature }
    }
}

impl<N: Network> EventTrait for BatchSignature<N> {
    /// Returns the event name.
    #[inline]
    fn name(&self) -> Cow<'static, str> {
        "BatchSignature".into()
    }
}

impl<N: Network> ToBytes for BatchSignature<N> {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        self.batch_id.write_le(&mut writer)?;
        self.signature.write_le(&mut writer)?;
        Ok(())
    }
}

impl<N: Network> FromBytes for BatchSignature<N> {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        let batch_id = Field::read_le(&mut reader)?;
        let signature = Signature::read_le(&mut reader)?;

        Ok(Self { batch_id, signature })
    }
}

#[cfg(any(test, feature = "fuzz-helpers"))]
pub mod prop_tests {
    #![cfg_attr(not(test), allow(unused_imports))]
    use crate::{
        BatchSignature,
        certificate_request::prop_tests::any_field,
        challenge_response::prop_tests::any_signature,
    };
    use snarkvm::console::prelude::{FromBytes, ToBytes};

    use bytes::{Buf, BufMut, BytesMut};
    use proptest::prelude::{BoxedStrategy, Strategy};
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    pub fn any_batch_signature() -> BoxedStrategy<BatchSignature<CurrentNetwork>> {
        (any_field(), any_signature())
            .prop_map(|(certificate_id, signature)| BatchSignature::new(certificate_id, signature))
            .boxed()
    }

    #[proptest]
    fn serialize_deserialize(#[strategy(any_batch_signature())] original: BatchSignature<CurrentNetwork>) {
        let mut buf = BytesMut::default().writer();
        BatchSignature::write_le(&original, &mut buf).unwrap();

        let deserialized: BatchSignature<CurrentNetwork> = BatchSignature::read_le(buf.into_inner().reader()).unwrap();
        assert_eq!(original, deserialized);
    }
}
