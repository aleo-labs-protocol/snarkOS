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
pub struct BatchCertified<N: Network> {
    pub certificate: Data<BatchCertificate<N>>,
}

impl<N: Network> BatchCertified<N> {
    /// Initializes a new batch certified event.
    pub fn new(certificate: Data<BatchCertificate<N>>) -> Self {
        Self { certificate }
    }
}

impl<N: Network> From<BatchCertificate<N>> for BatchCertified<N> {
    /// Initializes a new batch certified event.
    fn from(certificate: BatchCertificate<N>) -> Self {
        Self::new(Data::Object(certificate))
    }
}

impl<N: Network> EventTrait for BatchCertified<N> {
    /// Returns the event name.
    #[inline]
    fn name(&self) -> Cow<'static, str> {
        "BatchCertified".into()
    }
}

impl<N: Network> ToBytes for BatchCertified<N> {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        self.certificate.write_le(&mut writer)?;
        Ok(())
    }
}

impl<N: Network> FromBytes for BatchCertified<N> {
    fn read_le<R: Read>(reader: R) -> IoResult<Self> {
        let certificate = Data::read_le(reader)?;

        Ok(Self { certificate })
    }
}

#[cfg(any(test, feature = "fuzz-helpers"))]
pub mod prop_tests {
    #![cfg_attr(not(test), allow(unused_imports))]
    use crate::{BatchCertified, certificate_response::prop_tests::any_batch_certificate};
    use snarkvm::console::prelude::{FromBytes, ToBytes};

    use bytes::{Buf, BufMut, BytesMut};
    use proptest::prelude::{BoxedStrategy, Strategy};
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    pub fn any_batch_certified() -> BoxedStrategy<BatchCertified<CurrentNetwork>> {
        any_batch_certificate().prop_map(BatchCertified::from).boxed()
    }

    #[proptest]
    fn serialize_deserialize(#[strategy(any_batch_certified())] original: BatchCertified<CurrentNetwork>) {
        let mut buf = BytesMut::default().writer();
        BatchCertified::write_le(&original, &mut buf).unwrap();

        let deserialized: BatchCertified<CurrentNetwork> = BatchCertified::read_le(buf.into_inner().reader()).unwrap();
        assert_eq!(
            original.certificate.deserialize_blocking().unwrap(),
            deserialized.certificate.deserialize_blocking().unwrap()
        );
    }
}
