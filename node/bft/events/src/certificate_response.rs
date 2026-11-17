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
pub struct CertificateResponse<N: Network> {
    pub certificate: BatchCertificate<N>,
}

impl<N: Network> CertificateResponse<N> {
    /// Initializes a new certificate response event.
    pub fn new(certificate: BatchCertificate<N>) -> Self {
        Self { certificate }
    }
}

impl<N: Network> From<BatchCertificate<N>> for CertificateResponse<N> {
    /// Initializes a new certificate response event.
    fn from(certificate: BatchCertificate<N>) -> Self {
        Self::new(certificate)
    }
}

impl<N: Network> EventTrait for CertificateResponse<N> {
    /// Returns the event name.
    #[inline]
    fn name(&self) -> Cow<'static, str> {
        "CertificateResponse".into()
    }
}

impl<N: Network> ToBytes for CertificateResponse<N> {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        self.certificate.write_le(&mut writer)?;
        Ok(())
    }
}

impl<N: Network> FromBytes for CertificateResponse<N> {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        let certificate = BatchCertificate::read_le(&mut reader)?;

        Ok(Self { certificate })
    }
}

#[cfg(any(test, feature = "fuzz-helpers"))]
pub mod prop_tests {
    #![cfg_attr(not(test), allow(unused_imports))]
    use crate::{
        CertificateResponse,
        committee_prop_tests::{CommitteeContext, ValidatorSet},
        prop_tests::now,
        transmission_response::prop_tests::any_transmission,
    };
    use snarkvm::{
        console::{
            account::Signature,
            prelude::{FromBytes, ToBytes},
        },
        ledger::narwhal::{BatchCertificate, BatchHeader},
        prelude::TestRng,
    };

    use bytes::{Buf, BufMut, BytesMut};
    use indexmap::IndexSet;
    use proptest::{
        collection::vec,
        prelude::{BoxedStrategy, Just, Strategy, any},
        sample::Selector,
    };
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    pub fn any_batch_header(committee: &CommitteeContext) -> BoxedStrategy<BatchHeader<CurrentNetwork>> {
        (Just(committee.clone()), any::<Selector>(), vec(any_transmission(), 0..16))
            .prop_map(|(committee, selector, transmissions)| {
                let mut rng = TestRng::default();
                let CommitteeContext(committee, ValidatorSet(validators)) = committee;
                let signer = selector.select(validators);
                let transmission_ids = transmissions.into_iter().map(|(id, _)| id).collect();

                BatchHeader::new(
                    &signer.private_key,
                    0,
                    now(),
                    committee.id(),
                    transmission_ids,
                    Default::default(),
                    &mut rng,
                )
                .unwrap()
            })
            .boxed()
    }

    pub fn any_batch_certificate() -> BoxedStrategy<BatchCertificate<CurrentNetwork>> {
        any::<CommitteeContext>()
            .prop_flat_map(|committee| (Just(committee.clone()), any_batch_header(&committee)))
            .prop_map(|(committee, batch_header)| {
                let CommitteeContext(_, mut validator_set) = committee;
                let mut rng = TestRng::default();

                // Remove the author from the validator set passed to create the batch
                // certificate, the author should not sign their own batch.
                validator_set.0.retain(|v| v.address != batch_header.author());
                BatchCertificate::from(batch_header.clone(), sign_batch_header(&validator_set, &batch_header, &mut rng))
                    .unwrap()
            })
            .boxed()
    }

    pub fn sign_batch_header(
        validator_set: &ValidatorSet,
        batch_header: &BatchHeader<CurrentNetwork>,
        rng: &mut TestRng,
    ) -> IndexSet<Signature<CurrentNetwork>> {
        let mut signatures = IndexSet::with_capacity(validator_set.0.len());
        for validator in validator_set.0.iter() {
            let private_key = validator.private_key;
            signatures.insert(private_key.sign(&[batch_header.batch_id()], rng).unwrap());
        }
        signatures
    }

    pub fn any_certificate_response() -> BoxedStrategy<CertificateResponse<CurrentNetwork>> {
        any_batch_certificate().prop_map(CertificateResponse::new).boxed()
    }

    #[proptest]
    fn serialize_deserialize(#[strategy(any_certificate_response())] original: CertificateResponse<CurrentNetwork>) {
        let mut buf = BytesMut::default().writer();
        CertificateResponse::write_le(&original, &mut buf).unwrap();

        let deserialized: CertificateResponse<CurrentNetwork> =
            CertificateResponse::read_le(buf.into_inner().reader()).unwrap();
        assert_eq!(original, deserialized);
    }
}
