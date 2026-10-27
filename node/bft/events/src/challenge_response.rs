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
pub struct ChallengeResponse<N: Network> {
    pub restrictions_id: Field<N>,
    pub signature: Data<Signature<N>>,
    pub nonce: u64,
}

impl<N: Network> EventTrait for ChallengeResponse<N> {
    /// Returns the event name.
    #[inline]
    fn name(&self) -> Cow<'static, str> {
        "ChallengeResponse".into()
    }
}

impl<N: Network> ToBytes for ChallengeResponse<N> {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        self.restrictions_id.write_le(&mut writer)?;
        self.signature.write_le(&mut writer)?;
        self.nonce.write_le(&mut writer)?;
        Ok(())
    }
}

impl<N: Network> FromBytes for ChallengeResponse<N> {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        let restrictions_id = Field::read_le(&mut reader)?;
        let signature = Data::read_le(&mut reader)?;
        let nonce = u64::read_le(&mut reader)?;

        Ok(Self { restrictions_id, signature, nonce })
    }
}

#[cfg(test)]
pub mod prop_tests {
    use crate::ChallengeResponse;
    use snarkvm::{
        console::prelude::{FromBytes, ToBytes},
        ledger::narwhal::Data,
        prelude::{Field, PrivateKey, Signature},
        utilities::rand::{TestRng, Uniform},
    };

    use bytes::{Buf, BufMut, BytesMut};
    use proptest::prelude::{BoxedStrategy, ProptestConfig, Strategy, any};
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    pub fn any_restrictions_id() -> Field<CurrentNetwork> {
        Uniform::rand(&mut TestRng::default())
    }

    pub fn any_signature() -> BoxedStrategy<Signature<CurrentNetwork>> {
        (0..64)
            .prop_map(|message_size| {
                let rng = &mut TestRng::default();
                let message: Vec<_> = (0..message_size).map(|_| Uniform::rand(rng)).collect();
                let private_key = PrivateKey::new(rng).unwrap();
                Signature::sign(&private_key, &message, rng).unwrap()
            })
            .boxed()
    }

    pub fn any_challenge_response() -> BoxedStrategy<ChallengeResponse<CurrentNetwork>> {
        (any_signature(), any::<u64>())
            .prop_map(|(sig, nonce)| ChallengeResponse {
                restrictions_id: any_restrictions_id(),
                signature: Data::Object(sig),
                nonce,
            })
            .boxed()
    }

    #[proptest(ProptestConfig { cases: 10, ..ProptestConfig::default() })]
    fn serialize_deserialize(#[strategy(any_challenge_response())] original: ChallengeResponse<CurrentNetwork>) {
        let mut buf = BytesMut::default().writer();
        ChallengeResponse::write_le(&original, &mut buf).unwrap();

        let deserialized: ChallengeResponse<CurrentNetwork> =
            ChallengeResponse::read_le(buf.into_inner().reader()).unwrap();
        assert_eq!(
            original.signature.deserialize_blocking().unwrap(),
            deserialized.signature.deserialize_blocking().unwrap()
        );
    }
}
