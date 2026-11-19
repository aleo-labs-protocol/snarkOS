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
pub struct ValidatorsResponse<N: Network> {
    pub validators: IndexMap<SocketAddr, Address<N>>,
}

impl<N: Network> EventTrait for ValidatorsResponse<N> {
    /// Returns the event name.
    #[inline]
    fn name(&self) -> Cow<'static, str> {
        "ValidatorsResponse".into()
    }
}

impl<N: Network> ToBytes for ValidatorsResponse<N> {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        // Write the number of validators.
        u16::try_from(self.validators.len()).map_err(error)?.write_le(&mut writer)?;
        // Write the validators.
        for (socket_addr, address) in &self.validators {
            socket_addr.write_le(&mut writer)?;
            address.write_le(&mut writer)?;
        }
        Ok(())
    }
}

impl<N: Network> FromBytes for ValidatorsResponse<N> {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        // Read the number of validators.
        let num_validators = u16::read_le(&mut reader)?;
        let max_certificates = N::LATEST_MAX_CERTIFICATES();

        // Ensure the number of validators is within bounds
        if num_validators > max_certificates {
            return Err(error(format!(
                "Number of validators exceeds the maximum number of validators ({num_validators} > {max_certificates})",
            )));
        }

        // Read the validators.
        let mut validators = IndexMap::with_capacity(num_validators as usize);
        for _ in 0..num_validators {
            let socket_addr = SocketAddr::read_le(&mut reader)?;
            let address = Address::<N>::read_le(&mut reader)?;
            validators.insert(socket_addr, address);
        }
        Ok(Self { validators })
    }
}

#[cfg(any(test, feature = "fuzz-helpers"))]
pub mod prop_tests {
    #![cfg_attr(not(test), allow(unused_imports))]
    use crate::{ValidatorsResponse, challenge_request::prop_tests::any_valid_address};

    use bytes::{Buf, BufMut, BytesMut};
    use indexmap::IndexMap;
    use proptest::{
        collection::hash_map,
        prelude::{BoxedStrategy, Strategy, any},
    };
    use snarkvm::{
        prelude::{Address, Network, TestRng, Uniform},
        utilities::{FromBytes, ToBytes},
    };
    use std::{
        cmp::min,
        net::{IpAddr, Ipv4Addr, SocketAddr},
    };
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    pub fn any_valid_socket_addr() -> BoxedStrategy<SocketAddr> {
        any::<(IpAddr, u16)>().prop_map(|(ip_addr, port)| SocketAddr::new(ip_addr, port)).boxed()
    }

    pub fn any_index_map() -> BoxedStrategy<IndexMap<SocketAddr, Address<CurrentNetwork>>> {
        let max_certificates = min(CurrentNetwork::LATEST_MAX_CERTIFICATES() + 1, 50);

        hash_map(any_valid_socket_addr(), any_valid_address(), 0..(max_certificates as usize))
            .prop_map(|map| map.iter().map(|(k, v)| (*k, *v)).collect())
            .boxed()
    }

    pub fn any_validators_response() -> BoxedStrategy<ValidatorsResponse<CurrentNetwork>> {
        any_index_map().prop_map(|map| ValidatorsResponse { validators: map }).boxed()
    }

    #[proptest]
    fn validators_response_roundtrip(
        #[strategy(any_validators_response())] validators_response: ValidatorsResponse<CurrentNetwork>,
    ) {
        let mut bytes = BytesMut::default().writer();
        validators_response.write_le(&mut bytes).unwrap();
        let decoded = ValidatorsResponse::<CurrentNetwork>::read_le(&mut bytes.into_inner().reader()).unwrap();
        assert_eq![decoded, validators_response];
    }

    #[test]
    fn oversized_validators_response_roundtrip() {
        let mut rng = TestRng::default();

        let num_too_many_certificates = (CurrentNetwork::LATEST_MAX_CERTIFICATES() + 1) as usize;

        let mut too_many_certificates = IndexMap::with_capacity(num_too_many_certificates);
        for i in 0..num_too_many_certificates {
            let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4130 + i as u16);
            let address = Address::<CurrentNetwork>::rand(&mut rng);
            too_many_certificates.insert(socket_addr, address);
        }

        let oversized_validators_response = ValidatorsResponse { validators: too_many_certificates };

        let mut bytes = BytesMut::default().writer();
        oversized_validators_response.write_le(&mut bytes).unwrap();

        let error = ValidatorsResponse::<CurrentNetwork>::read_le(&mut bytes.into_inner().reader()).unwrap_err();
        assert!(error.to_string().contains("Number of validators exceeds the maximum number of validators"));
    }
}
