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

use snarkvm::{
    ledger::narwhal::{DATA_ENCODING_OVERHEAD, Data},
    prelude::{Field, FromBytes, ToBytes},
};

use std::borrow::Cow;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnconfirmedTransaction<N: Network> {
    pub transaction_id: N::TransactionID,
    pub transaction: Data<Transaction<N>>,
}

impl<N: Network> UnconfirmedTransaction<N> {
    /// The bytes an `UnconfirmedTransaction` frame carries in addition to the transaction itself:
    /// the message ID that precedes every message payload (written by `Message::write_le` in
    /// lib.rs, not here), `transaction_id`, and the `Data` envelope around `transaction` that
    /// `write_le` below emits.
    ///
    /// Each term names what defines it rather than a literal, so widening any of them is a
    /// compile-time change here too. Adding or removing a *field* still isn't, which is why
    /// `unconfirmed_transaction_overhead_matches_the_real_encoding` pins the total against the
    /// actual serializer rather than trusting this arithmetic on its own - getting it wrong would
    /// silently reintroduce the mismatch `check_size` was fixed to avoid.
    pub const OVERHEAD: usize = size_of::<u16>() // the message ID
        + Field::<N>::SIZE_IN_BYTES // `transaction_id`, an `AleoID` wrapping one field element
        + DATA_ENCODING_OVERHEAD; // `Data`'s envelope around `transaction`
}

impl<N: Network> From<Transaction<N>> for UnconfirmedTransaction<N> {
    /// Initializes a new `UnconfirmedTransaction` message.
    fn from(transaction: Transaction<N>) -> Self {
        Self { transaction_id: transaction.id(), transaction: Data::Object(transaction) }
    }
}

impl<N: Network> MessageTrait for UnconfirmedTransaction<N> {
    /// Returns the message name.
    #[inline]
    fn name(&self) -> Cow<'static, str> {
        "UnconfirmedTransaction".into()
    }
}

impl<N: Network> ToBytes for UnconfirmedTransaction<N> {
    fn write_le<W: io::Write>(&self, mut writer: W) -> io::Result<()> {
        self.transaction_id.write_le(&mut writer)?;
        self.transaction.write_le(&mut writer)?;
        Ok(())
    }
}

impl<N: Network> FromBytes for UnconfirmedTransaction<N> {
    fn read_le<R: io::Read>(mut reader: R) -> io::Result<Self> {
        Ok(Self { transaction_id: N::TransactionID::read_le(&mut reader)?, transaction: Data::read_le(reader)? })
    }
}

#[cfg(test)]
pub mod prop_tests {
    use crate::{Transaction, UnconfirmedTransaction};
    use snarkvm::{
        ledger::{narwhal::Data, test_helpers::sample_fee_public_transaction},
        prelude::{Field, FromBytes, Network, TestRng, ToBytes, Uniform},
    };

    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use proptest::prelude::{BoxedStrategy, Strategy, any};
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    pub fn any_transaction() -> BoxedStrategy<Transaction<CurrentNetwork>> {
        any::<u64>()
            .prop_map(|seed| {
                let mut rng = TestRng::fixed(seed);
                sample_fee_public_transaction(&mut rng)
            })
            .boxed()
    }

    pub fn any_unconfirmed_transaction() -> BoxedStrategy<UnconfirmedTransaction<CurrentNetwork>> {
        any_transaction()
            .prop_map(|tx| UnconfirmedTransaction { transaction_id: tx.id(), transaction: Data::Object(tx) })
            .boxed()
    }

    /// Fills a `Vec<u8>` of the given length, eight bytes per RNG call rather than one, since the
    /// callers below need this at up to `LATEST_MAX_TRANSACTION_SIZE` (hundreds of KB).
    fn random_bytes(rng: &mut TestRng, len: usize) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(len);
        while bytes.len() < len {
            bytes.extend_from_slice(&u64::rand(rng).to_le_bytes());
        }
        bytes.truncate(len);
        bytes
    }

    /// Builds an `UnconfirmedTransaction` whose `Data::Buffer` is exactly `len` bytes of junk -
    /// content doesn't matter for a pure frame-size test.
    fn unconfirmed_transaction_of_size(seed: u64, len: usize) -> UnconfirmedTransaction<CurrentNetwork> {
        let mut rng = TestRng::fixed(seed);
        let tx_id_field = Field::<CurrentNetwork>::rand(&mut rng);
        UnconfirmedTransaction {
            transaction_id: tx_id_field.into(),
            transaction: Data::Buffer(Bytes::from(random_bytes(&mut rng, len))),
        }
    }

    /// A transaction body one byte larger than the cap allows - genuinely too large, since the
    /// frame carrying it exceeds `LATEST_MAX_TRANSACTION_SIZE` plus its envelope either way.
    pub fn any_large_unconfirmed_transaction() -> BoxedStrategy<UnconfirmedTransaction<CurrentNetwork>> {
        any::<u64>()
            .prop_map(|seed| unconfirmed_transaction_of_size(seed, CurrentNetwork::LATEST_MAX_TRANSACTION_SIZE() + 1))
            .boxed()
    }

    /// A transaction body of exactly `LATEST_MAX_TRANSACTION_SIZE` - the largest a transaction is
    /// actually allowed to be, per the ledger service and the REST endpoint. The frame carrying
    /// it is necessarily larger than the transaction itself (message ID, transaction ID, and
    /// `Data`'s own tag and length prefix), so this is the case `check_size` must still accept.
    pub fn any_max_size_unconfirmed_transaction() -> BoxedStrategy<UnconfirmedTransaction<CurrentNetwork>> {
        any::<u64>()
            .prop_map(|seed| unconfirmed_transaction_of_size(seed, CurrentNetwork::LATEST_MAX_TRANSACTION_SIZE()))
            .boxed()
    }

    #[proptest]
    fn unconfirmed_transaction_roundtrip(
        #[strategy(any_unconfirmed_transaction())] original: UnconfirmedTransaction<CurrentNetwork>,
    ) {
        let mut buf = BytesMut::default().writer();
        UnconfirmedTransaction::write_le(&original, &mut buf).unwrap();

        let deserialized: UnconfirmedTransaction<CurrentNetwork> =
            UnconfirmedTransaction::read_le(buf.into_inner().reader()).unwrap();
        assert_eq!(original.transaction_id, deserialized.transaction_id);
        assert_eq!(
            original.transaction.deserialize_blocking().unwrap(),
            deserialized.transaction.deserialize_blocking().unwrap(),
        );
    }
}
