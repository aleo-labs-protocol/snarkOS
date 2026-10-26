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

use snarkvm::{
    ledger::{Transaction, narwhal::Data, puzzle::Solution},
    prelude::{FromBytes, Network, Result},
};

use anyhow::ensure;
use std::io::Cursor;

/// Deserializes a transaction, requiring raw buffers to contain exactly one serialized transaction.
///
/// For buffered transmissions, this rejects buffers that exceed the network transaction size limit or leave trailing
/// bytes after `Transaction::read_le` succeeds. Object transmissions are returned as-is.
pub fn deserialize_transaction_strict<N: Network>(transaction: Data<Transaction<N>>) -> Result<Transaction<N>> {
    match transaction {
        Data::Object(transaction) => Ok(transaction),
        Data::Buffer(bytes) => {
            ensure!(
                bytes.len() <= N::LATEST_MAX_TRANSACTION_SIZE(),
                "Transaction exceeds maximum size - {} bytes > {} bytes",
                bytes.len(),
                N::LATEST_MAX_TRANSACTION_SIZE()
            );

            let bytes_len = u64::try_from(bytes.len())?;
            let mut reader = Cursor::new(bytes.as_ref());
            let transaction = Transaction::<N>::read_le(&mut reader)?;
            ensure!(
                reader.position() == bytes_len,
                "Transaction buffer contains {} trailing bytes",
                bytes_len.saturating_sub(reader.position())
            );
            Ok(transaction)
        }
    }
}

/// Deserializes a solution, requiring raw buffers to contain exactly one serialized solution.
///
/// For buffered transmissions, this rejects buffers that leave trailing bytes after `Solution::read_le` succeeds.
/// Object transmissions are returned as-is.
pub fn deserialize_solution_strict<N: Network>(solution: Data<Solution<N>>) -> Result<Solution<N>> {
    match solution {
        Data::Object(solution) => Ok(solution),
        Data::Buffer(bytes) => {
            let bytes_len = u64::try_from(bytes.len())?;
            let mut reader = Cursor::new(bytes.as_ref());
            let solution = Solution::<N>::read_le(&mut reader)?;
            ensure!(
                reader.position() == bytes_len,
                "Solution buffer contains {} trailing bytes",
                bytes_len.saturating_sub(reader.position())
            );
            Ok(solution)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use snarkvm::{
        console::account::{Address, PrivateKey},
        ledger::{puzzle::PartialSolution, test_helpers::sample_execution_transaction_with_fee},
        prelude::{MainnetV0, Rng, TestRng, ToBytes},
    };

    type CurrentNetwork = MainnetV0;

    fn sample_solution(rng: &mut TestRng) -> Solution<CurrentNetwork> {
        let private_key = PrivateKey::<CurrentNetwork>::new(rng).unwrap();
        let address = Address::try_from(private_key).unwrap();
        let partial_solution = PartialSolution::new(rng.random(), address, rng.random()).unwrap();
        Solution::new(partial_solution, rng.random())
    }

    #[test]
    fn deserialize_transaction_strict_accepts_object() -> Result<()> {
        let rng = &mut TestRng::default();
        let expected = sample_execution_transaction_with_fee(false, rng, 0);

        let actual = deserialize_transaction_strict(Data::Object(expected.clone()))?;

        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn deserialize_transaction_strict_accepts_canonical_buffer() -> Result<()> {
        let rng = &mut TestRng::default();
        let expected = sample_execution_transaction_with_fee(false, rng, 0);
        let bytes = expected.to_bytes_le()?;

        let actual = deserialize_transaction_strict(Data::Buffer(bytes.into()))?;

        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn deserialize_transaction_strict_rejects_padded_buffer() -> Result<()> {
        let rng = &mut TestRng::default();
        let transaction = sample_execution_transaction_with_fee(false, rng, 0);
        let mut bytes = transaction.to_bytes_le()?;
        bytes.push(0);

        let result = deserialize_transaction_strict::<CurrentNetwork>(Data::Buffer(bytes.into()));

        assert!(result.unwrap_err().to_string().contains("trailing bytes"));
        Ok(())
    }

    #[test]
    fn deserialize_transaction_strict_rejects_oversize_buffer() {
        let bytes = vec![0; CurrentNetwork::LATEST_MAX_TRANSACTION_SIZE() + 1];

        let result = deserialize_transaction_strict::<CurrentNetwork>(Data::Buffer(bytes.into()));

        assert!(result.unwrap_err().to_string().contains("exceeds maximum size"));
    }

    #[test]
    fn deserialize_solution_strict_accepts_object() -> Result<()> {
        let rng = &mut TestRng::default();
        let expected = sample_solution(rng);

        let actual = deserialize_solution_strict(Data::Object(expected))?;

        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn deserialize_solution_strict_accepts_exact_buffer() -> Result<()> {
        let rng = &mut TestRng::default();
        let expected = sample_solution(rng);
        let bytes = expected.to_bytes_le()?;

        let actual = deserialize_solution_strict(Data::Buffer(bytes.into()))?;

        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn deserialize_solution_strict_rejects_padded_buffer() -> Result<()> {
        let rng = &mut TestRng::default();
        let solution = sample_solution(rng);
        let mut bytes = solution.to_bytes_le()?;
        bytes.push(0);

        let result = deserialize_solution_strict::<CurrentNetwork>(Data::Buffer(bytes.into()));

        assert!(result.unwrap_err().to_string().contains("trailing bytes"));
        Ok(())
    }
}
