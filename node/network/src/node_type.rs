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

use snarkvm::prelude::{FromBytes, ToBytes, error};

use serde::{Deserialize, Serialize};
use std::io;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[repr(u8)]
pub enum NodeType {
    /// A client node is a full node, capable of syncing with the network.
    Client = 0,
    /// A prover is a light node, capable of producing proofs for consensus.
    Prover,
    /// A validator is a full node, capable of validating blocks.
    Validator,
    /// A bootstrapclient is a light node dedicated to serving peer lists.
    BootstrapClient,
}

impl NodeType {
    /// Returns a string representation of the node type.
    pub const fn description(&self) -> &str {
        match self {
            Self::Client => "a client node",
            Self::Prover => "a prover node",
            Self::Validator => "a validator node",
            Self::BootstrapClient => "a bootstrap client node",
        }
    }

    /// Returns `true` if the node type is a client.
    pub const fn is_client(&self) -> bool {
        matches!(self, Self::Client)
    }

    /// Returns `true` if the node type is a prover.
    pub const fn is_prover(&self) -> bool {
        matches!(self, Self::Prover)
    }

    /// Returns `true` if the node type is a validator.
    pub const fn is_validator(&self) -> bool {
        matches!(self, Self::Validator)
    }
}

impl core::fmt::Display for NodeType {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", match self {
            Self::Client => "Client",
            Self::Prover => "Prover",
            Self::Validator => "Validator",
            Self::BootstrapClient => "Bootstrap Client",
        })
    }
}

impl ToBytes for NodeType {
    fn write_le<W: io::Write>(&self, writer: W) -> io::Result<()> {
        (*self as u8).write_le(writer)
    }
}

impl FromBytes for NodeType {
    fn read_le<R: io::Read>(reader: R) -> io::Result<Self> {
        match u8::read_le(reader)? {
            0 => Ok(Self::Client),
            1 => Ok(Self::Prover),
            2 => Ok(Self::Validator),
            3 => Ok(Self::BootstrapClient),
            x => Err(error(format!("Invalid node type: expected 0..=3, got {x}."))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variant, so that adding one to the enum without extending this list is a compile
    /// error rather than a silently untested case.
    const ALL: [NodeType; 4] = [NodeType::Client, NodeType::Prover, NodeType::Validator, NodeType::BootstrapClient];

    #[test]
    fn every_variant_round_trips_through_bytes() {
        for node_type in ALL {
            let bytes = node_type.to_bytes_le().unwrap();
            assert_eq!(NodeType::read_le(&*bytes).unwrap(), node_type);
        }
    }

    #[test]
    fn the_wire_discriminants_are_pinned() {
        // These bytes go out on the wire in every `ChallengeRequest` and `Ping`, so they are a
        // compatibility surface: reordering the variants silently repurposes them.
        assert_eq!(NodeType::Client.to_bytes_le().unwrap(), [0]);
        assert_eq!(NodeType::Prover.to_bytes_le().unwrap(), [1]);
        assert_eq!(NodeType::Validator.to_bytes_le().unwrap(), [2]);
        assert_eq!(NodeType::BootstrapClient.to_bytes_le().unwrap(), [3]);
    }

    #[test]
    fn each_discriminant_decodes_to_its_own_variant() {
        for (discriminant, expected) in ALL.iter().enumerate() {
            let decoded = NodeType::read_le(&[discriminant as u8][..]).unwrap();
            assert_eq!(decoded, *expected);
        }
    }

    #[test]
    fn an_unknown_discriminant_is_rejected() {
        // A peer is free to send anything here, so the first byte past the valid range and the
        // top of the byte space both have to be refused rather than mapped onto a variant.
        for discriminant in [ALL.len() as u8, 100, u8::MAX] {
            assert!(NodeType::read_le(&[discriminant][..]).is_err());
        }
    }

    #[test]
    fn a_truncated_encoding_is_rejected() {
        assert!(NodeType::read_le(&[][..]).is_err());
    }

    #[test]
    fn the_predicates_agree_with_the_variant() {
        assert!(NodeType::Client.is_client());
        assert!(NodeType::Prover.is_prover());
        assert!(NodeType::Validator.is_validator());

        // Each predicate matches exactly one variant.
        for node_type in ALL {
            let matches =
                [node_type.is_client(), node_type.is_prover(), node_type.is_validator()].iter().filter(|m| **m).count();
            assert!(matches <= 1, "{node_type} matched more than one predicate");
        }
    }

    #[test]
    fn a_bootstrap_client_is_not_a_client_by_the_predicates() {
        // Despite the name, `BootstrapClient` satisfies none of the predicates, and there is no
        // predicate of its own. Call sites that mean "any client" have to say so explicitly, as
        // `Client::on_connect` does by comparing the variant directly.
        assert!(!NodeType::BootstrapClient.is_client());
        assert!(!NodeType::BootstrapClient.is_prover());
        assert!(!NodeType::BootstrapClient.is_validator());
    }

    #[test]
    fn every_variant_has_a_distinct_description_and_display_form() {
        let descriptions: Vec<String> = ALL.iter().map(|node_type| node_type.description().to_string()).collect();
        let displays: Vec<String> = ALL.iter().map(|node_type| node_type.to_string()).collect();

        for list in [&descriptions, &displays] {
            for (i, entry) in list.iter().enumerate() {
                assert!(!entry.is_empty());
                assert!(!list[i + 1..].contains(entry), "duplicate label: {entry}");
            }
        }
    }
}
