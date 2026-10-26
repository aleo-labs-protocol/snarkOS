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

pub(super) const COUNTER_NAMES: [&str; 4] = [
    bft::LEADERS_ELECTED,
    consensus::STALE_UNCONFIRMED_TRANSACTIONS,
    consensus::STALE_UNCONFIRMED_SOLUTIONS,
    consensus::DROPPED_TRANSACTIONS,
];

pub(super) const GAUGE_NAMES: [&str; 33] = [
    bft::CONNECTED,
    bft::CONNECTED_STAKE,
    bft::CONNECTED_STAKE_WITH_MATCHING_SHA,
    bft::CONNECTING,
    bft::LAST_STORED_ROUND,
    bft::PROPOSAL_ROUND,
    bft::CERTIFIED_BATCHES,
    bft::HEIGHT,
    bft::LAST_COMMITTED_ROUND,
    bft::IS_SYNCED,
    blocks::SOLUTIONS,
    blocks::TRANSACTIONS,
    blocks::ACCEPTED_DEPLOY,
    blocks::ACCEPTED_EXECUTE,
    blocks::REJECTED_DEPLOY,
    blocks::REJECTED_EXECUTE,
    blocks::ABORTED_TRANSACTIONS,
    blocks::ABORTED_SOLUTIONS,
    blocks::PROOF_TARGET,
    blocks::COINBASE_TARGET,
    blocks::CUMULATIVE_PROOF_TARGET,
    consensus::COMMITTED_CERTIFICATES,
    consensus::UNCONFIRMED_SOLUTIONS,
    consensus::UNCONFIRMED_TRANSACTIONS,
    consensus::DEPLOYMENTS_PRIORITY_QUEUE_SIZE,
    consensus::DEPLOYMENTS_ZERO_FEE_QUEUE_SIZE,
    consensus::EXECUTIONS_PRIORITY_QUEUE_SIZE,
    consensus::EXECUTIONS_ZERO_FEE_QUEUE_SIZE,
    consensus::VERSION,
    router::CONNECTED,
    router::CANDIDATE,
    router::RESTRICTED,
    tcp::QUEUED_INBOUND_MESSAGES,
];

pub(super) const HISTOGRAM_NAMES: [&str; 13] = [
    bft::COMMIT_ROUNDS_LATENCY,
    bft::COMMIT_LEADER_CERTIFICATE_LATENCY,
    bft::BATCH_CERTIFICATION_LATENCY,
    bft::SUBDAG_CERTIFICATES_PER_ROUND,
    bft::SUBDAG_CERTIFICATE_SIGNATURES,
    bft::SUBDAG_CERTIFICATE_PREVIOUS_REFS,
    bft::SUBDAG_ROUNDS_PER_BLOCK,
    consensus::CERTIFICATE_COMMIT_LATENCY,
    consensus::BLOCK_LATENCY,
    consensus::BLOCK_LAG,
    consensus::PREPARE_ADVANCE_SECS,
    consensus::CHECK_NEXT_BLOCK_SECS,
    consensus::ADVANCE_TO_NEXT_BLOCK_SECS,
];

pub mod bft {
    pub const BATCH_CERTIFICATION_LATENCY: &str = "snarkos_bft_batch_certification_latency_secs";
    pub const COMMIT_LEADER_CERTIFICATE_LATENCY: &str = "snarkos_bft_commit_leader_certificate_latency_secs";
    pub const COMMIT_ROUNDS_LATENCY: &str = "snarkos_bft_commit_rounds_latency_secs"; // <-- This one doesn't even make sense.
    pub const CONNECTED: &str = "snarkos_bft_connected_total";
    pub const CONNECTED_STAKE: &str = "snarkos_bft_connected_stake_as_percentage";
    pub const CONNECTED_STAKE_WITH_MATCHING_SHA: &str = "snarkos_bft_connected_stake_with_matching_sha_as_percentage";
    pub const CONNECTING: &str = "snarkos_bft_connecting_total";
    pub const LAST_STORED_ROUND: &str = "snarkos_bft_last_stored_round";
    pub const LEADERS_ELECTED: &str = "snarkos_bft_leaders_elected_total";
    pub const PROPOSAL_ROUND: &str = "snarkos_bft_primary_proposal_round";
    pub const CERTIFIED_BATCHES: &str = "snarkos_bft_primary_certified_batches";
    pub const HEIGHT: &str = "snarkos_bft_height_total";
    pub const LAST_COMMITTED_ROUND: &str = "snarkos_bft_last_committed_round";
    pub const IS_SYNCED: &str = "snarkos_bft_is_synced";
    /// The number of certificates in a round of a committed subdag (one observation per round).
    pub const SUBDAG_CERTIFICATES_PER_ROUND: &str = "snarkos_bft_subdag_certificates_per_round";
    /// The number of signatures on a certificate in a committed subdag (one observation per certificate).
    pub const SUBDAG_CERTIFICATE_SIGNATURES: &str = "snarkos_bft_subdag_certificate_signatures";
    /// The number of previous-round certificate references on a certificate in a committed subdag
    /// (one observation per certificate) — a proxy for DAG density/connectivity.
    pub const SUBDAG_CERTIFICATE_PREVIOUS_REFS: &str = "snarkos_bft_subdag_certificate_previous_refs";
    /// The number of rounds spanned by a committed subdag (one observation per committed block).
    pub const SUBDAG_ROUNDS_PER_BLOCK: &str = "snarkos_bft_subdag_rounds_per_block";
}

pub mod blocks {
    pub const TRANSACTIONS: &str = "snarkos_blocks_transactions_total";
    pub const SOLUTIONS: &str = "snarkos_blocks_solutions_total";
    pub const ACCEPTED_DEPLOY: &str = "snarkos_blocks_accepted_deploy";
    pub const ACCEPTED_EXECUTE: &str = "snarkos_blocks_accepted_execute";
    pub const REJECTED_DEPLOY: &str = "snarkos_blocks_rejected_deploy";
    pub const REJECTED_EXECUTE: &str = "snarkos_blocks_rejected_execute";
    pub const ABORTED_TRANSACTIONS: &str = "snarkos_blocks_aborted_transactions";
    pub const ABORTED_SOLUTIONS: &str = "snarkos_blocks_aborted_solutions";
    pub const PROOF_TARGET: &str = "snarkos_blocks_proof_target";
    pub const COINBASE_TARGET: &str = "snarkos_blocks_coinbase_target";
    pub const CUMULATIVE_PROOF_TARGET: &str = "snarkos_blocks_cumulative_proof_target";
}

pub mod consensus {
    pub const CERTIFICATE_COMMIT_LATENCY: &str = "snarkos_consensus_certificate_commit_latency_secs";
    pub const COMMITTED_CERTIFICATES: &str = "snarkos_consensus_committed_certificates_total";
    pub const BLOCK_LATENCY: &str = "snarkos_consensus_block_latency_secs";
    pub const BLOCK_LAG: &str = "snarkos_consensus_block_lag_secs";
    /// Time spent in prepare_advance_to_next_quorum_block (block construction).
    pub const PREPARE_ADVANCE_SECS: &str = "snarkos_consensus_prepare_advance_secs";
    /// Time spent in check_next_block.
    pub const CHECK_NEXT_BLOCK_SECS: &str = "snarkos_consensus_check_next_block_secs";
    /// Time spent in advance_to_next_block (ledger write).
    pub const ADVANCE_TO_NEXT_BLOCK_SECS: &str = "snarkos_consensus_advance_to_next_block_secs";
    pub const UNCONFIRMED_TRANSACTIONS: &str = "snarkos_consensus_unconfirmed_transactions_total";
    pub const UNCONFIRMED_SOLUTIONS: &str = "snarkos_consensus_unconfirmed_solutions_total";
    /// The active `ConsensusVersion` at the current block height.
    pub const VERSION: &str = "snarkos_consensus_version";
    pub const TRANSMISSION_LATENCY: &str = "snarkos_consensus_transmission_latency";
    pub const STALE_UNCONFIRMED_TRANSACTIONS: &str = "snarkos_consensus_stale_unconfirmed_transactions";
    pub const STALE_UNCONFIRMED_SOLUTIONS: &str = "snarkos_consensus_stale_unconfirmed_solutions";
    /// The number of transactions dropped by the mempool, either on insertion or by eviction.
    pub const DROPPED_TRANSACTIONS: &str = "snarkos_consensus_dropped_transactions";
    /// The number of deployment transactions in the priority queue.
    pub const DEPLOYMENTS_PRIORITY_QUEUE_SIZE: &str = "snarkos_consensus_deployments_priority_queue_size";
    /// The number of zero-fee deployment transactions in the fifo queue.
    pub const DEPLOYMENTS_ZERO_FEE_QUEUE_SIZE: &str = "snarkos_consensus_deployments_zero_fee_queue_size";
    /// The number of execution transactions in the priority queue.
    pub const EXECUTIONS_PRIORITY_QUEUE_SIZE: &str = "snarkos_consensus_executions_priority_queue_size";
    /// The number of zero-fee execution transactions in the fifo queue.
    pub const EXECUTIONS_ZERO_FEE_QUEUE_SIZE: &str = "snarkos_consensus_executions_zero_fee_queue_size";
    pub const VALIDATOR_CERTIFICATE_PARTICIPATION: &str = "snarkos_consensus_validator_certificate_participation";
    pub const VALIDATOR_SIGNATURE_PARTICIPATION: &str = "snarkos_consensus_validator_signature_participation";
    /// The garbage collection round the published participation scores were computed at.
    pub const VALIDATOR_PARTICIPATION_GC_ROUND: &str = "snarkos_consensus_validator_participation_gc_round";
    /// The number of telemetry updates dropped because the worker queue was full.
    pub const VALIDATOR_PARTICIPATION_DROPPED: &str = "snarkos_consensus_validator_participation_dropped_total";
}

pub mod router {
    pub const CONNECTED: &str = "snarkos_router_connected_total";
    pub const CANDIDATE: &str = "snarkos_router_candidate_total";
    pub const RESTRICTED: &str = "snarkos_router_restricted_total";
    /// Router messages, labeled by `direction` (`inbound` or `outbound`) and `type`.
    pub const EVENTS: &str = "snarkos_router_events_total";
}

pub mod gateway {
    /// Gateway events, labeled by `direction` (`inbound` or `outbound`) and `type`.
    pub const EVENTS: &str = "snarkos_gateway_events_total";
}

pub mod rest {
    /// REST requests, labeled by `method`, `endpoint` (the matched route), and `status`.
    pub const REQUESTS: &str = "snarkos_rest_requests_total";
    /// REST request latency in seconds, labeled by `method` and `endpoint`.
    pub const REQUEST_DURATION: &str = "snarkos_rest_request_duration_secs";
}

pub mod tcp {
    /// The number of inbound messages that have been read off a socket and are waiting to be
    /// processed, across all connections.
    pub const QUEUED_INBOUND_MESSAGES: &str = "snarkos_tcp_queued_inbound_messages";
}

pub mod build {
    pub const BUILD_INFO: &str = "snarkos_build_info";
}
