# snarkos-node-bft

[![Crates.io](https://img.shields.io/crates/v/snarkos-node-bft.svg?color=neon)](https://crates.io/crates/snarkos-node-bft)
[![Authors](https://img.shields.io/badge/authors-Aleo-orange.svg)](https://aleo.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](./LICENSE.md)

The `snarkos-node-bft` crate provides a node implementation for a BFT-based memory pool.

## Primary

The primary is the coordinator, responsible for advancing rounds and broadcasting the anchor.

#### Round Advancement

A round advances once a quorum (`n - f`) of validators have submitted certificates for that round
and the following round-type-specific conditions are met:

- **Even rounds**: the elected leader's certificate is present among the quorum, confirming the
  leader was reachable. If the leader's certificate is absent, the node waits up to
  `MAX_LEADER_CERTIFICATE_DELAY` before advancing without it.
- **Odd rounds**: at least `f + 1` certificates from the current round reference the previous
  even round's leader certificate (availability threshold), or `n - f` do not (non-leader
  quorum). If neither threshold is reached, the node again falls back to the timeout.

In both cases the timeout is `MAX_LEADER_CERTIFICATE_DELAY` (currently 5 seconds), reset at the
start of each round. This follows the [Bullshark](https://arxiv.org/abs/2209.05633) protocol.

#### Batch Proposal

Batch proposals are driven by a dedicated **`ProposalTask`** that runs in a loop and is the only place that calls `Primary::propose_batch()`.
This keeps proposal on a single execution path and avoids concurrent proposal attempts. Each loop iteration covers the round of one batch and proceeds through three stages:

**Stage 1 — Wait until ready to propose**

The task blocks until all of the following conditions are satisfied:
1. The node is synced. If it is currently syncing, the task waits via `wait_for_synced_if_syncing()` before continuing.
2. `MIN_BATCH_DELAY` has elapsed since the last proposal, enforcing a minimum inter-proposal interval.
3. One of two events fires:
   - **Ready signal** — `ProposalTask::signal(round)` is called from `try_increment_to_the_next_round()` when the primary successfully advances to the new round `round` (e.g. after a leader certificate is committed). This is delivered via a `watch` channel that holds the highest signalled round. The task is ready when that round is above the round of its last proposal, so it keeps a signal that arrives while it proposes.
   - **`MAX_BATCH_DELAY` timeout** — If no signal arrives within `MAX_BATCH_DELAY` of the round start, the task proceeds anyway. This handles the case where the elected leader's certificate never arrives.

A short `CREATE_BATCH_INTERVAL` heartbeat keeps the round-change check alive while waiting.

**Stage 2 — Propose**

The task calls `propose_batch()` in a loop until it returns `Ok(Some(round))` (batch submitted for `round`). On `Ok(None)` or a transient error it retries, first after `CREATE_BATCH_INTERVAL` and then with a backoff of at most `MAX_BATCH_DELAY`. If the round advances during retries, the task restarts from Stage 1.

**Stage 3 — Wait for signatures**

Once the batch is broadcast, the task periodically calls `propose_batch()` every `MAX_BATCH_DELAY` to rebroadcast to any validators that have not yet signed. It exits this stage as soon as the round advances (detected either via the ready signal or by polling `current_round()`). If the round advances just before a rebroadcast, that call proposes the batch of the new round, and the task stays in this stage for that round.

### Ledger Advancement

The BFT module also advances the ledger as new certificates are added to the DAG. There are two different ways the ledger can advance.

#### 1. Consensus Path (Normal Operation)

When the node is actively participating in consensus and is synced with the network:

1. **Certificate Collection**: The Primary receives batch certificates from validators and passes them to the BFT using `add_new_certificate()`, which then updates the DAG.
2. **Leader Election**: Leaders are elected in even rounds. When a certificate arrives for round `r`, the BFT checks if the leader certificate for round `r-1` can be committed.
3. **Availability Threshold**: The leader certificate is ready to commit when the availability threshold is reached—i.e., enough validators in round `r` have included the leader's certificate in their previous certificate IDs.
4. **Commit Chain**: `commit_leader_certificate()` is called, which:
   - Walks backwards through the DAG to find all uncommitted leader certificates that are linked to the current one
   - Builds a subDAG containing all certificates to be committed
   - Sends the subDAG to the Consensus module via `tx_consensus_subdag`
5. **Block Creation**: The Consensus module receives the subDAG and calls `try_advance_to_next_block()`, which:
   - Calls `ledger.begin_ledger_update()` to obtain a LedgerUpdate (blocking other writers until the handle is dropped)
   - Uses the handle to prepare a new block from the subDAG and its transmissions (`prepare_advance_to_next_quorum_block()`), validate it (`check_next_block()`), and advance the ledger (`advance_to_next_block()`)
   - Drops the handle so the ledger lock is released

#### 2. Sync Path (Catching Up)

When the node is behind and syncing blocks from peers, the `bft::Sync` module handles synchronization. The behavior differs based on how far behind the node is:

##### Within GC Range (Normal Sync)

When the node is within the garbage collection range of the network tip:

1. **Block Reception**: `bft::Sync` requests and receives blocks from peer nodes via `BlockSync`.
2. **Block Verification**: Blocks are verified using `check_block_subdag()` and added to a queue of `pending_blocks`.
3. **Certificate Insertion**: Each certificate from the block's subDAG is added to storage via `sync_certificate_with_block()` and sent to the BFT. This populates the DAG with the certificates needed for consensus.
4. **BFT-Driven Ledger Advancement**: The BFT module handles block creation through its normal consensus path -- when enough certificates are added to the DAG, the BFT commits leader certificates and creates blocks just as it does during normal operation.
5. **Pending Block Cleanup**: When the ledger advances because of leader commits (see 4), pending blocks are removed from the queue.

##### Outside GC Range (Fast Sync)
When the node is too far behind (outside the GC range):

1. **Block Reception**: `bft::Sync` requests and receives blocks from peer nodes via `BlockSync` (same as with normal sync).
2. **Block Verification**: Blocks are verified using `check_block_subdag()` and added to a queue of `pending_blocks` (same as with normal sync).
3. **No DAG Updates**: Certificates are **not** added to the BFT's DAG, since they are too old to be useful for consensus.
4. **Availability Threshold Check**: The Sync module checks whether each pending block's leader certificate has reached the availability threshold via `is_block_availability_threshold_reached()`. This uses certificates from subsequent pending blocks that reference the leader certificate.
5. **Ledger Advancement**: Once the availability threshold is confirmed, the Sync module acquires a LedgerUpdate via `ledger.begin_ledger_update()` and, for each confirmed block in sequence, calls `ledger_update.check_block_content(pending_block)` and `ledger_update.advance_to_next_block(&block)`. It also updates storage height and round. The single update handle ensures no concurrent advancement from the consensus path while sync is applying these blocks.
6. **Transition to Normal Sync**: Once the node catches up to within the GC range, `sync_storage_with_ledger_at_bootup()` is called to populate the BFT DAG with recent certificates, and the node switches back to normal BFT-driven sync.

### Startup Initialization

When a node starts, the sync module reconstructs the BFT DAG for the most recent rounds from the ledger's disk state. This is handled by `Sync::initialize()`, which calls `sync_storage_with_ledger_at_bootup()`:

1. **Determine the GC Height**: The sync module calculates the earliest block height that corresponds to rounds not yet garbage collected. Since at most one block is created every two rounds, this is computed as:
   ```
   gc_height = latest_block_height - (max_gc_rounds / 2)
   ```

2. **Load Blocks from Ledger**: All blocks from `gc_height` to the latest block are retrieved from the ledger (RocksDB).

3. **Sync Storage State**: The in-memory storage is synchronized with the latest block:
   - `sync_height_with_block()` updates the current height
   - `sync_round_with_block()` updates the current round
   - `garbage_collect_certificates()` removes any stale certificates

4. **Reconstruct Certificate Storage**: For each block in the range, if it has a quorum authority (subDAG):
   - The unconfirmed transactions are reconstructed from the block's transactions
   - Each certificate from the subDAG is inserted into storage via `sync_certificate_with_block()`
   - This populates the in-memory certificate maps and persists transmissions that are missing from disk

5. **Populate the BFT DAG**: All certificates from the loaded blocks are passed to the BFT module via `add_certificate_from_sync` and marked as committed using `commit_certificate_from_sync`, so the BFT won't try to re-commit them.

6. **Set Sync Height**: Finally, `BlockSync::set_sync_height()` is called to inform the block sync module of the current synchronized height.

After initialization completes:
- The BFT DAG contains all certificates from recent blocks (within GC range)
- The storage contains the corresponding transmissions
- The node is ready to participate in consensus or continue syncing from peers

## Workers

The workers are simple entry replicators that receive transactions from the network and append them to their memory pool.

In order to function properly, workers must be synced to the latest round, and capable of performing verification
on the entries they receive from other validators' workers.

## Test Cases

- Two validators, one with X workers, another with Y workers. Check that they are compatible.
- If a primary sees that f+1 other primaries have certified this round, it should skip to the next round if it has not been certified yet.
- Ensure taking a set number of transmissions from workers leaves the remaining transmissions in place for the next round.
- Send back a mismatching transmission for a transmission ID, ensure it catches it.
- Send back a mismatching certificate for a certificate ID, ensure it catches it.

## Open Questions

1. How does one guarantee the number of accepted transactions and solutions does not exceed the block limits?
   - We need to set limits on the number of transmissions for the workers, but also the primary.
