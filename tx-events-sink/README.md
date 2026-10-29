# tx-events-sink

`tx-events-sink` receives the transaction events that snarkOS nodes send with `--tx-events`.
It keeps the events in memory and serves them over HTTP.
Use it to follow one transaction through one or more nodes.

The status that the sink derives from events is advisory.
The chain is the source of truth.

## Run

```bash
cargo run --release -p snarkos-tx-events-sink -- --udp 127.0.0.1:9150 --http 127.0.0.1:9151
snarkos start --tx-events 127.0.0.1:9150 ...
```

| option | default | meaning |
|---|---|---|
| `--udp` | `127.0.0.1:9150` | The UDP address that receives events. |
| `--http` | `127.0.0.1:9151` | The HTTP address that serves the API. |
| `--max-txs` | `10000` | The number of transactions kept. The least recently updated transaction is evicted first. |
| `--max-events-per-tx` | `64` | The number of events kept per transaction. The oldest event is evicted first. |

UDP is not authenticated.
Bind the sink to an address that only your nodes can reach.

## Node options

| option | meaning |
|---|---|
| `--tx-events <addr>` | Send events as JSON over UDP to `<addr>`. Off by default. |

The node never waits for the sink.
If a send fails, or block events queue up faster than the node sends them, the node counts the lost events in `emit_dropped` and continues.

## Event schema

Each datagram holds one JSON object per line.
A datagram is at most about 1200 bytes.

```json
{"v":1,"net":"mainnet","node":"aleo1...","ntype":"validator","boot":"9f3c...","seq":18233,
 "ts_ms":1793000000000,"height":1234567,"ev":"mempool_evicted","tx":"at1...","cks":"1234...",
 "kind":"execute","reason":"lower_fee","peer":null,"extra":{"by":"at1...","by_fee":5000}}
```

| field | meaning |
|---|---|
| `v` | The schema version. This document describes version 1. |
| `net` | The network: `mainnet`, `testnet` or `canary`. |
| `node` | The address of the node. |
| `ntype` | The node type: `validator`, `client`, `prover` or `bootstrap_client`. |
| `boot` | A random id for each start of the node process. |
| `seq` | A counter for each `boot`. A gap in `seq` is a lost event. |
| `ts_ms` | The wall clock of the node, in milliseconds since the Unix epoch. |
| `height` | The ledger height of the node. |
| `ev` | The event name. |
| `tx` | The transaction id. `null` for `heartbeat`. |
| `cks` | The transmission checksum, as a decimal string. `null` if the node did not compute it. |
| `kind` | `execute`, `deploy` or `fee`, if known. |
| `reason` | A fixed code. Never error text. |
| `peer` | Always `null` in version 1. |
| `extra` | Fields for each event, as listed below. |

The key of a transaction is `(tx, cks)`.
One id can arrive with several checksums.
Before a node checks a transaction, its id is only what the bytes claim.

### Events

| `ev` | sent by | `reason` | `extra` |
|---|---|---|---|
| `received` | REST, router | | `via`: `rest` or `router` |
| `checked` | REST, client relay, worker, proposer, peer-proposal and ping fetches | `check_failed` on failure | `site`: `rest`, `client_relay`, `worker`, `propose_batch`, `peer_proposal` or `worker_ping`; `outcome`: `ok` or `err`; `dur_us`; `err` (truncated text) |
| `gossiped` | REST, client, validator | | `site`: `rest`, `client_unverified`, `client_verified` or `validator` |
| `router_dropped` | router | `syncing`, `seen`, `bad_encoding` or `id_mismatch` | `via`: `router` |
| `mempool_inserted` | validator | | `prio_fee`; `qlen` |
| `mempool_refused` | validator | `recently_seen`, `already_queued`, `in_ledger`, `rejected_pool_full` or `rejected_fee_transaction` | |
| `mempool_evicted` | validator | `oldest_zero_fee` or `lower_fee` | `by` (the id that took the place); `by_fee` |
| `fetched` | validator | | `site`: `peer_proposal` or `worker_ping` |
| `propose_dropped` | validator | `spend_cost` or `spend_overflow` | |
| `proposed` | validator | | `round` |
| `certified` | validator | | `round`; `author` |
| `speculated` | validator | | `outcome`: `accepted`, `rejected` or `aborted`; `height` |
| `in_block` | validator, client | | `outcome`: `accepted`, `rejected` or `aborted`; `height` |
| `heartbeat` | all, every 10 s | | `emit_dropped` (lost events); `rest_status` (error responses of `POST /transaction/broadcast`, by status) |

`site` and `outcome` values match the labels of the Prometheus metrics at the same points.
Each event is one datagram, except that the events of one REST call, batch, certificate or block are packed into as few datagrams as fit.

The node sends no event for a REST request that returns an error status.
The caller already has that answer.
A `recently_seen` or `already_queued` refusal can return success to the caller, so the node sends it.
A validator sends every refusal of a transaction from a peer.
The source of a `mempool_inserted` or `mempool_refused` event is in the preceding `received` event of the same node.

`in_block` is sent when a node adds a block to its ledger, from consensus or from block sync.
Blocks loaded from the CDN at startup send no events.

## HTTP API

### `GET /tx/{id}/events`

Returns all events for the id, for all checksums.
The events are in order of `ts_ms`, then `node` and `seq`.
Returns `404` if the sink has no events for the id.

```json
{"tx":"at1...","evicted_events":0,"events":[{"v":1,"ev":"received",...}]}
```

### `GET /tx/{id}/status`

Returns the status that the sink derives from the events.
Returns `404` if the sink has no events for the id.

```json
{"tx":"at1...","status":"pending","advisory":true,"final":null,"stage":"certified",
 "holders":["aleo1..."],"last_drop":null,
 "checksums":{"1234...":{"stage":"certified","passed_check":true,"failed_check":false,
   "holders":["aleo1..."],"last_drop":null,"final":null}}}
```

| field | meaning |
|---|---|
| `status` | `accepted`, `rejected` or `aborted` after an `in_block` event. Otherwise `dropped`: every reporting node that held the transaction has released it, and at least one held it. Else `pending`. A drop from a node that never held the transaction appears only in `last_drop`. |
| `final` | The outcome, height, checksum and reporting nodes of the `in_block` event. `accepted` has priority over `rejected`, and `rejected` over `aborted`. |
| `stage` | The furthest stage: `seen`, `checked`, `mempool`, `proposed`, `certified`, `speculated` or `in_block`. |
| `holders` | The nodes whose last event shows that they hold the transaction. |
| `last_drop` | The most recent drop: `node`, `ev`, `reason`, `site` and `ts_ms`. |
| `checksums` | The same fields for each checksum. `unknown` collects the events that have no checksum. |

If one checksum of the id passed a check, the summary fields use only the checksums that passed a check.
A failure under a different checksum does not change them.

### `GET /stats`

Returns the counters of the sink, and for each node its `boot`, the `seq` range, the events received, the events lost, and the last heartbeat.
A late event from a previous `boot` of a node does not change these statistics.

`router_dropped`, `mempool_refused`, and `received` with `via: router` can carry an id that a peer claims.
The sink does not create an entry for such an event of an unknown id.
It counts the event in `unattached`.
