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

//! Receives the transaction events that snarkOS nodes send with `--tx-events`, keeps them in
//! memory, and serves them over HTTP.
//!
//! The status derived from events is advisory. The chain is the source of truth.

use axum::{
    Json,
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use lru::LruCache;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::NonZeroUsize,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};
use tokio::net::{TcpListener, UdpSocket};

/// The largest datagram the sink reads.
const MAX_DATAGRAM_BYTES: usize = 65536;
/// The number of nodes the sink tracks; events from further nodes are rejected.
const MAX_NODES: usize = 4096;
/// The checksum key for events that carry no checksum.
const UNKNOWN_CHECKSUM: &str = "unknown";
/// The stages a transaction passes through, in order.
const STAGES: [&str; 7] = ["seen", "checked", "mempool", "proposed", "certified", "speculated", "in_block"];

/// One event, as a node sends it.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Event {
    pub v: u8,
    pub net: String,
    pub node: String,
    pub ntype: String,
    pub boot: String,
    pub seq: u64,
    pub ts_ms: u64,
    pub height: u32,
    pub ev: String,
    pub tx: Option<String>,
    pub cks: Option<String>,
    pub kind: Option<String>,
    pub reason: Option<String>,
    pub peer: Option<String>,
    #[serde(default)]
    pub extra: Value,
}

impl Event {
    fn extra_str(&self, field: &str) -> Option<&str> {
        self.extra.get(field).and_then(Value::as_str)
    }

    fn checksum_key(&self) -> &str {
        self.cks.as_deref().unwrap_or(UNKNOWN_CHECKSUM)
    }

    /// Events keyed by an id that a peer claims, before any check, for which the sink creates no new
    /// entry.
    fn is_claimed(&self) -> bool {
        self.ev == "router_dropped"
            || (matches!(self.ev.as_str(), "received" | "mempool_refused") && self.extra_str("via") == Some("router"))
    }

    fn stage(&self) -> Option<usize> {
        let stage = match self.ev.as_str() {
            "received" | "gossiped" | "fetched" | "router_dropped" | "mempool_refused" => 0,
            "checked" if self.extra_str("outcome") != Some("ok") => 0,
            "checked" if self.extra_str("site") == Some("worker") => 2,
            "checked" => 1,
            "mempool_inserted" | "mempool_evicted" | "propose_dropped" => 2,
            "proposed" => 3,
            "certified" => 4,
            "speculated" => 5,
            "in_block" => 6,
            _ => return None,
        };
        Some(stage)
    }

    /// Returns `true` if the event shows that the node holds the transaction for a batch.
    fn is_hold(&self) -> bool {
        match self.ev.as_str() {
            "mempool_inserted" | "fetched" | "proposed" | "certified" => true,
            "checked" => self.extra_str("site") == Some("worker") && self.extra_str("outcome") == Some("ok"),
            _ => false,
        }
    }

    /// Returns `true` if the event shows that the node no longer holds, or refused, the transaction.
    fn is_drop(&self) -> bool {
        match self.ev.as_str() {
            "mempool_evicted" | "propose_dropped" => true,
            // A failed background check leaves the transaction in the worker.
            "checked" => {
                self.extra_str("outcome") == Some("err")
                    && !matches!(self.extra_str("site"), Some("peer_proposal" | "worker_ping"))
            }
            "mempool_refused" => {
                matches!(self.reason.as_deref(), Some("rejected_pool_full" | "rejected_fee_transaction"))
            }
            "router_dropped" => matches!(self.reason.as_deref(), Some("syncing" | "bad_encoding" | "id_mismatch")),
            _ => false,
        }
    }
}

/// The memory limits of the sink.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// The number of transactions kept; the least recently updated one is evicted first.
    pub max_txs: NonZeroUsize,
    /// The number of events kept per transaction; the oldest one is evicted first.
    pub max_events_per_tx: usize,
}

/// Counts of what the sink received and discarded.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Counters {
    pub datagrams: u64,
    pub events: u64,
    pub heartbeats: u64,
    /// Lines that are not an event.
    pub malformed: u64,
    /// Events with a claimed id for a transaction with no entry.
    pub unattached: u64,
    /// Events from nodes beyond the node limit.
    pub rejected_nodes: u64,
    pub evicted_txs: u64,
    pub evicted_events: u64,
}

/// The sequence numbers seen from one node.
#[derive(Clone, Debug, Serialize)]
pub struct NodeStats {
    pub ntype: String,
    pub net: String,
    pub boot: String,
    pub boots: u64,
    pub min_seq: u64,
    pub max_seq: u64,
    pub received: u64,
    /// Events lost in transit: the gaps in `seq`, summed over all boots.
    pub lost: u64,
    #[serde(skip)]
    lost_previous_boots: u64,
    pub last_heartbeat: Option<Value>,
}

impl NodeStats {
    fn new(event: &Event) -> Self {
        Self {
            ntype: event.ntype.clone(),
            net: event.net.clone(),
            boot: event.boot.clone(),
            boots: 1,
            min_seq: event.seq,
            max_seq: event.seq,
            received: 0,
            lost: 0,
            lost_previous_boots: 0,
            last_heartbeat: None,
        }
    }

    fn lost_this_boot(&self) -> u64 {
        (self.max_seq - self.min_seq + 1).saturating_sub(self.received)
    }

    fn record(&mut self, event: &Event) {
        if self.boot != event.boot {
            self.lost_previous_boots += self.lost_this_boot();
            self.boot = event.boot.clone();
            self.boots += 1;
            self.min_seq = event.seq;
            self.max_seq = event.seq;
            self.received = 0;
        }
        self.min_seq = self.min_seq.min(event.seq);
        self.max_seq = self.max_seq.max(event.seq);
        self.received += 1;
        self.lost = self.lost_previous_boots + self.lost_this_boot();
    }
}

struct TxEntry {
    events: VecDeque<Event>,
    evicted_events: u64,
}

struct Store {
    txs: LruCache<String, TxEntry>,
    nodes: BTreeMap<String, NodeStats>,
    counters: Counters,
}

/// The in-memory event store.
pub struct Sink {
    store: Mutex<Store>,
    max_events_per_tx: usize,
}

impl Sink {
    pub fn new(limits: Limits) -> Self {
        let store = Store { txs: LruCache::new(limits.max_txs), nodes: BTreeMap::new(), counters: Counters::default() };
        Self { store: Mutex::new(store), max_events_per_tx: limits.max_events_per_tx.max(1) }
    }

    fn lock(&self) -> MutexGuard<'_, Store> {
        self.store.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Ingests one datagram of newline-delimited events.
    pub fn ingest(&self, datagram: &[u8]) {
        let mut store = self.lock();
        store.counters.datagrams += 1;
        for line in datagram.split(|byte| *byte == b'\n') {
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            match serde_json::from_slice::<Event>(line) {
                Ok(event) => self.insert(&mut store, event),
                Err(_) => store.counters.malformed += 1,
            }
        }
    }

    fn insert(&self, store: &mut Store, event: Event) {
        if !store.nodes.contains_key(&event.node) && store.nodes.len() >= MAX_NODES {
            store.counters.rejected_nodes += 1;
            return;
        }
        store.counters.events += 1;
        store.nodes.entry(event.node.clone()).or_insert_with(|| NodeStats::new(&event)).record(&event);

        if event.ev == "heartbeat" {
            store.counters.heartbeats += 1;
            if let Some(node) = store.nodes.get_mut(&event.node) {
                node.last_heartbeat = Some(event.extra);
            }
            return;
        }
        let Some(tx) = event.tx.clone() else {
            store.counters.malformed += 1;
            return;
        };
        if let Some(entry) = store.txs.get_mut(&tx) {
            if entry.events.len() >= self.max_events_per_tx {
                entry.events.pop_front();
                entry.evicted_events += 1;
                store.counters.evicted_events += 1;
            }
            entry.events.push_back(event);
        } else if event.is_claimed() {
            store.counters.unattached += 1;
        } else if store.txs.push(tx, TxEntry { events: VecDeque::from([event]), evicted_events: 0 }).is_some() {
            store.counters.evicted_txs += 1;
        }
    }

    /// Returns the events for the transaction, ordered by time, then node and sequence number.
    pub fn events(&self, tx: &str) -> Option<Value> {
        let store = self.lock();
        let entry = store.txs.peek(tx)?;
        let mut events: Vec<&Event> = entry.events.iter().collect();
        events.sort_by(|a, b| (a.ts_ms, &a.node, &a.boot, a.seq).cmp(&(b.ts_ms, &b.node, &b.boot, b.seq)));
        Some(json!({ "tx": tx, "evicted_events": entry.evicted_events, "events": events }))
    }

    /// Returns the status of the transaction derived from its events.
    pub fn status(&self, tx: &str) -> Option<TxStatus> {
        let store = self.lock();
        let entry = store.txs.peek(tx)?;
        Some(TxStatus::derive(tx, entry.events.iter()))
    }

    /// Returns the counters and the per-node sequence statistics.
    pub fn stats(&self) -> Value {
        let store = self.lock();
        json!({ "txs": store.txs.len(), "counters": store.counters, "nodes": store.nodes })
    }
}

/// The most recent drop of a transaction on one node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Drop {
    pub node: String,
    pub ev: String,
    pub reason: Option<String>,
    pub site: Option<String>,
    pub ts_ms: u64,
}

/// The outcome of a transaction in a block.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Final {
    pub outcome: String,
    pub height: Option<u64>,
    pub cks: Option<String>,
    pub nodes: BTreeSet<String>,
}

/// The status of one `(tx, cks)` key.
#[derive(Clone, Debug, Default, Serialize)]
pub struct KeyStatus {
    pub stage: Option<&'static str>,
    pub passed_check: bool,
    pub failed_check: bool,
    pub holders: BTreeSet<String>,
    pub last_drop: Option<Drop>,
    #[serde(rename = "final")]
    pub final_outcome: Option<Final>,
}

/// The status of a transaction id, over all of its checksums.
#[derive(Clone, Debug, Serialize)]
pub struct TxStatus {
    pub tx: String,
    /// `accepted`, `rejected` or `aborted` once a node reports the transaction in a block;
    /// otherwise `dropped` if no node holds it and one dropped it, else `pending`.
    pub status: &'static str,
    /// Always `true`: the chain is the source of truth.
    pub advisory: bool,
    #[serde(rename = "final")]
    pub final_outcome: Option<Final>,
    pub stage: Option<&'static str>,
    pub holders: BTreeSet<String>,
    pub last_drop: Option<Drop>,
    pub checksums: BTreeMap<String, KeyStatus>,
}

impl TxStatus {
    fn derive<'a>(tx: &str, events: impl Iterator<Item = &'a Event>) -> Self {
        let mut events: Vec<&Event> = events.collect();
        events.sort_by(|a, b| (&a.node, &a.boot, a.seq).cmp(&(&b.node, &b.boot, b.seq)));

        let mut checksums: BTreeMap<String, KeyStatus> = BTreeMap::new();
        let mut holding: BTreeMap<(String, String), bool> = BTreeMap::new();
        let mut stages: BTreeMap<String, usize> = BTreeMap::new();
        for event in &events {
            let key = event.checksum_key().to_string();
            let status = checksums.entry(key.clone()).or_default();
            if let Some(stage) = event.stage() {
                let furthest = stages.entry(key.clone()).or_insert(stage);
                *furthest = (*furthest).max(stage);
            }
            if event.ev == "checked" {
                match event.extra_str("outcome") {
                    Some("ok") => status.passed_check = true,
                    _ => status.failed_check = true,
                }
            }
            if event.is_hold() {
                holding.insert((key.clone(), event.node.clone()), true);
            } else if event.is_drop() {
                holding.insert((key.clone(), event.node.clone()), false);
                let drop = Drop {
                    node: event.node.clone(),
                    ev: event.ev.clone(),
                    reason: event.reason.clone(),
                    site: event.extra_str("site").map(str::to_string),
                    ts_ms: event.ts_ms,
                };
                if status.last_drop.as_ref().is_none_or(|last| last.ts_ms <= drop.ts_ms) {
                    status.last_drop = Some(drop);
                }
            }
            if event.ev == "in_block" {
                let outcome = event.extra_str("outcome").unwrap_or("unknown").to_string();
                let final_outcome = status.final_outcome.get_or_insert_with(|| Final {
                    outcome,
                    height: event.extra.get("height").and_then(Value::as_u64),
                    cks: event.cks.clone(),
                    nodes: BTreeSet::new(),
                });
                final_outcome.nodes.insert(event.node.clone());
            }
        }
        for ((key, node), holds) in holding {
            if let (true, Some(status)) = (holds, checksums.get_mut(&key)) {
                status.holders.insert(node);
            }
        }
        for (key, stage) in stages {
            if let Some(status) = checksums.get_mut(&key) {
                status.stage = Some(STAGES[stage]);
            }
        }

        // A key that has not passed a check does not affect a key that has.
        let any_passed = checksums.values().any(|status| status.passed_check);
        let considered: Vec<&KeyStatus> =
            checksums.values().filter(|status| !any_passed || status.passed_check).collect();

        let final_rank = |outcome: &str| match outcome {
            "accepted" => 0,
            "rejected" => 1,
            _ => 2,
        };
        let final_outcome = checksums
            .values()
            .filter_map(|status| status.final_outcome.as_ref())
            .min_by_key(|final_outcome| final_rank(&final_outcome.outcome))
            .cloned();
        let stage = considered
            .iter()
            .filter_map(|status| status.stage)
            .max_by_key(|stage| STAGES.iter().position(|candidate| candidate == stage));
        let holders: BTreeSet<String> = considered.iter().flat_map(|status| status.holders.iter().cloned()).collect();
        let last_drop =
            considered.iter().filter_map(|status| status.last_drop.as_ref()).max_by_key(|drop| drop.ts_ms).cloned();

        let status = match &final_outcome {
            Some(final_outcome) => match final_outcome.outcome.as_str() {
                "accepted" => "accepted",
                "rejected" => "rejected",
                _ => "aborted",
            },
            None if holders.is_empty() && last_drop.is_some() => "dropped",
            None => "pending",
        };
        let stage = if final_outcome.is_some() { Some("in_block") } else { stage };
        Self { tx: tx.to_string(), status, advisory: true, final_outcome, stage, holders, last_drop, checksums }
    }
}

/// Returns the HTTP routes of the sink.
pub fn router(sink: Arc<Sink>) -> Router {
    Router::new()
        .route("/tx/{id}/events", get(get_events))
        .route("/tx/{id}/status", get(get_status))
        .route("/stats", get(get_stats))
        .with_state(sink)
}

fn not_found() -> Response {
    (StatusCode::NOT_FOUND, Json(json!({ "error": "unknown transaction" }))).into_response()
}

async fn get_events(State(sink): State<Arc<Sink>>, Path(id): Path<String>) -> Response {
    sink.events(&id).map_or_else(not_found, |events| Json(events).into_response())
}

async fn get_status(State(sink): State<Arc<Sink>>, Path(id): Path<String>) -> Response {
    sink.status(&id).map_or_else(not_found, |status| Json(status).into_response())
}

async fn get_stats(State(sink): State<Arc<Sink>>) -> Json<Value> {
    Json(sink.stats())
}

/// Ingests the datagrams that arrive on `udp`, and serves the HTTP routes on `http`.
pub async fn run(udp: UdpSocket, http: TcpListener, sink: Arc<Sink>) -> anyhow::Result<()> {
    let ingest_sink = sink.clone();
    let ingest = tokio::spawn(async move {
        let mut buffer = vec![0u8; MAX_DATAGRAM_BYTES];
        loop {
            // A receive error concerns one datagram; the next one may succeed.
            if let Ok((size, _)) = udp.recv_from(&mut buffer).await {
                ingest_sink.ingest(&buffer[..size]);
            }
        }
    });
    let result = axum::serve(http, router(sink)).await;
    ingest.abort();
    Ok(result?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(max_txs: usize, max_events_per_tx: usize) -> Limits {
        Limits { max_txs: NonZeroUsize::new(max_txs).unwrap(), max_events_per_tx }
    }

    fn line(node: &str, seq: u64, ev: &str, tx: &str, cks: Option<&str>, reason: Option<&str>, extra: Value) -> String {
        json!({
            "v": 1, "net": "testnet", "node": node, "ntype": "validator", "boot": "b0", "seq": seq,
            "ts_ms": 1000 + seq, "height": 10, "ev": ev, "tx": tx, "cks": cks, "kind": "execute",
            "reason": reason, "peer": null, "extra": extra,
        })
        .to_string()
    }

    fn worker_check(node: &str, seq: u64, cks: &str, outcome: &str) -> String {
        line(node, seq, "checked", "at1x", Some(cks), None, json!({ "site": "worker", "outcome": outcome }))
    }

    #[test]
    fn test_ndjson_and_malformed_lines() {
        let sink = Sink::new(limits(10, 10));
        let datagram = [
            line("n1", 0, "received", "at1x", Some("1"), None, json!({ "via": "rest" })),
            "not json".to_string(),
            r#"{"ev":"checked"}"#.to_string(),
            String::new(),
            line("n1", 1, "mempool_inserted", "at1x", Some("1"), None, json!({})),
        ]
        .join("\n");
        sink.ingest(datagram.as_bytes());

        let stats = sink.stats();
        assert_eq!(stats["counters"]["events"], 2);
        assert_eq!(stats["counters"]["malformed"], 2);
        let events = sink.events("at1x").unwrap();
        assert_eq!(events["events"].as_array().unwrap().len(), 2);
        let status = sink.status("at1x").unwrap();
        assert_eq!(status.status, "pending");
        assert_eq!(status.stage, Some("mempool"));
        assert_eq!(status.holders, BTreeSet::from(["n1".to_string()]));
    }

    #[test]
    fn test_failure_of_other_checksum_does_not_change_status() {
        let sink = Sink::new(limits(10, 10));
        sink.ingest(worker_check("n1", 0, "1", "ok").as_bytes());
        sink.ingest(worker_check("n2", 0, "2", "err").as_bytes());

        let status = sink.status("at1x").unwrap();
        assert_eq!(status.status, "pending");
        assert_eq!(status.stage, Some("mempool"));
        assert_eq!(status.holders, BTreeSet::from(["n1".to_string()]));
        assert_eq!(status.last_drop, None);
        assert!(status.checksums["2"].failed_check);
        assert_eq!(status.checksums["2"].last_drop.as_ref().unwrap().node, "n2");
    }

    #[test]
    fn test_dropped_then_final() {
        let sink = Sink::new(limits(10, 10));
        sink.ingest(line("n1", 0, "mempool_inserted", "at1x", Some("1"), None, json!({})).as_bytes());
        sink.ingest(line("n1", 1, "mempool_evicted", "at1x", Some("1"), Some("lower_fee"), json!({})).as_bytes());
        let status = sink.status("at1x").unwrap();
        assert_eq!(status.status, "dropped");
        assert_eq!(status.last_drop.unwrap().reason.as_deref(), Some("lower_fee"));

        let in_block = |seq, cks, outcome| {
            line("n2", seq, "in_block", "at1x", cks, None, json!({ "outcome": outcome, "height": 11 }))
        };
        sink.ingest(in_block(0, Some("2"), "aborted").as_bytes());
        assert_eq!(sink.status("at1x").unwrap().status, "aborted");
        sink.ingest(in_block(1, Some("1"), "accepted").as_bytes());
        let status = sink.status("at1x").unwrap();
        assert_eq!(status.status, "accepted");
        assert_eq!(status.stage, Some("in_block"));
        let final_outcome = status.final_outcome.unwrap();
        assert_eq!(final_outcome.height, Some(11));
        assert_eq!(final_outcome.cks.as_deref(), Some("1"));
    }

    #[test]
    fn test_background_check_failure_keeps_holder() {
        let sink = Sink::new(limits(10, 10));
        sink.ingest(line("n1", 0, "fetched", "at1x", Some("1"), None, json!({ "site": "peer_proposal" })).as_bytes());
        let failed = json!({ "site": "peer_proposal", "outcome": "err" });
        sink.ingest(line("n1", 1, "checked", "at1x", Some("1"), Some("check_failed"), failed).as_bytes());
        let status = sink.status("at1x").unwrap();
        assert_eq!(status.status, "pending");
        assert_eq!(status.holders, BTreeSet::from(["n1".to_string()]));
    }

    #[test]
    fn test_claimed_events_do_not_create_entries() {
        let sink = Sink::new(limits(10, 10));
        let dropped = line("n1", 0, "router_dropped", "at1x", None, Some("seen"), json!({ "via": "router" }));
        sink.ingest(dropped.as_bytes());
        assert!(sink.status("at1x").is_none());
        assert_eq!(sink.stats()["counters"]["unattached"], 1);

        let refused =
            line("n2", 0, "mempool_refused", "at1y", Some("1"), Some("recently_seen"), json!({ "via": "rest" }));
        sink.ingest(refused.as_bytes());
        assert!(sink.status("at1y").is_some());

        sink.ingest(line("n1", 1, "gossiped", "at1x", Some("1"), None, json!({ "site": "rest" })).as_bytes());
        sink.ingest(line("n1", 2, "router_dropped", "at1x", None, Some("seen"), json!({})).as_bytes());
        assert_eq!(sink.events("at1x").unwrap()["events"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_memory_limits() {
        let sink = Sink::new(limits(2, 3));
        for seq in 0..5 {
            sink.ingest(line("n1", seq, "gossiped", "at1a", Some("1"), None, json!({})).as_bytes());
        }
        let events = sink.events("at1a").unwrap();
        assert_eq!(events["events"].as_array().unwrap().len(), 3);
        assert_eq!(events["evicted_events"], 2);
        assert_eq!(events["events"][0]["seq"], 2);

        sink.ingest(line("n1", 5, "gossiped", "at1b", Some("1"), None, json!({})).as_bytes());
        sink.ingest(line("n1", 6, "gossiped", "at1c", Some("1"), None, json!({})).as_bytes());
        assert!(sink.events("at1a").is_none());
        assert!(sink.events("at1b").is_some());
        assert!(sink.events("at1c").is_some());
        let stats = sink.stats();
        assert_eq!(stats["txs"], 2);
        assert_eq!(stats["counters"]["evicted_txs"], 1);
        assert_eq!(stats["counters"]["evicted_events"], 2);
    }

    #[test]
    fn test_sequence_gaps() {
        let sink = Sink::new(limits(10, 10));
        for seq in [0, 1, 4, 3, 7] {
            sink.ingest(line("n1", seq, "gossiped", "at1x", Some("1"), None, json!({})).as_bytes());
        }
        assert_eq!(sink.stats()["nodes"]["n1"]["lost"], 3);

        let restarted = line("n1", 0, "gossiped", "at1x", Some("1"), None, json!({})).replace("\"b0\"", "\"b1\"");
        sink.ingest(restarted.as_bytes());
        let stats = sink.stats();
        assert_eq!(stats["nodes"]["n1"]["boots"], 2);
        assert_eq!(stats["nodes"]["n1"]["lost"], 3);
    }

    #[test]
    fn test_heartbeat() {
        let sink = Sink::new(limits(10, 10));
        let heartbeat = json!({
            "v": 1, "net": "testnet", "node": "n1", "ntype": "client", "boot": "b0", "seq": 0, "ts_ms": 0,
            "height": 0, "ev": "heartbeat", "tx": null, "cks": null, "kind": null, "reason": null, "peer": null,
            "extra": { "emit_dropped": 4, "rest_status": { "429": 2 } },
        });
        sink.ingest(heartbeat.to_string().as_bytes());
        let stats = sink.stats();
        assert_eq!(stats["counters"]["heartbeats"], 1);
        assert_eq!(stats["nodes"]["n1"]["last_heartbeat"]["emit_dropped"], 4);
    }
}
