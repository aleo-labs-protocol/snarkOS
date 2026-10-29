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

//! Per-transaction events, sent as JSON lines over UDP to an address set with `--tx-events`.
//!
//! Delivery is best-effort: an event whose send fails is counted in the `emit_dropped` field of the
//! next heartbeat and is otherwise ignored. Each line carries the process's `boot` id and a `seq`
//! number, so a receiver can count lost events.

use serde::Serialize;
pub use serde_json::{Value, json};
use snarkvm::{
    ledger::{
        ConfirmedTransaction,
        authority::Authority,
        narwhal::{Data, TransmissionID},
    },
    prelude::{Block, Network, ToBytes, Transaction},
};
use std::{
    cell::RefCell,
    collections::HashMap,
    hash::{BuildHasher, RandomState},
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket},
    sync::{
        OnceLock,
        atomic::{AtomicU32, AtomicU64, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// The schema version in the `v` field.
const SCHEMA_VERSION: u8 = 1;
/// The size up to which lines are packed into one datagram. A datagram of this size is not
/// fragmented on a path with a 1280-byte MTU or larger.
const MAX_DATAGRAM_BYTES: usize = 1200;
/// The interval between heartbeats.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
/// The length to which error text in `extra.err` is truncated.
const MAX_ERR_BYTES: usize = 256;
/// The lowest HTTP status counted in a heartbeat's `rest_status`.
const MIN_ERROR_STATUS: u16 = 400;
/// The number of blocks queued for the background thread; the events of further blocks are dropped.
const MAX_QUEUED_BLOCKS: usize = 256;

static TX_EVENTS: OnceLock<TxEvents> = OnceLock::new();
static HEIGHT: AtomicU32 = AtomicU32::new(0);

thread_local! {
    static LINE: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(MAX_DATAGRAM_BYTES));
}

/// Returns the emitter, or `None` if `--tx-events` is not set.
pub fn get() -> Option<&'static TxEvents> {
    TX_EVENTS.get()
}

/// Sets the ledger height reported in the `height` field.
pub fn set_height(height: u32) {
    HEIGHT.store(height, Ordering::Relaxed);
}

/// Returns the `height` and `ts_ms` fields for an event sent now.
fn stamp() -> (u32, u64) {
    let ts_ms = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |since| since.as_millis() as u64);
    (HEIGHT.load(Ordering::Relaxed), ts_ms)
}

/// Starts sending events to `target`, and a heartbeat and the block events from a background thread.
pub fn initialize(target: SocketAddr, net: &'static str, node: String, ntype: &'static str) -> io::Result<()> {
    let bind: SocketAddr =
        if target.is_ipv4() { (Ipv4Addr::UNSPECIFIED, 0).into() } else { (Ipv6Addr::UNSPECIFIED, 0).into() };
    let socket = UdpSocket::bind(bind)?;
    socket.connect(target)?;
    socket.set_nonblocking(true)?;

    let boot = format!("{:016x}", RandomState::new().hash_one(SystemTime::now()));
    let (blocks, queued_blocks) = mpsc::sync_channel(MAX_QUEUED_BLOCKS);
    let mut fresh = false;
    let events = TX_EVENTS.get_or_init(|| {
        fresh = true;
        TxEvents {
            socket,
            net,
            node,
            ntype,
            boot,
            seq: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            rest_status: std::array::from_fn(|_| AtomicU64::new(0)),
            blocks,
        }
    });
    if !fresh {
        return Err(io::Error::new(io::ErrorKind::AlreadyExists, "transaction events are already initialized"));
    }
    thread::Builder::new().name("tx-events".to_string()).spawn(move || events.run(queued_blocks))?;
    Ok(())
}

/// One transaction event. The envelope fields are added when it is sent.
#[derive(Clone)]
pub struct TxEvent {
    pub ev: &'static str,
    pub tx: Option<String>,
    pub cks: Option<String>,
    pub kind: Option<&'static str>,
    pub reason: Option<&'static str>,
    pub extra: Value,
}

impl TxEvent {
    /// Returns an event named `ev` for the transaction, with no `kind`, `reason` or `extra`.
    pub fn new<N: Network>(
        ev: &'static str,
        transaction_id: &N::TransactionID,
        checksum: Option<N::TransmissionChecksum>,
    ) -> Self {
        Self {
            ev,
            tx: Some(transaction_id.to_string()),
            cks: checksum.map(|checksum| checksum.to_string()),
            kind: None,
            reason: None,
            extra: Value::Null,
        }
    }

    /// Returns an event named `ev` with `extra` for a transaction transmission, or `None` for other
    /// transmissions.
    pub fn for_transmission<N: Network>(
        ev: &'static str,
        transmission_id: &TransmissionID<N>,
        extra: Value,
    ) -> Option<Self> {
        let transaction_id = transmission_id.transaction()?;
        Some(Self { extra, ..Self::new::<N>(ev, &transaction_id, transmission_id.checksum()) })
    }

    /// Returns a `checked` event for a transaction check at `site`.
    pub fn checked<N: Network, T, E: std::fmt::Display>(
        site: &'static str,
        kind: &'static str,
        transaction_id: &N::TransactionID,
        checksum: Option<N::TransmissionChecksum>,
        result: &Result<T, E>,
        elapsed: Duration,
    ) -> Self {
        let dur_us = elapsed.as_micros() as u64;
        let (reason, extra) = match result {
            Ok(_) => (None, json!({ "site": site, "outcome": "ok", "dur_us": dur_us })),
            Err(err) => {
                let err = err.to_string();
                let err = &err[..err.floor_char_boundary(MAX_ERR_BYTES)];
                (Some("check_failed"), json!({ "site": site, "outcome": "err", "dur_us": dur_us, "err": err }))
            }
        };
        Self { kind: Some(kind), reason, extra, ..Self::new::<N>("checked", transaction_id, checksum) }
    }
}

#[derive(Serialize)]
struct Envelope<'a> {
    v: u8,
    net: &'a str,
    node: &'a str,
    ntype: &'a str,
    boot: &'a str,
    seq: u64,
    ts_ms: u64,
    height: u32,
    ev: &'a str,
    tx: Option<&'a str>,
    cks: Option<&'a str>,
    kind: Option<&'a str>,
    reason: Option<&'a str>,
    peer: Option<&'a str>,
    extra: &'a Value,
}

/// The events of one block, for the background thread.
struct BlockJob {
    ev: &'static str,
    hash: String,
    num_events: u64,
    stamp: (u32, u64),
    outcomes: Box<dyn FnOnce() -> Vec<TxEvent> + Send>,
}

/// The process-wide event emitter.
pub struct TxEvents {
    socket: UdpSocket,
    net: &'static str,
    node: String,
    ntype: &'static str,
    boot: String,
    seq: AtomicU64,
    dropped: AtomicU64,
    rest_status: [AtomicU64; 200],
    blocks: SyncSender<BlockJob>,
}

impl TxEvents {
    /// Sends the event in its own datagram.
    pub fn emit(&self, event: TxEvent) {
        LINE.with_borrow_mut(|line| {
            line.clear();
            self.encode(&event, stamp(), line);
            self.send(line, 1);
        });
    }

    /// Sends the events, packed into as few datagrams as fit.
    pub fn emit_all(&self, events: impl IntoIterator<Item = TxEvent>) {
        self.emit_all_at(events, stamp());
    }

    fn emit_all_at(&self, events: impl IntoIterator<Item = TxEvent>, stamp: (u32, u64)) {
        let mut datagram = Vec::with_capacity(MAX_DATAGRAM_BYTES);
        let mut lines = 0;
        LINE.with_borrow_mut(|line| {
            for event in events {
                line.clear();
                self.encode(&event, stamp, line);
                self.pack(&mut datagram, &mut lines, line);
            }
        });
        if lines > 0 {
            self.send(&datagram, lines);
        }
    }

    /// Counts a REST response with an error status, for the next heartbeat.
    pub fn count_rest_status(&self, status: u16) {
        if let Some(count) = status.checked_sub(MIN_ERROR_STATUS).and_then(|i| self.rest_status.get(i as usize)) {
            count.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn encode(&self, event: &TxEvent, (height, ts_ms): (u32, u64), out: &mut Vec<u8>) {
        let envelope = Envelope {
            v: SCHEMA_VERSION,
            net: self.net,
            node: &self.node,
            ntype: self.ntype,
            boot: &self.boot,
            seq: self.seq.fetch_add(1, Ordering::Relaxed),
            ts_ms,
            height,
            ev: event.ev,
            tx: event.tx.as_deref(),
            cks: event.cks.as_deref(),
            kind: event.kind,
            reason: event.reason,
            peer: None,
            extra: &event.extra,
        };
        // Serializing a struct of strings, integers and a `Value` into a `Vec` cannot fail.
        if serde_json::to_writer(&mut *out, &envelope).is_err() {
            out.clear();
        }
    }

    /// Appends `line` to `datagram`, which holds `lines` lines, first sending `datagram` if `line`
    /// would not fit.
    fn pack(&self, datagram: &mut Vec<u8>, lines: &mut u64, line: &[u8]) {
        if !datagram.is_empty() && datagram.len() + 1 + line.len() > MAX_DATAGRAM_BYTES {
            self.send(datagram, *lines);
            datagram.clear();
            *lines = 0;
        }
        if !datagram.is_empty() {
            datagram.push(b'\n');
        }
        datagram.extend_from_slice(line);
        *lines += 1;
    }

    /// Sends `datagram`, which holds `lines` lines, or counts them as dropped.
    fn send(&self, datagram: &[u8], lines: u64) {
        if datagram.is_empty() || self.socket.send(datagram).is_err() {
            self.dropped.fetch_add(lines, Ordering::Relaxed);
        }
    }

    /// Queues the block for the background thread, or counts its events as dropped.
    fn queue_block(&self, job: BlockJob) {
        if let Err(TrySendError::Full(job) | TrySendError::Disconnected(job)) = self.blocks.try_send(job) {
            self.dropped.fetch_add(job.num_events, Ordering::Relaxed);
        }
    }

    fn heartbeat(&self) {
        let rest_status: serde_json::Map<String, Value> = self
            .rest_status
            .iter()
            .enumerate()
            .filter_map(|(i, count)| {
                let count = count.load(Ordering::Relaxed);
                (count > 0).then(|| ((MIN_ERROR_STATUS as usize + i).to_string(), count.into()))
            })
            .collect();
        let extra = json!({
            "emit_dropped": self.dropped.load(Ordering::Relaxed),
            "rest_status": rest_status,
        });
        self.emit(TxEvent { ev: "heartbeat", tx: None, cks: None, kind: None, reason: None, extra });
    }

    /// Sends a heartbeat every `HEARTBEAT_INTERVAL`, and the events of the queued blocks. The
    /// outcomes of the last block are reused for the next job of the same block.
    fn run(&self, blocks: Receiver<BlockJob>) {
        let mut last_block: Option<(String, Vec<TxEvent>)> = None;
        let mut next_heartbeat = Instant::now();
        loop {
            if Instant::now() >= next_heartbeat {
                self.heartbeat();
                next_heartbeat = Instant::now() + HEARTBEAT_INTERVAL;
            }
            // The sender lives as long as `self`, so this fails only on a timeout.
            let Ok(job) = blocks.recv_timeout(next_heartbeat.saturating_duration_since(Instant::now())) else {
                continue;
            };
            let outcomes = match last_block.take() {
                Some((hash, outcomes)) if hash == job.hash => outcomes,
                _ => (job.outcomes)(),
            };
            self.emit_all_at(outcomes.iter().map(|event| TxEvent { ev: job.ev, ..event.clone() }), job.stamp);
            last_block = Some((job.hash, outcomes));
        }
    }
}

/// Returns the transmission checksum of the transaction, as the memory pool and the BFT compute it.
pub fn transaction_checksum<N: Network>(transaction: &Transaction<N>) -> Option<N::TransmissionChecksum> {
    Data::<Transaction<N>>::Buffer(transaction.to_bytes_le().ok()?.into()).to_checksum::<N>().ok()
}

/// Emits one event named `ev` per transaction in the block, including aborted transactions, with
/// `extra.outcome` set to `accepted`, `rejected` or `aborted`. This copies the ids and checksums, and
/// clones only the transactions whose id or checksum is costly to find. The background thread builds
/// and sends the events.
pub fn emit_block_outcomes<N: Network>(ev: &'static str, block: &Block<N>) {
    let Some(events) = get() else {
        return;
    };
    let mut checksums: HashMap<N::TransactionID, Vec<N::TransmissionChecksum>> =
        HashMap::with_capacity(block.transactions().len() + block.aborted_transaction_ids().len());
    if let Authority::Quorum(subdag) = block.authority() {
        for transmission_id in subdag.transmission_ids() {
            if let TransmissionID::Transaction(transaction_id, checksum) = transmission_id {
                checksums.entry(*transaction_id).or_default().push(*checksum);
            }
        }
    }
    // A rejected transaction, or an id that appears with several checksums, needs the transaction.
    let confirmed: Vec<_> = block
        .transactions()
        .iter()
        .map(|confirmed| {
            let (kind, outcome, transaction_id) = match confirmed {
                ConfirmedTransaction::AcceptedDeploy(_, transaction, _) => {
                    ("deploy", "accepted", Some(transaction.id()))
                }
                ConfirmedTransaction::AcceptedExecute(_, transaction, _) => {
                    ("execute", "accepted", Some(transaction.id()))
                }
                ConfirmedTransaction::RejectedDeploy(..) => ("deploy", "rejected", None),
                ConfirmedTransaction::RejectedExecute(..) => ("execute", "rejected", None),
            };
            let needs_transaction = transaction_id.is_none_or(|transaction_id| {
                checksums.get(&transaction_id).is_some_and(|candidates| candidates.len() > 1)
            });
            (kind, outcome, transaction_id, needs_transaction.then(|| confirmed.clone()))
        })
        .collect();
    let aborted = block.aborted_transaction_ids().clone();
    let height = block.height();
    let num_events = confirmed.len() + aborted.len();
    let outcomes = move || {
        let mut out = Vec::with_capacity(num_events);
        for (kind, outcome, transaction_id, confirmed) in confirmed {
            let Some(transaction_id) =
                transaction_id.or_else(|| confirmed.as_ref()?.to_unconfirmed_transaction_id().ok())
            else {
                continue;
            };
            // An id that appears with several checksums is resolved by hashing the transaction.
            let checksum = match checksums.get_mut(&transaction_id) {
                Some(candidates) if candidates.len() > 1 => {
                    let checksum = confirmed
                        .and_then(|confirmed| confirmed.to_unconfirmed_transaction().ok())
                        .and_then(|tx| transaction_checksum(&tx));
                    candidates.retain(|candidate| Some(*candidate) != checksum);
                    checksum
                }
                Some(_) => checksums.remove(&transaction_id).and_then(|candidates| candidates.first().copied()),
                None => None,
            };
            let extra = json!({ "outcome": outcome, "height": height });
            out.push(TxEvent { kind: Some(kind), extra, ..TxEvent::new::<N>(ev, &transaction_id, checksum) });
        }
        for transaction_id in &aborted {
            let extra = json!({ "outcome": "aborted", "height": height });
            match checksums.remove(transaction_id) {
                Some(candidates) if !candidates.is_empty() => out.extend(candidates.into_iter().map(|checksum| {
                    TxEvent { extra: extra.clone(), ..TxEvent::new::<N>(ev, transaction_id, Some(checksum)) }
                })),
                _ => out.push(TxEvent { extra, ..TxEvent::new::<N>(ev, transaction_id, None) }),
            }
        }
        out
    };
    events.queue_block(BlockJob {
        ev,
        hash: block.hash().to_string(),
        num_events: num_events as u64,
        stamp: stamp(),
        outcomes: Box::new(outcomes),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use snarkvm::prelude::MainnetV0;

    type CurrentNetwork = MainnetV0;

    fn sample_events(target: SocketAddr) -> TxEvents {
        let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        socket.connect(target).unwrap();
        TxEvents {
            socket,
            net: "mainnet",
            node: "test".to_string(),
            ntype: "validator",
            boot: "0123456789abcdef".to_string(),
            seq: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            rest_status: std::array::from_fn(|_| AtomicU64::new(0)),
            blocks: mpsc::sync_channel(1).0,
        }
    }

    fn sample_event(ev: &'static str) -> TxEvent {
        let transaction_id = <CurrentNetwork as Network>::TransactionID::default();
        TxEvent::new::<CurrentNetwork>(ev, &transaction_id, Some(7))
    }

    #[test]
    fn test_encode_envelope() {
        let events = sample_events((Ipv4Addr::LOCALHOST, 9).into());
        let mut line = Vec::new();
        events.encode(&sample_event("mempool_inserted"), stamp(), &mut line);
        let value: Value = serde_json::from_slice(&line).unwrap();
        for field in
            ["v", "net", "node", "ntype", "boot", "seq", "ts_ms", "height", "ev", "tx", "cks", "kind", "reason", "peer"]
        {
            assert!(value.get(field).is_some(), "missing field {field}");
        }
        assert_eq!(value["ev"], "mempool_inserted");
        assert_eq!(value["cks"], "7");
        assert!(value["tx"].as_str().unwrap().starts_with("at1"));
    }

    #[test]
    fn test_checked_truncates_error() {
        let transaction_id = <CurrentNetwork as Network>::TransactionID::default();
        let result: Result<(), String> = Err("é".repeat(MAX_ERR_BYTES));
        let event = TxEvent::checked::<CurrentNetwork, _, _>(
            "worker",
            "execute",
            &transaction_id,
            None,
            &result,
            Duration::ZERO,
        );
        assert_eq!(event.reason, Some("check_failed"));
        assert!(event.extra["err"].as_str().unwrap().len() <= MAX_ERR_BYTES);
    }

    #[test]
    fn test_emit_all_packs_datagrams() {
        let receiver = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        receiver.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let events = sample_events(receiver.local_addr().unwrap());
        events.emit_all((0..20).map(|_| sample_event("in_block")));

        let mut seqs = Vec::new();
        let mut buffer = [0u8; 65536];
        while seqs.len() < 20 {
            let size = receiver.recv(&mut buffer).unwrap();
            assert!(size <= MAX_DATAGRAM_BYTES);
            for line in buffer[..size].split(|byte| *byte == b'\n') {
                let value: Value = serde_json::from_slice(line).unwrap();
                seqs.push(value["seq"].as_u64().unwrap());
            }
        }
        assert_eq!(seqs, (0..20).collect::<Vec<_>>());
    }

    #[test]
    fn test_count_rest_status() {
        let events = sample_events((Ipv4Addr::LOCALHOST, 9).into());
        events.count_rest_status(429);
        events.count_rest_status(200);
        events.count_rest_status(600);
        assert_eq!(events.rest_status[29].load(Ordering::Relaxed), 1);
        assert_eq!(events.rest_status.iter().map(|count| count.load(Ordering::Relaxed)).sum::<u64>(), 1);
    }
}
