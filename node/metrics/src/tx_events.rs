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
//! Delivery is best-effort: a send that fails, or finds the batch queue full, is counted in the
//! `emit_dropped` field of the next heartbeat and is otherwise ignored. Each line carries the
//! process's `boot` id and a `seq` number, so a receiver can count lost events.

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
        mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// The schema version in the `v` field.
pub const SCHEMA_VERSION: u8 = 1;
/// The size up to which lines are packed into one datagram. A datagram of this size is not
/// fragmented on a path with a 1280-byte MTU or larger.
pub const MAX_DATAGRAM_BYTES: usize = 1200;
/// The number of lines the batch queue holds before it drops new ones.
const BATCH_QUEUE_CAPACITY: usize = 4096;
/// The interval at which queued lines are sent.
const FLUSH_INTERVAL: Duration = Duration::from_millis(50);
/// The interval between heartbeats.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
/// The length to which error text in `extra.err` is truncated.
const MAX_ERR_BYTES: usize = 256;
/// The lowest HTTP status counted in a heartbeat's `rest_status`.
const MIN_ERROR_STATUS: u16 = 400;

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

/// Starts sending events to `target`. With `batched` set, router drops and memory-pool refusals
/// are also sent, packed into datagrams by a background thread.
pub fn initialize(
    target: SocketAddr,
    batched: bool,
    net: &'static str,
    node: String,
    ntype: &'static str,
) -> io::Result<()> {
    let bind: SocketAddr =
        if target.is_ipv4() { (Ipv4Addr::UNSPECIFIED, 0).into() } else { (Ipv6Addr::UNSPECIFIED, 0).into() };
    let socket = UdpSocket::bind(bind)?;
    socket.connect(target)?;
    socket.set_nonblocking(true)?;

    let (batch, receiver) = match batched {
        true => {
            let (sender, receiver) = sync_channel(BATCH_QUEUE_CAPACITY);
            (Some(sender), Some(receiver))
        }
        false => (None, None),
    };
    let boot = format!("{:016x}", RandomState::new().hash_one(SystemTime::now()));
    let events = TxEvents {
        socket,
        net,
        node,
        ntype,
        boot,
        seq: AtomicU64::new(0),
        dropped: AtomicU64::new(0),
        batch,
        rest_status: std::array::from_fn(|_| AtomicU64::new(0)),
    };
    if TX_EVENTS.set(events).is_err() {
        return Err(io::Error::new(io::ErrorKind::AlreadyExists, "transaction events are already initialized"));
    }
    let events = TX_EVENTS.get().ok_or_else(|| io::Error::other("transaction events are not initialized"))?;
    thread::Builder::new().name("tx-events".to_string()).spawn(move || events.run_background(receiver))?;
    Ok(())
}

/// One transaction event. The envelope fields are added when it is sent.
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

    /// Returns an event named `ev` for a transaction transmission, or `None` for other transmissions.
    pub fn for_transmission<N: Network>(ev: &'static str, transmission_id: &TransmissionID<N>) -> Option<Self> {
        match transmission_id {
            TransmissionID::Transaction(transaction_id, checksum) => {
                Some(Self::new::<N>(ev, transaction_id, Some(*checksum)))
            }
            _ => None,
        }
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

/// The process-wide event emitter.
pub struct TxEvents {
    socket: UdpSocket,
    net: &'static str,
    node: String,
    ntype: &'static str,
    boot: String,
    seq: AtomicU64,
    dropped: AtomicU64,
    batch: Option<SyncSender<Vec<u8>>>,
    rest_status: [AtomicU64; 200],
}

impl TxEvents {
    /// Sends the event in its own datagram.
    pub fn emit(&self, event: TxEvent) {
        LINE.with_borrow_mut(|line| {
            line.clear();
            self.encode(&event, line);
            self.send(line);
        });
    }

    /// Sends the events, packed into as few datagrams as fit.
    pub fn emit_all(&self, events: impl IntoIterator<Item = TxEvent>) {
        let mut datagram = Vec::with_capacity(MAX_DATAGRAM_BYTES);
        LINE.with_borrow_mut(|line| {
            for event in events {
                line.clear();
                self.encode(&event, line);
                self.pack(&mut datagram, line);
            }
        });
        if !datagram.is_empty() {
            self.send(&datagram);
        }
    }

    /// Queues the event for the background thread, if `--tx-events-level all` is set.
    pub fn emit_batched(&self, event: TxEvent) {
        let Some(batch) = &self.batch else {
            return;
        };
        let mut line = Vec::with_capacity(MAX_DATAGRAM_BYTES / 4);
        self.encode(&event, &mut line);
        if let Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) = batch.try_send(line) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Counts a REST response with an error status, for the next heartbeat.
    pub fn count_rest_status(&self, status: u16) {
        if let Some(count) = status.checked_sub(MIN_ERROR_STATUS).and_then(|i| self.rest_status.get(i as usize)) {
            count.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn encode(&self, event: &TxEvent, out: &mut Vec<u8>) {
        let envelope = Envelope {
            v: SCHEMA_VERSION,
            net: self.net,
            node: &self.node,
            ntype: self.ntype,
            boot: &self.boot,
            seq: self.seq.fetch_add(1, Ordering::Relaxed),
            ts_ms: SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |since| since.as_millis() as u64),
            height: HEIGHT.load(Ordering::Relaxed),
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

    /// Appends `line` to `datagram`, first sending `datagram` if `line` would not fit.
    fn pack(&self, datagram: &mut Vec<u8>, line: &[u8]) {
        if !datagram.is_empty() && datagram.len() + 1 + line.len() > MAX_DATAGRAM_BYTES {
            self.send(datagram);
            datagram.clear();
        }
        if !datagram.is_empty() {
            datagram.push(b'\n');
        }
        datagram.extend_from_slice(line);
    }

    fn send(&self, datagram: &[u8]) {
        if datagram.is_empty() || self.socket.send(datagram).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
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
            "batched": self.batch.is_some(),
        });
        self.emit(TxEvent { ev: "heartbeat", tx: None, cks: None, kind: None, reason: None, extra });
    }

    fn run_background(&self, receiver: Option<Receiver<Vec<u8>>>) {
        let mut datagram = Vec::with_capacity(MAX_DATAGRAM_BYTES);
        let mut next_flush = Instant::now() + FLUSH_INTERVAL;
        let mut next_heartbeat = Instant::now();
        loop {
            let timeout = next_flush.min(next_heartbeat).saturating_duration_since(Instant::now());
            match &receiver {
                Some(receiver) => match receiver.recv_timeout(timeout) {
                    Ok(line) => self.pack(&mut datagram, &line),
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => return,
                },
                None => thread::sleep(timeout),
            }
            let now = Instant::now();
            if now >= next_flush {
                if !datagram.is_empty() {
                    self.send(&datagram);
                    datagram.clear();
                }
                next_flush = now + FLUSH_INTERVAL;
            }
            if now >= next_heartbeat {
                self.heartbeat();
                next_heartbeat = now + HEARTBEAT_INTERVAL;
            }
        }
    }
}

/// Emits a `gossiped` event for a transaction propagated from `site`.
pub fn emit_gossiped<N: Network>(
    site: &'static str,
    transaction_id: &N::TransactionID,
    checksum: Option<N::TransmissionChecksum>,
) {
    if let Some(events) = get() {
        events.emit(TxEvent {
            extra: json!({ "site": site }),
            ..TxEvent::new::<N>("gossiped", transaction_id, checksum)
        });
    }
}

/// Returns the transmission checksum of the transaction, as the memory pool and the BFT compute it.
pub fn transaction_checksum<N: Network>(transaction: &Transaction<N>) -> Option<N::TransmissionChecksum> {
    Data::<Transaction<N>>::Buffer(transaction.to_bytes_le().ok()?.into()).to_checksum::<N>().ok()
}

/// Emits one event named `ev` per transaction in the block, including aborted transactions, with
/// `extra.outcome` set to `accepted`, `rejected` or `aborted`.
pub fn emit_block_outcomes<N: Network>(ev: &'static str, block: &Block<N>) {
    let Some(events) = get() else {
        return;
    };
    let mut checksums: HashMap<N::TransactionID, Vec<N::TransmissionChecksum>> = HashMap::new();
    if let Authority::Quorum(subdag) = block.authority() {
        for transmission_id in subdag.transmission_ids() {
            if let TransmissionID::Transaction(transaction_id, checksum) = transmission_id {
                checksums.entry(*transaction_id).or_default().push(*checksum);
            }
        }
    }
    let height = block.height();
    let mut out = Vec::with_capacity(block.transactions().len() + block.aborted_transaction_ids().len());
    for confirmed in block.transactions().iter() {
        let Ok(transaction_id) = confirmed.to_unconfirmed_transaction_id() else {
            continue;
        };
        let (kind, outcome) = match confirmed {
            ConfirmedTransaction::AcceptedDeploy(..) => ("deploy", "accepted"),
            ConfirmedTransaction::AcceptedExecute(..) => ("execute", "accepted"),
            ConfirmedTransaction::RejectedDeploy(..) => ("deploy", "rejected"),
            ConfirmedTransaction::RejectedExecute(..) => ("execute", "rejected"),
        };
        // An id that appears with several checksums is resolved by hashing the transaction.
        let checksum = match checksums.get_mut(&transaction_id) {
            Some(candidates) if candidates.len() > 1 => {
                let checksum = confirmed.to_unconfirmed_transaction().ok().and_then(|tx| transaction_checksum(&tx));
                candidates.retain(|candidate| Some(*candidate) != checksum);
                checksum
            }
            Some(_) => checksums.remove(&transaction_id).and_then(|candidates| candidates.first().copied()),
            None => None,
        };
        let extra = json!({ "outcome": outcome, "height": height });
        out.push(TxEvent { kind: Some(kind), extra, ..TxEvent::new::<N>(ev, &transaction_id, checksum) });
    }
    for transaction_id in block.aborted_transaction_ids() {
        let extra = json!({ "outcome": "aborted", "height": height });
        match checksums.remove(transaction_id) {
            Some(candidates) if !candidates.is_empty() => out.extend(candidates.into_iter().map(|checksum| TxEvent {
                extra: extra.clone(),
                ..TxEvent::new::<N>(ev, transaction_id, Some(checksum))
            })),
            _ => out.push(TxEvent { extra, ..TxEvent::new::<N>(ev, transaction_id, None) }),
        }
    }
    events.emit_all(out);
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
            batch: None,
            rest_status: std::array::from_fn(|_| AtomicU64::new(0)),
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
        events.encode(&sample_event("mempool_inserted"), &mut line);
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
