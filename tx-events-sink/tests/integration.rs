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

use serde_json::{Value, json};
use snarkos_node_metrics::tx_events::{self, TxEvent};
use snarkos_tx_events_sink::{Limits, Sink, run};
use snarkvm::prelude::{MainnetV0, Network};
use std::{
    net::{SocketAddr, UdpSocket},
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket as TokioUdpSocket},
};

/// Starts a sink on ephemeral ports and returns its UDP and HTTP addresses.
async fn start_sink() -> (SocketAddr, SocketAddr) {
    let udp = TokioUdpSocket::bind("127.0.0.1:0").await.unwrap();
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addresses = (udp.local_addr().unwrap(), http.local_addr().unwrap());
    let limits = Limits { max_txs: NonZeroUsize::new(100).unwrap(), max_events_per_tx: NonZeroUsize::new(64).unwrap() };
    tokio::spawn(run(udp, http, Arc::new(Sink::new(limits))));
    addresses
}

/// Sends an HTTP GET and returns the status code and the JSON body.
async fn get(http: SocketAddr, path: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(http).await.unwrap();
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: sink\r\nConnection: close\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, serde_json::from_str(body).unwrap())
}

/// Polls `path` until `done` holds for the body, or panics after five seconds.
async fn get_until(http: SocketAddr, path: &str, done: impl Fn(&Value) -> bool) -> Value {
    let started = Instant::now();
    loop {
        let (status, body) = get(http, path).await;
        if status == 200 && done(&body) {
            return body;
        }
        assert!(started.elapsed() < Duration::from_secs(5), "timed out on {path}: {body}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn line(seq: u64, ev: &str, cks: &str, extra: Value) -> String {
    json!({
        "v": 1, "net": "testnet", "node": "n1", "ntype": "validator", "boot": "b0", "seq": seq,
        "ts_ms": 1000 + seq, "height": 10, "ev": ev, "tx": "at1x", "cks": cks, "kind": "execute",
        "reason": null, "peer": null, "extra": extra,
    })
    .to_string()
}

#[tokio::test]
async fn test_udp_ingest_and_http_queries() {
    let (udp, http) = start_sink().await;
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

    let (status, _) = get(http, "/tx/at1x/status").await;
    assert_eq!(status, 404);

    let batch = [
        line(0, "received", "1", json!({ "via": "rest" })),
        line(1, "mempool_inserted", "1", json!({ "via": "rest" })),
        line(2, "checked", "1", json!({ "site": "worker", "outcome": "ok" })),
    ]
    .join("\n");
    socket.send_to(batch.as_bytes(), udp).unwrap();
    socket.send_to(line(4, "certified", "1", json!({ "round": 7 })).as_bytes(), udp).unwrap();

    let status = get_until(http, "/tx/at1x/status", |body| body["stage"] == "certified").await;
    assert_eq!(status["status"], "pending");
    assert_eq!(status["holders"], json!(["n1"]));

    socket.send_to(line(5, "in_block", "1", json!({ "outcome": "accepted", "height": 11 })).as_bytes(), udp).unwrap();
    let status = get_until(http, "/tx/at1x/status", |body| body["status"] == "accepted").await;
    assert_eq!(status["final"]["height"], 11);
    assert_eq!(status["advisory"], true);

    let events = get_until(http, "/tx/at1x/events", |body| body["events"].as_array().unwrap().len() == 5).await;
    let seqs: Vec<u64> =
        events["events"].as_array().unwrap().iter().map(|event| event["seq"].as_u64().unwrap()).collect();
    assert_eq!(seqs, [0, 1, 2, 4, 5]);

    let (_, stats) = get(http, "/stats").await;
    assert_eq!(stats["nodes"]["n1"]["lost"], 1);
}

#[tokio::test]
async fn test_node_emitter_output_is_accepted() {
    let (udp, http) = start_sink().await;
    tx_events::initialize(udp, "testnet", "node-a".to_string(), "validator").unwrap();
    let events = tx_events::get().unwrap();

    let transaction_id = <MainnetV0 as Network>::TransactionID::default();
    let tx = transaction_id.to_string();
    let event = |ev| TxEvent::new::<MainnetV0>(ev, &transaction_id, Some(5));
    events.emit(TxEvent { extra: json!({ "via": "rest" }), ..event("received") });
    events.emit(TxEvent {
        kind: Some("execute"),
        extra: json!({ "prio_fee": 0, "qlen": 1 }),
        ..event("mempool_inserted")
    });
    events.emit(TxEvent { reason: Some("recently_seen"), ..event("mempool_refused") });
    events.emit_all([TxEvent { extra: json!({ "outcome": "rejected", "height": 3 }), ..event("in_block") }]);

    let path = format!("/tx/{tx}/events");
    let body = get_until(http, &path, |body| body["events"].as_array().unwrap().len() == 4).await;
    for event in body["events"].as_array().unwrap() {
        assert_eq!(event["node"], "node-a");
        assert_eq!(event["cks"], "5");
    }
    let status = get_until(http, &format!("/tx/{tx}/status"), |body| body["status"] == "rejected").await;
    assert_eq!(status["final"]["cks"], "5");

    let stats = get_until(http, "/stats", |body| body["counters"]["heartbeats"] == 1).await;
    assert_eq!(stats["counters"]["malformed"], 0);
    assert_eq!(stats["nodes"]["node-a"]["lost"], 0);
    assert_eq!(stats["nodes"]["node-a"]["last_heartbeat"]["emit_dropped"], 0);
}
