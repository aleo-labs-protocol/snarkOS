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

use clap::Parser;
use snarkos_tx_events_sink::{Limits, Sink, run};
use std::{net::SocketAddr, num::NonZeroUsize, sync::Arc};
use tokio::net::{TcpListener, UdpSocket};

/// Receives the transaction events of snarkOS nodes and serves them over HTTP.
#[derive(Parser)]
struct Args {
    /// The UDP address to receive events on; pass it to the node's `--tx-events`
    #[clap(long, default_value = "127.0.0.1:9150")]
    udp: SocketAddr,
    /// The HTTP address to serve `/tx/{id}/events`, `/tx/{id}/status` and `/stats` on
    #[clap(long, default_value = "127.0.0.1:9151")]
    http: SocketAddr,
    /// The number of transactions kept in memory
    #[clap(long, default_value = "10000")]
    max_txs: NonZeroUsize,
    /// The number of events kept per transaction
    #[clap(long, default_value = "64")]
    max_events_per_tx: NonZeroUsize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let udp = UdpSocket::bind(args.udp).await?;
    let http = TcpListener::bind(args.http).await?;
    println!("Receiving events on udp://{} and serving http://{}", udp.local_addr()?, http.local_addr()?);
    let sink = Arc::new(Sink::new(Limits { max_txs: args.max_txs, max_events_per_tx: args.max_events_per_tx }));
    run(udp, http, sink).await
}
