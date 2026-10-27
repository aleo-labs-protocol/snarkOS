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

use crate::events::{
    BatchPropose,
    BatchSignature,
    CertificateRequest,
    CertificateResponse,
    TransmissionRequest,
    TransmissionResponse,
};
use snarkos_node_sync::{InsertBlockResponseError, locators::BlockLocators};
use snarkvm::{
    console::network::*,
    ledger::{
        block::{Block, Transaction},
        narwhal::{BatchCertificate, Data, Subdag, Transmission, TransmissionID},
        puzzle::{Solution, SolutionID},
    },
    prelude::Result,
};

use indexmap::IndexMap;
use std::net::SocketAddr;
use tokio::sync::{mpsc, oneshot};

const MAX_CHANNEL_SIZE: usize = 8192;

#[derive(Debug)]
pub struct ConsensusSender<N: Network> {
    pub tx_consensus_subdag:
        mpsc::Sender<(Subdag<N>, IndexMap<TransmissionID<N>, Transmission<N>>, oneshot::Sender<Result<bool>>)>,
}

#[derive(Debug)]
pub struct ConsensusReceiver<N: Network> {
    pub rx_consensus_subdag:
        mpsc::Receiver<(Subdag<N>, IndexMap<TransmissionID<N>, Transmission<N>>, oneshot::Sender<Result<bool>>)>,
}

/// Initializes the consensus channels.
pub fn init_consensus_channels<N: Network>() -> (ConsensusSender<N>, ConsensusReceiver<N>) {
    let (tx_consensus_subdag, rx_consensus_subdag) = mpsc::channel(MAX_CHANNEL_SIZE);

    let sender = ConsensusSender { tx_consensus_subdag };
    let receiver = ConsensusReceiver { rx_consensus_subdag };

    (sender, receiver)
}

#[derive(Clone, Debug)]
pub struct PrimarySender<N: Network> {
    pub tx_batch_propose: mpsc::Sender<(SocketAddr, BatchPropose<N>)>,
    pub tx_batch_signature: mpsc::Sender<(SocketAddr, BatchSignature<N>)>,
    pub tx_batch_certified: mpsc::Sender<(SocketAddr, Data<BatchCertificate<N>>)>,
    pub tx_primary_ping: mpsc::Sender<(SocketAddr, Data<BatchCertificate<N>>)>,
    pub tx_unconfirmed_solution: mpsc::Sender<(SolutionID<N>, Data<Solution<N>>, oneshot::Sender<Result<bool>>)>,
    pub tx_unconfirmed_transaction:
        mpsc::Sender<(N::TransactionID, Data<Transaction<N>>, oneshot::Sender<Result<bool>>)>,
}

impl<N: Network> PrimarySender<N> {
    /// Sends the unconfirmed solution to the primary.
    ///
    /// # Returns
    /// - `Ok(true)` if the solution was added to the ready queue.
    /// - `Ok(false)` if the solution was valid but already exists in the ready queue.
    /// - `Err(anyhow::Error)` if the solution was invalid.
    pub async fn send_unconfirmed_solution(
        &self,
        solution_id: SolutionID<N>,
        solution: Data<Solution<N>>,
    ) -> Result<bool> {
        // Initialize a callback sender and receiver.
        let (callback_sender, callback_receiver) = oneshot::channel();
        // Send the unconfirmed solution to the primary.
        self.tx_unconfirmed_solution.send((solution_id, solution, callback_sender)).await?;
        // Await the callback to continue.
        callback_receiver.await?
    }

    /// Sends the unconfirmed transaction to the primary.
    ///
    /// # Returns
    /// - `Ok(true)` if the transaction was added to the ready queue.
    /// - `Ok(false)` if the transaction was valid but already exists in the ready queue.
    /// - `Err(anyhow::Error)` if the transaction was invalid.
    pub async fn send_unconfirmed_transaction(
        &self,
        transaction_id: N::TransactionID,
        transaction: Data<Transaction<N>>,
    ) -> Result<bool> {
        // Initialize a callback sender and receiver.
        let (callback_sender, callback_receiver) = oneshot::channel();
        // Send the unconfirmed transaction to the primary.
        self.tx_unconfirmed_transaction.send((transaction_id, transaction, callback_sender)).await?;
        // Await the callback to continue.
        callback_receiver.await?
    }
}

#[derive(Debug)]
pub struct PrimaryReceiver<N: Network> {
    pub rx_batch_propose: mpsc::Receiver<(SocketAddr, BatchPropose<N>)>,
    pub rx_batch_signature: mpsc::Receiver<(SocketAddr, BatchSignature<N>)>,
    pub rx_batch_certified: mpsc::Receiver<(SocketAddr, Data<BatchCertificate<N>>)>,
    pub rx_primary_ping: mpsc::Receiver<(SocketAddr, Data<BatchCertificate<N>>)>,
    pub rx_unconfirmed_solution: mpsc::Receiver<(SolutionID<N>, Data<Solution<N>>, oneshot::Sender<Result<bool>>)>,
    pub rx_unconfirmed_transaction:
        mpsc::Receiver<(N::TransactionID, Data<Transaction<N>>, oneshot::Sender<Result<bool>>)>,
}

/// Initializes the primary channels.
pub fn init_primary_channels<N: Network>() -> (PrimarySender<N>, PrimaryReceiver<N>) {
    let (tx_batch_propose, rx_batch_propose) = mpsc::channel(MAX_CHANNEL_SIZE);
    let (tx_batch_signature, rx_batch_signature) = mpsc::channel(MAX_CHANNEL_SIZE);
    let (tx_batch_certified, rx_batch_certified) = mpsc::channel(MAX_CHANNEL_SIZE);
    let (tx_primary_ping, rx_primary_ping) = mpsc::channel(MAX_CHANNEL_SIZE);
    let (tx_unconfirmed_solution, rx_unconfirmed_solution) = mpsc::channel(MAX_CHANNEL_SIZE);
    let (tx_unconfirmed_transaction, rx_unconfirmed_transaction) = mpsc::channel(MAX_CHANNEL_SIZE);

    let sender = PrimarySender {
        tx_batch_propose,
        tx_batch_signature,
        tx_batch_certified,
        tx_primary_ping,
        tx_unconfirmed_solution,
        tx_unconfirmed_transaction,
    };
    let receiver = PrimaryReceiver {
        rx_batch_propose,
        rx_batch_signature,
        rx_batch_certified,
        rx_primary_ping,
        rx_unconfirmed_solution,
        rx_unconfirmed_transaction,
    };

    (sender, receiver)
}

#[derive(Debug)]
pub struct WorkerSender<N: Network> {
    pub tx_worker_ping: mpsc::Sender<(SocketAddr, TransmissionID<N>)>,
    pub tx_transmission_request: mpsc::Sender<(SocketAddr, TransmissionRequest<N>)>,
    pub tx_transmission_response: mpsc::Sender<(SocketAddr, TransmissionResponse<N>)>,
}

#[derive(Debug)]
pub struct WorkerReceiver<N: Network> {
    pub rx_worker_ping: mpsc::Receiver<(SocketAddr, TransmissionID<N>)>,
    pub rx_transmission_request: mpsc::Receiver<(SocketAddr, TransmissionRequest<N>)>,
    pub rx_transmission_response: mpsc::Receiver<(SocketAddr, TransmissionResponse<N>)>,
}

/// Initializes the worker channels.
pub fn init_worker_channels<N: Network>() -> (WorkerSender<N>, WorkerReceiver<N>) {
    let (tx_worker_ping, rx_worker_ping) = mpsc::channel(MAX_CHANNEL_SIZE);
    let (tx_transmission_request, rx_transmission_request) = mpsc::channel(MAX_CHANNEL_SIZE);
    let (tx_transmission_response, rx_transmission_response) = mpsc::channel(MAX_CHANNEL_SIZE);

    let sender = WorkerSender { tx_worker_ping, tx_transmission_request, tx_transmission_response };
    let receiver = WorkerReceiver { rx_worker_ping, rx_transmission_request, rx_transmission_response };

    (sender, receiver)
}

#[derive(Debug)]
pub struct SyncSender<N: Network> {
    pub tx_block_sync_insert_block_response: mpsc::Sender<(
        SocketAddr,
        u32,
        Vec<Block<N>>,
        Option<ConsensusVersion>,
        oneshot::Sender<Result<(), InsertBlockResponseError<N>>>,
    )>,
    pub tx_block_sync_remove_peer: mpsc::Sender<(SocketAddr, oneshot::Sender<()>)>,
    pub tx_block_sync_update_peer_locators: mpsc::Sender<(SocketAddr, BlockLocators<N>, oneshot::Sender<Result<()>>)>,
    pub tx_certificate_request: mpsc::Sender<(SocketAddr, CertificateRequest<N>)>,
    pub tx_certificate_response: mpsc::Sender<(SocketAddr, CertificateResponse<N>)>,
}

impl<N: Network> SyncSender<N> {
    /// Sends the request to update the peer locators.
    pub async fn update_peer_locators(&self, peer_ip: SocketAddr, block_locators: BlockLocators<N>) -> Result<()> {
        // Initialize a callback sender and receiver.
        let (callback_sender, callback_receiver) = oneshot::channel();
        // Send the request to update the peer locators.
        // This `tx_block_sync_update_peer_locators.send()` call
        // causes the `rx_block_sync_update_peer_locators.recv()` call
        // in one of the loops in [`Sync::run()`] to return.
        self.tx_block_sync_update_peer_locators.send((peer_ip, block_locators, callback_sender)).await?;
        // Await the callback to continue.
        callback_receiver.await?
    }

    /// Sends the request to insert a new block response.
    pub async fn insert_block_response(
        &self,
        peer_ip: SocketAddr,
        start_height: u32,
        blocks: Vec<Block<N>>,
        latest_consensus_version: Option<ConsensusVersion>,
    ) -> Result<(), InsertBlockResponseError<N>> {
        // Initialize a callback sender and receiver.
        let (callback_sender, callback_receiver) = oneshot::channel();
        // Send the request to advance with sync blocks.
        // This `tx_block_sync_advance_with_sync_blocks.send()` call
        // causes the `rx_block_sync_advance_with_sync_blocks.recv()` call
        // in one of the loops in [`Sync::run()`] to return.
        if let Err(err) = self
            .tx_block_sync_insert_block_response
            .send((peer_ip, start_height, blocks, latest_consensus_version, callback_sender))
            .await
        {
            return Err(anyhow!("Failed to send block response - {err}").into());
        }

        // Await the callback to continue.
        match callback_receiver.await {
            Ok(result) => result,
            Err(err) => Err(anyhow!("Failed to wait for block response insertion - {err}").into()),
        }
    }
}

#[derive(Debug)]
pub struct SyncReceiver<N: Network> {
    pub rx_block_sync_insert_block_response: mpsc::Receiver<(
        SocketAddr,
        u32,
        Vec<Block<N>>,
        Option<ConsensusVersion>,
        oneshot::Sender<Result<(), InsertBlockResponseError<N>>>,
    )>,
    pub rx_block_sync_remove_peer: mpsc::Receiver<(SocketAddr, oneshot::Sender<()>)>,
    pub rx_block_sync_update_peer_locators: mpsc::Receiver<(SocketAddr, BlockLocators<N>, oneshot::Sender<Result<()>>)>,
    pub rx_certificate_request: mpsc::Receiver<(SocketAddr, CertificateRequest<N>)>,
    pub rx_certificate_response: mpsc::Receiver<(SocketAddr, CertificateResponse<N>)>,
}

/// Initializes the sync channels.
pub fn init_sync_channels<N: Network>() -> (SyncSender<N>, SyncReceiver<N>) {
    let (tx_block_sync_insert_block_response, rx_block_sync_insert_block_response) = mpsc::channel(MAX_CHANNEL_SIZE);
    let (tx_block_sync_remove_peer, rx_block_sync_remove_peer) = mpsc::channel(MAX_CHANNEL_SIZE);
    let (tx_block_sync_update_peer_locators, rx_block_sync_update_peer_locators) = mpsc::channel(MAX_CHANNEL_SIZE);
    let (tx_certificate_request, rx_certificate_request) = mpsc::channel(MAX_CHANNEL_SIZE);
    let (tx_certificate_response, rx_certificate_response) = mpsc::channel(MAX_CHANNEL_SIZE);

    let sender = SyncSender {
        tx_block_sync_insert_block_response,
        tx_block_sync_remove_peer,
        tx_block_sync_update_peer_locators,
        tx_certificate_request,
        tx_certificate_response,
    };
    let receiver = SyncReceiver {
        rx_block_sync_insert_block_response,
        rx_block_sync_remove_peer,
        rx_block_sync_update_peer_locators,
        rx_certificate_request,
        rx_certificate_response,
    };

    (sender, receiver)
}
