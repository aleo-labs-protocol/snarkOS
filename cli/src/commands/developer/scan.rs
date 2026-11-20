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

use super::DEFAULT_ENDPOINT;
use crate::{
    commands::Developer,
    helpers::{args::prepare_endpoint, dev::get_development_key},
};

use snarkos_node_cdn::CDN_BASE_URL;
use snarkos_utilities::{SimpleStoppable, locks::parking_lot::RwLock};

use snarkvm::{
    console::network::Network,
    prelude::{Ciphertext, Field, Plaintext, PrivateKey, Record, ViewKey, block::Block},
};

#[cfg(not(feature = "test_targets"))]
use snarkvm::prelude::FromBytes;

use anyhow::{Context, Result, anyhow, bail, ensure};
use clap::{Parser, builder::NonEmptyStringValueParser};
use std::{
    io::{Write, stdout},
    str::FromStr,
    sync::Arc,
};
use tracing::debug;
use ureq::http::Uri;
use zeroize::Zeroize;

const MAX_BLOCK_RANGE: u32 = 50;

/// Scan the snarkOS node for records.
#[derive(Debug, Parser)]
#[clap(
    // The user needs to set view_key, private_key, or dev_key,
    // but they cannot set private_key and dev_key.
    group(clap::ArgGroup::new("key").required(true).multiple(false))
)]
pub struct Scan {
    /// The private key used for unspent records.
    #[clap(short, long, group = "key", value_parser=NonEmptyStringValueParser::default())]
    private_key: Option<String>,

    /// The view key used to scan for records.
    /// (if a private key is given, the view key is automatically derived and should not be set)
    #[clap(short, long, group = "key", value_parser=NonEmptyStringValueParser::default())]
    view_key: Option<String>,

    /// Use a development private key to scan for records.
    #[clap(long, group = "key")]
    dev_key: Option<u16>,

    /// The block height to start scanning from.
    /// Will scan until the most recent block or the height specified with `--end`.
    #[clap(long, conflicts_with = "last")]
    start: Option<u32>,

    /// The block height to stop scanning at (exclusive).
    /// Will start scanning at the geneiss block or the height specified with `--start`.
    #[clap(long, conflicts_with = "last")]
    end: Option<u32>,

    /// Scan the latest `n` blocks.
    #[clap(long)]
    last: Option<u32>,

    /// The endpoint to scan blocks from.
    #[clap(long, default_value = DEFAULT_ENDPOINT)]
    endpoint: Uri,

    /// Sets verbosity of log output. By default, no logs are shown.
    #[clap(long)]
    verbosity: Option<u8>,
}

impl Drop for Scan {
    /// Zeroize the private key when the `Execute` struct goes out of scope.
    fn drop(&mut self) {
        if let Some(mut pk) = self.private_key.take() {
            pk.zeroize()
        }
    }
}

impl Scan {
    /// Scan the network for records.
    pub fn parse<N: Network>(self) -> Result<String> {
        let endpoint = prepare_endpoint(self.endpoint.clone())?;

        // Derive the view key and optional private key.
        let (private_key, view_key) = self.parse_account::<N>()?;

        // Find the start and end height to scan.
        let (start_height, end_height) = self.parse_block_range::<N>(&endpoint)?;

        // Fetch the records from the network.
        let records = Self::fetch_records::<N>(private_key, &view_key, &endpoint, start_height, end_height)
            .with_context(|| "Failed to fetch records")?;

        // Output the decrypted records associated with the view key.
        if records.is_empty() {
            Ok("No records found".to_string())
        } else {
            if private_key.is_none() {
                println!("⚠️  This list may contain records that have already been spent.\n");
            }

            Ok(serde_json::to_string_pretty(&records)?.replace("\\n", ""))
        }
    }

    /// Returns the view key and optional private key, from the given configurations.
    fn parse_account<N: Network>(&self) -> Result<(Option<PrivateKey<N>>, ViewKey<N>)> {
        if let Some(private_key) = &self.private_key {
            let private_key = PrivateKey::<N>::from_str(private_key)?;
            let view_key = ViewKey::<N>::try_from(private_key)?;
            Ok((Some(private_key), view_key))
        } else if let Some(index) = &self.dev_key {
            let private_key = get_development_key(*index)?;
            let view_key = ViewKey::<N>::try_from(private_key)?;
            Ok((Some(private_key), view_key))
        } else if let Some(view_key) = &self.view_key {
            Ok((None, ViewKey::<N>::from_str(view_key)?))
        } else {
            // This will be caught by clap
            unreachable!();
        }
    }

    /// Returns the `start` and `end` blocks to scan.
    fn parse_block_range<N: Network>(&self, endpoint: &Uri) -> Result<(u32, u32)> {
        // Compute the end height.
        let end = if let Some(end) = self.end {
            end
        } else {
            // If not end height was given, request the latest block height from the endpoint.
            let (endpoint, _api_version) = Developer::build_endpoint::<N>(endpoint, "block/height/latest")?;
            let result = ureq::get(&endpoint).call().map_err(|e| e.into());
            let end: u32 = Developer::handle_ureq_result(result)
                .and_then(|body| body.ok_or(anyhow!("Endpoint returned 404 for latest block height")))?
                .read_to_string()?
                .parse()?;

            debug!("Set end height to {end} based on latest block height of the endpoint");
            end
        };

        // Compute the start height.
        let start = if let Some(start) = self.start {
            start
        } else if let Some(last) = self.last {
            let start = end.saturating_sub(last);
            debug!("Setting start height to {start} (based on last={last} and end={end})");
            start
        } else {
            debug!("Picking default value (0) for start height");
            0
        };

        ensure!(end > start, "The given scan range is invalid (start = {start}, end = {end})");

        // Print a warning message if the user is attempting to scan the whole chain.
        if start == 0 && self.end.is_none() {
            println!("⚠️  Attention - Scanning the entire chain. This may take a while...\n");
        }

        Ok((start, end))
    }

    /// Returns the CDN to prefetch initial blocks from, from the given configurations.
    fn parse_cdn<N: Network>() -> Result<Uri> {
        // This should always succeed as the base URL is hardcoded.
        Uri::try_from(format!("{CDN_BASE_URL}/{}", N::SHORT_NAME)).with_context(|| "Unexpected error")
    }

    /// Fetch owned ciphertext records from the endpoint.
    fn fetch_records<N: Network>(
        private_key: Option<PrivateKey<N>>,
        view_key: &ViewKey<N>,
        endpoint: &Uri,
        start_height: u32,
        end_height: u32,
    ) -> Result<Vec<Record<N, Plaintext<N>>>> {
        // Check the bounds of the request.
        if start_height > end_height {
            bail!("Invalid block range. Start height ({start_height}) is not smaller than end height ({end_height}).");
        }

        // Derive the x-coordinate of the address corresponding to the given view key.
        let address_x_coordinate = view_key.to_address().to_x_coordinate();

        // Initialize a vector to store the records.
        let records = Arc::new(RwLock::new(Vec::new()));

        // Calculate the number of blocks to scan.
        let total_blocks = end_height.saturating_sub(start_height);

        // Log the initial progress.
        print!("\rScanning {total_blocks} blocks for records (0% complete)...");
        stdout().flush()?;

        // If the CLI was compiled with test targets, always assume the endpoint is on a development network.
        #[cfg(feature = "test_targets")]
        let is_development_network = true;

        // Otherwise, determine if the endpoint is on a development network based on its genesis block.
        #[cfg(not(feature = "test_targets"))]
        let is_development_network = {
            // Fetch the genesis block from the endpoint.
            let endpoint_genesis_block: Block<N> = match Developer::http_get_json::<N, _>(endpoint, "block/0")? {
                Some(block) => block,
                None => bail!("Enpoint returend 404 for genesis block"),
            };

            // If the endpoint's block differs from our (production) block, it is on a development network.
            endpoint_genesis_block != Block::from_bytes_le(N::genesis_bytes())?
        };

        // Determine the request start height.
        let mut request_start = match is_development_network {
            true => start_height,
            false => {
                // Parse the CDN endpoint.
                let cdn_endpoint = Self::parse_cdn::<N>()?;
                // Scan the CDN first for records.
                let new_start_height = Self::scan_from_cdn(
                    start_height,
                    end_height,
                    &cdn_endpoint,
                    endpoint,
                    private_key,
                    *view_key,
                    address_x_coordinate,
                    records.clone(),
                )?;

                // Scan the remaining blocks from the endpoint.
                new_start_height.max(start_height)
            }
        };

        // Scan the endpoint for the remaining blocks.
        while request_start <= end_height {
            // Log the progress.
            let percentage_complete = request_start.saturating_sub(start_height) as f64 * 100.0 / total_blocks as f64;
            print!("\rScanning {total_blocks} blocks for records ({percentage_complete:.2}% complete)...");
            stdout().flush()?;

            let num_blocks_to_request =
                std::cmp::min(MAX_BLOCK_RANGE, end_height.saturating_sub(request_start).saturating_add(1));
            let request_end = request_start.saturating_add(num_blocks_to_request);

            // Fetch blocks.
            let blocks: Vec<Block<N>> =
                Developer::http_get_json::<N, _>(endpoint, &format!("blocks?start={request_start}&end={request_end}"))
                    .and_then(|blocks| blocks.ok_or(anyhow!("Enpoint returend 404 for the specified block range")))
                    .with_context(|| format!("Failed to fetch blocks range {request_start}..{request_end}"))?;

            // Scan the blocks for owned records.
            for block in &blocks {
                Self::scan_block(block, endpoint, private_key, view_key, &address_x_coordinate, records.clone())
                    .with_context(|| format!("Failed to parse block {}", block.hash()))?;
            }

            request_start = request_start.saturating_add(num_blocks_to_request);
        }

        // Print the final complete message.
        println!("\rScanning {total_blocks} blocks for records (100% complete)...   \n");
        stdout().flush()?;

        let result = records.read().clone();
        Ok(result)
    }

    /// Scan the blocks from the CDN. Returns the current height scanned to.
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    fn scan_from_cdn<N: Network>(
        start_height: u32,
        end_height: u32,
        cdn: &Uri,
        endpoint: &Uri,
        private_key: Option<PrivateKey<N>>,
        view_key: ViewKey<N>,
        address_x_coordinate: Field<N>,
        records: Arc<RwLock<Vec<Record<N, Plaintext<N>>>>>,
    ) -> Result<u32> {
        // Calculate the number of blocks to scan.
        let total_blocks = end_height.saturating_sub(start_height);

        // Get the start_height with
        let cdn_request_start = start_height.saturating_sub(start_height % MAX_BLOCK_RANGE);
        let cdn_request_end = end_height.saturating_sub(end_height % MAX_BLOCK_RANGE).saturating_add(MAX_BLOCK_RANGE);

        // Construct the runtime.
        let rt = tokio::runtime::Runtime::new()?;

        // Create a placeholder shutdown flag.
        let _shutdown = SimpleStoppable::new();

        // Copy endpoint for background task.
        let endpoint = endpoint.clone();

        // Scan the blocks via the CDN.
        rt.block_on(async move {
            let result =
                snarkos_node_cdn::load_blocks(cdn, cdn_request_start, Some(cdn_request_end), _shutdown, move |block| {
                    // Check if the block is within the requested range.
                    if block.height() < start_height || block.height() > end_height {
                        return Ok(());
                    }

                    // Log the progress.
                    let percentage_complete =
                        block.height().saturating_sub(start_height) as f64 * 100.0 / total_blocks as f64;
                    print!("\rScanning {total_blocks} blocks for records ({percentage_complete:.2}% complete)...");
                    stdout().flush()?;

                    // Scan the block for records.
                    Self::scan_block(
                        &block,
                        &endpoint,
                        private_key,
                        &view_key,
                        &address_x_coordinate,
                        records.clone(),
                    )?;

                    Ok(())
                })
                .await;
            match result {
                Ok(height) => Ok(height),
                Err(error) => {
                    eprintln!("Error loading blocks from CDN - (height, error):{error:?}");
                    Ok(error.0)
                }
            }
        })
    }

    /// Scan a block for owned records.
    #[allow(clippy::type_complexity)]
    fn scan_block<N: Network>(
        block: &Block<N>,
        endpoint: &Uri,
        private_key: Option<PrivateKey<N>>,
        view_key: &ViewKey<N>,
        address_x_coordinate: &Field<N>,
        records: Arc<RwLock<Vec<Record<N, Plaintext<N>>>>>,
    ) -> Result<()> {
        for (commitment, ciphertext_record) in block.records() {
            // Check if the record is owned by the given view key.
            if ciphertext_record.is_owner_with_address_x_coordinate(view_key, address_x_coordinate) {
                // Decrypt and optionally filter the records.
                if let Some(record) =
                    Self::decrypt_record(private_key, view_key, endpoint, *commitment, ciphertext_record)
                        .with_context(|| "Failed to decrypt record")?
                {
                    records.write().push(record);
                }
            }
        }

        Ok(())
    }

    /// Decrypts the ciphertext record and filters spend record if a private key was provided.
    fn decrypt_record<N: Network>(
        private_key: Option<PrivateKey<N>>,
        view_key: &ViewKey<N>,
        endpoint: &Uri,
        commitment: Field<N>,
        ciphertext_record: &Record<N, Ciphertext<N>>,
    ) -> Result<Option<Record<N, Plaintext<N>>>> {
        // Check if a private key was provided.
        if let Some(private_key) = private_key {
            // Compute the serial number.
            let serial_number = Record::<N, Plaintext<N>>::serial_number(private_key, commitment)?;

            // Establish the endpoint.
            let (endpoint, _api_version) =
                Developer::build_endpoint::<N>(endpoint, &format!("find/transitionID/{serial_number}"))?;

            // Check if the record is spent.
            match ureq::get(&endpoint).call() {
                // On success, skip as the record is spent.
                Ok(_) => Ok(None),
                // On error, add the record.
                Err(_error) => {
                    // TODO: Dedup the error types. We're adding the record as valid because the endpoint failed,
                    //  meaning it couldn't find the serial number (ie. unspent). However if there's a DNS error or request error,
                    //  we have a false positive here then.
                    // Decrypt the record.
                    Ok(Some(ciphertext_record.decrypt(view_key)?))
                }
            }
        } else {
            // If no private key was provided, return the record.
            Ok(Some(ciphertext_record.decrypt(view_key)?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use snarkvm::prelude::{MainnetV0, TestRng};

    type CurrentNetwork = MainnetV0;

    #[test]
    fn test_parse_account() {
        let rng = &mut TestRng::default();

        // Generate private key and view key.
        let private_key = PrivateKey::<CurrentNetwork>::new(rng).unwrap();
        let view_key = ViewKey::try_from(private_key).unwrap();

        // Test passing the private key only
        let config = Scan::try_parse_from(
            ["snarkos", "--private-key", &format!("{private_key}"), "--last", "10", "--endpoint", "localhost"].iter(),
        )
        .unwrap();
        assert!(config.parse_account::<CurrentNetwork>().is_ok());

        let (result_pkey, result_vkey) = config.parse_account::<CurrentNetwork>().unwrap();
        assert_eq!(result_pkey, Some(private_key));
        assert_eq!(result_vkey, view_key);

        // Passing an invalid view key should fail.
        // Note: the current validation only rejects empty strings.
        let err = Scan::try_parse_from(["snarkos", "--view-key", "", "--last", "10", "--endpoint", "localhost"].iter())
            .unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidValue);

        // Test passing the view key only
        let config = Scan::try_parse_from(
            ["snarkos", "--view-key", &format!("{view_key}"), "--last", "10", "--endpoint", "localhost"].iter(),
        )
        .unwrap();

        let (result_pkey, result_vkey) = config.parse_account::<CurrentNetwork>().unwrap();
        assert_eq!(result_pkey, None);
        assert_eq!(result_vkey, view_key);

        // Passing both will generate an error.
        let err = Scan::try_parse_from(
            [
                "snarkos",
                "--private-key",
                &format!("{private_key}"),
                "--view-key",
                &format!("{view_key}"),
                "--last",
                "10",
                "--endpoint",
                "localhost",
            ]
            .iter(),
        )
        .unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn test_parse_block_range() -> Result<()> {
        // Hardcoded viewkey to ensure view key validation succeeds
        const TEST_VIEW_KEY: &str = "AViewKey1qQVfici7WarfXgmq9iuH8tzRcrWtb8qYq1pEyRRE4kS7";

        let config = Scan::try_parse_from(
            ["snarkos", "--view-key", TEST_VIEW_KEY, "--start", "0", "--end", "10", "--endpoint", "localhost"].iter(),
        )?;

        let endpoint = Uri::default();
        config.parse_block_range::<CurrentNetwork>(&endpoint).with_context(|| "Failed to parse block range")?;

        // `start` height can't be greater than `end` height.
        let config = Scan::try_parse_from(
            ["snarkos", "--view-key", TEST_VIEW_KEY, "--start", "10", "--end", "5", "--endpoint", "localhost"].iter(),
        )?;

        let endpoint = Uri::default();
        assert!(config.parse_block_range::<CurrentNetwork>(&endpoint).is_err());

        // `last` conflicts with `start`
        let err = Scan::try_parse_from(
            ["snarkos", "--view-key", TEST_VIEW_KEY, "--start", "0", "--last", "10", "--endpoint=localhost"].iter(),
        )
        .unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);

        // `last` conflicts with `end`
        let err = Scan::try_parse_from(
            ["snarkos", "--view-key", TEST_VIEW_KEY, "--end", "10", "--last", "10", "--endpoint", "localhost"].iter(),
        )
        .unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);

        // `last` conflicts with `start` and `end`
        let err = Scan::try_parse_from(
            [
                "snarkos",
                "--view-key",
                TEST_VIEW_KEY,
                "--start",
                "0",
                "--end",
                "01",
                "--last",
                "10",
                "--endpoint",
                "localhost",
            ]
            .iter(),
        )
        .unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);

        Ok(())
    }
}
