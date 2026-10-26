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

mod decrypt;
pub use decrypt::*;

mod deploy;
pub use deploy::*;

mod execute;
pub use execute::*;

mod scan;
pub use scan::*;

mod transfer_private;
pub use transfer_private::*;

use crate::helpers::{
    args::{network_id_parser, prepare_endpoint},
    logger::initialize_terminal_logger,
};

use snarkos_node_rest::{API_VERSION_V1, API_VERSION_V2};
use snarkvm::{
    ledger::store::helpers::memory::BlockMemory,
    package::Package,
    prelude::{query::Query, *},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use clap::{Parser, ValueEnum};
use colored::Colorize;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    path::PathBuf,
    str::FromStr,
    thread,
    time::{Duration, Instant},
};
use tracing::debug;
use ureq::http::{self, Uri};

/// The format to store a generated transaction as.
#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum StoreFormat {
    String,
    Bytes,
}

/// The API version used by an endpoint
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ApiVersion {
    V1,
    V2,
}

/// A ledger `Query` parsed from an endpoint argument, with the REST base URL when the input is a URL.
struct ParsedQuery<N: Network> {
    query: Query<N, BlockMemory<N>>,
    endpoint: Option<Uri>,
}

/// Commands to deploy and execute transactions
#[derive(Debug, Parser)]
pub enum DeveloperCommand {
    /// Decrypt a ciphertext.
    Decrypt(Decrypt),
    /// Deploy a program.
    Deploy(Deploy),
    /// Execute a program function.
    Execute(Execute),
    /// Scan the node for records.
    Scan(Scan),
    /// Execute the `credits.aleo/transfer_private` function.
    TransferPrivate(TransferPrivate),
}

/// Use the Provable explorer's API by default.
/// Note, `v2` here refers to the explorer version, not the snarkOS API version.
const DEFAULT_ENDPOINT: &str = "https://api.explorer.provable.com/v2";

#[derive(Debug, Parser)]
pub struct Developer {
    /// The specific developer command to run.
    #[clap(subcommand)]
    command: DeveloperCommand,
    /// Specify the network to create an execution for.
    /// [options: 0 = mainnet, 1 = testnet, 2 = canary]
    #[clap(long, default_value_t=MainnetV0::ID, long, global=true, value_parser = network_id_parser())]
    network: u16,
    /// Sets verbosity of log output. By default, no logs are shown.
    #[clap(long, global = true)]
    verbosity: Option<u8>,
}

/// The serialized REST error sent over the network.
#[derive(Debug, Deserialize)]
struct RestError {
    /// The type of error (corresponding to the HTTP status code).
    error_type: String,
    /// The top-level error message.
    message: String,
    /// The chain of errors that led to the top-level error.
    /// Default to an empty vector if no error chain was given.
    #[serde(default)]
    chain: Vec<String>,
}

impl RestError {
    /// Converts a `RestError` into an `anyhow::Error`.
    pub fn parse(self) -> anyhow::Error {
        let mut error: Option<anyhow::Error> = None;
        for next in self.chain.into_iter() {
            if let Some(previous) = error {
                error = Some(previous.context(next));
            } else {
                error = Some(anyhow!(next));
            }
        }

        let toplevel = format!("{}: {}", self.error_type, self.message);
        if let Some(error) = error { error.context(toplevel) } else { anyhow!(toplevel) }
    }
}

impl Developer {
    /// Runs the developer subcommand chosen by the user.
    pub fn parse(self) -> Result<String> {
        if let Some(verbosity) = self.verbosity {
            initialize_terminal_logger(verbosity).with_context(|| "Failed to initialize terminal logger")?
        }

        match self.network {
            MainnetV0::ID => self.parse_inner::<MainnetV0>(),
            TestnetV0::ID => self.parse_inner::<TestnetV0>(),
            CanaryV0::ID => self.parse_inner::<CanaryV0>(),
            unknown_id => bail!("Unknown network ID ({unknown_id})"),
        }
    }

    /// Internal logic of [`Self::parse`] for each of the different networks.
    fn parse_inner<N: Network>(self) -> Result<String> {
        use DeveloperCommand::*;

        match self.command {
            Decrypt(decrypt) => decrypt.parse::<N>(),
            Deploy(deploy) => deploy.parse::<N>(),
            Execute(execute) => execute.parse::<N>(),
            Scan(scan) => scan.parse::<N>(),
            TransferPrivate(transfer_private) => transfer_private.parse::<N>(),
        }
    }

    /// Parse the package from the directory.
    fn parse_package<N: Network>(program_id: ProgramID<N>, path: &Option<String>) -> Result<Package<N>> {
        // Instantiate a path to the directory containing the manifest file.
        let directory = match path {
            Some(path) => PathBuf::from_str(path)?,
            None => std::env::current_dir()?,
        };

        // Load the package.
        let package = Package::open(&directory)?;

        ensure!(
            package.program_id() == &program_id,
            "The program name in the package does not match the specified program name"
        );

        // Return the package.
        Ok(package)
    }

    /// Parses the record string. If the string is a ciphertext, then attempt to decrypt it.
    fn parse_record<N: Network>(private_key: &PrivateKey<N>, record: &str) -> Result<Record<N, Plaintext<N>>> {
        match record.starts_with("record1") {
            true => {
                // Parse the ciphertext.
                let ciphertext = Record::<N, Ciphertext<N>>::from_str(record)?;
                // Derive the view key.
                let view_key = ViewKey::try_from(private_key)?;
                // Decrypt the ciphertext.
                ciphertext.decrypt(&view_key)
            }
            false => Record::<N, Plaintext<N>>::from_str(record),
        }
    }

    /// Parses an endpoint URL or a JSON static query.
    ///
    /// Endpoint URLs are normalized before `Query::from_str`.
    fn parse_query<N: Network>(input: &str) -> Result<ParsedQuery<N>> {
        let input = input.trim();

        // JSON static queries are not endpoint URLs, so they skip URI normalization.
        if input.starts_with('{') {
            let query = Query::<N, BlockMemory<N>>::from_str(input)?;
            return Ok(ParsedQuery { query, endpoint: None });
        }

        let endpoint = prepare_endpoint(input.parse().with_context(|| "Invalid endpoint URL")?)?;
        let query = Query::<N, BlockMemory<N>>::from_str(&endpoint.to_string())?;
        Ok(ParsedQuery { query, endpoint: Some(endpoint) })
    }

    /// Builds the full endpoint Uri from the base and path. Used internally for all REST API calls (copied from `snarkvm_ledger_query::Query`).
    /// This will add the API version number and network name to the resulting endpoint URL.
    ///
    /// # Arguments
    ///  - `base_url`: the hostname (and path prefix) of the node to query. this must exclude the network name.
    ///  - `route`: the route to the endpoint (e.g., `stateRoot/latest`). This cannot start with a slash.
    ///
    /// # Returns
    /// The full endpoint Uri and the API version used by it.
    fn build_endpoint<N: Network>(base_url: &http::Uri, route: &str) -> Result<(String, ApiVersion)> {
        // This function is only called internally but check for additional sanity.
        ensure!(!route.starts_with('/'), "path cannot start with a slash");

        // Determine the API version we are interacting with.
        let api_version = {
            let r = base_url.path().trim_end_matches('/');

            if r.ends_with(API_VERSION_V1) {
                ApiVersion::V1
            } else if r.ends_with(API_VERSION_V2) {
                ApiVersion::V2
            } else {
                // Default to v1.
                // Note: If the snarkos-node-rest switches default to v2, this needs to be updated.
                ApiVersion::V1
            }
        };

        // Work around a bug in the `http` crate where empty paths will be set to '/'
        // but other paths are not appended with a slash.
        // See https://github.com/hyperium/http/issues/507
        let sep = if base_url.path().ends_with('/') { "" } else { "/" };

        // Build "{base}/{maybe_version}/{network}/{route}"
        let full_uri = format!("{base_url}{sep}{network}/{route}", network = N::SHORT_NAME);
        Ok((full_uri, api_version))
    }

    /// Converts the returned JSON error (if any) into an anyhow Error chain.
    /// If the error was 404, this simply returns `Ok(None)`.
    fn handle_ureq_result(result: Result<http::Response<ureq::Body>>) -> Result<Option<ureq::Body>> {
        let response = result?;

        if response.status().is_success() {
            Ok(Some(response.into_body()))
        } else if response.status() == http::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            // V2 returns the error in JSON format.
            let is_json = response
                .headers()
                .get(http::header::CONTENT_TYPE)
                .and_then(|h| h.to_str().ok())
                .map(|ct| ct.contains("json"))
                .unwrap_or(false);

            if is_json {
                let rest_error: RestError =
                    response.into_body().read_json().with_context(|| "Failed to parse error JSON")?;

                Err(rest_error.parse())
            } else {
                // V1 returns the error message a string.
                let err_msg = response.into_body().read_to_string()?;
                Err(anyhow!(err_msg))
            }
        }
    }

    /// Extracts the API version from a custom endpoint.
    fn parse_custom_endpoint<N: Network>(url: &Uri) -> (String, ApiVersion) {
        // Determine API version for custom endpoint.
        if let Some(pq) = url.path_and_query()
            && pq.path().ends_with(&format!("{API_VERSION_V2}/{}/transaction/broadcast", N::SHORT_NAME))
        {
            (url.to_string(), ApiVersion::V2)
        } else {
            (url.to_string(), ApiVersion::V1)
        }
    }

    /// Helper function to send a POST request with a JSON body to an endpoint and await a JSON response.
    fn http_post_json<I: Serialize, O: DeserializeOwned>(path: &str, arg: &I) -> Result<Option<O>> {
        debug!("Issuing POST request to \"{path}\"");

        let result =
            ureq::post(path).config().http_status_as_error(false).build().send_json(arg).map_err(|err| err.into());

        match Self::handle_ureq_result(result).with_context(|| format!("HTTP POST request to {path} failed"))? {
            Some(mut body) => {
                let json = body.read_json().with_context(|| format!("Failed to parse JSON response from {path}"))?;
                Ok(Some(json))
            }
            None => Ok(None),
        }
    }

    /// Helper function to send a GET request to an endpoint and await a JSON response.
    fn http_get_json<N: Network, O: DeserializeOwned>(base_url: &http::Uri, route: &str) -> Result<Option<O>> {
        let (endpoint, _api_version) = Self::build_endpoint::<N>(base_url, route)?;
        debug!("Issuing GET request to \"{endpoint}\"");

        let result = ureq::get(&endpoint).config().http_status_as_error(false).build().call().map_err(|err| err.into());

        match Self::handle_ureq_result(result).with_context(|| format!("HTTP GET request to {endpoint} failed"))? {
            Some(mut body) => {
                let json =
                    body.read_json().with_context(|| format!("Failed to parse JSON response from {endpoint}"))?;
                Ok(Some(json))
            }
            None => Ok(None),
        }
    }

    /// Helper function to send a GET request to an endpoint and await the response.
    fn http_get<N: Network>(base_url: &http::Uri, route: &str) -> Result<Option<ureq::Body>> {
        let (endpoint, _api_version) = Self::build_endpoint::<N>(base_url, route)?;
        debug!("Issuing GET request to \"{endpoint}\"");

        let result = ureq::get(&endpoint).config().http_status_as_error(false).build().call().map_err(|err| err.into());

        Self::handle_ureq_result(result).with_context(|| format!("HTTP GET request to {endpoint} failed"))
    }

    /// Wait for a transaction to be confirmed by the network.
    fn wait_for_transaction_confirmation<N: Network>(
        endpoint: &Uri,
        transaction_id: &N::TransactionID,
        timeout_seconds: u64,
        api_version: ApiVersion,
    ) -> Result<()> {
        let start_time = Instant::now();
        let timeout_duration = Duration::from_secs(timeout_seconds);
        let poll_interval = Duration::from_secs(1); // Poll every second

        while start_time.elapsed() < timeout_duration {
            // Check if transaction exists in a confirmed block
            let result = Self::http_get::<N>(endpoint, &format!("transaction/{transaction_id}"));

            match api_version {
                ApiVersion::V1 => match result {
                    Ok(Some(_)) => return Ok(()),
                    Ok(None) => {
                        // Transaction not found yet, continue polling.
                    }
                    Err(err) => {
                        // The V1 API returns 500 on missing transactions. Retroy on any error.
                        eprintln!("Got error when fetching transaction ({err}). Will retry...");
                    }
                },
                ApiVersion::V2 => {
                    // With the V2 API, we can differentiate between benign errors and fatal errors.
                    match result.with_context(|| "Failed to check transaction status")? {
                        Some(_) => return Ok(()),
                        None => {
                            // Transaction not found yet, continue polling.
                        }
                    }
                }
            }

            thread::sleep(poll_interval);
        }

        // Timeout reached
        bail!("❌ Transaction {transaction_id} was not confirmed within {timeout_seconds} seconds");
    }

    /// Gets the latest eidtion of an Aleo program.
    fn get_latest_edition<N: Network>(endpoint: &Uri, program_id: &ProgramID<N>) -> Result<u16> {
        match Self::http_get_json::<N, _>(endpoint, &format!("program/{program_id}/latest_edition"))? {
            Some(edition) => Ok(edition),
            None => bail!("Got unexpected 404 response"),
        }
    }

    /// Gets the public account balance of an Aleo Address (in microcredits).
    fn get_public_balance<N: Network>(endpoint: &Uri, address: &Address<N>) -> Result<Option<u64>> {
        // Initialize the program id and account identifier.
        let account_mapping = Identifier::<N>::from_str("account")?;
        let credits = ProgramID::<N>::from_str("credits.aleo")?;

        // Request the balance from the endpoint.
        // If no such balance/account exists, the node returns status code 200 with `null` as the response body.
        // Nodes should never return 404 for this endpoint.
        let result: Option<Value<N>> =
            Self::http_get_json::<N, _>(endpoint, &format!("program/{credits}/mapping/{account_mapping}/{address}"))?
                .ok_or_else(|| anyhow!("Got unexpected 404 error when fetching public balance"))?;

        // Return the balance in microcredits.
        match result {
            Some(Value::Plaintext(Plaintext::Literal(Literal::<N>::U64(amount), _))) => Ok(Some(*amount)),
            Some(..) => bail!("Failed to deserialize balance for {address}"),
            None => Ok(None),
        }
    }

    /// Determine if the transaction should be broadcast or displayed to user.
    ///
    /// This function expects that exactly one of `dry_run`, `store`, and `broadcast` are `true` (or `Some`).
    /// `endpoint` is the REST base URL used to query node state, or `None` for a static query.
    /// `broadcast` can be set to `Some(None)` to broadcast using the query endpoint.
    /// Alternatively, it can be set to `Some(Some(url))` to providifferent
    /// endpoint than that used for querying.
    #[allow(clippy::too_many_arguments)]
    fn handle_transaction<N: Network>(
        endpoint: Option<&Uri>,
        broadcast: &Option<Option<Uri>>,
        dry_run: bool,
        store: &Option<String>,
        store_format: StoreFormat,
        wait: bool,
        timeout: u64,
        transaction: Transaction<N>,
        operation: String,
    ) -> Result<String> {
        // Get the transaction id.
        let transaction_id = transaction.id();

        // Ensure the transaction is not a fee transaction.
        ensure!(!transaction.is_fee(), "The transaction is a fee transaction and cannot be broadcast");

        // Determine if the transaction should be stored.
        if let Some(path) = store {
            match PathBuf::from_str(path) {
                Ok(file_path) => {
                    match store_format {
                        StoreFormat::Bytes => {
                            let transaction_bytes = transaction.to_bytes_le()?;
                            std::fs::write(&file_path, transaction_bytes)?;
                        }
                        StoreFormat::String => {
                            let transaction_string = transaction.to_string();
                            std::fs::write(&file_path, transaction_string)?;
                        }
                    }

                    println!(
                        "Transaction {transaction_id} was stored to {} as {:?}",
                        file_path.display(),
                        store_format
                    );
                }
                Err(err) => {
                    println!("The transaction was unable to be stored due to: {err}");
                }
            }
        };

        // Determine if the transaction should be broadcast to the network.
        if let Some(broadcast_value) = broadcast {
            let (broadcast_endpoint, api_version) = if let Some(url) = broadcast_value {
                debug!("Using custom endpoint for broadcasting: {url}");
                Self::parse_custom_endpoint::<N>(url)
            } else {
                let endpoint = endpoint
                    .context("Cannot broadcast a transaction built from a static query without a broadcast URL")?;
                Self::build_endpoint::<N>(endpoint, "transaction/broadcast")?
            };

            let result: Result<String> = match Self::http_post_json(&broadcast_endpoint, &transaction) {
                Ok(Some(s)) => Ok(s),
                Ok(None) => Err(anyhow!("Got unexpected 404 error")),
                Err(err) => Err(err),
            };

            match result {
                Ok(response_string) => {
                    ensure!(
                        response_string == transaction_id.to_string(),
                        "The response does not match the transaction id. ({response_string} != {transaction_id})"
                    );

                    match transaction {
                        Transaction::Deploy(..) => {
                            println!(
                                "⌛ Deployment {transaction_id} ('{}') has been broadcast to {}.",
                                operation.bold(),
                                broadcast_endpoint
                            )
                        }
                        Transaction::Execute(..) => {
                            println!(
                                "⌛ Execution {transaction_id} ('{}') has been broadcast to {}.",
                                operation.bold(),
                                broadcast_endpoint
                            )
                        }
                        _ => unreachable!(),
                    }

                    // If wait is enabled, wait for transaction confirmation
                    if wait {
                        let endpoint = endpoint
                            .context("Cannot wait for confirmation of a transaction built from a static query")?;
                        println!("⏳ Waiting for transaction confirmation (timeout: {timeout}s)...");
                        Self::wait_for_transaction_confirmation::<N>(endpoint, &transaction_id, timeout, api_version)?;

                        match transaction {
                            Transaction::Deploy(..) => {
                                println!("✅ Deployment {transaction_id} ('{}') confirmed!", operation.bold())
                            }
                            Transaction::Execute(..) => {
                                println!("✅ Execution {transaction_id} ('{}') confirmed!", operation.bold())
                            }
                            Transaction::Fee(..) => unreachable!(),
                        }
                    }
                }
                Err(error) => match transaction {
                    Transaction::Deploy(..) => {
                        return Err(error.context(anyhow!(
                            "Failed to deploy '{op}' to {broadcast_endpoint}",
                            op = operation.bold()
                        )));
                    }
                    Transaction::Execute(..) => {
                        return Err(error.context(anyhow!(
                            "Failed to broadcast execution '{op}' to {broadcast_endpoint}",
                            op = operation.bold()
                        )));
                    }
                    Transaction::Fee(..) => unreachable!(),
                },
            };

            // Output the transaction id.
            Ok(transaction_id.to_string())
        } else if dry_run {
            // Output the transaction string.
            Ok(transaction.to_string())
        } else {
            Ok("".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use snarkvm::{console::network::TestnetV0, ledger::test_helpers::CurrentNetwork, prelude::query::QueryTrait};

    const STATIC_QUERY: &str =
        r#"{"state_root": "sr1dz06ur5spdgzkguh4pr42mvft6u3nwsg5drh9rdja9v8jpcz3czsls9geg", "height": 14}"#;

    #[test]
    fn test_parse_query_static() {
        let ParsedQuery { query, endpoint } = Developer::parse_query::<TestnetV0>(STATIC_QUERY).unwrap();
        assert!(matches!(query, Query::STATIC(_)));
        assert!(endpoint.is_none());
        assert_eq!(query.current_block_height().unwrap(), 14);
    }

    #[test]
    fn test_parse_query_static_invalid() {
        let json = r#"{"invalid_key": "sr1dz06ur5spdgzkguh4pr42mvft6u3nwsg5drh9rdja9v8jpcz3czsls9geg", "height": 14}"#;
        assert!(Developer::parse_query::<TestnetV0>(json).is_err());
    }

    #[test]
    fn test_parse_query_rest_defaults_scheme() {
        let ParsedQuery { query, endpoint } = Developer::parse_query::<TestnetV0>("localhost:3030").unwrap();
        assert!(matches!(query, Query::REST(_)));
        assert_eq!(endpoint.unwrap(), "http://localhost:3030/");
    }

    #[test]
    fn test_parse_query_rest_keeps_https_path() {
        let ParsedQuery { query, endpoint } = Developer::parse_query::<TestnetV0>(DEFAULT_ENDPOINT).unwrap();
        assert!(matches!(query, Query::REST(_)));
        assert_eq!(endpoint.unwrap(), DEFAULT_ENDPOINT);
    }

    /// Test that the default endpoints (V1) work as expected.
    ///
    /// Note, if the default endpoint ever changes, this test needs to be updated.
    #[test]
    fn test_build_endpoint_default_v1() {
        let base_uri_str = "http://localhost:3030";
        let base_uri = Uri::try_from(base_uri_str).unwrap();
        let (endpoint, api_version) =
            Developer::build_endpoint::<CurrentNetwork>(&base_uri, "transaction/broadcast").unwrap();

        assert_eq!(endpoint, format!("{base_uri_str}/{}/transaction/broadcast", CurrentNetwork::SHORT_NAME));
        assert_eq!(api_version, ApiVersion::V1);
    }

    /// Ensure that the V1 endpoints work as expected.
    #[test]
    fn test_build_endpoint_v1() {
        let base_uri_str = "http://localhost:3030/v1";
        let base_uri = Uri::try_from(base_uri_str).unwrap();
        let (endpoint, api_version) =
            Developer::build_endpoint::<CurrentNetwork>(&base_uri, "transaction/broadcast").unwrap();

        assert_eq!(endpoint, format!("{base_uri_str}/{}/transaction/broadcast", CurrentNetwork::SHORT_NAME));
        assert_eq!(api_version, ApiVersion::V1);
    }

    /// Ensure that the V2 endpoints work as expected.
    #[test]
    fn test_build_endpoint_v2() {
        let base_uri_str = "http://localhost:3030/v2";
        let base_uri = Uri::try_from(base_uri_str).unwrap();
        let (endpoint, api_version) =
            Developer::build_endpoint::<CurrentNetwork>(&base_uri, "transaction/broadcast").unwrap();

        assert_eq!(endpoint, format!("{base_uri_str}/{}/transaction/broadcast", CurrentNetwork::SHORT_NAME));
        assert_eq!(api_version, ApiVersion::V2);
    }

    #[test]
    fn test_custom_endpoint_v1() {
        let endpoint_str = "http://localhost:3030/v1/mainnet/transaction/broadcast";
        let endpoint = Uri::try_from(endpoint_str).unwrap();

        let (parsed, api_version) = Developer::parse_custom_endpoint::<CurrentNetwork>(&endpoint);

        assert_eq!(parsed, endpoint_str);
        assert_eq!(api_version, ApiVersion::V1);
    }

    #[test]
    fn test_custom_endpoint_v2() {
        let endpoint_str = "http://localhost:3030/v2/mainnet/transaction/broadcast";
        let endpoint = Uri::try_from(endpoint_str).unwrap();

        let (parsed, api_version) = Developer::parse_custom_endpoint::<CurrentNetwork>(&endpoint);

        assert_eq!(parsed, endpoint_str);
        assert_eq!(api_version, ApiVersion::V2);
    }
}
