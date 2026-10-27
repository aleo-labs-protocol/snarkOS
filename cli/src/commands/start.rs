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

use crate::helpers::{
    args::{network_id_parser, parse_node_data_dir},
    dev::*,
};

use snarkos_account::Account;
use snarkos_display::Display;
use snarkos_node::{
    Node,
    bft::MEMORY_POOL_PORT,
    network::{NodeType, bootstrap_peers},
    rest::{DEFAULT_REST_PORT, RestVerificationLimits},
    router::DEFAULT_NODE_PORT,
};
use snarkos_utilities::{DevHotswapConfig, NodeDataDir, SignalHandler, jwt_secret_file, node_data};

use snarkvm::{
    console::{
        account::{Address, PrivateKey},
        algorithms::Hash,
        network::{CanaryV0, MainnetV0, Network, TestnetV0},
    },
    ledger::{
        block::Block,
        committee::{Committee, MIN_DELEGATOR_STAKE, MIN_VALIDATOR_STAKE},
        store::{ConsensusStore, helpers::memory::ConsensusMemory},
    },
    prelude::{FromBytes, Itertools, ToBits, ToBytes},
    synthesizer::VM,
    utilities::to_bytes_le,
};

use aleo_std::{StorageMode, aleo_dir, aleo_ledger_dir};
use anyhow::{Context, Result, anyhow, bail, ensure};
use base64::prelude::{BASE64_STANDARD, Engine};
use clap::{Parser, builder::RangedU64ValueParser};
use colored::Colorize;
use core::str::FromStr;
use indexmap::IndexMap;
use rand::{RngExt, SeedableRng};
use rand_chacha::ChaChaRng;
use serde::{Deserialize, Serialize};

use std::{
    fs,
    io::IsTerminal,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, ToSocketAddrs},
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicBool},
};
use tokio::{
    runtime::{self, Handle, Runtime},
    sync::mpsc,
    task,
};
use tracing::{debug, info, warn};
use ureq::http;

/// The recommended minimum number of 'open files' limit for a validator.
/// Validators should be able to handle at least 1000 concurrent connections, each requiring 2 sockets.
#[cfg(target_family = "unix")]
const RECOMMENDED_MIN_NOFILES_LIMIT: u64 = 2048;

/// Default (and maximum) concurrent REST deploy verifications.
const DEFAULT_NUM_VERIFYING_DEPLOYS: usize =
    VM::<MainnetV0, ConsensusMemory<MainnetV0>>::MAX_PARALLEL_DEPLOY_VERIFICATIONS;
/// Default (and maximum) concurrent REST execute verifications.
const DEFAULT_NUM_VERIFYING_EXECUTIONS: usize =
    VM::<MainnetV0, ConsensusMemory<MainnetV0>>::MAX_PARALLEL_EXECUTE_VERIFICATIONS;
/// Default (and maximum) concurrent REST solution verifications.
const DEFAULT_NUM_VERIFYING_SOLUTIONS: usize = MainnetV0::MAX_SOLUTIONS;

fn num_verifying_deploys_parser() -> RangedU64ValueParser<usize> {
    RangedU64ValueParser::<usize>::new().range(1..=DEFAULT_NUM_VERIFYING_DEPLOYS as u64)
}

fn num_verifying_executions_parser() -> RangedU64ValueParser<usize> {
    RangedU64ValueParser::<usize>::new().range(1..=DEFAULT_NUM_VERIFYING_EXECUTIONS as u64)
}

fn num_verifying_solutions_parser() -> RangedU64ValueParser<usize> {
    RangedU64ValueParser::<usize>::new().range(1..=DEFAULT_NUM_VERIFYING_SOLUTIONS as u64)
}

fn proposal_spent_limit_multiplier_parser() -> RangedU64ValueParser<u64> {
    RangedU64ValueParser::<u64>::new().range(1..)
}

// A mapping of `staker_address` to `(validator_address, withdrawal_address, amount)`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct BondedBalances(IndexMap<String, (String, String, u64)>);

impl FromStr for BondedBalances {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s)
    }
}

// Starts the snarkOS node.
#[derive(Clone, Debug, Parser)]
#[command(
    // Use kebab-case for all arguments (e.g., use the `private-key` flag for the `private_key` field).
    // This is already the default, but we specify it in case clap's default changes in the future.
    rename_all = "kebab-case",

    // Ensure at most one node type is specified.
    group(clap::ArgGroup::new("node_type").required(false).multiple(false)
),

    // Ensure all other dev flags can only be set if `--dev` is set.
    group(clap::ArgGroup::new("dev_flags").required(false).multiple(true).requires("dev")
),
    // Ensure any rest flag (including `--rest`) cannot be set
    // if `--norest` is set.
    group(clap::ArgGroup::new("rest_flags").required(false).multiple(true).conflicts_with("norest")),

    // Ensure you cannot set --verbosity and --log-filter flags at the same time.
    group(clap::ArgGroup::new("log_flags").required(false).multiple(false)),

    // Ensure you need to set either --jwt-secret and --jwt-timestamp or --nojwt flags.
    group(clap::ArgGroup::new("jwt_flags").required(false).multiple(true).conflicts_with("nojwt").conflicts_with("norest")),
)]
pub struct Start {
    /// Specify the network ID of this node
    /// [options: 0 = mainnet, 1 = testnet, 2 = canary]
    #[clap(long, default_value_t=MainnetV0::ID, long, value_parser = network_id_parser())]
    pub network: u16,

    /// Start the node as a prover.
    #[clap(long, group = "node_type")]
    pub prover: bool,

    /// Start the node as a client (default).
    ///
    /// Client are "full nodes", i.e, validate and execute all blocks they receive, but they do not participate in AleoBFT consensus.
    #[clap(long, group = "node_type", verbatim_doc_comment)]
    pub client: bool,

    /// Start the node as a bootstrap client.
    #[clap(long = "bootstrap-client", group = "node_type", conflicts_with_all = ["peers", "validators"], verbatim_doc_comment)]
    pub bootstrap_client: bool,

    /// Start the node as a validator.
    ///
    /// Validators are "full nodes", like clients, but also participate in AleoBFT.
    #[clap(long, group = "node_type", verbatim_doc_comment)]
    pub validator: bool,

    /// Specify the account private key of the node
    #[clap(long)]
    pub private_key: Option<String>,

    /// Specify the path to a file containing the account private key of the node
    #[clap(long = "private-key-file")]
    pub private_key_file: Option<PathBuf>,

    /// Set the IP address and port used for P2P communication.
    #[clap(long)]
    pub node: Option<SocketAddr>,

    /// Set the IP address and port used for BFT communication.
    /// This argument is only allowed for validator nodes.
    #[clap(long, requires = "validator")]
    pub bft: Option<SocketAddr>,

    /// Multiply the batch spend limit used when this validator builds a proposal.
    ///
    /// This argument is only allowed for validator nodes.
    /// The default of 1 leaves the batch spend limit unchanged.
    #[clap(
        long,
        requires = "validator",
        conflicts_with_all = ["client", "prover", "bootstrap_client"],
        default_value_t = 1,
        value_parser = proposal_spent_limit_multiplier_parser()
    )]
    pub proposal_spent_limit_multiplier: u64,

    /// Specify the host:port address pairs of the peer(s) to connect to (as a comma-separated list).
    ///
    /// These peers will be set as "trusted", which means the node will not disconnect from them when performing peer rotation.
    ///
    /// Setting peers to "" has the same effect as not setting the flag at all, except when using `--dev`.
    #[clap(long, verbatim_doc_comment)]
    pub peers: Option<String>,

    /// Specify the host:port address pairs of the validator(s) to connect to.
    #[clap(long)]
    pub validators: Option<String>,

    /// [DEPRECATED] [NO-OP] Allow untrusted peers (not listed in `--peers`) to connect.
    ///
    /// The flag will be ignored by client and prover nodes, as this behavior is always enabled for these types of nodes.
    #[clap(long, verbatim_doc_comment)]
    pub allow_external_peers: bool,

    /// [DEPRECATED] [NO-OP] If the flag is set, a client will periodically evict more external peers
    #[clap(long)]
    pub rotate_external_peers: bool,

    /// Specify the IP address and port for the REST server
    #[clap(long, group = "rest_flags")]
    pub rest: Option<SocketAddr>,

    /// Specify the requests per second (RPS) rate limit per IP for the REST server
    #[clap(long, default_value_t = 10, group = "rest_flags")]
    pub rest_rps: u32,

    /// Specify the maximum number of concurrent REST deploy transaction verifications.
    ///
    /// Defaults to, and cannot exceed, `MAX_PARALLEL_DEPLOY_VERIFICATIONS`.
    #[clap(
        long,
        default_value_t = DEFAULT_NUM_VERIFYING_DEPLOYS,
        value_parser = num_verifying_deploys_parser(),
        group = "rest_flags"
    )]
    pub num_verifying_deploys: usize,

    /// Specify the maximum number of concurrent REST execute transaction verifications.
    ///
    /// Defaults to, and cannot exceed, `MAX_PARALLEL_EXECUTE_VERIFICATIONS`.
    #[clap(
        long,
        default_value_t = DEFAULT_NUM_VERIFYING_EXECUTIONS,
        value_parser = num_verifying_executions_parser(),
        group = "rest_flags"
    )]
    pub num_verifying_executions: usize,

    /// Specify the maximum number of concurrent REST solution verifications.
    ///
    /// Defaults to, and cannot exceed, `MAX_SOLUTIONS`.
    #[clap(
        long,
        default_value_t = DEFAULT_NUM_VERIFYING_SOLUTIONS,
        value_parser = num_verifying_solutions_parser(),
        group = "rest_flags"
    )]
    pub num_verifying_solutions: usize,

    /// Serve the routes of the removed `history` feature (`/program/{id}/mapping/{name}/{key}/history/{height}`,
    /// `/staking/rewards/{address}/{height}`, ...) from the Provable historical staking API instead of local
    /// tables. Takes an optional base URL of that API; by default, the network's own instance is used.
    #[clap(long, value_name = "URL", num_args = 0..=1, group = "rest_flags", conflicts_with = "history_json")]
    pub history_compat_mode: Option<Option<String>>,

    /// Write credits.aleo mapping history as JSON while this node finalizes blocks.
    ///
    /// Files are `history-{network}/group-{g}/block-{height}/block-{height}-{mapping}.json` beside the
    /// ledger. A client or a validator can set this flag. `GET /block/{height}/history/{mapping}` reads
    /// those files.
    #[clap(long, conflicts_with = "history_compat_mode")]
    pub history_json: bool,

    /// Specify the JWT secret for the REST server (16B, base64-encoded).
    #[clap(long, group = "jwt_flags")]
    pub jwt_secret: Option<String>,

    /// Specify the JWT creation timestamp; can be any time in the last 10 years.
    #[clap(long, group = "jwt_flags")]
    pub jwt_timestamp: Option<i64>,

    /// If the flag is set, the node will not initialize the REST server.
    #[clap(long)]
    pub norest: bool,

    /// If the flag is set, the node will not require JWT authentication for the REST server.
    #[clap(long, group = "rest_flags")]
    pub nojwt: bool,

    /// If the flag is set, the node will only connect to trusted peers and validators.
    #[clap(long)]
    pub trusted_peers_only: bool,

    /// Write log message to stdout instead of showing a terminal UI.
    ///
    /// This is useful, for example, for running a node as a service instead of in the foreground or to pipe its output into a file.
    #[clap(long, verbatim_doc_comment)]
    pub nodisplay: bool,

    /// Do not show the Aleo banner and information about the node on startup.
    #[clap(long, hide = true)]
    pub nobanner: bool,

    /// Specify the log verbosity of the node.
    /// [options: 0 (lowest log level) to 6 (highest level)]
    #[clap(long, default_value_t = 1, group = "log_flags")]
    pub verbosity: u8,

    /// Set a custom log filtering scheme, e.g., "off,snarkos_bft=trace", to show all log messages of snarkos_bft but nothing else.
    #[clap(long, group = "log_flags")]
    pub log_filter: Option<String>,

    /// Specify the path to the file where logs will be stored
    #[clap(long, default_value_os_t = std::env::temp_dir().join("snarkos.log"))]
    pub logfile: PathBuf,

    /// Enable the metrics exporter
    #[cfg(feature = "metrics")]
    #[clap(long)]
    pub metrics: bool,

    /// Specify the IP address and port for the metrics exporter
    #[cfg(feature = "metrics")]
    #[clap(long, requires = "metrics")]
    pub metrics_ip: Option<SocketAddr>,

    /// Specify the directory that holds all ledger data, e.g., blocks and transactions.
    /// This flag overrides the default path, even when `--dev` is set.
    ///
    /// The old name for this flag (`--storage`) is DEPRECATED and will eventually be removed.
    #[clap(long, verbatim_doc_comment, alias = "storage")]
    pub ledger_storage: Option<PathBuf>,

    /// Specify the directory that holds node-specific data, that is not part of the global ledger.
    /// This flag overrides the default path, even when `--dev` is set.
    ///
    /// That folder may contain sensitive data, such as the JWT secret, and should not be shared with untrusted parties.
    /// For validators, it also contains the latest proposal cache, which is required to participate in consensus.
    #[clap(long, verbatim_doc_comment)]
    pub node_data_storage: Option<PathBuf>,

    /// If specified, the node will automatically save database checkpoints.
    #[clap(long)]
    pub auto_db_checkpoints: Option<PathBuf>,

    /// Enables the node to prefetch initial blocks from a CDN
    #[clap(long, conflicts_with = "nocdn")]
    pub cdn: Option<http::Uri>,

    /// If the flag is set, the node will not prefetch from a CDN
    #[clap(long)]
    pub nocdn: bool,

    /// Enables development mode used to set up test networks.
    ///
    /// The purpose of this flag is to run multiple nodes on the same machine and in the same working directory.
    /// To do this, set the value to a unique ID within the test work. For example if there are four nodes in the network, pass `--dev 0` for the first node, `--dev 1` for the second, and so forth.
    ///
    /// If you do not explicitly set the `--peers` flag, this will also populate the set of trusted peers, so that the network is fully connected.
    /// Additionally, if you do not set the `--rest` or the `--norest` flags, it will also set the REST port to `3030` for the first node, `3031` for the second, and so forth.
    #[clap(long, verbatim_doc_comment)]
    pub dev: Option<u16>,

    /// If development mode is enabled, specify the number of genesis validator.
    #[clap(long, group = "dev_flags", default_value_t=DEVELOPMENT_MODE_NUM_GENESIS_COMMITTEE_MEMBERS)]
    pub dev_num_validators: u16,

    /// If development mode is enabled, specify the number of clients.
    /// This is only used by validators to automatically populate their set of trusted peers.
    ///
    /// This option cannot be used while also passing the `--peers` flag.
    #[clap(long, group = "dev_flags", conflicts_with = "peers")]
    pub dev_num_clients: Option<u16>,

    /// If development mode is enabled, specify whether node 0 should generate traffic to drive the network.
    #[clap(long, group = "dev_flags")]
    pub no_dev_txs: bool,

    /// If development mode is enabled, specify the custom bonded balances as a JSON object.
    #[clap(long, group = "dev_flags")]
    pub dev_bonded_balances: Option<BondedBalances>,

    /// If development mode is enabled, specify whether to run the node on a production ledger.
    #[clap(long, group = "dev_flags", requires = "dev_num_validators", default_value_t = false)]
    pub dev_on_prod: bool,

    /// If the flag is set, the node will attempt to automatically migrate the node data to the new format.
    #[clap(long)]
    pub auto_migrate_node_data: bool,
}

impl Start {
    /// Starts the snarkOS node and blocks until it terminates.
    pub fn parse(self) -> Result<String> {
        // Prepare the shutdown flag.
        let shutdown: Arc<AtomicBool> = Default::default();

        // Initialize the logger.
        let log_receiver = crate::helpers::initialize_logger(
            self.verbosity,
            &self.log_filter,
            self.nodisplay,
            self.logfile.clone(),
            shutdown.clone(),
        )
        .with_context(|| "Failed to set up logger")?;

        // When running in a non-interactive session, disallow the use of the terminal UI.
        if !std::io::stdout().is_terminal() && !self.nodisplay {
            anyhow::bail!(
                "snarkOS cannot use the terminal UI in a non-interactive session. Please restart with `--nodisplay`."
            );
        }

        // Initialize the runtime.
        let runtime = Self::runtime();
        let handle = runtime.handle().clone();
        runtime.block_on(async move {
            // Error messages.
            let node_parse_error = || "Failed to start node";

            // Periodically check if the number of file descriptors isn't becoming insufficient.
            #[cfg(unix)]
            let fd_check_handle = crate::helpers::spawn_fd_monitor();

            // Clone the configurations.
            let mut self_ = self.clone();

            // Parse the node arguments, start it, and block until shutdown.
            match self_.network {
                MainnetV0::ID => {
                    self_.parse_node::<MainnetV0>(handle, log_receiver).await.with_context(node_parse_error)?
                }
                TestnetV0::ID => {
                    self_.parse_node::<TestnetV0>(handle, log_receiver).await.with_context(node_parse_error)?
                }
                CanaryV0::ID => {
                    self_.parse_node::<CanaryV0>(handle, log_receiver).await.with_context(node_parse_error)?
                }
                _ => panic!("Invalid network ID specified"),
            };

            #[cfg(unix)]
            fd_check_handle.abort();

            Ok(String::new())
        })
    }
}

impl Start {
    /// Returns the initial peer(s) to connect to, from the given configurations.
    fn parse_trusted_addrs(&self, list: &Option<String>) -> Result<Vec<SocketAddr>> {
        let Some(list) = list else { return Ok(vec![]) };

        match list.is_empty() {
            // Split on an empty string returns an empty string.
            true => Ok(vec![]),
            false => list.split(',').map(resolve_potential_hostnames).collect(),
        }
    }

    /// Returns the base URL of the historical staking API to serve the `history` routes from, if
    /// `--history-compat-mode` is set: the given URL, or by default the network's own instance,
    /// `https://{network}.historical-staking.provable.com` (mainnet and testnet have one; canary
    /// does not, so it needs an explicit URL).
    ///
    /// A `--dev` network has no upstream: its heights and addresses are unrelated to any public
    /// network's, so the default would serve unrelated data, and an explicit URL is required.
    fn parse_history_api_url<N: Network>(&self) -> Result<Option<String>> {
        let Some(url) = &self.history_compat_mode else {
            return Ok(None);
        };
        let url = match url {
            Some(url) => url.clone(),
            None if self.dev.is_some() => {
                bail!("`--history-compat-mode` needs an explicit URL on a development network")
            }
            None => format!("https://{}.historical-staking.provable.com", N::SHORT_NAME),
        };
        // Fail at startup rather than on every request.
        let parsed =
            http::Uri::try_from(&url).with_context(|| format!("Invalid `--history-compat-mode` URL '{url}'"))?;
        ensure!(
            parsed.scheme().is_some() && parsed.host().is_some(),
            "The `--history-compat-mode` URL '{url}' must be absolute, e.g. https://mainnet.historical-staking.provable.com"
        );
        Ok(Some(url))
    }

    /// Returns the CDN to prefetch initial blocks from, or `None` if fetching from the CDN is disabled.
    fn parse_cdn<N: Network>(&self) -> Result<Option<http::Uri>> {
        // Disable CDN if:
        //  1. The node is in development mode.
        //  2. The user has explicitly disabled CDN.
        //  3. The node is a prover (no need to sync).
        let no_cdn_reasons = [("--dev", self.dev.is_some()), ("--nocdn", self.nocdn), ("--prover", self.prover)]
            .into_iter()
            .filter_map(|(reason, flag_set)| flag_set.then_some(reason))
            .join(" and ");
        if !no_cdn_reasons.is_empty() {
            info!("CDN disabled because the following flags are set: {no_cdn_reasons}.");
            Ok(None)
        }
        // Enable the CDN otherwise.
        else {
            // Determine the CDN URL.
            match &self.cdn {
                // Use the provided CDN URL if it is not empty.
                Some(cdn) => match cdn.to_string().is_empty() {
                    true => Ok(None),
                    false => Ok(Some(cdn.clone())),
                },
                // If no CDN URL is provided, determine the CDN URL based on the network ID.
                None => {
                    let uri = format!("{}/{}", snarkos_node_cdn::CDN_BASE_URL, N::SHORT_NAME);
                    Ok(Some(http::Uri::try_from(&uri).with_context(|| "Unexpected error")?))
                }
            }
        }
    }

    /// Read the private key directly from an argument or from a filesystem location,
    /// returning the Aleo account.
    fn parse_private_key<N: Network>(&self) -> Result<Account<N>> {
        match self.dev {
            None => match (&self.private_key, &self.private_key_file) {
                // Parse the private key directly.
                (Some(private_key), None) => Account::from_str(private_key.trim()),
                // Parse the private key from a file.
                (None, Some(path)) => {
                    check_permissions(path)?;
                    Account::from_str(std::fs::read_to_string(path)?.trim())
                }
                // Ensure the private key is provided to the CLI, except for clients or nodes in development mode.
                (None, None) => match self.client {
                    true => Account::new(&mut rand::rng()),
                    false => bail!("Missing the '--private-key' or '--private-key-file' argument"),
                },
                // Ensure only one private key flag is provided to the CLI.
                (Some(_), Some(_)) => {
                    bail!("Cannot use '--private-key' and '--private-key-file' simultaneously, please use only one")
                }
            },
            Some(index) => {
                let private_key = get_development_key(index)?;
                if !self.nobanner {
                    println!(
                        "🔑 Your development private key for node {index} is {}.\n",
                        private_key.to_string().bold()
                    );
                }
                Account::try_from(private_key)
            }
        }
    }

    /// Updates the configurations if the node is in development mode.
    fn parse_development(
        &mut self,
        trusted_peers: &mut Vec<SocketAddr>,
        trusted_validators: &mut Vec<SocketAddr>,
    ) -> Result<()> {
        // If `--dev` is not set, return early.
        let Some(dev) = self.dev else {
            return Ok(());
        };

        // Determine the number of development validators.
        let num_validators = self.dev_num_validators;
        ensure!(num_validators >= 4, "Value for `dev_num_validators` is too low. Needs to be at least 4.");

        // If `--dev` is set, assume the dev nodes are initialized from 0 to `dev`,
        // and add each of them to the trusted peers. In addition, set the node IP to `4130 + dev`,
        // and the REST port to `3030 + dev`.
        info!("Development mode enabled with index={dev} and num_validators={num_validators}.");

        // Nodes only start as validators if the `--validator` flag is set, because the default mode is "client".
        let is_validator = self.validator;

        // Ensure the node type and `dev_num_validators` are compatible.
        if is_validator {
            ensure!(
                dev < num_validators,
                "Development validator index is too high (dev={dev}, dev_num_validators={num_validators})",
            );
        }
        // A dev client or prover is allowed to have an index lower than
        // `dev_num_validators` in order to have a balance at startup.

        // Add the dev nodes to the trusted validators.
        if trusted_validators.is_empty() && is_validator {
            // Validators add all other validators as trusted.
            for idx in 0..num_validators {
                if idx == dev {
                    continue;
                }
                trusted_validators.push(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, MEMORY_POOL_PORT + idx)));
            }

            debug!("Trusted validators set to: {trusted_validators:?}");
        }

        // Determine if we need to populate `trusted_peers`.
        if trusted_peers.is_empty() {
            if is_validator {
                if let Some(num_clients) = self.dev_num_clients {
                    // Ensure the clients that added this validator as a trusted peer are able to connect to it.
                    for client_idx in 0..num_clients {
                        if get_devnet_validators_for_client(client_idx, num_validators).contains(&dev) {
                            let node_idx = num_validators + client_idx;
                            trusted_peers.push(get_devnet_router_address_for_node(node_idx));
                        }
                    }
                } else {
                    warn!(
                        "Development validator started without trusted peers or `--dev-num-clients`. No clients will be able to connect to it."
                    );
                }
            } else {
                // Clients/provers add two validators to connect to.
                for validator_idx in get_devnet_validators_for_client(dev, num_validators) {
                    trusted_peers.push(get_devnet_router_address_for_node(validator_idx));
                }
            }

            debug!("Trusted peers set to: {trusted_peers:?}");
        } else {
            debug!("Trusted peers/validators was set manually. Will not populate them with development addresses.")
        }

        // Set the node's listening port to `4130 + dev`.
        //
        // Note: the `node` flag is an option to detect remote devnet testing.
        if self.node.is_none() {
            // Pick 0.0.0.0 here, not localhost.
            let port = get_devnet_router_address_for_node(dev).port();
            let address = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port));
            debug!("Setting node address to {address} due to dev={dev}");
            self.node = Some(address);
        }

        // If the `norest` flag is not set and the REST IP is not already specified set the REST IP to `3030 + dev`.
        if !self.norest && self.rest.is_none() {
            let port = DEFAULT_REST_PORT + dev;
            debug!("Setting REST port to {port} due to dev={dev}");
            self.rest = Some(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port)));
        }

        Ok(())
    }

    /// Returns the path to where the JWT secret for the node is stored.
    fn jwt_secret_path<N: Network>(node_data_dir: &NodeDataDir, address: &Address<N>) -> PathBuf {
        node_data_dir.path().join(jwt_secret_file(address))
    }

    /// Returns an alternative genesis block if the node is in development mode.
    /// Otherwise, returns the actual genesis block.
    fn parse_genesis<N: Network>(&self) -> Result<Block<N>> {
        if self.dev.is_some() && !self.dev_on_prod {
            // Determine the number of genesis committee members.
            let num_committee_members = self.dev_num_validators;
            ensure!(
                num_committee_members >= DEVELOPMENT_MODE_NUM_GENESIS_COMMITTEE_MEMBERS,
                "Number of genesis committee members is too low"
            );

            // Initialize the (fixed) RNG.
            let mut rng = ChaChaRng::seed_from_u64(DEVELOPMENT_MODE_RNG_SEED);
            // Initialize the development private keys.
            let dev_keys =
                (0..num_committee_members).map(|_| PrivateKey::<N>::new(&mut rng)).collect::<Result<Vec<_>>>()?;
            // Initialize the development addresses.
            let development_addresses = dev_keys.iter().map(Address::<N>::try_from).collect::<Result<Vec<_>>>()?;

            // Construct the committee based on the state of the bonded balances.
            let (committee, bonded_balances) = match &self.dev_bonded_balances {
                Some(bonded_balances) => {
                    // Parse the bonded balances.
                    let bonded_balances = bonded_balances
                        .0
                        .iter()
                        .map(|(staker_address, (validator_address, withdrawal_address, amount))| {
                            let staker_addr = Address::<N>::from_str(staker_address)?;
                            let validator_addr = Address::<N>::from_str(validator_address)?;
                            let withdrawal_addr = Address::<N>::from_str(withdrawal_address)?;
                            Ok((staker_addr, (validator_addr, withdrawal_addr, *amount)))
                        })
                        .collect::<Result<IndexMap<_, _>>>()?;

                    // Construct the committee members.
                    let mut members = IndexMap::new();
                    for (staker_address, (validator_address, _, amount)) in bonded_balances.iter() {
                        // Ensure that the staking amount is sufficient.
                        match staker_address == validator_address {
                            true => ensure!(amount >= &MIN_VALIDATOR_STAKE, "Validator stake is too low"),
                            false => ensure!(amount >= &MIN_DELEGATOR_STAKE, "Delegator stake is too low"),
                        }

                        // Ensure that the validator address is included in the list of development addresses.
                        ensure!(
                            development_addresses.contains(validator_address),
                            "Validator address {validator_address} is not included in the list of development addresses"
                        );

                        // Add or update the validator entry in the list of members
                        members.entry(*validator_address).and_modify(|(stake, _, _)| *stake += amount).or_insert((
                            *amount,
                            true,
                            rng.random_range(0..100),
                        ));
                    }
                    // Construct the committee.
                    let committee = Committee::<N>::new(0u64, members)?;
                    (committee, bonded_balances)
                }
                None => {
                    // Calculate the committee stake per member.
                    let stake_per_member =
                        N::STARTING_SUPPLY.saturating_div(2).saturating_div(num_committee_members as u64);
                    ensure!(stake_per_member >= MIN_VALIDATOR_STAKE, "Committee stake per member is too low");

                    // Construct the committee members and distribute stakes evenly among committee members.
                    let members = development_addresses
                        .iter()
                        .map(|address| (*address, (stake_per_member, true, rng.random_range(0..100))))
                        .collect::<IndexMap<_, _>>();

                    // Construct the bonded balances.
                    // Note: The withdrawal address is set to the staker address.
                    let bonded_balances = members
                        .iter()
                        .map(|(address, (stake, _, _))| (*address, (*address, *address, *stake)))
                        .collect::<IndexMap<_, _>>();
                    // Construct the committee.
                    let committee = Committee::<N>::new(0u64, members)?;

                    (committee, bonded_balances)
                }
            };

            // Ensure that the number of committee members is correct.
            ensure!(
                committee.members().len() == num_committee_members as usize,
                "Number of committee members {} does not match the expected number of members {num_committee_members}",
                committee.members().len()
            );

            // Calculate the public balance per validator.
            let remaining_balance = N::STARTING_SUPPLY.saturating_sub(committee.total_stake());
            let public_balance_per_validator = remaining_balance.saturating_div(num_committee_members as u64);

            // Construct the public balances with fairly equal distribution.
            let mut public_balances = dev_keys
                .iter()
                .map(|private_key| Ok((Address::try_from(private_key)?, public_balance_per_validator)))
                .collect::<Result<indexmap::IndexMap<_, _>>>()?;

            // If there is some leftover balance, add it to the 0-th validator.
            let leftover =
                remaining_balance.saturating_sub(public_balance_per_validator * num_committee_members as u64);
            if leftover > 0 {
                let (_, balance) = public_balances.get_index_mut(0).unwrap();
                *balance += leftover;
            }

            // Check if the sum of committee stakes and public balances equals the total starting supply.
            let public_balances_sum: u64 = public_balances.values().copied().sum();
            if committee.total_stake() + public_balances_sum != N::STARTING_SUPPLY {
                bail!("Sum of committee stakes and public balances does not equal total starting supply.");
            }

            // Construct the genesis block.
            std::thread::spawn(move || {
                load_or_compute_genesis(dev_keys[0], committee, public_balances, bonded_balances, &mut rng)
            })
            .join()
            .unwrap()
        } else {
            Block::from_bytes_le(N::genesis_bytes())
        }
    }

    /// Returns the node type specified in the command-line arguments.
    /// This will return `NodeType::Client` if no node type was specified by the user.
    const fn parse_node_type(&self) -> NodeType {
        if self.validator {
            NodeType::Validator
        } else if self.prover {
            NodeType::Prover
        } else if self.bootstrap_client {
            NodeType::BootstrapClient
        } else {
            NodeType::Client
        }
    }

    /// Start the node and blocks until it terminates.
    #[rustfmt::skip]
    async fn parse_node<N: Network>(&mut self, handle: Handle, log_receiver: mpsc::Receiver<Vec<u8>>) -> Result<()> {
        if !self.nobanner {
            // Print the welcome banner.
            println!("{}", crate::helpers::welcome_message());
        }

        // Only allow dev mode if we built with the 'test_network' feature.
        if self.dev.is_some() && cfg!(not(feature = "test_network")) {
            bail!("The 'dev' flag is set, but the 'test_network' feature is not enabled");
        }

        // Parse the trusted peers to connect to.
        let mut trusted_peers = self.parse_trusted_addrs(&self.peers)?;
        // Parse the trusted validators to connect to.
        let mut trusted_validators = self.parse_trusted_addrs(&self.validators)?;

        // Ensure there are no bootstrappers among the trusted peers and validators.
        let bootstrap_peers = bootstrap_peers::<N>(self.dev.is_some());
        for trusted in [&mut trusted_peers, &mut trusted_validators] {
            let initial_peer_count = trusted.len();
            trusted.retain(|addr| !bootstrap_peers.contains(addr));
            let final_peer_count = trusted.len();
            // Warn if this had to be corrected.
            if final_peer_count != initial_peer_count {
                warn!(
                    "Removed some ({}) trusted peers due to them also being bootstrap peers.",
                    initial_peer_count - final_peer_count
                );
            }
        }

        // Parse the development configurations.
        self.parse_development(&mut trusted_peers, &mut trusted_validators)?;

        // Determine if the node should sync from CDn..
        let cdn = self.parse_cdn::<N>().with_context(|| "Failed to parse given CDN URL")?;

        // Parse the genesis block.
        let start = self.clone();
        let genesis = task::spawn_blocking(move || start.parse_genesis::<N>()).await??;
        // Parse the private key of the node.
        let account = self.parse_private_key::<N>()?;
        // Parse the node type.
        let node_type = self.parse_node_type();

        // Parse the node IP or use the default IP/port.
        let node_ip = self.node.unwrap_or(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, DEFAULT_NODE_PORT)));

        // Parse the REST IP.
        let rest_ip = match self.norest {
            true => None,
            false => self.rest.or_else(|| Some("0.0.0.0:3030".parse().unwrap())),
        };

        // Initialize the storage mode.
        let storage_mode = match &self.ledger_storage {
            Some(path) => StorageMode::from(path.clone()),
            None => match self.dev {
                Some(id) => StorageMode::Development(id),
                None => StorageMode::Production,
            },
        };

        // Users may have unintentionally set a custom path for the ledger, but not for the node data.
        // For validators, we make this an errors, so important files like the proposal cache are stored at the location
        // exepcted by the node operator.
        if self.node_data_storage.is_some() && !matches!(storage_mode, StorageMode::Custom(..)) {
            if node_type == NodeType::Validator {
                bail!("Custom path set for `--node-data-storage`, but not for `--ledger-storage`.")
            } else {
                warn!("Custom path set for `--node-data-storage`, but not for `--ledger-storage`. The latter will use the default path.");   
            }
        } else if matches!(storage_mode, StorageMode::Custom(..)) && self.node_data_storage.is_none() {
            if node_type == NodeType::Validator {
                bail!("Custom path set for `--ledger-storage`, but not for `--node-data-storage`.");
            } else {
                warn!("Custom path set for `--ledger-storage`, but not for `--node-data-storage`. The latter will use the default path.");
            }
        }

        // Parse the node data directory.
        let node_data_dir = parse_node_data_dir(&self.node_data_storage, N::ID, self.dev).with_context(|| "Failed to setup node configuration directory")?;

        // Make sure the directory exists before continue.
        let data_path = node_data_dir.path();
        if !data_path.exists() {
            info!("Creating directore for node data storage at {data_path:?}");
            std::fs::create_dir_all(data_path)
                .with_context(|| format!("Failed to create directory for node data storage at {data_path:?}"))?
        } else if !data_path.is_dir() {
            bail!("Node data storage location at {data_path:?} is not a directory");
        } else {
            debug!("Using existing directory at {data_path:?} for node data storage");
        }

        // Checks for the old storage format and prints instructions to migrate.
        // We perform this check after creating the node data directory, so that migrating the data is easier.
        Self::check_for_old_storage_format(&aleo_ledger_dir(N::ID, &storage_mode), &account.address(), &node_data_dir, self.dev, self.auto_migrate_node_data).with_context(|| "Node still uses the old storage format.")?;

        // Compute the optional REST server JWT.
        let jwt_token = if self.nojwt {
            None
        } else if let Some(jwt_b64) = &self.jwt_secret {
            // Decode the JWT secret.
            let jwt_bytes = BASE64_STANDARD.decode(jwt_b64).map_err(|_| anyhow::anyhow!("Invalid JWT secret"))?;
            if jwt_bytes.len() != 16 {
                bail!("The JWT secret must be 16 bytes long");
            }
            // Create the JWT token based on the given secret.
            let jwt_token = snarkos_node_rest::Claims::new(account.address(), Some(jwt_bytes), self.jwt_timestamp).to_jwt_string()?;
            // Store the JWT secret to a file.
            let path = Self::jwt_secret_path(&node_data_dir, &account.address());
            std::fs::write(path, &jwt_token)?;
            // Return the JWT token for optional printing.
            Some(jwt_token)
        } else {
            // Create a random JWT token.
            let jwt_token = snarkos_node_rest::Claims::new(account.address(), None, self.jwt_timestamp).to_jwt_string()?;
            // Store the JWT secret to a file.
            let path = Self::jwt_secret_path(&node_data_dir, &account.address());
            std::fs::write(path, &jwt_token)? ;
            // Return the JWT token for optional printing.
            Some(jwt_token)
        };

        if !self.nobanner {
            // Print the Aleo address.
            println!("👛 Your Aleo address is {}.\n", account.address().to_string().bold());
            // Print the node type and network.
            println!(
                "🧭 Starting {} on {} at {}.\n",
                node_type.description().bold(),
                N::NAME.bold(),
                node_ip.to_string().bold()
            );
            // If the node is running a REST server, determine the JWT.
            if let Some(rest_ip) = rest_ip {
                println!("🌐 Starting the REST server at {}.\n", rest_ip.to_string().bold());
                if let Some(jwt_token) = jwt_token {
                    println!("🔑 Your one-time JWT token is {}\n", jwt_token.dimmed());
                }
            }
        }

        // If the node is a validator, check if the open files limit is lower than recommended.
        #[cfg(target_family = "unix")]
        if node_type.is_validator() {
            crate::helpers::check_open_files_limit(RECOMMENDED_MIN_NOFILES_LIMIT);
        }
        // Check if the machine meets the minimum requirements for a validator.
        crate::helpers::check_validator_machine(node_type);

        // Initialize the metrics.
        #[cfg(feature = "metrics")]
        if self.metrics {
            metrics::initialize_metrics(self.metrics_ip);
        }

        // Determine whether to generate background transactions in dev mode.
        let dev_txs = match self.dev {
            Some(_) => !self.no_dev_txs,
            None => {
                // If the `no_dev_txs` flag is set, inform the user that it is ignored.
                if self.no_dev_txs {
                    eprintln!("The '--no-dev-txs' flag is ignored because '--dev' is not set");
                }
                false
            }
        };

        // Determine the dev committee hotswap configuration.
        let dev_hotswap_config = self.dev_on_prod.then_some(DevHotswapConfig {
            dev_num_validators: self.dev_num_validators,
        });

        // JSON history is written while a client or validator finalizes blocks.
        if self.history_json && !matches!(node_type, NodeType::Client | NodeType::Validator) {
            bail!("`--history-json` writes JSON history and is only supported on a client or validator");
        }
        if self.history_json {
            println!("Indexing credits.aleo history as JSON");
        }

        // Determine the historical API to serve the `history` routes from, if in compatibility mode.
        let history_api_url = self.parse_history_api_url::<N>()?;
        if let (Some(url), Some(_)) = (&history_api_url, rest_ip) {
            println!("🕰️  History compatibility mode is enabled; historical routes are served from {url}");
        }

        // TODO(kaimast): start the display earlier and show sync progress.
        if !self.nodisplay && cdn.is_some() {
            println!("🪧 The terminal UI will not start until the node has finished syncing from the CDN. If this step takes too long, consider restarting with `--nodisplay`.");
        }

        // Register the signal handler.
        let signal_handler = SignalHandler::new(Some(handle));

        let rest_verification_limits = RestVerificationLimits::new::<N, ConsensusMemory<N>>(
            self.num_verifying_deploys,
            self.num_verifying_executions,
            self.num_verifying_solutions,
        )?;

        // Initialize the node.
        let node = match node_type {
            NodeType::Validator => Node::new_validator(node_ip, self.bft, rest_ip, self.rest_rps, rest_verification_limits, history_api_url.clone(), self.history_json, account, &trusted_peers, &trusted_validators, genesis, cdn, storage_mode, node_data_dir, self.trusted_peers_only, self.auto_db_checkpoints.clone(), dev_txs, self.dev, dev_hotswap_config, self.proposal_spent_limit_multiplier, signal_handler.clone()).await,
            NodeType::Prover => Node::new_prover(node_ip, account, &trusted_peers, genesis, node_data_dir, self.trusted_peers_only, self.dev, signal_handler.clone()).await,
            NodeType::Client => Node::new_client(node_ip, rest_ip, self.rest_rps, rest_verification_limits, history_api_url.clone(), self.history_json, account, &trusted_peers, genesis, cdn, storage_mode, node_data_dir, self.trusted_peers_only, self.auto_db_checkpoints.clone(), self.dev, signal_handler.clone()).await,
            NodeType::BootstrapClient => Node::new_bootstrap_client(node_ip, account, *genesis.header(), self.dev).await,
        }?;

        if !self.nodisplay {
            Display::start(node.clone(), log_receiver, signal_handler.clone()).with_context(|| "Failed to start the display")?;
        }

        node.wait_for_signals(&signal_handler).await;
        Ok(())
    }

    /// Check if the node is still using the old storage format,
    /// in which case we print an error and exit.
    /// We detect this by checking if
    /// - a peer-cache file exists inside the ledger directory,
    /// - a current-proposal-cache file exists at the parent directory of the ledger directory
    /// - a jwt_secret_*.txt file exists at the parent directory of the ledger directory
    fn check_for_old_storage_format<N: Network>(
        ledger_dir: &Path,
        address: &Address<N>,
        node_data_dir: &NodeDataDir,
        dev: Option<u16>,
        auto_migrate: bool,
    ) -> Result<()> {
        let ledger_parent_dir = ledger_dir.parent().unwrap();

        // Determine the old paths used for node configuration files.
        let old_router_cache_path = ledger_dir.join(node_data::LEGACY_ROUTER_PEER_CACHE_FILE);
        let old_gateway_cache_path = ledger_dir.join(node_data::LEGACY_GATEWAY_PEER_CACHE_FILE);
        let old_proposal_cache_path = ledger_dir.join(node_data::legacy_current_proposal_cache_file(N::ID, dev));
        let old_jwt_secret_path = ledger_parent_dir.join(node_data::jwt_secret_file(address));

        if auto_migrate {
            if old_router_cache_path.exists() {
                let new_router_cache_path = node_data_dir.router_peer_cache_path();
                info!("Migrating node data file \"{old_router_cache_path:?}\" to \"{new_router_cache_path:?}\"");
                fs::rename(old_router_cache_path, new_router_cache_path)
                    .with_context(|| "Failed to migrate node data file")?;
            }

            if old_gateway_cache_path.exists() {
                let new_gateway_cache_path = node_data_dir.gateway_peer_cache_path();
                info!("Migrating node data file \"{old_gateway_cache_path:?}\" to \"{new_gateway_cache_path:?}\"");
                fs::rename(old_gateway_cache_path, new_gateway_cache_path)
                    .with_context(|| "Failed to migrate node data file")?;
            }

            if old_proposal_cache_path.exists() {
                let new_proposal_cache_path = node_data_dir.current_proposal_cache_path();
                info!("Migrating node data file \"{old_proposal_cache_path:?}\" to \"{new_proposal_cache_path:?}\"");
                fs::rename(old_proposal_cache_path, new_proposal_cache_path)
                    .with_context(|| "Failed to migrate node data file")?;
            }

            if old_jwt_secret_path.exists() {
                let new_jwt_secret_path = node_data_dir.jwt_secret_path(&address);
                info!("Migrating node data file \"{old_jwt_secret_path:?}\" to \"{new_jwt_secret_path:?}\"");
                fs::rename(old_jwt_secret_path, new_jwt_secret_path)
                    .with_context(|| "Failed to migrate node data file")?;
            }
        } else {
            if old_router_cache_path.exists() {
                let new_router_cache_path = node_data_dir.router_peer_cache_path();
                bail!(
                    "Please migrate the node data file \"{old_router_cache_path:?}\" to \"{new_router_cache_path:?}\" before restarting, or restart with `--auto-migrate-node-data`."
                );
            }

            if old_gateway_cache_path.exists() {
                let new_gateway_cache_path = node_data_dir.gateway_peer_cache_path();
                bail!(
                    "Please migrate the node data file \"{old_gateway_cache_path:?}\" to \"{new_gateway_cache_path:?}\" before restarting, or restart with `--auto-migrate-node-data`."
                );
            }

            if old_proposal_cache_path.exists() {
                let new_proposal_cache_path = node_data_dir.current_proposal_cache_path();
                bail!(
                    "Please migrate the node data file \"{old_proposal_cache_path:?}\" to \"{new_proposal_cache_path:?}\" before restarting, or restart with `--auto-migrate-node-data`."
                );
            }

            if old_jwt_secret_path.exists() {
                let new_jwt_secret_path = node_data_dir.jwt_secret_path(&address);
                bail!(
                    "Please migrate the node data file \"{old_jwt_secret_path:?}\" to \"{new_jwt_secret_path:?}\" before restarting, or restart with `--auto-migrate-node-data`."
                );
            }
        }

        Ok(())
    }

    /// Starts a rayon thread pool and tokio runtime for the node, and returns the tokio `Runtime`.
    fn runtime() -> Runtime {
        // Retrieve the number of cores.
        let num_cores = num_cpus::get();

        // Initialize the number of tokio worker threads, max tokio blocking threads, and rayon cores.
        //
        // One worker per core is enough because the async runtime only drives I/O and coordination:
        // everything CPU-bound is handed to the blocking pool, and from there to rayon. Adding
        // workers beyond the core count buys no parallelism, it only adds threads for the OS to
        // schedule against rayon.
        //
        // The blocking pool is a safety valve rather than a target, so its cap sits well above the
        // concurrency the node is expected to reach. Lowering it to around peak demand would turn
        // bursts into queueing ahead of rayon.
        let (num_tokio_worker_threads, max_tokio_blocking_threads, num_rayon_cores_global) =
            (num_cores, 256, num_cores);

        // Set up the rayon thread pool.
        // A custom panic handler is not needed here, as rayon propagates the panic to the calling thread by default (except for `rayon::spawn` which we do not use).
        rayon::ThreadPoolBuilder::new()
            .stack_size(8 * 1024 * 1024)
            .num_threads(num_rayon_cores_global)
            .build_global()
            .unwrap();

        // Set up the tokio Runtime.
        // TODO(kaimast): set up a panic handler here for each worker thread once [`tokio::runtime::Builder::unhandled_panic`](https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.unhandled_panic) is stabilized.
        runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_stack_size(8 * 1024 * 1024)
            .worker_threads(num_tokio_worker_threads)
            .max_blocking_threads(max_tokio_blocking_threads)
            .build()
            .expect("Failed to initialize a runtime for the router")
    }
}

/// Checks whether a file can only be read/written by the owner. It also allows more restrictive permissions, where only the owner can read it.
fn check_permissions(path: &PathBuf) -> Result<(), snarkvm::prelude::Error> {
    #[cfg(target_family = "unix")]
    {
        use std::os::unix::fs::PermissionsExt;
        ensure!(path.exists(), "The file '{path:?}' does not exist");
        crate::check_parent_permissions(path)?;

        let permissions = path.metadata()?.permissions().mode();
        ensure!(
            matches!(permissions & 0o777, 0o400 | 0o600),
            "The file {} must be readable and writable only by the owner (0600)",
            path.display()
        );
    }
    Ok(())
}

/// Loads or computes the genesis block.
fn load_or_compute_genesis<N: Network>(
    genesis_private_key: PrivateKey<N>,
    committee: Committee<N>,
    public_balances: indexmap::IndexMap<Address<N>, u64>,
    bonded_balances: indexmap::IndexMap<Address<N>, (Address<N>, Address<N>, u64)>,
    rng: &mut ChaChaRng,
) -> Result<Block<N>> {
    // Construct the preimage.
    let mut preimage = Vec::new();

    // Input the network ID.
    preimage.extend(&N::ID.to_le_bytes());
    // Input the genesis coinbase target.
    preimage.extend(&to_bytes_le![N::GENESIS_COINBASE_TARGET]?);
    // Input the genesis proof target.
    preimage.extend(&to_bytes_le![N::GENESIS_PROOF_TARGET]?);

    // Input the genesis private key, committee, and public balances.
    preimage.extend(genesis_private_key.to_bytes_le()?);
    preimage.extend(committee.to_bytes_le()?);
    preimage.extend(&to_bytes_le![public_balances.iter().collect::<Vec<(_, _)>>()]?);
    preimage.extend(&to_bytes_le![
        bonded_balances
            .iter()
            .flat_map(|(staker, (validator, withdrawal, amount))| to_bytes_le![staker, validator, withdrawal, amount])
            .collect::<Vec<_>>()
    ]?);

    // Input the parameters' metadata based on network
    match N::ID {
        snarkvm::console::network::MainnetV0::ID => {
            preimage.extend(snarkvm::parameters::mainnet::BondValidatorVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::mainnet::BondPublicVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::mainnet::UnbondPublicVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::mainnet::ClaimUnbondPublicVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::mainnet::SetValidatorStateVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::mainnet::TransferPrivateVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::mainnet::TransferPublicVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::mainnet::TransferPrivateToPublicVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::mainnet::TransferPublicToPrivateVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::mainnet::FeePrivateVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::mainnet::FeePublicVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::mainnet::InclusionVerifier::METADATA.as_bytes());
        }
        snarkvm::console::network::TestnetV0::ID => {
            preimage.extend(snarkvm::parameters::testnet::BondValidatorVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::testnet::BondPublicVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::testnet::UnbondPublicVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::testnet::ClaimUnbondPublicVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::testnet::SetValidatorStateVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::testnet::TransferPrivateVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::testnet::TransferPublicVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::testnet::TransferPrivateToPublicVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::testnet::TransferPublicToPrivateVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::testnet::FeePrivateVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::testnet::FeePublicVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::testnet::InclusionVerifier::METADATA.as_bytes());
        }
        snarkvm::console::network::CanaryV0::ID => {
            preimage.extend(snarkvm::parameters::canary::BondValidatorVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::canary::BondPublicVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::canary::UnbondPublicVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::canary::ClaimUnbondPublicVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::canary::SetValidatorStateVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::canary::TransferPrivateVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::canary::TransferPublicVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::canary::TransferPrivateToPublicVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::canary::TransferPublicToPrivateVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::canary::FeePrivateVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::canary::FeePublicVerifier::METADATA.as_bytes());
            preimage.extend(snarkvm::parameters::canary::InclusionVerifier::METADATA.as_bytes());
        }
        _ => {
            // Unrecognized Network ID
            bail!("Unrecognized Network ID: {}", N::ID);
        }
    }

    // Initialize the hasher.
    let hasher = snarkvm::console::algorithms::BHP256::<N>::setup("aleo.dev.block")?;
    // Compute the hash.
    // NOTE: this is a fast-to-compute but *IMPERFECT* identifier for the genesis block;
    //       to know the actual genesis block hash, you need to compute the block itself.
    let hash = hasher.hash(&preimage.to_bits_le())?.to_string();

    // A closure to load the block.
    let load_block = |file_path| -> Result<Block<N>> {
        // Attempts to load the genesis block file locally.
        let buffer = std::fs::read(file_path)?;
        // Return the genesis block.
        Block::from_bytes_le(&buffer)
    };

    // Cached dev genesis blocks live in ~/.aleo/dev-genesis, named by this preimage hash.
    // CircleCI restores and saves that directory; see restore_dev_genesis_cache in .circleci/config.yml.
    let cache_dir = aleo_dir().join("dev-genesis");
    let file_path = cache_dir.join(&hash);
    // Check if the genesis block exists.
    if file_path.exists() {
        // If the block loads successfully, return it.
        if let Ok(block) = load_block(&file_path) {
            info!("Loaded dev genesis block from {}", file_path.display());
            return Ok(block);
        }
    }

    /* Otherwise, compute the genesis block and store it. */

    info!("Computing dev genesis block");

    // Initialize a new VM.
    let vm = VM::from(ConsensusStore::<N, ConsensusMemory<N>>::open(StorageMode::new_test(None))?)?;
    // Initialize the genesis block.
    let block = vm.genesis_quorum(&genesis_private_key, committee, public_balances, bonded_balances, rng)?;
    // Write the genesis block to the file.
    std::fs::create_dir_all(&cache_dir)?;
    std::fs::write(&file_path, block.to_bytes_le()?)?;
    // Return the genesis block.
    Ok(block)
}

// Resolve socket addresses (not URLs) in a host:port format compliant with C::getaddrinfo.
fn resolve_potential_hostnames(ip_or_hostname: &str) -> Result<SocketAddr> {
    let trimmed = ip_or_hostname.trim();
    // Perform some basic validity checks.
    if !trimmed.contains(':') {
        bail!(
            "The supplied trusted hostname or IP ('{trimmed}') is malformed: missing colon separating the host from the port"
        );
    }
    if trimmed.contains("://") {
        bail!("The supplied trusted hostname or IP ('{trimmed}') is malformed: URLs are not supported");
    }
    match trimmed.to_socket_addrs() {
        Ok(mut ip_iter) => {
            // A hostname might resolve to multiple IP addresses. We will use only the first one,
            // assuming this aligns with the user's expectations.
            let Some(ip) = ip_iter.next() else {
                bail!("The supplied trusted hostname ('{trimmed}') does not reference any ip.");
            };
            Ok(ip)
        }
        Err(e) => Err(anyhow!("The supplied trusted hostname or IP ('{trimmed}') is malformed: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{CLI, Command};
    use snarkvm::prelude::MainnetV0;

    use ureq::http;

    type CurrentNetwork = MainnetV0;

    #[test]
    fn test_parse_trusted_addrs() {
        let config = Start::try_parse_from(["snarkos", "--peers", ""].iter()).unwrap();
        assert!(config.parse_trusted_addrs(&config.peers).is_ok());
        assert!(config.parse_trusted_addrs(&config.peers).unwrap().is_empty());

        let config = Start::try_parse_from(["snarkos", "--peers", "1.2.3.4:5"].iter()).unwrap();
        assert!(config.parse_trusted_addrs(&config.peers).is_ok());
        assert_eq!(config.parse_trusted_addrs(&config.peers).unwrap(), vec![
            SocketAddr::from_str("1.2.3.4:5").unwrap()
        ]);

        let config = Start::try_parse_from(["snarkos", "--peers", "1.2.3.4:5,6.7.8.9:0"].iter()).unwrap();
        assert!(config.parse_trusted_addrs(&config.peers).is_ok());
        assert_eq!(config.parse_trusted_addrs(&config.peers).unwrap(), vec![
            SocketAddr::from_str("1.2.3.4:5").unwrap(),
            SocketAddr::from_str("6.7.8.9:0").unwrap()
        ]);
    }

    #[test]
    fn test_parse_trusted_validators() {
        let config = Start::try_parse_from(["snarkos", "--validators", ""].iter()).unwrap();
        assert!(config.parse_trusted_addrs(&config.validators).is_ok());
        assert!(config.parse_trusted_addrs(&config.validators).unwrap().is_empty());

        let config = Start::try_parse_from(["snarkos", "--validators", "1.2.3.4:5"].iter()).unwrap();
        assert!(config.parse_trusted_addrs(&config.validators).is_ok());
        assert_eq!(config.parse_trusted_addrs(&config.validators).unwrap(), vec![
            SocketAddr::from_str("1.2.3.4:5").unwrap()
        ]);

        let config = Start::try_parse_from(["snarkos", "--validators", "1.2.3.4:5,6.7.8.9:0"].iter()).unwrap();
        assert!(config.parse_trusted_addrs(&config.validators).is_ok());
        assert_eq!(config.parse_trusted_addrs(&config.validators).unwrap(), vec![
            SocketAddr::from_str("1.2.3.4:5").unwrap(),
            SocketAddr::from_str("6.7.8.9:0").unwrap()
        ]);
    }

    #[test]
    fn test_parse_log_filter() {
        // Ensure we cannot set, both, log-filter and verbosity
        let result = Start::try_parse_from(["snarkos", "--verbosity=5", "--log-filter=warn"].iter());
        assert!(result.is_err(), "Must not be able to set log-filter and verbosity at the same time");

        // Ensure the values are set correctly.
        let config = Start::try_parse_from(["snarkos", "--verbosity=5"].iter()).unwrap();
        assert_eq!(config.verbosity, 5);
        let config = Start::try_parse_from(["snarkos", "--log-filter=snarkos=warn"].iter()).unwrap();
        assert_eq!(config.log_filter, Some("snarkos=warn".to_string()));
    }

    #[test]
    fn test_parse_cdn() -> Result<()> {
        // Validator (Prod)
        let config = Start::try_parse_from(["snarkos", "--validator", "--private-key", "aleo1xx"].iter()).unwrap();
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_some());
        let config =
            Start::try_parse_from(["snarkos", "--validator", "--private-key", "aleo1xx", "--cdn", "url"].iter())
                .unwrap();
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_some());
        let config = Start::try_parse_from(["snarkos", "--validator", "--private-key", "aleo1xx", "--nocdn"].iter())?;
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_none());

        // Validator (Dev)
        let config =
            Start::try_parse_from(["snarkos", "--dev", "0", "--validator", "--private-key", "aleo1xx"].iter()).unwrap();
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_none());
        let config = Start::try_parse_from(
            ["snarkos", "--dev", "0", "--validator", "--private-key", "aleo1xx", "--cdn", "url"].iter(),
        )
        .unwrap();
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_none());
        let config = Start::try_parse_from(
            ["snarkos", "--dev", "0", "--validator", "--private-key", "aleo1xx", "--nocdn"].iter(),
        )?;
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_none());

        // Prover (Prod)
        let config = Start::try_parse_from(["snarkos", "--prover", "--private-key", "aleo1xx"].iter())?;
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_none());
        let config = Start::try_parse_from(["snarkos", "--prover", "--private-key", "aleo1xx", "--cdn", "url"].iter())?;
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_none());
        let config = Start::try_parse_from(["snarkos", "--prover", "--private-key", "aleo1xx", "--nocdn"].iter())?;
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_none());

        // Prover (Dev)
        let config =
            Start::try_parse_from(["snarkos", "--dev", "0", "--prover", "--private-key", "aleo1xx"].iter()).unwrap();
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_none());
        let config = Start::try_parse_from(
            ["snarkos", "--dev", "0", "--prover", "--private-key", "aleo1xx", "--cdn", "url"].iter(),
        )
        .unwrap();
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_none());
        let config =
            Start::try_parse_from(["snarkos", "--dev", "0", "--prover", "--private-key", "aleo1xx", "--nocdn"].iter())
                .unwrap();
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_none());

        // Client (Prod)
        let config = Start::try_parse_from(["snarkos", "--client", "--private-key", "aleo1xx"].iter()).unwrap();
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_some());
        let config =
            Start::try_parse_from(["snarkos", "--client", "--private-key", "aleo1xx", "--cdn", "url"].iter()).unwrap();
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_some());
        let config =
            Start::try_parse_from(["snarkos", "--client", "--private-key", "aleo1xx", "--nocdn"].iter()).unwrap();
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_none());

        // Client (Dev)
        let config =
            Start::try_parse_from(["snarkos", "--dev", "0", "--client", "--private-key", "aleo1xx"].iter()).unwrap();
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_none());
        let config = Start::try_parse_from(
            ["snarkos", "--dev", "0", "--client", "--private-key", "aleo1xx", "--cdn", "url"].iter(),
        )
        .unwrap();
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_none());
        let config =
            Start::try_parse_from(["snarkos", "--dev", "0", "--client", "--private-key", "aleo1xx", "--nocdn"].iter())
                .unwrap();
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_none());

        // Default (Prod)
        let config = Start::try_parse_from(["snarkos"].iter()).unwrap();
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_some());
        let config = Start::try_parse_from(["snarkos", "--cdn", "url"].iter()).unwrap();
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_some());
        let config = Start::try_parse_from(["snarkos", "--nocdn"].iter()).unwrap();
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_none());

        // Default (Dev)
        let config = Start::try_parse_from(["snarkos", "--dev", "0"].iter()).unwrap();
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_none());
        let config = Start::try_parse_from(["snarkos", "--dev", "0", "--cdn", "url"].iter()).unwrap();
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_none());
        let config = Start::try_parse_from(["snarkos", "--dev", "0", "--nocdn"].iter()).unwrap();
        assert!(config.parse_cdn::<CurrentNetwork>()?.is_none());

        Ok(())
    }

    #[test]
    fn test_parse_development_and_genesis() {
        let prod_genesis = Block::from_bytes_le(CurrentNetwork::genesis_bytes()).unwrap();

        let mut trusted_peers = vec![];
        let mut trusted_validators = vec![];
        let mut config = Start::try_parse_from(["snarkos"].iter()).unwrap();
        config.parse_development(&mut trusted_peers, &mut trusted_validators).unwrap();
        let candidate_genesis = config.parse_genesis::<CurrentNetwork>().unwrap();
        assert_eq!(trusted_peers.len(), 0);
        assert_eq!(trusted_validators.len(), 0);
        assert_eq!(candidate_genesis, prod_genesis);

        let _config = Start::try_parse_from(["snarkos", "--dev", ""].iter()).unwrap_err();

        // Validator dev mode with default settings.
        let mut trusted_peers = vec![];
        let mut trusted_validators = vec![];
        let mut config = Start::try_parse_from(["snarkos", "--dev", "1", "--validator"].iter()).unwrap();
        config.parse_development(&mut trusted_peers, &mut trusted_validators).unwrap();
        assert_eq!(config.rest, Some(SocketAddr::from_str("0.0.0.0:3031").unwrap()));
        assert_eq!(trusted_validators.len(), 3);

        // Validator dev mode with `--rest` flag.
        let mut trusted_peers = vec![];
        let mut trusted_validators = vec![];
        let mut config =
            Start::try_parse_from(["snarkos", "--dev", "1", "--rest", "127.0.0.1:8080", "--validator"].iter()).unwrap();
        config.parse_development(&mut trusted_peers, &mut trusted_validators).unwrap();
        assert_eq!(config.rest, Some(SocketAddr::from_str("127.0.0.1:8080").unwrap()));
        assert_eq!(trusted_validators.len(), 3);

        // Validator dev mode with `--norest` flag.
        let mut trusted_peers = vec![];
        let mut trusted_validators = vec![];
        let mut config = Start::try_parse_from(["snarkos", "--dev", "1", "--norest", "--validator"].iter()).unwrap();
        config.parse_development(&mut trusted_peers, &mut trusted_validators).unwrap();
        assert!(config.rest.is_none());
        assert_eq!(trusted_validators.len(), 3);

        // Client dev node.
        let mut trusted_peers = vec![];
        let mut trusted_validators = vec![];
        let mut config = Start::try_parse_from(["snarkos", "--dev", "5"].iter()).unwrap();
        config.parse_development(&mut trusted_peers, &mut trusted_validators).unwrap();
        let expected_genesis = config.parse_genesis::<CurrentNetwork>().unwrap();
        assert_eq!(config.node, Some(SocketAddr::from_str("0.0.0.0:4135").unwrap()));
        assert_eq!(config.rest, Some(SocketAddr::from_str("0.0.0.0:3035").unwrap()));
        assert_eq!(trusted_peers.len(), 2);
        assert_eq!(trusted_validators.len(), 0);
        assert!(!config.validator);
        assert!(!config.prover);
        assert!(!config.client);
        assert_ne!(expected_genesis, prod_genesis);

        // Validator dev node with `--private-key` flag.
        let mut trusted_peers = vec![];
        let mut trusted_validators = vec![];
        let mut config =
            Start::try_parse_from(["snarkos", "--dev", "1", "--validator", "--private-key", ""].iter()).unwrap();
        config.parse_development(&mut trusted_peers, &mut trusted_validators).unwrap();
        let genesis = config.parse_genesis::<CurrentNetwork>().unwrap();
        assert_eq!(config.node, Some(SocketAddr::from_str("0.0.0.0:4131").unwrap()));
        assert_eq!(config.rest, Some(SocketAddr::from_str("0.0.0.0:3031").unwrap()));
        assert_eq!(trusted_peers.len(), 0);
        assert_eq!(trusted_validators.len(), 3);
        assert!(config.validator);
        assert!(!config.prover);
        assert!(!config.client);
        assert_eq!(genesis, expected_genesis);

        // Prover dev node with `--private-key` flag.
        let mut trusted_peers = vec![];
        let mut trusted_validators = vec![];
        let mut config =
            Start::try_parse_from(["snarkos", "--dev", "6", "--prover", "--private-key", ""].iter()).unwrap();
        config.parse_development(&mut trusted_peers, &mut trusted_validators).unwrap();
        let genesis = config.parse_genesis::<CurrentNetwork>().unwrap();
        assert_eq!(config.node, Some(SocketAddr::from_str("0.0.0.0:4136").unwrap()));
        assert_eq!(config.rest, Some(SocketAddr::from_str("0.0.0.0:3036").unwrap()));
        assert_eq!(trusted_peers.len(), 2);
        assert_eq!(trusted_validators.len(), 0);
        assert!(!config.validator);
        assert!(config.prover);
        assert!(!config.client);
        assert_eq!(genesis, expected_genesis);

        // Client dev node with `--private-key` flag.
        let mut trusted_peers = vec![];
        let mut trusted_validators = vec![];
        let mut config =
            Start::try_parse_from(["snarkos", "--dev", "10", "--client", "--private-key", ""].iter()).unwrap();
        config.parse_development(&mut trusted_peers, &mut trusted_validators).unwrap();
        let genesis = config.parse_genesis::<CurrentNetwork>().unwrap();
        assert_eq!(config.node, Some(SocketAddr::from_str("0.0.0.0:4140").unwrap()));
        assert_eq!(config.rest, Some(SocketAddr::from_str("0.0.0.0:3040").unwrap()));
        assert_eq!(trusted_peers.len(), 2);
        assert_eq!(trusted_validators.len(), 0);
        assert!(!config.validator);
        assert!(!config.prover);
        assert!(config.client);
        assert_eq!(genesis, expected_genesis);
    }

    /// Tests that you cannot pass the `--dev-num-clients` flag while also passing the `--peers` flag.
    #[test]
    fn test_parse_development_num_clients_and_peers() {
        let result = Start::try_parse_from(
            ["snarkos", "--validator", "--dev", "1", "--peers", "127.0.0.1:3030", "--dev-num-clients", "1"].iter(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn history_compat_mode_flag() {
        type N = snarkvm::prelude::MainnetV0;

        // Absent.
        let config = Start::try_parse_from(["snarkos"].iter()).unwrap();
        assert_eq!(config.parse_history_api_url::<N>().unwrap(), None);

        // Present without a URL: the network's own instance.
        let config = Start::try_parse_from(["snarkos", "--history-compat-mode"].iter()).unwrap();
        assert_eq!(
            config.parse_history_api_url::<N>().unwrap().as_deref(),
            Some("https://mainnet.historical-staking.provable.com")
        );

        // Present with a URL.
        let config =
            Start::try_parse_from(["snarkos", "--history-compat-mode", "http://127.0.0.1:8080"].iter()).unwrap();
        assert_eq!(config.parse_history_api_url::<N>().unwrap().as_deref(), Some("http://127.0.0.1:8080"));

        // A URL without a scheme, or an empty one, is refused at startup.
        for url in ["example.com", ""] {
            let config = Start::try_parse_from(["snarkos", "--history-compat-mode", url].iter()).unwrap();
            assert!(config.parse_history_api_url::<N>().is_err(), "{url:?}");
        }

        // A development network has no default upstream.
        let config = Start::try_parse_from(["snarkos", "--dev", "0", "--history-compat-mode"].iter()).unwrap();
        assert!(config.parse_history_api_url::<N>().is_err());
        let config =
            Start::try_parse_from(["snarkos", "--dev", "0", "--history-compat-mode", "http://127.0.0.1:8080"].iter())
                .unwrap();
        assert_eq!(config.parse_history_api_url::<N>().unwrap().as_deref(), Some("http://127.0.0.1:8080"));

        // The flag belongs to the REST server.
        assert!(Start::try_parse_from(["snarkos", "--norest", "--history-compat-mode"].iter()).is_err());
    }

    #[test]
    fn proposal_spent_limit_multiplier_requires_validator() {
        assert!(Start::try_parse_from(["snarkos", "--proposal-spent-limit-multiplier", "5"]).is_err());
        assert!(Start::try_parse_from(["snarkos", "--client", "--proposal-spent-limit-multiplier", "5"]).is_err());
        assert!(Start::try_parse_from(["snarkos", "--prover", "--proposal-spent-limit-multiplier", "5"]).is_err());
        assert!(
            Start::try_parse_from(["snarkos", "--bootstrap-client", "--proposal-spent-limit-multiplier", "5"]).is_err()
        );
        assert!(Start::try_parse_from(["snarkos", "--validator", "--proposal-spent-limit-multiplier", "0"]).is_err());

        let config = Start::try_parse_from(["snarkos", "--client"]).unwrap();
        assert_eq!(config.proposal_spent_limit_multiplier, 1);

        let config = Start::try_parse_from(["snarkos", "--validator"]).unwrap();
        assert_eq!(config.proposal_spent_limit_multiplier, 1);

        let config =
            Start::try_parse_from(["snarkos", "--validator", "--proposal-spent-limit-multiplier", "5"]).unwrap();
        assert_eq!(config.proposal_spent_limit_multiplier, 5);
    }

    #[test]
    fn history_json_flag_conflicts_with_compat_mode() {
        let config = Start::try_parse_from(["snarkos", "--client", "--history-json"].iter()).unwrap();
        assert!(config.history_json);
        assert!(config.history_compat_mode.is_none());

        assert!(
            Start::try_parse_from(["snarkos", "--client", "--history-json", "--history-compat-mode"].iter()).is_err()
        );
    }

    #[test]
    fn clap_snarkos_start() {
        let arg_vec = vec![
            "snarkos",
            "start",
            "--nodisplay",
            "--dev",
            "2",
            "--validator",
            "--private-key",
            "PRIVATE_KEY",
            "--cdn",
            "CDN",
            "--peers",
            "IP1,IP2,IP3",
            "--validators",
            "IP1,IP2,IP3",
            "--rest",
            "127.0.0.1:3030",
        ];
        let cli = CLI::parse_from(arg_vec);

        let Command::Start(start) = cli.command else {
            panic!("Unexpected result of clap parsing!");
        };

        assert!(start.nodisplay);
        assert_eq!(start.dev, Some(2));
        assert!(start.validator);
        assert_eq!(start.private_key.as_deref(), Some("PRIVATE_KEY"));
        assert_eq!(start.cdn, Some(http::Uri::try_from("CDN").unwrap()));
        assert_eq!(start.rest, Some("127.0.0.1:3030".parse().unwrap()));
        assert_eq!(start.network, 0);
        assert_eq!(start.peers, Some("IP1,IP2,IP3".to_string()));
        assert_eq!(start.validators, Some("IP1,IP2,IP3".to_string()));
    }

    /// Ensure two clients do not connect to the same validators.
    #[test]
    fn test_parse_development_client_validators() {
        let mut client1_config =
            Start::try_parse_from(["snarkos", "--dev", "10", "--client", "--private-key", ""].iter()).unwrap();
        let mut trusted_peers1 = vec![];
        let mut trusted_validators1 = vec![];
        client1_config.parse_development(&mut trusted_peers1, &mut trusted_validators1).unwrap();

        let mut client2_config =
            Start::try_parse_from(["snarkos", "--dev", "11", "--client", "--private-key", ""].iter()).unwrap();
        let mut trusted_peers2 = vec![];
        let mut trusted_validators2 = vec![];
        client2_config.parse_development(&mut trusted_peers2, &mut trusted_validators2).unwrap();

        assert_ne!(trusted_peers1, trusted_peers2);
    }

    #[test]
    fn parse_peers_when_ips() {
        let arg_vec = vec!["snarkos", "start", "--peers", "127.0.0.1:3030,127.0.0.2:3030"];
        let cli = CLI::parse_from(arg_vec);

        if let Command::Start(start) = cli.command {
            let peers = start.parse_trusted_addrs(&start.peers);
            assert!(peers.is_ok());
            assert_eq!(peers.unwrap().len(), 2, "Expected two peers");
        } else {
            panic!("Unexpected result of clap parsing!");
        }
    }

    #[test]
    fn parse_peers_when_hostnames() {
        let arg_vec = vec!["snarkos", "start", "--peers", "www.example.com:4130,www.google.com:4130"];
        let cli = CLI::parse_from(arg_vec);

        if let Command::Start(start) = cli.command {
            let peers = start.parse_trusted_addrs(&start.peers);
            assert!(peers.is_ok());
            assert_eq!(peers.unwrap().len(), 2, "Expected two peers");
        } else {
            panic!("Unexpected result of clap parsing!");
        }
    }

    #[test]
    fn parse_peers_when_mixed_and_with_whitespaces() {
        let arg_vec = vec!["snarkos", "start", "--peers", "  127.0.0.1:3030,  www.google.com:4130 "];
        let cli = CLI::parse_from(arg_vec);

        if let Command::Start(start) = cli.command {
            let peers = start.parse_trusted_addrs(&start.peers);
            assert!(peers.is_ok());
            assert_eq!(peers.unwrap().len(), 2, "Expected two peers");
        } else {
            panic!("Unexpected result of clap parsing!");
        }
    }

    #[test]
    fn parse_peers_when_unknown_hostname_gracefully() {
        let arg_vec = vec!["snarkos", "start", "--peers", "banana.cake.eafafdaeefasdfasd.com"];
        let cli = CLI::parse_from(arg_vec);

        if let Command::Start(start) = cli.command {
            assert!(start.parse_trusted_addrs(&start.peers).is_err());
        } else {
            panic!("Unexpected result of clap parsing!");
        }
    }

    #[test]
    fn rest_verification_limits_default_to_protocol_maximums() {
        let config = Start::try_parse_from(["snarkos"].iter()).unwrap();
        assert_eq!(config.num_verifying_deploys, DEFAULT_NUM_VERIFYING_DEPLOYS);
        assert_eq!(config.num_verifying_executions, DEFAULT_NUM_VERIFYING_EXECUTIONS);
        assert_eq!(config.num_verifying_solutions, DEFAULT_NUM_VERIFYING_SOLUTIONS);
        assert_eq!(
            RestVerificationLimits::new::<CurrentNetwork, ConsensusMemory<CurrentNetwork>>(
                config.num_verifying_deploys,
                config.num_verifying_executions,
                config.num_verifying_solutions,
            )
            .unwrap(),
            RestVerificationLimits::max::<CurrentNetwork, ConsensusMemory<CurrentNetwork>>()
        );
    }

    #[test]
    fn rest_verification_limits_accept_values_at_or_below_maximums() {
        let config = Start::try_parse_from(
            [
                "snarkos",
                "--num-verifying-deploys",
                "1",
                "--num-verifying-executions",
                "1",
                "--num-verifying-solutions",
                "1",
            ]
            .iter(),
        )
        .unwrap();
        assert_eq!(config.num_verifying_deploys, 1);
        assert_eq!(config.num_verifying_executions, 1);
        assert_eq!(config.num_verifying_solutions, 1);
    }

    #[test]
    fn rest_verification_limits_reject_values_above_maximums() {
        assert!(
            Start::try_parse_from(
                ["snarkos", "--num-verifying-deploys", &(DEFAULT_NUM_VERIFYING_DEPLOYS + 1).to_string()].iter()
            )
            .is_err()
        );
        assert!(
            Start::try_parse_from(
                ["snarkos", "--num-verifying-executions", &(DEFAULT_NUM_VERIFYING_EXECUTIONS + 1).to_string()].iter()
            )
            .is_err()
        );
        assert!(
            Start::try_parse_from(
                ["snarkos", "--num-verifying-solutions", &(DEFAULT_NUM_VERIFYING_SOLUTIONS + 1).to_string()].iter()
            )
            .is_err()
        );

        assert!(
            RestVerificationLimits::new::<CurrentNetwork, ConsensusMemory<CurrentNetwork>>(
                DEFAULT_NUM_VERIFYING_DEPLOYS + 1,
                DEFAULT_NUM_VERIFYING_EXECUTIONS,
                DEFAULT_NUM_VERIFYING_SOLUTIONS,
            )
            .is_err()
        );
        assert!(
            RestVerificationLimits::new::<CurrentNetwork, ConsensusMemory<CurrentNetwork>>(
                DEFAULT_NUM_VERIFYING_DEPLOYS,
                DEFAULT_NUM_VERIFYING_EXECUTIONS + 1,
                DEFAULT_NUM_VERIFYING_SOLUTIONS,
            )
            .is_err()
        );
        assert!(
            RestVerificationLimits::new::<CurrentNetwork, ConsensusMemory<CurrentNetwork>>(
                DEFAULT_NUM_VERIFYING_DEPLOYS,
                DEFAULT_NUM_VERIFYING_EXECUTIONS,
                DEFAULT_NUM_VERIFYING_SOLUTIONS + 1,
            )
            .is_err()
        );
    }

    #[test]
    fn rest_verification_limits_conflict_with_norest() {
        assert!(Start::try_parse_from(["snarkos", "--norest", "--num-verifying-deploys", "1"].iter()).is_err());
        assert!(Start::try_parse_from(["snarkos", "--norest", "--num-verifying-executions", "1"].iter()).is_err());
        assert!(Start::try_parse_from(["snarkos", "--norest", "--num-verifying-solutions", "1"].iter()).is_err());
    }
}
