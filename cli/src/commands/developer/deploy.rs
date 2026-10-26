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

use super::{DEFAULT_ENDPOINT, Developer, ParsedQuery};
use crate::{commands::StoreFormat, helpers::args::parse_private_key};

use snarkvm::{
    circuit::{Aleo, AleoCanaryV0, AleoTestnetV0, AleoV0},
    console::{
        network::{CanaryV0, MainnetV0, Network, TestnetV0},
        program::ProgramOwner,
    },
    prelude::{
        ProgramID,
        VM,
        block::Transaction,
        deployment_cost,
        query::QueryTrait,
        store::{ConsensusStore, helpers::memory::ConsensusMemory},
    },
};

use aleo_std::StorageMode;
use anyhow::Result;
use clap::{Parser, builder::NonEmptyStringValueParser};
use colored::Colorize;
use snarkvm::prelude::{Address, ConsensusVersion};
use std::str::FromStr;
use ureq::http::Uri;
use zeroize::Zeroize;

use anyhow::Context;

/// Deploys an Aleo program.
#[derive(Debug, Parser)]
#[command(
    group(clap::ArgGroup::new("mode").required(true).multiple(false)),
    group(clap::ArgGroup::new("key").required(true).multiple(false))
)]
pub struct Deploy {
    /// The name of the program to deploy.
    program_id: String,
    /// A path to a directory containing a manifest file. Defaults to the current working directory.
    #[clap(long)]
    path: Option<String>,
    /// The private key used to generate the deployment.
    #[clap(short = 'p', long, group = "key", value_parser=NonEmptyStringValueParser::default())]
    private_key: Option<String>,
    /// Use a developer validator key tok generate the deployment.
    #[clap(long, group = "key")]
    dev_key: Option<u16>,
    /// Specify the path to a file containing the account private key of the node
    #[clap(long, group = "key", value_parser=NonEmptyStringValueParser::default())]
    private_key_file: Option<String>,
    /// The endpoint to query node state from and broadcast to (if set to broadcast).
    ///
    /// The given value is expected to be the base URL, e.g., "https://mynode.com", and will be extended automatically
    /// to fit the network type and query.
    /// For example, the base URL may extend to "http://mynode.com/testnet/transaction/unconfirmed/ID" to retrieve
    /// an unconfirmed transaction on the test network.
    ///
    /// The given value may also be a JSON serialized `StaticQuery` struct.
    #[clap(short, long, alias="query", default_value=DEFAULT_ENDPOINT, verbatim_doc_comment)]
    endpoint: String,
    /// The priority fee in microcredits.
    #[clap(long, default_value_t = 0)]
    priority_fee: u64,
    /// The record to spend the fee from.
    #[clap(short, long)]
    record: Option<String>,
    /// Set the URL used to broadcast the transaction (if no value is given, the query endpoint is used).
    ///
    /// The given value is expected the full URL of the endpoint, not just the base URL, e.g., "http://mynode.com/testnet/transaction/broadcast".
    #[clap(short, long, group = "mode", verbatim_doc_comment)]
    broadcast: Option<Option<Uri>>,
    /// Performs a dry-run of transaction generation.
    #[clap(short, long, group = "mode")]
    dry_run: bool,
    /// Store generated deployment transaction to a local file.
    #[clap(long, group = "mode")]
    store: Option<String>,
    /// If --store is specified, the format in which the transaction should be stored : string or
    /// bytes, by default : bytes.
    #[clap(long, value_enum, default_value_t = StoreFormat::Bytes, requires="store")]
    store_format: StoreFormat,
    /// Wait for the transaction to be accepted by the network. Requires --broadcast.
    #[clap(long, requires = "broadcast")]
    wait: bool,
    /// Timeout in seconds when waiting for transaction confirmation. Default is 60 seconds.
    #[clap(long, default_value_t = 60, requires = "wait")]
    timeout: u64,
}

impl Drop for Deploy {
    /// Zeroize the private key when the `Deploy` struct goes out of scope.
    fn drop(&mut self) {
        self.private_key.zeroize();
    }
}

impl Deploy {
    /// Deploys an Aleo program.
    pub fn parse<N: Network>(self) -> Result<String> {
        // Construct the deployment for the specified network.
        match N::ID {
            MainnetV0::ID => self.construct_deployment::<MainnetV0, AleoV0>(),
            TestnetV0::ID => self.construct_deployment::<TestnetV0, AleoTestnetV0>(),
            CanaryV0::ID => self.construct_deployment::<CanaryV0, AleoCanaryV0>(),
            _ => unreachable!(),
        }
        .with_context(|| "Deployment failed")
    }

    /// Construct and process the deployment transaction.
    fn construct_deployment<N: Network, A: Aleo<Network = N, BaseField = N::Field>>(self) -> Result<String> {
        let ParsedQuery { query, endpoint } = Developer::parse_query::<N>(&self.endpoint)?;

        // Retrieve the private key.
        let private_key = parse_private_key(self.private_key.clone(), self.private_key_file.clone(), self.dev_key)?;

        // Retrieve the program ID.
        let program_id = ProgramID::from_str(&self.program_id).with_context(|| "Failed to parse program ID")?;

        // Fetch the package from the directory.
        let package =
            Developer::parse_package(program_id, &self.path).with_context(|| "Failed to parse program package")?;

        println!("📦 Creating deployment transaction for '{}'...\n", &program_id.to_string().bold());

        // Generate the process with the appropriate imports.
        let process = package.get_process()?;

        // Generate the deployment
        let mut deployment =
            package.deploy::<A>(&process, None).with_context(|| "Failed to generate the deployment")?;

        // Get the consensus version.
        let consensus_version =
            N::CONSENSUS_VERSION(query.current_block_height().with_context(|| "Failed to query consensus height")?)?;

        // If the consensus version is less than `V9`, unset the program checksum and owner in the deployment.
        // Otherwise, set it to the appropriate values.
        if consensus_version < ConsensusVersion::V9 {
            deployment.set_program_checksum_raw(None);
            deployment.set_program_owner_raw(None);
        } else {
            deployment.set_program_checksum_raw(Some(package.program().to_checksum()));
            deployment.set_program_owner_raw(Some(Address::try_from(&private_key)?));
        };

        // Compute the deployment ID.
        let deployment_id = deployment.to_deployment_id().with_context(|| "Failed to compute deployment ID")?;

        // Compute the minimum deployment cost.
        let (minimum_deployment_cost, (_, _, _, _)) = deployment_cost(&process, &deployment, consensus_version)
            .with_context(|| "Failed to compute the minimum deployment cost")?;

        // Generate the deployment transaction.
        let transaction = {
            // Initialize an RNG.
            let rng = &mut rand::rng();

            // Initialize the storage.
            let store = ConsensusStore::<N, ConsensusMemory<N>>::open(StorageMode::Production)
                .with_context(|| "Failed to open the consensus store")?;

            // Initialize the VM.
            let vm = VM::from(store).with_context(|| "Failed to initialize the virtual machine")?;

            // Prepare the fees.
            let fee = match &self.record {
                Some(record) => {
                    let fee_record =
                        Developer::parse_record(&private_key, record).with_context(|| "Failed to parse record")?;
                    let fee_authorization = vm.authorize_fee_private(
                        &private_key,
                        fee_record,
                        minimum_deployment_cost,
                        self.priority_fee,
                        deployment_id,
                        rng,
                    )?;
                    vm.execute_fee_authorization(fee_authorization, Some(&query), rng)
                        .with_context(|| "Failed to execute fee authorization")?
                }
                None => {
                    let fee_authorization = vm.authorize_fee_public(
                        &private_key,
                        minimum_deployment_cost,
                        self.priority_fee,
                        deployment_id,
                        rng,
                    )?;
                    vm.execute_fee_authorization(fee_authorization, Some(&query), rng)
                        .with_context(|| "Failed to execute fee authorization")?
                }
            };
            // Construct the owner.
            let owner = ProgramOwner::new(&private_key, deployment_id, rng)
                .with_context(|| "Failed to construct program owner")?;

            // Create a new transaction.
            Transaction::from_deployment(owner, deployment, fee).with_context(|| "Failed to crate transaction")?
        };
        println!("✅ Created deployment transaction for '{}'", program_id.to_string().bold());

        // Determine if the transaction should be broadcast, stored, or displayed to the user.
        Developer::handle_transaction(
            endpoint.as_ref(),
            &self.broadcast,
            self.dry_run,
            &self.store,
            self.store_format,
            self.wait,
            self.timeout,
            transaction,
            program_id.to_string(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{CLI, Command, DeveloperCommand};

    use anyhow::bail;

    #[test]
    fn clap_snarkos_deploy_missing_mode() {
        let arg_vec = &[
            "snarkos",
            "developer",
            "deploy",
            "--private-key=PRIVATE_KEY",
            "--endpoint=ENDPOINT",
            "--priority-fee=77",
            "--record=RECORD",
            "hello.aleo",
        ];

        // Should fail because no mode is specified.
        let err = CLI::try_parse_from(arg_vec).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn clap_snarkos_deploy() -> Result<()> {
        let arg_vec = &[
            "snarkos",
            "developer",
            "deploy",
            "--private-key=PRIVATE_KEY",
            "--endpoint=ENDPOINT",
            "--priority-fee=77",
            "--dry-run",
            "--record=RECORD",
            "hello.aleo",
        ];
        // Use try parse here, as parse calls `exit()`.
        let cli = CLI::try_parse_from(arg_vec)?;

        let Command::Developer(developer) = cli.command else {
            bail!("Unexpected result of clap parsing!");
        };
        let DeveloperCommand::Deploy(deploy) = developer.command else {
            bail!("Unexpected result of clap parsing!");
        };

        assert_eq!(developer.network, 0);
        assert_eq!(deploy.program_id, "hello.aleo");
        assert_eq!(deploy.private_key, Some("PRIVATE_KEY".to_string()));
        assert_eq!(deploy.private_key_file, None);
        assert_eq!(deploy.endpoint, "ENDPOINT");
        assert!(deploy.dry_run);
        assert!(deploy.broadcast.is_none());
        assert_eq!(deploy.store, None);
        assert_eq!(deploy.priority_fee, 77);
        assert_eq!(deploy.record, Some("RECORD".to_string()));

        Ok(())
    }

    #[test]
    fn clap_snarkos_deploy_broadcast() -> Result<()> {
        let arg_vec = &[
            "snarkos",
            "developer",
            "deploy",
            "--private-key=PRIVATE_KEY",
            "--endpoint=ENDPOINT",
            "--priority-fee=77",
            "--broadcast=ENDPOINT2",
            "--record=RECORD",
            "hello.aleo",
        ];
        // Use try parse here, as parse calls `exit()`.
        let cli = CLI::try_parse_from(arg_vec)?;

        let Command::Developer(developer) = cli.command else {
            bail!("Unexpected result of clap parsing!");
        };
        let DeveloperCommand::Deploy(deploy) = developer.command else {
            bail!("Unexpected result of clap parsing!");
        };

        assert_eq!(developer.network, 0);
        assert_eq!(deploy.program_id, "hello.aleo");
        assert_eq!(deploy.private_key, Some("PRIVATE_KEY".to_string()));
        assert_eq!(deploy.private_key_file, None);
        assert_eq!(deploy.endpoint, "ENDPOINT");
        assert!(!deploy.dry_run);
        // Check that the custom endpoint for broadcasting is used.
        assert_eq!(Some(Some(Uri::try_from("ENDPOINT2").unwrap())), deploy.broadcast);
        assert_eq!(deploy.store, None);
        assert_eq!(deploy.priority_fee, 77);
        assert_eq!(deploy.record, Some("RECORD".to_string()));

        Ok(())
    }

    #[test]
    fn clap_snarkos_deploy_static_query() -> Result<()> {
        let query = r#"{"state_root": "sr1dz06ur5spdgzkguh4pr42mvft6u3nwsg5drh9rdja9v8jpcz3czsls9geg", "height": 14}"#;
        let arg_vec = &[
            "snarkos",
            "developer",
            "deploy",
            "--private-key=PRIVATE_KEY",
            "--query",
            query,
            "--dry-run",
            "hello.aleo",
        ];
        let cli = CLI::try_parse_from(arg_vec)?;

        let Command::Developer(developer) = cli.command else {
            bail!("Unexpected result of clap parsing!");
        };
        let DeveloperCommand::Deploy(deploy) = developer.command else {
            bail!("Unexpected result of clap parsing!");
        };

        assert_eq!(deploy.endpoint, query);

        Ok(())
    }
}
