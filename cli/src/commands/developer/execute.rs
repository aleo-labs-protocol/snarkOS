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

use super::{Developer, ExecuteOptions, ParsedQuery};
use crate::helpers::args::parse_private_key;

use snarkvm::{
    console::network::Network,
    ledger::{
        query::QueryTrait,
        store::{StorageMode, helpers::memory::BlockMemory},
    },
    prelude::{
        Address,
        Identifier,
        Locator,
        ProcessExclusiveGuard,
        ProgramID,
        VM,
        Value,
        query::Query,
        store::{ConsensusStore, helpers::memory::ConsensusMemory},
    },
};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, builder::NonEmptyStringValueParser};
use colored::Colorize;
use std::str::FromStr;
use tracing::debug;
use ureq::http::Uri;

/// Executes an Aleo program function.
#[derive(Debug, Parser)]
pub struct Execute {
    /// The program identifier.
    #[clap(value_parser=NonEmptyStringValueParser::default())]
    program_id: String,
    /// The function name.
    #[clap(value_parser=NonEmptyStringValueParser::default())]
    function: String,
    /// The function inputs.
    inputs: Vec<String>,
    #[clap(flatten)]
    options: ExecuteOptions,
    /// Send the transaction without checking if sufficient funds are available (intended for testing purposes only).
    #[clap(long, hide = true)]
    skip_funds_check: bool,
}

impl Execute {
    /// Returns the command that executes `program_id/function` on the given inputs.
    pub(super) fn new(program_id: String, function: String, inputs: Vec<String>, options: ExecuteOptions) -> Self {
        Self { program_id, function, inputs, options, skip_funds_check: false }
    }

    /// Executes an Aleo program function with the provided inputs.
    pub fn parse<N: Network>(self) -> Result<String> {
        let options = &self.options;
        let ParsedQuery { query, endpoint } = Developer::parse_query::<N>(&options.endpoint)?;

        // Check if the query is a static query.
        let is_static_query = matches!(query, Query::STATIC(_));

        // Retrieve the private key.
        let private_key =
            parse_private_key(options.private_key.clone(), options.private_key_file.clone(), options.dev_key)?;

        // Retrieve the program ID.
        let program_id = ProgramID::from_str(&self.program_id).with_context(|| "Failed to parse program ID")?;

        // Retrieve the function.
        let function = Identifier::from_str(&self.function).with_context(|| "Failed to parse function ID")?;

        // Retrieve the inputs.
        let inputs = self.inputs.iter().map(|input| Value::from_str(input)).collect::<Result<Vec<Value<N>>>>()?;

        let locator = Locator::<N>::from_str(&format!("{program_id}/{function}"))?;
        println!("📦 Creating execution transaction for '{}'...\n", &locator.to_string().bold());

        // Generate the execution transaction.
        let transaction = {
            // Initialize an RNG.
            let rng = &mut rand::rng();

            // Initialize the storage.
            let store = ConsensusStore::<N, ConsensusMemory<N>>::open(StorageMode::Production)?;

            // Initialize the VM.
            let vm = VM::from(store)?;

            // Built-in programs, such as `credits.aleo`, are already in the process.
            if !is_static_query && !vm.contains_program(&program_id) {
                let endpoint = endpoint.as_ref().context("REST query has no endpoint")?;
                let height = query.current_block_height().with_context(|| "Failed to retrieve current block height")?;
                let version = N::CONSENSUS_VERSION(height)?;
                debug!("At block height {height} and consensus {version:?}");

                // Load the program and it's imports into the process.
                load_program(&query, &vm.process().lock(), &program_id, endpoint)?;
            }

            // Prepare the fee.
            let fee_record = match &options.record {
                Some(record_string) => Some(
                    Developer::parse_record(&private_key, record_string).with_context(|| "Failed to parse record")?,
                ),
                None => None,
            };

            // Create a new transaction.
            vm.execute(
                &private_key,
                (program_id, function),
                inputs.iter(),
                fee_record,
                options.priority_fee,
                Some(&query),
                rng,
            )
            .with_context(|| "VM failed to execute transaction locally")?
        };

        // Check if the public balance is sufficient.
        if options.record.is_none() && !is_static_query && !self.skip_funds_check {
            let endpoint = endpoint.as_ref().context("REST query has no endpoint")?;
            // Fetch the public balance.
            let address = Address::try_from(&private_key)?;
            let public_balance = Developer::get_public_balance::<N>(endpoint, &address)
                .with_context(|| "Failed to check for sufficient funds to send transaction")?
                .ok_or_else(|| {
                    anyhow!(
                        "No public balance found for sending account `{}`. It may not exist.",
                        address.to_string().bold()
                    )
                })?;

            // Check if the public balance is sufficient.
            let storage_cost = transaction
                .execution()
                .with_context(|| "Failed to get execution cost of transaction")?
                .size_in_bytes()?;

            // Calculate the base fee.
            // This fee is the minimum fee required to pay for the transaction,
            // excluding any finalize fees that the execution may incur.
            let base_fee = storage_cost.saturating_add(options.priority_fee);

            // If the public balance is insufficient, return an error.
            if public_balance < base_fee {
                bail!(
                    "The public balance of {} is insufficient to pay the base fee for `{}`",
                    public_balance,
                    locator.to_string().bold()
                );
            }
        }

        println!("✅ Created execution transaction for '{}'", locator.to_string().bold());

        // Determine if the transaction should be broadcast, stored, or displayed to the user.
        Developer::handle_transaction(
            endpoint.as_ref(),
            &options.broadcast,
            options.dry_run,
            &options.store,
            options.store_format,
            options.wait,
            options.timeout,
            transaction,
            locator.to_string(),
        )
    }
}

/// A helper function to recursively load the program and all of its imports into the process.
fn load_program<N: Network>(
    query: &Query<N, BlockMemory<N>>,
    process: &ProcessExclusiveGuard<N>,
    program_id: &ProgramID<N>,
    endpoint: &Uri,
) -> Result<()> {
    // Fetch the program.
    let program = query.get_program(program_id).with_context(|| "Failed to fetch program")?;
    // Fetch the latest edition of the program.
    let edition = Developer::get_latest_edition(endpoint, program_id)
        .with_context(|| format!("Failed to get latest edition for program {program_id}"))?;

    // Return early if the program is already loaded.
    if process.contains_program(program.id()) {
        return Ok(());
    }

    // Iterate through the program imports.
    for import_program_id in program.imports().keys() {
        // Add the imports to the process if does not exist yet.
        if !process.contains_program(import_program_id) {
            // Recursively load the program and its imports.
            load_program(query, process, import_program_id, endpoint)
                .with_context(|| format!("Failed to load imported program {import_program_id}"))?;
        }
    }

    // Add the program to the process if it does not already exist.
    if !process.contains_program(program.id()) {
        debug!("Adding program {program_id} with edition {edition}");
        process
            .add_programs_with_editions(&[(program, edition)])
            .with_context(|| format!("Failed to add program {program_id}"))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{CLI, Command, DeveloperCommand};

    #[test]
    fn clap_snarkos_execute() -> Result<()> {
        let arg_vec = &[
            "snarkos",
            "developer",
            "execute",
            "--private-key",
            "PRIVATE_KEY",
            "--endpoint=ENDPOINT",
            "--priority-fee",
            "77",
            "--record",
            "RECORD",
            "--dry-run",
            "hello.aleo",
            "hello",
            "1u32",
            "2u32",
        ];
        let cli = CLI::try_parse_from(arg_vec)?;

        let Command::Developer(developer) = cli.command else {
            bail!("Unexpected result of clap parsing!");
        };
        let DeveloperCommand::Execute(execute) = developer.command else {
            bail!("Unexpected result of clap parsing!");
        };

        assert_eq!(developer.network, 0);
        assert_eq!(execute.options.private_key, Some("PRIVATE_KEY".to_string()));
        assert_eq!(execute.options.endpoint, "ENDPOINT");
        assert_eq!(execute.options.priority_fee, 77);
        assert_eq!(execute.options.record, Some("RECORD".into()));
        assert_eq!(execute.program_id, "hello.aleo".to_string());
        assert_eq!(execute.function, "hello".to_string());
        assert_eq!(execute.inputs, vec!["1u32".to_string(), "2u32".to_string()]);

        Ok(())
    }

    #[test]
    fn clap_snarkos_execute_pk_file() -> Result<()> {
        let arg_vec = &[
            "snarkos",
            "developer",
            "execute",
            "--private-key-file",
            "PRIVATE_KEY_FILE",
            "--endpoint=ENDPOINT",
            "--record",
            "RECORD",
            "--dry-run",
            "hello.aleo",
            "hello",
            "1u32",
            "2u32",
        ];
        let cli = CLI::try_parse_from(arg_vec)?;

        let Command::Developer(developer) = cli.command else {
            bail!("Unexpected result of clap parsing!");
        };
        let DeveloperCommand::Execute(execute) = developer.command else {
            bail!("Unexpected result of clap parsing!");
        };

        assert_eq!(developer.network, 0);
        assert_eq!(execute.options.private_key_file, Some("PRIVATE_KEY_FILE".to_string()));
        assert_eq!(execute.options.endpoint, "ENDPOINT");
        assert_eq!(execute.options.priority_fee, 0); // Default value.
        assert_eq!(execute.options.record, Some("RECORD".into()));
        assert_eq!(execute.program_id, "hello.aleo".to_string());
        assert_eq!(execute.function, "hello".to_string());
        assert_eq!(execute.inputs, vec!["1u32".to_string(), "2u32".to_string()]);

        Ok(())
    }

    #[test]
    fn clap_snarkos_execute_two_private_keys() {
        let arg_vec = &[
            "snarkos",
            "developer",
            "execute",
            "--private-key",
            "PRIVATE_KEY",
            "--private-key-file",
            "PRIVATE_KEY_FILE",
            "--endpoint=ENDPOINT",
            "--priority-fee",
            "77",
            "--record",
            "RECORD",
            "--dry-run",
            "hello.aleo",
            "hello",
            "1u32",
            "2u32",
        ];

        let err = CLI::try_parse_from(arg_vec).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn clap_snarkos_execute_no_private_keys() {
        let arg_vec = &[
            "snarkos",
            "developer",
            "execute",
            "--endpoint=ENDPOINT",
            "--priority-fee",
            "77",
            "--record",
            "RECORD",
            "--dry-run",
            "hello.aleo",
            "hello",
            "1u32",
            "2u32",
        ];

        let err = CLI::try_parse_from(arg_vec).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn clap_snarkos_execute_static_query() -> Result<()> {
        let query = r#"{"state_root": "sr1dz06ur5spdgzkguh4pr42mvft6u3nwsg5drh9rdja9v8jpcz3czsls9geg", "height": 14}"#;
        let arg_vec = &[
            "snarkos",
            "developer",
            "execute",
            "--private-key",
            "PRIVATE_KEY",
            "--query",
            query,
            "--dry-run",
            "hello.aleo",
            "hello",
            "1u32",
            "2u32",
        ];
        let cli = CLI::try_parse_from(arg_vec)?;

        let Command::Developer(developer) = cli.command else {
            bail!("Unexpected result of clap parsing!");
        };
        let DeveloperCommand::Execute(execute) = developer.command else {
            bail!("Unexpected result of clap parsing!");
        };

        assert_eq!(execute.options.endpoint, query);

        Ok(())
    }
}
