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

use super::*;

use clap::builder::NonEmptyStringValueParser;

#[derive(Debug, Zeroize, Parser)]
#[command(
    group(clap::ArgGroup::new("key").required(true).multiple(false))
)]
pub struct Sign {
    /// Specify the network to create an execution for.
    /// [options: 0 = mainnet, 1 = testnet]
    #[clap(long, default_value_t=MainnetV0::ID, long, value_parser = network_id_parser())]
    pub(super) network: u16,

    /// Specify the account private key of the node
    #[clap(long, group = "key", value_parser=NonEmptyStringValueParser::default())]
    pub(super) private_key: Option<String>,

    /// Specify the path to a file containing the account private key of the node
    #[clap(long, group = "key", value_parser=NonEmptyStringValueParser::default())]
    pub(super) private_key_file: Option<String>,

    /// Use a developer validator key to generate the deployment.
    #[clap(long, group = "key")]
    pub(super) dev_key: Option<u16>,

    /// Message (Aleo value) to sign
    #[clap(short = 'm', long)]
    pub(super) message: String,

    /// When enabled, parses the message as bytes instead of Aleo literals
    #[clap(short = 'r', long)]
    pub(super) raw: bool,
}

impl Sign {
    pub fn execute(self) -> Result<String> {
        // Sign the message for the specified network.
        match self.network {
            MainnetV0::ID => self.sign::<MainnetV0>(),
            TestnetV0::ID => self.sign::<TestnetV0>(),
            unknown_id => bail!("Unknown network ID ({unknown_id})"),
        }
    }

    // Sign a message with an Aleo private key
    fn sign<N: Network>(self) -> Result<String> {
        // Sample a random field element.
        let mut rng = ChaChaRng::from_rng(&mut rand::rng());

        // Parse the private key
        let private_key = parse_private_key(self.private_key.clone(), self.private_key_file.clone(), self.dev_key)?;

        // Sign the message
        let signature = if self.raw {
            private_key.sign_bytes(self.message.as_bytes(), &mut rng)
        } else {
            let fields = aleo_literal_to_fields::<N>(&self.message)
                .map_err(|_| anyhow!("Failed to parse a valid Aleo literal"))?;
            private_key.sign(&fields, &mut rng)
        }
        .map_err(|_| anyhow!("Failed to sign the message"))?
        .to_string();
        // Return the signature as a string
        Ok(signature)
    }
}
