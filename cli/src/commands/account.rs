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

use crate::helpers::args::{network_id_parser, parse_private_key};

mod sign;
use sign::Sign;

use snarkvm::console::{
    account::{Address, PrivateKey, Signature},
    network::{CanaryV0, MainnetV0, Network, TestnetV0},
    prelude::{Environment, Uniform},
    program::{ToFields, Value},
    types::Field,
};

use anyhow::{Result, anyhow, bail};
use clap::Parser;
use colored::Colorize;
use core::str::FromStr;
use crossterm::ExecutableCommand;
use rand::SeedableRng;
use rand_chacha::ChaChaRng;
use rayon::prelude::*;
use std::{
    fs::File,
    io::{Read, Write},
};

use zeroize::Zeroize;

/// Commands to manage Aleo accounts.
#[derive(Debug, Parser, Zeroize)]
#[command(
    // Use kebab-case for all arguments (e.g., use the `private-key` flag for the `private_key` field).
    // This is already the default, but we specify it in case clap's default changes in the future.
    rename_all = "kebab-case",
)]
pub enum Account {
    /// Generates a new Aleo account
    New {
        /// Specify the network to create an execution for.
        /// [options: 0 = mainnet, 1 = testnet, 2 = canary]
        #[clap(long, default_value_t=MainnetV0::ID, long, value_parser = network_id_parser())]
        network: u16,
        /// Seed the RNG with a numeric value
        #[clap(short = 's', long)]
        seed: Option<String>,
        /// Try until an address with the vanity string is found
        #[clap(short = 'v', long, conflicts_with_all = &["seed", "save_to_file"])]
        vanity: Option<String>,
        /// Print sensitive information (such as the private key) discreetly in an alternate screen
        #[clap(long)]
        discreet: bool,
        /// Specify the path to a file where to save the account in addition to printing it
        #[clap(long, conflicts_with = "discreet")]
        save_to_file: Option<String>,
    },
    /// Derive an Aleo account from a private key
    Import {
        /// Account private key
        private_key: Option<String>,
        /// Specify the network to create an execution for.
        /// [options: 0 = mainnet, 1 = testnet, 2 = canary]
        #[clap(long, default_value_t=MainnetV0::ID, long, value_parser = network_id_parser())]
        network: u16,
        /// Print sensitive information (such as the private key) discreetly in an alternate screen
        #[clap(long)]
        discreet: bool,
        /// Specify the path to a file where to save the account in addition to printing it
        #[clap(long, conflicts_with = "discreet")]
        save_to_file: Option<String>,
    },
    Sign(Sign),
    Verify {
        /// Specify the network to create an execution for.
        /// [options: 0 = mainnet, 1 = testnet, 2 = canary]
        #[clap(long, default_value_t=MainnetV0::ID, long, value_parser = network_id_parser())]
        network: u16,
        /// Address to use for verification
        #[clap(short = 'a', long)]
        address: String,
        /// Signature to verify
        #[clap(short = 's', long)]
        signature: String,
        /// Message (Aleo value) to verify the signature against
        #[clap(short = 'm', long)]
        message: String,
        /// When enabled, parses the message as bytes instead of Aleo literals
        #[clap(short = 'r', long)]
        raw: bool,
    },
}

/// Parse a raw Aleo input into fields
pub(crate) fn aleo_literal_to_fields<N: Network>(input: &str) -> Result<Vec<Field<N>>> {
    Value::<N>::from_str(input)?.to_fields()
}

impl Account {
    /// Run a account command
    pub fn parse(self) -> Result<String> {
        match self {
            Self::New { network, seed, vanity, discreet, save_to_file } => {
                match vanity {
                    // Generate a vanity account for the specified network.
                    Some(vanity) => match network {
                        MainnetV0::ID => Self::new_vanity::<MainnetV0>(vanity.as_str(), discreet),
                        TestnetV0::ID => Self::new_vanity::<TestnetV0>(vanity.as_str(), discreet),
                        CanaryV0::ID => Self::new_vanity::<CanaryV0>(vanity.as_str(), discreet),
                        unknown_id => bail!("Unknown network ID ({unknown_id})"),
                    },
                    // Generate a seeded account for the specified network.
                    None => match network {
                        MainnetV0::ID => Self::new_seeded::<MainnetV0>(seed, discreet, save_to_file),
                        TestnetV0::ID => Self::new_seeded::<TestnetV0>(seed, discreet, save_to_file),
                        CanaryV0::ID => Self::new_seeded::<CanaryV0>(seed, discreet, save_to_file),
                        unknown_id => bail!("Unknown network ID ({unknown_id})"),
                    },
                }
            }
            Self::Import { private_key, network, discreet, save_to_file } => {
                // Import the account for the specified network.
                match network {
                    MainnetV0::ID => Self::import::<MainnetV0>(private_key, discreet, save_to_file),
                    TestnetV0::ID => Self::import::<TestnetV0>(private_key, discreet, save_to_file),
                    CanaryV0::ID => Self::import::<CanaryV0>(private_key, discreet, save_to_file),
                    unknown_id => bail!("Unknown network ID ({unknown_id})"),
                }
            }
            Self::Sign(sign) => sign.execute(),
            Self::Verify { network, address, signature, message, raw } => {
                // Verify the signature for the specified network.
                match network {
                    MainnetV0::ID => Self::verify::<MainnetV0>(address, signature, message, raw),
                    TestnetV0::ID => Self::verify::<TestnetV0>(address, signature, message, raw),
                    CanaryV0::ID => Self::verify::<CanaryV0>(address, signature, message, raw),
                    unknown_id => bail!("Unknown network ID ({unknown_id})"),
                }
            }
        }
    }

    /// Generates a new Aleo account with the given vanity string.
    fn new_vanity<N: Network>(vanity: &str, discreet: bool) -> Result<String> {
        // A closure to generate a new Aleo account.
        let sample_account = || snarkos_account::Account::<N>::new(&mut rand::rng());

        const ITERATIONS: u128 = u16::MAX as u128;
        const ITERATIONS_STR: &str = "65,535";

        // Ensure the vanity string is valid.
        if !crate::helpers::is_in_bech32m_charset(vanity) {
            bail!(
                "The vanity string '{vanity}' contains invalid bech32m characters. Try using characters from the bech32m character set: {}",
                crate::helpers::BECH32M_CHARSET
            );
        }

        // Output a message if the character set is more than 4 characters.
        if vanity.len() > 4 {
            let message =
                format!(" The vanity string '{vanity}' contains 5 or more characters and will take a while to find.\n");
            println!("{}", message.yellow());
        }

        loop {
            // Initialize a timer.
            let timer = std::time::Instant::now();

            // Generates bech32m addresses in parallel until one is found that
            // includes the desired vanity string at the start or end of the address.
            let account = (0..ITERATIONS).into_par_iter().find_map_any(|_| {
                // Initialize the result.
                let mut account = None;
                // Sample a random account.
                if let Ok(candidate) = sample_account() {
                    // Encode the address as a bech32m string.
                    let address = candidate.address().to_string();
                    // Set the candidate if the address includes the desired vanity string
                    // at the start or end of the address.
                    if crate::helpers::has_vanity_string(&address, vanity) {
                        account = Some(candidate);
                    }
                }
                // Return the result.
                account
            });

            // Return the result if a candidate was found.
            if let Some(account) = account {
                println!(); // Add a newline for formatting.
                if !discreet {
                    return Ok(account.to_string());
                }
                display_string_discreetly(
                    &format!("{:>12}  {}", "Private Key".cyan().bold(), account.private_key()),
                    "### Do not share or lose this private key! Press any key to complete. ###",
                )
                .unwrap();
                let account_info = format!(
                    " {:>12}  {}\n {:>12}  {}",
                    "View Key".cyan().bold(),
                    account.view_key(),
                    "Address".cyan().bold(),
                    account.address()
                );
                return Ok(account_info);
            } else {
                let rate = ITERATIONS / timer.elapsed().as_millis();
                let rate = format!("[{rate} a/ms]");
                println!(" {} Sampled {ITERATIONS_STR} accounts, searching...", rate.dimmed());
            }
        }
    }

    /// Generates a new Aleo account with an optional seed.
    fn new_seeded<N: Network>(seed: Option<String>, discreet: bool, save_to_file: Option<String>) -> Result<String> {
        // Recover the seed.
        let seed = match seed {
            // Recover the field element deterministically.
            Some(seed) => {
                Field::new(<N as Environment>::Field::from_str(&seed).map_err(|e| anyhow!("Invalid seed - {e}"))?)
            }
            // Sample a random field element.
            None => Field::rand(&mut ChaChaRng::from_rng(&mut rand::rng())),
        };
        // Recover the private key from the seed as a field element.
        let private_key =
            PrivateKey::try_from(seed).map_err(|_| anyhow!("Failed to convert the seed into a valid private key"))?;
        // Construct the account.
        let account = snarkos_account::Account::<N>::try_from(private_key)?;
        // Save to file in addition to printing it back to the user
        if let Some(path) = save_to_file {
            crate::check_parent_permissions(&path)?;
            let mut file = File::create_new(path)?;
            file.write_all(account.private_key().to_string().as_bytes())?;
            crate::set_user_read_only(&file)?;
        }
        // Print the new Aleo account.
        if !discreet {
            return Ok(account.to_string());
        }
        display_string_discreetly(
            &format!("{:>12}  {}", "Private Key".cyan().bold(), account.private_key()),
            "### Do not share or lose this private key! Press any key to complete. ###",
        )
        .unwrap();
        let account_info = format!(
            " {:>12}  {}\n {:>12}  {}",
            "View Key".cyan().bold(),
            account.view_key(),
            "Address".cyan().bold(),
            account.address()
        );
        Ok(account_info)
    }

    /// Generates a new Aleo account from a given private key
    fn import<N: Network>(private_key: Option<String>, discreet: bool, save_to_file: Option<String>) -> Result<String> {
        // Read the private key.
        let private_key = match discreet {
            true => {
                let private_key_input = rpassword::prompt_password("Please enter your private key: ").unwrap();
                PrivateKey::from_str(&private_key_input)
            }
            false => match private_key {
                Some(private_key) => PrivateKey::from_str(&private_key),
                None => bail!("PRIVATE_KEY shouldn't be empty when --discreet is false"),
            },
        }?;

        // Construct the account.
        let account = snarkos_account::Account::<N>::try_from(private_key)?;
        // Save to file in addition to printing it back to the user
        if let Some(path) = save_to_file {
            crate::check_parent_permissions(&path)?;
            let mut file = File::create_new(path)?;
            file.write_all(account.private_key().to_string().as_bytes())?;
            crate::set_user_read_only(&file)?;
        }
        // Print the new Aleo account.
        if !discreet {
            return Ok(account.to_string());
        }
        display_string_discreetly(
            &format!("{:>12}  {}", "Private Key".cyan().bold(), account.private_key()),
            "### Do not share or lose this private key! Press any key to complete. ###",
        )
        .unwrap();
        let account_info = format!(
            " {:>12}  {}\n {:>12}  {}",
            "View Key".cyan().bold(),
            account.view_key(),
            "Address".cyan().bold(),
            account.address()
        );
        Ok(account_info)
    }

    // Verify a signature with an Aleo address
    fn verify<N: Network>(address: String, signature: String, message: String, raw: bool) -> Result<String> {
        // Parse the address
        let address = Address::<N>::from_str(&address).map_err(|_| anyhow!("Failed to parse a valid address"))?;
        // Parse the signature
        let signature =
            Signature::<N>::from_str(&signature).map_err(|_| anyhow!("Failed to parse a valid signature"))?;
        // Verify the signature
        let verified = if raw {
            signature.verify_bytes(&address, message.as_bytes())
        } else {
            let fields =
                aleo_literal_to_fields(&message).map_err(|_| anyhow!("Failed to parse a valid Aleo literal"))?;
            signature.verify(&address, &fields)
        };

        // Return the verification result
        match verified {
            true => Ok("✅ The signature is valid".to_string()),
            false => bail!("❌ The signature is invalid"),
        }
    }
}

// Print the string to an alternate screen, so that the string won't been printed to the terminal.
fn display_string_discreetly(discreet_string: &str, continue_message: &str) -> Result<()> {
    use crossterm::{
        style::Print,
        terminal::{EnterAlternateScreen, LeaveAlternateScreen},
    };
    let mut stdout = std::io::stdout();
    stdout.execute(EnterAlternateScreen)?;
    // print msg on the alternate screen
    stdout.execute(Print(format!("{discreet_string}\n{continue_message}")))?;
    stdout.flush()?;
    wait_for_keypress();
    stdout.execute(LeaveAlternateScreen)?;
    Ok(())
}

fn wait_for_keypress() {
    let mut single_key = [0u8];
    std::io::stdin().read_exact(&mut single_key).unwrap();
}

#[cfg(test)]
mod tests {
    use super::{Account, Sign};

    use std::{fs, fs::Permissions, io::Write};
    use tempfile::{NamedTempFile, TempDir};

    use anyhow::{Context, Result};
    use colored::Colorize;

    #[test]
    fn test_new() -> Result<()> {
        for _ in 0..3 {
            let account = Account::New { network: 0, seed: None, vanity: None, discreet: false, save_to_file: None };
            account.parse().with_context(|| "Account creation failed")?;
        }

        Ok(())
    }

    #[test]
    fn test_new_seeded() -> Result<()> {
        let seed = Some(1231275789u64.to_string());

        let mut expected = format!(
            " {:>12}  {}\n",
            "Private Key".cyan().bold(),
            "APrivateKey1zkp2n22c19hNdGF8wuEoQcuiyuWbquY6up4CtG5DYKqPX2X"
        );
        expected += &format!(
            " {:>12}  {}\n",
            "View Key".cyan().bold(),
            "AViewKey1pNxZHn79XVJ4D2WG5Vn2YWsAzf5wzAs3dAuQtUAmUFF7"
        );
        expected += &format!(
            " {:>12}  {}",
            "Address".cyan().bold(),
            "aleo1uxl69laseuv3876ksh8k0nd7tvpgjt6ccrgccedpjk9qwyfensxst9ftg5"
        );

        let vanity = None;
        let account = Account::New { network: 0, seed, vanity, discreet: false, save_to_file: None };
        let actual = account.parse().with_context(|| "Command execution failed")?;
        assert_eq!(expected, actual);

        Ok(())
    }

    #[test]
    fn test_new_seeded_with_256bits_input() -> anyhow::Result<()> {
        let seed = Some("38868010450269069756484274649022187108349082664538872491798902858296683054657".to_string());

        let mut expected = format!(
            " {:>12}  {}\n",
            "Private Key".cyan().bold(),
            "APrivateKey1zkp61PAYmrYEKLtRWeWhUoDpFnGLNuHrCciSqN49T86dw3p"
        );
        expected += &format!(
            " {:>12}  {}\n",
            "View Key".cyan().bold(),
            "AViewKey1eYEGtb78FVg38SSYyzAeXnBdnWCba5t5YxUxtkTtvNAE"
        );
        expected += &format!(
            " {:>12}  {}",
            "Address".cyan().bold(),
            "aleo1zecnqchckrzw7dlsyf65g6z5le2rmys403ecwmcafrag0e030yxqrnlg8j"
        );

        let vanity = None;
        let account = Account::New { network: 0, seed, vanity, discreet: false, save_to_file: None };

        let actual = account.parse().with_context(|| "Command execution failed")?;
        assert_eq!(expected, actual);

        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn test_new_save_to_file() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().expect("Failed to create temp folder");
        let dir_path = dir.path();
        fs::set_permissions(dir_path, Permissions::from_mode(0o700)).expect("Failed to set permissions");

        let mut file = dir.path().to_owned();
        file.push("my-private-key-file");
        let file = file.display().to_string();

        let seed = Some(1231275789u64.to_string());
        let vanity = None;
        let discreet = false;
        let save_to_file = Some(file.clone());
        let account = Account::New { network: 0, seed, vanity, discreet, save_to_file };

        let actual = account.parse().with_context(|| "Command execution failed")?;

        let expected = "APrivateKey1zkp2n22c19hNdGF8wuEoQcuiyuWbquY6up4CtG5DYKqPX2X";
        assert!(actual.contains(expected));

        let content = fs::read_to_string(&file).expect("Failed to read private-key-file");
        assert_eq!(expected, content);

        // check the permissions - to read-only for the owner
        let metadata = fs::metadata(file).unwrap();
        let permissions = metadata.permissions();
        assert_eq!(permissions.mode() & 0o777, 0o400, "File permissions are not 0o400");

        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn test_new_prevent_save_to_file_in_non_protected_folder() {
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().expect("Failed to create temp folder");
        let dir_path = dir.path();
        fs::set_permissions(dir_path, Permissions::from_mode(0o444)).expect("Failed to set permissions");

        let mut file = dir.path().to_owned();
        file.push("my-private-key-file");
        let file = file.display().to_string();

        let seed = None;
        let vanity = None;
        let discreet = false;
        let save_to_file = Some(file);
        let account = Account::New { network: 0, seed, vanity, discreet, save_to_file };

        let res = account.parse();
        assert!(res.is_err());
    }

    #[test]
    fn test_new_prevent_save_to_file_in_non_existing_folder() {
        let dir = TempDir::new().expect("Failed to create temp folder");

        let mut file = dir.path().to_owned();
        file.push("missing-folder");
        file.push("my-private-key-file");
        let file = file.display().to_string();

        let seed = None;
        let vanity = None;
        let discreet = false;
        let save_to_file = Some(file);
        let account = Account::New { network: 0, seed, vanity, discreet, save_to_file };

        let res = account.parse();
        assert!(res.is_err());
    }

    #[test]
    fn test_new_prevent_overwrite_existing_file() {
        let mut file = NamedTempFile::new().expect("Failed to create temp file");
        write!(file, "don't overwrite me").expect("Failed to write secret to file");

        let seed = None;
        let vanity = None;
        let discreet = false;
        let path = file.path().display().to_string();
        let account = Account::New { network: 0, seed, vanity, discreet, save_to_file: Some(path) };

        let res = account.parse();
        assert!(res.is_err());

        let expected = "don't overwrite me";
        let content = fs::read_to_string(file).expect("Failed to read private-key-file");
        assert_eq!(expected, content);
    }

    #[test]
    fn test_new_disallow_save_to_file_with_discreet() {
        let seed = None;
        let vanity = None;
        let discreet = true;
        let save_to_file = Some("/tmp/not-important".to_string());
        let account = Account::New { network: 0, seed, vanity, discreet, save_to_file };

        let res = account.parse();
        assert!(res.is_err());
    }

    #[test]
    fn test_new_disallow_save_to_file_with_vanity() {
        let seed = None;
        let vanity = Some("foo".to_string());
        let discreet = false;
        let save_to_file = Some("/tmp/not-important".to_string());
        let account = Account::New { network: 0, seed, vanity, discreet, save_to_file };

        let res = account.parse();
        assert!(res.is_err());
    }

    #[test]
    fn test_import() -> Result<()> {
        let account = Account::Import { network: 0, private_key: None, discreet: false, save_to_file: None };
        assert!(account.parse().is_err());

        let mut expected = format!(
            " {:>12}  {}\n",
            "Private Key".cyan().bold(),
            "APrivateKey1zkp2n22c19hNdGF8wuEoQcuiyuWbquY6up4CtG5DYKqPX2X"
        );
        expected += &format!(
            " {:>12}  {}\n",
            "View Key".cyan().bold(),
            "AViewKey1pNxZHn79XVJ4D2WG5Vn2YWsAzf5wzAs3dAuQtUAmUFF7"
        );
        expected += &format!(
            " {:>12}  {}",
            "Address".cyan().bold(),
            "aleo1uxl69laseuv3876ksh8k0nd7tvpgjt6ccrgccedpjk9qwyfensxst9ftg5"
        );

        let account = Account::Import {
            network: 0,
            private_key: Some("APrivateKey1zkp2n22c19hNdGF8wuEoQcuiyuWbquY6up4CtG5DYKqPX2X".to_string()),
            discreet: false,
            save_to_file: None,
        };
        let actual = account.parse().with_context(|| "Command execution failed")?;
        assert_eq!(expected, actual);

        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn test_import_save_to_file() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().expect("Failed to create temp folder");
        let dir_path = dir.path();
        fs::set_permissions(dir_path, Permissions::from_mode(0o700)).expect("Failed to set permissions");

        let mut file = dir.path().to_owned();
        file.push("my-private-key-file");
        let file = file.display().to_string();

        let discreet = false;
        let save_to_file = Some(file.clone());
        let account = Account::Import {
            network: 0,
            private_key: Some("APrivateKey1zkp2n22c19hNdGF8wuEoQcuiyuWbquY6up4CtG5DYKqPX2X".to_string()),
            discreet,
            save_to_file,
        };

        let actual = account.parse().with_context(|| "Command execution failed")?;

        let expected = "APrivateKey1zkp2n22c19hNdGF8wuEoQcuiyuWbquY6up4CtG5DYKqPX2X";
        assert!(actual.contains(expected));

        let content = fs::read_to_string(&file).expect("Failed to read private-key-file");
        assert_eq!(expected, content);

        // check the permissions - to read-only for the owner
        let metadata = fs::metadata(file).unwrap();
        let permissions = metadata.permissions();
        assert_eq!(permissions.mode() & 0o777, 0o400, "File permissions are not 0o400");

        Ok(())
    }

    #[test]
    fn test_signature_raw() -> Result<()> {
        let key = "APrivateKey1zkp61PAYmrYEKLtRWeWhUoDpFnGLNuHrCciSqN49T86dw3p".to_string();
        let message = "Hello, world!".to_string();
        let account = Account::Sign(Sign {
            network: 0,
            private_key: Some(key),
            private_key_file: None,
            message,
            raw: true,
            dev_key: None,
        });

        account.parse().with_context(|| "Command execution failed")?;
        Ok(())
    }

    #[test]
    fn test_signature_raw_using_private_key_file() -> Result<()> {
        let key = "APrivateKey1zkp61PAYmrYEKLtRWeWhUoDpFnGLNuHrCciSqN49T86dw3p".to_string();
        let message = "Hello, world!".to_string();

        let mut file = NamedTempFile::new().expect("Failed to create temp file");
        writeln!(file, "{key}").expect("Failed to write key to temp file");

        let path = file.path().display().to_string();
        let account = Account::Sign(Sign {
            network: 0,
            private_key: None,
            private_key_file: Some(path),
            message,
            raw: true,
            dev_key: None,
        });

        account.parse().with_context(|| "Command execution failed")?;
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn test_signature_raw_using_private_key_file_from_account_new() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let message = "Hello, world!".to_string();

        let dir = TempDir::new().expect("Failed to create temp folder");
        let dir_path = dir.path();
        fs::set_permissions(dir_path, Permissions::from_mode(0o700)).expect("Failed to set permissions");

        let mut file = dir.path().to_owned();
        file.push("my-private-key-file");
        let file = file.display().to_string();

        let seed = None;
        let vanity = None;
        let discreet = false;
        let account = Account::New { network: 0, seed, vanity, discreet, save_to_file: Some(file.clone()) };
        account.parse().with_context(|| "Command execution failed")?;

        let account = Account::Sign(Sign {
            network: 0,
            private_key: None,
            private_key_file: Some(file),
            message,
            raw: true,
            dev_key: None,
        });

        account.parse().with_context(|| "Command execution failed")?;
        Ok(())
    }

    #[test]
    fn test_signature() -> Result<()> {
        let key = "APrivateKey1zkp61PAYmrYEKLtRWeWhUoDpFnGLNuHrCciSqN49T86dw3p".to_string();
        let message = "5field".to_string();
        let account = Account::Sign(Sign {
            network: 0,
            private_key: Some(key),
            private_key_file: None,
            message,
            raw: false,
            dev_key: None,
        });

        account.parse().with_context(|| "Command execution failed")?;
        Ok(())
    }

    #[test]
    fn test_signature_fail() {
        let key = "APrivateKey1zkp61PAYmrYEKLtRWeWhUoDpFnGLNuHrCciSqN49T86dw3p".to_string();
        let message = "not a literal value".to_string();
        let account = Account::Sign(Sign {
            network: 0,
            private_key: Some(key),
            private_key_file: None,
            message,
            raw: false,
            dev_key: None,
        });
        assert!(account.parse().is_err());
    }

    #[test]
    fn test_verify_raw() -> Result<()> {
        // test signature of "Hello, world!"
        let address = "aleo1zecnqchckrzw7dlsyf65g6z5le2rmys403ecwmcafrag0e030yxqrnlg8j";
        let signature = "sign1nnvrjlksrkxdpwsrw8kztjukzhmuhe5zf3srk38h7g32u4kqtqpxn3j5a6k8zrqcfx580a96956nsjvluzt64cqf54pdka9mgksfqp8esm5elrqqunzqzmac7kzutl6zk7mqht3c0m9kg4hklv7h2js0qmxavwnpuwyl4lzldl6prs4qeqy9wxyp8y44nnydg3h8sg6ue99qkwsnaqq".to_string();
        let message = "Hello, world!".to_string();
        let account = Account::Verify { network: 0, address: address.to_string(), signature, message, raw: true };

        account.parse().with_context(|| "Command execution failed")?;

        // test signature of "Hello, world!" against the message "Different Message"
        let signature = "sign1nnvrjlksrkxdpwsrw8kztjukzhmuhe5zf3srk38h7g32u4kqtqpxn3j5a6k8zrqcfx580a96956nsjvluzt64cqf54pdka9mgksfqp8esm5elrqqunzqzmac7kzutl6zk7mqht3c0m9kg4hklv7h2js0qmxavwnpuwyl4lzldl6prs4qeqy9wxyp8y44nnydg3h8sg6ue99qkwsnaqq".to_string();
        let message = "Different Message".to_string();
        let account = Account::Verify { network: 0, address: address.to_string(), signature, message, raw: true };
        let actual = account.parse();
        assert!(actual.is_err());

        // test signature of "Hello, world!" against the wrong address
        let signature = "sign1nnvrjlksrkxdpwsrw8kztjukzhmuhe5zf3srk38h7g32u4kqtqpxn3j5a6k8zrqcfx580a96956nsjvluzt64cqf54pdka9mgksfqp8esm5elrqqunzqzmac7kzutl6zk7mqht3c0m9kg4hklv7h2js0qmxavwnpuwyl4lzldl6prs4qeqy9wxyp8y44nnydg3h8sg6ue99qkwsnaqq".to_string();
        let message = "Hello, world!".to_string();
        let wrong_address = "aleo1uxl69laseuv3876ksh8k0nd7tvpgjt6ccrgccedpjk9qwyfensxst9ftg5".to_string();
        let account = Account::Verify { network: 0, address: wrong_address, signature, message, raw: true };
        let actual = account.parse();
        assert!(actual.is_err());

        // test a valid signature of "Different Message"
        let signature = "sign1424ztyt9hcm77nq450gvdszrvtg9kvhc4qadg4nzy9y0ah7wdqq7t36cxal42p9jj8e8pjpmc06lfev9nvffcpqv0cxwyr0a2j2tjqlesm5elrqqunzqzmac7kzutl6zk7mqht3c0m9kg4hklv7h2js0qmxavwnpuwyl4lzldl6prs4qeqy9wxyp8y44nnydg3h8sg6ue99qk3yrr50".to_string();
        let message = "Different Message".to_string();
        let account = Account::Verify { network: 0, address: address.to_string(), signature, message, raw: true };
        account.parse().with_context(|| "Command execution failed")?;

        Ok(())
    }

    #[test]
    fn test_verify() -> Result<()> {
        // test signature of 5u8
        let address = "aleo1zecnqchckrzw7dlsyf65g6z5le2rmys403ecwmcafrag0e030yxqrnlg8j";
        let signature = "sign1j7swjfnyujt2vme3ulu88wdyh2ddj85arh64qh6c6khvrx8wvsp8z9wtzde0sahqj2qwz8rgzt803c0ceega53l4hks2mf5sfsv36qhesm5elrqqunzqzmac7kzutl6zk7mqht3c0m9kg4hklv7h2js0qmxavwnpuwyl4lzldl6prs4qeqy9wxyp8y44nnydg3h8sg6ue99qkdetews".to_string();
        let message = "5field".to_string();
        let account = Account::Verify { network: 0, address: address.to_string(), signature, message, raw: false };
        account.parse().with_context(|| "Command execution failed")?;

        // test signature of 5u8 against the message 10u8
        let signature = "sign1j7swjfnyujt2vme3ulu88wdyh2ddj85arh64qh6c6khvrx8wvsp8z9wtzde0sahqj2qwz8rgzt803c0ceega53l4hks2mf5sfsv36qhesm5elrqqunzqzmac7kzutl6zk7mqht3c0m9kg4hklv7h2js0qmxavwnpuwyl4lzldl6prs4qeqy9wxyp8y44nnydg3h8sg6ue99qkdetews".to_string();
        let message = "10field".to_string();
        let account = Account::Verify { network: 0, address: address.to_string(), signature, message, raw: false };
        let actual = account.parse();
        assert!(actual.is_err());

        // test signature of 5u8 against the wrong address
        let signature = "sign1j7swjfnyujt2vme3ulu88wdyh2ddj85arh64qh6c6khvrx8wvsp8z9wtzde0sahqj2qwz8rgzt803c0ceega53l4hks2mf5sfsv36qhesm5elrqqunzqzmac7kzutl6zk7mqht3c0m9kg4hklv7h2js0qmxavwnpuwyl4lzldl6prs4qeqy9wxyp8y44nnydg3h8sg6ue99qkdetews".to_string();
        let message = "5field".to_string();
        let wrong_address = "aleo1uxl69laseuv3876ksh8k0nd7tvpgjt6ccrgccedpjk9qwyfensxst9ftg5".to_string();
        let account = Account::Verify { network: 0, address: wrong_address, signature, message, raw: false };
        let actual = account.parse();
        assert!(actual.is_err());

        // test a valid signature of 10u8
        let signature = "sign1t9v2t5tljk8pr5t6vkcqgkus0a3v69vryxmfrtwrwg0xtj7yv5qj2nz59e5zcyl50w23lhntxvt6vzeqfyu6dt56698zvfj2l6lz6q0esm5elrqqunzqzmac7kzutl6zk7mqht3c0m9kg4hklv7h2js0qmxavwnpuwyl4lzldl6prs4qeqy9wxyp8y44nnydg3h8sg6ue99qk8rh9kt".to_string();
        let message = "10field".to_string();
        let account = Account::Verify { network: 0, address: address.to_string(), signature, message, raw: false };

        account.parse().with_context(|| "Command execution failed")?;

        Ok(())
    }
}
