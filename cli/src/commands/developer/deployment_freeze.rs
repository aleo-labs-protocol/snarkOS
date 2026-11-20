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

use super::{DEFAULT_ENDPOINT, Developer, Execute, ExecuteOptions};
use crate::commands::aleo_literal_to_fields;

use snarkvm::{
    ledger::committee::Committee,
    prelude::{Address, Literal, Network, Plaintext, Signature, Value},
    synthesizer::process::deployment_freeze::{
        ACTION_FREEZE,
        ACTION_THAW,
        DEPLOYMENT_FREEZE_CONSENSUS_VERSION,
        DEPLOYMENT_FREEZE_PROGRAM_ID,
        FREEZE_MAPPING,
        FreezeVote,
        FreezeWindow,
        NONCE_MAPPING,
        SUBMIT_VOTES_FUNCTION,
        committee_list,
        is_deployment_freeze_enforced,
        signed_slots,
        submit_votes_inputs,
    },
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use clap::{Args, Parser, Subcommand, ValueEnum};
use indexmap::IndexMap;
use std::str::FromStr;
use ureq::http::Uri;

/// The default freeze duration, in blocks (about one day).
pub const DEFAULT_FREEZE_DURATION: u32 = 28_800;
/// The default number of blocks a signed vote stays valid for (about one day).
pub const DEFAULT_EXPIRES_IN: u32 = 28_800;

/// Prepares, tallies and submits signed votes on the deployment freeze (`aleo_deployment_freeze.aleo`),
/// or shows it.
///
/// Validators do not send transactions. Each signs the payload printed by `payload` with
/// `snarkos account sign`; anyone collects the signatures, checks them with `tally`, and submits them
/// in one transaction with `submit`, paying its fee.
#[derive(Debug, Parser)]
pub struct DeploymentFreeze {
    #[clap(subcommand)]
    command: DeploymentFreezeCommand,
}

/// The action of a vote.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Action {
    /// Freeze deployments for `--duration` blocks.
    Freeze,
    /// End the current freeze.
    Thaw,
}

/// The node to query.
#[derive(Debug, Args)]
struct EndpointArgs {
    /// The base URL of the node to query.
    #[clap(short, long, alias = "query", default_value = DEFAULT_ENDPOINT)]
    endpoint: String,
}

/// The collected votes.
#[derive(Debug, Args)]
struct VoteArgs {
    /// A file with one `<address> <signature>` per line, as validators returned them.
    #[clap(long)]
    signatures: String,
    /// The payload the validators signed, as `payload` printed it.
    #[clap(long)]
    payload: String,
}

#[derive(Debug, Subcommand)]
enum DeploymentFreezeCommand {
    /// Print the freeze, the vote nonce, and the committee's quorum threshold.
    Status {
        #[clap(flatten)]
        endpoint: EndpointArgs,
    },
    /// Print the payload validators sign for a vote on the current nonce, and the command to sign it.
    Payload {
        /// The action to vote on.
        #[clap(long, value_enum)]
        action: Action,
        /// The length of the freeze, in blocks. Ignored for a thaw.
        #[clap(long, default_value_t = DEFAULT_FREEZE_DURATION)]
        duration: u32,
        /// The number of blocks, from the latest block, for which the signatures stay valid.
        #[clap(long, default_value_t = DEFAULT_EXPIRES_IN)]
        expires_in: u32,
        #[clap(flatten)]
        endpoint: EndpointArgs,
    },
    /// Check collected signatures against the latest committee, and sum their stake. Sends nothing.
    Tally {
        #[clap(flatten)]
        votes: VoteArgs,
        #[clap(flatten)]
        endpoint: EndpointArgs,
    },
    /// Submit collected signatures that reach the quorum threshold, in one transaction you pay for.
    Submit {
        #[clap(flatten)]
        votes: VoteArgs,
        #[clap(flatten)]
        options: Box<ExecuteOptions>,
    },
}

impl DeploymentFreeze {
    /// Runs the deployment freeze subcommand.
    pub fn parse<N: Network>(self) -> Result<String> {
        match self.command {
            DeploymentFreezeCommand::Status { endpoint } => status::<N>(&endpoint.endpoint),
            DeploymentFreezeCommand::Payload { action, duration, expires_in, endpoint } => {
                payload::<N>(&endpoint.endpoint, action, duration, expires_in)
            }
            DeploymentFreezeCommand::Tally { votes, endpoint } => {
                Ok(tally_from_node::<N>(&endpoint.endpoint, &votes)?.report())
            }
            DeploymentFreezeCommand::Submit { votes, options } => {
                ensure_submissions_accepted::<N>(&options.endpoint)?;
                let tally = tally_from_node::<N>(&options.endpoint, &votes)?;
                let report = tally.report();
                ensure!(
                    tally.reaches_quorum(),
                    "{report}\nThe signatures do not reach the quorum threshold; nothing was submitted"
                );
                let inputs = submission_inputs(&tally)?;
                let execution = Execute::new(
                    DEPLOYMENT_FREEZE_PROGRAM_ID.to_string(),
                    SUBMIT_VOTES_FUNCTION.to_string(),
                    inputs,
                    *options,
                )
                .parse::<N>()?;
                Ok(format!("{report}\n{execution}"))
            }
        }
    }
}

/// Parses a payload and checks it as the program will: the program's exact form, this network, and
/// a valid action and duration.
fn parse_vote<N: Network>(literal: &str) -> Result<FreezeVote> {
    let vote = FreezeVote::parse::<N>(literal)?;
    ensure!(vote.network == N::ID, "The payload is for network {}, not {}", vote.network, N::ID);
    vote.validate()?;
    Ok(vote)
}

/// Describes a vote's action.
fn describe(vote: &FreezeVote) -> String {
    match vote.action {
        ACTION_FREEZE => format!("freeze deployments for {} blocks", vote.duration),
        _ => "thaw".to_string(),
    }
}

/// The result of checking collected signatures against a committee.
#[derive(Debug)]
struct Tally<N: Network> {
    vote: FreezeVote,
    committee: Committee<N>,
    /// The valid signatures from committee members, with each member's stake.
    signed: IndexMap<Address<N>, (Signature<N>, u64)>,
    /// One line per signature that does not count, saying why.
    problems: Vec<String>,
}

impl<N: Network> Tally<N> {
    fn signed_stake(&self) -> u64 {
        self.signed.values().map(|(_, stake)| *stake).sum()
    }

    fn reaches_quorum(&self) -> bool {
        self.signed_stake() >= self.committee.quorum_threshold()
    }

    fn report(&self) -> String {
        let mut lines = vec![format!(
            "Vote: {} (nonce {}, valid before block {})",
            describe(&self.vote),
            self.vote.nonce,
            self.vote.expires
        )];
        for (address, (_, stake)) in &self.signed {
            lines.push(format!("  signed: {address} ({stake} microcredits)"));
        }
        for problem in &self.problems {
            lines.push(format!("  ignored: {problem}"));
        }
        lines.push(format!(
            "Signed stake: {} of {} (threshold {}): {}",
            self.signed_stake(),
            self.committee.total_stake(),
            self.committee.quorum_threshold(),
            if self.reaches_quorum() { "reaches the quorum threshold" } else { "below the quorum threshold" }
        ));
        lines.join("\n")
    }
}

/// Parses `<address> <signature>` lines. Blank lines and lines starting with `#` are skipped.
fn parse_signature_lines<N: Network>(text: &str) -> Result<Vec<(Address<N>, Signature<N>)>> {
    text.lines()
        .map(str::trim)
        .enumerate()
        .filter(|(_, line)| !line.is_empty() && !line.starts_with('#'))
        .map(|(index, line)| {
            let mut words = line.split_whitespace();
            let (Some(address), Some(signature), None) = (words.next(), words.next(), words.next()) else {
                bail!("Line {}: expected `<address> <signature>`", index + 1);
            };
            let address = Address::from_str(address).map_err(|_| anyhow!("Line {}: invalid address", index + 1))?;
            let signature =
                Signature::from_str(signature).map_err(|_| anyhow!("Line {}: invalid signature", index + 1))?;
            Ok((address, signature))
        })
        .collect()
}

/// Checks each signature over the vote against `committee`, as the program will: it must come from a
/// committee member and verify; a member counts once.
fn tally<N: Network>(
    committee: Committee<N>,
    vote: FreezeVote,
    entries: Vec<(Address<N>, Signature<N>)>,
) -> Result<Tally<N>> {
    // The fields `snarkos account sign --message` signs for the payload.
    let fields = aleo_literal_to_fields::<N>(&vote.literal())?;
    let mut signed = IndexMap::new();
    let mut problems = vec![];
    for (address, signature) in entries {
        if signed.contains_key(&address) {
            problems.push(format!("{address}: duplicate"));
            continue;
        }
        let Some((stake, _, _)) = committee.members().get(&address) else {
            problems.push(format!("{address}: not in the committee"));
            continue;
        };
        if signature.verify(&address, &fields) {
            signed.insert(address, (signature, *stake));
        } else {
            problems.push(format!("{address}: the signature does not match the payload"));
        }
    }
    Ok(Tally { vote, committee, signed, problems })
}

/// Parses the payload, checks it against the node's nonce and height, reads the signatures file, and
/// tallies it against the latest committee. Fails if the payload is not for the current nonce, or has
/// expired by the next block.
fn tally_from_node<N: Network>(endpoint: &str, votes: &VoteArgs) -> Result<Tally<N>> {
    let vote = parse_vote::<N>(&votes.payload)?;
    let text =
        std::fs::read_to_string(&votes.signatures).with_context(|| format!("Failed to read {}", votes.signatures))?;
    let entries = parse_signature_lines::<N>(&text)?;
    let endpoint = endpoint_uri::<N>(endpoint)?;
    let nonce = fetch_nonce::<N>(&endpoint)?;
    ensure!(vote.nonce == nonce, "The payload is for nonce {}, but the current nonce is {nonce}", vote.nonce);
    let next_height = fetch_height::<N>(&endpoint)?.saturating_add(1);
    ensure!(
        next_height < vote.expires,
        "The payload expired: it is valid before block {}, and the next block is {next_height}",
        vote.expires
    );
    tally(fetch_committee::<N>(&endpoint)?, vote, entries)
}

/// Returns the inputs of `submit_votes` for the tallied signatures.
fn submission_inputs<N: Network>(tally: &Tally<N>) -> Result<Vec<String>> {
    let (list, count) = committee_list(tally.committee.members().keys().copied())?;
    let (mask, signatures) = signed_slots(&list, |member| tally.signed.get(member).map(|(signature, _)| *signature))?;
    let inputs = submit_votes_inputs(&list, count, mask, &signatures, &tally.vote.literal())?;
    Ok(inputs.iter().map(ToString::to_string).collect())
}

/// Prints the payload for a vote on the current nonce.
fn payload<N: Network>(endpoint: &str, action: Action, duration: u32, expires_in: u32) -> Result<String> {
    let (action, duration) = match action {
        Action::Freeze => (ACTION_FREEZE, duration),
        Action::Thaw => (ACTION_THAW, 0),
    };
    // Check the action and duration before querying the node.
    FreezeVote { network: N::ID, action, duration, nonce: 0, expires: 0 }.validate()?;
    let endpoint = endpoint_uri::<N>(endpoint)?;
    let nonce = fetch_nonce::<N>(&endpoint)?;
    let height = fetch_height::<N>(&endpoint)?;
    let expires = height.checked_add(expires_in).ok_or_else(|| anyhow!("--expires-in is too large"))?;
    let vote = FreezeVote { network: N::ID, action, duration, nonce, expires };
    Ok(payload_instructions(&vote.literal(), N::ID))
}

/// The payload, and how a validator signs it.
fn payload_instructions(literal: &str, network: u16) -> String {
    [
        "Payload:".to_string(),
        format!("  {literal}"),
        String::new(),
        "Each validator signs it with its validator key:".to_string(),
        format!(
            "  snarkos account sign --network {network} --private-key-file <validator key file> --message '{literal}'"
        ),
        "and returns one line: `<validator address> <signature>`.".to_string(),
    ]
    .join("\n")
}

fn endpoint_uri<N: Network>(endpoint: &str) -> Result<Uri> {
    Developer::parse_query::<N>(endpoint)?.endpoint.context("The endpoint must be a node URL, not a static query")
}

fn fetch_committee<N: Network>(endpoint: &Uri) -> Result<Committee<N>> {
    Developer::http_get_json::<N, Committee<N>>(endpoint, "committee/latest")?
        .ok_or_else(|| anyhow!("The node returned 404 for the latest committee"))
}

/// Reads `aleo_deployment_freeze.aleo/{mapping}[{key}]`.
fn get_mapping_value<N: Network>(endpoint: &Uri, mapping: &str, key: &str) -> Result<Option<Value<N>>> {
    Developer::get_mapping_value(endpoint, DEPLOYMENT_FREEZE_PROGRAM_ID, mapping, key)
}

fn fetch_height<N: Network>(endpoint: &Uri) -> Result<u32> {
    Developer::http_get_json::<N, u32>(endpoint, "block/height/latest")?
        .ok_or_else(|| anyhow!("The node returned 404 for the latest height"))
}

/// Reads the vote nonce, which is 0 until a vote passes.
fn fetch_nonce<N: Network>(endpoint: &Uri) -> Result<u64> {
    match get_mapping_value::<N>(endpoint, NONCE_MAPPING, "0u8")? {
        Some(Value::Plaintext(Plaintext::Literal(Literal::U64(nonce), _))) => Ok(*nonce),
        Some(value) => bail!("Unexpected nonce value: {value}"),
        None => Ok(0),
    }
}

/// Describes when the freeze version activates on this network.
fn activation<N: Network>() -> String {
    match N::CONSENSUS_HEIGHT(DEPLOYMENT_FREEZE_CONSENSUS_VERSION) {
        Ok(activation) if activation != u32::MAX => {
            format!("{DEPLOYMENT_FREEZE_CONSENSUS_VERSION:?}, block {activation}")
        }
        _ => format!("{DEPLOYMENT_FREEZE_CONSENSUS_VERSION:?}, once it is scheduled"),
    }
}

/// Fails unless the network accepts a submission now. The program is built in, but nodes accept
/// transactions that execute it only once the latest block is at the freeze version.
fn ensure_submissions_accepted<N: Network>(endpoint: &str) -> Result<()> {
    let height = fetch_height::<N>(&endpoint_uri::<N>(endpoint)?)?;
    ensure!(
        is_deployment_freeze_enforced::<N>(height),
        "{DEPLOYMENT_FREEZE_PROGRAM_ID} accepts submissions from the block after the freeze version activates ({}); the latest block is {height}",
        activation::<N>()
    );
    Ok(())
}

/// Prints the freeze, the nonce, and the committee's quorum threshold.
fn status<N: Network>(endpoint: &str) -> Result<String> {
    let endpoint = endpoint_uri::<N>(endpoint)?;
    let height = fetch_height::<N>(&endpoint)?;
    let mut lines = vec![format!("Latest height: {height}")];
    let freeze = get_mapping_value::<N>(&endpoint, FREEZE_MAPPING, "0u8")?
        .map(|value| FreezeWindow::from_value(&value))
        .transpose()?;
    let nonce = fetch_nonce::<N>(&endpoint)?;
    let committee = fetch_committee::<N>(&endpoint)?;

    let next_height = height.saturating_add(1);
    if !is_deployment_freeze_enforced::<N>(height) {
        lines.push(format!(
            "The deployment freeze is not active yet: votes can be submitted from the block after {} activates",
            activation::<N>()
        ));
    }
    lines.push(match freeze {
        Some(freeze) if freeze.is_frozen_at(next_height) => {
            format!("Deployments are frozen from block {} until block {} (exclusive)", freeze.start, freeze.end)
        }
        Some(freeze) if freeze.is_policy_active_at(next_height) => {
            format!("A freeze has passed: it starts at block {} and ends at block {}", freeze.start, freeze.end)
        }
        _ => "Deployments are not frozen".to_string(),
    });
    lines.push(format!("Vote nonce: {nonce}"));
    lines.push(format!(
        "Committee: {} members, stake {}; a vote needs signatures holding {}",
        committee.members().len(),
        committee.total_stake(),
        committee.quorum_threshold()
    ));
    lines.push("Votes are signed off chain; check collected signatures with `tally`.".to_string());
    Ok(lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use snarkvm::{
        prelude::{MainnetV0, PrivateKey, TestRng},
        synthesizer::process::deployment_freeze::{COMMITTEE_LIST_LENGTH, FREEZE_DELAY},
    };

    type CurrentNetwork = MainnetV0;

    fn sample_vote() -> FreezeVote {
        FreezeVote {
            network: CurrentNetwork::ID,
            action: ACTION_FREEZE,
            duration: DEFAULT_FREEZE_DURATION,
            nonce: 3,
            expires: 1_000_000,
        }
    }

    #[test]
    fn test_parse_vote() {
        let vote = sample_vote();
        let literal = vote.literal();
        assert_eq!(parse_vote::<CurrentNetwork>(&literal).unwrap(), vote);
        // The instructions quote the literal in the sign command.
        let instructions = payload_instructions(&literal, CurrentNetwork::ID);
        assert!(instructions.contains(&format!("--message '{literal}'")));
        // Another network, or an action or duration the program refuses, is refused.
        let testnet = FreezeVote { network: 1, ..vote }.literal();
        assert!(parse_vote::<CurrentNetwork>(&testnet).is_err());
        let short = FreezeVote { duration: FREEZE_DELAY, ..vote }.literal();
        assert!(parse_vote::<CurrentNetwork>(&short).is_err());
        let thaw = FreezeVote { action: ACTION_THAW, duration: 0, ..vote };
        assert_eq!(describe(&parse_vote::<CurrentNetwork>(&thaw.literal()).unwrap()), "thaw");
    }

    /// The literal `payload` prints, signed by `snarkos account sign --message`, verifies as the program
    /// and `tally` check it.
    #[test]
    fn test_account_sign_signature_verifies() {
        use crate::commands::CLI;
        let rng = &mut TestRng::default();
        let private_key = PrivateKey::<CurrentNetwork>::new(rng).unwrap();
        let address = Address::try_from(&private_key).unwrap();
        let literal = sample_vote().literal();
        let private_key_string = private_key.to_string();
        let cli = CLI::try_parse_from([
            "snarkos",
            "account",
            "sign",
            "--private-key",
            private_key_string.as_str(),
            "--message",
            literal.as_str(),
        ])
        .unwrap();
        let signature = Signature::<CurrentNetwork>::from_str(cli.command.parse().unwrap().trim()).unwrap();
        assert!(signature.verify(&address, &aleo_literal_to_fields::<CurrentNetwork>(&literal).unwrap()));
        // The line a validator returns parses.
        let lines =
            parse_signature_lines::<CurrentNetwork>(&format!("# collected\n\n{address} {signature}\n")).unwrap();
        assert_eq!(lines, vec![(address, signature)]);
        assert!(parse_signature_lines::<CurrentNetwork>(&format!("{address}")).is_err());
        assert!(parse_signature_lines::<CurrentNetwork>(&format!("{address} {signature} extra")).is_err());
    }

    #[test]
    fn test_tally() {
        let rng = &mut TestRng::default();
        // Five members with stakes 10, 20, 30, 40, 50 (scaled to the minimum stake): total 150, threshold 101.
        let unit = 1_000_000_000_000u64;
        let keys: Vec<_> = (0..5).map(|_| PrivateKey::<CurrentNetwork>::new(rng).unwrap()).collect();
        let addresses: Vec<_> = keys.iter().map(|key| Address::try_from(key).unwrap()).collect();
        let members: IndexMap<_, _> = addresses
            .iter()
            .enumerate()
            .map(|(index, address)| (*address, ((index as u64 + 1) * 10 * unit, true, 0u8)))
            .collect();
        let committee = Committee::<CurrentNetwork>::new(1, members).unwrap();
        assert_eq!(committee.quorum_threshold(), 100 * unit + 1);
        let vote = sample_vote();
        let fields = aleo_literal_to_fields::<CurrentNetwork>(&vote.literal()).unwrap();
        let sign = |index: usize, rng: &mut TestRng| (addresses[index], keys[index].sign(&fields, rng).unwrap());

        // Members 1, 2 and 3 hold 90: below the threshold. A duplicate, a signature by the wrong key,
        // and a non-member are reported and do not count.
        let outsider = PrivateKey::<CurrentNetwork>::new(rng).unwrap();
        let outsider_address = Address::try_from(&outsider).unwrap();
        let entries = vec![
            sign(1, rng),
            sign(2, rng),
            sign(3, rng),
            sign(1, rng),
            (addresses[4], keys[0].sign(&fields, rng).unwrap()),
            (outsider_address, outsider.sign(&fields, rng).unwrap()),
        ];
        let result = tally(committee.clone(), vote, entries).unwrap();
        assert_eq!(result.signed_stake(), 90 * unit);
        assert_eq!(result.problems.len(), 3, "{:?}", result.problems);
        assert!(!result.reaches_quorum());
        assert!(result.report().contains("below the quorum threshold"));

        // Members 1, 2, 3 and 4 hold 140: the threshold is reached, and the inputs set their slots.
        let entries = (1..5).map(|index| sign(index, rng)).collect();
        let result = tally(committee, vote, entries).unwrap();
        assert_eq!(result.signed_stake(), 140 * unit);
        assert!(result.reaches_quorum());
        let inputs = submission_inputs(&result).unwrap();
        assert_eq!(inputs.len(), 5);
        assert_eq!(inputs[1], "5u32");
        let mask = u128::from_str(inputs[2].trim_end_matches("u128")).unwrap();
        let (list, _) = committee_list(addresses.iter().copied()).unwrap();
        for (slot, address) in list.iter().enumerate().take(5) {
            assert_eq!(mask & (1 << slot) != 0, *address != addresses[0], "slot {slot}");
        }
        assert_eq!(mask >> 5, 0);
        // Every input parses as the program's types.
        let signatures = Value::<CurrentNetwork>::from_str(&inputs[3]).unwrap();
        assert!(
            matches!(signatures, Value::Plaintext(Plaintext::Array(ref elements, _)) if elements.len() == COMMITTEE_LIST_LENGTH)
        );
        assert_eq!(FreezeVote::parse::<CurrentNetwork>(&inputs[4]).unwrap(), vote);
    }

    #[test]
    fn test_parse_commands() {
        use crate::commands::{CLI, Command, DeveloperCommand};
        let parse =
            |args: &[&str]| CLI::try_parse_from([&["snarkos", "developer", "deployment-freeze"], args].concat());
        let cli = parse(&["payload", "--action", "freeze", "--endpoint", "http://localhost:3030"]).unwrap();
        let Command::Developer(developer) = cli.command else { panic!("expected a developer command") };
        let DeveloperCommand::DeploymentFreeze(freeze) = developer.command else {
            panic!("expected deployment-freeze")
        };
        let DeploymentFreezeCommand::Payload { action, duration, expires_in, .. } = freeze.command else {
            panic!("expected payload")
        };
        assert_eq!((action, duration, expires_in), (Action::Freeze, DEFAULT_FREEZE_DURATION, DEFAULT_EXPIRES_IN));
        assert!(parse(&["payload", "--action", "thaw"]).is_ok());
        assert!(parse(&["payload"]).is_err());
        assert!(parse(&["tally", "--signatures", "votes.txt", "--payload", "{}"]).is_ok());
        // Submit needs a key and a mode, as `execute` does.
        assert!(parse(&["submit", "--signatures", "votes.txt", "--payload", "{}", "--dry-run"]).is_err());
        let cli = parse(&[
            "submit",
            "--signatures",
            "votes.txt",
            "--payload",
            "{}",
            "--private-key-file",
            "key",
            "--broadcast",
        ])
        .unwrap();
        let Command::Developer(developer) = cli.command else { panic!("expected a developer command") };
        let DeveloperCommand::DeploymentFreeze(freeze) = developer.command else {
            panic!("expected deployment-freeze")
        };
        let DeploymentFreezeCommand::Submit { options, .. } = freeze.command else { panic!("expected submit") };
        assert_eq!(options.private_key_file.as_deref(), Some("key"));
        assert!(parse(&["status"]).is_ok());
    }
}
