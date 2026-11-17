<p align="center"> 
    <img alt="snarkOS" width="1412" src=".resources/snarkOS-banner.png">
</p>

<p align="center">
    <a href="https://github.com/aleo-labs-protocol/snarkOS/actions/workflows/ci.yml?query=branch%3Amainnet"><img src="https://github.com/aleo-labs-protocol/snarkOS/actions/workflows/ci.yml/badge.svg?branch=mainnet"></a>
    <a href="https://codecov.io/gh/ProvableHQ/snarkOS"><img src="https://codecov.io/gh/ProvableHQ/snarkOS/branch/master/graph/badge.svg?token=cck8tS9HpO"/></a>
    <a href="https://discord.gg/aleo"><img src="https://img.shields.io/discord/700454073459015690?logo=discord"/></a>
    <a href="https://twitter.com/AleoHQ"><img src="https://img.shields.io/twitter/follow/AleoHQ?style=social"/></a>
    <a href="https://GitHub.com/ProvableHQ/snarkOS"><img src="https://img.shields.io/badge/contributors-59-ee8449"/></a>
</p>

## <a name='TableofContents'></a>Table of Contents 

* [1. Overview](#1-overview)
* [2. Build Guide](#2-build-guide)
  * [2.1 Requirements](#21-requirements)
  * [2.2 Installation](#22-installation)
* [3. Run an Aleo Node](#3-run-an-aleo-node)
  * [3.1 Run an Aleo Client](#31-run-an-aleo-client)
  * [3.2 Run an Aleo Validator](#32-run-an-aleo-validator)
  * [3.3 Run an Aleo Prover](#33-run-an-aleo-prover)
* [4. FAQs](#4-faqs)
* [5. Command Line Interface](#5-command-line-interface)
* [6. Development Guide](#6-development-guide)
  * [6.1 Quick Start](#61-quick-start)
  * [6.2 Operations](#62-operations)
  * [6.3 Local Devnet](#63-local-devnet)
  * [6.4 Feature Flags](#64-feature-flags)
  * [6.5 Local Backups](#65-local-backups)
* [7. Contributors](#7-contributors)
* [8. License](#8-license)

[comment]: <> (* [4. JSON-RPC Interface]&#40;#4-json-rpc-interface&#41;)
[comment]: <> (* [5. Additional Information]&#40;#5-additional-information&#41;)

## 1. Overview

__snarkOS__ is a decentralized operating system for zero-knowledge applications.
This code forms the backbone of [Aleo](https://aleo.org/) network,
which verifies transactions and stores the encrypted state applications in a publicly-verifiable manner.

## 2. Build Guide

### 2.1 Definitions

The following snarkOS node types exist in the Aleo network:
 - **Validator**: Validator nodes participate in consensus and must be started with an account that is bonded into the committee.
 - **Client**: Clients do not participate in consensus but maintain a ledger. They are capable of providing information about the network as well as accepting solutions and transactions and communicating them to their peers. All clients run the same software, however, for the purposes of configuration management, this document defines two types of clients:
    - Core Client: Client node connected directly to a validator node.
    - Outer Client: Client node connected only to other clients or prover nodes.
 - **Prover**: Prover nodes are dedicated to solving the Aleo puzzle. They do not participate in consensus or maintain a copy of the ledger.

### 2.2 Requirements

The following are the requirements to run an Aleo node:
 - **OS**: 64-bit architectures only, latest up-to-date for security
    - Clients: Ubuntu 22.04 (LTS), macOS Ventura or later, Windows 11 or later
    - Validators: Ubuntu 22.04 (LTS)
 - **CPU**: 64-bit architectures only, Latest Intel Xeon or Better
    - Clients: 24-cores (32-cores or larger preferred)
    - Validators: 64-cores (128-cores or larger preferred)
 - **RAM**: DDR4 or better
    - Clients: 128GiB of memory (192GiB or larger preferred)
    - Validators: 256GiB of memory (384GiB or larger preferred)
 - **Storage**: PCIe Gen 3 x4, PCIe Gen 4 x2 NVME SSD, or better
    - Clients: 2TB of disk space (4TB or larger preferred)
    - Validators: 4TB of disk space (6TB or larger preferred)
 - **Network**: Symmetric, commercial, always-on
    - Clients: 250Mbps of upload **and** download bandwidth
    - Validators: 500Mbps of upload **and** download bandwidth

No explicit recommendations are made for proving nodes as proving hardware may be highly variable. If interested in running Aleo Provers nodes, please refer to resources published by the Aleo community.

### 2.3 Installation

Before beginning, please ensure your machine has Rust installed, with at least [this version](rust-toolchain). Instructions to [install Rust can be found here.](https://www.rust-lang.org/tools/install)

Start by cloning this GitHub repository:
```
git clone --branch mainnet --single-branch https://github.com/ProvableHQ/snarkOS.git
```

Next, move into the `snarkOS` directory:
```
cd snarkOS
```

**[For Ubuntu users]** A helper script to install dependencies is available. From the `snarkOS` directory, run:
```
./build_ubuntu.sh
```

Lastly, install `snarkOS`:
```
cargo install --locked --path .
```

#### Optional: CUDA Acceleration for Provers

> This CUDA build is optional. The current snarkOS puzzle does not leverage CUDA acceleration—it is a leftover from a previous event, although CUDA may become relevant again with ARC-43.
>
> The CUDA feature is considered unstable and experimental; expect breaking changes.

For prover operators who want to experiment with GPU support:
```
cargo install --locked --path . --features cuda
```

Please ensure ports `4130/tcp` and `3030/tcp` are open on your router and OS firewall.
### 2.4 Port Configuration

#### 2.4.1 For Core Clients

| Port     | Protocol | Allow/Deny | Source                       | Explanation                                                |
|----------|----------|------------|------------------------------|------------------------------------------------------------|
| 4130/tcp | TCP      | Allow      | All IPv4/IPv6 | TCP traffic to peers               |

#### 2.4.2 For Outer Clients

| Port     | Protocol | Allow/Deny | Source                       | Explanation                                                |
|----------|----------|------------|------------------------------|------------------------------------------------------------|
| 3030/tcp | TCP      | Allow      | All IPv4/IPv6                | REST server                                                |
| 4130/tcp | TCP      | Allow      | All IPv4/IPv6 | TCP traffic to peers                |

#### 2.4.3 For Validators

| Port     | Protocol | Allow/Deny | Source                       | Explanation                                                |
|----------|----------|------------|------------------------------|------------------------------------------------------------|
| 4130/tcp | TCP      | Allow      | All IPv4/IPv6 | TCP traffic to peers                |
| 5000/tcp | TCP      | Allow      | Trusted Validator IPs        | TCP traffic between validators for BFT communication       |
| 3000/tcp | TCP      | Allow      | Internal VPC or VPN          | Metrics dashboard, should only be open within an internal VPC or VPN |
| 3030/tcp | TCP      | Deny       | All IPv4/IPv6                | REST server. This should **always** be disabled for validators |
| 9000/tcp | TCP      | Allow      | Internal VPC or VPN          | Metrics export, should only be open within an internal VPC or VPN |
| 9090/tcp | TCP      | Allow      | Internal VPC or VPN          | Prometheus metrics, should only be open within an internal VPC or VPN |

**Note:** Ensure that your open file limit is set to 16,384 or above.
For the recommended setting run:
```
# Increase the open file limit for the current user (replace <username> with your username)
echo "<username> - nofile 65536" | sudo tee -a /etc/security/limits.conf
# Increase the default system open file limit
sudo bash -c 'echo "DefaultLimitNOFILE=65536" >> /etc/systemd/system.conf'
```

## 3. Run an Aleo Node

## 3.1 Run an Aleo Client

Start by following the instructions in the [Build Guide](#2-build-guide).
The guide below provides information on running `core` and `outer` clients (as defined in Section 2.2.) Aleo community members running validators are recommended to run 1-3 `core` clients as their exclusive client peers. This will ensure network traffic from the public internet is verified prior to reaching the validator.

Any client **not** connected directly to a validator can be considered an `outer` client.

### 3.1.1 Run an Aleo Core Client

The following command is recommended when starting a client node that is connected to a validator:
`snarkos start --client --nodisplay --node 0.0.0.0:4130 --peers "validator_ip:4130,core_client_ip_1:4130,core_client_ip_2:4130,core_client_ip3:4130,outer_client_ip_1:4130,..." --verbosity 1 --norest`

To start a core client node, you can also run the following command from the `snarkOS` directory:
```
./scripts/run-core-client.sh
```

### 3.1.2 Run an Aleo Outer Client

The following command is recommended when starting a client node that is NOT connected to a validator:
`snarkos start --client --nodisplay --node 0.0.0.0:4130 --peers "core_client_ip_1:4130,core_client_ip_2:4130,core_client_ip3:4130,outer_client_ip_1:4130,..." --verbosity 1 --rest 0.0.0.0:3030`

To start an outer client node, you can also run the following command from the `snarkOS` directory:
```
./scripts/run-outer-client.sh
```

Outer clients can be bootstrap clients that serve as accessible entry points for new nodes joining the network with publicly known or static IPs.
For bootstrap clients, we also recommend the use of `--rotate-external-peers` to avoid the bootstrap peerlist from filling up.

## 3.2 Run an Aleo Validator

Start by following the instructions in the [Build Guide](#2-build-guide).

The following command is recommended when starting a validator node:
`snarkos start --validator --nodisplay --bft 0.0.0.0:5000 --node 0.0.0.0:4130 --peers "validator_ip_1:4130,validator_ip_2:4130,...,core_client_ip_1:4130,core_client_ip_2:4130,..." --validators "validator_ip_1:5000,validator_ip_2:5000,..." --verbosity 1 --norest --private-key-file ~/snarkOS/privatekey`

Instead of specifying a private key file (`--private-key-file` flag), the private key can also be defined explicitly (`--private-key` flag).

To start a validator, you can also run the following command from the `snarkOS` directory:
```
./scripts/run-validator.sh
```

### 3.2.1 Validator Telemetry Metrics

Validator telemetry allows you to track participation in consensus. This is enabled by default in the standard build.

Once enabled, telemetry metrics are available through:

1. Node logs 
2. REST API endpoints
    ``` 
    // GET /{network}/validators/participation
    // GET /{network}/validators/participation?metadata={true}
    ```

Telemetry is part of the `metrics` feature, which is enabled by default. The `telemetry` feature flag
is kept as an alias for `metrics`, so commands that name it explicitly still work:

#### 1. [Installation](#2.3-installation)

Use the default installation command:
```
cargo install --locked --path .
```

#### 2. `./run-validator.sh`

Telemetry is included by default when running `./scripts/run-validator.sh`.

### 3.2.2 Required Consensus Upgrades

During the handshake, each validator shares the consensus version schedule of its build. Your validator needs an upgrade when validators that hold at least a third of the stake schedule a consensus version that your build does not run at that height. Your validator learns this as soon as those validators connect, which is usually long before the activation height.

When an upgrade is required, your validator:

1. Exports the metric `snarkos_consensus_needs_upgrade` with the value `1`. The metrics `snarkos_consensus_required_upgrade_height` and `snarkos_consensus_required_upgrade_version` give the activation height and the version.
2. Logs an error every 10 minutes, with the version and the activation height.
3. Keeps building blocks until 100 blocks before the activation height. There it stops and exits with an error, instead of forking.

If you run a validator, start it with `--metrics`, and alert when `snarkos_consensus_needs_upgrade` is greater than `0`. Then upgrade snarkOS at least 100 blocks before the height in `snarkos_consensus_required_upgrade_height`. For example, as a Prometheus alerting rule:

```yaml
- alert: SnarkOSNeedsUpgrade
  expr: snarkos_consensus_needs_upgrade > 0
  annotations:
    summary: "{{ $labels.instance }} must be upgraded 100 blocks before the height in snarkos_consensus_required_upgrade_height"
```

A validator that exits with this error needs an upgrade, not a restart. It writes the requirement to `required-consensus-upgrade` in its node data directory, and a restart with the same build exits again. An upgraded build removes the file.

## 3.3 Run an Aleo Prover

Start by following the instructions in the [Build Guide](#2-build-guide).

Next, generate an Aleo account address:
```
snarkos account new
```
This will output a new Aleo account in the terminal.

**Please remember to save the account private key and view key.** The following is an example output:
```
 Attention - Remember to store this account private key and view key.

  Private Key  APrivateKey1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx  <-- Save Me And Use In The Next Step
     View Key  AViewKey1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx  <-- Save Me
      Address  aleo1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx  <-- Save Me
```

Next, to start a proving node, from the `snarkOS` directory, run:
```
./scripts/run-prover.sh
```
When prompted, enter your Aleo private key:
```
Enter the Aleo Prover account private key:
APrivateKey1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

## 4. FAQs

### 1. My node is unable to compile.

- Ensure your machine has Rust installed, with at least [this version](rust-toolchain). Instructions to [install Rust can be found here.](https://www.rust-lang.org/tools/install)
- If large errors appear during compilation, try running `cargo clean`.
- Ensure `snarkOS` is started using `./scripts/run-client.sh` or `./scripts/run-prover.sh`.

### 2. My node is unable to connect to peers on the network.

- Ensure ports `4130/tcp` and `3030/tcp` are open on your router and OS firewall.
- Ensure `snarkOS` is started using `./scripts/run-client.sh` or `./scripts/run-prover.sh`.

### 3. I can't generate a new address ### 

- Before running the command above (`snarkos account new`) try `source ~/.bashrc`
- Also double-check the spelling of `snarkos`. Note the directory is `/snarkOS`, and the command is `snarkos`

### 4. How do I use the CLI to sign and verify a message?

1. Generate an account with `snarkos account new` if you haven't already
2. Sign a message with your private key using `snarkos account sign --raw -m "Message" --private-key-file=<PRIVATE_KEY_FILE>`
3. Verify your signature with `snarkos account verify --raw -m "Message" -s sign1SignatureHere -a aleo1YourAccountAddress`

Note, using the `--raw` flag with the command will sign plaintext messages as bytes rather than [Aleo values](https://developer.aleo.org/guides/aleo/language#data-types-and-values) such as `1u8` or `100field`.

### 5. What is the different between `node-data` and `ledger`?
Ledger contains only public ledger information, while `node-data` contains information specific to the node that created it. The latter should not be shared with untrusted sources, and, for validators, contains data required to participate in consensus.

### 6. At startup I get an error telling me the nodes still uses the old storage format. What should I do?
The node should have created a new folder for the node data. For example, for mainnet a folder should exist at `~/.aleo/storage/node-data-0`. 

The error message will tell you what data to migrate. Alternatively, you can start the node with `--auto-migrate-node-data` and it will attempt to do this automatically.

The following is an overview of all files that may be needed to be migrated. 
- The JSON Web token secret, located at `~/.aleo/storage/jwt_secrect_{address}.txt`. Move it to `~/.aleo/storage/node-data-0/jwt_secret_{address}.txt`.
  Note, if you are running different nodes with different addresses there may be multiple of these. The error message will tell you which one to migrate. 
- The router peer cache located at `~/.aleo/storage/ledger-0/cache_router_peers`. Migrate it to `~/.aleo/storage/node-data-0/router-peer-cache`.
- The gateway peer cache located at `~/.aleo/storage/ledger-0/cache_gateway_peers` (only for validators). Migrate it to `~/.aleo/storage/node-data-0/gateway-peer-cache`.
- The latest proposal cache located at `~/.aleo/storage/current-proposal-cache-0` (only for validators). Migrate it to `~/.aleo/storage/node-data-0/current-proposal-cache`.

## 5. Command Line Interface

To run a node with custom settings, refer to the options and flags available in the `snarkOS` CLI.

The full list of CLI flags and options can be viewed with `snarkos --help`:
```
snarkOS 
The Aleo Team <hello@aleo.org>

USAGE:
    snarkos [OPTIONS] <SUBCOMMAND>

OPTIONS:
    -h, --help                     Print help information
    -v, --verbosity <VERBOSITY>    Specify the verbosity [options: 0, 1, 2, 3] [default: 2]

SUBCOMMANDS:
    account    Commands to manage Aleo accounts
    clean      Cleans the snarkOS node storage
    help       Print this message or the help of the given subcommand(s)
    start      Starts the snarkOS node
    update     Update snarkOS
```

The following are the options for the `snarkos start` command:
```
      --network <NETWORK>
          Specify the network ID of this node [options: 0 = mainnet, 1 = testnet, 2 = canary]
          
          [default: 0]

      --prover
          Start the node as a prover

      --client
          Start the node as a client (default).
          
          Client are "full nodes", i.e, validate and execute all blocks they receive, but they do not participate in AleoBFT consensus.

      --bootstrap-client
          Start the node as a bootstrap client.

      --validator
          Start the node as a validator.
          
          Validators are "full nodes", like clients, but also participate in AleoBFT.

      --noupdater
          Disable checking for new versions at startup

      --private-key <PRIVATE_KEY>
          Specify the account private key of the node

      --private-key-file <PRIVATE_KEY_FILE>
          Specify the path to a file containing the account private key of the node

      --node <NODE>
          Set the IP address and port used for P2P communication

      --bft <BFT>
          Set the IP address and port used for BFT communication. This argument is only allowed for validator nodes

      --peers <PEERS>
          Specify the IP address and port of the peer(s) to connect to (as a comma-separated list).
          
          These peers will be set as "trusted", which means the node will not disconnect from them when performing peer rotation.
          
          Setting peers to "" has the same effect as not setting the flag at all, except when using `--dev`.

      --validators <VALIDATORS>
          Specify the IP address and port of the validator(s) to connect to

      --rest <REST>
          Specify the IP address and port for the REST server

      --rest-rps <REST_RPS>
          Specify the requests per second (RPS) rate limit per IP for the REST server
          
          [default: 10]

      --history-compat-mode [<URL>]
          Serve the routes of the removed `history` feature (`/program/{id}/mapping/{name}/{key}/history/{height}`, `/staking/rewards/{address}/{height}`, ...) from the Provable historical staking API instead of local tables. Takes an optional base URL of that API; by default, the network's own instance is used

      --jwt-secret <JWT_SECRET>
          Specify the JWT secret for the REST server (16B, base64-encoded)

      --jwt-timestamp <JWT_TIMESTAMP>
          Specify the JWT creation timestamp; can be any time in the last 10 years

      --norest
          If the flag is set, the node will not initialize the REST server

      --nojwt
          If the flag is set, the node will not require JWT authentication for the REST server

      --trusted-peers-only
          If the flag is set, the node will only connect to trusted peers and validators

      --nodisplay
          Write log message to stdout instead of showing a terminal UI.
          
          This is useful, for example, for running a node as a service instead of in the foreground or to pipe its output into a file.

      --verbosity <VERBOSITY>
          Specify the log verbosity of the node. [options: 0 (lowest log level) to 6 (highest level)]
          
          [default: 1]

      --log-filter <LOG_FILTER>
          Set a custom log filtering scheme, e.g., "off,snarkos_bft=trace", to show all log messages of snarkos_bft but nothing else

      --logfile <LOGFILE>
          Specify the path to the file where logs will be stored
          
          [default: /var/folders/6v/1bwnpyjd1r5f9wr_9hq25qsm0000gn/T/snarkos.log]

      --metrics
          Enable the metrics exporter

      --metrics-ip <METRICS_IP>
          Specify the IP address and port for the metrics exporter

      --ledger-storage <LEDGER_STORAGE>
          Specify the directory that holds all ledger data, e.g., blocks and transactions.
          This flag overrides the default path, even when `--dev` is set.
          
          The old name for this flag (`--storage`) is DEPRECATED and will eventually be removed.

      --node-data-storage <NODE_DATA_STORAGE>
          Specify the directory that holds node-specific data, that is not part of the global ledger.
          This flag overrides the default path, even when `--dev` is set.
          
          That folder may contain sensitive data, such as the JWT secret, and should not be shared with untrusted parties.
          For validators, it also contains the latest proposal cache, which is required to participate in consensus.

      --cdn <CDN>
          Enables the node to prefetch initial blocks from a CDN

      --nocdn
          If the flag is set, the node will not prefetch from a CDN

      --dev <DEV>
          Enables development mode used to set up test networks.
          
          The purpose of this flag is to run multiple nodes on the same machine and in the same working directory.
          To do this, set the value to a unique ID within the test work. For example if there are four nodes in the network, pass `--dev 0` for the first node, `--dev 1` for the second, and so forth.
          
          If you do not explicitly set the `--peers` flag, this will also populate the set of trusted peers, so that the network is fully connected.
          Additionally, if you do not set the `--rest` or the `--norest` flags, it will also set the REST port to `3030` for the first node, `3031` for the second, and so forth.

      --dev-num-validators <DEV_NUM_VALIDATORS>
          If development mode is enabled, specify the number of genesis validator
          
          [default: 4]

      --dev-num-clients <DEV_NUM_CLIENTS>
          If development mode is enabled, specify the number of clients. This is only used by validators to automatically populate their set of trusted peers.
          
          This option cannot be used while also passing the `--peers` flag.

      --no-dev-txs
          If development mode is enabled, specify whether node 0 should generate traffic to drive the network

      --dev-bonded-balances <DEV_BONDED_BALANCES>
          If development mode is enabled, specify the custom bonded balances as a JSON object

  -h, --help
          Print help (see a summary with '-h')
```

## 6. Development Guide

### 6.1 Quick Start

In the first terminal, start the first validator by running:
```
cargo run --release -- start --nodisplay --dev 0 --validator
```
In the second terminal, start the second validator by running:
```
cargo run --release -- start --nodisplay --dev 1 --validator
```
In the third terminal, start the third validator by running:
```
cargo run --release -- start --nodisplay --dev 2 --validator
```
In the fourth terminal, start the fourth validator by running:
```
cargo run --release -- start --nodisplay --dev 3 --validator
```

From here, this procedure can be used to further start-up provers and clients.

### 6.2 Operations

It is important to initialize the nodes starting from `0` and incrementing by `1` for each new node.

The following is a list of options to initialize a node (replace `<NODE_ID>` with a number starting from `0`):
```
cargo run --release -- start --nodisplay --dev <NODE_ID> --validator
cargo run --release -- start --nodisplay --dev <NODE_ID> --prover
cargo run --release -- start --nodisplay --dev <NODE_ID> --client
cargo run --release -- start --nodisplay --dev <NODE_ID>
```

When no node type is specified, the node will default to `--client`.

### 6.3 Local Devnet

#### 6.3.1 Install `tmux`

To run a local devnet with the script, start by installing `tmux`.

<details><summary>macOS</summary>

To install `tmux` on macOS, you can use the `Homebrew` package manager.
If you haven't installed `Homebrew` yet, you can find instructions at [their website](https://brew.sh/).
```bash
# Once Homebrew is installed, run:
brew install tmux
```

</details>

<details><summary>Ubuntu</summary>

On Ubuntu and other Debian-based systems, you can use the `apt` package manager:
```bash
sudo apt update
sudo apt install tmux
```

</details>

<details><summary>Windows</summary>

There are a couple of ways to use `tmux` on Windows:

### Using Windows Subsystem for Linux (WSL)

1. First, install [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/install).
2. Once WSL is set up and you have a Linux distribution installed (e.g., Ubuntu), open your WSL terminal and install `tmux` as you would on a native Linux system:
```bash
sudo apt update
sudo apt install tmux
```

</details>

#### 6.3.2 Start a Local Devnet

To start a local devnet, run:
```
./scripts/devnet.sh
```
Follow the instructions in the terminal to start the devnet.

#### 6.3.3 View a Local Devnet

#### Switch Nodes (forward)

To toggle to the next node in a local devnet, run:
```
Ctrl+b n
```

#### Switch Nodes (backwards)

To toggle to the previous node in a local devnet, run:
```
Ctrl+b p
```

#### Select a Node (choose-tree)

To select a node in a local devnet, run:
```
Ctrl+b w
```

#### Select a Node (manually)

To select a node manually in a local devnet, run:
```
Ctrl+b :select-window -t {NODE_ID}
```

#### 6.3.4 Stop a Local Devnet

To stop a local devnet, run:
```
Ctrl+b :kill-session
```
Then, press `Enter`.

### Clean Up

To clean up the node storage, run:
```
cargo run --release -- clean --dev <NODE_ID>
```

## 6.4 Feature Flags

By default, the metrics feature is turned on for some internal crates. It carries the validator
telemetry described in [Validator Telemetry Metrics](#321-validator-telemetry-metrics).

* **history** -
  Enables a /history REST endpoint.
* **telemetry** -
  Deprecated alias for **metrics**, kept so that existing build commands keep working.
  Validator telemetry is part of **metrics**.
* **cuda** -
  Allows some operations to run on the (NVidia) GPU, instead of on the CPU. See [CUDA acceleration for provers](#optional-cuda-acceleration-for-provers) for install tips and current puzzle status.
* **locktick** -
  This feature turns on code for detecting deadlocks.
* **test_targets** -
  This feature allows the lowering of coinbase and proof targets for testing.

## 6.5 Local Backups

The snarkOS node implementation uses rocksdb under the hood. By using its native checkpointing mechanism, you can create backups locally and efficiently. The backups leverage hard links on your filesystem, thereby incurring only a marginal amount of extra space. The aim of these local backups is for you to be able to recover quickly in case your node were to halt.

Note: in order for the backups to be incremental and lightweight, they need to be stored at the same filesystem (this includes `btrfs` subvolumes) as the ledger; otherwise, they become full copies.

You can find a basic sample script in `scripts/backup.sh` which you can run as a cron-job e.g. every minute. Each run of the script creates a new backup folder with a timestamp postfix. It will ensure a backup is kept which is 1 minute old, 5 minutes old, 1 hour old and 1 day old. In more detail, on each run it will:
- always overwrite the latest backup
- only overwrite the 5 minute backup if it is older than 5 minutes
- only overwrite the 1 hour backup if it is older than 1 hour
- only overwrite the 1 day backup if it is older than 1 day

You may want to change the `NETWORK`, `BASE_DIR`, `ENDPOINT` and `JWT` variables.

## 7. Contributors
Thank you for helping make snarkOS better!  
[🧐 What do the emojis mean?](https://allcontributors.org/docs/en/emoji-key)

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/howardwu"><img src="https://avatars.githubusercontent.com/u/9260812?v=4?s=100" width="100px;" alt="Howard Wu"/><br /><sub><b>Howard Wu</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=howardwu" title="Code">💻</a> <a href="#maintenance-howardwu" title="Maintenance">🚧</a> <a href="#ideas-howardwu" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/ProvableHQ/snarkOS/pulls?q=is%3Apr+reviewed-by%3Ahowardwu" title="Reviewed Pull Requests">👀</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/raychu86"><img src="https://avatars.githubusercontent.com/u/14917648?v=4?s=100" width="100px;" alt="Raymond Chu"/><br /><sub><b>Raymond Chu</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=raychu86" title="Code">💻</a> <a href="#maintenance-raychu86" title="Maintenance">🚧</a> <a href="#ideas-raychu86" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/ProvableHQ/snarkOS/pulls?q=is%3Apr+reviewed-by%3Araychu86" title="Reviewed Pull Requests">👀</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/ljedrz"><img src="https://avatars.githubusercontent.com/u/3750347?v=4?s=100" width="100px;" alt="ljedrz"/><br /><sub><b>ljedrz</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=ljedrz" title="Code">💻</a> <a href="#maintenance-ljedrz" title="Maintenance">🚧</a> <a href="#ideas-ljedrz" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/ProvableHQ/snarkOS/pulls?q=is%3Apr+reviewed-by%3Aljedrz" title="Reviewed Pull Requests">👀</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/niklaslong"><img src="https://avatars.githubusercontent.com/u/13221615?v=4?s=100" width="100px;" alt="Niklas Long"/><br /><sub><b>Niklas Long</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=niklaslong" title="Code">💻</a> <a href="#maintenance-niklaslong" title="Maintenance">🚧</a> <a href="#ideas-niklaslong" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/ProvableHQ/snarkOS/pulls?q=is%3Apr+reviewed-by%3Aniklaslong" title="Reviewed Pull Requests">👀</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/collinc97"><img src="https://avatars.githubusercontent.com/u/16715212?v=4?s=100" width="100px;" alt="Collin Chin"/><br /><sub><b>Collin Chin</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=collinc97" title="Code">💻</a> <a href="https://github.com/ProvableHQ/snarkOS/commits?author=collinc97" title="Documentation">📖</a> <a href="https://github.com/ProvableHQ/snarkOS/pulls?q=is%3Apr+reviewed-by%3Acollinc97" title="Reviewed Pull Requests">👀</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/iamalwaysuncomfortable"><img src="https://avatars.githubusercontent.com/u/26438809?v=4?s=100" width="100px;" alt="Mike Turner"/><br /><sub><b>Mike Turner</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=iamalwaysuncomfortable" title="Code">💻</a> <a href="https://github.com/ProvableHQ/snarkOS/commits?author=iamalwaysuncomfortable" title="Documentation">📖</a> <a href="https://github.com/ProvableHQ/snarkOS/pulls?q=is%3Apr+reviewed-by%3Aiamalwaysuncomfortable" title="Reviewed Pull Requests">👀</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://gakonst.com/"><img src="https://avatars.githubusercontent.com/u/17802178?v=4?s=100" width="100px;" alt="Georgios Konstantopoulos"/><br /><sub><b>Georgios Konstantopoulos</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=gakonst" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/kobigurk"><img src="https://avatars.githubusercontent.com/u/3520024?v=4?s=100" width="100px;" alt="Kobi Gurkan"/><br /><sub><b>Kobi Gurkan</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=kobigurk" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/vvp"><img src="https://avatars.githubusercontent.com/u/700877?v=4?s=100" width="100px;" alt="Vesa-Ville"/><br /><sub><b>Vesa-Ville</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=vvp" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/jules"><img src="https://avatars.githubusercontent.com/u/30194392?v=4?s=100" width="100px;" alt="jules"/><br /><sub><b>jules</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=jules" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/daniilr"><img src="https://avatars.githubusercontent.com/u/1212355?v=4?s=100" width="100px;" alt="Daniil"/><br /><sub><b>Daniil</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=daniilr" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/akattis"><img src="https://avatars.githubusercontent.com/u/4978114?v=4?s=100" width="100px;" alt="akattis"/><br /><sub><b>akattis</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=akattis" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/wcannon"><img src="https://avatars.githubusercontent.com/u/910589?v=4?s=100" width="100px;" alt="William Cannon"/><br /><sub><b>William Cannon</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=wcannon" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/wcannon-aleo"><img src="https://avatars.githubusercontent.com/u/93155840?v=4?s=100" width="100px;" alt="wcannon-aleo"/><br /><sub><b>wcannon-aleo</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=wcannon-aleo" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/sadroeck"><img src="https://avatars.githubusercontent.com/u/31270289?v=4?s=100" width="100px;" alt="Sam De Roeck"/><br /><sub><b>Sam De Roeck</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=sadroeck" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/soft2dev"><img src="https://avatars.githubusercontent.com/u/35427355?v=4?s=100" width="100px;" alt="soft2dev"/><br /><sub><b>soft2dev</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=soft2dev" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/amousa11"><img src="https://avatars.githubusercontent.com/u/12452142?v=4?s=100" width="100px;" alt="Ali Mousa"/><br /><sub><b>Ali Mousa</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=amousa11" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://pyk.sh/"><img src="https://avatars.githubusercontent.com/u/2213646?v=4?s=100" width="100px;" alt="pyk"/><br /><sub><b>pyk</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=pyk" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/whalelephant"><img src="https://avatars.githubusercontent.com/u/18553484?v=4?s=100" width="100px;" alt="Belsy"/><br /><sub><b>Belsy</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=whalelephant" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/apruden2008"><img src="https://avatars.githubusercontent.com/u/39969542?v=4?s=100" width="100px;" alt="apruden2008"/><br /><sub><b>apruden2008</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=apruden2008" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://stackoverflow.com/story/fabianoprestes"><img src="https://avatars.githubusercontent.com/u/976612?v=4?s=100" width="100px;" alt="Fabiano Prestes"/><br /><sub><b>Fabiano Prestes</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=zosorock" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/HarukaMa"><img src="https://avatars.githubusercontent.com/u/861659?v=4?s=100" width="100px;" alt="Haruka"/><br /><sub><b>Haruka</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=HarukaMa" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/e4m7he6g"><img src="https://avatars.githubusercontent.com/u/95574065?v=4?s=100" width="100px;" alt="e4m7he6g"/><br /><sub><b>e4m7he6g</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=e4m7he6g" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/w4ll3"><img src="https://avatars.githubusercontent.com/u/8595904?v=4?s=100" width="100px;" alt="Gregório Granado Magalhães"/><br /><sub><b>Gregório Granado Magalhães</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=w4ll3" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://stake.nodes.guru/"><img src="https://avatars.githubusercontent.com/u/44749897?v=4?s=100" width="100px;" alt="Evgeny Garanin"/><br /><sub><b>Evgeny Garanin</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=evgeny-garanin" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/macro-ss"><img src="https://avatars.githubusercontent.com/u/59944291?v=4?s=100" width="100px;" alt="Macro Hoober"/><br /><sub><b>Macro Hoober</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=macro-ss" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/code-pangolin"><img src="https://avatars.githubusercontent.com/u/89436546?v=4?s=100" width="100px;" alt="code-pangolin"/><br /><sub><b>code-pangolin</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=code-pangolin" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/kaola526"><img src="https://avatars.githubusercontent.com/u/88829586?v=4?s=100" width="100px;" alt="kaola526"/><br /><sub><b>kaola526</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=kaola526" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/clarenous"><img src="https://avatars.githubusercontent.com/u/18611530?v=4?s=100" width="100px;" alt="clarenous"/><br /><sub><b>clarenous</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=clarenous" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/unordered-set"><img src="https://avatars.githubusercontent.com/u/78592281?v=4?s=100" width="100px;" alt="Kostyan"/><br /><sub><b>Kostyan</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=unordered-set" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/austinabell"><img src="https://avatars.githubusercontent.com/u/24993711?v=4?s=100" width="100px;" alt="Austin Abell"/><br /><sub><b>Austin Abell</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=austinabell" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/yelhousni"><img src="https://avatars.githubusercontent.com/u/16170090?v=4?s=100" width="100px;" alt="Youssef El Housni"/><br /><sub><b>Youssef El Housni</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=yelhousni" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/ghostant-1017"><img src="https://avatars.githubusercontent.com/u/53888545?v=4?s=100" width="100px;" alt="ghostant-1017"/><br /><sub><b>ghostant-1017</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=ghostant-1017" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://pencil.li/"><img src="https://avatars.githubusercontent.com/u/5947268?v=4?s=100" width="100px;" alt="Miguel Gargallo"/><br /><sub><b>Miguel Gargallo</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=miguelgargallo" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/wang384670111"><img src="https://avatars.githubusercontent.com/u/78151109?v=4?s=100" width="100px;" alt="Chines Wang"/><br /><sub><b>Chines Wang</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=wang384670111" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/ayushgw"><img src="https://avatars.githubusercontent.com/u/14152340?v=4?s=100" width="100px;" alt="Ayush Goswami"/><br /><sub><b>Ayush Goswami</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=ayushgw" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/timsmith1337"><img src="https://avatars.githubusercontent.com/u/77958700?v=4?s=100" width="100px;" alt="Tim - o2Stake"/><br /><sub><b>Tim - o2Stake</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=timsmith1337" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/liusen-adalab"><img src="https://avatars.githubusercontent.com/u/74092505?v=4?s=100" width="100px;" alt="liu-sen"/><br /><sub><b>liu-sen</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=liusen-adalab" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Pa1amar"><img src="https://avatars.githubusercontent.com/u/20955327?v=4?s=100" width="100px;" alt="Palamar"/><br /><sub><b>Palamar</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=Pa1amar" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/swift-mx"><img src="https://avatars.githubusercontent.com/u/80231732?v=4?s=100" width="100px;" alt="swift-mx"/><br /><sub><b>swift-mx</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=swift-mx" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/dtynn"><img src="https://avatars.githubusercontent.com/u/1426666?v=4?s=100" width="100px;" alt="Caesar Wang"/><br /><sub><b>Caesar Wang</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=dtynn" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/paulip1792"><img src="https://avatars.githubusercontent.com/u/52645166?v=4?s=100" width="100px;" alt="Paul IP"/><br /><sub><b>Paul IP</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=paulip1792" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://philipglazman.com/"><img src="https://avatars.githubusercontent.com/u/8378656?v=4?s=100" width="100px;" alt="Philip Glazman"/><br /><sub><b>Philip Glazman</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=philipglazman" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Avadon"><img src="https://avatars.githubusercontent.com/u/404177?v=4?s=100" width="100px;" alt="Ruslan Nigmatulin"/><br /><sub><b>Ruslan Nigmatulin</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=Avadon" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.garillot.net/"><img src="https://avatars.githubusercontent.com/u/4142?v=4?s=100" width="100px;" alt="François Garillot"/><br /><sub><b>François Garillot</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=huitseeker" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/aolcr"><img src="https://avatars.githubusercontent.com/u/67066732?v=4?s=100" width="100px;" alt="aolcr"/><br /><sub><b>aolcr</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=aolcr" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/zvolin"><img src="https://avatars.githubusercontent.com/u/34972409?v=4?s=100" width="100px;" alt="Maciej Zwoliński"/><br /><sub><b>Maciej Zwoliński</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=zvolin" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.linkedin.com/in/ignacio-avecilla-39386a191/"><img src="https://avatars.githubusercontent.com/u/63374472?v=4?s=100" width="100px;" alt="Nacho Avecilla"/><br /><sub><b>Nacho Avecilla</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=IAvecilla" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Protryon"><img src="https://avatars.githubusercontent.com/u/8600837?v=4?s=100" width="100px;" alt="Max Bruce"/><br /><sub><b>Max Bruce</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=Protryon" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/whalelephant"><img src="https://avatars.githubusercontent.com/u/18553484?v=4?s=100" width="100px;" alt="whalelephant"/><br /><sub><b>Belsy</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=whalelephant" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/tranhoaison"><img src="https://avatars.githubusercontent.com/u/31094102?v=4?s=100" width="100px;" alt="tranhoaison"/><br /><sub><b>Santala</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=tranhoaison" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/web3deadline"><img src="https://avatars.githubusercontent.com/u/89900222?v=4?s=100" width="100px;" alt="web3deadline"/><br /><sub><b>deadline</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=web3deadline" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/CedricYanYuhui"><img src="https://avatars.githubusercontent.com/u/136431832?v=4?s=100" width="100px;" alt="CedricYanYuhui"/><br /><sub><b>CedricYanYuhui</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=CedricYanYuhui" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/craigjson"><img src="https://avatars.githubusercontent.com/u/16459396?v=4?s=100" width="100px;" alt="craigjson"/><br /><sub><b>Craig Johnson</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=craigjson" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/vbar"><img src="https://avatars.githubusercontent.com/u/108574?v=4?s=100" width="100px;" alt="vbar"/><br /><sub><b>Vaclav Barta</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=vbar" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/features/security"><img src="https://avatars.githubusercontent.com/u/27347476?v=4?s=100" width="100px;" alt="Dependabot"/><br /><sub><b>Dependabot</b></sub></a><br /><a href="https://github.com/ProvableHQ/snarkOS/commits?author=dependabot" title="Code">💻</a></td>
    </tr>
  </tbody>
  <tfoot>
    <tr>
      <td align="center" size="13px" colspan="7">
        <img src="https://raw.githubusercontent.com/all-contributors/all-contributors-cli/1b8533af435da9854653492b1327a23a4dbd0a10/assets/logo-small.svg">
          <a href="https://all-contributors.js.org/docs/en/bot/usage">Add your contributions</a>
        </img>
      </td>
    </tr>
  </tfoot>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind are welcome!

## 8. License

We welcome all contributions to `snarkOS`. Please refer to the [license](#7-license) for the terms of contributions.

[![License: GPL v3](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](./LICENSE.md)
