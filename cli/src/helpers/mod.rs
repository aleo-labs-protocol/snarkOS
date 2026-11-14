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

mod bech32m;
pub use bech32m::*;

mod log_writer;
use log_writer::*;

mod dynamic_format;
use dynamic_format::*;

mod log_fields;
use log_fields::*;

#[cfg(target_family = "unix")]
mod fd_check;
#[cfg(target_family = "unix")]
pub use fd_check::*;

pub(crate) mod args;

pub mod logger;
pub use logger::*;

pub mod dev;

pub mod updater;
pub use updater::*;

use snarkos_node::network::NodeType;

use anyhow::Result;
#[cfg(target_family = "unix")]
use anyhow::ensure;
use colored::*;

/// Raises the soft open-files limit of this process to `minimum`, if it is lower and the hard limit allows it.
///
/// Returns an error if the limit stays below `minimum`.
#[cfg(target_family = "unix")]
pub fn raise_open_files_limit(minimum: u64) -> Result<()> {
    let limits = rlimit::Resource::NOFILE
        .get()
        .and_then(|(soft_limit, _)| Ok((soft_limit, rlimit::increase_nofile_limit(minimum)?)));
    let (soft_limit, limit) = match limits {
        Ok(limits) => limits,
        Err(err) => {
            // Warn about unknown limit.
            let warning = [
                format!("⚠️  Unable to check or raise the open files limit for this process due to {err}."),
                format!("  • To ensure correct behavior of the node, please ensure it is at least {minimum}."),
                "  • See the `ulimit` command and `/etc/security/limits.conf` for more details.".to_owned(),
            ]
            .join("\n")
            .yellow()
            .bold();
            eprintln!("{warning}\n");
            return Ok(());
        }
    };
    if limit > soft_limit {
        tracing::info!("Raised the open files limit from {soft_limit} to {limit}");
    }
    ensure!(
        limit >= minimum,
        "The open files limit ({limit}) for this process is below {minimum}, and its hard limit does not allow raising it. \
        Raise the hard limit to at least {minimum}. \
        See the `ulimit -Hn` command, `/etc/security/limits.conf`, or `LimitNOFILE` for a systemd service."
    );
    Ok(())
}

/// Returns the RAM memory in GiB.
pub(crate) fn detect_ram_memory() -> Result<u64, sys_info::Error> {
    let ram_kib = sys_info::mem_info()?.total;
    let ram_mib = ram_kib / 1024;
    Ok(ram_mib / 1024)
}

/// Ensures the current system meets the minimum requirements for a validator.
/// Note: Some of the checks in this method are overly-permissive, in order to ensure
/// future hardware architecture changes do not prevent validators from running a node.
#[rustfmt::skip]
pub(crate) fn check_validator_machine(node_type: NodeType) {
    // If the node is a validator, ensure it meets the minimum requirements.
    if node_type.is_validator() {
        // Ensure the system is a Linux-based system.
        // Note: While macOS is not officially supported, we allow it for development purposes.
        if !cfg!(target_os = "linux") && !cfg!(target_os = "macos") {
            let message = "⚠️  The operating system of this machine is not supported for a validator (Ubuntu required)\n".to_string();
            println!("{}", message.yellow().bold());
        }
        // Retrieve the number of cores.
        let num_cores = num_cpus::get();
        // Enforce the minimum number of cores.
        let min_num_cores = 64;
        if num_cores < min_num_cores {
            let message = format!("⚠️  The number of cores ({num_cores} cores) on this machine is insufficient for a validator (minimum {min_num_cores} cores)\n");
            println!("{}", message.yellow().bold());
        }
        // Enforce the minimum amount of RAM.
        if let Ok(ram) = crate::helpers::detect_ram_memory() {
            let min_ram = 256;
            if ram < min_ram {
                let message = format!("⚠️  The amount of RAM ({ram} GiB) on this machine is insufficient for a validator (minimum {min_ram} GiB)\n");
                println!("{}", message.yellow().bold());
            }
        }
    }
}

#[cfg(all(test, target_family = "unix"))]
mod tests {
    use super::*;

    #[test]
    fn test_raise_open_files_limit() {
        // Reading the limits of this process does not fail on a unix target.
        let (soft_limit, hard_limit) = rlimit::Resource::NOFILE.get().unwrap();

        // A minimum at or below the soft limit leaves the soft limit as it is.
        raise_open_files_limit(soft_limit).unwrap();
        assert_eq!(rlimit::Resource::NOFILE.get().unwrap().0, soft_limit);

        // A minimum above the soft limit, and within the hard limit, raises the soft limit to it.
        if soft_limit != rlimit::INFINITY {
            // Lowering a finite soft limit of this process does not fail.
            rlimit::Resource::NOFILE.set(soft_limit - 1, hard_limit).unwrap();
            raise_open_files_limit(soft_limit).unwrap();
            assert_eq!(rlimit::Resource::NOFILE.get().unwrap().0, soft_limit);
        }

        // A minimum above the hard limit cannot be reached.
        if hard_limit != rlimit::INFINITY {
            assert!(raise_open_files_limit(hard_limit + 1).is_err());
        }
    }
}
