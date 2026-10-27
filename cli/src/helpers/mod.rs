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
use colored::*;
#[cfg(target_family = "unix")]
use nix::sys::resource::{Resource, getrlimit};

/// Check if process's open files limit is above minimum and warn if not.
#[cfg(target_family = "unix")]
pub fn check_open_files_limit(minimum: u64) {
    // Acquire current limits.
    match getrlimit(Resource::RLIMIT_NOFILE) {
        Ok((soft_limit, _)) => {
            // Check if requirements are met.
            if soft_limit < minimum {
                panic!(
                    "The open files limit ({soft_limit}) for this process is too low. \
                    Please raise it to at least {minimum} \
                    See the `ulimit` command and `/etc/security/limits.conf` for more details.",
                );
            }
        }
        Err(err) => {
            // Warn about unknown limit.
            let warning = [
                format!("⚠️  Unable to check the open files limit for this process due to {err}."),
                format!("  • To ensure correct behavior of the node, please ensure it is at least {minimum}."),
                "  • See the `ulimit` command and `/etc/security/limits.conf` for more details.".to_owned(),
            ]
            .join("\n")
            .yellow()
            .bold();
            eprintln!("{warning}\n");
        }
    };
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
