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

use snarkos_cli::{commands::CLI, helpers::Updater};
use snarkvm::utilities::{display_error, flatten_error};

use clap::Parser;
use std::{backtrace::Backtrace, env, io, io::IsTerminal, panic::catch_unwind};
use tracing::{log::logger, subscriber::NoSubscriber};

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

// Obtain information on the build.
include!(concat!(env!("OUT_DIR"), "/built.rs"));

/// True if a real tracing subscriber is set (not the default no-op).
fn has_tracing_subscriber() -> bool {
    tracing::dispatcher::get_default(|d| !d.is::<NoSubscriber>())
}

/// Uses stderr when interactive or when no logger is set; otherwise logs via tracing.
macro_rules! print_error {
    ($($arg:tt)*) => {
        if io::stderr().is_terminal() || !has_tracing_subscriber() {
            eprintln!($($arg)*);
        } else {
            tracing::error!($($arg)*);
        }
    };
}

/// Uses stdout when interactive or when no logger is set; otherwise logs via tracing.
macro_rules! print_info {
    ($($arg:tt)*) => {
        if io::stderr().is_terminal() || !has_tracing_subscriber() {
            println!($($arg)*);
        } else {
            tracing::info!($($arg)*);
        }
    };
}
/// Stops the process with the given exit code.
fn exit(exitcode: i32) -> ! {
    tracing::debug!("Stopping process with exitcode {exitcode}");

    // Ensure all log messages are written before the process terminates.
    logger().flush();

    // Perform the system call to exit the process.
    std::process::exit(exitcode);
}

fn main() {
    // A hack to avoid having to go through clap to display advanced version information.
    check_for_version();

    snarkos_utilities::locks::spawn_lock_reporter();

    // Set a custom hook here to show "pretty" errors when panicking.
    std::panic::set_hook(Box::new(|err| {
        print_error!("⚠️ {}\n", err.to_string().replace("panicked at", "snarkOS encountered an unexpected error at"));

        // Always show backtraces.
        let backtrace = Backtrace::force_capture().to_string();

        let mut msg = "Backtrace:\n".to_string();
        msg.push_str("      [...]\n");

        // Remove all the low level frames.
        // This can be done more cleanly once the `backtrace_frames` feature is stabilized.
        let lines = backtrace.lines().skip_while(|line| !line.contains("core::panicking"));

        for line in lines {
            // Stop printing once we hit the panic handler.
            if line.contains("snarkos::main") {
                break;
            }

            msg.push_str(&format!("{line}\n"));
        }

        // Print the entire backtrace as a single log message.
        print_error!("{msg}");
    }));

    // Run the CLI.
    // We use `catch_unwind` here to ensure a panic stops execution and not just a single thread.
    // Note: `catch_unwind` can be nested without problems.
    let result = catch_unwind(|| {
        // Parse the given arguments.
        let cli = CLI::parse();

        // Run the updater.
        if !cli.noupdater
            && let Some(msg) = Updater::print_cli()
        {
            print_info!("{msg}");
        }

        // Run the CLI.
        cli.command.parse()
    });

    // Process any errors (including panics).
    match result {
        Ok(Ok(output)) => {
            print_info!("{output}");
            exit(0);
        }
        Ok(Err(err)) => {
            // A regular error occurred during startup.
            if io::stderr().is_terminal() || !has_tracing_subscriber() {
                display_error(&err);
                eprintln!();
                eprintln!("Use `--help` for instructions on how to use this command");
            } else {
                tracing::error!("{}", flatten_error(&err));
            }
            exit(1);
        }
        Err(_) => {
            print_error!(
                "This is most likely a bug!\n\
                Please report it to the snarkOS developers: https://github.com/aleo-labs-protocol/snarkOS/issues/new?template=bug.md"
            );

            exit(1);
        }
    }
}

/// Checks whether the version information was requested and - if so - display it and exit.
fn check_for_version() {
    if let Some(first_arg) = env::args().nth(1)
        && ["--version", "-V"].contains(&&*first_arg)
    {
        let version = env!("SNARKOS_VERSION");
        let branch = GIT_HEAD_REF.unwrap_or("unknown_branch");
        let commit = GIT_COMMIT_HASH.unwrap_or("unknown_commit");
        let mut features = FEATURES_LOWERCASE_STR.to_owned();
        features.retain(|c| c != ' ');

        print_info!("snarkos {version} {branch} {commit} features=[{features}]");

        exit(0);
    }
}
