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

use crate::helpers::{DynamicFormatter, LogWriter, SnarkosFields};

use anyhow::{Result, bail};

use crossterm::tty::IsTty;
use std::{
    fs::File,
    io,
    path::Path,
    str::FromStr,
    sync::{Arc, atomic::AtomicBool},
};
use tokio::sync::mpsc;
use tracing_subscriber::{
    EnvFilter,
    layer::{Layer, SubscriberExt},
    util::SubscriberInitExt,
};

fn parse_log_verbosity(verbosity: u8) -> Result<EnvFilter> {
    // First, set default log verbosity.
    // Note, that this must not be prefixed with `RUST_LOG=`.
    let default_log_str = match verbosity {
        0 => "info",
        1 => "debug",
        2.. => "trace",
    };
    let filter = EnvFilter::from_str(default_log_str).unwrap();

    // Now, set rules for specific crates.
    let filter = if verbosity >= 2 {
        filter.add_directive("snarkos_node_sync=trace".parse().unwrap())
    } else if verbosity >= 1 {
        filter.add_directive("snarkos_node_sync=debug".parse().unwrap())
    } else {
        filter
    };

    let filter = if verbosity >= 3 {
        filter.add_directive("snarkos_node_bft=trace".parse().unwrap())
    } else if verbosity >= 1 {
        filter.add_directive("snarkos_node_bft=debug".parse().unwrap())
    } else {
        filter
    };

    let filter = if verbosity >= 4 {
        filter.add_directive("snarkos_node_bft::gateway=trace".parse().unwrap())
    } else if verbosity >= 1 {
        filter.add_directive("snarkos_node_bft::gateway=debug".parse().unwrap())
    } else {
        filter
    };

    let filter = if verbosity >= 4 {
        // At high log levels, also show warnings of third-party crates.
        filter
            .add_directive("mio=warn".parse().unwrap())
            .add_directive("tokio_util=warn".parse().unwrap())
            .add_directive("hyper=warn".parse().unwrap())
            .add_directive("reqwest=warn".parse().unwrap())
            .add_directive("want=warn".parse().unwrap())
            .add_directive("h2=warn".parse().unwrap())
            .add_directive("tower=warn".parse().unwrap())
            .add_directive("axum=warn".parse().unwrap())
            .add_directive("ureq=warn".parse().unwrap())
            .add_directive("rustls=warn".parse().unwrap())
    } else {
        // Disable logs from third-party crates by default.
        filter
            .add_directive("mio=off".parse().unwrap())
            .add_directive("tokio_util=off".parse().unwrap())
            .add_directive("hyper=off".parse().unwrap())
            .add_directive("reqwest=off".parse().unwrap())
            .add_directive("want=off".parse().unwrap())
            .add_directive("h2=off".parse().unwrap())
            .add_directive("tower=off".parse().unwrap())
            .add_directive("axum=off".parse().unwrap())
            .add_directive("ureq=off".parse().unwrap())
            .add_directive("rustls=off".parse().unwrap())
    };

    let filter = if verbosity >= 5 {
        filter.add_directive("snarkos_node_router=trace".parse().unwrap())
    } else if verbosity >= 1 {
        filter.add_directive("snarkos_node_router=debug".parse().unwrap())
    } else {
        filter
    };

    let filter = if verbosity >= 6 {
        filter.add_directive("snarkos_node_tcp=trace".parse().unwrap())
    } else {
        filter.add_directive("snarkos_node_tcp=off".parse().unwrap())
    };

    Ok(filter)
}

fn parse_log_filter(filter_str: &str) -> Result<EnvFilter> {
    EnvFilter::from_str(filter_str).map_err(|err| err.into())
}

/// Sets the log filter based on the given verbosity level.
/// Initializes the logger with the specified verbosity level, where 0 is the lowest verbosity and 6 the highest.
///
/// The following shows what messages are enabled at each level.
/// ```ignore
/// 0 => info
/// 1 => info, debug
/// 2 => info, debug, trace, snarkos_node_sync=trace
/// 3 => info, debug, trace, snarkos_node_bft=trace
/// 4 => info, debug, trace, snarkos_node_bft::gateway=trace,
///      [mio|tokio_util|hyper|reqwest|want|h2|tower|axum]=warn
/// 5 => info, debug, trace, snarkos_node_router=trace
/// 6 => info, debug, trace, snarkos_node_tcp=trace
/// ```
pub fn initialize_logger<P: AsRef<Path>>(
    verbosity: u8,
    log_filter: &Option<String>,
    nodisplay: bool,
    logfile: P,
    shutdown: Arc<AtomicBool>,
) -> Result<mpsc::Receiver<Vec<u8>>> {
    let [stdout_filter, logfile_filter] = std::array::from_fn(|_| {
        if let Some(filter) = log_filter { parse_log_filter(filter) } else { parse_log_verbosity(verbosity) }
    });

    // Create the directories tree for a logfile if it doesn't exist.
    let Some(logfile_dir) = logfile.as_ref().parent() else { bail!("Root directory passed as a logfile") };

    if !logfile_dir.exists()
        && let Err(err) = std::fs::create_dir_all(logfile_dir)
    {
        bail!("Failed to create a directory: '{}' ({err})", logfile_dir.display());
    }
    // Create a file to write logs to.
    let logfile = match File::options().append(true).create(true).open(logfile) {
        Ok(logfile) => logfile,
        Err(err) => bail!("Failed to open the file for writing logs: {err}"),
    };

    // Initialize the log channel.
    let (log_sender, log_receiver) = mpsc::channel(1024);

    // Initialize the log sender.
    // This is only needed when the terminal UI (display) wants to process log output.
    let log_sender = match nodisplay {
        true => None,
        false => Some(log_sender),
    };

    // At high verbosity or when there is a custom log filter we show the target
    // of the log event, i.e., the file/module where the log message was created.
    let show_target = verbosity > 2 || log_filter.is_some();

    // Attach tracing-subscriber.
    let layered = tracing_subscriber::registry()
        .with(
            // Add layer using LogWriter for stdout / terminal
            tracing_subscriber::fmt::Layer::default()
                .with_ansi(log_sender.is_none() && io::stdout().is_tty())
                .fmt_fields(SnarkosFields)
                .with_writer(move || LogWriter::new(&log_sender))
                .with_target(show_target)
                .event_format(DynamicFormatter::new(shutdown))
                .with_filter(stdout_filter?),
        )
        .with(
            // Add layer redirecting logs to the file
            tracing_subscriber::fmt::Layer::default()
                .with_ansi(false)
                .fmt_fields(SnarkosFields)
                .with_writer(logfile)
                .with_target(show_target)
                .with_filter(logfile_filter?),
        );

    // Attach console-subscriber, if enabled.
    #[cfg(feature = "tokio_console")]
    let layered = layered.with(console_subscriber::spawn());

    // Initialize tracing.
    let _ = layered.try_init();

    Ok(log_receiver)
}

/// Set up only terminal logging
pub fn initialize_terminal_logger(verbosity: u8) -> Result<()> {
    let stdout_filter = parse_log_verbosity(verbosity)?;

    // At high verbosity or when there is a custom log filter we show the target
    // of the log event, i.e., the file/module where the log message was created.
    let show_target = verbosity > 2;

    // Initialize tracing.
    let _ = tracing_subscriber::registry()
        .with(
            // Add layer using LogWriter for stdout / terminal
            tracing_subscriber::fmt::Layer::default()
                .with_ansi(io::stdout().is_tty())
                .fmt_fields(SnarkosFields)
                .with_target(show_target)
                .event_format(DynamicFormatter::new(Arc::new(AtomicBool::new(false))))
                .with_filter(stdout_filter),
        )
        .try_init();

    Ok(())
}

/// Returns the welcome message as a string.
pub fn welcome_message() -> String {
    use colored::Colorize;

    let mut output = String::new();
    output += &r#"

         ╦╬╬╬╬╬╦
        ╬╬╬╬╬╬╬╬╬                    ▄▄▄▄        ▄▄▄
       ╬╬╬╬╬╬╬╬╬╬╬                  ▐▓▓▓▓▌       ▓▓▓
      ╬╬╬╬╬╬╬╬╬╬╬╬╬                ▐▓▓▓▓▓▓▌      ▓▓▓     ▄▄▄▄▄▄       ▄▄▄▄▄▄
     ╬╬╬╬╬╬╬╬╬╬╬╬╬╬╬              ▐▓▓▓  ▓▓▓▌     ▓▓▓   ▄▓▓▀▀▀▀▓▓▄   ▐▓▓▓▓▓▓▓▓▌
    ╬╬╬╬╬╬╬╜ ╙╬╬╬╬╬╬╬            ▐▓▓▓▌  ▐▓▓▓▌    ▓▓▓  ▐▓▓▓▄▄▄▄▓▓▓▌ ▐▓▓▓    ▓▓▓▌
   ╬╬╬╬╬╬╣     ╠╬╬╬╬╬╬           ▓▓▓▓▓▓▓▓▓▓▓▓    ▓▓▓  ▐▓▓▀▀▀▀▀▀▀▀▘ ▐▓▓▓    ▓▓▓▌
  ╬╬╬╬╬╬╣       ╠╬╬╬╬╬╬         ▓▓▓▓▌    ▐▓▓▓▓   ▓▓▓   ▀▓▓▄▄▄▄▓▓▀   ▐▓▓▓▓▓▓▓▓▌
 ╬╬╬╬╬╬╣         ╠╬╬╬╬╬╬       ▝▀▀▀▀      ▀▀▀▀▘  ▀▀▀     ▀▀▀▀▀▀       ▀▀▀▀▀▀
╚╬╬╬╬╬╩           ╩╬╬╬╬╩


"#
    .white()
    .bold();
    output += &"👋 Welcome to Aleo! We thank you for running a node and supporting privacy.\n".bold();
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_filter() {
        let result = parse_log_filter("=");
        assert!(result.is_err(), "must disallow invalid log filter");

        let result = parse_log_filter("snarkos=trace");
        assert!(result.is_ok(), "must allow valid log filter");
    }
}
