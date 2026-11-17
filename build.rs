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

use std::{env, fs, path::Path, process};
use toml::Value;

fn check_locktick_profile() {
    let locktick_enabled = env::var("CARGO_FEATURE_LOCKTICK").is_ok();
    if locktick_enabled {
        // First check the env variables that can override the TOML values.
        let (mut valid_debug_override, mut valid_strip_override) = (false, false);

        if let Ok(val) = env::var("CARGO_PROFILE_RELEASE_DEBUG") {
            if val != "line-tables-only" {
                eprintln!(
                    "🔴 When enabling the locktick feature, CARGO_PROFILE_RELEASE_DEBUG may only be set to `line-tables-only`."
                );
                process::exit(1);
            } else {
                valid_debug_override = true;
            }
        }
        if let Ok(val) = env::var("CARGO_PROFILE_RELEASE_STRIP") {
            if val != "none" {
                eprintln!(
                    "🔴 When enabling the locktick feature, CARGO_PROFILE_RELEASE_STRIP may only be set to `none`."
                );
                process::exit(1);
            } else {
                valid_strip_override = true;
            }
        }

        if valid_debug_override && valid_strip_override {
            // Both overrides are compatible with locktick, no need to check the TOML.
            return;
        }

        // If the relevant overrides were either invalid or not present, check the TOML.
        let profile = env::var("PROFILE").unwrap_or_else(|_| "".to_string());
        let manifest = Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap()).join("Cargo.toml");
        let contents = fs::read_to_string(&manifest).expect("failed to read Cargo.toml");
        let doc: Value = toml::from_str(&contents).expect("invalid TOML in Cargo.toml");

        let profile_table = doc.get("profile").and_then(|p| p.get(profile));
        if let Some(Value::Table(profile_settings)) = profile_table {
            if let Some(debug) = profile_settings.get("debug") {
                match debug {
                    Value::String(s) if s == "line-tables-only" => {
                        println!("cargo:info=manifest has debuginfo=line-tables-only");
                    }
                    _ => {
                        eprintln!(
                            "🔴 When enabling the locktick feature, the profile must have debug set to `line-tables-only`. Uncomment the relevant lines in Cargo.toml."
                        );
                        process::exit(1);
                    }
                }
            } else {
                eprintln!(
                    "🔴 When enabling the locktick feature, the profile must have `debug` set to `line-tables-only`. Uncomment the relevant lines in Cargo.toml."
                );
                process::exit(1);
            }
            if let Some(debug) = profile_settings.get("strip") {
                match debug {
                    Value::String(s) if s == "none" => {
                        println!("cargo:info=manifest has strip=none");
                    }
                    _ => {
                        eprintln!(
                            "🔴 When enabling the locktick feature, the profile must have `strip` set to `none`. Uncomment the relevant lines in Cargo.toml."
                        );
                        process::exit(1);
                    }
                }
            }
        }
    }
}

fn is_clippy() -> bool {
    env::var("RUSTC_WORKSPACE_WRAPPER").is_ok_and(|var| var.contains("clippy"))
}

fn check_tokio_console_flags() {
    // Don't run this check under clippy, otherwise it will cause issues with --all-features.
    if is_clippy() {
        return;
    }

    // Skip if the feature is not used.
    let feature_enabled = env::var("CARGO_FEATURE_TOKIO_CONSOLE").is_ok();
    if !feature_enabled {
        return;
    }

    // Check for the presence of RUSTFLAGS.
    let Ok(rustflags) = env::var("CARGO_ENCODED_RUSTFLAGS") else {
        eprintln!(
            "🔴 When enabling the tokio_console feature, you must run with `RUSTFLAGS=\"--cfg tokio_unstable\"`."
        );
        process::exit(1);
    };

    // Check for the presence of `tokio_unstable` within RUSTFLAGS.
    if !rustflags.contains("tokio_unstable") {
        eprintln!(
            "🔴 When enabling the tokio_console feature, you must run with `RUSTFLAGS=\"--cfg tokio_unstable\"`."
        );
        process::exit(1);
    }
}

// The build script.

// Emits the `SNARKOS_VERSION` environment variable, sourced from the repository's
// `.cargo/release-version` file. Published packages cannot include files outside
// of the crate directory, so they fall back to the crate version, which is kept
// in sync with the release version.
fn emit_version_env() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("Failed to read CARGO_MANIFEST_DIR");
    let release_version = Path::new(&manifest_dir)
        .ancestors()
        .map(|dir| dir.join(".cargo").join("release-version"))
        .find(|path| path.is_file());
    let version = match &release_version {
        Some(path) => {
            println!("cargo:rerun-if-changed={}", path.display());
            fs::read_to_string(path).unwrap_or_else(|err| panic!("Failed to read '{}': {err}", path.display()))
        }
        None => env::var("CARGO_PKG_VERSION").expect("Failed to read CARGO_PKG_VERSION"),
    };
    let version = version.trim();
    let version = version.strip_prefix('v').unwrap_or(version);
    assert!(!version.is_empty(), "The snarkOS version is empty");
    println!("cargo:rustc-env=SNARKOS_VERSION={version}");
}

fn main() {
    // Check if locktick feature is correctly enabled.
    check_locktick_profile();
    // Check if the tokio_console feature is correctly enabled.
    check_tokio_console_flags();

    // Register the release version for runtime version reporting.
    emit_version_env();
    // Register build-time information.
    built::write_built_file().expect("Failed to acquire build-time information");
}
