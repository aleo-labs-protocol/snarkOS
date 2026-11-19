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

use std::{env, fs, path::Path};

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
    // Register the release version for runtime version reporting.
    emit_version_env();
    // Register build-time information.
    built::write_built_file().expect("Failed to acquire build-time information");
}
