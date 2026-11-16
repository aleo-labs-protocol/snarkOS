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

use crate::{commands::ApiVersion, helpers::dev::get_development_key};

use snarkos_utilities::NodeDataDir;

use snarkvm::{
    console::network::{CanaryV0, MainnetV0},
    prelude::{Network, PrivateKey},
};

use aleo_std::aleo_dir;
use anyhow::{Context, Result, anyhow, ensure};
use clap::builder::RangedU64ValueParser;
use snarkos_node_rest::{API_VERSION_V1, API_VERSION_V2};
use std::{path::PathBuf, str::FromStr};
use ureq::http::{Uri, uri};

pub(crate) fn network_id_parser() -> RangedU64ValueParser<u16> {
    RangedU64ValueParser::<u16>::new().range((MainnetV0::ID as u64)..=(CanaryV0::ID as u64))
}

/// Make sure a proper scheme (http or https) is set for the endpoint, and that it names an API
/// version.
///
/// An endpoint whose path does not end in `v1` or `v2` gets `/v2` appended: the unversioned
/// routes of a snarkOS node serve the deprecated v1 API.
///
/// An endpoint with a query string is rejected, since request routes are appended to its path.
pub(crate) fn prepare_endpoint(endpoint: Uri) -> Result<Uri> {
    ensure!(endpoint.query().is_none(), "The endpoint URL must not have a query string");
    let mut parts = endpoint.into_parts();

    if parts.scheme.is_none() {
        println!("No scheme given for endpoint. Defaulting to HTTP.");
        parts.scheme = Some(uri::Scheme::HTTP);
    }

    let path = parts.path_and_query.as_ref().map(|pq| pq.path()).unwrap_or("/").trim_end_matches('/').to_owned();
    if api_version_of(&path).is_none() {
        parts.path_and_query = Some(
            format!("{path}/{API_VERSION_V2}/").parse().with_context(|| "Invalid endpoint URL with an API version")?,
        );
    }

    // Given that the input URI is valid and the scheme we assign is valid, this should never fail.
    Uri::from_parts(parts).with_context(|| "Invalid endpoint URL")
}

/// Returns the API version named by the last segment of a URL path, if any.
pub(crate) fn api_version_of(path: &str) -> Option<ApiVersion> {
    match path.trim_end_matches('/').rsplit('/').next() {
        Some(API_VERSION_V1) => Some(ApiVersion::V1),
        Some(API_VERSION_V2) => Some(ApiVersion::V2),
        _ => None,
    }
}

/// Fetch the private key, either from the commandline or from a file.
pub(crate) fn parse_private_key<N: Network>(
    cmdline: Option<String>,
    file_name: Option<String>,
    dev_key: Option<u16>,
) -> Result<PrivateKey<N>> {
    if let Some(index) = dev_key {
        let private_key = get_development_key(index)?;
        println!("🔑 Using development private key for node {index} ({private_key})\n");
        return Ok(private_key);
    }

    let key_str = if let Some(keystr) = cmdline {
        keystr
    } else if let Some(file_name) = file_name {
        let path = file_name.parse::<PathBuf>().map_err(|e| anyhow!("Invalid path - {e}"))?;
        std::fs::read_to_string(path).with_context(|| "Failed to read private key from disk")?.trim().to_string()
    } else {
        unreachable!();
    };

    // Retrieve the private key.
    let private_key = PrivateKey::from_str(&key_str).with_context(|| "Failed to parse private key")?;

    Ok(private_key)
}

/// Returns the path to the node configuration directory.
pub(crate) fn parse_node_data_dir(
    custom_node_data_dir: &Option<PathBuf>,
    network: u16,
    dev: Option<u16>,
) -> Result<NodeDataDir> {
    if let Some(custom) = custom_node_data_dir {
        return Ok(NodeDataDir::new(custom.clone()));
    }

    let config = if let Some(dev) = dev {
        NodeDataDir::new_development(network, dev)
    } else {
        // Production mode
        let path = aleo_dir().join("storage").join(format!("node-data-{network}"));
        NodeDataDir::new(path)
    };

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prepare_endpoint() {
        // No version: v2 is requested explicitly, since the unversioned routes serve the
        // deprecated v1 API.
        for (before, after) in [
            ("localhost:3030", "http://localhost:3030/v2/"),
            ("http://localhost:3030/", "http://localhost:3030/v2/"),
            ("https://example.com/a/prefix", "https://example.com/a/prefix/v2/"),
            // An explicit version is kept as given.
            ("http://localhost:3030/v1", "http://localhost:3030/v1"),
            // A path that merely ends in the same characters is not a version.
            ("https://example.com/dev2", "https://example.com/dev2/v2/"),
            ("https://api.explorer.provable.com/v2", "https://api.explorer.provable.com/v2"),
        ] {
            let prepared = prepare_endpoint(Uri::try_from(before).unwrap()).unwrap();
            assert_eq!(prepared.to_string(), after, "unexpected endpoint for {before}");
        }

        for endpoint in ["http://localhost:3030/?foo=bar", "http://localhost:3030/v2?foo=bar"] {
            assert!(prepare_endpoint(Uri::try_from(endpoint).unwrap()).is_err(), "accepted {endpoint}");
        }
    }
}
