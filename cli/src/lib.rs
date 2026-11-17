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

// See https://github.com/aleo-labs-protocol/snarkVM/issues/2775
#![forbid(unsafe_code)]
#![recursion_limit = "256"]

#[macro_use]
extern crate thiserror;

#[cfg(feature = "metrics")]
extern crate snarkos_node_metrics as metrics;

pub mod commands;
pub use commands::CLI;

pub mod helpers;

use anyhow::Result;
use std::{
    fs::{File, Permissions},
    path::Path,
};

/// Checks whether the parent directory of a file can only be read and modified by the owner.
#[cfg(unix)]
pub fn check_parent_permissions<T: AsRef<Path>>(path: T) -> Result<()> {
    use anyhow::{bail, ensure};
    use std::os::unix::fs::PermissionsExt;

    if let Some(parent) = path.as_ref().parent() {
        let permissions = parent.metadata()?.permissions().mode();
        ensure!(
            permissions & 0o777 == 0o700,
            "The folder {} must be readable and writeable only by the owner (0700)",
            parent.display()
        );
    } else {
        let path = path.as_ref();
        bail!("Parent does not exist for path={}", path.display());
    }

    Ok(())
}

#[cfg(windows)]
pub fn check_parent_permissions<T: AsRef<Path>>(_path: T) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_user_read_only(file: &File) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let permissions = Permissions::from_mode(0o400);
    file.set_permissions(permissions)?;
    Ok(())
}

#[cfg(windows)]
fn set_user_read_only(file: &File) -> Result<()> {
    let mut permissions = file.metadata()?.permissions();
    permissions.set_readonly(true);
    file.set_permissions(permissions)?;
    Ok(())
}
