// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::process::Command;

use camino::Utf8Path;

use crate::XtaskConfig;

/// Verify that the binary at the provided path complies with the rules laid out
/// in the xtask.toml config file. Errors are pushed to a hashmap so that we can
/// display to a user the entire list of issues in one go.
pub fn verify_executable_libraries(
    config: &XtaskConfig,
    path: &Utf8Path,
    errors: &mut BTreeMap<String, Vec<LibraryError>>,
) -> std::io::Result<()> {
    if !cfg!(target_os = "illumos") {
        return Err(std::io::Error::other(
            "cannot verify libraries on non-illumos system",
        ));
    }

    let binary = path.file_name().ok_or_else(|| {
        std::io::Error::new(ErrorKind::InvalidInput, "path has no file name")
    })?;
    let command = Command::new("elfedit")
        .args(["-o", "simple", "-r", "-e", "dyn:tag NEEDED"])
        .arg(path)
        .output()?;
    if !command.status.success() {
        return Err(std::io::Error::other(format!(
            "elfedit exited unsuccessfully: {}",
            command.status
        )));
    }
    let stdout = String::from_utf8(command.stdout).map_err(|_| {
        std::io::Error::new(
            ErrorKind::InvalidData,
            "elfedit output was not valid UTF-8",
        )
    })?;
    // `elfedit -o simple -r -e "dyn:tag NEEDED" /file/path` will return
    // a new line seperated list of required libraries so we walk over
    // them looking for a match in our configuration file. If we find
    // the library we make sure the binary is allowed to pull it in via
    // the whitelist.
    for library in stdout.lines() {
        let library_config = match config.libraries.get(library.trim()) {
            Some(config) => config,
            None => {
                errors
                    .entry(binary.to_string())
                    .or_default()
                    .push(LibraryError::Unexpected(library.to_string()));

                continue;
            }
        };

        if let Some(allowed) = &library_config.binary_allow_list {
            if !allowed.contains(binary) {
                errors
                    .entry(binary.to_string())
                    .or_default()
                    .push(LibraryError::NotAllowed(library.to_string()));
            }
        }
    }

    Ok(())
}

#[derive(Debug)]
pub enum LibraryError {
    Unexpected(String),
    NotAllowed(String),
}

impl std::fmt::Display for LibraryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LibraryError::Unexpected(lib) => {
                write!(f, "UNEXPECTED dependency on {lib}")
            }
            LibraryError::NotAllowed(lib) => {
                write!(f, "NEEDS {lib} but is not allowed")
            }
        }
    }
}

impl std::error::Error for LibraryError {}
