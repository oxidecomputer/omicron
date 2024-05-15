// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{bail, Context, Result};
use camino::Utf8Path;
use cargo_metadata::Message;
use clap::Parser;
use fs_err as fs;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::BufReader,
    process::{Command, Stdio},
};
use swrite::{swriteln, SWrite};

use crate::load_workspace;

#[derive(Parser)]
pub struct Args {
    /// Build in release mode
    #[clap(long)]
    release: bool,
}

#[derive(Deserialize, Debug)]
struct LibraryConfig {
    binary_allow_list: Option<BTreeSet<String>>,
}

#[derive(Deserialize, Debug)]
struct XtaskConfig {
    libraries: BTreeMap<String, LibraryConfig>,
}

#[derive(Debug)]
enum LibraryError {
    Unexpected(String),
    NotAllowed(String),
}

/// Verify that the binary at the provided path complies with the rules laid out
/// in the xtask.toml config file. Errors are pushed to a hashmap so that we can
/// display to a user the entire list of issues in one go.
fn verify_executable(
    config: &XtaskConfig,
    path: &Utf8Path,
    errors: &mut BTreeMap<String, Vec<LibraryError>>,
) -> Result<()> {
    #[cfg(not(target_os = "illumos"))]
    unimplemented!("Library verification is only available on illumos!");

    let binary = path.file_name().context("basename of executable")?;

    let command = Command::new("elfedit")
        .args(["-o", "simple", "-r", "-e", "dyn:tag NEEDED"])
        .arg(path)
        .output()
        .context("exec elfedit")?;

    if !command.status.success() {
        bail!("Failed to execute elfedit successfully {}", command.status);
    }

    let stdout = String::from_utf8(command.stdout)?;
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

pub fn run_cmd(args: Args) -> Result<()> {
    let metadata = load_workspace()?;
    let mut config_path = metadata.workspace_root;
    config_path.push(".cargo/xtask.toml");
    let config = read_xtask_toml(&config_path)?;

    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let mut command = Command::new(cargo);
    command.args([
        "build",
        "--bins",
        "--message-format=json-render-diagnostics",
    ]);
    if args.release {
        command.arg("--release");
    }
    let mut child = command
        .stdout(Stdio::piped())
        .spawn()
        .context("failed to spawn cargo build")?;

    let reader = BufReader::new(child.stdout.take().context("take stdout")?);

    let mut errors = Default::default();
    for message in cargo_metadata::Message::parse_stream(reader) {
        if let Message::CompilerArtifact(artifact) = message? {
            // We are only interested in artifacts that are binaries
            if let Some(executable) = artifact.executable {
                verify_executable(&config, &executable, &mut errors)?;
            }
        }
    }

    let status = child.wait()?;
    if !status.success() {
        bail!("Failed to execute cargo build successfully {}", status);
    }

    if !errors.is_empty() {
        let mut msg = String::new();
        errors.iter().for_each(|(binary, errors)| {
            swriteln!(msg, "{binary}");
            errors.iter().for_each(|error| match error {
                LibraryError::Unexpected(lib) => {
                    swriteln!(msg, "\tUNEXPECTED dependency on {lib}");
                }
                LibraryError::NotAllowed(lib) => {
                    swriteln!(msg, "\tNEEDS {lib} but is not allowed");
                }
            });
        });

        bail!(
            "Found library issues with the following:\n{msg}\n\n\
        If depending on a new library was intended please add it to xtask.toml"
        );
    }

    Ok(())
}

fn read_xtask_toml(path: &Utf8Path) -> Result<XtaskConfig> {
    let config_str = fs::read_to_string(path)?;
    toml::from_str(&config_str).with_context(|| format!("parse {:?}", path))
}
