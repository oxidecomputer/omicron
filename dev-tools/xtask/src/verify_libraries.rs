// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, Result, bail};
use camino::Utf8Path;
use cargo_metadata::Message;
use clap::Parser;
use dev_tools_common::{
    CargoLocation, XtaskConfig, cargo_command, verify_executable_libraries,
};
use fs_err as fs;
use std::{io::BufReader, process::Stdio};
use swrite::{SWrite, swriteln};

use crate::load_workspace;

#[derive(Parser)]
pub struct Args {
    /// Build in release mode
    #[clap(long)]
    release: bool,
}

pub fn run_cmd(args: Args) -> Result<()> {
    let metadata = load_workspace()?;
    let mut config_path = metadata.workspace_root;
    config_path.push(".cargo/xtask.toml");
    let config = read_xtask_toml(&config_path)?;

    let mut command = cargo_command(CargoLocation::FromEnv);
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
                verify_executable_libraries(&config, &executable, &mut errors)?;
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
            errors.iter().for_each(|error| {
                swriteln!(msg, "\t{error}");
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
