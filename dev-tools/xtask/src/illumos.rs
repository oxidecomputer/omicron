use anyhow::{bail, Context, Result};
use camino::Utf8Path;
use cargo_metadata::Message;
use fs_err as fs;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    io::BufReader,
    process::{Command, Stdio},
};

use crate::load_workspace;

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
    errors: &mut HashMap<String, Vec<LibraryError>>,
) -> Result<()> {
    let binary = path.file_name().context("basename of executable")?;

    let command = Command::new("elfedit")
        .arg("-o")
        .arg("simple")
        .arg("-r")
        .arg("-e")
        .arg("dyn:tag NEEDED")
        .arg(&path)
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
fn cmd_verify_libraries() -> Result<()> {
    let metadata = load_workspace()?;
    let mut config_path = metadata.workspace_root;
    config_path.push(".cargo/xtask.toml");
    let config = read_xtask_toml(&config_path)?;

    let mut command = Command::new("cargo")
        .args(&["build", "--message-format=json-render-diagnostics"])
        .stdout(Stdio::piped())
        .spawn()
        .context("failed to spawn cargo build")?;

    let reader = BufReader::new(command.stdout.take().context("take stdout")?);

    let mut errors = Default::default();
    for message in cargo_metadata::Message::parse_stream(reader) {
        match message? {
            Message::CompilerArtifact(artifact) => {
                // We are only interested in artifacts that are binaries
                if let Some(executable) = artifact.executable {
                    verify_executable(&config, &executable, &mut errors)?;
                }
            }
            _ => (),
        }
    }

    let status = command.wait()?;
    if !status.success() {
        bail!("Failed to execute cargo build successfully {}", status);
    }

    if !errors.is_empty() {
        let mut msg = String::new();
        use std::fmt::Write;
        errors.iter().for_each(|(binary, errors)| {
            write!(msg, "{binary}\n").unwrap();
            errors.iter().for_each(|error| match error {
                LibraryError::Unexpected(lib) => {
                    write!(msg, "\tUNEXPECTED dependency on {lib}\n").unwrap()
                }
                LibraryError::NotAllowed(lib) => {
                    write!(msg, "\tNEEDS {lib} but is not allowed\n").unwrap()
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
