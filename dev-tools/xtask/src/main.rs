// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Workspace-related developer tools
//!
//! See <https://github.com/matklad/cargo-xtask>.

use anyhow::{bail, Context, Result};
use camino::Utf8Path;
use cargo_metadata::{Message, Metadata};
use cargo_toml::{Dependency, Manifest};
use clap::{Parser, Subcommand};
use fs_err as fs;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    io::BufReader,
    process::{Command, Stdio},
};

#[derive(Parser)]
#[command(name = "cargo xtask", about = "Workspace-related developer tools")]
struct Args {
    #[command(subcommand)]
    cmd: Cmds,
}

#[derive(Subcommand)]
enum Cmds {
    /// Check that dependencies are not duplicated in any packages in the
    /// workspace
    CheckWorkspaceDeps,
    /// Run configured clippy checks
    Clippy(ClippyArgs),
    /// Verify we are not leaking library bindings outside of intended
    /// crates
    VerifyLibraries,
}

#[derive(Parser)]
struct ClippyArgs {
    /// Automatically apply lint suggestions.
    #[clap(long)]
    fix: bool,
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

fn main() -> Result<()> {
    let args = Args::parse();
    match args.cmd {
        Cmds::Clippy(args) => cmd_clippy(args),
        Cmds::CheckWorkspaceDeps => cmd_check_workspace_deps(),
        Cmds::VerifyLibraries => cmd_verify_libraries(),
    }
}

fn cmd_clippy(args: ClippyArgs) -> Result<()> {
    let cargo =
        std::env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));
    let mut command = Command::new(&cargo);
    command.arg("clippy");

    if args.fix {
        command.arg("--fix");
    }

    command
        // Make sure we check everything.
        .arg("--all-targets")
        .arg("--")
        // For a list of lints, see
        // https://rust-lang.github.io/rust-clippy/master.
        //
        // We disallow warnings by default.
        .arg("--deny")
        .arg("warnings")
        // Clippy's style nits are useful, but not worth keeping in CI.  This
        // override belongs in src/lib.rs, and it is there, but that doesn't
        // reliably work due to rust-lang/rust-clippy#6610.
        .arg("--allow")
        .arg("clippy::style")
        // But continue to warn on anything in the "disallowed_" namespace.
        // (These will be turned into errors by `--deny warnings` above.)
        .arg("--warn")
        .arg("clippy::disallowed_macros")
        .arg("--warn")
        .arg("clippy::disallowed_methods")
        .arg("--warn")
        .arg("clippy::disallowed_names")
        .arg("--warn")
        .arg("clippy::disallowed_script_idents")
        .arg("--warn")
        .arg("clippy::disallowed_types");

    eprintln!(
        "running: {:?} {}",
        &cargo,
        command
            .get_args()
            .map(|arg| format!("{:?}", arg.to_str().unwrap()))
            .collect::<Vec<_>>()
            .join(" ")
    );

    let exit_status = command
        .spawn()
        .context("failed to spawn child process")?
        .wait()
        .context("failed to wait for child process")?;

    if !exit_status.success() {
        bail!("clippy failed: {}", exit_status);
    }

    Ok(())
}

const WORKSPACE_HACK_PACKAGE_NAME: &str = "omicron-workspace-hack";

fn cmd_check_workspace_deps() -> Result<()> {
    // Ignore issues with "pq-sys".  See the omicron-rpaths package for details.
    const EXCLUDED: &[&'static str] = &["pq-sys"];

    // Collect a list of all packages used in any workspace package as a
    // workspace dependency.
    let mut workspace_dependencies = BTreeMap::new();

    // Collect a list of all packages used in any workspace package as a
    // NON-workspace dependency.
    let mut non_workspace_dependencies = BTreeMap::new();

    // Load information about the Cargo workspace.
    let workspace = load_workspace()?;
    let mut nwarnings = 0;
    let mut nerrors = 0;

    // Iterate the workspace packages and fill out the maps above.
    for pkg_info in workspace.workspace_packages() {
        if pkg_info.name == WORKSPACE_HACK_PACKAGE_NAME {
            // Skip over workspace-hack because hakari doesn't yet support
            // workspace deps: https://github.com/guppy-rs/guppy/issues/7
            continue;
        }

        let manifest_path = &pkg_info.manifest_path;
        let manifest = read_cargo_toml(manifest_path)?;
        for tree in [
            &manifest.dependencies,
            &manifest.dev_dependencies,
            &manifest.build_dependencies,
        ] {
            for (name, dep) in tree {
                if let Dependency::Inherited(inherited) = dep {
                    if inherited.workspace {
                        workspace_dependencies
                            .entry(name.to_owned())
                            .or_insert_with(Vec::new)
                            .push(pkg_info.name.clone());

                        if !inherited.features.is_empty() {
                            eprintln!(
                                "warning: package is used as a workspace dep \
                                with extra features: {:?} (in {:?})",
                                name, pkg_info.name,
                            );
                            nwarnings += 1;
                        }

                        continue;
                    }
                }

                non_workspace_dependencies
                    .entry(name.to_owned())
                    .or_insert_with(Vec::new)
                    .push(pkg_info.name.clone());
            }
        }
    }

    // Look for any packages that are used as both a workspace dependency and a
    // non-workspace dependency.  Generally, the non-workspace dependency should
    // be replaced with a workspace dependency.
    for (pkgname, ws_examples) in &workspace_dependencies {
        if let Some(non_ws_examples) = non_workspace_dependencies.get(pkgname) {
            eprintln!(
                "error: package is used as both a workspace dep and a \
                non-workspace dep: {:?}",
                pkgname
            );
            eprintln!("      workspace dep: {}", ws_examples.join(", "));
            eprintln!("  non-workspace dep: {}", non_ws_examples.join(", "));
            nerrors += 1;
        }
    }

    // Look for any packages used as non-workspace dependencies by more than one
    // workspace package.  These should generally be moved to a workspace
    // dependency.
    for (pkgname, examples) in
        non_workspace_dependencies.iter().filter(|(pkgname, examples)| {
            examples.len() > 1 && !EXCLUDED.contains(&pkgname.as_str())
        })
    {
        eprintln!(
            "error: package is used by multiple workspace packages without \
            a workspace dependency: {:?}",
            pkgname
        );
        eprintln!("  used in: {}", examples.join(", "));
        nerrors += 1;
    }

    eprintln!(
        "check-workspace-deps: errors: {}, warnings: {}",
        nerrors, nwarnings
    );

    if nerrors != 0 {
        bail!("errors with workspace dependencies");
    }

    Ok(())
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

fn read_cargo_toml(path: &Utf8Path) -> Result<Manifest> {
    let bytes = fs::read(path)?;
    Manifest::from_slice(&bytes).with_context(|| format!("parse {:?}", path))
}

fn load_workspace() -> Result<Metadata> {
    cargo_metadata::MetadataCommand::new()
        .exec()
        .context("loading cargo metadata")
}

fn read_xtask_toml(path: &Utf8Path) -> Result<XtaskConfig> {
    let config_str = fs::read_to_string(path)?;
    toml::from_str(&config_str).with_context(|| format!("parse {:?}", path))
}
