// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Workspace-related developer tools
//!
//! See <https://github.com/matklad/cargo-xtask>.

use anyhow::{bail, Context, Result};
use camino::Utf8Path;
use cargo_metadata::Metadata;
use cargo_toml::{Dependency, Manifest};
use clap::{Parser, Subcommand};
use std::{collections::BTreeMap, process::Command};

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
    Clippy,
}

fn main() -> Result<()> {
    let args = Args::parse();
    match args.cmd {
        Cmds::Clippy => cmd_clippy(),
        Cmds::CheckWorkspaceDeps => cmd_check_workspace_deps(),
    }
}

fn cmd_clippy() -> Result<()> {
    let cargo =
        std::env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));
    let mut command = Command::new(&cargo);
    command
        .arg("clippy")
        // Make sure we check everything.
        .arg("--all-targets")
        .arg("--")
        // We disallow warnings by default.
        .arg("--deny")
        .arg("warnings")
        // Clippy's style nits are useful, but not worth keeping in CI.  This
        // override belongs in src/lib.rs, and it is there, but that doesn't
        // reliably work due to rust-lang/rust-clippy#6610.
        .arg("--allow")
        .arg("clippy::style");

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

                if name == WORKSPACE_HACK_PACKAGE_NAME {
                    // Skip over workspace-hack because hakari doesn't yet support
                    // workspace deps: https://github.com/guppy-rs/guppy/issues/7
                    continue;
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

fn read_cargo_toml(path: &Utf8Path) -> Result<Manifest> {
    let bytes =
        std::fs::read(path).with_context(|| format!("read {:?}", path))?;
    Manifest::from_slice(&bytes).with_context(|| format!("parse {:?}", path))
}

fn load_workspace() -> Result<Metadata> {
    cargo_metadata::MetadataCommand::new()
        .exec()
        .context("loading cargo metadata")
}
