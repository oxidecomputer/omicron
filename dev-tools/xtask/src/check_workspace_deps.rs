// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask check-workspace-deps

use anyhow::{Context, Result, bail};
use camino::Utf8Path;
use cargo_toml::{Dependency, Manifest};
use fs_err as fs;
use std::collections::{BTreeMap, BTreeSet};

const WORKSPACE_HACK_PACKAGE_NAME: &str = "omicron-workspace-hack";

pub fn run_cmd() -> Result<()> {
    // Ignore issues with "pq-sys".  See the omicron-rpaths package for details.
    const EXCLUDED: &[&'static str] = &["pq-sys"];

    // Collect a list of all packages used in any workspace package as a
    // workspace dependency.
    let mut workspace_dependencies = BTreeMap::new();

    // Collect a list of all packages used in any workspace package as a
    // NON-workspace dependency.
    let mut non_workspace_dependencies = BTreeMap::new();

    // Load information about the Cargo workspace.
    let workspace = crate::load_workspace()?;
    let mut nwarnings = 0;
    let mut nerrors = 0;

    // Iterate the workspace packages and fill out the maps above.
    for pkg_info in workspace.workspace_packages() {
        let manifest_path = &pkg_info.manifest_path;
        let manifest = read_cargo_toml(manifest_path)?;

        // Check that `[lints] workspace = true` is set.
        if !manifest.lints.map(|lints| lints.workspace).unwrap_or(false) {
            eprintln!(
                "error: package {:?} does not have `[lints] workspace = true` set",
                pkg_info.name
            );
            nerrors += 1;
        }

        if pkg_info.name.as_str() == WORKSPACE_HACK_PACKAGE_NAME {
            // Skip over workspace-hack because hakari doesn't yet support
            // workspace deps: https://github.com/guppy-rs/guppy/issues/7
            continue;
        }

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

    // Check that `default-members` is configured correctly.
    let non_default = workspace
        .packages
        .iter()
        .filter_map(|package| {
            [
                // Including xtask causes hakari to not work as well and build
                // times to be longer (omicron#4392).
                "xtask",
            ]
            .contains(&package.name.as_str())
            .then_some(&package.id)
        })
        .collect::<BTreeSet<_>>();
    let members = workspace.workspace_members.iter().collect::<BTreeSet<_>>();
    let default_members =
        workspace.workspace_default_members.iter().collect::<BTreeSet<_>>();
    for package in members.difference(&default_members) {
        if !non_default.contains(package) {
            eprintln!(
                "error: package {:?} not in default-members",
                package.repr
            );
            nerrors += 1;
        }
    }

    let mut seen_bins = BTreeSet::new();
    for package in &workspace.packages {
        if workspace.workspace_members.contains(&package.id) {
            for target in &package.targets {
                if target.is_bin() {
                    if !seen_bins.insert(&target.name) {
                        eprintln!(
                            "error: bin target {:?} seen multiple times",
                            target.name
                        );
                        nerrors += 1;
                    }
                }
            }
        }
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
    let bytes = fs::read(path)?;
    Manifest::from_slice(&bytes).with_context(|| format!("parse {:?}", path))
}
