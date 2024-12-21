// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use anyhow::bail;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use cargo_metadata::Metadata;
use omicron_zone_package::config::PackageMap;
use omicron_zone_package::config::PackageName;
use omicron_zone_package::package::PackageSource;
use slog::info;
use slog::Logger;
use tokio::process::Command;

/// For a configuration, build a plan: the set of packages, binaries, and
/// features to operate on in release and debug modes.
pub fn build_cargo_plan<'a>(
    metadata: &Metadata,
    package_map: PackageMap<'a>,
    features: &'a [String],
) -> Result<CargoPlan<'a>> {
    // Collect a map of all of the workspace packages
    let workspace_pkgs = metadata
        .packages
        .iter()
        .filter_map(|package| {
            metadata
                .workspace_members
                .contains(&package.id)
                .then_some((package.name.clone(), package))
        })
        .collect::<BTreeMap<_, _>>();

    let mut release = CargoTargets::new(BuildKind::Release);
    let mut debug = CargoTargets::new(BuildKind::Debug);

    for (name, pkg) in package_map.0 {
        // If this is a Rust package, `name` (the map key) is the name of the
        // corresponding Rust crate.
        if let PackageSource::Local { rust: Some(rust_pkg), .. } = &pkg.source {
            let plan = if rust_pkg.release { &mut release } else { &mut debug };
            // Add the package name to the plan
            plan.packages.insert(name);
            // Get the package metadata
            let metadata =
                workspace_pkgs.get(name.as_str()).with_context(|| {
                    format!("package '{name}' is not a workspace package")
                })?;
            // Add the binaries we want to build to the plan
            let bins = metadata
                .targets
                .iter()
                .filter_map(|target| target.is_bin().then_some(&target.name))
                .collect::<BTreeSet<_>>();
            for bin in &rust_pkg.binary_names {
                ensure!(
                    bins.contains(bin),
                    "bin target '{bin}' does not belong to package '{name}'"
                );
                plan.bins.insert(bin);
            }
            // Add all features we want to request to the plan
            plan.features.extend(
                features
                    .iter()
                    .filter(|feature| metadata.features.contains_key(*feature)),
            );
        }
    }

    Ok(CargoPlan { release, debug })
}

#[derive(Debug)]
pub struct CargoPlan<'a> {
    pub release: CargoTargets<'a>,
    pub debug: CargoTargets<'a>,
}

impl CargoPlan<'_> {
    pub async fn run(&self, command: &str, log: &Logger) -> Result<()> {
        self.release.run(command, log).await?;
        self.debug.run(command, log).await?;
        Ok(())
    }
}

/// A set of packages, binaries, and features to operate on.
#[derive(Debug)]
pub struct CargoTargets<'a> {
    pub kind: BuildKind,
    pub packages: BTreeSet<&'a PackageName>,
    pub bins: BTreeSet<&'a String>,
    pub features: BTreeSet<&'a String>,
}

impl CargoTargets<'_> {
    fn new(kind: BuildKind) -> Self {
        Self {
            kind,
            packages: BTreeSet::new(),
            bins: BTreeSet::new(),
            features: BTreeSet::new(),
        }
    }

    pub fn build_command(&self, command: &str) -> Option<Command> {
        if self.bins.is_empty() {
            return None;
        }

        let mut cmd = Command::new("cargo");
        // We rely on the rust-toolchain.toml file for toolchain information,
        // rather than specifying one within the packaging tool.
        cmd.arg(command);
        // We specify _both_ --package and --bin; --bin does not imply
        // --package, and without any --package options Cargo unifies features
        // across all workspace default members. See rust-lang/cargo#8157.
        for package in &self.packages {
            cmd.arg("--package").arg(package.as_str());
        }
        for bin in &self.bins {
            cmd.arg("--bin").arg(bin);
        }
        if !self.features.is_empty() {
            cmd.arg("--features").arg(self.features.iter().fold(
                String::new(),
                |mut acc, s| {
                    if !acc.is_empty() {
                        acc.push(' ');
                    }
                    acc.push_str(s);
                    acc
                },
            ));
        }
        match self.kind {
            BuildKind::Release => {
                cmd.arg("--release");
            }
            BuildKind::Debug => {}
        }

        Some(cmd)
    }

    pub async fn run(&self, command: &str, log: &Logger) -> Result<()> {
        let Some(mut cmd) = self.build_command(command) else {
            return Ok(());
        };

        info!(log, "running: {:?}", cmd.as_std());
        let status = cmd
            .status()
            .await
            .context(format!("Failed to run command: ({:?})", cmd))?;
        if !status.success() {
            bail!("Failed to build packages");
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuildKind {
    Release,
    Debug,
}
