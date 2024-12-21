// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{bail, Result};
use camino::Utf8Path;
use clap::Args;
use omicron_zone_package::{
    config::{Config as PackageConfig, PackageMap, PackageName},
    package::PackageSource,
    target::TargetMap,
};
use slog::{debug, Logger};
use std::{collections::BTreeMap, io::Write, str::FromStr, time::Duration};

use crate::target::{target_command_help, KnownTarget};

#[derive(Debug, Args)]
pub struct ConfigArgs {
    /// The name of the build target to use for this command
    #[clap(short, long)]
    pub target: Option<String>,

    /// Skip confirmation prompt for destructive operations
    #[clap(short, long, action, default_value_t = false)]
    pub force: bool,

    /// Number of retries to use when re-attempting failed package downloads
    #[clap(long, action, default_value_t = 10)]
    pub retry_count: usize,

    /// Duration, in ms, to wait before re-attempting failed package downloads
    #[clap(
        long,
        action,
        value_parser = parse_duration_ms,
        default_value = "1000",
    )]
    pub retry_duration: Duration,
}

fn parse_duration_ms(arg: &str) -> Result<std::time::Duration> {
    let ms = arg.parse()?;
    Ok(Duration::from_millis(ms))
}

#[derive(Debug)]
pub struct Config {
    log: Logger,
    // Description of all possible packages.
    package_config: PackageConfig,
    // Description of the target we're trying to operate on.
    target: TargetMap,
    // The list of packages the user wants us to build (all, if empty)
    only: Vec<PackageName>,
    // True if we should skip confirmations for destructive operations.
    force: bool,
    // Number of times to retry failed downloads.
    retry_count: usize,
    // Duration to wait before retrying failed downloads.
    retry_duration: Duration,
}

impl Config {
    /// The name reserved for the currently-in-use build target.
    pub const ACTIVE: &str = "active";

    /// Builds a new configuration.
    pub fn get_config(
        log: &Logger,
        package_config: PackageConfig,
        args: &ConfigArgs,
        artifact_dir: &Utf8Path,
    ) -> Result<Self> {
        // Within this path, the target is expected to be set.
        let target = args.target.as_deref().unwrap_or(Self::ACTIVE);

        let target_help_str = || -> String {
            format!(
                "Try calling: '{} target create' to create a new build target",
                target_command_help("default"),
            )
        };

        let target_path = artifact_dir.join("target").join(target);
        let raw_target =
            std::fs::read_to_string(&target_path).inspect_err(|_| {
                eprintln!(
                    "Failed to read build target: {}\n{}",
                    target_path,
                    target_help_str()
                );
            })?;
        let target: TargetMap = KnownTarget::from_str(&raw_target)
            .inspect_err(|_| {
                eprintln!(
                    "Failed to parse {} as target\n{}",
                    target_path,
                    target_help_str()
                );
            })?
            .into();
        debug!(log, "target[{}]: {:?}", target, target);

        Ok(Config {
            log: log.clone(),
            package_config,
            target,
            only: Vec::new(),
            force: args.force,
            retry_count: args.retry_count,
            retry_duration: args.retry_duration,
        })
    }

    /// Sets the `only` field.
    #[inline]
    pub fn set_only(&mut self, only: Vec<PackageName>) -> &mut Self {
        self.only = only;
        self
    }

    /// Returns the logger.
    #[inline]
    pub fn log(&self) -> &Logger {
        &self.log
    }

    /// Returns the target currently being operated on.
    #[inline]
    pub fn target(&self) -> &TargetMap {
        &self.target
    }

    /// Returns the underlying package configuration.
    #[inline]
    pub fn package_config(&self) -> &PackageConfig {
        &self.package_config
    }

    /// Returns the retry count.
    #[inline]
    pub fn retry_count(&self) -> usize {
        self.retry_count
    }

    /// Returns the retry duration.
    #[inline]
    pub fn retry_duration(&self) -> Duration {
        self.retry_duration
    }

    /// Prompts the user for input before proceeding with an operation.
    pub fn confirm(&self, prompt: &str) -> Result<()> {
        if self.force {
            return Ok(());
        }

        print!("{prompt}\n[yY to confirm] >> ");
        let _ = std::io::stdout().flush();

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        match input.as_str().trim() {
            "y" | "Y" => Ok(()),
            _ => bail!("Aborting"),
        }
    }

    /// Returns target packages to be assembled on the builder machine, limited
    /// to those specified in `only` (if set).
    pub fn packages_to_build(&self) -> PackageMap<'_> {
        let packages = self.package_config.packages_to_build(&self.target);
        if self.only.is_empty() {
            return packages;
        }

        let mut filtered_packages = PackageMap(BTreeMap::new());
        let mut to_walk = PackageMap(BTreeMap::new());
        // add the requested packages to `to_walk`
        for package_name in &self.only {
            to_walk.0.insert(
                package_name,
                packages.0.get(package_name).unwrap_or_else(|| {
                    panic!(
                        "Explicitly-requested package '{}' does not exist",
                        package_name
                    )
                }),
            );
        }
        // dependencies are listed by output name, so create a lookup table to
        // get a package by its output name.
        let lookup_by_output = packages
            .0
            .iter()
            .map(|(name, package)| {
                (package.get_output_file(name), (*name, *package))
            })
            .collect::<BTreeMap<_, _>>();
        // packages yet to be walked are added to `to_walk`. pop each entry and
        // add its dependencies to `to_walk`, then add the package we finished
        // walking to `filtered_packages`.
        while let Some((package_name, package)) = to_walk.0.pop_first() {
            if let PackageSource::Composite { packages } = &package.source {
                for output in packages {
                    // find the package by output name
                    let (dep_name, dep_package) =
                        lookup_by_output.get(output).unwrap_or_else(|| {
                            panic!(
                                "Could not find a package which creates '{}'",
                                output
                            )
                        });
                    if *dep_name == package_name {
                        panic!("'{}' depends on itself", package_name);
                    }
                    // if we've seen this package already, it will be in
                    // `filtered_packages`. otherwise, add it to `to_walk`.
                    if !filtered_packages.0.contains_key(dep_name) {
                        to_walk.0.insert(dep_name, dep_package);
                    }
                }
            }
            // we're done looking at this package's deps
            filtered_packages.0.insert(package_name, package);
        }
        filtered_packages
    }

    /// Return a list of all possible Cargo features that could be requested for
    /// the packages being built.
    ///
    /// Out of these, the features that actually get requested are determined by
    /// which features are available for the list of packages being built.
    pub fn cargo_features(&self) -> Vec<String> {
        self.target
            .0
            .iter()
            .map(|(name, value)| format!("{name}-{value}"))
            .collect::<Vec<_>>()
    }
}
