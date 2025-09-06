// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands for reconfigurator configuration

use crate::Omdb;
use crate::check_allow_destructive::DestructiveOperationToken;
use clap::ArgAction;
use clap::Args;
use clap::Subcommand;
use daft::Diffable;
use http::StatusCode;
use indent_write::io::IndentWriter;
use nexus_types::deployment::PlannerConfig;
use nexus_types::deployment::ReconfiguratorConfig;
use nexus_types::deployment::ReconfiguratorConfigParam;
use std::io;
use std::io::Write;
use std::num::ParseIntError;
use std::str::FromStr;

#[derive(Debug, Args)]
pub struct ReconfiguratorConfigArgs {
    #[command(subcommand)]
    command: ReconfiguratorConfigCommands,
}

#[derive(Debug, Subcommand)]
pub enum ReconfiguratorConfigCommands {
    /// Show a configuration at a given version
    Show(ReconfiguratorConfigShowArgs),

    /// Set the value of all config options for the latest version
    /// Values carry over from the latest version if unset on the CLI.
    Set(ReconfiguratorConfigSetArgs),
}

#[derive(Debug, Clone, Args)]
pub struct ReconfiguratorConfigSetArgs {
    #[clap(flatten)]
    config: ReconfiguratorConfigOpts,
}

// Define the options separately so we can use `group(required = true, multiple
// = true).`
#[derive(Debug, Clone, Args)]
#[group(required = true, multiple = true)]
pub struct ReconfiguratorConfigOpts {
    #[clap(long, action = ArgAction::Set)]
    planner_enabled: Option<bool>,

    #[clap(long, action = ArgAction::Set)]
    add_zones_with_mupdate_override: Option<bool>,
}

impl ReconfiguratorConfigOpts {
    /// Returns an updated `ReconfiguratorConfigParam` regardless of
    /// whether any values were modified.
    fn update(&self, current: &ReconfiguratorConfig) -> ReconfiguratorConfig {
        ReconfiguratorConfig {
            planner_enabled: self
                .planner_enabled
                .unwrap_or(current.planner_enabled),
            planner_config: PlannerConfig {
                add_zones_with_mupdate_override: self
                    .add_zones_with_mupdate_override
                    .unwrap_or(
                        current.planner_config.add_zones_with_mupdate_override,
                    ),
            },
        }
    }

    /// Returns an updated `ReconfiguratorConfigParam` if any
    /// values were modified, or `None` if no changes were made.
    fn update_if_modified(
        &self,
        current: &ReconfiguratorConfig,
        next_version: u32,
    ) -> Option<ReconfiguratorConfigParam> {
        let new = self.update(current);
        (&new != current).then(|| ReconfiguratorConfigParam {
            version: next_version,
            config: new,
        })
    }
}

#[derive(Debug, Clone, Copy, Args)]
pub struct ReconfiguratorConfigShowArgs {
    version: ReconfiguratorConfigVersionOrCurrent,
}

#[derive(Debug, Clone, Copy)]
pub enum ReconfiguratorConfigVersionOrCurrent {
    Current,
    Version(u32),
}

impl FromStr for ReconfiguratorConfigVersionOrCurrent {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if matches!(s, "current" | "latest") {
            Ok(Self::Current)
        } else {
            let version = s.parse()?;
            Ok(Self::Version(version))
        }
    }
}

pub async fn cmd_nexus_reconfigurator_config(
    omdb: &Omdb,
    client: &nexus_client::Client,
    args: &ReconfiguratorConfigArgs,
) -> Result<(), anyhow::Error> {
    match &args.command {
        ReconfiguratorConfigCommands::Show(version) => {
            reconfigurator_config_show(&client, version).await
        }
        ReconfiguratorConfigCommands::Set(args) => {
            let token = omdb.check_allow_destructive()?;
            reconfigurator_config_set(&client, args, token).await
        }
    }
}
async fn reconfigurator_config_show(
    client: &nexus_client::Client,
    args: &ReconfiguratorConfigShowArgs,
) -> Result<(), anyhow::Error> {
    let res = match args.version {
        ReconfiguratorConfigVersionOrCurrent::Current => {
            client.reconfigurator_config_show_current().await
        }
        ReconfiguratorConfigVersionOrCurrent::Version(version) => {
            client.reconfigurator_config_show(version).await
        }
    };

    match res {
        Ok(config) => {
            println!("Reconfigurator config:");
            let stdout = io::stdout();
            let mut indented = IndentWriter::new("    ", stdout.lock());
            // No need for writeln! here because .display() adds its own
            // newlines.
            write!(indented, "{}", config.display()).unwrap();
        }
        Err(err) => {
            if err.status() == Some(StatusCode::NOT_FOUND) {
                println!("No config specified");
            } else {
                eprintln!("error: {:#}", err)
            }
        }
    }

    Ok(())
}

async fn reconfigurator_config_set(
    client: &nexus_client::Client,
    args: &ReconfiguratorConfigSetArgs,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    let (current_config, new_config) =
        match client.reconfigurator_config_show_current().await {
            Ok(config) => {
                let Some(next_version) = config.version.checked_add(1) else {
                    eprintln!(
                        "ERROR: Failed to update config. Max version reached."
                    );
                    return Ok(());
                };
                let config = config.into_inner();
                // Future fields should use the following pattern, and only update
                // the values if a setting changed.
                let Some(new_config) = args
                    .config
                    .update_if_modified(&config.config, next_version)
                else {
                    println!("no modifications made to current config values:");
                    let stdout = io::stdout();
                    let mut indented = IndentWriter::new("    ", stdout.lock());
                    // No need for writeln! here because .display() adds its own
                    // newlines.
                    write!(indented, "{}", config.display()).unwrap();
                    return Ok(());
                };
                (Some(config), new_config)
            }
            Err(err) => {
                if err.status() == Some(StatusCode::NOT_FOUND) {
                    let default_config = ReconfiguratorConfig::default();
                    // In this initial case, the operator expects that we always set
                    // a config.
                    let new_config = ReconfiguratorConfigParam {
                        version: 1,
                        config: args.config.update(&default_config),
                    };
                    (None, new_config)
                } else {
                    eprintln!("error: {:#}", err);
                    return Ok(());
                }
            }
        };

    client.reconfigurator_config_set(&new_config).await?;
    println!(
        "reconfigurator config updated to version {}:",
        new_config.version
    );
    match current_config {
        Some(current_config) => {
            // ReconfiguratorConfigDiffDisplay does its own
            // indentation, so more isn't required.
            print!(
                "{}",
                current_config.config.diff(&new_config.config).display(),
            );
        }
        None => {
            let stdout = io::stdout();
            let mut indented = IndentWriter::new("    ", stdout.lock());
            write!(indented, "{}", new_config.config.display()).unwrap();
        }
    }

    Ok(())
}
