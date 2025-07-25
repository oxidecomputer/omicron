// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands for reconfigurator chicken switches

use crate::Omdb;
use crate::check_allow_destructive::DestructiveOperationToken;
use clap::ArgAction;
use clap::Args;
use clap::Subcommand;
use daft::Diffable;
use http::StatusCode;
use indent_write::io::IndentWriter;
use nexus_types::deployment::PlannerChickenSwitches;
use nexus_types::deployment::ReconfiguratorChickenSwitches;
use nexus_types::deployment::ReconfiguratorChickenSwitchesParam;
use std::io;
use std::io::Write;
use std::num::ParseIntError;
use std::str::FromStr;

#[derive(Debug, Args)]
pub struct ChickenSwitchesArgs {
    #[command(subcommand)]
    command: ChickenSwitchesCommands,
}

#[derive(Debug, Subcommand)]
pub enum ChickenSwitchesCommands {
    /// Show a chicken switch at a given version
    Show(ChickenSwitchesShowArgs),

    /// Set the value of all chicken switches for the latest version
    /// Values carry over from the latest version if unset on the CLI.
    Set(ChickenSwitchesSetArgs),
}

#[derive(Debug, Clone, Args)]
pub struct ChickenSwitchesSetArgs {
    #[clap(flatten)]
    switches: ChickenSwitchesOpts,
}

// Define the switches separately so we can use `group(required = true, multiple
// = true).`
#[derive(Debug, Clone, Args)]
#[group(required = true, multiple = true)]
pub struct ChickenSwitchesOpts {
    #[clap(long, action = ArgAction::Set)]
    planner_enabled: Option<bool>,

    #[clap(long, action = ArgAction::Set)]
    add_zones_with_mupdate_override: Option<bool>,
}

impl ChickenSwitchesOpts {
    /// Returns an updated `ReconfiguratorChickenSwitchesParam` regardless of
    /// whether any switches were modified.
    fn update(
        &self,
        current: &ReconfiguratorChickenSwitches,
    ) -> ReconfiguratorChickenSwitches {
        ReconfiguratorChickenSwitches {
            planner_enabled: self
                .planner_enabled
                .unwrap_or(current.planner_enabled),
            planner_switches: PlannerChickenSwitches {
                add_zones_with_mupdate_override: self
                    .add_zones_with_mupdate_override
                    .unwrap_or(
                        current
                            .planner_switches
                            .add_zones_with_mupdate_override,
                    ),
            },
        }
    }

    /// Returns an updated `ReconfiguratorChickenSwitchesParam` if any
    /// switches were modified, or `None` if no changes were made.
    fn update_if_modified(
        &self,
        current: &ReconfiguratorChickenSwitches,
        next_version: u32,
    ) -> Option<ReconfiguratorChickenSwitchesParam> {
        let new = self.update(current);
        (&new != current).then(|| ReconfiguratorChickenSwitchesParam {
            version: next_version,
            switches: new,
        })
    }
}

#[derive(Debug, Clone, Copy, Args)]
pub struct ChickenSwitchesShowArgs {
    version: ChickenSwitchesVersionOrCurrent,
}

#[derive(Debug, Clone, Copy)]
pub enum ChickenSwitchesVersionOrCurrent {
    Current,
    Version(u32),
}

impl FromStr for ChickenSwitchesVersionOrCurrent {
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

pub async fn cmd_nexus_chicken_switches(
    omdb: &Omdb,
    client: &nexus_client::Client,
    args: &ChickenSwitchesArgs,
) -> Result<(), anyhow::Error> {
    match &args.command {
        ChickenSwitchesCommands::Show(version) => {
            chicken_switches_show(&client, version).await
        }
        ChickenSwitchesCommands::Set(args) => {
            let token = omdb.check_allow_destructive()?;
            chicken_switches_set(&client, args, token).await
        }
    }
}
async fn chicken_switches_show(
    client: &nexus_client::Client,
    args: &ChickenSwitchesShowArgs,
) -> Result<(), anyhow::Error> {
    let res = match args.version {
        ChickenSwitchesVersionOrCurrent::Current => {
            client.reconfigurator_chicken_switches_show_current().await
        }
        ChickenSwitchesVersionOrCurrent::Version(version) => {
            client.reconfigurator_chicken_switches_show(version).await
        }
    };

    match res {
        Ok(switches) => {
            println!("Reconfigurator chicken switches:");
            let stdout = io::stdout();
            let mut indented = IndentWriter::new("    ", stdout.lock());
            // No need for writeln! here because .display() adds its own
            // newlines.
            write!(indented, "{}", switches.display()).unwrap();
        }
        Err(err) => {
            if err.status() == Some(StatusCode::NOT_FOUND) {
                println!("No chicken switches enabled");
            } else {
                eprintln!("error: {:#}", err)
            }
        }
    }

    Ok(())
}

async fn chicken_switches_set(
    client: &nexus_client::Client,
    args: &ChickenSwitchesSetArgs,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    let (current_switches, new_switches) = match client
        .reconfigurator_chicken_switches_show_current()
        .await
    {
        Ok(switches) => {
            let Some(next_version) = switches.version.checked_add(1) else {
                eprintln!(
                    "ERROR: Failed to update chicken switches. Max version reached."
                );
                return Ok(());
            };
            let switches = switches.into_inner();
            // Future switches should use the following pattern, and only update
            // the current switch values if a setting changed.
            let Some(new_switches) = args
                .switches
                .update_if_modified(&switches.switches, next_version)
            else {
                println!("no modifications made to current switch values:");
                let stdout = io::stdout();
                let mut indented = IndentWriter::new("    ", stdout.lock());
                // No need for writeln! here because .display() adds its own
                // newlines.
                write!(indented, "{}", switches.display()).unwrap();
                return Ok(());
            };
            (Some(switches), new_switches)
        }
        Err(err) => {
            if err.status() == Some(StatusCode::NOT_FOUND) {
                let default_switches = ReconfiguratorChickenSwitches::default();
                // In this initial case, the operator expects that we always set
                // switches.
                let new_switches = ReconfiguratorChickenSwitchesParam {
                    version: 1,
                    switches: args.switches.update(&default_switches),
                };
                (None, new_switches)
            } else {
                eprintln!("error: {:#}", err);
                return Ok(());
            }
        }
    };

    client.reconfigurator_chicken_switches_set(&new_switches).await?;
    println!("chicken switches updated to version {}:", new_switches.version);
    match current_switches {
        Some(current_switches) => {
            // ReconfiguratorChickenSwitchesDiffDisplay does its own
            // indentation, so more isn't required.
            print!(
                "{}",
                current_switches
                    .switches
                    .diff(&new_switches.switches)
                    .display(),
            );
        }
        None => {
            let stdout = io::stdout();
            let mut indented = IndentWriter::new("    ", stdout.lock());
            write!(indented, "{}", new_switches.switches.display()).unwrap();
        }
    }

    Ok(())
}
