// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands for reconfigurator chicken switches

use crate::Omdb;
use crate::check_allow_destructive::DestructiveOperationToken;
use clap::Args;
use clap::Subcommand;
use http::StatusCode;
use nexus_client::types::{
    ReconfiguratorChickenSwitches, ReconfiguratorChickenSwitchesParam,
};
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
    #[clap(long)]
    planner_enabled: bool,
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
            let ReconfiguratorChickenSwitches {
                version,
                planner_enabled,
                time_modified,
            } = switches.into_inner();
            println!("Reconfigurator Chicken Switches: ");
            println!("    version: {version}");
            println!("    modified time: {time_modified}");
            println!("    planner enabled: {planner_enabled}");
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
    let switches = match client
        .reconfigurator_chicken_switches_show_current()
        .await
    {
        Ok(switches) => {
            let Some(version) = switches.version.checked_add(1) else {
                eprintln!(
                    "ERROR: Failed to update chicken switches. Max version reached."
                );
                return Ok(());
            };
            let switches = switches.into_inner();
            // Future switches should use the following pattern, and only update
            // the current switch values if a setting changed.
            //
            // We may want to use `Options` in `args` to allow defaulting to
            // the current setting rather than forcing the user to update all
            // settings if the number of switches grows significantly. However,
            // this will not play nice with the `NOT_FOUND` case below.
            let mut modified = false;
            if args.planner_enabled != switches.planner_enabled {
                modified = true;
            }
            if modified {
                ReconfiguratorChickenSwitchesParam {
                    version,
                    planner_enabled: args.planner_enabled,
                }
            } else {
                println!("No modifications made to current switch values");
                return Ok(());
            }
        }
        Err(err) => {
            if err.status() == Some(StatusCode::NOT_FOUND) {
                ReconfiguratorChickenSwitchesParam {
                    version: 1,
                    planner_enabled: args.planner_enabled,
                }
            } else {
                eprintln!("error: {:#}", err);
                return Ok(());
            }
        }
    };

    client.reconfigurator_chicken_switches_set(&switches).await?;
    println!("Chicken switches updated at version {}", switches.version);

    Ok(())
}
