// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that query or update specific Sleds

use crate::Omdb;
use crate::check_allow_destructive::DestructiveOperationToken;
use crate::helpers::CONNECTION_OPTIONS_HEADING;
use anyhow::Context;
use anyhow::bail;
use clap::Args;
use clap::Subcommand;
use sled_agent_client::types::ChickenSwitchDestroyOrphanedDatasets;

/// Arguments to the "omdb sled-agent" subcommand
#[derive(Debug, Args)]
pub struct SledAgentArgs {
    /// URL of the Sled internal API
    #[clap(
        long,
        env = "OMDB_SLED_AGENT_URL",
        global = true,
        help_heading = CONNECTION_OPTIONS_HEADING,
    )]
    sled_agent_url: Option<String>,

    #[command(subcommand)]
    command: SledAgentCommands,
}

/// Subcommands for the "omdb sled-agent" subcommand
#[derive(Debug, Subcommand)]
enum SledAgentCommands {
    /// print information about zones
    #[clap(subcommand)]
    Zones(ZoneCommands),

    /// print information about the local bootstore node
    #[clap(subcommand)]
    Bootstore(BootstoreCommands),

    /// control "chicken switches" (potentially-destructive sled-agent behavior
    /// that can be toggled on or off via `omdb`)
    #[clap(subcommand)]
    ChickenSwitch(ChickenSwitchCommands),
}

#[derive(Debug, Subcommand)]
enum ZoneCommands {
    /// Print list of all running control plane zones
    List,
}

#[derive(Debug, Subcommand)]
enum BootstoreCommands {
    /// Show the internal state of the local bootstore node
    Status,

    /// Read the network config
    ReadNetworkConfigCache,
    WriteNetworkConfig,
}

#[derive(Debug, Subcommand)]
enum ChickenSwitchCommands {
    /// interact with the "destroy orphaned datasets" chicken switch
    DestroyOrphans(DestroyOrphansArgs),
}

#[derive(Debug, Args)]
struct DestroyOrphansArgs {
    #[command(subcommand)]
    command: DestroyOrphansCommands,
}

#[derive(Debug, Subcommand)]
enum DestroyOrphansCommands {
    /// Get the current chicken switch setting
    Get,
    /// Enable the current chicken switch setting
    Enable,
    /// Disable the current chicken switch setting
    Disable,
}

impl SledAgentArgs {
    /// Run a `omdb sled-agent` subcommand.
    pub(crate) async fn run_cmd(
        &self,
        omdb: &Omdb,
        log: &slog::Logger,
    ) -> Result<(), anyhow::Error> {
        // This is a little goofy. The sled URL is required, but can come
        // from the environment, in which case it won't be on the command line.
        let Some(sled_agent_url) = &self.sled_agent_url else {
            bail!(
                "sled URL must be specified with --sled-agent-url or \
                OMDB_SLED_AGENT_URL"
            );
        };
        let client =
            sled_agent_client::Client::new(sled_agent_url, log.clone());

        match &self.command {
            SledAgentCommands::Zones(ZoneCommands::List) => {
                cmd_zones_list(&client).await
            }
            SledAgentCommands::Bootstore(BootstoreCommands::Status) => {
                cmd_bootstore_status(&client).await
            }
            SledAgentCommands::Bootstore(
                BootstoreCommands::ReadNetworkConfigCache,
            ) => cmd_read_network_config_cache(&client).await,
            SledAgentCommands::Bootstore(
                BootstoreCommands::WriteNetworkConfig,
            ) => cmd_write_network_config(&client).await,
            SledAgentCommands::ChickenSwitch(
                ChickenSwitchCommands::DestroyOrphans(DestroyOrphansArgs {
                    command: DestroyOrphansCommands::Get,
                }),
            ) => cmd_chicken_switch_destroy_orphans_get(&client).await,
            SledAgentCommands::ChickenSwitch(
                ChickenSwitchCommands::DestroyOrphans(DestroyOrphansArgs {
                    command: DestroyOrphansCommands::Enable,
                }),
            ) => {
                let token = omdb.check_allow_destructive()?;
                cmd_chicken_switch_destroy_orphans_set(&client, true, token)
                    .await
            }
            SledAgentCommands::ChickenSwitch(
                ChickenSwitchCommands::DestroyOrphans(DestroyOrphansArgs {
                    command: DestroyOrphansCommands::Disable,
                }),
            ) => {
                let token = omdb.check_allow_destructive()?;
                cmd_chicken_switch_destroy_orphans_set(&client, false, token)
                    .await
            }
        }
    }
}

/// Runs `omdb sled-agent zones list`
async fn cmd_zones_list(
    client: &sled_agent_client::Client,
) -> Result<(), anyhow::Error> {
    let response = client.zones_list().await.context("listing zones")?;
    let zones = response.into_inner();
    let zones: Vec<_> = zones.into_iter().collect();

    println!("zones:");
    if zones.is_empty() {
        println!("    <none>");
    }
    for zone in &zones {
        println!("    {:?}", zone);
    }

    Ok(())
}

/// Runs `omdb sled-agent bootstore read-network-config-cache`
async fn cmd_read_network_config_cache(
    client: &sled_agent_client::Client,
) -> Result<(), anyhow::Error> {
    let config_cache = client
        .read_network_bootstore_config_cache()
        .await
        .context("bootstore network config")?;

    let out = serde_json::json!(*config_cache);

    println!("{out}");

    Ok(())
}

/// Runs `omdb sled-agent bootstore read-network-config-cache`
async fn cmd_write_network_config(
    client: &sled_agent_client::Client,
) -> Result<(), anyhow::Error> {
    let config = sled_agent_client::types::EarlyNetworkConfig {
        body: todo!(),
        generation: todo!(),
        schema_version: todo!(),
    };
    let config_cache = client
        .write_network_bootstore_config(&config)
        .await
        .context("bootstore network config")?;

    println!("{config_cache:#?}");

    Ok(())
}

/// Runs `omdb sled-agent bootstore status`
async fn cmd_bootstore_status(
    client: &sled_agent_client::Client,
) -> Result<(), anyhow::Error> {
    let status = client.bootstore_status().await.context("bootstore status")?;
    let config_cache = client
        .read_network_bootstore_config_cache()
        .await
        .context("bootstore network config")?;
    println!("fsm ledger generation: {}", status.fsm_ledger_generation);
    println!(
        "network config ledger generation: {:?}",
        status.network_config_ledger_generation
    );
    println!("fsm state: {}", status.fsm_state);
    println!("peers (found by ddmd):");
    if status.peers.is_empty() {
        println!("    <none>");
    }
    for peer in status.peers.iter() {
        println!("    {peer}");
    }
    println!("established connections:");
    if status.established_connections.is_empty() {
        println!("    <none>");
    }
    for c in status.established_connections.iter() {
        println!("     {:?} : {}", c.baseboard, c.addr);
    }
    println!("accepted connections:");
    if status.accepted_connections.is_empty() {
        println!("    <none>");
    }
    for addr in status.accepted_connections.iter() {
        println!("    {addr}");
    }
    println!("negotiating connections:");
    if status.negotiating_connections.is_empty() {
        println!("    <none>");
    }
    for addr in status.negotiating_connections.iter() {
        println!("    {addr}");
    }

    Ok(())
}

/// Runs `omdb sled-agent chicken-switch destroy-orphans get`
async fn cmd_chicken_switch_destroy_orphans_get(
    client: &sled_agent_client::Client,
) -> Result<(), anyhow::Error> {
    let ChickenSwitchDestroyOrphanedDatasets { destroy_orphans } = client
        .chicken_switch_destroy_orphaned_datasets_get()
        .await
        .context("get chicken switch value")?
        .into_inner();
    let status = if destroy_orphans { "enabled" } else { "disabled" };
    println!("destroy orphaned datasets {status}");
    Ok(())
}

/// Runs `omdb sled-agent chicken-switch destroy-orphans {enable,disable}`
async fn cmd_chicken_switch_destroy_orphans_set(
    client: &sled_agent_client::Client,
    destroy_orphans: bool,
    _token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    let options = ChickenSwitchDestroyOrphanedDatasets { destroy_orphans };
    client
        .chicken_switch_destroy_orphaned_datasets_put(&options)
        .await
        .context("put chicken switch value")?;
    let status = if destroy_orphans { "enabled" } else { "disabled" };
    println!("destroy orphaned datasets {status}");
    Ok(())
}
