// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that query or update specific Sleds

use crate::Omdb;
use anyhow::Context;
use async_trait::async_trait;
use clap::Args;
use clap::Subcommand;

/// Arguments to the "omdb sled-agent" subcommand
#[derive(Debug, Args)]
pub struct SledAgentArgs {
    /// URL of the Nexus internal API
    #[clap(long, env("OMDB_NEXUS_URL"))]
    nexus_internal_url: Option<String>,

    /// URL of the Sled internal API
    #[clap(long, env("OMDB_SLED_AGENT_URL"))]
    sled_agent_url: Option<String>,

    #[command(subcommand)]
    command: SledAgentCommands,
}

/// Subcommands for the "omdb sled-agent" subcommand
#[derive(Debug, Subcommand)]
enum SledAgentCommands {
    /// print information about the sled inventory
    #[clap(subcommand)]
    Inventory(InventoryCommands),

    /// print information about zones
    #[clap(subcommand)]
    Zones(ZoneCommands),

    /// print information about zpools
    #[clap(subcommand)]
    Zpools(ZpoolCommands),
}

#[derive(Debug, Subcommand)]
enum InventoryCommands {
    /// Print basic information about the sled's inventory
    Show,
}

#[derive(Debug, Subcommand)]
enum ZoneCommands {
    /// Print list of all running control plane zones
    List,
}

#[derive(Debug, Subcommand)]
enum ZpoolCommands {
    /// Print list of all zpools managed by the sled agent
    List,
}

#[async_trait]
trait Command {
    // Emit the header, if requested
    fn emit_header(&self);

    // Execute the command, return the result
    async fn run(
        &self,
        client: &sled_agent_client::Client,
    ) -> Result<(), anyhow::Error>;
}

/// Runs `omdb sled-agent inventory show`
struct InventoryShow {}

#[async_trait]
impl Command for InventoryShow {
    fn emit_header(&self) {
        println!("{:<40} {:<30} {:<10}", "UUID", "ADDRESS", "ROLE");
    }

    async fn run(
        &self,
        client: &sled_agent_client::Client,
    ) -> Result<(), anyhow::Error> {
        let response =
            client.inventory().await.context("accessing inventory")?;
        let inventory = response.into_inner();

        let id = inventory.sled_id.to_string();
        let address = inventory.sled_agent_address;
        let role = match inventory.sled_role {
            sled_agent_client::types::SledRole::Scrimlet => "scrimlet",
            sled_agent_client::types::SledRole::Gimlet => "gimlet",
        };

        println!("{:<40} {:<30} {:<10}", id, address, role);

        Ok(())
    }
}

/// Runs `omdb sled-agent zones list`
struct ZonesList {}

#[async_trait]
impl Command for ZonesList {
    fn emit_header(&self) {
        println!("{:<40} {:<20}", "CLIENT", "ZONE");
    }

    async fn run(
        &self,
        client: &sled_agent_client::Client,
    ) -> Result<(), anyhow::Error> {
        let response = client.zones_list().await.context("listing zones")?;
        let zones = response.into_inner();
        let zones: Vec<_> = zones.into_iter().collect();

        if zones.is_empty() {
            println!("{:<40} {:<20}", client.baseurl(), "<none>");
        }
        for zone in &zones {
            println!("{:<40} {:<20?}", client.baseurl(), zone);
        }

        Ok(())
    }
}

/// Runs `omdb sled-agent zpools list`
struct ZpoolsList {}

#[async_trait]
impl Command for ZpoolsList {
    fn emit_header(&self) {
        println!("{:<40} {:<20}", "CLIENT", "ZPOOL");
    }

    async fn run(
        &self,
        client: &sled_agent_client::Client,
    ) -> Result<(), anyhow::Error> {
        let response = client.zpools_get().await.context("listing zpools")?;
        let zpools = response.into_inner();

        if zpools.is_empty() {
            println!("{:<40} {:<20}", client.baseurl(), "<none>");
        }
        for zpool in &zpools {
            println!("{:<40} {:<20?}", client.baseurl(), zpool);
        }

        Ok(())
    }
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
        let sled_agent_urls = if let Some(sled_agent_url) = &self.sled_agent_url
        {
            vec![sled_agent_url.clone()]
        } else {
            eprintln!("note: Sled URL not specified. Will ask Nexus for sleds");
            let nexus_client =
                omdb.get_nexus_client(&self.nexus_internal_url, log).await?;

            let mut sled_agent_urls = vec![];

            let limit = None;
            let mut marker: Option<String> = None;
            let sort = Some(nexus_client::types::IdSortMode::IdAscending);

            loop {
                let sleds = nexus_client
                    .sled_list(limit, marker.as_deref(), sort)
                    .await?
                    .into_inner();
                // Gather all the sleds observed in this query
                sled_agent_urls.extend(
                    sleds
                        .items
                        .into_iter()
                        .map(|sled| format!("http://{}", sled.address)),
                );

                // If there are additional sleds, get them too
                if sleds.next_page.is_some() {
                    marker = sleds.next_page;
                    continue;
                }
                break;
            }
            sled_agent_urls
        };
        let clients = sled_agent_urls
            .into_iter()
            .map(|url| sled_agent_client::Client::new(&url, log.clone()));

        let command: Box<dyn Command> = match &self.command {
            SledAgentCommands::Inventory(InventoryCommands::Show) => {
                Box::new(InventoryShow {})
            }
            SledAgentCommands::Zones(ZoneCommands::List) => {
                Box::new(ZonesList {})
            }
            SledAgentCommands::Zpools(ZpoolCommands::List) => {
                Box::new(ZpoolsList {})
            }
        };

        command.emit_header();

        // Try to issue commands to all viable sled agents
        // before returning any possible error.
        let mut err = None;
        for client in clients {
            if let Err(e) = command.run(&client).await {
                eprintln!(
                    "    <failed to access sled at {}>",
                    client.baseurl()
                );
                err = e.into();
            }
        }

        if let Some(err) = err {
            return Err(err);
        }
        Ok(())
    }
}
