// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that query oximeter

use anyhow::Context;
use clap::Args;
use clap::Subcommand;
use futures::TryStreamExt;
use oximeter_client::types::ProducerEndpoint;
use oximeter_client::Client;
use slog::Logger;
use std::net::SocketAddr;
use std::time::Duration;
use tabled::Table;
use tabled::Tabled;
use uuid::Uuid;

#[derive(Debug, Args)]
pub struct OximeterArgs {
    /// URL of the oximeter collector to query
    #[arg(long, env("OMDB_OXIMETER_URL"))]
    oximeter_url: String,

    #[command(subcommand)]
    command: OximeterCommands,
}

/// Subcommands that query oximeter collector state
#[derive(Debug, Subcommand)]
enum OximeterCommands {
    /// List the producers the collector is assigned to poll
    ListProducers,
}

impl OximeterArgs {
    fn client(&self, log: &Logger) -> Client {
        Client::new(
            &self.oximeter_url,
            log.new(slog::o!("component" => "oximeter-client")),
        )
    }

    pub async fn run_cmd(&self, log: &Logger) -> anyhow::Result<()> {
        let client = self.client(log);
        match self.command {
            OximeterCommands::ListProducers => {
                self.list_producers(client).await
            }
        }
    }

    async fn list_producers(&self, client: Client) -> anyhow::Result<()> {
        let info = client
            .collector_info()
            .await
            .context("failed to fetch collector info")?;
        let producers: Vec<Producer> = client
            .producers_list_stream(None)
            .map_ok(Producer::from)
            .try_collect()
            .await
            .context("failed to list producers")?;
        let table = Table::new(producers)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0))
            .to_string();
        println!("Collector ID: {}\n", info.id);
        let last_refresh = info
            .last_refresh
            .map(|r| r.to_string())
            .unwrap_or(String::from("Never"));
        println!("Last refresh: {}\n", last_refresh);
        println!("{table}");
        Ok(())
    }
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct Producer {
    id: Uuid,
    address: SocketAddr,
    base_route: String,
    interval: String,
}

impl From<ProducerEndpoint> for Producer {
    fn from(p: ProducerEndpoint) -> Self {
        let interval = Duration::new(p.interval.secs, p.interval.nanos);
        Self {
            id: p.id,
            address: p.address.parse().unwrap(),
            base_route: p.base_route,
            interval: humantime::format_duration(interval).to_string(),
        }
    }
}
