// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that query oximeter

use crate::Omdb;
use crate::helpers::CONNECTION_OPTIONS_HEADING;
use anyhow::Context;
use chrono::SecondsFormat;
use clap::Args;
use clap::Subcommand;
use futures::TryStreamExt;
use internal_dns_types::names::ServiceName;
use oximeter_client::Client;
use oximeter_client::types::FailedCollection;
use oximeter_client::types::ProducerDetails;
use oximeter_client::types::ProducerEndpoint;
use oximeter_client::types::SuccessfulCollection;
use slog::Logger;
use std::net::SocketAddr;
use std::time::Duration;
use tabled::Table;
use tabled::Tabled;
use uuid::Uuid;

/// Arguments for the oximeter subcommand.
#[derive(Debug, Args)]
pub struct OximeterArgs {
    /// URL of the oximeter collector to query
    #[arg(
        long,
        env = "OMDB_OXIMETER_URL",
        global = true,
        help_heading = CONNECTION_OPTIONS_HEADING,
    )]
    oximeter_url: Option<String>,

    #[command(subcommand)]
    command: OximeterCommands,
}

/// Subcommands that query oximeter collector state
#[derive(Debug, Subcommand)]
enum OximeterCommands {
    /// List the producers the collector is assigned to poll.
    ListProducers,
    /// Fetch details about a single assigned producer.
    ProducerDetails {
        /// The ID of the producer to fetch.
        producer_id: Uuid,
    },
}

impl OximeterArgs {
    async fn client(
        &self,
        omdb: &Omdb,
        log: &Logger,
    ) -> Result<Client, anyhow::Error> {
        let oximeter_url = match &self.oximeter_url {
            Some(cli_or_env_url) => cli_or_env_url.clone(),
            None => {
                eprintln!(
                    "note: Oximeter URL not specified.  Will pick one from DNS."
                );
                let addr = omdb
                    .dns_lookup_one(log.clone(), ServiceName::Oximeter)
                    .await?;
                format!("http://{}", addr)
            }
        };
        eprintln!("note: using Oximeter URL {}", &oximeter_url);

        let client = Client::new(
            &oximeter_url,
            log.new(slog::o!("component" => "oximeter-client")),
        );
        Ok(client)
    }

    /// Run the command.
    pub async fn run_cmd(
        &self,
        omdb: &Omdb,
        log: &Logger,
    ) -> anyhow::Result<()> {
        let client = self.client(omdb, log).await?;
        match self.command {
            OximeterCommands::ListProducers => {
                self.list_producers(client).await
            }
            OximeterCommands::ProducerDetails { producer_id } => {
                self.producer_details(client, producer_id).await
            }
        }
    }

    async fn producer_details(
        &self,
        client: Client,
        producer_id: Uuid,
    ) -> anyhow::Result<()> {
        let details = client
            .producer_details(&producer_id)
            .await
            .context("failed to fetch producer details")?
            .into_inner();
        print_producer_details(details);
        Ok(())
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
    interval: String,
}

impl From<ProducerEndpoint> for Producer {
    fn from(p: ProducerEndpoint) -> Self {
        Self {
            id: p.id,
            address: p.address.parse().unwrap(),
            interval: duration_to_humantime(&p.interval),
        }
    }
}

fn duration_to_humantime(d: &oximeter_client::types::Duration) -> String {
    let interval = Duration::new(d.secs, d.nanos);
    humantime::format_duration(interval).to_string()
}

const WIDTH: usize = 12;

fn print_producer_details(details: ProducerDetails) {
    println!();
    println!("{:>WIDTH$}: {}", "ID", details.id);
    println!("{:>WIDTH$}: {}", "Address", details.address);
    println!(
        "{:>WIDTH$}: {}",
        "Registered",
        details.registered.to_rfc3339_opts(SecondsFormat::Millis, true)
    );
    println!(
        "{:>WIDTH$}: {}",
        "Updated",
        details.updated.to_rfc3339_opts(SecondsFormat::Millis, true)
    );
    println!(
        "{:>WIDTH$}: {}",
        "Interval",
        duration_to_humantime(&details.interval)
    );
    println!("{:>WIDTH$}: {}", "Successes", details.n_collections);
    println!("{:>WIDTH$}: {}", "Failures", details.n_failures);
    println!();
    print_last_success(details.last_success.as_ref());
    println!();
    print_last_failure(details.last_failure.as_ref());
}

fn print_last_success(maybe_success: Option<&SuccessfulCollection>) {
    print!("{:>WIDTH$}: ", "Last success");
    match maybe_success {
        None => println!("None"),
        Some(success) => {
            println!();
            println!(
                "{:>WIDTH$}: {}",
                "Started at",
                success.started_at.to_rfc3339_opts(SecondsFormat::Millis, true)
            );
            println!(
                "{:>WIDTH$}: {:?}",
                "Queued for",
                Duration::new(
                    success.time_queued.secs,
                    success.time_queued.nanos
                )
            );
            println!(
                "{:>WIDTH$}: {:?}",
                "Duration",
                Duration::new(
                    success.time_collecting.secs,
                    success.time_collecting.nanos
                )
            );
            println!("{:>WIDTH$}: {}", "Samples", success.n_samples);
        }
    }
}

fn print_last_failure(maybe_failure: Option<&FailedCollection>) {
    print!("{:>WIDTH$}: ", "Last failure");
    match maybe_failure {
        None => println!("None"),
        Some(failure) => {
            println!();
            println!(
                "{:>WIDTH$}: {}",
                "Started at",
                failure.started_at.to_rfc3339_opts(SecondsFormat::Millis, true)
            );
            println!(
                "{:>WIDTH$}: {:?}",
                "Queued for",
                Duration::new(
                    failure.time_queued.secs,
                    failure.time_queued.nanos
                )
            );
            println!(
                "{:>WIDTH$}: {:?}",
                "Duration",
                Duration::new(
                    failure.time_collecting.secs,
                    failure.time_collecting.nanos
                )
            );
            println!("{:>WIDTH$}: {}", "Reason", failure.reason);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::print_producer_details;
    use chrono::Utc;
    use oximeter_client::types::FailedCollection;
    use oximeter_client::types::ProducerDetails;
    use oximeter_client::types::SuccessfulCollection;
    use std::time::Duration;
    use uuid::Uuid;

    #[test]
    fn test_print_producer_details_success_only() {
        let now = Utc::now();
        let details = ProducerDetails {
            id: Uuid::new_v4(),
            address: "[::1]:12345".parse().unwrap(),
            interval: Duration::from_secs(10).into(),
            last_success: Some(SuccessfulCollection {
                n_samples: 100,
                started_at: now,
                time_collecting: Duration::from_millis(100).into(),
                time_queued: Duration::from_millis(10).into(),
            }),
            last_failure: None,
            n_collections: 1,
            n_failures: 0,
            registered: now,
            updated: now,
        };
        print_producer_details(details);
    }

    #[test]
    fn test_print_producer_details_with_failure() {
        let now = Utc::now();
        let details = ProducerDetails {
            id: Uuid::new_v4(),
            interval: Duration::from_secs(10).into(),
            address: "[::1]:12345".parse().unwrap(),
            last_success: Some(SuccessfulCollection {
                n_samples: 100,
                started_at: now,
                time_collecting: Duration::from_millis(100).into(),
                time_queued: Duration::from_millis(10).into(),
            }),
            last_failure: Some(FailedCollection {
                started_at: now,
                time_collecting: Duration::from_millis(100).into(),
                time_queued: Duration::from_millis(10).into(),
                reason: String::from("unreachable"),
            }),
            n_collections: 1,
            n_failures: 1,
            registered: now,
            updated: now,
        };
        print_producer_details(details);
    }
}
