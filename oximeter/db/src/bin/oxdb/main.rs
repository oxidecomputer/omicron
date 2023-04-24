// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tool for developing against the Oximeter timeseries database, populating data and querying.

// Copyright 2023 Oxide Computer Company

use anyhow::bail;
use anyhow::Context;
use chrono::DateTime;
use chrono::Utc;
use clap::Args;
use clap::Parser;
use oximeter_db::query::Timestamp;
use oximeter_db::Client;
use oximeter_db::DbWrite;
use slog::o;
use slog::Drain;
use slog::Level;
use slog::Logger;
use std::net::IpAddr;
use std::net::SocketAddr;

mod dashboard;
mod query;
mod sim;

fn level_from_str(s: &str) -> Result<Level, anyhow::Error> {
    if let Ok(level) = s.parse() {
        Ok(level)
    } else {
        bail!(format!("Invalid log level: {}", s))
    }
}

/// Tools for developing with the Oximeter timeseries database.
#[derive(Debug, Parser)]
struct OxDb {
    /// IP address at which to connect to the database
    #[clap(short, long, default_value = "::1")]
    address: IpAddr,

    /// Port on which to connect to the database
    #[clap(short, long, default_value = "8123", action)]
    port: u16,

    /// Logging level
    #[clap(short, long, default_value = "info", value_parser = level_from_str)]
    log_level: Level,

    #[clap(subcommand)]
    cmd: Subcommand,
}

/// Arguments used to populate the database with simulated data.
#[derive(Debug, Args)]
pub struct PopulateArgs {
    /// The number of samples to generate, per timeseries
    #[clap(short = 'n', long, default_value = "100", action)]
    n_samples: usize,

    /// Number of projects to simulate
    #[clap(short = 'p', long, default_value = "2", action)]
    n_projects: usize,

    /// Number of VM instances to simulate, _per project_
    #[clap(short = 'i', long, default_value = "2", action)]
    n_instances: usize,

    /// Number of vCPUs to simulate, per instance.
    #[clap(short = 'c', long, default_value = "4", action)]
    n_cpus: usize,

    /// If true, generate data and report logs, but do not actually insert anything into the
    /// database.
    #[clap(short, long, action)]
    dry_run: bool,
}

// The main `oxdb` subcommand.
#[derive(Debug, clap::Subcommand)]
enum Subcommand {
    /// Populate the database with test data
    ///
    /// This simulates CPU time data from one or more virtual machine instances. Use the `describe`
    /// subcommand to describe the schema of each timeseries.
    Populate {
        #[clap(flatten)]
        populate_args: PopulateArgs,
    },

    /// Describe the schema of the simulated data
    Describe,

    /// Wipe the database and any data in it. CAREFUL.
    Wipe,

    /// Run a query against the database, assuming it is populated with data.
    Query {
        #[clap(flatten)]
        query: QueryArgs,
    },

    /// List the schema for all available timeseries.
    ListSchema,

    /// Inspect timeseries in a dashboard.
    #[clap(visible_alias = "dash")]
    Dashboard {
        #[clap(flatten)]
        query: QueryArgs,
    },
}

/// Arguments for querying the timeseries database.
#[derive(Clone, Debug, Args)]
pub struct QueryArgs {
    /// The name of the timeseries to search for.
    #[clap(action)]
    timeseries_name: String,

    /// Filters applied to the timeseries's fields.
    #[clap(required = true, num_args(1..), action)]
    filters: Vec<String>,

    /// The start time to which the search is constrained, inclusive.
    #[clap(long, action)]
    start: Option<DateTime<Utc>>,

    /// The start time to which the search is constrained, exclusive.
    #[clap(long, conflicts_with("start"), action)]
    start_exclusive: Option<DateTime<Utc>>,

    /// The stop time to which the search is constrained, inclusive.
    #[clap(long, action)]
    end: Option<DateTime<Utc>>,

    /// The start time to which the search is constrained, exclusive.
    #[clap(long, conflicts_with("end"), action)]
    end_exclusive: Option<DateTime<Utc>>,
}

/// Create a client to the timeseries database, and initialize the DB.
///
/// This will fail if the ClickHouse server cannot be reached or if the
/// initialization fails.
pub async fn make_client(
    address: IpAddr,
    port: u16,
    log: &Logger,
) -> Result<Client, anyhow::Error> {
    let address = SocketAddr::new(address, port);
    let client = Client::new(address, &log);
    client
        .init_db()
        .await
        .context("Failed to initialize timeseries database")?;
    Ok(client)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = OxDb::parse();
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator)
        .build()
        .filter_level(args.log_level)
        .fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = Logger::root(drain, o!("component" => "oxdb"));
    match args.cmd {
        Subcommand::Describe => {
            sim::describe_data();
            Ok(())
        }
        Subcommand::Populate { populate_args } => {
            let client = make_client(args.address, args.port, &log).await?;
            sim::populate(&client, &log, populate_args).await
        }
        Subcommand::Wipe => {
            let client = make_client(args.address, args.port, &log).await?;
            client.wipe_db().await.context("Failed to wipe database")
        }
        Subcommand::ListSchema => {
            let client = make_client(args.address, args.port, &log).await?;
            query::list_schema(&client).await
        }
        Subcommand::Query { query } => {
            let client = make_client(args.address, args.port, &log).await?;
            let start = match (query.start, query.start_exclusive) {
                (Some(start), _) => Some(Timestamp::Inclusive(start)),
                (_, Some(start)) => Some(Timestamp::Exclusive(start)),
                (None, None) => None,
            };
            let end = match (query.end, query.end_exclusive) {
                (Some(end), _) => Some(Timestamp::Inclusive(end)),
                (_, Some(end)) => Some(Timestamp::Exclusive(end)),
                (None, None) => None,
            };
            query::run_query(
                &client,
                query.timeseries_name,
                query.filters,
                start,
                end,
            )
            .await
        }
        Subcommand::Dashboard { query } => {
            let client = make_client(args.address, args.port, &log).await?;
            dashboard::run(&client, &log, &query).await
        }
    }
}
