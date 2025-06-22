// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CLI-Tool for developing against the Oximeter timeseries database, populating
//! data and querying.

// Copyright 2024 Oxide Computer Company

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use clap::{Args, Parser};
use omicron_common::address::CLICKHOUSE_TCP_PORT;
use oximeter::{
    Metric, Target,
    types::{Cumulative, Sample},
};
use oximeter_db::{Client, DbWrite, make_client, query};
use slog::{Drain, Level, Logger, debug, info, o};
use std::net::IpAddr;
use uuid::Uuid;

/// Samples are inserted in chunks of this size, to avoid large allocations when inserting huge
/// numbers of timeseries.
const INSERT_CHUNK_SIZE: usize = 100_000;

/// A target identifying a single virtual machine instance
#[derive(Debug, Clone, Copy, Target)]
struct VirtualMachine {
    pub project_id: Uuid,
    pub instance_id: Uuid,
}

impl VirtualMachine {
    pub fn new() -> Self {
        Self { project_id: Uuid::new_v4(), instance_id: Uuid::new_v4() }
    }
}

/// A metric recording the total time a vCPU is busy, by its ID
#[derive(Debug, Clone, Copy, Metric)]
struct CpuBusy {
    pub cpu_id: i64,
    #[datum]
    pub busy: Cumulative<f64>,
}

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

    /// Port on which to connect to the database using the native TCP interface.
    #[clap(short, long, default_value_t = CLICKHOUSE_TCP_PORT)]
    port: u16,

    /// Logging level
    #[clap(short, long, default_value = "info", value_parser = level_from_str)]
    log_level: Level,

    #[clap(subcommand)]
    cmd: Subcommand,
}

#[derive(Debug, Args)]
struct PopulateArgs {
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
        /// The name of the timeseries to search for. (Currently only `virtual_machine:cpu_busy`
        /// makes any sense.)
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
    },

    /// Enter a SQL shell for interactive querying.
    #[cfg(feature = "sql")]
    Sql {
        #[clap(flatten)]
        opts: oximeter_db::shells::sql::ShellOptions,
    },

    /// Enter the Oximeter Query Language shell for interactive querying.
    #[cfg(feature = "oxql")]
    Oxql {
        #[clap(flatten)]
        opts: oximeter_db::shells::oxql::ShellOptions,
    },

    /// Start a native SQL shell to a ClickHouse server.
    #[cfg(feature = "native-sql-shell")]
    NativeSql,
}

fn describe_data() {
    let vm = VirtualMachine::new();
    print!("Target:\n\n Name: {target_name:?}\n", target_name = vm.name());
    for (i, (field_name, field_type)) in
        vm.field_names().into_iter().zip(vm.field_types()).enumerate()
    {
        print!(
            " Field {i}:\n   Name: {field_name:?}\n   Type: {field_type}\n",
            i = i,
            field_name = field_name,
            field_type = field_type
        );
    }

    let cpu = CpuBusy { cpu_id: 0, busy: 0.0f64.into() };
    print!(
        "\nMetric:\n\n Name: {metric_name:?}\n Type: {ty:?}\n",
        metric_name = cpu.name(),
        ty = cpu.datum_type()
    );
    for (i, (field_name, field_type)) in
        cpu.field_names().into_iter().zip(cpu.field_types()).enumerate()
    {
        print!(
            " Field {i}:\n   Name: {field_name:?}\n   Type: {field_type}\n",
            i = i,
            field_name = field_name,
            field_type = field_type
        );
    }
}

async fn insert_samples(
    client: &Client,
    samples: &Vec<Sample>,
    log: &Logger,
    dry_run: bool,
) -> Result<(), anyhow::Error> {
    debug!(
        log,
        "inserting {} simulated samples data into database",
        samples.len();
        "dry_run" => dry_run
    );
    if !dry_run {
        client
            .insert_samples(&samples)
            .await
            .context("Failed to insert samples")?;
    }
    Ok(())
}

async fn populate(
    address: IpAddr,
    port: u16,
    log: Logger,
    args: PopulateArgs,
) -> Result<(), anyhow::Error> {
    info!(log, "populating Oximeter database");
    let client = make_client(address, port, &log).await?;
    let n_timeseries = args.n_projects * args.n_instances * args.n_cpus;
    debug!(
        log,
        "generating simulated data";
        "n_projects" => args.n_projects,
        "n_instances" => args.n_instances,
        "n_cpus" => args.n_cpus,
        "n_samples" => args.n_samples,
        "n_timeseries" => n_timeseries,
    );

    let chunk_size = (args.n_samples * n_timeseries).min(INSERT_CHUNK_SIZE);
    let mut samples = Vec::with_capacity(chunk_size);
    for _ in 0..args.n_projects {
        let project_id = Uuid::new_v4();
        for _ in 0..args.n_instances {
            let vm = VirtualMachine { project_id, instance_id: Uuid::new_v4() };
            info!(
                log,
                "simulating vm";
                "project_id" => %vm.project_id,
                "instance_id" => %vm.instance_id
            );
            for cpu in 0..args.n_cpus {
                for sample in 0..args.n_samples {
                    let cpu_busy = CpuBusy {
                        cpu_id: cpu as _,
                        busy: Cumulative::from(sample as f64),
                    };
                    let sample = Sample::new(&vm, &cpu_busy)?;
                    samples.push(sample);
                    if samples.len() == chunk_size {
                        insert_samples(&client, &samples, &log, args.dry_run)
                            .await?;
                        samples.clear();
                    }
                }
            }
        }
    }
    if !samples.is_empty() {
        insert_samples(&client, &samples, &log, args.dry_run).await?;
    }
    Ok(())
}

async fn wipe_single_node_db(
    address: IpAddr,
    port: u16,
    log: Logger,
) -> Result<(), anyhow::Error> {
    let client = make_client(address, port, &log).await?;
    client.wipe_single_node_db().await.context("Failed to wipe database")
}

#[allow(clippy::too_many_arguments)]
async fn query(
    address: IpAddr,
    port: u16,
    log: Logger,
    timeseries_name: String,
    filters: Vec<String>,
    start: Option<query::Timestamp>,
    end: Option<query::Timestamp>,
) -> Result<(), anyhow::Error> {
    let client = make_client(address, port, &log).await?;
    let filters = filters.iter().map(|s| s.as_str()).collect::<Vec<_>>();
    let timeseries = client
        .select_timeseries_with(
            &timeseries_name,
            filters.as_slice(),
            start,
            end,
            None,
            None,
        )
        .await?;
    println!("{}", serde_json::to_string(&timeseries).unwrap());
    Ok(())
}

fn main() -> anyhow::Result<()> {
    omicron_runtime::run(async {
        let args = OxDb::parse();
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator)
            .build()
            .filter_level(args.log_level)
            .fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        let drain = slog_dtrace::with_drain(drain).0.fuse();
        let log = Logger::root(drain, o!("component" => "oxdb"));
        match args.cmd {
            Subcommand::Describe => describe_data(),
            Subcommand::Populate { populate_args } => {
                populate(args.address, args.port, log, populate_args).await?
            }
            Subcommand::Wipe => {
                wipe_single_node_db(args.address, args.port, log).await?
            }
            Subcommand::Query {
                timeseries_name,
                filters,
                start,
                start_exclusive,
                end,
                end_exclusive,
            } => {
                let start = match (start, start_exclusive) {
                    (Some(start), _) => {
                        Some(query::Timestamp::Inclusive(start))
                    }
                    (_, Some(start)) => {
                        Some(query::Timestamp::Exclusive(start))
                    }
                    (None, None) => None,
                };
                let end = match (end, end_exclusive) {
                    (Some(end), _) => Some(query::Timestamp::Inclusive(end)),
                    (_, Some(end)) => Some(query::Timestamp::Exclusive(end)),
                    (None, None) => None,
                };
                query(
                    args.address,
                    args.port,
                    log,
                    timeseries_name,
                    filters,
                    start,
                    end,
                )
                .await?;
            }
            #[cfg(feature = "sql")]
            Subcommand::Sql { opts } => {
                oximeter_db::shells::sql::shell(
                    args.address,
                    args.port,
                    log,
                    opts,
                )
                .await?
            }
            #[cfg(feature = "oxql")]
            Subcommand::Oxql { opts } => {
                oximeter_db::shells::oxql::shell(
                    args.address,
                    args.port,
                    log,
                    opts,
                )
                .await?
            }
            #[cfg(feature = "native-sql-shell")]
            Subcommand::NativeSql => {
                oximeter_db::shells::native::shell(args.address, args.port)
                    .await?
            }
        }
        Ok(())
    })
}
