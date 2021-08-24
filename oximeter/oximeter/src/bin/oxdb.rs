//! Tool for developing against the Oximeter timeseries database, populating data and querying.
// Copyright 2021 Oxide Computer Company

use anyhow::{bail, Context};
use chrono::{DateTime, Duration, Utc};
use oximeter::{
    db::{query, Client},
    types::{Cumulative, Sample},
    Metric, Target,
};
use slog::{debug, info, o, Drain, Level, Logger};
use std::net::SocketAddr;
use structopt::StructOpt;
use uuid::Uuid;

// Samples are inserted in chunks of this size, to avoid large allocations when inserting huge
// numbers of timeseries.
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
    pub value: Cumulative<f64>,
}

impl CpuBusy {
    pub fn new(id: i64) -> Self {
        Self { cpu_id: id, value: Cumulative::new(0.0) }
    }
}

fn level_from_str(s: &str) -> Result<Level, anyhow::Error> {
    if let Ok(level) = s.parse() {
        Ok(level)
    } else {
        bail!(format!("Invalid log level: {}", s))
    }
}

fn duration_from_str(s: &str) -> Result<Duration, anyhow::Error> {
    let duration = std::time::Duration::from_secs_f64(
        s.parse().context("Invalid interval")?,
    );
    Duration::from_std(duration).context("Duration out of range")
}

/// Tools for developing with the Oximeter timeseries database.
#[derive(Debug, StructOpt)]
struct OxDb {
    /// Port on which to connect to the database
    #[structopt(short, long, default_value = "8123")]
    port: u16,

    /// Logging level
    #[structopt(short, long, default_value = "info", parse(try_from_str = level_from_str))]
    log_level: Level,

    #[structopt(subcommand)]
    cmd: Subcommand,
}

#[derive(Debug, StructOpt)]
struct PopulateArgs {
    /// The number of samples to generate, per timeseries
    #[structopt(short = "n", long, default_value = "100")]
    n_samples: usize,

    /// Number of projects to simulate
    #[structopt(short = "p", long, default_value = "2")]
    n_projects: usize,

    /// Number of VM instances to simulate, _per project_
    #[structopt(short = "i", long, default_value = "2")]
    n_instances: usize,

    /// Number of vCPUs to simulate, per instance.
    #[structopt(short = "c", long, default_value = "4")]
    n_cpus: usize,

    /// The interval between simulated samples from each metric, in seconds
    #[structopt(short = "I", long, default_value = "1.0", parse(try_from_str = duration_from_str))]
    interval: Duration,

    /// If true, generate data and report logs, but do not actually insert anything into the
    /// database.
    #[structopt(short, long)]
    dry_run: bool,
}

#[derive(Debug, StructOpt)]
enum Subcommand {
    /// Populate the database with test data
    ///
    /// This simulates CPU time data from one or more virtual machine instances. Use the `describe`
    /// subcommand to describe the schema of each timeseries.
    Populate {
        #[structopt(flatten)]
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
        timeseries_name: String,

        /// Filters applied to the timeseries's fields, specificed as `name=value` pairs.
        #[structopt(required = true, min_values(1))]
        filters: Vec<query::Filter>,

        /// The start time to which the search is constrained. None means the beginning of time.
        #[structopt(long)]
        after: Option<DateTime<Utc>>,

        /// The stop time to which the search is constrained. None means the current time.
        #[structopt(long)]
        before: Option<DateTime<Utc>>,
    },
}

async fn make_client(port: u16, log: &Logger) -> Result<Client, anyhow::Error> {
    let client_log = log.new(o!("component" => "oximeter_client"));
    let address = SocketAddr::new("::1".parse().unwrap(), port);
    Client::new(address, client_log).await.context("Failed to connect to DB")
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

    let cpu = CpuBusy::new(0);
    print!(
        "\nMetric:\n\n Name: {metric_name:?}\n Type: {ty:?}\n",
        metric_name = cpu.name(),
        ty = cpu.measurement_type()
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
    port: u16,
    log: Logger,
    args: PopulateArgs,
) -> Result<(), anyhow::Error> {
    info!(log, "populating Oximeter database");
    let client = make_client(port, &log).await?;
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
            let start_time = Utc::now();
            for cpu in 0..args.n_cpus {
                for sample in 0..args.n_samples {
                    let cpu_busy = CpuBusy {
                        cpu_id: cpu as _,
                        value: Cumulative::new(sample as _),
                    };
                    let sample = Sample::new(
                        &vm,
                        &cpu_busy,
                        Some(start_time + args.interval * sample as i32),
                    );
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

async fn wipe_db(port: u16, log: Logger) -> Result<(), anyhow::Error> {
    let client = make_client(port, &log).await?;
    client.wipe_db().await.context("Failed to wipe database")
}

async fn query(
    port: u16,
    log: Logger,
    timeseries_name: String,
    filters: Vec<query::Filter>,
    after: Option<DateTime<Utc>>,
    before: Option<DateTime<Utc>>,
) -> Result<(), anyhow::Error> {
    let client = make_client(port, &log).await?;
    let timeseries = client
        .filter_timeseries_with(&timeseries_name, &filters, after, before)
        .await?;
    println!("{}", serde_json::to_string(&timeseries).unwrap());
    Ok(())
}

#[tokio::main]
async fn main() {
    let args = OxDb::from_args();
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator)
        .build()
        .filter_level(args.log_level)
        .fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = Logger::root(drain, o!("component" => "oxdb"));
    match args.cmd {
        Subcommand::Describe => describe_data(),
        Subcommand::Populate { populate_args } => {
            populate(args.port, log, populate_args).await.unwrap();
        }
        Subcommand::Wipe => wipe_db(args.port, log).await.unwrap(),
        Subcommand::Query { timeseries_name, filters, after, before } => {
            query(args.port, log, timeseries_name, filters, after, before)
                .await
                .unwrap();
        }
    }
}
