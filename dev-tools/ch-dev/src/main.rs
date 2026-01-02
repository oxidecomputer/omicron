// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::path::PathBuf;

use anyhow::{Context, bail};
use clap::{Args, Parser, Subcommand};
use dropshot::test_util::LogContext;
use futures::StreamExt;
use libc::SIGINT;
use omicron_common::address::CLICKHOUSE_TCP_PORT;
use omicron_test_utils::dev::{self, clickhouse::ClickHousePorts};
use signal_hook_tokio::Signals;

#[expect(
    clippy::disallowed_macros,
    reason = "this is a dev-tool, and avoiding a dependency on \
     `omicron-runtime` helps minimize compile time."
)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = ChDevApp::parse();
    args.exec().await
}

/// Tools for working with a ClickHouse database.
#[derive(Clone, Debug, Parser)]
#[clap(version)]
struct ChDevApp {
    #[clap(subcommand)]
    command: ChDevCmd,
}

impl ChDevApp {
    async fn exec(&self) -> Result<(), anyhow::Error> {
        match &self.command {
            ChDevCmd::Run(args) => args.exec().await,
        }
    }
}

#[derive(Clone, Debug, Subcommand)]
enum ChDevCmd {
    /// Run a ClickHouse server
    Run(ChRunArgs),
}

#[derive(Clone, Debug, Args)]
struct ChRunArgs {
    /// The port on which the native protocol server will listen
    #[clap(short, long, default_value_t = CLICKHOUSE_TCP_PORT, action)]
    port: u16,
    /// Starts a ClickHouse replicated cluster of 2 replicas and 3 keeper nodes
    #[clap(long, conflicts_with_all = ["port"], action)]
    replicated: bool,
}

impl ChRunArgs {
    async fn exec(&self) -> Result<(), anyhow::Error> {
        let logctx = LogContext::new(
            "ch-dev",
            &dropshot::ConfigLogging::StderrTerminal {
                level: dropshot::ConfigLoggingLevel::Info,
            },
        );
        if self.replicated {
            start_replicated_cluster(&logctx).await?;
        } else {
            start_single_node(&logctx, self.port).await?;
        }
        Ok(())
    }
}

async fn start_single_node(
    logctx: &LogContext,
    native_port: u16,
) -> Result<(), anyhow::Error> {
    // Start a stream listening for SIGINT
    let signals = Signals::new(&[SIGINT]).expect("failed to wait for SIGINT");
    let mut signal_stream = signals.fuse();

    // Start the database server process, possibly on a specific port
    let ports = ClickHousePorts::new(0, native_port)?;
    let mut deployment =
        dev::clickhouse::ClickHouseDeployment::new_single_node_with_ports(
            logctx, ports,
        )
        .await?;
    let db_instance = deployment
        .replicas()
        .next()
        .expect("Should have launched a ClickHouse instance");
    println!(
        "ch-dev: running ClickHouse with full command:\n\"clickhouse {}\"",
        db_instance.cmdline().join(" ")
    );
    println!("ch-dev: ClickHouse environment:");
    for (k, v) in db_instance.environment() {
        println!("\t{k}={v}");
    }
    println!(
        "ch-dev: ClickHouse is running with PID {}",
        db_instance
            .pid()
            .expect("Failed to get process PID, it may not have started")
    );
    println!(
        "ch-dev: ClickHouse HTTP server listening on port {}",
        db_instance.http_address.port()
    );
    println!(
        "ch-dev: ClickHouse Native server listening on port {}",
        db_instance.native_address.port()
    );
    println!("ch-dev: ClickHouse data stored in: {}", db_instance.data_path());

    // Wait for the DB to exit itself (an error), or for SIGINT
    tokio::select! {
        _ = deployment.wait_for_shutdown() => {
            deployment.cleanup().await.context("clean up after shutdown")?;
            bail!("ch-dev: ClickHouse shutdown unexpectedly");
        }
        caught_signal = signal_stream.next() => {
            assert_eq!(caught_signal.unwrap(), SIGINT);

            // As above, we don't need to explicitly kill the DB process, since
            // the shell will have delivered the signal to the whole process group.
            eprintln!(
                "ch-dev: caught signal, shutting down and removing \
                temporary directory"
            );

            // Remove the data directory.
            deployment
                .wait_for_shutdown()
                .await
                .context("clean up after SIGINT shutdown")?;
        }
    }
    Ok(())
}

async fn start_replicated_cluster(
    logctx: &LogContext,
) -> Result<(), anyhow::Error> {
    // Start a stream listening for SIGINT
    let signals = Signals::new(&[SIGINT]).expect("failed to wait for SIGINT");
    let mut signal_stream = signals.fuse();

    // Start the database server and keeper processes
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let replica_config = manifest_dir
        .as_path()
        .join("../../oximeter/db/src/configs/replica_config.xml")
        .canonicalize()
        .context("Failed to canonicalize replica config path")?;
    let keeper_config = manifest_dir
        .as_path()
        .join("../../oximeter/db/src/configs/keeper_config.xml")
        .canonicalize()
        .context("Failed to canonicalize keeper config path")?;

    let mut cluster = dev::clickhouse::ClickHouseDeployment::new_cluster(
        logctx,
        replica_config,
        keeper_config,
    )
    .await?;
    println!(
        "ch-dev: running ClickHouse cluster with configuration files:\n \
        replicas: {}\n keepers: {}",
        cluster.replica_config_path().unwrap().display(),
        cluster.keeper_config_path().unwrap().display()
    );
    for replica in cluster.replicas() {
        println!(
            "ch-dev: running ClickHouse replica with full command:\
            \n\"clickhouse {}\"",
            replica.cmdline().join(" ")
        );
        println!("ch-dev: ClickHouse replica environment:");
        for (k, v) in replica.environment() {
            println!("\t{k}={v}");
        }
        println!(
            "ch-dev: ClickHouse replica PID is {}",
            replica.pid().context("Failed to get instance PID")?
        );
        println!(
            "ch-dev: ClickHouse replica data path is {}",
            replica.data_path(),
        );
        println!(
            "ch-dev: ClickHouse replica HTTP server is listening on port {}",
            replica.http_address.port(),
        );
        println!(
            "ch-dev: ClickHouse replica Native server is listening on port {}",
            replica.native_address.port(),
        );
    }
    for keeper in cluster.keepers() {
        println!(
            "ch-dev: running ClickHouse Keeper with full command:\
            \n\"clickhouse {}\"",
            keeper.cmdline().join(" ")
        );
        println!("ch-dev: ClickHouse Keeper environment:");
        for (k, v) in keeper.environment() {
            println!("\t{k}={v}");
        }
        println!(
            "ch-dev: ClickHouse Keeper PID is {}",
            keeper.pid().context("Failed to get Keeper PID")?
        );
        println!(
            "ch-dev: ClickHouse Keeper data path is {}",
            keeper.data_path(),
        );
        println!(
            "ch-dev: ClickHouse Keeper server is listening on port {}",
            keeper.address.port(),
        );
    }

    // Wait for the replicas and keepers to exit themselves (an error), or for SIGINT
    tokio::select! {
        res = cluster.wait_for_shutdown() => {
            cluster.cleanup().await.context("cleaning up after shutdown")?;
            match res {
                Ok(node) => {
                    bail!(
                        "ch-dev: ClickHouse cluster {:?} node {} shutdown unexpectedly",
                        node.kind,
                        node.index,
                    );
                }
                Err(e) => {
                    bail!(
                        "ch-dev: Failed to wait for cluster node: {}",
                        e,
                    );
                }
            }
        }
        caught_signal = signal_stream.next() => {
            assert_eq!(caught_signal.unwrap(), SIGINT);
            eprintln!(
                "ch-dev: caught signal, shutting down and removing \
                temporary directories"
            );
            cluster
                .cleanup()
                .await
                .context("clean up after SIGINT shutdown")?;
        }
    }
    Ok(())
}
