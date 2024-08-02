// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::path::PathBuf;

use anyhow::{bail, Context};
use clap::{Args, Parser, Subcommand};
use dropshot::test_util::LogContext;
use futures::StreamExt;
use libc::SIGINT;
use omicron_test_utils::dev;
use signal_hook_tokio::Signals;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = ChDevApp::parse();
    args.exec().await
}

#[derive(Clone, Debug, Parser)]
pub struct ChDevApp {
    #[clap(subcommand)]
    command: ChDevCmd,
}

impl ChDevApp {
    pub async fn exec(&self) -> Result<(), anyhow::Error> {
        match &self.command {
            ChDevCmd::Run(args) => args.exec().await,
        }
    }
}

#[derive(Clone, Debug, Subcommand)]
pub(crate) enum ChDevCmd {
    /// Run a ClickHouse server
    Run(ChRunArgs),
}

#[derive(Clone, Debug, Args)]
pub(crate) struct ChRunArgs {
    /// The HTTP port on which the server will listen
    #[clap(short, long, default_value = "8123", action)]
    port: u16,
    /// Starts a ClickHouse replicated cluster of 2 replicas and 3 keeper nodes
    #[clap(long, conflicts_with = "port", action)]
    replicated: bool,
}

impl ChRunArgs {
    pub(crate) async fn exec(&self) -> Result<(), anyhow::Error> {
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
    port: u16,
) -> Result<(), anyhow::Error> {
    // Start a stream listening for SIGINT
    let signals = Signals::new(&[SIGINT]).expect("failed to wait for SIGINT");
    let mut signal_stream = signals.fuse();

    // Start the database server process, possibly on a specific port
    let mut db_instance =
        dev::clickhouse::ClickHouseInstance::new_single_node(logctx, port)
            .await?;
    println!(
        "ch-dev: running ClickHouse with full command:\n\"clickhouse {}\"",
        db_instance.cmdline().join(" ")
    );
    println!(
        "ch-dev: ClickHouse is running with PID {}",
        db_instance
            .pid()
            .expect("Failed to get process PID, it may not have started")
    );
    println!(
        "ch-dev: ClickHouse HTTP server listening on port {}",
        db_instance.port()
    );
    println!(
        "ch-dev: using {} for ClickHouse data storage",
        db_instance.data_path()
    );

    // Wait for the DB to exit itself (an error), or for SIGINT
    tokio::select! {
        _ = db_instance.wait_for_shutdown() => {
            db_instance.cleanup().await.context("clean up after shutdown")?;
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
            db_instance
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
        .join("../../oximeter/db/src/configs/replica_config.xml");
    let keeper_config = manifest_dir
        .as_path()
        .join("../../oximeter/db/src/configs/keeper_config.xml");

    let mut cluster = dev::clickhouse::ClickHouseCluster::new(
        logctx,
        replica_config,
        keeper_config,
    )
    .await?;
    println!(
        "ch-dev: running ClickHouse cluster with configuration files:\n \
        replicas: {}\n keepers: {}",
        cluster.replica_config_path().display(),
        cluster.keeper_config_path().display()
    );
    let pid_error_msg = "Failed to get process PID, it may not have started";
    println!(
        "ch-dev: ClickHouse cluster is running with: server PIDs = [{}, {}] \
        and keeper PIDs = [{}, {}, {}]",
        cluster.replica_1.pid().expect(pid_error_msg),
        cluster.replica_2.pid().expect(pid_error_msg),
        cluster.keeper_1.pid().expect(pid_error_msg),
        cluster.keeper_2.pid().expect(pid_error_msg),
        cluster.keeper_3.pid().expect(pid_error_msg),
    );
    println!(
        "ch-dev: ClickHouse HTTP servers listening on ports: {}, {}",
        cluster.replica_1.port(),
        cluster.replica_2.port()
    );
    println!(
        "ch-dev: using {} and {} for ClickHouse data storage",
        cluster.replica_1.data_path(),
        cluster.replica_2.data_path()
    );

    // Wait for the replicas and keepers to exit themselves (an error), or for SIGINT
    tokio::select! {
        _ = cluster.replica_1.wait_for_shutdown() => {
            cluster.replica_1.cleanup().await.context(
                format!("clean up {} after shutdown", cluster.replica_1.data_path())
            )?;
            bail!("ch-dev: ClickHouse replica 1 shutdown unexpectedly");
        }
        _ = cluster.replica_2.wait_for_shutdown() => {
            cluster.replica_2.cleanup().await.context(
                format!("clean up {} after shutdown", cluster.replica_2.data_path())
            )?;
            bail!("ch-dev: ClickHouse replica 2 shutdown unexpectedly");
        }
        _ = cluster.keeper_1.wait_for_shutdown() => {
            cluster.keeper_1.cleanup().await.context(
                format!("clean up {} after shutdown", cluster.keeper_1.data_path())
            )?;
            bail!("ch-dev: ClickHouse keeper 1 shutdown unexpectedly");
        }
        _ = cluster.keeper_2.wait_for_shutdown() => {
            cluster.keeper_2.cleanup().await.context(
                format!("clean up {} after shutdown", cluster.keeper_2.data_path())
            )?;
            bail!("ch-dev: ClickHouse keeper 2 shutdown unexpectedly");
        }
        _ = cluster.keeper_3.wait_for_shutdown() => {
            cluster.keeper_3.cleanup().await.context(
                format!("clean up {} after shutdown", cluster.keeper_3.data_path())
            )?;
            bail!("ch-dev: ClickHouse keeper 3 shutdown unexpectedly");
        }
        caught_signal = signal_stream.next() => {
            assert_eq!(caught_signal.unwrap(), SIGINT);
            eprintln!(
                "ch-dev: caught signal, shutting down and removing \
                temporary directories"
            );

            // Remove the data directories.
            let mut instances = vec![
                cluster.replica_1,
                cluster.replica_2,
                cluster.keeper_1,
                cluster.keeper_2,
                cluster.keeper_3,
            ];
            for instance in instances.iter_mut() {
                instance
                .wait_for_shutdown()
                .await
                .context(format!("clean up {} after SIGINT shutdown", instance.data_path()))?;
            };
        }
    }
    Ok(())
}
