// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Developer tool for setting up a local database for use by Omicron

use anyhow::bail;
use anyhow::Context;
use clap::Args;
use clap::Parser;
use futures::stream::StreamExt;
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use omicron_test_utils::dev;
use signal_hook::consts::signal::SIGINT;
use signal_hook_tokio::Signals;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let subcmd = OmicronDb::parse();
    let result = match subcmd {
        OmicronDb::DbRun { ref args } => cmd_db_run(args).await,
        OmicronDb::DbPopulate { ref args } => cmd_db_populate(args).await,
        OmicronDb::DbWipe { ref args } => cmd_db_wipe(args).await,
        OmicronDb::ChRun { ref args } => cmd_clickhouse_run(args).await,
    };
    if let Err(error) = result {
        fatal(CmdError::Failure(format!("{:#}", error)));
    }
    Ok(())
}

/// Manage a local CockroachDB database for Omicron development
#[derive(Debug, Parser)]
#[clap(version)]
enum OmicronDb {
    /// Start a CockroachDB cluster for development
    DbRun {
        #[clap(flatten)]
        args: DbRunArgs,
    },

    /// Populate an existing CockroachDB cluster with the Omicron schema
    DbPopulate {
        #[clap(flatten)]
        args: DbPopulateArgs,
    },

    /// Wipe the Omicron schema (and all data) from an existing CockroachDB
    /// cluster
    DbWipe {
        #[clap(flatten)]
        args: DbWipeArgs,
    },

    /// Run a ClickHouse database server for development
    ChRun {
        #[clap(flatten)]
        args: ChRunArgs,
    },
}

#[derive(Debug, Args)]
struct DbRunArgs {
    /// Path to store database data (default: temp dir cleaned up on exit)
    #[clap(long, action)]
    store_dir: Option<PathBuf>,

    /// Database (SQL) listen port.  Use `0` to request any available port.
    // We choose an arbitrary default port that's different from the default
    // CockroachDB port to avoid conflicting.  We don't use 0 because this port
    // is specified in a few other places, like the default Nexus config file.
    // TODO We could load that file at compile time and use the value there.
    #[clap(long, default_value = "32221", action)]
    listen_port: u16,

    // This unusual clap configuration makes "populate" default to true,
    // allowing a --no-populate override on the CLI.
    /// Do not populate the database with any schema
    #[clap(long = "--no-populate", action(clap::ArgAction::SetFalse))]
    populate: bool,
}

async fn cmd_db_run(args: &DbRunArgs) -> Result<(), anyhow::Error> {
    // Set ourselves up to wait for SIGINT.  It's important to do this early,
    // before we've created resources that we want to have cleaned up on SIGINT
    // (e.g., the temporary directory created by the database starter).
    let signals = Signals::new(&[SIGINT]).expect("failed to wait for SIGINT");
    let mut signal_stream = signals.fuse();

    // Now start CockroachDB.  This process looks bureaucratic (create arg
    // builder, then create starter, then start it) because we want to be able
    // to print what's happening before we do it.
    let mut db_arg_builder =
        dev::db::CockroachStarterBuilder::new().listen_port(args.listen_port);

    // NOTE: The stdout strings here are not intended to be stable, but they are
    // used by the test suite.

    if let Some(store_dir) = &args.store_dir {
        println!(
            "omicron-dev: using user-provided path for database store: {}",
            store_dir.display()
        );
        db_arg_builder = db_arg_builder.store_dir(store_dir);
    } else {
        println!(
            "omicron-dev: using temporary directory for database store \
            (cleaned up on clean exit)"
        );
    }

    let db_starter = db_arg_builder.build()?;
    println!(
        "omicron-dev: will run this to start CockroachDB:\n{}",
        db_starter.cmdline()
    );
    println!(
        "omicron-dev: temporary directory: {}",
        db_starter.temp_dir().display()
    );

    let mut db_instance = db_starter.start().await?;
    println!("\nomicron-dev: child process: pid {}", db_instance.pid());
    println!(
        "omicron-dev: CockroachDB listening at: {}",
        db_instance.listen_url()
    );

    if args.populate {
        // Populate the database with our schema.
        println!("omicron-dev: populating database");
        db_instance.populate().await.context("populating database")?;
        println!("omicron-dev: populated database");
    }

    // Wait for either the child process to shut down on its own or for us to
    // receive SIGINT.
    tokio::select! {
        _ = db_instance.wait_for_shutdown() => {
            db_instance.cleanup().await.context("clean up after shutdown")?;
            bail!(
                "omicron-dev: database shut down unexpectedly \
                (see error output above)"
            );
        }
        caught_signal = signal_stream.next() => {
            assert_eq!(caught_signal.unwrap(), SIGINT);

            /*
             * We don't have to do anything to trigger shutdown because the
             * shell will have delivered the same SIGINT that we got to the
             * cockroach process as well.
             */
            eprintln!(
                "omicron-dev: caught signal, shutting down and removing \
                temporary directory"
            );

            db_instance
                .wait_for_shutdown()
                .await
                .context("clean up after SIGINT shutdown")?;
        }
    }

    Ok(())
}

#[derive(Debug, Args)]
struct DbPopulateArgs {
    /// URL for connecting to the database (postgresql:///...)
    #[clap(long, action)]
    database_url: String,

    /// Wipe any existing schema (and data!) before populating
    #[clap(long, action)]
    wipe: bool,
}

async fn cmd_db_populate(args: &DbPopulateArgs) -> Result<(), anyhow::Error> {
    let config =
        args.database_url.parse::<tokio_postgres::Config>().with_context(
            || format!("parsing database URL {:?}", args.database_url),
        )?;
    let client = dev::db::Client::connect(&config, tokio_postgres::NoTls)
        .await
        .with_context(|| format!("connecting to {:?}", args.database_url))?;

    if args.wipe {
        println!("omicron-dev: wiping any existing database");
        dev::db::wipe(&client).await?;
    }

    println!("omicron-dev: populating database");
    dev::db::populate(&client).await?;
    println!("omicron-dev: populated database");
    client.cleanup().await.expect("connection failed");
    Ok(())
}

#[derive(Debug, Args)]
struct DbWipeArgs {
    /// URL for connecting to the database (postgresql:///...)
    #[clap(long, action)]
    database_url: String,
}

async fn cmd_db_wipe(args: &DbWipeArgs) -> Result<(), anyhow::Error> {
    let config =
        args.database_url.parse::<tokio_postgres::Config>().with_context(
            || format!("parsing database URL {:?}", args.database_url),
        )?;
    let client = dev::db::Client::connect(&config, tokio_postgres::NoTls)
        .await
        .with_context(|| format!("connecting to {:?}", args.database_url))?;

    println!("omicron-dev: wiping any existing database");
    dev::db::wipe(&client).await?;
    println!("omicron-dev: wiped");
    client.cleanup().await.expect("connection failed");
    Ok(())
}

#[derive(Debug, Args)]
struct ChRunArgs {
    /// The HTTP port on which the server will listen
    #[clap(short, long, default_value = "8123", action)]
    port: u16,
}

async fn cmd_clickhouse_run(args: &ChRunArgs) -> Result<(), anyhow::Error> {
    // Start a stream listening for SIGINT
    let signals = Signals::new(&[SIGINT]).expect("failed to wait for SIGINT");
    let mut signal_stream = signals.fuse();

    // Start the database server process, possibly on a specific port
    let mut db_instance =
        dev::clickhouse::ClickHouseInstance::new(args.port).await?;
    println!(
        "omicron-dev: running ClickHouse with full command:\n\"clickhouse {}\"",
        db_instance.cmdline().join(" ")
    );
    println!(
        "omicron-dev: ClickHouse is running with PID {}",
        db_instance
            .pid()
            .expect("Failed to get process PID, it may not have started")
    );
    println!(
        "omicron-dev: ClickHouse HTTP server listening on port {}",
        db_instance.port()
    );
    println!(
        "omicron-dev: using {} for ClickHouse data storage",
        db_instance.data_path().display()
    );

    // Wait for the DB to exit itself (an error), or for SIGINT
    tokio::select! {
        _ = db_instance.wait_for_shutdown() => {
            db_instance.cleanup().await.context("clean up after shutdown")?;
            bail!("omicron-dev: ClickHouse shutdown unexpectedly");
        }
        caught_signal = signal_stream.next() => {
            assert_eq!(caught_signal.unwrap(), SIGINT);

            // As above, we don't need to explicitly kill the DB process, since
            // the shell will have delivered the signal to the whole process group.
            eprintln!(
                "omicron-dev: caught signal, shutting down and removing \
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
