// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Developer tool for operating on Nexus databases.

use anyhow::{Context, Result, bail};
use camino::Utf8PathBuf;
use clap::{Args, Parser, Subcommand};
use futures::stream::StreamExt;
use libc::SIGINT;
use omicron_test_utils::dev;
use signal_hook_tokio::Signals;

#[expect(
    clippy::disallowed_macros,
    reason = "this is a dev-tool, and avoiding a dependency on \
     `omicron-runtime` helps minimize compile time."
)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = DbDevApp::parse();
    args.exec().await
}

/// Tools for working with a CockroachDB database.
#[derive(Clone, Debug, Parser)]
#[clap(version)]
struct DbDevApp {
    #[clap(subcommand)]
    command: DbDevCmd,
}

impl DbDevApp {
    async fn exec(&self) -> Result<()> {
        match &self.command {
            DbDevCmd::Run(args) => args.exec().await,
            DbDevCmd::Populate(args) => args.exec().await,
            DbDevCmd::Wipe(args) => args.exec().await,
        }
    }
}

#[derive(Clone, Debug, Subcommand)]
enum DbDevCmd {
    /// Run a CockroachDB server
    Run(DbRunArgs),
    /// Populate a database with schema
    Populate(DbPopulateArgs),
    /// Wipe a database
    Wipe(DbWipeArgs),
}

#[derive(Clone, Debug, Args)]
struct DbRunArgs {
    /// Path to store database data (default: temp dir cleaned up on exit)
    #[clap(long, action)]
    store_dir: Option<Utf8PathBuf>,

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
    #[clap(long = "no-populate", action(clap::ArgAction::SetFalse))]
    populate: bool,
}

impl DbRunArgs {
    async fn exec(&self) -> Result<()> {
        // Set ourselves up to wait for SIGINT.  It's important to do this early,
        // before we've created resources that we want to have cleaned up on SIGINT
        // (e.g., the temporary directory created by the database starter).
        let signals =
            Signals::new(&[SIGINT]).expect("failed to wait for SIGINT");
        let mut signal_stream = signals.fuse();

        // Now start CockroachDB.  This process looks bureaucratic (create arg
        // builder, then create starter, then start it) because we want to be able
        // to print what's happening before we do it.
        let mut db_arg_builder = dev::db::CockroachStarterBuilder::new()
            .listen_port(self.listen_port);

        // NOTE: The stdout strings here are not intended to be stable, but they are
        // used by the test suite.

        if let Some(store_dir) = &self.store_dir {
            println!(
                "db-dev: using user-provided path for database store: {}",
                store_dir,
            );
            db_arg_builder = db_arg_builder.store_dir(store_dir);
        } else {
            println!(
                "db-dev: using temporary directory for database store \
            (cleaned up on clean exit)"
            );
        }

        let db_starter = db_arg_builder.build()?;
        println!(
            "db-dev: will run this to start CockroachDB:\n{}",
            db_starter.cmdline()
        );
        println!("db-dev: environment:");
        for (k, v) in db_starter.environment() {
            println!("    {}={}", k, v);
        }
        println!(
            "db-dev: temporary directory: {}",
            db_starter.temp_dir().display()
        );

        let mut db_instance = db_starter.start().await?;
        println!("\ndb-dev: child process: pid {}", db_instance.pid());
        println!(
            "db-dev: CockroachDB listening at: {}",
            db_instance.listen_url()
        );

        if self.populate {
            // Populate the database with our schema.
            let start = tokio::time::Instant::now();
            println!("db-dev: populating database");
            db_instance.populate().await.context("populating database")?;
            let end = tokio::time::Instant::now();
            let duration = end.duration_since(start);
            println!(
                "db-dev: populated database in {}.{} seconds",
                duration.as_secs(),
                duration.subsec_millis()
            );
        }

        // Wait for either the child process to shut down on its own or for us to
        // receive SIGINT.
        tokio::select! {
            _ = db_instance.wait_for_shutdown() => {
                bail!(
                    "db-dev: database shut down unexpectedly \
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
                    "db-dev: caught signal, shutting down and removing \
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
}

#[derive(Clone, Debug, Args)]
struct DbPopulateArgs {
    /// URL for connecting to the database (postgresql:///...)
    #[clap(long, action)]
    database_url: String,

    /// Wipe any existing schema (and data!) before populating
    #[clap(long, action)]
    wipe: bool,
}

impl DbPopulateArgs {
    async fn exec(&self) -> Result<()> {
        let config =
            self.database_url.parse::<tokio_postgres::Config>().with_context(
                || format!("parsing database URL {:?}", self.database_url),
            )?;
        let client = dev::db::Client::connect(&config, tokio_postgres::NoTls)
            .await
            .with_context(|| {
                format!("connecting to {:?}", self.database_url)
            })?;

        if self.wipe {
            println!("db-dev: wiping any existing database");
            dev::db::wipe(&client).await?;
        }

        println!("db-dev: populating database");
        dev::db::populate(&client).await?;
        println!("db-dev: populated database");
        client.cleanup().await.expect("connection failed");
        Ok(())
    }
}

#[derive(Clone, Debug, Args)]
struct DbWipeArgs {
    /// URL for connecting to the database (postgresql:///...)
    #[clap(long, action)]
    database_url: String,
}

impl DbWipeArgs {
    async fn exec(&self) -> Result<()> {
        let config =
            self.database_url.parse::<tokio_postgres::Config>().with_context(
                || format!("parsing database URL {:?}", self.database_url),
            )?;
        let client = dev::db::Client::connect(&config, tokio_postgres::NoTls)
            .await
            .with_context(|| {
                format!("connecting to {:?}", self.database_url)
            })?;

        println!("db-dev: wiping any existing database");
        dev::db::wipe(&client).await?;
        println!("db-dev: wiped");
        client.cleanup().await.expect("connection failed");
        Ok(())
    }
}
