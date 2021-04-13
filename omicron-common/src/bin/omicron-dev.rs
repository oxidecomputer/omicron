/*!
 * Developer tool for setting up a local database for use by Omicron
 */

use anyhow::bail;
use anyhow::Context;
use futures::stream::StreamExt;
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use omicron_common::dev;
use signal_hook::consts::signal::SIGINT;
use signal_hook_tokio::Signals;
use std::path::PathBuf;
use structopt::StructOpt;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let subcmd = OmicronDb::from_args_safe().unwrap_or_else(|err| {
        fatal(CmdError::Usage(format!("parsing arguments: {}", err.message)))
    });
    let result = match subcmd {
        OmicronDb::DbRun { ref args } => cmd_db_run(args).await,
        OmicronDb::DbPopulate { ref args } => cmd_db_populate(args).await,
        OmicronDb::DbWipe { ref args } => cmd_db_wipe(args).await,
    };
    if let Err(error) = result {
        fatal(CmdError::Failure(format!("{:#}", error)));
    }
    Ok(())
}

/// Manage a local CockroachDB database for Omicron development
#[derive(Debug, StructOpt)]
enum OmicronDb {
    /// Start a CockroachDB cluster for development
    DbRun {
        #[structopt(flatten)]
        args: DbRunArgs,
    },

    /// Populate an existing CockroachDB cluster with the Omicron schema
    DbPopulate {
        #[structopt(flatten)]
        args: DbPopulateArgs,
    },

    /// Wipe the Omicron schema (and all data) from an existing CockroachDB
    /// cluster
    DbWipe {
        #[structopt(flatten)]
        args: DbWipeArgs,
    },
}

#[derive(Debug, StructOpt)]
struct DbRunArgs {
    /// Path to store database data (default: temp dir cleaned up on exit)
    #[structopt(long, parse(from_os_str))]
    store_dir: Option<PathBuf>,

    /// Database (SQL) listen port.  Use `0` to request any available port.
    /*
     * We choose an arbitrary default port that's different from the default
     * CockroachDB port to avoid conflicting.  We don't use 0 because this port
     * is specified in a few other places, like the default Nexus config file.
     * TODO We could load that file at compile time and use the value there.
     */
    #[structopt(long, default_value = "32221")]
    listen_port: u16,

    /*
     * This unusual structopt configuration makes "populate" default to true,
     * allowing a --no-populate override on the CLI.
     */
    /// Do not populate the database with any schema
    #[structopt(long = "--no-populate", parse(from_flag = std::ops::Not::not))]
    populate: bool,
}

async fn cmd_db_run(args: &DbRunArgs) -> Result<(), anyhow::Error> {
    /*
     * Set ourselves up to wait for SIGINT.  It's important to do this early,
     * before we've created resources that we want to have cleaned up on SIGINT
     * (e.g., the temporary directory created by the database starter).
     */
    let signals = Signals::new(&[SIGINT]).expect("failed to wait for SIGINT");
    let mut signal_stream = signals.fuse();

    /*
     * Now start CockroachDB.  This process looks bureaucratic (create arg
     * builder, then create starter, then start it) because we want to be able
     * to print what's happening before we do it.
     */
    let mut db_arg_builder =
        dev::db::CockroachStarterBuilder::new().listen_port(args.listen_port);

    /*
     * NOTE: The stdout strings here are not intended to be stable, but they are
     * used by the test suite.
     */

    if let Some(store_dir) = &args.store_dir {
        println!(
            "omicron_dev: using user-provided path for database store: {}",
            store_dir.display()
        );
        db_arg_builder = db_arg_builder.store_dir(store_dir);
    } else {
        println!(
            "omicron_dev: using temporary directory for database store \
            (cleaned up on clean exit)"
        );
    }

    let db_starter = db_arg_builder.build()?;
    println!(
        "omicron_dev: will run this to start CockroachDB:\n{}",
        db_starter.cmdline()
    );
    println!(
        "omicron_dev: temporary directory: {}",
        db_starter.temp_dir().display()
    );

    let mut db_instance = db_starter.start().await?;
    println!("\nomicron_dev: child process: pid {}", db_instance.pid());
    println!(
        "omicron_dev: CockroachDB listening at: {}",
        db_instance.listen_url()
    );

    if args.populate {
        /*
         * Populate the database with our schema.
         */
        println!("omicron_dev: populating database");
        db_instance.populate().await.context("populating database")?;
        println!("omicron_dev: populated database");
    }

    /*
     * Wait for either the child process to shut down on its own or for us to
     * receive SIGINT.
     */
    tokio::select! {
        _ = db_instance.wait_for_shutdown() => {
            db_instance.cleanup().await.context("clean up after shutdown")?;
            bail!(
                "omicron_dev: database shut down unexpectedly \
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
                "omicron_dev: caught signal, shutting down and removing \
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

#[derive(Debug, StructOpt)]
struct DbPopulateArgs {
    /// URL for connecting to the database (postgresql:///...)
    #[structopt(long)]
    database_url: String,

    /// Wipe any existing schema (and data!) before populating
    #[structopt(long)]
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
        println!("omicron_dev: wiping any existing database");
        dev::db::wipe(&client).await?;
    }

    println!("omicron_dev: populating database");
    dev::db::populate(&client).await?;
    println!("omicron_dev: populated database");
    client.cleanup().await.expect("connection failed");
    Ok(())
}

#[derive(Debug, StructOpt)]
struct DbWipeArgs {
    /// URL for connecting to the database (postgresql:///...)
    #[structopt(long)]
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

    println!("omicron_dev: wiping any existing database");
    dev::db::wipe(&client).await?;
    println!("omicron_dev: wiped");
    client.cleanup().await.expect("connection failed");
    Ok(())
}
