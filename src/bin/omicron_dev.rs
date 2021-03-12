/*!
 * Developer tool for setting up a local database for use by Omicron
 */

use anyhow::bail;
use anyhow::Context;
use omicron::cmd::fatal;
use omicron::cmd::CmdError;
use omicron::dev;
use std::path::PathBuf;
use structopt::StructOpt;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let subcmd = OmicronDb::from_args_safe().unwrap_or_else(|err| {
        fatal(CmdError::Usage(format!("parsing arguments: {}", err.message)))
    });
    match subcmd {
        OmicronDb::DbRun { ref args } => cmd_db_run(args).await,
    }
}

/// Manage a local CockroachDB database for Omicron development
#[derive(Debug, StructOpt)]
enum OmicronDb {
    /// Start a CockroachDB cluster using a temporary directory for storage
    DbRun {
        #[structopt(flatten)]
        args: DbRunArgs,
    },
}

#[derive(Debug, StructOpt)]
struct DbRunArgs {
    #[structopt(long, parse(from_os_str))]
    store_dir: Option<PathBuf>,
}

async fn cmd_db_run(args: &DbRunArgs) -> Result<(), anyhow::Error> {
    /*
     * Set ourselves up to wait for SIGINT.  It's important to do this early,
     * before we've created resources that we want to have cleaned up on SIGINT
     * (e.g., the temporary directory created by the database starter).
     */
    let (tx, mut rx) = tokio::sync::watch::channel(());
    ctrlc::set_handler(move || {
        tx.send(()).expect("internal error: failed to send CTRL-C message");
    })
    .expect("failed to wait for SIGINT");

    /*
     * Now start CockroachDB.  This process looks bureaucratic (create arg
     * builder, then create starter, then start it) because we want to be able
     * to print what's happening before we do it.
     */
    let mut db_arg_builder = dev::db::CockroachStarterBuilder::new();

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
        _ = rx.changed() => {
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
