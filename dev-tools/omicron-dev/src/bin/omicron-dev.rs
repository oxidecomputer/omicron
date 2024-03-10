// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Developer tool for easily running bits of Omicron

use anyhow::{bail, Context};
use camino::Utf8Path;
use camino::Utf8PathBuf;
use clap::Args;
use clap::Parser;
use dropshot::test_util::LogContext;
use futures::stream::StreamExt;
use nexus_config::NexusConfig;
use nexus_test_interface::NexusServer;
use omicron_common::cmd::fatal;
use omicron_common::cmd::CmdError;
use omicron_test_utils::dev;
use signal_hook::consts::signal::SIGINT;
use signal_hook_tokio::Signals;
use std::io::Write;
use std::os::unix::prelude::OpenOptionsExt;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let subcmd = OmicronDb::parse();
    let result = match subcmd {
        OmicronDb::DbRun { ref args } => cmd_db_run(args).await,
        OmicronDb::DbPopulate { ref args } => cmd_db_populate(args).await,
        OmicronDb::DbWipe { ref args } => cmd_db_wipe(args).await,
        OmicronDb::ChRun { ref args } => cmd_clickhouse_run(args).await,
        OmicronDb::MgsRun { ref args } => cmd_mgs_run(args).await,
        OmicronDb::RunAll { ref args } => cmd_run_all(args).await,
        OmicronDb::CertCreate { ref args } => cmd_cert_create(args).await,
    };
    if let Err(error) = result {
        fatal(CmdError::Failure(error));
    }
    Ok(())
}

/// Tools for working with a local Omicron deployment
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

    /// Run a simulated Management Gateway Service for development
    MgsRun {
        #[clap(flatten)]
        args: MgsRunArgs,
    },

    /// Run a full simulated control plane
    RunAll {
        #[clap(flatten)]
        args: RunAllArgs,
    },

    /// Create a self-signed certificate for use with Omicron
    CertCreate {
        #[clap(flatten)]
        args: CertCreateArgs,
    },
}

#[derive(Clone, Debug, Args)]
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
    #[clap(long = "no-populate", action(clap::ArgAction::SetFalse))]
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
    println!("omicron-dev: environment:");
    for (k, v) in db_starter.environment() {
        println!("    {}={}", k, v);
    }
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
        let start = tokio::time::Instant::now();
        println!("omicron-dev: populating database");
        db_instance.populate().await.context("populating database")?;
        let end = tokio::time::Instant::now();
        let duration = end.duration_since(start);
        println!(
            "omicron-dev: populated database in {}.{} seconds",
            duration.as_secs(),
            duration.subsec_millis()
        );
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

#[derive(Clone, Debug, Args)]
struct ChRunArgs {
    /// The HTTP port on which the server will listen
    #[clap(short, long, default_value = "8123", action)]
    port: u16,
    /// Starts a ClickHouse replicated cluster of 2 replicas and 3 keeper nodes
    #[clap(long, conflicts_with = "port", action)]
    replicated: bool,
}

async fn cmd_clickhouse_run(args: &ChRunArgs) -> Result<(), anyhow::Error> {
    let logctx = LogContext::new(
        "omicron-dev",
        &dropshot::ConfigLogging::StderrTerminal {
            level: dropshot::ConfigLoggingLevel::Info,
        },
    );
    if args.replicated {
        start_replicated_cluster(&logctx).await?;
    } else {
        start_single_node(&logctx, args.port).await?;
    }
    Ok(())
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
        db_instance.data_path()
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
        "omicron-dev: running ClickHouse cluster with configuration files:\n \
        replicas: {}\n keepers: {}",
        cluster.replica_config_path().display(),
        cluster.keeper_config_path().display()
    );
    let pid_error_msg = "Failed to get process PID, it may not have started";
    println!(
        "omicron-dev: ClickHouse cluster is running with: server PIDs = [{}, {}] \
        and keeper PIDs = [{}, {}, {}]",
        cluster.replica_1
            .pid()
            .expect(pid_error_msg),
        cluster.replica_2
            .pid()
            .expect(pid_error_msg),
        cluster.keeper_1
            .pid()
            .expect(pid_error_msg),
        cluster.keeper_2
            .pid()
            .expect(pid_error_msg),
        cluster.keeper_3
            .pid()
            .expect(pid_error_msg),
    );
    println!(
        "omicron-dev: ClickHouse HTTP servers listening on ports: {}, {}",
        cluster.replica_1.port(),
        cluster.replica_2.port()
    );
    println!(
        "omicron-dev: using {} and {} for ClickHouse data storage",
        cluster.replica_1.data_path(),
        cluster.replica_2.data_path()
    );

    // Wait for the replicas and keepers to exit themselves (an error), or for SIGINT
    tokio::select! {
        _ = cluster.replica_1.wait_for_shutdown() => {
            cluster.replica_1.cleanup().await.context(
                format!("clean up {} after shutdown", cluster.replica_1.data_path())
            )?;
            bail!("omicron-dev: ClickHouse replica 1 shutdown unexpectedly");
        }
        _ = cluster.replica_2.wait_for_shutdown() => {
            cluster.replica_2.cleanup().await.context(
                format!("clean up {} after shutdown", cluster.replica_2.data_path())
            )?;
            bail!("omicron-dev: ClickHouse replica 2 shutdown unexpectedly");
        }
        _ = cluster.keeper_1.wait_for_shutdown() => {
            cluster.keeper_1.cleanup().await.context(
                format!("clean up {} after shutdown", cluster.keeper_1.data_path())
            )?;
            bail!("omicron-dev: ClickHouse keeper 1 shutdown unexpectedly");
        }
        _ = cluster.keeper_2.wait_for_shutdown() => {
            cluster.keeper_2.cleanup().await.context(
                format!("clean up {} after shutdown", cluster.keeper_2.data_path())
            )?;
            bail!("omicron-dev: ClickHouse keeper 2 shutdown unexpectedly");
        }
        _ = cluster.keeper_3.wait_for_shutdown() => {
            cluster.keeper_3.cleanup().await.context(
                format!("clean up {} after shutdown", cluster.keeper_3.data_path())
            )?;
            bail!("omicron-dev: ClickHouse keeper 3 shutdown unexpectedly");
        }
        caught_signal = signal_stream.next() => {
            assert_eq!(caught_signal.unwrap(), SIGINT);
            eprintln!(
                "omicron-dev: caught signal, shutting down and removing \
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

#[derive(Clone, Debug, Args)]
struct RunAllArgs {
    /// Nexus external API listen port.  Use `0` to request any available port.
    #[clap(long, action)]
    nexus_listen_port: Option<u16>,
}

async fn cmd_run_all(args: &RunAllArgs) -> Result<(), anyhow::Error> {
    // Start a stream listening for SIGINT
    let signals = Signals::new(&[SIGINT]).expect("failed to wait for SIGINT");
    let mut signal_stream = signals.fuse();

    // Read configuration.
    let config_str = include_str!("../../../../nexus/examples/config.toml");
    let mut config: NexusConfig =
        toml::from_str(config_str).context("parsing example config")?;
    config.pkg.log = dropshot::ConfigLogging::File {
        // See LogContext::new(),
        path: "UNUSED".to_string().into(),
        level: dropshot::ConfigLoggingLevel::Trace,
        if_exists: dropshot::ConfigLoggingIfExists::Fail,
    };

    if let Some(p) = args.nexus_listen_port {
        config.deployment.dropshot_external.dropshot.bind_address.set_port(p);
    }

    println!("omicron-dev: setting up all services ... ");
    let cptestctx = nexus_test_utils::omicron_dev_setup_with_config::<
        omicron_nexus::Server,
    >(&mut config)
    .await
    .context("error setting up services")?;
    println!("omicron-dev: services are running.");

    // Print out basic information about what was started.
    // NOTE: The stdout strings here are not intended to be stable, but they are
    // used by the test suite.
    let addr = cptestctx.external_client.bind_address;
    println!("omicron-dev: nexus external API:    {:?}", addr);
    println!(
        "omicron-dev: nexus internal API:    {:?}",
        cptestctx.server.get_http_server_internal_address().await,
    );
    println!(
        "omicron-dev: cockroachdb pid:       {}",
        cptestctx.database.pid(),
    );
    println!(
        "omicron-dev: cockroachdb URL:       {}",
        cptestctx.database.pg_config()
    );
    println!(
        "omicron-dev: cockroachdb directory: {}",
        cptestctx.database.temp_dir().display()
    );
    println!(
        "omicron-dev: internal DNS HTTP:     http://{}",
        cptestctx.internal_dns.dropshot_server.local_addr()
    );
    println!(
        "omicron-dev: internal DNS:          {}",
        cptestctx.internal_dns.dns_server.local_address()
    );
    println!(
        "omicron-dev: external DNS name:     {}",
        cptestctx.external_dns_zone_name,
    );
    println!(
        "omicron-dev: external DNS HTTP:     http://{}",
        cptestctx.external_dns.dropshot_server.local_addr()
    );
    println!(
        "omicron-dev: external DNS:          {}",
        cptestctx.external_dns.dns_server.local_address()
    );
    println!(
        "omicron-dev:   e.g. `dig @{} -p {} {}.sys.{}`",
        cptestctx.external_dns.dns_server.local_address().ip(),
        cptestctx.external_dns.dns_server.local_address().port(),
        cptestctx.silo_name,
        cptestctx.external_dns_zone_name,
    );
    for (location, gateway) in &cptestctx.gateway {
        println!(
            "omicron-dev: management gateway:    http://{} ({})",
            gateway.client.bind_address, location,
        );
    }
    println!("omicron-dev: silo name:             {}", cptestctx.silo_name,);
    println!(
        "omicron-dev: privileged user name:  {}",
        cptestctx.user_name.as_ref(),
    );

    // Wait for a signal.
    let caught_signal = signal_stream.next().await;
    assert_eq!(caught_signal.unwrap(), SIGINT);
    eprintln!(
        "omicron-dev: caught signal, shutting down and removing \
        temporary directory"
    );

    cptestctx.teardown().await;
    Ok(())
}

#[derive(Clone, Debug, Args)]
struct CertCreateArgs {
    /// path to where the generated certificate and key files should go
    /// (e.g., "out/initial-" would cause the files to be called
    /// "out/initial-cert.pem" and "out/initial-key.pem")
    #[clap(action)]
    output_base: Utf8PathBuf,

    /// DNS names that the certificate claims to be valid for (subject
    /// alternative names)
    #[clap(action, required = true)]
    server_names: Vec<String>,
}

async fn cmd_cert_create(args: &CertCreateArgs) -> Result<(), anyhow::Error> {
    let cert = rcgen::generate_simple_self_signed(args.server_names.clone())
        .context("generating certificate")?;
    let cert_pem =
        cert.serialize_pem().context("serializing certificate as PEM")?;
    let key_pem = cert.serialize_private_key_pem();

    let cert_path = Utf8PathBuf::from(format!("{}cert.pem", args.output_base));
    write_private_file(&cert_path, cert_pem.as_bytes())
        .context("writing certificate file")?;
    println!("wrote certificate to {}", cert_path);

    let key_path = Utf8PathBuf::from(format!("{}key.pem", args.output_base));
    write_private_file(&key_path, key_pem.as_bytes())
        .context("writing private key file")?;
    println!("wrote private key to {}", key_path);

    Ok(())
}

#[cfg_attr(not(mac), allow(clippy::useless_conversion))]
fn write_private_file(
    path: &Utf8Path,
    contents: &[u8],
) -> Result<(), anyhow::Error> {
    // The file should be readable and writable by the user only.
    let perms = libc::S_IRUSR | libc::S_IWUSR;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(perms.into()) // into() needed on mac only
        .open(path)
        .with_context(|| format!("open {:?} for writing", path))?;
    file.write_all(contents).with_context(|| format!("write to {:?}", path))
}

#[derive(Clone, Debug, Args)]
struct MgsRunArgs {}

async fn cmd_mgs_run(_args: &MgsRunArgs) -> Result<(), anyhow::Error> {
    // Start a stream listening for SIGINT
    let signals = Signals::new(&[SIGINT]).expect("failed to wait for SIGINT");
    let mut signal_stream = signals.fuse();

    println!("omicron-dev: setting up MGS ... ");
    let gwtestctx = gateway_test_utils::setup::test_setup(
        "omicron-dev",
        gateway_messages::SpPort::One,
    )
    .await;
    println!("omicron-dev: MGS is running.");

    let addr = gwtestctx.client.bind_address;
    println!("omicron-dev: MGS API: http://{:?}", addr);

    // Wait for a signal.
    let caught_signal = signal_stream.next().await;
    assert_eq!(caught_signal.unwrap(), SIGINT);
    eprintln!(
        "omicron-dev: caught signal, shutting down and removing \
        temporary directory"
    );

    gwtestctx.teardown().await;
    Ok(())
}
