// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use futures::StreamExt;
use libc::SIGINT;
use nexus_config::NexusConfig;
use nexus_test_interface::NexusServer;
use nexus_test_utils::resource_helpers::DiskTest;
use signal_hook_tokio::Signals;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = OmicronDevApp::parse();
    args.exec().await
}

/// Tools for working with a local Omicron deployment.
#[derive(Clone, Debug, Parser)]
#[clap(version)]
struct OmicronDevApp {
    #[clap(subcommand)]
    command: OmicronDevCmd,
}

impl OmicronDevApp {
    async fn exec(&self) -> Result<(), anyhow::Error> {
        match &self.command {
            OmicronDevCmd::RunAll(args) => args.exec().await,
        }
    }
}

#[derive(Clone, Debug, Subcommand)]
enum OmicronDevCmd {
    /// Run a full simulated control plane
    RunAll(RunAllArgs),
}

#[derive(Clone, Debug, Args)]
struct RunAllArgs {
    /// Nexus external API listen port.  Use `0` to request any available port.
    #[clap(long, action)]
    nexus_listen_port: Option<u16>,
}

impl RunAllArgs {
    async fn exec(&self) -> Result<(), anyhow::Error> {
        // Start a stream listening for SIGINT
        let signals =
            Signals::new(&[SIGINT]).expect("failed to wait for SIGINT");
        let mut signal_stream = signals.fuse();

        // Read configuration.
        let config_str = include_str!("../../../nexus/examples/config.toml");
        let mut config: NexusConfig =
            toml::from_str(config_str).context("parsing example config")?;
        config.pkg.log = dropshot::ConfigLogging::File {
            // See LogContext::new(),
            path: "UNUSED".to_string().into(),
            level: dropshot::ConfigLoggingLevel::Trace,
            if_exists: dropshot::ConfigLoggingIfExists::Fail,
        };

        if let Some(p) = self.nexus_listen_port {
            config
                .deployment
                .dropshot_external
                .dropshot
                .bind_address
                .set_port(p);
        }

        println!("omicron-dev: setting up all services ... ");
        let cptestctx = nexus_test_utils::omicron_dev_setup_with_config::<
            omicron_nexus::Server,
        >(&mut config, 0)
        .await
        .context("error setting up services")?;

        println!("omicron-dev: Adding disks to first sled agent");

        // This is how our integration tests are identifying that "disks exist"
        // within the database.
        //
        // This inserts:
        // - DEFAULT_ZPOOL_COUNT zpools, each of which contains:
        //   - A crucible dataset
        //   - A debug dataset
        DiskTest::new(&cptestctx).await;

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
}
