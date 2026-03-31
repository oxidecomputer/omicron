// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use camino::Utf8PathBuf;
use clap::{Args, Parser, Subcommand};
use futures::StreamExt;
use gateway_client::ClientInfo as _;
use gateway_test_utils::setup::DEFAULT_SP_SIM_CONFIG;
use libc::SIGINT;
use mg_common::test as mg_test;
use nexus_config::NexusConfig;
use nexus_test_interface::NexusServer;
use nexus_test_utils::resource_helpers::DiskTest;
use signal_hook_tokio::Signals;
use slog::{Drain, o};
use std::{
    fs,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6},
};

const DEFAULT_NEXUS_CONFIG: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../../nexus/examples/config.toml");

fn main() -> anyhow::Result<()> {
    oxide_tokio_rt::run(async {
        let args = OmicronDevApp::parse();
        args.exec().await
    })
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
            OmicronDevCmd::RunMultiple(args) => args.exec().await,
        }
    }
}

#[derive(Clone, Debug, Subcommand)]
enum OmicronDevCmd {
    /// Run a full simulated control plane
    RunAll(RunAllArgs),
    /// Run multiple simulated control planes
    RunMultiple(RunMultipleArgs),
}

#[derive(Clone, Debug, Args)]
struct RunAllArgs {
    /// Nexus external API listen port.  Use `0` to request any available port.
    #[clap(long, action)]
    nexus_listen_port: Option<u16>,
    /// Override the gateway server configuration file.
    #[clap(long, default_value = DEFAULT_SP_SIM_CONFIG)]
    gateway_config: Utf8PathBuf,
    /// Override the nexus configuration file.
    #[clap(long, default_value = DEFAULT_NEXUS_CONFIG)]
    nexus_config: Utf8PathBuf,
}

trait Configurable {
    fn nexus_config(&self) -> &Utf8PathBuf;
}

impl Configurable for RunAllArgs {
    fn nexus_config(&self) -> &Utf8PathBuf {
        &self.nexus_config
    }
}

impl RunAllArgs {
    async fn exec(&self) -> Result<(), anyhow::Error> {
        // Start a stream listening for SIGINT
        let mut signal_stream = start_stream();

        // Read configuration.
        let mut config = read_config(self)?;

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
        >(&mut config, 1, self.gateway_config.clone())
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
        // NOTE: The stdout strings here are not intended to be stable, but they
        // are used by the test suite.
        let addr = cptestctx.external_client.bind_address;
        println!("omicron-dev: nexus external API:     {:?}", addr);
        println!(
            "omicron-dev: nexus internal API:     {:?}",
            cptestctx.server.get_http_server_internal_address(),
        );
        println!(
            "omicron-dev: nexus lockstep API:     {:?}",
            cptestctx.server.get_http_server_lockstep_address(),
        );
        println!(
            "omicron-dev: sled agent API:         http://{:?}",
            cptestctx.sled_agents[0].local_addr(),
        );
        println!(
            "omicron-dev: cockroachdb pid:        {}",
            cptestctx.database.pid(),
        );
        println!(
            "omicron-dev: cockroachdb URL:        {}",
            cptestctx.database.pg_config()
        );
        println!(
            "omicron-dev: cockroachdb directory:  {}",
            cptestctx.database.temp_dir().display()
        );
        println!(
            "omicron-dev: clickhouse native addr: {}",
            cptestctx.clickhouse.native_address(),
        );
        println!(
            "omicron-dev: clickhouse http addr:   {}",
            cptestctx.clickhouse.http_address(),
        );
        println!(
            "omicron-dev: internal DNS HTTP:      http://{}",
            cptestctx.internal_dns.dropshot_server.local_addr()
        );
        println!(
            "omicron-dev: internal DNS:           {}",
            cptestctx.internal_dns.dns_server.local_address()
        );
        println!(
            "omicron-dev: external DNS name:      {}",
            cptestctx.external_dns_zone_name,
        );
        println!(
            "omicron-dev: external DNS HTTP:      http://{}",
            cptestctx.external_dns.dropshot_server.local_addr()
        );
        println!(
            "omicron-dev: external DNS:           {}",
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
                "omicron-dev: management gateway:     {} ({})",
                gateway.client.baseurl(),
                location,
            );
        }
        for (location, dendrite) in cptestctx.dendrite.read().unwrap().iter() {
            println!(
                "omicron-dev: dendrite:               http://[::1]:{} ({})",
                dendrite.port, location,
            );
        }
        for (location, lldpd) in &cptestctx.lldpd {
            println!(
                "omicron-dev: lldp:                   http://[::1]:{} ({})",
                lldpd.port, location,
            );
        }
        for (location, mgd) in &cptestctx.mgd {
            println!(
                "omicron-dev: maghemite:              http://[::1]:{} ({})",
                mgd.port, location,
            );
        }
        println!(
            "omicron-dev: silo name:              {}",
            cptestctx.silo_name,
        );
        println!(
            "omicron-dev: privileged user name:   {}",
            cptestctx.user_name.as_ref(),
        );
        println!("omicron-dev: privileged password:    {}", cptestctx.password);

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

fn start_stream() -> futures::stream::Fuse<signal_hook_tokio::SignalsInfo> {
    let signals = Signals::new(&[SIGINT]).expect("failed to wait for SIGINT");
    signals.fuse()
}

fn read_config(args: &dyn Configurable) -> Result<NexusConfig, anyhow::Error> {
    let config_str = fs::read_to_string(&args.nexus_config())?;
    let mut config: NexusConfig = toml::from_str(&config_str)
        .context(format!("parsing config: {}", args.nexus_config().as_str()))?;
    config.pkg.log = dropshot::ConfigLogging::File {
        // See LogContext::new(),
        path: "UNUSED".to_string().into(),
        level: dropshot::ConfigLoggingLevel::Trace,
        if_exists: dropshot::ConfigLoggingIfExists::Fail,
    };
    Ok(config)
}

#[derive(Clone, Debug, Args)]
struct RunMultipleArgs {
    /// Override the gateway server configuration file.
    #[clap(long, default_value = DEFAULT_SP_SIM_CONFIG)]
    gateway_config: Utf8PathBuf,
    /// Override the nexus configuration file.
    #[clap(long, default_value = DEFAULT_NEXUS_CONFIG)]
    nexus_config: Utf8PathBuf,
    /// Number of "racks" to launch
    #[clap(long, default_value_t = 1)]
    count: u8,
    /// Launch peer router mgd instances
    #[clap(long, default_value_t = 0)]
    peer_routers: u8,
}

impl Configurable for RunMultipleArgs {
    fn nexus_config(&self) -> &Utf8PathBuf {
        &self.nexus_config
    }
}

impl RunMultipleArgs {
    async fn exec(&self) -> Result<(), anyhow::Error> {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();

        let log = slog::Logger::root(drain, o!());

        // Start a stream listening for SIGINT
        let mut signal_stream = start_stream();

        // Read configuration.
        let mut config = read_config(self)?;

        let mut contexts = vec![];

        let mut peer_routers = vec![];

        let mut loopback_manager =
            mg_test::LoopbackIpManager::new("lo0", log.clone());

        for n in 0..self.peer_routers {
            let mgd_bgp_addr =
                SocketAddr::new(Ipv4Addr::new(127, 0, n, 1).into(), 1049);

            loopback_manager.add(&[mgd_bgp_addr.ip()]);

            let mgd = omicron_test_utils::dev::maghemite::MgdInstance::start(
                0,
                mgd_bgp_addr,
                None,
            )
            .await
            .unwrap();

            println!(
                "peer router {n}: mgd api:                http://[::1]:{}",
                mgd.port
            );

            println!(
                "peer router {n}: mgd bgp-dispatcher:     tcp {}",
                mgd_bgp_addr
            );

            if let Some(dir) = &mgd.data_dir {
                println!(
                    "peer router {n} tmp dir:                 {}",
                    dir.display()
                );
            }

            let api_socket_addr =
                SocketAddrV6::new(Ipv6Addr::LOCALHOST, mgd.port, 0, 0);

            let client = mg_admin_client::Client::new(
                &format!("http://{api_socket_addr}"),
                slog::Logger::new(
                    &log,
                    o!(
                        "component" => "MgdClient",
                        "peer_router" => n,
                    ),
                ),
            );

            let router = mg_admin_client::types::Router {
                asn: 65100 + u32::from(n),
                id: 65100 + u32::from(n),
                graceful_shutdown: true,
                listen: mgd.bgp_dispatcher_addr.to_string(),
            };

            println!(
                "peer router {n}: bgp ASN:                {}\n",
                router.asn,
            );

            client.create_router(&router).await?;

            peer_routers.push(mgd);
        }

        // TODO: bug causes loopbacks to hang around even after tearing down
        // the topology
        // loopback_manager.install()?;

        for n in 0..self.count {
            config
                .deployment
                .dropshot_external
                .dropshot
                .bind_address
                .set_ip("0.0.0.0".parse().unwrap());
            config
                .deployment
                .dropshot_external
                .dropshot
                .bind_address
                .set_port(0);

            config.deployment.dropshot_internal.bind_address.set_port(0);
            config.deployment.dropshot_lockstep.bind_address.set_port(0);
            config.deployment.techport_external_server_port = 0;

            println!("\nomicron-dev: setting up all services for rack {n}... ");
            let cptestctx =
                nexus_test_utils::omicron_dev_setup_with_config::<
                    omicron_nexus::Server,
                >(&mut config, 1, self.gateway_config.clone())
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
            // NOTE: The stdout strings here are not intended to be stable, but they
            // are used by the test suite.
            let addr = cptestctx.external_client.bind_address;
            println!("omicron-dev: nexus external API:     {:?}", addr);
            println!(
                "omicron-dev: nexus internal API:     {:?}",
                cptestctx.server.get_http_server_internal_address(),
            );
            println!(
                "omicron-dev: nexus lockstep API:     {:?}",
                cptestctx.server.get_http_server_lockstep_address(),
            );
            println!(
                "omicron-dev: cockroachdb pid:        {}",
                cptestctx.database.pid(),
            );
            println!(
                "omicron-dev: cockroachdb URL:        {}",
                cptestctx.database.pg_config()
            );
            println!(
                "omicron-dev: cockroachdb directory:  {}",
                cptestctx.database.temp_dir().display()
            );
            println!(
                "omicron-dev: clickhouse native addr: {}",
                cptestctx.clickhouse.native_address(),
            );
            println!(
                "omicron-dev: clickhouse http addr:   {}",
                cptestctx.clickhouse.http_address(),
            );
            println!(
                "omicron-dev: internal DNS HTTP:      http://{}",
                cptestctx.internal_dns.dropshot_server.local_addr()
            );
            println!(
                "omicron-dev: internal DNS:           {}",
                cptestctx.internal_dns.dns_server.local_address()
            );
            println!(
                "omicron-dev: external DNS name:      {}",
                cptestctx.external_dns_zone_name,
            );
            println!(
                "omicron-dev: external DNS HTTP:      http://{}",
                cptestctx.external_dns.dropshot_server.local_addr()
            );
            println!(
                "omicron-dev: external DNS:           {}",
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
                    "omicron-dev: management gateway:     {} ({})",
                    gateway.client.baseurl(),
                    location,
                );
            }
            for (location, dendrite) in
                cptestctx.dendrite.read().unwrap().iter()
            {
                println!(
                    "omicron-dev: dendrite:               http://[::1]:{} ({})",
                    dendrite.port, location,
                );
            }
            for (location, lldpd) in &cptestctx.lldpd {
                println!(
                    "omicron-dev: lldp:                   http://[::1]:{} ({})",
                    lldpd.port, location,
                );
            }
            for (location, mgd) in &cptestctx.mgd {
                println!(
                    "omicron-dev: mgd api:                http://[::1]:{} ({})",
                    mgd.port, location,
                );

                println!(
                    "omicron-dev: mgd bgp-dispatcher:     tcp {} ({})",
                    mgd.bgp_dispatcher_addr, location,
                );
            }
            println!(
                "omicron-dev: silo name:              {}",
                cptestctx.silo_name,
            );
            println!(
                "omicron-dev: privileged user name:   {}",
                cptestctx.user_name.as_ref(),
            );
            println!(
                "omicron-dev: privileged password:    {}",
                cptestctx.password
            );
            contexts.push(cptestctx);
        }

        // Wait for a signal.
        let caught_signal = signal_stream.next().await;
        assert_eq!(caught_signal.unwrap(), SIGINT);
        eprintln!(
            "omicron-dev: caught signal, shutting down and removing \
             temporary directory"
        );

        for context in contexts {
            context.teardown().await;
        }

        for mut router in peer_routers {
            if let Err(e) = router.cleanup().await {
                eprintln!("error cleaning up peer router: {e}")
            }
        }

        // loopback_manager.uninstall();

        Ok(())
    }
}
