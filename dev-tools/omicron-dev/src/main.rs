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
use nexus_config::NexusConfig;
use nexus_test_interface::NexusServer;
use nexus_test_utils::resource_helpers::DiskTest;
use signal_hook_tokio::Signals;
use sled_agent_types::early_networking::SwitchSlot;
use slog::{Drain, o};
use std::{
    collections::BTreeMap,
    fs,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6},
    sync::{Arc, Mutex},
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
        for (switch_slot, gateway) in &cptestctx.gateway {
            println!(
                "omicron-dev: management gateway:     {} ({:?})",
                gateway.client.baseurl(),
                switch_slot,
            );
        }
        for (location, dendrite) in cptestctx.dendrite.read().unwrap().iter() {
            println!(
                "omicron-dev: dendrite:               http://[::1]:{} ({:?})",
                dendrite.port, location,
            );
        }
        for (location, lldpd) in &cptestctx.lldpd {
            println!(
                "omicron-dev: lldp:                   http://[::1]:{} ({:?})",
                lldpd.port, location,
            );
        }
        for (location, mgd) in &cptestctx.mgd {
            println!(
                "omicron-dev: maghemite:              http://[::1]:{} ({:?})",
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

fn slot_index(slot: SwitchSlot) -> u32 {
    match slot {
        SwitchSlot::Switch0 => 0,
        SwitchSlot::Switch1 => 1,
    }
}

fn ipv4_as_u32(a: u8, b: u8, c: u8, d: u8) -> u32 {
    (u32::from(a) << 24)
        | (u32::from(b) << 16)
        | (u32::from(c) << 8)
        | u32::from(d)
}

fn make_neighbor(
    local_asn: u32,
    name: String,
    peer_addr: SocketAddr,
    src_addr: Option<IpAddr>,
) -> mg_admin_client::types::Neighbor {
    mg_admin_client::types::Neighbor {
        asn: local_asn,
        name,
        group: "omicron-dev".to_string(),
        host: peer_addr.to_string(),
        hold_time: 6000,
        keepalive: 2000,
        connect_retry: 3000,
        idle_hold_time: 0,
        delay_open: 0,
        resolution: 100,
        passive: false,
        enforce_first_as: false,
        deterministic_collision_resolution: true,
        communities: vec![],
        ipv4_unicast: Some(mg_admin_client::types::Ipv4UnicastConfig {
            import_policy:
                mg_admin_client::types::ImportExportPolicy4::NoFiltering,
            export_policy:
                mg_admin_client::types::ImportExportPolicy4::NoFiltering,
            nexthop: None,
        }),
        ipv6_unicast: None,
        md5_auth_key: None,
        min_ttl: None,
        remote_asn: None,
        local_pref: None,
        multi_exit_discriminator: None,
        connect_retry_jitter: None,
        idle_hold_jitter: None,
        src_addr,
        src_port: None,
        vlan_id: None,
    }
}

async fn setup_bgp_peering(
    log: &slog::Logger,
    peer_routers: &[omicron_test_utils::dev::maghemite::MgdInstance],
    contexts: &[nexus_test_utils::ControlPlaneTestContext<
        omicron_nexus::Server,
    >],
) -> Result<(), anyhow::Error> {
    for (rack_n, ctx) in contexts.iter().enumerate() {
        for (slot, rack_mgd) in &ctx.mgd {
            let slot_idx = slot_index(*slot);
            let rack_asn = 65200 + 2 * rack_n as u32 + slot_idx;
            let rack_id = ipv4_as_u32(127, 2, rack_n as u8, slot_idx as u8);

            let rack_api_addr =
                SocketAddrV6::new(Ipv6Addr::LOCALHOST, rack_mgd.port, 0, 0);
            let rack_client = mg_admin_client::Client::new(
                &format!("http://{rack_api_addr}"),
                slog::Logger::new(
                    log,
                    slog::o!(
                        "component" => "MgdClient",
                        "rack" => rack_n,
                        "slot" => format!("{:?}", slot),
                    ),
                ),
            );

            rack_client
                .create_router(&mg_admin_client::types::Router {
                    asn: rack_asn,
                    id: rack_id,
                    graceful_shutdown: false,
                    listen: rack_mgd.bgp_dispatcher_addr.to_string(),
                })
                .await
                .with_context(|| {
                    format!("create_router for rack {rack_n} {slot:?}")
                })?;

            for (peer_n, peer_mgd) in peer_routers.iter().enumerate() {
                let peer_asn = 65100 + peer_n as u32;

                let peer_api_addr =
                    SocketAddrV6::new(Ipv6Addr::LOCALHOST, peer_mgd.port, 0, 0);
                let peer_client = mg_admin_client::Client::new(
                    &format!("http://{peer_api_addr}"),
                    slog::Logger::new(
                        log,
                        slog::o!(
                            "component" => "MgdClient",
                            "peer_router" => peer_n,
                        ),
                    ),
                );

                // rack mgd → peer router
                // src_addr ensures the outgoing connection uses the rack
                // mgd's own loopback IP as the source, so the peer router
                // dispatcher can match the incoming connection to the right
                // session (keyed by the rack mgd's IP).
                rack_client
                    .create_neighbor(&make_neighbor(
                        rack_asn,
                        format!("peer-router-{peer_n}"),
                        peer_mgd.bgp_dispatcher_addr,
                        Some(rack_mgd.bgp_dispatcher_addr.ip()),
                    ))
                    .await
                    .with_context(|| {
                        format!(
                            "rack {rack_n} {slot:?} create_neighbor \
                             peer-router-{peer_n}"
                        )
                    })?;

                // peer router → rack mgd
                // src_addr ensures the outgoing connection uses the peer
                // router's own loopback IP as the source, so the rack mgd
                // dispatcher can match the incoming connection to the right
                // session (keyed by the peer router's IP).
                peer_client
                    .create_neighbor(&make_neighbor(
                        peer_asn,
                        format!("rack-{rack_n}-{slot:?}"),
                        rack_mgd.bgp_dispatcher_addr,
                        Some(peer_mgd.bgp_dispatcher_addr.ip()),
                    ))
                    .await
                    .with_context(|| {
                        format!(
                            "peer-router-{peer_n} create_neighbor \
                             rack-{rack_n}-{slot:?}"
                        )
                    })?;
            }
        }
    }
    Ok(())
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

        // Disable physical disk adoption: DiskTest manages disks explicitly,
        // and the adoption task (period = 30 s) races with DiskTest::new when
        // setup takes ~30 s per rack. Unit tests disable this for the same
        // reason via config.test.toml.
        config.pkg.background_tasks.physical_disk_adoption.disable = true;

        let mut contexts = vec![];

        let mut peer_routers = vec![];
        let mut peer_router_loopback_allocations = vec![];

        let loopback_manager = Arc::new(Mutex::new(
            loopback_ip_mgr::LoopbackIpManager::new("lo0", log.clone()),
        ));

        for n in 0..self.peer_routers {
            let mgd_bgp_addr =
                SocketAddr::new(Ipv4Addr::new(127, 0, n, 1).into(), 1049);

            // Allocate the loopback IP for this peer router's BGP listener.
            // 127.0.0.1 (n == 0) is always present and treated as a no-op by
            // the manager; addresses for n > 0 are added to the interface and
            // removed when the allocation is dropped.
            let alloc = loopback_ip_mgr::LoopbackIpManager::allocate(
                loopback_manager.clone(),
                &[mgd_bgp_addr.ip()],
            )
            .expect("allocate loopback IP for peer router BGP listener");
            peer_router_loopback_allocations.push(alloc);

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

            // Assign unique loopback IPs to this rack's pair of mgd BGP
            // dispatchers so they can coexist with other rack instances and
            // with the peer routers above.
            let mut mgd_bgp_addrs = BTreeMap::new();
            mgd_bgp_addrs
                .insert(SwitchSlot::Switch0, Ipv4Addr::new(127, 2, n, 0));
            mgd_bgp_addrs
                .insert(SwitchSlot::Switch1, Ipv4Addr::new(127, 2, n, 1));

            println!("\nomicron-dev: setting up all services for rack {n}... ");
            let cptestctx =
                nexus_test_utils::omicron_dev_setup_with_bgp_loopbacks::<
                    omicron_nexus::Server,
                >(
                    &mut config,
                    1,
                    self.gateway_config.clone(),
                    loopback_manager.clone(),
                    mgd_bgp_addrs,
                )
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
                    "omicron-dev: management gateway:     {} ({:?})",
                    gateway.client.baseurl(),
                    location,
                );
            }
            for (location, dendrite) in
                cptestctx.dendrite.read().unwrap().iter()
            {
                println!(
                    "omicron-dev: dendrite:               http://[::1]:{} ({:?})",
                    dendrite.port, location,
                );
            }
            for (location, lldpd) in &cptestctx.lldpd {
                println!(
                    "omicron-dev: lldp:                   http://[::1]:{} ({:?})",
                    lldpd.port, location,
                );
            }
            for (location, mgd) in &cptestctx.mgd {
                println!(
                    "omicron-dev: mgd api:                http://[::1]:{} ({:?})",
                    mgd.port, location,
                );

                println!(
                    "omicron-dev: mgd bgp-dispatcher:     tcp {} ({:?})",
                    mgd.bgp_dispatcher_addr, location,
                );

                if let Some(dir) = &mgd.data_dir {
                    println!(
                        "omicron-dev: mgd tmp dir:            {} ({:?})",
                        dir.display(),
                        location,
                    );
                }
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

        if self.peer_routers > 0 {
            setup_bgp_peering(&log, &peer_routers, &contexts)
                .await
                .context("setting up BGP peering")?;
            println!("\nomicron-dev: BGP peering configured.\n");
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

        // Loopback allocations for both peer routers and rack mgd instances
        // are released here. The rack allocations were already released by the
        // ControlPlaneTestContext teardown above (via MgdInstance drop), so
        // this only actively removes the peer router addresses.
        drop(peer_router_loopback_allocations);

        Ok(())
    }
}
