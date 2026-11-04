// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use camino::Utf8PathBuf;
use clap::Parser;
use futures::StreamExt;
use gateway_client::ClientInfo as _;
use gateway_test_utils::setup::DEFAULT_SP_SIM_CONFIG;
use libc::SIGINT;
use nexus_test_interface::NexusServer;
use nexus_test_utils::resource_helpers::DiskTest;
use omicron_common::address::BGP_LISTEN_PORT;
use omicron_dev_lib::config::{Configurable, read_config, start_stream};
use sled_agent_types::early_networking::SwitchSlot;
use slog::{Drain, o};
use std::{
    collections::BTreeMap,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6},
    sync::{Arc, Mutex},
};

const DEFAULT_NEXUS_CONFIG: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../../nexus/examples/config.toml");

/// Autonomous System Number used for BGP configuration.  We will increment this
/// number for each peer router participating in a unique BGP Autonomous System.
const BGP_BASE_ASN: u32 = 65100;

fn main() -> anyhow::Result<()> {
    oxide_tokio_rt::run(async {
        let args = JborArgs::parse();
        args.exec().await
    })
}

/// Number of simulated racks (1–100).
#[derive(Clone, Copy, Debug)]
struct RackCount(u8);

impl std::str::FromStr for RackCount {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let n: u8 =
            s.parse().map_err(|e| format!("invalid rack count: {e}"))?;
        if n < 1 || n > 100 {
            return Err(format!(
                "rack count must be between 1 and 100, got {n}"
            ));
        }
        Ok(RackCount(n))
    }
}

impl std::fmt::Display for RackCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Number of simulated peer routers (0–100).  Peer routers are assigned BGP
/// ASNs from the range 65100–65199 (one per peer). Limiting to 100 prevents us
/// from running out of ASNs.
#[derive(Clone, Copy, Debug)]
struct PeerRouterCount(u8);

impl std::str::FromStr for PeerRouterCount {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let n: u8 =
            s.parse().map_err(|e| format!("invalid peer router count: {e}"))?;
        if n > 100 {
            return Err(format!(
                "peer router count must be between 0 and 100, got {n}"
            ));
        }
        Ok(PeerRouterCount(n))
    }
}

impl std::fmt::Display for PeerRouterCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Spin up N simulated Oxide racks for integration testing.
///
/// Requires `pfexec` / root (zone and network privileges are needed).
#[derive(Clone, Debug, Parser)]
#[clap(version)]
struct JborArgs {
    /// Override the gateway server configuration file.
    #[clap(long, default_value = DEFAULT_SP_SIM_CONFIG)]
    gateway_config: Utf8PathBuf,
    /// Override the nexus configuration file.
    #[clap(long, default_value = DEFAULT_NEXUS_CONFIG)]
    nexus_config: Utf8PathBuf,
    /// Number of "racks" to launch (1–100)
    #[clap(long, default_value_t = RackCount(1))]
    count: RackCount,
    /// Launch peer router mgd instances (0–100)
    #[clap(long, default_value_t = PeerRouterCount(0))]
    peer_routers: PeerRouterCount,
}

impl Configurable for JborArgs {
    fn nexus_config(&self) -> &Utf8PathBuf {
        &self.nexus_config
    }
}

impl JborArgs {
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

        for n in 0..self.peer_routers.0 {
            let mgd_bgp_addr = SocketAddr::new(
                Ipv4Addr::new(100, 64, 0, n + 1).into(),
                BGP_LISTEN_PORT,
            );

            // Allocate the loopback IP for this peer router's BGP listener.
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
            .context("failed to start peer router mgd")?;

            println!(
                "peer router {n}: mgd api:                http://[::1]:{}",
                mgd.port
            );

            println!(
                "peer router {n}: mgd bgp-dispatcher:     tcp {mgd_bgp_addr}",
            );

            if let Some(dir) = &mgd.data_dir {
                println!("peer router {n} tmp dir:                 {dir}",);
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

            let router = mg_api_types::bgp::config::Router {
                asn: BGP_BASE_ASN + u32::from(n),
                id: BGP_BASE_ASN + u32::from(n),
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

        for n in 0..self.count.0 {
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

            // Assign unique IPs to this rack's pair of mgd BGP dispatchers so
            // they can coexist with other rack instances and with the peer
            // routers above. Using 100.64.x.x (RFC 6598) ensures the Nexus API
            // (which rejects loopback addresses) can reference these as BGP
            // peer addresses.

            let mut mgd_bgp_addrs = BTreeMap::new();
            mgd_bgp_addrs
                .insert(SwitchSlot::Switch0, Ipv4Addr::new(100, 64, n + 1, 0));
            mgd_bgp_addrs
                .insert(SwitchSlot::Switch1, Ipv4Addr::new(100, 64, n + 1, 1));

            println!("\njbor: setting up all services for rack {n}... ");
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

            println!("jbor: Adding disks to first sled agent");

            // This is how our integration tests are identifying that "disks exist"
            // within the database.
            //
            // This inserts:
            // - DEFAULT_ZPOOL_COUNT zpools, each of which contains:
            //   - A crucible dataset
            //   - A debug dataset
            DiskTest::new(&cptestctx).await;

            println!("jbor: services are running.");

            // Print out basic information about what was started.
            let addr = cptestctx.external_client.bind_address;
            println!("jbor: nexus external API:     {:?}", addr);
            println!(
                "jbor: nexus internal API:     {:?}",
                cptestctx.server.get_http_server_internal_address(),
            );
            println!(
                "jbor: nexus lockstep API:     {:?}",
                cptestctx.server.get_http_server_lockstep_address(),
            );
            println!(
                "jbor: cockroachdb pid:        {}",
                cptestctx.database.pid(),
            );
            println!(
                "jbor: cockroachdb URL:        {}",
                cptestctx.database.pg_config()
            );
            println!(
                "jbor: cockroachdb directory:  {}",
                cptestctx.database.temp_dir()
            );
            println!(
                "jbor: clickhouse native addr: {}",
                cptestctx.clickhouse.native_address(),
            );
            println!(
                "jbor: clickhouse http addr:   {}",
                cptestctx.clickhouse.http_address(),
            );
            println!(
                "jbor: internal DNS HTTP:      http://{}",
                cptestctx.internal_dns.dropshot_server.local_addr()
            );
            println!(
                "jbor: internal DNS:           {}",
                cptestctx
                    .internal_dns
                    .dns_server
                    .sole_local_address()
                    .context("Expected exactly 1 internal DNS address")?
            );
            println!(
                "jbor: external DNS name:      {}",
                cptestctx.external_dns_zone_name,
            );
            println!(
                "jbor: external DNS HTTP:      http://{}",
                cptestctx.external_dns.dropshot_server.local_addr()
            );
            for addr in cptestctx.external_dns.dns_server.local_addresses() {
                println!("jbor: external DNS:           {}", addr,);
                println!(
                    "jbor:   e.g. `dig @{} -p {} {}.sys.{}`",
                    addr.ip(),
                    addr.port(),
                    cptestctx.silo_name,
                    cptestctx.external_dns_zone_name,
                );
            }
            for (location, gateway) in &cptestctx.gateway {
                println!(
                    "jbor: management gateway:     {} ({:?})",
                    gateway.client.baseurl(),
                    location,
                );
            }
            for (location, dendrite) in
                cptestctx.dendrite.read().unwrap().iter()
            {
                println!(
                    "jbor: dendrite:               http://[::1]:{} ({:?})",
                    dendrite.port(),
                    location,
                );
            }
            for (location, mgd) in &cptestctx.mgd {
                println!(
                    "jbor: mgd api:                http://[::1]:{} ({:?})",
                    mgd.port, location,
                );

                println!(
                    "jbor: mgd bgp-dispatcher:     tcp {} ({:?})",
                    mgd.bgp_dispatcher_addr, location,
                );

                if let Some(dir) = &mgd.data_dir {
                    println!(
                        "jbor: mgd tmp dir:            {} ({:?})",
                        dir, location,
                    );
                }
            }
            println!("jbor: silo name:              {}", cptestctx.silo_name,);
            println!(
                "jbor: privileged user name:   {}",
                cptestctx.user_name.as_ref(),
            );
            println!("jbor: privileged password:    {}", cptestctx.password);
            contexts.push(cptestctx);
        }

        // Wait for a signal.
        let caught_signal = signal_stream.next().await;
        assert_eq!(caught_signal.unwrap(), SIGINT);
        eprintln!(
            "jbor: caught signal, shutting down and removing \
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
