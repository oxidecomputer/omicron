// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Executable program to run a simulated sled agent

use anyhow::Context;
use camino::Utf8PathBuf;
use clap::Parser;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use omicron_common::api::external::Name;
use omicron_common::api::external::UserId;
use omicron_common::api::internal::nexus::Certificate;
use omicron_common::cmd::CmdError;
use omicron_common::cmd::fatal;
// TODO-RAINCLAUDE: linking the instrumentation crate is what lets an Antithesis build load libvoidstar for coverage.
#[cfg(feature = "antithesis")]
use antithesis_instrumentation as _;
use omicron_sled_agent::sim::InternalDnsConfig;
use omicron_sled_agent::sim::RssArgs;
use omicron_sled_agent::sim::{
    Config, ConfigHardware, ConfigStorage, ConfigZpool, SimMode, ZpoolConfig,
    run_standalone_server,
};
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use oxnet::Ipv6Net;
use sled_agent_types::inventory::ZpoolHealth;
use sled_hardware_types::{Baseboard, SledCpuFamily};
use std::net::SocketAddr;
use std::net::SocketAddrV6;

fn parse_sim_mode(src: &str) -> Result<SimMode, String> {
    match src {
        "auto" => Ok(SimMode::Auto),
        "explicit" => Ok(SimMode::Explicit),
        mode => Err(format!("Invalid sim mode: {}", mode)),
    }
}

#[derive(Debug, Parser)]
#[clap(name = "sled_agent", about = "See README.adoc for more information")]
struct Args {
    #[clap(
        long = "sim-mode",
        value_parser = parse_sim_mode,
        default_value = "auto",
        help = "Automatically simulate transitions",
    )]
    sim_mode: SimMode,

    #[clap(name = "SA_UUID", action)]
    uuid: SledUuid,

    #[clap(name = "SA_IP:PORT", action)]
    sled_agent_addr: SocketAddrV6,

    #[clap(name = "NEXUS_IP:PORT", action)]
    nexus_addr: SocketAddr,

    #[clap(action)]
    nexus_lockstep_port: u16,

    #[clap(long, name = "NEXUS_EXTERNAL_IP:PORT", action)]
    /// If specified, when the simulated sled agent initializes the rack, it
    /// will record the Nexus service running with the specified external IP
    /// address.  When combined with `EXTERNAL_DNS_INTERNAL_IP:PORT`, this will
    /// cause Nexus to publish DNS names to external DNS.
    rss_nexus_external_addr: Option<SocketAddr>,

    // TODO-RAINCLAUDE: must equal `deployment.id` in the Nexus config, or Nexus never finds itself in the initial blueprint and never opens its external API.
    #[clap(long, name = "NEXUS_ZONE_ID", action)]
    rss_nexus_id: Option<OmicronZoneUuid>,

    #[clap(long, name = "EXTERNAL_DNS_INTERNAL_IP:PORT", action)]
    /// If specified, when the simulated sled agent initializes the rack, it
    /// will record the external DNS service running with the specified internal
    /// IP address.  When combined with `NEXUS_EXTERNAL_IP:PORT`, this will cause
    /// Nexus to publish DNS names to external DNS.
    rss_external_dns_internal_addr: Option<SocketAddrV6>,

    #[clap(long, name = "INTERNAL_DNS_INTERNAL_IP:PORT", action)]
    /// If specified, the sled agent will create a DNS server exposing the
    /// following socket address for the DNS interface.
    rss_internal_dns_dns_addr: Option<SocketAddrV6>,

    // TODO-RAINCLAUDE: with this set, no DNS server is started; the one at this HTTP address is populated instead, and INTERNAL_DNS_INTERNAL_IP:PORT names where it serves DNS.
    #[clap(
        long,
        name = "INTERNAL_DNS_HTTP_IP:PORT",
        requires = "INTERNAL_DNS_INTERNAL_IP:PORT",
        action
    )]
    rss_internal_dns_http_addr: Option<SocketAddrV6>,

    // TODO-RAINCLAUDE: repeatable; each CockroachDB node is recorded in internal DNS and the initial blueprint.
    #[clap(long = "cockroach-addr", name = "COCKROACH_IP:PORT", action = clap::ArgAction::Append)]
    cockroach_addrs: Vec<SocketAddrV6>,

    // TODO-RAINCLAUDE: defaults to the /56 containing SA_IP.
    #[clap(long, name = "RACK_SUBNET", action)]
    rack_subnet: Option<Ipv6Net>,

    #[clap(long, name = "RECOVERY_SILO_NAME", action)]
    rss_recovery_silo_name: Option<Name>,

    // TODO-RAINCLAUDE: the recovery user's password is always "oxide".
    #[clap(long, name = "RECOVERY_USER_NAME", action)]
    rss_recovery_user_name: Option<UserId>,

    #[clap(long, name = "TLS_CERT_PEM_FILE", action)]
    /// If this flag and TLS_KEY_PEM_FILE are specified, when the simulated sled
    /// agent initializes the rack, the specified certificate and private keys
    /// will be provided for the initial TLS certificates for the recovery silo.
    rss_tls_cert: Option<Utf8PathBuf>,

    #[clap(long, name = "TLS_KEY_PEM_FILE", action)]
    /// If this flag and TLS_CERT_PEM_FILE are specified, when the simulated sled
    /// agent initializes the rack, the specified certificate and private keys
    /// will be provided for the initial TLS certificates for the recovery silo.
    rss_tls_key: Option<Utf8PathBuf>,
}

fn main() {
    if let Err(message) = oxide_tokio_rt::run(do_run()) {
        fatal(message);
    }
}

async fn do_run() -> Result<(), CmdError> {
    let args = Args::parse();

    let config = Config {
        dropshot: ConfigDropshot {
            bind_address: args.sled_agent_addr.into(),
            // TODO-RAINCLAUDE: dropshot's default of 1 KiB rejects every sled config and firewall rule push from Nexus; this matches `Config::for_testing`.
            default_request_body_max_bytes: 1024 * 1024,
            ..Default::default()
        },
        storage: ConfigStorage {
            // Create 10 "virtual" U.2s, with 1 TB of storage.
            zpools: vec![
                ConfigZpool {
                    size: 1 << 40,
                    health: ZpoolHealth::Online
                };
                10
            ],
            ip: (*args.sled_agent_addr.ip()).into(),
        },
        hardware: ConfigHardware {
            hardware_threads: 32,
            physical_ram: 64 * (1 << 30),
            reservoir_ram: 32 * (1 << 30),
            cpu_family: SledCpuFamily::AmdMilan,
            baseboard: Baseboard::Gimlet {
                identifier: format!("sim-{}", args.uuid),
                model: String::from(sp_sim::FAKE_GIMLET_MODEL),
                revision: 3,
            },
        },
        ..Config::for_testing(
            args.uuid,
            args.sim_mode,
            Some(args.nexus_addr),
            ZpoolConfig::TenVirtualU2s,
            SledCpuFamily::AmdMilan,
        )
    };

    let tls_certificate = match (args.rss_tls_cert, args.rss_tls_key) {
        (None, None) => None,
        (Some(cert_path), Some(key_path)) => {
            let cert_bytes = std::fs::read_to_string(&cert_path)
                .with_context(|| format!("read {:?}", cert_path))
                .map_err(CmdError::Failure)?;
            let key_bytes = std::fs::read_to_string(&key_path)
                .with_context(|| format!("read {:?}", key_path))
                .map_err(CmdError::Failure)?;
            Some(Certificate { cert: cert_bytes, key: key_bytes })
        }
        _ => {
            return Err(CmdError::Usage(String::from(
                "--rss-tls-key and --rss-tls-cert must be specified together",
            )));
        }
    };

    // TODO-RAINCLAUDE: clap's `requires` already rejects an HTTP address without a DNS address, but that is not visible to the type system, so the usage error is spelled out here as well.
    let internal_dns =
        match (args.rss_internal_dns_http_addr, args.rss_internal_dns_dns_addr)
        {
            (Some(http_addr), Some(dns_addr)) => {
                InternalDnsConfig::External { http_addr, dns_addr }
            }
            (None, dns_addr) => InternalDnsConfig::InProcess { dns_addr },
            (Some(_), None) => {
                return Err(CmdError::Usage(String::from(
                    "--rss-internal-dns-http-addr requires \
                     --rss-internal-dns-dns-addr (Nexus must be told where \
                     the external DNS server serves DNS)",
                )));
            }
        };

    let rss_args = RssArgs {
        nexus_external_addr: args.rss_nexus_external_addr,
        nexus_id: args.rss_nexus_id,
        external_dns_internal_addr: args.rss_external_dns_internal_addr,
        internal_dns,
        cockroach_addrs: args.cockroach_addrs,
        rack_subnet: args.rack_subnet,
        recovery_silo_name: args.rss_recovery_silo_name,
        recovery_user_name: args.rss_recovery_user_name,
        tls_certificate,
    };

    let config_logging =
        ConfigLogging::StderrTerminal { level: ConfigLoggingLevel::Info };
    run_standalone_server(
        &config,
        args.nexus_lockstep_port,
        &config_logging,
        &rss_args,
    )
    .await
    .map_err(CmdError::Failure)
}
