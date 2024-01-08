// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that query services

use crate::Omdb;

use anyhow::bail;
use clap::Args;
use clap::Subcommand;
use clap::ValueEnum;
use dns_service_client::{types::DnsRecord, Client};
use internal_dns::ServiceName;
use slog::{Logger, info, warn};
use std::collections::BTreeMap;
use std::net::Ipv6Addr;

#[derive(Debug, Args)]
pub struct ServiceArgs {
    /// URL of the Nexus internal API
    #[clap(long, env("OMDB_NEXUS_URL"))]
    nexus_internal_url: Option<String>,

    #[command(subcommand)]
    command: ServiceCommands,
}

#[derive(Clone, Debug, ValueEnum)]
enum ServiceSource {
    /// Ask DNS for records of services
    ///
    /// This collects SRV records and associated AAAA records
    /// to construct a view of the services on the rack.
    Dns,

    /// Ask Nexus for records of services
    ///
    /// This queries the Nexus internal API to ask for service
    /// records.
    Nexus
}

/// Subcommands that query oximeter collector state
#[derive(Debug, Subcommand)]
enum ServiceCommands {
    /// List services
    List {
        #[arg(long = "source")]
        source: ServiceSource,
    }
}

impl ServiceArgs {
    async fn list_by_dns(
        &self,
        omdb: &Omdb,
        log: &Logger,
    ) -> anyhow::Result<()> {
        let addrs = omdb
            .dns_lookup_all(
                log.clone(),
                internal_dns::ServiceName::InternalDns,
            )
            .await?;
        let addr = addrs.into_iter().next().expect(
            "expected at least one DNS address from \
            successful DNS lookup",
        );
        let dns_server_url = format!("http://{}", addr);
        info!(log, "Communicating with internal DNS server: {dns_server_url}");

        let client = Client::new(&dns_server_url, log.clone());
        let config = client.dns_config_get().await?;

        let Some(zone_config) = config.zones.iter().find(|zone_config| {
            zone_config.zone_name == internal_dns::names::DNS_ZONE
        }) else {
            bail!("Could not find DNS zone config for {}", internal_dns::names::DNS_ZONE);
        };

        let mut srvs: BTreeMap<ServiceName, Vec<String>> = BTreeMap::new();
        let mut aaaas: BTreeMap<String, Ipv6Addr> = BTreeMap::new();

        for (name, records) in &zone_config.records {
            for record in records {
                match record {
                    DnsRecord::A(addr) => {
                        warn!(log, "Ignoring A record (underlay should be IPv6 only): {addr:?}");
                    }
                    DnsRecord::Aaaa(addr) => {
                        let full_aaaa_name = [name, internal_dns::names::DNS_ZONE].join(".");
                        aaaas.insert(full_aaaa_name.to_string(), *addr);
                    }
                    DnsRecord::Srv(srv) => {
                        let Ok(service_name) = ServiceName::from_dns_name(&name) else {
                            warn!(log, "Failed to parse {name} as Service Name");
                            continue;
                        };
                        // TODO: port?
                        let entry = srvs.entry(service_name);
                        let targets = entry.or_insert(vec![]);
                        targets.push(srv.target.clone());
                    }
                }
            }
        }
        println!("DNS-Specific Info:");
        println!("generation {}", config.generation);
        println!("   created {}", config.time_created);
        println!("   applied {}", config.time_applied);

        println!("{:<15} {:<25}", "SERVICE", "ADDRESS");
        for (service, targets) in &srvs {
            for target in targets {
                let service_kind = service.service_kind();
                let address = aaaas.get(target)
                    .map(|address| address.to_string())
                    .unwrap_or_else(|| "Unknown".to_string());
                println!("{service_kind:<15} {address:<25}");
            }
        }
        Ok(())
    }

    async fn list_by_nexus(
        &self,
        _omdb: &Omdb,
        _log: &Logger,
    ) -> anyhow::Result<()> {
        todo!();
    }

    pub async fn run_cmd(
        &self,
        omdb: &Omdb,
        log: &Logger,
    ) -> anyhow::Result<()> {
        match &self.command {
            ServiceCommands::List { source } => {
                match source {
                    ServiceSource::Dns => {
                        self.list_by_dns(omdb, log).await?;
                    },
                    ServiceSource::Nexus => {
                        self.list_by_nexus(omdb, log).await?;
                    },
                }

                // TODO: Group by sleds?
                // TODO: Provide an option to lookup via Nexus?
                // TODO: Provide an option to lookup by asking sleds?


            }
        }
        Ok(())
    }
}
