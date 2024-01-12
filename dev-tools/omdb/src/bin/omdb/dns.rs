// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that query DNS

use crate::Omdb;

use anyhow::bail;
use clap::Args;
use clap::Subcommand;
use clap::ValueEnum;
use dns_service_client::{
    types::{DnsRecord, Srv},
    Client
};
use internal_dns::ServiceName;
use slog::{Logger, info, warn};
use std::collections::BTreeMap;
use std::net::Ipv6Addr;

#[derive(Debug, Args)]
pub struct DnsArgs {
    #[command(subcommand)]
    command: DnsCommands,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum SortOrder {
    /// Sort the records by service
    ByService,

    /// Sorts the records by address
    ByAddress,
}

/// Subcommands that query internal DNS servers
#[derive(Debug, Subcommand)]
enum DnsCommands {
    /// List all DNS records
    List {
        #[clap(value_enum, default_value_t=SortOrder::ByService)]
        order: SortOrder,
    }
}

impl DnsArgs {
    async fn list_by_dns(
        &self,
        omdb: &Omdb,
        order: SortOrder,
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

        let mut srvs: BTreeMap<ServiceName, Vec<Srv>> = BTreeMap::new();
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
                        let entry = srvs.entry(service_name);
                        let targets = entry.or_insert(vec![]);
                        targets.push(srv.clone());
                    }
                }
            }
        }

        // Convert the results to this "Output" struct we can manipulate output
        // separately from performing the lookup
        struct Output {
            service_kind: String,
            address: String,
            port: u16,
        }
        let mut outputs = vec![];
        for (service, srvs) in &srvs {
            for srv in srvs {
                let service_kind = service.service_kind().to_string();
                let address = aaaas.get(&srv.target)
                    .map(|address| address.to_string())
                    .unwrap_or_else(|| "Unknown".to_string());
                let port = srv.port;

                outputs.push(
                    Output {
                        service_kind,
                        address,
                        port,
                    }
                );
            }
        }

        match order {
            SortOrder::ByService => {
                outputs.sort_by(|a, b| {
                    a.service_kind.cmp(&b.service_kind)
                });
            },
            SortOrder::ByAddress => {
                outputs.sort_by(|a, b| {
                    match a.address.cmp(&b.address) {
                        std::cmp::Ordering::Equal => a.port.cmp(&b.port),
                        other => other,
                    }
                });
            },
        }

        println!("DNS-Specific Info:");
        println!("generation {}", config.generation);
        println!("   created {}", config.time_created);
        println!("   applied {}", config.time_applied);

        println!("{:<15} {:<25} {:<10}", "SERVICE", "ADDRESS", "PORT");

        for output in &outputs {
            let Output { service_kind, address, port } = output;
            println!("{service_kind:<15} {address:<25} {port:<10}");
        }
        Ok(())
    }

    pub async fn run_cmd(
        &self,
        omdb: &Omdb,
        log: &Logger,
    ) -> anyhow::Result<()> {
        match &self.command {
            DnsCommands::List { order } => {
                self.list_by_dns(omdb, *order, log).await?;
            }
        }
        Ok(())
    }
}
