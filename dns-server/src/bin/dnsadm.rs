// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Basic CLI client for our configurable DNS server
//!
//! This is primarily a development and debugging tool.  Writes have two big
//! caveats:
//!
//! - Writes are only supported to a special namespace of test zones ending in
//!   ".oxide.test" to avoid ever conflicting with a deployed server
//! - All writes involve a read-modify-write with no ability to avoid clobbering
//!   a concurrent write.

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::ensure;
use clap::{Args, Parser, Subcommand};
use dns_service_client::Client;
use internal_dns_types::config::DnsConfig;
use internal_dns_types::config::DnsConfigParams;
use internal_dns_types::config::DnsConfigZone;
use internal_dns_types::config::DnsRecord;
use internal_dns_types::config::Srv;
use slog::{Drain, Logger};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::iter::once;
use std::net::{Ipv4Addr, Ipv6Addr};

#[derive(Debug, Parser)]
#[clap(name = "dnsadm", about = "Administer DNS records (for testing only)")]
struct Opt {
    #[clap(short, long, action)]
    address: Option<String>,

    #[clap(subcommand)]
    subcommand: SubCommand,
}

#[derive(Debug, Subcommand)]
enum SubCommand {
    /// List all records in all zones operated by the DNS server
    ListRecords,
    /// Add an A record (non-transactionally) to the DNS server
    AddA(AddACommand),
    /// Add a AAAA record (non-transactionally) to the DNS server
    AddAAAA(AddAAAACommand),
    /// Add a SRV record (non-transactionally) to the DNS server
    AddSRV(AddSRVCommand),
    /// Delete all records for a name (non-transactionally) in the DNS server
    DeleteRecord(DeleteRecordCommand),
}

#[derive(Debug, Args)]
struct AddACommand {
    /// name of one of the server's DNS zones (under ".oxide.test")
    #[clap(action)]
    zone_name: String,
    /// name under which the new record should be added
    #[clap(action)]
    name: String,
    /// IP address for the new A record
    #[clap(action)]
    addr: Ipv4Addr,
}

#[derive(Debug, Args)]
struct AddAAAACommand {
    /// name of one of the server's DNS zones (under ".oxide.test")
    #[clap(action)]
    zone_name: String,
    /// name under which the new record should be added
    #[clap(action)]
    name: String,
    /// IP address for the new AAAA record
    #[clap(action)]
    addr: Ipv6Addr,
}

#[derive(Debug, Args)]
struct AddSRVCommand {
    /// name of one of the server's DNS zones (under ".oxide.test")
    #[clap(action)]
    zone_name: String,
    /// name under which the new record should be added
    #[clap(action)]
    name: String,
    /// new SRV record priority
    #[clap(action)]
    prio: u16,
    /// new SRV record weight
    #[clap(action)]
    weight: u16,
    /// new SRV record port
    #[clap(action)]
    port: u16,
    /// new SRV record target
    #[clap(action)]
    target: String,
}

#[derive(Debug, Args)]
struct DeleteRecordCommand {
    /// name of one of the server's DNS zones (under ".oxide.test")
    #[clap(action)]
    zone_name: String,
    /// name whose records should be deleted
    #[clap(action)]
    name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::parse();
    let log = init_logger();

    let addr = opt.address.unwrap_or_else(|| "localhost".to_string());

    let endpoint = format!("http://{}", addr);
    let client = Client::new(&endpoint, log.clone());

    match opt.subcommand {
        SubCommand::ListRecords => {
            let config = client.dns_config_get().await?;
            println!("generation {}", config.generation);
            println!("    created {}", config.time_created);
            println!("    applied {}", config.time_applied);
            println!("    zones:  {}", config.zones.len());

            for zone_config in &config.zones {
                println!("\nzone {:?}", zone_config.zone_name);

                // Sort the records so that we get consistent ordering.
                let names: BTreeMap<_, _> = zone_config.names.iter().collect();
                for (name, records) in names {
                    println!("    key {:?}:", name);
                    for record in records {
                        match record {
                            DnsRecord::A(addr) => {
                                println!("        A:    {:?}", addr);
                            }
                            DnsRecord::Aaaa(addr) => {
                                println!("        AAAA: {:?}", addr);
                            }
                            DnsRecord::Srv(srv) => {
                                println!("        SRV:  {}", srv.target);
                                println!("              port     {}", srv.port);
                                println!("              priority {}", srv.prio);
                                println!(
                                    "              weight   {}",
                                    srv.weight
                                );
                            }
                            DnsRecord::Ns(name) => {
                                println!("        NS: {:?}", name);
                            }
                        }
                    }
                }
            }
        }

        SubCommand::AddA(cmd) => {
            let old_config = client.dns_config_get().await?.into_inner();
            let new_config = add_record(
                old_config,
                &cmd.zone_name,
                &cmd.name,
                DnsRecord::A(cmd.addr),
            )?;
            client.dns_config_put(&new_config).await.context("updating DNS")?;
        }

        SubCommand::AddAAAA(cmd) => {
            let old_config = client.dns_config_get().await?.into_inner();
            let new_config = add_record(
                old_config,
                &cmd.zone_name,
                &cmd.name,
                DnsRecord::Aaaa(cmd.addr),
            )?;
            client.dns_config_put(&new_config).await.context("updating DNS")?;
        }

        SubCommand::AddSRV(cmd) => {
            let old_config = client.dns_config_get().await?.into_inner();
            let new_config = add_record(
                old_config,
                &cmd.zone_name,
                &cmd.name,
                DnsRecord::Srv(Srv {
                    prio: cmd.prio,
                    weight: cmd.weight,
                    port: cmd.port,
                    target: cmd.target,
                }),
            )?;
            client.dns_config_put(&new_config).await.context("updating DNS")?;
        }

        SubCommand::DeleteRecord(cmd) => {
            let old_config = client.dns_config_get().await?.into_inner();
            verify_zone_name(&cmd.zone_name)?;
            let zones = old_config
                .zones
                .into_iter()
                .map(|dns_zone| {
                    if dns_zone.zone_name != cmd.zone_name {
                        dns_zone
                    } else {
                        DnsConfigZone {
                            zone_name: dns_zone.zone_name,
                            names: dns_zone
                                .names
                                .into_iter()
                                .filter(|(name, _)| *name != cmd.name)
                                .collect(),
                        }
                    }
                })
                .collect();

            let new_config = DnsConfigParams {
                generation: old_config.generation.next(),
                serial: old_config.serial + 1,
                time_created: chrono::Utc::now(),
                zones,
            };
            client.dns_config_put(&new_config).await.context("updating DNS")?;
        }
    }

    Ok(())
}

/// Verify that the given DNS zone name provided by the user falls under the
/// ".oxide.test" name to ensure that it can never conflict with a real deployed
/// zone name.
fn verify_zone_name(zone_name: &str) -> Result<()> {
    ensure!(
        zone_name.trim_end_matches('.').ends_with(".oxide.test"),
        "zone name must be under \".oxide.test\""
    );
    Ok(())
}

fn init_logger() -> Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_envlogger::new(drain).fuse();
    let drain = slog_async::Async::new(drain).chan_size(0x2000).build().fuse();
    slog::Logger::root(drain, slog::o!())
}

fn add_record(
    config: DnsConfig,
    zone_name: &str,
    name: &str,
    record: DnsRecord,
) -> Result<DnsConfigParams> {
    verify_zone_name(zone_name)?;

    let generation = config.generation;
    let serial = config.serial.checked_add(1).ok_or_else(|| {
        anyhow!("Cannot produce new serial for {}", config.serial)
    })?;
    let (our_zone, other_zones): (Vec<_>, Vec<_>) =
        config.zones.into_iter().partition(|z| z.zone_name == zone_name);
    let our_records = our_zone
        .into_iter()
        .next()
        .map(|z| z.names)
        .unwrap_or_else(HashMap::new);
    let (our_kv, other_kvs): (Vec<_>, Vec<_>) =
        our_records.into_iter().partition(|(n, _)| n == name);
    let mut our_kv = our_kv
        .into_iter()
        .next()
        .unwrap_or_else(|| (name.to_owned(), Vec::new()));
    our_kv.1.push(record);

    Ok(DnsConfigParams {
        generation: generation.next(),
        serial,
        time_created: chrono::Utc::now(),
        zones: other_zones
            .into_iter()
            .chain(once(DnsConfigZone {
                zone_name: zone_name.to_owned(),
                names: other_kvs.into_iter().chain(once(our_kv)).collect(),
            }))
            .collect(),
    })
}
