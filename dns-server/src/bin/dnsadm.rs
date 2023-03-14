// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Basic CLI client for the DNS server, using the Dropshot interface to inspect
//! and update the configuration
//!
//! Note that writes in production are not supported as Nexus is the source of
//! truth for these updates.

use anyhow::Context;
use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use dns_service_client::{
    types::{
        DnsConfigParams, DnsConfigZone, DnsKv, DnsRecord, DnsRecordKey, Srv,
    },
    Client,
};
use slog::{Drain, Logger};
use std::iter::once;
use std::net::Ipv6Addr;

#[derive(Debug, Parser)]
#[clap(name = "dnsadm", about = "Administer DNS records (for testing only)")]
struct Opt {
    #[clap(short, long, action)]
    address: Option<String>,

    #[clap(short, long, action)]
    port: Option<usize>,

    #[clap(subcommand)]
    subcommand: SubCommand,
}

#[derive(Debug, Subcommand)]
enum SubCommand {
    ListRecords,
    AddAAAA(AddAAAACommand),
    AddSRV(AddSRVCommand),
    DeleteRecord(DeleteRecordCommand),
}

#[derive(Debug, Args)]
struct AddAAAACommand {
    #[clap(action)]
    zone: String,
    #[clap(action)]
    name: String,
    #[clap(action)]
    addr: Ipv6Addr,
}

#[derive(Debug, Args)]
struct AddSRVCommand {
    #[clap(action)]
    zone: String,
    #[clap(action)]
    name: String,
    #[clap(action)]
    prio: u16,
    #[clap(action)]
    weight: u16,
    #[clap(action)]
    port: u16,
    #[clap(action)]
    target: String,
}

#[derive(Debug, Args)]
struct DeleteRecordCommand {
    #[clap(action)]
    zone: String,
    #[clap(action)]
    name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::parse();
    let log = init_logger();

    let addr = match opt.address {
        Some(a) => a,
        None => "localhost".into(),
    };
    let port = opt.port.unwrap_or(5353);

    let endpoint = format!("http://{}:{}", addr, port);
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

                for kv in &zone_config.records {
                    println!("    key {:?}:", kv.key.name);
                    for record in &kv.records {
                        match record {
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
                        }
                    }
                }
            }
        }
        SubCommand::AddAAAA(cmd) => {
            let config = client.dns_config_get().await?.into_inner();
            let (our_zone, other_zones): (Vec<_>, Vec<_>) =
                config.zones.into_iter().partition(|z| z.zone_name == cmd.zone);
            let our_records = our_zone
                .into_iter()
                .next()
                .map(|z| z.records)
                .unwrap_or_else(Vec::new);
            let (our_kv, other_kvs): (Vec<_>, Vec<_>) =
                our_records.into_iter().partition(|kv| kv.key.name == cmd.name);
            let mut our_kv =
                our_kv.into_iter().next().unwrap_or_else(|| DnsKv {
                    key: DnsRecordKey { name: cmd.name },
                    records: Vec::new(),
                });
            our_kv.records.push(DnsRecord::Aaaa(cmd.addr));

            let new_config = DnsConfigParams {
                generation: config.generation + 1,
                time_created: chrono::Utc::now(),
                zones: other_zones
                    .into_iter()
                    .chain(once(DnsConfigZone {
                        zone_name: cmd.zone,
                        records: other_kvs
                            .into_iter()
                            .chain(once(our_kv))
                            .collect(),
                    }))
                    .collect(),
            };

            client.dns_config_put(&new_config).await.context("updating DNS")?;
        }
        SubCommand::AddSRV(cmd) => {
            todo!(); // XXX-dap
                     //client
                     //    .dns_records_create(&vec![DnsKv {
                     //        key: DnsRecordKey { name: cmd.name },
                     //        records: vec![DnsRecord::Srv(Srv {
                     //            prio: cmd.prio,
                     //            weight: cmd.weight,
                     //            port: cmd.port,
                     //            target: cmd.target,
                     //        })],
                     //    }])
                     //    .await?;
        }
        SubCommand::DeleteRecord(cmd) => {
            todo!();
            //client
            //    .dns_records_delete(&vec![DnsRecordKey { name: cmd.name }])
            //    .await?;
        }
    }

    // XXX-dap
    Ok(())
}

fn init_logger() -> Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_envlogger::new(drain).fuse();
    let drain = slog_async::Async::new(drain).chan_size(0x2000).build().fuse();
    slog::Logger::root(drain, slog::o!())
}
