// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use dns_service_client::{
    types::{DnsKv, DnsRecord, DnsRecordKey, Srv},
    Client,
};
use slog::{Drain, Logger};
use std::net::Ipv6Addr;

#[derive(Debug, Parser)]
#[clap(name = "dnsadm", about = "Administer DNS records")]
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
    name: String,
    #[clap(action)]
    addr: Ipv6Addr,
}

#[derive(Debug, Args)]
struct AddSRVCommand {
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
            todo!(); // XXX-dap
            // let records = client.dns_records_list().await?;
            // println!("{:#?}", records);
        }
        SubCommand::AddAAAA(cmd) => {
            todo!(); // XXX-dap
            //client
            //    .dns_records_create(&vec![DnsKv {
            //        key: DnsRecordKey { name: cmd.name },
            //        records: vec![DnsRecord::Aaaa(cmd.addr)],
            //    }])
            //    .await?;
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
    // Ok(())
}

fn init_logger() -> Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_envlogger::new(drain).fuse();
    let drain = slog_async::Async::new(drain).chan_size(0x2000).build().fuse();
    slog::Logger::root(drain, slog::o!())
}
