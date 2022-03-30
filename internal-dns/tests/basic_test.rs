// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use internal_dns_client::{
    types::{DnsKv, DnsRecord, DnsRecordKey, Srv},
    Client,
};
use std::net::Ipv6Addr;
use trust_dns_resolver::config::{
    NameServerConfig, Protocol, ResolverConfig, ResolverOpts,
};
use trust_dns_resolver::TokioAsyncResolver;

#[tokio::test]
pub async fn aaaa_crud() -> Result<(), anyhow::Error> {
    let (client, resolver) = init_client_server().await?;

    // records should initially be empty
    let records = client.dns_records_get().await?;
    assert!(records.is_empty());

    // add an aaaa record
    let name = DnsRecordKey { name: "devron.system".into() };
    let addr = Ipv6Addr::new(0xfd, 0, 0, 0, 0, 0, 0, 0x1);
    let aaaa = DnsRecord::Aaaa(addr);
    client
        .dns_records_set(&vec![DnsKv {
            key: name.clone(),
            record: aaaa.clone(),
        }])
        .await?;

    // read back the aaaa record
    let records = client.dns_records_get().await?;
    assert_eq!(1, records.len());
    assert_eq!(records[0].key.name, name.name);
    match records[0].record {
        DnsRecord::Aaaa(ra) => {
            assert_eq!(ra, addr);
        }
        _ => {
            panic!("expected aaaa record")
        }
    }

    // resolve the name
    let response = resolver.lookup_ip(name.name + ".").await?;
    let address = response.iter().next().expect("no addresses returned!");
    assert_eq!(address, addr);

    Ok(())
}

#[tokio::test]
pub async fn srv_crud() -> Result<(), anyhow::Error> {
    let (client, resolver) = init_client_server().await?;

    // records should initially be empty
    let records = client.dns_records_get().await?;
    assert!(records.is_empty());

    // add a srv record
    let name = DnsRecordKey { name: "hromi.cluster".into() };
    let srv =
        Srv { prio: 47, weight: 74, port: 99, target: "outpost47".into() };
    let rec = DnsRecord::Srv(srv.clone());
    client
        .dns_records_set(&vec![DnsKv {
            key: name.clone(),
            record: rec.clone(),
        }])
        .await?;

    // read back the srv record
    let records = client.dns_records_get().await?;
    assert_eq!(1, records.len());
    assert_eq!(records[0].key.name, name.name);
    match records[0].record {
        DnsRecord::Srv(ref rs) => {
            assert_eq!(rs.prio, srv.prio);
            assert_eq!(rs.weight, srv.weight);
            assert_eq!(rs.port, srv.port);
            assert_eq!(rs.target, srv.target);
        }
        _ => {
            panic!("expected srv record")
        }
    }

    // resolve the srv
    let response = resolver.srv_lookup(name.name).await?;
    let srvr = response.iter().next().expect("no addresses returned!");
    assert_eq!(srvr.priority(), srv.prio);
    assert_eq!(srvr.weight(), srv.weight);
    assert_eq!(srvr.port(), srv.port);
    assert_eq!(srvr.target().to_string(), srv.target + ".");

    Ok(())
}

async fn init_client_server(
) -> Result<(Client, TokioAsyncResolver), anyhow::Error> {
    // initialize dns server config
    let (config, dropshot_port, dns_port) = test_config()?;
    let log = config
        .log
        .to_logger("internal-dns")
        .context("failed to create logger")?;

    // initialize dns server db
    let db = Arc::new(sled::open(&config.data.storage_path)?);
    db.clear()?;

    let client = Client::new(
        &format!("http://127.0.0.1:{}", dropshot_port),
        log.clone(),
    );

    let mut rc = ResolverConfig::new();
    rc.add_name_server(NameServerConfig {
        socket_addr: SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            dns_port,
        )),
        protocol: Protocol::Udp,
        tls_dns_name: None,
        trust_nx_responses: false,
        bind_addr: None,
    });

    let resolver =
        TokioAsyncResolver::tokio(rc, ResolverOpts::default()).unwrap();

    // launch a dns server
    {
        let db = db.clone();
        let log = log.clone();
        let config = config.dns.clone();

        tokio::spawn(async move {
            internal_dns::dns_server::run(log, db, config).await
        });
    }

    // launch a dropshot server
    tokio::spawn(async move {
        let server = internal_dns::start_server(config, log, db).await?;
        server.await.map_err(|error_message| {
            anyhow!("server exiting: {}", error_message)
        })
    });

    // wait for server to start
    tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;

    Ok((client, resolver))
}

fn test_config() -> Result<(internal_dns::Config, u16, u16), anyhow::Error> {
    let dropshot_port = portpicker::pick_unused_port().expect("pick port");
    let dns_port = portpicker::pick_unused_port().expect("pick port");
    let tmp_dir = tempdir::TempDir::new("internal-dns-test")?;
    let mut storage_path = tmp_dir.path().to_path_buf();
    storage_path.push("test");
    let storage_path = storage_path.to_str().unwrap().into();

    let config = internal_dns::Config {
        log: dropshot::ConfigLogging::StderrTerminal {
            level: dropshot::ConfigLoggingLevel::Info,
        },
        dropshot: dropshot::ConfigDropshot {
            bind_address: format!("127.0.0.1:{}", dropshot_port)
                .parse()
                .unwrap(),
            request_body_max_bytes: 1024,
            ..Default::default()
        },
        data: internal_dns::dns_data::Config {
            nmax_messages: 16,
            storage_path,
        },
        dns: internal_dns::dns_server::Config {
            bind_address: format!("127.0.0.1:{}", dns_port).parse().unwrap(),
        },
    };

    Ok((config, dropshot_port, dns_port))
}
