// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, Result};
use dns_service_client::{
    types::{
        DnsConfigParams, DnsConfigZone, DnsKv, DnsRecord, DnsRecordKey, Srv,
    },
    Client,
};
use dropshot::test_util::LogContext;
use omicron_test_utils::dev::test_setup_log;
use slog::o;
use std::net::Ipv6Addr;
use trust_dns_resolver::error::ResolveErrorKind;
use trust_dns_resolver::TokioAsyncResolver;
use trust_dns_resolver::{
    config::{NameServerConfig, Protocol, ResolverConfig, ResolverOpts},
    proto::op::ResponseCode,
};

const TEST_ZONE: &'static str = "oxide.internal";

#[tokio::test]
pub async fn aaaa_crud() -> Result<(), anyhow::Error> {
    let test_ctx = init_client_server("aaaa_crud").await?;
    let client = &test_ctx.client;
    let resolver = &test_ctx.resolver;

    // records should initially be empty
    let records = dns_records_list(client, TEST_ZONE).await?;
    assert!(records.is_empty());

    // add an aaaa record
    let name = DnsRecordKey { name: "devron".into() };
    let addr = Ipv6Addr::new(0xfd, 0, 0, 0, 0, 0, 0, 0x1);
    let aaaa = DnsRecord::Aaaa(addr);
    dns_records_create(
        client,
        TEST_ZONE,
        vec![DnsKv { key: name.clone(), records: vec![aaaa.clone()] }],
    )
    .await?;

    // read back the aaaa record
    let records = dns_records_list(client, TEST_ZONE).await?;
    assert_eq!(1, records.len());
    assert_eq!(records[0].key.name, name.name);

    assert_eq!(1, records[0].records.len());
    match records[0].records[0] {
        DnsRecord::Aaaa(ra) => {
            assert_eq!(ra, addr);
        }
        _ => {
            panic!("expected aaaa record")
        }
    }

    // resolve the name
    let response =
        resolver.lookup_ip(name.name + "." + TEST_ZONE + ".").await?;
    let address = response.iter().next().expect("no addresses returned!");
    assert_eq!(address, addr);

    test_ctx.cleanup().await;
    Ok(())
}

#[tokio::test]
pub async fn srv_crud() -> Result<(), anyhow::Error> {
    let test_ctx = init_client_server("srv_crud").await?;
    let client = &test_ctx.client;
    let resolver = &test_ctx.resolver;

    // records should initially be empty
    let records = dns_records_list(client, TEST_ZONE).await?;
    assert!(records.is_empty());

    // add a srv record
    let name = DnsRecordKey { name: "hromi".into() };
    let srv =
        Srv { prio: 47, weight: 74, port: 99, target: "outpost47".into() };
    let rec = DnsRecord::Srv(srv.clone());
    dns_records_create(
        client,
        TEST_ZONE,
        vec![DnsKv { key: name.clone(), records: vec![rec.clone()] }],
    )
    .await?;

    // read back the srv record
    let records = dns_records_list(client, TEST_ZONE).await?;
    assert_eq!(1, records.len());
    assert_eq!(records[0].key.name, name.name);

    assert_eq!(1, records[0].records.len());
    match records[0].records[0] {
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
    let response =
        resolver.srv_lookup(name.name + "." + TEST_ZONE + ".").await?;
    let srvr = response.iter().next().expect("no addresses returned!");
    assert_eq!(srvr.priority(), srv.prio);
    assert_eq!(srvr.weight(), srv.weight);
    assert_eq!(srvr.port(), srv.port);
    assert_eq!(srvr.target().to_string(), srv.target + ".");

    test_ctx.cleanup().await;
    Ok(())
}

#[tokio::test]
pub async fn multi_record_crud() -> Result<(), anyhow::Error> {
    let test_ctx = init_client_server("multi_record_crud").await?;
    let client = &test_ctx.client;
    let resolver = &test_ctx.resolver;

    // records should initially be empty
    let records = dns_records_list(client, TEST_ZONE).await?;
    assert!(records.is_empty());

    // Add multiple AAAA records
    let name = DnsRecordKey { name: "devron".into() };
    let addr1 = Ipv6Addr::new(0xfd, 0, 0, 0, 0, 0, 0, 0x1);
    let addr2 = Ipv6Addr::new(0xfd, 0, 0, 0, 0, 0, 0, 0x2);
    let aaaa1 = DnsRecord::Aaaa(addr1);
    let aaaa2 = DnsRecord::Aaaa(addr2);
    dns_records_create(
        client,
        TEST_ZONE,
        vec![DnsKv { key: name.clone(), records: vec![aaaa1, aaaa2] }],
    )
    .await?;

    // read back the aaaa records
    let records = dns_records_list(client, TEST_ZONE).await?;
    assert_eq!(1, records.len());
    assert_eq!(records[0].key.name, name.name);

    assert_eq!(2, records[0].records.len());
    match &records[0].records[0] {
        DnsRecord::Aaaa(ra) => {
            assert_eq!(*ra, addr1);
        }
        _ => {
            panic!("expected aaaa record")
        }
    }
    match &records[0].records[1] {
        DnsRecord::Aaaa(ra) => {
            assert_eq!(*ra, addr2);
        }
        _ => {
            panic!("expected aaaa record")
        }
    }

    // resolve the name
    let response =
        resolver.lookup_ip(name.name + "." + TEST_ZONE + ".").await?;
    let mut iter = response.iter();
    let address = iter.next().expect("no addresses returned!");
    assert_eq!(address, addr1);
    let address = iter.next().expect("expected two addresses, only saw one");
    assert_eq!(address, addr2);

    test_ctx.cleanup().await;
    Ok(())
}

async fn lookup_ip_expect_nxdomain(resolver: &TokioAsyncResolver, name: &str) {
    match resolver.lookup_ip(name).await {
        Ok(unexpected) => {
            panic!("Expected NXDOMAIN, got record {:?}", unexpected);
        }
        Err(e) => match e.kind() {
            ResolveErrorKind::NoRecordsFound {
                response_code,
                query: _,
                soa: _,
                negative_ttl: _,
                trusted: _,
            } => match response_code {
                ResponseCode::NXDomain => {}
                unexpected => {
                    panic!(
                        "Expected NXDOMAIN, got response code {:?}",
                        unexpected
                    );
                }
            },
            unexpected => {
                panic!("Expected NXDOMAIN, got error {:?}", unexpected);
            }
        },
    };
}

// XXX-dap ask ry: why does this test exist instead of just not inserting it?
#[tokio::test]
pub async fn empty_record() -> Result<(), anyhow::Error> {
    let test_ctx = init_client_server("empty_record").await?;
    let client = &test_ctx.client;
    let resolver = &test_ctx.resolver;

    // records should initially be empty
    let records = dns_records_list(client, TEST_ZONE).await?;
    assert!(records.is_empty());

    // Add an empty DNS record
    let name = DnsRecordKey { name: "devron".into() };
    dns_records_create(
        client,
        TEST_ZONE,
        vec![DnsKv { key: name.clone(), records: vec![] }],
    )
    .await?;

    // read back the aaaa record
    let records = dns_records_list(client, TEST_ZONE).await?;
    assert_eq!(1, records.len());
    assert_eq!(records[0].key.name, name.name);
    assert_eq!(0, records[0].records.len());

    // resolve the name
    lookup_ip_expect_nxdomain(&resolver, &(name.name + "." + TEST_ZONE + "."))
        .await;

    test_ctx.cleanup().await;
    Ok(())
}

// XXX-dap TODO-coverage add a test where the name contains the zone name in it

#[tokio::test]
pub async fn nxdomain() -> Result<(), anyhow::Error> {
    let test_ctx = init_client_server("nxdomain").await?;
    let resolver = &test_ctx.resolver;
    let client = &test_ctx.client;

    // records should initially be empty
    let records = dns_records_list(client, TEST_ZONE).await?;
    assert!(records.is_empty());

    // add a record, just to create a zone
    let name = DnsRecordKey { name: "devron".into() };
    let addr = Ipv6Addr::new(0xfd, 0, 0, 0, 0, 0, 0, 0x1);
    let aaaa = DnsRecord::Aaaa(addr);
    dns_records_create(
        client,
        TEST_ZONE,
        vec![DnsKv { key: name.clone(), records: vec![aaaa.clone()] }],
    )
    .await?;

    // asking for a nonexistent record within the domain of the internal DNS
    // server should result in an NXDOMAIN
    lookup_ip_expect_nxdomain(&resolver, &format!("unicorn.{}.", TEST_ZONE))
        .await;

    test_ctx.cleanup().await;
    Ok(())
}

#[tokio::test]
pub async fn servfail() -> Result<(), anyhow::Error> {
    let test_ctx = init_client_server("servfail").await?;
    let resolver = &test_ctx.resolver;

    // In this case, we haven't defined any zones yet, so any request should be
    // outside the server's authoritative zones.  That should result in a
    // SERVFAIL.
    match resolver.lookup_ip("unicorn.oxide.internal").await {
        Ok(unexpected) => {
            panic!("Expected SERVFAIL, got record {:?}", unexpected);
        }
        Err(e) => match e.kind() {
            ResolveErrorKind::NoRecordsFound {
                response_code,
                query: _,
                soa: _,
                negative_ttl: _,
                trusted: _,
            } => match response_code {
                ResponseCode::ServFail => {}
                unexpected => {
                    panic!(
                        "Expected SERVFAIL, got response code {:?}",
                        unexpected
                    );
                }
            },
            unexpected => {
                panic!("Expected SERVFAIL, got error {:?}", unexpected);
            }
        },
    };

    test_ctx.cleanup().await;
    Ok(())
}

struct TestContext {
    client: Client,
    resolver: TokioAsyncResolver,
    dns_server: dns_server::dns_server::ServerHandle,
    dropshot_server: dropshot::HttpServer<dns_server::http_server::Context>,
    tmp: tempdir::TempDir,
    logctx: LogContext,
}

impl TestContext {
    async fn cleanup(self) {
        drop(self.dns_server);
        self.dropshot_server.close().await.expect("Failed to clean up server");
        self.tmp.close().expect("Failed to clean up tmp directory");
        self.logctx.cleanup_successful();
    }
}

async fn init_client_server(
    test_name: &str,
) -> Result<TestContext, anyhow::Error> {
    // initialize dns server config
    let (tmp, config_storage, config_dropshot, logctx) =
        test_config(test_name)?;
    let log = logctx.log.clone();

    // initialize dns server db
    let store = dns_server::storage::Store::new(
        log.new(o!("component" => "store")),
        &config_storage,
    )
    .context("initializing storage")?;
    assert!(store.is_new());

    // launch a dns server
    let dns_server_config = dns_server::dns_server::Config {
        bind_address: "[::1]:0".parse().unwrap(),
    };
    let (dns_server, dropshot_server) = dns_server::start_servers(
        log.clone(),
        store,
        &dns_server_config,
        &config_dropshot,
    )
    .await?;

    let mut rc = ResolverConfig::new();
    rc.add_name_server(NameServerConfig {
        socket_addr: *dns_server.local_address(),
        protocol: Protocol::Udp,
        tls_dns_name: None,
        trust_nx_responses: false,
        bind_addr: None,
    });

    let resolver =
        TokioAsyncResolver::tokio(rc, ResolverOpts::default()).unwrap();

    // wait for server to start
    // XXX-dap wait_for_condition
    tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;

    let client =
        Client::new(&format!("http://{}", dropshot_server.local_addr()), log);

    Ok(TestContext {
        client,
        resolver,
        dns_server,
        dropshot_server,
        tmp,
        logctx,
    })
}

fn test_config(
    test_name: &str,
) -> Result<
    (
        tempdir::TempDir,
        dns_server::storage::Config,
        dropshot::ConfigDropshot,
        LogContext,
    ),
    anyhow::Error,
> {
    let logctx = test_setup_log(test_name);
    let tmp_dir = tempdir::TempDir::new("dns-server-test")?;
    let mut storage_path = tmp_dir.path().to_path_buf();
    storage_path.push("test");
    let storage_path = storage_path.to_str().unwrap().into();
    let config_storage = dns_server::storage::Config { storage_path };
    let config_dropshot = dropshot::ConfigDropshot {
        bind_address: "[::1]:0".to_string().parse().unwrap(),
        request_body_max_bytes: 1024,
        ..Default::default()
    };

    Ok((tmp_dir, config_storage, config_dropshot, logctx))
}

async fn dns_records_create(
    client: &Client,
    zone_name: &str,
    records: Vec<DnsKv>,
) -> anyhow::Result<()> {
    let before = client
        .dns_config_get()
        .await
        .context("fetch current generation")?
        .into_inner();

    let (our_zones, other_zones) = before
        .zones
        .into_iter()
        .partition::<Vec<_>, _>(|z| z.zone_name == zone_name);

    assert!(our_zones.len() <= 1);
    let zone_records = if let Some(our_zone) = our_zones.into_iter().next() {
        our_zone.records.into_iter().chain(records.into_iter()).collect()
    } else {
        records
    };

    let new_zone = DnsConfigZone {
        zone_name: zone_name.to_owned(),
        records: zone_records,
    };

    let zones =
        other_zones.into_iter().chain(std::iter::once(new_zone)).collect();
    let after = DnsConfigParams {
        generation: before.generation + 1,
        zones,
        time_created: chrono::Utc::now(),
    };
    client.dns_config_put(&after).await.context("updating generation")?;
    Ok(())
}

async fn dns_records_list(
    client: &Client,
    zone_name: &str,
) -> anyhow::Result<Vec<DnsKv>> {
    Ok(client
        .dns_config_get()
        .await
        .context("fetch current generation")?
        .into_inner()
        .zones
        .into_iter()
        .find(|z| z.zone_name == zone_name)
        .map(|z| z.records)
        .unwrap_or_else(Vec::new))
}
