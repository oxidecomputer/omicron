// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, Result};
use camino_tempfile::Utf8TempDir;
use dns_service_client::{
    types::{DnsConfigParams, DnsConfigZone, DnsRecord, Srv},
    Client,
};
use dropshot::{test_util::LogContext, HandlerTaskMode};
use hickory_resolver::error::ResolveErrorKind;
use hickory_resolver::TokioAsyncResolver;
use hickory_resolver::{
    config::{NameServerConfig, Protocol, ResolverConfig, ResolverOpts},
    proto::op::ResponseCode,
};
use omicron_test_utils::dev::test_setup_log;
use slog::o;
use std::{
    collections::HashMap,
    net::Ipv6Addr,
    net::{IpAddr, Ipv4Addr},
};

const TEST_ZONE: &'static str = "oxide.internal";

#[tokio::test]
pub async fn a_crud() -> Result<(), anyhow::Error> {
    let test_ctx = init_client_server("a_crud").await?;
    let client = &test_ctx.client;
    let resolver = &test_ctx.resolver;

    // records should initially be empty
    let records = dns_records_list(client, TEST_ZONE).await?;
    assert!(records.is_empty());

    // add an a record
    let name = "devron".to_string();
    let addr = Ipv4Addr::new(10, 1, 2, 3);
    let a = DnsRecord::A(addr);
    let input_records = HashMap::from([(name.clone(), vec![a])]);
    dns_records_create(client, TEST_ZONE, input_records.clone()).await?;

    // read back the a record
    let records = dns_records_list(client, TEST_ZONE).await?;
    assert_eq!(input_records, records);

    // resolve the name
    let response = resolver.lookup_ip(name + "." + TEST_ZONE + ".").await?;
    let address = response.iter().next().expect("no addresses returned!");
    assert_eq!(address, addr);

    test_ctx.cleanup().await;
    Ok(())
}

#[tokio::test]
pub async fn aaaa_crud() -> Result<(), anyhow::Error> {
    let test_ctx = init_client_server("aaaa_crud").await?;
    let client = &test_ctx.client;
    let resolver = &test_ctx.resolver;

    // records should initially be empty
    let records = dns_records_list(client, TEST_ZONE).await?;
    assert!(records.is_empty());

    // add an aaaa record
    let name = "devron".to_string();
    let addr = Ipv6Addr::new(0xfd, 0, 0, 0, 0, 0, 0, 0x1);
    let aaaa = DnsRecord::Aaaa(addr);
    let input_records = HashMap::from([(name.clone(), vec![aaaa])]);
    dns_records_create(client, TEST_ZONE, input_records.clone()).await?;

    // read back the aaaa record
    let records = dns_records_list(client, TEST_ZONE).await?;
    assert_eq!(input_records, records);

    // resolve the name
    let response = resolver.lookup_ip(name + "." + TEST_ZONE + ".").await?;
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
    let name = "hromi".to_string();
    let target = "outpost47";
    let srv = Srv {
        prio: 47,
        weight: 74,
        port: 99,
        target: format!("{target}.{TEST_ZONE}"),
    };
    let rec = DnsRecord::Srv(srv.clone());
    let input_records = HashMap::from([(name.clone(), vec![rec])]);
    dns_records_create(client, TEST_ZONE, input_records.clone()).await?;

    // read back the srv record
    let records = dns_records_list(client, TEST_ZONE).await?;
    assert_eq!(records, input_records);

    // add some aaaa records corresponding to the srv target
    let addr1 = Ipv6Addr::new(0xfd, 0, 0, 0, 0, 0, 0, 0x1);
    let addr2 = Ipv6Addr::new(0xfd, 0, 0, 0, 0, 0, 0, 0x2);
    let input_records = HashMap::from([(
        target.to_string(),
        vec![DnsRecord::Aaaa(addr1), DnsRecord::Aaaa(addr2)],
    )]);
    dns_records_create(client, TEST_ZONE, input_records.clone()).await?;

    // resolve the srv
    let response = resolver.srv_lookup(name + "." + TEST_ZONE + ".").await?;
    let srvr = response.iter().next().expect("no srv records returned!");
    assert_eq!(srvr.priority(), srv.prio);
    assert_eq!(srvr.weight(), srv.weight);
    assert_eq!(srvr.port(), srv.port);
    assert_eq!(srvr.target().to_string(), srv.target + ".");
    let mut aaaa_records = response.ip_iter().collect::<Vec<_>>();
    aaaa_records.sort();
    assert_eq!(aaaa_records, [IpAddr::from(addr1), IpAddr::from(addr2)]);

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
    let name = "devron".to_string();
    let addr1 = Ipv6Addr::new(0xfd, 0, 0, 0, 0, 0, 0, 0x1);
    let addr2 = Ipv6Addr::new(0xfd, 0, 0, 0, 0, 0, 0, 0x2);
    let aaaa1 = DnsRecord::Aaaa(addr1);
    let aaaa2 = DnsRecord::Aaaa(addr2);
    let input_records = HashMap::from([(name.clone(), vec![aaaa1, aaaa2])]);
    dns_records_create(client, TEST_ZONE, input_records.clone()).await?;

    // read back the aaaa records
    let records = dns_records_list(client, TEST_ZONE).await?;
    assert_eq!(records, input_records);

    // resolve the name
    let response = resolver.lookup_ip(name + "." + TEST_ZONE + ".").await?;
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

// Verify that the part of a name that's under the zone name can contain the
// zone name itself.  For example, you can say that "emy.oxide.internal" exists
// under "oxide.internal", meaning that the server would provide
// "emy.oxide.internal.oxide.internal".  This is a little obscure.  But as a
// client, it's easy to mess this up (by accidentally including the zone's name
// in one of the zone's record names) and it's useful for debuggability to make
// sure that we do the predictable thing here (namely: we *always* append the
// zone name to any names that are _in_ the zone, even if they already have it).
#[tokio::test]
pub async fn name_contains_zone() -> Result<(), anyhow::Error> {
    let test_ctx = init_client_server("name_contains_zone").await?;
    let client = &test_ctx.client;
    let resolver = &test_ctx.resolver;

    // add a aaaa record
    let name = "epsilon3.oxide.test".to_string();
    let addr = Ipv6Addr::new(0xfd, 0, 0, 0, 0, 0, 0, 0x1);
    let aaaa = DnsRecord::Aaaa(addr);
    let input_records = HashMap::from([(name.clone(), vec![aaaa])]);
    dns_records_create(client, "oxide.test", input_records.clone()).await?;

    // read back the aaaa record
    let records = dns_records_list(client, "oxide.test").await?;
    assert_eq!(records, input_records);

    // resolve the name
    let response = resolver.lookup_ip("epsilon3.oxide.test.oxide.test").await?;
    let address = response.iter().next().expect("no addresses returned!");
    assert_eq!(address, addr);

    // A lookup shouldn't work without the zone's name appended twice.
    lookup_ip_expect_nxdomain(resolver, "epsilon3.oxide.test").await;

    test_ctx.cleanup().await;
    Ok(())
}

#[tokio::test]
pub async fn empty_record() -> Result<(), anyhow::Error> {
    let test_ctx = init_client_server("empty_record").await?;
    let client = &test_ctx.client;
    let resolver = &test_ctx.resolver;

    // records should initially be empty
    let records = dns_records_list(client, TEST_ZONE).await?;
    assert!(records.is_empty());

    // Add an empty DNS record
    let name = "devron".to_string();
    let input_records = HashMap::from([(name.clone(), vec![])]);
    dns_records_create(client, TEST_ZONE, input_records).await?;
    let records = dns_records_list(client, TEST_ZONE).await?;
    assert!(records.is_empty());

    // resolve the name
    lookup_ip_expect_nxdomain(&resolver, &(name + "." + TEST_ZONE + ".")).await;

    test_ctx.cleanup().await;
    Ok(())
}

#[tokio::test]
pub async fn nxdomain() -> Result<(), anyhow::Error> {
    let test_ctx = init_client_server("nxdomain").await?;
    let resolver = &test_ctx.resolver;
    let client = &test_ctx.client;

    // records should initially be empty
    let records = dns_records_list(client, TEST_ZONE).await?;
    assert!(records.is_empty());

    // add a record, just to create a zone
    let name = "devron".to_string();
    let addr = Ipv6Addr::new(0xfd, 0, 0, 0, 0, 0, 0, 0x1);
    let aaaa = DnsRecord::Aaaa(addr);
    dns_records_create(
        client,
        TEST_ZONE,
        HashMap::from([(name.clone(), vec![aaaa.clone()])]),
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
    tmp: Utf8TempDir,
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

    let mut resolver_config = ResolverConfig::new();
    resolver_config.add_name_server(NameServerConfig {
        socket_addr: dns_server.local_address(),
        protocol: Protocol::Udp,
        tls_dns_name: None,
        trust_negative_responses: false,
        bind_addr: None,
    });
    let mut resolver_opts = ResolverOpts::default();
    // Enable edns for potentially larger records
    resolver_opts.edns0 = true;

    let resolver = TokioAsyncResolver::tokio(resolver_config, resolver_opts);
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
        Utf8TempDir,
        dns_server::storage::Config,
        dropshot::ConfigDropshot,
        LogContext,
    ),
    anyhow::Error,
> {
    let logctx = test_setup_log(test_name);
    let tmp_dir = Utf8TempDir::with_prefix("dns-server-test")?;
    let mut storage_path = tmp_dir.path().to_path_buf();
    storage_path.push("test");
    let config_storage =
        dns_server::storage::Config { storage_path, keep_old_generations: 3 };
    let config_dropshot = dropshot::ConfigDropshot {
        bind_address: "[::1]:0".to_string().parse().unwrap(),
        request_body_max_bytes: 1024,
        default_handler_task_mode: HandlerTaskMode::Detached,
        log_headers: vec![],
    };

    Ok((tmp_dir, config_storage, config_dropshot, logctx))
}

async fn dns_records_create(
    client: &Client,
    zone_name: &str,
    records: HashMap<String, Vec<DnsRecord>>,
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
) -> anyhow::Result<HashMap<String, Vec<DnsRecord>>> {
    Ok(client
        .dns_config_get()
        .await
        .context("fetch current generation")?
        .into_inner()
        .zones
        .into_iter()
        .find(|z| z.zone_name == zone_name)
        .map(|z| z.records)
        .unwrap_or_else(HashMap::new))
}
