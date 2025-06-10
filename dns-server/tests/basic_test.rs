// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, Result};
use camino_tempfile::Utf8TempDir;
use dns_service_client::Client;
use dropshot::{HandlerTaskMode, test_util::LogContext};
use hickory_client::{
    client::{AsyncClient, ClientHandle},
    error::ClientError,
    rr::RData,
    udp::UdpClientStream,
};
use hickory_resolver::TokioAsyncResolver;
use hickory_resolver::error::ResolveErrorKind;
use hickory_resolver::{
    config::{NameServerConfig, Protocol, ResolverConfig, ResolverOpts},
    error::ResolveError,
    proto::{
        op::ResponseCode,
        rr::{DNSClass, Name, RecordType, rdata::AAAA},
        xfer::DnsResponse,
    },
};
use internal_dns_types::{
    config::{DnsConfigParams, DnsConfigZone, DnsRecord, Srv},
    names::ZONE_APEX_NAME,
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
    let fqdn = name.clone() + "." + TEST_ZONE + ".";
    let addr = Ipv4Addr::new(10, 1, 2, 3);
    let a = DnsRecord::A(addr);
    let input_records = HashMap::from([(name.clone(), vec![a])]);
    dns_records_create(client, TEST_ZONE, input_records.clone()).await?;

    // read back the a record
    let records = dns_records_list(client, TEST_ZONE).await?;
    assert_eq!(input_records, records);

    // resolve the name
    let response = resolver.lookup_ip(fqdn.clone()).await?;
    let address = response.iter().next().expect("no addresses returned!");
    assert_eq!(address, addr);

    // as with other cases, `hickory-resolver` does not let us see if the answer
    // is authoritative, so we'll have to query again with a lower level
    // interface to validate that.
    let raw_response = raw_dns_client_query(
        test_ctx.dns_server.local_address(),
        Name::from_ascii(&fqdn).expect("name is valid"),
        RecordType::A,
    )
    .await
    .expect("can issue DNS query");

    assert!(raw_response.authoritative());
    assert_eq!(raw_response.answers().len(), 1);

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
    let fqdn = name.clone() + "." + TEST_ZONE + ".";
    let addr = Ipv6Addr::new(0xfd, 0, 0, 0, 0, 0, 0, 0x1);
    let aaaa = DnsRecord::Aaaa(addr);
    let input_records = HashMap::from([(name.clone(), vec![aaaa])]);
    dns_records_create(client, TEST_ZONE, input_records.clone()).await?;

    // read back the aaaa record
    let records = dns_records_list(client, TEST_ZONE).await?;
    assert_eq!(input_records, records);

    // resolve the name
    let response = resolver.lookup_ip(fqdn.clone()).await?;
    let address = response.iter().next().expect("no addresses returned!");
    assert_eq!(address, addr);

    // as with other cases, `hickory-resolver` does not let us see if the answer
    // is authoritative, so we'll have to query again with a lower level
    // interface to validate that.
    let raw_response = raw_dns_client_query(
        test_ctx.dns_server.local_address(),
        Name::from_ascii(&fqdn).expect("name is valid"),
        RecordType::AAAA,
    )
    .await
    .expect("can issue DNS query");

    assert!(raw_response.authoritative());
    assert_eq!(raw_response.answers().len(), 1);

    test_ctx.cleanup().await;
    Ok(())
}

#[tokio::test]
pub async fn answers_match_question() -> Result<(), anyhow::Error> {
    let test_ctx = init_client_server("answers_match_question").await?;
    let client = &test_ctx.client;

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

    let name = Name::from_ascii(&(name + "." + TEST_ZONE + "."))
        .expect("can construct name for query");

    // If a server returns answers incorrectly, such as sending AAAA answers to
    // an A query, it turns out `hickory-resolver`'s internal CachingClient
    // transparently corrects the misbehavior. The caching client will cache the
    // extra record, then see there are no A records matching the query, and
    // finally send a correct response with no answers.
    //
    // `raw_dns_client_query` avoids using a hickory Resolver, so we can assert
    // on the exact answer from our server.
    let raw_response = raw_dns_client_query(
        test_ctx.dns_server.local_address(),
        name,
        RecordType::A,
    )
    .await
    .expect("test query is ok");

    // The answer we expect is:
    // * no error: the domain exists, so NXDOMAIN would be wrong
    // * no answers: we ask specifically for a record type the server does not
    //   have
    // * no additionals: the server could return AAAA records as additionals to
    //   an A query, but does not currently.
    // * authoritative: the nameserver we've queried is the source of truth for
    //   this name (and zone!)
    assert_eq!(raw_response.header().response_code(), ResponseCode::NoError);
    assert_eq!(raw_response.answers(), &[]);
    assert_eq!(raw_response.additionals(), &[]);
    assert!(raw_response.authoritative());

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
    let test_fqdn = name.clone() + "." + TEST_ZONE + ".";
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

    // resolve the srv. we'll test this in two ways:
    // * the srv record as seen through `hickory_resolver`, the higher-level
    //   interface we use in many places
    // * the srv record as seen through `hickory_client`, to double-check that
    //   the exact DNS response has the answers/additionals sections as we'd
    //   expect it to be
    let response = resolver.srv_lookup(&test_fqdn).await?;
    let srvr = response.iter().next().expect("no srv records returned!");
    assert_eq!(srvr.priority(), srv.prio);
    assert_eq!(srvr.weight(), srv.weight);
    assert_eq!(srvr.port(), srv.port);
    assert_eq!(srvr.target().to_string(), srv.target + ".");
    let mut aaaa_records = response.ip_iter().collect::<Vec<_>>();
    aaaa_records.sort();
    assert_eq!(aaaa_records, [IpAddr::from(addr1), IpAddr::from(addr2)]);

    // OK, through `hickory_resolver` everything looks right. now double-check
    // that the additional records really do come back in the "Additionals"
    // section of the response.

    let name = hickory_client::rr::domain::Name::from_ascii(&test_fqdn)
        .expect("can construct name for query");

    let response = raw_dns_client_query(
        test_ctx.dns_server.local_address(),
        name,
        RecordType::SRV,
    )
    .await
    .expect("test query is ok");
    assert_eq!(response.header().response_code(), ResponseCode::NoError);
    assert_eq!(response.answers().len(), 1);
    assert_eq!(response.answers()[0].record_type(), RecordType::SRV);
    assert_eq!(response.additionals().len(), 2);
    assert_eq!(response.additionals()[0].record_type(), RecordType::AAAA);
    assert_eq!(response.additionals()[1].record_type(), RecordType::AAAA);
    assert!(response.authoritative());

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

async fn lookup_ip_expect_error_code(
    server_addr: std::net::SocketAddr,
    resolver: &TokioAsyncResolver,
    name: &str,
    expected_code: ResponseCode,
) {
    match resolver.lookup_ip(name).await {
        Ok(unexpected) => {
            panic!("Expected {expected_code}, got record {unexpected:?}");
        }
        Err(e) => expect_no_records_error_code(&e, expected_code),
    };

    // We are authoritative for all records served from internal DNS or external
    // DNS.  This means that if we return an NXDOMAIN answer, the queried name
    // is one that we would be authoritative for, if it existed.  For other
    // errors, we do not set the authoritative bit even if the name is one we
    // might have been authoritative for.  This is primarily for simplicity; the
    // authoritative non-NXDOMAIN errors have no RFC-defined meaning.
    let expected_authoritativeness = expected_code == ResponseCode::NXDomain;

    // `lookup_ip` doesn't let us find out if the actual DNS message was
    // authoritative.  So instead, query again via `hickory-client` to get the
    // exact Message from our DNS server.  `lookup_ip` queries both A and AAAA
    // and merges the answers, so we'll query both here too.

    let raw_response =
        raw_query_expect_err(server_addr, name, RecordType::A).await;

    assert_eq!(raw_response.authoritative(), expected_authoritativeness);
    assert_eq!(raw_response.response_code(), expected_code);

    let raw_response =
        raw_query_expect_err(server_addr, name, RecordType::AAAA).await;
    assert_eq!(raw_response.authoritative(), expected_authoritativeness);
    assert_eq!(raw_response.response_code(), expected_code);
}

async fn raw_query_expect_err(
    server_addr: std::net::SocketAddr,
    name: &str,
    query_ty: RecordType,
) -> DnsResponse {
    let name = Name::from_ascii(name).expect("can parse domain name");

    let raw_response = raw_dns_client_query(server_addr, name, query_ty)
        .await
        .expect("can issue DNS query");

    // The caller may have a specific error in mind, but we know that the
    // response definitely should be that there was *some* kind of error.
    assert_ne!(raw_response.response_code(), ResponseCode::NoError);

    // We do not currently return answers or additionals for any errors.
    assert!(raw_response.answers().is_empty());
    assert!(raw_response.additionals().is_empty());

    // Optionally, the DNS server is permitted to return SOA records with
    // negative answers as a guide for how long to cache the result. We don't do
    // that right now, so test that there are no name servers in the answer.
    // This should change if the DNS server is changed.
    assert!(raw_response.name_servers().is_empty());

    raw_response
}

fn expect_no_records_error_code(
    err: &ResolveError,
    expected_code: ResponseCode,
) {
    match err.kind() {
        ResolveErrorKind::NoRecordsFound {
            response_code,
            query: _,
            soa: _,
            negative_ttl: _,
            trusted: _,
        } => {
            if response_code == &expected_code {
                // Error matches on all the conditions we're checking. No
                // issues.
            } else {
                panic!(
                    "Expected {expected_code}, got response code {response_code:?}"
                );
            }
        }
        unexpected => {
            panic!("Expected {expected_code}, got error {unexpected:?}");
        }
    }
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
    lookup_ip_expect_error_code(
        test_ctx.dns_server.local_address(),
        resolver,
        "epsilon3.oxide.test",
        ResponseCode::NXDomain,
    )
    .await;

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
    lookup_ip_expect_error_code(
        test_ctx.dns_server.local_address(),
        &resolver,
        &(name + "." + TEST_ZONE + "."),
        ResponseCode::NXDomain,
    )
    .await;

    test_ctx.cleanup().await;
    Ok(())
}

#[tokio::test]
pub async fn soa() -> Result<(), anyhow::Error> {
    let test_ctx = init_client_server("soa").await?;
    let resolver = &test_ctx.resolver;
    let client = &test_ctx.client;

    let ns1_addr = Ipv6Addr::new(0xfd, 0, 0, 0, 0, 0, 0, 0x1);
    let ns1_aaaa = DnsRecord::Aaaa(ns1_addr);
    let ns1_name = format!("ns1.{TEST_ZONE}.");
    let ns1 = DnsRecord::Ns(ns1_name.clone());
    let ns2_addr = Ipv6Addr::new(0xfd, 0, 0, 0, 0, 0, 0, 0x2);
    let ns2_aaaa = DnsRecord::Aaaa(ns2_addr);
    let ns2_name = format!("ns2.{TEST_ZONE}.");
    let ns2 = DnsRecord::Ns(ns2_name.clone());
    let service_addr = Ipv6Addr::new(0xfd, 0, 0, 0, 0, 0, 0, 0x3);
    let service_aaaa = DnsRecord::Aaaa(service_addr);

    // If an update defines a zone with records, but defines no nameservers,
    // that should be acceptable. We won't be able to define an SOA record,
    // since we won't have a nameserver to include as the primary source, but
    // the zone should otherwise be acceptable.
    let mut records = HashMap::new();
    records.insert("service".to_string(), vec![service_aaaa.clone()]);

    dns_records_create(client, TEST_ZONE, records).await?;

    let service_ip_answer =
        resolver.lookup_ip(&format!("service.{TEST_ZONE}.")).await?;
    let mut ip_iter = service_ip_answer.iter();
    assert_eq!(ip_iter.next(), Some(IpAddr::V6(service_addr)));
    assert_eq!(ip_iter.next(), None);

    // When we let the DNS server construct its own SOA record, we should be
    // able to tell it about a zone.
    let mut records = HashMap::new();
    records.insert("ns1".to_string(), vec![ns1_aaaa.clone()]);
    records.insert("ns2".to_string(), vec![ns2_aaaa.clone()]);
    records.insert(ZONE_APEX_NAME.to_string(), vec![ns1.clone(), ns2.clone()]);

    dns_records_create(client, TEST_ZONE, records).await?;

    // Now the NS records should exist, and so should an SOA.
    let soa_answer = resolver.soa_lookup(TEST_ZONE).await?;

    let soa_records: Vec<&hickory_proto::rr::rdata::soa::SOA> =
        soa_answer.iter().collect();

    assert_eq!(soa_records.len(), 1);
    // As an implementation detail, we return the lowest-numbered name server
    // for the zone as the primary name server.
    assert_eq!(soa_records[0].mname().to_utf8(), ns1_name);

    // We should be able to query nameservers for the zone.
    let zone_ns_answer = resolver.ns_lookup(TEST_ZONE).await?;
    let has_ns_record =
        zone_ns_answer.as_lookup().records().iter().any(|record| {
            if let Some(RData::NS(nsdname)) = record.data() {
                nsdname.0.to_utf8().as_str() == &ns1_name
            } else {
                false
            }
        });
    assert!(has_ns_record);

    // The nameserver's AAAA record should be in additionals.
    let has_aaaa_additional =
        zone_ns_answer.as_lookup().records().iter().any(|record| {
            if let Some(RData::AAAA(AAAA(addr))) = record.data() {
                addr == &ns1_addr
            } else {
                false
            }
        });
    assert!(has_aaaa_additional);

    // And we should be able to directly query the SOA record's primary server
    let soa_ns_aaaa_answer =
        resolver.lookup_ip(soa_records[0].mname().to_owned()).await?;
    assert_eq!(soa_ns_aaaa_answer.iter().collect::<Vec<_>>(), vec![ns1_addr]);

    // SOA queries under the zone we now know we are authoritative for should
    // fail with NXDomain.
    let no_soa_name = format!("foo.{TEST_ZONE}.");
    let lookup_err = resolver
        .soa_lookup(&no_soa_name)
        .await
        .expect_err("test zone should not exist");
    expect_no_records_error_code(&lookup_err, ResponseCode::NXDomain);

    // As with other NXDomain answers, we should see the authoritative bit.
    let raw_response = raw_query_expect_err(
        test_ctx.dns_server.local_address(),
        &no_soa_name,
        RecordType::A,
    )
    .await;

    assert!(raw_response.authoritative());
    assert_eq!(raw_response.response_code(), ResponseCode::NXDomain);

    // If the zone has no ns1 for some reason, we ought to see an SOA record
    // referencing the next lowest-numbered name server.
    let mut records = HashMap::new();
    records.insert("ns2".to_string(), vec![ns2_aaaa.clone()]);
    records.insert(ZONE_APEX_NAME.to_string(), vec![ns2.clone()]);

    dns_records_create(client, TEST_ZONE, records).await?;

    let soa_records: Vec<hickory_proto::rr::rdata::soa::SOA> =
        resolver.soa_lookup(TEST_ZONE).await?.into_iter().collect();

    assert_eq!(soa_records.len(), 1);
    assert_eq!(soa_records[0].mname().to_utf8(), ns2_name);

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
    lookup_ip_expect_error_code(
        test_ctx.dns_server.local_address(),
        &resolver,
        &format!("unicorn.{}.", TEST_ZONE),
        ResponseCode::NXDomain,
    )
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
    // SERVFAIL.  Further, `lookup_ip_expect_error_code` will check that the
    // error is not authoritative.
    lookup_ip_expect_error_code(
        test_ctx.dns_server.local_address(),
        &resolver,
        "unicorn.oxide.internal",
        ResponseCode::ServFail,
    )
    .await;

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
        default_request_body_max_bytes: 1024,
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
        generation: before.generation.next(),
        serial: before.serial + 1,
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

/// Issue a DNS query of `record_ty` records for `name`.
///
/// In most tests we just use `hickory-resolver` for a higher-level interface to
/// making DNS queries and handling responses. This is also the crate used
/// elsewhere to handle DNS responses in Omicron. However, it is slightly
/// higher-level than the actual wire response we produce from the DNS server in
/// this same crate; to assert on responses we send on the wire, this issues a
/// query using `hickory-client` and returns the corresponding `DnsResponse`.
async fn raw_dns_client_query(
    resolver_addr: std::net::SocketAddr,
    name: Name,
    record_ty: RecordType,
) -> Result<DnsResponse, ClientError> {
    let stream = UdpClientStream::<tokio::net::UdpSocket>::new(resolver_addr);
    let (mut trust_client, bg) = AsyncClient::connect(stream).await.unwrap();

    tokio::spawn(bg);

    trust_client.query(name, DNSClass::IN, record_ty).await
}
