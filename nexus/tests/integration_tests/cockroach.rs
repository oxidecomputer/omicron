// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for accessing CockroachDB

use hickory_resolver::TokioAsyncResolver;
use hickory_resolver::config::{
    NameServerConfig, Protocol, ResolverConfig, ResolverOpts,
};
use nexus_test_utils_macros::nexus_test;
use std::net::SocketAddr;

// Creates a DNS resolver pointing to the given DNS server address
fn create_test_resolver(dns_addr: SocketAddr) -> TokioAsyncResolver {
    let mut resolver_config = ResolverConfig::new();
    resolver_config.add_name_server(NameServerConfig {
        socket_addr: dns_addr,
        protocol: Protocol::Udp,
        tls_dns_name: None,
        trust_negative_responses: false,
        bind_addr: None,
    });
    let mut resolver_opts = ResolverOpts::default();
    resolver_opts.edns0 = true;
    TokioAsyncResolver::tokio(resolver_config, resolver_opts)
}

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

// Validates that ControlPlaneTestContext setup creates both the listening and HTTP addresses of
// CockroachDB, and stores them in DNS.
#[nexus_test]
async fn test_cockroach_dns(cptestctx: &ControlPlaneTestContext) {
    // Create DNS resolver pointing to our test DNS server
    let dns_addr = cptestctx.internal_dns.dns_server.local_address();
    let resolver = create_test_resolver(dns_addr);

    // Test that both services are discoverable via DNS
    let cockroach_records = resolver
        .srv_lookup("_cockroach._tcp.control-plane.oxide.internal")
        .await
        .expect("Should find CockroachDB PostgreSQL service");
    assert_eq!(
        cockroach_records.iter().count(),
        1,
        "Should have exactly one Cockroach PostgreSQL record"
    );

    let cockroach_http_records = resolver
        .srv_lookup("_cockroach-http._tcp.control-plane.oxide.internal")
        .await
        .expect("Should find CockroachDB HTTP service");
    assert_eq!(
        cockroach_http_records.iter().count(),
        1,
        "Should have exactly one Cockroach HTTP record"
    );

    // Verify they point to the same IP but different ports
    let pg_record = cockroach_records.iter().next().unwrap();
    let http_record = cockroach_http_records.iter().next().unwrap();

    // Should be same target IP, different ports
    assert_eq!(
        pg_record.target(),
        http_record.target(),
        "Both services should point to same target host"
    );
    assert_ne!(
        pg_record.port(),
        http_record.port(),
        "Services should use different ports"
    );

    // Verify both ports are non-zero (actual allocated ports)
    assert_ne!(
        pg_record.port(),
        0,
        "PostgreSQL service should have allocated port"
    );
    assert_ne!(
        http_record.port(),
        0,
        "HTTP service should have allocated port"
    );
}
