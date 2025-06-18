// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for accessing CockroachDB

use internal_dns_resolver::Resolver;
use internal_dns_types::names::ServiceName;
use nexus_test_utils_macros::nexus_test;

// Creates an internal DNS resolver pointing to the given DNS server address
fn create_test_resolver(cptestctx: &ControlPlaneTestContext) -> Resolver {
    let dns_addr = cptestctx.internal_dns.dns_server.local_address();
    Resolver::new_from_addrs(cptestctx.logctx.log.clone(), &[dns_addr])
        .expect("Should be able to create resolver")
}

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

// Validates that ControlPlaneTestContext setup creates both the listening and HTTP addresses of
// CockroachDB, and stores them in DNS.
#[nexus_test]
async fn test_cockroach_dns(cptestctx: &ControlPlaneTestContext) {
    // Create DNS resolver pointing to our test DNS server
    let resolver = create_test_resolver(cptestctx);

    // Test that both services are discoverable via DNS using strongly-typed service names
    let cockroach_targets = resolver
        .lookup_srv(ServiceName::Cockroach)
        .await
        .expect("Should find CockroachDB PostgreSQL service");
    assert_eq!(
        cockroach_targets.len(),
        1,
        "Should have exactly one Cockroach PostgreSQL record"
    );

    let cockroach_http_targets = resolver
        .lookup_srv(ServiceName::CockroachHttp)
        .await
        .expect("Should find CockroachDB HTTP service");
    assert_eq!(
        cockroach_http_targets.len(),
        1,
        "Should have exactly one Cockroach HTTP record"
    );

    // Verify they point to the same target but different ports
    let (pg_target, pg_port) = &cockroach_targets[0];
    let (http_target, http_port) = &cockroach_http_targets[0];

    // Should be same target hostname, different ports
    assert_eq!(
        pg_target, http_target,
        "Both services should point to same target host"
    );
    assert_ne!(pg_port, http_port, "Services should use different ports");

    // Verify both ports are non-zero (actual allocated ports)
    assert_ne!(*pg_port, 0, "PostgreSQL service should have allocated port");
    assert_ne!(*http_port, 0, "HTTP service should have allocated port");
}
