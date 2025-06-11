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

// Test querying the CockroachDB HTTP interface for Prometheus metrics
#[nexus_test]
async fn test_cockroach_http_prometheus_metrics(
    cptestctx: &ControlPlaneTestContext,
) {
    use omicron_nexus::app::cockroach_http::CockroachHttpClient;

    // First, find the HTTP service address
    let resolver = create_test_resolver(cptestctx);
    let http_addr = resolver
        .lookup_socket_v6(ServiceName::CockroachHttp)
        .await
        .expect("Should find CockroachDB HTTP service");
    let http_addr = std::net::SocketAddr::V6(http_addr);

    // Create HTTP client and fetch Prometheus metrics
    let client = CockroachHttpClient::new(http_addr);

    let metrics = client
        .fetch_prometheus_metrics()
        .await
        .expect("Should be able to fetch Prometheus metrics from CockroachDB");

    // Verify we got some expected CockroachDB metrics
    assert!(!metrics.metrics.is_empty(), "Should have received some metrics");

    // Look for histogram metrics (metrics ending with "_bucket")
    println!("Looking for histogram metrics:");
    let histogram_metrics: Vec<_> = metrics
        .metrics
        .keys()
        .filter(|name| name.contains("_bucket") || name.contains("histogram"))
        .collect();

    if !histogram_metrics.is_empty() {
        println!("Found potential histogram metrics:");
        for metric_name in &histogram_metrics {
            println!("  {}", metric_name);
        }
    } else {
        println!("No histogram metrics found (metrics ending with '_bucket')");
    }

    // Test strongly-typed metric access using the CockroachMetric enum
    use omicron_nexus::app::cockroach_http::CockroachMetric;
    use strum::IntoEnumIterator;

    println!("\nTesting strongly-typed CockroachDB metrics:");

    let mut found_metrics = 0;
    let mut missing_metrics = Vec::new();

    // Check each strongly-typed metric
    for metric in CockroachMetric::iter() {
        if let Some(value) = metrics.get_cockroach_metric_number(metric) {
            println!("✓ {} = {}", metric.metric_name(), value);
            found_metrics += 1;
        } else {
            missing_metrics.push(metric);
        }
    }

    println!(
        "Found {} out of {} strongly-typed metrics",
        found_metrics,
        CockroachMetric::iter().count()
    );

    // We expect to find at least some of our key metrics in a running CockroachDB instance
    assert!(
        found_metrics > 0,
        "Should find at least some of the strongly-typed CockroachDB metrics"
    );

    // Print any missing metrics for debugging (but don't fail the test)
    if !missing_metrics.is_empty() {
        println!(
            "Missing metrics (may be normal if they haven't occurred yet):"
        );
        for metric in missing_metrics {
            println!("  {} - {}", metric.metric_name(), metric.description());
        }
    }

    // Test SQL execution latency histogram specifically
    println!("\nTesting SQL execution latency histogram:");
    if let Some(histogram) =
        metrics.get_cockroach_metric_histogram(CockroachMetric::SqlExecLatency)
    {
        println!(
            "✓ Found SQL execution latency histogram with {} buckets",
            histogram.len()
        );

        // Sort buckets by le value for proper display
        let mut sorted_buckets = histogram.clone();
        sorted_buckets.sort_by(|a, b| {
            a.le.partial_cmp(&b.le).unwrap_or(std::cmp::Ordering::Equal)
        });

        println!("  Histogram buckets:");
        for bucket in &sorted_buckets {
            if bucket.le == f64::INFINITY {
                println!("    le=+Inf: {}", bucket.count);
            } else {
                println!("    le={}: {}", bucket.le, bucket.count);
            }
        }

        // Verify histogram properties
        assert!(!histogram.is_empty(), "Histogram should have buckets");

        // Check that buckets are cumulative (each bucket count >= previous bucket count)
        for i in 1..sorted_buckets.len() {
            assert!(
                sorted_buckets[i].count >= sorted_buckets[i - 1].count,
                "Histogram buckets should be cumulative: bucket {} (le={}) has count {} < previous bucket count {}",
                i,
                sorted_buckets[i].le,
                sorted_buckets[i].count,
                sorted_buckets[i - 1].count
            );
        }

        println!("✓ Histogram structure is valid (cumulative buckets)");
    } else {
        println!("⚠ SQL execution latency histogram not found");
    }
}

// Test fetching CockroachDB node status information
#[nexus_test]
async fn test_cockroach_http_node_status(cptestctx: &ControlPlaneTestContext) {
    use omicron_nexus::app::cockroach_http::CockroachHttpClient;

    // Create DNS resolver to find the CockroachDB HTTP service
    let resolver = create_test_resolver(cptestctx);

    // Look up the CockroachDB HTTP service directly using the strongly-typed resolver
    let http_addr = resolver
        .lookup_socket_v6(ServiceName::CockroachHttp)
        .await
        .expect("Should find CockroachDB HTTP service");

    let http_addr = std::net::SocketAddr::V6(http_addr);

    // Create HTTP client and fetch node status
    let client = CockroachHttpClient::new(http_addr);

    let nodes_response = client
        .fetch_node_status()
        .await
        .expect("Should be able to fetch nodes status from CockroachDB");

    // Verify we got at least one node
    assert!(!nodes_response.nodes.is_empty(), "Should have at least one node");

    println!("Response: {nodes_response:#?}");

    // Verify basic node status structure for the first node
    let first_node = &nodes_response.nodes[0];
    assert_eq!(first_node.desc.node_id.as_u32(), 1);
    assert!(
        !first_node.build_info.tag.is_empty(),
        "Build tag should not be empty"
    );

    println!("✓ CockroachDB nodes status test completed successfully!");
    println!("  Number of nodes: {}", nodes_response.nodes.len());
    for node in &nodes_response.nodes {
        println!(
            "  Node ID: {}, Build tag: {}",
            node.desc.node_id, node.build_info.tag
        );
    }
}
