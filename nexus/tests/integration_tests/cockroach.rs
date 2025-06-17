// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for accessing CockroachDB

use internal_dns_resolver::Resolver;
use internal_dns_types::names::ServiceName;
use nexus_test_utils_macros::nexus_test;
use omicron_nexus::app::cockroach_http::CockroachClusterHttpClient;
use omicron_nexus::app::cockroach_http::CockroachMetric;
use omicron_nexus::app::cockroach_http::MetricValue;
use omicron_nexus::app::cockroach_http::NodeLiveness;

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
    // First, find the HTTP service address
    let resolver = create_test_resolver(cptestctx);
    let http_addr = resolver
        .lookup_socket_v6(ServiceName::CockroachHttp)
        .await
        .expect("Should find CockroachDB HTTP service");
    let http_addr = std::net::SocketAddr::V6(http_addr);

    // Create HTTP client and fetch Prometheus metrics
    let client = CockroachClusterHttpClient::new(cptestctx.logctx.log.clone());
    client.update_backends(&[http_addr]).await;

    let metrics = client
        .fetch_prometheus_metrics()
        .await
        .expect("Should be able to fetch Prometheus metrics from CockroachDB");

    // Verify we got some expected CockroachDB metrics
    assert!(!metrics.metrics.is_empty(), "Should have received some metrics");

    // Test strongly-typed metric access using the CockroachMetric enum
    use strum::IntoEnumIterator;

    println!("\nTesting strongly-typed CockroachDB metrics:");

    let mut found_metrics = 0;
    let mut missing_metrics = Vec::new();

    // Check each strongly-typed metric
    for metric in CockroachMetric::iter() {
        // Use new typed API that enforces expected types
        if let Some(value) = metrics.get_metric(metric) {
            match value {
                MetricValue::Number(val) => {
                    println!(" {} = {} (number)", metric.metric_name(), val);
                    found_metrics += 1;
                }
                MetricValue::Histogram(buckets) => {
                    println!(
                        " {} = {} buckets (histogram)",
                        metric.metric_name(),
                        buckets.len()
                    );
                    found_metrics += 1;
                }
                MetricValue::String(s) => {
                    println!(" {} = {} (string)", metric.metric_name(), s);
                    found_metrics += 1;
                }
            }
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
        println!("Missing metrics:");
        for metric in missing_metrics {
            println!("  {} - {}", metric.metric_name(), metric.description());
        }
        panic!("Missing metrics; failing test");
    }

    // Test SQL execution latency histogram specifically
    println!("\nTesting SQL execution latency histogram:");
    if let Some(histogram) =
        metrics.get_metric_histogram(CockroachMetric::SqlExecLatency)
    {
        println!(
            "Found SQL execution latency histogram with {} buckets",
            histogram.len()
        );

        // Buckets are automatically sorted by le value
        println!("  Histogram buckets:");
        for bucket in histogram {
            if bucket.le == f64::INFINITY {
                println!("    le=+Inf: {}", bucket.count);
            } else {
                println!("    le={}: {}", bucket.le, bucket.count);
            }
        }

        // Verify histogram properties
        assert!(!histogram.is_empty(), "Histogram should have buckets");

        // Check that buckets are cumulative and sorted (each bucket count >= previous bucket count)
        for i in 1..histogram.len() {
            assert!(
                histogram[i].count >= histogram[i - 1].count,
                "Histogram buckets should be cumulative: bucket {} (le={}) has count {} < previous bucket count {}",
                i,
                histogram[i].le,
                histogram[i].count,
                histogram[i - 1].count
            );
            assert!(
                histogram[i].le >= histogram[i - 1].le,
                "Histogram buckets should be sorted: bucket {} (le={}) < previous bucket (le={})",
                i,
                histogram[i].le,
                histogram[i - 1].le
            );
        }

        println!("Histogram structure is valid (cumulative buckets)");
    } else {
        panic!(" SQL execution latency histogram not found");
    }
}

// Test fetching CockroachDB node status information
#[nexus_test]
async fn test_cockroach_http_node_status(cptestctx: &ControlPlaneTestContext) {
    // Create DNS resolver to find the CockroachDB HTTP service
    let resolver = create_test_resolver(cptestctx);

    // Look up the CockroachDB HTTP service directly using the strongly-typed resolver
    let http_addr = resolver
        .lookup_socket_v6(ServiceName::CockroachHttp)
        .await
        .expect("Should find CockroachDB HTTP service");

    let http_addr = std::net::SocketAddr::V6(http_addr);

    // Create HTTP client and fetch node status
    let client = CockroachClusterHttpClient::new(cptestctx.logctx.log.clone());
    client.update_backends(&[http_addr]).await;

    let nodes_response = client
        .fetch_node_status()
        .await
        .expect("Should be able to fetch nodes status from CockroachDB");

    println!("Response: {nodes_response:#?}");

    // Verify we saw one node, and that it's alive.
    assert_eq!(nodes_response.nodes.len(), 1, "Should have one node");
    let first_node = &nodes_response.nodes[0];

    assert_eq!(
        nodes_response
            .liveness_by_node_id
            .get(&first_node.desc.node_id)
            .unwrap(),
        &NodeLiveness::Live
    );

    assert_eq!(first_node.desc.node_id.as_u32(), 1);
    assert!(
        !first_node.build_info.tag.is_empty(),
        "Build tag should not be empty"
    );
}
