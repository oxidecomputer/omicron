// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for accessing CockroachDB

use cockroach_admin_client::Client as CockroachAdminClient;
use nexus_test_utils_macros::nexus_test;
use omicron_nexus::app::cockroach_http::CockroachClusterAdminClient;
use omicron_nexus::app::cockroach_http::CockroachMetric;
use omicron_nexus::app::cockroach_http::MetricValue;
use omicron_nexus::app::cockroach_http::NodeLiveness;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_cockroach_admin_server(cptestctx: &ControlPlaneTestContext) {
    let admin_addr = cptestctx.database_admin.local_addr();
    let admin_url = format!("http://{}", admin_addr);

    let client =
        CockroachAdminClient::new(&admin_url, cptestctx.logctx.log.clone());

    // Test the /proxy/status/vars endpoint (proxy to CockroachDB's /_status/vars)
    let vars_response = client
        .status_vars()
        .await
        .expect("should be able to query /proxy/status/vars");

    // Basic sanity check - should be non-empty and contain some metrics
    let vars_text = vars_response.into_inner();
    assert!(!vars_text.is_empty(), "status vars response should not be empty");
    assert!(vars_text.contains("sql_"), "should contain SQL metrics");

    // Test the /proxy/status/nodes endpoint (proxy to CockroachDB's /_status/nodes)
    let nodes_response = client
        .status_nodes()
        .await
        .expect("should be able to query /proxy/status/nodes");

    // Basic sanity check - should be non-empty JSON with node information
    let nodes_text = nodes_response.into_inner();
    assert!(
        !nodes_text.is_empty(),
        "status nodes response should not be empty"
    );
    assert!(
        nodes_text.contains("nodes") || nodes_text.contains("node_id"),
        "should contain node information"
    );

    // Verify that these are working proxy endpoints by checking they return
    // different content types (vars is text metrics, nodes is typically JSON)
    assert_ne!(
        vars_text, nodes_text,
        "vars and nodes should return different data"
    );
}

// Test querying the CockroachDB HTTP interface for Prometheus metrics
#[nexus_test]
async fn test_cockroach_http_prometheus_metrics(
    cptestctx: &ControlPlaneTestContext,
) {
    let admin_addr = cptestctx.database_admin.local_addr();

    let client = CockroachClusterAdminClient::new(cptestctx.logctx.log.clone());
    client.update_backends(&[admin_addr]).await;

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

    for metric in CockroachMetric::iter() {
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

    // Print any missing metrics for debugging
    if !missing_metrics.is_empty() {
        println!("Missing metrics:");
        for metric in missing_metrics {
            println!("  {} - {}", metric.metric_name(), metric.description());
        }
        panic!("Missing metrics; failing test");
    }

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
    let admin_addr = cptestctx.database_admin.local_addr();

    let client = CockroachClusterAdminClient::new(cptestctx.logctx.log.clone());
    client.update_backends(&[admin_addr]).await;

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
