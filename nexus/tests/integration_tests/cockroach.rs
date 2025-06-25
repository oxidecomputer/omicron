// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for accessing CockroachDB

use cockroach_admin_client::Client as CockroachAdminClient;
use nexus_test_utils_macros::nexus_test;

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
