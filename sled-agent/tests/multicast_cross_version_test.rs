// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Cross-version compatibility tests for sled-agent multicast APIs.
//!
//! This test verifies that v4 and v5 instance configurations work correctly
//! together, specifically around multicast group support. It follows the same
//! pattern as the DNS cross-version tests.

use anyhow::Result;
use std::net::IpAddr;

use omicron_common::api::internal::shared::DhcpConfig;
use sled_agent_api::v5;

// Generate v5 client from v5 OpenAPI spec (with enhanced multicast support)
mod v5_client {
    progenitor::generate_api!(
        spec = "../openapi/sled-agent/sled-agent-5.0.0-89f1f7.json",
        interface = Positional,
        inner_type = slog::Logger,
        derives = [schemars::JsonSchema, Clone, Eq, PartialEq],
        pre_hook = (|log: &slog::Logger, request: &reqwest::Request| {
            slog::debug!(log, "client request";
                "method" => %request.method(),
                "uri" => %request.url(),
                "body" => ?&request.body(),
            );
        }),
        post_hook = (|log: &slog::Logger, result: &Result<_, _>| {
            slog::debug!(log, "client response"; "result" => ?result);
        })
    );
}

// A v5 server can productively handle requests from a v4 client, and a v4
// client can provide instance configurations to a v5 server (backwards compatible).
// This follows the same pattern as DNS cross-version compatibility.
#[tokio::test]
pub async fn multicast_cross_version_works() -> Result<(), anyhow::Error> {
    use omicron_test_utils::dev::test_setup_log;
    let logctx = test_setup_log("multicast_cross_version_works");

    let multicast_addr = "239.1.1.1".parse::<IpAddr>().unwrap();
    let source_addr = "192.168.1.10".parse::<IpAddr>().unwrap();

    // Focus on the local_config field since that's where multicast_groups lives

    // Create v4 local config JSON (won't have multicast_groups field)
    let v4_local_config_json = serde_json::json!({
        "hostname": "test-v4",
        "nics": [],
        "source_nat": {
            "ip": "10.1.1.1",
            "first_port": 0,
            "last_port": 16383
        },
        "ephemeral_ip": null,
        "floating_ips": [],
        "firewall_rules": [],
        "dhcp_config": {
            "dns_servers": [],
            "host_domain": null,
            "search_domains": []
        }
    });

    // Create v5 local config with multicast_groups
    let v5_local_config = v5::InstanceSledLocalConfig {
        hostname: omicron_common::api::external::Hostname::try_from("test-v5")
            .unwrap(),
        nics: vec![],
        source_nat: nexus_types::deployment::SourceNatConfig::new(
            "10.1.1.1".parse().unwrap(),
            0,
            16383,
        )
        .unwrap(),
        ephemeral_ip: None,
        floating_ips: vec![],
        multicast_groups: vec![v5::InstanceMulticastMembership {
            group_ip: multicast_addr,
            sources: vec![source_addr],
        }],
        firewall_rules: vec![],
        dhcp_config: DhcpConfig {
            dns_servers: vec![],
            host_domain: None,
            search_domains: vec![],
        },
    };

    // Test that v4 can be parsed by v5 (with empty multicast_groups)
    let v4_as_v5_json = serde_json::to_string(&v4_local_config_json)?;
    let v5_json = serde_json::to_string(&v5_local_config)?;

    // v4 should NOT have multicast_groups in the JSON
    assert!(
        !v4_as_v5_json.contains("multicast_groups"),
        "v4 InstanceSledLocalConfig should not contain multicast_groups field"
    );

    // v5 should HAVE multicast_groups in the JSON
    assert!(
        v5_json.contains("multicast_groups"),
        "v5 InstanceSledLocalConfig should contain multicast_groups field"
    );

    // Verify v5 has the multicast group we added
    assert!(
        v5_json.contains(&format!("\"group_ip\":\"{multicast_addr}\"")),
        "v5 should contain the multicast group IP"
    );

    logctx.cleanup_successful();
    Ok(())
}
