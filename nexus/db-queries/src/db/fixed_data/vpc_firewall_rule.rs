// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use lazy_static::lazy_static;
use omicron_common::api::external::VpcFirewallRuleUpdate;

lazy_static! {
    /// Built-in VPC firewall rule for External DNS.
    pub static ref DNS_VPC_FW_RULE: VpcFirewallRuleUpdate = serde_json::from_value(
        serde_json::json!({
            "name": "external-dns-inbound",
            "description": "allow inbound connections for DNS from anywhere",
            "status": "enabled",
            "direction": "inbound",
            "targets": [
                {
                    "type": "subnet",
                    "value": super::vpc_subnet::DNS_VPC_SUBNET.identity.name,
                }
            ],
            "filters": { "ports": [ "53" ], "protocols": [ "UDP" ] },
            "action": "allow",
            "priority": 65534
        })
    )
    .expect("invalid firewall rule for dns");

    /// Built-in VPC firewall rule for Nexus.
    pub static ref NEXUS_VPC_FW_RULE: VpcFirewallRuleUpdate = serde_json::from_value(
        serde_json::json!({
            "name": "nexus-inbound",
            "description": "allow inbound connections for console & api from anywhere",
            "status": "enabled",
            "direction": "inbound",
            "targets": [
                {
                    "type": "subnet",
                    "value": super::vpc_subnet::NEXUS_VPC_SUBNET.identity.name,
                }
            ],
            "filters": { "ports": [ "80", "443" ], "protocols": [ "TCP" ] },
            "action": "allow",
            "priority": 65534
        })
    )
    .expect("invalid firewall rule for nexus");
}
