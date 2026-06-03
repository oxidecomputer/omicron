// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Firewall rule types for version `RENAME_FIREWALL_ICMP_PROTOCOLS`.

use crate::v31;
use crate::v42::instance::ResolvedVpcFirewallRule;
use omicron_common::api::external;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// Update firewall rules for a VPC
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct VpcFirewallRulesEnsureBody {
    pub vni: external::Vni,
    pub rules: Vec<ResolvedVpcFirewallRule>,
}

impl From<v31::firewall_rules::VpcFirewallRulesEnsureBody>
    for VpcFirewallRulesEnsureBody
{
    fn from(old: v31::firewall_rules::VpcFirewallRulesEnsureBody) -> Self {
        Self {
            vni: old.vni,
            rules: old
                .rules
                .into_iter()
                .map(ResolvedVpcFirewallRule::from)
                .collect(),
        }
    }
}
