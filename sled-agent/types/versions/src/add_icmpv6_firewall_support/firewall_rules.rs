// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Firewall rule types for version `ADD_ICMPV6_FIREWALL_SUPPORT`.

use crate::v11;
use crate::v29::instance::ResolvedVpcFirewallRule;
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

impl From<v11::firewall_rules::VpcFirewallRulesEnsureBody>
    for VpcFirewallRulesEnsureBody
{
    fn from(v11: v11::firewall_rules::VpcFirewallRulesEnsureBody) -> Self {
        Self {
            vni: v11.vni,
            rules: v11
                .rules
                .into_iter()
                .map(ResolvedVpcFirewallRule::from)
                .collect(),
        }
    }
}
