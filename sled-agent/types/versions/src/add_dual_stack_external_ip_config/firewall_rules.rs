// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Firewall rule types for version `ADD_DUAL_STACK_EXTERNAL_IP_CONFIG`.

use crate::{v9, v10};
use omicron_common::api::external;
use omicron_common::api::internal::shared::ResolvedVpcFirewallRule;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Update firewall rules for a VPC
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct VpcFirewallRulesEnsureBody {
    pub vni: external::Vni,
    pub rules: Vec<ResolvedVpcFirewallRule>,
}

impl TryFrom<v10::firewall_rules::VpcFirewallRulesEnsureBody>
    for VpcFirewallRulesEnsureBody
{
    type Error = external::Error;

    fn try_from(
        v10: v10::firewall_rules::VpcFirewallRulesEnsureBody,
    ) -> Result<Self, Self::Error> {
        Ok(Self { vni: v10.vni, rules: v10.rules })
    }
}

impl TryFrom<v9::firewall_rules::VpcFirewallRulesEnsureBody>
    for VpcFirewallRulesEnsureBody
{
    type Error = external::Error;

    fn try_from(
        v9: v9::firewall_rules::VpcFirewallRulesEnsureBody,
    ) -> Result<Self, Self::Error> {
        // Chain through v10
        let v10 =
            v10::firewall_rules::VpcFirewallRulesEnsureBody::try_from(v9)?;
        Self::try_from(v10)
    }
}
