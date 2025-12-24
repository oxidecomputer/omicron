// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Firewall rule types for version `ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES`.

use crate::v9;
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

impl TryFrom<v9::firewall_rules::VpcFirewallRulesEnsureBody>
    for VpcFirewallRulesEnsureBody
{
    type Error = external::Error;

    fn try_from(
        v9: v9::firewall_rules::VpcFirewallRulesEnsureBody,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            vni: v9.vni,
            rules: v9
                .rules
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}
