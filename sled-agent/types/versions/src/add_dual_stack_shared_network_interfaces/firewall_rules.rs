// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Firewall rule types for version `ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES`.

use crate::v10::instance::ResolvedVpcFirewallRule;
use omicron_common::api::external;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Update firewall rules for a VPC
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct VpcFirewallRulesEnsureBody {
    pub vni: external::Vni,
    pub rules: Vec<ResolvedVpcFirewallRule>,
}
