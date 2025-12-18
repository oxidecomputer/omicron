// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Firewall rule types added in version `DELEGATE_ZVOL_TO_PROPOLIS`.

use omicron_common::api::external;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v1::instance::ResolvedVpcFirewallRule;

/// Update firewall rules for a VPC
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct VpcFirewallRulesEnsureBody {
    pub vni: external::Vni,
    pub rules: Vec<ResolvedVpcFirewallRule>,
}
