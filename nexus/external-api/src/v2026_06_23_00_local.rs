// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types from API versions before 2026_06_23_00 that cannot live in
//! `nexus-types-versions` because they convert to/from `omicron-common` types
//! (orphan rule).

use omicron_common::api::external;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// Updated list of firewall rules. Will replace all existing rules.
///
/// Before `STRICT_PUT_BODIES`, omitting `rules` was accepted and treated as an
/// empty ruleset.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcFirewallRuleUpdateParams {
    #[schemars(length(max = 1024))]
    #[serde(default)]
    pub rules: Vec<external::VpcFirewallRuleUpdate>,
}

impl From<VpcFirewallRuleUpdateParams>
    for external::VpcFirewallRuleUpdateParams
{
    fn from(p: VpcFirewallRuleUpdateParams) -> Self {
        Self { rules: p.rules }
    }
}
