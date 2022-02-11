// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Internal operations on firewall rules, such as compiling target or host filters.

/*
use omicron_common::api::external;
use crate::db::model;
use sled_agent_client::types as sled_types;
*/

#[derive(Clone, Debug)]
pub struct RuleTargets {
    pub vpcs: BTreeSet<Name>,
    pub subnets: BTreeSet<Name>,
    pub instances: BTreeSet<Name>,
}

#[derive(Clone, Debug)]
pub struct RuleHosts {
    pub vpcs: BTreeSet<Name>,
    pub subnets: BTreeSet<Name>,
    pub instances: BTreeSet<Name>,
    pub ips: BTreeSet<IpNetwork>,
}
