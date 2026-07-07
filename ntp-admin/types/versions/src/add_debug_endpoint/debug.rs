// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{collections::BTreeMap, net::IpAddr};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Diagnostic information
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct DebugInfo {
    pub is_boundary: bool,
    pub external_ntp_servers: Vec<String>,
    pub boundary_pool: String,
    // TODO-K: Clean these up?
    pub dns_look_up_results: BTreeMap<String, Vec<IpAddr>>,
    pub icmp_ping_results: Vec<(String, IpAddr, String)>,
}
