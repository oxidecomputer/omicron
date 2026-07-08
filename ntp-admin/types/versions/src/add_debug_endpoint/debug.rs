// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::net::IpAddr;
use std::time::Duration;

/// Diagnostic information
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct DebugInfo {
    pub is_boundary: bool,
    pub external_ntp_servers: Vec<String>,
    pub boundary_pool: String,
    pub dns_lookup_results: Vec<DnsLookup>,
    pub icmp_ping_results: Vec<IcmpPing>,
}

// TODO-K: Add some API documentation

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum IcmpPingResult {
    Reachable { rtt: Duration },
    // TODO-K: Make this a real error
    Unreachable { msg: String },
}

impl std::fmt::Display for IcmpPingResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            IcmpPingResult::Reachable { rtt } => {
                format!("reachable; rtt: {}ms", rtt.as_millis())
            }
            IcmpPingResult::Unreachable { msg } => {
                format!("unreachable; {}", msg)
            }
        };
        write!(f, "{s}")
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct IcmpPing {
    pub host: String,
    pub ip: IpAddr,
    pub result: IcmpPingResult,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct DnsLookup {
    pub host: String,
    pub ips: Vec<IpAddr>,
}
