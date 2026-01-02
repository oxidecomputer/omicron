// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::api::external::Generation;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{Ipv4Addr, Ipv6Addr};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct DnsConfigParams {
    pub generation: Generation,
    pub time_created: chrono::DateTime<chrono::Utc>,
    pub zones: Vec<DnsConfigZone>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct DnsConfig {
    pub generation: Generation,
    pub time_created: chrono::DateTime<chrono::Utc>,
    pub time_applied: chrono::DateTime<chrono::Utc>,
    pub zones: Vec<DnsConfigZone>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct DnsConfigZone {
    pub zone_name: String,
    pub records: HashMap<String, Vec<DnsRecord>>,
}

#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    JsonSchema,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
)]
#[serde(tag = "type", content = "data")]
pub enum DnsRecord {
    A(Ipv4Addr),
    // The renames are because openapi-lint complains about `Aaaa` and `Srv`
    // not being in screaming snake case. `Aaaa` and `Srv` are the idiomatic
    // Rust casings, though.
    #[serde(rename = "AAAA")]
    Aaaa(Ipv6Addr),
    #[serde(rename = "SRV")]
    Srv(Srv),
}

#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    JsonSchema,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
)]
pub struct Srv {
    pub prio: u16,
    pub weight: u16,
    pub port: u16,
    pub target: String,
}
