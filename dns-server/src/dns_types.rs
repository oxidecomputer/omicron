// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// types describing DNS records and configuration
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::Ipv6Addr;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DnsConfigParams {
    pub generation: u64,
    pub time_created: chrono::DateTime<chrono::Utc>,
    pub zones: Vec<DnsConfigZone>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DnsConfig {
    pub generation: u64,
    pub time_created: chrono::DateTime<chrono::Utc>,
    pub zones: Vec<DnsConfigZone>,
    pub time_applied: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DnsConfigZone {
    pub zone_name: String,
    pub records: Vec<DnsKV>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename = "DnsKv")]
pub struct DnsKV {
    pub key: DnsRecordKey,
    pub records: Vec<DnsRecord>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct DnsRecordKey {
    pub name: String,
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "type", content = "data")]
pub enum DnsRecord {
    AAAA(Ipv6Addr),
    SRV(SRV),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename = "Srv")]
pub struct SRV {
    pub prio: u16,
    pub weight: u16,
    pub port: u16,
    pub target: String,
}
