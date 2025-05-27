// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v2;
use anyhow::ensure;
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

impl DnsConfigParams {
    /// Given a high-level DNS configuration, return a reference to its sole
    /// DNS zone.
    ///
    /// # Errors
    ///
    /// Returns an error if there are 0 or more than one zones in this
    /// configuration.
    pub fn sole_zone(&self) -> Result<&DnsConfigZone, anyhow::Error> {
        ensure!(
            self.zones.len() == 1,
            "expected exactly one DNS zone, but found {}",
            self.zones.len()
        );
        Ok(&self.zones[0])
    }
}

pub enum TranslationError {
    GenerationTooLarge,
}

impl TryInto<v2::config::DnsConfigParams> for DnsConfigParams {
    type Error = TranslationError;

    fn try_into(self) -> Result<v2::config::DnsConfigParams, Self::Error> {
        let serial: u32 = self
            .generation
            .as_u64()
            .try_into()
            .map_err(|_| TranslationError::GenerationTooLarge)?;

        let mut converted_zones: Vec<v2::config::DnsConfigZone> = Vec::new();
        for zone in self.zones.into_iter() {
            converted_zones.push(zone.into());
        }

        Ok(v2::config::DnsConfigParams {
            generation: self.generation,
            serial,
            time_created: self.time_created,
            zones: converted_zones,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct DnsConfig {
    pub generation: Generation,
    pub time_created: chrono::DateTime<chrono::Utc>,
    pub time_applied: chrono::DateTime<chrono::Utc>,
    pub zones: Vec<DnsConfigZone>,
}

// See docs on [`v2::config::DnsConfigZone`] for more about this struct. They are functionally
// equivalent. We would include that doc comment here, but altering docs to existing types
// makes them appear different in OpenAPI terms and would be "breaking" for the time being.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct DnsConfigZone {
    pub zone_name: String,
    pub records: HashMap<String, Vec<DnsRecord>>,
}

impl Into<v2::config::DnsConfigZone> for DnsConfigZone {
    fn into(self) -> v2::config::DnsConfigZone {
        let converted_records: HashMap<String, Vec<v2::config::DnsRecord>> =
            self.records
                .into_iter()
                .filter_map(|(name, name_records)| {
                    let converted_name_records: Vec<v2::config::DnsRecord> =
                        name_records
                            .into_iter()
                            .map(|rec| rec.into())
                            .collect();
                    if converted_name_records.is_empty() {
                        None
                    } else {
                        Some((name, converted_name_records))
                    }
                })
                .collect();
        v2::config::DnsConfigZone {
            zone_name: self.zone_name,
            names: converted_records,
        }
    }
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

impl Into<v2::config::DnsRecord> for DnsRecord {
    fn into(self) -> v2::config::DnsRecord {
        match self {
            DnsRecord::A(ip) => v2::config::DnsRecord::A(ip),
            DnsRecord::Aaaa(ip) => v2::config::DnsRecord::Aaaa(ip),
            DnsRecord::Srv(srv) => v2::config::DnsRecord::Srv(srv.into()),
        }
    }
}

// The `From<Ipv4Addr>` and `From<Ipv6Addr>` implementations are very slightly
// dubious, because a v4 or v6 address could also theoretically map to a DNS
// PTR record
// (https://www.cloudflare.com/learning/dns/dns-records/dns-ptr-record/).
// However, we don't support PTR records at the moment, so this is fine. Would
// certainly be worth revisiting if we do in the future, though.

impl From<Ipv4Addr> for DnsRecord {
    fn from(ip: Ipv4Addr) -> Self {
        DnsRecord::A(ip)
    }
}

impl From<Ipv6Addr> for DnsRecord {
    fn from(ip: Ipv6Addr) -> Self {
        DnsRecord::Aaaa(ip)
    }
}

impl From<Srv> for DnsRecord {
    fn from(srv: Srv) -> Self {
        DnsRecord::Srv(srv)
    }
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

impl From<v2::config::Srv> for Srv {
    fn from(other: v2::config::Srv) -> Self {
        Srv {
            prio: other.prio,
            weight: other.weight,
            port: other.port,
            target: other.target,
        }
    }
}
