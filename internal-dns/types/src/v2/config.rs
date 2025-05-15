// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::v1;
use anyhow::ensure;
use omicron_common::api::external::Generation;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{Ipv4Addr, Ipv6Addr};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct DnsConfigParams {
    pub generation: Generation,
    /// See [`DnsConfig`]'s `serial` field for how this is different from `generation`
    pub serial: u32,
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

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct DnsConfig {
    pub generation: Generation,
    /// A serial number for this DNS configuration, as should be used in SOA
    /// records describing the configuration's zones. This is a property of the
    /// overall DNS configuration for convenience: Nexus versions DNS
    /// configurations at this granularity, and we expect Nexus will derive
    /// serial numbers from that version.
    pub serial: u32,
    pub time_created: chrono::DateTime<chrono::Utc>,
    pub time_applied: chrono::DateTime<chrono::Utc>,
    pub zones: Vec<DnsConfigZone>,
}

pub enum TranslationError {
    IncompatibleRecord,
}

impl TryFrom<DnsConfig> for v1::config::DnsConfig {
    type Error = TranslationError;

    fn try_from(v2: DnsConfig) -> Result<Self, Self::Error> {
        let DnsConfig { generation, serial: _, time_created, time_applied, zones } = v2;

        Ok(v1::config::DnsConfig {
            generation,
            time_created,
            time_applied,
            zones: zones
                .into_iter()
                .map(|zone| zone.try_into())
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

/// Configuration for a specific DNS zone, as opposed to illumos zones in which
/// the services described by these records run.
///
/// The name `@` is special: it describes records that should be provided for
/// queries about `zone_name`. This is used in favor of the empty string as `@`
/// is the name used for this purpose in zone files for most DNS configurations.
/// It also avoids potentially-confusing debug output from naively printing out
/// records and their names - if you've seen an `@` record and tools are unclear
/// about what that means, hopefully you've arrived here!
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct DnsConfigZone {
    pub zone_name: String,
    pub records: HashMap<String, Vec<DnsRecord>>,
}

impl TryFrom<DnsConfigZone> for v1::config::DnsConfigZone {
    type Error = TranslationError;

    fn try_from(v2: DnsConfigZone) -> Result<Self, Self::Error> {
        let DnsConfigZone { zone_name, records } = v2;

        Ok(v1::config::DnsConfigZone {
            zone_name,
            records: records
                .into_iter()
                .map(|(name, records)| {
                    let converted_records = records
                        .into_iter()
                        .map(|v| v.try_into())
                        .collect::<Result<_, _>>();
                    converted_records.map(|records| (name, records))
                })
                .collect::<Result<_, _>>()?,
        })
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
    #[serde(rename = "NS")]
    Ns(String),
}

impl TryFrom<DnsRecord> for v1::config::DnsRecord {
    type Error = TranslationError;

    fn try_from(v2: DnsRecord) -> Result<Self, Self::Error> {
        match v2 {
            DnsRecord::A(ip) => Ok(v1::config::DnsRecord::A(ip)),
            DnsRecord::Aaaa(ip) => Ok(v1::config::DnsRecord::Aaaa(ip)),
            DnsRecord::Srv(srv) => Ok(v1::config::DnsRecord::Srv(srv.into())),
            DnsRecord::Ns(_) => Err(TranslationError::IncompatibleRecord),
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

impl From<v1::config::Srv> for Srv {
    fn from(other: v1::config::Srv) -> Self {
        Srv {
            prio: other.prio,
            weight: other.weight,
            port: other.port,
            target: other.target,
        }
    }
}
