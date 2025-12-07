// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! DNS configuration types for API version 2.0.0 (SOA_AND_NS).
//!
//! This version adds:
//! - A `serial` field to [`DnsConfigParams`] and [`DnsConfig`] for SOA records
//! - The [`DnsRecord::Ns`] variant for nameserver records

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

/// Error type for conversions from v1 to v2.
pub enum V1ToV2TranslationError {
    /// The generation number is too large to fit in a u32 serial field.
    GenerationTooLarge,
}

/// Error type for conversions from v2 to v1.
pub enum V2ToV1TranslationError {
    /// The configuration contains records (such as NS records) that cannot
    /// be represented in v1.
    IncompatibleRecord,
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

impl TryFrom<v1::config::DnsConfigParams> for DnsConfigParams {
    type Error = V1ToV2TranslationError;

    fn try_from(v1: v1::config::DnsConfigParams) -> Result<Self, Self::Error> {
        let serial: u32 = v1
            .generation
            .as_u64()
            .try_into()
            .map_err(|_| V1ToV2TranslationError::GenerationTooLarge)?;

        let converted_zones: Vec<DnsConfigZone> =
            v1.zones.into_iter().map(|zone| zone.into()).collect();

        Ok(DnsConfigParams {
            generation: v1.generation,
            serial,
            time_created: v1.time_created,
            zones: converted_zones,
        })
    }
}

impl From<v1::config::DnsConfigZone> for DnsConfigZone {
    fn from(v1: v1::config::DnsConfigZone) -> Self {
        let converted_records: HashMap<String, Vec<DnsRecord>> = v1
            .records
            .into_iter()
            .filter_map(|(name, name_records)| {
                let converted_name_records: Vec<DnsRecord> =
                    name_records.into_iter().map(|rec| rec.into()).collect();
                if converted_name_records.is_empty() {
                    None
                } else {
                    Some((name, converted_name_records))
                }
            })
            .collect();
        DnsConfigZone { zone_name: v1.zone_name, records: converted_records }
    }
}

impl From<v1::config::DnsRecord> for DnsRecord {
    fn from(v1: v1::config::DnsRecord) -> Self {
        match v1 {
            v1::config::DnsRecord::A(ip) => DnsRecord::A(ip),
            v1::config::DnsRecord::Aaaa(ip) => DnsRecord::Aaaa(ip),
            v1::config::DnsRecord::Srv(srv) => DnsRecord::Srv(srv.into()),
        }
    }
}

impl From<v1::config::Srv> for Srv {
    fn from(v1: v1::config::Srv) -> Self {
        Srv {
            prio: v1.prio,
            weight: v1.weight,
            port: v1.port,
            target: v1.target,
        }
    }
}

impl TryFrom<DnsConfig> for v1::config::DnsConfig {
    type Error = V2ToV1TranslationError;

    fn try_from(v2: DnsConfig) -> Result<Self, Self::Error> {
        let DnsConfig {
            generation,
            serial: _,
            time_created,
            time_applied,
            zones,
        } = v2;

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

impl TryFrom<DnsConfigZone> for v1::config::DnsConfigZone {
    type Error = V2ToV1TranslationError;

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

impl TryFrom<DnsRecord> for v1::config::DnsRecord {
    type Error = V2ToV1TranslationError;

    fn try_from(v2: DnsRecord) -> Result<Self, Self::Error> {
        match v2 {
            DnsRecord::A(ip) => Ok(v1::config::DnsRecord::A(ip)),
            DnsRecord::Aaaa(ip) => Ok(v1::config::DnsRecord::Aaaa(ip)),
            DnsRecord::Srv(srv) => Ok(v1::config::DnsRecord::Srv(srv.into())),
            DnsRecord::Ns(_) => Err(V2ToV1TranslationError::IncompatibleRecord),
        }
    }
}

impl From<Srv> for v1::config::Srv {
    fn from(v2: Srv) -> Self {
        v1::config::Srv {
            prio: v2.prio,
            weight: v2.weight,
            port: v2.port,
            target: v2.target,
        }
    }
}
