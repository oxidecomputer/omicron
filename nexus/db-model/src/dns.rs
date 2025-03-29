// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{Generation, impl_enum_type};
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::{dns_name, dns_version, dns_zone};
use nexus_types::internal_api::params;
use omicron_common::api::external::Error;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::{fmt, net::Ipv6Addr};
use uuid::Uuid;

impl_enum_type!(
    DnsGroupEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum DnsGroup;

    // Enum values
    Internal => b"internal"
    External => b"external"
);

impl fmt::Display for DnsGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            DnsGroup::Internal => "internal",
            DnsGroup::External => "external",
        })
    }
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = dns_zone)]
pub struct DnsZone {
    pub id: Uuid,
    pub time_created: DateTime<Utc>,
    pub dns_group: DnsGroup,
    pub zone_name: String,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = dns_version)]
pub struct DnsVersion {
    pub dns_group: DnsGroup,
    pub version: Generation,
    pub time_created: DateTime<Utc>,
    pub creator: String,
    pub comment: String,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = dns_name)]
pub struct DnsName {
    pub dns_zone_id: Uuid,
    pub version_added: Generation,
    pub version_removed: Option<Generation>,
    pub name: String,

    // The raw record data in each row is stored as a JSON-serialized
    // `DnsRecord` (not to be confused with `params::DnsRecord, although the
    // types are identical).  This is an implementation detail hidden behind
    // this struct.  For writes, consumers provide a `Vec<params::DnsRecord>` in
    // `DnsName::new()`.  For reads, consumers use `DnsName::records()` to get a
    // `Vec<params::DnsRecord>`.  This struct takes care of converting to/from
    // the stored representation (which happens to be identical).
    dns_record_data: serde_json::Value,
}

impl DnsName {
    pub fn new(
        dns_zone_id: Uuid,
        name: String,
        version_added: Generation,
        version_removed: Option<Generation>,
        records: Vec<params::DnsRecord>,
    ) -> Result<DnsName, Error> {
        let dns_record_data = serde_json::to_value(
            records.into_iter().map(DnsRecord::from).collect::<Vec<_>>(),
        )
        .map_err(|e| {
            Error::internal_error(&format!(
                "failed to serialize DNS records: {:#}",
                e
            ))
        })?;

        Ok(DnsName {
            dns_zone_id,
            version_added,
            version_removed,
            name,
            dns_record_data,
        })
    }

    pub fn records(&self) -> Result<Vec<params::DnsRecord>, Error> {
        let serialized: Vec<DnsRecord> = serde_json::from_value(
            self.dns_record_data.clone(),
        )
        .map_err(|e| {
            Error::internal_error(&format!(
                "failed to deserialize DNS record from database: {:#}",
                e
            ))
        })?;
        Ok(serialized.into_iter().map(params::DnsRecord::from).collect())
    }
}

/// This type is identical to `dns_service_client::DnsRecord`.  It's defined
/// separately here for stability: this type is serialized to JSON and stored
/// into the database.  We don't want the serialized form to change accidentally
/// because someone happens to change the DNS server API.
///
/// BE CAREFUL MODIFYING THIS STRUCT.
#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DnsRecord {
    A(Ipv4Addr),
    AAAA(Ipv6Addr),
    SRV(SRV),
}

impl From<params::DnsRecord> for DnsRecord {
    fn from(value: params::DnsRecord) -> Self {
        match value {
            params::DnsRecord::A(addr) => DnsRecord::A(addr),
            params::DnsRecord::Aaaa(addr) => DnsRecord::AAAA(addr),
            params::DnsRecord::Srv(srv) => DnsRecord::SRV(SRV::from(srv)),
        }
    }
}

impl From<DnsRecord> for params::DnsRecord {
    fn from(value: DnsRecord) -> Self {
        match value {
            DnsRecord::A(addr) => params::DnsRecord::A(addr),
            DnsRecord::AAAA(addr) => params::DnsRecord::Aaaa(addr),
            DnsRecord::SRV(srv) => {
                params::DnsRecord::Srv(params::Srv::from(srv))
            }
        }
    }
}

/// This type is identical to `dns_service_client::SRV`.  It's defined
/// separately here for stability: this type is serialized to JSON and stored
/// into the database.  We don't want the serialized form to change accidentally
/// because someone happens to change the DNS server API.
///
/// BE CAREFUL MODIFYING THIS STRUCT.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename = "Srv")]
pub struct SRV {
    pub prio: u16,
    pub weight: u16,
    pub port: u16,
    pub target: String,
}

impl From<params::Srv> for SRV {
    fn from(srv: params::Srv) -> Self {
        SRV {
            prio: srv.prio,
            weight: srv.weight,
            port: srv.port,
            target: srv.target,
        }
    }
}

impl From<SRV> for params::Srv {
    fn from(srv: SRV) -> Self {
        params::Srv {
            prio: srv.prio,
            weight: srv.weight,
            port: srv.port,
            target: srv.target,
        }
    }
}

/// Describes the initial configuration for a DNS group
///
/// Provides helpers for constructing the database rows to describe that initial
/// configuration
///
/// We assume that there will be exactly one DNS zone in this group.
#[derive(Debug, Clone)]
pub struct InitialDnsGroup {
    dns_group: DnsGroup,
    zone_name: String,
    records: HashMap<String, Vec<params::DnsRecord>>,
    version: Generation,
    dns_zone_id: Uuid,
    time_created: DateTime<Utc>,
    creator: String,
    comment: String,
}

impl InitialDnsGroup {
    pub fn new(
        dns_group: DnsGroup,
        zone_name: &str,
        creator: &str,
        comment: &str,
        records: HashMap<String, Vec<params::DnsRecord>>,
    ) -> InitialDnsGroup {
        InitialDnsGroup {
            dns_group,
            zone_name: zone_name.to_owned(),
            records,
            dns_zone_id: Uuid::new_v4(),
            time_created: Utc::now(),
            version: Generation::new(),
            comment: comment.to_owned(),
            creator: creator.to_owned(),
        }
    }

    pub fn row_for_zone(&self) -> DnsZone {
        DnsZone {
            id: self.dns_zone_id,
            time_created: self.time_created,
            dns_group: self.dns_group,
            zone_name: self.zone_name.clone(),
        }
    }

    pub fn row_for_version(&self) -> DnsVersion {
        DnsVersion {
            dns_group: self.dns_group,
            version: self.version,
            time_created: self.time_created,
            creator: self.creator.clone(),
            comment: self.comment.clone(),
        }
    }

    pub fn rows_for_names(&self) -> Result<Vec<DnsName>, Error> {
        self.records
            .iter()
            .map(|(name, records)| {
                DnsName::new(
                    self.dns_zone_id,
                    name.clone(),
                    self.version,
                    None,
                    records.clone(),
                )
            })
            .collect::<Result<Vec<_>, _>>()
    }
}
