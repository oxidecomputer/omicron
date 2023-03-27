// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{impl_enum_type, Generation};
use crate::schema::{dns_name, dns_version, dns_zone};
use chrono::{DateTime, Utc};
use nexus_types::internal_api::params;
use std::fmt;
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "dns_group"))]
    pub struct DnsGroupEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = DnsGroupEnum)]
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

// XXX-dap what? why?
impl diesel::query_builder::QueryId for DnsGroupEnum {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = dns_zone)]
pub struct DnsZone {
    pub id: Uuid,
    pub time_created: DateTime<Utc>,
    pub dns_group: DnsGroup,
    pub zone_name: String, // XXX-dap some more specific type?
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
    pub dns_record_data: serde_json::Value,
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
    records: Vec<params::DnsKv>,
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
        records: Vec<params::DnsKv>,
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

    pub fn rows_for_names(&self) -> Result<Vec<DnsName>, serde_json::Error> {
        self.records
            .iter()
            .map(|r| {
                Ok(DnsName {
                    dns_zone_id: self.dns_zone_id,
                    version_added: self.version,
                    version_removed: None,
                    name: r.key.name.clone(),
                    dns_record_data: serde_json::to_value(&r.records)?,
                })
            })
            .collect::<Result<Vec<_>, _>>()
    }
}
