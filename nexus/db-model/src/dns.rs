// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{impl_enum_type, Generation};
use crate::schema::{dns_name, dns_version, dns_zone};
use chrono::{DateTime, Utc};
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug)]
    #[diesel(postgres_type(name = "dns_group"))]
    pub struct DnsGroupEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    #[diesel(sql_type = DnsGroupEnum)]
    pub enum DnsGroup;

    // Enum values
    System => b"internal"
    Custom => b"external"
);

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
    pub dns_zone_id: Uuid,
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
