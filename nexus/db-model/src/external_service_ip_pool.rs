// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Model types for assigning IP pools to individual external services.

use super::impl_enum_type;
use crate::DbTypedUuid;
use diesel::Insertable;
use diesel::Queryable;
use diesel::Selectable;
use nexus_db_schema::schema::external_service_ip_pool;
use omicron_uuid_kinds::IpPoolKind;
use serde::Deserialize;
use serde::Serialize;

impl_enum_type!(
    ExternalServiceKindEnum:

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        Eq,
        Serialize,
        Deserialize,
        AsExpression,
        FromSqlRow,
    )]
    #[serde(rename_all = "snake_case")]
    pub enum ExternalServiceKind;

    Nexus => b"nexus"
    BoundaryNtp => b"boundary_ntp"
    ExternalDns => b"external_dns"
);

impl std::fmt::Display for ExternalServiceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ExternalServiceKind::Nexus => "nexus",
            ExternalServiceKind::BoundaryNtp => "boundary_ntp",
            ExternalServiceKind::ExternalDns => "external_dns",
        };
        f.write_str(s)
    }
}

/// Assignment of an IP pool to an external service.
//
// NOTE: The associations here intentionally avoid any semantics like
// exclusivity or what an assignment means for the application. Each service
// could be different in that respect, and we haven't really fleshed out enough
// of the system to know how to use the records here. For now, we just enforce
// basic sanity checks, like that operators cannot delete the last pool assigned
// to a service, or concurrent operations on the pool and this record.
#[derive(
    Queryable, Clone, Copy, Debug, PartialEq, Eq, Selectable, Insertable,
)]
#[diesel(table_name = external_service_ip_pool)]
pub struct ExternalServiceIpPool {
    pub service: ExternalServiceKind,
    pub ip_pool_id: DbTypedUuid<IpPoolKind>,
}
