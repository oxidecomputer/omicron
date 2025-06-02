// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::SpMgsSlot;
use crate::SpType;
use crate::typed_uuid::DbTypedUuid;
use chrono::{DateTime, Utc};
use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use ereport_types::Ena;
use nexus_db_schema::schema::{host_ereport, sp_ereport};
use omicron_uuid_kinds::{EreporterRestartKind, OmicronZoneKind, SledKind};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

#[derive(
    Copy,
    Clone,
    Debug,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    AsExpression,
    FromSqlRow,
    Serialize,
    Deserialize,
)]
#[diesel(sql_type = sql_types::BigInt)]
#[repr(transparent)]
pub struct DbEna(pub Ena);

NewtypeFrom! { () pub struct DbEna(Ena); }
NewtypeDeref! { () pub struct DbEna(Ena); }

impl ToSql<sql_types::BigInt, Pg> for DbEna {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <i64 as ToSql<sql_types::BigInt, Pg>>::to_sql(
            &i64::try_from(self.0.0)?,
            &mut out.reborrow(),
        )
    }
}

impl<DB> FromSql<sql_types::BigInt, DB> for DbEna
where
    DB: Backend,
    i64: FromSql<sql_types::BigInt, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        Ena::try_from(i64::from_sql(bytes)?).map(DbEna).map_err(|e| e.into())
    }
}

#[derive(Clone, Debug, Queryable, Selectable)]
#[diesel(table_name = sp_ereport)]
pub struct EreportMetadata {
    pub restart_id: DbTypedUuid<EreporterRestartKind>,
    pub ena: DbEna,

    pub time_collected: DateTime<Utc>,
    pub collector_id: DbTypedUuid<OmicronZoneKind>,
}

#[derive(Clone, Debug, Queryable, Selectable)]
#[diesel(table_name = sp_ereport)]
pub struct SpEreport {
    #[diesel(embed)]
    pub metadata: EreportMetadata,

    pub sp_type: SpType,
    pub sp_slot: SpMgsSlot,

    pub report: serde_json::Value,
}

#[derive(Clone, Debug, Queryable, Selectable)]
#[diesel(table_name = host_ereport)]
pub struct HostEreport {
    #[diesel(embed)]
    pub metadata: EreportMetadata,

    pub sled_id: DbTypedUuid<SledKind>,
    pub sled_serial: String,

    pub report: serde_json::Value,
}
