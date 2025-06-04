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
use ereport_types::{Ena, EreportId};
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

#[derive(Clone, Debug)]
pub struct EreportMetadata {
    pub restart_id: DbTypedUuid<EreporterRestartKind>,
    pub ena: DbEna,

    pub time_collected: DateTime<Utc>,
    pub collector_id: DbTypedUuid<OmicronZoneKind>,
}

pub type EreportMetadataTuple = (
    DbTypedUuid<EreporterRestartKind>,
    DbEna,
    DateTime<Utc>,
    DbTypedUuid<OmicronZoneKind>,
);

impl EreportMetadata {
    pub fn id(&self) -> EreportId {
        EreportId { restart_id: self.restart_id.into(), ena: self.ena.into() }
    }
}

impl From<EreportMetadataTuple> for EreportMetadata {
    fn from(
        (restart_id, ena, time_collected, collector_id): EreportMetadataTuple,
    ) -> Self {
        EreportMetadata { restart_id, ena, time_collected, collector_id }
    }
}

#[derive(Clone, Debug, Insertable, Queryable, Selectable)]
#[diesel(table_name = sp_ereport)]
pub struct SpEreport {
    pub restart_id: DbTypedUuid<EreporterRestartKind>,
    pub ena: DbEna,

    pub time_collected: DateTime<Utc>,
    pub collector_id: DbTypedUuid<OmicronZoneKind>,

    //
    // The physical location of the reporting SP.
    //
    /// SP location: the type of SP slot (sled, switch, power shelf).
    ///
    /// This is always known, as SPs are indexed by physical location when
    /// collecting ereports from MGS.
    pub sp_type: SpType,
    /// SP location: the slot number.
    ///
    /// This is always known, as SPs are indexed by physical location when
    /// collecting ereports from MGS.
    pub sp_slot: SpMgsSlot,

    /// SP VPD identity: the baseboard part number of the reporting SP.
    ///
    /// This is nullable, as the ereport may have been generated in a condition
    /// where the SP was unable to determine its own part number. Consider that
    /// "I don't know what I am!" is an error condition for which we might want
    /// to generate an ereport!
    pub part_number: Option<String>,
    /// SP VPD identity: the baseboard serial number of the reporting SP.
    ///
    /// This is nullable, as the ereport may have been generated in a condition
    /// where the SP was unable to determine its own serial number. Consider that
    /// "I don't know who I am!" is an error condition for which we might want
    /// to generate an ereport!
    pub serial_number: Option<String>,

    pub report: serde_json::Value,
}

#[derive(Clone, Debug, Insertable, Queryable, Selectable)]
#[diesel(table_name = host_ereport)]
pub struct HostEreport {
    pub restart_id: DbTypedUuid<EreporterRestartKind>,
    pub ena: DbEna,

    pub time_collected: DateTime<Utc>,
    pub collector_id: DbTypedUuid<OmicronZoneKind>,

    pub sled_id: DbTypedUuid<SledKind>,
    pub sled_serial: String,

    pub report: serde_json::Value,
}
