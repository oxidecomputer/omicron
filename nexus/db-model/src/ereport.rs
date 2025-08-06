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
use omicron_uuid_kinds::{
    EreporterRestartKind, OmicronZoneKind, OmicronZoneUuid, SledKind, SledUuid,
};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::fmt;

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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Ereport {
    #[serde(flatten)]
    pub id: EreportId,
    #[serde(flatten)]
    pub metadata: EreportMetadata,
    pub reporter: Reporter,
    #[serde(flatten)]
    pub report: serde_json::Value,
}

impl From<SpEreport> for Ereport {
    fn from(sp_report: SpEreport) -> Self {
        let SpEreport {
            restart_id,
            ena,
            time_collected,
            time_deleted,
            collector_id,
            part_number,
            serial_number,
            sp_type,
            sp_slot,
            class,
            report,
        } = sp_report;
        Ereport {
            id: EreportId { restart_id: restart_id.into(), ena: ena.into() },
            metadata: EreportMetadata {
                time_collected,
                time_deleted,
                collector_id: collector_id.into(),
                part_number,
                serial_number,
                class,
            },
            reporter: Reporter::Sp { sp_type: sp_type.into(), slot: sp_slot.0 },
            report,
        }
    }
}

impl From<HostEreport> for Ereport {
    fn from(host_report: HostEreport) -> Self {
        let HostEreport {
            restart_id,
            ena,
            time_collected,
            time_deleted,
            collector_id,
            sled_serial,
            sled_id,
            class,
            report,
            part_number,
        } = host_report;
        Ereport {
            id: EreportId { restart_id: restart_id.into(), ena: ena.into() },
            metadata: EreportMetadata {
                time_collected,
                time_deleted,
                collector_id: collector_id.into(),
                part_number,
                serial_number: Some(sled_serial),
                class,
            },
            reporter: Reporter::HostOs { sled: sled_id.into() },
            report,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct EreportMetadata {
    pub time_collected: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
    pub collector_id: OmicronZoneUuid,
    pub part_number: Option<String>,
    pub serial_number: Option<String>,
    pub class: Option<String>,
}

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum Reporter {
    Sp { sp_type: nexus_types::inventory::SpType, slot: u16 },
    HostOs { sled: SledUuid },
}

impl std::fmt::Display for Reporter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sp {
                sp_type: nexus_types::inventory::SpType::Sled,
                slot,
            } => {
                write!(f, "Sled (SP) {slot:02}")
            }
            Self::Sp {
                sp_type: nexus_types::inventory::SpType::Switch,
                slot,
            } => {
                write!(f, "Switch {slot}")
            }
            Self::Sp {
                sp_type: nexus_types::inventory::SpType::Power,
                slot,
            } => {
                write!(f, "PSC {slot}")
            }
            Self::HostOs { sled } => {
                write!(f, "Sled (OS) {sled:?}")
            }
        }
    }
}

#[derive(Clone, Debug, Insertable, Queryable, Selectable)]
#[diesel(table_name = sp_ereport)]
pub struct SpEreport {
    pub restart_id: DbTypedUuid<EreporterRestartKind>,
    pub ena: DbEna,
    pub time_deleted: Option<DateTime<Utc>>,

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
    /// The ereport class, which indicates the category of event reported.
    ///
    /// This is nullable, as it is extracted from the report JSON, and reports
    /// missing class information must still be ingested.
    pub class: Option<String>,

    pub report: serde_json::Value,
}

#[derive(Clone, Debug, Insertable, Queryable, Selectable)]
#[diesel(table_name = host_ereport)]
pub struct HostEreport {
    pub restart_id: DbTypedUuid<EreporterRestartKind>,
    pub ena: DbEna,
    pub time_deleted: Option<DateTime<Utc>>,

    pub time_collected: DateTime<Utc>,
    pub collector_id: DbTypedUuid<OmicronZoneKind>,

    pub sled_id: DbTypedUuid<SledKind>,
    pub sled_serial: String,
    /// The ereport class, which indicates the category of event reported.
    ///
    /// This is nullable, as it is extracted from the report JSON, and reports
    /// missing class information must still be ingested.
    pub class: Option<String>,

    pub report: serde_json::Value,

    // It's a shame this has to be nullable, while the serial is not. However,
    // this field was added in a migration, and we have to be able to handle the
    // case where a sled record was hard-deleted when backfilling the ereport
    // table's part_number column. Sad.
    pub part_number: Option<String>,
}
