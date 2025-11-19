// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::EreporterType;
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
use nexus_db_schema::schema::ereport;
use nexus_types::fm::ereport::{self as types, Ena, EreportId};
use omicron_common::api::external::Error;
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

#[derive(Clone, Debug, Insertable, Queryable, Selectable)]
#[diesel(table_name = ereport)]
pub struct Ereport {
    pub restart_id: DbTypedUuid<EreporterRestartKind>,
    pub ena: DbEna,
    pub time_deleted: Option<DateTime<Utc>>,

    pub time_collected: DateTime<Utc>,
    pub collector_id: DbTypedUuid<OmicronZoneKind>,

    /// SP VPD identity: the baseboard part number of the reporter.
    ///
    /// This is nullable, as the ereport may have been generated in a condition
    /// where the SP was unable to determine its own part number, or the host OS
    /// was unable to ask the SP for it. Consider that "I don't know what I am!"
    /// is an error condition for which we might want to generate an ereport!
    pub part_number: Option<String>,
    /// VPD identity: the baseboard serial number of the reporter.
    ///
    /// This is nullable, as the ereport may have been generated in a condition
    /// where the SP was unable to determine its own serial number, or the host
    /// system was unable to ask the SP for it. Consider that "I don't know who
    /// I am!" is an error condition for which we might want to generate an
    /// ereport!
    pub serial_number: Option<String>,

    /// The ereport class, which indicates the category of event reported.
    ///
    /// This is nullable, as it is extracted from the report JSON, and reports
    /// missing class information must still be ingested.
    pub class: Option<String>,
    pub report: serde_json::Value,

    #[diesel(embed)]
    pub reporter: Reporter,
}

#[derive(Copy, Clone, Debug, Insertable, Queryable, Selectable)]
#[diesel(table_name = ereport)]
pub struct Reporter {
    pub reporter: EreporterType,

    //
    // The physical location of the reporting SP.
    //
    /// SP location: the type of SP slot (sled, switch, power shelf).
    ///
    /// For SP ereports (i.e. those with `reporter == EreporterType::Sp`) this
    /// is never NULL, which is enforced by the `reporter_identity_validity`
    /// CHECK constraint. This is because SPs are indexed by their physical
    /// location when requesting ereports through MGS.
    pub sp_type: Option<SpType>,
    /// SP location: the slot number.
    ///
    /// For SP ereports (i.e. those with `reporter == EreporterType::Sp`) this
    /// is never NULL, which is enforced by the `reporter_identity_validity`
    /// CHECK constraint. This is because SPs are indexed by their physical
    /// location when requesting ereports through MGS.
    pub sp_slot: Option<SpMgsSlot>,

    /// For host OS ereports, the sled UUID of the sled-agent from which this
    /// ereport was received.
    ///
    /// This is never NULL for host OS ereports (i.e. those with `reporter ==
    /// EreporterType::Host`). This is enforced by the
    /// `reporter_identity_validity` CHECK constraint.
    pub sled_id: Option<DbTypedUuid<SledKind>>,
}

impl Ereport {
    pub fn id(&self) -> EreportId {
        EreportId { ena: self.ena.into(), restart_id: self.restart_id.into() }
    }

    /// Returns the [`types::Reporter`] identifying the entity that reported
    /// this ereport.
    ///
    /// This function requires that the database record for this ereport is
    /// valid, and returns an error if it does not contain either a non-NULL
    /// `sp_type` and `sp_slot` XOR a non-NULL `sled_id`.
    ///
    /// This constraint is enforced by the `reporter_identity_validity` CHECK
    /// constraint in the database schema, so this should not return an error
    /// unless an ereport is manually constructed in memory with invalid
    /// values, or the database schema has changed.
    pub fn reporter(&self) -> Result<types::Reporter, Error> {
        self.reporter.try_into()
    }

    pub fn new(
        data: types::EreportData,
        reporter: impl Into<Reporter>,
    ) -> Self {
        let types::EreportData {
            id: EreportId { ena, restart_id },
            collector_id,
            time_collected,
            serial_number,
            part_number,
            class,
            report,
        } = data;

        Self {
            ena: ena.into(),
            restart_id: restart_id.into(),
            collector_id: collector_id.into(),
            time_collected,
            time_deleted: None,
            serial_number,
            part_number,
            class,
            report,
            reporter: reporter.into(),
        }
    }
}

impl From<types::Ereport> for Ereport {
    fn from(types::Ereport { data, reporter }: types::Ereport) -> Self {
        Self::new(data, reporter)
    }
}

impl TryFrom<Ereport> for types::Ereport {
    type Error = Error;
    fn try_from(ereport: Ereport) -> Result<Self, Self::Error> {
        let id = ereport.id();
        let Ereport {
            collector_id,
            time_collected,
            serial_number,
            part_number,
            class,
            report,
            reporter,
            ..
        } = ereport;
        let reporter = reporter.try_into().map_err(|e: Error| {
            e.internal_context(format!(
                "ereport {id} has an invalid reporter identity"
            ))
        })?;
        Ok(types::Ereport {
            data: types::EreportData {
                id,
                time_collected,
                collector_id: collector_id.into(),
                serial_number,
                part_number,
                class,
                report,
            },
            reporter,
        })
    }
}

impl From<types::Reporter> for Reporter {
    fn from(reporter: types::Reporter) -> Self {
        match reporter {
            types::Reporter::HostOs { sled } => Self {
                reporter: EreporterType::Host,
                sled_id: Some(sled.into()),
                sp_type: None,
                sp_slot: None,
            },
            types::Reporter::Sp { sp_type, slot } => Self {
                reporter: EreporterType::Sp,
                sp_type: Some(sp_type.into()),
                sp_slot: Some(slot.into()),
                sled_id: None,
            },
        }
    }
}

impl TryFrom<Reporter> for types::Reporter {
    type Error = Error;
    fn try_from(reporter: Reporter) -> Result<Self, Self::Error> {
        match reporter {
            Reporter {
                reporter: EreporterType::Sp,
                sp_type: Some(sp_type),
                sp_slot: Some(slot),
                ..
            } => Ok(Self::Sp {
                sp_type: sp_type.into(),
                slot: crate::SqlU16::from(slot).0,
            }),
            Reporter {
                reporter: EreporterType::Sp, sp_type, sp_slot, ..
            } => Err(Error::InternalError {
                internal_message: format!(
                    "the 'reporter_identity_validity' CHECK constraint \
                     should enforce that ereports with reporter='sp' have \
                     a non-NULL SP type and slot, but this ereport has \
                     sp_type={sp_type:?} and sp_slot={sp_slot:?}",
                ),
            }),
            Reporter {
                reporter: EreporterType::Host,
                sled_id: Some(id),
                ..
            } => Ok(Self::HostOs { sled: id.into() }),
            Reporter {
                reporter: EreporterType::Host, sled_id: None, ..
            } => Err(Error::internal_error(
                "the 'reporter_identity_validity' CHECK constraint \
                     should enforce that ereports with reporter='host' \
                     have a non-NULL sled_id, but this ereport does not",
            )),
        }
    }
}
