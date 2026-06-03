// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representation of the physical-disk diagnosis engine's facts.
//!
//! Each physical-disk fact is stored as typed columns in the
//! `fm_fact_physical_disk` table. The `kind` discriminant selects which payload
//! columns are populated; a CHECK constraint
//! (`zpool_unhealthy_columns_present`) enforces that the right columns are
//! non-NULL for each kind. See [`nexus_types::fm::DiskFact`] for semantics.

use crate::DbTypedUuid;
use crate::impl_enum_type;
use crate::inventory::InvZpoolHealth;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::fm_fact_physical_disk;
use nexus_types::fm;
use nexus_types::fm::{DiskFact, FactPayload, ZpoolUnhealthyFactPayload};
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{
    CaseKind, CollectionKind, FactKind, PhysicalDiskKind, SitrepKind, ZpoolKind,
};

impl_enum_type!(
    FmFactPhysicalDiskKindEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq, Eq)]
    pub enum FmFactPhysicalDiskKind;

    ZpoolUnhealthy => b"zpool_unhealthy"
);

/// Diesel row for the `fm_fact_physical_disk` table.
///
/// The payload columns are populated according to `kind`: a column is `Some`
/// iff it belongs to that `kind`'s payload. This is enforced in the database
/// by the per-kind CHECK constraints (e.g. `zpool_unhealthy_columns_present`).
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_fact_physical_disk)]
pub struct FmFactPhysicalDisk {
    pub id: DbTypedUuid<FactKind>,
    /// The sitrep to which this fact belongs.
    ///
    /// This will change as the fact is carried forward from one sitrep to the
    /// next.
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub case_id: DbTypedUuid<CaseKind>,
    /// Sitrep in which this fact was first added.
    ///
    /// Preserved unchanged when the fact is carried forward; debug-only.
    pub created_sitrep_id: DbTypedUuid<SitrepKind>,
    pub comment: String,
    pub kind: FmFactPhysicalDiskKind,

    // Columns for the `ZpoolUnhealthy` kind.
    pub physical_disk_id: Option<DbTypedUuid<PhysicalDiskKind>>,
    pub zpool_id: Option<DbTypedUuid<ZpoolKind>>,
    pub last_seen_health: Option<InvZpoolHealth>,
    pub observed_in_inv: Option<DbTypedUuid<CollectionKind>>,
    pub time_observed: Option<DateTime<Utc>>,
}

impl FmFactPhysicalDisk {
    /// Build a row from a fact's shared metadata (`fact`) and its
    /// already-dispatched physical-disk payload (`disk_fact`).
    ///
    /// Callers route each fact to its engine's table by matching on
    /// [`fact.payload`](fm::case::Fact::payload) and pass the matched payload
    /// here, so this never has to interpret another engine's payload.
    pub fn from_sitrep(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        case_id: impl Into<DbTypedUuid<CaseKind>>,
        fact: &fm::case::Fact,
        disk_fact: &DiskFact,
    ) -> Self {
        let base = Self {
            id: fact.id.into(),
            sitrep_id: sitrep_id.into(),
            case_id: case_id.into(),
            created_sitrep_id: fact.created_sitrep_id.into(),
            comment: fact.comment.clone(),
            kind: FmFactPhysicalDiskKind::ZpoolUnhealthy,
            physical_disk_id: None,
            zpool_id: None,
            last_seen_health: None,
            observed_in_inv: None,
            time_observed: None,
        };
        match disk_fact {
            DiskFact::ZpoolUnhealthy(p) => Self {
                kind: FmFactPhysicalDiskKind::ZpoolUnhealthy,
                physical_disk_id: Some(p.physical_disk_id.into()),
                zpool_id: Some(p.zpool_id.into()),
                last_seen_health: Some(p.last_seen_health.into()),
                observed_in_inv: Some(p.observed_in_inv.into()),
                time_observed: Some(p.time_observed),
                ..base
            },
        }
    }

    /// Reconstruct an in-memory fact from a row.
    ///
    /// The payload columns the database's CHECK constraint guarantees are
    /// non-NULL for this `kind` are unwrapped; a NULL where one is required
    /// indicates a corrupt row (e.g. hand-edited) and yields an internal
    /// error rather than a panic.
    pub fn into_fact(self) -> Result<fm::case::Fact, Error> {
        let kind = self.kind;
        let payload = match kind {
            FmFactPhysicalDiskKind::ZpoolUnhealthy => {
                FactPayload::PhysicalDisk(DiskFact::ZpoolUnhealthy(
                    ZpoolUnhealthyFactPayload {
                        physical_disk_id: self
                            .physical_disk_id
                            .ok_or_else(|| {
                                missing_column(kind, "physical_disk_id")
                            })?
                            .into(),
                        zpool_id: self
                            .zpool_id
                            .ok_or_else(|| missing_column(kind, "zpool_id"))?
                            .into(),
                        last_seen_health: self
                            .last_seen_health
                            .ok_or_else(|| {
                                missing_column(kind, "last_seen_health")
                            })?
                            .into(),
                        observed_in_inv: self
                            .observed_in_inv
                            .ok_or_else(|| {
                                missing_column(kind, "observed_in_inv")
                            })?
                            .into(),
                        time_observed: self.time_observed.ok_or_else(|| {
                            missing_column(kind, "time_observed")
                        })?,
                    },
                ))
            }
        };
        Ok(fm::case::Fact {
            id: self.id.into(),
            created_sitrep_id: self.created_sitrep_id.into(),
            payload,
            comment: self.comment,
        })
    }
}

fn missing_column(kind: FmFactPhysicalDiskKind, column: &str) -> Error {
    Error::internal_error(&format!(
        "fm_fact_physical_disk row of kind {kind:?} has a NULL {column}, \
         violating the CHECK constraint requiring it to be non-NULL for \
         this kind"
    ))
}
