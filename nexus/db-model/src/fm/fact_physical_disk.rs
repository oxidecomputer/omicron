// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representation of the physical-disk diagnosis engine's facts.
//!
//! Each physical-disk fact is stored as typed columns in the
//! `fm_fact_physical_disk` table. The `kind` discriminant selects which payload
//! columns are populated.

use crate::DbTypedUuid;
use crate::impl_enum_type;
use crate::inventory::InvZpoolHealth;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::fm_fact_physical_disk;
use nexus_types::fm;
use nexus_types::fm::case::FactMetadata;
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
/// if it belongs to that `kind`'s payload, and `None` otherwise.
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

    /// The physical disk this fact is about. Common to every `kind`.
    pub physical_disk_id: DbTypedUuid<PhysicalDiskKind>,
    pub kind: FmFactPhysicalDiskKind,

    // Columns for the `ZpoolUnhealthy` kind.
    pub zpool_id: Option<DbTypedUuid<ZpoolKind>>,
    pub last_seen_health: Option<InvZpoolHealth>,
    pub observed_in_inv: Option<DbTypedUuid<CollectionKind>>,
    pub time_observed: Option<DateTime<Utc>>,
}

impl FmFactPhysicalDisk {
    /// Build a row from a fact's shared metadata (`metadata`) and its
    /// physical-disk payload (`disk_fact`).
    pub fn from_sitrep(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        case_id: impl Into<DbTypedUuid<CaseKind>>,
        metadata: &FactMetadata,
        disk_fact: &DiskFact,
    ) -> Self {
        // Destructure exhaustively: a new `FactMetadata` field will fail to
        // compile here until it is mapped to a column.
        let FactMetadata { id, created_sitrep_id, comment } = metadata;
        let mut row = Self {
            id: (*id).into(),
            sitrep_id: sitrep_id.into(),
            case_id: case_id.into(),
            created_sitrep_id: (*created_sitrep_id).into(),
            comment: comment.clone(),
            physical_disk_id: disk_fact.physical_disk_id().into(),
            kind: db_kind(disk_fact),
            zpool_id: None,
            last_seen_health: None,
            observed_in_inv: None,
            time_observed: None,
        };
        // Each arm populates the columns belonging to its `kind` and leaves
        // every other payload column `None`. A column missed here is caught
        // at insert time by the table's per-kind CHECK constraint.
        match disk_fact {
            DiskFact::ZpoolUnhealthy(p) => {
                row.zpool_id = Some(p.zpool_id.into());
                row.last_seen_health = Some(p.last_seen_health.into());
                row.observed_in_inv = Some(p.observed_in_inv.into());
                row.time_observed = Some(p.time_observed);
            }
        }
        row
    }

    /// Reconstruct an in-memory fact from a row.
    ///
    /// A NULL in a column the CHECK constraint requires for this `kind`
    /// yields an internal error rather than a panic.
    pub fn into_fact(self) -> Result<fm::case::Fact, Error> {
        let kind = self.kind;
        let payload = match kind {
            FmFactPhysicalDiskKind::ZpoolUnhealthy => {
                FactPayload::PhysicalDisk(DiskFact::ZpoolUnhealthy(
                    ZpoolUnhealthyFactPayload {
                        physical_disk_id: self.physical_disk_id.into(),
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
            metadata: FactMetadata {
                id: self.id.into(),
                created_sitrep_id: self.created_sitrep_id.into(),
                comment: self.comment,
            },
            payload,
        })
    }
}

/// The `kind` discriminant for a fact's payload. Exhaustive by construction:
/// adding a `DiskFact` variant will not compile until it is mapped here, so
/// `from_sitrep` can never write a row whose `kind` was defaulted rather than
/// derived from the payload.
fn db_kind(disk_fact: &DiskFact) -> FmFactPhysicalDiskKind {
    match disk_fact {
        DiskFact::ZpoolUnhealthy(_) => FmFactPhysicalDiskKind::ZpoolUnhealthy,
    }
}

fn missing_column(kind: FmFactPhysicalDiskKind, column: &str) -> Error {
    Error::internal_error(&format!(
        "fm_fact_physical_disk row of kind {kind:?} has a NULL {column}, \
         violating the CHECK constraint requiring it to be non-NULL for \
         this kind"
    ))
}
