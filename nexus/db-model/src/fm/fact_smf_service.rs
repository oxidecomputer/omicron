// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representation of the SMF service diagnosis engine's facts.
//!
//! Each SMF service fact is stored as typed columns in the
//! `fm_fact_smf_service` table. See [`nexus_types::fm::SmfServiceFact`] for
//! semantics.

use crate::DbTypedUuid;
use crate::impl_enum_type;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::fm_fact_smf_service;
use nexus_types::fm;
use nexus_types::fm::case::FactMetadata;
use nexus_types::fm::{FactPayload, SmfServiceFact, SmfServiceFactPayload};
use omicron_uuid_kinds::{
    CaseKind, CollectionKind, FactKind, SitrepKind, SledKind,
};

impl_enum_type!(
    FmFactSmfServiceKindEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq, Eq)]
    pub enum FmFactSmfServiceKind;

    InMaintenance => b"in_maintenance"
);

/// Diesel row for the `fm_fact_smf_service` table.
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_fact_smf_service)]
pub struct FmFactSmfService {
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

    pub sled_id: DbTypedUuid<SledKind>,
    pub zone_name: String,
    pub fmri: String,
    pub kind: FmFactSmfServiceKind,

    pub observed_in_inv: DbTypedUuid<CollectionKind>,
    pub time_observed: DateTime<Utc>,
}

impl FmFactSmfService {
    /// Build a row from a fact's shared metadata (`metadata`) and its
    /// already-dispatched SMF service payload (`smf_fact`).
    pub fn from_sitrep(
        sitrep_id: impl Into<DbTypedUuid<SitrepKind>>,
        case_id: impl Into<DbTypedUuid<CaseKind>>,
        metadata: &FactMetadata,
        smf_fact: &SmfServiceFact,
    ) -> Self {
        let FactMetadata { id, created_sitrep_id, comment } = metadata;
        let kind = match smf_fact {
            SmfServiceFact::InMaintenance(_) => {
                FmFactSmfServiceKind::InMaintenance
            }
        };
        let SmfServiceFactPayload {
            sled_id,
            zone_name,
            fmri,
            observed_in_inv,
            time_observed,
        } = smf_fact.payload();
        Self {
            id: (*id).into(),
            sitrep_id: sitrep_id.into(),
            case_id: case_id.into(),
            created_sitrep_id: (*created_sitrep_id).into(),
            comment: comment.clone(),
            sled_id: (*sled_id).into(),
            zone_name: zone_name.clone(),
            fmri: fmri.clone(),
            kind,
            observed_in_inv: (*observed_in_inv).into(),
            time_observed: *time_observed,
        }
    }

    /// Reconstruct an in-memory fact from a row.
    pub fn into_fact(self) -> fm::case::Fact {
        let payload = SmfServiceFactPayload {
            sled_id: self.sled_id.into(),
            zone_name: self.zone_name,
            fmri: self.fmri,
            observed_in_inv: self.observed_in_inv.into(),
            time_observed: self.time_observed,
        };
        let payload = match self.kind {
            FmFactSmfServiceKind::InMaintenance => {
                FactPayload::SmfService(SmfServiceFact::InMaintenance(payload))
            }
        };
        fm::case::Fact {
            metadata: fm::case::FactMetadata {
                id: self.id.into(),
                created_sitrep_id: self.created_sitrep_id.into(),
                comment: self.comment,
            },
            payload,
        }
    }
}
