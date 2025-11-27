// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault management cases.

use super::AlertRequest;
use super::DiagnosisEngine;
use crate::DbTypedUuid;
use crate::SpMgsSlot;
use crate::SpType;
use crate::ereport;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::{
    fm_case, fm_case_impacts_location, fm_ereport_in_case,
};
use nexus_types::fm;
use omicron_uuid_kinds::{
    CaseKind, EreporterRestartKind, SitrepKind, SitrepUuid,
};

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_case)]
pub struct CaseMetadata {
    pub id: DbTypedUuid<CaseKind>,
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub de: DiagnosisEngine,

    pub created_sitrep_id: DbTypedUuid<SitrepKind>,
    pub time_created: DateTime<Utc>,

    pub time_closed: Option<DateTime<Utc>>,
    pub closed_sitrep_id: Option<DbTypedUuid<SitrepKind>>,

    pub comment: String,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_ereport_in_case)]
pub struct CaseEreport {
    pub restart_id: DbTypedUuid<EreporterRestartKind>,
    pub ena: ereport::DbEna,
    pub case_id: DbTypedUuid<CaseKind>,
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub assigned_sitrep_id: DbTypedUuid<SitrepKind>,
    pub comment: String,
}

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_case_impacts_location)]
pub struct CaseImpactsLocation {
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub case_id: DbTypedUuid<CaseKind>,
    pub sp_type: SpType,
    pub sp_slot: SpMgsSlot,
    pub created_sitrep_id: DbTypedUuid<SitrepKind>,
    pub comment: String,
}

#[derive(Clone, Debug)]
pub struct Case {
    pub metadata: CaseMetadata,
    pub ereports: Vec<CaseEreport>,
    pub impacted_locations: Vec<CaseImpactsLocation>,
    pub alerts_requested: Vec<AlertRequest>,
}

impl Case {
    pub fn from_sitrep(sitrep_id: SitrepUuid, case: fm::Case) -> Self {
        let sitrep_id = sitrep_id.into();
        let case_id = case.id.into();
        let ereports = case
            .ereports
            .into_iter()
            .map(
                |fm::case::CaseEreport {
                     ereport,
                     assigned_sitrep_id,
                     comment,
                 }| {
                    let restart_id = ereport.id().restart_id.into();
                    let ena = ereport.id().ena.into();
                    CaseEreport {
                        case_id,
                        restart_id,
                        ena,
                        comment,
                        sitrep_id,
                        assigned_sitrep_id: assigned_sitrep_id.into(),
                    }
                },
            )
            .collect();
        let impacted_locations = case
            .impacted_locations
            .into_iter()
            .map(
                |fm::case::ImpactedLocation {
                     sp_type,
                     slot,
                     comment,
                     created_sitrep_id,
                 }| CaseImpactsLocation {
                    sitrep_id,
                    case_id,
                    sp_type: sp_type.into(),
                    sp_slot: SpMgsSlot::from(slot as u16),
                    created_sitrep_id: created_sitrep_id.into(),
                    comment,
                },
            )
            .collect();
        let alerts_requested = case
            .alerts_requested
            .into_iter()
            .map(
                |fm::AlertRequest {
                     id,
                     class,
                     payload,
                     requested_sitrep_id,
                 }| AlertRequest {
                    sitrep_id,
                    case_id,
                    class: class.into(),
                    id: id.into(),
                    payload,
                    requested_sitrep_id: requested_sitrep_id.into(),
                },
            )
            .collect();

        Self {
            metadata: CaseMetadata {
                id: case_id,
                sitrep_id,
                de: case.de.into(),
                created_sitrep_id: case.created_sitrep_id.into(),
                time_created: case.time_created.into(),
                time_closed: case.time_closed.map(Into::into),
                closed_sitrep_id: case.closed_sitrep_id.map(Into::into),
                comment: case.comment,
            },
            ereports,
            impacted_locations,
            alerts_requested,
        }
    }
}

impl From<CaseImpactsLocation> for fm::case::ImpactedLocation {
    fn from(loc: CaseImpactsLocation) -> Self {
        let CaseImpactsLocation {
            sitrep_id: _,
            case_id: _,
            sp_type,
            sp_slot,
            created_sitrep_id,
            comment,
        } = loc;
        fm::case::ImpactedLocation {
            sp_type: sp_type.into(),
            slot: sp_slot.0.into(),
            created_sitrep_id: created_sitrep_id.into(),
            comment,
        }
    }
}
