// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault management cases.

use super::DiagnosisEngine;
use crate::DbTypedUuid;
use crate::ereport;
use nexus_db_schema::schema::{fm_case, fm_ereport_in_case};
use nexus_types::fm;
use omicron_uuid_kinds::{
    CaseKind, EreporterRestartKind, SitrepKind, SitrepUuid,
};

/// Metadata describing a fault management case.
///
/// This corresponds to the fields in the `fm_case` table.
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_case)]
pub struct CaseMetadata {
    /// The ID of this case.
    pub id: DbTypedUuid<CaseKind>,
    /// The ID of the sitrep in which the case has this state.
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    /// The diagnosis engine which owns this case.
    pub de: DiagnosisEngine,

    /// The ID of the sitrep in which this case was created.
    pub created_sitrep_id: DbTypedUuid<SitrepKind>,
    /// If this case is closed, the ID of the sitrep in which it was closed.
    ///
    /// If this field is non-null, then the case has been closed. Closed cases
    /// need not be copied forward into child sitreps that descend from the
    /// sitrep in which the case was closed.
    pub closed_sitrep_id: Option<DbTypedUuid<SitrepKind>>,

    /// An optional, human-readable comment describing this case.
    ///
    /// Sitrep comments are intended for debugging purposes only; i.e., they are
    /// visible to Oxide support via OMDB, but are not presented to the
    /// operator. The contents of comment fields are not stable, and a DE may
    /// emit a different comment string for an analogous determination across
    /// different software versions.
    pub comment: String,
}

/// An association between an ereport and a case.
#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_ereport_in_case)]
pub struct CaseEreport {
    /// The restart ID of the reporter that produced this ereport.
    pub restart_id: DbTypedUuid<EreporterRestartKind>,
    /// The ENA of the ereport within that reporter restart.
    ///
    /// As long as this `CaseEreport` entry exists, the corresponding entry in
    /// the `ereport` table with this restart ID and ENA pair is assumed to also
    /// exist.
    pub ena: ereport::DbEna,
    /// ID of the case.
    ///
    /// This corresponds to a record in `fm_case` with this case ID and sitrep ID.
    pub case_id: DbTypedUuid<CaseKind>,
    /// ID of the current sitrep in which this association exists.
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    /// ID of the first sitrep in which this association was added.
    ///
    /// Since all relevant data for open cases is copied forward into new
    /// sitreps, this field exists primarily for debugging purposes. There is
    /// nothing that the sitrep in which an ereport was first assigned to a case
    /// can tell you which the current sitrep cannot.
    pub assigned_sitrep_id: DbTypedUuid<SitrepKind>,
    /// An optional, human-readable comment added by the diagnosis engine to
    /// explain why it felt that this ereport is related to this case.
    ///
    /// Sitrep comments are intended for debugging purposes only; i.e., they are
    /// visible to Oxide support via OMDB, but are not presented to the
    /// operator. The contents of comment fields are not stable, and a DE may
    /// emit a different comment string for an analogous determination across
    /// different software versions.
    pub comment: String,
}

/// The complete state of a case in a particular sitrep, consisting of the
/// [`CaseMetadata`] record and any other records belonging to the case.
#[derive(Clone, Debug)]
pub struct Case {
    pub metadata: CaseMetadata,
    pub ereports: Vec<CaseEreport>,
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

        Self {
            metadata: CaseMetadata {
                id: case_id,
                sitrep_id,
                de: case.de.into(),
                created_sitrep_id: case.created_sitrep_id.into(),
                closed_sitrep_id: case.closed_sitrep_id.map(Into::into),
                comment: case.comment,
            },
            ereports,
        }
    }
}
