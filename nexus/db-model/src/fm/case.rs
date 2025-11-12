// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault management cases.

use super::DiagnosisEngine;
use crate::DbTypedUuid;
use crate::ereport;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::{fm_case, fm_ereport_in_case};
use omicron_uuid_kinds::{CaseKind, EreporterRestartKind, SitrepKind};

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
}
