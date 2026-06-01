// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Human-readable debug reports describing the analysis process that produced a
//! sitrep.

use crate::DbTypedUuid;
use nexus_db_schema::schema::fm_sitrep_analysis_report;
use omicron_uuid_kinds::SitrepKind;

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_sitrep_analysis_report)]
pub struct SitrepAnalysisReport {
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub git_commit: String,
    pub input_report: serde_json::Value,
    pub analysis_report: serde_json::Value,
}
