// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Human-readable debug reports describing the analysis process that produced a
//! sitrep.

use crate::DbTypedUuid;
use anyhow::Context;
use nexus_db_schema::schema::fm_sitrep_analysis_report;
use nexus_types::fm::analysis_reports::{AnalysisReport, InputReport};
use omicron_uuid_kinds::SitrepKind;

#[derive(Queryable, Insertable, Clone, Debug, Selectable)]
#[diesel(table_name = fm_sitrep_analysis_report)]
pub struct SitrepAnalysisReport {
    pub sitrep_id: DbTypedUuid<SitrepKind>,
    pub git_commit: String,
    pub input_report: serde_json::Value,
    pub analysis_report: serde_json::Value,
}

impl SitrepAnalysisReport {
    /// Construct a new analysis report record from the [`InputReport`] and
    /// [`AnalysisReport`] values produced during a sitrep's analysis phase.
    ///
    /// The git commit of the Nexus producing this record is embedded so that
    /// `omdb` can warn when it was built from a different revision than the
    /// Nexus that generated the reports.
    pub fn new(
        input_report: &InputReport,
        analysis_report: &AnalysisReport,
    ) -> anyhow::Result<Self> {
        let sitrep_id = analysis_report.sitrep_id.into();
        let input_report = serde_json::to_value(input_report)
            .context("failed to serialize sitrep input report")?;
        let analysis_report = serde_json::to_value(analysis_report)
            .context("failed to serialize sitrep analysis report")?;

        let git_commit = omicron_git_version::GitVersion::current().to_string();

        Ok(Self { sitrep_id, git_commit, input_report, analysis_report })
    }
}
