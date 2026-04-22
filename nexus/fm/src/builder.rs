// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sitrep builder

use crate::analysis_input;
use iddqd::IdOrdMap;
use nexus_types::fm;
use nexus_types::inventory;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SitrepUuid;
use slog::Logger;

mod case;
pub use case::{AllCases, CaseBuilder};
pub(crate) mod rng;
pub use rng::SitrepBuilderRng;

#[derive(Debug)]
pub struct SitrepBuilder<'a> {
    pub log: Logger,
    pub inventory: &'a inventory::Collection,
    pub parent_sitrep: Option<&'a fm::Sitrep>,
    pub sitrep_id: SitrepUuid,
    pub cases: case::AllCases,
    closed_cases_copied_forward: &'a IdOrdMap<fm::Case>,
    comment: String,
}

impl<'a> SitrepBuilder<'a> {
    pub fn new(log: &Logger, inputs: &'a analysis_input::Input) -> Self {
        Self::new_with_rng(log, inputs, SitrepBuilderRng::from_entropy())
    }

    pub fn new_with_rng(
        log: &Logger,
        inputs: &'a analysis_input::Input,
        mut rng: SitrepBuilderRng,
    ) -> Self {
        let parent_sitrep = inputs.parent_sitrep();
        let inventory = inputs.inventory();

        // TODO(eliza): should the RNG also be seeded with the parent sitrep
        // UUID and/or the Omicron zone UUID? Hmm.
        let sitrep_id = rng.sitrep_id();
        let log = log.new(slog::o!(
            "sitrep_id" => format!("{sitrep_id:?}"),
            "parent_sitrep_id" => format!("{:?}", parent_sitrep.as_ref().map(|s| s.id())),
            "inv_collection_id" => format!("{:?}", inventory.id),
        ));

        let cases = case::AllCases::new(log.clone(), sitrep_id, inputs, rng);
        let closed_cases_copied_forward = inputs.closed_cases_copied_forward();

        slog::info!(
            &log,
            "building sitrep {sitrep_id:?}";
            "existing_open_cases" => cases.len(),
            "closed_cases_copied_forward" => closed_cases_copied_forward.len(),
        );

        SitrepBuilder {
            log,
            sitrep_id,
            inventory,
            parent_sitrep,
            comment: String::new(),
            closed_cases_copied_forward,
            cases,
        }
    }

    pub fn comment(&self) -> &str {
        &self.comment
    }

    pub fn comment_mut(&mut self) -> &mut String {
        &mut self.comment
    }

    pub fn build(
        self,
        creator_id: OmicronZoneUuid,
        time_created: chrono::DateTime<chrono::Utc>,
    ) -> (fm::Sitrep, fm::analysis_reports::AnalysisReport) {
        let mut ereports_by_id = iddqd::IdOrdMap::new();
        let mut report_cases = IdOrdMap::new();
        let cases = self
            .cases
            .cases
            .into_iter()
            // Note that entries are only pushed to `report_cases` for open
            // cases, as the closed cases which are just being copied forward
            // into the next sitrep have, by construction, not been changed in
            // this builder, since they weren't exposed for modification by the
            // builder API. Thus, we really don't have anything new to say about
            // them that's worth including in the report, as the fact that they
            // were copied forward will be recorded in the input report.
            .map(|case_builder| {
                let (case, report) = case_builder.build();
                report_cases.insert_unique(report).expect(
                    "we are iterating over an IdOrdMap, so the entries \
                     should already be unique",
                );
                case
            })
            .chain(self.closed_cases_copied_forward.iter().cloned())
            .inspect(|case| {
                ereports_by_id
                    .extend(case.ereports.iter().map(|ce| ce.ereport.clone()));
            })
            .collect();
        let report = fm::analysis_reports::AnalysisReport {
            sitrep_id: self.sitrep_id,
            comment: self.comment.clone(),
            cases: report_cases,
        };
        let sitrep = fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: self.sitrep_id,
                parent_sitrep_id: self.parent_sitrep.map(|s| s.metadata.id),
                inv_collection_id: self.inventory.id,
                creator_id,
                comment: self.comment,
                time_created,
                // When creating a new sitrep that is a child of this sitrep,
                // the input inventory collection must either be the same
                // inventory as this sitrep, or have started after this sitrep's
                // inventory collection ended.
                next_inv_min_time_started: self.inventory.time_done,
            },
            cases,
            ereports_by_id,
        };
        (sitrep, report)
    }
}
