// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::rng;
use anyhow::Context;
use fm::analysis_reports;
use iddqd::id_ord_map::{Entry, IdOrdMap, RefMut};
use nexus_types::alert::AlertClass;
use nexus_types::fm;
use nexus_types::support_bundle::BundleDataSelection;
use omicron_uuid_kinds::CaseUuid;
use omicron_uuid_kinds::SitrepUuid;
use std::sync::Arc;

#[derive(Debug)]
pub struct CaseBuilder {
    pub log: slog::Logger,
    case: fm::Case,
    sitrep_id: SitrepUuid,
    rng: rng::CaseBuilderRng,
    report_log: analysis_reports::DebugLog,
}

#[derive(Debug)]
pub struct AllCases {
    log: slog::Logger,
    sitrep_id: SitrepUuid,
    pub(super) cases: IdOrdMap<CaseBuilder>,
    rng: rng::SitrepBuilderRng,
}

impl AllCases {
    pub(super) fn new(
        log: slog::Logger,
        sitrep_id: SitrepUuid,
        inputs: &crate::analysis_input::Input,
        mut rng: rng::SitrepBuilderRng,
    ) -> Self {
        let cases = inputs
            .cases()
            .iter()
            .map(|case| {
                let rng = rng::CaseBuilderRng::new(case.id, &mut rng);
                CaseBuilder::new(&log, sitrep_id, case.clone(), rng)
            })
            .collect();

        Self { log, sitrep_id, cases, rng }
    }

    pub fn open_case(
        &mut self,
        de: fm::DiagnosisEngineKind,
    ) -> RefMut<'_, CaseBuilder> {
        let sitrep_id = self.sitrep_id;
        let (id, case_rng) = loop {
            let (id, case_rng) = self.rng.next_case();
            if !self.cases.contains_key(&id) {
                break (id, case_rng);
            }
        };
        let case = fm::Case {
            id,
            metadata: fm::case::Metadata {
                created_sitrep_id: self.sitrep_id,
                closed_sitrep_id: None,
                de,
                comment: String::new(),
            },
            ereports: Default::default(),
            alerts_requested: Default::default(),
            support_bundles_requested: Default::default(),
        };
        let mut builder =
            CaseBuilder::new(&self.log, sitrep_id, case, case_rng);
        builder.report_log.entry("opened case");
        let case = match self.cases.entry(&id) {
            Entry::Vacant(entry) => entry.insert(builder),
            Entry::Occupied(_) => {
                unreachable!("UUID was just confirmed vacant")
            }
        };

        slog::info!(
            self.log,
            "opened case {id:?}";
            "case_id" => ?id,
            "de" => %de
        );

        case
    }

    pub fn case(&self, id: &CaseUuid) -> Option<&CaseBuilder> {
        self.cases.get(id)
    }

    pub fn case_mut(
        &mut self,
        id: &CaseUuid,
    ) -> Option<RefMut<'_, CaseBuilder>> {
        self.cases.get_mut(id)
    }

    pub fn len(&self) -> usize {
        self.cases.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cases.is_empty()
    }
}

impl CaseBuilder {
    fn new(
        log: &slog::Logger,
        sitrep_id: SitrepUuid,
        case: fm::Case,
        rng: rng::CaseBuilderRng,
    ) -> Self {
        let log = log.new(slog::o!(
            "case_id" => case.id.to_string(),
            "de" => case.metadata.de.to_string(),
            "created_sitrep_id" => case.metadata.created_sitrep_id.to_string(),
        ));
        Self { log, case, sitrep_id, rng, report_log: Default::default() }
    }

    pub fn request_alert(
        &mut self,
        class: AlertClass,
        alert: &impl serde::Serialize,
        comment: impl ToString,
    ) -> anyhow::Result<()> {
        let payload = serde_json::to_value(&alert).with_context(|| {
            format!("failed to serialize payload for {class:?} alert")
        })?;
        let comment = comment.to_string();
        let id = loop {
            let id = self.rng.next_alert();
            let req = fm::case::AlertRequest {
                id,
                class,
                requested_sitrep_id: self.sitrep_id,
                payload: payload.clone(),
                comment: comment.clone(),
            };
            if self.case.alerts_requested.insert_unique(req).is_ok() {
                break id;
            }
        };

        slog::info!(
            &self.log,
            "requested an alert";
            "alert_id" => %id,
            "alert_class" => ?class,
            "comment" => %comment,
        );
        self.report_log
            .entry("requested alert")
            .kv("alert_id", id)
            .kv("alert_class", &class)
            .comment(comment);

        Ok(())
    }

    pub fn request_support_bundle(
        &mut self,
        data_selection: BundleDataSelection,
        comment: impl ToString,
    ) {
        let comment = comment.to_string();
        let id = loop {
            let id = self.rng.next_support_bundle();
            let req = fm::case::SupportBundleRequest {
                id,
                requested_sitrep_id: self.sitrep_id,
                data_selection: data_selection.clone(),
                comment: comment.clone(),
            };
            if self.case.support_bundles_requested.insert_unique(req).is_ok() {
                break id;
            }
        };

        slog::info!(
            &self.log,
            "requested a support bundle";
            "support_bundle_id" => %id,
            "comment" => %comment,
        );
        self.report_log
            .entry("requested support bundle")
            .kv("support_bundle_id", id)
            .comment(comment);
    }

    pub fn close(&mut self, comment: impl ToString) {
        self.case.metadata.closed_sitrep_id = Some(self.sitrep_id);

        let comment = comment.to_string();
        slog::info!(&self.log, "case closed"; "comment" => %comment);
        self.report_log.entry("case closed").comment(comment);
    }

    pub fn add_ereport(
        &mut self,
        report: &Arc<fm::Ereport>,
        comment: impl ToString,
    ) {
        let comment = comment.to_string();
        let assignment_id = self.rng.next_case_ereport();
        match self.case.ereports.insert_unique(fm::case::CaseEreport {
            id: assignment_id,
            ereport: report.clone(),
            assigned_sitrep_id: self.sitrep_id,
            comment: comment.clone(),
        }) {
            Ok(_) => {
                slog::info!(
                    self.log,
                    "assigned ereport {} to case", report.id();
                    "ereport_id" => %report.id(),
                    "ereport_class" => ?report.class,
                    "assignment_id" => %assignment_id,
                    "comment" => %comment,
                );

                self.report_log
                    .entry("assigned ereport to case")
                    .comment(comment)
                    .kv("ereport_id", &format_args!("{}", report.id()))
                    .kv(
                        "ereport_class",
                        &report.class.as_deref().unwrap_or("<none>"),
                    )
                    .kv("assignment_id", assignment_id);
            }
            Err(_) => {
                slog::warn!(
                    self.log,
                    "ereport {} already assigned to case", report.id();
                    "ereport_id" => %report.id(),
                    "ereport_class" => ?report.class,
                );
            }
        }
    }

    /// Returns an iterator over all ereports that were assigned to this case in
    /// the current sitrep.
    pub fn new_ereports(
        &self,
    ) -> impl Iterator<Item = &'_ Arc<fm::Ereport>> + '_ {
        self.ereports.iter().filter_map(|ereport| {
            if ereport.assigned_sitrep_id == self.sitrep_id {
                Some(&ereport.ereport)
            } else {
                None
            }
        })
    }

    /// Mutably borrows the case's `comment` field (i.e. to append to it).
    pub fn comment_mut(&mut self) -> &mut String {
        &mut self.case.metadata.comment
    }

    pub(crate) fn build(self) -> (fm::Case, fm::analysis_reports::CaseReport) {
        let Self { case, report_log, .. } = self;
        let report = fm::analysis_reports::CaseReport {
            id: case.id,
            metadata: case.metadata.clone(),
            log: report_log,
        };
        (case, report)
    }
}

impl core::ops::Deref for CaseBuilder {
    type Target = fm::Case;
    fn deref(&self) -> &Self::Target {
        &self.case
    }
}

impl iddqd::IdOrdItem for CaseBuilder {
    type Key<'a> = &'a CaseUuid;
    fn key(&self) -> Self::Key<'_> {
        &self.case.id
    }

    iddqd::id_upcast!();
}
