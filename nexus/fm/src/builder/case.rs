// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::rng;
use anyhow::Context;
use fm::analysis_reports;
use iddqd::id_ord_map::{self, IdOrdMap};
use nexus_types::alert::AlertPayload;
use nexus_types::fm;
use nexus_types::support_bundle::BundleDataSelection;
use omicron_uuid_kinds::CaseUuid;
use omicron_uuid_kinds::FactUuid;
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
            .open_cases()
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
    ) -> iddqd::id_ord_map::RefMut<'_, CaseBuilder> {
        let (id, case_rng) = loop {
            let (id, case_rng) = self.rng.next_case();
            if !self.cases.contains_key(&id) {
                break (id, case_rng);
            }
        };
        let sitrep_id = self.sitrep_id;
        let case = match self.cases.entry(&id) {
            iddqd::id_ord_map::Entry::Occupied(_) => {
                unreachable!("UUID should be unused")
            }
            iddqd::id_ord_map::Entry::Vacant(entry) => {
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
                    facts: Default::default(),
                };
                let mut builder =
                    CaseBuilder::new(&self.log, sitrep_id, case, case_rng);
                builder.report_log.entry("opened case");
                entry.insert(builder)
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
    ) -> Option<id_ord_map::RefMut<'_, CaseBuilder>> {
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

    pub fn request_alert<A: AlertPayload>(
        &mut self,
        alert: &A,
        comment: impl ToString,
    ) -> anyhow::Result<()> {
        let class = A::CLASS;
        let version = A::VERSION;
        let payload_type = std::any::type_name::<A>();

        let id = loop {
            let id = self.rng.next_alert();
            if !self.case.alerts_requested.contains_key(&id) {
                break id;
            }
        };
        let req = fm::case::AlertRequest {
            id,
            class,
            version,
            requested_sitrep_id: self.sitrep_id,
            payload: serde_json::to_value(alert).with_context(|| {
                format!(
                    "failed to serialize payload for {class} v{version} alert \
                     (Rust type {payload_type})",
                )
            })?,
            comment: comment.to_string(),
        };
        self.case
            .alerts_requested
            .insert_unique(req)
            .expect("UUID should be unused");

        let comment = comment.to_string();
        slog::info!(
            &self.log,
            "requested an alert";
            "alert_id" => %id,
            "alert_class" => ?class,
            "alert_version" => version,
            "alert_payload_type" => %payload_type,
            "comment" => %comment,
        );
        self.report_log
            .entry("requested alert")
            .kv("alert_id", id)
            .kv("alert_class", &class)
            .kv("alert_version", version)
            .kv("alert_payload_type", payload_type)
            .comment(comment);

        Ok(())
    }

    pub fn request_support_bundle(
        &mut self,
        data_selection: BundleDataSelection,
        comment: impl ToString,
    ) {
        let id = loop {
            let id = self.rng.next_support_bundle();
            if !self.case.support_bundles_requested.contains_key(&id) {
                break id;
            }
        };
        let req = fm::case::SupportBundleRequest {
            id,
            requested_sitrep_id: self.sitrep_id,
            data_selection,
            comment: comment.to_string(),
        };
        self.case
            .support_bundles_requested
            .insert_unique(req)
            .expect("UUID should be unused");

        let comment = comment.to_string();
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

    /// Replace this case's free-form comment string.
    pub fn set_comment(&mut self, comment: impl ToString) {
        self.case.metadata.comment = comment.to_string();
    }

    /// Emit a new fact under this case. The fact's UUID is freshly
    /// allocated from the case's deterministic RNG.
    pub fn add_fact(
        &mut self,
        payload: impl Into<fm::FactPayload>,
        comment: impl ToString,
    ) -> FactUuid {
        let id = loop {
            let id = self.rng.next_fact();
            if !self.case.facts.contains_key(&id) {
                break id;
            }
        };
        let payload = payload.into();
        let comment = comment.to_string();
        slog::info!(
            &self.log,
            "added a fact";
            "fact_id" => %id,
            "payload" => ?payload,
            "comment" => %comment,
        );
        self.report_log
            .entry("added fact")
            .kv("fact_id", id)
            .kv("payload", &payload)
            .comment(comment.clone());
        let fact = fm::case::Fact {
            metadata: fm::case::FactMetadata {
                id,
                created_sitrep_id: self.sitrep_id,
                comment,
            },
            payload,
        };
        self.case.facts.insert_unique(fact).expect("UUID should be unused");
        id
    }

    /// Remove a fact from this case. The fact will not be carried forward
    /// into the next sitrep. `comment` records why it was removed.
    pub fn remove_fact(&mut self, id: FactUuid, comment: impl ToString) {
        let comment = comment.to_string();
        if let Some(fact) = self.case.facts.remove(&id) {
            slog::info!(
                &self.log,
                "removed a fact";
                "fact_id" => %id,
                "payload" => ?fact.payload,
                "comment" => %comment,
            );
            self.report_log
                .entry("removed fact")
                .kv("fact_id", id)
                .kv("payload", &fact.payload)
                .comment(comment);
        } else {
            slog::warn!(
                &self.log,
                "tried to remove a fact that does not exist";
                "fact_id" => %id,
                "comment" => %comment,
            );
        }
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
