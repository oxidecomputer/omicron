// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::rng;
use anyhow::Context;
use iddqd::id_ord_map::{self, IdOrdMap};
use nexus_types::alert::AlertClass;
use nexus_types::fm;
use omicron_uuid_kinds::CaseUuid;
use omicron_uuid_kinds::SitrepUuid;
use std::sync::Arc;

#[derive(Debug)]
pub struct CaseBuilder {
    pub log: slog::Logger,
    pub case: fm::Case,
    pub sitrep_id: SitrepUuid,
    rng: rng::CaseBuilderRng,
}

#[derive(Debug)]
pub struct AllCases {
    log: slog::Logger,
    sitrep_id: SitrepUuid,
    pub cases: IdOrdMap<CaseBuilder>,
    rng: rng::SitrepBuilderRng,
}

impl AllCases {
    pub(super) fn new(
        log: slog::Logger,
        sitrep_id: SitrepUuid,
        parent_sitrep: Option<&fm::Sitrep>,
        mut rng: rng::SitrepBuilderRng,
    ) -> Self {
        // Copy forward any open cases from the parent sitrep.
        // If a case was closed in the parent sitrep, skip it.
        let cases: IdOrdMap<_> = parent_sitrep
            .iter()
            .flat_map(|s| s.open_cases())
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
    ) -> anyhow::Result<iddqd::id_ord_map::RefMut<'_, CaseBuilder>> {
        let (id, case_rng) = self.rng.next_case();
        let sitrep_id = self.sitrep_id;
        let case = match self.cases.entry(&id) {
            iddqd::id_ord_map::Entry::Occupied(_) => {
                panic!("generated a colliding UUID!")
            }
            iddqd::id_ord_map::Entry::Vacant(entry) => {
                let case = fm::Case {
                    id,
                    created_sitrep_id: self.sitrep_id,
                    closed_sitrep_id: None,
                    de,
                    comment: String::new(),
                    ereports: Default::default(),
                    alerts_requested: Default::default(),
                };
                entry.insert(CaseBuilder::new(
                    &self.log, sitrep_id, case, case_rng,
                ))
            }
        };

        slog::info!(
            self.log,
            "opened case {id:?}";
            "case_id" => ?id,
            "de" => %de
        );

        Ok(case)
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
            "de" => case.de.to_string(),
            "created_sitrep_id" => case.created_sitrep_id.to_string(),
        ));
        Self { log, case, sitrep_id, rng }
    }

    pub fn request_alert(
        &mut self,
        class: AlertClass,
        alert: &impl serde::Serialize,
    ) -> anyhow::Result<()> {
        let id = self.rng.next_alert();
        let req = fm::case::AlertRequest {
            id,
            class,
            requested_sitrep_id: self.sitrep_id,
            payload: serde_json::to_value(&alert).with_context(|| {
                format!("failed to serialize payload for {class:?} alert")
            })?,
        };
        self.case.alerts_requested.insert_unique(req).map_err(|_| {
            anyhow::anyhow!("an alert with ID {id:?} already exists")
        })?;

        slog::info!(
            &self.log,
            "requested an alert";
            "alert_id" => %id,
            "alert_class" => ?class,
        );

        Ok(())
    }

    pub fn close(&mut self) {
        self.case.closed_sitrep_id = Some(self.sitrep_id);

        slog::info!(&self.log, "case closed");
    }

    pub fn add_ereport(
        &mut self,
        report: &Arc<fm::Ereport>,
        comment: impl ToString,
    ) {
        let assignment_id = self.rng.next_case_ereport();
        match self.case.ereports.insert_unique(fm::case::CaseEreport {
            id: assignment_id,
            ereport: report.clone(),
            assigned_sitrep_id: self.sitrep_id,
            comment: comment.to_string(),
        }) {
            Ok(_) => {
                slog::info!(
                    self.log,
                    "assigned ereport {} to case", report.id();
                    "ereport_id" => %report.id(),
                    "ereport_class" => ?report.class,
                    "assignment_id" => %assignment_id,
                );
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
}

impl From<CaseBuilder> for fm::Case {
    fn from(CaseBuilder { case, .. }: CaseBuilder) -> Self {
        case
    }
}

impl core::ops::Deref for CaseBuilder {
    type Target = fm::Case;
    fn deref(&self) -> &Self::Target {
        &self.case
    }
}

impl core::ops::DerefMut for CaseBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.case
    }
}

impl iddqd::IdOrdItem for CaseBuilder {
    type Key<'a> = &'a CaseUuid;
    fn key(&self) -> Self::Key<'_> {
        &self.case.id
    }

    iddqd::id_upcast!();
}
