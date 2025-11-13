// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault management

use nexus_types::fm;
use nexus_types::inventory;
use omicron_uuid_kinds::AlertUuid;
use omicron_uuid_kinds::CaseUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SitrepUuid;
use slog::Logger;
// use std::fmt::Write;
use anyhow::Context;
use chrono::Utc;
use std::sync::Arc;

pub mod alert;
pub mod case;
pub mod de;

#[derive(Debug)]
pub struct SitrepBuilder<'a> {
    pub log: Logger,
    pub inventory: &'a inventory::Collection,
    pub parent_sitrep: Option<&'a fm::Sitrep>,
    pub sitrep_id: SitrepUuid,
    pub cases: iddqd::IdOrdMap<CaseBuilder>,
    comment: String,
}

impl<'a> SitrepBuilder<'a> {
    pub fn new(
        log: &Logger,
        inventory: &'a inventory::Collection,
        parent_sitrep: Option<&'a fm::Sitrep>,
    ) -> Self {
        let sitrep_id = SitrepUuid::new_v4();
        let log = log.new(slog::o!(
            "sitrep_id" => format!("{sitrep_id:?}"),
            "parent_sitrep_id" => format!("{:?}", parent_sitrep.as_ref().map(|s| s.id())),
            "inv_collection_id" => format!("{:?}", inventory.id),
        ));

        // Copy forward any open cases from the parent sitrep.
        // If a case was closed in the parent sitrep, skip it.
        let cases: iddqd::IdOrdMap<_> = parent_sitrep
            .iter()
            .flat_map(|s| s.open_cases())
            .map(|case| CaseBuilder::new(&log, sitrep_id, case.clone()))
            .collect();

        slog::info!(
            &log,
            "preparing sitrep {sitrep_id:?}";
            "existing_open_cases" => cases.len(),
        );

        SitrepBuilder {
            log,
            sitrep_id,
            inventory,
            parent_sitrep,
            comment: String::new(),
            cases,
        }
    }

    pub fn open_case(
        &mut self,
        de: fm::DiagnosisEngine,
    ) -> anyhow::Result<iddqd::id_ord_map::RefMut<'_, CaseBuilder>> {
        let id = CaseUuid::new_v4();
        let sitrep_id = self.sitrep_id;
        let case = match self.cases.entry(&id) {
            iddqd::id_ord_map::Entry::Occupied(_) => {
                panic!("generated a colliding UUID!")
            }
            iddqd::id_ord_map::Entry::Vacant(entry) => {
                let case = fm::Case {
                    id,
                    created_sitrep_id: self.sitrep_id,
                    time_created: chrono::Utc::now(),
                    closed_sitrep_id: None,
                    time_closed: None,
                    de,
                    comment: String::new(),
                    ereports: Default::default(),
                    alerts_requested: Default::default(),
                    impacted_sp_slots: Default::default(),
                };
                entry.insert(CaseBuilder::new(&self.log, sitrep_id, case))
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

    pub fn build(self, creator_id: OmicronZoneUuid) -> fm::Sitrep {
        fm::Sitrep {
            metadata: fm::SitrepMetadata {
                id: self.sitrep_id,
                parent_sitrep_id: self.parent_sitrep.map(|s| s.metadata.id),
                inv_collection_id: self.inventory.id,
                creator_id,
                comment: self.comment,
                time_created: chrono::Utc::now(),
            },
            cases: self
                .cases
                .into_iter()
                .map(|builder| fm::Case::from(builder))
                .collect(),
        }
    }
}

#[derive(Debug)]
pub struct CaseBuilder {
    pub log: slog::Logger,
    pub case: fm::Case,
    pub sitrep_id: SitrepUuid,
}

impl CaseBuilder {
    fn new(log: &slog::Logger, sitrep_id: SitrepUuid, case: fm::Case) -> Self {
        let log = log.new(slog::o!(
            "case_id" => format!("{:?}", case.id),
            "de" => case.de.to_string(),
            "created_sitrep_id" => format!("{:?}", case.created_sitrep_id),
        ));
        Self { log, case, sitrep_id }
    }

    pub fn request_alert<A: alert::Alert>(
        &mut self,
        alert: &A,
    ) -> anyhow::Result<()> {
        let id = AlertUuid::new_v4();
        let class = A::CLASS;
        let req = fm::AlertRequest {
            id,
            class,
            requested_sitrep_id: self.sitrep_id,
            payload: serde_json::to_value(&alert).with_context(|| {
                format!(
                    "failed to serialize payload for {class:?} alert {alert:?}"
                )
            })?,
        };
        self.case.alerts_requested.insert_unique(req).map_err(|_| {
            anyhow::anyhow!("an alert with ID {id:?} already exists")
        })?;

        slog::info!(
            &self.log,
            "requested an alert";
            "alert_id" => ?id,
            "alert_class" => ?class,
        );

        Ok(())
    }

    pub fn close(&mut self) {
        self.case.time_closed = Some(Utc::now());
        self.case.closed_sitrep_id = Some(self.sitrep_id);

        slog::info!(&self.log, "case closed");
    }

    pub fn add_ereport(&mut self, report: &Arc<fm::Ereport>) {
        match self.case.ereports.insert_unique(report.clone()) {
            Ok(_) => {
                slog::info!(
                    self.log,
                    "assigned ereport {} to case", report.id();
                    "ereport_id" => ?report.id(),
                    "ereport_class" => ?report.class,
                );
            }
            Err(_) => {
                slog::warn!(
                    self.log,
                    "ereport {} already assigned to case", report.id();
                    "ereport_id" => ?report.id(),
                    "ereport_class" => ?report.class,
                );
            }
        }
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
