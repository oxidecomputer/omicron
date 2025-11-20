// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::alert;
use anyhow::Context;
use chrono::Utc;
use iddqd::id_ord_map::{self, IdOrdMap};
use nexus_types::fm;
use nexus_types::inventory::SpType;
use omicron_uuid_kinds::AlertUuid;
use omicron_uuid_kinds::CaseUuid;
use omicron_uuid_kinds::SitrepUuid;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug)]
pub struct CaseBuilder {
    pub log: slog::Logger,
    pub case: fm::Case,
    pub sitrep_id: SitrepUuid,
}

#[derive(Debug)]
pub struct AllCases {
    log: slog::Logger,
    sitrep_id: SitrepUuid,
    pub cases: IdOrdMap<CaseBuilder>,
}

impl AllCases {
    pub fn new(
        log: slog::Logger,
        sitrep_id: SitrepUuid,
        parent_sitrep: Option<&fm::Sitrep>,
    ) -> (Self, ImpactLists) {
        // Copy forward any open cases from the parent sitrep.
        // If a case was closed in the parent sitrep, skip it.
        let mut cases_by_location: HashMap<_, HashSet<CaseUuid>> =
            HashMap::new();
        let cases: IdOrdMap<_> = parent_sitrep
            .iter()
            .flat_map(|s| s.open_cases())
            .map(|case| {
                for location in &case.impacted_locations {
                    cases_by_location
                        .entry((location.sp_type, location.slot))
                        .or_default()
                        .insert(case.id.clone());
                }
                CaseBuilder::new(&log, sitrep_id, case.clone())
            })
            .collect();

        let cases = Self { log, sitrep_id, cases };
        let impact_lists = ImpactLists { cases_by_sp: cases_by_location };
        (cases, impact_lists)
    }

    pub fn open_case(
        &mut self,
        de: fm::DiagnosisEngineKind,
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
                    impacted_locations: Default::default(),
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

    pub fn case(&self, id: CaseUuid) -> Option<&CaseBuilder> {
        self.cases.get(&id)
    }

    pub fn case_mut(
        &mut self,
        id: CaseUuid,
    ) -> Option<id_ord_map::RefMut<'_, CaseBuilder>> {
        self.cases.get_mut(&id)
    }
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

    pub fn add_ereport(
        &mut self,
        report: &Arc<fm::Ereport>,
        comment: impl std::fmt::Display,
    ) {
        match self.case.ereports.insert_unique(fm::case::CaseEreport {
            ereport: report.clone(),
            assigned_sitrep_id: self.sitrep_id,
            comment: comment.to_string(),
        }) {
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

    pub fn impacts_location(
        &mut self,
        impact_lists: &mut ImpactLists,
        sp_type: SpType,
        slot: u16,
        comment: impl ToString,
    ) -> anyhow::Result<()> {
        if self.impacted_locations.contains_key(&(sp_type, slot)) {
            return Err(anyhow::anyhow!(
                "case already impacts this location ({sp_type} {slot})"
            ));
        }

        impact_lists
            .cases_by_sp
            .entry((sp_type, slot))
            .or_default()
            .insert(self.id);

        let comment = comment.to_string();
        slog::info!(
            &self.log,
            "case impacts location";
            "sp_type" => %sp_type,
            "slot" => %slot,
            "comment" => %comment,
        );
        let created_sitrep_id = self.sitrep_id;
        self.impacted_locations
            .insert_unique(fm::case::ImpactedLocation {
                sp_type,
                slot,
                created_sitrep_id,
                comment: comment.to_string(),
            })
            .expect(
                "we just checked that there wasn't already an entry for this \
                 location",
            );

        Ok(())
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

#[derive(Debug)]
pub struct ImpactLists {
    cases_by_sp: HashMap<(SpType, u16), HashSet<CaseUuid>>,
}

impl ImpactLists {
    pub fn cases_impacting_sp(
        &self,
        sp_type: SpType,
        slot: u16,
    ) -> impl Iterator<Item = CaseUuid> + '_ {
        self.cases_by_sp
            .get(&(sp_type, slot))
            .into_iter()
            .flat_map(|ids| ids.iter().copied())
    }
}
