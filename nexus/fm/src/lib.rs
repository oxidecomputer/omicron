// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault management

use nexus_types::fm;
use nexus_types::inventory;
use omicron_uuid_kinds::CaseUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SitrepUuid;
use slog::Logger;
use std::fmt::Write;

pub mod de;

#[derive(Debug)]
pub struct SitrepBuilder<'a> {
    pub log: Logger,
    pub inventory: &'a inventory::Collection,
    pub parent_sitrep: Option<&'a fm::Sitrep>,
    pub sitrep_id: SitrepUuid,
    pub cases: iddqd::IdOrdMap<fm::Case>,
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
        SitrepBuilder {
            log,
            sitrep_id,
            inventory,
            parent_sitrep,
            comment: String::new(),
            cases: Default::default(),
        }
    }

    pub fn open_case(&mut self, case: fm::Case) -> anyhow::Result<CaseUuid> {
        let case_id = case.id;
        if self.cases.contains_key(&case_id) {
            anyhow::bail!("case with ID {case_id:?} already exists");
        }

        slog::info!(
            self.log,
            "opened case {case_id:?}";
            "case_id" => ?case_id,
            "de" => %case.de
        );

        writeln!(&mut self.comment, "* de {} opened case {case_id:?}", case.de)
            .unwrap();

        self.cases
            .insert_unique(case)
            .expect("we just checked that it doesn't exist");
        Ok(case_id)
    }

    pub fn request_alert(
        &mut self,
        case_id: CaseUuid,
        req: fm::AlertRequest,
    ) -> anyhow::Result<()> {
        let mut case = self.cases
            .get_mut(&case_id)
            .ok_or_else(|| anyhow::anyhow!(
                "cannot create an alert request for non-existent case ID {case_id:?}",
            ))?;
        let alert_id = req.id;
        let alert_class = req.class;

        case.alerts_requested.insert_unique(req).map_err(|_| {
            anyhow::anyhow!("an alert with ID {alert_id:?} already exists")
        })?;

        writeln!(
            &mut self.comment,
            "* de {} requested {alert_class:?} alert {alert_id:?} for case \
             {case_id:?}",
            case.de
        )
        .unwrap();

        slog::info!(
            self.log,
            "requested an alert for case {case_id:?}";
            "case_id" => ?case_id,
            "de" => %case.de,
            "alert_id" => ?alert_id,
            "alert_class" => ?alert_class,
        );

        Ok(())
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
            cases: self.cases,
        }
    }
}
