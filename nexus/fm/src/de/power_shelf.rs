// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Power shelf diagnosis

use crate::SitrepBuilder;
use crate::alert
use nexus_types::fm::AlertRequest;
use nexus_types::fm::DiagnosisEngine;
use nexus_types::fm::Ereport;
use nexus_types::fm::ereport;
use nexus_types::inventory::SpType;
use std::sync::Arc;

pub fn diagnose(
    sitrep: &mut SitrepBuilder<'_>,
    new_ereports: &[Arc<Ereport>],
) -> anyhow::Result<()> {
    for ereport in new_ereports {
        // Skip non-power shelf reports
        let ereport::Reporter::Sp { sp_type: SpType::Power, slot, } = ereport.reporter else {
            continue;
        };

        // TODO: check for existing cases tracked for this power shelf and see
        // if the ereport is related to them...

        match ereport.data.class.as_deref() {
            // PSU inserted
            Some("hw.insert.psu") => {
                let mut case = sitrep.open_case(DiagnosisEngine::PowerShelf)?;
                case.add_ereport(ereport);
                case.comment = "PSU inserted".to_string();
                let psu_id = match ereport.get("fruid") {
                    Some(serde_json::Value::Object(fruid)) => {
                        todo!()
                    },
                    None => {
                        todo!()
                    }
                };
                case.request_alert(alert::power_shelf::PsuInserted::V0 {
                    psc_psu: alert::power_shelf::PscPsu {
                        psc_id: alert::VpdIdentity {
                            serial_number: ereport.serial_number.clone(),
                            revision: ereport.report.get("baseboard_rev").map(ToString::to_string),
                            part_number: ereport.part_number.clone(),
                        },
                        psc_slot: slot,
                        psu_id,
                        psu_slot: ereport.report.get("slot").map(|s| todo!()),
                    }
                })
            }
            Some("hw.remove.psu") => {}
            Some(unknown) => {
                slog::warn!(
                    &sitrep.log,
                    "ignoring unhandled PSC ereport class";
                    "ereport_class" => %unknown,
                    "ereport" => %ereport.id,
                );
            }
            None => {
                slog::warn!(
                    &sitrep.log,
                    "ignoring PSC ereport with no class";
                    "ereport" => %ereport.id,
                );
            }
        }
    }

    Ok(())
}
