// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Power shelf diagnosis

use crate::SitrepBuilder;
use crate::alert;
use nexus_types::fm::DiagnosisEngine;
use nexus_types::fm::Ereport;
use nexus_types::fm::ereport;
use nexus_types::inventory::SpType;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::sync::Arc;

pub fn diagnose(
    sitrep: &mut SitrepBuilder<'_>,
    new_ereports: &[Arc<Ereport>],
) -> anyhow::Result<()> {
    for ereport in new_ereports {
        // Skip non-power shelf reports
        let ereport::Reporter::Sp { sp_type: SpType::Power, slot } =
            ereport.reporter
        else {
            continue;
        };

        // TODO: check for existing cases tracked for this power shelf and see
        // if the ereport is related to them...

        match ereport.data.class.as_deref() {
            // PSU inserted
            Some("hw.insert.psu") => {
                let psc_psu = extract_psc_psu(&ereport, slot, &sitrep.log);
                let mut case = sitrep.open_case(DiagnosisEngine::PowerShelf)?;
                case.add_ereport(ereport, "PSU inserted ereport");
                case.comment =
                    format!("PSC {slot} PSU {:?} inserted", psc_psu.psu_slot);
                case.request_alert(&alert::power_shelf::PsuInserted::V0 {
                    psc_psu,
                })?;
                // Nothing else to do at this time.
                case.close();
            }
            Some("hw.remove.psu") => {
                let psc_psu = extract_psc_psu(&ereport, slot, &sitrep.log);
                let mut case = sitrep.open_case(DiagnosisEngine::PowerShelf)?;
                case.add_ereport(ereport, "PSU removed ereport");
                case.comment =
                    format!("PSC {slot} PSU {:?} removed", psc_psu.psu_slot);
                case.request_alert(&alert::power_shelf::PsuRemoved::V0 {
                    psc_psu,
                })?;

                // Nothing else to do at this time.
                case.close();
            }
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

fn extract_psc_psu(
    ereport: &Ereport,
    psc_slot: u16,
    log: &slog::Logger,
) -> alert::power_shelf::PscPsu {
    let psc_id = extract_psc_id(ereport, log);
    let psu_id = extract_psu_id(ereport, log);
    let psu_slot = grab_json_value(ereport, "slot", &ereport.report, log);
    alert::power_shelf::PscPsu { psc_id, psc_slot, psu_id, psu_slot }
}

fn extract_psc_id(ereport: &Ereport, log: &slog::Logger) -> alert::VpdIdentity {
    let serial_number = ereport.serial_number.clone();
    let revision =
        grab_json_value(ereport, "baseboard_rev", &ereport.report, log);
    let part_number = ereport.part_number.clone();
    alert::VpdIdentity { serial_number, revision, part_number }
}

fn extract_psu_id(
    ereport: &Ereport,
    log: &slog::Logger,
) -> alert::power_shelf::PsuIdentity {
    // These are the same field names that Hubris uses in the ereport. See:
    // https://github.com/oxidecomputer/hubris/blob/ec18e4f11aaa14600c61f67335c32b250ef38269/drv/psc-seq-server/src/main.rs#L1107-L1117
    #[derive(serde::Deserialize, Default)]
    struct Fruid {
        mfr: Option<String>,
        mpn: Option<String>,
        serial: Option<String>,
        fw_rev: Option<String>,
    }

    let Fruid { mfr, mpn, serial, fw_rev } =
        grab_json_value(ereport, "fruid", &ereport.report, log)
            .unwrap_or_default();

    alert::power_shelf::PsuIdentity {
        serial_number: serial,
        part_number: mpn,
        firmware_revision: fw_rev,
        manufacturer: mfr,
    }
}

fn grab_json_value<T: DeserializeOwned>(
    ereport: &Ereport,
    key: &str,
    obj: &Value,
    log: &slog::Logger,
) -> Option<T> {
    let v = match obj.get("key") {
        Some(v) => v,
        None => {
            slog::warn!(
                log,
                "expected ereport to contain a '{key}' field";
                "ereport_id" => %ereport.id,
                "ereport_class" => ?ereport.class,
            );
            return None;
        }
    };
    match serde_json::from_value(v.clone()) {
        Ok(v) => Some(v),
        Err(e) => {
            slog::warn!(
                log,
                "expected ereport '{key}' field to deserialize as a {}",
                std::any::type_name::<T>();
                "ereport_id" => %ereport.id,
                "ereport_class" => ?ereport.class,
                "error" => %e,
            );
            None
        }
    }
}
