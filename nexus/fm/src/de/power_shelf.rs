// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Power shelf diagnosis

use super::DiagnosisEngine;
use crate::CaseBuilder;
use crate::SitrepBuilder;
use crate::alert;
use crate::ereport_analysis;
use nexus_types::fm::DiagnosisEngineKind;
use nexus_types::fm::Ereport;
use nexus_types::fm::case::CaseEreport;
use nexus_types::fm::case::ImpactedSpSlot;
use nexus_types::fm::ereport;
use nexus_types::inventory::SpType;
use omicron_uuid_kinds::CaseUuid;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PowerShelfDiagnosis {
    log: slog::Logger,
    cases_by_shelf: [HashMap<CaseUuid, PscCase>; 2],
}

#[derive(Default)]
struct PscCase {
    psus_impacted: PsuSet,
}

type PsuSet = [bool; N_PSUS];
const N_PSUS: usize = 6;

const KNOWN_EREPORT_CLASSES: &[&str] = &[
    "hw.remove.psu",
    "hw.insert.psu",
    "hw.pwr.pwr_good.good",
    "hw.pwr.pwr_good.bad",
];

impl PowerShelfDiagnosis {
    pub fn new(log: &slog::Logger) -> Self {
        Self {
            log: log.new(slog::o!("de" => "power_shelf")),
            cases_by_shelf: [HashMap::new(), HashMap::new()],
        }
    }
}

impl DiagnosisEngine for PowerShelfDiagnosis {
    fn kind(&self) -> DiagnosisEngineKind {
        DiagnosisEngineKind::PowerShelf
    }

    fn analyze_open_case(
        &mut self,
        sitrep: &mut SitrepBuilder<'_>,
        case: &mut CaseBuilder,
    ) -> anyhow::Result<()> {
        slog::debug!(
            self.log,
            "analyzing open case from parent sitrep...";
            "case_id" => %case.id
        );

        // ooh, a case we alerady opened! let's figure out what its deal is...
        for &ImpactedSpSlot { sp_type, slot, ref comment, .. } in
            &case.impacted_sp_slots
        {
            // skip non-PSC impacts
            if sp_type != SpType::Power {
                continue;
            }

            if matches!(slot, 0 | 1) {
                slog::debug!(
                    &self.log,
                    "open case impacts power shelf {slot}";
                    "case_id" => %case.id,
                    "power_shelf" => slot,
                    "comment" => %comment,
                );
                // make sure it's tracked.
                self.cases_by_shelf[slot as usize].entry(case.id).or_default();
            } else {
                slog::warn!(
                    &self.log,
                    "this is weird: I only know about power shelves numbered \
                     1 and 0, but found a case that claims to impact power \
                     shelf {slot}";
                    "case_id" => %case.id,
                    "power_shelf" => slot,
                    "comment" => %comment,
                );
            }
        }

        for CaseEreport { ereport, comment, assigned_sitrep_id } in
            &case.ereports
        {
            let class = match &ereport.class {
                // This is one we care about
                Some(ref class)
                    if KNOWN_EREPORT_CLASSES.contains(&class.as_ref()) =>
                {
                    slog::debug!(
                        self.log,
                        "analyzing ereport assigned to open case...";
                        "case_id" => %case.id,
                        "ereport_id" => %ereport.id,
                        "ereport_class" => %class,
                        "comment" => %comment,
                        "assigned_sitrep_id" => %assigned_sitrep_id,
                    );
                    class
                }
                class => {
                    slog::debug!(
                        &self.log,
                        "an ereport with an unknown or missing class was \
                         assigned to this case (presumably by another DE); \
                         skipping it...";
                        "case_id" => %case.id,
                        "ereport_id" => %ereport.id,
                        "ereport_class" => ?class,
                        "comment" => %comment,
                    );
                    continue;
                }
            };

            let ereport::Reporter::Sp { sp_type: SpType::Power, slot: shelf } =
                ereport.reporter
            else {
                slog::debug!(
                    self.log,
                    "skipping ereport that was not reported by a power shelf";
                    "case_id" => %case.id,
                    "ereport_id" => %ereport.id,
                    "ereport_class" => %class,
                    "ereport_id" => %ereport.id,
                    "reporter" => %ereport.reporter,
                );
                continue;
            };

            let tracked_case =
                self.cases_by_shelf[shelf as usize].entry(case.id).or_default();

            // Does the ereport include a PSU slot?
            if let Some(slot) = ereport.report["slot"].as_u64() {
                let slot = slot as usize;
                if slot >= N_PSUS {
                    slog::warn!(
                        &self.log,
                        "this is weird: I only know about power shelves with \
                         {N_PSUS} PSU SLOTS, but this ereport claims to \
                         involve slot {slot}";
                        "case_id" => %case.id,
                        "ereport_id" => %ereport.id,
                        "ereport_class" => %class,
                        "slot" => slot,
                    )
                } else {
                    slog::debug!(
                        &self.log,
                        "found an ereport associated with PSU slot {slot}";
                        "case_id" => %case.id,
                        "ereport_id" => %ereport.id,
                        "ereport_class" => %class,
                        "shelf" => shelf,
                        "slot" => slot,
                    );
                    tracked_case.psus_impacted[slot] = true;
                }
            }
        }

        Ok(())
    }

    fn analyze_ereport(
        &mut self,
        sitrep: &mut SitrepBuilder<'_>,
        ereport: &Arc<Ereport>,
    ) -> anyhow::Result<()> {
        // Skip non-power shelf reports
        let ereport::Reporter::Sp { sp_type: SpType::Power, slot } =
            ereport.reporter
        else {
            slog::debug!(
                self.log,
                "skipping ereport that was not reported by a power shelf";
                "ereport_id" => %ereport.id,
                "reporter" => %ereport.reporter,
            );
            return Ok(());
        };

        match ereport.data.class.as_deref() {
            // PSU inserted
            Some("hw.insert.psu") => {
                // TODO: Check for existing cases tracked for this power shelf
                // and see if the ereport is related to them.

                let psc_psu = extract_psc_psu(&ereport, slot, &sitrep.log);
                let mut case =
                    sitrep.cases.open_case(DiagnosisEngineKind::PowerShelf)?;
                case.add_ereport(ereport, "PSU inserted ereport");
                case.comment =
                    format!("PSC {slot} PSU {:?} inserted", psc_psu.psu_slot);
                case.request_alert(&alert::power_shelf::PsuInserted::V0 {
                    psc_psu,
                })?;
                case.impacts_sp(
                    &mut sitrep.impact_lists,
                    SpType::Power,
                    slot,
                    "this is the PSC on the power shelf where the PSU was inserted",
                )?;
                // Nothing else to do at this time.
                case.close();
            }
            Some("hw.remove.psu") => {
                // TODO: Check for existing cases tracked for this power shelf
                // and see if the ereport is related to them.

                let psc_psu = extract_psc_psu(&ereport, slot, &sitrep.log);
                let mut case =
                    sitrep.cases.open_case(DiagnosisEngineKind::PowerShelf)?;
                case.add_ereport(ereport, "PSU removed ereport");
                case.impacts_sp(
                    &mut sitrep.impact_lists,
                    SpType::Power,
                    slot,
                    "this is the PSC on the power shelf where the PSU was inserted",
                )?;
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

        Ok(())
    }

    fn finish(&mut self, sitrep: &mut SitrepBuilder<'_>) -> anyhow::Result<()> {
        todo!()
    }
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
    let PsuFruid { mfr, mpn, serial, fw_rev } =
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

#[derive(Debug, Eq, PartialEq, serde::Deserialize)]
struct PscEreport {
    #[serde(flatten)]
    metadata: ereport_analysis::HubrisMetadata,

    #[serde(flatten)]
    psu: PsuId,
    #[serde(flatten)]
    class: EreportClass,
}

#[derive(Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(tag = "k")]
enum EreportClass {
    #[serde(rename = "hw.insert.psu")]
    PsuInserted,
    #[serde(rename = "hw.remove.psu")]
    PsuRemoved,
    #[serde(rename = "hw.pwr.pwr_good.bad")]
    PwrBad { pmbus_status: PmbusStatus },
    #[serde(rename = "hw.pwr.pwr_good.good")]
    PwrGood { pmbus_status: PmbusStatus },
}

#[derive(Debug, Eq, PartialEq, serde::Deserialize)]
struct PsuId {
    refdes: String,
    rail: String,
    slot: u8,
    fruid: PsuFruid,
}

// These are the same field names that Hubris uses in the ereport. See:
// https://github.com/oxidecomputer/hubris/blob/ec18e4f11aaa14600c61f67335c32b250ef38269/drv/psc-seq-server/src/main.rs#L1107-L1117
#[derive(serde::Deserialize, Debug, PartialEq, Eq, Default)]
struct PsuFruid {
    mfr: Option<String>,
    mpn: Option<String>,
    serial: Option<String>,
    fw_rev: Option<String>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, serde::Deserialize)]
// TODO(eliza): bitflags types for these?
struct PmbusStatus {
    word: Option<u16>,
    input: Option<u8>,
    iout: Option<u8>,
    vout: Option<u8>,
    temp: Option<u8>,
    cml: Option<u8>,
    mfr: Option<u8>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_pwr_bad_ereport() {
        let json_value: serde_json::Value =
            serde_json::from_str(ereport_analysis::test::PSU_PWR_BAD_JSON)
                .expect("JSON should parse");
        let ereport: PscEreport = serde_json::from_value(dbg!(json_value))
            .expect("JSON value should be interpretable");
        eprintln!("{ereport:?}");
    }
}
