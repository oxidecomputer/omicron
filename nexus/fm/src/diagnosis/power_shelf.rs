// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Power shelf diagnosis

use super::DiagnosisEngine;
use crate::CaseBuilder;
use crate::SitrepBuilder;
use crate::alert;
use crate::ereport_analysis;
use crate::ereport_analysis::ParsedEreport;
use nexus_types::fm::DiagnosisEngineKind;
use nexus_types::fm::Ereport;
use nexus_types::fm::case::CaseEreport;
use nexus_types::fm::case::ImpactedLocation;
use nexus_types::fm::ereport;
use nexus_types::inventory::SpType;
use omicron_uuid_kinds::CaseUuid;
use serde::de::DeserializeOwned;
use serde_json::Value;
use slog_error_chain::InlineErrorChain;
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

const PSU_REMOVE_CLASS: &str = "hw.remove.psu";
const PSU_INSERT_CLASS: &str = "hw.insert.psu";
const PSU_PWR_GOOD_CLASS: &str = "hw.pwr.pwr_good.good";
const PSU_PWR_BAD_CLASS: &str = "hw.pwr.pwr_good.bad";
const PSU_EREPORT_CLASSES: &[&str] = &[
    PSU_REMOVE_CLASS,
    PSU_INSERT_CLASS,
    PSU_PWR_GOOD_CLASS,
    PSU_PWR_BAD_CLASS,
];

impl PowerShelfDiagnosis {
    pub fn new(log: &slog::Logger) -> Self {
        Self {
            log: log.new(slog::o!("de" => "power_shelf")),
            cases_by_shelf: [HashMap::new(), HashMap::new()],
        }
    }

    fn cases_for_shelf_and_psu(
        &self,
        shelf: u16,
        psu: usize,
    ) -> impl Iterator<Item = (&CaseUuid, &PscCase)> {
        self.cases_by_shelf[shelf as usize]
            .iter()
            .filter(move |(_, case)| case.psus_impacted[psu - 1])
    }
}

impl DiagnosisEngine for PowerShelfDiagnosis {
    fn kind(&self) -> DiagnosisEngineKind {
        DiagnosisEngineKind::PowerShelf
    }

    fn analyze_open_case(
        &mut self,
        _sitrep: &mut SitrepBuilder<'_>,
        case: &mut CaseBuilder,
    ) -> anyhow::Result<()> {
        slog::debug!(
            self.log,
            "analyzing open case from parent sitrep...";
            "case_id" => %case.id
        );

        // ooh, a case we alerady opened! let's figure out what its deal is...
        for &ImpactedLocation { sp_type, slot, ref comment, .. } in
            &case.impacted_locations
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
                    if PSU_EREPORT_CLASSES.contains(&class.as_ref()) =>
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
            if let Some(slot) = ereport_psu_slot(&ereport, &self.log) {
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

            // TODO: can we stuff a nice parsed representation in there?
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
            slog::trace!(
                self.log,
                "skipping ereport that was not reported by a power shelf";
                "ereport_id" => %ereport.id,
                "reporter" => %ereport.reporter,
            );
            return Ok(());
        };
        let shelf = slot;

        let Some(class) = ereport.data.class.as_deref() else {
            slog::warn!(
                &self.log,
                "ignoring PSC ereport with no class";
                "ereport" => %ereport.id,
                "shelf" =>  shelf,
            );
            return Ok(());
        };
        let comment = match class {
            c if c == PSU_REMOVE_CLASS => "was removed",
            c if c == PSU_INSERT_CLASS => "was inserted",
            c if c == PSU_PWR_GOOD_CLASS => "asserted PWR_GOOD",
            c if c == PSU_PWR_BAD_CLASS => "deasserted PWR_GOOD",
            unknown => {
                slog::warn!(
                    &self.log,
                    "ignoring unhandled PSC ereport class";
                    "ereport_class" => %unknown,
                    "ereport" => %ereport.id,
                    "shelf" => shelf,
                );
                return Ok(());
            }
        };

        // PSU-specific ereports: inserted, removed, faulted, or un-faulted
        let Some(psu_slot) = ereport_psu_slot(&ereport, &self.log) else {
            const MSG: &str =
                "ereports for PSU events should include a PSU slot";
            slog::warn!(
                self.log,
                "{MSG}; {} has class {class} but did not include one",
                    ereport.id;
                "ereport_id" => %ereport.id,
                "ereport_class" => ?ereport.class,
                "shelf" => shelf,
            );
            anyhow::bail!(
                "{MSG}; {} has class {class} but did not include one",
                ereport.id
            );
        };

        let mut tracked = false;
        for (case_id, _case) in self.cases_for_shelf_and_psu(shelf, psu_slot) {
            tracked = true;
            let mut case = sitrep.cases.case_mut(case_id).ok_or_else(|| {
                anyhow::anyhow!(
                    "we have tracked case {case_id} but it no longer exists \
                     in the sitrep builder (this is a bug)",
                )
            })?;
            case.add_ereport(ereport, format!("PSU {psu_slot} {comment}"));

            // TODO: can we stuff a nice parsed representation in there?
        }

        // we did not find existing case(s) involving this PSU; open a new one.
        //
        // TODO(eliza): this logic will need to change eventually as we get
        // smarter about analyzing faults effecting multiple PSUs in a shelf.
        if !tracked {
            let mut case =
                sitrep.cases.open_case(DiagnosisEngineKind::PowerShelf)?;
            case.add_ereport(ereport, format!("PSU {psu_slot} {comment}"));

            // TODO: can we stuff a nice parsed representation in there?
            case.comment = format!(
                "opened when power shelf {shelf} PSU {psu_slot} {comment}"
            );
            case.impacts_location(
                &mut sitrep.impact_lists,
                SpType::Power,
                shelf,
                "this is the power shelf where the PSU event occurred",
            )?;
            self.cases_by_shelf[shelf as usize].insert(case.id, {
                let mut case = PscCase { psus_impacted: [false; N_PSUS] };
                case.psus_impacted[psu_slot - 1] = true;
                case
            });
        }

        Ok(())
    }

    fn finish(&mut self, sitrep: &mut SitrepBuilder<'_>) -> anyhow::Result<()> {
        // TODO:
        //
        // - debouncing
        // - determine whether undiagnosed cases can now be diagnosed
        // - determine whether those cases have been resolved (looking at
        //   any newly-added ereports, and inventory data/health endpoint
        //   observations)
        // - determine what Active Problems should be requested, updated,
        //   and closed
        // - determine what alerts should be requested
        let tracked_cases = self.cases_by_shelf.iter().enumerate().flat_map(
            |(shelf, cases)| cases.iter().map(move |(k, v)| (shelf, k, v)),
        );
        for (shelf, case_id, slots) in tracked_cases {
            let mut case = sitrep.cases.case_mut(case_id).ok_or_else(|| {
                anyhow::anyhow!(
                    "we are tracking case {case_id} but it no longer exists \
                     in the sitrep builder (this is a bug)"
                )
            })?;
            slog::debug!(
                &self.log,
                "analyzing tracked case...";
                "case_id" => %case_id,
                "shelf" => shelf,
            );

            // These will be used for diagnosing problems eventually...
            // TODO(eliza): these currently always assume that we expect 6
            // PSU/shelf...
            let mut psus_ok = [true; 6];
            let mut psus_present = [true; 6];

            // TODO(eliza): this iterates over ereports ordered by
            // (restart_id,ENA)...but the restart IDs are not temporally
            // ordered. we should figure that out...
            let mut case_ereports =
                case.ereports.iter().map(|e| e.clone()).collect::<Vec<_>>();
            case_ereports
                .sort_by_key(|e| (e.ereport.time_collected, e.ereport.id));
            for CaseEreport { ereport, assigned_sitrep_id, comment } in
                case_ereports
            {
                let ereport::Reporter::Sp {
                    sp_type: SpType::Power,
                    slot: psc_slot,
                } = ereport.reporter
                else {
                    continue;
                };
                let Some(class) = &ereport.class else {
                    slog::warn!(
                        self.log,
                        "skipping ereport with no class";
                        "case_id" => %case_id,
                        "ereport_id" => %ereport.id(),
                        "ereport_reporter" => %ereport.reporter,
                        "assigned_in_sitrep" => ?assigned_sitrep_id,
                        "comment" => %comment,
                    );
                    continue;
                };

                let parsed_ereport = match ParsedEreport::from_raw(&ereport) {
                    Ok(e) => e,
                    Err(err) => {
                        slog::warn!(
                            self.log,
                            "could not interpret ereport!";
                            "case_id" => %case_id,
                            "ereport_id" => %ereport.id(),
                            "ereport_reporter" => %ereport.reporter,
                            "ereport_class" => %class,
                            "assigned_in_sitrep" => ?assigned_sitrep_id,
                            "comment" => %comment,
                            "error" => %InlineErrorChain::new(&err),
                        );
                        continue;
                    }
                };
                let PscEreport { psu, class } = parsed_ereport.report;
                if psu.slot as usize >= N_PSUS {
                    slog::warn!(
                        self.log,
                        "i only know about 6 PSU slots but blah blah blah; \
                         slot {} doesnt exist", psu.slot;
                        "case_id" => %case_id,
                        "ereport_id" => %ereport.id(),
                        "ereport_reporter" => %ereport.reporter,
                        "assigned_in_sitrep" => ?assigned_sitrep_id,
                        "comment" => %comment,
                    );
                    continue;
                }
                match class {
                    EreportClass::PsuInserted => {
                        psus_present[psu.slot as usize - 1] = true;
                        // assume that if the ereport was assigned in a previous
                        // case, any alerts needed were already requested.
                        //
                        // XXX(eliza): is this actually a good heuristic in the
                        // face of software updates potentially introducing new
                        // alerts or new DE logic for generating them? i dunno.
                        // on the other hand, we wouldn't want a new Nexus
                        // version that introduces alerting for new events to
                        // suddenly pop a bunch of alerts into existence for
                        // something that happened ages ago. but on the other
                        // hand, if a case was closed, we wouldn't even be
                        // analyzing it here...i dunno. figure this out.
                        //
                        // TODO(eliza): debounce; check if we currently think
                        // this PSU is in that slot already before alerting
                        // again...
                        if assigned_sitrep_id == sitrep.sitrep_id {
                            case.request_alert(
                                &alert::power_shelf::PsuInserted::V0 {
                                    psc_psu: alert::power_shelf::PscPsu {
                                        psc_id: parsed_ereport
                                            .baseboard
                                            .map(alert::VpdIdentity::from),
                                        psc_slot,
                                        psu_slot: Some(psu.slot as u16),
                                        psu_id: psu.fruid.into(),
                                    },
                                    time: ereport.time_collected,
                                },
                            )?;
                        }
                    }
                    EreportClass::PsuRemoved => {
                        psus_present[psu.slot as usize - 1] = false;
                        // assume that if the ereport was assigned in a previous
                        // case, any alerts needed were already requested.
                        //
                        // XXX(eliza): is this actually a good heuristic in the
                        // face of software updates potentially introducing new
                        // alerts or new DE logic for generating them? i dunno.
                        // on the other hand, we wouldn't want a new Nexus
                        // version that introduces alerting for new events to
                        // suddenly pop a bunch of alerts into existence for
                        // something that happened ages ago. but on the other
                        // hand, if a case was closed, we wouldn't even be
                        // analyzing it here...i dunno. figure this out.
                        if assigned_sitrep_id == sitrep.sitrep_id {
                            case.request_alert(
                                &alert::power_shelf::PsuRemoved::V0 {
                                    psc_psu: alert::power_shelf::PscPsu {
                                        psc_id: parsed_ereport
                                            .baseboard
                                            .map(alert::VpdIdentity::from),
                                        psc_slot,
                                        psu_slot: Some(psu.slot as u16),
                                        psu_id: psu.fruid.into(),
                                    },
                                    time: ereport.time_collected,
                                },
                            )?;
                        }
                    }
                    _ => {
                        eprintln!("TODO ELIZA {class:?}");
                    }
                }
            }

            slog::info!(&self.log,
                "analyzed all ereports for case {case_id}";
                "case_id" => %case_id,
                "shelf" => shelf,
                "psus_present" => ?psus_present,
                "psus_ok" => ?psus_ok,
            );
            // TODO(eliza): check this against inventory, expected number of
            //              PSUs...
            // TODO(eliza): this is where we would open/update/resolve Active
            //              Problems...
            if psus_present == [true; 6] && psus_ok == [true; 6] {
                case.close();
            }
        }

        Ok(())
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
enum Shelf {
    Power0 = 0,
    Power1 = 1,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
enum ShelfComponent {
    Unknown,
    Psc,
    MultiplePsus,
    SpecificPsu(Psu),
}

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[repr(u8)]
enum Psu {
    Psu0 = 0,
    Psu1 = 1,
    Psu2 = 2,
    Psu3 = 3,
    Psu4 = 4,
    Psu5 = 5,
}

fn ereport_psu_slot(ereport: &Ereport, log: &slog::Logger) -> Option<usize> {
    let slot =
        grab_json_value::<usize>(&ereport, "slot", &ereport.report, log)?;
    if slot >= N_PSUS {
        slog::warn!(
            &log,
            "this is weird: I only know about power shelves with \
             {N_PSUS} PSU SLOTS, but this ereport claims to \
             involve slot {slot}";
            "ereport_id" => %ereport.id,
            "ereport_class" => ?ereport.class,
            "slot" => slot,
        );
        None
    } else {
        Some(slot)
    }
}

fn grab_json_value<T: DeserializeOwned>(
    ereport: &Ereport,
    key: &str,
    obj: &Value,
    log: &slog::Logger,
) -> Option<T> {
    let v = match obj.get(key) {
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

impl ParsedEreport<PscEreport> {
    fn psc_location(&self) -> anyhow::Result<(Shelf, ShelfComponent)> {
        let shelf = match self.ereport.reporter {
            ereport::Reporter::Sp { sp_type: SpType::Power, slot: 0 } => {
                Shelf::Power0
            }
            ereport::Reporter::Sp { sp_type: SpType::Power, slot: 1 } => {
                Shelf::Power1
            }
            ereport::Reporter::Sp { sp_type: SpType::Power, slot } => {
                anyhow::bail!(
                    "I only know about power shelves 0 and 1, but ereport {} \
                     was (allegedly) reported by a power shelf in slot {slot}",
                    self.ereport.id
                )
            }
            other_thing => {
                anyhow::bail!(
                    "weird: ereport {} has a PSC-related ereport class, but \
                     was reported by {other_thing}",
                    self.ereport.id
                )
            }
        };

        Ok((shelf, ShelfComponent::SpecificPsu(self.report.psu.refdes)))
    }
}

#[derive(Debug, Eq, PartialEq, serde::Deserialize)]
struct PscEreport {
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
    refdes: Psu,
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

impl From<PsuFruid> for alert::power_shelf::PsuIdentity {
    fn from(fruid: PsuFruid) -> Self {
        let PsuFruid { mfr, mpn, serial, fw_rev } = fruid;
        Self {
            manufacturer: mfr,
            part_number: mpn,
            serial_number: serial,
            firmware_revision: fw_rev,
        }
    }
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
    use crate::ereport_analysis::test as ereport_test;
    use crate::test_util::FmTest;
    use chrono::{DateTime, Utc};
    use nexus_types::fm::{AlertClass, ereport::Reporter};
    use omicron_uuid_kinds::OmicronZoneUuid;
    use std::time::Duration;

    #[test]
    fn test_pwr_bad_ereport() {
        let FmTest { mut reporters, .. } = FmTest::new("test_pwr_bad_ereport");

        let mut reporter = reporters
            .reporter(Reporter::Sp { sp_type: SpType::Power, slot: 0 });

        let ereport = Arc::new(reporter.parse_ereport(
            DateTime::<Utc>::MIN_UTC,
            ereport_test::PSU_REMOVE_JSON,
        ));
        let ereport =
            match ParsedEreport::<PscEreport>::from_raw(dbg!(&ereport)) {
                Ok(ereport) => ereport,
                Err(e) => {
                    panic!("ereport could not be  {e}")
                }
            };
        dbg!(&ereport);
        assert_eq!(
            dbg!(ereport.psc_location()).unwrap(),
            (Shelf::Power0, ShelfComponent::SpecificPsu(Psu::Psu4))
        )
    }

    #[test]
    fn test_remove_insert_pwr_good() {
        let FmTest { logctx, mut reporters, system_builder, sitrep_rng } =
            FmTest::new("test_remove_insert_pwr_good");

        let mut reporter = reporters
            .reporter(Reporter::Sp { sp_type: SpType::Power, slot: 0 });
        let (example_system, _) = system_builder.nsleds(2).build();
        let mut sitrep = SitrepBuilder::new_with_rng(
            &logctx.log,
            &example_system.collection,
            None,
            sitrep_rng,
        );
        // It's the beginning of time!
        let t0 = DateTime::<Utc>::MIN_UTC;

        let mut de = PowerShelfDiagnosis::new(&logctx.log);
        de.analyze_ereport(
            &mut sitrep,
            &Arc::new(
                reporter.parse_ereport(t0, ereport_test::PSU_REMOVE_JSON),
            ),
        )
        .expect("analyzing ereport 1 should succeed");

        de.analyze_ereport(
            &mut sitrep,
            &Arc::new(reporter.parse_ereport(
                t0 + Duration::from_secs(1),
                ereport_test::PSU_INSERT_JSON,
            )),
        )
        .expect("analyzing ereport 2 should succeed");

        de.analyze_ereport(
            &mut sitrep,
            &Arc::new(reporter.parse_ereport(
                t0 + Duration::from_secs(2),
                ereport_test::PSU_PWR_GOOD_JSON,
            )),
        )
        .expect("analyzing ereport 3 should succeed");

        de.finish(&mut sitrep).expect("finish should return Ok");

        let sitrep = sitrep.build(OmicronZoneUuid::nil());

        eprintln!("--- SITREP ---\n\n{sitrep:#?}");

        let case0 = {
            let mut cases = sitrep.cases.iter();
            let case0 = cases.next().expect("sitrep should have a case");
            assert_eq!(
                cases.next(),
                None,
                "sitrep should have exactly one case"
            );
            case0
        };

        eprintln!("\n--- CASE ---\n\n{case0}");

        let mut insert_alert = None;
        let mut remove_alert = None;
        for alert in &case0.alerts_requested {
            match alert.class {
                AlertClass::PsuInserted if insert_alert.is_none() => {
                    insert_alert = Some(alert);
                }
                AlertClass::PsuInserted => {
                    panic!(
                        "expected only one PSU inserted alert, saw multiple:\n\
                         1: {insert_alert:#?}\n\n2: {alert:#?}"
                    );
                }
                AlertClass::PsuRemoved if remove_alert.is_none() => {
                    remove_alert = Some(alert);
                }
                AlertClass::PsuRemoved => {
                    panic!(
                        "expected only one PSU removed alert, saw multiple:\n\
                        1: {remove_alert:#?}\n\n2: {alert:#?}"
                    );
                }
            }
        }

        assert!(insert_alert.is_some(), "no PSU inserted alert was requested!");
        assert!(remove_alert.is_some(), "no PSU removed alert was requested!");
        assert!(
            !case0.is_open(),
            "case should have been closed since everything is okay"
        );

        logctx.cleanup_successful();
    }
}
