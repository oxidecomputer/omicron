// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::CaseBuilder;
use crate::SitrepBuilder;
use crate::ereport;
use crate::ereport::Ereport;
use anyhow::Context;
use iddqd::{IdHashItem, IdHashMap, IdOrdItem, IdOrdMap, id_upcast};
use nexus_db_model::EreporterRestart;
use nexus_types::alert::power_shelf as alert_types;
use nexus_types::external_api;
use nexus_types::fm::DiagnosisEngineKind;
use nexus_types::inventory;
use omicron_uuid_kinds::CaseUuid;
use omicron_uuid_kinds::EreporterRestartUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::RackUuid;
use serde::Deserialize;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use strum::VariantArray;

pub const PSU_REMOVE_EREPORT: &str = "hw.remove.psu";
pub const PSU_INSERT_EREPORT: &str = "hw.insert.psu";

const KNOWN_EREPORTS: &[&str] = &[PSU_INSERT_EREPORT, PSU_REMOVE_EREPORT];

pub fn analyze(builder: &mut SitrepBuilder<'_>) -> anyhow::Result<()> {
    let input = builder.input();
    let log = builder.log.new(slog::o!("de" => "power_shelf"));

    // Okay so basically, here's what we do:
    // 1. index existing cases
    // 2. look at ereports and open/close/assign to case
    //
    // There's two kinds of cases, which are:
    // - has an ereport which indicates the rectifier was removed,
    // - has only a rectifier inserted ereport
    let parent_cases = input
        .open_cases()
        .iter()
        .filter(|c| c.metadata.de == DiagnosisEngineKind::PowerShelf);

    let mut cases_by_id = IdOrdMap::new();
    let mut cases_by_psu = BTreeMap::new();
    'cases: for case in parent_cases {
        // Reconstruct the case by looking at its ereports:
        // - the ereports should all be associated with a single PSC at this
        //   point.
        // - put them in a map by PSC location
        let mut psc_case = cases_by_id
            .entry(&case.id)
            .or_insert_with(|| PscCase::new(case.id));
        for case_ereport in case.ereports.iter() {
            let ereport = &case_ereport.ereport;
            // These ereports were assigned to the case in an earlier sitrep,
            // so they are not new in this run and have already had their
            // alerts requested.
            let ereport = match PsuEreport::parse(
                input.ereporter_restarts(),
                &ereport,
                Provenance::Parent,
            ) {
                Ok(ereport) => ereport,
                Err(e) => {
                    // This is weird: a case in the parent sitrep created by
                    // this DE contained an ereport that we couldn't understand.
                    // Close the case since we don't know what to do with it.
                    let err = InlineErrorChain::new(&*e);
                    slog::warn!(
                        &log,
                        "couldn't interpret ereport assigned to a case in the \
                         parent sitrep!";
                        "case_id" => %case.id,
                        "ereport_id" => %ereport.id,
                        "case_ereport_id" => %case_ereport.id,
                        "error" => &err,
                    );
                    let comment = format!(
                        "I couldn't understand this case, as it \
                         contained an incomprehensible ereport {} \
                         (case ereport {}). The ereport could not be \
                         interpreted because: {err}",
                        ereport.id, case_ereport.id,
                    );
                    builder
                        .cases
                        .case_mut(&case.id)
                        .expect("open case in parent sitrep should be present")
                        .close(comment);
                    // `psc_case` is a `RefMut` that mutably borrows this case's
                    // entry in `cases_by_id`; as long as it exists, the whole
                    // map is borrowed mutably. We must therefore drop the
                    // `RefMut` to release the mut borrow of the map so tht the
                    // `cases_by_id.remove(...)` call on the subsequent line can
                    // compile, as it must also mutably borrow the whole map.
                    drop(psc_case);
                    cases_by_id.remove(&case.id);
                    continue 'cases;
                }
            };
            psc_case.insert_ereport(ereport).expect(
                "ereport can't possibly be a duplicate because it came \
                 from the case's existing ereport set",
            );
        }

        // Now, add the case to the index of cases by PSU.
        for location in psc_case.impacted_psus.iter() {
            // N.B. that this allows us to model multiple cases impacting the
            // same PSU. At present we will not do this, but it will become
            // important later.
            cases_by_psu
                .entry(location)
                .or_insert_with(HashSet::new)
                .insert(case.id);
        }
    }

    // For each ereport that we haven't already seen before:
    for ereport in input.new_ereports().iter() {
        let Some(class) = ereport.class.as_deref() else {
            // if there's no class, nothing we can do with this.
            continue;
        };
        if !KNOWN_EREPORTS.contains(&class) {
            // if it's not something we know what to do with, skip it.
            continue;
        }

        // This ereport is new in this run, so it'll have an alert requested
        // for it once it's assigned to a case below.
        let psu_ereport = match PsuEreport::parse(
            input.ereporter_restarts(),
            &ereport,
            Provenance::ThisSitrep,
        ) {
            Ok(psu_ereport) => psu_ereport,
            Err(e) => {
                slog::warn!(
                    &log,
                    "skipping a new ereport that isn't an interpretable PSU \
                     insert/remove event";
                    "ereport_id" => %ereport.id,
                    "ereport_class" => ?ereport.class,
                    "error" => InlineErrorChain::new(&*e),
                );
                continue;
            }
        };
        let location = psu_ereport.location;
        let verbed = match psu_ereport.kind {
            PsuEreportKind::Insert => "inserted",
            PsuEreportKind::Remove => "removed",
        };
        // See if there is an open case for the PSC and PSU slot named in the
        // ereport. `cases_by_psu` models multiple cases per PSU for the future,
        // but at present a PSU is impacted by at most one case, so we assign to
        // whichever case the index already knows about.
        let case_id = cases_by_psu
            .get(&location)
            .and_then(|case_ids| case_ids.iter().copied().next());
        let mut case_builder = match case_id {
            Some(id) => builder.cases.case_mut(&id).expect(
                "an open case from the parent sitrep should be in the builder",
            ),
            None => {
                let mut c =
                    builder.cases.open_case(DiagnosisEngineKind::PowerShelf);
                *c.comment_mut() = format!(
                    "opened because {location} was {verbed}\n\
                     this happened in ereport {}\n",
                    ereport.id
                );
                cases_by_psu
                    .entry(location)
                    .or_insert_with(HashSet::new)
                    .insert(c.id);
                cases_by_id
                    .insert_unique(PscCase::new(c.id))
                    .expect("a freshly-opened case has a unique id");
                c
            }
        };

        // that location sure got verbed!
        case_builder.add_ereport(&ereport, format!("{location} {verbed}"));
        cases_by_id
            .get_mut(&case_builder.id)
            .expect("the case must be present in the by-id index")
            .insert_ereport(psu_ereport)
            .expect("distinct new ereports have distinct IDs");
    }

    // Now that every case's ereport history has been assembled, decide each
    // case's fate and emit the alerts that its new ereports call for.
    for psc_case in cases_by_id.iter() {
        let mut case_builder = builder
            .cases
            .case_mut(&psc_case.case_id)
            .expect("every case in the by-id index is open in the builder");

        let mut latest: Option<PscState> = None;
        for restart in psc_case.restarts.iter() {
            // TODO(eliza): the initial state for this scan should begin with
            // the PSC inventory request for the SP after the restart was
            // detected...
            let state = restart.process_ereports(&mut case_builder);
            case_builder
                .log_event("analyzed PSC restart")
                .kv("restart_id", restart.metadata.id())
                .kv("first_seen_at", restart.metadata.time_first_seen)
                .kv("num_ereports", restart.ereports.len())
                .kv(
                    "psus_absent",
                    state
                        .psus_absent
                        .iter()
                        .map(|psu| psu.to_string())
                        .collect::<Vec<_>>(),
                );
            // Is this the latest restart of this PSC we've seen so far?
            let current_latest =
                latest.as_ref().map(|l| l.psc_restart.time_first_seen);
            if Some(state.psc_restart.time_first_seen) > current_latest {
                latest = Some(state);
            }
        }

        // Decide the case's fate from the latest restart's end state.
        match latest {
            Some(state) if !state.psus_absent.is_empty() => {
                case_builder
                    .log_event(
                        "case remains open, as some PSUs are still absent \
                         in the latest restart",
                    )
                    .kv("latest_restart_id", state.psc_restart.id())
                    .kv(
                        "latest_restart_first_seen",
                        state.psc_restart.time_first_seen,
                    );
            }
            Some(_) => {
                case_builder
                    .close("as of the latest restart, all PSUs are present");
            }
            None => {
                case_builder.close(
                    "case has no ereports, I dunno what the previous \
                     DE was thinking...",
                );
            }
        }
    }

    Ok(())
}

#[derive(
    Copy,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Debug,
    strum::VariantArray,
    strum::FromRepr,
)]
#[repr(u8)]
enum PsuSlot {
    Psu0 = 0,
    Psu1 = 1,
    Psu2 = 2,
    Psu3 = 3,
    Psu4 = 4,
    Psu5 = 5,
}

impl fmt::Display for PsuSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&(*self as u8), f)
    }
}

/// A set of PSUs across any number of power shelves.
#[derive(Default)]
struct PsuSet {
    shelves: HashMap<(RackUuid, u8), ShelfPsuSet>,
}

impl PsuSet {
    fn insert(&mut self, PsuLocation { rack, shelf, slot }: PsuLocation) {
        self.shelves.entry((rack, shelf)).or_default().insert(slot)
    }

    // TODO(eliza): perhaps this ought to return whether there was
    // previously a slot in the set? we are not currently using this but...
    fn remove(&mut self, PsuLocation { rack, shelf, slot }: PsuLocation) {
        let shelf_key = (rack, shelf);
        let emptied = if let Some(shelf) = self.shelves.get_mut(&shelf_key) {
            shelf.remove(slot);
            shelf.is_empty()
        } else {
            false
        };
        if emptied {
            self.shelves
                .remove(&shelf_key)
                .expect("we just removed it, it should be there");
        }
    }

    fn is_empty(&self) -> bool {
        // This is valid as we ensure that removing the last slot from a slot
        // set removes it from the map.
        self.shelves.is_empty()
    }

    fn iter(&self) -> impl Iterator<Item = PsuLocation> + '_ {
        self.shelves.iter().flat_map(|(&(rack, shelf), slots)| {
            slots.iter().map(move |slot| PsuLocation { rack, shelf, slot })
        })
    }
}

impl fmt::Debug for PsuSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

/// A set of PSU slots for an individual power shelf.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Default)]
struct ShelfPsuSet(u8);

impl ShelfPsuSet {
    fn contains(&self, slot: PsuSlot) -> bool {
        (self.0 & ShelfPsuSet::bit(slot)) != 0
    }

    fn insert(&mut self, slot: PsuSlot) {
        self.0 |= ShelfPsuSet::bit(slot);
    }

    fn remove(&mut self, slot: PsuSlot) {
        self.0 &= !ShelfPsuSet::bit(slot);
    }

    fn is_empty(&self) -> bool {
        self.0 == 0
    }

    fn bit(slot: PsuSlot) -> u8 {
        1 << slot as u8
    }

    fn iter(&self) -> impl Iterator<Item = PsuSlot> + '_ {
        PsuSlot::VARIANTS.iter().copied().filter(|slot| self.contains(*slot))
    }
}

impl fmt::Debug for ShelfPsuSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

#[derive(Debug)]
struct PscCase {
    case_id: CaseUuid,
    impacted_psus: PsuSet,
    restarts: IdHashMap<Restart>,
}

impl IdOrdItem for PscCase {
    type Key<'a> = &'a CaseUuid;

    fn key(&self) -> Self::Key<'_> {
        &self.case_id
    }

    id_upcast!();
}

impl PscCase {
    fn new(case_id: CaseUuid) -> Self {
        Self {
            case_id,
            impacted_psus: PsuSet::default(),
            restarts: IdHashMap::default(),
        }
    }

    fn insert_ereport(&mut self, ereport: PsuEreport) -> anyhow::Result<()> {
        let ereport_id = ereport.ereport.id;
        let restart_id = ereport_id.restart_id;
        let location = ereport.location;
        self.restarts
            .entry(&restart_id)
            .or_insert_with(|| Restart {
                metadata: ereport.reporter.clone(),
                ereports: IdOrdMap::default(),
            })
            .ereports
            .insert_unique(ereport)
            .map_err(|e| {
                anyhow::anyhow!(
                    "an ereport with id {ereport_id} already exists: {e}"
                )
            })?;
        self.impacted_psus.insert(location);
        Ok(())
    }
}

struct PscState {
    psc_restart: Arc<EreporterRestart>,
    psus_absent: PsuSet,
}

#[derive(Debug)]
struct Restart {
    metadata: Arc<EreporterRestart>,
    ereports: IdOrdMap<PsuEreport>,
}

impl Restart {
    fn restart_id(&self) -> &EreporterRestartUuid {
        self.metadata.id()
    }

    fn process_ereports(&self, case: &mut CaseBuilder) -> PscState {
        // replay all the ereports in this restart of the PSC, tracking
        // changes in PSU presence and happiness
        // TODO(eliza): also track happiness
        let mut psus_absent = PsuSet::default();
        for ereport in &self.ereports {
            let psu = ereport.location;
            // TODO(eliza): some kind of debouncing if two ereports of the
            // same type are seen close together.
            let alert_result = match ereport.kind {
                PsuEreportKind::Insert => {
                    // hello!
                    psus_absent.remove(psu);
                    // TODO(eliza): once we have a model of what rectifiers
                    // we believed to be present, rather than just which are
                    // known to be *absent*, we should try to reason about
                    // whether the *same* PSU was already believed to be
                    // there when deciding whether to alert?
                    if ereport.provenance == Provenance::ThisSitrep {
                        case.request_alert(
                            &alert_types::PsuInsertedV0 {
                                psu: ereport.alert_psu(),
                                power_shelf: ereport
                                    .alert_power_shelf(&case.log),
                                time: ereport.ereport.time_collected,
                            },
                            format_args!(
                                "requested for ereport {}\nereport class: {}\n\
                                 location: {psu}",
                                ereport.id(),
                                ereport.class(),
                            ),
                        )
                    } else {
                        Ok(())
                    }
                }
                PsuEreportKind::Remove => {
                    // goodbye!
                    psus_absent.insert(psu);
                    if ereport.provenance == Provenance::ThisSitrep {
                        case.request_alert(
                            &alert_types::PsuRemovedV0 {
                                psu: ereport.alert_psu(),
                                power_shelf: ereport
                                    .alert_power_shelf(&case.log),
                                time: ereport.ereport.time_collected,
                            },
                            format_args!(
                                "requested for ereport {}\nereport class: {}\n\
                                 location: {psu}",
                                ereport.id(),
                                ereport.class(),
                            ),
                        )
                    } else {
                        Ok(())
                    }
                }
            };
            if let Err(err) = alert_result {
                slog::error!(
                    &case.log,
                    "failed to request alert for ereport";
                    "ereport" => %ereport.id(),
                    "ereport_class" => %ereport.class(),
                    "ereport_location" => %psu,
                    "error" => &InlineErrorChain::new(&*err)
                );
            }
        }

        PscState { psc_restart: self.metadata.clone(), psus_absent }
    }
}

impl IdHashItem for Restart {
    type Key<'a> = &'a EreporterRestartUuid;

    fn key(&self) -> Self::Key<'_> {
        &self.restart_id()
    }

    id_upcast!();
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Provenance {
    /// The ereport was first observed in this sitrep.
    ThisSitrep,
    /// The ereport was assigned to a case in the parent sitrep.
    Parent,
}

#[derive(Debug)]
struct PsuEreport {
    location: PsuLocation,
    ereport: Arc<Ereport>,
    reporter: Arc<EreporterRestart>,
    kind: PsuEreportKind,
    data: PsuEreportData,
    provenance: Provenance,
}

impl IdOrdItem for PsuEreport {
    type Key<'a> = &'a ereport::Ena;

    fn key(&self) -> Self::Key<'_> {
        &self.ereport.id.ena
    }

    id_upcast!();
}

impl PsuEreport {
    fn id(&self) -> &ereport::EreportId {
        &self.ereport.id
    }

    fn class(&self) -> &str {
        self.ereport
            .class
            .as_deref()
            .expect("if an ereport was parsed, it must have a class")
    }

    fn parse(
        restarts: &IdOrdMap<Arc<EreporterRestart>>,
        ereport: &Arc<ereport::Ereport>,
        provenance: Provenance,
    ) -> anyhow::Result<Self> {
        let kind = match ereport.data.class.as_deref() {
            Some(k) if k == PSU_INSERT_EREPORT => PsuEreportKind::Insert,
            Some(k) if k == PSU_REMOVE_EREPORT => PsuEreportKind::Remove,
            k => anyhow::bail!("unknown ereport class: {k:?}"),
        };
        let restart_id = &ereport.id.restart_id;
        let reporter = restarts.get(restart_id).ok_or_else(|| {
            anyhow::anyhow!("no restart ID entry for {restart_id}")
        })?;
        let shelf = match ereport.reporter {
            ereport::Reporter::Sp {
                sp_type: inventory::SpType::Power,
                slot,
            } => u8::try_from(slot).with_context(|| {
                format!("power shelf slot number {slot} is way too big")
            })?,
            reporter => anyhow::bail!(
                "invalid reporter type for what seems to be a PSC ereport: \
                 {reporter:?}"
            ),
        };
        let data: PsuEreportData =
            serde_json::from_value(ereport.data.report.clone())
                .context("invalid data for a PSC ereport")?;
        let slot = PsuSlot::from_repr(data.slot).ok_or_else(|| {
            anyhow::anyhow!("PSU slot {} out of range (must be 0-5)", data.slot)
        })?;
        let location = PsuLocation { rack: *reporter.rack_id(), shelf, slot };
        Ok(Self {
            kind,
            data,
            location,
            ereport: ereport.clone(),
            provenance,
            reporter: reporter.clone(),
        })
    }

    fn psc_baseboard(
        &self,
    ) -> anyhow::Result<external_api::hardware::Baseboard> {
        Ok(external_api::hardware::Baseboard {
            serial: self.ereport.serial_number.clone().ok_or_else(|| {
                anyhow::anyhow!("ereport has no serial number")
            })?,
            part: self
                .ereport
                .part_number
                .clone()
                .ok_or_else(|| anyhow::anyhow!("ereport has no part number"))?,
            // TODO(eliza): if this is missing we should be able to get it from
            // inventory?
            revision: self.data.baseboard_rev.ok_or_else(|| {
                anyhow::anyhow!("ereport has no baseboard revision")
            })?,
        })
    }

    fn alert_psu(&self) -> alert_types::Psu {
        alert_types::Psu {
            identity: self
                .data
                .fruid
                .as_ref()
                .cloned()
                .map(alert_types::PsuIdentity::from),
            slot: self.location.slot as u8,
        }
    }

    fn alert_power_shelf(&self, log: &slog::Logger) -> alert_types::PowerShelf {
        let baseboard = match self.psc_baseboard() {
            Ok(bb) => Some(bb),
            Err(err) => {
                let err = InlineErrorChain::new(&*err);
                slog::warn!(
                    &log,
                    "couldn't determine ereport PSC baseboard identity \
                    for alert payload";
                    "ereport" => %self.id(),
                    "ereport_class" => ?self.ereport.class,
                    &err,
                );
                None
            }
        };
        alert_types::PowerShelf {
            rack_id: self.location.rack.into_untyped_uuid(),
            shelf: self.location.shelf,
            baseboard,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum PsuEreportKind {
    Insert,
    Remove,
}

#[derive(Debug, Eq, PartialEq, serde::Deserialize)]
struct PsuEreportData {
    fruid: Option<PsuFruid>,
    rail: String,
    slot: u8,
    refdes: String,
    baseboard_rev: Option<u32>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
struct PsuLocation {
    rack: RackUuid,
    shelf: u8,
    slot: PsuSlot,
}

impl fmt::Display for PsuLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { rack, shelf, slot } = *self;
        write!(f, "rack {rack}, shelf {shelf}, PSU {slot}")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct PsuFruid {
    fw_rev: String,
    mfr: String,
    mpn: String,
    serial: String,
}

impl From<PsuFruid> for alert_types::PsuIdentity {
    fn from(fruid: PsuFruid) -> Self {
        let PsuFruid { mfr, mpn, fw_rev, serial } = fruid;
        Self { manufacturer: mfr, part: mpn, firmware_revision: fw_rev, serial }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis_input::Input;
    use crate::builder::SitrepBuilderRng;
    use crate::test_util::FmTest;
    use chrono::{Duration, Utc};
    use nexus_types::alert::{AlertClass, AlertPayload};
    use nexus_types::fm::case::{AlertRequest, CaseEreport};
    use nexus_types::fm::{self, Sitrep, SitrepVersion};
    use nexus_types::inventory::SpType;
    use omicron_common::api::external::Generation;
    use omicron_uuid_kinds::{
        AlertUuid, CaseEreportUuid, CaseUuid, CollectionUuid, OmicronZoneUuid,
        SitrepUuid,
    };

    // These are real life ereports I copied from the dogfood rack.
    mod ereports {

        pub(super) const PSU_REMOVE_JSON: &str = r#"{
            "baseboard_part_number": "913-0000003",
            "baseboard_rev": 8,
            "baseboard_serial_number": "BRM45220004",
            "ereport_message_version": 0,
            "fruid": {
                "fw_rev": "0701",
                "mfr": "Murata-PS",
                "mpn": "MWOCP68-3600-D-RM",
                "serial": "LL2216RB003Z"
            },
            "hubris_archive_id": "qSm4IUtvQe0",
            "hubris_task_gen": 0,
            "hubris_task_name": "sequencer",
            "hubris_uptime_ms": 1197337481,
            "k": "hw.remove.psu",
            "rail": "V54_PSU4",
            "refdes": "PSU4",
            "slot": 4,
            "v": 0
        }"#;

        pub(super) const PSU_INSERT_JSON: &str = r#"{
            "baseboard_part_number": "913-0000003",
            "baseboard_rev": 8,
            "baseboard_serial_number": "BRM45220004",
            "ereport_message_version": 0,
            "fruid": {
                "fw_rev": "0701",
                "mfr": "Murata-PS",
                "mpn": "MWOCP68-3600-D-RM",
                "serial": "LL2216RB003Z"
            },
            "hubris_archive_id": "qSm4IUtvQe0",
            "hubris_task_gen": 0,
            "hubris_task_name": "sequencer",
            "hubris_uptime_ms": 1197337481,
            "k": "hw.insert.psu",
            "rail": "V54_PSU4",
            "refdes": "PSU4",
            "slot": 4,
            "v": 0
        }"#;

        // Like `PSU_REMOVE_JSON`, but for a different rectifier (PSU2 on rail
        // V54_PSU2) in the same shelf, so that tests can confirm distinct PSU
        // slots are tracked as distinct cases.
        pub(super) const PSU_REMOVE_SLOT2_JSON: &str = r#"{
            "baseboard_part_number": "913-0000003",
            "baseboard_rev": 8,
            "baseboard_serial_number": "BRM45220004",
            "ereport_message_version": 0,
            "fruid": {
                "fw_rev": "0701",
                "mfr": "Murata-PS",
                "mpn": "MWOCP68-3600-D-RM",
                "serial": "LL2216RB0042"
            },
            "hubris_archive_id": "qSm4IUtvQe0",
            "hubris_task_gen": 0,
            "hubris_task_name": "sequencer",
            "hubris_uptime_ms": 1197337481,
            "k": "hw.remove.psu",
            "rail": "V54_PSU2",
            "refdes": "PSU2",
            "slot": 2,
            "v": 0
        }"#;

        pub(super) const PSU_PWR_BAD_JSON: &str = r#"{
            "baseboard_part_number": "913-0000003",
            "baseboard_rev": 8,
            "baseboard_serial_number": "BRM45220004",
            "ereport_message_version": 0,
            "fruid": {
                "fw_rev": "0701",
                "mfr": "Murata-PS",
                "mpn": "MWOCP68-3600-D-RM",
                "serial": "LL2216RB003Z"
            },
            "hubris_archive_id": "qSm4IUtvQe0",
            "hubris_task_gen": 0,
            "hubris_task_name": "sequencer",
            "hubris_uptime_ms": 1197408566,
            "k": "hw.pwr.pwr_good.bad",
            "pmbus_status": {
                "cml": 0,
                "input": 48,
                "iout": 0,
                "mfr": 0,
                "temp": 0,
                "vout": 0,
                "word": 10312
            },
            "rail": "V54_PSU4",
            "refdes": "PSU4",
            "slot": 4,
            "v": 0
        }"#;

        pub(super) const PSU_PWR_GOOD_JSON: &str = r#"{
            "baseboard_part_number": "913-0000003",
            "baseboard_rev": 8,
            "baseboard_serial_number": "BRM45220004",
            "ereport_message_version": 0,
            "fruid": {
                "fw_rev": "0701",
                "mfr": "Murata-PS",
                "mpn": "MWOCP68-3600-D-RM",
                "serial": "LL2216RB003Z"
            },
            "hubris_archive_id": "qSm4IUtvQe0",
            "hubris_task_gen": 0,
            "hubris_task_name": "sequencer",
            "hubris_uptime_ms": 1197408580,
            "k": "hw.pwr.pwr_good.good",
            "pmbus_status": {
                "cml": 0,
                "input": 0,
                "iout": 0,
                "mfr": 0,
                "temp": 0,
                "vout": 0,
                "word": 0
            },
            "rail": "V54_PSU4",
            "refdes": "PSU4",
            "slot": 4,
            "v": 0
        }"#;
    }

    fn test_dogfood_ereport_parses(
        test_name: &str,
        expected_class: PsuEreportKind,
        json: &str,
    ) {
        const SHELF: u16 = 0;
        let (mut fmtest, logctx) = FmTest::new_with_logctx(test_name);
        let mut reporter = fmtest.reporters.reporter(
            ereport::Reporter::Sp { sp_type: SpType::Power, slot: SHELF },
            Utc::now(),
        );
        let ereport = dbg!(Arc::new(reporter.parse_ereport(Utc::now(), json)));
        let parsed = dbg!(PsuEreport::parse(
            fmtest.reporters.ereporter_restarts(),
            &ereport,
            Provenance::ThisSitrep
        ))
        .expect("dogfood ereport should parse as a PsuEreport");

        // The payload fields shared by every dogfood ereport above; all of them
        // describe PSU4 on rail V54_PSU4.
        let expected_data = PsuEreportData {
            baseboard_rev: Some(8),
            fruid: Some(PsuFruid {
                fw_rev: "0701".to_string(),
                mfr: "Murata-PS".to_string(),
                mpn: "MWOCP68-3600-D-RM".to_string(),
                serial: "LL2216RB003Z".to_string(),
            }),
            rail: "V54_PSU4".to_string(),
            slot: 4,
            refdes: "PSU4".to_string(),
        };

        assert_eq!(parsed.kind, expected_class);
        assert_eq!(
            parsed.location,
            PsuLocation {
                rack: fmtest.rack_id,
                shelf: SHELF as u8,
                slot: PsuSlot::Psu4
            }
        );
        assert_eq!(parsed.data, expected_data);
        logctx.cleanup_successful();
    }

    #[test]
    fn test_psu_remove_json_parses() {
        test_dogfood_ereport_parses(
            "test_psu_remove_json_parses",
            PsuEreportKind::Remove,
            ereports::PSU_REMOVE_JSON,
        );
    }

    #[test]
    fn test_psu_insert_json_parses() {
        test_dogfood_ereport_parses(
            "test_psu_insert_json_parses",
            PsuEreportKind::Insert,
            ereports::PSU_INSERT_JSON,
        );
    }

    /// The shelf slot (i.e. `Reporter::Sp { sp_type: Power, slot }`) used by
    /// every dogfood fixture above.
    const SHELF: u16 = 0;

    /// Constructs an analysis `Input` from an inventory collection, an
    /// optional parent sitrep, and a set of brand-new ereports.
    fn build_input(
        fmtest: &FmTest,
        collection: inventory::Collection,
        parent_sitrep: Option<Sitrep>,
        new_ereports: impl IntoIterator<Item = Ereport>,
    ) -> Input {
        let parent = parent_sitrep.map(|s| {
            Arc::new((
                SitrepVersion {
                    id: s.id(),
                    version: 0,
                    time_made_current: Utc::now(),
                },
                s,
            ))
        });
        let mut builder = fmtest
            .input_builder(
                parent,
                Arc::new(collection),
                Arc::new(IdOrdMap::new()),
            )
            .expect("input builder should accept fresh inventory");
        builder.add_unmarked_ereports(new_ereports);
        let (input, report) = builder.build();
        eprintln!("\n--- inputs ---\n{}", report.display_multiline(0));
        input
    }

    /// Runs the power shelf diagnosis engine over `input` and returns the
    /// resulting sitrep.
    fn run_analyze(log: &slog::Logger, input: &Input) -> Sitrep {
        let mut builder = SitrepBuilder::new_with_rng(
            log,
            input,
            SitrepBuilderRng::from_seed("power-shelf-analyze"),
        );
        analyze(&mut builder).expect("power shelf analysis should succeed");
        let (sitrep, report) =
            builder.build(OmicronZoneUuid::new_v4(), Utc::now());
        eprintln!("\n--- analysis report ---\n{}", report.display_multiline(0));
        eprintln!("\n--- sitrep! ---\n");
        let mut seen_any_cases = false;
        for case in sitrep.open_cases() {
            seen_any_cases = true;
            eprintln!("{}", case.display_indented(0, Some(sitrep.id())));
        }
        if !seen_any_cases {
            eprintln!("(that was the whole sitrep, btw)")
        }
        sitrep
    }

    /// Wraps an ereport in a `CaseEreport` assigned in `sitrep_id`, for
    /// seeding a parent sitrep's case.
    fn case_ereport(
        ereport: &Arc<Ereport>,
        sitrep_id: SitrepUuid,
    ) -> CaseEreport {
        CaseEreport {
            id: CaseEreportUuid::new_v4(),
            ereport: ereport.clone(),
            assigned_sitrep_id: sitrep_id,
            comment: "seeded by test".to_string(),
        }
    }

    /// Builds a parent sitrep with a single open power shelf case containing
    /// `ereports` (and any `alerts` already requested for them).
    fn parent_with_open_case(
        inv_collection_id: CollectionUuid,
        ereports: impl IntoIterator<Item = Arc<Ereport>>,
        alerts: impl IntoIterator<Item = AlertRequest>,
    ) -> Sitrep {
        let parent_sitrep_id = SitrepUuid::new_v4();
        let ereports: Vec<_> = ereports.into_iter().collect();
        let case = fm::Case {
            id: CaseUuid::new_v4(),
            metadata: fm::case::Metadata {
                created_sitrep_id: parent_sitrep_id,
                closed_sitrep_id: None,
                de: DiagnosisEngineKind::PowerShelf,
                comment: "seeded open case".to_string(),
            },
            ereports: ereports
                .iter()
                .map(|e| case_ereport(e, parent_sitrep_id))
                .collect(),
            alerts_requested: alerts.into_iter().collect(),
            support_bundles_requested: Default::default(),
            facts: Default::default(),
        };
        let mut cases = IdOrdMap::new();
        cases.insert_unique(case).unwrap();
        Sitrep {
            metadata: fm::SitrepMetadata {
                id: parent_sitrep_id,
                inv_collection_id,
                creator_id: OmicronZoneUuid::new_v4(),
                parent_sitrep_id: None,
                time_created: Utc::now(),
                next_inv_min_time_started: Utc::now(),
                comment: "parent sitrep for test".to_string(),
                alert_generation: Generation::new(),
                support_bundle_generation: Generation::new(),
            },
            cases,
            ereports_by_id: ereports.into_iter().collect(),
        }
    }

    /// Collects the single case from a sitrep, panicking unless exactly one is
    /// present.
    fn sole_case(sitrep: &Sitrep) -> &fm::Case {
        assert_eq!(
            sitrep.cases.len(),
            1,
            "expected exactly one case in the sitrep"
        );
        sitrep.cases.iter().next().unwrap()
    }

    /// A single rectifier insert opens a case, immediately closes it (the PSU
    /// is present again), and requests one `PsuInserted` alert carrying the
    /// inserted PSU's identity.
    #[test]
    fn single_insert_closes_case_with_inserted_alert() {
        let (mut fmtest, logctx) = FmTest::new_with_logctx(
            "single_insert_closes_case_with_inserted_alert",
        );
        let (example, _bp) = fmtest.system_builder.build();
        let mut reporter = fmtest.reporters.reporter(
            ereport::Reporter::Sp { sp_type: SpType::Power, slot: SHELF },
            Utc::now(),
        );
        let insert =
            reporter.parse_ereport(Utc::now(), ereports::PSU_INSERT_JSON);

        let input = build_input(&fmtest, example.collection, None, [insert]);
        let sitrep = run_analyze(&logctx.log, &input);

        let case = sole_case(&sitrep);
        assert!(
            !case.is_open(),
            "an inserted PSU is present, so its case should be closed"
        );
        assert_eq!(
            case.alerts_requested.len(),
            1,
            "a single insert should request exactly one alert"
        );
        let alert = case.alerts_requested.iter().next().unwrap();
        assert_eq!(alert.class, AlertClass::PsuInserted);
        let alert_payload = dbg!(serde_json::from_value::<
            alert_types::PsuInsertedV0,
        >(alert.payload.clone()))
        .unwrap();
        assert_eq!(alert_payload.power_shelf.shelf, 0);
        assert_eq!(alert_payload.psu.slot, 4);
        assert_eq!(
            alert_payload.psu.identity.as_ref().map(|id| id.serial.as_str()),
            Some("LL2216RB003Z")
        );

        logctx.cleanup_successful();
    }

    /// A single rectifier remove opens a case and leaves it open (the PSU is
    /// still missing), requesting one `PsuRemoved` alert.
    #[test]
    fn single_remove_opens_case_with_removed_alert() {
        let (mut fmtest, logctx) = FmTest::new_with_logctx(
            "single_remove_opens_case_with_removed_alert",
        );
        let (example, _bp) = fmtest.system_builder.build();
        let mut reporter = fmtest.reporters.reporter(
            ereport::Reporter::Sp { sp_type: SpType::Power, slot: SHELF },
            Utc::now(),
        );
        let remove =
            reporter.parse_ereport(Utc::now(), ereports::PSU_REMOVE_JSON);

        let input = build_input(&fmtest, example.collection, None, [remove]);
        let sitrep = run_analyze(&logctx.log, &input);

        let case = sole_case(&sitrep);
        assert!(
            case.is_open(),
            "a removed PSU is still missing, so its case should stay open"
        );
        assert_eq!(case.alerts_requested.len(), 1);
        let alert = case.alerts_requested.iter().next().unwrap();
        assert_eq!(alert.class, AlertClass::PsuRemoved);
        let alert_payload = dbg!(serde_json::from_value::<
            alert_types::PsuRemovedV0,
        >(alert.payload.clone()))
        .unwrap();
        assert_eq!(alert_payload.power_shelf.shelf, 0);
        assert_eq!(alert_payload.psu.slot, 4);

        logctx.cleanup_successful();
    }

    /// A remove followed by an insert within a single analysis run collapses
    /// into one case: it ends up closed (the PSU is present after the insert),
    /// but still requests one alert for each event.
    #[test]
    fn remove_then_insert_in_one_sitrep_closes_with_both_alerts() {
        let (mut fmtest, logctx) = FmTest::new_with_logctx(
            "remove_then_insert_in_one_sitrep_closes_with_both_alerts",
        );
        let (example, _bp) = fmtest.system_builder.build();
        let mut reporter = fmtest.reporters.reporter(
            ereport::Reporter::Sp { sp_type: SpType::Power, slot: SHELF },
            Utc::now(),
        );
        // Same reporter restart, so the insert gets the greater ENA and is
        // recognized as the more recent event.
        let remove =
            reporter.parse_ereport(Utc::now(), ereports::PSU_REMOVE_JSON);
        let insert =
            reporter.parse_ereport(Utc::now(), ereports::PSU_INSERT_JSON);

        let input =
            build_input(&fmtest, example.collection, None, [remove, insert]);
        let sitrep = run_analyze(&logctx.log, &input);

        let case = sole_case(&sitrep);
        assert!(
            !case.is_open(),
            "the most recent event was an insert, so the case should close"
        );
        assert_eq!(
            case.alerts_requested.len(),
            2,
            "both the remove and the insert should each request an alert"
        );
        let has_class =
            |class| case.alerts_requested.iter().any(|a| a.class == class);
        assert!(has_class(AlertClass::PsuRemoved));
        assert!(has_class(AlertClass::PsuInserted));

        logctx.cleanup_successful();
    }

    /// Removes of two different rectifiers in the same shelf open two distinct
    /// cases, since each case concerns a single PSU location.
    #[test]
    fn distinct_slots_get_distinct_cases() {
        let (mut fmtest, logctx) =
            FmTest::new_with_logctx("distinct_slots_get_distinct_cases");
        let (example, _bp) = fmtest.system_builder.build();
        let mut reporter = fmtest.reporters.reporter(
            ereport::Reporter::Sp { sp_type: SpType::Power, slot: SHELF },
            Utc::now(),
        );
        let remove_psu4 =
            reporter.parse_ereport(Utc::now(), ereports::PSU_REMOVE_JSON);
        let remove_psu2 =
            reporter.parse_ereport(Utc::now(), ereports::PSU_REMOVE_SLOT2_JSON);

        let input = build_input(
            &fmtest,
            example.collection,
            None,
            [remove_psu4, remove_psu2],
        );
        let sitrep = run_analyze(&logctx.log, &input);

        assert_eq!(
            sitrep.cases.len(),
            2,
            "each rectifier slot should get its own case"
        );
        for case in sitrep.cases.iter() {
            assert!(case.is_open(), "both removed PSUs should stay open");
            assert_eq!(case.alerts_requested.len(), 1);
        }

        logctx.cleanup_successful();
    }

    /// Ereports whose classes the engine doesn't recognize (here, a power-good
    /// transition) are ignored: no case is opened.
    #[test]
    fn unknown_ereports_are_ignored() {
        let (mut fmtest, logctx) =
            FmTest::new_with_logctx("unknown_ereports_are_ignored");
        let (example, _bp) = fmtest.system_builder.build();
        let mut reporter = fmtest.reporters.reporter(
            ereport::Reporter::Sp { sp_type: SpType::Power, slot: SHELF },
            Utc::now(),
        );
        let pwr_bad =
            reporter.parse_ereport(Utc::now(), ereports::PSU_PWR_BAD_JSON);
        let pwr_good =
            reporter.parse_ereport(Utc::now(), ereports::PSU_PWR_GOOD_JSON);

        let input =
            build_input(&fmtest, example.collection, None, [pwr_bad, pwr_good]);
        let sitrep = run_analyze(&logctx.log, &input);

        assert!(
            sitrep.cases.is_empty(),
            "power-good transitions are not insert/remove events"
        );

        logctx.cleanup_successful();
    }

    /// A case carried forward from the parent sitrep for a removed PSU (with a
    /// `PsuRemoved` alert already requested) closes when a fresh insert
    /// arrives, and requests exactly one new alert (the insert) without
    /// re-requesting the carried-forward remove.
    #[test]
    fn carried_forward_remove_then_insert_closes_without_duplicate_alert() {
        let (mut fmtest, logctx) = FmTest::new_with_logctx(
            "carried_forward_remove_then_insert_closes_without_duplicate_alert",
        );
        let (example, _bp) = fmtest.system_builder.build();
        let inv_collection_id = example.collection.id;
        let mut reporter = fmtest.reporters.reporter(
            ereport::Reporter::Sp { sp_type: SpType::Power, slot: SHELF },
            Utc::now(),
        );

        // The parent sitrep already saw the remove and requested its alert.
        let remove = Arc::new(
            reporter.parse_ereport(Utc::now(), ereports::PSU_REMOVE_JSON),
        );
        let remove_alert = AlertRequest {
            id: AlertUuid::new_v4(),
            class: AlertClass::PsuRemoved,
            version: alert_types::PsuRemovedV0::VERSION,
            payload: serde_json::json!({}),
            requested_sitrep_id: SitrepUuid::new_v4(),
            comment: "removed earlier".to_string(),
        };
        let parent =
            parent_with_open_case(inv_collection_id, [remove], [remove_alert]);

        // Now a fresh insert arrives.
        let insert =
            reporter.parse_ereport(Utc::now(), ereports::PSU_INSERT_JSON);
        let input =
            build_input(&fmtest, example.collection, Some(parent), [insert]);
        let sitrep = run_analyze(&logctx.log, &input);

        let case = sole_case(&sitrep);
        assert!(
            !case.is_open(),
            "the insert means the PSU is present again, so the case closes"
        );
        assert_eq!(
            case.alerts_requested.len(),
            2,
            "the carried-forward remove alert plus exactly one new insert alert"
        );
        let inserted = case
            .alerts_requested
            .iter()
            .filter(|a| a.class == AlertClass::PsuInserted)
            .count();
        let removed = case
            .alerts_requested
            .iter()
            .filter(|a| a.class == AlertClass::PsuRemoved)
            .count();
        assert_eq!(inserted, 1, "exactly one new insert alert");
        assert_eq!(removed, 1, "the remove alert must not be duplicated");

        logctx.cleanup_successful();
    }

    /// Across reporter restarts, the most recent event is the one in the
    /// restart that was first seen *latest*, not the one with the greatest ENA
    /// or the latest collection time.
    ///
    /// The scenario is constructed so those bases disagree: restart A (seen
    /// first) sees a remove and then an insert, and that insert is both the
    /// highest-ENA ereport and the latest-collected one (Nexus ingested it
    /// after a collection lag). Restart B, which began later, sees only a
    /// remove, with a lower ENA. Because restart B is the later reporter
    /// session, its remove is the most recent *event*, so the PSU is absent
    /// and the case must stay open.
    #[test]
    fn most_recent_event_follows_restart_first_seen() {
        let (mut fmtest, logctx) = FmTest::new_with_logctx(
            "most_recent_event_follows_restart_first_seen",
        );
        let (example, _bp) = fmtest.system_builder.build();

        let t0 = Utc::now();
        // Restart A is first seen at `t0`.
        let mut restart_a = fmtest.reporters.reporter(
            ereport::Reporter::Sp { sp_type: SpType::Power, slot: SHELF },
            t0,
        );
        // Restart A: remove, then a later-collected insert with a higher ENA.
        let remove_a = restart_a.parse_ereport(t0, ereports::PSU_REMOVE_JSON);
        let insert_a = restart_a.parse_ereport(
            t0 + Duration::seconds(10),
            ereports::PSU_INSERT_JSON,
        );

        // The reporter restarts (ENAs reset): restart B is first seen after
        // restart A but before A's insert was collected.
        let mut restart_b = fmtest.reporters.reporter(
            ereport::Reporter::Sp { sp_type: SpType::Power, slot: SHELF },
            t0 + Duration::seconds(5),
        );
        let remove_b = restart_b.parse_ereport(
            t0 + Duration::seconds(5),
            ereports::PSU_REMOVE_JSON,
        );

        let input = build_input(
            &fmtest,
            example.collection,
            None,
            [remove_a, insert_a, remove_b],
        );
        let sitrep = run_analyze(&logctx.log, &input);

        let case = sole_case(&sitrep);
        assert!(
            case.is_open(),
            "the latest reporter restart saw a remove, so the PSU is absent \
             and its case must stay open"
        );
        assert_eq!(
            case.alerts_requested.len(),
            3,
            "each of the three events should request its own alert"
        );

        logctx.cleanup_successful();
    }
}
