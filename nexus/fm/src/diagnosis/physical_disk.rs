// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk diagnosis engine.
//!
//! A Disk case is opened for an in-service disk when the latest inventory
//! reports a problem with it, and closed once inventory shows the problem
//! gone. Two signals are read, each tracked by its own kind of fact:
//!
//! - the health of the disk's zpool, when it is not `Online`
//! - the critical warnings of the disk's NVMe controller, when it reports
//!   that its reliability is degraded or that its media is read-only

use crate::SitrepBuilder;
use iddqd::{BiHashItem, BiHashMap, IdOrdItem, IdOrdMap, bi_upcast, id_upcast};
use nexus_types::fm;
use nexus_types::fm::DiagnosisEngineKind;
use nexus_types::fm::{
    DiskFact, NvmeHealthWarningFactPayload, ZpoolUnhealthyFactPayload,
};
use nexus_types::inventory::ZpoolHealth;
use omicron_uuid_kinds::{
    CaseUuid, FactUuid, PhysicalDiskUuid, SledUuid, ZpoolUuid,
};
use std::collections::BTreeMap;
use std::fmt;

/// A [`DiskFact::ZpoolUnhealthy`] payload paired with the `FactUuid` it
/// lives under.
#[derive(Clone, Copy, Debug)]
struct ZpoolUnhealthyFact {
    fact_id: FactUuid,
    payload: ZpoolUnhealthyFactPayload,
}

impl IdOrdItem for ZpoolUnhealthyFact {
    type Key<'a> = FactUuid;
    fn key(&self) -> Self::Key<'_> {
        self.fact_id
    }
    id_upcast!();
}

/// A [`DiskFact::NvmeHealthWarning`] payload paired with the `FactUuid` it
/// lives under.
#[derive(Clone, Copy, Debug)]
struct NvmeHealthWarningFact {
    fact_id: FactUuid,
    payload: NvmeHealthWarningFactPayload,
}

impl IdOrdItem for NvmeHealthWarningFact {
    type Key<'a> = FactUuid;
    fn key(&self) -> Self::Key<'_> {
        self.fact_id
    }
    id_upcast!();
}

/// The NVMe critical warnings this engine acts on.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NvmeWarnings {
    reliability_degraded: bool,
    read_only: bool,
}

impl NvmeWarnings {
    fn any(self) -> bool {
        self.reliability_degraded || self.read_only
    }
}

impl fmt::Display for NvmeWarnings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names = [
            (self.reliability_degraded, "reliability degraded"),
            (self.read_only, "read-only"),
        ]
        .into_iter()
        .filter_map(|(raised, name)| raised.then_some(name))
        .collect::<Vec<_>>();
        if names.is_empty() {
            write!(f, "none")
        } else {
            write!(f, "{}", names.join(", "))
        }
    }
}

/// What the current inventory says about one of a disk's health signals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Signal<T> {
    /// The inventory has no observation. This is not evidence either way:
    /// the sled could be powered off, or the collection could be lossy.
    Unknown,
    Healthy,
    Faulty(T),
}

/// One in-service disk paired with what the current inventory observed of
/// it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DiskHealthSnapshot {
    physical_disk_id: PhysicalDiskUuid,
    zpool_id: ZpoolUuid,
    /// `None` when the disk's zpool was not seen in the current inventory
    /// (e.g., sled down, lossy collection).
    zpool_health: Option<ZpoolHealth>,
    /// `None` when the current inventory has no NVMe health reading for the
    /// disk: its sled reported no such disk, or reported an error in place
    /// of the reading.
    nvme_warnings: Option<NvmeWarnings>,
}

impl DiskHealthSnapshot {
    fn zpool_signal(&self) -> Signal<ZpoolHealth> {
        match self.zpool_health {
            None => Signal::Unknown,
            Some(ZpoolHealth::Online) => Signal::Healthy,
            Some(health) => Signal::Faulty(health),
        }
    }

    fn nvme_signal(&self) -> Signal<NvmeWarnings> {
        match self.nvme_warnings {
            None => Signal::Unknown,
            Some(warnings) if warnings.any() => Signal::Faulty(warnings),
            Some(_) => Signal::Healthy,
        }
    }
}

impl IdOrdItem for DiskHealthSnapshot {
    type Key<'a> = PhysicalDiskUuid;
    fn key(&self) -> Self::Key<'_> {
        self.physical_disk_id
    }
    id_upcast!();
}

/// A parent-forwarded Disk case, parsed into the form this engine acts on.
/// Each Disk case is about a single physical disk; every fact on the case
/// must reference that disk.
struct ParsedDiskCase {
    /// The case this was parsed from.
    case_id: CaseUuid,
    /// The physical disk this case is about.
    physical_disk_id: PhysicalDiskUuid,
    /// All `ZpoolUnhealthy` facts on this case. Normally at most one;
    /// pathological cases may have more. Regardless, the diagnosis engine
    /// keeps all of them.
    unhealthy_facts: IdOrdMap<ZpoolUnhealthyFact>,
    /// All `NvmeHealthWarning` facts on this case. As with
    /// `unhealthy_facts`, normally at most one.
    nvme_warning_facts: IdOrdMap<NvmeHealthWarningFact>,
}

/// A `ParsedDiskCase` is indexed by both the case it came from and the disk it
/// concerns. Keying on `physical_disk_id` makes "at most one case per disk" an
/// invariant of the map itself: inserting a second case for a disk already
/// present fails (see [`analyze`]).
impl BiHashItem for ParsedDiskCase {
    type K1<'a> = CaseUuid;
    type K2<'a> = PhysicalDiskUuid;
    fn key1(&self) -> Self::K1<'_> {
        self.case_id
    }
    fn key2(&self) -> Self::K2<'_> {
        self.physical_disk_id
    }
    bi_upcast!();
}

/// Why a parent-forwarded Disk case could not be interpreted.
///
/// Uninterpretable cases are closed by [`analyze`]: an open case this engine
/// cannot process would otherwise be carried forward into every future
/// sitrep with no path to closure.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
enum UninterpretableCase {
    #[error(transparent)]
    ForeignFact(#[from] fm::case::ForeignFact),
    #[error(
        "facts reference different physical disks ({expected} and {found}, \
         1 expected)"
    )]
    DisagreeingDisks { expected: PhysicalDiskUuid, found: PhysicalDiskUuid },
    #[error("case has no facts, so the disk it concerns cannot be determined")]
    NoFacts,
}

/// Parse one parent-forwarded Disk case into a [`ParsedDiskCase`], or explain
/// why it cannot be interpreted.
fn parse_case(case: &fm::Case) -> Result<ParsedDiskCase, UninterpretableCase> {
    let mut unhealthy_facts: IdOrdMap<ZpoolUnhealthyFact> = IdOrdMap::new();
    let mut nvme_warning_facts: IdOrdMap<NvmeHealthWarningFact> =
        IdOrdMap::new();
    let mut case_disk_id: Option<PhysicalDiskUuid> = None;
    for fact in case.facts.iter() {
        // Every fact on a physical-disk case must carry a physical-disk
        // payload; a foreign payload is a data-model violation.
        let disk_fact = fact.as_physical_disk()?;
        let fact_disk_id = disk_fact.physical_disk_id();
        let disk_id = *case_disk_id.get_or_insert(fact_disk_id);
        if disk_id != fact_disk_id {
            return Err(UninterpretableCase::DisagreeingDisks {
                expected: disk_id,
                found: fact_disk_id,
            });
        }
        let fact_id = fact.metadata.id;
        match disk_fact {
            DiskFact::ZpoolUnhealthy(payload) => {
                unhealthy_facts
                    .insert_unique(ZpoolUnhealthyFact {
                        fact_id,
                        payload: *payload,
                    })
                    .expect("fact ids are unique within a case");
            }
            DiskFact::NvmeHealthWarning(payload) => {
                nvme_warning_facts
                    .insert_unique(NvmeHealthWarningFact {
                        fact_id,
                        payload: *payload,
                    })
                    .expect("fact ids are unique within a case");
            }
        }
    }
    let Some(physical_disk_id) = case_disk_id else {
        return Err(UninterpretableCase::NoFacts);
    };
    Ok(ParsedDiskCase {
        case_id: case.id,
        physical_disk_id,
        unhealthy_facts,
        nvme_warning_facts,
    })
}

pub(super) fn analyze(builder: &mut SitrepBuilder<'_>) -> anyhow::Result<()> {
    let input = builder.input();
    let inv_collection_id = input.inventory().id;
    let inv_time_done = input.inventory().time_done;

    // Index every zpool we observed in this inventory, so we can distinguish
    // "saw it, it's Online" from "didn't see it at all" when looking up by
    // an in-service disk's zpool below.
    let observed_zpool_health: BTreeMap<ZpoolUuid, ZpoolHealth> = input
        .inventory()
        .sled_agents
        .iter()
        .flat_map(|sa| sa.zpools.iter())
        .map(|z| (z.id, z.health))
        .collect();

    // Likewise, index every NVMe health reading in this inventory by the
    // sled that reported it and the disk's (vendor, model, serial). A disk
    // whose sled reported an error instead of a reading is left out, just as
    // a disk the sled did not report at all is.
    let observed_nvme_warnings: BTreeMap<
        (SledUuid, &str, &str, &str),
        NvmeWarnings,
    > = input
        .inventory()
        .sled_agents
        .iter()
        .flat_map(|sa| {
            sa.disks.iter().filter_map(move |disk| {
                let health = disk.nvme_health.as_ref().ok()?;
                let key = (
                    sa.sled_id,
                    disk.identity.vendor.as_str(),
                    disk.identity.model.as_str(),
                    disk.identity.serial.as_str(),
                );
                let warnings = NvmeWarnings {
                    reliability_degraded: health.reliability_degraded,
                    read_only: health.read_only,
                };
                Some((key, warnings))
            })
        })
        .collect();

    // The current health snapshot for every in-service disk, keyed by
    // physical_disk_id. Absence from this index is a positive signal that
    // the control plane has moved on from the disk (expungement /
    // decommissioning); see the analysis task's input preparation.
    let in_service_health: IdOrdMap<DiskHealthSnapshot> = input
        .in_service_disks()
        .iter()
        .map(|d| DiskHealthSnapshot {
            physical_disk_id: d.physical_disk_id,
            zpool_id: d.zpool_id,
            zpool_health: observed_zpool_health.get(&d.zpool_id).copied(),
            nvme_warnings: observed_nvme_warnings
                .get(&(
                    d.sled_id,
                    d.vendor.as_str(),
                    d.model.as_str(),
                    d.serial.as_str(),
                ))
                .copied(),
        })
        .collect();

    // Index the Disk cases from the parent sitrep, keyed by both the case id
    // and the physical disk the case concerns.
    //
    // Keying on the disk makes "at most one case per disk" an invariant of the
    // map: `insert_unique` rejects a second case for a disk already present.
    // A disk with two parent cases is pathological; we keep the first one
    // inserted and close the rest as duplicates.
    let mut cases = BiHashMap::<ParsedDiskCase>::new();
    for case in input
        .open_cases()
        .iter()
        .filter(|c| c.metadata.de == DiagnosisEngineKind::PhysicalDisk)
    {
        let parsed_case = match parse_case(case) {
            Ok(parsed_case) => parsed_case,
            Err(reason) => {
                // Close the cases we couldn't interpret, so they don't ride
                // along as open-but-unprocessable in every future sitrep.
                builder
                    .log_warning("closing uninterpretable Disk case")
                    .kv("case_id", case.id)
                    .kv("reason", reason.to_string())
                    .finish();
                builder
                    .cases
                    .case_mut(&case.id)
                    .expect("case_id came from builder's open cases")
                    .close(format!("cannot interpret case: {reason}"));
                continue;
            }
        };

        if let Err(dup) = cases.insert_unique(parsed_case) {
            // A case for this disk is already present; keep it and close this
            // one. The disk id is the only key that can collide here, since
            // case ids are unique across `open_cases()`, so there is exactly
            // one duplicate.
            let kept = dup.duplicates()[0];
            builder
                .cases
                .case_mut(&case.id)
                .expect("case_id came from builder's open cases")
                .close(format!(
                    "duplicate of case {} for disk {}",
                    kept.case_id, kept.physical_disk_id,
                ));
        }
    }

    // Close the surviving parent case for any disk that has left service.
    for parsed_case in cases.iter() {
        if in_service_health.contains_key(&parsed_case.physical_disk_id) {
            continue;
        }
        builder
            .cases
            .case_mut(&parsed_case.case_id)
            .expect("case_id came from builder's open cases")
            .close(format!(
                "disk {} no longer in service",
                parsed_case.physical_disk_id,
            ));
    }

    // For each in-service disk, make its case carry exactly the facts that
    // match the current observations: reuse the parent-forwarded case if any
    // (dropping its stale facts), open a fresh case if the disk is newly
    // faulty, and close the case once nothing on it still holds.
    //
    // A signal absent from this inventory changes nothing. Its facts stay,
    // and keep their case open: absence is NOT a recovery signal.
    for disk in in_service_health.iter() {
        let parsed_case = cases.get2(&disk.physical_disk_id);
        let zpool = disk.zpool_signal();
        let nvme = disk.nvme_signal();

        // Sort the parent's facts into those to keep and those to remove,
        // and work out which signals still need a fact added.
        let mut kept_facts = 0;
        let mut stale_facts: Vec<(FactUuid, &'static str)> = Vec::new();
        let mut zpool_fact_needed = matches!(zpool, Signal::Faulty(_));
        let mut nvme_fact_needed = matches!(nvme, Signal::Faulty(_));
        // Although we expect at most one fact of each kind on a case, the
        // schema allows more. We look at all of them, so that every
        // out-of-date fact is removed.
        for fact in parsed_case.iter().flat_map(|c| c.unhealthy_facts.iter()) {
            match zpool {
                Signal::Unknown => kept_facts += 1,
                Signal::Healthy => {
                    stale_facts.push((fact.fact_id, "zpool is back to Online"));
                }
                Signal::Faulty(health)
                    if fact.payload.zpool_id == disk.zpool_id
                        && fact.payload.last_seen_health == health =>
                {
                    // An accurate fact is already present; keep it.
                    kept_facts += 1;
                    zpool_fact_needed = false;
                }
                Signal::Faulty(_) => stale_facts.push((
                    fact.fact_id,
                    "stale zpool observation, superseded by current \
                     inventory",
                )),
            }
        }
        for fact in parsed_case.iter().flat_map(|c| c.nvme_warning_facts.iter())
        {
            let fact_warnings = NvmeWarnings {
                reliability_degraded: fact.payload.reliability_degraded,
                read_only: fact.payload.read_only,
            };
            match nvme {
                Signal::Unknown => kept_facts += 1,
                Signal::Healthy => stale_facts
                    .push((fact.fact_id, "NVMe health warnings have cleared")),
                Signal::Faulty(warnings) if fact_warnings == warnings => {
                    kept_facts += 1;
                    nvme_fact_needed = false;
                }
                Signal::Faulty(_) => stale_facts.push((
                    fact.fact_id,
                    "stale NVMe health observation, superseded by current \
                     inventory",
                )),
            }
        }

        if kept_facts == 0 && !zpool_fact_needed && !nvme_fact_needed {
            // Nothing is known to be wrong with this disk. If it has a case,
            // every fact on that case was just found to no longer hold.
            if let Some(parsed_case) = parsed_case {
                let mut recovered = Vec::new();
                if !parsed_case.unhealthy_facts.is_empty() {
                    recovered.push(format!(
                        "zpool {} back to Online",
                        disk.zpool_id
                    ));
                }
                if !parsed_case.nvme_warning_facts.is_empty() {
                    recovered.push(format!(
                        "NVMe health warnings on disk {} cleared",
                        disk.physical_disk_id
                    ));
                }
                builder
                    .cases
                    .case_mut(&parsed_case.case_id)
                    .expect("case_id came from builder's open cases")
                    .close(recovered.join("; "));
            }
            continue;
        }

        let mut case_mut = match parsed_case {
            Some(parsed_case) => builder
                .cases
                .case_mut(&parsed_case.case_id)
                .expect("case_id came from builder's open cases"),
            None => {
                let mut problems = Vec::new();
                if let Signal::Faulty(health) = zpool {
                    problems
                        .push(format!("zpool {} was {health}", disk.zpool_id));
                }
                if let Signal::Faulty(warnings) = nvme {
                    problems.push(format!(
                        "it reported NVMe health warnings: {warnings}"
                    ));
                }
                builder.cases.open_case(
                    DiagnosisEngineKind::PhysicalDisk,
                    format!(
                        "opened for disk {} because {}",
                        disk.physical_disk_id,
                        problems.join(" and ")
                    ),
                )
            }
        };

        for (fact_id, comment) in stale_facts {
            case_mut.remove_fact(fact_id, comment);
        }
        if let (true, Signal::Faulty(health)) = (zpool_fact_needed, zpool) {
            case_mut.add_fact(
                DiskFact::ZpoolUnhealthy(ZpoolUnhealthyFactPayload {
                    physical_disk_id: disk.physical_disk_id,
                    zpool_id: disk.zpool_id,
                    last_seen_health: health,
                    observed_in_inv: inv_collection_id,
                    time_observed: inv_time_done,
                }),
                format!("zpool {} health={health}", disk.zpool_id),
            );
        }
        if let (true, Signal::Faulty(warnings)) = (nvme_fact_needed, nvme) {
            case_mut.add_fact(
                DiskFact::NvmeHealthWarning(NvmeHealthWarningFactPayload {
                    physical_disk_id: disk.physical_disk_id,
                    reliability_degraded: warnings.reliability_degraded,
                    read_only: warnings.read_only,
                    observed_in_inv: inv_collection_id,
                    time_observed: inv_time_done,
                }),
                format!("NVMe health warnings: {warnings}"),
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{
        FmTest, build_input, make_disk_case, make_in_service_disks,
        make_parent_sitrep, make_zpool_degraded_fact, run_analyze, set_health,
    };
    use chrono::Utc;
    use nexus_types::external_api::physical_disk::PhysicalDiskKind;
    use nexus_types::fm::{self, Sitrep};
    use nexus_types::in_service_disk::InServiceDisk;
    use nexus_types::inventory;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::{PhysicalDiskUuid, SitrepUuid};
    use sled_agent_types::inventory::{
        NvmeHealth, NvmeHealthError, NvmeHealthErrorKind,
    };

    /// Find the `physical_disk_id` for the given `zpool_id` in the
    /// in-service set, or fabricate a fresh one if not present (e.g., when
    /// simulating an expunged disk whose case should still reference some
    /// stable PhysicalDiskUuid).
    fn disk_id_for(
        in_service: &IdOrdMap<InServiceDisk>,
        zpool_id: ZpoolUuid,
    ) -> PhysicalDiskUuid {
        in_service
            .iter()
            .find(|d| d.zpool_id == zpool_id)
            .map(|d| d.physical_disk_id)
            .unwrap_or_else(PhysicalDiskUuid::new_v4)
    }

    /// Make a synthetic test scenario from the example system: returns a
    /// `LogContext` (the caller must `cleanup_successful()` it), the
    /// example collection, and every zpool ID in that collection.
    fn setup(
        test_name: &'static str,
    ) -> (dev::LogContext, inventory::Collection, Vec<ZpoolUuid>) {
        let (fm_test, logctx) = FmTest::new_with_logctx(test_name);
        // Build the example system once to get a Collection with zpools.
        let (example, _bp) = fm_test.system_builder.build();
        let zpool_ids: Vec<ZpoolUuid> = example
            .collection
            .sled_agents
            .iter()
            .flat_map(|sa| sa.zpools.iter().map(|z| z.id))
            .collect();
        assert!(
            !zpool_ids.is_empty(),
            "example system should have at least one zpool"
        );
        (logctx, example.collection, zpool_ids)
    }

    fn make_parent_with_disk_case(
        parent_sitrep_id: SitrepUuid,
        inv_collection_id: omicron_uuid_kinds::CollectionUuid,
        physical_disk_id: PhysicalDiskUuid,
        zpool_id: ZpoolUuid,
    ) -> Sitrep {
        let fact = make_zpool_degraded_fact(
            parent_sitrep_id,
            inv_collection_id,
            physical_disk_id,
            zpool_id,
        );
        let case = make_disk_case(
            omicron_uuid_kinds::CaseUuid::new_v4(),
            parent_sitrep_id,
            [fact],
        );
        make_parent_sitrep(parent_sitrep_id, inv_collection_id, [case])
    }

    /// A physical-disk fact found in a sitrep, paired with the case it lives
    /// on and its decoded [`DiskFact`] payload.
    #[derive(Debug)]
    struct DiskFactRef<'a> {
        case: &'a fm::Case,
        fact: &'a fm::case::Fact,
        disk_fact: DiskFact,
    }

    /// Collect every physical-disk fact in a sitrep, with its case and decoded
    /// payload. Optionally filtered to open cases only.
    fn disk_facts(sitrep: &Sitrep, open_only: bool) -> Vec<DiskFactRef<'_>> {
        sitrep
            .cases
            .iter()
            .filter(|c| c.metadata.de == DiagnosisEngineKind::PhysicalDisk)
            .filter(|c| !open_only || c.is_open())
            .flat_map(|c| {
                c.facts.iter().filter_map(move |f| {
                    f.payload.as_physical_disk().map(|d| DiskFactRef {
                        case: c,
                        fact: f,
                        disk_fact: d.clone(),
                    })
                })
            })
            .collect()
    }

    /// The fact UUID of the one physical-disk fact on the one physical-disk
    /// case in `sitrep`. Panics unless there is exactly one.
    fn sole_disk_fact_id(sitrep: &Sitrep) -> omicron_uuid_kinds::FactUuid {
        let facts = disk_facts(sitrep, false);
        assert_eq!(facts.len(), 1, "expected exactly one physical-disk fact");
        facts[0].fact.metadata.id
    }

    #[test]
    fn opens_on_degraded_in_service() {
        let (logctx, mut collection, zpools) =
            setup("disk_open_degraded_in_service");
        let target = zpools[0];
        set_health(&mut collection, target, ZpoolHealth::Degraded);
        let in_service = make_in_service_disks(zpools.iter().copied());
        let expected_disk_id = disk_id_for(&in_service, target);
        let input = build_input(collection, None, in_service);

        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let facts = disk_facts(&sitrep, true);
        assert_eq!(facts.len(), 1);
        match &facts[0].disk_fact {
            DiskFact::ZpoolUnhealthy(ZpoolUnhealthyFactPayload {
                physical_disk_id,
                zpool_id,
                last_seen_health,
                ..
            }) => {
                assert_eq!(*physical_disk_id, expected_disk_id);
                assert_eq!(*zpool_id, target);
                assert_eq!(*last_seen_health, ZpoolHealth::Degraded);
            }
            other => panic!("expected a ZpoolUnhealthy fact, got {other:?}"),
        }
        logctx.cleanup_successful();
    }

    #[test]
    fn skips_degraded_when_expunged() {
        let (logctx, mut collection, zpools) = setup("disk_skip_expunged");
        let target = zpools[0];
        set_health(&mut collection, target, ZpoolHealth::Faulted);
        // target is *not* in the in-service set.
        let in_service = make_in_service_disks(zpools.iter().copied().skip(1));
        let input = build_input(collection, None, in_service);

        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let cases = disk_facts(&sitrep, false);
        assert!(
            cases.is_empty(),
            "no Disk cases should be opened for expunged zpool, got: {:?}",
            cases
        );
        logctx.cleanup_successful();
    }

    #[test]
    fn idempotent_when_case_already_open() {
        let (logctx, mut collection, zpools) = setup("disk_idempotent");
        let target = zpools[0];
        set_health(&mut collection, target, ZpoolHealth::Degraded);
        let in_service = make_in_service_disks(zpools.iter().copied());
        let target_disk_id = disk_id_for(&in_service, target);
        let parent_id = SitrepUuid::new_v4();
        let parent = make_parent_with_disk_case(
            parent_id,
            collection.id,
            target_disk_id,
            target,
        );

        let input = build_input(collection, Some(parent), in_service);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let open_cases = disk_facts(&sitrep, true);
        assert_eq!(open_cases.len(), 1);
        match &open_cases[0].disk_fact {
            DiskFact::ZpoolUnhealthy(ZpoolUnhealthyFactPayload {
                zpool_id,
                ..
            }) => {
                assert_eq!(*zpool_id, target);
            }
            other => panic!("expected a ZpoolUnhealthy fact, got {other:?}"),
        }
        logctx.cleanup_successful();
    }

    #[test]
    fn closes_on_recovery() {
        let (logctx, collection, zpools) = setup("disk_close_on_recovery");
        let target = zpools[0];
        // The example system reports zpools as Online by default.
        let in_service = make_in_service_disks(zpools.iter().copied());
        let target_disk_id = disk_id_for(&in_service, target);
        let parent_id = SitrepUuid::new_v4();
        let parent = make_parent_with_disk_case(
            parent_id,
            collection.id,
            target_disk_id,
            target,
        );

        let input = build_input(collection, Some(parent), in_service);
        let (sitrep, report) = run_analyze(&logctx.log, &input);
        let all = disk_facts(&sitrep, false);
        assert_eq!(all.len(), 1);
        assert!(
            !all[0].case.is_open(),
            "case should be closed when zpool returns to Online",
        );
        let report_str = format!("{}", report.display_multiline(0));
        assert!(
            report_str.contains("back to Online"),
            "close comment should call out the recovery cause, got: \
             {report_str}",
        );
        logctx.cleanup_successful();
    }

    #[test]
    fn closes_on_expungement() {
        let (logctx, mut collection, zpools) =
            setup("disk_close_on_expungement");
        let target = zpools[0];
        set_health(&mut collection, target, ZpoolHealth::Degraded);
        // Target is NOT in-service in this sitrep (just expunged), so
        // disk_id_for fabricates a stable PhysicalDiskUuid for it.
        let in_service = make_in_service_disks(zpools.iter().copied().skip(1));
        let target_disk_id = disk_id_for(&in_service, target);
        let parent_id = SitrepUuid::new_v4();
        let parent = make_parent_with_disk_case(
            parent_id,
            collection.id,
            target_disk_id,
            target,
        );

        let input = build_input(collection, Some(parent), in_service);
        let (sitrep, report) = run_analyze(&logctx.log, &input);
        let all = disk_facts(&sitrep, false);
        assert_eq!(all.len(), 1);
        assert!(
            !all[0].case.is_open(),
            "case should be closed when zpool's disk is expunged",
        );
        let report_str = format!("{}", report.display_multiline(0));
        assert!(
            report_str.contains("no longer in service"),
            "close comment should call out the expungement cause, got: \
             {report_str}",
        );
        logctx.cleanup_successful();
    }

    #[test]
    fn keeps_open_on_absence_from_inventory() {
        // A zpool the case is about does NOT appear in the inventory at all
        // (sled powered off, lossy collection, etc.). The case should stay
        // open: absence is not a recovery signal.
        let (logctx, collection, zpools) = setup("disk_keep_open_on_absence");
        let phantom = ZpoolUuid::new_v4();
        assert!(!zpools.contains(&phantom));
        let in_service =
            make_in_service_disks(zpools.iter().copied().chain([phantom]));
        let phantom_disk_id = disk_id_for(&in_service, phantom);
        let parent_id = SitrepUuid::new_v4();
        let parent = make_parent_with_disk_case(
            parent_id,
            collection.id,
            phantom_disk_id,
            phantom,
        );

        let input = build_input(collection, Some(parent), in_service);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let all = disk_facts(&sitrep, false);
        assert_eq!(all.len(), 1);
        assert!(
            all[0].case.is_open(),
            "case should remain open when its zpool is absent from the \
             current inventory collection (sled could be down or inventory \
             is lossy)",
        );
        logctx.cleanup_successful();
    }

    /// A parent Disk case with zero facts has no derivable disk ID; the
    /// engine closes it as uninterpretable rather than carrying an
    /// unprocessable open case forward into every future sitrep.
    #[test]
    fn empty_case_is_closed() {
        let (logctx, collection, _zpools) = setup("disk_empty_case_closed");
        let in_service = make_in_service_disks(std::iter::empty());

        let parent_sitrep_id = SitrepUuid::new_v4();
        let empty_case_id = omicron_uuid_kinds::CaseUuid::new_v4();
        let empty_case = make_disk_case(empty_case_id, parent_sitrep_id, []);
        let parent =
            make_parent_sitrep(parent_sitrep_id, collection.id, [empty_case]);

        let input = build_input(collection, Some(parent), in_service);
        let (sitrep, report) = run_analyze(&logctx.log, &input);

        let case = sitrep
            .cases
            .iter()
            .find(|c| c.id == empty_case_id)
            .expect("empty case should still be in the output sitrep");
        assert!(!case.is_open(), "uninterpretable empty case should be closed",);
        let report_str = format!("{}", report.display_multiline(0));
        assert!(
            report_str.contains("cannot interpret case"),
            "close comment should say the case was uninterpretable, got: \
             {report_str}",
        );
        logctx.cleanup_successful();
    }

    /// Two open parent cases about the same disk: the engine keeps and
    /// maintains the first one it encounters and closes the other as a
    /// duplicate. Because `open_cases()` is ordered by case id, the survivor is
    /// deterministically the lowest-ID case. (A half-maintained duplicate would
    /// otherwise decay into an uninterpretable empty case.)
    #[test]
    fn duplicate_case_is_closed() {
        let (logctx, mut collection, zpools) = setup("disk_duplicate_closed");
        let target = zpools[0];
        set_health(&mut collection, target, ZpoolHealth::Degraded);
        let in_service = make_in_service_disks(zpools.iter().copied());
        let target_disk_id = disk_id_for(&in_service, target);
        let parent_id = SitrepUuid::new_v4();

        let id_a = omicron_uuid_kinds::CaseUuid::new_v4();
        let id_b = omicron_uuid_kinds::CaseUuid::new_v4();
        let (kept_id, dup_id) =
            if id_a < id_b { (id_a, id_b) } else { (id_b, id_a) };
        let kept_fact = make_zpool_degraded_fact(
            parent_id,
            collection.id,
            target_disk_id,
            target,
        );
        let kept_fact_id = kept_fact.metadata.id;
        let dup_fact = make_zpool_degraded_fact(
            parent_id,
            collection.id,
            target_disk_id,
            target,
        );
        let parent = make_parent_sitrep(
            parent_id,
            collection.id,
            [
                make_disk_case(kept_id, parent_id, [kept_fact]),
                make_disk_case(dup_id, parent_id, [dup_fact]),
            ],
        );

        let input = build_input(collection, Some(parent), in_service);
        let (sitrep, report) = run_analyze(&logctx.log, &input);

        let kept = sitrep.cases.get(&kept_id).expect("kept case present");
        let dup = sitrep.cases.get(&dup_id).expect("duplicate case present");
        assert!(kept.is_open(), "lowest-ID case should remain open");
        assert!(!dup.is_open(), "duplicate case should be closed");
        // The kept case's fact is still accurate (Degraded), so it should
        // carry forward unchanged.
        assert!(
            kept.facts.contains_key(&kept_fact_id),
            "kept case should retain its fact",
        );
        // No third case should have been opened for this disk.
        let disk_case_count = sitrep
            .cases
            .iter()
            .filter(|c| c.metadata.de == DiagnosisEngineKind::PhysicalDisk)
            .count();
        assert_eq!(disk_case_count, 2);
        let report_str = format!("{}", report.display_multiline(0));
        assert!(
            report_str.contains("duplicate of case"),
            "close comment should call out the duplicate, got: {report_str}",
        );
        logctx.cleanup_successful();
    }

    /// A case whose facts disagree about which disk they concern is closed
    /// as uninterpretable. Because fault detection is independent of case
    /// bookkeeping, the still-unhealthy disk gets a fresh, well-formed case
    /// in the same pass.
    #[test]
    fn uninterpretable_case_is_replaced() {
        let (logctx, mut collection, zpools) =
            setup("disk_uninterpretable_replaced");
        let target = zpools[0];
        set_health(&mut collection, target, ZpoolHealth::Degraded);
        let in_service = make_in_service_disks(zpools.iter().copied());
        let target_disk_id = disk_id_for(&in_service, target);
        let parent_id = SitrepUuid::new_v4();

        // One fact about the target disk, one about an unrelated disk: the
        // case is self-contradictory.
        let corrupt_case_id = omicron_uuid_kinds::CaseUuid::new_v4();
        let corrupt_case = make_disk_case(
            corrupt_case_id,
            parent_id,
            [
                make_zpool_degraded_fact(
                    parent_id,
                    collection.id,
                    target_disk_id,
                    target,
                ),
                make_zpool_degraded_fact(
                    parent_id,
                    collection.id,
                    PhysicalDiskUuid::new_v4(),
                    ZpoolUuid::new_v4(),
                ),
            ],
        );
        let parent =
            make_parent_sitrep(parent_id, collection.id, [corrupt_case]);

        let input = build_input(collection, Some(parent), in_service);
        let (sitrep, report) = run_analyze(&logctx.log, &input);

        let corrupt = sitrep
            .cases
            .get(&corrupt_case_id)
            .expect("corrupt case should still be in the output sitrep");
        assert!(
            !corrupt.is_open(),
            "case with disagreeing facts should be closed",
        );
        // The target disk is still Degraded and in service, so a fresh case
        // should have been opened for it.
        let open = disk_facts(&sitrep, true);
        assert_eq!(
            open.len(),
            1,
            "expected exactly one open Disk fact (on the replacement case)",
        );
        assert_ne!(open[0].case.id, corrupt_case_id);
        match &open[0].disk_fact {
            DiskFact::ZpoolUnhealthy(ZpoolUnhealthyFactPayload {
                physical_disk_id,
                zpool_id,
                last_seen_health,
                ..
            }) => {
                assert_eq!(*physical_disk_id, target_disk_id);
                assert_eq!(*zpool_id, target);
                assert_eq!(*last_seen_health, ZpoolHealth::Degraded);
            }
            other => panic!("expected a ZpoolUnhealthy fact, got {other:?}"),
        }
        let report_str = format!("{}", report.display_multiline(0));
        assert!(
            report_str.contains("cannot interpret case"),
            "close comment should say the case was uninterpretable, got: \
             {report_str}",
        );
        logctx.cleanup_successful();
    }

    /// When the parent sitrep's fact content matches the diagnosis engine's
    /// current observation, the fact carries forward with the same UUID,
    /// with no remove-and-readd churn.
    #[test]
    fn fact_uuid_stable_when_observation_unchanged() {
        let (logctx, mut collection, zpools) = setup("disk_fact_uuid_stable");
        let target = zpools[0];
        set_health(&mut collection, target, ZpoolHealth::Degraded);
        let in_service = make_in_service_disks(zpools.iter().copied());
        let target_disk_id = disk_id_for(&in_service, target);
        let parent_id = SitrepUuid::new_v4();
        let parent = make_parent_with_disk_case(
            parent_id,
            collection.id,
            target_disk_id,
            target,
        );
        // Capture the parent's fact UUID for the target zpool.
        let parent_fact_id = sole_disk_fact_id(&parent);

        let input = build_input(collection, Some(parent), in_service);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let open = disk_facts(&sitrep, true);
        assert_eq!(open.len(), 1, "expected exactly one open Disk fact");
        assert_eq!(
            open[0].fact.metadata.id, parent_fact_id,
            "fact UUID should be stable across sitreps when the \
             observation hasn't changed",
        );
        match &open[0].disk_fact {
            DiskFact::ZpoolUnhealthy(ZpoolUnhealthyFactPayload {
                zpool_id,
                last_seen_health,
                ..
            }) => {
                assert_eq!(*zpool_id, target);
                assert_eq!(*last_seen_health, ZpoolHealth::Degraded);
            }
            other => panic!("expected a ZpoolUnhealthy fact, got {other:?}"),
        }
        logctx.cleanup_successful();
    }

    /// When the parent's fact recorded a different `last_seen_health` than
    /// what we observe now, the diagnosis engine removes the stale fact and emits
    /// a fresh one (new UUID). The case stays open because the zpool is
    /// still unhealthy, just with a different value.
    #[test]
    fn fact_uuid_rotates_when_observation_changes() {
        let (logctx, mut collection, zpools) = setup("disk_fact_uuid_rotates");
        let target = zpools[0];
        // Parent recorded Degraded; current inventory shows Faulted.
        set_health(&mut collection, target, ZpoolHealth::Faulted);
        let in_service = make_in_service_disks(zpools.iter().copied());
        let target_disk_id = disk_id_for(&in_service, target);
        let parent_id = SitrepUuid::new_v4();
        let parent = make_parent_with_disk_case(
            parent_id,
            collection.id,
            target_disk_id,
            target,
        );
        let parent_fact_id = sole_disk_fact_id(&parent);

        let input = build_input(collection, Some(parent), in_service);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let open = disk_facts(&sitrep, true);
        assert_eq!(
            open.len(),
            1,
            "expected exactly one open Disk fact (the refreshed one)",
        );
        assert_ne!(
            open[0].fact.metadata.id, parent_fact_id,
            "fact UUID should rotate because last_seen_health changed",
        );
        match &open[0].disk_fact {
            DiskFact::ZpoolUnhealthy(ZpoolUnhealthyFactPayload {
                zpool_id,
                last_seen_health,
                ..
            }) => {
                assert_eq!(*zpool_id, target);
                assert_eq!(*last_seen_health, ZpoolHealth::Faulted);
            }
            other => panic!("expected a ZpoolUnhealthy fact, got {other:?}"),
        }
        // The case itself should still be the same one that was carried
        // forward; only the fact rotated.
        assert!(open[0].case.is_open());
        logctx.cleanup_successful();
    }

    /// When the parent's fact references a different zpool than the one
    /// currently backing the disk (the zpool was destroyed and recreated),
    /// the fact rotates even though the observed health is unchanged.
    /// Today's adoption flow mints a new physical disk UUID alongside a new
    /// zpool, so this can't happen via normal control plane operation; this
    /// pins the engine's behavior if that ever changes.
    #[test]
    fn fact_uuid_rotates_when_zpool_replaced() {
        let (logctx, mut collection, zpools) =
            setup("disk_fact_uuid_rotates_zpool_replaced");
        let target = zpools[0];
        set_health(&mut collection, target, ZpoolHealth::Degraded);
        let in_service = make_in_service_disks(zpools.iter().copied());
        let target_disk_id = disk_id_for(&in_service, target);
        // The parent's fact records the same disk and the same health
        // (Degraded), but a zpool that no longer exists.
        let old_zpool_id = ZpoolUuid::new_v4();
        let parent_id = SitrepUuid::new_v4();
        let parent = make_parent_with_disk_case(
            parent_id,
            collection.id,
            target_disk_id,
            old_zpool_id,
        );
        let parent_fact_id = sole_disk_fact_id(&parent);

        let input = build_input(collection, Some(parent), in_service);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let open = disk_facts(&sitrep, true);
        assert_eq!(
            open.len(),
            1,
            "expected exactly one open Disk fact (the refreshed one)",
        );
        assert_ne!(
            open[0].fact.metadata.id, parent_fact_id,
            "fact UUID should rotate because the disk's zpool changed",
        );
        match &open[0].disk_fact {
            DiskFact::ZpoolUnhealthy(ZpoolUnhealthyFactPayload {
                zpool_id,
                last_seen_health,
                ..
            }) => {
                assert_eq!(
                    *zpool_id, target,
                    "refreshed fact should reference the current zpool",
                );
                assert_eq!(*last_seen_health, ZpoolHealth::Degraded);
            }
            other => panic!("expected a ZpoolUnhealthy fact, got {other:?}"),
        }
        assert!(open[0].case.is_open());
        logctx.cleanup_successful();
    }

    /// A Disk case carrying a saga fact payload is a data-model violation;
    /// the engine closes it as uninterpretable.
    #[test]
    fn foreign_payload_case_is_closed() {
        let (logctx, collection, _zpools) =
            setup("disk_foreign_payload_closed");
        let inv_id = collection.id;
        let in_service = make_in_service_disks(std::iter::empty());
        let parent_id = SitrepUuid::new_v4();
        let case_id = omicron_uuid_kinds::CaseUuid::new_v4();
        let foreign_fact = fm::case::Fact {
            metadata: fm::FactMetadata {
                id: omicron_uuid_kinds::FactUuid::new_v4(),
                created_sitrep_id: parent_id,
                comment: "a fact belonging to the saga engine".to_string(),
            },
            payload: fm::FactPayload::Saga(fm::SagaFact::Abandoned(
                fm::SagaAbandonedFactPayload {
                    saga_id: steno::SagaId::from_u128(1),
                },
            )),
        };
        let parent = make_parent_sitrep(
            parent_id,
            inv_id,
            [make_disk_case(case_id, parent_id, [foreign_fact])],
        );

        let input = build_input(collection, Some(parent), in_service);
        let (sitrep, report) = run_analyze(&logctx.log, &input);

        let case = sitrep
            .cases
            .get(&case_id)
            .expect("case should still be in the output sitrep");
        assert!(
            !case.is_open(),
            "case with a foreign fact payload should be closed",
        );
        let report_str = format!("{}", report.display_multiline(0));
        assert!(
            report_str.contains("cannot interpret case"),
            "close comment should say the case was uninterpretable, got: \
             {report_str}",
        );
        logctx.cleanup_successful();
    }

    /// Make an in-service disk for every disk in `collection` that has a
    /// zpool, with the identity and sled the inventory reports for it, so
    /// that the engine can find each disk's NVMe health.
    fn in_service_disks_in_inventory(
        collection: &inventory::Collection,
    ) -> IdOrdMap<InServiceDisk> {
        let in_service: IdOrdMap<InServiceDisk> = collection
            .sled_agents
            .iter()
            .flat_map(|sa| {
                // Inventory does not say which zpool is on which disk, and
                // these tests do not care.
                sa.disks.iter().zip(sa.zpools.iter()).map(
                    move |(disk, zpool)| InServiceDisk {
                        physical_disk_id: PhysicalDiskUuid::new_v4(),
                        zpool_id: zpool.id,
                        sled_id: sa.sled_id,
                        vendor: disk.identity.vendor.clone(),
                        serial: disk.identity.serial.clone(),
                        model: disk.identity.model.clone(),
                        variant: PhysicalDiskKind::U2,
                    },
                )
            })
            .collect();
        assert!(
            !in_service.is_empty(),
            "example system should have at least one disk with a zpool"
        );
        in_service
    }

    /// Set what `collection` reports as the NVMe health of `disk`, panicking
    /// if the disk is not found.
    fn set_nvme_health(
        collection: &mut inventory::Collection,
        disk: &InServiceDisk,
        nvme_health: Result<NvmeHealth, NvmeHealthError>,
    ) {
        for mut sa in collection.sled_agents.iter_mut() {
            if sa.sled_id != disk.sled_id {
                continue;
            }
            for inv_disk in sa.disks.iter_mut() {
                if inv_disk.identity.serial == disk.serial {
                    inv_disk.nvme_health = nvme_health;
                    return;
                }
            }
        }
        panic!("disk {} not found in collection", disk.serial);
    }

    fn nvme_health(reliability_degraded: bool, read_only: bool) -> NvmeHealth {
        NvmeHealth {
            reliability_degraded,
            read_only,
            ..NvmeHealth::new_fake(Utc::now())
        }
    }

    /// Make a parent sitrep with one open Disk case for `disk`, carrying an
    /// `NvmeHealthWarning` fact with the given warnings.
    fn make_parent_with_nvme_case(
        collection: &inventory::Collection,
        disk: &InServiceDisk,
        reliability_degraded: bool,
        read_only: bool,
    ) -> Sitrep {
        let parent_sitrep_id = SitrepUuid::new_v4();
        let fact = make_nvme_warning_fact(
            parent_sitrep_id,
            collection,
            disk,
            reliability_degraded,
            read_only,
        );
        let case = make_disk_case(
            omicron_uuid_kinds::CaseUuid::new_v4(),
            parent_sitrep_id,
            [fact],
        );
        make_parent_sitrep(parent_sitrep_id, collection.id, [case])
    }

    /// Make a parent sitrep with one open Disk case for `disk`, carrying
    /// both a degraded `ZpoolUnhealthy` fact and an `NvmeHealthWarning` fact.
    fn make_parent_with_zpool_and_nvme_case(
        collection: &inventory::Collection,
        disk: &InServiceDisk,
    ) -> Sitrep {
        let parent_sitrep_id = SitrepUuid::new_v4();
        let zpool_fact = make_zpool_degraded_fact(
            parent_sitrep_id,
            collection.id,
            disk.physical_disk_id,
            disk.zpool_id,
        );
        let nvme_fact = make_nvme_warning_fact(
            parent_sitrep_id,
            collection,
            disk,
            true,
            false,
        );
        let case = make_disk_case(
            omicron_uuid_kinds::CaseUuid::new_v4(),
            parent_sitrep_id,
            [zpool_fact, nvme_fact],
        );
        make_parent_sitrep(parent_sitrep_id, collection.id, [case])
    }

    fn make_nvme_warning_fact(
        parent_sitrep_id: SitrepUuid,
        collection: &inventory::Collection,
        disk: &InServiceDisk,
        reliability_degraded: bool,
        read_only: bool,
    ) -> fm::case::Fact {
        fm::case::Fact {
            metadata: fm::FactMetadata {
                id: omicron_uuid_kinds::FactUuid::new_v4(),
                created_sitrep_id: parent_sitrep_id,
                comment: "NVMe health warnings".to_string(),
            },
            payload: DiskFact::NvmeHealthWarning(
                NvmeHealthWarningFactPayload {
                    physical_disk_id: disk.physical_disk_id,
                    reliability_degraded,
                    read_only,
                    observed_in_inv: collection.id,
                    time_observed: Utc::now(),
                },
            )
            .into(),
        }
    }

    /// The (reliability_degraded, read_only) of an `NvmeHealthWarning` fact
    /// about `disk`. Panics if the fact is anything else.
    #[track_caller]
    fn nvme_warnings_of(
        fact: &DiskFactRef<'_>,
        disk: &InServiceDisk,
    ) -> (bool, bool) {
        match &fact.disk_fact {
            DiskFact::NvmeHealthWarning(payload) => {
                assert_eq!(payload.physical_disk_id, disk.physical_disk_id);
                (payload.reliability_degraded, payload.read_only)
            }
            other => {
                panic!("expected an NvmeHealthWarning fact, got {other:?}")
            }
        }
    }

    #[test]
    fn opens_on_nvme_warning() {
        for (reliability_degraded, read_only) in
            [(true, false), (false, true), (true, true)]
        {
            let (logctx, mut collection, _zpools) =
                setup("disk_open_nvme_warning");
            let in_service = in_service_disks_in_inventory(&collection);
            let target = in_service.iter().next().unwrap().clone();
            set_nvme_health(
                &mut collection,
                &target,
                Ok(nvme_health(reliability_degraded, read_only)),
            );
            let input = build_input(collection, None, in_service);

            let (sitrep, _report) = run_analyze(&logctx.log, &input);
            let facts = disk_facts(&sitrep, true);
            assert_eq!(facts.len(), 1);
            assert_eq!(
                nvme_warnings_of(&facts[0], &target),
                (reliability_degraded, read_only)
            );
            logctx.cleanup_successful();
        }
    }

    #[test]
    fn no_case_without_nvme_warning() {
        let (logctx, mut collection, _zpools) =
            setup("disk_no_case_without_nvme_warning");
        let in_service = in_service_disks_in_inventory(&collection);
        let mut disks = in_service.iter();
        // One disk has a reading with no warnings, and another has no
        // reading at all. Neither is evidence of a problem.
        let healthy = disks.next().unwrap().clone();
        let unread = disks.next().unwrap().clone();
        set_nvme_health(
            &mut collection,
            &healthy,
            Ok(nvme_health(false, false)),
        );
        set_nvme_health(
            &mut collection,
            &unread,
            Err(NvmeHealthError {
                kind: NvmeHealthErrorKind::LogPageReadFailed,
                message: "test read failure".to_string(),
            }),
        );
        let input = build_input(collection, None, in_service);

        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let facts = disk_facts(&sitrep, false);
        assert!(facts.is_empty(), "no Disk cases expected, got: {facts:?}");
        logctx.cleanup_successful();
    }

    #[test]
    fn nvme_fact_stable_when_warnings_unchanged() {
        let (logctx, mut collection, _zpools) = setup("disk_nvme_fact_stable");
        let in_service = in_service_disks_in_inventory(&collection);
        let target = in_service.iter().next().unwrap().clone();
        set_nvme_health(&mut collection, &target, Ok(nvme_health(false, true)));
        let parent =
            make_parent_with_nvme_case(&collection, &target, false, true);
        let parent_fact_id = sole_disk_fact_id(&parent);

        let input = build_input(collection, Some(parent), in_service);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let facts = disk_facts(&sitrep, true);
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].fact.metadata.id, parent_fact_id);
        logctx.cleanup_successful();
    }

    #[test]
    fn nvme_fact_rotates_when_warnings_change() {
        let (logctx, mut collection, _zpools) = setup("disk_nvme_fact_rotates");
        let in_service = in_service_disks_in_inventory(&collection);
        let target = in_service.iter().next().unwrap().clone();
        // The parent saw only `read_only`; now both warnings are raised.
        set_nvme_health(&mut collection, &target, Ok(nvme_health(true, true)));
        let parent =
            make_parent_with_nvme_case(&collection, &target, false, true);
        let parent_fact_id = sole_disk_fact_id(&parent);
        let parent_case_id = parent.cases.iter().next().unwrap().id;

        let input = build_input(collection, Some(parent), in_service);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let facts = disk_facts(&sitrep, true);
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].case.id, parent_case_id);
        assert_ne!(facts[0].fact.metadata.id, parent_fact_id);
        assert_eq!(nvme_warnings_of(&facts[0], &target), (true, true));
        logctx.cleanup_successful();
    }

    #[test]
    fn closes_when_nvme_warnings_clear() {
        let (logctx, mut collection, _zpools) =
            setup("disk_close_nvme_warnings_clear");
        let in_service = in_service_disks_in_inventory(&collection);
        let target = in_service.iter().next().unwrap().clone();
        set_nvme_health(
            &mut collection,
            &target,
            Ok(nvme_health(false, false)),
        );
        let parent =
            make_parent_with_nvme_case(&collection, &target, true, false);

        let input = build_input(collection, Some(parent), in_service);
        let (sitrep, report) = run_analyze(&logctx.log, &input);
        let all = disk_facts(&sitrep, false);
        assert_eq!(all.len(), 1);
        assert!(
            !all[0].case.is_open(),
            "case should be closed when the NVMe warnings clear",
        );
        let report_str = format!("{}", report.display_multiline(0));
        assert!(
            report_str.contains("NVMe health warnings on disk"),
            "close comment should call out the recovery cause, got: \
             {report_str}",
        );
        logctx.cleanup_successful();
    }

    #[test]
    fn keeps_nvme_case_open_without_a_reading() {
        // Sled-agent reported an error in place of the disk's health. The
        // case should stay open with its fact: a missing reading is not a
        // recovery signal.
        let (logctx, mut collection, _zpools) =
            setup("disk_keep_nvme_case_open_without_reading");
        let in_service = in_service_disks_in_inventory(&collection);
        let target = in_service.iter().next().unwrap().clone();
        set_nvme_health(
            &mut collection,
            &target,
            Err(NvmeHealthError {
                kind: NvmeHealthErrorKind::NotPolled,
                message: "test unpolled disk".to_string(),
            }),
        );
        let parent =
            make_parent_with_nvme_case(&collection, &target, true, false);
        let parent_fact_id = sole_disk_fact_id(&parent);

        let input = build_input(collection, Some(parent), in_service);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let facts = disk_facts(&sitrep, true);
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].fact.metadata.id, parent_fact_id);
        logctx.cleanup_successful();
    }

    #[test]
    fn zpool_and_nvme_problems_share_a_case() {
        let (logctx, mut collection, _zpools) =
            setup("disk_zpool_and_nvme_share_case");
        let in_service = in_service_disks_in_inventory(&collection);
        let target = in_service.iter().next().unwrap().clone();
        set_health(&mut collection, target.zpool_id, ZpoolHealth::Degraded);
        set_nvme_health(&mut collection, &target, Ok(nvme_health(true, false)));

        // Both problems appear at once: one case, with a fact for each.
        let input = build_input(collection.clone(), None, in_service.clone());
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let facts = disk_facts(&sitrep, true);
        assert_eq!(facts.len(), 2);
        assert_eq!(facts[0].case.id, facts[1].case.id);
        let nvme_fact = facts
            .iter()
            .find(|f| matches!(f.disk_fact, DiskFact::NvmeHealthWarning(_)))
            .expect("an NvmeHealthWarning fact");
        assert_eq!(nvme_warnings_of(nvme_fact, &target), (true, false));
        assert!(
            facts
                .iter()
                .any(|f| matches!(f.disk_fact, DiskFact::ZpoolUnhealthy(_)))
        );

        // The zpool is degraded while the parent case only knows about the
        // NVMe warning: the zpool fact joins the existing case.
        let parent =
            make_parent_with_nvme_case(&collection, &target, true, false);
        let parent_case_id = parent.cases.iter().next().unwrap().id;
        let parent_fact_id = sole_disk_fact_id(&parent);
        let input = build_input(
            collection.clone(),
            Some(parent.clone()),
            in_service.clone(),
        );
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let facts = disk_facts(&sitrep, true);
        assert_eq!(facts.len(), 2);
        assert!(facts.iter().all(|f| f.case.id == parent_case_id));
        assert!(facts.iter().any(|f| f.fact.metadata.id == parent_fact_id));

        // The NVMe warning clears while the zpool is still degraded: the
        // case stays open, carrying only a zpool fact.
        set_nvme_health(
            &mut collection,
            &target,
            Ok(nvme_health(false, false)),
        );
        let input = build_input(collection, Some(parent), in_service);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let facts = disk_facts(&sitrep, true);
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].case.id, parent_case_id);
        assert!(matches!(facts[0].disk_fact, DiskFact::ZpoolUnhealthy(_)));
        logctx.cleanup_successful();
    }

    #[test]
    fn zpool_recovery_leaves_nvme_fact_on_open_case() {
        // The parent case has both kinds of fact, and the zpool is back to
        // Online. Whether the NVMe warning is still raised or the disk has
        // no reading at all, the case stays open with only its NVMe fact.
        for nvme_reading in [
            Ok(nvme_health(true, false)),
            Err(NvmeHealthError {
                kind: NvmeHealthErrorKind::NotPolled,
                message: "test unpolled disk".to_string(),
            }),
        ] {
            let (logctx, mut collection, _zpools) =
                setup("disk_zpool_recovery_leaves_nvme_fact");
            let in_service = in_service_disks_in_inventory(&collection);
            let target = in_service.iter().next().unwrap().clone();
            set_nvme_health(&mut collection, &target, nvme_reading);
            let parent =
                make_parent_with_zpool_and_nvme_case(&collection, &target);
            let parent_case_id = parent.cases.iter().next().unwrap().id;

            let input = build_input(collection, Some(parent), in_service);
            let (sitrep, _report) = run_analyze(&logctx.log, &input);
            let facts = disk_facts(&sitrep, true);
            assert_eq!(facts.len(), 1);
            assert_eq!(facts[0].case.id, parent_case_id);
            assert_eq!(nvme_warnings_of(&facts[0], &target), (true, false));
            logctx.cleanup_successful();
        }
    }

    #[test]
    fn closes_when_zpool_and_nvme_both_recover() {
        let (logctx, mut collection, _zpools) =
            setup("disk_close_zpool_and_nvme_recover");
        let in_service = in_service_disks_in_inventory(&collection);
        let target = in_service.iter().next().unwrap().clone();
        set_nvme_health(
            &mut collection,
            &target,
            Ok(nvme_health(false, false)),
        );
        let parent = make_parent_with_zpool_and_nvme_case(&collection, &target);

        let input = build_input(collection, Some(parent), in_service);
        let (sitrep, report) = run_analyze(&logctx.log, &input);
        assert!(disk_facts(&sitrep, true).is_empty());
        let report_str = format!("{}", report.display_multiline(0));
        assert!(
            report_str.contains("back to Online; NVMe health warnings on disk"),
            "close comment should call out both recoveries, got: \
             {report_str}",
        );
        logctx.cleanup_successful();
    }
}
