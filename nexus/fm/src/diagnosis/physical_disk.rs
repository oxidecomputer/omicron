// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk diagnosis engine.

use crate::SitrepBuilder;
use iddqd::{BiHashItem, BiHashMap, IdOrdItem, IdOrdMap, bi_upcast, id_upcast};
use nexus_types::fm;
use nexus_types::fm::DiagnosisEngineKind;
use nexus_types::fm::{DiskFact, ZpoolUnhealthyFactPayload};
use nexus_types::inventory::ZpoolHealth;
use omicron_uuid_kinds::{CaseUuid, FactUuid, PhysicalDiskUuid, ZpoolUuid};
use std::collections::BTreeMap;

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

/// One in-service disk paired with the current observed health of its zpool.
/// `zpool_health` is `None` when the disk's zpool was not seen in the current
/// inventory (e.g., sled down, lossy collection).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DiskHealthSnapshot {
    physical_disk_id: PhysicalDiskUuid,
    zpool_id: ZpoolUuid,
    zpool_health: Option<ZpoolHealth>,
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
    /// All `ZpoolUnhealthy` facts on this case. Normally one; pathological
    /// cases may have multiple. Regardless, the diagnosis engine keeps all of
    /// them.
    unhealthy_facts: IdOrdMap<ZpoolUnhealthyFact>,
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
    #[error(
        "fact {fact_id} does not belong to the physical-disk diagnosis engine"
    )]
    ForeignFactPayload { fact_id: FactUuid },
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
    let mut case_disk_id: Option<PhysicalDiskUuid> = None;
    for fact in case.facts.iter() {
        // Every fact on a physical-disk case must carry a physical-disk
        // payload; a foreign payload is a data-model violation.
        let Some(disk_fact) = fact.payload.as_physical_disk() else {
            return Err(UninterpretableCase::ForeignFactPayload {
                fact_id: fact.metadata.id,
            });
        };
        match disk_fact {
            DiskFact::ZpoolUnhealthy(payload) => {
                let payload = *payload;
                let disk_id =
                    *case_disk_id.get_or_insert(payload.physical_disk_id);
                if disk_id != payload.physical_disk_id {
                    return Err(UninterpretableCase::DisagreeingDisks {
                        expected: disk_id,
                        found: payload.physical_disk_id,
                    });
                }
                unhealthy_facts
                    .insert_unique(ZpoolUnhealthyFact {
                        fact_id: fact.metadata.id,
                        payload,
                    })
                    .expect("fact ids are unique within a case");
            }
        }
    }
    let Some(physical_disk_id) = case_disk_id else {
        return Err(UninterpretableCase::NoFacts);
    };
    Ok(ParsedDiskCase { case_id: case.id, physical_disk_id, unhealthy_facts })
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

    // Close the surviving parent case for any disk that has recovered or left
    // service. A still-faulty disk's facts are reconciled in the next loop; a
    // disk in service but absent from this inventory is left alone (absence is
    // NOT a recovery signal: the sled could be powered off, or the collection
    // could be lossy).
    for parsed_case in cases.iter() {
        let mut case_mut = builder
            .cases
            .case_mut(&parsed_case.case_id)
            .expect("case_id came from builder's open cases");
        match in_service_health.get(&parsed_case.physical_disk_id) {
            None => {
                case_mut.close(format!(
                    "disk {} no longer in service",
                    parsed_case.physical_disk_id,
                ));
            }
            Some(snap) if snap.zpool_health == Some(ZpoolHealth::Online) => {
                case_mut
                    .close(format!("zpool {} back to Online", snap.zpool_id));
            }
            // Faulty or absent from inventory: leave open; the next loop
            // reconciles the faulty ones and leaves the absent ones untouched.
            Some(_) => {}
        }
    }

    // For each currently-faulty in-service disk, ensure its case carries
    // exactly one fact matching the current observation: reuse the
    // parent-forwarded case if any (dropping any stale facts), otherwise open
    // a fresh case.
    for disk in in_service_health.iter() {
        // Only modify "facts" for disk cases where the zpool health is known,
        // and it's not "Online" already.
        //
        // For all other cases, the zpool is healthy, so we don't have
        // a case anymore.
        let Some(current_health) = disk.zpool_health else {
            continue;
        };
        if current_health == ZpoolHealth::Online {
            continue;
        }

        let mut case_mut = match cases.get2(&disk.physical_disk_id) {
            Some(parsed_case) => {
                let mut case_mut = builder
                    .cases
                    .case_mut(&parsed_case.case_id)
                    .expect("case_id came from builder's open cases");
                let mut has_match = false;
                // Although we currently expect only one unhealthy fact for
                // each case, the schema does allow for more than one.
                // We iterate over all facts of these type to invalidate
                // all the facts which are out-of-date.
                for fact in parsed_case.unhealthy_facts.iter() {
                    if fact.payload.zpool_id == disk.zpool_id
                        && fact.payload.last_seen_health == current_health
                    {
                        // An accurate fact is already present; keep it.
                        has_match = true;
                    } else {
                        // Stale observation; drop it.
                        case_mut.remove_fact(
                            fact.fact_id,
                            "stale zpool observation, superseded by current \
                             inventory",
                        );
                    }
                }
                if has_match {
                    continue;
                }
                case_mut
            }
            None => builder.cases.open_case(DiagnosisEngineKind::PhysicalDisk),
        };

        case_mut.add_fact(
            DiskFact::ZpoolUnhealthy(ZpoolUnhealthyFactPayload {
                physical_disk_id: disk.physical_disk_id,
                zpool_id: disk.zpool_id,
                last_seen_health: current_health,
                observed_in_inv: inv_collection_id,
                time_observed: inv_time_done,
            }),
            format!("zpool {} health={current_health}", disk.zpool_id),
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{
        FmTest, build_input, make_degraded_fact, make_disk_case,
        make_parent_sitrep, mk_in_service, run_analyze, set_health,
    };
    use nexus_types::fm::{self, Sitrep};
    use nexus_types::in_service_disk::InServiceDisk;
    use nexus_types::inventory;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::{PhysicalDiskUuid, SitrepUuid};

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
        let fact = make_degraded_fact(
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
        let in_service = mk_in_service(zpools.iter().copied());
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
        }
        logctx.cleanup_successful();
    }

    #[test]
    fn skips_degraded_when_expunged() {
        let (logctx, mut collection, zpools) = setup("disk_skip_expunged");
        let target = zpools[0];
        set_health(&mut collection, target, ZpoolHealth::Faulted);
        // target is *not* in the in-service set.
        let in_service = mk_in_service(zpools.iter().copied().skip(1));
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
        let in_service = mk_in_service(zpools.iter().copied());
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
        }
        logctx.cleanup_successful();
    }

    #[test]
    fn closes_on_recovery() {
        let (logctx, collection, zpools) = setup("disk_close_on_recovery");
        let target = zpools[0];
        // The example system reports zpools as Online by default.
        let in_service = mk_in_service(zpools.iter().copied());
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
        let in_service = mk_in_service(zpools.iter().copied().skip(1));
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
        let in_service = mk_in_service(zpools.iter().copied().chain([phantom]));
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
        let in_service = mk_in_service(std::iter::empty());

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
        let in_service = mk_in_service(zpools.iter().copied());
        let target_disk_id = disk_id_for(&in_service, target);
        let parent_id = SitrepUuid::new_v4();

        let id_a = omicron_uuid_kinds::CaseUuid::new_v4();
        let id_b = omicron_uuid_kinds::CaseUuid::new_v4();
        let (kept_id, dup_id) =
            if id_a < id_b { (id_a, id_b) } else { (id_b, id_a) };
        let kept_fact = make_degraded_fact(
            parent_id,
            collection.id,
            target_disk_id,
            target,
        );
        let kept_fact_id = kept_fact.metadata.id;
        let dup_fact = make_degraded_fact(
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
        let in_service = mk_in_service(zpools.iter().copied());
        let target_disk_id = disk_id_for(&in_service, target);
        let parent_id = SitrepUuid::new_v4();

        // One fact about the target disk, one about an unrelated disk: the
        // case is self-contradictory.
        let corrupt_case_id = omicron_uuid_kinds::CaseUuid::new_v4();
        let corrupt_case = make_disk_case(
            corrupt_case_id,
            parent_id,
            [
                make_degraded_fact(
                    parent_id,
                    collection.id,
                    target_disk_id,
                    target,
                ),
                make_degraded_fact(
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
        let in_service = mk_in_service(zpools.iter().copied());
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
        let in_service = mk_in_service(zpools.iter().copied());
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
        let in_service = mk_in_service(zpools.iter().copied());
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
        }
        assert!(open[0].case.is_open());
        logctx.cleanup_successful();
    }
}
