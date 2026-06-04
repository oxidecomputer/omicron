// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk diagnosis engine.

use crate::SitrepBuilder;
use crate::analysis_input::Input;
use iddqd::{IdOrdItem, IdOrdMap, id_upcast};
use nexus_types::fm::DiagnosisEngineKind;
use nexus_types::fm::{DiskFact, ZpoolUnhealthyFactPayload};
use nexus_types::inventory::ZpoolHealth;
use omicron_uuid_kinds::{CaseUuid, FactUuid, PhysicalDiskUuid, ZpoolUuid};
use std::collections::BTreeMap;

/// A [`DiskFact::ZpoolUnhealthy`] payload paired with the `FactUuid` it
/// lives under. Used to build in-memory indices over facts during
/// analysis; not serialized.
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

/// Per-case summary built from a case's facts. Each Disk case is about a
/// single physical disk; every fact on the case must reference that disk.
struct ParentCaseSummary {
    /// The physical disk this case is about.
    physical_disk_id: PhysicalDiskUuid,
    /// All `ZpoolUnhealthy` facts on this case. Normally one; pathological
    /// cases may have multiple. Regardless, the diagnosis engine keeps all of
    /// them.
    unhealthy_facts: IdOrdMap<ZpoolUnhealthyFact>,
}

pub(super) fn analyze(
    input: &Input,
    builder: &mut SitrepBuilder<'_>,
) -> anyhow::Result<()> {
    let inv_collection_id = input.inventory().id;
    let inv_time_done = input.inventory().time_done;

    // Index every zpool we observed in this inventory, so we can distinguish
    // "saw it, it's Online" from "didn't see it at all" when looking up by
    // an in-service disk's zpool below.
    let observed_health: BTreeMap<ZpoolUuid, ZpoolHealth> = input
        .inventory()
        .sled_agents
        .iter()
        .flat_map(|sa| sa.zpools.iter())
        .map(|z| (z.id, z.health))
        .collect();

    // The current health snapshot for every in-service disk, keyed by
    // physical_disk_id. Absence from this index is a positive signal that
    // the control plane has moved on from the disk (expungement /
    // decommissioning); see prepare_inputs in
    // nexus/src/app/background/tasks/fm_analysis.rs.
    let in_service_health: IdOrdMap<DiskHealthSnapshot> = input
        .in_service_disks()
        .iter()
        .map(|d| DiskHealthSnapshot {
            physical_disk_id: d.physical_disk_id,
            zpool_id: d.zpool_id,
            zpool_health: observed_health.get(&d.zpool_id).copied(),
        })
        .collect();

    // Index parent-forwarded Disk cases from the input — the state copied
    // from the parent sitrep.
    // Every case is about one physical disk; we derive the disk
    // from its facts. Skip (with a warning) any case we can't safely
    // interpret.
    let parent_cases: BTreeMap<CaseUuid, ParentCaseSummary> = input
        .open_cases()
        .iter()
        .filter(|c| c.metadata.de == DiagnosisEngineKind::PhysicalDisk)
        .filter_map(|c| {
            let case_id = c.id;
            let mut unhealthy_facts: IdOrdMap<ZpoolUnhealthyFact> =
                IdOrdMap::new();
            let mut case_disk_id: Option<PhysicalDiskUuid> = None;
            for fact in c.facts.iter() {
                // Every fact on a physical-disk case must carry a
                // physical-disk payload. A foreign payload is a data-model
                // violation; skip the whole case rather than acting on
                // inconsistent state.
                let Some(disk_fact) = fact.payload.as_physical_disk() else {
                    slog::warn!(
                        &builder.log,
                        "skipping Disk case: fact payload does not belong to \
                         the physical-disk diagnosis engine";
                        "case_id" => %case_id,
                        "fact_id" => %fact.id,
                    );
                    return None;
                };
                match disk_fact {
                    DiskFact::ZpoolUnhealthy(payload) => {
                        let payload = *payload;
                        let disk_id = *case_disk_id
                            .get_or_insert(payload.physical_disk_id);
                        if disk_id != payload.physical_disk_id {
                            slog::warn!(
                                &builder.log,
                                "skipping Disk case: facts reference \
                                 different physical disks (1 expected)";
                                "case_id" => %case_id,
                                "expected_physical_disk_id" => %disk_id,
                                "fact_physical_disk_id" =>
                                    %payload.physical_disk_id,
                            );
                            return None;
                        }
                        unhealthy_facts
                            .insert_unique(ZpoolUnhealthyFact {
                                fact_id: fact.id,
                                payload,
                            })
                            .expect("fact ids are unique within a case");
                    }
                }
            }
            let Some(physical_disk_id) = case_disk_id else {
                slog::warn!(
                    &builder.log,
                    "skipping Disk case with no facts; cannot derive disk id";
                    "case_id" => %case_id,
                );
                return None;
            };
            Some((
                case_id,
                ParentCaseSummary { physical_disk_id, unhealthy_facts },
            ))
        })
        .collect();

    // For each parent case, decide what to do based on its disk's current
    // state:
    //  - disk no longer in service → close the case (expungement)
    //  - disk's zpool back to Online → close the case (recovery)
    //  - disk still unhealthy → drop any facts whose recorded health no
    //    longer matches; the matching loop below will re-add a fresh fact
    //  - disk in service but absent from inventory → leave alone (absence
    //    is NOT a recovery signal: sled could be powered off, or
    //    inventory could be lossy)
    for (case_id, summary) in &parent_cases {
        let mut case_mut = builder
            .cases
            .case_mut(case_id)
            .expect("case_id came from iterating builder.cases");
        match in_service_health.get(&summary.physical_disk_id) {
            None => {
                case_mut.close(format!(
                    "disk {} no longer in service",
                    summary.physical_disk_id,
                ));
            }
            Some(snap) if snap.zpool_health == Some(ZpoolHealth::Online) => {
                case_mut
                    .close(format!("zpool {} back to Online", snap.zpool_id,));
            }
            Some(snap) => {
                let Some(current_health) = snap.zpool_health else {
                    continue;
                };
                for fact_ref in summary.unhealthy_facts.iter() {
                    if fact_ref.payload.last_seen_health != current_health {
                        case_mut.remove_fact(fact_ref.fact_id);
                    }
                }
            }
        }
    }

    // For each currently-faulty in-service disk: ensure a case exists
    // (reusing the parent-forwarded one for this disk if any) and add a
    // fresh fact if one with this exact health isn't already present.
    for disk in in_service_health.iter() {
        let Some(current_health) = disk.zpool_health else {
            continue;
        };
        if current_health == ZpoolHealth::Online {
            continue;
        }

        let parent_for_disk =
            parent_cases.iter().find_map(|(case_id, summary)| {
                if summary.physical_disk_id == disk.physical_disk_id {
                    Some((*case_id, summary))
                } else {
                    None
                }
            });

        let case_id_for_fact = match parent_for_disk {
            // Parent case already has an accurate fact — fully covered.
            Some((_, summary))
                if summary
                    .unhealthy_facts
                    .iter()
                    .any(|f| f.payload.last_seen_health == current_health) =>
            {
                continue;
            }
            // Parent case exists; its stale facts were removed above.
            // Refresh under the same case.
            Some((case_id, _)) => case_id,
            // No parent case for this disk — open one.
            None => {
                let mut new_case =
                    builder.cases.open_case(DiagnosisEngineKind::PhysicalDisk);
                new_case.set_comment(format!(
                    "physical disk {} unhealthy",
                    disk.physical_disk_id,
                ));
                new_case.id
            }
        };

        builder
            .cases
            .case_mut(&case_id_for_fact)
            .expect("case_id came from this fn")
            .add_fact(
                DiskFact::ZpoolUnhealthy(ZpoolUnhealthyFactPayload {
                    physical_disk_id: disk.physical_disk_id,
                    zpool_id: disk.zpool_id,
                    last_seen_health: current_health,
                    observed_in_inv: inv_collection_id,
                    time_observed: inv_time_done,
                }),
                format!("zpool {} health={current_health}", disk.zpool_id,),
            );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis_input::Input;
    use crate::builder::{SitrepBuilder, SitrepBuilderRng};
    use crate::test_util::FmTest;
    use chrono::Utc;
    use iddqd::IdOrdMap;
    use nexus_types::external_api::physical_disk::PhysicalDiskKind;
    use nexus_types::fm::{self, Sitrep, SitrepVersion};
    use nexus_types::in_service_disk::InServiceDisk;
    use nexus_types::inventory;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::{
        OmicronZoneUuid, PhysicalDiskUuid, SitrepUuid, SledUuid,
    };
    use std::sync::Arc;

    /// Synthesize a synthetic in-service disk set from a list of zpool IDs.
    /// Each zpool gets its own fresh `PhysicalDiskUuid` and dummy identity
    /// facts — tests in this module only care about the zpool dimension.
    fn mk_in_service(
        zpool_ids: impl IntoIterator<Item = ZpoolUuid>,
    ) -> IdOrdMap<InServiceDisk> {
        zpool_ids
            .into_iter()
            .map(|zpool_id| InServiceDisk {
                physical_disk_id: PhysicalDiskUuid::new_v4(),
                zpool_id,
                sled_id: SledUuid::new_v4(),
                vendor: "test-vendor".to_string(),
                serial: format!("test-serial-{zpool_id}"),
                model: "test-model".to_string(),
                variant: PhysicalDiskKind::U2,
            })
            .collect()
    }

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

    /// Set the zpool with `zpool_id` to `health`, panicking if not found.
    fn set_health(
        collection: &mut inventory::Collection,
        zpool_id: ZpoolUuid,
        health: ZpoolHealth,
    ) {
        for mut sa in collection.sled_agents.iter_mut() {
            for z in sa.zpools.iter_mut() {
                if z.id == zpool_id {
                    z.health = health;
                    return;
                }
            }
        }
        panic!("zpool {zpool_id} not found in collection");
    }

    /// Build an `Input` from a collection, an optional parent sitrep, and a
    /// pre-built set of in-service disks.
    fn build_input(
        collection: inventory::Collection,
        parent_sitrep: Option<Sitrep>,
        in_service: IdOrdMap<InServiceDisk>,
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
        let builder = Input::builder(
            parent,
            Arc::new(collection),
            Arc::new(in_service),
            Arc::new(IdOrdMap::new()),
        )
        .expect("input builder should accept fresh inventory");
        let (input, _report) = builder.build();
        input
    }

    /// Run `disk::analyze` over an input and return the resulting Sitrep
    /// along with the analysis report (whose log entries the close-comment
    /// assertions in `closes_*` tests inspect).
    fn run_analyze(
        log: &slog::Logger,
        input: &Input,
    ) -> (Sitrep, fm::analysis_reports::AnalysisReport) {
        let mut builder = SitrepBuilder::new_with_rng(
            log,
            input,
            SitrepBuilderRng::from_seed("disk-analyze"),
        );
        analyze(input, &mut builder).expect("analyze ok");
        builder.build(OmicronZoneUuid::new_v4(), Utc::now())
    }

    fn make_parent_with_disk_case(
        parent_sitrep_id: SitrepUuid,
        inv_collection_id: omicron_uuid_kinds::CollectionUuid,
        physical_disk_id: PhysicalDiskUuid,
        zpool_id: ZpoolUuid,
    ) -> Sitrep {
        let mut cases = iddqd::IdOrdMap::new();
        let case_id = omicron_uuid_kinds::CaseUuid::new_v4();
        let mut facts = iddqd::IdOrdMap::new();
        facts
            .insert_unique(fm::case::Fact {
                id: omicron_uuid_kinds::FactUuid::new_v4(),
                created_sitrep_id: parent_sitrep_id,
                payload: DiskFact::ZpoolUnhealthy(ZpoolUnhealthyFactPayload {
                    physical_disk_id,
                    zpool_id,
                    last_seen_health: ZpoolHealth::Degraded,
                    observed_in_inv: inv_collection_id,
                    time_observed: Utc::now(),
                })
                .into(),
                comment: format!("zpool {zpool_id} degraded"),
            })
            .unwrap();
        cases
            .insert_unique(fm::Case {
                id: case_id,
                metadata: fm::case::Metadata {
                    created_sitrep_id: parent_sitrep_id,
                    closed_sitrep_id: None,
                    de: DiagnosisEngineKind::PhysicalDisk,
                    comment: format!("zpool {zpool_id} degraded"),
                },
                ereports: Default::default(),
                alerts_requested: Default::default(),
                support_bundles_requested: Default::default(),
                facts,
            })
            .unwrap();
        Sitrep {
            metadata: fm::SitrepMetadata {
                id: parent_sitrep_id,
                inv_collection_id,
                creator_id: OmicronZoneUuid::new_v4(),
                parent_sitrep_id: None,
                time_created: Utc::now(),
                next_inv_min_time_started: Utc::now(),
                comment: String::new(),
            },
            cases,
            ereports_by_id: Default::default(),
        }
    }

    /// Helper: collect (case, fact, DiskFact) triples for every fact on a
    /// physical-disk case in a sitrep. Optionally filtered to open cases
    /// only.
    fn disk_facts(
        sitrep: &Sitrep,
        open_only: bool,
    ) -> Vec<(&fm::Case, &fm::case::Fact, DiskFact)> {
        sitrep
            .cases
            .iter()
            .filter(|c| c.metadata.de == DiagnosisEngineKind::PhysicalDisk)
            .filter(|c| !open_only || c.is_open())
            .flat_map(|c| {
                c.facts.iter().filter_map(move |f| {
                    f.payload.as_physical_disk().map(|d| (c, f, d.clone()))
                })
            })
            .collect()
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
        match &facts[0].2 {
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
        match &open_cases[0].2 {
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
            !all[0].0.is_open(),
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
        // Target is NOT in-service in this sitrep (just expunged).
        let in_service = mk_in_service(zpools.iter().copied().skip(1));
        // Target isn't in the in-service set; fabricate a stable PhysicalDiskUuid.
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
            !all[0].0.is_open(),
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
            all[0].0.is_open(),
            "case should remain open when its zpool is absent from the \
             current inventory collection (sled could be down or inventory \
             is lossy)",
        );
        logctx.cleanup_successful();
    }

    /// A parent Disk case with zero facts has no derivable disk ID, so
    /// the diagnosis engine leaves it alone (carried forward unchanged).
    #[test]
    fn empty_case_is_left_open() {
        let (logctx, collection, _zpools) = setup("disk_empty_case_left_open");
        let in_service = mk_in_service(std::iter::empty());

        let parent_sitrep_id = SitrepUuid::new_v4();
        let empty_case_id = omicron_uuid_kinds::CaseUuid::new_v4();
        let mut parent_cases = iddqd::IdOrdMap::new();
        parent_cases
            .insert_unique(fm::Case {
                id: empty_case_id,
                metadata: fm::case::Metadata {
                    created_sitrep_id: parent_sitrep_id,
                    closed_sitrep_id: None,
                    de: DiagnosisEngineKind::PhysicalDisk,
                    comment: "an open case with no facts".to_string(),
                },
                ereports: Default::default(),
                alerts_requested: Default::default(),
                support_bundles_requested: Default::default(),
                facts: Default::default(),
            })
            .unwrap();
        let parent = Sitrep {
            metadata: fm::SitrepMetadata {
                id: parent_sitrep_id,
                inv_collection_id: collection.id,
                creator_id: OmicronZoneUuid::new_v4(),
                parent_sitrep_id: None,
                time_created: Utc::now(),
                next_inv_min_time_started: Utc::now(),
                comment: String::new(),
            },
            cases: parent_cases,
            ereports_by_id: Default::default(),
        };

        let input = build_input(collection, Some(parent), in_service);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);

        let case = sitrep
            .cases
            .iter()
            .find(|c| c.id == empty_case_id)
            .expect("empty case should still be in the output sitrep");
        assert!(
            case.is_open(),
            "empty case should be left open (no disk to verify)",
        );
        logctx.cleanup_successful();
    }

    /// When the parent sitrep's fact content matches the diagnosis engine's current
    /// observation, the fact carries forward with the same UUID — no
    /// remove-and-readd churn.
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
        let parent_fact_id = parent
            .cases
            .iter()
            .find(|c| c.metadata.de == DiagnosisEngineKind::PhysicalDisk)
            .expect("parent should have one Disk case")
            .facts
            .iter()
            .next()
            .expect("parent case should have one fact")
            .id;

        let input = build_input(collection, Some(parent), in_service);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let open = disk_facts(&sitrep, true);
        assert_eq!(open.len(), 1, "expected exactly one open Disk fact");
        assert_eq!(
            open[0].1.id, parent_fact_id,
            "fact UUID should be stable across sitreps when the \
             observation hasn't changed",
        );
        match &open[0].2 {
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
    /// still unhealthy — just with a different value.
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
        let parent_fact_id = parent
            .cases
            .iter()
            .find(|c| c.metadata.de == DiagnosisEngineKind::PhysicalDisk)
            .expect("parent should have one Disk case")
            .facts
            .iter()
            .next()
            .expect("parent case should have one fact")
            .id;

        let input = build_input(collection, Some(parent), in_service);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let open = disk_facts(&sitrep, true);
        assert_eq!(
            open.len(),
            1,
            "expected exactly one open Disk fact (the refreshed one)",
        );
        assert_ne!(
            open[0].1.id, parent_fact_id,
            "fact UUID should rotate because last_seen_health changed",
        );
        match &open[0].2 {
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
        // forward — only the fact rotated.
        assert!(open[0].0.is_open());
        logctx.cleanup_successful();
    }
}
