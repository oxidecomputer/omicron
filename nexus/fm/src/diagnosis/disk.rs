// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk diagnosis engine.

use crate::SitrepBuilder;
use crate::analysis_input::Input;
use nexus_types::fm::DiagnosisEngineKind;
use nexus_types::in_service_disk::InServiceDisk;
use nexus_types::inventory::ZpoolHealth;
use omicron_uuid_kinds::{CaseFactUuid, CaseUuid, ZpoolUuid};
use serde::{Deserialize, Serialize};
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;

/// Per-fact state for the disk diagnosis engine, serialized into the
/// `fm_case_fact.payload` JSONB column. Other diagnosis engines must not
/// inspect or modify this; shared FM code treats it as opaque bytes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DiskFact {
    /// The zpool's most recently observed health is non-`Online`.
    ZpoolUnhealthy { zpool_id: ZpoolUuid, last_seen_health: ZpoolHealth },
}

/// Per-case summary built from a case's parent-forwarded facts.
struct ParentCaseSummary {
    /// IDs of `ZpoolUnhealthy` facts on this case, grouped by their
    /// recorded zpool. There can be multiple facts in pathological cases
    /// (e.g., two zpool ids on the same case after a hand-edit); the
    /// diagnoser keeps all of them in its accounting.
    zpool_unhealthy: BTreeMap<ZpoolUuid, Vec<(CaseFactUuid, ZpoolHealth)>>,
}

pub(super) fn analyze(
    input: &Input,
    builder: &mut SitrepBuilder<'_>,
) -> anyhow::Result<()> {
    // The disk DE's primary key today is `zpool_id`, so we build a local
    // index keyed by zpool. Future variants of `DiskFact` are welcome to
    // derive their own secondary indices (e.g., by `sled_id` for FMD).
    let in_service_by_zpool: BTreeMap<ZpoolUuid, &InServiceDisk> =
        input.in_service_disks().iter().map(|d| (d.zpool_id, d)).collect();

    // Index every zpool we observed in this inventory by ID, so we can
    // distinguish "saw it, it's Online" from "didn't see it at all" below.
    let observed: BTreeMap<ZpoolUuid, ZpoolHealth> = input
        .inventory()
        .sled_agents
        .iter()
        .flat_map(|sa| sa.zpools.iter())
        .map(|z| (z.id, z.health))
        .collect();

    // Currently-faulty, control-plane-managed zpools.
    //
    // Out-of-service zpools are intentionally ignored: a non-`Online` zpool
    // whose disk has been expunged is no longer the control plane's concern.
    let faulty: BTreeMap<ZpoolUuid, ZpoolHealth> = observed
        .iter()
        .filter(|(id, _)| in_service_by_zpool.contains_key(*id))
        .filter(|(_, h)| **h != ZpoolHealth::Online)
        .map(|(id, h)| (*id, *h))
        .collect();

    // Inspect parent-forwarded Disk cases from the input (i.e., the state
    // copied from the parent sitrep — *not* the in-progress builder, which
    // we will mutate below). Each case's facts are JSON blobs owned by this
    // engine; deserialize each one as DiskFact. Skip (with a warning) any
    // fact we can't read.
    let parent_summaries: BTreeMap<CaseUuid, ParentCaseSummary> = input
        .open_cases()
        .iter()
        .filter(|c| c.metadata.de == DiagnosisEngineKind::PhysicalDisk)
        .map(|c| {
            let case_id = c.id;
            let mut summary =
                ParentCaseSummary { zpool_unhealthy: BTreeMap::new() };
            for fact in c.facts.iter() {
                match fact.payload_to::<DiskFact>() {
                    Ok(DiskFact::ZpoolUnhealthy {
                        zpool_id,
                        last_seen_health,
                    }) => {
                        summary
                            .zpool_unhealthy
                            .entry(zpool_id)
                            .or_default()
                            .push((fact.id, last_seen_health));
                    }
                    Err(e) => {
                        slog::warn!(
                            &builder.log,
                            "skipping Disk case fact with unreadable \
                             payload; this run will not modify it";
                            "case_id" => %case_id,
                            "fact_id" => %fact.id,
                            "error" => InlineErrorChain::new(&*e).to_string(),
                        );
                    }
                }
            }
            (case_id, summary)
        })
        .collect();

    // Close any open Disk case whose entire set of (interpretable) facts
    // points at zpools that are now Online or expunged. Closed cases are not
    // copied forward, so their facts naturally drop with them.
    //
    // Absence from inventory is NOT a recovery signal: a sled could be
    // powered off, or inventory could be lossy.
    for (case_id, summary) in &parent_summaries {
        if summary.zpool_unhealthy.is_empty() {
            continue;
        }
        let any_still_unhealthy =
            summary.zpool_unhealthy.keys().any(|zpool_id| {
                in_service_by_zpool.contains_key(zpool_id)
                    && observed.get(zpool_id) != Some(&ZpoolHealth::Online)
            });
        if !any_still_unhealthy {
            builder
                .cases
                .case_mut(case_id)
                .expect("case_id came from iterating builder.cases")
                .close(
                    "all ZpoolUnhealthy facts have resolved (zpool back to \
                     Online, or disk no longer in service)",
                );
        }
    }

    // For each parent-forwarded ZpoolUnhealthy fact: if the recorded
    // last_seen_health no longer matches current observation, remove the
    // stale fact. We'll re-add a fresh one below with current data (new UUID).
    for (case_id, summary) in &parent_summaries {
        let mut case_mut = builder
            .cases
            .case_mut(case_id)
            .expect("case_id came from iterating builder.cases");
        if !case_mut.is_open() {
            continue;
        }
        for (zpool_id, facts) in &summary.zpool_unhealthy {
            let Some(current_health) = faulty.get(zpool_id) else {
                continue;
            };
            for (fact_id, last_seen_health) in facts {
                if *current_health != *last_seen_health {
                    case_mut.remove_fact(*fact_id);
                }
            }
        }
    }

    // For each currently-faulty in-service zpool: if there's no open case +
    // accurate fact already covering it, ensure a case exists (reuse the
    // parent-forwarded one for this zpool if any) and add a fresh fact.
    for (zpool_id, current_health) in faulty {
        let parent_for_zpool =
            parent_summaries.iter().find_map(|(case_id, summary)| {
                summary
                    .zpool_unhealthy
                    .get(&zpool_id)
                    .map(|facts| (*case_id, facts))
            });

        let case_id_for_fact = match parent_for_zpool {
            // Parent case already has an accurate fact — fully covered.
            Some((_, facts))
                if facts.iter().any(|(_, h)| *h == current_health) =>
            {
                continue;
            }
            // Parent case exists; its stale facts were removed above.
            // Refresh under the same case.
            Some((case_id, _)) => case_id,
            // No parent case for this zpool — open one.
            None => {
                let mut new_case =
                    builder.cases.open_case(DiagnosisEngineKind::PhysicalDisk);
                new_case.set_comment(format!("zpool {zpool_id} unhealthy"));
                new_case.id
            }
        };

        builder
            .cases
            .case_mut(&case_id_for_fact)
            .expect("case_id came from this fn")
            .add_fact(
                &DiskFact::ZpoolUnhealthy {
                    zpool_id,
                    last_seen_health: current_health,
                },
                format!("zpool {zpool_id} health={current_health}"),
            )?;
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
        let builder =
            Input::builder(parent, Arc::new(collection), Arc::new(in_service))
                .expect("input builder should accept fresh inventory");
        let (input, _report) = builder.build();
        input
    }

    /// Run `disk::analyze` over an input and return the resulting Sitrep.
    fn run_analyze(log: &slog::Logger, input: &Input) -> Sitrep {
        let mut builder = SitrepBuilder::new_with_rng(
            log,
            input,
            SitrepBuilderRng::from_seed("disk-analyze"),
        );
        analyze(input, &mut builder).expect("analyze ok");
        let (sitrep, _report) =
            builder.build(OmicronZoneUuid::new_v4(), Utc::now());
        sitrep
    }

    fn make_parent_with_disk_case(
        parent_sitrep_id: SitrepUuid,
        inv_collection_id: omicron_uuid_kinds::CollectionUuid,
        zpool_id: ZpoolUuid,
    ) -> Sitrep {
        let mut cases = iddqd::IdOrdMap::new();
        let case_id = omicron_uuid_kinds::CaseUuid::new_v4();
        let mut facts = iddqd::IdOrdMap::new();
        facts
            .insert_unique(fm::case::Fact {
                id: omicron_uuid_kinds::CaseFactUuid::new_v4(),
                created_sitrep_id: parent_sitrep_id,
                payload: serde_json::to_value(&DiskFact::ZpoolUnhealthy {
                    zpool_id,
                    last_seen_health: ZpoolHealth::Degraded,
                })
                .unwrap(),
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

    /// Helper: collect (case, fact) pairs in a sitrep where the fact parses
    /// as `DiskFact`. Optionally filtered to open cases only.
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
                    f.payload_to::<DiskFact>().ok().map(|d| (c, f, d))
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
        let input = build_input(collection, None, in_service);

        let sitrep = run_analyze(&logctx.log, &input);
        let facts = disk_facts(&sitrep, true);
        assert_eq!(facts.len(), 1);
        match &facts[0].2 {
            DiskFact::ZpoolUnhealthy { zpool_id, last_seen_health } => {
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

        let sitrep = run_analyze(&logctx.log, &input);
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
        let parent_id = SitrepUuid::new_v4();
        let parent =
            make_parent_with_disk_case(parent_id, collection.id, target);

        let input = build_input(collection, Some(parent), in_service);
        let sitrep = run_analyze(&logctx.log, &input);
        let open_cases = disk_facts(&sitrep, true);
        assert_eq!(open_cases.len(), 1);
        match &open_cases[0].2 {
            DiskFact::ZpoolUnhealthy { zpool_id, .. } => {
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
        let parent_id = SitrepUuid::new_v4();
        let parent =
            make_parent_with_disk_case(parent_id, collection.id, target);

        let input = build_input(collection, Some(parent), in_service);
        let sitrep = run_analyze(&logctx.log, &input);
        let all = disk_facts(&sitrep, false);
        assert_eq!(all.len(), 1);
        assert!(
            !all[0].0.is_open(),
            "case should be closed when zpool returns to Online",
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
        let parent_id = SitrepUuid::new_v4();
        let parent =
            make_parent_with_disk_case(parent_id, collection.id, target);

        let input = build_input(collection, Some(parent), in_service);
        let sitrep = run_analyze(&logctx.log, &input);
        let all = disk_facts(&sitrep, false);
        assert_eq!(all.len(), 1);
        assert!(
            !all[0].0.is_open(),
            "case should be closed when zpool's disk is expunged",
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
        let parent_id = SitrepUuid::new_v4();
        let parent =
            make_parent_with_disk_case(parent_id, collection.id, phantom);

        let input = build_input(collection, Some(parent), in_service);
        let sitrep = run_analyze(&logctx.log, &input);
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

    /// A parent Disk case carries a fact whose JSON `kind` doesn't match any
    /// variant we know. The diagnoser should leave that case alone (not
    /// touch its open/closed state, not mutate the fact, not count it
    /// toward dedup) and freely open a new case for a separate live signal.
    #[test]
    fn unreadable_fact_is_skipped() {
        let (logctx, mut collection, zpools) = setup("disk_unreadable_payload");
        // Live signal on zpools[0].
        let live_target = zpools[0];
        set_health(&mut collection, live_target, ZpoolHealth::Degraded);
        let in_service = mk_in_service(zpools.iter().copied());

        // Build a parent sitrep with one Disk case carrying a payload that
        // doesn't match any variant.
        let parent_sitrep_id = SitrepUuid::new_v4();
        let unreadable_case_id = omicron_uuid_kinds::CaseUuid::new_v4();
        let mut parent_cases = iddqd::IdOrdMap::new();
        let unreadable_fact_id = omicron_uuid_kinds::CaseFactUuid::new_v4();
        let unreadable_payload = serde_json::json!({
            "kind": "this_variant_does_not_exist",
            "mystery": "data",
        });
        let mut parent_facts = iddqd::IdOrdMap::new();
        parent_facts
            .insert_unique(fm::case::Fact {
                id: unreadable_fact_id,
                created_sitrep_id: parent_sitrep_id,
                payload: unreadable_payload.clone(),
                comment: "fact with payload from the future".to_string(),
            })
            .unwrap();
        parent_cases
            .insert_unique(fm::Case {
                id: unreadable_case_id,
                metadata: fm::case::Metadata {
                    created_sitrep_id: parent_sitrep_id,
                    closed_sitrep_id: None,
                    de: DiagnosisEngineKind::PhysicalDisk,
                    comment: "case with fact from the future".to_string(),
                },
                ereports: Default::default(),
                alerts_requested: Default::default(),
                support_bundles_requested: Default::default(),
                facts: parent_facts,
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
        let sitrep = run_analyze(&logctx.log, &input);

        // The unreadable case must still be present, still open, and its
        // fact unchanged.
        let unreadable = sitrep
            .cases
            .iter()
            .find(|c| c.id == unreadable_case_id)
            .expect("unreadable case should be copied forward");
        assert!(
            unreadable.is_open(),
            "unreadable case must not be closed by the diagnoser",
        );
        let kept_fact = unreadable
            .facts
            .iter()
            .find(|f| f.id == unreadable_fact_id)
            .expect("unreadable fact should be preserved");
        assert_eq!(kept_fact.payload, unreadable_payload);

        // A fresh ZpoolUnhealthy fact must still open for the live signal
        // — the unreadable case did not consume the dedup slot.
        let readable = disk_facts(&sitrep, true);
        assert_eq!(
            readable.len(),
            1,
            "expected one readable open ZpoolUnhealthy fact for the live \
             signal",
        );
        match &readable[0].2 {
            DiskFact::ZpoolUnhealthy { zpool_id, .. } => {
                assert_eq!(*zpool_id, live_target);
            }
        }
        logctx.cleanup_successful();
    }

    /// When the parent sitrep's fact content matches the diagnoser's current
    /// observation, the fact carries forward with the same UUID — no
    /// remove-and-readd churn.
    #[test]
    fn fact_uuid_stable_when_observation_unchanged() {
        let (logctx, mut collection, zpools) = setup("disk_fact_uuid_stable");
        let target = zpools[0];
        set_health(&mut collection, target, ZpoolHealth::Degraded);
        let in_service = mk_in_service(zpools.iter().copied());
        let parent_id = SitrepUuid::new_v4();
        let parent =
            make_parent_with_disk_case(parent_id, collection.id, target);
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
        let sitrep = run_analyze(&logctx.log, &input);
        let open = disk_facts(&sitrep, true);
        assert_eq!(open.len(), 1, "expected exactly one open Disk fact");
        assert_eq!(
            open[0].1.id, parent_fact_id,
            "fact UUID should be stable across sitreps when the \
             observation hasn't changed",
        );
        match &open[0].2 {
            DiskFact::ZpoolUnhealthy { zpool_id, last_seen_health } => {
                assert_eq!(*zpool_id, target);
                assert_eq!(*last_seen_health, ZpoolHealth::Degraded);
            }
        }
        logctx.cleanup_successful();
    }

    /// When the parent's fact recorded a different `last_seen_health` than
    /// what we observe now, the diagnoser removes the stale fact and emits
    /// a fresh one (new UUID). The case stays open because the zpool is
    /// still unhealthy — just with a different value.
    #[test]
    fn fact_uuid_rotates_when_observation_changes() {
        let (logctx, mut collection, zpools) = setup("disk_fact_uuid_rotates");
        let target = zpools[0];
        // Parent recorded Degraded; current inventory shows Faulted.
        set_health(&mut collection, target, ZpoolHealth::Faulted);
        let in_service = mk_in_service(zpools.iter().copied());
        let parent_id = SitrepUuid::new_v4();
        let parent =
            make_parent_with_disk_case(parent_id, collection.id, target);
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
        let sitrep = run_analyze(&logctx.log, &input);
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
            DiskFact::ZpoolUnhealthy { zpool_id, last_seen_health } => {
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
