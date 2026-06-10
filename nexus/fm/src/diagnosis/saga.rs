// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Saga diagnosis engine.
//!
//! Opens a case (keyed by `saga_id`) for any non-terminal saga that is either
//! *not making progress* (no node event recorded for a while) or *orphaned*
//! (owned by a Nexus that is no longer of the current generation). These are
//! two independent fact kinds; a saga's case may carry either or both. A case
//! is closed once the saga reaches a terminal state (it drops out of the set
//! of non-terminal sagas the preparation phase observed).
//!
//! See omicron#10530 for motivation.

use crate::SitrepBuilder;
use chrono::{DateTime, TimeDelta, Utc};
use nexus_types::fm::DiagnosisEngineKind;
use nexus_types::fm::{
    SagaFact, SagaNotProgressingFactPayload, SagaOwnerNotCurrentFactPayload,
};
use nexus_types::observed_saga::ObservedSaga;
use omicron_uuid_kinds::{CaseUuid, FactUuid};
use std::collections::BTreeMap;

/// A saga is flagged as "not progressing" once it has recorded no node event
/// for at least this long. This is a wall-clock, cadence-independent quantity
/// (`reference_time - last_event_time`), deliberately not a count of analysis
/// passes.
const STALE_SAGA_THRESHOLD: TimeDelta = TimeDelta::minutes(30);

/// Per-case view of a parent saga case, built from its facts. Every fact on a
/// saga case is about the same `saga_id`; at most one fact of each kind is
/// expected.
struct ParentSagaCase {
    saga_id: steno::SagaId,
    not_progressing: Option<(FactUuid, SagaNotProgressingFactPayload)>,
    owner_not_current: Option<(FactUuid, SagaOwnerNotCurrentFactPayload)>,
}

pub(super) fn analyze(builder: &mut SitrepBuilder<'_>) -> anyhow::Result<()> {
    // The input borrow has lifetime 'a, not a borrow of `builder`, so we may
    // hold it while mutating the builder below.
    let input = builder.input();
    // Reference "now" for staleness. We use the inventory collection's
    // completion time (rather than `Utc::now()`) so analysis is deterministic
    // and reproducible in tests, matching the physical-disk engine's use of
    // inventory timestamps.
    let reference_time = input.inventory().time_done;
    let observed = input.observed_sagas();

    // Index parent-forwarded Saga cases by case ID, and maintain a saga_id ->
    // case_id index for the second pass. Every case is about one saga, derived
    // from its facts; skip (with a warning) any case we can't interpret.
    let mut parent_cases: BTreeMap<CaseUuid, ParentSagaCase> = BTreeMap::new();
    let mut case_for_saga: BTreeMap<steno::SagaId, CaseUuid> = BTreeMap::new();
    'cases: for case in input
        .open_cases()
        .iter()
        .filter(|c| c.metadata.de == DiagnosisEngineKind::Saga)
    {
        let case_id = case.id;
        let mut saga_id: Option<steno::SagaId> = None;
        let mut not_progressing = None;
        let mut owner_not_current = None;
        for fact in case.facts.iter() {
            let Some(saga_fact) = fact.payload.as_saga() else {
                slog::warn!(
                    &builder.log,
                    "skipping Saga case: fact payload does not belong to the \
                     saga diagnosis engine";
                    "case_id" => %case_id,
                    "fact_id" => %fact.id,
                );
                continue 'cases;
            };
            let this_saga = saga_fact.saga_id();
            if *saga_id.get_or_insert(this_saga) != this_saga {
                slog::warn!(
                    &builder.log,
                    "skipping Saga case: facts reference different sagas";
                    "case_id" => %case_id,
                );
                continue 'cases;
            }
            match saga_fact {
                SagaFact::NotProgressing(p) => {
                    not_progressing = Some((fact.id, p.clone()));
                }
                SagaFact::OwnerNotCurrentGeneration(p) => {
                    owner_not_current = Some((fact.id, p.clone()));
                }
            }
        }
        let Some(saga_id) = saga_id else {
            slog::warn!(
                &builder.log,
                "skipping Saga case with no facts; cannot derive saga id";
                "case_id" => %case_id,
            );
            continue 'cases;
        };
        parent_cases.insert(
            case_id,
            ParentSagaCase { saga_id, not_progressing, owner_not_current },
        );
        case_for_saga.insert(saga_id, case_id);
    }

    // First pass: for each parent case, close it if its saga has reached a
    // terminal state (no longer observed) or has fully recovered (no
    // condition holds anymore), otherwise drop any facts whose recorded
    // contents no longer match the current observation. The second pass
    // re-adds a fresh fact if the condition still holds.
    for (case_id, summary) in &parent_cases {
        let mut case_mut = builder.cases.case_mut(case_id).expect(
            "builder.cases is seeded from the open cases of builder.input(), \
             which is where this case_id came from",
        );
        let Some(obs) = observed.get(&summary.saga_id) else {
            case_mut.close(format!(
                "saga {} reached a terminal state",
                summary.saga_id,
            ));
            continue;
        };
        let desired_np = desired_not_progressing(obs, reference_time);
        let desired_owner = desired_owner_not_current(obs);
        // A case is an episode of a problem, not a dossier on the saga: when
        // no condition holds anymore, the episode is over and the case
        // closes. Its facts stay attached as the record of why it existed;
        // they age out with the case once it stops being copied forward. If
        // the saga becomes a problem again later, a fresh case opens.
        if desired_np.is_none() && desired_owner.is_none() {
            case_mut.close(format!(
                "saga {} is progressing under a current owner again",
                summary.saga_id,
            ));
            continue;
        }
        if let Some((fact_id, payload)) = &summary.not_progressing {
            if desired_np.as_ref() != Some(payload) {
                case_mut.remove_fact(*fact_id);
            }
        }
        if let Some((fact_id, payload)) = &summary.owner_not_current {
            if desired_owner.as_ref() != Some(payload) {
                case_mut.remove_fact(*fact_id);
            }
        }
    }

    // Second pass: for each observed saga with a problem, ensure a case exists
    // (reusing the parent-forwarded one if any) and add a fresh fact for each
    // condition that isn't already represented by a matching, carried-forward
    // fact.
    for obs in observed.iter() {
        let desired_np = desired_not_progressing(obs, reference_time);
        let desired_owner = desired_owner_not_current(obs);
        if desired_np.is_none() && desired_owner.is_none() {
            continue;
        }

        let parent = case_for_saga.get(&obs.saga_id).and_then(|case_id| {
            parent_cases.get(case_id).map(|s| (*case_id, s))
        });

        // A carried-forward fact already covers a condition only if its
        // recorded payload exactly matches what we'd emit now (otherwise the
        // first pass removed it).
        let np_already = matches!(
            (&desired_np, parent.and_then(|(_, s)| s.not_progressing.as_ref())),
            (Some(want), Some((_, have))) if want == have
        );
        let owner_already = matches!(
            (
                &desired_owner,
                parent.and_then(|(_, s)| s.owner_not_current.as_ref()),
            ),
            (Some(want), Some((_, have))) if want == have
        );

        let case_id = match parent {
            Some((case_id, _)) => case_id,
            None => {
                let mut new_case =
                    builder.cases.open_case(DiagnosisEngineKind::Saga);
                new_case.set_comment(format!(
                    "saga {} ({}) needs attention",
                    obs.saga_id, obs.saga_name,
                ));
                new_case.id
            }
        };

        if let Some(payload) = desired_np {
            if !np_already {
                let staleness = reference_time
                    .signed_duration_since(payload.last_event_time);
                let comment = format!(
                    "no saga node event in {}",
                    omicron_common::format_time_delta(staleness),
                );
                builder
                    .cases
                    .case_mut(&case_id)
                    .expect("case_id came from this fn")
                    .add_fact(SagaFact::NotProgressing(payload), comment);
            }
        }
        if let Some(payload) = desired_owner {
            if !owner_already {
                let comment = format!(
                    "owned by non-current Nexus {} ({:?})",
                    payload.current_sec, payload.orphan_reason,
                );
                builder
                    .cases
                    .case_mut(&case_id)
                    .expect("case_id came from this fn")
                    .add_fact(
                        SagaFact::OwnerNotCurrentGeneration(payload),
                        comment,
                    );
            }
        }
    }

    Ok(())
}

/// The `NotProgressing` fact this saga should carry now, if any. When a saga
/// has recorded no node event at all, its creation time stands in for the
/// last-progress timestamp (a saga that has existed past the threshold without
/// recording a single step is itself stuck at the start).
fn desired_not_progressing(
    obs: &ObservedSaga,
    reference_time: DateTime<Utc>,
) -> Option<SagaNotProgressingFactPayload> {
    let last_progress = obs.last_event_time.unwrap_or(obs.time_created);
    if reference_time.signed_duration_since(last_progress)
        > STALE_SAGA_THRESHOLD
    {
        Some(SagaNotProgressingFactPayload {
            saga_id: obs.saga_id,
            saga_name: obs.saga_name.clone(),
            saga_state: obs.saga_state,
            time_created: obs.time_created,
            last_event_time: last_progress,
        })
    } else {
        None
    }
}

/// The `OwnerNotCurrentGeneration` fact this saga should carry now, if any.
/// Only fires when the saga has a `current_sec` whose owner is orphaned
/// (quiesced or expunged); a saga with no current SEC is between adoptions and
/// is not treated as orphaned.
fn desired_owner_not_current(
    obs: &ObservedSaga,
) -> Option<SagaOwnerNotCurrentFactPayload> {
    let reason = obs.owner_state?.orphaned_reason()?;
    let current_sec = obs.current_sec?;
    Some(SagaOwnerNotCurrentFactPayload {
        saga_id: obs.saga_id,
        saga_name: obs.saga_name.clone(),
        current_sec,
        orphan_reason: reason,
        adopt_generation: obs.adopt_generation,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis_input::Input;
    use crate::builder::{SitrepBuilder, SitrepBuilderRng};
    use crate::test_util::FmTest;
    use chrono::Utc;
    use iddqd::IdOrdMap;
    use nexus_types::fm::{self, Sitrep, SitrepVersion};
    use nexus_types::inventory;
    use nexus_types::observed_saga::{
        OrphanedReason, SagaOwnerState, SagaProgressState,
    };
    use omicron_common::api::external::Generation;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::{OmicronZoneUuid, SitrepUuid};
    use std::sync::Arc;

    fn saga_id(n: u128) -> steno::SagaId {
        steno::SagaId(uuid::Uuid::from_u128(n))
    }

    /// Build a synthetic example collection (only used here for its
    /// `time_done`, which is the staleness reference time).
    fn setup(
        test_name: &'static str,
    ) -> (dev::LogContext, inventory::Collection) {
        let (fm_test, logctx) = FmTest::new_with_logctx(test_name);
        let (example, _bp) = fm_test.system_builder.build();
        (logctx, example.collection)
    }

    /// An observed saga with sensible defaults; callers override the fields
    /// each test cares about.
    fn mk_observed(
        id: steno::SagaId,
        last_event_time: Option<chrono::DateTime<Utc>>,
        current_sec: Option<OmicronZoneUuid>,
        owner_state: Option<SagaOwnerState>,
    ) -> ObservedSaga {
        ObservedSaga {
            saga_id: id,
            saga_name: "test-saga".to_string(),
            saga_state: SagaProgressState::Unwinding,
            time_created: Utc::now() - TimeDelta::days(1),
            current_sec,
            adopt_generation: Generation::new(),
            last_event_time,
            owner_state,
        }
    }

    fn observed_map(
        sagas: impl IntoIterator<Item = ObservedSaga>,
    ) -> IdOrdMap<ObservedSaga> {
        sagas.into_iter().collect()
    }

    fn build_input(
        collection: inventory::Collection,
        parent_sitrep: Option<Sitrep>,
        observed: IdOrdMap<ObservedSaga>,
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
            Arc::new(IdOrdMap::new()),
            Arc::new(observed),
        )
        .expect("input builder should accept fresh inventory");
        builder.build().0
    }

    fn run_analyze(
        log: &slog::Logger,
        input: &Input,
    ) -> (Sitrep, fm::analysis_reports::AnalysisReport) {
        let mut builder = SitrepBuilder::new_with_rng(
            log,
            input,
            SitrepBuilderRng::from_seed("saga-analyze"),
        );
        analyze(&mut builder).expect("analyze ok");
        builder.build(OmicronZoneUuid::new_v4(), Utc::now())
    }

    /// Collect every saga fact in the sitrep, optionally only on open cases.
    fn saga_facts(
        sitrep: &Sitrep,
        open_only: bool,
    ) -> Vec<(fm::case::Fact, SagaFact)> {
        sitrep
            .cases
            .iter()
            .filter(|c| c.metadata.de == DiagnosisEngineKind::Saga)
            .filter(|c| !open_only || c.is_open())
            .flat_map(|c| {
                c.facts.iter().filter_map(|f| {
                    f.payload.as_saga().map(|s| (f.clone(), s.clone()))
                })
            })
            .collect()
    }

    fn make_parent_with_saga_case(
        parent_sitrep_id: SitrepUuid,
        inv_collection_id: omicron_uuid_kinds::CollectionUuid,
        fact_payloads: impl IntoIterator<Item = SagaFact>,
    ) -> Sitrep {
        let mut facts = IdOrdMap::new();
        for fact_payload in fact_payloads {
            facts
                .insert_unique(fm::case::Fact {
                    id: omicron_uuid_kinds::FactUuid::new_v4(),
                    created_sitrep_id: parent_sitrep_id,
                    payload: fact_payload.into(),
                    comment: "parent saga fact".to_string(),
                })
                .unwrap();
        }
        let mut cases = IdOrdMap::new();
        cases
            .insert_unique(fm::Case {
                id: omicron_uuid_kinds::CaseUuid::new_v4(),
                metadata: fm::case::Metadata {
                    created_sitrep_id: parent_sitrep_id,
                    closed_sitrep_id: None,
                    de: DiagnosisEngineKind::Saga,
                    comment: "parent saga case".to_string(),
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

    #[test]
    fn opens_not_progressing_when_stale() {
        let (logctx, collection) = setup("saga_open_not_progressing");
        let stale = collection.time_done
            - (STALE_SAGA_THRESHOLD + TimeDelta::minutes(1));
        let id = saga_id(1);
        let observed = observed_map([mk_observed(id, Some(stale), None, None)]);
        let input = build_input(collection, None, observed);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);

        let facts = saga_facts(&sitrep, true);
        assert_eq!(facts.len(), 1);
        match &facts[0].1 {
            SagaFact::NotProgressing(p) => {
                assert_eq!(p.saga_id, id);
                assert_eq!(p.last_event_time, stale);
            }
            other => panic!("expected NotProgressing, got {other:?}"),
        }
        logctx.cleanup_successful();
    }

    #[test]
    fn no_case_when_progress_recent() {
        let (logctx, collection) = setup("saga_no_case_when_recent");
        let recent = collection.time_done - TimeDelta::minutes(1);
        let observed =
            observed_map([mk_observed(saga_id(1), Some(recent), None, None)]);
        let input = build_input(collection, None, observed);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        assert!(
            saga_facts(&sitrep, false).is_empty(),
            "a saga making recent progress should not be flagged",
        );
        logctx.cleanup_successful();
    }

    #[test]
    fn opens_owner_not_current_when_quiesced() {
        let (logctx, collection) = setup("saga_open_owner_quiesced");
        // Recent progress, so the only problem is the orphaned owner.
        let recent = collection.time_done - TimeDelta::minutes(1);
        let sec = OmicronZoneUuid::new_v4();
        let observed = observed_map([mk_observed(
            saga_id(1),
            Some(recent),
            Some(sec),
            Some(SagaOwnerState::Quiesced),
        )]);
        let input = build_input(collection, None, observed);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);

        let facts = saga_facts(&sitrep, true);
        assert_eq!(facts.len(), 1);
        match &facts[0].1 {
            SagaFact::OwnerNotCurrentGeneration(p) => {
                assert_eq!(p.current_sec, sec);
                assert_eq!(p.orphan_reason, OrphanedReason::Quiesced);
            }
            other => {
                panic!("expected OwnerNotCurrentGeneration, got {other:?}")
            }
        }
        logctx.cleanup_successful();
    }

    #[test]
    fn active_owner_not_flagged() {
        let (logctx, collection) = setup("saga_active_owner_not_flagged");
        let recent = collection.time_done - TimeDelta::minutes(1);
        let observed = observed_map([mk_observed(
            saga_id(1),
            Some(recent),
            Some(OmicronZoneUuid::new_v4()),
            Some(SagaOwnerState::Active),
        )]);
        let input = build_input(collection, None, observed);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        assert!(saga_facts(&sitrep, false).is_empty());
        logctx.cleanup_successful();
    }

    #[test]
    fn notyet_owner_not_flagged() {
        let (logctx, collection) = setup("saga_notyet_owner_not_flagged");
        let recent = collection.time_done - TimeDelta::minutes(1);
        let observed = observed_map([mk_observed(
            saga_id(1),
            Some(recent),
            Some(OmicronZoneUuid::new_v4()),
            Some(SagaOwnerState::NotYet),
        )]);
        let input = build_input(collection, None, observed);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        assert!(saga_facts(&sitrep, false).is_empty());
        logctx.cleanup_successful();
    }

    #[test]
    fn both_facts_on_one_case() {
        let (logctx, collection) = setup("saga_both_facts");
        let stale = collection.time_done
            - (STALE_SAGA_THRESHOLD + TimeDelta::minutes(1));
        let sec = OmicronZoneUuid::new_v4();
        let observed = observed_map([mk_observed(
            saga_id(1),
            Some(stale),
            Some(sec),
            Some(SagaOwnerState::Absent),
        )]);
        let input = build_input(collection, None, observed);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);

        let facts = saga_facts(&sitrep, true);
        assert_eq!(facts.len(), 2, "expected both fact kinds on one case");
        // Both facts must be on the same case (one case per saga).
        let open_cases: Vec<_> = sitrep
            .cases
            .iter()
            .filter(|c| {
                c.metadata.de == DiagnosisEngineKind::Saga && c.is_open()
            })
            .collect();
        assert_eq!(open_cases.len(), 1);
        assert!(
            facts.iter().any(|(_, f)| matches!(f, SagaFact::NotProgressing(_)))
        );
        assert!(facts.iter().any(|(_, f)| matches!(
            f,
            SagaFact::OwnerNotCurrentGeneration(p)
                if p.orphan_reason == OrphanedReason::Expunged
        )));
        logctx.cleanup_successful();
    }

    #[test]
    fn closes_on_terminal() {
        let (logctx, collection) = setup("saga_closes_on_terminal");
        let id = saga_id(1);
        let stale = collection.time_done
            - (STALE_SAGA_THRESHOLD + TimeDelta::minutes(1));
        let parent_id = SitrepUuid::new_v4();
        let inv_id = collection.id;
        let parent = make_parent_with_saga_case(
            parent_id,
            inv_id,
            [SagaFact::NotProgressing(SagaNotProgressingFactPayload {
                saga_id: id,
                saga_name: "test-saga".to_string(),
                saga_state: SagaProgressState::Unwinding,
                time_created: Utc::now() - TimeDelta::days(1),
                last_event_time: stale,
            })],
        );
        // The saga is no longer observed (it reached a terminal state).
        let input = build_input(collection, Some(parent), observed_map([]));
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let all = saga_facts(&sitrep, false);
        assert_eq!(all.len(), 1, "the fact carries forward on the closed case");
        let case = sitrep
            .cases
            .iter()
            .find(|c| c.metadata.de == DiagnosisEngineKind::Saga)
            .expect("saga case should still be present");
        assert!(!case.is_open(), "case should be closed when saga terminates");
        logctx.cleanup_successful();
    }

    #[test]
    fn fact_uuid_stable_when_unchanged() {
        let (logctx, collection) = setup("saga_fact_stable");
        let id = saga_id(1);
        let stale = collection.time_done
            - (STALE_SAGA_THRESHOLD + TimeDelta::minutes(5));
        let payload = SagaNotProgressingFactPayload {
            saga_id: id,
            saga_name: "test-saga".to_string(),
            saga_state: SagaProgressState::Unwinding,
            time_created: Utc::now() - TimeDelta::days(1),
            last_event_time: stale,
        };
        let parent_id = SitrepUuid::new_v4();
        let inv_id = collection.id;
        let parent = make_parent_with_saga_case(
            parent_id,
            inv_id,
            [SagaFact::NotProgressing(payload.clone())],
        );
        let parent_fact_id =
            parent.cases.iter().next().unwrap().facts.iter().next().unwrap().id;
        // Observed saga matches the parent fact exactly (same last_event_time,
        // same created time, same state).
        let observed = observed_map([ObservedSaga {
            saga_id: id,
            saga_name: "test-saga".to_string(),
            saga_state: SagaProgressState::Unwinding,
            time_created: payload.time_created,
            current_sec: None,
            adopt_generation: Generation::new(),
            last_event_time: Some(stale),
            owner_state: None,
        }]);
        let input = build_input(collection, Some(parent), observed);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let facts = saga_facts(&sitrep, true);
        assert_eq!(facts.len(), 1);
        assert_eq!(
            facts[0].0.id, parent_fact_id,
            "fact UUID should be stable when the observation is unchanged",
        );
        logctx.cleanup_successful();
    }

    #[test]
    fn fact_uuid_rotates_when_last_event_changes() {
        let (logctx, collection) = setup("saga_fact_rotates");
        let id = saga_id(1);
        let old =
            collection.time_done - (STALE_SAGA_THRESHOLD + TimeDelta::hours(2));
        let new = collection.time_done
            - (STALE_SAGA_THRESHOLD + TimeDelta::minutes(1));
        let time_created = Utc::now() - TimeDelta::days(1);
        let inv_id = collection.id;
        let parent = make_parent_with_saga_case(
            SitrepUuid::new_v4(),
            inv_id,
            [SagaFact::NotProgressing(SagaNotProgressingFactPayload {
                saga_id: id,
                saga_name: "test-saga".to_string(),
                saga_state: SagaProgressState::Unwinding,
                time_created,
                last_event_time: old,
            })],
        );
        let parent_fact_id =
            parent.cases.iter().next().unwrap().facts.iter().next().unwrap().id;
        // Still stale, but last_event_time advanced.
        let observed = observed_map([ObservedSaga {
            saga_id: id,
            saga_name: "test-saga".to_string(),
            saga_state: SagaProgressState::Unwinding,
            time_created,
            current_sec: None,
            adopt_generation: Generation::new(),
            last_event_time: Some(new),
            owner_state: None,
        }]);
        let input = build_input(collection, Some(parent), observed);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let facts = saga_facts(&sitrep, true);
        assert_eq!(facts.len(), 1);
        assert_ne!(
            facts[0].0.id, parent_fact_id,
            "fact UUID should rotate when last_event_time changes",
        );
        match &facts[0].1 {
            SagaFact::NotProgressing(p) => {
                assert_eq!(p.last_event_time, new)
            }
            other => panic!("expected NotProgressing, got {other:?}"),
        }
        logctx.cleanup_successful();
    }

    /// A saga that was flagged as not progressing but has since resumed
    /// recording node events: the episode is over, so the case closes. Its
    /// fact stays attached as the record of why the case existed.
    #[test]
    fn closes_on_progress_resumed() {
        let (logctx, collection) = setup("saga_closes_on_progress_resumed");
        let id = saga_id(1);
        let stale =
            collection.time_done - (STALE_SAGA_THRESHOLD + TimeDelta::hours(1));
        let recent = collection.time_done - TimeDelta::minutes(1);
        let parent = make_parent_with_saga_case(
            SitrepUuid::new_v4(),
            collection.id,
            [SagaFact::NotProgressing(SagaNotProgressingFactPayload {
                saga_id: id,
                saga_name: "test-saga".to_string(),
                saga_state: SagaProgressState::Unwinding,
                time_created: Utc::now() - TimeDelta::days(1),
                last_event_time: stale,
            })],
        );
        let observed =
            observed_map([mk_observed(id, Some(recent), None, None)]);
        let input = build_input(collection, Some(parent), observed);
        let (sitrep, report) = run_analyze(&logctx.log, &input);

        let case = sitrep
            .cases
            .iter()
            .find(|c| c.metadata.de == DiagnosisEngineKind::Saga)
            .expect("saga case should be present in the closing sitrep");
        assert!(
            !case.is_open(),
            "case should close when the saga resumes progress",
        );
        assert_eq!(
            case.facts.len(),
            1,
            "the stale fact stays attached to the closed case as evidence",
        );
        let report_str = format!("{}", report.display_multiline(0));
        assert!(
            report_str.contains("progressing under a current owner again"),
            "close comment should call out the recovery, got: {report_str}",
        );
        logctx.cleanup_successful();
    }

    /// A saga whose case was opened because its owner was quiesced, since
    /// re-adopted by an active Nexus: the case closes, fact attached.
    #[test]
    fn closes_on_owner_readopted() {
        let (logctx, collection) = setup("saga_closes_on_owner_readopted");
        let id = saga_id(1);
        let recent = collection.time_done - TimeDelta::minutes(1);
        let parent = make_parent_with_saga_case(
            SitrepUuid::new_v4(),
            collection.id,
            [SagaFact::OwnerNotCurrentGeneration(
                SagaOwnerNotCurrentFactPayload {
                    saga_id: id,
                    saga_name: "test-saga".to_string(),
                    current_sec: OmicronZoneUuid::new_v4(),
                    orphan_reason: OrphanedReason::Quiesced,
                    adopt_generation: Generation::new(),
                },
            )],
        );
        // The saga has been re-adopted by an active Nexus and is making
        // progress.
        let observed = observed_map([mk_observed(
            id,
            Some(recent),
            Some(OmicronZoneUuid::new_v4()),
            Some(SagaOwnerState::Active),
        )]);
        let input = build_input(collection, Some(parent), observed);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);

        let case = sitrep
            .cases
            .iter()
            .find(|c| c.metadata.de == DiagnosisEngineKind::Saga)
            .expect("saga case should be present in the closing sitrep");
        assert!(
            !case.is_open(),
            "case should close when the saga is re-adopted by a current Nexus",
        );
        assert_eq!(case.facts.len(), 1);
        logctx.cleanup_successful();
    }

    /// One condition clears (progress resumes) while the other persists
    /// (owner still quiesced): the case stays open, the cleared condition's
    /// fact is removed, and the persisting fact carries forward with a
    /// stable UUID.
    #[test]
    fn partial_recovery_keeps_case_open() {
        let (logctx, collection) = setup("saga_partial_recovery");
        let id = saga_id(1);
        let stale =
            collection.time_done - (STALE_SAGA_THRESHOLD + TimeDelta::hours(1));
        let recent = collection.time_done - TimeDelta::minutes(1);
        let sec = OmicronZoneUuid::new_v4();
        let owner_payload = SagaOwnerNotCurrentFactPayload {
            saga_id: id,
            saga_name: "test-saga".to_string(),
            current_sec: sec,
            orphan_reason: OrphanedReason::Quiesced,
            adopt_generation: Generation::new(),
        };
        let parent = make_parent_with_saga_case(
            SitrepUuid::new_v4(),
            collection.id,
            [
                SagaFact::NotProgressing(SagaNotProgressingFactPayload {
                    saga_id: id,
                    saga_name: "test-saga".to_string(),
                    saga_state: SagaProgressState::Unwinding,
                    time_created: Utc::now() - TimeDelta::days(1),
                    last_event_time: stale,
                }),
                SagaFact::OwnerNotCurrentGeneration(owner_payload.clone()),
            ],
        );
        let parent_owner_fact_id = parent
            .cases
            .iter()
            .next()
            .unwrap()
            .facts
            .iter()
            .find_map(|f| match f.payload.as_saga() {
                Some(SagaFact::OwnerNotCurrentGeneration(_)) => Some(f.id),
                _ => None,
            })
            .expect("parent case should have an owner fact");
        // Progress has resumed, but the owner is still quiesced with the
        // same SEC and adopt generation, so the owner fact still matches.
        let observed = observed_map([mk_observed(
            id,
            Some(recent),
            Some(sec),
            Some(SagaOwnerState::Quiesced),
        )]);
        let input = build_input(collection, Some(parent), observed);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);

        let facts = saga_facts(&sitrep, true);
        assert_eq!(
            facts.len(),
            1,
            "only the owner fact should remain on the open case",
        );
        assert_eq!(
            facts[0].0.id, parent_owner_fact_id,
            "the persisting fact carries forward with a stable UUID",
        );
        assert!(matches!(
            &facts[0].1,
            SagaFact::OwnerNotCurrentGeneration(p) if p.current_sec == sec
        ));
        logctx.cleanup_successful();
    }
}
