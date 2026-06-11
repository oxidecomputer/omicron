// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Saga diagnosis engine.
//!
//! Opens a case (keyed by `saga_id`) for any unfinished saga that is
//! *not making progress* (no node event recorded for a while), *orphaned*
//! (owned by a Nexus that is no longer of the current generation), or
//! *abandoned* (Nexus permanently gave up on recovering it; see
//! omicron#10581 / RFD 555). A live saga's case may carry the first two
//! facts together; abandonment supersedes both. A case closes when its
//! saga completes or recovers, or, for an abandoned saga, only when the
//! saga row is removed from the database: abandonment is the beginning of
//! an escalation, not a resolution.
//!
//! The owner facts detect *stranded* sagas, not *wrongly-resumed* ones: if
//! an orphaned saga is re-adopted by a current-generation Nexus, the owner
//! fact clears, even though running a saga built by a different Nexus
//! version is itself dangerous. That risk is mitigated upstream: recovery
//! abandons sagas it cannot recover (omicron#10581), and saga quiesce
//! prevents cross-version adoption in the normal path.
//!
//! See omicron#10530 for motivation.

use crate::SitrepBuilder;
use chrono::{DateTime, TimeDelta, Utc};
use nexus_types::fm::DiagnosisEngineKind;
use nexus_types::fm::{
    SagaAbandonedFactPayload, SagaFact, SagaNotProgressingFactPayload,
    SagaOwnerNotCurrentFactPayload,
};
use nexus_types::observed_saga::{
    ObservedSaga, ObservedSagaState, SagaProgressState,
};
use omicron_uuid_kinds::{CaseUuid, FactUuid};
use std::collections::BTreeMap;

/// A saga is flagged as "not progressing" once it has recorded no node event
/// for at least this long. This is a wall-clock, cadence-independent quantity
/// (`reference_time - last_event_time`), deliberately not a count of analysis
/// passes.
const STALE_SAGA_THRESHOLD: TimeDelta = TimeDelta::minutes(30);

/// Per-case view of a parent saga case, built from its facts. Every fact on a
/// saga case is about the same `saga_id`, and a case carries at most one fact
/// of each kind.
struct ParentSagaCase {
    saga_id: steno::SagaId,
    /// The fact to consider when advancing the case: at most one per kind
    /// (the lowest fact UUID wins if a case pathologically carries several).
    not_progressing: Option<(FactUuid, SagaNotProgressingFactPayload)>,
    owner_not_current: Option<(FactUuid, SagaOwnerNotCurrentFactPayload)>,
    abandoned: Option<(FactUuid, SagaAbandonedFactPayload)>,
    /// Facts that should not exist: duplicates of a kind beyond the first.
    /// These carry no information the kept fact doesn't; they are removed
    /// unconditionally, regardless of what the observation says.
    duplicate_facts: Vec<FactUuid>,
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
        let mut not_progressing: Option<(
            FactUuid,
            SagaNotProgressingFactPayload,
        )> = None;
        let mut owner_not_current: Option<(
            FactUuid,
            SagaOwnerNotCurrentFactPayload,
        )> = None;
        let mut abandoned: Option<(FactUuid, SagaAbandonedFactPayload)> = None;
        let mut duplicate_facts = Vec::new();
        // `case.facts` iterates in fact UUID order, so the kept fact for
        // each kind is deterministically the one with the lowest UUID.
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
                    if not_progressing.is_none() {
                        not_progressing = Some((fact.id, p.clone()));
                    } else {
                        duplicate_facts.push(fact.id);
                    }
                }
                SagaFact::OwnerNotCurrentGeneration(p) => {
                    if owner_not_current.is_none() {
                        owner_not_current = Some((fact.id, p.clone()));
                    } else {
                        duplicate_facts.push(fact.id);
                    }
                }
                SagaFact::Abandoned(p) => {
                    if abandoned.is_none() {
                        abandoned = Some((fact.id, p.clone()));
                    } else {
                        duplicate_facts.push(fact.id);
                    }
                }
            }
        }
        if !duplicate_facts.is_empty() {
            slog::warn!(
                &builder.log,
                "Saga case has more than one fact of the same kind; \
                 the duplicates will be removed";
                "case_id" => %case_id,
                "duplicate_fact_ids" => ?duplicate_facts,
            );
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
            ParentSagaCase {
                saga_id,
                not_progressing,
                owner_not_current,
                abandoned,
                duplicate_facts,
            },
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
                "saga {} completed or was removed",
                summary.saga_id,
            ));
            continue;
        };
        let desired_np = desired_not_progressing(obs, reference_time);
        let desired_owner = desired_owner_not_current(obs);
        let desired_abandoned = desired_abandoned(obs);
        // A case is an episode of a problem, not a dossier on the saga: when
        // no condition holds anymore, the episode is over and the case
        // closes. Its facts stay attached as the record of why it existed;
        // they age out with the case once it stops being copied forward. If
        // the saga becomes a problem again later, a fresh case opens.
        //
        // An abandoned saga never reaches this close: `desired_abandoned`
        // holds until the saga row itself is removed (the `else` branch
        // above), keeping the case open while remediation is pending.
        if desired_np.is_none()
            && desired_owner.is_none()
            && desired_abandoned.is_none()
        {
            case_mut.close(format!(
                "saga {} is progressing under a current owner again",
                summary.saga_id,
            ));
            continue;
        }
        // Duplicate facts carry no information the kept facts don't; remove
        // them regardless of what the observation says.
        for fact_id in &summary.duplicate_facts {
            case_mut.remove_fact(*fact_id);
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
        if let Some((fact_id, payload)) = &summary.abandoned {
            if desired_abandoned.as_ref() != Some(payload) {
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
        let desired_abandoned = desired_abandoned(obs);
        if desired_np.is_none()
            && desired_owner.is_none()
            && desired_abandoned.is_none()
        {
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
        let abandoned_already = matches!(
            (&desired_abandoned, parent.and_then(|(_, s)| s.abandoned.as_ref())),
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
        if let Some(payload) = desired_abandoned {
            if !abandoned_already {
                // The payload is pure identity; the human-readable context
                // (which a promoted problem would otherwise look up from the
                // saga row) goes in the comment.
                let comment = format!(
                    "saga {} ({}) abandoned by Nexus; created at {}, last \
                     node event: {}",
                    obs.saga_id,
                    obs.saga_name,
                    obs.time_created,
                    obs.last_event_time
                        .map(|t| t.to_string())
                        .unwrap_or_else(|| "<none recorded>".to_string()),
                );
                builder
                    .cases
                    .case_mut(&case_id)
                    .expect("case_id came from this fn")
                    .add_fact(SagaFact::Abandoned(payload), comment);
            }
        }
    }

    Ok(())
}

/// The `NotProgressing` fact this saga should carry now, if any. When a saga
/// has recorded no node event at all, its creation time stands in for the
/// last-progress timestamp (a saga that has existed past the threshold without
/// recording a single step is itself stuck at the start).
///
/// An abandoned saga is never "not progressing": [`desired_abandoned`]
/// supersedes the live-saga conditions.
fn desired_not_progressing(
    obs: &ObservedSaga,
    reference_time: DateTime<Utc>,
) -> Option<SagaNotProgressingFactPayload> {
    let saga_state = match obs.saga_state {
        ObservedSagaState::Running => SagaProgressState::Running,
        ObservedSagaState::Unwinding => SagaProgressState::Unwinding,
        ObservedSagaState::Abandoned => return None,
    };
    let last_progress = obs.last_event_time.unwrap_or(obs.time_created);
    if reference_time.signed_duration_since(last_progress)
        > STALE_SAGA_THRESHOLD
    {
        Some(SagaNotProgressingFactPayload {
            saga_id: obs.saga_id,
            saga_state,
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
///
/// An abandoned saga has no meaningful owner (nobody will ever run it):
/// [`desired_abandoned`] supersedes the live-saga conditions.
fn desired_owner_not_current(
    obs: &ObservedSaga,
) -> Option<SagaOwnerNotCurrentFactPayload> {
    if obs.saga_state == ObservedSagaState::Abandoned {
        return None;
    }
    let reason = obs.owner_state?.orphaned_reason()?;
    let current_sec = obs.current_sec?;
    Some(SagaOwnerNotCurrentFactPayload {
        saga_id: obs.saga_id,
        current_sec,
        orphan_reason: reason,
    })
}

/// The `Abandoned` fact this saga should carry now, if any. The condition is
/// boolean, so the payload is pure identity and never rotates; the case stays
/// open, carrying this fact, until the saga row is removed from the database.
fn desired_abandoned(obs: &ObservedSaga) -> Option<SagaAbandonedFactPayload> {
    (obs.saga_state == ObservedSagaState::Abandoned)
        .then(|| SagaAbandonedFactPayload { saga_id: obs.saga_id })
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
            saga_state: ObservedSagaState::Unwinding,
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
        make_parent_with_saga_case_from_facts(
            parent_sitrep_id,
            inv_collection_id,
            facts,
        )
    }

    /// Like [`make_parent_with_saga_case`], but with caller-controlled
    /// `Fact`s (e.g., for tests that need specific fact UUIDs).
    fn make_parent_with_saga_case_from_facts(
        parent_sitrep_id: SitrepUuid,
        inv_collection_id: omicron_uuid_kinds::CollectionUuid,
        facts: IdOrdMap<fm::case::Fact>,
    ) -> Sitrep {
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
                saga_state: SagaProgressState::Unwinding,
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
            saga_state: SagaProgressState::Unwinding,
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
        // same state).
        let observed = observed_map([ObservedSaga {
            saga_id: id,
            saga_name: "test-saga".to_string(),
            saga_state: ObservedSagaState::Unwinding,
            time_created: Utc::now() - TimeDelta::days(1),
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
                saga_state: SagaProgressState::Unwinding,
                last_event_time: old,
            })],
        );
        let parent_fact_id =
            parent.cases.iter().next().unwrap().facts.iter().next().unwrap().id;
        // Still stale, but last_event_time advanced.
        let observed = observed_map([ObservedSaga {
            saga_id: id,
            saga_name: "test-saga".to_string(),
            saga_state: ObservedSagaState::Unwinding,
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
                saga_state: SagaProgressState::Unwinding,
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
                    current_sec: OmicronZoneUuid::new_v4(),
                    orphan_reason: OrphanedReason::Quiesced,
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
            current_sec: sec,
            orphan_reason: OrphanedReason::Quiesced,
        };
        let parent = make_parent_with_saga_case(
            SitrepUuid::new_v4(),
            collection.id,
            [
                SagaFact::NotProgressing(SagaNotProgressingFactPayload {
                    saga_id: id,
                    saga_state: SagaProgressState::Unwinding,
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

    fn fact_uuid(n: u128) -> FactUuid {
        use omicron_uuid_kinds::GenericUuid;
        FactUuid::from_untyped_uuid(uuid::Uuid::from_u128(n))
    }

    fn np_fact(
        id: FactUuid,
        created_sitrep_id: SitrepUuid,
        saga: steno::SagaId,
        last_event_time: chrono::DateTime<Utc>,
    ) -> fm::case::Fact {
        fm::case::Fact {
            id,
            created_sitrep_id,
            payload: SagaFact::NotProgressing(SagaNotProgressingFactPayload {
                saga_id: saga,
                saga_state: SagaProgressState::Unwinding,
                last_event_time,
            })
            .into(),
            comment: "parent saga fact".to_string(),
        }
    }

    /// A pathological parent case carrying two `NotProgressing` facts
    /// (which the engine never creates itself), where the kept fact (lowest
    /// UUID) matches the current observation: the duplicate is removed and
    /// the kept fact survives with its UUID intact.
    #[test]
    fn duplicate_fact_removed_when_kept_fact_matches() {
        let (logctx, collection) = setup("saga_dup_kept_matches");
        let id = saga_id(1);
        let time_created = collection.time_done - TimeDelta::days(1);
        let current = collection.time_done
            - (STALE_SAGA_THRESHOLD + TimeDelta::minutes(5));
        let old =
            collection.time_done - (STALE_SAGA_THRESHOLD + TimeDelta::hours(2));
        let parent_id = SitrepUuid::new_v4();
        let kept_id = fact_uuid(1);
        let dup_id = fact_uuid(2);
        let mut parent_facts = IdOrdMap::new();
        parent_facts
            .insert_unique(np_fact(kept_id, parent_id, id, current))
            .unwrap();
        parent_facts
            .insert_unique(np_fact(dup_id, parent_id, id, old))
            .unwrap();
        let parent = make_parent_with_saga_case_from_facts(
            parent_id,
            collection.id,
            parent_facts,
        );
        // Still stale; the observation matches the kept fact exactly.
        let observed = observed_map([ObservedSaga {
            saga_id: id,
            saga_name: "test-saga".to_string(),
            saga_state: ObservedSagaState::Unwinding,
            time_created,
            current_sec: None,
            adopt_generation: Generation::new(),
            last_event_time: Some(current),
            owner_state: None,
        }]);
        let input = build_input(collection, Some(parent), observed);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);

        let facts = saga_facts(&sitrep, true);
        assert_eq!(facts.len(), 1, "the duplicate fact should be removed");
        assert_eq!(
            facts[0].0.id, kept_id,
            "the kept fact matches the observation, so its UUID is stable",
        );
        logctx.cleanup_successful();
    }

    /// As above, but the kept fact (lowest UUID) is the stale one: both
    /// parent facts are removed (the kept one for mismatching, the
    /// duplicate unconditionally) and one fresh fact replaces them.
    #[test]
    fn duplicate_fact_removed_when_kept_fact_is_stale() {
        let (logctx, collection) = setup("saga_dup_kept_stale");
        let id = saga_id(1);
        let time_created = collection.time_done - TimeDelta::days(1);
        let current = collection.time_done
            - (STALE_SAGA_THRESHOLD + TimeDelta::minutes(5));
        let old =
            collection.time_done - (STALE_SAGA_THRESHOLD + TimeDelta::hours(2));
        let parent_id = SitrepUuid::new_v4();
        let kept_id = fact_uuid(1);
        let dup_id = fact_uuid(2);
        let mut parent_facts = IdOrdMap::new();
        parent_facts
            .insert_unique(np_fact(kept_id, parent_id, id, old))
            .unwrap();
        parent_facts
            .insert_unique(np_fact(dup_id, parent_id, id, current))
            .unwrap();
        let parent = make_parent_with_saga_case_from_facts(
            parent_id,
            collection.id,
            parent_facts,
        );
        // Still stale; the observation matches the duplicate, not the kept
        // fact.
        let observed = observed_map([ObservedSaga {
            saga_id: id,
            saga_name: "test-saga".to_string(),
            saga_state: ObservedSagaState::Unwinding,
            time_created,
            current_sec: None,
            adopt_generation: Generation::new(),
            last_event_time: Some(current),
            owner_state: None,
        }]);
        let input = build_input(collection, Some(parent), observed);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);

        let facts = saga_facts(&sitrep, true);
        assert_eq!(
            facts.len(),
            1,
            "both parent facts should be removed and one fresh fact added",
        );
        assert_ne!(facts[0].0.id, kept_id, "the stale kept fact was removed");
        assert_ne!(
            facts[0].0.id, dup_id,
            "the duplicate was removed unconditionally",
        );
        match &facts[0].1 {
            SagaFact::NotProgressing(p) => {
                assert_eq!(p.last_event_time, current);
            }
            other => panic!("expected NotProgressing, got {other:?}"),
        }
        logctx.cleanup_successful();
    }

    fn mk_abandoned(id: steno::SagaId) -> ObservedSaga {
        ObservedSaga {
            saga_id: id,
            saga_name: "test-saga".to_string(),
            saga_state: ObservedSagaState::Abandoned,
            time_created: Utc::now() - TimeDelta::days(1),
            current_sec: None,
            adopt_generation: Generation::new(),
            last_event_time: None,
            owner_state: None,
        }
    }

    fn abandoned_fact(
        id: FactUuid,
        created_sitrep_id: SitrepUuid,
        saga: steno::SagaId,
    ) -> fm::case::Fact {
        fm::case::Fact {
            id,
            created_sitrep_id,
            payload: SagaFact::Abandoned(SagaAbandonedFactPayload {
                saga_id: saga,
            })
            .into(),
            comment: "parent abandoned fact".to_string(),
        }
    }

    /// An abandoned saga opens a case carrying a single `Abandoned` fact.
    #[test]
    fn opens_abandoned_case() {
        let (logctx, collection) = setup("saga_opens_abandoned");
        let id = saga_id(1);
        let observed = observed_map([mk_abandoned(id)]);
        let input = build_input(collection, None, observed);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);

        let facts = saga_facts(&sitrep, true);
        assert_eq!(facts.len(), 1);
        match &facts[0].1 {
            SagaFact::Abandoned(p) => assert_eq!(p.saga_id, id),
            other => panic!("expected Abandoned, got {other:?}"),
        }
        logctx.cleanup_successful();
    }

    /// A stuck saga that Nexus then abandons keeps its case (it is the same
    /// episode, escalated), swapping the `NotProgressing` fact for an
    /// `Abandoned` fact.
    #[test]
    fn stuck_saga_escalates_to_abandoned() {
        let (logctx, collection) = setup("saga_escalates_to_abandoned");
        let id = saga_id(1);
        let stale =
            collection.time_done - (STALE_SAGA_THRESHOLD + TimeDelta::hours(1));
        let parent = make_parent_with_saga_case(
            SitrepUuid::new_v4(),
            collection.id,
            [SagaFact::NotProgressing(SagaNotProgressingFactPayload {
                saga_id: id,
                saga_state: SagaProgressState::Unwinding,
                last_event_time: stale,
            })],
        );
        let parent_case_id = parent.cases.iter().next().unwrap().id;
        let observed = observed_map([mk_abandoned(id)]);
        let input = build_input(collection, Some(parent), observed);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);

        let case = sitrep
            .cases
            .iter()
            .find(|c| c.metadata.de == DiagnosisEngineKind::Saga)
            .expect("saga case should be present");
        assert_eq!(
            case.id, parent_case_id,
            "the same case carries forward; abandonment escalates the \
             episode rather than starting a new one",
        );
        assert!(case.is_open(), "abandonment must not close the case");
        let facts = saga_facts(&sitrep, true);
        assert_eq!(facts.len(), 1, "the NotProgressing fact is superseded");
        match &facts[0].1 {
            SagaFact::Abandoned(p) => assert_eq!(p.saga_id, id),
            other => panic!("expected Abandoned, got {other:?}"),
        }
        logctx.cleanup_successful();
    }

    /// The `Abandoned` payload is pure identity, so the fact carries forward
    /// with a stable UUID for as long as the saga stays abandoned.
    #[test]
    fn abandoned_fact_uuid_stable() {
        let (logctx, collection) = setup("saga_abandoned_stable");
        let id = saga_id(1);
        let parent_id = SitrepUuid::new_v4();
        let parent_fact_id = fact_uuid(1);
        let mut parent_facts = IdOrdMap::new();
        parent_facts
            .insert_unique(abandoned_fact(parent_fact_id, parent_id, id))
            .unwrap();
        let parent = make_parent_with_saga_case_from_facts(
            parent_id,
            collection.id,
            parent_facts,
        );
        let observed = observed_map([mk_abandoned(id)]);
        let input = build_input(collection, Some(parent), observed);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);

        let facts = saga_facts(&sitrep, true);
        assert_eq!(facts.len(), 1);
        assert_eq!(
            facts[0].0.id, parent_fact_id,
            "the Abandoned fact UUID is stable across sitreps",
        );
        logctx.cleanup_successful();
    }

    /// An abandoned saga's case closes only when the saga row is removed
    /// from the database (i.e., it drops out of the observed set).
    #[test]
    fn abandoned_case_closes_when_saga_removed() {
        let (logctx, collection) = setup("saga_abandoned_closes_on_removal");
        let id = saga_id(1);
        let parent_id = SitrepUuid::new_v4();
        let mut parent_facts = IdOrdMap::new();
        parent_facts
            .insert_unique(abandoned_fact(fact_uuid(1), parent_id, id))
            .unwrap();
        let parent = make_parent_with_saga_case_from_facts(
            parent_id,
            collection.id,
            parent_facts,
        );
        let input = build_input(collection, Some(parent), observed_map([]));
        let (sitrep, _report) = run_analyze(&logctx.log, &input);

        let case = sitrep
            .cases
            .iter()
            .find(|c| c.metadata.de == DiagnosisEngineKind::Saga)
            .expect("saga case should be present in the closing sitrep");
        assert!(!case.is_open(), "case should close once the saga row is gone",);
        assert_eq!(case.facts.len(), 1, "the fact stays attached as evidence");
        logctx.cleanup_successful();
    }
}
