// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Saga diagnosis engine.
//!
//! Opens a case (keyed by `saga_id`) for any unfinished saga that is:
//!  - not making progress (no node event recorded for a while),
//!  - orphaned (owned by a Nexus that is no longer of the current generation),
//!  or
//!  - abandoned (Nexus permanently gave up on recovering it)
//!
//! A case closes when its saga completes or recovers, or, for
//! an abandoned saga, only when the saga row is removed from the database:
//! abandonment is the beginning of an escalation, not a resolution.
//!
//! See omicron#10530 for motivation.

use crate::SitrepBuilder;
use chrono::{DateTime, TimeDelta, Utc};
use nexus_types::fm;
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
use std::collections::btree_map::Entry;

/// A saga is flagged as "not progressing" once it has recorded no node event
/// for at least this long (`reference_time - last_event_time`).
const STALE_SAGA_THRESHOLD: TimeDelta = TimeDelta::minutes(30);

/// A parent-forwarded Saga case, parsed into the form this engine acts on.
/// Every fact on a saga case is about the same `saga_id`, and a case carries
/// at most one fact of each kind.
struct ParsedSagaCase {
    saga_id: steno::SagaId,
    /// The fact to consider when advancing the case: at most one per kind
    not_progressing: Option<(FactUuid, SagaNotProgressingFactPayload)>,
    owner_not_current: Option<(FactUuid, SagaOwnerNotCurrentFactPayload)>,
    abandoned: Option<(FactUuid, SagaAbandonedFactPayload)>,
    /// Facts that should not exist: duplicates of a kind beyond the first.
    /// They carry no information the kept fact doesn't.
    duplicate_facts: Vec<FactUuid>,
}

/// Why a parent-forwarded Saga case could not be interpreted.
///
/// Uninterpretable cases are closed by [`analyze`]: an open case this engine
/// cannot process would otherwise be carried forward into every future
/// sitrep with no path to closure.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
enum UninterpretableCase {
    #[error("fact {fact_id} does not belong to the saga diagnosis engine")]
    ForeignFactPayload { fact_id: FactUuid },
    #[error(
        "facts reference different sagas ({expected} and {found}, 1 expected)"
    )]
    DisagreeingSagas { expected: steno::SagaId, found: steno::SagaId },
    #[error("case has no facts, so the saga it concerns cannot be determined")]
    NoFacts,
}

/// Parse one parent-forwarded Saga case into a [`ParsedSagaCase`], or explain
/// why it cannot be interpreted.
fn parse_case(case: &fm::Case) -> Result<ParsedSagaCase, UninterpretableCase> {
    let mut saga_id: Option<steno::SagaId> = None;
    let mut not_progressing: Option<(FactUuid, SagaNotProgressingFactPayload)> =
        None;
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
            return Err(UninterpretableCase::ForeignFactPayload {
                fact_id: fact.metadata.id,
            });
        };
        let this_saga = saga_fact.saga_id();
        let expected = *saga_id.get_or_insert(this_saga);
        if expected != this_saga {
            return Err(UninterpretableCase::DisagreeingSagas {
                expected,
                found: this_saga,
            });
        }
        match saga_fact {
            SagaFact::NotProgressing(p) => {
                if not_progressing.is_none() {
                    not_progressing = Some((fact.metadata.id, p.clone()));
                } else {
                    duplicate_facts.push(fact.metadata.id);
                }
            }
            SagaFact::OwnerNotCurrentGeneration(p) => {
                if owner_not_current.is_none() {
                    owner_not_current = Some((fact.metadata.id, p.clone()));
                } else {
                    duplicate_facts.push(fact.metadata.id);
                }
            }
            SagaFact::Abandoned(p) => {
                if abandoned.is_none() {
                    abandoned = Some((fact.metadata.id, p.clone()));
                } else {
                    duplicate_facts.push(fact.metadata.id);
                }
            }
        }
    }
    let Some(saga_id) = saga_id else {
        return Err(UninterpretableCase::NoFacts);
    };
    Ok(ParsedSagaCase {
        saga_id,
        not_progressing,
        owner_not_current,
        abandoned,
        duplicate_facts,
    })
}

pub(super) fn analyze(builder: &mut SitrepBuilder<'_>) -> anyhow::Result<()> {
    let input = builder.input();
    // Reference the latest inventory for a concept of "now" when determining
    // staleness. This makes analysis deterministic and reproducible in tests.
    let reference_time = input.inventory().time_done;
    let observed_sagas = input.observed_sagas();

    // Parse the Saga cases copied forward from the parent sitrep. Every case
    // is about one saga, derived from its facts. Cases we cannot interpret are
    // closed inline, so they don't ride along as open-but-unprocessable in
    // every future sitrep. This is safe with respect to fault coverage:
    // detection below is independent of case bookkeeping, so if a closed case
    // concerned a saga that genuinely needs attention, a fresh, well-formed
    // case is opened in this same pass.
    let mut parent_cases: BTreeMap<CaseUuid, ParsedSagaCase> = BTreeMap::new();
    for case in input
        .open_cases()
        .iter()
        .filter(|c| c.metadata.de == DiagnosisEngineKind::Saga)
    {
        match parse_case(case) {
            Ok(parsed_case) => {
                parent_cases.insert(case.id, parsed_case);
            }
            Err(reason) => {
                builder
                    .cases
                    .case_mut(&case.id)
                    .expect("case_id came from builder's open cases")
                    .close(format!("cannot interpret case: {reason}"));
            }
        }
    }

    // Inverse index: which parent case is about which saga. Cases are per-saga,
    // so a saga with two parent cases is already pathological. We keep one and
    // close the rest as duplicates; which one we keep is arbitrary.
    // `parent_cases` iterates ascending by CaseUuid, so we deterministically
    // keep the lowest-ID case.
    let mut case_for_saga: BTreeMap<steno::SagaId, CaseUuid> = BTreeMap::new();
    for (case_id, parsed_case) in &parent_cases {
        match case_for_saga.entry(parsed_case.saga_id) {
            Entry::Vacant(slot) => {
                slot.insert(*case_id);
            }
            Entry::Occupied(kept) => {
                let kept_case_id = *kept.get();
                slog::warn!(
                    &builder.log,
                    "closing duplicate Saga case";
                    "case_id" => %case_id,
                    "kept_case_id" => %kept_case_id,
                    "saga_id" => %parsed_case.saga_id,
                );
                builder
                    .cases
                    .case_mut(case_id)
                    .expect("case_id came from builder's open cases")
                    .close(format!(
                        "duplicate of case {kept_case_id} for saga {}",
                        parsed_case.saga_id,
                    ));
            }
        }
    }

    // Close the surviving parent case for any saga that has reached a terminal
    // state (no longer observed) or has fully recovered (no condition holds
    // anymore). A still-problematic saga's facts are reconciled in the next
    // loop, which owns all fact state for the saga.
    for (saga_id, case_id) in &case_for_saga {
        let mut case_mut = builder
            .cases
            .case_mut(case_id)
            .expect("case_id came from builder's open cases");
        let Some(obs) = observed_sagas.get(saga_id) else {
            case_mut.close(format!("saga {saga_id} completed or was removed"));
            continue;
        };

        // If no conditions exist which should keep the case open,
        // it can (and should) close.
        if desired_not_progressing(obs, reference_time).is_none()
            && desired_owner_not_current(obs).is_none()
            && desired_abandoned(obs).is_none()
        {
            case_mut.close(format!(
                "saga {saga_id} is progressing under a current owner again",
            ));
        }
    }

    // For each observed saga with a problem, ensure its case carries exactly
    // the facts matching the current observation: reuse the parent-forwarded
    // case if any (dropping duplicate and stale facts), otherwise open a fresh
    // case. This loop owns all fact state for a saga.
    for obs in observed_sagas.iter() {
        let desired_np = desired_not_progressing(obs, reference_time);
        let desired_owner = desired_owner_not_current(obs);
        let desired_abandoned = desired_abandoned(obs);
        if desired_np.is_none()
            && desired_owner.is_none()
            && desired_abandoned.is_none()
        {
            continue;
        }

        let parent = case_for_saga
            .get(&obs.saga_id)
            .map(|case_id| (*case_id, &parent_cases[case_id]));

        let mut case_mut = match parent {
            Some((case_id, _)) => builder
                .cases
                .case_mut(&case_id)
                .expect("case_id came from builder's open cases"),
            None => {
                let mut new_case =
                    builder.cases.open_case(DiagnosisEngineKind::Saga);
                new_case.set_comment(format!(
                    "saga {} ({}) needs attention",
                    obs.saga_id, obs.saga_name,
                ));
                new_case
            }
        };

        // Duplicate facts carry no information the kept fact doesn't; remove
        // them whether or not the kept fact still matches the observation.
        if let Some((_, parsed_case)) = parent {
            for fact_id in &parsed_case.duplicate_facts {
                case_mut.remove_fact(
                    *fact_id,
                    "duplicate fact of the same kind on the case",
                );
            }
        }

        // Update facts about "NotProgressing"
        let carried_np = parent.and_then(|(_, p)| p.not_progressing.as_ref());
        if carried_np.map(|(_, p)| p) != desired_np.as_ref() {
            if let Some((fact_id, _)) = carried_np {
                case_mut.remove_fact(
                    *fact_id,
                    "NotProgressing fact no longer matches the current saga \
                     state",
                );
            }
            if let Some(payload) = desired_np {
                let staleness = reference_time
                    .signed_duration_since(payload.last_event_time);
                let comment = format!(
                    "no saga node event in {}",
                    omicron_common::format_time_delta(staleness),
                );
                case_mut.add_fact(SagaFact::NotProgressing(payload), comment);
            }
        }

        // Update facts about "OwnerNotCurrentGeneration"
        let carried_owner =
            parent.and_then(|(_, p)| p.owner_not_current.as_ref());
        if carried_owner.map(|(_, p)| p) != desired_owner.as_ref() {
            if let Some((fact_id, _)) = carried_owner {
                case_mut.remove_fact(
                    *fact_id,
                    "OwnerNotCurrentGeneration fact no longer matches the \
                     current saga state",
                );
            }
            if let Some(payload) = desired_owner {
                let comment = format!(
                    "owned by non-current Nexus {} ({:?})",
                    payload.current_sec, payload.orphan_reason,
                );
                case_mut.add_fact(
                    SagaFact::OwnerNotCurrentGeneration(payload),
                    comment,
                );
            }
        }

        // Abandoned: same reconciliation. The payload has no fields that can
        // change, so it only ever drops when the condition clears.
        let carried_abandoned = parent.and_then(|(_, p)| p.abandoned.as_ref());
        if carried_abandoned.map(|(_, p)| p) != desired_abandoned.as_ref() {
            if let Some((fact_id, _)) = carried_abandoned {
                case_mut.remove_fact(
                    *fact_id,
                    "Abandoned fact no longer matches the current saga state",
                );
            }
            if let Some(payload) = desired_abandoned {
                // Human-readable context (which a promoted problem would
                // otherwise look up from the saga row) goes in the comment,
                // not the payload.
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
                case_mut.add_fact(SagaFact::Abandoned(payload), comment);
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
/// boolean, so the payload never changes and the fact never rotates; the case
/// stays open, carrying this fact, until the saga row is removed from the
/// database.
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

    /// A saga fact found in a sitrep, paired with its decoded [`SagaFact`]
    /// payload.
    #[derive(Debug)]
    struct SagaFactRef<'a> {
        fact: &'a fm::case::Fact,
        saga_fact: SagaFact,
    }

    /// Collect every saga fact in the sitrep, with its decoded payload.
    /// Optionally filtered to open cases only.
    fn saga_facts(sitrep: &Sitrep, open_only: bool) -> Vec<SagaFactRef<'_>> {
        sitrep
            .cases
            .iter()
            .filter(|c| c.metadata.de == DiagnosisEngineKind::Saga)
            .filter(|c| !open_only || c.is_open())
            .flat_map(|c| {
                c.facts.iter().filter_map(|f| {
                    f.payload
                        .as_saga()
                        .map(|s| SagaFactRef { fact: f, saga_fact: s.clone() })
                })
            })
            .collect()
    }

    /// The fact UUID of the one saga fact on the one saga case in `sitrep`.
    /// Panics unless there is exactly one.
    fn sole_saga_fact_id(sitrep: &Sitrep) -> FactUuid {
        let facts = saga_facts(sitrep, false);
        assert_eq!(facts.len(), 1, "expected exactly one saga fact");
        facts[0].fact.metadata.id
    }

    /// Make a `Fact` carrying the given saga payload.
    fn mk_fact(
        parent_sitrep_id: SitrepUuid,
        payload: SagaFact,
    ) -> fm::case::Fact {
        fm::case::Fact {
            metadata: fm::case::FactMetadata {
                id: omicron_uuid_kinds::FactUuid::new_v4(),
                created_sitrep_id: parent_sitrep_id,
                comment: "parent saga fact".to_string(),
            },
            payload: payload.into(),
        }
    }

    fn fact_map(
        facts: impl IntoIterator<Item = fm::case::Fact>,
    ) -> IdOrdMap<fm::case::Fact> {
        let mut map = IdOrdMap::new();
        for fact in facts {
            map.insert_unique(fact).unwrap();
        }
        map
    }

    /// Make an open `Saga` case carrying the given facts.
    fn make_saga_case(
        case_id: omicron_uuid_kinds::CaseUuid,
        parent_sitrep_id: SitrepUuid,
        facts: IdOrdMap<fm::case::Fact>,
    ) -> fm::Case {
        fm::Case {
            id: case_id,
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
        }
    }

    /// Make a parent sitrep containing the given cases.
    fn make_parent_sitrep(
        parent_sitrep_id: SitrepUuid,
        inv_collection_id: omicron_uuid_kinds::CollectionUuid,
        cases: impl IntoIterator<Item = fm::Case>,
    ) -> Sitrep {
        let mut case_map = IdOrdMap::new();
        for case in cases {
            case_map.insert_unique(case).unwrap();
        }
        Sitrep {
            metadata: fm::SitrepMetadata {
                id: parent_sitrep_id,
                inv_collection_id,
                creator_id: OmicronZoneUuid::new_v4(),
                parent_sitrep_id: None,
                time_created: Utc::now(),
                next_inv_min_time_started: Utc::now(),
                comment: String::new(),
                alert_generation:
                    omicron_common::api::external::Generation::new(),
                support_bundle_generation:
                    omicron_common::api::external::Generation::new(),
            },
            cases: case_map,
            ereports_by_id: Default::default(),
        }
    }

    fn make_parent_with_saga_case(
        parent_sitrep_id: SitrepUuid,
        inv_collection_id: omicron_uuid_kinds::CollectionUuid,
        fact_payloads: impl IntoIterator<Item = SagaFact>,
    ) -> Sitrep {
        let facts = fact_map(
            fact_payloads
                .into_iter()
                .map(|payload| mk_fact(parent_sitrep_id, payload)),
        );
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
        let case = make_saga_case(
            omicron_uuid_kinds::CaseUuid::new_v4(),
            parent_sitrep_id,
            facts,
        );
        make_parent_sitrep(parent_sitrep_id, inv_collection_id, [case])
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
        match &facts[0].saga_fact {
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
        match &facts[0].saga_fact {
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
            facts
                .iter()
                .any(|fr| matches!(&fr.saga_fact, SagaFact::NotProgressing(_)))
        );
        assert!(facts.iter().any(|fr| matches!(
            &fr.saga_fact,
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
        let parent_fact_id = sole_saga_fact_id(&parent);
        // Observed saga matches the parent fact exactly (same last_event_time,
        // same state).
        let observed = observed_map([ObservedSaga {
            saga_id: id,
            saga_name: "test-saga".to_string(),
            saga_state: ObservedSagaState::Unwinding,
            time_created: Utc::now() - TimeDelta::days(1),
            current_sec: None,
            last_event_time: Some(stale),
            owner_state: None,
        }]);
        let input = build_input(collection, Some(parent), observed);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let facts = saga_facts(&sitrep, true);
        assert_eq!(facts.len(), 1);
        assert_eq!(
            facts[0].fact.metadata.id, parent_fact_id,
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
        let parent_fact_id = sole_saga_fact_id(&parent);
        // Still stale, but last_event_time advanced.
        let observed = observed_map([ObservedSaga {
            saga_id: id,
            saga_name: "test-saga".to_string(),
            saga_state: ObservedSagaState::Unwinding,
            time_created,
            current_sec: None,
            last_event_time: Some(new),
            owner_state: None,
        }]);
        let input = build_input(collection, Some(parent), observed);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);
        let facts = saga_facts(&sitrep, true);
        assert_eq!(facts.len(), 1);
        assert_ne!(
            facts[0].fact.metadata.id, parent_fact_id,
            "fact UUID should rotate when last_event_time changes",
        );
        match &facts[0].saga_fact {
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
                Some(SagaFact::OwnerNotCurrentGeneration(_)) => {
                    Some(f.metadata.id)
                }
                _ => None,
            })
            .expect("parent case should have an owner fact");
        // Progress has resumed, but the owner is still quiesced with the
        // same SEC, so the owner fact still matches.
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
            facts[0].fact.metadata.id, parent_owner_fact_id,
            "the persisting fact carries forward with a stable UUID",
        );
        assert!(matches!(
            &facts[0].saga_fact,
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
            metadata: fm::case::FactMetadata {
                id,
                created_sitrep_id,
                comment: "parent saga fact".to_string(),
            },
            payload: SagaFact::NotProgressing(SagaNotProgressingFactPayload {
                saga_id: saga,
                saga_state: SagaProgressState::Unwinding,
                last_event_time,
            })
            .into(),
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
            last_event_time: Some(current),
            owner_state: None,
        }]);
        let input = build_input(collection, Some(parent), observed);
        let (sitrep, _report) = run_analyze(&logctx.log, &input);

        let facts = saga_facts(&sitrep, true);
        assert_eq!(facts.len(), 1, "the duplicate fact should be removed");
        assert_eq!(
            facts[0].fact.metadata.id, kept_id,
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
        assert_ne!(
            facts[0].fact.metadata.id, kept_id,
            "the stale kept fact was removed"
        );
        assert_ne!(
            facts[0].fact.metadata.id, dup_id,
            "the duplicate was removed unconditionally",
        );
        match &facts[0].saga_fact {
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
            metadata: fm::case::FactMetadata {
                id,
                created_sitrep_id,
                comment: "parent abandoned fact".to_string(),
            },
            payload: SagaFact::Abandoned(SagaAbandonedFactPayload {
                saga_id: saga,
            })
            .into(),
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
        match &facts[0].saga_fact {
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
        match &facts[0].saga_fact {
            SagaFact::Abandoned(p) => assert_eq!(p.saga_id, id),
            other => panic!("expected Abandoned, got {other:?}"),
        }
        logctx.cleanup_successful();
    }

    /// The `Abandoned` payload never changes, so the fact carries forward
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
            facts[0].fact.metadata.id, parent_fact_id,
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

    /// A parent Saga case with zero facts has no derivable saga ID; the
    /// engine closes it as uninterpretable rather than carrying an
    /// unprocessable open case forward into every future sitrep.
    #[test]
    fn empty_case_is_closed() {
        let (logctx, collection) = setup("saga_empty_case_closed");
        let inv_id = collection.id;
        let parent_id = SitrepUuid::new_v4();
        let empty_case_id = omicron_uuid_kinds::CaseUuid::new_v4();
        let parent = make_parent_sitrep(
            parent_id,
            inv_id,
            [make_saga_case(empty_case_id, parent_id, IdOrdMap::new())],
        );

        let input = build_input(collection, Some(parent), observed_map([]));
        let (sitrep, report) = run_analyze(&logctx.log, &input);

        let case = sitrep
            .cases
            .get(&empty_case_id)
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

    /// Two open parent cases about the same saga: the engine keeps and
    /// maintains the one with the lowest case ID, and closes the other as a
    /// duplicate. (A half-maintained duplicate would otherwise decay into
    /// an uninterpretable empty case.)
    #[test]
    fn duplicate_case_is_closed() {
        let (logctx, collection) = setup("saga_duplicate_closed");
        let inv_id = collection.id;
        let reference_time = collection.time_done;
        let parent_id = SitrepUuid::new_v4();
        let id = saga_id(1);
        // Stale enough to keep the NotProgressing condition holding, and
        // matching the observation exactly so the kept fact carries forward.
        let last_event = reference_time - TimeDelta::hours(2);
        let payload = SagaNotProgressingFactPayload {
            saga_id: id,
            saga_state: SagaProgressState::Unwinding,
            last_event_time: last_event,
        };

        let id_a = omicron_uuid_kinds::CaseUuid::new_v4();
        let id_b = omicron_uuid_kinds::CaseUuid::new_v4();
        let (kept_id, dup_id) =
            if id_a < id_b { (id_a, id_b) } else { (id_b, id_a) };
        let kept_fact =
            mk_fact(parent_id, SagaFact::NotProgressing(payload.clone()));
        let kept_fact_id = kept_fact.metadata.id;
        let dup_fact =
            mk_fact(parent_id, SagaFact::NotProgressing(payload.clone()));
        let parent = make_parent_sitrep(
            parent_id,
            inv_id,
            [
                make_saga_case(kept_id, parent_id, fact_map([kept_fact])),
                make_saga_case(dup_id, parent_id, fact_map([dup_fact])),
            ],
        );

        let observed =
            observed_map([mk_observed(id, Some(last_event), None, None)]);
        let input = build_input(collection, Some(parent), observed);
        let (sitrep, report) = run_analyze(&logctx.log, &input);

        let kept = sitrep.cases.get(&kept_id).expect("kept case present");
        let dup = sitrep.cases.get(&dup_id).expect("duplicate case present");
        assert!(kept.is_open(), "lowest-ID case should remain open");
        assert!(!dup.is_open(), "duplicate case should be closed");
        // The kept case's fact is still accurate, so it should carry
        // forward unchanged.
        assert!(
            kept.facts.contains_key(&kept_fact_id),
            "kept case should retain its fact",
        );
        // No third case should have been opened for this saga.
        let saga_case_count = sitrep
            .cases
            .iter()
            .filter(|c| c.metadata.de == DiagnosisEngineKind::Saga)
            .count();
        assert_eq!(saga_case_count, 2);
        let report_str = format!("{}", report.display_multiline(0));
        assert!(
            report_str.contains("duplicate of case"),
            "close comment should call out the duplicate, got: {report_str}",
        );
        logctx.cleanup_successful();
    }

    /// A case whose facts disagree about which saga they concern is closed
    /// as uninterpretable. Because detection is independent of case
    /// bookkeeping, the still-stuck saga gets a fresh, well-formed case in
    /// the same pass.
    #[test]
    fn uninterpretable_case_is_replaced() {
        let (logctx, collection) = setup("saga_uninterpretable_replaced");
        let inv_id = collection.id;
        let reference_time = collection.time_done;
        let parent_id = SitrepUuid::new_v4();
        let stuck_saga = saga_id(1);
        let last_event = reference_time - TimeDelta::hours(2);

        // One fact about the stuck saga, one about an unrelated saga: the
        // case is self-contradictory.
        let corrupt_case_id = omicron_uuid_kinds::CaseUuid::new_v4();
        let corrupt_case = make_saga_case(
            corrupt_case_id,
            parent_id,
            fact_map([
                mk_fact(
                    parent_id,
                    SagaFact::NotProgressing(SagaNotProgressingFactPayload {
                        saga_id: stuck_saga,
                        saga_state: SagaProgressState::Unwinding,
                        last_event_time: last_event,
                    }),
                ),
                mk_fact(
                    parent_id,
                    SagaFact::Abandoned(SagaAbandonedFactPayload {
                        saga_id: saga_id(2),
                    }),
                ),
            ]),
        );
        let parent = make_parent_sitrep(parent_id, inv_id, [corrupt_case]);

        let observed = observed_map([mk_observed(
            stuck_saga,
            Some(last_event),
            None,
            None,
        )]);
        let input = build_input(collection, Some(parent), observed);
        let (sitrep, report) = run_analyze(&logctx.log, &input);

        let corrupt = sitrep
            .cases
            .get(&corrupt_case_id)
            .expect("corrupt case should still be in the output sitrep");
        assert!(
            !corrupt.is_open(),
            "case with disagreeing facts should be closed",
        );
        // The stuck saga still needs attention, so a fresh case should have
        // been opened for it.
        let open = saga_facts(&sitrep, true);
        assert_eq!(
            open.len(),
            1,
            "expected exactly one open Saga fact (on the replacement case)",
        );
        match &open[0].saga_fact {
            SagaFact::NotProgressing(p) => {
                assert_eq!(p.saga_id, stuck_saga);
                assert_eq!(p.last_event_time, last_event);
            }
            other => panic!("expected NotProgressing fact, got {other:?}"),
        }
        let report_str = format!("{}", report.display_multiline(0));
        assert!(
            report_str.contains("cannot interpret case"),
            "close comment should say the case was uninterpretable, got: \
             {report_str}",
        );
        logctx.cleanup_successful();
    }

    /// A Saga case carrying a physical-disk fact payload is a data-model
    /// violation; the engine closes it as uninterpretable.
    #[test]
    fn foreign_payload_case_is_closed() {
        let (logctx, collection) = setup("saga_foreign_payload_closed");
        let inv_id = collection.id;
        let parent_id = SitrepUuid::new_v4();
        let case_id = omicron_uuid_kinds::CaseUuid::new_v4();
        let foreign_fact = fm::case::Fact {
            metadata: fm::case::FactMetadata {
                id: omicron_uuid_kinds::FactUuid::new_v4(),
                created_sitrep_id: parent_id,
                comment: "a fact belonging to the physical-disk engine"
                    .to_string(),
            },
            payload: fm::FactPayload::PhysicalDisk(
                fm::DiskFact::ZpoolUnhealthy(fm::ZpoolUnhealthyFactPayload {
                    physical_disk_id:
                        omicron_uuid_kinds::PhysicalDiskUuid::new_v4(),
                    zpool_id: omicron_uuid_kinds::ZpoolUuid::new_v4(),
                    last_seen_health:
                        nexus_types::inventory::ZpoolHealth::Degraded,
                    observed_in_inv: inv_id,
                    time_observed: Utc::now(),
                }),
            ),
        };
        let parent = make_parent_sitrep(
            parent_id,
            inv_id,
            [make_saga_case(case_id, parent_id, fact_map([foreign_fact]))],
        );

        let input = build_input(collection, Some(parent), observed_map([]));
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
}
