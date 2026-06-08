// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rust integration tests for
//! [`nexus_types::deployment::UnstableReconfiguratorState::read_series`].
//!
//! These complement the script-based tests in `tests/input/cmds-load-*.txt`
//! by directly exercising the merge logic, including the warning and error
//! paths that aren't reachable from a clean CLI session.

use chrono::Duration;
use nexus_types::deployment::BlueprintSource;
use nexus_types::deployment::ReconfiguratorStateInput;
use nexus_types::deployment::UnstableReconfiguratorState;
use omicron_test_utils::dev::test_setup_log;
use reconfigurator_cli::test_utils::ReconfiguratorCliTestState;
use slog_error_chain::InlineErrorChain;

/// Builds a baseline state by running `load_example` and then the planner, so
/// the resulting state has at least one blueprint whose source is
/// `BlueprintSource::Planner(report)` and at least one non-genesis blueprint.
fn base_state(test_name: &str) -> UnstableReconfiguratorState {
    let logctx = test_setup_log(test_name);
    let mut sim = ReconfiguratorCliTestState::new(test_name, &logctx.log);
    sim.load_example().expect("loaded example system");
    sim.run_planner().expect("planning succeeded");
    let state = sim
        .current_state()
        .to_serializable()
        .expect("serialize state");
    logctx.cleanup_successful();
    state
}

fn serialize(state: &UnstableReconfiguratorState) -> Vec<u8> {
    serde_json::to_vec(state).expect("serialize")
}

fn input<'a>(
    label: &str,
    bytes: &'a [u8],
) -> ReconfiguratorStateInput<&'a [u8]> {
    ReconfiguratorStateInput { label: label.to_owned(), reader: bytes }
}

/// Render an error and its source chain into a single string suitable for
/// substring matching.
fn err_chain(err: &anyhow::Error) -> String {
    InlineErrorChain::new(err.as_ref()).to_string()
}

/// Assert that `haystack` contains `needle`, with a useful failure message.
#[track_caller]
fn assert_contains(haystack: &str, needle: &str) {
    assert!(
        haystack.contains(needle),
        "expected to find {needle:?} in:\n{haystack}"
    );
}

/// Build a small DNS config entry suitable as a fixture for the DNS-mismatch
/// tests.  The zone contents don't matter; the test only needs the entry to
/// compare unequal to a perturbed copy.
fn make_dns_entry(
    generation: omicron_common::api::external::Generation,
) -> internal_dns_types::config::DnsConfigParams {
    internal_dns_types::config::DnsConfigParams {
        generation,
        serial: 1,
        time_created: chrono::Utc::now(),
        zones: Vec::new(),
    }
}

/// Case 1: a single valid input round-trips cleanly through `read_series`.
#[test]
fn case_01_single_input_happy_path() {
    let state = base_state("multi_load_single_input");
    let bytes = serialize(&state);
    let result = UnstableReconfiguratorState::read_series([input(
        "only.out", &bytes,
    )])
    .unwrap_or_else(|err| panic!("read_series failed: {}", err_chain(&err)));
    assert_eq!(result.latest, "only.out");
    assert!(
        result.warnings.is_empty(),
        "expected no warnings, got: {:?}",
        result.warnings
    );
    // The merged state should agree with the input on the things `read_series`
    // tracks (target blueprint and the set of blueprints / collections).
    assert_eq!(
        result.state.target_blueprint.target_id,
        state.target_blueprint.target_id,
    );
    assert_eq!(result.state.blueprints.len(), state.blueprints.len());
    assert_eq!(result.state.collections.len(), state.collections.len());
}

/// Case 2: feeding byte-identical content under two different labels also
/// succeeds without warnings.  The retained `latest` label is the first one
/// processed (the merge keeps whichever label first entered `all_states`).
#[test]
fn case_02_identical_inputs() {
    let state = base_state("multi_load_identical_inputs");
    let bytes = serialize(&state);
    let result = UnstableReconfiguratorState::read_series([
        input("first.out", &bytes),
        input("second.out", &bytes),
    ])
    .unwrap_or_else(|err| panic!("read_series failed: {}", err_chain(&err)));
    assert_eq!(result.latest, "first.out");
    assert!(
        result.warnings.is_empty(),
        "expected no warnings, got: {:?}",
        result.warnings
    );
    assert_eq!(result.state.blueprints.len(), state.blueprints.len());
    assert_eq!(result.state.collections.len(), state.collections.len());
}

/// Case 3: two consistent inputs whose targets sit on the same parent chain.
/// The latest is unambiguously whichever has the descendant target, and that
/// must hold regardless of the order in which inputs were passed.
#[test]
fn case_03_overlapping_inputs_order_independent() {
    // The base state has a parent blueprint and a planner-generated child.
    // We construct two inputs that differ only in which one each declares as
    // its `target_blueprint`: file A targets the parent, file B targets the
    // child.  read_series should always identify file B as the latest.
    let state = base_state("multi_load_overlapping_inputs");
    assert!(
        state.blueprints.len() >= 2,
        "base_state should have parent + planner-generated child"
    );
    let child_target = state.target_blueprint.target_id;
    let child_blueprint = state
        .blueprints
        .iter()
        .find(|b| b.id == child_target)
        .expect("target blueprint present in state");
    let parent_target = child_blueprint
        .parent_blueprint_id
        .expect("planner output has a parent blueprint");

    let mut state_a = state.clone();
    state_a.target_blueprint.target_id = parent_target;
    let mut state_b = state.clone();
    state_b.target_blueprint.target_id = child_target;
    let bytes_a = serialize(&state_a);
    let bytes_b = serialize(&state_b);

    for (label, inputs) in [
        (
            "a then b",
            vec![input("a.out", &bytes_a), input("b.out", &bytes_b)],
        ),
        (
            "b then a",
            vec![input("b.out", &bytes_b), input("a.out", &bytes_a)],
        ),
    ] {
        let result =
            UnstableReconfiguratorState::read_series(inputs)
                .unwrap_or_else(|err| {
                    panic!("read_series failed ({label}): {}", err_chain(&err))
                });
        assert_eq!(result.latest, "b.out", "case {label}");
        assert_eq!(
            result.state.target_blueprint.target_id, child_target,
            "case {label}",
        );
        assert!(
            result.warnings.is_empty(),
            "case {label}: unexpected warnings: {:?}",
            result.warnings,
        );
    }
}

/// Case 4: two inputs share a collection id but differ in collection content.
/// The merge must reject this as inconsistent.
#[test]
fn case_04_collection_mismatch() {
    let state = base_state("multi_load_collection_mismatch");
    assert!(!state.collections.is_empty(), "need a collection to mutate");
    let mut mutated = state.clone();
    // Bump `time_done` on the first collection without changing its id.  Any
    // observable change is enough to trip the equality check.
    mutated.collections[0].time_done =
        mutated.collections[0].time_done + Duration::seconds(1);
    let mismatched_id = mutated.collections[0].id;

    let bytes_a = serialize(&state);
    let bytes_b = serialize(&mutated);
    let err = UnstableReconfiguratorState::read_series(
        [input("a.out", &bytes_a), input("b.out", &bytes_b)],
    )
    .err()
    .expect("expected mismatch error");
    let rendered = err_chain(&err);
    assert_contains(&rendered, &format!("collection {mismatched_id}"));
    assert_contains(&rendered, "does not match");
    assert_contains(&rendered, "b.out");
}

/// Case 5: two inputs share a blueprint id but differ on a content field.
/// The merge must reject the inconsistency.
#[test]
fn case_05_blueprint_mismatch() {
    let state = base_state("multi_load_blueprint_mismatch");
    assert!(!state.blueprints.is_empty(), "need a blueprint to mutate");
    let mut mutated = state.clone();
    // `creator` is a free-form string that doesn't affect any other invariant.
    mutated.blueprints[0].creator = "mutated-creator".to_owned();
    let mismatched_id = mutated.blueprints[0].id;

    let bytes_a = serialize(&state);
    let bytes_b = serialize(&mutated);
    let err = UnstableReconfiguratorState::read_series(
        [input("a.out", &bytes_a), input("b.out", &bytes_b)],
    )
    .err()
    .expect("expected mismatch error");
    let rendered = err_chain(&err);
    assert_contains(&rendered, &format!("blueprint {mismatched_id}"));
    assert_contains(&rendered, "does not match");
    assert_contains(&rendered, "b.out");
}

/// Case 6: the planner-report reconciliation branch.  Two inputs share a
/// blueprint id, but one has `BlueprintSource::Planner(report)` and the other
/// has `BlueprintSource::PlannerLoadedFromDatabase`.  The merge should accept
/// both and keep the report regardless of which input was processed first.
#[test]
fn case_06_blueprint_source_reconciliation() {
    let state = base_state("multi_load_source_reconciliation");
    let planner_blueprint = state
        .blueprints
        .iter()
        .find(|b| matches!(b.source, BlueprintSource::Planner(_)))
        .expect("base_state runs the planner, so a Planner blueprint exists");
    let target_id = planner_blueprint.id;

    let mut stripped = state.clone();
    for b in stripped.blueprints.iter_mut() {
        if b.id == target_id {
            b.source = BlueprintSource::PlannerLoadedFromDatabase;
        }
    }

    let bytes_with = serialize(&state);
    let bytes_without = serialize(&stripped);

    for (label, inputs) in [
        (
            "with-report first",
            vec![
                input("with.out", &bytes_with),
                input("without.out", &bytes_without),
            ],
        ),
        (
            "without-report first",
            vec![
                input("without.out", &bytes_without),
                input("with.out", &bytes_with),
            ],
        ),
    ] {
        let result =
            UnstableReconfiguratorState::read_series(inputs)
                .unwrap_or_else(|err| {
                    panic!("read_series failed ({label}): {}", err_chain(&err))
                });
        assert!(
            result.warnings.is_empty(),
            "case {label}: unexpected warnings: {:?}",
            result.warnings,
        );
        let merged_blueprint = result
            .state
            .blueprints
            .iter()
            .find(|b| b.id == target_id)
            .expect("blueprint preserved through merge");
        assert!(
            matches!(merged_blueprint.source, BlueprintSource::Planner(_)),
            "case {label}: expected planning report to be retained, got {:?}",
            merged_blueprint.source,
        );
    }
}

/// Case 7: two inputs share an internal-DNS generation but differ in
/// contents.  The merge must reject the inconsistency.
#[test]
fn case_07_internal_dns_mismatch() {
    use omicron_common::api::external::Generation;

    let state = base_state("multi_load_internal_dns_mismatch");
    // The example builder doesn't populate DNS, so insert a minimal entry
    // first and then mutate it for the second input.
    let mut with_dns = state.clone();
    let generation = Generation::from(1u32);
    with_dns.internal_dns.insert(generation, make_dns_entry(generation));

    let mut mutated = with_dns.clone();
    mutated
        .internal_dns
        .get_mut(&generation)
        .expect("entry just inserted")
        .serial += 1;

    let bytes_a = serialize(&with_dns);
    let bytes_b = serialize(&mutated);
    let err = UnstableReconfiguratorState::read_series(
        [input("a.out", &bytes_a), input("b.out", &bytes_b)],
    )
    .err()
    .expect("expected mismatch error");
    let rendered = err_chain(&err);
    assert_contains(
        &rendered,
        &format!("internal DNS generation {generation}"),
    );
    assert_contains(&rendered, "does not match");
    assert_contains(&rendered, "b.out");
}

/// Case 8: same as case 7, for external DNS.
#[test]
fn case_08_external_dns_mismatch() {
    use omicron_common::api::external::Generation;

    let state = base_state("multi_load_external_dns_mismatch");
    let mut with_dns = state.clone();
    let generation = Generation::from(1u32);
    with_dns.external_dns.insert(generation, make_dns_entry(generation));

    let mut mutated = with_dns.clone();
    mutated
        .external_dns
        .get_mut(&generation)
        .expect("entry just inserted")
        .serial += 1;

    let bytes_a = serialize(&with_dns);
    let bytes_b = serialize(&mutated);
    let err = UnstableReconfiguratorState::read_series(
        [input("a.out", &bytes_a), input("b.out", &bytes_b)],
    )
    .err()
    .expect("expected mismatch error");
    let rendered = err_chain(&err);
    assert_contains(
        &rendered,
        &format!("external DNS generation {generation}"),
    );
    assert_contains(&rendered, "does not match");
    assert_contains(&rendered, "b.out");
}

/// Case 9: a file whose `target_blueprint` refers to a blueprint id that
/// isn't in that same file.  read_series should emit a warning (not an
/// error) and still return a valid merged state, drawing the target from
/// the other input.
#[test]
fn case_09_target_missing_from_its_own_file() {
    let state = base_state("multi_load_target_missing_from_file");
    let target_id = state.target_blueprint.target_id;

    // File A: same target but without the target blueprint in its
    // `blueprints` list.  File B: the original, intact state.  We need both
    // because only the second file provides the target blueprint that lets
    // `read_series` finish successfully.
    let mut missing = state.clone();
    missing.blueprints.retain(|b| b.id != target_id);

    let bytes_missing = serialize(&missing);
    let bytes_full = serialize(&state);
    let result = UnstableReconfiguratorState::read_series([
        input("missing.out", &bytes_missing),
        input("full.out", &bytes_full),
    ])
    .unwrap_or_else(|err| panic!("read_series failed: {}", err_chain(&err)));

    assert_eq!(result.warnings.len(), 1, "expected exactly one warning");
    let warning_text = result.warnings[0].to_string();
    assert_contains(&warning_text, "missing.out");
    assert_contains(
        &warning_text,
        &format!("target blueprint {target_id}"),
    );
    assert_contains(&warning_text, "missing from this file");
    // The merged state should still contain the target blueprint, sourced
    // from full.out.
    assert!(
        result.state.blueprints.iter().any(|b| b.id == target_id),
        "merged state should contain the target blueprint",
    );
}

/// Case 10: two files have different target blueprints that share the same
/// parent.  Neither target is the parent of the other, so both look like
/// "latest" candidates.  Expect a warning and a `time_made_target` tiebreak
/// that's stable regardless of input order.
#[test]
fn case_10_multiple_latest_candidates() {
    use omicron_uuid_kinds::BlueprintUuid;

    let state = base_state("multi_load_multiple_latest_candidates");
    let original_target_id = state.target_blueprint.target_id;
    let original_target_blueprint = state
        .blueprints
        .iter()
        .find(|b| b.id == original_target_id)
        .expect("target blueprint present")
        .clone();
    // Use the original target as the common parent for two synthetic
    // sibling blueprints.
    let shared_parent_id = original_target_id;

    // Two new blueprint ids and timestamps.  The later timestamp belongs to
    // sibling B and should win the tiebreaker.
    let sibling_a_id = BlueprintUuid::new_v4();
    let sibling_b_id = BlueprintUuid::new_v4();
    let now = chrono::Utc::now();
    let t1 = now;
    let t2 = now + Duration::seconds(10);

    // Build state_a: blueprints = original set + sibling_a; target = sibling_a.
    let mut state_a = state.clone();
    let mut sibling_a = original_target_blueprint.clone();
    sibling_a.id = sibling_a_id;
    sibling_a.parent_blueprint_id = Some(shared_parent_id);
    state_a.blueprints.push(sibling_a);
    state_a.target_blueprint.target_id = sibling_a_id;
    state_a.target_blueprint.time_made_target = t1;

    // Build state_b similarly, with sibling_b and the later timestamp.
    let mut state_b = state.clone();
    let mut sibling_b = original_target_blueprint.clone();
    sibling_b.id = sibling_b_id;
    sibling_b.parent_blueprint_id = Some(shared_parent_id);
    state_b.blueprints.push(sibling_b);
    state_b.target_blueprint.target_id = sibling_b_id;
    state_b.target_blueprint.time_made_target = t2;

    let bytes_a = serialize(&state_a);
    let bytes_b = serialize(&state_b);

    for (label, inputs) in [
        (
            "a then b",
            vec![input("a.out", &bytes_a), input("b.out", &bytes_b)],
        ),
        (
            "b then a",
            vec![input("b.out", &bytes_b), input("a.out", &bytes_a)],
        ),
    ] {
        let result =
            UnstableReconfiguratorState::read_series(inputs)
                .unwrap_or_else(|err| {
                    panic!("read_series failed ({label}): {}", err_chain(&err))
                });
        assert_eq!(result.warnings.len(), 1, "case {label}");
        let warning_text = result.warnings[0].to_string();
        assert_contains(&warning_text, "2 blueprint ids");
        assert_contains(
            &warning_text,
            "were not the parent of some other blueprint",
        );
        // The later `time_made_target` wins, which is sibling_b.
        assert_eq!(
            result.state.target_blueprint.target_id, sibling_b_id,
            "case {label}: latest target should be sibling_b",
        );
        assert_eq!(result.latest, "b.out", "case {label}");
    }
}

/// Case 11: passing no inputs at all bails immediately.
#[test]
fn case_11_no_inputs() {
    let inputs: Vec<ReconfiguratorStateInput<&[u8]>> = Vec::new();
    let err = UnstableReconfiguratorState::read_series(inputs)
        .err()
        .expect("expected error from empty input set");
    let rendered = err_chain(&err);
    assert_contains(
        &rendered,
        "found no inputs containing a valid target blueprint id",
    );
}

/// Case 12: feeding bytes that aren't valid JSON yields a parse error whose
/// chain includes the input's label and a serde error.
#[test]
fn case_12_malformed_json() {
    let bytes: &[u8] = b"not json at all";
    let err = UnstableReconfiguratorState::read_series([input(
        "garbage.out",
        bytes,
    )])
    .err()
    .expect("expected parse error");
    let rendered = err_chain(&err);
    assert_contains(&rendered, "parse \"garbage.out\"");
}
