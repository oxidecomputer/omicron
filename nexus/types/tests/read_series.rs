// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for
//! [`nexus_types::deployment::UnstableReconfiguratorState::read_series`].
//!
//! These complement the script-based tests in `reconfigurator-cli`'s
//! `tests/input/cmds-load-*.txt` by directly exercising the merge logic,
//! including the warning and error paths that aren't reachable from a
//! clean CLI session.
//!
//! These are integration tests (rather than unit tests in
//! `nexus/types/src/deployment.rs`) so that we can pull
//! `reconfigurator-cli::test_utils::ReconfiguratorCliTestState` in as a
//! dev-dependency without running into Cargo's "two instances of
//! `nexus-types`" problem.  Integration tests link against the same lib
//! instance that dev-deps see, so the `UnstableReconfiguratorState` we
//! get out of the helper is the same type we feed to `read_series`.

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
    let state = sim.current_state().to_serializable().expect("serialize state");
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

/// Build a small DNS config entry suitable as a fixture for the
/// DNS-mismatch tests.  The zone contents don't matter; the test only needs
/// the entry to compare unequal to a perturbed copy.
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

/// Verifies that a single valid input round-trips cleanly through
/// `read_series`.
#[test]
fn single_input_happy_path() {
    let state = base_state("multi_load_single_input");
    let bytes = serialize(&state);
    let result =
        UnstableReconfiguratorState::read_series([input("only.out", &bytes)])
            .unwrap_or_else(|err| {
                panic!("read_series failed: {}", err_chain(&err))
            });
    assert_eq!(result.latest, "only.out");
    assert!(
        result.warnings.is_empty(),
        "expected no warnings, got: {:?}",
        result.warnings
    );
    assert_eq!(state, result.state);
}

/// Verifies that reading two files of byte-identical content under two
/// different labels also succeeds without warnings.  We don't care which one
/// gets labeled "latest".
#[test]
fn identical_inputs() {
    let state = base_state("multi_load_identical_inputs");
    let bytes = serialize(&state);
    let result = UnstableReconfiguratorState::read_series([
        input("first.out", &bytes),
        input("second.out", &bytes),
    ])
    .unwrap_or_else(|err| panic!("read_series failed: {}", err_chain(&err)));
    assert!(
        result.warnings.is_empty(),
        "expected no warnings, got: {:?}",
        result.warnings
    );
    assert_eq!(state, result.state);
}

/// Verifies that with two inputs whose targets are part of the same chain,
/// the latest is unambiguously whichever has the descendant target.  That
/// must hold regardless of the order in which inputs were passed.
#[test]
fn overlapping_inputs_order_independent() {
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
        ("a then b", vec![input("a.out", &bytes_a), input("b.out", &bytes_b)]),
        ("b then a", vec![input("b.out", &bytes_b), input("a.out", &bytes_a)]),
    ] {
        let result = UnstableReconfiguratorState::read_series(inputs)
            .unwrap_or_else(|err| {
                panic!("read_series failed ({label}): {}", err_chain(&err))
            });
        assert_eq!(result.latest, "b.out", "case {label}");
        assert_eq!(
            state_b, result.state,
            "case {label}: merged state should equal file B's state",
        );
        assert!(
            result.warnings.is_empty(),
            "case {label}: unexpected warnings: {:?}",
            result.warnings,
        );
    }
}

/// Verifies that with two inputs that share a collection id but differ in
/// collection content, loading fails with an explicit error.
#[test]
fn collection_mismatch() {
    let state = base_state("multi_load_collection_mismatch");
    assert!(!state.collections.is_empty(), "need a collection to mutate");
    let mut mutated = state.clone();
    // Bump `time_done` on one collection without changing its id.  Any
    // observable change is enough to trip the equality check.
    let mut entry =
        mutated.collections.iter_mut().next().expect("at least one collection");
    entry.time_done = entry.time_done + Duration::seconds(1);
    let mismatched_id = entry.id;
    drop(entry);

    let bytes_a = serialize(&state);
    let bytes_b = serialize(&mutated);
    let err = UnstableReconfiguratorState::read_series([
        input("a.out", &bytes_a),
        input("b.out", &bytes_b),
    ])
    .err()
    .expect("expected mismatch error");
    let rendered = err_chain(&err);
    assert_contains(&rendered, &format!("collection {mismatched_id}"));
    assert_contains(&rendered, "does not match");
    assert_contains(&rendered, "b.out");
}

/// Verifies that when two inputs share a blueprint but differ on contents,
/// loading fails with an explicit error.
#[test]
fn blueprint_mismatch() {
    let state = base_state("multi_load_blueprint_mismatch");
    assert!(!state.blueprints.is_empty(), "need a blueprint to mutate");
    let mut mutated = state.clone();
    // `creator` is a free-form string that doesn't affect any other
    // invariant.
    let mut entry =
        mutated.blueprints.iter_mut().next().expect("at least one blueprint");
    entry.creator = "mutated-creator".to_owned();
    let mismatched_id = entry.id;
    drop(entry);

    let bytes_a = serialize(&state);
    let bytes_b = serialize(&mutated);
    let err = UnstableReconfiguratorState::read_series([
        input("a.out", &bytes_a),
        input("b.out", &bytes_b),
    ])
    .err()
    .expect("expected mismatch error");
    let rendered = err_chain(&err);
    assert_contains(&rendered, &format!("blueprint {mismatched_id}"));
    assert_contains(&rendered, "does not match");
    assert_contains(&rendered, "b.out");
}

/// Verifies that when two inputs share an internal-DNS generation but differ
/// in its contents, loading fails with an explicit error.
#[test]
fn internal_dns_mismatch() {
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
    let err = UnstableReconfiguratorState::read_series([
        input("a.out", &bytes_a),
        input("b.out", &bytes_b),
    ])
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

/// Same as the internal-DNS mismatch test above, but for external DNS.
#[test]
fn external_dns_mismatch() {
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
    let err = UnstableReconfiguratorState::read_series([
        input("a.out", &bytes_a),
        input("b.out", &bytes_b),
    ])
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

/// Verifies the behavior around preserving planner reports.  We use two
/// inputs with the same blueprint, but one has
/// `BlueprintSource::Planner(report)` and the other has
/// `BlueprintSource::PlannerLoadedFromDatabase`.  The merge should accept
/// both and keep the report regardless of which input was processed first.
#[test]
fn blueprint_source_reconciliation() {
    let state = base_state("multi_load_source_reconciliation");
    let planner_blueprint = state
        .blueprints
        .iter()
        .find(|b| matches!(b.source, BlueprintSource::Planner(_)))
        .expect("base_state runs the planner, so a Planner blueprint exists");
    let target_id = planner_blueprint.id;

    let mut stripped = state.clone();
    for mut b in stripped.blueprints.iter_mut() {
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
        let result = UnstableReconfiguratorState::read_series(inputs)
            .unwrap_or_else(|err| {
                panic!("read_series failed ({label}): {}", err_chain(&err))
            });
        assert!(
            result.warnings.is_empty(),
            "case {label}: unexpected warnings: {:?}",
            result.warnings,
        );
        // Regardless of the order in which the inputs were processed, the
        // merged state should match the with-report side (the reconciliation
        // copies the report over to the other copy).
        assert_eq!(
            state, result.state,
            "case {label}: planning report should be retained",
        );
    }
}

/// Verifies the behavior when given a file whose `target_blueprint` refers to
/// a blueprint id that isn't in that same file.  `read_series` should emit a
/// warning (not an error) and still return a valid merged state, drawing the
/// target from the other input.
#[test]
fn target_missing_from_its_own_file() {
    let state = base_state("multi_load_target_missing_from_file");
    let target_id = state.target_blueprint.target_id;

    // File A: same target but without the target blueprint in its
    // `blueprints` list.  File B: the original, intact state.  We need both
    // because only the second file provides the target blueprint that lets
    // `read_series` finish successfully.
    let mut missing = state.clone();
    missing.blueprints.remove(&target_id).expect("target blueprint present");

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
    assert_contains(&warning_text, &format!("target blueprint {target_id}"));
    assert_contains(&warning_text, "missing from this file");
    // The merged state should still contain the target blueprint, sourced
    // from full.out.
    assert!(
        result.state.blueprints.iter().any(|b| b.id == target_id),
        "merged state should contain the target blueprint",
    );
}

/// Verifies the behavior when we're given a sequential history with a gap in
/// the middle.
///
/// The motivating scenario: the system planned three blueprints in sequence
/// (B1 → B2 → B3) and saved a state file each time it changed the target.
/// Of those three saves, we only have files 1 and 3 — file 2 is missing.
/// Now both B1 (the target of file 1) and B3 (the target of file 3) look
/// like "latest" candidates to the merge, because the merge only inspects
/// each file's target's *direct* parent: B3's parent is B2 (so B2 gets
/// marked non-leaf), but nothing in our input set tells the merge that B1
/// is also a non-leaf.
///
/// Expect a warning and a `time_made_target` tiebreak that picks file 3
/// regardless of the order in which inputs were passed.
#[test]
fn multiple_latest_candidates() {
    use omicron_uuid_kinds::BlueprintUuid;

    let state = base_state("multi_load_multiple_latest_candidates");

    // B1 is the planner-generated blueprint from the base state.  It was the
    // target at the time of file 1's save.
    let planner_blueprint = state
        .blueprints
        .iter()
        .find(|b| matches!(b.source, BlueprintSource::Planner(_)))
        .expect("base_state runs the planner, so a Planner blueprint exists")
        .clone();
    let blueprint_b1_id = planner_blueprint.id;

    // Synthesize B2 (child of B1) and B3 (child of B2).  In the realistic
    // scenario, file 2 would have been saved when B2 was the target, but
    // that file is missing from our input set.
    let blueprint_b2_id = BlueprintUuid::new_v4();
    let blueprint_b3_id = BlueprintUuid::new_v4();
    let mut blueprint_b2 = planner_blueprint.clone();
    blueprint_b2.id = blueprint_b2_id;
    blueprint_b2.parent_blueprint_id = Some(blueprint_b1_id);
    let mut blueprint_b3 = planner_blueprint.clone();
    blueprint_b3.id = blueprint_b3_id;
    blueprint_b3.parent_blueprint_id = Some(blueprint_b2_id);

    let now = chrono::Utc::now();
    let t1 = now;
    let t3 = now + Duration::seconds(20);

    // File 1: blueprints = [...base state..., B1]; target = B1.
    let mut file_1 = state.clone();
    file_1.target_blueprint.target_id = blueprint_b1_id;
    file_1.target_blueprint.time_made_target = t1;

    // File 3: blueprints = [...base state..., B1, B2, B3]; target = B3.
    let mut file_3 = state.clone();
    file_3.blueprints.insert_unique(blueprint_b2).unwrap();
    file_3.blueprints.insert_unique(blueprint_b3).unwrap();
    file_3.target_blueprint.target_id = blueprint_b3_id;
    file_3.target_blueprint.time_made_target = t3;

    let bytes_1 = serialize(&file_1);
    let bytes_3 = serialize(&file_3);

    // Expected merged state: file 3 wins the tiebreaker, and its blueprint
    // set is already a superset of file 1's, so the merge is just file 3.
    let expected = file_3.clone();

    for (label, inputs) in [
        ("1 then 3", vec![input("1.out", &bytes_1), input("3.out", &bytes_3)]),
        ("3 then 1", vec![input("3.out", &bytes_3), input("1.out", &bytes_1)]),
    ] {
        let result = UnstableReconfiguratorState::read_series(inputs)
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
        assert_eq!(result.latest, "3.out", "case {label}");
        assert_eq!(expected, result.state, "case {label}");
    }
}

/// Passing no inputs at all bails immediately.
#[test]
fn no_inputs() {
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

/// Feeding bytes that aren't valid JSON yields a parse error whose chain
/// includes the input's label and a serde error.
#[test]
fn malformed_json() {
    let bytes: &[u8] = b"not json at all";
    let err =
        UnstableReconfiguratorState::read_series([input("garbage.out", bytes)])
            .err()
            .expect("expected parse error");
    let rendered = err_chain(&err);
    assert_contains(&rendered, "parse \"garbage.out\"");
}
