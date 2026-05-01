// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test assertions for inspecting CockroachDB `EXPLAIN` output.

/// Asserts that an EXPLAIN plan:
///   1. Names the expected partial index in its `table:` line.
///   2. Does not contain any `FULL SCAN` over a non-partial index. A
///      `FULL SCAN` over a partial index is acceptable (the partial
///      predicate already bounds the rows touched), but a `FULL SCAN`
///      over the primary key or a non-partial secondary index means
///      we walked the whole table.
#[track_caller]
pub fn assert_uses_partial_index_only(
    explanation: &str,
    expected_index: &str,
) {
    eprintln!("{explanation}");

    let mut last_table_was_partial = false;
    let mut found_expected = false;
    let mut bad_full_scan = false;
    for line in explanation.lines() {
        let line = line.trim();
        if line.starts_with("table:") {
            last_table_was_partial = line.contains("(partial index)");
            if line.contains(&format!("@{expected_index}")) {
                found_expected = true;
            }
        } else if line.contains("FULL SCAN") && !last_table_was_partial {
            bad_full_scan = true;
        }
    }

    assert!(
        !bad_full_scan,
        "Found a FULL SCAN over a non-partial index in plan:\n{explanation}"
    );
    assert!(
        found_expected,
        "Expected plan to use index `{expected_index}`, but plan was:\n{explanation}"
    );
}
