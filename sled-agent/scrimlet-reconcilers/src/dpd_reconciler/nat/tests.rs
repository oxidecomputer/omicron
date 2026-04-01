// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::*;
use test_strategy::proptest;

#[proptest]
fn proptest_plan_all_unchanged(service_nat_entries: ServiceZoneNatEntries) {
    let logctx =
        omicron_test_utils::dev::test_setup_log("proptest_plan_all_unchanged");
    let log = &logctx.log;

    let dpd_current_entries =
        service_nat_entries.iter().map(NatEntry::from).collect();
    let plan = ReconciliationPlan::new(
        &dpd_current_entries,
        &service_nat_entries,
        log,
    )
    .expect("planning should succeed");

    let expected = ReconciliationPlan {
        unchanged: service_nat_entries.iter().map(|e| e.zone_id).collect(),
        to_remove: BTreeSet::new(),
        to_create: BTreeMap::new(),
    };

    assert_eq!(plan, expected);

    logctx.cleanup_successful();
}

#[proptest]
fn proptest_plan_create_all(service_nat_entries: ServiceZoneNatEntries) {
    let logctx =
        omicron_test_utils::dev::test_setup_log("proptest_plan_create_all");
    let log = &logctx.log;

    let dpd_current_entries = BTreeSet::new();
    let plan = ReconciliationPlan::new(
        &dpd_current_entries,
        &service_nat_entries,
        log,
    )
    .expect("planning should succeed");

    let expected = ReconciliationPlan {
        unchanged: BTreeSet::new(),
        to_remove: BTreeSet::new(),
        to_create: service_nat_entries
            .iter()
            .map(|e| (e.zone_id, NatEntry::from(e)))
            .collect(),
    };

    assert_eq!(plan, expected);

    logctx.cleanup_successful();
}

#[proptest]
fn proptest_plan_mix(
    mut dpd_current_entries: BTreeSet<NatEntry>,
    service_nat_entries: ServiceZoneNatEntries,
    #[strategy(0..=#service_nat_entries.len())]
    service_nat_entries_to_add: usize,
) {
    let logctx = omicron_test_utils::dev::test_setup_log("proptest_plan_mix");
    let log = &logctx.log;

    // Remove any `dpd_current_entries` that happen to overlap with
    // `service_nat_entries` from random chance in the `Arbitrary` inputs.
    for e in service_nat_entries.iter().map(NatEntry::from) {
        dpd_current_entries.remove(&e);
    }

    let expected_to_remove = dpd_current_entries.clone();

    // Now add the number of common entries we expect.
    let mut expected_unchanged = BTreeSet::new();
    let mut expected_to_create = BTreeMap::new();
    for (i, entry) in service_nat_entries.iter().enumerate() {
        if i < service_nat_entries_to_add {
            dpd_current_entries.insert(NatEntry::from(entry));
            expected_unchanged.insert(entry.zone_id);
        } else {
            expected_to_create.insert(entry.zone_id, NatEntry::from(entry));
        }
    }

    let plan = ReconciliationPlan::new(
        &dpd_current_entries,
        &service_nat_entries,
        log,
    )
    .expect("planning should succeed");

    let expected = ReconciliationPlan {
        unchanged: expected_unchanged,
        to_remove: expected_to_remove,
        to_create: expected_to_create,
    };

    assert_eq!(plan, expected);

    logctx.cleanup_successful();
}
