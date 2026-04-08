// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::*;
use mg_admin_client::types::BfdPeerConfig as MgdBfdPeerConfig;
use mg_admin_client::types::SessionMode as MgdSessionMode;
use sled_agent_types::early_networking::BfdMode;
use sled_agent_types::early_networking::BfdPeerConfig;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::early_networking::SwitchSlot;

use crate::switch_zone_slot::ThisSledSwitchSlot;

fn desired_bfd_peer(
    remote: &str,
    local: Option<&str>,
    detection_threshold: u8,
    required_rx: u64,
    mode: BfdMode,
    switch: SwitchSlot,
) -> BfdPeerConfig {
    BfdPeerConfig {
        local: local.map(|s| s.parse().unwrap()),
        remote: remote.parse().unwrap(),
        detection_threshold,
        required_rx,
        mode,
        switch,
    }
}

fn rack_config(bfd: Vec<BfdPeerConfig>) -> RackNetworkConfig {
    RackNetworkConfig {
        rack_subnet: "fd00::/48".parse().unwrap(),
        infra_ip_first: "10.0.0.1".parse().unwrap(),
        infra_ip_last: "10.0.0.100".parse().unwrap(),
        ports: Vec::new(),
        bgp: Vec::new(),
        bfd,
    }
}

fn mgd_bfd_peer(
    peer: &str,
    listen: &str,
    detection_threshold: u8,
    required_rx: u64,
    mode: MgdSessionMode,
) -> MgdBfdPeerConfig {
    MgdBfdPeerConfig {
        peer: peer.parse().unwrap(),
        listen: listen.parse().unwrap(),
        detection_threshold,
        required_rx,
        mode,
    }
}

#[test]
fn plan_all_unchanged() {
    let logctx = omicron_test_utils::dev::test_setup_log("plan_all_unchanged");
    let log = &logctx.log;

    let config = rack_config(vec![desired_bfd_peer(
        "10.0.0.2",
        Some("10.0.0.1"),
        3,
        300000,
        BfdMode::SingleHop,
        SwitchSlot::Switch0,
    )]);

    let current = vec![mgd_bfd_peer(
        "10.0.0.2",
        "10.0.0.1",
        3,
        300000,
        MgdSessionMode::SingleHop,
    )];

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    );

    assert_eq!(plan.unchanged.len(), 1);
    assert!(plan.to_remove.is_empty());
    assert!(plan.to_add.is_empty());

    logctx.cleanup_successful();
}

#[test]
fn plan_add_all() {
    let logctx = omicron_test_utils::dev::test_setup_log("plan_add_all");
    let log = &logctx.log;

    let config = rack_config(vec![
        desired_bfd_peer(
            "10.0.0.2",
            Some("10.0.0.1"),
            3,
            300000,
            BfdMode::SingleHop,
            SwitchSlot::Switch0,
        ),
        desired_bfd_peer(
            "10.0.0.3",
            None,
            5,
            500000,
            BfdMode::MultiHop,
            SwitchSlot::Switch0,
        ),
    ]);

    // mgd is empty.
    let current = vec![];

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    );

    assert_eq!(plan.unchanged.len(), 0);
    assert!(plan.to_remove.is_empty());
    assert_eq!(plan.to_add.len(), 2);

    logctx.cleanup_successful();
}

#[test]
fn plan_remove_all() {
    let logctx = omicron_test_utils::dev::test_setup_log("plan_remove_all");
    let log = &logctx.log;

    // Desired: no BFD peers.
    let config = rack_config(vec![]);

    let current = vec![
        mgd_bfd_peer(
            "10.0.0.2",
            "10.0.0.1",
            3,
            300000,
            MgdSessionMode::SingleHop,
        ),
        mgd_bfd_peer(
            "10.0.0.3",
            "0.0.0.0",
            5,
            500000,
            MgdSessionMode::MultiHop,
        ),
    ];

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    );

    assert_eq!(plan.unchanged.len(), 0);
    assert_eq!(plan.to_remove.len(), 2);
    assert!(plan.to_add.is_empty());

    logctx.cleanup_successful();
}

#[test]
fn plan_mix() {
    let logctx = omicron_test_utils::dev::test_setup_log("plan_mix");
    let log = &logctx.log;

    // Desired: keep 10.0.0.2, add 10.0.0.4, remove 10.0.0.3.
    let config = rack_config(vec![
        desired_bfd_peer(
            "10.0.0.2",
            Some("10.0.0.1"),
            3,
            300000,
            BfdMode::SingleHop,
            SwitchSlot::Switch0,
        ),
        desired_bfd_peer(
            "10.0.0.4",
            Some("10.0.0.1"),
            3,
            300000,
            BfdMode::SingleHop,
            SwitchSlot::Switch0,
        ),
    ]);

    let current = vec![
        mgd_bfd_peer(
            "10.0.0.2",
            "10.0.0.1",
            3,
            300000,
            MgdSessionMode::SingleHop,
        ),
        mgd_bfd_peer(
            "10.0.0.3",
            "10.0.0.1",
            3,
            300000,
            MgdSessionMode::SingleHop,
        ),
    ];

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    );

    assert_eq!(plan.unchanged, BTreeSet::from(["10.0.0.2".parse().unwrap()]));
    assert_eq!(plan.to_remove, BTreeSet::from(["10.0.0.3".parse().unwrap()]));
    assert_eq!(
        plan.to_add,
        BTreeSet::from([DiffableBfdPeer {
            peer: "10.0.0.4".parse().unwrap(),
            listen: "10.0.0.1".parse().unwrap(),
            detection_threshold: 3,
            required_rx: 300000,
            mode: DiffableSessionMode::SingleHop,
        }])
    );

    logctx.cleanup_successful();
}

#[test]
fn plan_filters_other_switch_slot() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "plan_filters_other_switch_slot",
    );
    let log = &logctx.log;

    // Config has peers on both switches; our reconciler is Switch0 (TEST_FAKE).
    let config = rack_config(vec![
        desired_bfd_peer(
            "10.0.0.2",
            Some("10.0.0.1"),
            3,
            300000,
            BfdMode::SingleHop,
            SwitchSlot::Switch0,
        ),
        desired_bfd_peer(
            "10.0.0.3",
            Some("10.0.0.1"),
            3,
            300000,
            BfdMode::SingleHop,
            SwitchSlot::Switch1,
        ),
    ]);

    // mgd is empty.
    let current = vec![];

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    );

    // Only the Switch0 peer should be added.
    assert_eq!(plan.unchanged.len(), 0);
    assert!(plan.to_remove.is_empty());
    assert_eq!(plan.to_add.len(), 1);

    logctx.cleanup_successful();
}

#[test]
fn plan_parameter_change_is_remove_plus_add() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "plan_parameter_change_is_remove_plus_add",
    );
    let log = &logctx.log;

    // Desired: same peer IP but different detection_threshold.
    let config = rack_config(vec![desired_bfd_peer(
        "10.0.0.2",
        Some("10.0.0.1"),
        5, // changed from 3
        300000,
        BfdMode::SingleHop,
        SwitchSlot::Switch0,
    )]);

    let current = vec![mgd_bfd_peer(
        "10.0.0.2",
        "10.0.0.1",
        3,
        300000,
        MgdSessionMode::SingleHop,
    )];

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    );

    // Different parameter means old peer removed, new peer added.
    assert_eq!(plan.unchanged.len(), 0);
    assert_eq!(plan.to_remove, BTreeSet::from(["10.0.0.2".parse().unwrap()]));
    assert_eq!(
        plan.to_add,
        BTreeSet::from([DiffableBfdPeer {
            peer: "10.0.0.2".parse().unwrap(),
            listen: "10.0.0.1".parse().unwrap(),
            detection_threshold: 5,
            required_rx: 300000,
            mode: DiffableSessionMode::SingleHop,
        }])
    );

    logctx.cleanup_successful();
}

#[test]
fn plan_both_empty() {
    let logctx = omicron_test_utils::dev::test_setup_log("plan_both_empty");
    let log = &logctx.log;

    let config = rack_config(vec![]);
    let current = vec![];

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    );

    assert_eq!(plan.unchanged.len(), 0);
    assert!(plan.to_remove.is_empty());
    assert!(plan.to_add.is_empty());

    logctx.cleanup_successful();
}

#[test]
fn plan_local_none_defaults_to_unspecified() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "plan_local_none_defaults_to_unspecified",
    );
    let log = &logctx.log;

    // Desired: local = None (should default to 0.0.0.0).
    let config = rack_config(vec![desired_bfd_peer(
        "10.0.0.2",
        None,
        3,
        300000,
        BfdMode::SingleHop,
        SwitchSlot::Switch0,
    )]);

    // mgd has the same peer with listen = 0.0.0.0 (the default).
    let current = vec![mgd_bfd_peer(
        "10.0.0.2",
        "0.0.0.0",
        3,
        300000,
        MgdSessionMode::SingleHop,
    )];

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    );

    // Should be unchanged because None defaults to 0.0.0.0.
    assert_eq!(plan.unchanged.len(), 1);
    assert!(plan.to_remove.is_empty());
    assert!(plan.to_add.is_empty());

    logctx.cleanup_successful();
}

#[test]
fn plan_mode_change_is_remove_plus_add() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "plan_mode_change_is_remove_plus_add",
    );
    let log = &logctx.log;

    // Desired: same peer but MultiHop instead of SingleHop.
    let config = rack_config(vec![desired_bfd_peer(
        "10.0.0.2",
        Some("10.0.0.1"),
        3,
        300000,
        BfdMode::MultiHop, // changed
        SwitchSlot::Switch0,
    )]);

    let current = vec![mgd_bfd_peer(
        "10.0.0.2",
        "10.0.0.1",
        3,
        300000,
        MgdSessionMode::SingleHop,
    )];

    let plan = ReconciliationPlan::new(
        current,
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        log,
    );

    assert_eq!(plan.unchanged.len(), 0);
    assert_eq!(plan.to_remove, BTreeSet::from(["10.0.0.2".parse().unwrap()]));
    assert_eq!(
        plan.to_add,
        BTreeSet::from([DiffableBfdPeer {
            peer: "10.0.0.2".parse().unwrap(),
            listen: "10.0.0.1".parse().unwrap(),
            detection_threshold: 3,
            required_rx: 300000,
            mode: DiffableSessionMode::MultiHop,
        }])
    );

    logctx.cleanup_successful();
}
