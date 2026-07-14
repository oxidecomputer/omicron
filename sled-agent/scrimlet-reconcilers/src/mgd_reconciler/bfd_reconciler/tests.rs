// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::*;
use crate::switch_zone_slot::ThisSledSwitchSlot;
use mg_admin_client::types::BfdPeerConfig as MgdBfdPeerConfig;
use mg_admin_client::types::SessionMode as MgdSessionMode;
use sled_agent_types::early_networking::BfdMode;
use sled_agent_types::early_networking::BfdPeerConfig;
use sled_agent_types::early_networking::LinkSpeed;
use sled_agent_types::early_networking::PortConfig;
use sled_agent_types::early_networking::RackNetworkConfig;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::early_networking::UplinkPorts;

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

// The tests in this module don't care about the details of the network
// config uplink ports, but we're required by construction to have a
// nonempty set.
fn any_uplink_ports() -> UplinkPorts {
    UplinkPorts::new(vec![PortConfig {
        routes: Vec::new(),
        addresses: Vec::new(),
        switch: SwitchSlot::Switch0,
        port: "does-not-matter".to_owned(),
        uplink_port_speed: LinkSpeed::Speed0G,
        uplink_port_fec: None,
        bgp_peers: Vec::new(),
        autoneg: false,
        lldp: None,
        tx_eq: None,
    }])
    .unwrap()
}

fn rack_config(bfd: Vec<BfdPeerConfig>) -> RackNetworkConfig {
    RackNetworkConfig {
        rack_subnet: "fd00::/48".parse().unwrap(),
        infra_ip_first: "10.0.0.1".parse().unwrap(),
        infra_ip_last: "10.0.0.100".parse().unwrap(),
        ports: any_uplink_ports(),
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

// TODO-FIXME Proptests are commented out because the current implementation of
// BFD in mgd has various problems that prevent it from succeeding (either at
// all, for some strange inputs, or in a reasonable amount of time, for
// basically all inputs). Once
// <https://github.com/oxidecomputer/maghemite/pull/795> and
// <https://github.com/oxidecomputer/maghemite/pull/796> are merged and pulled
// into omicron, we can uncomment this test: it passes with that BFD
// implementation.
/*
use std::collections::BTreeMap;
use gateway_messages::SpPort;
use proptest::prelude::proptest;
use proptest::prelude::ProptestConfig;
use tokio::task::block_in_place;

#[derive(Debug, Clone, test_strategy::Arbitrary)]
struct TestInput {
    desired_peers: Vec<BfdPeerConfig>,
}

impl TestInput {
    fn rack_network_config(&self) -> RackNetworkConfig {
        // TODO-correctness `RackNetworkConfig`'s `Vec<BfdPeerConfig>` isn't the
        // right shape: we can't actually have multiple peers with the same
        // remote address. Squash into a map here to ensure uniqueness, but we
        // should fix the structure of the real type.
        let mut desired_peers = BTreeMap::new();
        for peer in &self.desired_peers {
            desired_peers.insert(peer.remote, peer);
        }

        rack_config(desired_peers.values().copied().cloned().collect())
    }
}

// Main implementation of our proptest.
//
// Unlike many proptests, we _don't_ try to clear the state of the mgd we're
// talking to before attempting to reconcile to the config described by `input`.
// Instead, we (ab)use the fact that the state changes between each input as
// part of our test: any reconciliation should succeed regardless of any state
// left behind by a prior test invocation. This messes with shrinking, but is a
// cheap and easy way to put the reconciler through its paces.
async fn run_one_proptest_input(
    input: &TestInput,
    client: &Client,
    log: &Logger,
) {
    let desired_config = input.rack_network_config();

    match reconcile(client, &desired_config, ThisSledSwitchSlot::TEST_FAKE, log)
        .await
    {
        MgdBfdReconcilerStatus::Success { .. } => {}
        status => {
            panic!("unexpected status: {status:?}")
        }
    }

    // After the successful reconciliation above, reconciling again should do
    // nothing. To check this, we partially reimplement `reconcile()` only up
    // through the construction of the `ReconciliationPlan`, then confirm that
    // plan is empty.
    let current_peers = mgd_get_current_bfd_peers(client).await.unwrap();
    let ReconciliationPlan { unchanged: _, to_remove, to_add } =
        ReconciliationPlan::new(
            current_peers,
            &desired_config,
            ThisSledSwitchSlot::TEST_FAKE,
            log,
        );
    assert_eq!(to_remove, BTreeSet::new());
    assert_eq!(to_add, BTreeSet::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn proptest_full_reconciliation() {
    let logctx =
        omicron_test_utils::dev::test_setup_log("proptest_full_reconciliation");
    let mgsctx = gateway_test_utils::setup::test_setup(
        "proptest_full_reconciliation",
        SpPort::One,
    )
    .await;
    let mut mgdctx = omicron_test_utils::dev::maghemite::MgdInstance::start(
        0,
        mgsctx.address().into(),
    )
    .await
    .expect("started mgd");
    let client = Client::new(
        &format!("http://{}", mgdctx.address()),
        logctx.log.clone(),
    );
    let rt = tokio::runtime::Handle::current();

    proptest!(|(input: TestInput)| {
        // Do a little dance to call our async `one_test_invocation` within the
        // non-async `proptest_macro!()` context.
        block_in_place(|| rt.block_on(run_one_proptest_input(
            &input,
            &client,
            &logctx.log,
        )));
    });

    mgdctx.cleanup().await.expect("mgd cleanup succeeded");
    mgsctx.teardown().await;
    logctx.cleanup_successful();
}
*/
