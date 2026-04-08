// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::*;
use sled_agent_types::early_networking::MaxPathConfig;
use sled_agent_types::early_networking::PortConfig;
use sled_agent_types::early_networking::PortSpeed;
use sled_agent_types::early_networking::SwitchSlot;

/// Build a minimal `BgpPeerConfig` with a numbered peer address.
fn numbered_peer(asn: u32, addr: &str) -> BgpPeerConfig {
    BgpPeerConfig {
        asn,
        port: String::new(),
        addr: RouterPeerType::Numbered { ip: addr.parse().unwrap() },
        hold_time: None,
        idle_hold_time: None,
        delay_open: None,
        connect_retry: None,
        keepalive: None,
        remote_asn: None,
        min_ttl: None,
        md5_auth_key: None,
        multi_exit_discriminator: None,
        communities: Vec::new(),
        local_pref: None,
        enforce_first_as: false,
        allowed_import: ImportExportPolicy::NoFiltering,
        allowed_export: ImportExportPolicy::NoFiltering,
        vlan_id: None,
    }
}

/// Build a minimal `BgpPeerConfig` with an unnumbered peer.
fn unnumbered_peer(asn: u32) -> BgpPeerConfig {
    BgpPeerConfig {
        asn,
        port: String::new(),
        addr: RouterPeerType::Unnumbered {
            router_lifetime: RouterLifetimeConfig::default(),
        },
        hold_time: None,
        idle_hold_time: None,
        delay_open: None,
        connect_retry: None,
        keepalive: None,
        remote_asn: None,
        min_ttl: None,
        md5_auth_key: None,
        multi_exit_discriminator: None,
        communities: Vec::new(),
        local_pref: None,
        enforce_first_as: false,
        allowed_import: ImportExportPolicy::NoFiltering,
        allowed_export: ImportExportPolicy::NoFiltering,
        vlan_id: None,
    }
}

fn bgp_config(asn: u32) -> BgpConfig {
    BgpConfig {
        asn,
        originate: Vec::new(),
        shaper: None,
        checker: None,
        max_paths: MaxPathConfig::default(),
    }
}

/// Build a minimal `PortConfig` on the given switch with the given BGP peers.
fn port_config(
    switch: SwitchSlot,
    port: &str,
    bgp_peers: Vec<BgpPeerConfig>,
) -> PortConfig {
    PortConfig {
        routes: Vec::new(),
        addresses: Vec::new(),
        switch,
        port: port.to_string(),
        uplink_port_speed: PortSpeed::Speed100G,
        uplink_port_fec: None,
        bgp_peers,
        autoneg: false,
        lldp: None,
        tx_eq: None,
    }
}

fn rack_config(
    ports: Vec<PortConfig>,
    bgp: Vec<BgpConfig>,
) -> RackNetworkConfig {
    RackNetworkConfig {
        rack_subnet: "fd00::/48".parse().unwrap(),
        infra_ip_first: "10.0.0.1".parse().unwrap(),
        infra_ip_last: "10.0.0.100".parse().unwrap(),
        ports,
        bgp,
        bfd: Vec::new(),
    }
}

#[test]
fn no_bgp_config() {
    let logctx = omicron_test_utils::dev::test_setup_log("no_bgp_config");
    let config = rack_config(vec![], vec![]);

    let result = build_apply_request(
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        &logctx.log,
    );
    assert!(
        matches!(result, BuildApplyRequestResult::SkippedNoConfig),
        "expected SkippedNoConfig, got {result:?}"
    );

    logctx.cleanup_successful();
}

#[test]
fn no_ports_on_our_switch() {
    let logctx =
        omicron_test_utils::dev::test_setup_log("no_ports_on_our_switch");

    // Port on Switch1, but we are Switch0 (TEST_FAKE).
    let config = rack_config(
        vec![port_config(
            SwitchSlot::Switch1,
            "qsfp0",
            vec![numbered_peer(65000, "10.0.0.1")],
        )],
        vec![bgp_config(65000)],
    );

    let result = build_apply_request(
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        &logctx.log,
    );
    assert!(
        matches!(result, BuildApplyRequestResult::SkippedNoConfig),
        "expected SkippedNoConfig, got {result:?}"
    );

    logctx.cleanup_successful();
}

#[test]
fn single_numbered_peer() {
    let logctx =
        omicron_test_utils::dev::test_setup_log("single_numbered_peer");

    let config = rack_config(
        vec![port_config(
            SwitchSlot::Switch0,
            "qsfp0",
            vec![numbered_peer(65000, "10.0.0.1")],
        )],
        vec![bgp_config(65000)],
    );

    let result = build_apply_request(
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        &logctx.log,
    );
    let BuildApplyRequestResult::Ok {
        bgp_request,
        fanout_request,
        bad_peer_asns,
    } = result
    else {
        panic!("expected Ok, got {result:?}");
    };

    assert_eq!(bgp_request.asn, 65000);
    assert_eq!(bgp_request.peers.len(), 1);
    assert!(bgp_request.unnumbered_peers.is_empty());
    assert_eq!(bgp_request.peers["qsfp0"].len(), 1);
    assert_eq!(bgp_request.peers["qsfp0"][0].host, "10.0.0.1:179");
    assert_eq!(fanout_request.fanout.get(), 1);
    assert!(bad_peer_asns.is_empty());

    logctx.cleanup_successful();
}

#[test]
fn single_unnumbered_peer() {
    let logctx =
        omicron_test_utils::dev::test_setup_log("single_unnumbered_peer");

    let config = rack_config(
        vec![port_config(
            SwitchSlot::Switch0,
            "qsfp0",
            vec![unnumbered_peer(65000)],
        )],
        vec![bgp_config(65000)],
    );

    let result = build_apply_request(
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        &logctx.log,
    );
    let BuildApplyRequestResult::Ok { bgp_request, bad_peer_asns, .. } = result
    else {
        panic!("expected Ok, got {result:?}");
    };

    assert_eq!(bgp_request.asn, 65000);
    assert!(bgp_request.peers.is_empty());
    assert_eq!(bgp_request.unnumbered_peers.len(), 1);
    assert_eq!(bgp_request.unnumbered_peers["qsfp0"].len(), 1);
    assert_eq!(
        bgp_request.unnumbered_peers["qsfp0"][0].name,
        "unnumbered-qsfp0"
    );
    assert_eq!(
        bgp_request.unnumbered_peers["qsfp0"][0].interface,
        "tfportqsfp0_0"
    );
    assert!(bad_peer_asns.is_empty());

    logctx.cleanup_successful();
}

#[test]
fn multiple_peers_on_multiple_ports() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "multiple_peers_on_multiple_ports",
    );

    let config = rack_config(
        vec![
            port_config(
                SwitchSlot::Switch0,
                "qsfp0",
                vec![numbered_peer(65000, "10.0.0.1")],
            ),
            port_config(
                SwitchSlot::Switch0,
                "qsfp1",
                vec![
                    numbered_peer(65000, "10.0.1.1"),
                    numbered_peer(65000, "10.0.1.2"),
                ],
            ),
        ],
        vec![bgp_config(65000)],
    );

    let result = build_apply_request(
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        &logctx.log,
    );
    let BuildApplyRequestResult::Ok { bgp_request, bad_peer_asns, .. } = result
    else {
        panic!("expected Ok, got {result:?}");
    };

    assert_eq!(bgp_request.peers.len(), 2);
    assert_eq!(bgp_request.peers["qsfp0"].len(), 1);
    assert_eq!(bgp_request.peers["qsfp1"].len(), 2);
    assert!(bad_peer_asns.is_empty());

    logctx.cleanup_successful();
}

#[test]
fn filters_other_switch_slot() {
    let logctx =
        omicron_test_utils::dev::test_setup_log("filters_other_switch_slot");

    // Ports on both switches; we should only get Switch0's peer.
    let config = rack_config(
        vec![
            port_config(
                SwitchSlot::Switch0,
                "qsfp0",
                vec![numbered_peer(65000, "10.0.0.1")],
            ),
            port_config(
                SwitchSlot::Switch1,
                "qsfp0",
                vec![numbered_peer(65000, "10.1.0.1")],
            ),
        ],
        vec![bgp_config(65000)],
    );

    let result = build_apply_request(
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        &logctx.log,
    );
    let BuildApplyRequestResult::Ok { bgp_request, .. } = result else {
        panic!("expected Ok, got {result:?}");
    };

    assert_eq!(bgp_request.peers.len(), 1);
    assert_eq!(bgp_request.peers["qsfp0"][0].host, "10.0.0.1:179");

    logctx.cleanup_successful();
}

#[test]
fn peer_asn_not_in_bgp_config_all_fail() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "peer_asn_not_in_bgp_config_all_fail",
    );

    // Peer references ASN 65000, but no top-level BgpConfig has that ASN.
    let config = rack_config(
        vec![port_config(
            SwitchSlot::Switch0,
            "qsfp0",
            vec![numbered_peer(65000, "10.0.0.1")],
        )],
        vec![bgp_config(65001)],
    );

    let result = build_apply_request(
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        &logctx.log,
    );
    let BuildApplyRequestResult::SkippedInvalidConfig { bad_peer_asns } =
        result
    else {
        panic!("expected SkippedInvalidConfig, got {result:?}");
    };

    assert_eq!(bad_peer_asns, BTreeSet::from([65000]));

    logctx.cleanup_successful();
}

#[test]
fn mismatched_asn_across_ports() {
    let logctx =
        omicron_test_utils::dev::test_setup_log("mismatched_asn_across_ports");

    // First port has ASN 65000 (matches a config), second port has ASN 65001
    // (different ASN, should be skipped).
    let config = rack_config(
        vec![
            port_config(
                SwitchSlot::Switch0,
                "qsfp0",
                vec![numbered_peer(65000, "10.0.0.1")],
            ),
            port_config(
                SwitchSlot::Switch0,
                "qsfp1",
                vec![numbered_peer(65001, "10.0.1.1")],
            ),
        ],
        vec![bgp_config(65000), bgp_config(65001)],
    );

    let result = build_apply_request(
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        &logctx.log,
    );
    let BuildApplyRequestResult::Ok { bgp_request, bad_peer_asns, .. } = result
    else {
        panic!("expected Ok with bad_peer_asns, got {result:?}");
    };

    // First peer should succeed, second should be skipped.
    assert_eq!(bgp_request.asn, 65000);
    assert_eq!(bgp_request.peers.len(), 1);
    assert!(bgp_request.peers.contains_key("qsfp0"));
    assert_eq!(bad_peer_asns, BTreeSet::from([65001]));

    logctx.cleanup_successful();
}

#[test]
fn originate_prefixes() {
    let logctx = omicron_test_utils::dev::test_setup_log("originate_prefixes");

    let mut bgp = bgp_config(65000);
    bgp.originate =
        vec!["10.0.0.0/24".parse().unwrap(), "2001:db8::/48".parse().unwrap()];

    let config = rack_config(
        vec![port_config(
            SwitchSlot::Switch0,
            "qsfp0",
            vec![numbered_peer(65000, "10.0.0.1")],
        )],
        vec![bgp],
    );

    let result = build_apply_request(
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        &logctx.log,
    );
    let BuildApplyRequestResult::Ok { bgp_request, .. } = result else {
        panic!("expected Ok, got {result:?}");
    };

    assert_eq!(bgp_request.originate.len(), 2);

    logctx.cleanup_successful();
}

#[test]
fn checker_and_shaper() {
    let logctx = omicron_test_utils::dev::test_setup_log("checker_and_shaper");

    let mut bgp = bgp_config(65000);
    bgp.checker = Some("checker_code".to_string());
    bgp.shaper = Some("shaper_code".to_string());

    let config = rack_config(
        vec![port_config(
            SwitchSlot::Switch0,
            "qsfp0",
            vec![numbered_peer(65000, "10.0.0.1")],
        )],
        vec![bgp],
    );

    let result = build_apply_request(
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        &logctx.log,
    );
    let BuildApplyRequestResult::Ok { bgp_request, .. } = result else {
        panic!("expected Ok, got {result:?}");
    };

    let checker = bgp_request.checker.unwrap();
    assert_eq!(checker.asn, 65000);
    assert_eq!(checker.code, "checker_code");

    let shaper = bgp_request.shaper.unwrap();
    assert_eq!(shaper.asn, 65000);
    assert_eq!(shaper.code, "shaper_code");

    logctx.cleanup_successful();
}

#[test]
fn max_paths_fanout() {
    let logctx = omicron_test_utils::dev::test_setup_log("max_paths_fanout");

    let mut bgp = bgp_config(65000);
    bgp.max_paths = MaxPathConfig::new(8).unwrap();

    let config = rack_config(
        vec![port_config(
            SwitchSlot::Switch0,
            "qsfp0",
            vec![numbered_peer(65000, "10.0.0.1")],
        )],
        vec![bgp],
    );

    let result = build_apply_request(
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        &logctx.log,
    );
    let BuildApplyRequestResult::Ok { fanout_request, .. } = result else {
        panic!("expected Ok, got {result:?}");
    };

    assert_eq!(fanout_request.fanout.get(), 8);

    logctx.cleanup_successful();
}

#[test]
fn mixed_numbered_and_unnumbered_peers() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "mixed_numbered_and_unnumbered_peers",
    );

    let config = rack_config(
        vec![
            port_config(
                SwitchSlot::Switch0,
                "qsfp0",
                vec![numbered_peer(65000, "10.0.0.1")],
            ),
            port_config(
                SwitchSlot::Switch0,
                "qsfp1",
                vec![unnumbered_peer(65000)],
            ),
        ],
        vec![bgp_config(65000)],
    );

    let result = build_apply_request(
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        &logctx.log,
    );
    let BuildApplyRequestResult::Ok { bgp_request, bad_peer_asns, .. } = result
    else {
        panic!("expected Ok, got {result:?}");
    };

    assert_eq!(bgp_request.peers.len(), 1);
    assert_eq!(bgp_request.unnumbered_peers.len(), 1);
    assert!(bgp_request.peers.contains_key("qsfp0"));
    assert!(bgp_request.unnumbered_peers.contains_key("qsfp1"));
    assert!(bad_peer_asns.is_empty());

    logctx.cleanup_successful();
}

#[test]
fn peer_defaults_applied() {
    let logctx =
        omicron_test_utils::dev::test_setup_log("peer_defaults_applied");

    let config = rack_config(
        vec![port_config(
            SwitchSlot::Switch0,
            "qsfp0",
            vec![numbered_peer(65000, "10.0.0.1")],
        )],
        vec![bgp_config(65000)],
    );

    let result = build_apply_request(
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        &logctx.log,
    );
    let BuildApplyRequestResult::Ok { bgp_request, .. } = result else {
        panic!("expected Ok, got {result:?}");
    };

    let peer = &bgp_request.peers["qsfp0"][0];
    assert_eq!(peer.hold_time, BgpPeerConfig::DEFAULT_HOLD_TIME);
    assert_eq!(peer.idle_hold_time, BgpPeerConfig::DEFAULT_IDLE_HOLD_TIME);
    assert_eq!(peer.delay_open, BgpPeerConfig::DEFAULT_DELAY_OPEN);
    assert_eq!(peer.connect_retry, BgpPeerConfig::DEFAULT_CONNECT_RETRY);
    assert_eq!(peer.keepalive, BgpPeerConfig::DEFAULT_KEEPALIVE);
    assert_eq!(peer.resolution, BGP_SESSION_RESOLUTION);
    assert!(!peer.passive);

    logctx.cleanup_successful();
}

#[test]
fn import_export_policies_carried_through() {
    let logctx = omicron_test_utils::dev::test_setup_log(
        "import_export_policies_carried_through",
    );

    let mut peer = numbered_peer(65000, "10.0.0.1");
    peer.allowed_import =
        ImportExportPolicy::Allow(vec!["10.0.0.0/24".parse().unwrap()]);
    peer.allowed_export =
        ImportExportPolicy::Allow(vec!["10.1.0.0/24".parse().unwrap()]);

    let config = rack_config(
        vec![port_config(SwitchSlot::Switch0, "qsfp0", vec![peer])],
        vec![bgp_config(65000)],
    );

    let result = build_apply_request(
        &config,
        ThisSledSwitchSlot::TEST_FAKE,
        &logctx.log,
    );
    let BuildApplyRequestResult::Ok { bgp_request, .. } = result else {
        panic!("expected Ok, got {result:?}");
    };

    let mgd_peer = &bgp_request.peers["qsfp0"][0];
    let ipv4_unicast = mgd_peer.ipv4_unicast.as_ref().unwrap();

    match &ipv4_unicast.import_policy {
        MgdImportExportPolicy4::Allow(prefixes) => {
            assert_eq!(prefixes.len(), 1);
        }
        other => panic!("expected Allow, got {other:?}"),
    }

    match &ipv4_unicast.export_policy {
        MgdImportExportPolicy4::Allow(prefixes) => {
            assert_eq!(prefixes.len(), 1);
        }
        other => panic!("expected Allow, got {other:?}"),
    }

    logctx.cleanup_successful();
}
