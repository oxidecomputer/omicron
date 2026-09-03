// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::*;
use assert_matches::assert_matches;
use gateway_messages::SpPort;
use sled_agent_types::early_networking::BfdPeerConfig as UplinkBfdPeerConfig;
use sled_agent_types::early_networking::LinkSpeed;
use sled_agent_types::early_networking::MaxPathConfig;
use sled_agent_types::early_networking::NumberedRouter;
use sled_agent_types::early_networking::PortConfig;
use sled_agent_types::early_networking::RouteConfig;
use sled_agent_types::early_networking::RouterLifetimeConfig;
use sled_agent_types::early_networking::RouterPeerIpAddr;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::early_networking::UnnumberedRouter;
use sled_agent_types::early_networking::UplinkPorts;
use sled_agent_types::router_config::RouterConfigBfdPeer;
use sled_agent_types::router_config::RouterConfigBgpPeer;
use sled_agent_types::router_config::RouterConfigStaticRoute4;
use sled_agent_types::router_config::RouterConfigStaticRoute6;
use sled_agent_types::router_config::RouterConfigUnnumberedBgpPeer;
use sled_agent_types::router_config::SwitchRouterConfigs;
use sled_agent_types::router_config::default_router_list;
use uuid::Uuid;

fn port_config(
    switch: SwitchSlot,
    port: &str,
    bgp_peers: Vec<UplinkBgpPeerConfig>,
    routes: Vec<RouteConfig>,
) -> PortConfig {
    PortConfig {
        routes,
        addresses: Vec::new(),
        switch,
        port: port.to_string(),
        uplink_port_speed: LinkSpeed::Speed100G,
        uplink_port_fec: None,
        bgp_peers,
        autoneg: false,
        lldp: None,
        tx_eq: None,
        allow_ddm_traffic: false,
    }
}

fn rack_config(
    ports: Vec<PortConfig>,
    bgp: Vec<BgpConfig>,
    bfd: Vec<UplinkBfdPeerConfig>,
) -> RackNetworkConfig {
    RackNetworkConfig {
        rack_subnet: "fd00::/48".parse().unwrap(),
        infra_ip_first: "10.0.0.1".parse().unwrap(),
        infra_ip_last: "10.0.0.100".parse().unwrap(),
        ports: UplinkPorts::new(ports).unwrap(),
        bgp,
        bfd,
    }
}

fn system_config(
    rack_network_config: RackNetworkConfig,
    switch_router_configs: SwitchRouterConfigs,
) -> SystemNetworkingConfig {
    SystemNetworkingConfig {
        rack_network_config,
        blueprint_external_networking_config: None,
        switch_router_configs,
        control_plane_router_list: default_router_list(),
    }
}

fn uplink_peer(
    asn: u32,
    port: &str,
    addr: RouterPeerType,
) -> UplinkBgpPeerConfig {
    UplinkBgpPeerConfig {
        asn,
        port: port.to_string(),
        addr,
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

fn numbered(ip: &str) -> RouterPeerType {
    NumberedRouter::new(ip.parse::<RouterPeerIpAddr>().unwrap(), None)
        .unwrap()
        .into()
}

fn unnumbered(router_lifetime: u16) -> RouterPeerType {
    UnnumberedRouter {
        router_lifetime: RouterLifetimeConfig::new(router_lifetime).unwrap(),
    }
    .into()
}

fn operator_peer_parameters() -> RouterConfigBgpPeerParameters {
    RouterConfigBgpPeerParameters {
        hold_time: 6,
        idle_hold_time: 3,
        delay_open: 0,
        connect_retry: 3,
        keepalive: 2,
        remote_asn: None,
        min_ttl: None,
        md5_auth_key: None,
        multi_exit_discriminator: None,
        communities: Vec::new(),
        local_pref: None,
        enforce_first_as: false,
        vlan_id: None,
        allowed_import: None,
        allowed_export: None,
    }
}

fn empty_operator_spec(name: &str) -> RouterConfigSpec {
    RouterConfigSpec {
        name: name.to_string(),
        id: Uuid::new_v4(),
        bgp: None,
        static4: Vec::new(),
        static6: Vec::new(),
        bfd_peers: Vec::new(),
    }
}

#[test]
fn uplink_render_full() {
    let mut peer_a = uplink_peer(65000, "qsfp0", numbered("192.0.2.1"));
    peer_a.allowed_import = ImportExportPolicy::Allow(vec![
        "10.1.0.0/16".parse().unwrap(),
        "fd00:1::/64".parse().unwrap(),
    ]);
    peer_a.hold_time = Some(30);
    let peer_b = uplink_peer(65000, "qsfp0", unnumbered(3600));
    // Everything on the other switch must be filtered out, including the
    // conflicting ASN.
    let other_switch_peer =
        uplink_peer(64999, "qsfp1", numbered("203.0.113.1"));

    let routes = vec![
        RouteConfig {
            destination: "0.0.0.0/0".parse().unwrap(),
            nexthop: "192.0.2.254".parse().unwrap(),
            vlan_id: Some(5),
            rib_priority: Some(7),
        },
        // v4 prefix with a v6 nexthop (v4-over-v6) is valid.
        RouteConfig {
            destination: "10.1.0.0/16".parse().unwrap(),
            nexthop: "fe80::1".parse().unwrap(),
            vlan_id: None,
            rib_priority: None,
        },
        RouteConfig {
            destination: "fd00:1::/64".parse().unwrap(),
            nexthop: "fe80::2".parse().unwrap(),
            vlan_id: None,
            rib_priority: None,
        },
    ];
    let other_switch_route = RouteConfig {
        destination: "192.168.0.0/16".parse().unwrap(),
        nexthop: "192.168.1.1".parse().unwrap(),
        vlan_id: None,
        rib_priority: None,
    };

    let bfd = vec![
        UplinkBfdPeerConfig {
            local: None,
            remote: "192.0.2.7".parse().unwrap(),
            detection_threshold: 3,
            required_rx: 100,
            mode: BfdMode::SingleHop,
            switch: SwitchSlot::Switch0,
        },
        UplinkBfdPeerConfig {
            local: None,
            remote: "192.0.2.8".parse().unwrap(),
            detection_threshold: 3,
            required_rx: 100,
            mode: BfdMode::SingleHop,
            switch: SwitchSlot::Switch1,
        },
    ];

    let config = system_config(
        rack_config(
            vec![
                port_config(
                    SwitchSlot::Switch0,
                    "qsfp0",
                    vec![peer_a, peer_b],
                    routes,
                ),
                port_config(
                    SwitchSlot::Switch1,
                    "qsfp1",
                    vec![other_switch_peer],
                    vec![other_switch_route],
                ),
            ],
            vec![
                BgpConfig {
                    asn: 65000,
                    originate: vec![
                        "198.51.100.0/24".parse().unwrap(),
                        "fd00:99::/32".parse().unwrap(),
                    ],
                    shaper: Some("shaper code".to_string()),
                    checker: Some("checker code".to_string()),
                    max_paths: MaxPathConfig::new(4).unwrap(),
                },
                BgpConfig {
                    asn: 64999,
                    originate: Vec::new(),
                    shaper: None,
                    checker: None,
                    max_paths: MaxPathConfig::default(),
                },
            ],
            bfd,
        ),
        SwitchRouterConfigs::new(),
    );

    let routers =
        render_desired_routers(&config, ThisSledSwitchSlot::TEST_FAKE)
            .expect("rendered routers");
    assert_eq!(routers.len(), 1);
    let router = &routers[0];
    assert_eq!(router.name, DEFAULT_ROUTER_NAME);
    assert_eq!(router.id.0, Uuid::nil());

    let bgp = router.bgp.as_ref().expect("default router has BGP");
    assert_eq!(bgp.asn, 65000);
    assert_eq!(bgp.id, 65000);
    assert_eq!(bgp.listen, "[::]:179");
    assert_eq!(bgp.originate.len(), 2);
    assert_eq!(bgp.max_paths, NonZeroU8::new(4));
    let checker = bgp.checker.as_ref().unwrap();
    assert_eq!((checker.asn, checker.code.as_str()), (65000, "checker code"));
    let shaper = bgp.shaper.as_ref().unwrap();
    assert_eq!((shaper.asn, shaper.code.as_str()), (65000, "shaper code"));

    // Numbered peer: keyed by port, named by IP, standard BGP port.
    assert_eq!(bgp.peers.len(), 1);
    let group = &bgp.peers["qsfp0"];
    assert_eq!(group.len(), 1);
    let peer = &group[0];
    assert_eq!(peer.name, "192.0.2.1");
    assert_eq!(peer.host.ip().to_string(), "192.0.2.1");
    assert_eq!(peer.host.port(), BGP_PORT);
    assert_eq!(peer.parameters.hold_time, 30);
    assert_eq!(
        peer.parameters.idle_hold_time,
        UplinkBgpPeerConfig::DEFAULT_IDLE_HOLD_TIME
    );
    assert_eq!(peer.parameters.resolution, BGP_SESSION_RESOLUTION);
    // The allowed_import list splits per address family.
    let v4 = peer.parameters.ipv4_unicast.as_ref().unwrap();
    assert_matches!(
        &v4.import_policy,
        MgdImportExportPolicy4::Allow(nets)
            if nets.len() == 1 && nets.contains(&"10.1.0.0/16".parse().unwrap())
    );
    assert_matches!(&v4.export_policy, MgdImportExportPolicy4::NoFiltering);
    let v6 = peer.parameters.ipv6_unicast.as_ref().unwrap();
    assert_matches!(
        &v6.import_policy,
        MgdImportExportPolicy6::Allow(nets)
            if nets.len() == 1 && nets.contains(&"fd00:1::/64".parse().unwrap())
    );
    assert_matches!(&v6.export_policy, MgdImportExportPolicy6::NoFiltering);

    // Unnumbered peer: tfport-derived interface name.
    assert_eq!(bgp.unnumbered_peers.len(), 1);
    let group = &bgp.unnumbered_peers["qsfp0"];
    assert_eq!(group.len(), 1);
    let peer = &group[0];
    assert_eq!(peer.interface, "tfportqsfp0_0");
    assert_eq!(peer.name, "unnumbered-qsfp0");
    assert_eq!(peer.router_lifetime, 3600);

    // Static routes: the other switch's route is filtered out.
    assert_eq!(router.static4.len(), 2);
    let default_route = &router.static4[0];
    assert_eq!(default_route.prefix, "0.0.0.0/0".parse().unwrap());
    assert_eq!(default_route.nexthop, "192.0.2.254".parse::<IpAddr>().unwrap());
    assert_eq!(default_route.vlan_id, Some(5));
    assert_eq!(default_route.rib_priority, 7);
    let v4_over_v6 = &router.static4[1];
    assert_eq!(v4_over_v6.prefix, "10.1.0.0/16".parse().unwrap());
    assert_eq!(v4_over_v6.nexthop, "fe80::1".parse::<IpAddr>().unwrap());
    assert_eq!(v4_over_v6.rib_priority, DEFAULT_RIB_PRIORITY_STATIC);
    assert_eq!(router.static6.len(), 1);
    assert_eq!(router.static6[0].prefix, "fd00:1::/64".parse().unwrap());
    assert_eq!(
        router.static6[0].nexthop,
        "fe80::2".parse::<Ipv6Addr>().unwrap()
    );

    // BFD: the other switch's peer is filtered out; unset local address
    // becomes the family-matching unspecified listen address.
    assert_eq!(router.bfd_peers.len(), 1);
    let bfd_peer = &router.bfd_peers[0];
    assert_eq!(bfd_peer.peer, "192.0.2.7".parse::<IpAddr>().unwrap());
    assert_eq!(bfd_peer.listen, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    assert_eq!(bfd_peer.required_rx, 100);
    assert_eq!(bfd_peer.detection_threshold, NonZeroU8::new(3).unwrap());
    assert_matches!(bfd_peer.mode, MgdSessionMode::SingleHop);
}

#[test]
fn uplink_render_no_bgp() {
    let config = system_config(
        rack_config(
            vec![port_config(
                SwitchSlot::Switch0,
                "qsfp0",
                Vec::new(),
                Vec::new(),
            )],
            Vec::new(),
            Vec::new(),
        ),
        SwitchRouterConfigs::new(),
    );

    let routers =
        render_desired_routers(&config, ThisSledSwitchSlot::TEST_FAKE)
            .expect("rendered routers");
    assert_eq!(routers.len(), 1);
    let router = &routers[0];
    assert_eq!(router.name, DEFAULT_ROUTER_NAME);
    assert!(router.bgp.is_none());
    assert!(router.static4.is_empty());
    assert!(router.static6.is_empty());
    assert!(router.bfd_peers.is_empty());
}

#[test]
fn uplink_render_multiple_asns_is_an_error() {
    let config = system_config(
        rack_config(
            vec![port_config(
                SwitchSlot::Switch0,
                "qsfp0",
                vec![
                    uplink_peer(65000, "qsfp0", numbered("192.0.2.1")),
                    uplink_peer(65001, "qsfp0", numbered("192.0.2.2")),
                ],
                Vec::new(),
            )],
            Vec::new(),
            Vec::new(),
        ),
        SwitchRouterConfigs::new(),
    );

    let err = render_desired_routers(&config, ThisSledSwitchSlot::TEST_FAKE)
        .expect_err("multiple ASNs should fail");
    assert!(err.to_string().contains("multiple ASNs"), "{err:#}");
}

#[test]
fn uplink_render_missing_bgp_config_is_an_error() {
    let config = system_config(
        rack_config(
            vec![port_config(
                SwitchSlot::Switch0,
                "qsfp0",
                vec![uplink_peer(65000, "qsfp0", numbered("192.0.2.1"))],
                Vec::new(),
            )],
            Vec::new(),
            Vec::new(),
        ),
        SwitchRouterConfigs::new(),
    );

    let err = render_desired_routers(&config, ThisSledSwitchSlot::TEST_FAKE)
        .expect_err("missing BGP config should fail");
    assert!(
        err.to_string().contains("doesn't have a corresponding BGP config"),
        "{err:#}"
    );
}

#[test]
fn uplink_render_v4_nexthop_for_v6_prefix_is_an_error() {
    let config = system_config(
        rack_config(
            vec![port_config(
                SwitchSlot::Switch0,
                "qsfp0",
                Vec::new(),
                vec![RouteConfig {
                    destination: "fd00:1::/64".parse().unwrap(),
                    nexthop: "10.0.0.1".parse().unwrap(),
                    vlan_id: None,
                    rib_priority: None,
                }],
            )],
            Vec::new(),
            Vec::new(),
        ),
        SwitchRouterConfigs::new(),
    );

    let err = render_desired_routers(&config, ThisSledSwitchSlot::TEST_FAKE)
        .expect_err("v4 nexthop for v6 prefix should fail");
    assert!(err.to_string().contains("unsupported mix"), "{err:#}");
}

#[test]
fn uplink_render_duplicate_peers_are_an_error() {
    fn bgp_config() -> BgpConfig {
        BgpConfig {
            asn: 65000,
            originate: Vec::new(),
            shaper: None,
            checker: None,
            max_paths: MaxPathConfig::default(),
        }
    }

    let duplicate_numbered = system_config(
        rack_config(
            vec![
                port_config(
                    SwitchSlot::Switch0,
                    "qsfp0",
                    vec![uplink_peer(65000, "qsfp0", numbered("192.0.2.1"))],
                    Vec::new(),
                ),
                port_config(
                    SwitchSlot::Switch0,
                    "qsfp1",
                    vec![uplink_peer(65000, "qsfp1", numbered("192.0.2.1"))],
                    Vec::new(),
                ),
            ],
            vec![bgp_config()],
            Vec::new(),
        ),
        SwitchRouterConfigs::new(),
    );
    let err = render_desired_routers(
        &duplicate_numbered,
        ThisSledSwitchSlot::TEST_FAKE,
    )
    .expect_err("duplicate numbered peers should fail");
    assert!(err.to_string().contains("multiple numbered peers"), "{err:#}");

    let duplicate_unnumbered = system_config(
        rack_config(
            vec![port_config(
                SwitchSlot::Switch0,
                "qsfp0",
                vec![
                    uplink_peer(65000, "qsfp0", unnumbered(3600)),
                    uplink_peer(65000, "qsfp0", unnumbered(1800)),
                ],
                Vec::new(),
            )],
            vec![bgp_config()],
            Vec::new(),
        ),
        SwitchRouterConfigs::new(),
    );
    let err = render_desired_routers(
        &duplicate_unnumbered,
        ThisSledSwitchSlot::TEST_FAKE,
    )
    .expect_err("duplicate unnumbered peers should fail");
    assert!(err.to_string().contains("multiple unnumbered peers"), "{err:#}");
}

#[test]
fn operator_default_spec_replaces_uplink_render() {
    // The uplink config has BGP peers, but an operator spec named "default"
    // takes over the default router entirely.
    let spec_id = Uuid::new_v4();
    let mut spec = empty_operator_spec(DEFAULT_ROUTER_NAME);
    spec.id = spec_id;
    spec.bgp = Some(RouterConfigBgpSpec {
        asn: 65042,
        originate: vec!["203.0.113.0/24".parse().unwrap()],
        max_paths: None,
        checker: None,
        shaper: None,
        peers: vec![RouterConfigBgpPeer {
            name: "peer-a".to_string(),
            addr: "192.0.2.50".parse().unwrap(),
            parameters: operator_peer_parameters(),
        }],
        unnumbered_peers: Vec::new(),
    });

    let mut switch_router_configs = SwitchRouterConfigs::new();
    switch_router_configs.insert(SwitchSlot::Switch0, vec![spec]);

    let config = system_config(
        rack_config(
            vec![port_config(
                SwitchSlot::Switch0,
                "qsfp0",
                vec![uplink_peer(65000, "qsfp0", numbered("192.0.2.1"))],
                Vec::new(),
            )],
            vec![BgpConfig {
                asn: 65000,
                originate: Vec::new(),
                shaper: None,
                checker: None,
                max_paths: MaxPathConfig::default(),
            }],
            Vec::new(),
        ),
        switch_router_configs,
    );

    let routers =
        render_desired_routers(&config, ThisSledSwitchSlot::TEST_FAKE)
            .expect("rendered routers");
    assert_eq!(routers.len(), 1);
    let router = &routers[0];
    assert_eq!(router.name, DEFAULT_ROUTER_NAME);
    assert_eq!(router.id.0, spec_id);
    let bgp = router.bgp.as_ref().unwrap();
    assert_eq!(bgp.asn, 65042);
    // Numbered operator peers all live under the "default" group.
    assert_eq!(bgp.peers.len(), 1);
    assert_eq!(bgp.peers["default"].len(), 1);
    assert_eq!(bgp.peers["default"][0].name, "peer-a");
}

#[test]
fn operator_named_specs_are_appended_to_the_uplink_default() {
    let spec_id = Uuid::new_v4();
    let spec = RouterConfigSpec {
        name: "experiment".to_string(),
        id: spec_id,
        bgp: Some(RouterConfigBgpSpec {
            asn: 65100,
            originate: vec!["203.0.113.0/24".parse().unwrap()],
            max_paths: NonZeroU8::new(8),
            checker: Some("checker code".to_string()),
            shaper: None,
            peers: vec![RouterConfigBgpPeer {
                name: "peer-a".to_string(),
                addr: "192.0.2.50".parse().unwrap(),
                parameters: RouterConfigBgpPeerParameters {
                    hold_time: 12,
                    allowed_export: Some(vec![
                        "203.0.113.0/24".parse().unwrap(),
                    ]),
                    ..operator_peer_parameters()
                },
            }],
            unnumbered_peers: vec![RouterConfigUnnumberedBgpPeer {
                name: "u0".to_string(),
                port: "qsfp1".to_string(),
                router_lifetime: 1800,
                parameters: operator_peer_parameters(),
            }],
        }),
        static4: vec![RouterConfigStaticRoute4 {
            prefix: "10.9.0.0/16".parse().unwrap(),
            nexthop: "192.0.2.9".parse().unwrap(),
            vlan_id: None,
            rib_priority: 1,
        }],
        static6: vec![RouterConfigStaticRoute6 {
            prefix: "fd00:9::/64".parse().unwrap(),
            nexthop: "fe80::9".parse().unwrap(),
            vlan_id: Some(9),
            rib_priority: 2,
        }],
        bfd_peers: vec![RouterConfigBfdPeer {
            peer: "192.0.2.77".parse().unwrap(),
            listen: "0.0.0.0".parse().unwrap(),
            required_rx: 50,
            detection_threshold: NonZeroU8::new(3).unwrap(),
            mode: BfdMode::MultiHop,
        }],
    };

    let mut switch_router_configs = SwitchRouterConfigs::new();
    switch_router_configs.insert(SwitchSlot::Switch0, vec![spec]);

    let config = system_config(
        rack_config(
            vec![port_config(
                SwitchSlot::Switch0,
                "qsfp0",
                Vec::new(),
                Vec::new(),
            )],
            Vec::new(),
            Vec::new(),
        ),
        switch_router_configs,
    );

    let routers =
        render_desired_routers(&config, ThisSledSwitchSlot::TEST_FAKE)
            .expect("rendered routers");
    assert_eq!(routers.len(), 2);

    let experiment = &routers[0];
    assert_eq!(experiment.name, "experiment");
    assert_eq!(experiment.id.0, spec_id);
    let bgp = experiment.bgp.as_ref().unwrap();
    assert_eq!(bgp.asn, 65100);
    assert_eq!(bgp.id, 65100);
    assert_eq!(bgp.listen, "[::]:179");
    assert_eq!(bgp.max_paths, NonZeroU8::new(8));
    assert_eq!(bgp.checker.as_ref().unwrap().code, "checker code");
    assert!(bgp.shaper.is_none());

    let peer = &bgp.peers["default"][0];
    assert_eq!(peer.name, "peer-a");
    assert_eq!(peer.host.ip().to_string(), "192.0.2.50");
    assert_eq!(peer.host.port(), BGP_PORT);
    assert_eq!(peer.parameters.hold_time, 12);
    assert_eq!(peer.parameters.resolution, BGP_SESSION_RESOLUTION);
    let v4 = peer.parameters.ipv4_unicast.as_ref().unwrap();
    assert_matches!(&v4.import_policy, MgdImportExportPolicy4::NoFiltering);
    assert_matches!(
        &v4.export_policy,
        MgdImportExportPolicy4::Allow(nets) if nets.len() == 1
    );
    let v6 = peer.parameters.ipv6_unicast.as_ref().unwrap();
    // The export list only contains a v4 prefix, so the v6 side allows
    // nothing.
    assert_matches!(
        &v6.export_policy,
        MgdImportExportPolicy6::Allow(nets) if nets.is_empty()
    );

    let peer = &bgp.unnumbered_peers["qsfp1"][0];
    assert_eq!(peer.interface, "tfportqsfp1_0");
    assert_eq!(peer.name, "u0");
    assert_eq!(peer.router_lifetime, 1800);

    assert_eq!(experiment.static4.len(), 1);
    assert_eq!(experiment.static4[0].prefix, "10.9.0.0/16".parse().unwrap());
    assert_eq!(experiment.static4[0].rib_priority, 1);
    assert_eq!(experiment.static6.len(), 1);
    assert_eq!(experiment.static6[0].vlan_id, Some(9));
    assert_eq!(experiment.bfd_peers.len(), 1);
    assert_matches!(experiment.bfd_peers[0].mode, MgdSessionMode::MultiHop);

    // The uplink-rendered (empty) default router is still present.
    let default = &routers[1];
    assert_eq!(default.name, DEFAULT_ROUTER_NAME);
    assert!(default.bgp.is_none());
}

#[test]
fn operator_specs_for_the_other_switch_are_ignored() {
    let mut switch_router_configs = SwitchRouterConfigs::new();
    switch_router_configs
        .insert(SwitchSlot::Switch1, vec![empty_operator_spec("experiment")]);

    let config = system_config(
        rack_config(
            vec![port_config(
                SwitchSlot::Switch0,
                "qsfp0",
                Vec::new(),
                Vec::new(),
            )],
            Vec::new(),
            Vec::new(),
        ),
        switch_router_configs,
    );

    // TEST_FAKE is Switch0; the Switch1 specs must not be rendered.
    let routers =
        render_desired_routers(&config, ThisSledSwitchSlot::TEST_FAKE)
            .expect("rendered routers");
    assert_eq!(routers.len(), 1);
    assert_eq!(routers[0].name, DEFAULT_ROUTER_NAME);
}

#[tokio::test(flavor = "multi_thread")]
async fn mgd_apply_end_to_end() {
    let logctx =
        omicron_test_utils::dev::test_setup_log("mgd_apply_end_to_end");
    let mgsctx = gateway_test_utils::setup::test_setup(
        "mgd_apply_end_to_end",
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
    let mut reconciler = MgdReconciler {
        client: client.clone(),
        switch_slot: ThisSledSwitchSlot::TEST_FAKE,
    };

    // Config A: uplink BGP on the default router plus one operator router.
    let mut switch_router_configs = SwitchRouterConfigs::new();
    let mut experiment = empty_operator_spec("experiment");
    experiment.static4 = vec![RouterConfigStaticRoute4 {
        prefix: "10.9.0.0/16".parse().unwrap(),
        nexthop: "192.0.2.9".parse().unwrap(),
        vlan_id: None,
        rib_priority: 1,
    }];
    switch_router_configs.insert(SwitchSlot::Switch0, vec![experiment]);
    let config_a = system_config(
        rack_config(
            vec![port_config(
                SwitchSlot::Switch0,
                "qsfp0",
                vec![uplink_peer(65000, "qsfp0", numbered("192.0.2.1"))],
                Vec::new(),
            )],
            vec![BgpConfig {
                asn: 65000,
                originate: vec!["198.51.100.0/24".parse().unwrap()],
                shaper: None,
                checker: None,
                max_paths: MaxPathConfig::default(),
            }],
            Vec::new(),
        ),
        switch_router_configs,
    );

    let status = reconciler.do_reconciliation(&config_a, &logctx.log).await;
    assert_matches!(status, MgdReconcilerStatus::Success { routers: 2 });

    let routers = client.list_routers().await.unwrap().into_inner();
    let mut names = routers.iter().map(|r| r.name.as_str()).collect::<Vec<_>>();
    names.sort_unstable();
    assert_eq!(names, ["default", "experiment"]);

    // The legacy per-object reads target the default router.
    let legacy_routers = client.read_routers().await.unwrap().into_inner();
    assert_eq!(legacy_routers.len(), 1);
    let origin4 = client.read_origin4(65000).await.unwrap().into_inner();
    assert_eq!(
        origin4.prefixes,
        vec!["198.51.100.0/24".parse::<oxnet::Ipv4Net>().unwrap()]
    );
    let neighbors = client.read_neighbors(65000).await.unwrap().into_inner();
    assert_eq!(neighbors.len(), 1);

    // Config B: no uplink BGP and no operator routers. The apply must tear
    // down "experiment" and empty the default router.
    let config_b = system_config(
        rack_config(
            vec![port_config(
                SwitchSlot::Switch0,
                "qsfp0",
                Vec::new(),
                Vec::new(),
            )],
            Vec::new(),
            Vec::new(),
        ),
        SwitchRouterConfigs::new(),
    );

    let status = reconciler.do_reconciliation(&config_b, &logctx.log).await;
    assert_matches!(status, MgdReconcilerStatus::Success { routers: 1 });

    let routers = client.list_routers().await.unwrap().into_inner();
    assert_eq!(routers.len(), 1);
    assert_eq!(routers[0].name, "default");
    let legacy_routers = client.read_routers().await.unwrap().into_inner();
    assert!(legacy_routers.is_empty(), "{legacy_routers:?}");

    mgdctx.cleanup().await.expect("mgd cleanup succeeded");
    mgsctx.teardown().await;
    logctx.cleanup_successful();
}
