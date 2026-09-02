// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests that EarlyNetworkConfig deserializes across versions.

use bootstore::schemes::v0 as bootstore;
use iddqd::IdOrdMap;
use omicron_common::api::external::Vni;
use omicron_generation_kinds::Generation;
use omicron_test_utils::dev::test_setup_log;
use sled_agent_types::early_networking::{
    BfdMode, BgpConfig, BgpPeerConfig, EarlyNetworkConfigEnvelope,
    ImportExportPolicy, LinkFec, LinkSpeed, LldpAdminStatus, LldpPortConfig,
    MaxPathConfig, NumberedRouter, PortConfig, RackNetworkConfig,
    RouterLifetimeConfig, SwitchSlot, UnnumberedRouter, UplinkAddress,
    UplinkAddressConfig, UplinkPorts,
};
use sled_agent_types::inventory::SourceNatConfigGeneric;
use sled_agent_types::router_config::{
    RouterConfigBfdPeer, RouterConfigBgpPeer, RouterConfigBgpPeerParameters,
    RouterConfigBgpSpec, RouterConfigListEntry, RouterConfigSpec,
    RouterConfigStaticRoute4, RouterConfigStaticRoute6,
    RouterConfigUnnumberedBgpPeer,
};
use sled_agent_types::system_networking::{
    BlueprintExternalNetworkingConfig, ServiceZoneNatEntries,
    ServiceZoneNatEntry, ServiceZoneNatKind, SystemNetworkingConfig,
};
use slog_error_chain::InlineErrorChain;

const BLOB_PATH: &str = "tests/data/early_network_blobs.txt";

/// Test that previous and current versions of `EarlyNetworkConfig` blobs
/// deserialize correctly.
#[test]
fn early_network_blobs_deserialize() {
    let logctx = test_setup_log("early_network_blobs_deserialize");

    let (current_desc, current_envelope) = current_config_example();
    assert!(
        !current_desc.contains(',') && !current_desc.contains('\n'),
        "current_desc must not contain commas or newlines"
    );
    let current_config = current_envelope.deserialize_body().unwrap();

    // Read old blobs as newline-delimited JSON.
    let mut known_blobs = std::fs::read_to_string(BLOB_PATH)
        .expect("error reading early_network_blobs.txt");
    let mut current_blob_is_known = false;
    for (blob_idx, line) in known_blobs.lines().enumerate() {
        let blob_lineno = blob_idx + 1;
        let (blob_desc, blob_json) =
            line.split_once(',').unwrap_or_else(|| {
                panic!(
                    "error parsing early_network_blobs.txt \
                     line {blob_lineno}: missing comma",
                );
            });

        // Convert the blob into a bootstore type; the generation doesn't matter
        // here. (We used to duplicate it between the bootstore type and the
        // inner type, so some lines of `known_blobs` will contain a generation
        // - we ignore those.)
        let blob_network_config = bootstore::NetworkConfig {
            generation: 0,
            blob: blob_json.as_bytes().to_vec(),
        };

        // Attempt to deserialize this blob.
        let envelope = EarlyNetworkConfigEnvelope::deserialize_from_bootstore(
            &blob_network_config,
        )
        .unwrap_or_else(|error| {
            panic!(
                "error deserializing early_network_blobs.txt envelope \
                 \"{blob_desc}\" (line {blob_lineno}): {}",
                InlineErrorChain::new(&error),
            );
        });
        let config = envelope.deserialize_body().unwrap_or_else(|error| {
            panic!(
                "error deserializing early_network_blobs.txt body \
                 \"{blob_desc}\" (line {blob_lineno}): {}",
                InlineErrorChain::new(&error),
            );
        });

        // Does this config match the current config?
        if blob_desc == current_desc {
            assert_eq!(
                config, current_config,
                "early_network_blobs.txt line {blob_lineno}: \
                 {blob_desc} does not match current config",
            );
            current_blob_is_known = true;
        }

        // Now attempt to put this blob into a bootstore config, and deserialize
        // that.
        let network_config = envelope.serialize_to_bootstore_with_generation(0);
        let config2 = EarlyNetworkConfigEnvelope::deserialize_from_bootstore(
            &network_config,
        )
        .and_then(|envelope| envelope.deserialize_body())
        .unwrap_or_else(|error| {
            panic!(
                "error deserializing early_network_blobs.txt \
                 \"{blob_desc}\" (line {blob_lineno}) as bootstore config: {}",
                InlineErrorChain::new(&error),
            );
        });

        assert_eq!(
            config, config2,
            "early_network_blobs.txt line {}: {} does not match deserialization \
             as bootstore config",
            blob_lineno, blob_desc
        );
    }

    // If the current blob was not covered, add it to the list of known blobs.
    if !current_blob_is_known {
        let current_blob_json =
            serde_json::to_string(&current_envelope).unwrap();
        let current_blob = format!("{},{}", current_desc, current_blob_json);
        known_blobs.push_str(&current_blob);
        known_blobs.push('\n');
    }

    expectorate::assert_contents(BLOB_PATH, &known_blobs);

    logctx.cleanup_successful();
}

/// Returns a current version of the EarlyNetworkConfig blob, along with a
/// short description of the current version. The values can be arbitrary, but
/// this should be a nontrivial blob where no vectors are empty.
///
/// The goal is that if the definition of `EarlyNetworkConfig` changes in the
/// future, older blobs can still be deserialized correctly.
fn current_config_example() -> (&'static str, EarlyNetworkConfigEnvelope) {
    // NOTE: the description must not contain commas or newlines.
    let description = "2026-09-02 router configs in bootstore";
    let config = EarlyNetworkConfigEnvelope::from(&SystemNetworkingConfig {
        rack_network_config: RackNetworkConfig {
            rack_subnet: "fd00:1122:3344:100::/56".parse().unwrap(),
            infra_ip_first: "172.20.15.21".parse().unwrap(),
            infra_ip_last: "172.20.15.22".parse().unwrap(),
            ports: UplinkPorts::new(vec![
                PortConfig {
                    routes: vec![],
                    addresses: vec![],
                    switch: SwitchSlot::Switch1,
                    port: "qsfp0".to_owned(),
                    uplink_port_speed: LinkSpeed::Speed100G,
                    uplink_port_fec: None,
                    bgp_peers: vec![],
                    autoneg: false,
                    tx_eq: None,
                    lldp: None,
                    allow_ddm_traffic: false,
                },
                PortConfig {
                    routes: vec![],
                    addresses: vec![],
                    switch: SwitchSlot::Switch1,
                    port: "qsfp26".to_owned(),
                    uplink_port_speed: LinkSpeed::Speed100G,
                    uplink_port_fec: Some(LinkFec::Rs),
                    bgp_peers: vec![],
                    autoneg: false,
                    tx_eq: None,
                    lldp: Some(LldpPortConfig {
                        status: LldpAdminStatus::Disabled,
                        chassis_id: None,
                        port_id: None,
                        port_description: None,
                        system_name: None,
                        system_description: None,
                        management_addrs: None,
                    }),
                    allow_ddm_traffic: false,
                },
                PortConfig {
                    routes: vec![],
                    addresses: vec![UplinkAddressConfig {
                        address: UplinkAddress::AddrConf,
                        vlan_id: Some(1),
                    }],
                    switch: SwitchSlot::Switch1,
                    port: "qsfp18".to_owned(),
                    uplink_port_speed: LinkSpeed::Speed100G,
                    uplink_port_fec: Some(LinkFec::Rs),
                    bgp_peers: vec![BgpPeerConfig {
                        asn: 65002,
                        port: "qsfp18".to_owned(),
                        addr: UnnumberedRouter {
                            router_lifetime: RouterLifetimeConfig::new(1234)
                                .unwrap(),
                        }
                        .into(),
                        hold_time: Some(6),
                        idle_hold_time: Some(3),
                        delay_open: Some(3),
                        connect_retry: Some(3),
                        keepalive: Some(2),
                        remote_asn: None,
                        min_ttl: None,
                        md5_auth_key: None,
                        multi_exit_discriminator: None,
                        communities: Vec::new(),
                        local_pref: None,
                        enforce_first_as: false,
                        allowed_import: ImportExportPolicy::NoFiltering,
                        allowed_export: ImportExportPolicy::Allow(vec![
                            "172.20.52.0/22".parse().unwrap(),
                            "172.20.26.0/24".parse().unwrap(),
                        ]),
                        vlan_id: Some(1),
                    }],
                    autoneg: false,
                    tx_eq: None,
                    lldp: Some(LldpPortConfig {
                        status: LldpAdminStatus::Disabled,
                        chassis_id: None,
                        port_id: None,
                        port_description: None,
                        system_name: None,
                        system_description: None,
                        management_addrs: None,
                    }),
                    allow_ddm_traffic: false,
                },
                PortConfig {
                    routes: vec![],
                    addresses: vec![UplinkAddressConfig::without_vlan(
                        "172.20.15.45/29".parse().unwrap(),
                    )],
                    switch: SwitchSlot::Switch0,
                    port: "qsfp18".to_owned(),
                    uplink_port_speed: LinkSpeed::Speed100G,
                    uplink_port_fec: Some(LinkFec::Rs),
                    bgp_peers: vec![BgpPeerConfig {
                        asn: 65002,
                        port: "qsfp18".to_owned(),
                        addr: NumberedRouter::new(
                            "172.20.15.43".parse().unwrap(),
                            Some("172.20.15.44".parse().unwrap()),
                        )
                        .unwrap()
                        .into(),
                        hold_time: Some(6),
                        idle_hold_time: Some(0),
                        delay_open: Some(3),
                        connect_retry: Some(3),
                        keepalive: Some(2),
                        remote_asn: None,
                        min_ttl: None,
                        md5_auth_key: None,
                        multi_exit_discriminator: None,
                        communities: Vec::new(),
                        local_pref: None,
                        enforce_first_as: false,
                        allowed_import: ImportExportPolicy::NoFiltering,
                        allowed_export: ImportExportPolicy::Allow(vec![
                            "172.20.52.0/22".parse().unwrap(),
                            "172.20.26.0/24".parse().unwrap(),
                        ]),
                        vlan_id: None,
                    }],
                    autoneg: false,
                    tx_eq: None,
                    lldp: Some(LldpPortConfig {
                        status: LldpAdminStatus::Disabled,
                        chassis_id: None,
                        port_id: None,
                        port_description: None,
                        system_name: None,
                        system_description: None,
                        management_addrs: None,
                    }),
                    allow_ddm_traffic: false,
                },
                PortConfig {
                    routes: vec![],
                    addresses: vec![],
                    switch: SwitchSlot::Switch0,
                    port: "qsfp0".to_owned(),
                    uplink_port_speed: LinkSpeed::Speed100G,
                    uplink_port_fec: None,
                    bgp_peers: vec![],
                    autoneg: false,
                    tx_eq: None,
                    lldp: None,
                    allow_ddm_traffic: false,
                },
                PortConfig {
                    routes: vec![],
                    addresses: vec![],
                    switch: SwitchSlot::Switch0,
                    port: "qsfp26".to_owned(),
                    uplink_port_speed: LinkSpeed::Speed100G,
                    uplink_port_fec: Some(LinkFec::Rs),
                    bgp_peers: vec![],
                    autoneg: false,
                    tx_eq: None,
                    lldp: Some(LldpPortConfig {
                        status: LldpAdminStatus::Disabled,
                        chassis_id: None,
                        port_id: None,
                        port_description: None,
                        system_name: None,
                        system_description: None,
                        management_addrs: None,
                    }),
                    allow_ddm_traffic: false,
                },
            ])
            .expect("test port list is non-empty"),
            bgp: vec![BgpConfig {
                asn: 65002,
                originate: vec![
                    "172.20.52.0/22".parse().unwrap(),
                    "172.20.26.0/24".parse().unwrap(),
                ],
                shaper: None,
                checker: None,
                max_paths: MaxPathConfig::default(),
            }],
            bfd: vec![],
        },
        blueprint_external_networking_config: Some(
            BlueprintExternalNetworkingConfig {
                blueprint_external_networking_generation: Generation::new(),
                service_zone_nat_entries: ServiceZoneNatEntries::try_from(
                    [
                        ServiceZoneNatEntry {
                            zone_id: "b922e5ec-a05e-4d8a-8378-5277f19426bc"
                                .parse()
                                .unwrap(),
                            sled_underlay_ip: "fd00:1122:3344:103::1"
                                .parse()
                                .unwrap(),
                            nic_mac: "A8:40:25:FF:80:00".parse().unwrap(),
                            vni: Vni::SERVICES_VNI,
                            kind: ServiceZoneNatKind::BoundaryNtp {
                                snat_cfg: SourceNatConfigGeneric::new(
                                    "172.20.26.7".parse().unwrap(),
                                    0,
                                    16383,
                                )
                                .expect("valid snat cfg"),
                            },
                        },
                        ServiceZoneNatEntry {
                            zone_id: "1683d46d-69c4-4adb-a113-70eba32de76f"
                                .parse()
                                .unwrap(),
                            sled_underlay_ip: "fd00:1122:3344:101::1"
                                .parse()
                                .unwrap(),
                            nic_mac: "A8:40:25:FF:80:02".parse().unwrap(),
                            vni: Vni::SERVICES_VNI,
                            kind: ServiceZoneNatKind::BoundaryNtp {
                                snat_cfg: SourceNatConfigGeneric::new(
                                    "172.20.26.7".parse().unwrap(),
                                    16384,
                                    32767,
                                )
                                .expect("valid snat cfg"),
                            },
                        },
                        ServiceZoneNatEntry {
                            zone_id: "84be6867-c3b1-4f54-92c8-1ba3390a9ff7"
                                .parse()
                                .unwrap(),
                            sled_underlay_ip: "fd00:1122:3344:108::1"
                                .parse()
                                .unwrap(),
                            nic_mac: "A8:40:25:FF:80:05".parse().unwrap(),
                            vni: Vni::SERVICES_VNI,
                            kind: ServiceZoneNatKind::Nexus {
                                external_ip: "172.20.26.8".parse().unwrap(),
                            },
                        },
                        ServiceZoneNatEntry {
                            zone_id: "03ee5ea0-a003-4ff3-9125-bf54d41b1868"
                                .parse()
                                .unwrap(),
                            sled_underlay_ip: "fd00:1122:3344:102::1"
                                .parse()
                                .unwrap(),
                            nic_mac: "A8:40:25:FF:80:04".parse().unwrap(),
                            vni: Vni::SERVICES_VNI,
                            kind: ServiceZoneNatKind::Nexus {
                                external_ip: "172.20.26.6".parse().unwrap(),
                            },
                        },
                        ServiceZoneNatEntry {
                            zone_id: "45aa654b-77b9-4f73-b0e0-fbf1be4bf30f"
                                .parse()
                                .unwrap(),
                            sled_underlay_ip: "fd00:1122:3344:102::1"
                                .parse()
                                .unwrap(),
                            nic_mac: "A8:40:25:FF:80:03".parse().unwrap(),
                            vni: Vni::SERVICES_VNI,
                            kind: ServiceZoneNatKind::ExternalDns {
                                external_ip: "172.20.26.1".parse().unwrap(),
                            },
                        },
                        ServiceZoneNatEntry {
                            zone_id: "7d6c20e7-92ca-46b9-8ec2-9c003d05cc83"
                                .parse()
                                .unwrap(),
                            sled_underlay_ip: "fd00:1122:3344:105::1"
                                .parse()
                                .unwrap(),
                            nic_mac: "A8:40:25:FF:80:01".parse().unwrap(),
                            vni: Vni::SERVICES_VNI,
                            kind: ServiceZoneNatKind::ExternalDns {
                                external_ip: "172.20.26.2".parse().unwrap(),
                            },
                        },
                    ]
                    .into_iter()
                    .collect::<IdOrdMap<_>>(),
                )
                .expect("valid service zone NAT entries"),
            },
        ),
        switch_router_configs: [(
            SwitchSlot::Switch0,
            vec![RouterConfigSpec {
                name: "transit".to_owned(),
                id: "4f9c2ea1-53a3-4b76-9cf8-7f2be4e3e6c1".parse().unwrap(),
                bgp: Some(RouterConfigBgpSpec {
                    asn: 65010,
                    originate: vec!["172.20.60.0/24".parse().unwrap()],
                    max_paths: std::num::NonZeroU8::new(2),
                    checker: Some("fn open(msg) { \"accept\" }".to_owned()),
                    shaper: None,
                    peers: vec![RouterConfigBgpPeer {
                        name: "upstream0".to_owned(),
                        addr: "172.20.15.51".parse().unwrap(),
                        parameters: RouterConfigBgpPeerParameters {
                            hold_time: 6,
                            idle_hold_time: 3,
                            delay_open: 3,
                            connect_retry: 3,
                            keepalive: 2,
                            remote_asn: Some(65011),
                            min_ttl: None,
                            md5_auth_key: None,
                            multi_exit_discriminator: None,
                            communities: vec![100, 200],
                            local_pref: Some(100),
                            enforce_first_as: false,
                            vlan_id: None,
                            allowed_import: Some(vec![
                                "172.20.0.0/16".parse().unwrap(),
                            ]),
                            allowed_export: None,
                        },
                    }],
                    unnumbered_peers: vec![RouterConfigUnnumberedBgpPeer {
                        name: "upstream1".to_owned(),
                        port: "qsfp1".to_owned(),
                        router_lifetime: 9000,
                        parameters: RouterConfigBgpPeerParameters {
                            hold_time: 6,
                            idle_hold_time: 3,
                            delay_open: 3,
                            connect_retry: 3,
                            keepalive: 2,
                            remote_asn: None,
                            min_ttl: Some(255),
                            md5_auth_key: None,
                            multi_exit_discriminator: None,
                            communities: Vec::new(),
                            local_pref: None,
                            enforce_first_as: false,
                            vlan_id: None,
                            allowed_import: None,
                            allowed_export: Some(vec![
                                "172.20.60.0/24".parse().unwrap(),
                            ]),
                        },
                    }],
                }),
                static4: vec![RouterConfigStaticRoute4 {
                    prefix: "172.20.70.0/24".parse().unwrap(),
                    nexthop: "172.20.15.49".parse().unwrap(),
                    vlan_id: None,
                    rib_priority: 1,
                }],
                static6: vec![RouterConfigStaticRoute6 {
                    prefix: "fd00:99::/64".parse().unwrap(),
                    nexthop: "fe80::1".parse().unwrap(),
                    vlan_id: Some(2),
                    rib_priority: 1,
                }],
                bfd_peers: vec![RouterConfigBfdPeer {
                    peer: "172.20.15.51".parse().unwrap(),
                    listen: "0.0.0.0".parse().unwrap(),
                    required_rx: 100_000,
                    detection_threshold: std::num::NonZeroU8::new(3).unwrap(),
                    mode: BfdMode::SingleHop,
                }],
            }],
        )]
        .into_iter()
        .collect(),
        control_plane_router_list: vec![
            RouterConfigListEntry {
                priority: 500,
                router_id: Some(
                    "4f9c2ea1-53a3-4b76-9cf8-7f2be4e3e6c1".parse().unwrap(),
                ),
            },
            RouterConfigListEntry { priority: 1000, router_id: None },
        ],
    });

    (description, config)
}
