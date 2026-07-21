// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Example data structures for use in tests and documentation.

use std::{collections::BTreeMap, net::Ipv6Addr};

use gateway_types::component::{SpState, SpType};
use gateway_types::rot::{RotSlot, RotState};
use iddqd::id_ord_map;
use maplit::btreemap;
use sled_agent_types::early_networking::{
    BgpConfig, BgpPeerConfig, LinkFec, LinkSpeed, LldpAdminStatus,
    LldpPortConfig, MaxPathConfig, RouteConfig, RouterLifetimeConfig,
    TxEqConfig,
};
use sled_hardware_types::Baseboard;

use crate::{
    inventory::{MgsV1Inventory, SpIdentifier, SpInventory},
    rack_setup::{BootstrapSledDescription, CurrentRssUserConfigInsensitive},
};
use wicketd_commission_types::rack_setup::{
    AllowedSourceIps, BgpAuthKeyId, IpRange, Ipv4Range, ManualPortConfig,
    PutRssUserConfigInsensitive, UplinkAddress, UserSpecifiedBgpPeerConfig,
    UserSpecifiedImportExportPolicy, UserSpecifiedPortConfig,
    UserSpecifiedRackNetworkConfig, UserSpecifiedRouterPeerAddr,
    UserSpecifiedUplinkAddressConfig,
};

/// A collection of example data structures.
pub struct ExampleRackSetupData {
    /// The example baseboard where wicket/wicketd is presumed to be running.
    pub our_baseboard: Option<Baseboard>,
    pub put_insensitive: PutRssUserConfigInsensitive,
    pub current_insensitive: CurrentRssUserConfigInsensitive,
    pub bgp_auth_keys: Vec<BgpAuthKeyId>,
    pub inventory: MgsV1Inventory,
    pub ddm_discovered_sleds: BTreeMap<Baseboard, Ipv6Addr>,
}

impl ExampleRackSetupData {
    /// A standard, non-empty [`ExampleRackSetupData`]. Since this is used for
    /// roundtrip testing, every field is expected to be set to some kind of
    /// non-default value.
    ///
    /// If some code paths aren't being exercised, feel free to add more data
    /// here.
    pub fn non_empty() -> Self {
        Self::new(&[])
    }

    /// An [`ExampleRackSetupData`] which is the same as `non_empty`, but with
    /// one BGP peer per port.
    pub fn one_bgp_peer() -> Self {
        Self::new(&[ExampleRackSetupDataTweak::OneBgpPeerPerPort])
    }

    fn new(tweaks: &[ExampleRackSetupDataTweak]) -> Self {
        let bgp_key_1_id: BgpAuthKeyId = "bgp-key-1".parse().unwrap();
        let bgp_key_2_id: BgpAuthKeyId = "bgp-key-2".parse().unwrap();

        // our_baseboard matches the baseboard of the first sled in
        // bootstrap_sleds.
        let our_baseboard = Baseboard::Gimlet {
            model: "model1".into(),
            revision: 3,
            identifier: "serial 1 2 3".into(),
        };

        let mut sp0 =
            SpInventory::new(SpIdentifier { slot: 1, typ: SpType::Sled });
        sp0.state = Some(SpState {
            serial_number: "serial 1 2 3".into(),
            model: "model1".into(),
            revision: 3,
            hubris_archive_id: "fake".into(),
            base_mac_address: [0u8; 6],
            power_state: gateway_types::component::PowerState::A0,
            rot: RotState::V2 {
                active: RotSlot::A,
                persistent_boot_preference: RotSlot::A,
                pending_persistent_boot_preference: None,
                transient_boot_preference: None,
                slot_a_sha3_256_digest: None,
                slot_b_sha3_256_digest: None,
            },
        });
        let mut sp1 =
            SpInventory::new(SpIdentifier { slot: 5, typ: SpType::Sled });
        sp1.state = Some(SpState {
            serial_number: "serial 4 5 6".into(),
            model: "model2".into(),
            revision: 5,
            hubris_archive_id: "fake".into(),
            base_mac_address: [0u8; 6],
            power_state: gateway_types::component::PowerState::A0,
            rot: RotState::V2 {
                active: RotSlot::A,
                persistent_boot_preference: RotSlot::A,
                pending_persistent_boot_preference: None,
                transient_boot_preference: None,
                slot_a_sha3_256_digest: None,
                slot_b_sha3_256_digest: None,
            },
        });
        let inventory = MgsV1Inventory { sps: id_ord_map! { sp0, sp1 } };

        let ddm_discovered_sleds: BTreeMap<_, _> = [
            (our_baseboard.clone(), Ipv6Addr::LOCALHOST),
            (
                Baseboard::Gimlet {
                    model: "model2".into(),
                    revision: 5,
                    identifier: "serial 4 5 6".into(),
                },
                Ipv6Addr::LOCALHOST,
            ),
        ]
        .into_iter()
        .collect();

        let bootstrap_sleds = id_ord_map! {
            BootstrapSledDescription {
                id: SpIdentifier { slot: 1, typ: SpType::Sled },
                baseboard: our_baseboard.clone(),
                bootstrap_ip: Some(Ipv6Addr::LOCALHOST)
            },
            BootstrapSledDescription {
                id: SpIdentifier { slot: 5, typ: SpType::Sled },
                baseboard: Baseboard::Gimlet {
                    model: "model2".into(),
                    revision: 5,
                    identifier: "serial 4 5 6".into(),
                },
                bootstrap_ip: None
            },
        };

        let dns_servers =
            vec!["1.1.1.1".parse().unwrap(), "2.2.2.2".parse().unwrap()];
        let external_dns_zone_name = "oxide.computer".to_owned();
        let internal_services_ip_pool_ranges = vec![IpRange::V4(Ipv4Range {
            first: "10.0.0.1".parse().unwrap(),
            last: "10.0.0.5".parse().unwrap(),
        })];
        let external_dns_ips = vec!["10.0.0.1".parse().unwrap()];
        let ntp_servers = vec!["ntp1.com".into(), "ntp2.com".into()];

        let switch0_port0_bgp_peers = vec![
            UserSpecifiedBgpPeerConfig {
                asn: 47,
                addr: UserSpecifiedRouterPeerAddr::Unnumbered,
                port: "port0".into(),
                hold_time: Some(BgpPeerConfig::DEFAULT_HOLD_TIME),
                idle_hold_time: Some(BgpPeerConfig::DEFAULT_IDLE_HOLD_TIME),
                connect_retry: Some(BgpPeerConfig::DEFAULT_CONNECT_RETRY),
                delay_open: Some(BgpPeerConfig::DEFAULT_DELAY_OPEN),
                keepalive: Some(BgpPeerConfig::DEFAULT_KEEPALIVE),
                communities: Vec::new(),
                enforce_first_as: false,
                local_pref: None,
                min_ttl: None,
                auth_key_id: Some(bgp_key_1_id.clone()),
                multi_exit_discriminator: None,
                remote_asn: None,
                allowed_import: UserSpecifiedImportExportPolicy::NoFiltering,
                allowed_export: UserSpecifiedImportExportPolicy::Allow(vec![
                    "127.0.0.1/8".parse().unwrap(),
                ]),
                vlan_id: None,
                router_lifetime: RouterLifetimeConfig::default(),
            },
            UserSpecifiedBgpPeerConfig {
                asn: 28,
                addr: UserSpecifiedRouterPeerAddr::Numbered(
                    "10.2.3.5".parse().unwrap(),
                ),
                port: "port0".into(),
                remote_asn: Some(200),
                hold_time: Some(10),
                idle_hold_time: Some(20),
                connect_retry: Some(30),
                delay_open: Some(40),
                keepalive: Some(50),
                communities: vec![60, 70],
                enforce_first_as: true,
                local_pref: Some(80),
                min_ttl: Some(90),
                auth_key_id: Some(bgp_key_2_id.clone()),
                multi_exit_discriminator: Some(100),
                allowed_import: UserSpecifiedImportExportPolicy::Allow(vec![
                    "64:ff9b::/96".parse().unwrap(),
                    "255.255.0.0/16".parse().unwrap(),
                ]),
                allowed_export: UserSpecifiedImportExportPolicy::Allow(vec![]),
                vlan_id: None,
                router_lifetime: RouterLifetimeConfig::default(),
            },
        ];

        let switch1_port0_bgp_peers = vec![UserSpecifiedBgpPeerConfig {
            asn: 47,
            addr: UserSpecifiedRouterPeerAddr::Numbered(
                "10.2.3.4".parse().unwrap(),
            ),
            port: "port0".into(),
            hold_time: Some(BgpPeerConfig::DEFAULT_HOLD_TIME),
            idle_hold_time: Some(BgpPeerConfig::DEFAULT_IDLE_HOLD_TIME),
            connect_retry: Some(BgpPeerConfig::DEFAULT_CONNECT_RETRY),
            delay_open: Some(BgpPeerConfig::DEFAULT_DELAY_OPEN),
            keepalive: Some(BgpPeerConfig::DEFAULT_KEEPALIVE),
            communities: Vec::new(),
            enforce_first_as: false,
            local_pref: None,
            min_ttl: None,
            auth_key_id: Some(bgp_key_1_id.clone()),
            multi_exit_discriminator: None,
            remote_asn: None,
            allowed_import: UserSpecifiedImportExportPolicy::Allow(vec![
                "224.0.0.0/4".parse().unwrap(),
            ]),
            allowed_export: UserSpecifiedImportExportPolicy::NoFiltering,
            vlan_id: None,
            router_lifetime: RouterLifetimeConfig::default(),
        }];

        let switch0_port0_lldp = Some(LldpPortConfig {
            status: LldpAdminStatus::Enabled,
            chassis_id: Some("chassid id override".to_string()),
            port_id: Some("port id override".to_string()),
            system_name: Some("system name override".to_string()),
            system_description: Some("system description override".to_string()),
            port_description: Some("port description override".to_string()),
            management_addrs: None,
        });

        let tx_eq = Some(TxEqConfig {
            pre1: Some(0),
            pre2: Some(0),
            main: Some(26),
            post2: Some(0),
            post1: Some(0),
        });

        let switch1_port0_lldp = Some(LldpPortConfig {
            status: LldpAdminStatus::Enabled,
            chassis_id: Some("chassid id override".to_string()),
            port_id: Some("port id override".to_string()),
            system_name: Some("system name override".to_string()),
            system_description: Some("system description override".to_string()),
            port_description: Some("port description override".to_string()),
            management_addrs: Some(vec!["172.32.0.4".parse().unwrap()]),
        });

        let rack_subnet_address =
            Some(Ipv6Addr::new(0xfd00, 0x1122, 0x3344, 0x0100, 0, 0, 0, 0));

        let rack_network_config = UserSpecifiedRackNetworkConfig {
            rack_subnet_address,
            infra_ip_first: "172.30.0.1".parse().unwrap(),
            infra_ip_last: "172.30.0.10".parse().unwrap(),
            #[rustfmt::skip]
            switch0: btreemap! {
                "port0".to_owned() => UserSpecifiedPortConfig::Manual(ManualPortConfig {
                    addresses: vec![UserSpecifiedUplinkAddressConfig {
                        address: UplinkAddress::AddrConf,
                        vlan_id: Some(1),
                    }],
                    routes: vec![RouteConfig {
                        destination: "0.0.0.0/0".parse().unwrap(),
                        nexthop: "172.30.0.10".parse().unwrap(),
                        vlan_id: Some(1),
                        rib_priority: None,
                    }],
                    bgp_peers: switch0_port0_bgp_peers,
                    uplink_port_speed: LinkSpeed::Speed400G,
                    uplink_port_fec: Some(LinkFec::Firecode),
                    lldp: switch0_port0_lldp,
                    tx_eq,
                    autoneg: true,
                }),
            },
            #[rustfmt::skip]
            switch1: btreemap! {
                // Use the same port name as in switch0 to test that it doesn't
                // collide.
                "port0".to_owned() => UserSpecifiedPortConfig::Manual(ManualPortConfig {
                    addresses: vec![UserSpecifiedUplinkAddressConfig::without_vlan(
                        "172.30.0.1/24".parse().unwrap(),
                    )],
                    routes: vec![RouteConfig {
                        destination: "0.0.0.0/0".parse().unwrap(),
                        nexthop: "172.33.0.10".parse().unwrap(),
                        vlan_id: Some(1),
                        rib_priority: None,
                    }],
                    bgp_peers: switch1_port0_bgp_peers,
                    uplink_port_speed: LinkSpeed::Speed400G,
                    uplink_port_fec: None,
                    lldp: switch1_port0_lldp,
                    tx_eq,
                    autoneg: true,
                }),
            },
            bgp: vec![BgpConfig {
                asn: 47,
                originate: vec!["10.0.0.0/16".parse().unwrap()],
                shaper: None,
                checker: None,
                max_paths: MaxPathConfig::default(),
            }],
        };

        let mut current_insensitive = CurrentRssUserConfigInsensitive {
            bootstrap_sleds,
            dns_servers,
            external_dns_zone_name,
            internal_services_ip_pool_ranges,
            external_dns_ips,
            ntp_servers,
            rack_network_config: Some(rack_network_config),
            allowed_source_ips: Some(AllowedSourceIps::Any),
            external_jumbo_frames_opt_in_enabled: false,
        };

        for tweak in tweaks {
            apply_tweak(&mut current_insensitive, tweak.clone());
        }

        // Build the list of BGP auth keys from the tweaked data.
        let bgp_auth_keys = current_insensitive
            .rack_network_config
            .as_ref()
            .unwrap()
            .get_bgp_auth_key_ids();

        // Build the PutRssUserConfigInsensitive from the tweaked data.
        let put_insensitive = PutRssUserConfigInsensitive {
            bootstrap_sleds: current_insensitive
                .bootstrap_sleds
                .iter()
                .map(|sled| sled.id.slot)
                .collect(),
            dns_servers: current_insensitive.dns_servers.clone(),
            external_dns_zone_name: current_insensitive
                .external_dns_zone_name
                .clone(),
            internal_services_ip_pool_ranges: current_insensitive
                .internal_services_ip_pool_ranges
                .clone(),
            external_dns_ips: current_insensitive.external_dns_ips.clone(),
            ntp_servers: current_insensitive.ntp_servers.clone(),
            rack_network_config: current_insensitive
                .rack_network_config
                .clone()
                .unwrap(),
            allowed_source_ips: AllowedSourceIps::Any,
            external_jumbo_frames_opt_in_enabled: false,
        };

        Self {
            our_baseboard: Some(our_baseboard),
            current_insensitive,
            put_insensitive,
            bgp_auth_keys: bgp_auth_keys.into_iter().collect(),
            inventory,
            ddm_discovered_sleds,
        }
    }
}

/// A modification that can be applied to an [`ExampleRackSetupData`].
#[derive(Debug, Clone, PartialEq, Eq)]
enum ExampleRackSetupDataTweak {
    /// Only retain one BGP peer per port.
    OneBgpPeerPerPort,
}

fn apply_tweak(
    current_insensitive: &mut CurrentRssUserConfigInsensitive,
    tweak: ExampleRackSetupDataTweak,
) {
    match tweak {
        ExampleRackSetupDataTweak::OneBgpPeerPerPort => {
            let rnc = current_insensitive.rack_network_config.as_mut().unwrap();
            for (_, _, port) in rnc.iter_uplinks_mut() {
                // Remove all but the first BGP peer.
                let UserSpecifiedPortConfig::Manual(port) = port else {
                    unimplemented!("DdmAutoPortConfig currently unsupported")
                };
                port.bgp_peers.drain(1..);
            }
        }
    }
}
