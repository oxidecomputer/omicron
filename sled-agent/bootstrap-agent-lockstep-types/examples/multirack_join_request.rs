// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! One-off CLI: prints a hardcoded `MultirackJoinRequest` as JSON.

use std::collections::BTreeSet;

use bootstrap_agent_lockstep_types::MultirackJoinRequest;
use sled_agent_types::early_networking::{
    BgpConfig, BgpPeerConfig, ImportExportPolicy, LinkFec, LinkSpeed,
    LldpAdminStatus, LldpPortConfig, MaxPathConfig, NumberedRouter, PortConfig,
    RackNetworkConfig, SwitchSlot, UplinkAddressConfig, UplinkPorts,
};
use sled_hardware_types::BaseboardId;

fn main() {
    let trust_quorum_peers: BTreeSet<BaseboardId> = [
        BaseboardId {
            part_number: "913-0000019".to_owned(),
            serial_number: "2FAKE003".to_owned(),
        },
        BaseboardId {
            part_number: "913-0000019".to_owned(),
            serial_number: "2FAKE004".to_owned(),
        },
        BaseboardId {
            part_number: "913-0000019".to_owned(),
            serial_number: "2FAKE005".to_owned(),
        },
    ]
    .into_iter()
    .collect();

    let rack_network_config = RackNetworkConfig {
        rack_subnet: "fd00:1122:3344:200::/56".parse().unwrap(),
        infra_ip_first: "172.20.16.21".parse().unwrap(),
        infra_ip_last: "172.20.16.22".parse().unwrap(),
        ports: UplinkPorts::new(vec![
            PortConfig {
                routes: vec![],
                addresses: vec![UplinkAddressConfig::without_vlan(
                    "172.20.16.45/29".parse().unwrap(),
                )],
                switch: SwitchSlot::Switch0,
                port: "qsfp0".to_owned(),
                uplink_port_speed: LinkSpeed::Speed100G,
                uplink_port_fec: Some(LinkFec::Rs),
                bgp_peers: vec![BgpPeerConfig {
                    asn: 65002,
                    port: "qsfp0".to_owned(),
                    addr: NumberedRouter::new(
                        "172.20.16.43".parse().unwrap(),
                        None,
                    )
                    .expect("bgp peer addressing is valid")
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
                    allowed_export: ImportExportPolicy::NoFiltering,
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
            // A DDM port: only physical-layer settings, no layer-3
            // configuration.
            PortConfig {
                routes: vec![],
                addresses: vec![],
                switch: SwitchSlot::Switch0,
                port: "qsfp1".to_owned(),
                uplink_port_speed: LinkSpeed::Speed100G,
                uplink_port_fec: None,
                bgp_peers: vec![],
                autoneg: false,
                tx_eq: None,
                lldp: None,
                allow_ddm_traffic: true,
            },
        ])
        .expect("port list is non-empty"),
        bgp: vec![BgpConfig {
            asn: 65002,
            originate: vec!["172.20.52.0/22".parse().unwrap()],
            shaper: None,
            checker: None,
            max_paths: MaxPathConfig::default(),
        }],
        bfd: vec![],
    };

    let req = MultirackJoinRequest { trust_quorum_peers, rack_network_config };

    println!("{}", serde_json::to_string_pretty(&req).unwrap());
}
