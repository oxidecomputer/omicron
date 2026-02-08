// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests that EarlyNetworkConfig deserializes across versions.

use std::str::FromStr;

use bootstore::schemes::v0 as bootstore;
use omicron_common::api::internal::shared::rack_init::MaxPathConfig;
use omicron_common::api::internal::shared::{LldpAdminStatus, LldpPortConfig};
use omicron_common::api::{
    external::{ImportExportPolicy, SwitchLocation},
    internal::shared::{
        BgpConfig, BgpPeerConfig, PortConfig, PortFec, PortSpeed,
        RackNetworkConfig,
    },
};
use omicron_test_utils::dev::test_setup_log;
use sled_agent_types::early_networking::{
    EarlyNetworkConfig, EarlyNetworkConfigBody,
};

const BLOB_PATH: &str = "tests/data/early_network_blobs.txt";

/// Test that previous and current versions of `EarlyNetworkConfig` blobs
/// deserialize correctly.
#[test]
fn early_network_blobs_deserialize() {
    let logctx = test_setup_log("early_network_blobs_deserialize");

    let (current_desc, current_config) = current_config_example();
    assert!(
        !current_desc.contains(',') && !current_desc.contains('\n'),
        "current_desc must not contain commas or newlines"
    );

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

        // Attempt to deserialize this blob.
        let config =
            EarlyNetworkConfig::from_str(blob_json).unwrap_or_else(|error| {
                panic!(
                    "error deserializing early_network_blobs.txt \
                    \"{blob_desc}\" (line {blob_lineno}): {error}",
                );
            });

        // Does this config match the current config?
        if blob_desc == current_desc {
            assert_eq!(
                config, current_config,
                "early_network_blobs.txt line {}: {} does not match current config",
                blob_lineno, blob_desc
            );
            current_blob_is_known = true;
        }

        // Now attempt to put this blob into a bootstore config, and deserialize that.
        let network_config = bootstore::NetworkConfig {
            generation: config.generation,
            blob: blob_json.to_owned().into(),
        };
        let config2 = EarlyNetworkConfig::deserialize_bootstore_config(
            &logctx.log,
            &network_config,
        ).unwrap_or_else(|error| {
            panic!(
                "error deserializing early_network_blobs.txt \
                \"{blob_desc}\" (line {blob_lineno}) as bootstore config: {error}",
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
        let current_blob_json = serde_json::to_string(&current_config).unwrap();
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
fn current_config_example() -> (&'static str, EarlyNetworkConfig) {
    // NOTE: the description must not contain commas or newlines.
    let description = "2026-01-22 r17";
    let config = EarlyNetworkConfig {
        generation: 114,
        schema_version: EarlyNetworkConfig::schema_version(),
        body: EarlyNetworkConfigBody {
            ntp_servers: vec![],
            rack_network_config: Some(RackNetworkConfig {
                rack_subnet: "fd00:1122:3344:100::/56".parse().unwrap(),
                infra_ip_first: "172.20.15.21".parse().unwrap(),
                infra_ip_last: "172.20.15.22".parse().unwrap(),
                ports: vec![
                    PortConfig {
                        routes: vec![],
                        addresses: vec![],
                        switch: SwitchLocation::Switch1,
                        port: "qsfp0".to_owned(),
                        uplink_port_speed: PortSpeed::Speed100G,
                        uplink_port_fec: None,
                        bgp_peers: vec![],
                        autoneg: false,
                        tx_eq: None,
                        lldp: None,
                    },
                    PortConfig {
                        routes: vec![],
                        addresses: vec![],
                        switch: SwitchLocation::Switch1,
                        port: "qsfp26".to_owned(),
                        uplink_port_speed: PortSpeed::Speed100G,
                        uplink_port_fec: Some(PortFec::Rs),
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
                    },
                    PortConfig {
                        routes: vec![],
                        addresses: vec!["172.20.15.53/29".parse().unwrap()],
                        switch: SwitchLocation::Switch1,
                        port: "qsfp18".to_owned(),
                        uplink_port_speed: PortSpeed::Speed100G,
                        uplink_port_fec: Some(PortFec::Rs),
                        bgp_peers: vec![BgpPeerConfig {
                            asn: 65002,
                            port: "qsfp18".to_owned(),
                            addr: "172.20.15.51".parse().unwrap(),
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
                            vlan_id: None,
                            router_lifetime: 0,
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
                    },
                    PortConfig {
                        routes: vec![],
                        addresses: vec!["172.20.15.45/29".parse().unwrap()],
                        switch: SwitchLocation::Switch0,
                        port: "qsfp18".to_owned(),
                        uplink_port_speed: PortSpeed::Speed100G,
                        uplink_port_fec: Some(PortFec::Rs),
                        bgp_peers: vec![BgpPeerConfig {
                            asn: 65002,
                            port: "qsfp18".to_owned(),
                            addr: "172.20.15.43".parse().unwrap(),
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
                            router_lifetime: 0,
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
                    },
                    PortConfig {
                        routes: vec![],
                        addresses: vec![],
                        switch: SwitchLocation::Switch0,
                        port: "qsfp0".to_owned(),
                        uplink_port_speed: PortSpeed::Speed100G,
                        uplink_port_fec: None,
                        bgp_peers: vec![],
                        autoneg: false,
                        tx_eq: None,
                        lldp: None,
                    },
                    PortConfig {
                        routes: vec![],
                        addresses: vec![],
                        switch: SwitchLocation::Switch0,
                        port: "qsfp26".to_owned(),
                        uplink_port_speed: PortSpeed::Speed100G,
                        uplink_port_fec: Some(PortFec::Rs),
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
                    },
                ],
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
            }),
        },
    };

    (description, config)
}
