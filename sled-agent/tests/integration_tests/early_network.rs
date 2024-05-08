// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests that EarlyNetworkConfig deserializes across versions.

use std::net::Ipv4Addr;

use bootstore::schemes::v0 as bootstore;
use omicron_common::api::{
    external::{ImportExportPolicy, SwitchLocation},
    internal::shared::{
        BgpConfig, BgpPeerConfig, PortConfigV1, PortFec, PortSpeed,
        RackNetworkConfig, RouteConfig,
    },
};
use omicron_sled_agent::bootstrap::early_networking::{
    EarlyNetworkConfig, EarlyNetworkConfigBody,
};
use omicron_test_utils::dev::test_setup_log;

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
        let config = serde_json::from_str::<EarlyNetworkConfig>(blob_json)
            .unwrap_or_else(|error| {
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
    let description = "2023-12-06 config";
    let config = EarlyNetworkConfig {
        generation: 20,
        schema_version: 1,
        body: EarlyNetworkConfigBody {
            ntp_servers: vec!["ntp.example.com".to_owned()],
            rack_network_config: Some(RackNetworkConfig {
                rack_subnet: "ff01::0/32".parse().unwrap(),
                infra_ip_first: Ipv4Addr::new(127, 0, 0, 1),
                infra_ip_last: Ipv4Addr::new(127, 1, 0, 1),
                ports: vec![PortConfigV1 {
                    routes: vec![RouteConfig {
                        destination: "10.1.9.32/16".parse().unwrap(),
                        nexthop: "10.1.9.32".parse().unwrap(),
                        vlan_id: None,
                    }],
                    addresses: vec!["2001:db8::/96".parse().unwrap()],
                    switch: SwitchLocation::Switch0,
                    port: "foo".to_owned(),
                    uplink_port_speed: PortSpeed::Speed200G,
                    uplink_port_fec: PortFec::Firecode,
                    bgp_peers: vec![BgpPeerConfig {
                        asn: 65000,
                        port: "bar".to_owned(),
                        addr: Ipv4Addr::new(1, 2, 3, 4),
                        hold_time: Some(20),
                        idle_hold_time: Some(50),
                        delay_open: None,
                        connect_retry: Some(30),
                        keepalive: Some(10),
                        remote_asn: None,
                        min_ttl: None,
                        md5_auth_key: None,
                        multi_exit_discriminator: None,
                        communities: Vec::new(),
                        local_pref: None,
                        enforce_first_as: false,
                        allowed_export: ImportExportPolicy::NoFiltering,
                        allowed_import: ImportExportPolicy::NoFiltering,
                        vlan_id: None,
                    }],
                    autoneg: true,
                }],
                bgp: vec![BgpConfig {
                    asn: 20000,
                    originate: vec!["192.168.0.0/24".parse().unwrap()],
                    shaper: None,
                    checker: None,
                }],
                bfd: vec![],
            }),
        },
    };

    (description, config)
}
