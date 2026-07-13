// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#[cfg(test)]
mod tests {
    use crate::latest::rack_setup::{
        ManualPortConfig, UserSpecifiedImportExportPolicy,
        UserSpecifiedPortConfig, UserSpecifiedRouterPeerAddr,
        UserSpecifiedUplinkAddressConfig,
    };
    use crate::v1::rack_setup::uplink_address_serde;
    use serde::{Deserialize, Serialize};
    use sled_agent_types::early_networking::{
        LinkFec, LinkSpeed, UplinkAddress,
    };

    #[test]
    fn roundtrip_import_export_policy() {
        let inputs = [
            UserSpecifiedImportExportPolicy::Allow(vec![
                "64:ff9b::/96".parse().unwrap(),
                "255.255.0.0/16".parse().unwrap(),
            ]),
            UserSpecifiedImportExportPolicy::NoFiltering,
            UserSpecifiedImportExportPolicy::Allow(vec![]),
        ];

        for input in &inputs {
            let input = ImportExportPolicyWrapper { policy: input.clone() };

            eprintln!("** input: {:?}, testing JSON", input);
            // Check that serialization to JSON and back works.
            let serialized = serde_json::to_string(&input).unwrap();
            eprintln!("serialized JSON: {serialized}");
            let deserialized: ImportExportPolicyWrapper =
                serde_json::from_str(&serialized).unwrap();
            assert_eq!(input, deserialized);

            eprintln!("** input: {:?}, testing TOML", input);
            // Check that serialization to TOML and back works.
            let serialized = toml::to_string(&input).unwrap();
            eprintln!("serialized TOML: {serialized}");
            let deserialized: ImportExportPolicyWrapper =
                toml::from_str(&serialized).unwrap();
            assert_eq!(input, deserialized);
        }
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
    struct ImportExportPolicyWrapper {
        #[serde(default)]
        policy: UserSpecifiedImportExportPolicy,
    }

    #[test]
    fn roundtrip_router_peer_address() {
        let inputs = [
            (UserSpecifiedRouterPeerAddr::Unnumbered, "unnumbered"),
            (
                UserSpecifiedRouterPeerAddr::Numbered(
                    "1.1.1.1".parse().unwrap(),
                ),
                "1.1.1.1",
            ),
            (
                UserSpecifiedRouterPeerAddr::Numbered(
                    "fd00::1".parse().unwrap(),
                ),
                "fd00::1",
            ),
        ];

        for (input, expected_str) in inputs {
            let input = RouterPeerAddressWrapper { addr: input };

            eprintln!("** input: {:?}, testing JSON", input);
            // Check that serialization to JSON and back works.
            let serialized = serde_json::to_string(&input).unwrap();
            eprintln!("serialized JSON: {serialized}");
            let deserialized: RouterPeerAddressWrapper =
                serde_json::from_str(&serialized).unwrap();
            assert_eq!(input, deserialized);

            eprintln!("** input: {:?}, testing TOML", input);
            // Check that serialization to TOML and back works.
            let serialized = toml::to_string(&input).unwrap();
            eprintln!("serialized TOML: {serialized}");
            let deserialized: RouterPeerAddressWrapper =
                toml::from_str(&serialized).unwrap();
            assert_eq!(input, deserialized);

            assert_eq!(serialized, format!("addr = \"{expected_str}\"\n"));
        }
    }

    #[test]
    fn invalid_router_peer_address() {
        let invalid_inputs = [
            "0.0.0.0",
            "::",
            "foobar",
            "127.0.0.1",
            "255.255.255.255",
            "ff02::1",
            "fe80::1",
        ];

        for input in invalid_inputs {
            let toml_input = format!("addr = \"{input}\"\n");
            match toml::from_str::<RouterPeerAddressWrapper>(&toml_input) {
                Ok(addr) => panic!("unexpected success: parsed {addr:?}"),
                Err(err) => {
                    let err = err.to_string();
                    assert!(
                        err.contains(&format!(
                            "invalid router peer address `{input}`"
                        )),
                        "unexpected error for input `{input}`: {err}"
                    );
                }
            }
        }
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
    struct RouterPeerAddressWrapper {
        pub addr: UserSpecifiedRouterPeerAddr,
    }

    #[test]
    fn roundtrip_uplink_address() {
        let inputs = [
            (UplinkAddress::AddrConf, "addrconf"),
            (
                UplinkAddress::Static { ip_net: "1.1.1.0/24".parse().unwrap() },
                "1.1.1.0/24",
            ),
            (
                UplinkAddress::Static { ip_net: "fd00::/64".parse().unwrap() },
                "fd00::/64",
            ),
        ];

        for (input, expected_str) in inputs {
            let input = UplinkAddressWrapper { addr: input };

            eprintln!("** input: {:?}, testing JSON", input);
            // Check that serialization to JSON and back works.
            let serialized = serde_json::to_string(&input).unwrap();
            eprintln!("serialized JSON: {serialized}");
            let deserialized: UplinkAddressWrapper =
                serde_json::from_str(&serialized).unwrap();
            assert_eq!(input, deserialized);

            eprintln!("** input: {:?}, testing TOML", input);
            // Check that serialization to TOML and back works.
            let serialized = toml::to_string(&input).unwrap();
            eprintln!("serialized TOML: {serialized}");
            let deserialized: UplinkAddressWrapper =
                toml::from_str(&serialized).unwrap();
            assert_eq!(input, deserialized);

            assert_eq!(serialized, format!("addr = \"{expected_str}\"\n"));
        }
    }

    #[test]
    fn invalid_uplink_address() {
        let invalid_inputs = [
            "0.0.0.0/0",
            "::/128",
            "255.255.255.255/16",
            "ff80::1/64",
            "fe80::1/64",
            "foobar",
        ];

        for input in invalid_inputs {
            let toml_input = format!("addr = \"{input}\"\n");
            match toml::from_str::<UplinkAddressWrapper>(&toml_input) {
                Ok(addr) => panic!("unexpected success: parsed {addr:?}"),
                Err(err) => {
                    let err = err.to_string();
                    assert!(
                        err.contains(&format!(
                            "invalid uplink ipnet `{input}`"
                        )),
                        "unexpected error for input `{input}`: {err}"
                    );
                }
            }
        }
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
    struct UplinkAddressWrapper {
        // This attribute matches the one on
        // `UserSpecifiedUplinkAddressConfig::address` above.
        #[serde(with = "uplink_address_serde")]
        pub addr: UplinkAddress,
    }

    #[test]
    fn empty_map_deserializes_to_ddm_auto() {
        let from_json: UserSpecifiedPortConfig =
            serde_json::from_str("{}").unwrap();
        assert_eq!(from_json, UserSpecifiedPortConfig::DdmAutoPortConfig);

        let from_toml: PortConfigWrapper =
            toml::from_str("port = {}\n").unwrap();
        assert_eq!(from_toml.port, UserSpecifiedPortConfig::DdmAutoPortConfig);
    }

    #[test]
    fn ddm_auto_serializes_to_empty_map() {
        let config = UserSpecifiedPortConfig::DdmAutoPortConfig;

        let json = serde_json::to_string(&config).unwrap();
        assert_eq!(json, "{}");
        let roundtripped: UserSpecifiedPortConfig =
            serde_json::from_str(&json).unwrap();
        assert_eq!(roundtripped, config);

        let wrapper = PortConfigWrapper { port: config.clone() };
        let toml_str = toml::to_string(&wrapper).unwrap();
        let roundtripped: PortConfigWrapper =
            toml::from_str(&toml_str).unwrap();
        assert_eq!(roundtripped.port, config);
    }

    #[test]
    fn manual_config_roundtrips() {
        let expected = UserSpecifiedPortConfig::Manual(ManualPortConfig {
            routes: vec![],
            addresses: vec![UserSpecifiedUplinkAddressConfig::without_vlan(
                "1.1.1.0/24".parse().unwrap(),
            )],
            uplink_port_speed: LinkSpeed::Speed40G,
            uplink_port_fec: Some(LinkFec::Rs),
            autoneg: false,
            bgp_peers: vec![],
            lldp: None,
            tx_eq: None,
        });

        eprintln!("** testing JSON round-trip");
        let json = serde_json::to_string(&expected).unwrap();
        eprintln!("serialized JSON: {json}");
        let from_json: UserSpecifiedPortConfig =
            serde_json::from_str(&json).unwrap();
        assert_eq!(from_json, expected);
        assert!(from_json.manual().is_some());

        eprintln!("** testing TOML deserialization");
        let toml_manual = r#"
            routes = []
            addresses = [{ address = "1.1.1.0/24" }]
            uplink_port_speed = "speed40_g"
            uplink_port_fec = "rs"
            autoneg = false
        "#;
        let from_toml: UserSpecifiedPortConfig =
            toml::from_str(toml_manual).unwrap();
        assert_eq!(from_toml, expected);
    }

    #[test]
    fn misspelled_field_names_unknown_field() {
        let err =
            serde_json::from_str::<UserSpecifiedPortConfig>(r#"{"route": []}"#)
                .expect_err("misspelled field should fail to deserialize");
        let err = err.to_string();
        assert!(
            err.contains("unknown field `route`"),
            "error should name the unknown field, got: {err}"
        );
        assert!(
            err.contains("expected one of"),
            "error should list the expected fields, got: {err}"
        );
    }

    #[test]
    fn non_map_input_fails_cleanly() {
        let err =
            serde_json::from_str::<UserSpecifiedPortConfig>(r#""not-a-map""#)
                .expect_err("a string is not a valid port configuration");
        let err = err.to_string();
        assert!(
            err.contains("invalid type: string"),
            "error should report an invalid type, got: {err}"
        );
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
    struct PortConfigWrapper {
        port: UserSpecifiedPortConfig,
    }
}
