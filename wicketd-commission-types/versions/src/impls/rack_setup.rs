// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use omicron_common::api::external::Name;
use sled_agent_types_versions::latest::early_networking::{
    BgpPeerConfig, ImportExportPolicy, SwitchSlot,
};
use sled_agent_types_versions::v30::early_networking::UplinkAddressConfig;

use crate::latest::rack_setup::{
    BgpAuthKeyId, ManualPortConfig, UplinkAddress, UplinkIpNet,
    UserSpecifiedBgpPeerConfig, UserSpecifiedImportExportPolicy,
    UserSpecifiedPortConfig, UserSpecifiedRackNetworkConfig,
    UserSpecifiedUplinkAddressConfig,
};

impl UserSpecifiedRackNetworkConfig {
    /// Returns all BGP auth key IDs in the rack network config.
    pub fn get_bgp_auth_key_ids(&self) -> BTreeSet<BgpAuthKeyId> {
        self.iter_uplinks()
            .flat_map(|(_, _, cfg)| cfg.bgp_peers.iter())
            .filter_map(|peer| peer.auth_key_id.as_ref())
            .cloned()
            .collect()
    }

    /// Returns the port map for a particular switch location.
    pub fn port_map(
        &self,
        switch: SwitchSlot,
    ) -> &BTreeMap<String, UserSpecifiedPortConfig> {
        match switch {
            SwitchSlot::Switch0 => &self.switch0,
            SwitchSlot::Switch1 => &self.switch1,
        }
    }

    /// Returns true if there is at least one uplink configured.
    pub fn has_any_uplinks(&self) -> bool {
        !self.switch0.is_empty() || !self.switch1.is_empty()
    }

    /// Returns an iterator over all uplinks -- (switch, port, config) triples.
    pub fn iter_uplinks(
        &self,
    ) -> impl Iterator<Item = (SwitchSlot, &str, &ManualPortConfig)> {
        let iter0 = self.switch0.iter().filter_map(|(port, cfg)| match cfg {
            UserSpecifiedPortConfig::Manual(cfg) => {
                Some((SwitchSlot::Switch0, port.as_str(), cfg))
            }
            UserSpecifiedPortConfig::DdmAutoPortConfig {} => None,
        });

        let iter1 = self.switch1.iter().filter_map(|(port, cfg)| match cfg {
            UserSpecifiedPortConfig::Manual(cfg) => {
                Some((SwitchSlot::Switch1, port.as_str(), cfg))
            }
            UserSpecifiedPortConfig::DdmAutoPortConfig {} => None,
        });

        iter0.chain(iter1)
    }

    /// Returns a mutable iterator over all uplinks -- (switch, port, config) triples.
    pub fn iter_uplinks_mut(
        &mut self,
    ) -> impl Iterator<Item = (SwitchSlot, &str, &mut UserSpecifiedPortConfig)>
    {
        let iter0 = self
            .switch0
            .iter_mut()
            .map(|(port, cfg)| (SwitchSlot::Switch0, port.as_str(), cfg));

        let iter1 = self
            .switch1
            .iter_mut()
            .map(|(port, cfg)| (SwitchSlot::Switch1, port.as_str(), cfg));

        iter0.chain(iter1)
    }
}

impl UserSpecifiedPortConfig {
    pub fn manual(&self) -> Option<&ManualPortConfig> {
        match self {
            Self::Manual(cfg) => Some(cfg),
            Self::DdmAutoPortConfig {} => None,
        }
    }

    pub fn manual_mut(&mut self) -> Option<&mut ManualPortConfig> {
        match self {
            Self::Manual(cfg) => Some(cfg),
            Self::DdmAutoPortConfig {} => None,
        }
    }
}

impl From<UserSpecifiedUplinkAddressConfig> for UplinkAddressConfig {
    fn from(value: UserSpecifiedUplinkAddressConfig) -> Self {
        Self { address: value.address, vlan_id: value.vlan_id }
    }
}

impl UserSpecifiedUplinkAddressConfig {
    /// Helper to construct a `UserSpecifiedUplinkAddressConfig` with a
    /// specified IP net and no VLAN ID.
    pub fn without_vlan(ip_net: UplinkIpNet) -> Self {
        Self { address: UplinkAddress::Static { ip_net }, vlan_id: None }
    }
}

impl UserSpecifiedBgpPeerConfig {
    pub fn hold_time(&self) -> u64 {
        self.hold_time.unwrap_or(BgpPeerConfig::DEFAULT_HOLD_TIME)
    }

    pub fn idle_hold_time(&self) -> u64 {
        self.idle_hold_time.unwrap_or(BgpPeerConfig::DEFAULT_IDLE_HOLD_TIME)
    }

    pub fn delay_open(&self) -> u64 {
        self.delay_open.unwrap_or(BgpPeerConfig::DEFAULT_DELAY_OPEN)
    }

    pub fn connect_retry(&self) -> u64 {
        self.connect_retry.unwrap_or(BgpPeerConfig::DEFAULT_CONNECT_RETRY)
    }

    pub fn keepalive(&self) -> u64 {
        self.keepalive.unwrap_or(BgpPeerConfig::DEFAULT_KEEPALIVE)
    }
}

impl BgpAuthKeyId {
    /// Returns the key ID string.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Returns the key ID as a `Name`.
    pub fn as_name(&self) -> &Name {
        &self.0
    }
}

impl FromStr for BgpAuthKeyId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl fmt::Display for BgpAuthKeyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl From<UserSpecifiedImportExportPolicy> for ImportExportPolicy {
    fn from(policy: UserSpecifiedImportExportPolicy) -> Self {
        match policy {
            UserSpecifiedImportExportPolicy::NoFiltering => {
                ImportExportPolicy::NoFiltering
            }
            UserSpecifiedImportExportPolicy::Allow(list) => {
                ImportExportPolicy::Allow(list)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::latest::rack_setup::{
        UplinkAddress, UserSpecifiedImportExportPolicy,
        UserSpecifiedRouterPeerAddr,
    };
    use crate::v1::rack_setup::uplink_address_serde;
    use serde::{Deserialize, Serialize};

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
        let invalid_inputs =
            ["foobar", "not-an-ip", "banana", "1.2.3.4.5", "hello world"];

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
        let invalid_inputs =
            ["foobar", "not-an-ipnet", "banana", "1.2.3.4.5", "hello world"];

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
}
