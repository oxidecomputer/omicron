// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2024 Oxide Computer Company

use omicron_common::address;
use omicron_common::api::external::Name;
use omicron_common::api::internal::shared::AllowedSourceIps;
use owo_colors::OwoColorize;
use owo_colors::Style;
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde::Serializer;
use sha2::Digest;
use sha2::Sha256;
use sled_agent_types::early_networking::BgpConfig;
use sled_agent_types::early_networking::BgpPeerConfig;
use sled_agent_types::early_networking::ImportExportPolicy;
use sled_agent_types::early_networking::LldpPortConfig;
use sled_agent_types::early_networking::PortFec;
use sled_agent_types::early_networking::PortSpeed;
use sled_agent_types::early_networking::RouteConfig;
use sled_agent_types::early_networking::RouterLifetimeConfig;
use sled_agent_types::early_networking::RouterPeerAddress;
use sled_agent_types::early_networking::SpecifiedIpNet;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::early_networking::TxEqConfig;
use sled_agent_types::early_networking::UplinkAddress;
use sled_agent_types::early_networking::UplinkAddressConfig;
use sled_hardware_types::Baseboard;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::str::FromStr;
use tufaceous_artifact::ArtifactHash;

use crate::inventory::SpIdentifier;

/// The subset of `RackInitializeRequest` that the user fills in as clear text
/// (e.g., via an uploaded config file).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct CurrentRssUserConfigInsensitive {
    pub bootstrap_sleds: BTreeSet<BootstrapSledDescription>,
    pub ntp_servers: Vec<String>,
    pub dns_servers: Vec<IpAddr>,
    pub internal_services_ip_pool_ranges: Vec<address::IpRange>,
    pub external_dns_ips: Vec<IpAddr>,
    pub external_dns_zone_name: String,
    pub rack_network_config: Option<UserSpecifiedRackNetworkConfig>,
    pub allowed_source_ips: Option<AllowedSourceIps>,
}

/// The portion of `CurrentRssUserConfig` that can be posted in one shot; it is
/// provided by the wicket user uploading a TOML file, currently.
///
/// This is the "write" version of [`CurrentRssUserConfigInsensitive`], with
/// some different fields.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PutRssUserConfigInsensitive {
    /// List of slot numbers only.
    ///
    /// `wicketd` will map this back to sleds with the correct `SpIdentifier`
    /// based on the `bootstrap_sleds` it provides in
    /// `CurrentRssUserConfigInsensitive`.
    pub bootstrap_sleds: BTreeSet<u16>,
    pub ntp_servers: Vec<String>,
    pub dns_servers: Vec<IpAddr>,
    pub internal_services_ip_pool_ranges: Vec<address::IpRange>,
    pub external_dns_ips: Vec<IpAddr>,
    pub external_dns_zone_name: String,
    pub rack_network_config: UserSpecifiedRackNetworkConfig,
    pub allowed_source_ips: AllowedSourceIps,
}

#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    JsonSchema,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
)]
pub struct BootstrapSledDescription {
    pub id: SpIdentifier,
    pub baseboard: Baseboard,
    /// The sled's bootstrap address, if the host is on and we've discovered it
    /// on the bootstrap network.
    pub bootstrap_ip: Option<Ipv6Addr>,
}

/// User-specified parts of `RackNetworkConfig`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UserSpecifiedRackNetworkConfig {
    pub rack_subnet_address: Option<Ipv6Addr>,
    pub infra_ip_first: IpAddr,
    pub infra_ip_last: IpAddr,
    // Map of switch -> port -> configuration, under the assumption that
    // (switch, port) is unique.
    pub switch0: BTreeMap<String, UserSpecifiedPortConfig>,
    pub switch1: BTreeMap<String, UserSpecifiedPortConfig>,
    pub bgp: Vec<BgpConfig>,
}

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
    ) -> impl Iterator<Item = (SwitchSlot, &str, &UserSpecifiedPortConfig)>
    {
        let iter0 = self
            .switch0
            .iter()
            .map(|(port, cfg)| (SwitchSlot::Switch0, port.as_str(), cfg));

        let iter1 = self
            .switch1
            .iter()
            .map(|(port, cfg)| (SwitchSlot::Switch1, port.as_str(), cfg));

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

/// User-specified version of `PortConfig`.
///
/// All of `PortConfig` is user-specified. But we expect the port name to
/// be a key, rather than a field as in `PortConfig`. So this has all of
/// the fields other than the port name.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UserSpecifiedPortConfig {
    pub routes: Vec<RouteConfig>,
    pub addresses: Vec<UserSpecifiedUplinkAddressConfig>,
    pub uplink_port_speed: PortSpeed,
    pub uplink_port_fec: Option<PortFec>,
    pub autoneg: bool,
    #[serde(default)]
    pub bgp_peers: Vec<UserSpecifiedBgpPeerConfig>,
    #[serde(default)]
    pub lldp: Option<LldpPortConfig>,
    #[serde(default)]
    pub tx_eq: Option<TxEqConfig>,
}

/// User-specified version of
/// [`sled_agent_types::early_networking::UplinkAddressConfig`].
///
/// This allows us to have a nicer TOML representation of [`UplinkAddress`].
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct UserSpecifiedUplinkAddressConfig {
    /// The address to be used on the uplink.
    // This type is used in both JSON (via OpenAPI) and TOML (for operator
    // uploads to wicket). For the TOML case specifically, we want to use a more
    // user-friendly representation, so we serialize/deserialize this field as a
    // string. See the `uplink_address_serde` module below for the specific
    // mapping.
    #[serde(with = "uplink_address_serde")]
    #[schemars(with = "String")]
    pub address: UplinkAddress,

    /// The VLAN id (if any) associated with this address.
    #[serde(default)]
    pub vlan_id: Option<u16>,
}

impl From<UserSpecifiedUplinkAddressConfig> for UplinkAddressConfig {
    fn from(value: UserSpecifiedUplinkAddressConfig) -> Self {
        Self { address: value.address, vlan_id: value.vlan_id }
    }
}

impl UserSpecifiedUplinkAddressConfig {
    /// String representation for [`UplinkAddress::LinkLocal`] when
    /// serializing/deserializing [`UserSpecifiedUplinkAddressConfig`].
    pub const LINK_LOCAL: &str = "link-local";

    /// Helper to construct a `UserSpecifiedUplinkAddressConfig` with a
    /// specified IP net and no VLAN ID.
    pub fn without_vlan(ip_net: SpecifiedIpNet) -> Self {
        Self { address: UplinkAddress::Address { ip_net }, vlan_id: None }
    }
}

/// Special handling to serialize/deserialize [`UplinkAddress`] as a flat
/// string for a nicer TOML representation.
mod uplink_address_serde {
    use super::{UplinkAddress, UserSpecifiedUplinkAddressConfig};
    use oxnet::IpNet;
    use serde::{Deserialize, Deserializer, Serializer};
    use sled_agent_types::early_networking::SpecifiedIpNet;

    pub fn serialize<S: Serializer>(
        addr: &UplinkAddress,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        match addr {
            UplinkAddress::LinkLocal => {
                s.serialize_str(UserSpecifiedUplinkAddressConfig::LINK_LOCAL)
            }
            UplinkAddress::Address { ip_net } => {
                s.serialize_str(&ip_net.to_string())
            }
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<UplinkAddress, D::Error> {
        let s = String::deserialize(d)?;
        if s.eq_ignore_ascii_case(UserSpecifiedUplinkAddressConfig::LINK_LOCAL)
        {
            Ok(UplinkAddress::LinkLocal)
        } else {
            let ip_net: IpNet = s.parse().map_err(|_| {
                serde::de::Error::custom(format!(
                    "invalid uplink address `{s}`: \
                     expected `link-local` or an IP network",
                ))
            })?;
            let ip_net = SpecifiedIpNet::try_from(ip_net).map_err(|_| {
                serde::de::Error::custom(format!(
                    "invalid uplink address `{s}`: \
                     uplink addresses cannot have an unspecified IP; \
                     use `link-local` for link local addresses",
                ))
            })?;
            Ok(UplinkAddress::Address { ip_net })
        }
    }
}

/// User-specified version of [`BgpPeerConfig`].
///
/// This is similar to [`BgpPeerConfig`], except it doesn't have the sensitive
/// `md5_auth_key` parameter, instead requiring that the user provide the key
/// separately.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UserSpecifiedBgpPeerConfig {
    /// The autonomous sysetm number of the router the peer belongs to.
    pub asn: u32,
    /// Switch port the peer is reachable on.
    pub port: String,
    /// Address of the peer.
    // This type is used in both JSON (via OpenAPI) and TOML (for operator
    // uploads to wicket). For the TOML case specifically, we want to use a more
    // user-friendly representation, so we serialize/deserialize this field as a
    // string. See the `bgp_peer_addr_serde` module below for the specific
    // mapping.
    #[serde(with = "bgp_peer_addr_serde")]
    #[schemars(with = "String")]
    pub addr: RouterPeerAddress,
    /// How long to keep a session alive without a keepalive in seconds.
    /// Defaults to 6 seconds.
    pub hold_time: Option<u64>,
    /// How long to keep a peer in idle after a state machine reset in seconds.
    /// Defaults to 3 seconds.
    pub idle_hold_time: Option<u64>,
    /// How long to delay sending open messages to a peer in seconds. Defaults
    /// to 0.
    pub delay_open: Option<u64>,
    /// The interval in seconds between peer connection retry attempts.
    /// Defaults to 3 seconds.
    pub connect_retry: Option<u64>,
    /// The interval to send keepalive messages at, in seconds. Defaults to 2
    /// seconds.
    pub keepalive: Option<u64>,
    /// Require that a peer has a specified ASN.
    #[serde(default)]
    pub remote_asn: Option<u32>,
    /// Require messages from a peer have a minimum IP time to live field.
    #[serde(default)]
    pub min_ttl: Option<u8>,
    /// The key identifier for authentication to use with the peer.
    #[serde(default)]
    pub auth_key_id: Option<BgpAuthKeyId>,
    /// Apply the provided multi-exit discriminator (MED) updates sent to the peer.
    #[serde(default)]
    pub multi_exit_discriminator: Option<u32>,
    /// Include the provided communities in updates sent to the peer.
    #[serde(default)]
    pub communities: Vec<u32>,
    /// Apply a local preference to routes received from this peer.
    #[serde(default)]
    pub local_pref: Option<u32>,
    /// Enforce that the first AS in paths received from this peer is the peer's AS.
    #[serde(default)]
    pub enforce_first_as: bool,
    /// Apply import policy to this peer with an allow list.
    #[serde(default)]
    pub allowed_import: UserSpecifiedImportExportPolicy,
    /// Apply export policy to this peer with an allow list.
    #[serde(default)]
    pub allowed_export: UserSpecifiedImportExportPolicy,
    /// Associate a VLAN ID with a BGP peer session.
    #[serde(default)]
    pub vlan_id: Option<u16>,
    /// Router lifetime in seconds for unnumbered BGP peers.
    #[serde(default)]
    pub router_lifetime: RouterLifetimeConfig,
}

/// Special handling to serialize/deserialize [`RouterPeerAddress`] as a flat
/// string for a nicer TOML representation.
mod bgp_peer_addr_serde {
    use super::{RouterPeerAddress, UserSpecifiedBgpPeerConfig};
    use serde::{Deserialize, Deserializer, Serializer};
    use sled_agent_types::early_networking::SpecifiedIpAddr;
    use std::net::IpAddr;

    pub fn serialize<S: Serializer>(
        addr: &RouterPeerAddress,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        match addr {
            RouterPeerAddress::Unnumbered => {
                s.serialize_str(UserSpecifiedBgpPeerConfig::UNNUMBERED_PEER)
            }
            RouterPeerAddress::Numbered { ip } => {
                s.serialize_str(&ip.to_string())
            }
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<RouterPeerAddress, D::Error> {
        let s = String::deserialize(d)?;
        if s.eq_ignore_ascii_case(UserSpecifiedBgpPeerConfig::UNNUMBERED_PEER) {
            Ok(RouterPeerAddress::Unnumbered)
        } else {
            let ip: IpAddr = s.parse().map_err(|_| {
                serde::de::Error::custom(format!(
                    "invalid BGP peer address `{s}`: \
                     expected `unnumbered` or an IP address",
                ))
            })?;
            let ip = SpecifiedIpAddr::try_from(ip).map_err(|_| {
                serde::de::Error::custom(format!(
                    "invalid BGP peer address `{s}`: \
                     peer address cannot be an unspecified IP; \
                     use `unnumbered` for unnumbered peers",
                ))
            })?;
            Ok(RouterPeerAddress::Numbered { ip })
        }
    }
}

impl UserSpecifiedBgpPeerConfig {
    /// String representation for [`RouterPeerAddress::Unnumbered`] when
    /// serializing/deserializing [`UserSpecifiedBgpPeerConfig`].
    pub const UNNUMBERED_PEER: &str = "unnumbered";

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

/// The key identifier for authentication to use with a BGP peer.
#[derive(
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
pub struct BgpAuthKeyId(Name);

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

/// Displayer for a slice of `fmt::Display` structs.
///
/// This is useful when `.join` is unavailable, e.g. if T doesn't implement
/// `Borrow<str>`.
#[derive(Clone, Copy, Debug)]
pub struct DisplaySlice<'a, T>(pub &'a [T]);

impl<T: fmt::Display> fmt::Display for DisplaySlice<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, id) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{id}")?;
        }
        Ok(())
    }
}

/// Describes the actual authentication key to use with a BGP peer.
///
/// Currently, only TCP-MD5 authentication is supported.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BgpAuthKey {
    /// TCP-MD5 authentication.
    TcpMd5 {
        /// The pre-shared key.
        key: String,
    },
}

impl BgpAuthKey {
    /// Returns information about the key that is safe to display in the UI.
    pub fn info(&self) -> BgpAuthKeyInfo {
        match self {
            BgpAuthKey::TcpMd5 { key } => {
                let sha256 =
                    ArtifactHash(Sha256::digest(key.as_bytes()).into());
                BgpAuthKeyInfo::TcpMd5 { sha256 }
            }
        }
    }
}

// Ensure that the key is not displayed in debug output.
impl fmt::Debug for BgpAuthKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BgpAuthKey::TcpMd5 { key: _ } => {
                f.debug_struct("TcpMd5").field("key", &"********").finish()
            }
        }
    }
}

/// Describes insensitive information about a BGP authentication key.
///
/// This information is considered okay to display in the UI.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    JsonSchema,
    PartialOrd,
    Ord,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BgpAuthKeyInfo {
    /// TCP-MD5 authentication.
    TcpMd5 {
        /// A SHA-256 digest of the key.
        // XXX we use ArtifactHash for convenience, this should be its own kind
        // of hash probably.
        sha256: ArtifactHash,
    },
}

impl BgpAuthKeyInfo {
    pub fn to_string_styled(&self, label_style: Style) -> String {
        match self {
            BgpAuthKeyInfo::TcpMd5 { sha256 } => {
                format!(
                    "{} (SHA-256: {})",
                    "TCP-MD5".style(label_style),
                    sha256.style(label_style)
                )
            }
        }
    }
}

/// Returns information about BGP keys for rack network setup.
///
/// This is part of a wicketd response, but is returned here because our
/// tooling turns BTreeMaps into HashMaps. So we use a `replace` directive
/// instead.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    JsonSchema,
    PartialOrd,
    Ord,
)]
pub struct GetBgpAuthKeyInfoResponse {
    /// Information about the requested keys.
    ///
    /// None indicates that the key ID has not been set yet. An error indicates
    /// that the key was not specified in the rack setup config.
    pub data: BTreeMap<BgpAuthKeyId, BgpAuthKeyStatus>,
}

/// The status of a BGP authentication key.
///
/// This is part of a wicketd response, but is returned here because our
/// tooling turns BTreeMaps into HashMaps. So we use a `replace` directive
/// instead.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    JsonSchema,
    PartialOrd,
    Ord,
)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum BgpAuthKeyStatus {
    /// The key was specified but hasn't been set yet.
    Unset,

    /// The key has been set.
    Set {
        /// Information about the key.
        info: BgpAuthKeyInfo,
    },
}

impl BgpAuthKeyStatus {
    /// Returns true if the key is set.
    #[inline]
    pub fn is_set(&self) -> bool {
        matches!(self, BgpAuthKeyStatus::Set { .. })
    }

    /// Returns true if the key is unset.
    #[inline]
    pub fn is_unset(&self) -> bool {
        matches!(self, BgpAuthKeyStatus::Unset)
    }
}

/// User-friendly serializer and deserializer for `ImportExportPolicy`.
///
/// This serializes the "NoFiltering" variant as `null`, and the "Allow"
/// variant as a list.
///
/// This would ordinarily just be a module used with `#[serde(with)]`, but
/// schemars requires that it be a type since it needs to know the JSON schema
/// corresponding to it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum UserSpecifiedImportExportPolicy {
    #[default]
    NoFiltering,
    Allow(Vec<IpNet>),
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

impl Serialize for UserSpecifiedImportExportPolicy {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            UserSpecifiedImportExportPolicy::NoFiltering => {
                serializer.serialize_none()
            }
            UserSpecifiedImportExportPolicy::Allow(list) => {
                list.serialize(serializer)
            }
        }
    }
}

impl<'de> Deserialize<'de> for UserSpecifiedImportExportPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct V;

        impl<'de> serde::de::Visitor<'de> for V {
            type Value = UserSpecifiedImportExportPolicy;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an array of IP prefixes, or null")
            }

            // Note: null is represented by visit_unit, not visit_none.
            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(UserSpecifiedImportExportPolicy::NoFiltering)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut list = Vec::new();
                while let Some(ipnet) = seq.next_element::<IpNet>()? {
                    list.push(ipnet);
                }
                Ok(UserSpecifiedImportExportPolicy::Allow(list))
            }
        }

        deserializer.deserialize_any(V)
    }
}

impl JsonSchema for UserSpecifiedImportExportPolicy {
    fn schema_name() -> String {
        "UserSpecifiedImportExportPolicy".to_string()
    }

    fn json_schema(
        r#gen: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        // The above is equivalent to an Option<Vec<IpNet>>.
        Option::<Vec<IpNet>>::json_schema(r#gen)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            (RouterPeerAddress::Unnumbered, "unnumbered"),
            (
                RouterPeerAddress::Numbered { ip: "1.1.1.1".parse().unwrap() },
                "1.1.1.1",
            ),
            (
                RouterPeerAddress::Numbered { ip: "ff80::1".parse().unwrap() },
                "ff80::1",
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
        let invalid_inputs = ["0.0.0.0", "::", "foobar"];

        for input in invalid_inputs {
            let toml_input = format!("addr = \"{input}\"\n");
            match toml::from_str::<RouterPeerAddressWrapper>(&toml_input) {
                Ok(addr) => panic!("unexpected success: parsed {addr:?}"),
                Err(err) => {
                    let err = err.to_string();
                    assert!(
                        err.contains(&format!(
                            "invalid BGP peer address `{input}`"
                        )),
                        "unexpected error for input `{input}`: {err}"
                    );
                }
            }
        }
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
    struct RouterPeerAddressWrapper {
        // This attribute matches the one on `UserSpecifiedBgpPeerConfig::addr`
        // above.
        #[serde(with = "bgp_peer_addr_serde")]
        pub addr: RouterPeerAddress,
    }

    #[test]
    fn roundtrip_uplink_address() {
        let inputs = [
            (UplinkAddress::LinkLocal, "link-local"),
            (
                UplinkAddress::Address {
                    ip_net: "1.1.1.0/24".parse().unwrap(),
                },
                "1.1.1.0/24",
            ),
            (
                UplinkAddress::Address { ip_net: "ff80::/64".parse().unwrap() },
                "ff80::/64",
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
        let invalid_inputs = ["0.0.0.0/0", "::/128", "foobar"];

        for input in invalid_inputs {
            let toml_input = format!("addr = \"{input}\"\n");
            match toml::from_str::<UplinkAddressWrapper>(&toml_input) {
                Ok(addr) => panic!("unexpected success: parsed {addr:?}"),
                Err(err) => {
                    let err = err.to_string();
                    assert!(
                        err.contains(&format!(
                            "invalid uplink address `{input}`"
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
