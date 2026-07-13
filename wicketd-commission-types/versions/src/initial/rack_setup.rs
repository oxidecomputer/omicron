// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack setup (RSS) types for the commissioning API.
//!
//! The RSS configuration tree (rooted at [`PutRssUserConfigInsensitive`]) is
//! copied verbatim from `wicket-common`, `sled-agent-types`, and
//! `omicron-common` so that its serde shape is byte-for-byte compatible with
//! the internal types (see the round-trip tests in wicketd). The functional
//! machinery of the originals (validation error types, inherent methods, and
//! conversions to internal types) lives elsewhere: validation and conversion
//! happen at the wicketd boundary.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::{IpAddr, Ipv6Addr};

use omicron_common::api::external::Name;
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, Serializer};
use slog_error_chain::InlineErrorChain;

// Re-exports of pinned types from sled-agent-types-versions.
pub use sled_agent_types_versions::v1::early_networking::{
    LinkFec, LinkSpeed, LldpAdminStatus, LldpPortConfig, RouteConfig,
    TxEqConfig,
};
pub use sled_agent_types_versions::v20::early_networking::{
    BgpConfig, MaxPathConfig, RouterLifetimeConfig,
};
pub use sled_agent_types_versions::v30::early_networking::{
    RouterPeerIpAddr, UplinkAddress, UplinkIpNet,
};

// Re-exports of types from omicron-common that should never change.
pub use omicron_common::address::{IpRange, Ipv4Range, Ipv6Range};
pub use omicron_common::api::internal::shared::{
    AllowedSourceIps, IpAllowList,
};

/// The portion of `CurrentRssUserConfig` that can be posted in one shot; it is
/// provided by the wicket user uploading a TOML file, currently.
///
/// This is the "write" version of `CurrentRssUserConfigInsensitive`, with
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
    pub internal_services_ip_pool_ranges: Vec<IpRange>,
    pub external_dns_ips: Vec<IpAddr>,
    pub external_dns_zone_name: String,
    pub rack_network_config: UserSpecifiedRackNetworkConfig,
    pub allowed_source_ips: AllowedSourceIps,
    /// Enable the fleet-wide jumbo-frames opt-in.
    #[serde(default)]
    pub external_jumbo_frames_opt_in_enabled: bool,
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

/// User-specified version of `PortConfig`.
///
/// All of `PortConfig` is user-specified. But we expect the port name to
/// be a key, rather than a field as in `PortConfig`. So this has all of
/// the fields other than the port name.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ManualPortConfig {
    pub routes: Vec<RouteConfig>,
    pub addresses: Vec<UserSpecifiedUplinkAddressConfig>,
    pub uplink_port_speed: LinkSpeed,
    pub uplink_port_fec: Option<LinkFec>,
    pub autoneg: bool,
    #[serde(default)]
    pub bgp_peers: Vec<UserSpecifiedBgpPeerConfig>,
    #[serde(default)]
    pub lldp: Option<LldpPortConfig>,
    #[serde(default)]
    pub tx_eq: Option<TxEqConfig>,
}

// We use `serde(untagged)` here, since the variants are vastly different.
// This prevents having backwards incompatible changes in the RSS config before
// multirack ships. Once multirack ships, we may wish to use internal tagging,
// but it's not mandatory.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case", untagged)]
#[allow(clippy::large_enum_variant)]
pub enum UserSpecifiedPortConfig {
    Manual(ManualPortConfig),
    DdmAutoPortConfig {},
}

/// User-specified version of `UplinkAddressConfig`.
///
/// This allows us to have a nicer TOML representation of [`UplinkAddress`].
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct UserSpecifiedUplinkAddressConfig {
    /// The address to be used on the uplink.
    #[serde(with = "uplink_address_serde")]
    #[schemars(with = "String")]
    pub address: UplinkAddress,
    /// The VLAN id (if any) associated with this address.
    #[serde(default)]
    pub vlan_id: Option<u16>,
}

impl UserSpecifiedUplinkAddressConfig {
    /// String representation for the "addrconf" uplink address.
    pub const ADDR_CONF: &str = "addrconf";
}

pub(crate) mod uplink_address_serde {
    use super::{UplinkAddress, UplinkIpNet, UserSpecifiedUplinkAddressConfig};
    use oxnet::IpNet;
    use serde::{Deserialize, Deserializer, Serializer};
    use slog_error_chain::InlineErrorChain;

    pub fn serialize<S: Serializer>(
        addr: &UplinkAddress,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        match addr {
            UplinkAddress::AddrConf => {
                s.serialize_str(UserSpecifiedUplinkAddressConfig::ADDR_CONF)
            }
            UplinkAddress::Static { ip_net } => {
                s.serialize_str(&ip_net.to_string())
            }
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<UplinkAddress, D::Error> {
        let s = String::deserialize(d)?;
        if s.eq_ignore_ascii_case(UserSpecifiedUplinkAddressConfig::ADDR_CONF) {
            Ok(UplinkAddress::AddrConf)
        } else {
            let ip_net: IpNet = s.parse().map_err(|_| {
                serde::de::Error::custom(format!(
                    "invalid uplink ipnet `{s}`: \
                     expected `addrconf` or an IP network",
                ))
            })?;
            let ip_net = UplinkIpNet::try_from(ip_net).map_err(|err| {
                serde::de::Error::custom(InlineErrorChain::new(&err))
            })?;
            Ok(UplinkAddress::Static { ip_net })
        }
    }
}

/// User-specified version of `BgpPeerConfig`.
///
/// This is similar to `BgpPeerConfig`, except it doesn't have the sensitive
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
    pub addr: UserSpecifiedRouterPeerAddr,
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

/// The address of a BGP peer: either `unnumbered` or an IP address.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum UserSpecifiedRouterPeerAddr {
    /// An unnumbered BGP peer.
    Unnumbered,
    /// A numbered BGP peer with the given address.
    Numbered(RouterPeerIpAddr),
}

impl UserSpecifiedRouterPeerAddr {
    /// String representation for the unnumbered peer.
    pub const UNNUMBERED_PEER: &str = "unnumbered";
}

impl JsonSchema for UserSpecifiedRouterPeerAddr {
    fn schema_name() -> String {
        "UserSpecifiedRouterPeerAddr".to_string()
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        String::json_schema(generator)
    }
}

impl Serialize for UserSpecifiedRouterPeerAddr {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Unnumbered => s.serialize_str(Self::UNNUMBERED_PEER),
            Self::Numbered(ip) => s.serialize_str(&ip.to_string()),
        }
    }
}

impl<'de> Deserialize<'de> for UserSpecifiedRouterPeerAddr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.eq_ignore_ascii_case(Self::UNNUMBERED_PEER) {
            Ok(Self::Unnumbered)
        } else {
            let ip: IpAddr = s.parse().map_err(|_| {
                serde::de::Error::custom(format!(
                    "invalid router peer address `{s}`: \
                     expected `unnumbered` or an IP address",
                ))
            })?;
            let ip = RouterPeerIpAddr::try_from(ip).map_err(|err| {
                serde::de::Error::custom(InlineErrorChain::new(&err))
            })?;
            Ok(Self::Numbered(ip))
        }
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
pub struct BgpAuthKeyId(pub(crate) Name);

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CertificateUploadResponse {
    /// The key has been uploaded, but we're waiting on its corresponding
    /// certificate chain.
    WaitingOnCert,
    /// The cert chain has been uploaded, but we're waiting on its corresponding
    /// private key.
    WaitingOnKey,
    /// A cert chain and its key have been accepted.
    CertKeyAccepted,
    /// A cert chain and its key are valid, but have already been uploaded.
    CertKeyDuplicateIgnored,
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
        Option::<Vec<IpNet>>::json_schema(r#gen)
    }
}
