// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::address;
use omicron_common::api::external::Name;
use omicron_common::api::internal::shared::AllowedSourceIps;
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde::Serializer;
use sled_agent_types::early_networking::BgpConfig;
use sled_agent_types::early_networking::BgpPeerConfig;
use sled_agent_types::early_networking::ImportExportPolicy;
use sled_agent_types::early_networking::LinkFec;
use sled_agent_types::early_networking::LinkSpeed;
use sled_agent_types::early_networking::LldpPortConfig;
use sled_agent_types::early_networking::RouteConfig;
use sled_agent_types::early_networking::RouterLifetimeConfig;
use sled_agent_types::early_networking::RouterPeerIpAddr;
use sled_agent_types::early_networking::SwitchSlot;
use sled_agent_types::early_networking::TxEqConfig;
use sled_agent_types::early_networking::UplinkAddress;
use sled_agent_types::early_networking::UplinkAddressConfig;
use sled_agent_types::early_networking::UplinkIpNet;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::str::FromStr;

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
    pub internal_services_ip_pool_ranges: Vec<address::IpRange>,
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
            UserSpecifiedPortConfig::DdmAutoPortConfig => None,
        });

        let iter1 = self.switch1.iter().filter_map(|(port, cfg)| match cfg {
            UserSpecifiedPortConfig::Manual(cfg) => {
                Some((SwitchSlot::Switch1, port.as_str(), cfg))
            }
            UserSpecifiedPortConfig::DdmAutoPortConfig => None,
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

/// A user-specified port configuration.
///
/// An empty map is serialized and deserialized as an auto config.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum UserSpecifiedPortConfig {
    /// A manually-configured port.
    Manual(ManualPortConfig),
    /// A port configured automatically via DDM.
    DdmAutoPortConfig,
}

// Hand-roll Serialize and Deserialize impls so we don't have to use
// serde(untagged) and don't fall invalid configs back to auto.
//
// We may wish to switch this to internal tagging in the future, but that will
// cause changes to the TOML config as well as the JSON schema.
impl Serialize for UserSpecifiedPortConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Manual(cfg) => cfg.serialize(serializer),
            Self::DdmAutoPortConfig => {
                use serde::ser::SerializeMap;
                serializer.serialize_map(Some(0))?.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for UserSpecifiedPortConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct PortConfigVisitor;

        impl<'de> serde::de::Visitor<'de> for PortConfigVisitor {
            type Value = UserSpecifiedPortConfig;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str(
                    "a map of manual port configuration fields, or an empty \
                     map for a DDM-automatic port",
                )
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let Some(first_key) = map.next_key::<String>()? else {
                    return Ok(UserSpecifiedPortConfig::DdmAutoPortConfig);
                };

                let replay =
                    ReplayFirstKey { first_key: Some(first_key), inner: map };
                let manual = ManualPortConfig::deserialize(
                    serde::de::value::MapAccessDeserializer::new(replay),
                )?;
                Ok(UserSpecifiedPortConfig::Manual(manual))
            }
        }

        deserializer.deserialize_map(PortConfigVisitor)
    }
}

/// A `MapAccess` adaptor that yields the already-consumed first key before
/// delegating the rest of the map to the inner `MapAccess`.
struct ReplayFirstKey<A> {
    first_key: Option<String>,
    inner: A,
}

impl<'de, A> serde::de::MapAccess<'de> for ReplayFirstKey<A>
where
    A: serde::de::MapAccess<'de>,
{
    type Error = A::Error;

    fn next_key_seed<K>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        match self.first_key.take() {
            Some(first_key) => {
                use serde::de::IntoDeserializer;
                let de = first_key.into_deserializer();
                seed.deserialize(de).map(Some)
            }
            None => self.inner.next_key_seed(seed),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        self.inner.next_value_seed(seed)
    }

    fn size_hint(&self) -> Option<usize> {
        self.inner.size_hint()
    }
}

impl JsonSchema for UserSpecifiedPortConfig {
    fn schema_name() -> String {
        "UserSpecifiedPortConfig".to_string()
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        use schemars::schema::InstanceType;
        use schemars::schema::Metadata;
        use schemars::schema::ObjectValidation;
        use schemars::schema::Schema;
        use schemars::schema::SchemaObject;
        use schemars::schema::SubschemaValidation;

        let mut manual =
            generator.subschema_for::<ManualPortConfig>().into_object();
        manual.metadata().description =
            Some("A manually-configured port.".to_string());

        let ddm_auto = SchemaObject {
            metadata: Some(Box::new(Metadata {
                description: Some(
                    "A port configured automatically via DDM.".to_string(),
                ),
                ..Default::default()
            })),
            instance_type: Some(InstanceType::Object.into()),
            object: Some(Box::new(ObjectValidation {
                additional_properties: Some(Box::new(Schema::Bool(false))),
                ..Default::default()
            })),
            ..Default::default()
        };

        SchemaObject {
            metadata: Some(Box::new(Metadata {
                description: Some(
                    "A user-specified port configuration.".to_string(),
                ),
                ..Default::default()
            })),
            subschemas: Some(Box::new(SubschemaValidation {
                any_of: Some(vec![
                    Schema::Object(manual),
                    Schema::Object(ddm_auto),
                ]),
                ..Default::default()
            })),
            ..Default::default()
        }
        .into()
    }
}

impl UserSpecifiedPortConfig {
    pub fn manual(&self) -> Option<&ManualPortConfig> {
        match self {
            Self::Manual(cfg) => Some(cfg),
            Self::DdmAutoPortConfig => None,
        }
    }

    pub fn manual_mut(&mut self) -> Option<&mut ManualPortConfig> {
        match self {
            Self::Manual(cfg) => Some(cfg),
            Self::DdmAutoPortConfig => None,
        }
    }
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
    /// String representation for [`UplinkAddress::AddrConf`] when
    /// serializing/deserializing [`UserSpecifiedUplinkAddressConfig`].
    pub const ADDR_CONF: &str = "addrconf";

    /// Helper to construct a `UserSpecifiedUplinkAddressConfig` with a
    /// specified IP net and no VLAN ID.
    pub fn without_vlan(ip_net: UplinkIpNet) -> Self {
        Self { address: UplinkAddress::Static { ip_net }, vlan_id: None }
    }
}

/// Special handling to serialize/deserialize [`UplinkAddress`] as a flat
/// string for a nicer TOML representation.
pub(crate) mod uplink_address_serde {
    use super::{UplinkAddress, UserSpecifiedUplinkAddressConfig};
    use oxnet::IpNet;
    use serde::{Deserialize, Deserializer, Serializer};
    use sled_agent_types::early_networking::UplinkIpNet;
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

// Type that allows either the string "unnumbered" or an IP address. Has custom
// implementations of `JsonSchema`, `Serialize`, and `Deserialize` to support
// this.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum UserSpecifiedRouterPeerAddr {
    Unnumbered,
    Numbered(RouterPeerIpAddr),
}

impl UserSpecifiedRouterPeerAddr {
    /// String representation for [`UserSpecifiedRouterPeerAddr::Unnumbered`] in
    /// serialization.
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
