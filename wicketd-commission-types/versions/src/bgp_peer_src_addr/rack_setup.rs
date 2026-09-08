// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack setup (RSS) types for the commissioning API.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    net::{IpAddr, Ipv6Addr},
};

use iddqd::IdOrdMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, Serializer};
use sled_agent_types_versions::v30::early_networking::RouterPeerIpAddr;
use sled_agent_types_versions::{
    v1::early_networking::{
        LinkFec, LinkSpeed, LldpPortConfig, RouteConfig, TxEqConfig,
    },
    v20::early_networking::{BgpConfig, RouterLifetimeConfig},
};

use crate::{
    v1::rack_setup::{
        BgpAuthKeyId, UserSpecifiedImportExportPolicy,
        UserSpecifiedRouterPeerAddr, UserSpecifiedUplinkAddressConfig,
    },
    v2::rack_setup::ServiceIpPoolConfig,
};

// Re-exports of types from omicron-common that should never change.
pub use omicron_common::address::{IpRange, Ipv4Range, Ipv6Range};
pub use omicron_common::api::internal::shared::{
    AllowedSourceIps, IpAllowList,
};

/// User-specified configuration for a BGP peer.
///
/// This is similar to the internal BGP peer configuration, except it does not
/// carry the sensitive `md5_auth_key`; the operator provides the key
/// separately, referenced by `auth_key_id`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UserSpecifiedBgpPeerConfig {
    /// The autonomous system number of the router the peer belongs to.
    pub asn: u32,
    /// Switch port the peer is reachable on.
    pub port: String,
    /// Address of the peer.
    pub addr: UserSpecifiedRouterPeerAddr,
    /// How long to keep a session alive without a keepalive, in seconds.
    /// Defaults to 6 seconds.
    pub hold_time: Option<u64>,
    /// How long to keep a peer in idle after a state machine reset, in seconds.
    /// Defaults to 3 seconds.
    pub idle_hold_time: Option<u64>,
    /// How long to delay sending open messages to a peer, in seconds.
    /// Defaults to 0.
    pub delay_open: Option<u64>,
    /// The interval in seconds between peer connection retry attempts.
    /// Defaults to 3 seconds.
    pub connect_retry: Option<u64>,
    /// The interval to send keepalive messages at, in seconds.
    /// Defaults to 2 seconds.
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
    /// Apply the provided multi-exit discriminator (MED) updates sent to the
    /// peer.
    #[serde(default)]
    pub multi_exit_discriminator: Option<u32>,
    /// Include the provided communities in updates sent to the peer.
    #[serde(default)]
    pub communities: Vec<u32>,
    /// Apply a local preference to routes received from this peer.
    #[serde(default)]
    pub local_pref: Option<u32>,
    /// Enforce that the first AS in paths received from this peer is the peer's
    /// AS.
    #[serde(default)]
    pub enforce_first_as: bool,
    /// Apply import policy to this peer with an allow list.
    #[serde(default)]
    pub allowed_import: UserSpecifiedImportExportPolicy,
    /// Apply export policy to this peer with an allow list.
    #[serde(default)]
    pub allowed_export: UserSpecifiedImportExportPolicy,
    /// The optional source address to use for starting a
    /// BGP session with a numbered peer.
    #[serde(default)]
    pub src_addr: Option<RouterPeerIpAddr>,
    /// Associate a VLAN ID with a BGP peer session.
    #[serde(default)]
    pub vlan_id: Option<u16>,
    /// Router lifetime in seconds for unnumbered BGP peers.
    #[serde(default)]
    pub router_lifetime: RouterLifetimeConfig,
}

/// User-specified parts of the rack network configuration.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UserSpecifiedRackNetworkConfig {
    /// The rack subnet address, if statically assigned.
    pub rack_subnet_address: Option<Ipv6Addr>,
    /// The first address of the infrastructure IP range.
    pub infra_ip_first: IpAddr,
    /// The last address of the infrastructure IP range.
    pub infra_ip_last: IpAddr,
    /// Per-port configuration for switch 0, keyed by port name.
    pub switch0: BTreeMap<String, UserSpecifiedPortConfig>,
    /// Per-port configuration for switch 1, keyed by port name.
    pub switch1: BTreeMap<String, UserSpecifiedPortConfig>,
    /// BGP configuration for the rack.
    pub bgp: Vec<BgpConfig>,
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

// Hand-roll the Serialize and Deserialize impls so we don't have to use
// serde(untagged), under which invalid manual configs would silently fall back
// to the auto variant.
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
        let inner = self.inner.size_hint();
        match self.first_key {
            Some(_) => inner.map(|n| n + 1),
            None => inner,
        }
    }
}

// The descriptions and shape here must stay in sync with the variant doc
// comments and the hand-rolled Serialize/Deserialize impls above.
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

/// User-specified per-port configuration.
///
/// This contains all of the fields of a port configuration other than the port
/// name, which is used as the map key.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ManualPortConfig {
    /// Static routes for this port.
    pub routes: Vec<RouteConfig>,
    /// Addresses configured on this port.
    pub addresses: Vec<UserSpecifiedUplinkAddressConfig>,
    /// The port speed.
    pub uplink_port_speed: LinkSpeed,
    /// The forward error correction mode, if any.
    pub uplink_port_fec: Option<LinkFec>,
    /// Whether autonegotiation is enabled.
    pub autoneg: bool,
    /// BGP peers reachable on this port.
    #[serde(default)]
    pub bgp_peers: Vec<UserSpecifiedBgpPeerConfig>,
    /// LLDP configuration for this port.
    #[serde(default)]
    pub lldp: Option<LldpPortConfig>,
    /// Transmit equalization overrides for this port.
    #[serde(default)]
    pub tx_eq: Option<TxEqConfig>,
}

impl From<crate::v1::rack_setup::UserSpecifiedBgpPeerConfig>
    for UserSpecifiedBgpPeerConfig
{
    fn from(old: crate::v1::rack_setup::UserSpecifiedBgpPeerConfig) -> Self {
        Self {
            asn: old.asn,
            port: old.port,
            addr: old.addr,
            hold_time: old.hold_time,
            idle_hold_time: old.idle_hold_time,
            delay_open: old.delay_open,
            connect_retry: old.connect_retry,
            keepalive: old.keepalive,
            remote_asn: old.remote_asn,
            min_ttl: old.min_ttl,
            auth_key_id: old.auth_key_id,
            multi_exit_discriminator: old.multi_exit_discriminator,
            communities: old.communities,
            local_pref: old.local_pref,
            enforce_first_as: old.enforce_first_as,
            allowed_import: old.allowed_import,
            allowed_export: old.allowed_export,
            src_addr: None,
            vlan_id: old.vlan_id,
            router_lifetime: old.router_lifetime,
        }
    }
}

impl From<crate::v1::rack_setup::ManualPortConfig> for ManualPortConfig {
    fn from(old: crate::v1::rack_setup::ManualPortConfig) -> Self {
        Self {
            routes: old.routes,
            addresses: old.addresses,
            uplink_port_speed: old.uplink_port_speed,
            uplink_port_fec: old.uplink_port_fec,
            autoneg: old.autoneg,
            bgp_peers: old.bgp_peers.into_iter().map(From::from).collect(),
            lldp: old.lldp,
            tx_eq: old.tx_eq,
        }
    }
}

impl From<crate::v1::rack_setup::UserSpecifiedPortConfig>
    for UserSpecifiedPortConfig
{
    fn from(old: crate::v1::rack_setup::UserSpecifiedPortConfig) -> Self {
        match old {
            crate::v1::rack_setup::UserSpecifiedPortConfig::Manual(cfg) => {
                Self::Manual(cfg.into())
            }
            crate::v1::rack_setup::UserSpecifiedPortConfig::DdmAutoPortConfig => {
                Self::DdmAutoPortConfig
            }
        }
    }
}

impl From<crate::v1::rack_setup::UserSpecifiedRackNetworkConfig>
    for UserSpecifiedRackNetworkConfig
{
    fn from(
        old: crate::v1::rack_setup::UserSpecifiedRackNetworkConfig,
    ) -> Self {
        Self {
            rack_subnet_address: old.rack_subnet_address,
            infra_ip_first: old.infra_ip_first,
            infra_ip_last: old.infra_ip_last,
            switch0: old
                .switch0
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            switch1: old
                .switch1
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            bgp: old.bgp,
        }
    }
}

impl From<crate::v2::rack_setup::PutRssUserConfigInsensitive>
    for PutRssUserConfigInsensitive
{
    fn from(old: crate::v2::rack_setup::PutRssUserConfigInsensitive) -> Self {
        Self {
            bootstrap_sleds: old.bootstrap_sleds,
            ntp_servers: old.ntp_servers,
            dns_servers: old.dns_servers,
            external_dns_ips: old.external_dns_ips,
            external_dns_zone_name: old.external_dns_zone_name,
            rack_network_config: old.rack_network_config.into(),
            allowed_source_ips: old.allowed_source_ips,
            external_jumbo_frames_opt_in_enabled: old
                .external_jumbo_frames_opt_in_enabled,
            service_ip_pools: old.service_ip_pools,
        }
    }
}

/// The portion of the RSS configuration that can be posted in one shot.
///
/// It is provided by the operator uploading a TOML file. Sensitive values
/// (certificates, the recovery password hash, and BGP authentication keys) are
/// set separately.
///
/// This version updates the [`UserSpecifiedRackNetworkConfig`] to use an updated
/// bgp peer config that allows specification of the source address that will
/// be used for the TCP socket for the peering session for numbered peers.
///
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(try_from = "UnvalidatedPutRssUserConfigInsensitive")]
pub struct PutRssUserConfigInsensitive {
    /// The slot numbers of the sleds to bring up during RSS.
    ///
    /// wicketd maps these back to sleds with the correct identifiers based on
    /// the bootstrap sleds it reports.
    pub bootstrap_sleds: BTreeSet<u16>,
    /// The external NTP server addresses.
    pub ntp_servers: Vec<String>,
    /// The external DNS server addresses.
    pub dns_servers: Vec<IpAddr>,
    /// The service IP pools which may be used for internal services.
    pub service_ip_pools: IdOrdMap<ServiceIpPoolConfig>,
    /// Service IP addresses on which external DNS servers are run.
    pub external_dns_ips: Vec<IpAddr>,
    /// The DNS zone name delegated to the rack for external DNS.
    pub external_dns_zone_name: String,
    /// The user-specified rack network configuration.
    pub rack_network_config: UserSpecifiedRackNetworkConfig,
    /// IPs or subnets allowed to make requests to user-facing services.
    pub allowed_source_ips: AllowedSourceIps,
    /// Enable the fleet-wide jumbo-frames opt-in.
    #[serde(default)]
    pub external_jumbo_frames_opt_in_enabled: bool,
}

// Shadow of `PutRssUserConfigInsensitive` that deserializes the pools as a
// plain `Vec` (the natural TOML/JSON array shape) before the `TryFrom` folds
// them into an `IdOrdMap`, failing on duplicate pool names.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UnvalidatedPutRssUserConfigInsensitive {
    bootstrap_sleds: BTreeSet<u16>,
    ntp_servers: Vec<String>,
    dns_servers: Vec<IpAddr>,
    service_ip_pools: Vec<ServiceIpPoolConfig>,
    external_dns_ips: Vec<IpAddr>,
    external_dns_zone_name: String,
    rack_network_config: UserSpecifiedRackNetworkConfig,
    allowed_source_ips: AllowedSourceIps,
    #[serde(default)]
    external_jumbo_frames_opt_in_enabled: bool,
}

impl TryFrom<UnvalidatedPutRssUserConfigInsensitive>
    for PutRssUserConfigInsensitive
{
    type Error = String;

    fn try_from(
        value: UnvalidatedPutRssUserConfigInsensitive,
    ) -> Result<Self, Self::Error> {
        let service_ip_pools =
            IdOrdMap::from_iter_unique(value.service_ip_pools)
                .map_err(|e| format!("duplicate service IP pool name: {e}"))?;
        Ok(Self {
            bootstrap_sleds: value.bootstrap_sleds,
            ntp_servers: value.ntp_servers,
            dns_servers: value.dns_servers,
            service_ip_pools,
            external_dns_ips: value.external_dns_ips,
            external_dns_zone_name: value.external_dns_zone_name,
            rack_network_config: value.rack_network_config,
            allowed_source_ips: value.allowed_source_ips,
            external_jumbo_frames_opt_in_enabled: value
                .external_jumbo_frames_opt_in_enabled,
        })
    }
}
