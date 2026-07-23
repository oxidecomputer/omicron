// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack setup (RSS) types for the commissioning API.
//!
//! The root struct is [`PutRssUserConfigInsensitive`].

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::{IpAddr, Ipv6Addr};

use omicron_common::api::external::Name;
use omicron_uuid_kinds::{RackInitUuid, RackResetUuid};
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

/// The portion of the RSS configuration that can be posted in one shot.
///
/// It is provided by the operator uploading a TOML file. Sensitive values
/// (certificates, the recovery password hash, and BGP authentication keys) are
/// set separately.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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
    /// Ranges of the service IP pool which may be used for internal services.
    pub internal_services_ip_pool_ranges: Vec<IpRange>,
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

/// A user-specified uplink address configuration.
///
/// This provides a friendlier TOML representation of an uplink address.
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

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SetBgpAuthKeyStatus {
    /// The key was accepted and replaced an old key.
    Replaced,

    /// The key was accepted, and is the same as the existing key.
    Unchanged,

    /// The key was accepted and is new.
    Added,
}

/// Identifies the BGP authentication key being set.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BgpAuthKeyPath {
    /// The key ID, as referenced by a BGP peer in the RSS configuration.
    pub key_id: BgpAuthKeyId,
}

/// The result of uploading half of a certificate/key pair.
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

/// User-friendly import/export policy for a BGP peer.
///
/// Serializes as `null` for the no-filtering variant, or as a list of IP
/// prefixes for the allow variant.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum UserSpecifiedImportExportPolicy {
    /// Do not perform any filtering.
    #[default]
    NoFiltering,
    /// Only allow the listed prefixes.
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

/// A recovery-silo user password hash, in PHC string format.
///
/// This shares its name with the validated `omicron_passwords::NewPasswordHash`
/// it converts into, but holds an unvalidated string. The hash is validated (as
/// an Argon2id PHC string) only at the wicketd conversion boundary.
#[derive(Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(transparent)]
pub struct NewPasswordHash(pub String);

impl fmt::Debug for NewPasswordHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("NewPasswordHash").field(&"********").finish()
    }
}

/// The body of a request to set the recovery-silo user password hash.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PutRecoveryUserPasswordHash {
    /// The password hash, in PHC string format.
    pub hash: NewPasswordHash,
}

/// Information about the current RSS step.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct RssStepInfo {
    /// The 1-based index of the current step.
    pub step: u32,
    /// The total number of RSS steps.
    pub total_steps: u32,
    /// A human-readable description of the current step.
    pub description: String,
}

/// The current status of any rack-level operation being performed.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RackOperationStatus {
    /// Rack initialization is in progress.
    Initializing {
        /// The ID of the initialization operation.
        id: RackInitUuid,
        /// Information about the current step.
        step: RssStepInfo,
    },
    /// The rack is initialized. `id` is `None` if the rack was already
    /// initialized on startup.
    Initialized {
        /// The ID of the initialization operation, if one was performed.
        id: Option<RackInitUuid>,
    },
    /// Rack initialization failed.
    InitializationFailed {
        /// The ID of the initialization operation.
        id: RackInitUuid,
        /// A message describing the failure.
        message: String,
    },
    /// Rack initialization panicked.
    InitializationPanicked {
        /// The ID of the initialization operation.
        id: RackInitUuid,
    },
    /// The rack is being reset.
    Resetting {
        /// The ID of the reset operation.
        id: RackResetUuid,
    },
    /// The rack is uninitialized. `reset_id` is `None` if it was uninitialized
    /// on startup, or `Some` if a reset operation completed.
    Uninitialized {
        /// The ID of the reset operation, if one was performed.
        reset_id: Option<RackResetUuid>,
    },
    /// Rack reset failed.
    ResetFailed {
        /// The ID of the reset operation.
        id: RackResetUuid,
        /// A message describing the failure.
        message: String,
    },
    /// Rack reset panicked.
    ResetPanicked {
        /// The ID of the reset operation.
        id: RackResetUuid,
    },
}
