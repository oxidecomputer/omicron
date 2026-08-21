// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack setup (RSS) types for the `MULTIRACK_JOIN` version.
//!
//! This version adds the multirack join types, [`MultirackJoinRequest`] and
//! [`RunMultirackJoinResponse`].
//!
//! [`UserSpecifiedPortConfig`] is also restructured here: its variants are now
//! [`UplinkPortConfig`] (the former `ManualPortConfig`) and a DDM variant
//! carrying the new [`L1PortConfig`].
//! [`UserSpecifiedRackNetworkConfig`] and [`PutRssUserConfigInsensitive`] are
//! redefined because they transitively contain it; every other rack-setup type
//! is re-exported unchanged from [`crate::v1::rack_setup`],
//! [`crate::v2::rack_setup`] and [`crate::v3::rack_setup`].

use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv6Addr};

use iddqd::IdOrdMap;
use omicron_uuid_kinds::MultirackJoinUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, Serializer};

use crate::v1::rack_setup::{
    AllowedSourceIps, BgpConfig, LinkFec, LinkSpeed, LldpPortConfig,
    RouteConfig, TxEqConfig, UserSpecifiedUplinkAddressConfig,
};
use crate::v2::rack_setup::ServiceIpPoolConfig;
use crate::v3;
use crate::v3::rack_setup::UserSpecifiedBgpPeerConfig;

/// The portion of the RSS configuration that can be posted in one shot.
///
/// It is provided by the operator uploading a TOML file. Sensitive values
/// (certificates, the recovery password hash, and BGP authentication keys) are
/// set separately.
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

impl From<v3::rack_setup::PutRssUserConfigInsensitive>
    for PutRssUserConfigInsensitive
{
    fn from(old: v3::rack_setup::PutRssUserConfigInsensitive) -> Self {
        Self {
            bootstrap_sleds: old.bootstrap_sleds,
            ntp_servers: old.ntp_servers,
            dns_servers: old.dns_servers,
            service_ip_pools: old.service_ip_pools,
            external_dns_ips: old.external_dns_ips,
            external_dns_zone_name: old.external_dns_zone_name,
            rack_network_config: old.rack_network_config.into(),
            allowed_source_ips: old.allowed_source_ips,
            external_jumbo_frames_opt_in_enabled: old
                .external_jumbo_frames_opt_in_enabled,
        }
    }
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

impl From<v3::rack_setup::UserSpecifiedRackNetworkConfig>
    for UserSpecifiedRackNetworkConfig
{
    fn from(old: v3::rack_setup::UserSpecifiedRackNetworkConfig) -> Self {
        let convert_ports = |ports: BTreeMap<
            String,
            v3::rack_setup::UserSpecifiedPortConfig,
        >| {
            ports.into_iter().map(|(name, cfg)| (name, cfg.into())).collect()
        };
        Self {
            rack_subnet_address: old.rack_subnet_address,
            infra_ip_first: old.infra_ip_first,
            infra_ip_last: old.infra_ip_last,
            switch0: convert_ports(old.switch0),
            switch1: convert_ports(old.switch1),
            bgp: old.bgp,
        }
    }
}

/// User-specified per-port configuration.
///
/// This contains all of the fields of a port configuration other than the port
/// name, which is used as the map key.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UplinkPortConfig {
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
    /// Whether the switch should carry DDM traffic on this port.
    ///
    /// Derived from which [`UserSpecifiedPortConfig`] variant the port came
    /// from rather than supplied by the operator, so it is absent from both the
    /// TOML config and the API schema.
    #[serde(skip)]
    pub allow_ddm_traffic: bool,
}

impl From<v3::rack_setup::ManualPortConfig> for UplinkPortConfig {
    fn from(old: v3::rack_setup::ManualPortConfig) -> Self {
        Self {
            routes: old.routes,
            addresses: old.addresses,
            uplink_port_speed: old.uplink_port_speed,
            uplink_port_fec: old.uplink_port_fec,
            autoneg: old.autoneg,
            bgp_peers: old.bgp_peers,
            lldp: old.lldp,
            tx_eq: old.tx_eq,
            allow_ddm_traffic: false,
        }
    }
}

/// Configuration for the physical layer of a port
///
// TODO: Use this in `ManualPortConfig` once we start restructuring toml.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct L1PortConfig {
    /// The port speed.
    pub speed: LinkSpeed,
    /// The forward error correction mode, if any.
    pub fec: Option<LinkFec>,
    /// Whether autonegotiation is enabled.
    pub autoneg: bool,
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
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(try_from = "UnvalidatedPortConfig")]
#[allow(clippy::large_enum_variant)]
pub enum UserSpecifiedPortConfig {
    /// A front port intended for use as an uplink
    Uplink(UplinkPortConfig),
    /// A front port running DDM for multirack
    Ddm(L1PortConfig),
}

// The physical-layer settings a `v3` DDM-automatic port is upgraded to. That
// version encoded such a port as an empty map, so there is no prior value to
// carry forward and these have to be synthesized.
const UPGRADED_DDM_L1_CONFIG: L1PortConfig = L1PortConfig {
    speed: LinkSpeed::Speed100G,
    fec: None,
    autoneg: false,
    lldp: None,
    tx_eq: None,
};

impl From<v3::rack_setup::UserSpecifiedPortConfig> for UserSpecifiedPortConfig {
    fn from(old: v3::rack_setup::UserSpecifiedPortConfig) -> Self {
        match old {
            v3::rack_setup::UserSpecifiedPortConfig::Manual(cfg) => {
                Self::Uplink(cfg.into())
            }
            v3::rack_setup::UserSpecifiedPortConfig::DdmAutoPortConfig => {
                Self::Ddm(UPGRADED_DDM_L1_CONFIG)
            }
        }
    }
}

// Hand-roll the Serialize impl so we don't have to use serde(untagged), under
// which invalid uplink configs would silently fall back to the DDM variant.
//
// We may wish to switch this to internal tagging in the future, but that will
// cause changes to the TOML config as well as the JSON schema.
impl Serialize for UserSpecifiedPortConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Uplink(cfg) => cfg.serialize(serializer),
            Self::Ddm(cfg) => cfg.serialize(serializer),
        }
    }
}

// Superset of both variants' fields, used to pick a variant without
// `serde(untagged)`. The fields a variant requires are optional here so that
// the `TryFrom` can report a precise error; it re-imposes each variant's own
// requirements.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UnvalidatedPortConfig {
    routes: Option<Vec<RouteConfig>>,
    addresses: Option<Vec<UserSpecifiedUplinkAddressConfig>>,
    uplink_port_speed: Option<LinkSpeed>,
    uplink_port_fec: Option<LinkFec>,
    bgp_peers: Option<Vec<UserSpecifiedBgpPeerConfig>>,
    speed: Option<LinkSpeed>,
    fec: Option<LinkFec>,
    autoneg: bool,
    lldp: Option<LldpPortConfig>,
    tx_eq: Option<TxEqConfig>,
}

impl TryFrom<UnvalidatedPortConfig> for UserSpecifiedPortConfig {
    type Error = String;

    fn try_from(value: UnvalidatedPortConfig) -> Result<Self, Self::Error> {
        let UnvalidatedPortConfig {
            routes,
            addresses,
            uplink_port_speed,
            uplink_port_fec,
            bgp_peers,
            speed,
            fec,
            autoneg,
            lldp,
            tx_eq,
        } = value;

        // The two variants name their speed and FEC fields differently, so the
        // required speed key is what selects the variant.
        match (uplink_port_speed, speed) {
            (Some(_), Some(_)) => Err("a port configuration sets both \
                 `uplink_port_speed` and `speed`"
                .to_string()),
            (None, None) => Err("a port configuration must set either \
                 `uplink_port_speed` (uplink) or `speed` (DDM)"
                .to_string()),
            (Some(uplink_port_speed), None) => {
                if fec.is_some() {
                    return Err("an uplink port configuration uses \
                         `uplink_port_fec`, not `fec`"
                        .to_string());
                }
                let (Some(routes), Some(addresses)) = (routes, addresses)
                else {
                    return Err("an uplink port configuration requires \
                         `routes` and `addresses`"
                        .to_string());
                };
                Ok(Self::Uplink(UplinkPortConfig {
                    routes,
                    addresses,
                    uplink_port_speed,
                    uplink_port_fec,
                    autoneg,
                    bgp_peers: bgp_peers.unwrap_or_default(),
                    lldp,
                    tx_eq,
                    allow_ddm_traffic: false,
                }))
            }
            (None, Some(speed)) => {
                if routes.is_some()
                    || addresses.is_some()
                    || bgp_peers.is_some()
                    || uplink_port_fec.is_some()
                {
                    return Err("a DDM port configuration cannot set \
                         `routes`, `addresses`, `bgp_peers` or \
                         `uplink_port_fec`"
                        .to_string());
                }
                Ok(Self::Ddm(L1PortConfig { speed, fec, autoneg, lldp, tx_eq }))
            }
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
        use schemars::schema::Metadata;
        use schemars::schema::Schema;
        use schemars::schema::SchemaObject;
        use schemars::schema::SubschemaValidation;

        let mut uplink =
            generator.subschema_for::<UplinkPortConfig>().into_object();
        uplink.metadata().description =
            Some("A front port intended for use as an uplink".to_string());

        let mut ddm = generator.subschema_for::<L1PortConfig>().into_object();
        ddm.metadata().description =
            Some("A front port running DDM for multirack".to_string());

        SchemaObject {
            metadata: Some(Box::new(Metadata {
                description: Some(
                    "A user-specified port configuration.".to_string(),
                ),
                ..Default::default()
            })),
            subschemas: Some(Box::new(SubschemaValidation {
                any_of: Some(vec![Schema::Object(uplink), Schema::Object(ddm)]),
                ..Default::default()
            })),
            ..Default::default()
        }
        .into()
    }
}

// Multirack join.
//
// The root struct is [`MultirackJoinRequest`]. Unlike the RSS configuration,
// which is staged field by field as `UserSpecified*` types and resolved by
// wicketd, a join request is posted whole and forwarded to the bootstrap agent
// as-is, so it carries the same fully-resolved [`RackNetworkConfig`] the
// bootstrap agent's own multirack-join endpoint takes.

// Re-exports of pinned types from sled-agent-types-versions.
pub use sled_agent_types_versions::v1::early_networking::{
    BfdMode, BfdPeerConfig, ImportExportPolicy,
};
pub use sled_agent_types_versions::v30::early_networking::UplinkAddressConfig;
pub use sled_agent_types_versions::v47::early_networking::{
    BgpPeerConfig, NumberedRouter, RouterPeerType, UnnumberedRouter,
};
pub use sled_agent_types_versions::v48::early_networking::{
    PortConfig, RackNetworkConfig, UplinkPorts,
};

// Re-export of a type from sled-hardware-types that should never change.
pub use sled_hardware_types::BaseboardId;

/// A request to join this rack into an existing multirack cluster.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct MultirackJoinRequest {
    /// The peers required to initialize this rack's trust quorum.
    ///
    /// Unlike RSS, this is not optional: the bootstrap agent discovers
    /// bootstrap addresses and maps them to these `BaseboardId`s.
    pub trust_quorum_peers: BTreeSet<BaseboardId>,

    /// The network configuration for this joining rack.
    pub rack_network_config: RackNetworkConfig,
}

/// The response to a request to join a multirack cluster.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct RunMultirackJoinResponse {
    /// The ID of the multirack join that was started.
    ///
    /// A query for the state of rack setup reports this same ID, in untyped
    /// form, as `RackOperation::id` with `kind` set to `multirack-join`.
    pub id: MultirackJoinUuid,
}
