// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for network setup required to bring up the control plane.

use crate::v1::early_networking as v1;
use oxnet::{IpNet, Ipv6Net};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::num::NonZeroU8;

/// Network configuration required to bring up the control plane
///
/// The fields in this structure are those from
/// `RackInitializeRequest` necessary for use beyond RSS.
/// This is just for the initial rack configuration and cold boot purposes.
/// Updates come from Nexus.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct EarlyNetworkConfig {
    // The current generation number of data as stored in CRDB.
    // The initial generation is set during RSS time and then only mutated
    // by Nexus.
    pub generation: u64,

    // Which version of the data structure do we have. This is to help with
    // deserialization and conversion in future updates.
    pub schema_version: u32,

    // The actual configuration details
    pub body: EarlyNetworkConfigBody,
}

impl From<v1::EarlyNetworkConfig> for EarlyNetworkConfig {
    fn from(value: v1::EarlyNetworkConfig) -> Self {
        let rack_network_config =
            value.body.rack_network_config.map(|v1_config| {
                RackNetworkConfig {
                    rack_subnet: v1_config.rack_subnet,
                    infra_ip_first: std::net::IpAddr::V4(
                        v1_config.infra_ip_first,
                    ),
                    infra_ip_last: std::net::IpAddr::V4(
                        v1_config.infra_ip_last,
                    ),
                    ports: v1_config
                        .ports
                        .into_iter()
                        .map(|p| PortConfig {
                            routes: p.routes,
                            addresses: p
                                .addresses
                                .into_iter()
                                .map(|a| UplinkAddressConfig {
                                    address: if a
                                        .address
                                        .addr()
                                        .is_unspecified()
                                    {
                                        None
                                    } else {
                                        Some(a.address)
                                    },
                                    vlan_id: a.vlan_id,
                                })
                                .collect(),
                            switch: p.switch,
                            port: p.port,
                            uplink_port_speed: p.uplink_port_speed,
                            uplink_port_fec: p.uplink_port_fec,
                            bgp_peers: p
                                .bgp_peers
                                .into_iter()
                                .map(|peer| BgpPeerConfig {
                                    asn: peer.asn,
                                    port: peer.port,
                                    addr: peer.addr.into(), // Ipv4Addr -> IpAddr
                                    hold_time: peer.hold_time,
                                    idle_hold_time: peer.idle_hold_time,
                                    delay_open: peer.delay_open,
                                    connect_retry: peer.connect_retry,
                                    keepalive: peer.keepalive,
                                    remote_asn: peer.remote_asn,
                                    min_ttl: peer.min_ttl,
                                    md5_auth_key: peer.md5_auth_key,
                                    multi_exit_discriminator: peer
                                        .multi_exit_discriminator,
                                    communities: peer.communities,
                                    local_pref: peer.local_pref,
                                    enforce_first_as: peer.enforce_first_as,
                                    allowed_import: peer.allowed_import,
                                    allowed_export: peer.allowed_export,
                                    vlan_id: peer.vlan_id,
                                    router_lifetime: Default::default(),
                                })
                                .collect(),
                            autoneg: p.autoneg,
                            lldp: p.lldp,
                            tx_eq: p.tx_eq,
                        })
                        .collect(),
                    bgp: v1_config
                        .bgp
                        .into_iter()
                        .map(|b| BgpConfig {
                            asn: b.asn,
                            originate: b
                                .originate
                                .iter()
                                .map(|i| IpNet::V4(*i))
                                .collect(),
                            shaper: b.shaper,
                            checker: b.checker,
                            max_paths: MaxPathConfig::default(),
                        })
                        .collect(),
                    bfd: v1_config.bfd,
                }
            });

        EarlyNetworkConfig {
            generation: value.generation,
            schema_version: value.schema_version,
            body: EarlyNetworkConfigBody {
                ntp_servers: value.body.ntp_servers,
                rack_network_config,
            },
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConversionError {
    #[error("{field} should be Ipv4Addr, but is {addr}")]
    NotIpv4 { field: String, addr: std::net::IpAddr },
    #[error("{field} should be Ipv4Net, but is {net}")]
    NotIpv4Net { field: String, net: oxnet::IpNet },
}

impl TryFrom<EarlyNetworkConfig> for v1::EarlyNetworkConfig {
    type Error = ConversionError;

    fn try_from(value: EarlyNetworkConfig) -> Result<Self, Self::Error> {
        let rack_network_config = value
            .body
            .rack_network_config
            .map(|v2_config| {
                let ports = v2_config
                    .ports
                    .into_iter()
                    .map(|p| {
                        let bgp_peers = p
                            .bgp_peers
                            .into_iter()
                            .map(|peer| {
                                let addr = match peer.addr {
                                    IpAddr::V4(v4) => Ok(v4),
                                    other => {
                                        let err = ConversionError::NotIpv4 {
                                            field: "BgpPeerConfig.addr".into(),
                                            addr: other,
                                        };
                                        Err(err)
                                    }
                                }?;
                                Ok(v1::BgpPeerConfig {
                                    asn: peer.asn,
                                    port: peer.port,
                                    addr,
                                    hold_time: peer.hold_time,
                                    idle_hold_time: peer.idle_hold_time,
                                    delay_open: peer.delay_open,
                                    connect_retry: peer.connect_retry,
                                    keepalive: peer.keepalive,
                                    remote_asn: peer.remote_asn,
                                    min_ttl: peer.min_ttl,
                                    md5_auth_key: peer.md5_auth_key,
                                    multi_exit_discriminator: peer
                                        .multi_exit_discriminator,
                                    communities: peer.communities,
                                    local_pref: peer.local_pref,
                                    enforce_first_as: peer.enforce_first_as,
                                    allowed_import: peer.allowed_import,
                                    allowed_export: peer.allowed_export,
                                    vlan_id: peer.vlan_id,
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?;

                        Ok(v1::PortConfig {
                            routes: p.routes,
                            addresses: p
                                .addresses
                                .into_iter()
                                .map(|a| v1::UplinkAddressConfig {
                                    address: a.address.unwrap_or_else(|| {
                                        IpNet::host_net(IpAddr::V6(
                                            Ipv6Addr::UNSPECIFIED,
                                        ))
                                    }),
                                    vlan_id: a.vlan_id,
                                })
                                .collect(),
                            switch: p.switch,
                            port: p.port,
                            uplink_port_speed: p.uplink_port_speed,
                            uplink_port_fec: p.uplink_port_fec,
                            bgp_peers,
                            autoneg: p.autoneg,
                            lldp: p.lldp,
                            tx_eq: p.tx_eq,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let infra_ip_first = match v2_config.infra_ip_first {
                    IpAddr::V4(ipv4_addr) => Ok(ipv4_addr),
                    _ => Err(ConversionError::NotIpv4 {
                        field: "RackNetworkConfig.infra_ip_first".into(),
                        addr: v2_config.infra_ip_first,
                    }),
                }?;

                let infra_ip_last = match v2_config.infra_ip_last {
                    IpAddr::V4(ipv4_addr) => Ok(ipv4_addr),
                    _ => Err(ConversionError::NotIpv4 {
                        field: "RackNetworkConfig.infra_ip_last".into(),
                        addr: v2_config.infra_ip_last,
                    }),
                }?;

                let mut bgp = vec![];

                for bgp_config in v2_config.bgp {
                    let mut originate = vec![];

                    for prefix in bgp_config.originate {
                        match prefix {
                            IpNet::V4(ipv4_net) => {
                                originate.push(ipv4_net);
                                Ok(())
                            }
                            _ => Err(ConversionError::NotIpv4Net {
                                field: "BgpConfig.originate".into(),
                                net: prefix,
                            }),
                        }?
                    }
                    let converted_config = v1::BgpConfig {
                        asn: bgp_config.asn,
                        originate,
                        shaper: bgp_config.shaper,
                        checker: bgp_config.checker,
                    };

                    bgp.push(converted_config);
                }

                Ok(v1::RackNetworkConfig {
                    rack_subnet: v2_config.rack_subnet,
                    infra_ip_first,
                    infra_ip_last,
                    ports,
                    bgp,
                    bfd: v2_config.bfd,
                })
            })
            .transpose()?;

        Ok(Self {
            generation: value.generation,
            schema_version: value.schema_version,
            body: v1::EarlyNetworkConfigBody {
                ntp_servers: value.body.ntp_servers,
                rack_network_config,
            },
        })
    }
}

/// This is the actual configuration of EarlyNetworking.
///
/// We nest it below the "header" of `generation` and `schema_version` so that
/// we can perform partial deserialization of `EarlyNetworkConfig` to only read
/// the header and defer deserialization of the body once we know the schema
/// version. This is possible via the use of [`serde_json::value::RawValue`] in
/// future (post-v1) deserialization paths.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct EarlyNetworkConfigBody {
    /// The external NTP server addresses.
    pub ntp_servers: Vec<String>,

    // Rack network configuration as delivered from RSS or Nexus
    pub rack_network_config: Option<RackNetworkConfig>,
}

// This impl must be here, not in the crate-level `impls` module, because every
// version of `EarlyNetworkConfigBody` has its own distinct `SCHEMA_VERSION`.
impl EarlyNetworkConfigBody {
    pub const SCHEMA_VERSION: u32 = 2;
}

/// Initial network configuration
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
pub struct RackNetworkConfig {
    pub rack_subnet: Ipv6Net,
    // TODO: #3591 Consider making infra-ip ranges implicit for uplinks
    /// First ip address to be used for configuring network infrastructure
    pub infra_ip_first: IpAddr,
    /// Last ip address to be used for configuring network infrastructure
    pub infra_ip_last: IpAddr,
    /// Uplinks for connecting the rack to external networks
    pub ports: Vec<PortConfig>,
    /// BGP configurations for connecting the rack to external networks
    pub bgp: Vec<BgpConfig>,
    /// BFD configuration for connecting the rack to external networks
    #[serde(default)]
    pub bfd: Vec<v1::BfdPeerConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct BgpPeerConfig {
    /// The autonomous system number of the router the peer belongs to.
    pub asn: u32,
    /// Switch port the peer is reachable on.
    pub port: String,
    /// Address of the peer. Use `UNSPECIFIED` to indicate an unnumbered BGP
    /// session established over the interface specified by `port`.
    pub addr: IpAddr,
    /// How long to keep a session alive without a keepalive in seconds.
    /// Defaults to 6.
    pub hold_time: Option<u64>,
    /// How long to keep a peer in idle after a state machine reset in seconds.
    pub idle_hold_time: Option<u64>,
    /// How long to delay sending open messages to a peer. In seconds.
    pub delay_open: Option<u64>,
    /// The interval in seconds between peer connection retry attempts.
    pub connect_retry: Option<u64>,
    /// The interval to send keepalive messages at.
    pub keepalive: Option<u64>,
    /// Require that a peer has a specified ASN.
    #[serde(default)]
    pub remote_asn: Option<u32>,
    /// Require messages from a peer have a minimum IP time to live field.
    #[serde(default)]
    pub min_ttl: Option<u8>,
    /// Use the given key for TCP-MD5 authentication with the peer.
    #[serde(default)]
    pub md5_auth_key: Option<String>,
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
    /// Define import policy for a peer.
    #[serde(default)]
    pub allowed_import: v1::ImportExportPolicy,
    /// Define export policy for a peer.
    #[serde(default)]
    pub allowed_export: v1::ImportExportPolicy,
    /// Associate a VLAN ID with a BGP peer session.
    #[serde(default)]
    pub vlan_id: Option<u16>,
    /// Router lifetime in seconds for unnumbered BGP peers.
    #[serde(default)]
    pub router_lifetime: RouterLifetimeConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct PortConfig {
    /// The set of routes associated with this port.
    pub routes: Vec<v1::RouteConfig>,
    /// This port's addresses and optional vlan IDs
    pub addresses: Vec<UplinkAddressConfig>,
    /// Switch the port belongs to.
    pub switch: v1::SwitchLocation,
    /// Nmae of the port this config applies to.
    pub port: String,
    /// Port speed.
    pub uplink_port_speed: v1::PortSpeed,
    /// Port forward error correction type.
    pub uplink_port_fec: Option<v1::PortFec>,
    /// BGP peers on this port
    pub bgp_peers: Vec<BgpPeerConfig>,
    /// Whether or not to set autonegotiation
    #[serde(default)]
    pub autoneg: bool,
    /// LLDP configuration for this port
    pub lldp: Option<v1::LldpPortConfig>,
    /// TX-EQ configuration for this port
    pub tx_eq: Option<v1::TxEqConfig>,
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema, Hash,
)]
pub struct UplinkAddressConfig {
    /// The address to be used on the uplink.
    /// Set to `None` for an Ipv6 Link Local address.
    pub address: Option<IpNet>,

    /// The VLAN id (if any) associated with this address.
    #[serde(default)]
    pub vlan_id: Option<u16>,
}

impl From<v1::UplinkAddressConfig> for UplinkAddressConfig {
    fn from(value: v1::UplinkAddressConfig) -> Self {
        let address = if value.address.addr().is_unspecified() {
            None
        } else {
            Some(value.address)
        };

        Self { address, vlan_id: value.vlan_id }
    }
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct UplinkAddressConfigError(pub(crate) String);

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct BgpConfig {
    /// The autonomous system number for the BGP configuration.
    pub asn: u32,

    /// The set of prefixes for the BGP router to originate.
    pub originate: Vec<IpNet>,

    /// Shaper to apply to outgoing messages.
    #[serde(default)]
    pub shaper: Option<String>,

    /// Checker to apply to incoming messages.
    #[serde(default)]
    pub checker: Option<String>,

    /// Maximum number of paths to use when multiple "best paths" exist
    #[serde(default)]
    pub max_paths: MaxPathConfig,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize)]
pub struct MaxPathConfig(u8);

impl MaxPathConfig {
    const MIN: u8 = 1;
    const MAX: u8 = 32;

    pub fn new(v: u8) -> Result<Self, MaxPathConfigError> {
        if v < Self::MIN {
            return Err(MaxPathConfigError::ValueTooSmall);
        }

        if v > Self::MAX {
            return Err(MaxPathConfigError::ValueTooLarge);
        }

        Ok(Self(v))
    }

    pub fn as_u8(&self) -> u8 {
        self.0
    }

    pub fn as_nonzero_u8(&self) -> NonZeroU8 {
        // By construction, we guarantee self.0 is at least Self::MIN (1), so we
        // can unwrap this conversion.
        NonZeroU8::new(self.0).unwrap()
    }
}

impl Default for MaxPathConfig {
    fn default() -> Self {
        Self(Self::MIN)
    }
}

impl<'de> Deserialize<'de> for MaxPathConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = u8::deserialize(deserializer)?;
        MaxPathConfig::new(value).map_err(serde::de::Error::custom)
    }
}

impl JsonSchema for MaxPathConfig {
    fn schema_name() -> String {
        "MaxPathConfig".to_string()
    }

    fn json_schema(
        _: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::Schema::Object(schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::Integer.into()),
            format: Some("uint8".to_string()),
            number: Some(Box::new(schemars::schema::NumberValidation {
                minimum: Some(f64::from(Self::MIN)),
                maximum: Some(f64::from(Self::MAX)),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MaxPathConfigError {
    #[error("max path value cannot be less than {}", MaxPathConfig::MIN)]
    ValueTooSmall,
    #[error("max path value cannot be greater than {}", MaxPathConfig::MAX)]
    ValueTooLarge,
    #[error(
        "max path value must be an integer between {} and {}",
        MaxPathConfig::MIN,
        MaxPathConfig::MAX
    )]
    ParseIntError(#[from] std::num::ParseIntError),
}

/// Router lifetime in seconds for unnumbered BGP peers.
///
/// This value is used in IPv6 Router Advertisements to indicate how long
/// the router should be considered valid by neighbors.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize)]
pub struct RouterLifetimeConfig(u16);

impl RouterLifetimeConfig {
    /// Default router lifetime: 0 seconds (disabled)
    pub const DEFAULT: u16 = 0;

    // Maximum valid router lifetime is 9000 seconds (2.5 hours) per RFC 4861
    const MAX: u16 = 9000;

    pub fn new(v: u16) -> Result<Self, RouterLifetimeConfigError> {
        if v > Self::MAX {
            return Err(RouterLifetimeConfigError::ValueTooLarge);
        }

        Ok(Self(v))
    }

    pub fn as_u16(&self) -> u16 {
        self.0
    }
}

impl Default for RouterLifetimeConfig {
    fn default() -> Self {
        Self(Self::DEFAULT)
    }
}

impl<'de> Deserialize<'de> for RouterLifetimeConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = u16::deserialize(deserializer)?;
        RouterLifetimeConfig::new(value).map_err(serde::de::Error::custom)
    }
}

impl JsonSchema for RouterLifetimeConfig {
    fn schema_name() -> String {
        "RouterLifetimeConfig".to_string()
    }

    fn json_schema(
        _: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::Schema::Object(schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                description: Some(
                    "Router lifetime in seconds for unnumbered BGP peers"
                        .to_string(),
                ),
                ..Default::default()
            })),
            instance_type: Some(schemars::schema::InstanceType::Integer.into()),
            format: Some("uint16".to_string()),
            number: Some(Box::new(schemars::schema::NumberValidation {
                minimum: Some(f64::from(Self::DEFAULT)),
                maximum: Some(f64::from(Self::MAX)),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RouterLifetimeConfigError {
    #[error(
        "router lifetime config cannot be greater than {}",
        RouterLifetimeConfig::MAX
    )]
    ValueTooLarge,
    #[error(
        "max path value must be an integer between {} and {}",
        RouterLifetimeConfig::DEFAULT,
        RouterLifetimeConfig::MAX
    )]
    ParseIntError(#[from] std::num::ParseIntError),
}
