// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for network setup required to bring up the control plane.

use std::str::FromStr;

use bootstore::schemes::v0 as bootstore;
use omicron_common::api::internal::shared::RackNetworkConfig;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::{Logger, warn};

/// Network configuration required to bring up the control plane
///
/// The fields in this structure are those from
/// [`RackInitializeRequest`] necessary for use beyond RSS.
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

impl FromStr for EarlyNetworkConfig {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        #[derive(Deserialize)]
        struct ShadowConfig {
            generation: u64,
            schema_version: u32,
            body: EarlyNetworkConfigBody,
        }

        let v2_err = match serde_json::from_str::<ShadowConfig>(&value) {
            Ok(cfg) => {
                return Ok(EarlyNetworkConfig {
                    generation: cfg.generation,
                    schema_version: cfg.schema_version,
                    body: cfg.body,
                });
            }
            Err(e) => format!("unable to parse EarlyNetworkConfig: {e:?}"),
        };
        // If we fail to parse the config as any known version, we return the
        // error corresponding to the parse failure of the newest schema.
        serde_json::from_str::<back_compat::EarlyNetworkConfigV1>(&value)
            .map(|v1| EarlyNetworkConfig {
                generation: v1.generation,
                schema_version: Self::schema_version(),
                body: v1.body.into(),
            })
            .map_err(|_| v2_err)
    }
}

impl EarlyNetworkConfig {
    pub fn schema_version() -> u32 {
        2
    }

    // Note: This currently only converts between v0 and v1 or deserializes v1 of
    // `EarlyNetworkConfig`.
    pub fn deserialize_bootstore_config(
        log: &Logger,
        config: &bootstore::NetworkConfig,
    ) -> Result<Self, serde_json::Error> {
        // Try to deserialize the latest version of the data structure (v2). If
        // that succeeds we are done.
        let v2_error =
            match serde_json::from_slice::<EarlyNetworkConfig>(&config.blob) {
                Ok(val) => return Ok(val),
                Err(error) => {
                    // Log this error and continue trying to deserialize older
                    // versions.
                    warn!(
                        log,
                        "Failed to deserialize EarlyNetworkConfig \
                         as v2, trying next as v1: {}",
                        error,
                    );
                    error
                }
            };

        match serde_json::from_slice::<back_compat::EarlyNetworkConfigV1>(
            &config.blob,
        ) {
            Ok(v1) => {
                // Convert from v1 to v2
                return Ok(EarlyNetworkConfig {
                    generation: v1.generation,
                    schema_version: EarlyNetworkConfig::schema_version(),
                    body: v1.body.into(),
                });
            }
            Err(error) => {
                // Log this error.
                warn!(
                    log,
                    "Failed to deserialize EarlyNetworkConfig \
                         as v1, trying next as v0: {}",
                    error
                );
            }
        };

        match serde_json::from_slice::<back_compat::EarlyNetworkConfigV0>(
            &config.blob,
        ) {
            Ok(val) => {
                // Convert from v0 to v2
                return Ok(EarlyNetworkConfig {
                    generation: val.generation,
                    schema_version: 2,
                    body: EarlyNetworkConfigBody {
                        ntp_servers: val.ntp_servers,
                        rack_network_config: val.rack_network_config.map(
                            |v0_config| {
                                back_compat::RackNetworkConfigV0::to_v2(
                                    val.rack_subnet,
                                    v0_config,
                                )
                            },
                        ),
                    },
                });
            }
            Err(error) => {
                // Log this error.
                warn!(
                    log,
                    "Failed to deserialize EarlyNetworkConfig as v0: {}", error,
                );
            }
        };

        // If we fail to parse the config as any known version, we return the
        // error corresponding to the parse failure of the newest schema.
        Err(v2_error)
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

impl From<EarlyNetworkConfig> for bootstore::NetworkConfig {
    fn from(value: EarlyNetworkConfig) -> Self {
        // Can this ever actually fail?
        // We literally just deserialized the same data in RSS
        let blob = serde_json::to_vec(&value).unwrap();

        // Yes this is duplicated, but that seems fine.
        let generation = value.generation;

        bootstore::NetworkConfig { generation, blob }
    }
}

/// Structures and routines used to maintain backwards compatibility.  The
/// contents of this module should only be used to convert older data into the
/// current format, and not for any ongoing run-time operations.
pub mod back_compat {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use omicron_common::api::{
        external::SwitchLocation,
        internal::shared::{
            BfdPeerConfig, BgpConfig, BgpPeerConfig, PortConfigV2, PortFec,
            PortSpeed, RackNetworkConfigV2, RouteConfig, UplinkAddressConfig,
        },
    };
    use oxnet::{IpNet, Ipv4Net, Ipv6Net};
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    use super::EarlyNetworkConfigBody;

    #[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
    pub struct EarlyNetworkConfigBodyV1 {
        /// The external NTP server addresses.
        pub ntp_servers: Vec<String>,

        // Rack network configuration as delivered from RSS or Nexus
        pub rack_network_config: Option<RackNetworkConfigV1>,
    }

    impl From<EarlyNetworkConfigBodyV1> for EarlyNetworkConfigBody {
        fn from(v1: EarlyNetworkConfigBodyV1) -> Self {
            EarlyNetworkConfigBody {
                ntp_servers: v1.ntp_servers,
                rack_network_config: v1
                    .rack_network_config
                    .map(|v1_config| v1_config.into()),
            }
        }
    }

    /// Deprecated, use `RackNetworkConfig` instead. Cannot actually deprecate due to
    /// <https://github.com/serde-rs/serde/issues/2195>
    ///
    /// Our first version of `RackNetworkConfig`. If this exists in the bootstore, we
    /// upgrade out of it into `RackNetworkConfigV1` or later versions if possible.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
    pub struct RackNetworkConfigV0 {
        // TODO: #3591 Consider making infra-ip ranges implicit for uplinks
        /// First ip address to be used for configuring network infrastructure
        pub infra_ip_first: Ipv4Addr,
        /// Last ip address to be used for configuring network infrastructure
        pub infra_ip_last: Ipv4Addr,
        /// Uplinks for connecting the rack to external networks
        pub uplinks: Vec<UplinkConfig>,
    }

    impl RackNetworkConfigV0 {
        /// Convert from `RackNetworkConfigV0` to `RackNetworkConfigV1`
        ///
        /// We cannot use `From<RackNetworkConfigV0> for `RackNetworkConfigV2`
        /// because the `rack_subnet` field does not exist in `RackNetworkConfigV0`
        /// and must be passed in from the `EarlyNetworkConfigV0` struct which
        /// contains the `RackNetworkConfigV0` struct.
        pub fn to_v2(
            rack_subnet: Ipv6Addr,
            v0: RackNetworkConfigV0,
        ) -> RackNetworkConfigV2 {
            RackNetworkConfigV2 {
                rack_subnet: Ipv6Net::new(rack_subnet, 56).unwrap(),
                infra_ip_first: v0.infra_ip_first,
                infra_ip_last: v0.infra_ip_last,
                ports: v0
                    .uplinks
                    .into_iter()
                    .map(|uplink| PortConfigV2::from(uplink))
                    .collect(),
                bgp: vec![],
                bfd: vec![],
            }
        }
    }

    /// Deprecated, use PortConfigV2 instead. Cannot actually deprecate due to
    /// <https://github.com/serde-rs/serde/issues/2195>
    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
    pub struct PortConfigV1 {
        /// The set of routes associated with this port.
        pub routes: Vec<RouteConfig>,
        /// This port's addresses and optional vlan IDs
        pub addresses: Vec<IpNet>,
        /// Switch the port belongs to.
        pub switch: SwitchLocation,
        /// Nmae of the port this config applies to.
        pub port: String,
        /// Port speed.
        pub uplink_port_speed: PortSpeed,
        /// Port forward error correction type.
        pub uplink_port_fec: PortFec,
        /// BGP peers on this port
        pub bgp_peers: Vec<BgpPeerConfig>,
        /// Whether or not to set autonegotiation
        #[serde(default)]
        pub autoneg: bool,
    }

    impl From<PortConfigV1> for PortConfigV2 {
        fn from(v1: PortConfigV1) -> Self {
            PortConfigV2 {
                routes: v1.routes.clone(),
                addresses: v1
                    .addresses
                    .iter()
                    .map(|a| UplinkAddressConfig { address: *a, vlan_id: None })
                    .collect(),
                switch: v1.switch,
                port: v1.port,
                uplink_port_speed: v1.uplink_port_speed,
                uplink_port_fec: Some(v1.uplink_port_fec),
                bgp_peers: v1.bgp_peers.clone(),
                autoneg: v1.autoneg,
                lldp: None,
                tx_eq: None,
            }
        }
    }

    /// Deprecated, use PortConfigV2 instead. Cannot actually deprecate due to
    /// <https://github.com/serde-rs/serde/issues/2195>
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
    pub struct UplinkConfig {
        /// Gateway address
        pub gateway_ip: Ipv4Addr,
        /// Switch to use for uplink
        pub switch: SwitchLocation,
        /// Switchport to use for external connectivity
        pub uplink_port: String,
        /// Speed for the Switchport
        pub uplink_port_speed: PortSpeed,
        /// Forward Error Correction setting for the uplink port
        pub uplink_port_fec: PortFec,
        /// IP Address and prefix (e.g., `192.168.0.1/16`) to apply to switchport
        /// (must be in infra_ip pool)
        pub uplink_cidr: Ipv4Net,
        /// VLAN id to use for uplink
        pub uplink_vid: Option<u16>,
        /// RIB Priority
        pub rib_priority: Option<u8>,
    }

    impl From<UplinkConfig> for PortConfigV2 {
        fn from(value: UplinkConfig) -> Self {
            PortConfigV2 {
                routes: vec![RouteConfig {
                    destination: "0.0.0.0/0".parse().unwrap(),
                    nexthop: value.gateway_ip.into(),
                    vlan_id: value.uplink_vid,
                    rib_priority: value.rib_priority,
                }],
                addresses: vec![UplinkAddressConfig {
                    address: value.uplink_cidr.into(),
                    vlan_id: value.uplink_vid,
                }],
                switch: value.switch,
                port: value.uplink_port,
                uplink_port_speed: value.uplink_port_speed,
                uplink_port_fec: Some(value.uplink_port_fec),
                bgp_peers: vec![],
                autoneg: false,
                lldp: None,
                tx_eq: None,
            }
        }
    }

    /// Deprecated, use `RackNetworkConfig` instead. Cannot actually deprecate due to
    /// <https://github.com/serde-rs/serde/issues/2195>
    ///
    /// Our second version of `RackNetworkConfig`. If this exists in the bootstore,
    /// we upgrade out of it into `RackNetworkConfigV1` or later versions if
    /// possible.
    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
    pub struct RackNetworkConfigV1 {
        pub rack_subnet: Ipv6Net,
        // TODO: #3591 Consider making infra-ip ranges implicit for uplinks
        /// First ip address to be used for configuring network infrastructure
        pub infra_ip_first: Ipv4Addr,
        /// Last ip address to be used for configuring network infrastructure
        pub infra_ip_last: Ipv4Addr,
        /// Uplinks for connecting the rack to external networks
        pub ports: Vec<PortConfigV1>,
        /// BGP configurations for connecting the rack to external networks
        pub bgp: Vec<BgpConfig>,
        /// BFD configuration for connecting the rack to external networks
        #[serde(default)]
        pub bfd: Vec<BfdPeerConfig>,
    }

    impl From<RackNetworkConfigV1> for RackNetworkConfigV2 {
        fn from(v1: RackNetworkConfigV1) -> Self {
            RackNetworkConfigV2 {
                rack_subnet: v1.rack_subnet,
                infra_ip_first: v1.infra_ip_first,
                infra_ip_last: v1.infra_ip_last,
                ports: v1
                    .ports
                    .into_iter()
                    .map(|ports| PortConfigV2::from(ports))
                    .collect(),
                bgp: v1.bgp.clone(),
                bfd: v1.bfd.clone(),
            }
        }
    }

    // The second production version of the `EarlyNetworkConfig`.
    //
    // If this version is in the bootstore than we need to convert it to
    // `EarlyNetworkConfigV2`.
    //
    // Once we do this for all customers that have initialized racks with the
    // old version we can go ahead and remove this type and its conversion code
    // altogether.
    #[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
    pub struct EarlyNetworkConfigV1 {
        // The current generation number of data as stored in CRDB.
        // The initial generation is set during RSS time and then only mutated
        // by Nexus.
        pub generation: u64,

        // Which version of the data structure do we have. This is to help with
        // deserialization and conversion in future updates.
        pub schema_version: u32,

        // The actual configuration details
        pub body: EarlyNetworkConfigBodyV1,
    }

    // The first production version of the `EarlyNetworkConfig`.
    //
    // If this version is in the bootstore than we need to convert it to
    // `EarlyNetworkConfigV2`.
    //
    // Once we do this for all customers that have initialized racks with the
    // old version we can go ahead and remove this type and its conversion code
    // altogether.
    #[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
    pub struct EarlyNetworkConfigV0 {
        // The current generation number of data as stored in CRDB.
        // The initial generation is set during RSS time and then only mutated
        // by Nexus.
        pub generation: u64,

        pub rack_subnet: Ipv6Addr,

        /// The external NTP server addresses.
        pub ntp_servers: Vec<String>,

        // Rack network configuration as delivered from RSS and only existing at
        // generation 1
        pub rack_network_config: Option<RackNetworkConfigV0>,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;

    use omicron_common::api::external::SwitchLocation;
    use omicron_common::api::internal::shared::PortConfigV2;
    use omicron_common::api::internal::shared::PortFec;
    use omicron_common::api::internal::shared::PortSpeed;
    use omicron_common::api::internal::shared::RackNetworkConfigV2;
    use omicron_common::api::internal::shared::RouteConfig;
    use omicron_common::api::internal::shared::UplinkAddressConfig;
    use omicron_test_utils::dev::test_setup_log;
    use oxnet::Ipv6Net;

    #[test]
    fn serialized_early_network_config_v0_to_v2_conversion() {
        let logctx = test_setup_log(
            "serialized_early_network_config_v0_to_v2_conversion",
        );
        let v0 = back_compat::EarlyNetworkConfigV0 {
            generation: 1,
            rack_subnet: Ipv6Addr::UNSPECIFIED,
            ntp_servers: Vec::new(),
            rack_network_config: Some(back_compat::RackNetworkConfigV0 {
                infra_ip_first: Ipv4Addr::UNSPECIFIED,
                infra_ip_last: Ipv4Addr::UNSPECIFIED,
                uplinks: vec![back_compat::UplinkConfig {
                    gateway_ip: Ipv4Addr::UNSPECIFIED,
                    switch: SwitchLocation::Switch0,
                    uplink_port: "Port0".to_string(),
                    uplink_port_speed: PortSpeed::Speed100G,
                    uplink_port_fec: PortFec::None,
                    uplink_cidr: "192.168.0.1/16".parse().unwrap(),
                    uplink_vid: None,
                    rib_priority: None,
                }],
            }),
        };

        let v0_serialized = serde_json::to_vec(&v0).unwrap();
        let bootstore_conf =
            bootstore::NetworkConfig { generation: 1, blob: v0_serialized };

        let v2 = EarlyNetworkConfig::deserialize_bootstore_config(
            &logctx.log,
            &bootstore_conf,
        )
        .unwrap();
        let v0_rack_network_config = v0.rack_network_config.unwrap();
        let uplink = v0_rack_network_config.uplinks[0].clone();
        let expected = EarlyNetworkConfig {
            generation: 1,
            schema_version: EarlyNetworkConfig::schema_version(),
            body: EarlyNetworkConfigBody {
                ntp_servers: v0.ntp_servers.clone(),
                rack_network_config: Some(RackNetworkConfigV2 {
                    rack_subnet: Ipv6Net::new(v0.rack_subnet, 56).unwrap(),
                    infra_ip_first: v0_rack_network_config.infra_ip_first,
                    infra_ip_last: v0_rack_network_config.infra_ip_last,
                    ports: vec![PortConfigV2 {
                        routes: vec![RouteConfig {
                            destination: "0.0.0.0/0".parse().unwrap(),
                            nexthop: uplink.gateway_ip.into(),
                            vlan_id: None,
                            rib_priority: None,
                        }],
                        addresses: vec![UplinkAddressConfig {
                            address: uplink.uplink_cidr.into(),
                            vlan_id: None,
                        }],
                        switch: uplink.switch,
                        port: uplink.uplink_port,
                        uplink_port_speed: uplink.uplink_port_speed,
                        uplink_port_fec: Some(uplink.uplink_port_fec),
                        autoneg: false,
                        bgp_peers: vec![],
                        lldp: None,
                        tx_eq: None,
                    }],
                    bgp: vec![],
                    bfd: vec![],
                }),
            },
        };

        assert_eq!(expected, v2);

        logctx.cleanup_successful();
    }

    #[test]
    fn serialized_early_network_config_v1_to_v2_conversion() {
        let logctx = test_setup_log(
            "serialized_early_network_config_v1_to_v2_conversion",
        );

        let v1 = back_compat::EarlyNetworkConfigV1 {
            generation: 1,
            schema_version: 1,
            body: back_compat::EarlyNetworkConfigBodyV1 {
                ntp_servers: Vec::new(),
                rack_network_config: Some(back_compat::RackNetworkConfigV1 {
                    rack_subnet: Ipv6Net::new(Ipv6Addr::UNSPECIFIED, 56)
                        .unwrap(),
                    infra_ip_first: Ipv4Addr::UNSPECIFIED,
                    infra_ip_last: Ipv4Addr::UNSPECIFIED,
                    ports: vec![back_compat::PortConfigV1 {
                        routes: vec![RouteConfig {
                            destination: "0.0.0.0/0".parse().unwrap(),
                            nexthop: "192.168.0.2".parse().unwrap(),
                            vlan_id: None,
                            rib_priority: None,
                        }],
                        addresses: vec!["192.168.0.1/16".parse().unwrap()],
                        switch: SwitchLocation::Switch0,
                        port: "Port0".to_string(),
                        uplink_port_speed: PortSpeed::Speed100G,
                        uplink_port_fec: PortFec::None,
                        bgp_peers: Vec::new(),
                        autoneg: false,
                    }],
                    bgp: Vec::new(),
                    bfd: Vec::new(),
                }),
            },
        };

        let v1_serialized = serde_json::to_vec(&v1).unwrap();
        let bootstore_conf =
            bootstore::NetworkConfig { generation: 1, blob: v1_serialized };

        let v2 = EarlyNetworkConfig::deserialize_bootstore_config(
            &logctx.log,
            &bootstore_conf,
        )
        .unwrap();
        let v1_rack_network_config = v1.body.rack_network_config.unwrap();
        let port = v1_rack_network_config.ports[0].clone();
        let expected = EarlyNetworkConfig {
            generation: 1,
            schema_version: EarlyNetworkConfig::schema_version(),
            body: EarlyNetworkConfigBody {
                ntp_servers: v1.body.ntp_servers.clone(),
                rack_network_config: Some(RackNetworkConfigV2 {
                    rack_subnet: v1_rack_network_config.rack_subnet,
                    infra_ip_first: v1_rack_network_config.infra_ip_first,
                    infra_ip_last: v1_rack_network_config.infra_ip_last,
                    ports: vec![PortConfigV2 {
                        routes: port.routes.clone(),
                        addresses: vec![UplinkAddressConfig {
                            address: port.addresses[0],
                            vlan_id: None,
                        }],
                        switch: port.switch,
                        port: port.port,
                        uplink_port_speed: port.uplink_port_speed,
                        uplink_port_fec: Some(port.uplink_port_fec),
                        autoneg: false,
                        bgp_peers: vec![],
                        lldp: None,
                        tx_eq: None,
                    }],
                    bgp: vec![],
                    bfd: vec![],
                }),
            },
        };

        assert_eq!(expected, v2);

        logctx.cleanup_successful();
    }
}
