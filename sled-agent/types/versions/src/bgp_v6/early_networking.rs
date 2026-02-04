// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for network setup required to bring up the control plane.

use bootstore::schemes::v0 as bootstore;
use omicron_common::api::internal::shared::RackNetworkConfig;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use oxnet::IpNet;

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

impl From<crate::v1::early_networking::EarlyNetworkConfig>
    for EarlyNetworkConfig
{
    fn from(value: crate::v1::early_networking::EarlyNetworkConfig) -> Self {
        use omicron_common::api::internal::shared::rack_init::v1;
        use omicron_common::api::internal::shared::{
            BfdPeerConfig, BgpConfig, BgpPeerConfig, LldpAdminStatus,
            LldpPortConfig, PortConfig, PortFec, PortSpeed, RouteConfig,
            SwitchLocation, TxEqConfig, UplinkAddressConfig,
        };

        let rack_network_config =
            value.body.rack_network_config.map(|v1_config| {
                RackNetworkConfig {
                    rack_subnet: v1_config.rack_subnet,
                    infra_ip_first: std::net::IpAddr::V4(v1_config.infra_ip_first),
                    infra_ip_last: std::net::IpAddr::V4(v1_config.infra_ip_last),
                    ports: v1_config
                        .ports
                        .into_iter()
                        .map(|p| PortConfig {
                            routes: p
                                .routes
                                .into_iter()
                                .map(|r| RouteConfig {
                                    destination: r.destination,
                                    nexthop: r.nexthop,
                                    vlan_id: r.vlan_id,
                                    rib_priority: r.rib_priority,
                                })
                                .collect(),
                            addresses: p
                                .addresses
                                .into_iter()
                                .map(|a| UplinkAddressConfig {
                                    address: Some(a.address),
                                    vlan_id: a.vlan_id,
                                })
                                .collect(),
                            switch: match p.switch {
                                v1::SwitchLocation::Switch0 => {
                                    SwitchLocation::Switch0
                                }
                                v1::SwitchLocation::Switch1 => {
                                    SwitchLocation::Switch1
                                }
                            },
                            port: p.port,
                            uplink_port_speed: match p.uplink_port_speed {
                                v1::PortSpeed::Speed0G => PortSpeed::Speed0G,
                                v1::PortSpeed::Speed1G => PortSpeed::Speed1G,
                                v1::PortSpeed::Speed10G => PortSpeed::Speed10G,
                                v1::PortSpeed::Speed25G => PortSpeed::Speed25G,
                                v1::PortSpeed::Speed40G => PortSpeed::Speed40G,
                                v1::PortSpeed::Speed50G => PortSpeed::Speed50G,
                                v1::PortSpeed::Speed100G => {
                                    PortSpeed::Speed100G
                                }
                                v1::PortSpeed::Speed200G => {
                                    PortSpeed::Speed200G
                                }
                                v1::PortSpeed::Speed400G => {
                                    PortSpeed::Speed400G
                                }
                            },
                            uplink_port_fec: p.uplink_port_fec.map(
                                |f| match f {
                                    v1::PortFec::Firecode => PortFec::Firecode,
                                    v1::PortFec::None => PortFec::None,
                                    v1::PortFec::Rs => PortFec::Rs,
                                },
                            ),
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
                                })
                                .collect(),
                            autoneg: p.autoneg,
                            lldp: p.lldp.map(|l| LldpPortConfig {
                                status: match l.status {
                                    v1::LldpAdminStatus::Enabled => {
                                        LldpAdminStatus::Enabled
                                    }
                                    v1::LldpAdminStatus::Disabled => {
                                        LldpAdminStatus::Disabled
                                    }
                                    v1::LldpAdminStatus::RxOnly => {
                                        LldpAdminStatus::RxOnly
                                    }
                                    v1::LldpAdminStatus::TxOnly => {
                                        LldpAdminStatus::TxOnly
                                    }
                                },
                                chassis_id: l.chassis_id,
                                port_id: l.port_id,
                                port_description: l.port_description,
                                system_name: l.system_name,
                                system_description: l.system_description,
                                management_addrs: l.management_addrs,
                            }),
                            tx_eq: p.tx_eq.map(|t| TxEqConfig {
                                pre1: t.pre1,
                                pre2: t.pre2,
                                main: t.main,
                                post2: t.post2,
                                post1: t.post1,
                            }),
                        })
                        .collect(),
                    bgp: v1_config
                        .bgp
                        .into_iter()
                        .map(|b| BgpConfig
{
                            asn: b.asn,
                            originate: b.originate.iter().map(|i| IpNet::V4(*i)).collect(),
                            shaper: b.shaper,
                            checker: b.checker,
                        })
                        .collect(),
                    bfd: v1_config
                        .bfd
                        .into_iter()
                        .map(|b| BfdPeerConfig {
                            local: b.local,
                            remote: b.remote,
                            detection_threshold: b.detection_threshold,
                            required_rx: b.required_rx,
                            mode: b.mode,
                            switch: match b.switch {
                                v1::SwitchLocation::Switch0 => {
                                    SwitchLocation::Switch0
                                }
                                v1::SwitchLocation::Switch1 => {
                                    SwitchLocation::Switch1
                                }
                            },
                        })
                        .collect(),
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
