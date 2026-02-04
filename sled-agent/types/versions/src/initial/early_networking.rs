// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for network setup required to bring up the control plane.

use bootstore::schemes::v0 as bootstore;
use omicron_common::api::internal::shared::rack_init::v1::RackNetworkConfig;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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

/// Error converting from a newer EarlyNetworkConfig to v1.
#[derive(Debug, thiserror::Error)]
#[error("BgpPeerConfig address {addr} is not IPv4")]
pub struct BgpPeerAddrNotIpv4 {
    addr: std::net::IpAddr,
}

impl TryFrom<crate::v19::early_networking::EarlyNetworkConfig>
    for EarlyNetworkConfig
{
    type Error = BgpPeerAddrNotIpv4;

    fn try_from(
        value: crate::v19::early_networking::EarlyNetworkConfig,
    ) -> Result<Self, Self::Error> {
        use omicron_common::api::internal::shared::rack_init::v1;
        use std::net::IpAddr;

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
                                        Err(BgpPeerAddrNotIpv4 { addr: other })
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
                            routes: p
                                .routes
                                .into_iter()
                                .map(|r| v1::RouteConfig {
                                    destination: r.destination,
                                    nexthop: r.nexthop,
                                    vlan_id: r.vlan_id,
                                    rib_priority: r.rib_priority,
                                })
                                .collect(),
                            addresses: p
                                .addresses
                                .into_iter()
                                .map(|a| v1::UplinkAddressConfig {
                                    address: a.address,
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

                Ok(RackNetworkConfig {
                    rack_subnet: v2_config.rack_subnet,
                    infra_ip_first: v2_config.infra_ip_first,
                    infra_ip_last: v2_config.infra_ip_last,
                    ports,
                    bgp: v2_config
                        .bgp
                        .into_iter()
                        .map(|b| v1::BgpConfig {
                            asn: b.asn,
                            originate: b.originate,
                            shaper: b.shaper,
                            checker: b.checker,
                        })
                        .collect(),
                    bfd: v2_config
                        .bfd
                        .into_iter()
                        .map(|b| v1::BfdPeerConfig {
                            local: b.local,
                            remote: b.remote,
                            detection_threshold: b.detection_threshold,
                            required_rx: b.required_rx,
                            mode: b.mode,
                            switch: b.switch,
                        })
                        .collect(),
                })
            })
            .transpose()?;

        Ok(EarlyNetworkConfig {
            generation: value.generation,
            schema_version: value.schema_version,
            body: EarlyNetworkConfigBody {
                ntp_servers: value.body.ntp_servers,
                rack_network_config,
            },
        })
    }
}
