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
//! redefined because they transitively contain it.

use anyhow::{Context, anyhow, bail};
use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv6Addr};

use iddqd::IdOrdMap;
use omicron_uuid_kinds::MultirackJoinUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v1::rack_setup::{
    AllowedSourceIps, BgpConfig, LinkFec, LinkSpeed, LldpPortConfig,
    RouteConfig, TxEqConfig, UserSpecifiedUplinkAddressConfig,
};
use crate::v2::rack_setup::ServiceIpPoolConfig;
use crate::v3;
use crate::v3::rack_setup::UserSpecifiedBgpPeerConfig;

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
    type Error = anyhow::Error;

    fn try_from(
        value: UnvalidatedPutRssUserConfigInsensitive,
    ) -> Result<Self, Self::Error> {
        let service_ip_pools =
            IdOrdMap::from_iter_unique(value.service_ip_pools)
                .context("duplicate service IP pool name")?;
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

impl TryFrom<v3::rack_setup::PutRssUserConfigInsensitive>
    for PutRssUserConfigInsensitive
{
    type Error = anyhow::Error;
    fn try_from(
        old: v3::rack_setup::PutRssUserConfigInsensitive,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            bootstrap_sleds: old.bootstrap_sleds,
            ntp_servers: old.ntp_servers,
            dns_servers: old.dns_servers,
            service_ip_pools: old.service_ip_pools,
            external_dns_ips: old.external_dns_ips,
            external_dns_zone_name: old.external_dns_zone_name,
            rack_network_config: old.rack_network_config.try_into()?,
            allowed_source_ips: old.allowed_source_ips,
            external_jumbo_frames_opt_in_enabled: old
                .external_jumbo_frames_opt_in_enabled,
        })
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

impl TryFrom<v3::rack_setup::UserSpecifiedRackNetworkConfig>
    for UserSpecifiedRackNetworkConfig
{
    type Error = anyhow::Error;

    fn try_from(
        old: v3::rack_setup::UserSpecifiedRackNetworkConfig,
    ) -> Result<Self, Self::Error> {
        let convert_ports = |ports: BTreeMap<
            String,
            v3::rack_setup::UserSpecifiedPortConfig,
        >|
         -> Result<
            BTreeMap<String, UserSpecifiedPortConfig>,
            anyhow::Error,
        > {
            let mut new_ports = BTreeMap::new();
            for (name, cfg) in ports {
                new_ports.insert(name, cfg.try_into()?);
            }
            Ok(new_ports)
        };
        Ok(Self {
            rack_subnet_address: old.rack_subnet_address,
            infra_ip_first: old.infra_ip_first,
            infra_ip_last: old.infra_ip_last,
            switch0: convert_ports(old.switch0)?,
            switch1: convert_ports(old.switch1)?,
            bgp: old.bgp,
        })
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
        }
    }
}

/// Configuration for the physical layer of a port
///
// TODO: Use this in `UplinkPortConfig` once we start restructuring toml.
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(
    try_from = "UnvalidatedPortConfig",
    tag = "tag",
    content = "val",
    rename_all = "snake_case"
)]
#[allow(clippy::large_enum_variant)]
pub enum UserSpecifiedPortConfig {
    /// A front port intended for use as an uplink
    Uplink(UplinkPortConfig),
    /// A front port running DDM for multirack
    Ddm(L1PortConfig),
}

impl TryFrom<v3::rack_setup::UserSpecifiedPortConfig>
    for UserSpecifiedPortConfig
{
    type Error = anyhow::Error;

    fn try_from(
        old: v3::rack_setup::UserSpecifiedPortConfig,
    ) -> Result<Self, Self::Error> {
        match old {
            v3::rack_setup::UserSpecifiedPortConfig::Manual(cfg) => {
                Ok(Self::Uplink(cfg.into()))
            }
            v3::rack_setup::UserSpecifiedPortConfig::DdmAutoPortConfig => {
                Err(anyhow!("Cannot upgrade from DdmAutoPortConfig"))
            }
        }
    }
}

/// A representation of a serialized tag for `UserSpecifiedPortConfig`
///
/// This is used to allow deserializing `UserSpecifiedPortConfig` or untagged
/// `UplinkPortConfig` into `UnvalidatedPortConfig`, which we can then convert
/// via `TryFrom` into `UserSpecifiedPortConfig`.
#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
enum PortConfigTag {
    Uplink,
    Ddm,
}

// Allow serde to deserialize `UserSpecifiedPortConfig` into an untagged or
// tagged form so that it can automatically convert legacy untagged uplinks into
// a form that can be converted to a `UserSpecifiedPortConfig`.
#[derive(Deserialize)]
#[serde(untagged)]
enum UnvalidatedPortConfig {
    Tagged(TaggedPortConfig),
    LegacyUplink(UplinkPortConfig),
}

// An alternate representation of a tagged `UserSpecifiedPortConfig`. This can be used
// to deserialize the tagged form with an explicit tag, so we can tell if the
// tag exists. If not, we deserialize into the untagged `UplinkPortConfig`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaggedPortConfig {
    tag: PortConfigTag,
    val: TaggedPortConfigVal,
}

// An untagged representation of `UserSpecifiedPortConfig` used as in
// intermediate value for deserialization.
#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum TaggedPortConfigVal {
    Uplink(UplinkPortConfig),
    Ddm(L1PortConfig),
}

impl TryFrom<UnvalidatedPortConfig> for UserSpecifiedPortConfig {
    type Error = anyhow::Error;

    fn try_from(value: UnvalidatedPortConfig) -> Result<Self, Self::Error> {
        let TaggedPortConfig { tag, val } = match value {
            UnvalidatedPortConfig::LegacyUplink(uplink) => {
                return Ok(UserSpecifiedPortConfig::Uplink(uplink));
            }
            UnvalidatedPortConfig::Tagged(tagged) => tagged,
        };

        match (tag, val) {
            (PortConfigTag::Uplink, TaggedPortConfigVal::Uplink(uplink)) => {
                Ok(UserSpecifiedPortConfig::Uplink(uplink))
            }
            (PortConfigTag::Ddm, TaggedPortConfigVal::Ddm(ddm)) => {
                Ok(UserSpecifiedPortConfig::Ddm(ddm))
            }
            (tag, val) => bail!("Tag {tag:?} does not match value {val:?}"),
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    fn uplink_port_config() -> UplinkPortConfig {
        UplinkPortConfig {
            routes: vec![RouteConfig {
                destination: "0.0.0.0/0".parse().unwrap(),
                nexthop: "172.30.0.10".parse().unwrap(),
                vlan_id: Some(1),
                rib_priority: None,
            }],
            addresses: vec![UserSpecifiedUplinkAddressConfig::without_vlan(
                "1.1.1.0/24".parse().unwrap(),
            )],
            uplink_port_speed: LinkSpeed::Speed40G,
            uplink_port_fec: Some(LinkFec::Rs),
            autoneg: false,
            bgp_peers: vec![],
            lldp: None,
            tx_eq: None,
        }
    }

    fn l1_port_config() -> L1PortConfig {
        L1PortConfig {
            speed: LinkSpeed::Speed100G,
            fec: Some(LinkFec::Rs),
            autoneg: true,
            lldp: None,
            tx_eq: None,
        }
    }

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct PortConfigWrapper {
        port: UserSpecifiedPortConfig,
    }

    fn assert_roundtrips(config: UserSpecifiedPortConfig) {
        let json = serde_json::to_string(&config).unwrap();
        let from_json: UserSpecifiedPortConfig = serde_json::from_str(&json)
            .unwrap_or_else(|err| panic!("JSON {json}: {err}"));
        assert_eq!(from_json, config);

        let wrapper = PortConfigWrapper { port: config };
        let toml_str = toml::to_string(&wrapper).unwrap();
        let from_toml: PortConfigWrapper = toml::from_str(&toml_str)
            .unwrap_or_else(|err| panic!("TOML {toml_str}: {err}"));
        assert_eq!(from_toml, wrapper);
    }

    #[test]
    fn uplink_roundtrips() {
        assert_roundtrips(
            UserSpecifiedPortConfig::Uplink(uplink_port_config()),
        );
    }

    #[test]
    fn ddm_roundtrips() {
        assert_roundtrips(UserSpecifiedPortConfig::Ddm(l1_port_config()));
    }

    #[test]
    fn untagged_uplink_deserializes_to_uplink_variant() {
        let expected = UserSpecifiedPortConfig::Uplink(uplink_port_config());

        let json = serde_json::to_string(&uplink_port_config()).unwrap();
        let from_json: UserSpecifiedPortConfig = serde_json::from_str(&json)
            .unwrap_or_else(|err| panic!("JSON {json}: {err}"));
        assert_eq!(from_json, expected);

        let toml_str = r#"
            [port]
            uplink_port_speed = "speed40_g"
            uplink_port_fec = "rs"
            autoneg = false

            [[port.routes]]
            destination = "0.0.0.0/0"
            nexthop = "172.30.0.10"
            vlan_id = 1

            [[port.addresses]]
            address = "1.1.1.0/24"
        "#;
        let from_toml: PortConfigWrapper = toml::from_str(toml_str)
            .unwrap_or_else(|err| panic!("TOML {toml_str}: {err}"));
        assert_eq!(from_toml.port, expected);
    }

    #[test]
    fn tagged_ddm_deserializes_to_ddm_variant() {
        let json = serde_json::json!({
            "tag": "ddm",
            "val": {
                "speed": "speed100_g",
                "fec": "rs",
                "autoneg": true,
            },
        });
        assert_eq!(
            serde_json::from_value::<UserSpecifiedPortConfig>(json).unwrap(),
            UserSpecifiedPortConfig::Ddm(l1_port_config()),
        );
    }

    #[test]
    fn tag_payload_mismatch_is_rejected() {
        let json = serde_json::json!({
            "tag": "ddm",
            "val": serde_json::to_value(uplink_port_config()).unwrap(),
        });

        let err = serde_json::from_value::<UserSpecifiedPortConfig>(json)
            .expect_err("a ddm tag over uplink data should fail");
        assert!(
            err.to_string().contains("does not match"),
            "unexpected error: {err}"
        );
    }
}
