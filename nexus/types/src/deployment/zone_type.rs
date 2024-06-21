// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types representing types of Omicron zones managed by blueprints
//!
//! These types are closely related to the `OmicronZoneType` in sled-agent's
//! internal API, but include additional information needed by Reconfigurator
//! that is not needed by sled-agent.

use super::OmicronZoneExternalIp;
use omicron_common::api::internal::shared::NetworkInterface;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_client::types::OmicronZoneDataset;
use sled_agent_client::types::OmicronZoneType;
use sled_agent_client::ZoneKind;

#[derive(Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BlueprintZoneType {
    BoundaryNtp(blueprint_zone_type::BoundaryNtp),
    Clickhouse(blueprint_zone_type::Clickhouse),
    ClickhouseKeeper(blueprint_zone_type::ClickhouseKeeper),
    CockroachDb(blueprint_zone_type::CockroachDb),
    Crucible(blueprint_zone_type::Crucible),
    CruciblePantry(blueprint_zone_type::CruciblePantry),
    ExternalDns(blueprint_zone_type::ExternalDns),
    InternalDns(blueprint_zone_type::InternalDns),
    InternalNtp(blueprint_zone_type::InternalNtp),
    Nexus(blueprint_zone_type::Nexus),
    Oximeter(blueprint_zone_type::Oximeter),
}

impl BlueprintZoneType {
    /// Returns the zpool being used by this zone, if any.
    pub fn zpool(&self) -> Option<&omicron_common::zpool_name::ZpoolName> {
        match self {
            BlueprintZoneType::ExternalDns(
                blueprint_zone_type::ExternalDns { dataset, .. },
            )
            | BlueprintZoneType::Clickhouse(
                blueprint_zone_type::Clickhouse { dataset, .. },
            )
            | BlueprintZoneType::ClickhouseKeeper(
                blueprint_zone_type::ClickhouseKeeper { dataset, .. },
            )
            | BlueprintZoneType::CockroachDb(
                blueprint_zone_type::CockroachDb { dataset, .. },
            )
            | BlueprintZoneType::Crucible(blueprint_zone_type::Crucible {
                dataset,
                ..
            })
            | BlueprintZoneType::InternalDns(
                blueprint_zone_type::InternalDns { dataset, .. },
            ) => Some(&dataset.pool_name),
            BlueprintZoneType::BoundaryNtp(_)
            | BlueprintZoneType::InternalNtp(_)
            | BlueprintZoneType::Nexus(_)
            | BlueprintZoneType::Oximeter(_)
            | BlueprintZoneType::CruciblePantry(_) => None,
        }
    }

    pub fn external_networking(
        &self,
    ) -> Option<(OmicronZoneExternalIp, &NetworkInterface)> {
        match self {
            BlueprintZoneType::Nexus(nexus) => Some((
                OmicronZoneExternalIp::Floating(nexus.external_ip),
                &nexus.nic,
            )),
            BlueprintZoneType::ExternalDns(dns) => Some((
                OmicronZoneExternalIp::Floating(dns.dns_address.into_ip()),
                &dns.nic,
            )),
            BlueprintZoneType::BoundaryNtp(ntp) => {
                Some((OmicronZoneExternalIp::Snat(ntp.external_ip), &ntp.nic))
            }
            BlueprintZoneType::Clickhouse(_)
            | BlueprintZoneType::ClickhouseKeeper(_)
            | BlueprintZoneType::CockroachDb(_)
            | BlueprintZoneType::Crucible(_)
            | BlueprintZoneType::CruciblePantry(_)
            | BlueprintZoneType::InternalDns(_)
            | BlueprintZoneType::InternalNtp(_)
            | BlueprintZoneType::Oximeter(_) => None,
        }
    }

    /// Identifies whether this is an NTP zone (any flavor)
    pub fn is_ntp(&self) -> bool {
        match self {
            BlueprintZoneType::InternalNtp(_)
            | BlueprintZoneType::BoundaryNtp(_) => true,
            BlueprintZoneType::Nexus(_)
            | BlueprintZoneType::ExternalDns(_)
            | BlueprintZoneType::Clickhouse(_)
            | BlueprintZoneType::ClickhouseKeeper(_)
            | BlueprintZoneType::CockroachDb(_)
            | BlueprintZoneType::Crucible(_)
            | BlueprintZoneType::CruciblePantry(_)
            | BlueprintZoneType::InternalDns(_)
            | BlueprintZoneType::Oximeter(_) => false,
        }
    }

    /// Identifies whether this is a Nexus zone
    pub fn is_nexus(&self) -> bool {
        match self {
            BlueprintZoneType::Nexus(_) => true,
            BlueprintZoneType::BoundaryNtp(_)
            | BlueprintZoneType::ExternalDns(_)
            | BlueprintZoneType::Clickhouse(_)
            | BlueprintZoneType::ClickhouseKeeper(_)
            | BlueprintZoneType::CockroachDb(_)
            | BlueprintZoneType::Crucible(_)
            | BlueprintZoneType::CruciblePantry(_)
            | BlueprintZoneType::InternalDns(_)
            | BlueprintZoneType::InternalNtp(_)
            | BlueprintZoneType::Oximeter(_) => false,
        }
    }

    /// Identifies whether this a Crucible (not Crucible pantry) zone
    pub fn is_crucible(&self) -> bool {
        match self {
            BlueprintZoneType::Crucible(_) => true,
            BlueprintZoneType::BoundaryNtp(_)
            | BlueprintZoneType::Clickhouse(_)
            | BlueprintZoneType::ClickhouseKeeper(_)
            | BlueprintZoneType::CockroachDb(_)
            | BlueprintZoneType::CruciblePantry(_)
            | BlueprintZoneType::ExternalDns(_)
            | BlueprintZoneType::InternalDns(_)
            | BlueprintZoneType::InternalNtp(_)
            | BlueprintZoneType::Nexus(_)
            | BlueprintZoneType::Oximeter(_) => false,
        }
    }

    /// Returns a durable dataset associated with this zone, if any exists.
    pub fn durable_dataset(&self) -> Option<&OmicronZoneDataset> {
        match self {
            BlueprintZoneType::Clickhouse(
                blueprint_zone_type::Clickhouse { dataset, .. },
            )
            | BlueprintZoneType::ClickhouseKeeper(
                blueprint_zone_type::ClickhouseKeeper { dataset, .. },
            )
            | BlueprintZoneType::CockroachDb(
                blueprint_zone_type::CockroachDb { dataset, .. },
            )
            | BlueprintZoneType::Crucible(blueprint_zone_type::Crucible {
                dataset,
                ..
            })
            | BlueprintZoneType::ExternalDns(
                blueprint_zone_type::ExternalDns { dataset, .. },
            )
            | BlueprintZoneType::InternalDns(
                blueprint_zone_type::InternalDns { dataset, .. },
            ) => Some(dataset),
            // Transient-dataset-only zones
            BlueprintZoneType::BoundaryNtp(_)
            | BlueprintZoneType::CruciblePantry(_)
            | BlueprintZoneType::InternalNtp(_)
            | BlueprintZoneType::Nexus(_)
            | BlueprintZoneType::Oximeter(_) => None,
        }
    }
}

impl From<BlueprintZoneType> for OmicronZoneType {
    fn from(zone_type: BlueprintZoneType) -> Self {
        match zone_type {
            BlueprintZoneType::BoundaryNtp(zone) => Self::BoundaryNtp {
                address: zone.address.to_string(),
                ntp_servers: zone.ntp_servers,
                dns_servers: zone.dns_servers,
                domain: zone.domain,
                nic: zone.nic,
                snat_cfg: zone.external_ip.snat_cfg,
            },
            BlueprintZoneType::Clickhouse(zone) => Self::Clickhouse {
                address: zone.address.to_string(),
                dataset: zone.dataset,
            },
            BlueprintZoneType::ClickhouseKeeper(zone) => {
                Self::ClickhouseKeeper {
                    address: zone.address.to_string(),
                    dataset: zone.dataset,
                }
            }
            BlueprintZoneType::CockroachDb(zone) => Self::CockroachDb {
                address: zone.address.to_string(),
                dataset: zone.dataset,
            },
            BlueprintZoneType::Crucible(zone) => Self::Crucible {
                address: zone.address.to_string(),
                dataset: zone.dataset,
            },
            BlueprintZoneType::CruciblePantry(zone) => {
                Self::CruciblePantry { address: zone.address.to_string() }
            }
            BlueprintZoneType::ExternalDns(zone) => Self::ExternalDns {
                dataset: zone.dataset,
                http_address: zone.http_address.to_string(),
                dns_address: zone.dns_address.addr.to_string(),
                nic: zone.nic,
            },
            BlueprintZoneType::InternalDns(zone) => Self::InternalDns {
                dataset: zone.dataset,
                http_address: zone.http_address.to_string(),
                dns_address: zone.dns_address.to_string(),
                gz_address: zone.gz_address,
                gz_address_index: zone.gz_address_index,
            },
            BlueprintZoneType::InternalNtp(zone) => Self::InternalNtp {
                address: zone.address.to_string(),
                ntp_servers: zone.ntp_servers,
                dns_servers: zone.dns_servers,
                domain: zone.domain,
            },
            BlueprintZoneType::Nexus(zone) => Self::Nexus {
                internal_address: zone.internal_address.to_string(),
                external_ip: zone.external_ip.ip,
                nic: zone.nic,
                external_tls: zone.external_tls,
                external_dns_servers: zone.external_dns_servers,
            },
            BlueprintZoneType::Oximeter(zone) => {
                Self::Oximeter { address: zone.address.to_string() }
            }
        }
    }
}

impl BlueprintZoneType {
    /// Returns the [`ZoneKind`] corresponding to this variant.
    pub fn kind(&self) -> ZoneKind {
        match self {
            Self::BoundaryNtp(_) => ZoneKind::BoundaryNtp,
            Self::Clickhouse(_) => ZoneKind::Clickhouse,
            Self::ClickhouseKeeper(_) => ZoneKind::ClickhouseKeeper,
            Self::CockroachDb(_) => ZoneKind::CockroachDb,
            Self::Crucible(_) => ZoneKind::Crucible,
            Self::CruciblePantry(_) => ZoneKind::CruciblePantry,
            Self::ExternalDns(_) => ZoneKind::ExternalDns,
            Self::InternalDns(_) => ZoneKind::InternalDns,
            Self::InternalNtp(_) => ZoneKind::InternalNtp,
            Self::Nexus(_) => ZoneKind::Nexus,
            Self::Oximeter(_) => ZoneKind::Oximeter,
        }
    }
}

pub mod blueprint_zone_type {
    use crate::deployment::OmicronZoneExternalFloatingAddr;
    use crate::deployment::OmicronZoneExternalFloatingIp;
    use crate::deployment::OmicronZoneExternalSnatIp;
    use crate::inventory::OmicronZoneDataset;
    use omicron_common::api::internal::shared::NetworkInterface;
    use schemars::JsonSchema;
    use serde::Deserialize;
    use serde::Serialize;
    use std::net::IpAddr;
    use std::net::Ipv6Addr;
    use std::net::SocketAddrV6;

    #[derive(
        Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize,
    )]
    pub struct BoundaryNtp {
        pub address: SocketAddrV6,
        pub ntp_servers: Vec<String>,
        pub dns_servers: Vec<IpAddr>,
        pub domain: Option<String>,
        /// The service vNIC providing outbound connectivity using OPTE.
        pub nic: NetworkInterface,
        pub external_ip: OmicronZoneExternalSnatIp,
    }

    #[derive(
        Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize,
    )]
    pub struct Clickhouse {
        pub address: SocketAddrV6,
        pub dataset: OmicronZoneDataset,
    }

    #[derive(
        Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize,
    )]
    pub struct ClickhouseKeeper {
        pub address: SocketAddrV6,
        pub dataset: OmicronZoneDataset,
    }

    #[derive(
        Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize,
    )]
    pub struct CockroachDb {
        pub address: SocketAddrV6,
        pub dataset: OmicronZoneDataset,
    }

    #[derive(
        Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize,
    )]
    pub struct Crucible {
        pub address: SocketAddrV6,
        pub dataset: OmicronZoneDataset,
    }

    #[derive(
        Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize,
    )]
    pub struct CruciblePantry {
        pub address: SocketAddrV6,
    }

    #[derive(
        Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize,
    )]
    pub struct ExternalDns {
        pub dataset: OmicronZoneDataset,
        /// The address at which the external DNS server API is reachable.
        pub http_address: SocketAddrV6,
        /// The address at which the external DNS server is reachable.
        pub dns_address: OmicronZoneExternalFloatingAddr,
        /// The service vNIC providing external connectivity using OPTE.
        pub nic: NetworkInterface,
    }

    #[derive(
        Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize,
    )]
    pub struct InternalDns {
        pub dataset: OmicronZoneDataset,
        pub http_address: SocketAddrV6,
        pub dns_address: SocketAddrV6,
        /// The addresses in the global zone which should be created
        ///
        /// For the DNS service, which exists outside the sleds's typical subnet
        /// - adding an address in the GZ is necessary to allow inter-zone
        /// traffic routing.
        pub gz_address: Ipv6Addr,

        /// The address is also identified with an auxiliary bit of information
        /// to ensure that the created global zone address can have a unique
        /// name.
        pub gz_address_index: u32,
    }

    #[derive(
        Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize,
    )]
    pub struct InternalNtp {
        pub address: SocketAddrV6,
        pub ntp_servers: Vec<String>,
        pub dns_servers: Vec<IpAddr>,
        pub domain: Option<String>,
    }

    #[derive(
        Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize,
    )]
    pub struct Nexus {
        /// The address at which the internal nexus server is reachable.
        pub internal_address: SocketAddrV6,
        /// The address at which the external nexus server is reachable.
        pub external_ip: OmicronZoneExternalFloatingIp,
        /// The service vNIC providing external connectivity using OPTE.
        pub nic: NetworkInterface,
        /// Whether Nexus's external endpoint should use TLS
        pub external_tls: bool,
        /// External DNS servers Nexus can use to resolve external hosts.
        pub external_dns_servers: Vec<IpAddr>,
    }

    #[derive(
        Debug, Clone, Eq, PartialEq, JsonSchema, Deserialize, Serialize,
    )]
    pub struct Oximeter {
        pub address: SocketAddrV6,
    }
}
