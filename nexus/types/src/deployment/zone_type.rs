// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types representing types of Omicron zones managed by blueprints
//!
//! These types are closely related to the `OmicronZoneType` in sled-agent's
//! internal API, but include additional information needed by Reconfigurator
//! that is not needed by sled-agent.

use super::OmicronZoneExternalIp;
use daft::Diffable;
use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
use nexus_sled_agent_shared::inventory::OmicronZoneType;
use nexus_sled_agent_shared::inventory::ZoneKind;
use omicron_common::api::internal::shared::DatasetKind;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::disk::DatasetName;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::net::Ipv6Addr;

#[derive(
    Debug,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    JsonSchema,
    Deserialize,
    Serialize,
    Diffable,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BlueprintZoneType {
    BoundaryNtp(blueprint_zone_type::BoundaryNtp),
    Clickhouse(blueprint_zone_type::Clickhouse),
    ClickhouseKeeper(blueprint_zone_type::ClickhouseKeeper),
    ClickhouseServer(blueprint_zone_type::ClickhouseServer),
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
    /// Returns the underlay IP address associated with this zone.
    ///
    /// Assumes all zone have exactly one underlay IP address (which is
    /// currently true).
    pub fn underlay_ip(&self) -> Ipv6Addr {
        match self {
            BlueprintZoneType::BoundaryNtp(
                blueprint_zone_type::BoundaryNtp { address, .. },
            )
            | BlueprintZoneType::Clickhouse(
                blueprint_zone_type::Clickhouse { address, .. },
            )
            | BlueprintZoneType::ClickhouseKeeper(
                blueprint_zone_type::ClickhouseKeeper { address, .. },
            )
            | BlueprintZoneType::ClickhouseServer(
                blueprint_zone_type::ClickhouseServer { address, .. },
            )
            | BlueprintZoneType::CockroachDb(
                blueprint_zone_type::CockroachDb { address, .. },
            )
            | BlueprintZoneType::Crucible(blueprint_zone_type::Crucible {
                address,
                ..
            })
            | BlueprintZoneType::CruciblePantry(
                blueprint_zone_type::CruciblePantry { address },
            )
            | BlueprintZoneType::ExternalDns(
                blueprint_zone_type::ExternalDns {
                    http_address: address, ..
                },
            )
            | BlueprintZoneType::InternalNtp(
                blueprint_zone_type::InternalNtp { address },
            )
            | BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                internal_address: address,
                ..
            })
            | BlueprintZoneType::Oximeter(blueprint_zone_type::Oximeter {
                address,
            }) => *address.ip(),
            BlueprintZoneType::InternalDns(
                blueprint_zone_type::InternalDns {
                    http_address: address,
                    dns_address,
                    ..
                },
            ) => {
                // InternalDns is the only variant that carries two
                // `SocketAddrV6`s that are both on the underlay network. We
                // expect these to have the same IP address.
                debug_assert_eq!(address.ip(), dns_address.ip());
                *address.ip()
            }
        }
    }

    /// Returns the zpool being used by this zone, if any.
    pub fn durable_zpool(
        &self,
    ) -> Option<&omicron_common::zpool_name::ZpoolName> {
        self.durable_dataset().map(|dataset| &dataset.dataset.pool_name)
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
            | BlueprintZoneType::ClickhouseServer(_)
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
        matches!(
            self,
            BlueprintZoneType::InternalNtp(_)
                | BlueprintZoneType::BoundaryNtp(_)
        )
    }

    /// Identifies whether this is a boundary NTP zone
    pub fn is_boundary_ntp(&self) -> bool {
        matches!(self, BlueprintZoneType::BoundaryNtp(_))
    }

    /// Identifies whether this is a Nexus zone
    pub fn is_nexus(&self) -> bool {
        matches!(self, BlueprintZoneType::Nexus(_))
    }

    /// Identifies whether this is an internal DNS zone
    pub fn is_internal_dns(&self) -> bool {
        matches!(self, BlueprintZoneType::InternalDns(_))
    }

    /// Identifies whether this is an external DNS zone
    pub fn is_external_dns(&self) -> bool {
        matches!(self, BlueprintZoneType::ExternalDns(_))
    }

    /// Identifies whether this a CockroachDB zone
    pub fn is_cockroach(&self) -> bool {
        matches!(self, BlueprintZoneType::CockroachDb(_))
    }

    /// Identifies whether this a Crucible (not Crucible pantry) zone
    pub fn is_crucible(&self) -> bool {
        matches!(self, BlueprintZoneType::Crucible(_))
    }

    /// Identifies whether this is a Crucible pantry zone
    pub fn is_crucible_pantry(&self) -> bool {
        matches!(self, BlueprintZoneType::CruciblePantry(_))
    }

    /// Identifies whether this is a clickhouse keeper zone
    pub fn is_clickhouse_keeper(&self) -> bool {
        matches!(self, BlueprintZoneType::ClickhouseKeeper(_))
    }

    /// Identifies whether this is a clickhouse server zone
    pub fn is_clickhouse_server(&self) -> bool {
        matches!(self, BlueprintZoneType::ClickhouseServer(_))
    }

    /// Identifies whether this is a single-node clickhouse zone
    pub fn is_clickhouse(&self) -> bool {
        matches!(self, BlueprintZoneType::Clickhouse(_))
    }

    /// Returns the durable dataset associated with this zone, if any exists.
    pub fn durable_dataset(&self) -> Option<DurableDataset<'_>> {
        let (dataset, kind) = match self {
            BlueprintZoneType::Clickhouse(
                blueprint_zone_type::Clickhouse { dataset, .. },
            ) => (dataset, DatasetKind::Clickhouse),
            BlueprintZoneType::ClickhouseKeeper(
                blueprint_zone_type::ClickhouseKeeper { dataset, .. },
            ) => (dataset, DatasetKind::ClickhouseKeeper),
            BlueprintZoneType::ClickhouseServer(
                blueprint_zone_type::ClickhouseServer { dataset, .. },
            ) => (dataset, DatasetKind::ClickhouseServer),
            BlueprintZoneType::CockroachDb(
                blueprint_zone_type::CockroachDb { dataset, .. },
            ) => (dataset, DatasetKind::Cockroach),
            BlueprintZoneType::Crucible(blueprint_zone_type::Crucible {
                dataset,
                ..
            }) => (dataset, DatasetKind::Crucible),
            BlueprintZoneType::ExternalDns(
                blueprint_zone_type::ExternalDns { dataset, .. },
            ) => (dataset, DatasetKind::ExternalDns),
            BlueprintZoneType::InternalDns(
                blueprint_zone_type::InternalDns { dataset, .. },
            ) => (dataset, DatasetKind::InternalDns),
            // Transient-dataset-only zones
            BlueprintZoneType::BoundaryNtp(_)
            | BlueprintZoneType::CruciblePantry(_)
            | BlueprintZoneType::InternalNtp(_)
            | BlueprintZoneType::Nexus(_)
            | BlueprintZoneType::Oximeter(_) => return None,
        };

        Some(DurableDataset { dataset, kind })
    }
}

pub struct DurableDataset<'a> {
    pub dataset: &'a OmicronZoneDataset,
    pub kind: DatasetKind,
}

impl<'a> From<DurableDataset<'a>> for DatasetName {
    fn from(d: DurableDataset<'a>) -> Self {
        DatasetName::new(d.dataset.pool_name, d.kind)
    }
}

impl From<BlueprintZoneType> for OmicronZoneType {
    fn from(zone_type: BlueprintZoneType) -> Self {
        match zone_type {
            BlueprintZoneType::BoundaryNtp(zone) => Self::BoundaryNtp {
                address: zone.address,
                ntp_servers: zone.ntp_servers,
                dns_servers: zone.dns_servers,
                domain: zone.domain,
                nic: zone.nic,
                snat_cfg: zone.external_ip.snat_cfg,
            },
            BlueprintZoneType::Clickhouse(zone) => Self::Clickhouse {
                address: zone.address,
                dataset: zone.dataset,
            },
            BlueprintZoneType::ClickhouseKeeper(zone) => {
                Self::ClickhouseKeeper {
                    address: zone.address,
                    dataset: zone.dataset,
                }
            }
            BlueprintZoneType::ClickhouseServer(zone) => {
                Self::ClickhouseServer {
                    address: zone.address,
                    dataset: zone.dataset,
                }
            }
            BlueprintZoneType::CockroachDb(zone) => Self::CockroachDb {
                address: zone.address,
                dataset: zone.dataset,
            },
            BlueprintZoneType::Crucible(zone) => {
                Self::Crucible { address: zone.address, dataset: zone.dataset }
            }
            BlueprintZoneType::CruciblePantry(zone) => {
                Self::CruciblePantry { address: zone.address }
            }
            BlueprintZoneType::ExternalDns(zone) => Self::ExternalDns {
                dataset: zone.dataset,
                http_address: zone.http_address,
                dns_address: zone.dns_address.addr,
                nic: zone.nic,
            },
            BlueprintZoneType::InternalDns(zone) => Self::InternalDns {
                dataset: zone.dataset,
                http_address: zone.http_address,
                dns_address: zone.dns_address,
                gz_address: zone.gz_address,
                gz_address_index: zone.gz_address_index,
            },
            BlueprintZoneType::InternalNtp(zone) => {
                Self::InternalNtp { address: zone.address }
            }
            BlueprintZoneType::Nexus(zone) => Self::Nexus {
                internal_address: zone.internal_address,
                lockstep_port: zone.lockstep_port,
                external_ip: zone.external_ip.ip,
                nic: zone.nic,
                external_tls: zone.external_tls,
                external_dns_servers: zone.external_dns_servers,
            },
            BlueprintZoneType::Oximeter(zone) => {
                Self::Oximeter { address: zone.address }
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
            Self::ClickhouseServer(_) => ZoneKind::ClickhouseServer,
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
    use daft::Diffable;
    use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
    use omicron_common::api::external::Generation;
    use omicron_common::api::internal::shared::NetworkInterface;
    use schemars::JsonSchema;
    use serde::Deserialize;
    use serde::Serialize;
    use std::net::IpAddr;
    use std::net::Ipv6Addr;
    use std::net::SocketAddrV6;

    #[derive(
        Debug,
        Clone,
        Eq,
        PartialEq,
        Ord,
        PartialOrd,
        JsonSchema,
        Deserialize,
        Serialize,
        Diffable,
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

    /// Used in single-node clickhouse setups
    #[derive(
        Debug,
        Clone,
        Eq,
        PartialEq,
        Ord,
        PartialOrd,
        JsonSchema,
        Deserialize,
        Serialize,
        Diffable,
    )]
    pub struct Clickhouse {
        pub address: SocketAddrV6,
        pub dataset: OmicronZoneDataset,
    }

    #[derive(
        Debug,
        Clone,
        Eq,
        PartialEq,
        Ord,
        PartialOrd,
        JsonSchema,
        Deserialize,
        Serialize,
        Diffable,
    )]
    pub struct ClickhouseKeeper {
        pub address: SocketAddrV6,
        pub dataset: OmicronZoneDataset,
    }

    /// Used in replicated clickhouse setups
    #[derive(
        Debug,
        Clone,
        Eq,
        PartialEq,
        Ord,
        PartialOrd,
        JsonSchema,
        Deserialize,
        Serialize,
        Diffable,
    )]
    pub struct ClickhouseServer {
        pub address: SocketAddrV6,
        pub dataset: OmicronZoneDataset,
    }

    #[derive(
        Debug,
        Clone,
        Eq,
        PartialEq,
        Ord,
        PartialOrd,
        JsonSchema,
        Deserialize,
        Serialize,
        Diffable,
    )]
    pub struct CockroachDb {
        pub address: SocketAddrV6,
        pub dataset: OmicronZoneDataset,
    }

    #[derive(
        Debug,
        Clone,
        Eq,
        PartialEq,
        Ord,
        PartialOrd,
        JsonSchema,
        Deserialize,
        Serialize,
        Diffable,
    )]
    pub struct Crucible {
        pub address: SocketAddrV6,
        pub dataset: OmicronZoneDataset,
    }

    #[derive(
        Debug,
        Clone,
        Eq,
        PartialEq,
        Ord,
        PartialOrd,
        JsonSchema,
        Deserialize,
        Serialize,
        Diffable,
    )]
    pub struct CruciblePantry {
        pub address: SocketAddrV6,
    }

    #[derive(
        Debug,
        Clone,
        Eq,
        PartialEq,
        Ord,
        PartialOrd,
        JsonSchema,
        Deserialize,
        Serialize,
        Diffable,
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
        Debug,
        Clone,
        Eq,
        PartialEq,
        Ord,
        PartialOrd,
        JsonSchema,
        Deserialize,
        Serialize,
        Diffable,
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
        Debug,
        Clone,
        Eq,
        PartialEq,
        Ord,
        PartialOrd,
        JsonSchema,
        Deserialize,
        Serialize,
        Diffable,
    )]
    pub struct InternalNtp {
        pub address: SocketAddrV6,
    }

    #[derive(
        Debug,
        Clone,
        Eq,
        PartialEq,
        Ord,
        PartialOrd,
        JsonSchema,
        Deserialize,
        Serialize,
        Diffable,
    )]
    pub struct Nexus {
        /// The address at which the internal nexus server is reachable.
        pub internal_address: SocketAddrV6,
        /// The port at which the lockstep server is reachable. This shares the
        /// same IP address with `internal_address`.
        pub lockstep_port: u16,
        /// The address at which the external nexus server is reachable.
        pub external_ip: OmicronZoneExternalFloatingIp,
        /// The service vNIC providing external connectivity using OPTE.
        pub nic: NetworkInterface,
        /// Whether Nexus's external endpoint should use TLS
        pub external_tls: bool,
        /// External DNS servers Nexus can use to resolve external hosts.
        pub external_dns_servers: Vec<IpAddr>,
        /// Generation number for this Nexus zone.
        /// This is used to coordinate handoff between old and new Nexus instances
        /// during updates. See RFD 588.
        pub nexus_generation: Generation,
    }

    impl Nexus {
        pub fn lockstep_address(&self) -> SocketAddrV6 {
            SocketAddrV6::new(
                *self.internal_address.ip(),
                self.lockstep_port,
                0,
                0,
            )
        }
    }

    #[derive(
        Debug,
        Clone,
        Eq,
        PartialEq,
        Ord,
        PartialOrd,
        JsonSchema,
        Deserialize,
        Serialize,
        Diffable,
    )]
    pub struct Oximeter {
        pub address: SocketAddrV6,
    }
}
