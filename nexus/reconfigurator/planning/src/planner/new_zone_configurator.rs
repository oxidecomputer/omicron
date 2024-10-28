// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Logic for configuring [`BlueprintZoneConfig`]s for new zones constructed by
//! the planner.

use crate::planner::ExternalSnatNetworkingChoice;

use super::allocators::ExternalNetworkingError;
use super::allocators::InternalDnsAllocation;
use super::allocators::SledAllocationError;
use super::allocators::SledAllocators;
use super::rng::PlannerRng;
use super::ExternalNetworkingChoice;
use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::OmicronZoneExternalFloatingAddr;
use nexus_types::deployment::OmicronZoneExternalFloatingIp;
use nexus_types::deployment::OmicronZoneExternalSnatIp;
use nexus_types::inventory::NetworkInterface;
use nexus_types::inventory::NetworkInterfaceKind;
use nexus_types::inventory::ZpoolName;
use omicron_common::address::CLICKHOUSE_HTTP_PORT;
use omicron_common::address::DNS_HTTP_PORT;
use omicron_common::address::DNS_PORT;
use omicron_common::address::NTP_PORT;
use omicron_common::api::external::Vni;
use omicron_uuid_kinds::GenericUuid as _;
use omicron_uuid_kinds::SledUuid;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;

#[derive(Debug, thiserror::Error)]
pub enum ZoneConfigurationError {
    #[error("resource allocation failed for {kind:?} zone on sled {sled_id}")]
    ResourceAllocationError {
        kind: ZoneKind,
        sled_id: SledUuid,
        #[source]
        err: SledAllocationError,
    },
    #[error(
        "external networking allocation failed for {kind:?} zone \
         on sled {sled_id}"
    )]
    ExternalNetworkingError {
        kind: ZoneKind,
        sled_id: SledUuid,
        #[source]
        err: ExternalNetworkingError,
    },
}

#[derive(Debug)]
pub(super) struct NewZoneConfigurator<'a> {
    rng: &'a mut PlannerRng,
    sled_allocators: SledAllocators<'a>,
}

impl<'a> NewZoneConfigurator<'a> {
    pub fn new(
        rng: &'a mut PlannerRng,
        sled_allocators: SledAllocators<'a>,
    ) -> Self {
        Self { rng, sled_allocators }
    }

    fn alloc_ip_and_zpool(
        &mut self,
        sled_id: SledUuid,
        kind: ZoneKind,
    ) -> Result<(Ipv6Addr, ZpoolName), ZoneConfigurationError> {
        let underlay_ip = self
            .sled_allocators
            .alloc_underlay_ip(sled_id)
            .map_err(|err| ZoneConfigurationError::ResourceAllocationError {
                kind,
                sled_id,
                err,
            })?;
        let pool_name = self
            .sled_allocators
            .alloc_zpool(sled_id, kind)
            .map_err(|err| ZoneConfigurationError::ResourceAllocationError {
                kind,
                sled_id,
                err,
            })?;
        Ok((underlay_ip, pool_name))
    }

    pub(super) fn boundary_ntp(
        &mut self,
        sled_id: SledUuid,
        ntp_servers: Vec<String>,
        dns_servers: Vec<IpAddr>,
        domain: Option<String>,
    ) -> Result<BlueprintZoneConfig, ZoneConfigurationError> {
        let kind = ZoneKind::BoundaryNtp;
        let new_zone_id = self.rng.next_zone();
        let ExternalSnatNetworkingChoice {
            snat_cfg,
            nic_ip,
            nic_subnet,
            nic_mac,
        } = self
            .sled_allocators
            .external_networking()
            .for_new_boundary_ntp()
            .map_err(|err| ZoneConfigurationError::ExternalNetworkingError {
            kind,
            sled_id,
            err,
        })?;
        let external_ip = OmicronZoneExternalSnatIp {
            id: self.rng.next_external_ip(),
            snat_cfg,
        };
        let nic = NetworkInterface {
            id: self.rng.next_network_interface(),
            kind: NetworkInterfaceKind::Service {
                id: new_zone_id.into_untyped_uuid(),
            },
            name: format!("ntp-{new_zone_id}").parse().unwrap(),
            ip: nic_ip,
            mac: nic_mac,
            subnet: nic_subnet,
            vni: Vni::SERVICES_VNI,
            primary: true,
            slot: 0,
            transit_ips: vec![],
        };

        let (underlay_ip, filesystem_pool) =
            self.alloc_ip_and_zpool(sled_id, kind)?;
        let port = omicron_common::address::NTP_PORT;
        let zone_type =
            BlueprintZoneType::BoundaryNtp(blueprint_zone_type::BoundaryNtp {
                address: SocketAddrV6::new(underlay_ip, port, 0, 0),
                ntp_servers,
                dns_servers,
                domain,
                nic,
                external_ip,
            });

        Ok(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: new_zone_id,
            filesystem_pool: Some(filesystem_pool),
            zone_type,
        })
    }

    pub(super) fn clickhouse(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<BlueprintZoneConfig, ZoneConfigurationError> {
        let kind = ZoneKind::Clickhouse;
        let id = self.rng.next_zone();
        let (underlay_ip, pool_name) =
            self.alloc_ip_and_zpool(sled_id, kind)?;
        let address =
            SocketAddrV6::new(underlay_ip, CLICKHOUSE_HTTP_PORT, 0, 0);
        let zone_type =
            BlueprintZoneType::Clickhouse(blueprint_zone_type::Clickhouse {
                address,
                dataset: OmicronZoneDataset { pool_name: pool_name.clone() },
            });

        Ok(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id,
            filesystem_pool: Some(pool_name),
            zone_type,
        })
    }

    pub(super) fn clickhouse_keeper(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<BlueprintZoneConfig, ZoneConfigurationError> {
        let kind = ZoneKind::ClickhouseKeeper;
        let zone_id = self.rng.next_zone();
        let (underlay_ip, pool_name) =
            self.alloc_ip_and_zpool(sled_id, kind)?;
        let port = omicron_common::address::CLICKHOUSE_KEEPER_TCP_PORT;
        let address = SocketAddrV6::new(underlay_ip, port, 0, 0);
        let zone_type = BlueprintZoneType::ClickhouseKeeper(
            blueprint_zone_type::ClickhouseKeeper {
                address,
                dataset: OmicronZoneDataset { pool_name: pool_name.clone() },
            },
        );
        let filesystem_pool = pool_name;

        Ok(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: zone_id,
            filesystem_pool: Some(filesystem_pool),
            zone_type,
        })
    }

    pub(super) fn clickhouse_server(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<BlueprintZoneConfig, ZoneConfigurationError> {
        let kind = ZoneKind::ClickhouseServer;
        let zone_id = self.rng.next_zone();
        let (underlay_ip, pool_name) =
            self.alloc_ip_and_zpool(sled_id, kind)?;
        let address =
            SocketAddrV6::new(underlay_ip, CLICKHOUSE_HTTP_PORT, 0, 0);
        let zone_type = BlueprintZoneType::ClickhouseServer(
            blueprint_zone_type::ClickhouseServer {
                address,
                dataset: OmicronZoneDataset { pool_name: pool_name.clone() },
            },
        );
        let filesystem_pool = pool_name;

        Ok(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: zone_id,
            filesystem_pool: Some(filesystem_pool),
            zone_type,
        })
    }

    pub(super) fn cockroachdb(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<BlueprintZoneConfig, ZoneConfigurationError> {
        let kind = ZoneKind::CockroachDb;
        let zone_id = self.rng.next_zone();
        let (underlay_ip, pool_name) =
            self.alloc_ip_and_zpool(sled_id, kind)?;
        let port = omicron_common::address::COCKROACH_PORT;
        let address = SocketAddrV6::new(underlay_ip, port, 0, 0);
        let zone_type =
            BlueprintZoneType::CockroachDb(blueprint_zone_type::CockroachDb {
                address,
                dataset: OmicronZoneDataset { pool_name: pool_name.clone() },
            });
        let filesystem_pool = pool_name;

        Ok(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: zone_id,
            filesystem_pool: Some(filesystem_pool),
            zone_type,
        })
    }

    pub(super) fn crucible(
        &mut self,
        sled_id: SledUuid,
        pool_name: ZpoolName,
    ) -> Result<BlueprintZoneConfig, ZoneConfigurationError> {
        let kind = ZoneKind::Crucible;
        let ip =
            self.sled_allocators.alloc_underlay_ip(sled_id).map_err(|err| {
                ZoneConfigurationError::ResourceAllocationError {
                    kind,
                    sled_id,
                    err,
                }
            })?;
        let port = omicron_common::address::CRUCIBLE_PORT;
        let address = SocketAddrV6::new(ip, port, 0, 0);
        let zone_type =
            BlueprintZoneType::Crucible(blueprint_zone_type::Crucible {
                address,
                dataset: OmicronZoneDataset { pool_name: pool_name.clone() },
            });
        let filesystem_pool = pool_name;

        Ok(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: self.rng.next_zone(),
            filesystem_pool: Some(filesystem_pool),
            zone_type,
        })
    }

    pub(super) fn crucible_pantry(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<BlueprintZoneConfig, ZoneConfigurationError> {
        let kind = ZoneKind::CruciblePantry;
        let pantry_id = self.rng.next_zone();
        let (ip, filesystem_pool) = self.alloc_ip_and_zpool(sled_id, kind)?;
        let port = omicron_common::address::CRUCIBLE_PANTRY_PORT;
        let address = SocketAddrV6::new(ip, port, 0, 0);
        let zone_type = BlueprintZoneType::CruciblePantry(
            blueprint_zone_type::CruciblePantry { address },
        );

        Ok(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: pantry_id,
            filesystem_pool: Some(filesystem_pool),
            zone_type,
        })
    }

    pub(super) fn external_dns(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<BlueprintZoneConfig, ZoneConfigurationError> {
        let kind = ZoneKind::ExternalDns;
        let id = self.rng.next_zone();
        let ExternalNetworkingChoice {
            external_ip,
            nic_ip,
            nic_subnet,
            nic_mac,
        } = self
            .sled_allocators
            .external_networking()
            .for_new_external_dns()
            .map_err(|err| ZoneConfigurationError::ExternalNetworkingError {
            kind,
            sled_id,
            err,
        })?;
        let nic = NetworkInterface {
            id: self.rng.next_network_interface(),
            kind: NetworkInterfaceKind::Service { id: id.into_untyped_uuid() },
            name: format!("external-dns-{id}").parse().unwrap(),
            ip: nic_ip,
            mac: nic_mac,
            subnet: nic_subnet,
            vni: Vni::SERVICES_VNI,
            primary: true,
            slot: 0,
            transit_ips: vec![],
        };

        let (underlay_ip, pool_name) =
            self.alloc_ip_and_zpool(sled_id, kind)?;
        let http_address = SocketAddrV6::new(underlay_ip, DNS_HTTP_PORT, 0, 0);
        let dns_address = OmicronZoneExternalFloatingAddr {
            id: self.rng.next_external_ip(),
            addr: SocketAddr::new(external_ip, DNS_PORT),
        };
        let zone_type =
            BlueprintZoneType::ExternalDns(blueprint_zone_type::ExternalDns {
                dataset: OmicronZoneDataset { pool_name: pool_name.clone() },
                http_address,
                dns_address,
                nic,
            });

        Ok(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id,
            filesystem_pool: Some(pool_name),
            zone_type,
        })
    }

    pub(super) fn internal_dns(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<BlueprintZoneConfig, ZoneConfigurationError> {
        let kind = ZoneKind::InternalDns;
        let InternalDnsAllocation { subnet, gz_address_index } = self
            .sled_allocators
            .alloc_internal_dns_subnet(sled_id)
            .map_err(|err| ZoneConfigurationError::ResourceAllocationError {
                kind,
                sled_id,
                err,
            })?;
        let address = subnet.dns_address();
        let zpool =
            self.sled_allocators.alloc_zpool(sled_id, kind).map_err(|err| {
                ZoneConfigurationError::ResourceAllocationError {
                    kind,
                    sled_id,
                    err,
                }
            })?;
        let zone_type =
            BlueprintZoneType::InternalDns(blueprint_zone_type::InternalDns {
                dataset: OmicronZoneDataset { pool_name: zpool.clone() },
                dns_address: SocketAddrV6::new(address, DNS_PORT, 0, 0),
                http_address: SocketAddrV6::new(address, DNS_HTTP_PORT, 0, 0),
                gz_address: subnet.gz_address(),
                gz_address_index,
            });

        Ok(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: self.rng.next_zone(),
            filesystem_pool: Some(zpool),
            zone_type,
        })
    }

    pub(super) fn internal_ntp(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<BlueprintZoneConfig, ZoneConfigurationError> {
        let kind = ZoneKind::InternalNtp;
        let (ip, filesystem_pool) = self.alloc_ip_and_zpool(sled_id, kind)?;
        let zone_type =
            BlueprintZoneType::InternalNtp(blueprint_zone_type::InternalNtp {
                address: SocketAddrV6::new(ip, NTP_PORT, 0, 0),
            });

        Ok(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: self.rng.next_zone(),
            filesystem_pool: Some(filesystem_pool),
            zone_type,
        })
    }

    pub(super) fn nexus(
        &mut self,
        sled_id: SledUuid,
        external_tls: bool,
        external_dns_servers: Vec<IpAddr>,
    ) -> Result<BlueprintZoneConfig, ZoneConfigurationError> {
        let kind = ZoneKind::Nexus;
        let nexus_id = self.rng.next_zone();
        let ExternalNetworkingChoice {
            external_ip,
            nic_ip,
            nic_subnet,
            nic_mac,
        } = self
            .sled_allocators
            .external_networking()
            .for_new_nexus()
            .map_err(|err| ZoneConfigurationError::ExternalNetworkingError {
                kind,
                sled_id,
                err,
            })?;
        let external_ip = OmicronZoneExternalFloatingIp {
            id: self.rng.next_external_ip(),
            ip: external_ip,
        };

        let nic = NetworkInterface {
            id: self.rng.next_network_interface(),
            kind: NetworkInterfaceKind::Service {
                id: nexus_id.into_untyped_uuid(),
            },
            name: format!("nexus-{nexus_id}").parse().unwrap(),
            ip: nic_ip,
            mac: nic_mac,
            subnet: nic_subnet,
            vni: Vni::SERVICES_VNI,
            primary: true,
            slot: 0,
            transit_ips: vec![],
        };

        let (ip, filesystem_pool) = self.alloc_ip_and_zpool(sled_id, kind)?;
        let port = omicron_common::address::NEXUS_INTERNAL_PORT;
        let internal_address = SocketAddrV6::new(ip, port, 0, 0);
        let zone_type = BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
            internal_address,
            external_ip,
            nic,
            external_tls,
            external_dns_servers: external_dns_servers.clone(),
        });

        Ok(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: nexus_id,
            filesystem_pool: Some(filesystem_pool),
            zone_type,
        })
    }

    pub(super) fn oximeter(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<BlueprintZoneConfig, ZoneConfigurationError> {
        let kind = ZoneKind::Oximeter;
        let oximeter_id = self.rng.next_zone();
        let (ip, filesystem_pool) = self.alloc_ip_and_zpool(sled_id, kind)?;
        let port = omicron_common::address::OXIMETER_PORT;
        let address = SocketAddrV6::new(ip, port, 0, 0);
        let zone_type =
            BlueprintZoneType::Oximeter(blueprint_zone_type::Oximeter {
                address,
            });

        Ok(BlueprintZoneConfig {
            disposition: BlueprintZoneDisposition::InService,
            id: oximeter_id,
            filesystem_pool: Some(filesystem_pool),
            zone_type,
        })
    }
}
