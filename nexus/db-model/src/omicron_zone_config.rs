// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for sharing nontrivial conversions between various `OmicronZoneConfig`
//! database serializations and the corresponding Nexus/sled-agent type
//!
//! Both inventory and deployment have nearly-identical tables to serialize
//! `OmicronZoneConfigs` that are collected or generated, respectively. We
//! expect those tables to diverge over time (e.g., inventory may start
//! collecting extra metadata like uptime). This module provides conversion
//! helpers for the parts of those tables that are common between the two.

use crate::inventory::ZoneType;
use crate::{ipv6, MacAddr, Name, SqlU16, SqlU32, SqlU8};
use anyhow::{anyhow, bail, ensure, Context};
use ipnetwork::IpNetwork;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::{
    blueprint_zone_type, OmicronZoneExternalFloatingAddr,
    OmicronZoneExternalFloatingIp, OmicronZoneExternalSnatIp,
};
use nexus_types::inventory::{NetworkInterface, OmicronZoneType};
use omicron_common::api::internal::shared::NetworkInterfaceKind;
use omicron_common::zpool_name::ZpoolName;
use omicron_uuid_kinds::{
    ExternalIpUuid, GenericUuid, OmicronZoneUuid, SledUuid, ZpoolUuid,
};
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct OmicronZone {
    pub(crate) sled_id: SledUuid,
    pub(crate) id: Uuid,
    pub(crate) underlay_address: ipv6::Ipv6Addr,
    pub(crate) filesystem_pool: ZpoolUuid,
    pub(crate) zone_type: ZoneType,
    pub(crate) primary_service_ip: ipv6::Ipv6Addr,
    pub(crate) primary_service_port: SqlU16,
    pub(crate) second_service_ip: Option<IpNetwork>,
    pub(crate) second_service_port: Option<SqlU16>,
    pub(crate) dataset_zpool_name: Option<String>,
    pub(crate) nic_id: Option<Uuid>,
    pub(crate) dns_gz_address: Option<ipv6::Ipv6Addr>,
    pub(crate) dns_gz_address_index: Option<SqlU32>,
    pub(crate) ntp_ntp_servers: Option<Vec<String>>,
    pub(crate) ntp_dns_servers: Option<Vec<IpNetwork>>,
    pub(crate) ntp_domain: Option<String>,
    pub(crate) nexus_external_tls: Option<bool>,
    pub(crate) nexus_external_dns_servers: Option<Vec<IpNetwork>>,
    pub(crate) snat_ip: Option<IpNetwork>,
    pub(crate) snat_first_port: Option<SqlU16>,
    pub(crate) snat_last_port: Option<SqlU16>,
    // Only present for BlueprintZoneConfig; always `None` for OmicronZoneConfig
    pub(crate) external_ip_id: Option<ExternalIpUuid>,
}

impl OmicronZone {
    pub(crate) fn new(
        sled_id: SledUuid,
        zone_id: Uuid,
        zone_underlay_address: Ipv6Addr,
        filesystem_pool: ZpoolUuid,
        zone_type: &nexus_types::inventory::OmicronZoneType,
        external_ip_id: Option<ExternalIpUuid>,
    ) -> anyhow::Result<Self> {
        let id = zone_id;
        let underlay_address = ipv6::Ipv6Addr::from(zone_underlay_address);
        let mut nic_id = None;
        let mut dns_gz_address = None;
        let mut dns_gz_address_index = None;
        let mut ntp_ntp_servers = None;
        let mut ntp_dns_servers = None;
        let mut ntp_ntp_domain = None;
        let mut nexus_external_tls = None;
        let mut nexus_external_dns_servers = None;
        let mut snat_ip = None;
        let mut snat_first_port = None;
        let mut snat_last_port = None;
        let mut second_service_ip = None;
        let mut second_service_port = None;

        let (zone_type, primary_service_sockaddr_str, dataset) = match zone_type
        {
            OmicronZoneType::BoundaryNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
                nic,
                snat_cfg,
            } => {
                let (first_port, last_port) = snat_cfg.port_range_raw();
                ntp_ntp_servers = Some(ntp_servers.clone());
                ntp_dns_servers = Some(dns_servers.clone());
                ntp_ntp_domain.clone_from(domain);
                snat_ip = Some(IpNetwork::from(snat_cfg.ip));
                snat_first_port = Some(SqlU16::from(first_port));
                snat_last_port = Some(SqlU16::from(last_port));
                nic_id = Some(nic.id);
                (ZoneType::BoundaryNtp, address, None)
            }
            OmicronZoneType::Clickhouse { address, dataset } => {
                (ZoneType::Clickhouse, address, Some(dataset))
            }
            OmicronZoneType::ClickhouseKeeper { address, dataset } => {
                (ZoneType::ClickhouseKeeper, address, Some(dataset))
            }
            OmicronZoneType::CockroachDb { address, dataset } => {
                (ZoneType::CockroachDb, address, Some(dataset))
            }
            OmicronZoneType::Crucible { address, dataset } => {
                (ZoneType::Crucible, address, Some(dataset))
            }
            OmicronZoneType::CruciblePantry { address } => {
                (ZoneType::CruciblePantry, address, None)
            }
            OmicronZoneType::ExternalDns {
                dataset,
                http_address,
                dns_address,
                nic,
            } => {
                nic_id = Some(nic.id);
                let sockaddr = dns_address
                    .parse::<std::net::SocketAddr>()
                    .with_context(|| {
                        format!(
                            "parsing address for external DNS server {:?}",
                            dns_address
                        )
                    })?;
                second_service_ip = Some(sockaddr.ip());
                second_service_port = Some(SqlU16::from(sockaddr.port()));
                (ZoneType::ExternalDns, http_address, Some(dataset))
            }
            OmicronZoneType::InternalDns {
                dataset,
                http_address,
                dns_address,
                gz_address,
                gz_address_index,
            } => {
                dns_gz_address = Some(ipv6::Ipv6Addr::from(gz_address));
                dns_gz_address_index = Some(SqlU32::from(*gz_address_index));
                let sockaddr = dns_address
                    .parse::<std::net::SocketAddr>()
                    .with_context(|| {
                        format!(
                            "parsing address for internal DNS server {:?}",
                            dns_address
                        )
                    })?;
                second_service_ip = Some(sockaddr.ip());
                second_service_port = Some(SqlU16::from(sockaddr.port()));
                (ZoneType::InternalDns, http_address, Some(dataset))
            }
            OmicronZoneType::InternalNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
            } => {
                ntp_ntp_servers = Some(ntp_servers.clone());
                ntp_dns_servers = Some(dns_servers.clone());
                ntp_ntp_domain.clone_from(domain);
                (ZoneType::InternalNtp, address, None)
            }
            OmicronZoneType::Nexus {
                internal_address,
                external_ip,
                nic,
                external_tls,
                external_dns_servers,
            } => {
                nic_id = Some(nic.id);
                nexus_external_tls = Some(*external_tls);
                nexus_external_dns_servers = Some(external_dns_servers.clone());
                second_service_ip = Some(*external_ip);
                (ZoneType::Nexus, internal_address, None)
            }
            OmicronZoneType::Oximeter { address } => {
                (ZoneType::Oximeter, address, None)
            }
        };

        let dataset_zpool_name = dataset.map(|d| d.pool_name.to_string());
        let primary_service_sockaddr = primary_service_sockaddr_str
            .parse::<std::net::SocketAddrV6>()
            .with_context(|| {
                format!(
                    "parsing socket address for primary IP {:?}",
                    primary_service_sockaddr_str
                )
            })?;
        let (primary_service_ip, primary_service_port) = (
            ipv6::Ipv6Addr::from(*primary_service_sockaddr.ip()),
            SqlU16::from(primary_service_sockaddr.port()),
        );

        Ok(Self {
            sled_id,
            id,
            underlay_address,
            filesystem_pool,
            zone_type,
            primary_service_ip,
            primary_service_port,
            second_service_ip: second_service_ip.map(IpNetwork::from),
            second_service_port,
            dataset_zpool_name,
            nic_id,
            dns_gz_address,
            dns_gz_address_index,
            ntp_ntp_servers,
            ntp_dns_servers: ntp_dns_servers
                .map(|list| list.into_iter().map(IpNetwork::from).collect()),
            ntp_domain: ntp_ntp_domain,
            nexus_external_tls,
            nexus_external_dns_servers: nexus_external_dns_servers
                .map(|list| list.into_iter().map(IpNetwork::from).collect()),
            snat_ip,
            snat_first_port,
            snat_last_port,
            external_ip_id,
        })
    }

    pub(crate) fn into_blueprint_zone_config(
        self,
        disposition: BlueprintZoneDisposition,
        nic_row: Option<OmicronZoneNic>,
    ) -> anyhow::Result<nexus_types::deployment::BlueprintZoneConfig> {
        let common = self.into_zone_config_common(nic_row)?;
        let address = common.primary_service_address;
        let zone_type = match common.zone_type {
            ZoneType::BoundaryNtp => {
                let snat_cfg = match (
                    common.snat_ip,
                    common.snat_first_port,
                    common.snat_last_port,
                ) {
                    (Some(ip), Some(first_port), Some(last_port)) => {
                        nexus_types::inventory::SourceNatConfig::new(
                            ip.ip(),
                            *first_port,
                            *last_port,
                        )
                        .context("bad SNAT config for boundary NTP")?
                    }
                    _ => bail!(
                        "expected non-NULL snat properties, \
                         found at least one NULL"
                    ),
                };
                BlueprintZoneType::BoundaryNtp(
                    blueprint_zone_type::BoundaryNtp {
                        address,
                        dns_servers: common.ntp_dns_servers?,
                        domain: common.ntp_domain,
                        nic: common.nic?,
                        ntp_servers: common.ntp_ntp_servers?,
                        external_ip: OmicronZoneExternalSnatIp {
                            id: common.external_ip_id?,
                            snat_cfg,
                        },
                    },
                )
            }
            ZoneType::Clickhouse => {
                BlueprintZoneType::Clickhouse(blueprint_zone_type::Clickhouse {
                    address,
                    dataset: common.dataset?,
                })
            }
            ZoneType::ClickhouseKeeper => BlueprintZoneType::ClickhouseKeeper(
                blueprint_zone_type::ClickhouseKeeper {
                    address,
                    dataset: common.dataset?,
                },
            ),
            ZoneType::CockroachDb => BlueprintZoneType::CockroachDb(
                blueprint_zone_type::CockroachDb {
                    address,
                    dataset: common.dataset?,
                },
            ),
            ZoneType::Crucible => {
                BlueprintZoneType::Crucible(blueprint_zone_type::Crucible {
                    address,
                    dataset: common.dataset?,
                })
            }
            ZoneType::CruciblePantry => BlueprintZoneType::CruciblePantry(
                blueprint_zone_type::CruciblePantry { address },
            ),
            ZoneType::ExternalDns => BlueprintZoneType::ExternalDns(
                blueprint_zone_type::ExternalDns {
                    dataset: common.dataset?,
                    dns_address: OmicronZoneExternalFloatingAddr {
                        id: common.external_ip_id?,
                        addr: common.dns_address?,
                    },
                    http_address: address,
                    nic: common.nic?,
                },
            ),
            ZoneType::InternalDns => BlueprintZoneType::InternalDns(
                blueprint_zone_type::InternalDns {
                    dataset: common.dataset?,
                    dns_address: match common.dns_address? {
                        SocketAddr::V4(addr) => {
                            bail!("expected V6 address; got {addr}")
                        }
                        SocketAddr::V6(addr) => addr,
                    },
                    http_address: address,
                    gz_address: *common.dns_gz_address.ok_or_else(|| {
                        anyhow!("expected dns_gz_address, found none")
                    })?,
                    gz_address_index: *common.dns_gz_address_index.ok_or_else(
                        || anyhow!("expected dns_gz_address_index, found none"),
                    )?,
                },
            ),
            ZoneType::InternalNtp => BlueprintZoneType::InternalNtp(
                blueprint_zone_type::InternalNtp {
                    address,
                    dns_servers: common.ntp_dns_servers?,
                    domain: common.ntp_domain,
                    ntp_servers: common.ntp_ntp_servers?,
                },
            ),
            ZoneType::Nexus => {
                BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                    internal_address: address,
                    nic: common.nic?,
                    external_tls: common
                        .nexus_external_tls
                        .ok_or_else(|| anyhow!("expected 'external_tls'"))?,
                    external_ip: OmicronZoneExternalFloatingIp {
                        id: common.external_ip_id?,
                        ip: common
                            .second_service_ip
                            .ok_or_else(|| {
                                anyhow!("expected second service IP")
                            })?
                            .ip(),
                    },
                    external_dns_servers: common
                        .nexus_external_dns_servers
                        .ok_or_else(|| {
                            anyhow!("expected 'external_dns_servers'")
                        })?
                        .into_iter()
                        .map(|i| i.ip())
                        .collect(),
                })
            }
            ZoneType::Oximeter => {
                BlueprintZoneType::Oximeter(blueprint_zone_type::Oximeter {
                    address,
                })
            }
        };
        Ok(nexus_types::deployment::BlueprintZoneConfig {
            disposition,
            id: OmicronZoneUuid::from_untyped_uuid(common.id),
            underlay_address: std::net::Ipv6Addr::from(common.underlay_address),
            filesystem_pool: ZpoolName::new_external(common.filesystem_pool),
            zone_type,
        })
    }

    pub(crate) fn into_omicron_zone_config(
        self,
        nic_row: Option<OmicronZoneNic>,
    ) -> anyhow::Result<nexus_types::inventory::OmicronZoneConfig> {
        let common = self.into_zone_config_common(nic_row)?;
        let address = common.primary_service_address.to_string();

        let zone_type = match common.zone_type {
            ZoneType::BoundaryNtp => {
                let snat_cfg = match (
                    common.snat_ip,
                    common.snat_first_port,
                    common.snat_last_port,
                ) {
                    (Some(ip), Some(first_port), Some(last_port)) => {
                        nexus_types::inventory::SourceNatConfig::new(
                            ip.ip(),
                            *first_port,
                            *last_port,
                        )
                        .context("bad SNAT config for boundary NTP")?
                    }
                    _ => bail!(
                        "expected non-NULL snat properties, \
                        found at least one NULL"
                    ),
                };
                OmicronZoneType::BoundaryNtp {
                    address,
                    dns_servers: common.ntp_dns_servers?,
                    domain: common.ntp_domain,
                    nic: common.nic?,
                    ntp_servers: common.ntp_ntp_servers?,
                    snat_cfg,
                }
            }
            ZoneType::Clickhouse => OmicronZoneType::Clickhouse {
                address,
                dataset: common.dataset?,
            },
            ZoneType::ClickhouseKeeper => OmicronZoneType::ClickhouseKeeper {
                address,
                dataset: common.dataset?,
            },
            ZoneType::CockroachDb => OmicronZoneType::CockroachDb {
                address,
                dataset: common.dataset?,
            },
            ZoneType::Crucible => {
                OmicronZoneType::Crucible { address, dataset: common.dataset? }
            }
            ZoneType::CruciblePantry => {
                OmicronZoneType::CruciblePantry { address }
            }
            ZoneType::ExternalDns => OmicronZoneType::ExternalDns {
                dataset: common.dataset?,
                dns_address: common.dns_address?.to_string(),
                http_address: address,
                nic: common.nic?,
            },
            ZoneType::InternalDns => OmicronZoneType::InternalDns {
                dataset: common.dataset?,
                dns_address: common.dns_address?.to_string(),
                http_address: address,
                gz_address: *common.dns_gz_address.ok_or_else(|| {
                    anyhow!("expected dns_gz_address, found none")
                })?,
                gz_address_index: *common.dns_gz_address_index.ok_or_else(
                    || anyhow!("expected dns_gz_address_index, found none"),
                )?,
            },
            ZoneType::InternalNtp => OmicronZoneType::InternalNtp {
                address,
                dns_servers: common.ntp_dns_servers?,
                domain: common.ntp_domain,
                ntp_servers: common.ntp_ntp_servers?,
            },
            ZoneType::Nexus => OmicronZoneType::Nexus {
                internal_address: address,
                nic: common.nic?,
                external_tls: common
                    .nexus_external_tls
                    .ok_or_else(|| anyhow!("expected 'external_tls'"))?,
                external_ip: common
                    .second_service_ip
                    .ok_or_else(|| anyhow!("expected second service IP"))?
                    .ip(),
                external_dns_servers: common
                    .nexus_external_dns_servers
                    .ok_or_else(|| anyhow!("expected 'external_dns_servers'"))?
                    .into_iter()
                    .map(|i| i.ip())
                    .collect(),
            },
            ZoneType::Oximeter => OmicronZoneType::Oximeter { address },
        };
        Ok(nexus_types::inventory::OmicronZoneConfig {
            id: common.id,
            underlay_address: std::net::Ipv6Addr::from(common.underlay_address),
            filesystem_pool: ZpoolName::new_external(common.filesystem_pool),
            zone_type,
        })
    }

    fn into_zone_config_common(
        self,
        nic_row: Option<OmicronZoneNic>,
    ) -> anyhow::Result<ZoneConfigCommon> {
        let primary_service_address = SocketAddrV6::new(
            std::net::Ipv6Addr::from(self.primary_service_ip),
            *self.primary_service_port,
            0,
            0,
        );

        // Assemble a value that we can use to extract the NIC _if necessary_
        // and report an error if it was needed but not found.
        //
        // Any error here should be impossible.  By the time we get here, the
        // caller should have provided `nic_row` iff there's a corresponding
        // `nic_id` in this row, and the ids should match up.  And whoever
        // created this row ought to have provided a nic_id iff this type of
        // zone needs a NIC.  This last issue is not under our control, though,
        // so we definitely want to handle that as an operational error.  The
        // others could arguably be programmer errors (i.e., we could `assert`),
        // but it seems excessive to crash here.
        //
        // Note that we immediately return for any of the caller errors here.
        // For the other error, we will return only later, if some code path
        // below tries to use `nic` when it's not present.
        let nic = match (self.nic_id, nic_row) {
            (Some(expected_id), Some(nic_row)) => {
                ensure!(expected_id == nic_row.id, "caller provided wrong NIC");
                Ok(nic_row.into_network_interface_for_zone(self.id)?)
            }
            // We don't expect and don't have a NIC. This is reasonable, so we
            // don't `bail!` like we do in the next two cases, but we also
            // _don't have a NIC_. Put an error into `nic`, and then if we land
            // in a zone below that expects one, we'll fail then.
            (None, None) => Err(anyhow!(
                "expected zone to have an associated NIC, but it doesn't"
            )),
            (Some(_), None) => bail!("caller provided no NIC"),
            (None, Some(_)) => bail!("caller unexpectedly provided a NIC"),
        };

        // Similarly, assemble a value that we can use to extract the dataset,
        // if necessary.  We only return this error if code below tries to use
        // this value.
        let dataset = self
            .dataset_zpool_name
            .map(|zpool_name| -> Result<_, anyhow::Error> {
                Ok(nexus_types::inventory::OmicronZoneDataset {
                    pool_name: zpool_name.parse().map_err(|e| {
                        anyhow!("parsing zpool name {:?}: {}", zpool_name, e)
                    })?,
                })
            })
            .transpose()?
            .ok_or_else(|| anyhow!("expected dataset zpool name, found none"));

        // Do the same for the DNS server address.
        let dns_address =
            match (self.second_service_ip, self.second_service_port) {
                (Some(dns_ip), Some(dns_port)) => {
                    Ok(std::net::SocketAddr::new(dns_ip.ip(), *dns_port))
                }
                _ => Err(anyhow!(
                    "expected second service IP and port, \
                            found one missing"
                )),
            };

        // Do the same for NTP zone properties.
        let ntp_dns_servers = self
            .ntp_dns_servers
            .ok_or_else(|| anyhow!("expected list of DNS servers, found null"))
            .map(|list| {
                list.into_iter().map(|ipnetwork| ipnetwork.ip()).collect()
            });
        let ntp_ntp_servers =
            self.ntp_ntp_servers.ok_or_else(|| anyhow!("expected ntp_servers"));

        // Do the same for the external IP ID.
        let external_ip_id =
            self.external_ip_id.context("expected an external IP ID");

        Ok(ZoneConfigCommon {
            id: self.id,
            underlay_address: self.underlay_address,
            filesystem_pool: self.filesystem_pool,
            zone_type: self.zone_type,
            primary_service_address,
            snat_ip: self.snat_ip,
            snat_first_port: self.snat_first_port,
            snat_last_port: self.snat_last_port,
            ntp_domain: self.ntp_domain,
            dns_gz_address: self.dns_gz_address,
            dns_gz_address_index: self.dns_gz_address_index,
            nexus_external_tls: self.nexus_external_tls,
            nexus_external_dns_servers: self.nexus_external_dns_servers,
            second_service_ip: self.second_service_ip,
            nic,
            dataset,
            dns_address,
            ntp_dns_servers,
            ntp_ntp_servers,
            external_ip_id,
        })
    }
}

struct ZoneConfigCommon {
    id: Uuid,
    underlay_address: ipv6::Ipv6Addr,
    filesystem_pool: ZpoolUuid,
    zone_type: ZoneType,
    primary_service_address: SocketAddrV6,
    snat_ip: Option<IpNetwork>,
    snat_first_port: Option<SqlU16>,
    snat_last_port: Option<SqlU16>,
    ntp_domain: Option<String>,
    dns_gz_address: Option<ipv6::Ipv6Addr>,
    dns_gz_address_index: Option<SqlU32>,
    nexus_external_tls: Option<bool>,
    nexus_external_dns_servers: Option<Vec<IpNetwork>>,
    second_service_ip: Option<IpNetwork>,
    // These properties may or may not be needed, depending on the zone type. We
    // store results here that can be unpacked once we determine our zone type.
    nic: anyhow::Result<NetworkInterface>,
    dataset: anyhow::Result<nexus_types::inventory::OmicronZoneDataset>,
    dns_address: anyhow::Result<SocketAddr>,
    ntp_dns_servers: anyhow::Result<Vec<IpAddr>>,
    ntp_ntp_servers: anyhow::Result<Vec<String>>,
    external_ip_id: anyhow::Result<ExternalIpUuid>,
}

#[derive(Debug)]
pub(crate) struct OmicronZoneNic {
    pub(crate) id: Uuid,
    pub(crate) name: Name,
    pub(crate) ip: IpNetwork,
    pub(crate) mac: MacAddr,
    pub(crate) subnet: IpNetwork,
    pub(crate) vni: SqlU32,
    pub(crate) is_primary: bool,
    pub(crate) slot: SqlU8,
}

impl OmicronZoneNic {
    pub(crate) fn new(
        zone_id: Uuid,
        nic: &nexus_types::inventory::NetworkInterface,
    ) -> anyhow::Result<Self> {
        // We do not bother storing the NIC's kind and associated id
        // because it should be inferrable from the other information
        // that we have.  Verify that here.
        ensure!(
            matches!(
                nic.kind,
                NetworkInterfaceKind::Service{ id } if id == zone_id
            ),
            "expected zone's NIC kind to be \"service\" and the \
                    id to match the zone's id ({zone_id})",
        );

        Ok(Self {
            id: nic.id,
            name: Name::from(nic.name.clone()),
            ip: IpNetwork::from(nic.ip),
            mac: MacAddr::from(nic.mac),
            subnet: IpNetwork::from(nic.subnet),
            vni: SqlU32::from(u32::from(nic.vni)),
            is_primary: nic.primary,
            slot: SqlU8::from(nic.slot),
        })
    }

    pub(crate) fn into_network_interface_for_zone(
        self,
        zone_id: Uuid,
    ) -> anyhow::Result<NetworkInterface> {
        Ok(NetworkInterface {
            id: self.id,
            ip: self.ip.ip(),
            kind: NetworkInterfaceKind::Service { id: zone_id },
            mac: *self.mac,
            name: self.name.into(),
            primary: self.is_primary,
            slot: *self.slot,
            vni: omicron_common::api::external::Vni::try_from(*self.vni)
                .context("parsing VNI")?,
            subnet: self.subnet.into(),
        })
    }
}
