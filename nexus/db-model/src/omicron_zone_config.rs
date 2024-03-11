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

use std::net::SocketAddrV6;

use crate::inventory::ZoneType;
use crate::{ipv6, MacAddr, Name, SqlU16, SqlU32, SqlU8};
use anyhow::{anyhow, bail, ensure, Context};
use ipnetwork::IpNetwork;
use nexus_types::inventory::OmicronZoneType;
use omicron_common::api::internal::shared::{
    NetworkInterface, NetworkInterfaceKind,
};
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct OmicronZone {
    pub(crate) sled_id: Uuid,
    pub(crate) id: Uuid,
    pub(crate) underlay_address: ipv6::Ipv6Addr,
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
}

impl OmicronZone {
    pub(crate) fn new(
        sled_id: Uuid,
        zone: &nexus_types::inventory::OmicronZoneConfig,
    ) -> anyhow::Result<Self> {
        let id = zone.id;
        let underlay_address = ipv6::Ipv6Addr::from(zone.underlay_address);
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

        let (zone_type, primary_service_sockaddr_str, dataset) = match &zone
            .zone_type
        {
            OmicronZoneType::BoundaryNtp {
                address,
                ntp_servers,
                dns_servers,
                domain,
                nic,
                snat_cfg,
            } => {
                ntp_ntp_servers = Some(ntp_servers.clone());
                ntp_dns_servers = Some(dns_servers.clone());
                ntp_ntp_domain = domain.clone();
                snat_ip = Some(IpNetwork::from(snat_cfg.ip));
                snat_first_port = Some(SqlU16::from(snat_cfg.first_port));
                snat_last_port = Some(SqlU16::from(snat_cfg.last_port));
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
                ntp_ntp_domain = domain.clone();
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

        let dataset_zpool_name =
            dataset.map(|d| d.pool_name.as_str().to_string());
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
        })
    }

    pub(crate) fn into_omicron_zone_config(
        self,
        nic_row: Option<OmicronZoneNic>,
    ) -> anyhow::Result<nexus_types::inventory::OmicronZoneConfig> {
        let address = SocketAddrV6::new(
            std::net::Ipv6Addr::from(self.primary_service_ip),
            *self.primary_service_port,
            0,
            0,
        )
        .to_string();

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
                    Ok(std::net::SocketAddr::new(dns_ip.ip(), *dns_port)
                        .to_string())
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

        let zone_type = match self.zone_type {
            ZoneType::BoundaryNtp => {
                let snat_cfg = match (
                    self.snat_ip,
                    self.snat_first_port,
                    self.snat_last_port,
                ) {
                    (Some(ip), Some(first_port), Some(last_port)) => {
                        nexus_types::inventory::SourceNatConfig {
                            ip: ip.ip(),
                            first_port: *first_port,
                            last_port: *last_port,
                        }
                    }
                    _ => bail!(
                        "expected non-NULL snat properties, \
                        found at least one NULL"
                    ),
                };
                OmicronZoneType::BoundaryNtp {
                    address,
                    dns_servers: ntp_dns_servers?,
                    domain: self.ntp_domain,
                    nic: nic?,
                    ntp_servers: ntp_ntp_servers?,
                    snat_cfg,
                }
            }
            ZoneType::Clickhouse => {
                OmicronZoneType::Clickhouse { address, dataset: dataset? }
            }
            ZoneType::ClickhouseKeeper => {
                OmicronZoneType::ClickhouseKeeper { address, dataset: dataset? }
            }
            ZoneType::CockroachDb => {
                OmicronZoneType::CockroachDb { address, dataset: dataset? }
            }
            ZoneType::Crucible => {
                OmicronZoneType::Crucible { address, dataset: dataset? }
            }
            ZoneType::CruciblePantry => {
                OmicronZoneType::CruciblePantry { address }
            }
            ZoneType::ExternalDns => OmicronZoneType::ExternalDns {
                dataset: dataset?,
                dns_address: dns_address?,
                http_address: address,
                nic: nic?,
            },
            ZoneType::InternalDns => OmicronZoneType::InternalDns {
                dataset: dataset?,
                dns_address: dns_address?,
                http_address: address,
                gz_address: *self.dns_gz_address.ok_or_else(|| {
                    anyhow!("expected dns_gz_address, found none")
                })?,
                gz_address_index: *self.dns_gz_address_index.ok_or_else(
                    || anyhow!("expected dns_gz_address_index, found none"),
                )?,
            },
            ZoneType::InternalNtp => OmicronZoneType::InternalNtp {
                address,
                dns_servers: ntp_dns_servers?,
                domain: self.ntp_domain,
                ntp_servers: ntp_ntp_servers?,
            },
            ZoneType::Nexus => OmicronZoneType::Nexus {
                internal_address: address,
                nic: nic?,
                external_tls: self
                    .nexus_external_tls
                    .ok_or_else(|| anyhow!("expected 'external_tls'"))?,
                external_ip: self
                    .second_service_ip
                    .ok_or_else(|| anyhow!("expected second service IP"))?
                    .ip(),
                external_dns_servers: self
                    .nexus_external_dns_servers
                    .ok_or_else(|| anyhow!("expected 'external_dns_servers'"))?
                    .into_iter()
                    .map(|i| i.ip())
                    .collect(),
            },
            ZoneType::Oximeter => OmicronZoneType::Oximeter { address },
        };
        Ok(nexus_types::inventory::OmicronZoneConfig {
            id: self.id,
            underlay_address: std::net::Ipv6Addr::from(self.underlay_address),
            zone_type,
        })
    }
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
        zone: &nexus_types::inventory::OmicronZoneConfig,
    ) -> anyhow::Result<Option<Self>> {
        match &zone.zone_type {
            OmicronZoneType::ExternalDns { nic, .. }
            | OmicronZoneType::BoundaryNtp { nic, .. }
            | OmicronZoneType::Nexus { nic, .. } => {
                // We do not bother storing the NIC's kind and associated id
                // because it should be inferrable from the other information
                // that we have.  Verify that here.
                ensure!(
                    matches!(
                        nic.kind,
                        NetworkInterfaceKind::Service{ id } if id == zone.id
                    ),
                    "expected zone's NIC kind to be \"service\" and the \
                    id to match the zone's id ({})",
                    zone.id
                );

                Ok(Some(Self {
                    id: nic.id,
                    name: Name::from(nic.name.clone()),
                    ip: IpNetwork::from(nic.ip),
                    mac: MacAddr::from(nic.mac),
                    subnet: IpNetwork::from(nic.subnet),
                    vni: SqlU32::from(u32::from(nic.vni)),
                    is_primary: nic.primary,
                    slot: SqlU8::from(nic.slot),
                }))
            }
            _ => Ok(None),
        }
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
