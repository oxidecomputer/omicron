// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper types and methods for sharing nontrivial conversions between various
//! `OmicronZoneConfig` database serializations and the corresponding Nexus/
//! sled-agent type
//!
//! Both inventory and deployment have nearly-identical tables to serialize
//! `OmicronZoneConfigs` that are collected or generated, respectively.
//! We expect those tables to diverge over time (e.g., inventory may start
//! collecting extra metadata like uptime). This module provides conversion
//! helpers for the parts of those tables that are common between the two.

use crate::{MacAddr, Name, SqlU8, SqlU16, SqlU32};
use anyhow::{Context, anyhow, bail, ensure};
use ipnetwork::IpNetwork;
use nexus_sled_agent_shared::inventory::OmicronZoneDataset;
use nexus_types::inventory::NetworkInterface;
use omicron_common::api::internal::shared::{
    NetworkInterfaceKind, PrivateIpConfig,
};
use omicron_uuid_kinds::{GenericUuid, OmicronZoneUuid};
use oxnet::{Ipv4Net, Ipv6Net};
use std::net::{IpAddr, SocketAddr, SocketAddrV6};
use uuid::Uuid;

/// Convert ntp server config from the DB representation to the
/// omicron internal representation
pub fn ntp_servers_to_omicron_internal(
    ntp_ntp_servers: Option<Vec<String>>,
) -> anyhow::Result<Vec<String>> {
    ntp_ntp_servers.ok_or_else(|| anyhow!("expected ntp servers"))
}

/// Convert ntp dns server config from the DB representation to
/// the omicron internal representation.
pub fn ntp_dns_servers_to_omicron_internal(
    ntp_dns_servers: Option<Vec<IpNetwork>>,
) -> anyhow::Result<Vec<IpAddr>> {
    ntp_dns_servers
        .ok_or_else(|| anyhow!("expected list of DNS servers, found null"))
        .map(|list| list.into_iter().map(|ipnetwork| ipnetwork.ip()).collect())
}

/// Assemble a value that we can use to extract the NIC _if necessary_
/// and report an error if it was needed but not found.
///
/// Any error here should be impossible.  By the time we get here, the
/// caller should have provided `nic_row` iff there's a corresponding
/// `nic_id` in this row, and the ids should match up.  And whoever
/// created this row ought to have provided a nic_id iff this type of
/// zone needs a NIC.  This last issue is not under our control, though,
/// so we definitely want to handle that as an operational error.  The
/// others could arguably be programmer errors (i.e., we could `assert`),
/// but it seems excessive to crash here.
///
/// The outer result represents a programmer error and should be unwrapped
/// immediately. The inner result represents an operational error and should
/// only be unwrapped when the nic is used.
pub fn nic_row_to_network_interface(
    zone_id: OmicronZoneUuid,
    nic_id: Option<Uuid>,
    nic_row: Option<OmicronZoneNic>,
) -> anyhow::Result<anyhow::Result<NetworkInterface>> {
    match (nic_id, nic_row) {
        (Some(expected_id), Some(nic_row)) => {
            ensure!(expected_id == nic_row.id, "caller provided wrong NIC");
            Ok(nic_row.into_network_interface_for_zone(zone_id))
        }
        (None, None) => Ok(Err(anyhow!(
            "expected zone to have an associated NIC, but it doesn't"
        ))),
        (Some(_), None) => bail!("caller provided no NIC"),
        (None, Some(_)) => bail!("caller unexpectedly provided a NIC"),
    }
}

/// Convert a dataset from a DB representation to a an Omicron internal
/// representation
pub fn dataset_zpool_name_to_omicron_zone_dataset(
    dataset_zpool_name: Option<String>,
) -> anyhow::Result<OmicronZoneDataset> {
    dataset_zpool_name
        .map(|zpool_name| -> Result<_, anyhow::Error> {
            Ok(OmicronZoneDataset {
                pool_name: zpool_name.parse().map_err(|e| {
                    anyhow!("parsing zpool name {:?}: {}", zpool_name, e)
                })?,
            })
        })
        .transpose()?
        .ok_or_else(|| anyhow!("expected dataset zpool name, found none"))
}

/// Convert the secondary ip and port to a dns address
pub fn secondary_ip_and_port_to_dns_address(
    second_service_ip: Option<IpNetwork>,
    second_service_port: Option<SqlU16>,
) -> anyhow::Result<SocketAddr> {
    match (second_service_ip, second_service_port) {
        (Some(dns_ip), Some(dns_port)) => {
            Ok(std::net::SocketAddr::new(dns_ip.ip(), *dns_port))
        }
        _ => Err(anyhow!(
            "expected second service IP and port, found one missing"
        )),
    }
}

/// Extract a SocketAddrV6 from a SocketAddr for a given dns address
pub fn to_internal_dns_address(
    address: SocketAddr,
) -> anyhow::Result<SocketAddrV6> {
    match address {
        SocketAddr::V4(address) => {
            bail!(
                "expected internal DNS address to be v6, found v4: {:?}",
                address
            )
        }
        SocketAddr::V6(v6) => Ok(v6),
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
        zone_id: OmicronZoneUuid,
        nic: &nexus_types::inventory::NetworkInterface,
    ) -> anyhow::Result<Self> {
        // We do not bother storing the NIC's kind and associated id
        // because it should be inferrable from the other information
        // that we have.  Verify that here.
        ensure!(
            matches!(
                nic.kind,
                NetworkInterfaceKind::Service { id } if id == zone_id.into_untyped_uuid()
            ),
            "expected zone's NIC kind to be \"service\" and the \
                    id to match the zone's id ({zone_id})",
        );

        // TODO-completeness: Support dual-stack NICs for Omicron zones.
        // See https://github.com/oxidecomputer/omicron/issues/9314.
        let (ip, subnet) = match &nic.ip_config {
            PrivateIpConfig::V4(ipv4) => (
                IpNetwork::V4((*ipv4.ip()).into()),
                IpNetwork::V4((*ipv4.subnet()).into()),
            ),
            PrivateIpConfig::V6(ipv6) => (
                IpNetwork::V6((*ipv6.ip()).into()),
                IpNetwork::V6((*ipv6.subnet()).into()),
            ),
            PrivateIpConfig::DualStack { .. } => {
                bail!(
                    "Found a dual-stack NIC, which isn't yet supported. \
                    nic_id=\"{}\" zone_id=\"{}\"",
                    nic.id,
                    zone_id,
                );
            }
        };

        Ok(Self {
            id: nic.id,
            name: Name::from(nic.name.clone()),
            ip,
            mac: MacAddr::from(nic.mac),
            subnet,
            vni: SqlU32::from(u32::from(nic.vni)),
            is_primary: nic.primary,
            slot: SqlU8::from(nic.slot),
        })
    }

    pub(crate) fn into_network_interface_for_zone(
        self,
        zone_id: OmicronZoneUuid,
    ) -> anyhow::Result<NetworkInterface> {
        let ip = match (self.ip.ip(), self.subnet) {
            (IpAddr::V4(addr), IpNetwork::V4(net)) => {
                PrivateIpConfig::new_ipv4(addr, Ipv4Net::from(net))?
            }
            (IpAddr::V6(addr), IpNetwork::V6(net)) => {
                PrivateIpConfig::new_ipv6(addr, Ipv6Net::from(net))?
            }
            (IpAddr::V4(_), IpNetwork::V6(_))
            | (IpAddr::V6(_), IpNetwork::V4(_)) => bail!(
                "OmicronZoneNic has a mix of IPv4 and IPv6 \
                addresses and subnets! nic_id=\"{}\" ip=\"{}\" \
                subnet=\"{}\"",
                self.id,
                self.ip.ip(),
                self.subnet,
            ),
        };
        Ok(NetworkInterface {
            id: self.id,
            ip_config: ip,
            kind: NetworkInterfaceKind::Service {
                id: zone_id.into_untyped_uuid(),
            },
            mac: *self.mac,
            name: self.name.into(),
            primary: self.is_primary,
            slot: *self.slot,
            vni: omicron_common::api::external::Vni::try_from(*self.vni)
                .context("parsing VNI")?,
        })
    }
}
