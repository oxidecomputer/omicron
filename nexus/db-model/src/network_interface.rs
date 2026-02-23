// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{MacAddr, VpcSubnet};
use crate::Ipv4Addr;
use crate::Ipv6Addr;
use crate::Name;
use crate::SqlU8;
use crate::impl_enum_type;
use chrono::DateTime;
use chrono::Utc;
use db_macros::Resource;
use diesel::AsChangeset;
use itertools::Either;
use itertools::Itertools;
use nexus_db_schema::schema::instance_network_interface;
use nexus_db_schema::schema::network_interface;
use nexus_db_schema::schema::service_network_interface;
use nexus_types::external_api::instance::InstanceNetworkInterfaceUpdate;
use nexus_types::external_api::instance::PrivateIpStackCreate;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use omicron_common::api::external::PrivateIpStack;
use omicron_common::api::external::PrivateIpv4Stack;
use omicron_common::api::external::PrivateIpv6Stack;
use omicron_common::api::internal::shared::PrivateIpConfig;
use omicron_common::api::internal::shared::PrivateIpv4Config;
use omicron_common::api::internal::shared::PrivateIpv6Config;
use omicron_common::api::{external, internal};
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::VnicUuid;
use oxnet::IpNet;
use sled_agent_types::inventory::ZoneKind;
use std::net::IpAddr;
use uuid::Uuid;

/// The max number of interfaces that may be associated with a resource,
/// e.g., instance or service.
///
/// RFD 135 caps instances at 8 interfaces and we use the same limit for
/// all types of interfaces for simplicity.
pub const MAX_NICS_PER_INSTANCE: usize = 8;

impl_enum_type! {
    NetworkInterfaceKindEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, PartialEq)]
    pub enum NetworkInterfaceKind;

    Instance => b"instance"
    Service => b"service"
    Probe => b"probe"
}

/// Generic Network Interface DB model.
#[derive(Selectable, Queryable, Clone, Debug, Resource)]
#[diesel(table_name = network_interface)]
pub struct NetworkInterface {
    #[diesel(embed)]
    pub identity: NetworkInterfaceIdentity,
    /// Which kind of parent this interface belongs to.
    pub kind: NetworkInterfaceKind,
    /// UUID of the parent.
    pub parent_id: Uuid,
    /// UUID of the VPC containing this interface.
    pub vpc_id: Uuid,
    /// UUID of the VPC Subnet containing this interface.
    pub subnet_id: Uuid,
    /// MAC address for this interface.
    pub mac: MacAddr,
    /// The VPC-private IPv4 address of the interface.
    ///
    /// At least one of the `ip` and `ipv6` fields will always be `Some(_)`, a
    /// constraint enforced by the database. Both may be `Some(_)` for
    /// dual-stack interfaces.
    // NOTE: At least one of the below will be non-None.
    //
    // We could use an enum to enforce this, but there's a lot of diesel
    // machinery needed and it makes sharing the type between this model and the
    // `InstanceNetworkInterface` below difficult. In particular, the db-lookup
    // stuff chokes because we can't make the same type selectable from two
    // different tables. In any case, we want to enforce this on the
    // `IncompleteNetworkInterface` type, and we already do enforce it via a
    // check constraint in the database itself.
    //
    // NOTE: The column in the database is still named `ip`, because renaming
    // columns isn't idempotent in CRDB as of today.
    #[diesel(column_name = ip)]
    pub ipv4: Option<Ipv4Addr>,
    /// The VPC-private IPv6 address of the interface.
    ///
    /// At least one of the `ip` and `ipv6` fields will always be `Some(_)`, a
    /// constraint enforced by the database. Both may be `Some(_)` for
    /// dual-stack interfaces.
    pub ipv6: Option<Ipv6Addr>,
    /// The PCI slot on the instance where the interface appears.
    pub slot: SqlU8,
    /// True if this is the instance's primary interface.
    #[diesel(column_name = is_primary)]
    pub primary: bool,
    /// List of additional IPv4 networks on which the instance is allowed to
    /// send / receive traffic.
    #[diesel(column_name = transit_ips)]
    pub transit_ips_v4: Vec<crate::Ipv4Net>,
    /// List of additional IPv6 networks on which the instance is allowed to
    /// send / receive traffic.
    pub transit_ips_v6: Vec<crate::Ipv6Net>,
}

impl NetworkInterface {
    pub fn into_internal(
        self,
        ipv4_subnet: oxnet::Ipv4Net,
        ipv6_subnet: oxnet::Ipv6Net,
    ) -> Result<internal::shared::NetworkInterface, Error> {
        let ip_config = match (self.ipv4, self.ipv6) {
            (None, None) => {
                return Err(Error::internal_error(&format!(
                    "NIC with ID '{}' is in the database with neither an IPv4 nor \
                IPv6 address, which out to be impossible and enforced by the \
                database constraints",
                    self.id(),
                )));
            }
            (None, Some(ip)) => {
                let transit_ips = self
                    .transit_ips_v6
                    .iter()
                    .copied()
                    .map(Into::into)
                    .collect();
                PrivateIpConfig::V6(PrivateIpv6Config::new_with_transit_ips(
                    *ip,
                    ipv6_subnet,
                    transit_ips,
                )?)
            }
            (Some(ip), None) => {
                let transit_ips = self
                    .transit_ips_v4
                    .iter()
                    .copied()
                    .map(Into::into)
                    .collect();
                PrivateIpConfig::V4(PrivateIpv4Config::new_with_transit_ips(
                    *ip,
                    ipv4_subnet,
                    transit_ips,
                )?)
            }
            (Some(ipv4), Some(ipv6)) => {
                let ipv4_transit_ips = self
                    .transit_ips_v4
                    .iter()
                    .copied()
                    .map(Into::into)
                    .collect();
                let ipv6_transit_ips = self
                    .transit_ips_v6
                    .iter()
                    .copied()
                    .map(Into::into)
                    .collect();
                let v4 = PrivateIpv4Config::new_with_transit_ips(
                    *ipv4,
                    ipv4_subnet,
                    ipv4_transit_ips,
                )?;
                let v6 = PrivateIpv6Config::new_with_transit_ips(
                    *ipv6,
                    ipv6_subnet,
                    ipv6_transit_ips,
                )?;
                PrivateIpConfig::DualStack { v4, v6 }
            }
        };
        Ok(internal::shared::NetworkInterface {
            id: self.id(),
            kind: match self.kind {
                NetworkInterfaceKind::Instance => {
                    internal::shared::NetworkInterfaceKind::Instance {
                        id: self.parent_id,
                    }
                }
                NetworkInterfaceKind::Service => {
                    internal::shared::NetworkInterfaceKind::Service {
                        id: self.parent_id,
                    }
                }
                NetworkInterfaceKind::Probe => {
                    internal::shared::NetworkInterfaceKind::Probe {
                        id: self.parent_id,
                    }
                }
            },
            name: self.name().clone(),
            ip_config,
            mac: self.mac.into(),
            vni: external::Vni::try_from(0).unwrap(),
            primary: self.primary,
            slot: *self.slot,
        })
    }
}

/// Instance Network Interface DB model.
///
/// The underlying "table" (`instance_network_interface`) is actually a view
/// over the `network_interface` table, that contains only rows with
/// `kind = 'instance'`.
#[derive(Selectable, Queryable, Clone, Debug, Resource)]
#[diesel(table_name = instance_network_interface)]
pub struct InstanceNetworkInterface {
    #[diesel(embed)]
    pub identity: InstanceNetworkInterfaceIdentity,
    pub instance_id: Uuid,
    pub vpc_id: Uuid,
    pub subnet_id: Uuid,
    pub mac: MacAddr,
    // NOTE: At least one of the below will be non-None.
    pub ipv4: Option<Ipv4Addr>,
    pub ipv6: Option<Ipv6Addr>,
    pub slot: SqlU8,
    #[diesel(column_name = is_primary)]
    pub primary: bool,
    pub transit_ips_v4: Vec<crate::Ipv4Net>,
    pub transit_ips_v6: Vec<crate::Ipv6Net>,
}

/// Service Network Interface DB model.
///
/// The underlying "table" (`service_network_interface`) is actually a view
/// over the `network_interface` table, that contains only rows with
/// `kind = 'service'`.
#[derive(Selectable, Queryable, Clone, Debug, PartialEq, Eq, Resource)]
#[diesel(table_name = service_network_interface)]
pub struct ServiceNetworkInterface {
    #[diesel(embed)]
    pub identity: ServiceNetworkInterfaceIdentity,
    pub service_id: Uuid,
    pub vpc_id: Uuid,
    pub subnet_id: Uuid,
    pub mac: MacAddr,
    // NOTE: At least one of the below will be non-None.
    pub ipv4: Option<Ipv4Addr>,
    pub ipv6: Option<Ipv6Addr>,
    pub slot: SqlU8,
    #[diesel(column_name = is_primary)]
    pub primary: bool,
}

impl ServiceNetworkInterface {
    /// Generate a suitable [`Name`] for the given Omicron zone ID and kind.
    pub fn name(zone_id: OmicronZoneUuid, zone_kind: ZoneKind) -> Name {
        // Most of these zone kinds do not get external networking and
        // therefore we don't need to be able to generate names for them, but
        // it's simpler to give them valid descriptions than worry about error
        // handling here.
        let prefix = zone_kind.name_prefix();

        // Now that we have a valid prefix, we know this format string
        // always produces a valid `Name`, so we'll unwrap here.
        let name = format!("{prefix}-{zone_id}")
            .parse()
            .expect("valid name failed to parse");

        Name(name)
    }
}

/// Errors converting from a ServiceNetworkInterface.
#[derive(Debug, thiserror::Error)]
pub enum ServiceNicError {
    #[error(
        "Service NIC {nic_id} has no IP addresses at all, which \
        ought to be impossible and enforced by the database constraints"
    )]
    NoIps { nic_id: Uuid },

    // TODO-remove: Remove this when we support dual-stack service NICs. See
    // https://github.com/oxidecomputer/omicron/issues/9314.
    #[error(
        "Service NIC {nic_id} is dual-stack, \
        only a single IPv4 or IPv6 address is supported"
    )]
    DualStack { nic_id: Uuid },
}

impl TryFrom<&'_ ServiceNetworkInterface>
    for nexus_types::deployment::OmicronZoneNic
{
    type Error = ServiceNicError;

    fn try_from(nic: &ServiceNetworkInterface) -> Result<Self, Self::Error> {
        let ip = match (nic.ipv4, nic.ipv6) {
            (None, None) => {
                return Err(ServiceNicError::NoIps { nic_id: nic.id() });
            }
            (None, Some(ip)) => ip.into(),
            (Some(ip), None) => ip.into(),
            (Some(_), Some(_)) => {
                return Err(ServiceNicError::DualStack { nic_id: nic.id() });
            }
        };
        Ok(Self {
            id: VnicUuid::from_untyped_uuid(nic.id()),
            mac: *nic.mac,
            ip,
            slot: *nic.slot,
            primary: nic.primary,
        })
    }
}

impl NetworkInterface {
    /// Treat this `NetworkInterface` as an `InstanceNetworkInterface`.
    ///
    /// # Panics
    /// Panics if this isn't an 'instance' kind network interface.
    pub fn as_instance(self) -> InstanceNetworkInterface {
        assert_eq!(self.kind, NetworkInterfaceKind::Instance);
        InstanceNetworkInterface {
            identity: InstanceNetworkInterfaceIdentity {
                id: self.identity.id,
                name: self.identity.name,
                description: self.identity.description,
                time_created: self.identity.time_created,
                time_modified: self.identity.time_modified,
                time_deleted: self.identity.time_deleted,
            },
            instance_id: self.parent_id,
            vpc_id: self.vpc_id,
            subnet_id: self.subnet_id,
            mac: self.mac,
            ipv4: self.ipv4,
            ipv6: self.ipv6,
            slot: self.slot,
            primary: self.primary,
            transit_ips_v4: self.transit_ips_v4,
            transit_ips_v6: self.transit_ips_v6,
        }
    }

    /// Treat this `NetworkInterface` as a `ServiceNetworkInterface`.
    ///
    /// # Panics
    /// Panics if this isn't a 'service' kind network interface.
    pub fn as_service(self) -> ServiceNetworkInterface {
        assert_eq!(self.kind, NetworkInterfaceKind::Service);
        ServiceNetworkInterface {
            identity: ServiceNetworkInterfaceIdentity {
                id: self.identity.id,
                name: self.identity.name,
                description: self.identity.description,
                time_created: self.identity.time_created,
                time_modified: self.identity.time_modified,
                time_deleted: self.identity.time_deleted,
            },
            service_id: self.parent_id,
            vpc_id: self.vpc_id,
            subnet_id: self.subnet_id,
            mac: self.mac,
            ipv4: self.ipv4,
            ipv6: self.ipv6,
            slot: self.slot,
            primary: self.primary,
        }
    }
}

impl From<InstanceNetworkInterface> for NetworkInterface {
    fn from(iface: InstanceNetworkInterface) -> Self {
        NetworkInterface {
            identity: NetworkInterfaceIdentity {
                id: iface.identity.id,
                name: iface.identity.name,
                description: iface.identity.description,
                time_created: iface.identity.time_created,
                time_modified: iface.identity.time_modified,
                time_deleted: iface.identity.time_deleted,
            },
            kind: NetworkInterfaceKind::Instance,
            parent_id: iface.instance_id,
            vpc_id: iface.vpc_id,
            subnet_id: iface.subnet_id,
            mac: iface.mac,
            ipv4: iface.ipv4,
            ipv6: iface.ipv6,
            slot: iface.slot,
            primary: iface.primary,
            transit_ips_v4: iface.transit_ips_v4,
            transit_ips_v6: iface.transit_ips_v6,
        }
    }
}

impl From<ServiceNetworkInterface> for NetworkInterface {
    fn from(iface: ServiceNetworkInterface) -> Self {
        NetworkInterface {
            identity: NetworkInterfaceIdentity {
                id: iface.identity.id,
                name: iface.identity.name,
                description: iface.identity.description,
                time_created: iface.identity.time_created,
                time_modified: iface.identity.time_modified,
                time_deleted: iface.identity.time_deleted,
            },
            kind: NetworkInterfaceKind::Service,
            parent_id: iface.service_id,
            vpc_id: iface.vpc_id,
            subnet_id: iface.subnet_id,
            mac: iface.mac,
            ipv4: iface.ipv4,
            ipv6: iface.ipv6,
            slot: iface.slot,
            primary: iface.primary,
            transit_ips_v4: vec![],
            transit_ips_v6: vec![],
        }
    }
}

/// A not fully constructed NetworkInterface. It may not yet have an IP
/// address allocated.
#[derive(Clone, Debug)]
pub struct IncompleteNetworkInterface {
    pub identity: NetworkInterfaceIdentity,
    pub kind: NetworkInterfaceKind,
    pub parent_id: Uuid,
    pub subnet: VpcSubnet,
    pub ip_config: PrivateIpStackCreate,
    pub mac: Option<external::MacAddr>,
    pub slot: Option<u8>,
}

impl IncompleteNetworkInterface {
    #[allow(clippy::too_many_arguments)]
    fn new(
        interface_id: Uuid,
        kind: NetworkInterfaceKind,
        parent_id: Uuid,
        subnet: VpcSubnet,
        identity: external::IdentityMetadataCreateParams,
        ip_config: PrivateIpStackCreate,
        mac: Option<external::MacAddr>,
        slot: Option<u8>,
    ) -> Result<Self, external::Error> {
        if let Some(ip) = ip_config.ipv4_addr() {
            subnet.check_requestable_addr(IpAddr::V4(*ip))?;
        };
        if let Some(ip) = ip_config.ipv6_addr() {
            subnet.check_requestable_addr(IpAddr::V6(*ip))?;
        };
        if let Some(mac) = mac {
            match kind {
                NetworkInterfaceKind::Instance => {
                    if !mac.is_guest() {
                        return Err(external::Error::invalid_request(format!(
                            "invalid MAC address {mac} for guest NIC",
                        )));
                    }
                }
                NetworkInterfaceKind::Probe => {
                    if !mac.is_guest() {
                        return Err(external::Error::invalid_request(format!(
                            "invalid MAC address {mac} for probe NIC",
                        )));
                    }
                }
                NetworkInterfaceKind::Service => {
                    if !mac.is_system() {
                        return Err(external::Error::invalid_request(format!(
                            "invalid MAC address {mac} for service NIC",
                        )));
                    }
                }
            }
        }
        if let Some(slot) = slot {
            if usize::from(slot) >= MAX_NICS_PER_INSTANCE {
                return Err(external::Error::invalid_request(format!(
                    "invalid slot {slot} for NIC (max slot = {})",
                    MAX_NICS_PER_INSTANCE - 1,
                )));
            }
        }
        let identity = NetworkInterfaceIdentity::new(interface_id, identity);
        Ok(IncompleteNetworkInterface {
            identity,
            kind,
            parent_id,
            subnet,
            ip_config,
            mac,
            slot,
        })
    }

    pub fn new_instance(
        interface_id: Uuid,
        instance_id: InstanceUuid,
        subnet: VpcSubnet,
        identity: external::IdentityMetadataCreateParams,
        ip_config: PrivateIpStackCreate,
    ) -> Result<Self, external::Error> {
        Self::new(
            interface_id,
            NetworkInterfaceKind::Instance,
            instance_id.into_untyped_uuid(),
            subnet,
            identity,
            ip_config,
            None,
            None,
        )
    }

    pub fn new_service(
        interface_id: Uuid,
        service_id: Uuid,
        subnet: VpcSubnet,
        identity: external::IdentityMetadataCreateParams,
        ip_config: PrivateIpStackCreate,
        mac: external::MacAddr,
        slot: u8,
    ) -> Result<Self, external::Error> {
        if ip_config.has_transit_ips() {
            return Err(external::Error::invalid_request(
                "Cannot specify transit IPs for service NICs",
            ));
        }
        Self::new(
            interface_id,
            NetworkInterfaceKind::Service,
            service_id,
            subnet,
            identity,
            ip_config,
            Some(mac),
            Some(slot),
        )
    }

    pub fn new_probe(
        interface_id: Uuid,
        probe_id: Uuid,
        subnet: VpcSubnet,
        identity: external::IdentityMetadataCreateParams,
        ip_config: PrivateIpStackCreate,
        mac: Option<external::MacAddr>,
    ) -> Result<Self, external::Error> {
        Self::new(
            interface_id,
            NetworkInterfaceKind::Probe,
            probe_id,
            subnet,
            identity,
            ip_config,
            mac,
            None,
        )
    }
}

/// Describes a set of updates for the [`NetworkInterface`] model.
#[derive(AsChangeset, Debug, Clone)]
#[diesel(table_name = network_interface)]
pub struct NetworkInterfaceUpdate {
    pub name: Option<Name>,
    pub description: Option<String>,
    pub time_modified: DateTime<Utc>,
    #[diesel(column_name = is_primary)]
    pub primary: Option<bool>,
    #[diesel(column_name = transit_ips)]
    pub transit_ips_v4: Vec<crate::Ipv4Net>,
    pub transit_ips_v6: Vec<crate::Ipv6Net>,
}

impl TryFrom<InstanceNetworkInterface> for external::InstanceNetworkInterface {
    type Error = external::Error;

    fn try_from(iface: InstanceNetworkInterface) -> Result<Self, Self::Error> {
        let maybe_ipv4_stack = iface.ipv4.map(|ip| PrivateIpv4Stack {
            ip: ip.into(),
            transit_ips: iface
                .transit_ips_v4
                .iter()
                .copied()
                .map(Into::into)
                .collect(),
        });
        let maybe_ipv6_stack = iface.ipv6.map(|ip| PrivateIpv6Stack {
            ip: ip.into(),
            transit_ips: iface
                .transit_ips_v6
                .iter()
                .copied()
                .map(Into::into)
                .collect(),
        });
        let ip_stack = match (maybe_ipv4_stack, maybe_ipv6_stack) {
            (None, None) => {
                return Err(Error::internal_error(
                    format!(
                        "Found a NIC in the database without any IP \
                    addresses, ID='{}'",
                        iface.identity.id,
                    )
                    .as_str(),
                ));
            }
            (None, Some(v6)) => PrivateIpStack::V6(v6),
            (Some(v4), None) => PrivateIpStack::V4(v4),
            (Some(v4), Some(v6)) => PrivateIpStack::DualStack { v4, v6 },
        };
        Ok(Self {
            identity: iface.identity(),
            instance_id: iface.instance_id,
            vpc_id: iface.vpc_id,
            subnet_id: iface.subnet_id,
            ip_stack,
            mac: *iface.mac,
            primary: iface.primary,
        })
    }
}

impl From<InstanceNetworkInterfaceUpdate> for NetworkInterfaceUpdate {
    fn from(params: InstanceNetworkInterfaceUpdate) -> Self {
        let primary = if params.primary { Some(true) } else { None };
        let (transit_ips_v4, transit_ips_v6): (Vec<_>, Vec<_>) =
            params.transit_ips.into_iter().partition_map(|net| match net {
                IpNet::V4(v4) => Either::Left(crate::Ipv4Net::from(v4)),
                IpNet::V6(v6) => Either::Right(crate::Ipv6Net::from(v6)),
            });
        Self {
            name: params.identity.name.map(|n| n.into()),
            description: params.identity.description,
            time_modified: Utc::now(),
            primary,
            transit_ips_v4,
            transit_ips_v6,
        }
    }
}
