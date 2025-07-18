// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{MacAddr, VpcSubnet};
use crate::Name;
use crate::SqlU8;
use crate::impl_enum_type;
use chrono::DateTime;
use chrono::Utc;
use db_macros::Resource;
use diesel::AsChangeset;
use ipnetwork::IpNetwork;
use ipnetwork::NetworkSize;
use nexus_db_schema::schema::instance_network_interface;
use nexus_db_schema::schema::network_interface;
use nexus_db_schema::schema::service_network_interface;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::{external, internal};
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::VnicUuid;
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

    pub kind: NetworkInterfaceKind,
    pub parent_id: Uuid,

    pub vpc_id: Uuid,
    pub subnet_id: Uuid,

    pub mac: MacAddr,
    // TODO-correctness: We need to split this into an optional V4 and optional V6 address, at
    // least one of which will always be specified.
    //
    // If user requests an address of either kind, give exactly that and not the other.
    // If neither is specified, auto-assign one of each?
    pub ip: IpNetwork,

    pub slot: SqlU8,
    #[diesel(column_name = is_primary)]
    pub primary: bool,

    pub transit_ips: Vec<IpNetwork>,
}

impl NetworkInterface {
    pub fn into_internal(
        self,
        subnet: oxnet::IpNet,
    ) -> internal::shared::NetworkInterface {
        internal::shared::NetworkInterface {
            id: self.id(),
            subnet_id: Some(self.subnet_id),
            vpc_id: Some(self.vpc_id),
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
            ip: self.ip.ip(),
            mac: self.mac.into(),
            subnet,
            vni: external::Vni::try_from(0).unwrap(),
            primary: self.primary,
            slot: *self.slot,
            transit_ips: self.transit_ips.into_iter().map(Into::into).collect(),
        }
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
    pub ip: IpNetwork,

    pub slot: SqlU8,
    #[diesel(column_name = is_primary)]
    pub primary: bool,

    pub transit_ips: Vec<IpNetwork>,
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
    pub ip: IpNetwork,

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

#[derive(Debug, thiserror::Error)]
#[error(
    "Service NIC {nic_id} has a range of IPs ({ip}); only a single IP is supported"
)]
pub struct ServiceNicNotSingleIpError {
    pub nic_id: Uuid,
    pub ip: ipnetwork::IpNetwork,
}

impl TryFrom<&'_ ServiceNetworkInterface>
    for nexus_types::deployment::OmicronZoneNic
{
    type Error = ServiceNicNotSingleIpError;

    fn try_from(nic: &ServiceNetworkInterface) -> Result<Self, Self::Error> {
        let size = match nic.ip.size() {
            NetworkSize::V4(n) => u128::from(n),
            NetworkSize::V6(n) => n,
        };
        if size != 1 {
            return Err(ServiceNicNotSingleIpError {
                nic_id: nic.id(),
                ip: nic.ip,
            });
        }
        Ok(Self {
            id: VnicUuid::from_untyped_uuid(nic.id()),
            mac: *nic.mac,
            ip: nic.ip.ip(),
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
            ip: self.ip,
            slot: self.slot,
            primary: self.primary,
            transit_ips: self.transit_ips,
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
            ip: self.ip,
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
            ip: iface.ip,
            slot: iface.slot,
            primary: iface.primary,
            transit_ips: iface.transit_ips,
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
            ip: iface.ip,
            slot: iface.slot,
            primary: iface.primary,
            transit_ips: vec![],
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
    pub ip: Option<std::net::IpAddr>,
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
        ip: Option<std::net::IpAddr>,
        mac: Option<external::MacAddr>,
        slot: Option<u8>,
    ) -> Result<Self, external::Error> {
        if let Some(ip) = ip {
            subnet.check_requestable_addr(ip)?;
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
            ip,
            mac,
            slot,
        })
    }

    pub fn new_instance(
        interface_id: Uuid,
        instance_id: InstanceUuid,
        subnet: VpcSubnet,
        identity: external::IdentityMetadataCreateParams,
        ip: Option<std::net::IpAddr>,
    ) -> Result<Self, external::Error> {
        Self::new(
            interface_id,
            NetworkInterfaceKind::Instance,
            instance_id.into_untyped_uuid(),
            subnet,
            identity,
            ip,
            None,
            None,
        )
    }

    pub fn new_service(
        interface_id: Uuid,
        service_id: Uuid,
        subnet: VpcSubnet,
        identity: external::IdentityMetadataCreateParams,
        ip: std::net::IpAddr,
        mac: external::MacAddr,
        slot: u8,
    ) -> Result<Self, external::Error> {
        Self::new(
            interface_id,
            NetworkInterfaceKind::Service,
            service_id,
            subnet,
            identity,
            Some(ip),
            Some(mac),
            Some(slot),
        )
    }

    pub fn new_probe(
        interface_id: Uuid,
        probe_id: Uuid,
        subnet: VpcSubnet,
        identity: external::IdentityMetadataCreateParams,
        ip: Option<std::net::IpAddr>,
        mac: Option<external::MacAddr>,
    ) -> Result<Self, external::Error> {
        Self::new(
            interface_id,
            NetworkInterfaceKind::Probe,
            probe_id,
            subnet,
            identity,
            ip,
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
    pub transit_ips: Vec<IpNetwork>,
}

impl From<InstanceNetworkInterface> for external::InstanceNetworkInterface {
    fn from(iface: InstanceNetworkInterface) -> Self {
        Self {
            identity: iface.identity(),
            instance_id: iface.instance_id,
            vpc_id: iface.vpc_id,
            subnet_id: iface.subnet_id,
            ip: iface.ip.ip(),
            mac: *iface.mac,
            primary: iface.primary,
            transit_ips: iface
                .transit_ips
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

impl From<params::InstanceNetworkInterfaceUpdate> for NetworkInterfaceUpdate {
    fn from(params: params::InstanceNetworkInterfaceUpdate) -> Self {
        let primary = if params.primary { Some(true) } else { None };
        Self {
            name: params.identity.name.map(|n| n.into()),
            description: params.identity.description,
            time_modified: Utc::now(),
            primary,
            transit_ips: params
                .transit_ips
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}
