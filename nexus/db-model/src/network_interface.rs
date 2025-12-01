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
use ipnetwork::IpNetwork;
use nexus_db_schema::schema::instance_network_interface;
use nexus_db_schema::schema::network_interface;
use nexus_db_schema::schema::service_network_interface;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::external::Error;
use omicron_common::api::internal::shared::PrivateIpConfig;
use omicron_common::api::internal::shared::PrivateIpv4Config;
use omicron_common::api::internal::shared::PrivateIpv6Config;
use omicron_common::api::{external, internal};
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::VnicUuid;
use oxnet::IpNet;
use oxnet::Ipv4Net;
use oxnet::Ipv6Net;
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
    /// List of additional networks on which the instance is allowed to send /
    /// receive traffic.
    pub transit_ips: Vec<IpNetwork>,
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
                // Check that all transit IPs are IPv6.
                let transit_ips = self
                    .transit_ips
                    .iter()
                    .map(|net| {
                        let IpNetwork::V6(net) = net else {
                            return Err(Error::internal_error(&format!(
                                "NIC with ID '{}' is IPv6-only, but has \
                                IPv4 transit IPs",
                                self.id(),
                            )));
                        };
                        Ok(Ipv6Net::from(*net))
                    })
                    .collect::<Result<_, _>>()?;
                PrivateIpConfig::V6(PrivateIpv6Config::new_with_transit_ips(
                    *ip,
                    ipv6_subnet,
                    transit_ips,
                )?)
            }
            (Some(ip), None) => {
                // Check that all transit IPs are IPv4.
                let transit_ips = self
                    .transit_ips
                    .iter()
                    .map(|net| {
                        let IpNetwork::V4(net) = net else {
                            return Err(Error::internal_error(&format!(
                                "NIC with ID '{}' is IPv4-only, but has \
                                IPv6 transit IPs",
                                self.id(),
                            )));
                        };
                        Ok(Ipv4Net::from(*net))
                    })
                    .collect::<Result<_, _>>()?;
                PrivateIpConfig::V4(PrivateIpv4Config::new_with_transit_ips(
                    *ip,
                    ipv4_subnet,
                    transit_ips,
                )?)
            }
            (Some(ipv4), Some(ipv6)) => {
                let ipv4_transit_ips = self
                    .transit_ips
                    .iter()
                    .filter_map(|net| match net {
                        IpNetwork::V4(net) => Some(Ipv4Net::from(*net)),
                        IpNetwork::V6(_) => None,
                    })
                    .collect();
                let ipv6_transit_ips = self
                    .transit_ips
                    .iter()
                    .filter_map(|net| match net {
                        IpNetwork::V6(net) => Some(Ipv6Net::from(*net)),
                        IpNetwork::V4(_) => None,
                    })
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
            ipv4: iface.ipv4,
            ipv6: iface.ipv6,
            slot: iface.slot,
            primary: iface.primary,
            transit_ips: vec![],
        }
    }
}

mod private {
    pub trait IpSealed: Clone + Copy + std::fmt::Debug {
        fn into_ipnet(self) -> ipnetwork::IpNetwork;
    }

    impl IpSealed for std::net::Ipv4Addr {
        fn into_ipnet(self) -> ipnetwork::IpNetwork {
            ipnetwork::IpNetwork::V4(ipnetwork::Ipv4Network::from(self))
        }
    }
    impl IpSealed for std::net::Ipv6Addr {
        fn into_ipnet(self) -> ipnetwork::IpNetwork {
            ipnetwork::IpNetwork::V6(ipnetwork::Ipv6Network::from(self))
        }
    }
}

pub trait Ip: private::IpSealed {}
impl<T> Ip for T where T: private::IpSealed {}

/// How an IP address is assigned to an interface.
#[derive(Clone, Copy, Debug, Default)]
pub enum IpAssignment<T: Ip> {
    /// Automatically assign an IP address.
    #[default]
    Auto,
    /// Explicitly assign a specific address, if available.
    Explicit(T),
}

/// How to assign an IPv4 address.
pub type Ipv4Assignment = IpAssignment<std::net::Ipv4Addr>;

/// How to assign an IPv6 address.
pub type Ipv6Assignment = IpAssignment<std::net::Ipv6Addr>;

/// Configuration for a network interface's IPv4 addressing.
#[derive(Clone, Debug, Default)]
pub struct Ipv4Config {
    /// The VPC-private address to assign to the interface.
    pub ip: Ipv4Assignment,
    /// Additional IP networks the interface can send / receive on.
    pub transit_ips: Vec<Ipv4Net>,
}

/// Configuration for a network interface's IPv6 addressing.
#[derive(Clone, Debug, Default)]
pub struct Ipv6Config {
    /// The VPC-private address to assign to the interface.
    pub ip: Ipv6Assignment,
    /// Additional IP networks the interface can send / receive on.
    pub transit_ips: Vec<Ipv6Net>,
}

/// Configuration for a network interface's IP addressing.
#[derive(Clone, Debug)]
pub enum IpConfig {
    /// The interface has only an IPv4 stack.
    V4(Ipv4Config),
    /// The interface has only an IPv6 stack.
    V6(Ipv6Config),
    /// The interface has both an IPv4 and IPv6 stack.
    DualStack { v4: Ipv4Config, v6: Ipv6Config },
}

impl IpConfig {
    /// Construct an IPv4 configuration with no transit IPs.
    pub fn from_ipv4(addr: std::net::Ipv4Addr) -> Self {
        IpConfig::V4(Ipv4Config {
            ip: Ipv4Assignment::Explicit(addr),
            transit_ips: vec![],
        })
    }

    /// Construct an IP configuration with only an automatic IPv4 address.
    pub fn auto_ipv4() -> Self {
        IpConfig::V4(Ipv4Config::default())
    }

    /// Return the IPv4 address assignment.
    pub fn ipv4_assignment(&self) -> Option<&Ipv4Assignment> {
        match self {
            IpConfig::V4(Ipv4Config { ip, .. }) => Some(ip),
            IpConfig::V6(_) => None,
            IpConfig::DualStack { v4: Ipv4Config { ip, .. }, .. } => Some(ip),
        }
    }

    /// Return the IPv4 address explicitly requested, if one exists.
    pub fn ipv4_addr(&self) -> Option<&std::net::Ipv4Addr> {
        self.ipv4_assignment().and_then(|assignment| match assignment {
            IpAssignment::Auto => None,
            IpAssignment::Explicit(addr) => Some(addr),
        })
    }

    /// Construct an IPv6 configuration with no transit IPs.
    pub fn from_ipv6(addr: std::net::Ipv6Addr) -> Self {
        IpConfig::V6(Ipv6Config {
            ip: Ipv6Assignment::Explicit(addr),
            transit_ips: vec![],
        })
    }

    /// Construct an IP configuration with only an automatic IPv6 address.
    pub fn auto_ipv6() -> Self {
        IpConfig::V6(Ipv6Config::default())
    }

    /// Return the IPv6 address assignment.
    pub fn ipv6_assignment(&self) -> Option<&Ipv6Assignment> {
        match self {
            IpConfig::V6(Ipv6Config { ip, .. }) => Some(ip),
            IpConfig::V4(_) => None,
            IpConfig::DualStack { v6: Ipv6Config { ip, .. }, .. } => Some(ip),
        }
    }

    /// Return the IPv6 address explicitly requested, if one exists.
    pub fn ipv6_addr(&self) -> Option<&std::net::Ipv6Addr> {
        self.ipv6_assignment().and_then(|assignment| match assignment {
            IpAssignment::Auto => None,
            IpAssignment::Explicit(addr) => Some(addr),
        })
    }

    /// Return the transit IPs requested in this configuration.
    pub fn transit_ips(&self) -> Vec<IpNet> {
        match self {
            IpConfig::V4(Ipv4Config { transit_ips, .. }) => {
                transit_ips.iter().copied().map(Into::into).collect()
            }
            IpConfig::V6(Ipv6Config { transit_ips, .. }) => {
                transit_ips.iter().copied().map(Into::into).collect()
            }
            IpConfig::DualStack {
                v4: Ipv4Config { transit_ips: ipv4_addrs, .. },
                v6: Ipv6Config { transit_ips: ipv6_addrs, .. },
            } => ipv4_addrs
                .iter()
                .copied()
                .map(Into::into)
                .chain(ipv6_addrs.iter().copied().map(Into::into))
                .collect(),
        }
    }

    /// Construct a dual-stack IP configuration with explicit IP addresses.
    pub fn new_dual_stack(
        ipv4: std::net::Ipv4Addr,
        ipv6: std::net::Ipv6Addr,
    ) -> Self {
        IpConfig::DualStack {
            v4: Ipv4Config {
                ip: Ipv4Assignment::Explicit(ipv4),
                transit_ips: Vec::new(),
            },
            v6: Ipv6Config {
                ip: Ipv6Assignment::Explicit(ipv6),
                transit_ips: Vec::new(),
            },
        }
    }

    /// Construct an IP configuration with both IPv4 / IPv6 addresses and no
    /// transit IPs.
    pub fn auto_dual_stack() -> Self {
        IpConfig::DualStack {
            v4: Ipv4Config::default(),
            v6: Ipv6Config::default(),
        }
    }

    /// Return true if this config has any transit IPs
    fn has_transit_ips(&self) -> bool {
        match self {
            IpConfig::V4(Ipv4Config { transit_ips, .. }) => {
                !transit_ips.is_empty()
            }
            IpConfig::V6(Ipv6Config { transit_ips, .. }) => {
                !transit_ips.is_empty()
            }
            IpConfig::DualStack {
                v4: Ipv4Config { transit_ips: ipv4_addrs, .. },
                v6: Ipv6Config { transit_ips: ipv6_addrs, .. },
            } => !ipv4_addrs.is_empty() || !ipv6_addrs.is_empty(),
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
    pub ip_config: IpConfig,
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
        ip_config: IpConfig,
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
        ip_config: IpConfig,
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
        ip_config: IpConfig,
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
        ip_config: IpConfig,
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
    pub transit_ips: Vec<IpNetwork>,
}

impl From<InstanceNetworkInterface> for external::InstanceNetworkInterface {
    fn from(iface: InstanceNetworkInterface) -> Self {
        // TODO-completeness: Support dual-stack in the public API, see
        // https://github.com/oxidecomputer/omicron/issues/9248.
        let ip = iface.ipv4.expect("only IPv4 addresses").into();
        Self {
            identity: iface.identity(),
            instance_id: iface.instance_id,
            vpc_id: iface.vpc_id,
            subnet_id: iface.subnet_id,
            ip,
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
