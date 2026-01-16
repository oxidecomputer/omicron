// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus external types that changed from 2026010100 to 2026010300.
//!
//! ## Network Interface Changes
//!
//! This version adds dual-stack NIC support with [`InstanceNetworkInterfaceAttachment`]
//! and [`InstanceNetworkInterfaceCreate`] changes.
//!
//! ## Multicast Changes
//!
//! `InstanceCreate.multicast_groups` uses `Vec<NameOrId>` instead of
//! `Vec<MulticastGroupJoinSpec>`. The conversion adds default values for
//! `source_ips` and `ip_version` fields.
//!
//! Affected endpoints:
//! - `POST /v1/instances` (instance_create)
//!
//! [`InstanceNetworkInterfaceAttachment`]: self::InstanceNetworkInterfaceAttachment
//! [`InstanceNetworkInterfaceCreate`]: self::InstanceNetworkInterfaceCreate

use api_identity::ObjectIdentity;
use itertools::Either;
use itertools::Itertools as _;
use nexus_types::external_api::params;

use nexus_types::external_api::params::IpAssignment;
use nexus_types::external_api::params::PrivateIpStackCreate;
use nexus_types::external_api::params::PrivateIpv4StackCreate;
use nexus_types::external_api::params::PrivateIpv6StackCreate;
use nexus_types::external_api::shared::ProbeExternalIp;
use omicron_common::api::external;
use omicron_common::api::external::IdentityMetadata;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::MacAddr;
use omicron_common::api::external::Name;
use omicron_common::api::external::ObjectIdentity;
use omicron_common::api::external::PrivateIpStack;
use omicron_common::api::external::PrivateIpv4Stack;
use omicron_common::api::external::PrivateIpv6Stack;
use omicron_common::api::internal::shared::v1::NetworkInterface as NetworkInterfaceV1;
use omicron_uuid_kinds::SledUuid;
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::net::IpAddr;
use uuid::Uuid;

use crate::v2026010300;

/// Describes an attachment of an `InstanceNetworkInterface` to an `Instance`,
/// at the time the instance is created.
// NOTE: VPC's are an organizing concept for networking resources, not for
// instances. It's true that all networking resources for an instance must
// belong to a single VPC, but we don't consider instances to be "scoped" to a
// VPC in the same way that they are scoped to projects, for example.
//
// This is slightly different than some other cloud providers, such as AWS,
// which use VPCs as both a networking concept, and a container more similar to
// our concept of a project. One example for why this is useful is that "moving"
// an instance to a new VPC can be done by detaching any interfaces in the
// original VPC and attaching interfaces in the new VPC.
//
// This type then requires the VPC identifiers, exactly because instances are
// _not_ scoped to a VPC, and so the VPC and/or VPC Subnet names are not present
// in the path of endpoints handling instance operations.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", content = "params", rename_all = "snake_case")]
pub enum InstanceNetworkInterfaceAttachment {
    /// Create one or more `InstanceNetworkInterface`s for the `Instance`.
    ///
    /// If more than one interface is provided, then the first will be
    /// designated the primary interface for the instance.
    Create(Vec<InstanceNetworkInterfaceCreate>),

    /// The default networking configuration for an instance is to create a
    /// single primary interface with an automatically-assigned IP address. The
    /// IP will be pulled from the Project's default VPC / VPC Subnet.
    #[default]
    Default,

    /// No network interfaces at all will be created for the instance.
    None,
}

// v2026010100 (PRE_DUAL_STACK_NICS) uses v2026011300's network interface types
// (pre-VPC_SUBNET_ATTACHMENT, with `subnet_name: Name`).
//
// The difference is in the NIC create parameters:
// - v2026010100 uses `ip: Option<IpAddr>` + `transit_ips: Vec<IpNet>`
// - v2026011300 uses `ip_config: PrivateIpStackCreate`
use crate::v2026011300;

impl TryFrom<InstanceNetworkInterfaceAttachment>
    for v2026011300::InstanceNetworkInterfaceAttachment
{
    type Error = external::Error;

    fn try_from(
        value: InstanceNetworkInterfaceAttachment,
    ) -> Result<Self, Self::Error> {
        match value {
            InstanceNetworkInterfaceAttachment::Create(nics) => nics
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()
                .map(Self::Create),
            InstanceNetworkInterfaceAttachment::Default => {
                Ok(Self::DefaultDualStack)
            }
            InstanceNetworkInterfaceAttachment::None => Ok(Self::None),
        }
    }
}

/// An `InstanceNetworkInterface` represents a virtual network interface device
/// attached to an instance.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct InstanceNetworkInterface {
    /// common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The Instance to which the interface belongs.
    pub instance_id: Uuid,

    /// The VPC to which the interface belongs.
    pub vpc_id: Uuid,

    /// The subnet to which the interface belongs.
    pub subnet_id: Uuid,

    /// The MAC address assigned to this interface.
    pub mac: MacAddr,

    /// The IP address assigned to this interface.
    pub ip: IpAddr,

    /// True if this interface is the primary for the instance to which it's
    /// attached.
    pub primary: bool,

    /// A set of additional networks that this interface may send and
    /// receive traffic on.
    #[serde(default)]
    pub transit_ips: Vec<IpNet>,
}

impl TryFrom<InstanceNetworkInterface> for external::InstanceNetworkInterface {
    type Error = external::Error;

    fn try_from(value: InstanceNetworkInterface) -> Result<Self, Self::Error> {
        let ip_stack = match value.ip {
            IpAddr::V4(ip) => {
                let transit_ips = value
                    .transit_ips
                    .into_iter()
                    .map(|ipnet| match ipnet {
                        IpNet::V4(v4) => Ok(v4),
                        IpNet::V6(_) => Err(external::Error::invalid_request(
                            "A network interface cannot have an IPv4 \
                                address and IPv6 transit IPs",
                        )),
                    })
                    .collect::<Result<_, _>>()?;
                PrivateIpStack::V4(PrivateIpv4Stack { ip, transit_ips })
            }
            IpAddr::V6(ip) => {
                let transit_ips = value
                    .transit_ips
                    .into_iter()
                    .map(|ipnet| match ipnet {
                        IpNet::V6(v6) => Ok(v6),
                        IpNet::V4(_) => Err(external::Error::invalid_request(
                            "A network interface cannot have an IPv6 \
                                address and IPv4 transit IPs",
                        )),
                    })
                    .collect::<Result<_, _>>()?;
                PrivateIpStack::V6(PrivateIpv6Stack { ip, transit_ips })
            }
        };
        Ok(external::InstanceNetworkInterface {
            identity: value.identity,
            instance_id: value.instance_id,
            vpc_id: value.vpc_id,
            subnet_id: value.subnet_id,
            mac: value.mac,
            primary: value.primary,
            ip_stack,
        })
    }
}

impl TryFrom<external::InstanceNetworkInterface> for InstanceNetworkInterface {
    type Error = external::Error;

    fn try_from(
        value: external::InstanceNetworkInterface,
    ) -> Result<Self, Self::Error> {
        let (ip, transit_ips) = match value.ip_stack {
            PrivateIpStack::V4(v4) => (
                v4.ip.into(),
                v4.transit_ips.into_iter().map(Into::into).collect(),
            ),
            PrivateIpStack::V6(v6) => (
                v6.ip.into(),
                v6.transit_ips.into_iter().map(Into::into).collect(),
            ),
            PrivateIpStack::DualStack { v4, v6 } => {
                return Err(external::Error::invalid_request(format!(
                    "The network interface with ID '{}' is \
                        a dual-stack NIC, with IPv4 address '{}' \
                        and IPv6 address '{}'. However, the version \
                        of the client being used is unable to fully \
                        represent both IPv4 and IPv6 addresses. \
                        Update your client and retry the request.",
                    value.identity.id, v4.ip, v6.ip,
                )));
            }
        };
        Ok(Self {
            identity: value.identity,
            instance_id: value.instance_id,
            vpc_id: value.vpc_id,
            subnet_id: value.subnet_id,
            mac: value.mac,
            ip,
            primary: value.primary,
            transit_ips,
        })
    }
}

/// Create-time parameters for an `InstanceNetworkInterface`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceNetworkInterfaceCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
    /// The VPC in which to create the interface.
    pub vpc_name: Name,
    /// The VPC Subnet in which to create the interface.
    pub subnet_name: Name,
    /// The IP address for the interface. One will be auto-assigned if not provided.
    pub ip: Option<IpAddr>,
    /// A set of additional networks that this interface may send and
    /// receive traffic on.
    #[serde(default)]
    pub transit_ips: Vec<IpNet>,
}

impl TryFrom<InstanceNetworkInterfaceCreate>
    for v2026011300::InstanceNetworkInterfaceCreate
{
    type Error = external::Error;

    fn try_from(
        value: InstanceNetworkInterfaceCreate,
    ) -> Result<Self, Self::Error> {
        let (ipv4_transit_ips, ipv6_transit_ips): (Vec<_>, Vec<_>) =
            value.transit_ips.into_iter().partition_map(|net| match net {
                IpNet::V4(ipv4) => Either::Left(ipv4),
                IpNet::V6(ipv6) => Either::Right(ipv6),
            });
        if !ipv4_transit_ips.is_empty() && !ipv6_transit_ips.is_empty() {
            return Err(external::Error::invalid_request(
                "Cannot specify both IPv4 and IPv6 transit IPs",
            ));
        }
        let ip_config = match value.ip {
            None => {
                if !ipv4_transit_ips.is_empty() {
                    PrivateIpStackCreate::V4(PrivateIpv4StackCreate {
                        ip: IpAssignment::Auto,
                        transit_ips: ipv4_transit_ips,
                    })
                } else if !ipv6_transit_ips.is_empty() {
                    PrivateIpStackCreate::V6(PrivateIpv6StackCreate {
                        ip: IpAssignment::Auto,
                        transit_ips: ipv6_transit_ips,
                    })
                } else {
                    PrivateIpStackCreate::auto_dual_stack()
                }
            }
            Some(IpAddr::V4(ipv4)) => {
                if !ipv6_transit_ips.is_empty() {
                    return Err(external::Error::invalid_request(
                        "Cannot specify IPv6 transit IPs with an IPv4 address",
                    ));
                }
                PrivateIpStackCreate::V4(PrivateIpv4StackCreate {
                    ip: IpAssignment::Explicit(ipv4),
                    transit_ips: ipv4_transit_ips,
                })
            }
            Some(IpAddr::V6(ipv6)) => {
                if !ipv4_transit_ips.is_empty() {
                    return Err(external::Error::invalid_request(
                        "Cannot specify IPv4 transit IPs with an IPv6 address",
                    ));
                }
                PrivateIpStackCreate::V6(PrivateIpv6StackCreate {
                    ip: IpAssignment::Explicit(ipv6),
                    transit_ips: ipv6_transit_ips,
                })
            }
        };
        Ok(Self {
            identity: value.identity,
            vpc_name: value.vpc_name,
            subnet_name: value.subnet_name,
            ip_config,
        })
    }
}

/// Create-time parameters for an `Instance`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceCreate {
    #[serde(flatten)]
    pub identity: external::IdentityMetadataCreateParams,
    /// The number of vCPUs to be allocated to the instance
    pub ncpus: external::InstanceCpuCount,
    /// The amount of RAM (in bytes) to be allocated to the instance
    pub memory: external::ByteCount,
    /// The hostname to be assigned to the instance
    pub hostname: external::Hostname,

    /// User data for instance initialization systems (such as cloud-init).
    /// Must be a Base64-encoded string, as specified in RFC 4648 § 4 (+ and /
    /// characters with padding). Maximum 32 KiB unencoded data.
    // While serde happily accepts #[serde(with = "<mod>")] as a shorthand for
    // specifying `serialize_with` and `deserialize_with`, schemars requires the
    // argument to `with` to be a type rather than merely a path prefix (i.e. a
    // mod or type). It's admittedly a bit tricky for schemars to address;
    // unlike `serialize` or `deserialize`, `JsonSchema` requires several
    // functions working together. It's unfortunate that schemars has this
    // built-in incompatibility, exacerbated by its glacial rate of progress
    // and immunity to offers of help.
    #[serde(default, with = "params::UserData")]
    pub user_data: Vec<u8>,

    /// The network interfaces to be created for this instance.
    #[serde(default)]
    pub network_interfaces: InstanceNetworkInterfaceAttachment,

    /// The external IP addresses provided to this instance.
    ///
    /// By default, all instances have outbound connectivity, but no inbound
    /// connectivity. These external addresses can be used to provide a fixed,
    /// known IP address for making inbound connections to the instance.
    #[serde(default)]
    pub external_ips: Vec<v2026010300::ExternalIpCreate>,

    /// The multicast groups this instance should join.
    ///
    /// The instance will be automatically added as a member of the specified
    /// multicast groups during creation, enabling it to send and receive
    /// multicast traffic for those groups.
    #[serde(default)]
    pub multicast_groups: Vec<external::NameOrId>,

    /// A list of disks to be attached to the instance.
    ///
    /// Disk attachments of type "create" will be created, while those of type
    /// "attach" must already exist.
    ///
    /// The order of this list does not guarantee a boot order for the instance.
    /// Use the boot_disk attribute to specify a boot disk. When boot_disk is
    /// specified it will count against the disk attachment limit.
    #[serde(default)]
    pub disks: Vec<params::InstanceDiskAttachment>,

    /// The disk the instance is configured to boot from.
    ///
    /// This disk can either be attached if it already exists or created along
    /// with the instance.
    ///
    /// Specifying a boot disk is optional but recommended to ensure predictable
    /// boot behavior. The boot disk can be set during instance creation or
    /// later if the instance is stopped. The boot disk counts against the disk
    /// attachment limit.
    ///
    /// An instance that does not have a boot disk set will use the boot
    /// options specified in its UEFI settings, which are controlled by both the
    /// instance's UEFI firmware and the guest operating system. Boot options
    /// can change as disks are attached and detached, which may result in an
    /// instance that only boots to the EFI shell until a boot disk is set.
    #[serde(default)]
    pub boot_disk: Option<params::InstanceDiskAttachment>,

    /// An allowlist of SSH public keys to be transferred to the instance via
    /// cloud-init during instance creation.
    ///
    /// If not provided, all SSH public keys from the user's profile will be sent.
    /// If an empty list is provided, no public keys will be transmitted to the
    /// instance.
    pub ssh_public_keys: Option<Vec<external::NameOrId>>,

    /// Should this instance be started upon creation; true by default.
    #[serde(default = "params::bool_true")]
    pub start: bool,

    /// The auto-restart policy for this instance.
    ///
    /// This policy determines whether the instance should be automatically
    /// restarted by the control plane on failure. If this is `null`, no
    /// auto-restart policy will be explicitly configured for this instance, and
    /// the control plane will select the default policy when determining
    /// whether the instance can be automatically restarted.
    ///
    /// Currently, the global default auto-restart policy is "best-effort", so
    /// instances with `null` auto-restart policies will be automatically
    /// restarted. However, in the future, the default policy may be
    /// configurable through other mechanisms, such as on a per-project basis.
    /// In that case, any configured default policy will be used if this is
    /// `null`.
    #[serde(default)]
    pub auto_restart_policy: Option<external::InstanceAutoRestartPolicy>,

    /// Anti-Affinity groups which this instance should be added.
    #[serde(default)]
    pub anti_affinity_groups: Vec<external::NameOrId>,

    /// The CPU platform to be used for this instance. If this is `null`, the
    /// instance requires no particular CPU platform; when it is started the
    /// instance will have the most general CPU platform supported by the sled
    /// it is initially placed on.
    #[serde(default)]
    pub cpu_platform: Option<external::InstanceCpuPlatform>,
}

impl TryFrom<InstanceCreate> for v2026010300::InstanceCreate {
    type Error = external::Error;

    fn try_from(value: InstanceCreate) -> Result<Self, Self::Error> {
        let network_interfaces = value.network_interfaces.try_into()?;
        Ok(Self {
            identity: value.identity,
            ncpus: value.ncpus,
            memory: value.memory,
            hostname: value.hostname,
            user_data: value.user_data,
            network_interfaces,
            external_ips: value.external_ips,
            multicast_groups: value.multicast_groups,
            disks: value.disks,
            boot_disk: value.boot_disk,
            ssh_public_keys: value.ssh_public_keys,
            start: value.start,
            auto_restart_policy: value.auto_restart_policy,
            anti_affinity_groups: value.anti_affinity_groups,
            cpu_platform: value.cpu_platform,
        })
    }
}

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct ProbeInfo {
    pub id: Uuid,
    pub name: Name,
    #[schemars(with = "Uuid")]
    pub sled: SledUuid,
    pub external_ips: Vec<ProbeExternalIp>,
    pub interface: NetworkInterfaceV1,
}

impl TryFrom<nexus_types::external_api::shared::ProbeInfo> for ProbeInfo {
    type Error = omicron_common::api::external::Error;
    fn try_from(
        value: nexus_types::external_api::shared::ProbeInfo,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            name: value.name,
            sled: value.sled,
            external_ips: value.external_ips,
            interface: value.interface.try_into()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_types::external_api::params::IpAssignment;
    use proptest::prelude::*;

    /// Helper to create a NIC with the given IP and transit IPs.
    fn make_nic(
        ip: Option<IpAddr>,
        transit_ips: Vec<IpNet>,
    ) -> InstanceNetworkInterfaceCreate {
        InstanceNetworkInterfaceCreate {
            identity: IdentityMetadataCreateParams {
                name: "nic0".parse().unwrap(),
                description: "test".to_string(),
            },
            vpc_name: "default".parse().unwrap(),
            subnet_name: "default".parse().unwrap(),
            ip,
            transit_ips,
        }
    }

    /// Strategy for generating IPv4 networks (address + prefix 8-30).
    fn arb_ipv4_net() -> impl Strategy<Value = oxnet::Ipv4Net> {
        (any::<std::net::Ipv4Addr>(), 8u8..=30).prop_map(|(addr, prefix)| {
            oxnet::Ipv4Net::new(addr, prefix)
                .unwrap_or_else(|_| oxnet::Ipv4Net::new(addr, 24).unwrap())
        })
    }

    /// Strategy for generating IPv6 networks (address + prefix 16-120).
    fn arb_ipv6_net() -> impl Strategy<Value = oxnet::Ipv6Net> {
        (any::<std::net::Ipv6Addr>(), 16u8..=120).prop_map(|(addr, prefix)| {
            oxnet::Ipv6Net::new(addr, prefix)
                .unwrap_or_else(|_| oxnet::Ipv6Net::new(addr, 64).unwrap())
        })
    }

    // =========================================================================
    // Semantic choice: old `Default` maps to new `DefaultDualStack`
    // =========================================================================

    #[test]
    fn attachment_default_converts_to_dual_stack() {
        let result: v2026011300::InstanceNetworkInterfaceAttachment =
            InstanceNetworkInterfaceAttachment::Default.try_into().unwrap();
        assert!(matches!(
            result,
            v2026011300::InstanceNetworkInterfaceAttachment::DefaultDualStack
        ));
    }

    // =========================================================================
    // Semantic choice: no IP + no transit IPs defaults to dual-stack
    // =========================================================================

    #[test]
    fn no_ip_no_transit_defaults_to_dual_stack() {
        let result: v2026011300::InstanceNetworkInterfaceCreate =
            make_nic(None, vec![]).try_into().unwrap();
        assert!(matches!(
            result.ip_config,
            PrivateIpStackCreate::DualStack { .. }
        ));
    }

    proptest! {
        // =====================================================================
        // Property: IPv4 address produces V4 stack with that address
        // =====================================================================

        #[test]
        fn ipv4_address_produces_v4_stack(ip in any::<std::net::Ipv4Addr>()) {
            let nic = make_nic(Some(ip.into()), vec![]);
            let result: v2026011300::InstanceNetworkInterfaceCreate =
                nic.try_into().unwrap();

            prop_assert!(matches!(
                result.ip_config,
                PrivateIpStackCreate::V4(v4)
                    if matches!(v4.ip, IpAssignment::Explicit(x) if x == ip)
            ));
        }

        // =====================================================================
        // Property: IPv6 address produces V6 stack with that address
        // =====================================================================

        #[test]
        fn ipv6_address_produces_v6_stack(ip in any::<std::net::Ipv6Addr>()) {
            let nic = make_nic(Some(ip.into()), vec![]);
            let result: v2026011300::InstanceNetworkInterfaceCreate =
                nic.try_into().unwrap();

            prop_assert!(matches!(
                result.ip_config,
                PrivateIpStackCreate::V6(v6)
                    if matches!(v6.ip, IpAssignment::Explicit(x) if x == ip)
            ));
        }

        // =====================================================================
        // Property: IPv4 transit IPs are preserved in output
        // =====================================================================

        #[test]
        fn ipv4_transit_ips_preserved(
            ip in any::<std::net::Ipv4Addr>(),
            transit in proptest::collection::vec(arb_ipv4_net(), 0..4),
        ) {
            let transit_ipnet: Vec<IpNet> =
                transit.iter().copied().map(Into::into).collect();
            let nic = make_nic(Some(ip.into()), transit_ipnet);
            let result: v2026011300::InstanceNetworkInterfaceCreate =
                nic.try_into().unwrap();

            prop_assert!(matches!(
                result.ip_config,
                PrivateIpStackCreate::V4(v4) if v4.transit_ips == transit
            ));
        }

        // =====================================================================
        // Property: IPv6 transit IPs are preserved in output
        // =====================================================================

        #[test]
        fn ipv6_transit_ips_preserved(
            ip in any::<std::net::Ipv6Addr>(),
            transit in proptest::collection::vec(arb_ipv6_net(), 0..4),
        ) {
            let transit_ipnet: Vec<IpNet> =
                transit.iter().copied().map(Into::into).collect();
            let nic = make_nic(Some(ip.into()), transit_ipnet);
            let result: v2026011300::InstanceNetworkInterfaceCreate =
                nic.try_into().unwrap();

            prop_assert!(matches!(
                result.ip_config,
                PrivateIpStackCreate::V6(v6) if v6.transit_ips == transit
            ));
        }

        // =====================================================================
        // Property: Mixed IPv4/IPv6 transit IPs always fail
        // =====================================================================

        #[test]
        fn mixed_transit_ip_families_fail(
            v4_transit in proptest::collection::vec(arb_ipv4_net(), 1..3),
            v6_transit in proptest::collection::vec(arb_ipv6_net(), 1..3),
        ) {
            let mut transit: Vec<IpNet> =
                v4_transit.into_iter().map(IpNet::from).collect();
            transit.extend(v6_transit.into_iter().map(IpNet::from));

            let nic = make_nic(None, transit);
            let result: Result<v2026011300::InstanceNetworkInterfaceCreate, _> =
                nic.try_into();
            prop_assert!(result.is_err());
        }

        // =====================================================================
        // Property: IPv4 address with IPv6 transit IPs fails
        // =====================================================================

        #[test]
        fn ipv4_with_ipv6_transit_fails(
            ip in any::<std::net::Ipv4Addr>(),
            v6_transit in proptest::collection::vec(arb_ipv6_net(), 1..3),
        ) {
            let transit: Vec<IpNet> =
                v6_transit.into_iter().map(Into::into).collect();
            let nic = make_nic(Some(ip.into()), transit);
            let result: Result<v2026011300::InstanceNetworkInterfaceCreate, _> =
                nic.try_into();
            prop_assert!(result.is_err());
        }

        // =====================================================================
        // Property: IPv6 address with IPv4 transit IPs fails
        // =====================================================================

        #[test]
        fn ipv6_with_ipv4_transit_fails(
            ip in any::<std::net::Ipv6Addr>(),
            v4_transit in proptest::collection::vec(arb_ipv4_net(), 1..3),
        ) {
            let transit: Vec<IpNet> =
                v4_transit.into_iter().map(Into::into).collect();
            let nic = make_nic(Some(ip.into()), transit);
            let result: Result<v2026011300::InstanceNetworkInterfaceCreate, _> =
                nic.try_into();
            prop_assert!(result.is_err());
        }

        // =====================================================================
        // Property: No IP + IPv4-only transit produces V4 stack with Auto IP
        // =====================================================================

        #[test]
        fn no_ip_with_ipv4_transit_produces_v4_auto(
            v4_transit in proptest::collection::vec(arb_ipv4_net(), 1..4),
        ) {
            let transit: Vec<IpNet> =
                v4_transit.iter().copied().map(Into::into).collect();
            let nic = make_nic(None, transit);
            let result: v2026011300::InstanceNetworkInterfaceCreate =
                nic.try_into().unwrap();

            prop_assert!(matches!(
                result.ip_config,
                PrivateIpStackCreate::V4(v4)
                    if matches!(v4.ip, IpAssignment::Auto) && v4.transit_ips == v4_transit
            ));
        }

        // =====================================================================
        // Property: No IP + IPv6-only transit produces V6 stack with Auto IP
        // =====================================================================

        #[test]
        fn no_ip_with_ipv6_transit_produces_v6_auto(
            v6_transit in proptest::collection::vec(arb_ipv6_net(), 1..4),
        ) {
            let transit: Vec<IpNet> =
                v6_transit.iter().copied().map(Into::into).collect();
            let nic = make_nic(None, transit);
            let result: v2026011300::InstanceNetworkInterfaceCreate =
                nic.try_into().unwrap();

            prop_assert!(matches!(
                result.ip_config,
                PrivateIpStackCreate::V6(v6)
                    if matches!(v6.ip, IpAssignment::Auto) && v6.transit_ips == v6_transit
            ));
        }
    }
}
