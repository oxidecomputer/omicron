// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types from API version 2026010100 that cannot live in `nexus-types-versions`
//! because they convert to/from `omicron-common` types (orphan rule).

use itertools::Either;
use itertools::Itertools as _;

use nexus_types::external_api::instance;
use nexus_types::external_api::instance::IpAssignment;
use nexus_types::external_api::instance::PrivateIpStackCreate;
use nexus_types::external_api::instance::PrivateIpv4StackCreate;
use nexus_types::external_api::instance::PrivateIpv6StackCreate;
use omicron_common::api::external;
use omicron_common::api::external::IdentityMetadata;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::MacAddr;
use omicron_common::api::external::Name;
use omicron_common::api::external::PrivateIpStack;
use omicron_common::api::external::PrivateIpv4Stack;
use omicron_common::api::external::PrivateIpv6Stack;
use oxnet::IpNet;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::net::IpAddr;
use uuid::Uuid;

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

impl TryFrom<InstanceNetworkInterfaceAttachment>
    for instance::InstanceNetworkInterfaceAttachment
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
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
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
    for instance::InstanceNetworkInterfaceCreate
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
