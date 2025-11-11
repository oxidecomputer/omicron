// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Network interface types version 2

use crate::api::external;
use crate::api::external::Name;
use crate::api::external::Vni;
use crate::api::internal::shared::network_interface::NetworkInterfaceKind;
use daft::Diffable;
use oxnet::IpNet;
use oxnet::Ipv4Net;
use oxnet::Ipv6Net;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use uuid::Uuid;

/// Information required to construct a virtual network interface
#[derive(
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Diffable,
)]
pub struct NetworkInterface {
    pub id: Uuid,
    pub kind: NetworkInterfaceKind,
    pub name: Name,
    pub ip_config: PrivateIpConfig,
    pub mac: external::MacAddr,
    pub vni: Vni,
    pub primary: bool,
    pub slot: u8,
}

impl TryFrom<super::v1::NetworkInterface> for NetworkInterface {
    type Error = external::Error;

    fn try_from(
        value: super::v1::NetworkInterface,
    ) -> Result<Self, Self::Error> {
        let super::v1::NetworkInterface {
            id,
            kind,
            name,
            ip,
            mac,
            subnet,
            vni,
            primary,
            slot,
            transit_ips,
        } = value;
        let ip_config = match (ip, subnet) {
            (IpAddr::V4(ip), IpNet::V4(subnet)) => {
                let transit_ips = transit_ips
                    .into_iter()
                    .map(|net| {
                        let IpNet::V4(subnet) = net else {
                            return Err(external::Error::invalid_request(
                                "Expected an IPv4 transit IP subnet, but found IPv6",
                            ));
                        };
                        Ok(subnet)
                    })
                    .collect::<Result<_, _>>()?;
                PrivateIpConfig::V4(PrivateIpv4Config::new_with_transit_ips(
                    ip,
                    subnet,
                    transit_ips,
                )?)
            }
            (IpAddr::V6(ip), IpNet::V6(subnet)) => {
                let transit_ips = transit_ips
                    .into_iter()
                    .map(|net| {
                        let IpNet::V6(subnet) = net else {
                            return Err(external::Error::invalid_request(
                                "Expected an IPv6 transit IP subnet, but found IPv4",
                            ));
                        };
                        Ok(subnet)
                    })
                    .collect::<Result<_, _>>()?;
                PrivateIpConfig::V6(PrivateIpv6Config::new_with_transit_ips(
                    ip,
                    subnet,
                    transit_ips,
                )?)
            }
            (IpAddr::V4(_), IpNet::V6(_)) | (IpAddr::V6(_), IpNet::V4(_)) => {
                return Err(external::Error::invalid_request(
                    "IP address and subnet must have the same IP version",
                ));
            }
        };
        Ok(Self { id, kind, name, ip_config, mac, vni, primary, slot })
    }
}

impl TryFrom<NetworkInterface> for super::v1::NetworkInterface {
    type Error = external::Error;

    fn try_from(value: NetworkInterface) -> Result<Self, Self::Error> {
        let NetworkInterface {
            id,
            kind,
            name,
            ip_config: ip,
            mac,
            vni,
            primary,
            slot,
        } = value;
        let (ip, subnet, transit_ips) = match ip {
            PrivateIpConfig::V4(v4) => (
                IpAddr::V4(v4.ip),
                IpNet::V4(v4.subnet),
                v4.transit_ips.into_iter().map(IpNet::V4).collect(),
            ),
            PrivateIpConfig::V6(v6) => (
                IpAddr::V6(v6.ip),
                IpNet::V6(v6.subnet),
                v6.transit_ips.into_iter().map(IpNet::V6).collect(),
            ),
            PrivateIpConfig::DualStack { .. } => {
                return Err(external::Error::invalid_request(
                    "Cannot convert dual-stack v2 NetworkInterface to v1",
                ));
            }
        };
        Ok(Self {
            id,
            kind,
            name,
            ip,
            mac,
            subnet,
            vni,
            primary,
            slot,
            transit_ips,
        })
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum PrivateIpConfigError {
    #[error("IP subnet {subnet} does not contain the requested addres {ip}")]
    IpNotInSubnet { subnet: IpNet, ip: IpAddr },
}

impl From<PrivateIpConfigError> for external::Error {
    fn from(e: PrivateIpConfigError) -> external::Error {
        match e {
            PrivateIpConfigError::IpNotInSubnet { .. } => {
                external::Error::invalid_request(e.to_string())
            }
        }
    }
}

/// VPC-private IP address configuration for a network interface.
#[derive(
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Diffable,
)]
#[serde(rename_all = "snake_case")]
pub enum PrivateIpConfig {
    /// The interface has only an IPv4 configuration.
    V4(PrivateIpv4Config),
    /// The interface has only an IPv6 configuration.
    V6(PrivateIpv6Config),
    /// The interface is dual-stack.
    DualStack {
        /// The interface's IPv4 configuration.
        v4: PrivateIpv4Config,
        /// The interface's IPv6 configuration.
        v6: PrivateIpv6Config,
    },
}

impl PrivateIpConfig {
    /// Construct an IPv4 IP configuration, with no transit IPs.
    ///
    /// An error is returned if the IP address is not within the subnet.
    pub fn new_ipv4(
        addr: Ipv4Addr,
        subnet: Ipv4Net,
    ) -> Result<Self, PrivateIpConfigError> {
        PrivateIpv4Config::new(addr, subnet).map(PrivateIpConfig::V4)
    }

    /// Construct an IPv6 IP configuration, with no transit IPs.
    ///
    /// An error is returned if the IP address is not within the subnet.
    pub fn new_ipv6(
        addr: Ipv6Addr,
        subnet: Ipv6Net,
    ) -> Result<Self, PrivateIpConfigError> {
        PrivateIpv6Config::new(addr, subnet).map(PrivateIpConfig::V6)
    }

    /// Return the IPv4 configuration, if one exists.
    pub fn ipv4_config(&self) -> Option<&PrivateIpv4Config> {
        match &self {
            PrivateIpConfig::V4(v4) | PrivateIpConfig::DualStack { v4, .. } => {
                Some(v4)
            }
            PrivateIpConfig::V6(_) => None,
        }
    }

    /// Return the IPv6 configuration, if one exists.
    pub fn ipv6_config(&self) -> Option<&PrivateIpv6Config> {
        match &self {
            PrivateIpConfig::V6(v6) | PrivateIpConfig::DualStack { v6, .. } => {
                Some(v6)
            }
            PrivateIpConfig::V4(_) => None,
        }
    }

    /// Return the IPv4 address for this configuration, if it exists.
    pub fn ipv4_addr(&self) -> Option<&Ipv4Addr> {
        self.ipv4_config().map(PrivateIpv4Config::ip)
    }

    /// Return the IPv6 address for this configuration, if it exists.
    pub fn ipv6_addr(&self) -> Option<&Ipv6Addr> {
        self.ipv6_config().map(PrivateIpv6Config::ip)
    }

    /// Return the IPv4 subnet for this configuration, if it exists.
    pub fn ipv4_subnet(&self) -> Option<&Ipv4Net> {
        self.ipv4_config().map(PrivateIpv4Config::subnet)
    }

    /// Return the IPv6 subnet for this configuration, if it exists.
    pub fn ipv6_subnet(&self) -> Option<&Ipv6Net> {
        self.ipv6_config().map(PrivateIpv6Config::subnet)
    }

    /// Return the IPv4 transit IPs, if they exist.
    pub fn ipv4_transit_ips(&self) -> Option<&[Ipv4Net]> {
        self.ipv4_config().map(|c| c.transit_ips.as_slice())
    }

    /// Return the IPv6 transit IPs, if they exist.
    pub fn ipv6_transit_ips(&self) -> Option<&[Ipv6Net]> {
        self.ipv6_config().map(|c| c.transit_ips.as_slice())
    }

    /// Return all transit IPs, of any IP version.
    pub fn all_transit_ips(&self) -> impl Iterator<Item = IpNet> + '_ {
        let v4 = self
            .ipv4_transit_ips()
            .into_iter()
            .flatten()
            .copied()
            .map(Into::into);
        let v6 = self
            .ipv6_transit_ips()
            .into_iter()
            .flatten()
            .copied()
            .map(Into::into);
        v4.chain(v6)
    }

    /// Return true if this is an IPv4-only configuration.
    pub fn is_ipv4_only(&self) -> bool {
        matches!(self, PrivateIpConfig::V4(_))
    }

    /// Return true if this is an IPv6-only configuration.
    pub fn is_ipv6_only(&self) -> bool {
        matches!(self, PrivateIpConfig::V6(_))
    }

    /// Return true if this is a dual-stack configuration.
    pub fn is_dual_stack(&self) -> bool {
        matches!(self, PrivateIpConfig::DualStack { .. })
    }

    /// Return true if this configuration has the provided IP address.
    pub fn has_addr(&self, ip: &IpAddr) -> bool {
        match ip {
            IpAddr::V4(ipv4) => {
                self.ipv4_addr().map(|ip| ip == ipv4).unwrap_or(false)
            }
            IpAddr::V6(ipv6) => {
                self.ipv6_addr().map(|ip| ip == ipv6).unwrap_or(false)
            }
        }
    }
}

/// VPC-private IPv4 configuration for a network interface.
#[derive(
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Diffable,
)]
pub struct PrivateIpv4Config {
    /// VPC-private IP address.
    ip: Ipv4Addr,
    /// The IP subnet.
    subnet: Ipv4Net,
    /// Additional networks on which the interface can send / receive traffic.
    #[serde(default)]
    pub transit_ips: Vec<Ipv4Net>,
}

impl PrivateIpv4Config {
    /// Construct a new IPv4 configuration.
    ///
    /// This fails if the provided address is not within the subnet.
    pub fn new(
        ip: Ipv4Addr,
        subnet: Ipv4Net,
    ) -> Result<Self, PrivateIpConfigError> {
        Self::new_with_transit_ips(ip, subnet, vec![])
    }

    /// Construct a new IPv4 configuration, with transit IPs.
    ///
    /// This fails if the provided address is not within the subnet.
    pub fn new_with_transit_ips(
        ip: Ipv4Addr,
        subnet: Ipv4Net,
        transit_ips: Vec<Ipv4Net>,
    ) -> Result<Self, PrivateIpConfigError> {
        if subnet.contains(ip) {
            Ok(Self { ip, subnet, transit_ips })
        } else {
            Err(PrivateIpConfigError::IpNotInSubnet {
                subnet: subnet.into(),
                ip: ip.into(),
            })
        }
    }

    /// Return the IPv4 address.
    pub fn ip(&self) -> &Ipv4Addr {
        &self.ip
    }

    /// Return the IPv4 subnet.
    pub fn subnet(&self) -> &Ipv4Net {
        &self.subnet
    }
}

/// VPC-private IPv6 configuration for a network interface.
#[derive(
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Diffable,
)]
pub struct PrivateIpv6Config {
    /// VPC-private IP address.
    ip: Ipv6Addr,
    /// The IP subnet.
    subnet: Ipv6Net,
    /// Additional networks on which the interface can send / receive traffic.
    pub transit_ips: Vec<Ipv6Net>,
}

impl PrivateIpv6Config {
    /// Construct a new IPv6 configuration with no transit IPs.
    ///
    /// This fails if the provided address is not within the subnet.
    pub fn new(
        ip: Ipv6Addr,
        subnet: Ipv6Net,
    ) -> Result<Self, PrivateIpConfigError> {
        Self::new_with_transit_ips(ip, subnet, vec![])
    }

    /// Construct a new IPv4 configuration, with transit IPs.
    ///
    /// This fails if the provided address is not within the subnet.
    pub fn new_with_transit_ips(
        ip: Ipv6Addr,
        subnet: Ipv6Net,
        transit_ips: Vec<Ipv6Net>,
    ) -> Result<Self, PrivateIpConfigError> {
        if subnet.contains(ip) {
            Ok(Self { ip, subnet, transit_ips })
        } else {
            Err(PrivateIpConfigError::IpNotInSubnet {
                subnet: subnet.into(),
                ip: ip.into(),
            })
        }
    }

    /// Return the IPv6 address.
    pub fn ip(&self) -> &Ipv6Addr {
        &self.ip
    }

    /// Return the IPv6 subnet.
    pub fn subnet(&self) -> &Ipv6Net {
        &self.subnet
    }
}
