// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for instance-related types.

use crate::latest::instance::{
    InstanceDiskAttach, InstanceDiskAttachment, IpAssignment, Ipv4Assignment,
    Ipv6Assignment, PrivateIpStackCreate, PrivateIpv4StackCreate,
    PrivateIpv6StackCreate,
};
use omicron_common::api::external::Name;
use oxnet::IpNet;
use oxnet::Ipv4Net;
use oxnet::Ipv6Net;

impl InstanceDiskAttachment {
    /// Get the name of the disk described by this attachment.
    pub fn name(&self) -> Name {
        match self {
            Self::Create(create) => create.identity.name.clone(),
            Self::Attach(InstanceDiskAttach { name }) => name.clone(),
        }
    }
}

impl PrivateIpStackCreate {
    /// Construct an IPv4 configuration with no transit IPs.
    pub fn from_ipv4(addr: std::net::Ipv4Addr) -> Self {
        PrivateIpStackCreate::V4(PrivateIpv4StackCreate {
            ip: Ipv4Assignment::Explicit(addr),
            transit_ips: vec![],
        })
    }

    /// Return the IPv4 create parameters, if they exist.
    pub fn as_ipv4_create(&self) -> Option<&PrivateIpv4StackCreate> {
        match self {
            PrivateIpStackCreate::V4(v4)
            | PrivateIpStackCreate::DualStack { v4, .. } => Some(v4),
            PrivateIpStackCreate::V6(_) => None,
        }
    }

    /// Return the IPv4 address assignment.
    pub fn ipv4_assignment(&self) -> Option<&Ipv4Assignment> {
        self.as_ipv4_create().map(|c| &c.ip)
    }

    /// Return the IPv4 address explicitly requested, if one exists.
    pub fn ipv4_addr(&self) -> Option<&std::net::Ipv4Addr> {
        self.ipv4_assignment().and_then(|assignment| match assignment {
            IpAssignment::Auto => None,
            IpAssignment::Explicit(addr) => Some(addr),
        })
    }

    /// Return the IPv4 transit IPs, if they exist.
    pub fn ipv4_transit_ips(&self) -> Option<&[Ipv4Net]> {
        self.as_ipv4_create().map(|c| c.transit_ips.as_slice())
    }

    /// Construct an IPv6 configuration with no transit IPs.
    pub fn from_ipv6(addr: std::net::Ipv6Addr) -> Self {
        PrivateIpStackCreate::V6(PrivateIpv6StackCreate {
            ip: Ipv6Assignment::Explicit(addr),
            transit_ips: vec![],
        })
    }

    /// Construct an IP configuration with only an automatic IPv6 address.
    pub fn auto_ipv6() -> Self {
        PrivateIpStackCreate::V6(PrivateIpv6StackCreate::default())
    }

    /// Return the IPv6 create parameters, if they exist.
    pub fn as_ipv6_create(&self) -> Option<&PrivateIpv6StackCreate> {
        match self {
            PrivateIpStackCreate::V6(v6)
            | PrivateIpStackCreate::DualStack { v6, .. } => Some(v6),
            PrivateIpStackCreate::V4(_) => None,
        }
    }

    /// Return the IPv6 address assignment.
    pub fn ipv6_assignment(&self) -> Option<&Ipv6Assignment> {
        self.as_ipv6_create().map(|c| &c.ip)
    }

    /// Return the IPv6 address explicitly requested, if one exists.
    pub fn ipv6_addr(&self) -> Option<&std::net::Ipv6Addr> {
        self.ipv6_assignment().and_then(|assignment| match assignment {
            IpAssignment::Auto => None,
            IpAssignment::Explicit(addr) => Some(addr),
        })
    }

    /// Return the IPv6 transit IPs, if they exist.
    pub fn ipv6_transit_ips(&self) -> Option<&[Ipv6Net]> {
        self.as_ipv6_create().map(|c| c.transit_ips.as_slice())
    }

    /// Return the transit IPs requested in this configuration.
    pub fn transit_ips(&self) -> Vec<IpNet> {
        self.ipv4_transit_ips()
            .unwrap_or_default()
            .iter()
            .copied()
            .map(Into::into)
            .chain(
                self.ipv6_transit_ips()
                    .unwrap_or_default()
                    .iter()
                    .copied()
                    .map(Into::into),
            )
            .collect()
    }

    /// Construct a dual-stack IP configuration with explicit IP addresses.
    pub fn new_dual_stack(
        ipv4: std::net::Ipv4Addr,
        ipv6: std::net::Ipv6Addr,
    ) -> Self {
        PrivateIpStackCreate::DualStack {
            v4: PrivateIpv4StackCreate {
                ip: Ipv4Assignment::Explicit(ipv4),
                transit_ips: Vec::new(),
            },
            v6: PrivateIpv6StackCreate {
                ip: Ipv6Assignment::Explicit(ipv6),
                transit_ips: Vec::new(),
            },
        }
    }

    /// Return true if this config has any transit IPs.
    pub fn has_transit_ips(&self) -> bool {
        match self {
            PrivateIpStackCreate::V4(PrivateIpv4StackCreate {
                transit_ips,
                ..
            }) => !transit_ips.is_empty(),
            PrivateIpStackCreate::V6(PrivateIpv6StackCreate {
                transit_ips,
                ..
            }) => !transit_ips.is_empty(),
            PrivateIpStackCreate::DualStack {
                v4: PrivateIpv4StackCreate { transit_ips: ipv4_addrs, .. },
                v6: PrivateIpv6StackCreate { transit_ips: ipv6_addrs, .. },
            } => !ipv4_addrs.is_empty() || !ipv6_addrs.is_empty(),
        }
    }

    /// Return true if this IP configuration has an IPv4 stack.
    pub fn has_ipv4_stack(&self) -> bool {
        self.ipv4_assignment().is_some()
    }

    /// Return true if this IP configuration has an IPv6 stack.
    pub fn has_ipv6_stack(&self) -> bool {
        self.ipv6_assignment().is_some()
    }
}
