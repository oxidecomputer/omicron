// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A single port on the OPTE virtual switch.

use crate::opte::Gateway;
use crate::opte::Handle;
use crate::opte::Vni;
use macaddr::MacAddr6;
use omicron_common::api::external;
use omicron_common::api::internal::shared::PrivateIpConfig;
use omicron_common::api::internal::shared::RouterId;
use omicron_common::api::internal::shared::RouterKind;
use oxnet::Ipv4Net;
use oxnet::Ipv6Net;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::sync::Arc;

#[derive(Debug)]
pub struct PortData {
    /// Name of the port as identified by OPTE
    pub(crate) name: String,
    /// The VPC-private IP configuration for the port.
    pub(crate) ip: PrivateIpConfig,
    /// VPC-private MAC address
    pub(crate) mac: MacAddr6,
    /// Emulated PCI slot for the guest NIC, passed to Propolis
    pub(crate) slot: u8,
    /// Geneve VNI for the VPC
    pub(crate) vni: Vni,
    /// Information about the virtual gateway, aka OPTE
    pub(crate) gateway: Gateway,
}

#[derive(Debug)]
struct PortInner(PortData);

impl core::ops::Deref for PortInner {
    type Target = PortData;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for PortInner {
    fn drop(&mut self) {
        let err = match Handle::new() {
            Ok(hdl) => {
                if let Err(e) = hdl.delete_xde(&self.name) {
                    e
                } else {
                    return;
                }
            }
            Err(e) => e,
        };
        eprintln!(
            "WARNING: Failed to delete the xde device. It must be deleted \
            out of band, and it will not be possible to recreate the xde \
            device until then. Error: {:?}",
            err,
        );
    }
}

/// A port on the OPTE virtual switch, providing the virtual networking
/// abstractions for guest instances.
///
/// Note that the type is clonable and refers to the same underlying port on the
/// system.
#[derive(Debug, Clone)]
pub struct Port {
    inner: Arc<PortInner>,
}

impl Port {
    pub fn new(data: PortData) -> Self {
        Self { inner: Arc::new(PortInner(data)) }
    }

    /// Return the VPC-private IPv4 address, if it exists.
    pub fn ipv4_addr(&self) -> Option<&Ipv4Addr> {
        self.inner.ip.ipv4_addr()
    }

    /// Return the VPC-private IPv6 address, if it exists.
    pub fn ipv6_addr(&self) -> Option<&Ipv6Addr> {
        self.inner.ip.ipv6_addr()
    }

    /// Return the VPC-private IPv4 address, if it exits, or the IPv6 address.
    ///
    /// One of these always exists.
    pub fn ipv4_or_ipv6_addr(&self) -> IpAddr {
        self.inner.ip.ipv4_addr().copied().map(IpAddr::V4).unwrap_or_else(
            || {
                self.inner
                    .ip
                    .ipv6_addr()
                    .copied()
                    .expect("At least one address always exists")
                    .into()
            },
        )
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn gateway(&self) -> &Gateway {
        &self.inner.gateway
    }

    #[allow(dead_code)]
    pub fn mac(&self) -> &MacAddr6 {
        &self.inner.mac
    }

    pub fn vni(&self) -> &Vni {
        &self.inner.vni
    }

    /// Return the VPC-private IPv4 subnet, if it exists.
    pub fn ipv4_subnet(&self) -> Option<&Ipv4Net> {
        self.inner.ip.ipv4_subnet()
    }

    /// Return the VPC-private IPv6 subnet, if it exists.
    pub fn ipv6_subnet(&self) -> Option<&Ipv6Net> {
        self.inner.ip.ipv6_subnet()
    }

    pub fn slot(&self) -> u8 {
        self.inner.slot
    }

    pub fn system_router_key(&self) -> RouterId {
        // Unwrap safety: both of these VNI types represent validated u24s.
        let vni = external::Vni::try_from(self.vni().as_u32()).unwrap();
        RouterId { vni, kind: RouterKind::System }
    }

    pub fn custom_ipv4_router_key(&self) -> Option<RouterId> {
        self.ipv4_subnet().copied().map(|subnet| RouterId {
            kind: RouterKind::Custom(subnet.into()),
            ..self.system_router_key()
        })
    }

    pub fn custom_ipv6_router_key(&self) -> Option<RouterId> {
        self.ipv6_subnet().copied().map(|subnet| RouterId {
            kind: RouterKind::Custom(subnet.into()),
            ..self.system_router_key()
        })
    }
}
