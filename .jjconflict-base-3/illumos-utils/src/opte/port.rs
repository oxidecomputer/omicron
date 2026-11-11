// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A single port on the OPTE virtual switch.

use crate::opte::Gateway;
use crate::opte::Handle;
use crate::opte::PortManager;
use crate::opte::Vni;
use macaddr::MacAddr6;
use omicron_common::api::external;
use omicron_common::api::internal::shared::PrivateIpConfig;
use omicron_common::api::internal::shared::RouterId;
use omicron_common::api::internal::shared::RouterKind;
use oxnet::Ipv4Net;
use oxnet::Ipv6Net;
use sled_agent_types::inventory::NetworkInterfaceKind;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use uuid::Uuid;

/// Data about an OPTE port.
#[derive(Clone, Debug)]
pub struct PortData {
    /// Identifying information about a port.
    info: PortInfo,
    /// The VPC-private IP configuration for the port.
    ip: PrivateIpConfig,
    /// VPC-private MAC address
    mac: MacAddr6,
    /// Emulated PCI slot for the guest NIC, passed to Propolis
    slot: u8,
    /// Geneve VNI for the VPC
    vni: Vni,
    /// Information about the virtual gateway, aka OPTE
    gateway: Gateway,
}

impl PortData {
    /// Return the VPC-private IPv4 address, if it exists.
    pub fn ipv4_addr(&self) -> Option<&Ipv4Addr> {
        self.ip.ipv4_addr()
    }

    /// Return the VPC-private IPv6 address, if it exists.
    pub fn ipv6_addr(&self) -> Option<&Ipv6Addr> {
        self.ip.ipv6_addr()
    }

    pub fn name(&self) -> &str {
        &self.info.name
    }

    /// Return the OPTE gateway IPv4 address and the private IPv4 address.
    ///
    /// If the port is not configured for IPv4, None is returned.
    // TODO-remove: <https://github.com/oxidecomputer/omicron/issues/2931>
    pub fn gateway_and_private_ipv4(&self) -> Option<(&Ipv4Addr, &Ipv4Addr)> {
        match (self.gateway.ipv4_addr(), self.ipv4_addr()) {
            (None, None) => None,
            (None, Some(_)) => unreachable!(),
            (Some(_), None) => unreachable!(),
            (Some(gateway_ip), Some(private_ip)) => {
                Some((gateway_ip, private_ip))
            }
        }
    }

    #[allow(dead_code)]
    pub fn mac(&self) -> &MacAddr6 {
        &self.mac
    }

    pub fn vni(&self) -> &Vni {
        &self.vni
    }

    /// Return the VPC-private IPv4 subnet, if it exists.
    pub fn ipv4_subnet(&self) -> Option<&Ipv4Net> {
        self.ip.ipv4_subnet()
    }

    /// Return the VPC-private IPv6 subnet, if it exists.
    pub fn ipv6_subnet(&self) -> Option<&Ipv6Net> {
        self.ip.ipv6_subnet()
    }

    pub fn slot(&self) -> u8 {
        self.slot
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

/// A port on the OPTE virtual switch, providing the virtual networking
/// abstractions for guest instances.
pub struct Port {
    data: PortData,
    manager: PortManager,
}

impl core::ops::Deref for Port {
    type Target = PortData;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl core::fmt::Debug for Port {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Port")
            .field("data", &self.data)
            .field("manager", &"{ .. }")
            .finish()
    }
}

/// Attempt to delete the named XDE device.
///
/// This currently doesn't return an error because all the places it's called
/// can't do anything other than log it anyway.
pub(super) fn delete_xde_device(log: &Logger, name: &str) {
    let err = match Handle::new() {
        Ok(hdl) => {
            if let Err(e) = hdl.delete_xde(name) {
                e
            } else {
                return;
            }
        }
        Err(e) => e,
    };
    slog::warn!(
        log,
        "Failed to delete the xde device. It must be deleted \
        out of band, and it will not be possible to recreate the xde \
        device until then.";
        InlineErrorChain::new(&err),
    );
}

impl Drop for Port {
    fn drop(&mut self) {
        self.manager
            .remove_port(self.data.info.nic_id, self.data.info.nic_kind);
        delete_xde_device(self.manager.log(), self.name());
    }
}

impl Port {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        nic_id: Uuid,
        nic_kind: NetworkInterfaceKind,
        ip: PrivateIpConfig,
        mac: MacAddr6,
        slot: u8,
        vni: Vni,
        manager: PortManager,
    ) -> Self {
        let gateway = Gateway::from_ip_config(&ip);
        let info = PortInfo { name, nic_id, nic_kind };
        let data = PortData { info, ip, mac, slot, vni, gateway };
        Self { data, manager }
    }

    /// Return the general data about the OPTE port.
    pub(super) fn data(&self) -> &PortData {
        &self.data
    }

    /// Return the identifying information about the OPTE port.
    pub(crate) fn info(&self) -> &PortInfo {
        &self.data.info
    }
}

/// Identifying information for an OPTE port.
#[derive(Clone, Debug)]
pub struct PortInfo {
    pub name: String,
    pub nic_id: Uuid,
    pub nic_kind: NetworkInterfaceKind,
}
