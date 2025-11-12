// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A single port on the OPTE virtual switch.

use crate::opte::Gateway;
use crate::opte::Handle;
use crate::opte::Vni;
use macaddr::MacAddr6;
use omicron_common::api::external;
use omicron_common::api::internal::shared::RouterId;
use omicron_common::api::internal::shared::RouterKind;
use oxnet::IpNet;
use std::net::IpAddr;
use std::sync::Arc;

#[derive(Debug)]
pub struct PortData {
    /// Name of the port as identified by OPTE
    pub(crate) name: String,
    /// IP address within the VPC Subnet
    pub(crate) ip: IpAddr,
    /// VPC-private MAC address
    pub(crate) mac: MacAddr6,
    /// Emulated PCI slot for the guest NIC, passed to Propolis
    pub(crate) slot: u8,
    /// Geneve VNI for the VPC
    pub(crate) vni: Vni,
    /// Subnet the port belong to within the VPC.
    pub(crate) subnet: IpNet,
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

    pub fn ip(&self) -> &IpAddr {
        &self.inner.ip
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

    pub fn subnet(&self) -> &IpNet {
        &self.inner.subnet
    }

    pub fn slot(&self) -> u8 {
        self.inner.slot
    }

    pub fn system_router_key(&self) -> RouterId {
        // Unwrap safety: both of these VNI types represent validated u24s.
        let vni = external::Vni::try_from(self.vni().as_u32()).unwrap();
        RouterId { vni, kind: RouterKind::System }
    }

    pub fn custom_router_key(&self) -> RouterId {
        RouterId {
            kind: RouterKind::Custom(*self.subnet()),
            ..self.system_router_key()
        }
    }
}
