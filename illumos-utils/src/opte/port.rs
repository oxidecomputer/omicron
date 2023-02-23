// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A single port on the OPTE virtual switch.

use crate::opte::Gateway;
use crate::opte::Vni;
use macaddr::MacAddr6;
use std::net::IpAddr;
use std::sync::Arc;

#[derive(Copy, Clone, Debug)]
pub enum PortType {
    /// Port attached to a guest instance.
    Guest {
        /// vNIC slot for the guest connected to this port.
        slot: u8,
    },
    /// Port created for a service zone.
    Service,
}

#[derive(Debug)]
#[cfg_attr(not(target_os = "illumos"), allow(dead_code))]
struct PortInner {
    // Name of the port as identified by OPTE
    name: String,
    // Port type (e.g. attached to a guest or part of a service zone)
    port_type: PortType,
    // IP address within the VPC Subnet
    ip: IpAddr,
    // VPC-private MAC address
    mac: MacAddr6,
    // Geneve VNI for the VPC
    vni: Vni,
    // Information about the virtual gateway, aka OPTE
    gateway: Gateway,
    // TODO-correctness: Remove this once we can put Viona directly on top of an
    // OPTE port device.
    //
    // NOTE: This is intentionally not an actual `Vnic` object. We'd like to
    // delete the VNIC manually in `PortInner::drop`, because we _can't_ delete
    // the xde device if we fail to delete the VNIC. See
    // https://github.com/oxidecomputer/opte/issues/178 for more details. This
    // can be changed back to a real VNIC when that is resolved, and the Drop
    // impl below can simplify to just call `drop(self.vnic)`.
    vnic: String,
    // TODO-remove: This is part of the external IP hack.
    externally_visible: bool,
}

#[cfg(target_os = "illumos")]
impl Drop for PortInner {
    fn drop(&mut self) {
        use crate::dladm::Dladm;
        if let Err(e) = Dladm::delete_vnic(&self.vnic) {
            eprintln!(
                "WARNING: Failed to delete OPTE port overlay VNIC \
                while dropping port. The VNIC will not be cleaned up \
                properly, and the xde device itself will not be deleted. \
                Both the VNIC and the xde device must be deleted out \
                of band, and it will not be possible to recreate the xde \
                device until then. Error: {:?}",
                e
            );
            return;
        }
        let err = match opte_ioctl::OpteHdl::open(opte_ioctl::OpteHdl::XDE_CTL)
        {
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
            "WARNING: OPTE port overlay VNIC deleted, but failed \
            to delete the xde device. It must be deleted out \
            of band, and it will not be possible to recreate the xde \
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        port_type: PortType,
        ip: IpAddr,
        mac: MacAddr6,
        vni: Vni,
        gateway: Gateway,
        vnic: String,
        externally_visible: bool,
    ) -> Self {
        Self {
            inner: Arc::new(PortInner {
                name,
                port_type,
                ip,
                mac,
                vni,
                gateway,
                vnic,
                externally_visible,
            }),
        }
    }

    pub fn ip(&self) -> IpAddr {
        self.inner.ip
    }

    pub fn mac(&self) -> MacAddr6 {
        self.inner.mac
    }

    pub fn vni(&self) -> Vni {
        self.inner.vni
    }

    pub fn gateway(&self) -> Gateway {
        self.inner.gateway
    }

    pub fn vnic_name(&self) -> &str {
        &self.inner.vnic
    }

    pub fn slot(&self) -> Option<u8> {
        match self.inner.port_type {
            PortType::Guest { slot } => Some(slot),
            PortType::Service => None,
        }
    }

    pub fn externally_visible(&self) -> bool {
        self.inner.externally_visible
    }
}
