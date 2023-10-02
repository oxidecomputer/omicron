// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A single port on the OPTE virtual switch.

use crate::opte::Gateway;
use crate::opte::Vni;
use debug_ignore::DebugIgnore;
use helios_fusion::BoxedExecutor;
use macaddr::MacAddr6;
use std::net::IpAddr;
use std::sync::Arc;

#[derive(Debug)]
struct PortInner {
    #[allow(dead_code)]
    executor: DebugIgnore<BoxedExecutor>,
    // Name of the port as identified by OPTE
    name: String,
    // IP address within the VPC Subnet
    _ip: IpAddr,
    // VPC-private MAC address
    mac: MacAddr6,
    // Emulated PCI slot for the guest NIC, passed to Propolis
    slot: u8,
    // Geneve VNI for the VPC
    vni: Vni,
    // Information about the virtual gateway, aka OPTE
    gateway: Gateway,
    // TODO-remove(#2932): Remove this once we can put Viona directly on top of an
    // OPTE port device.
    //
    // NOTE: This is intentionally not an actual `Vnic` object. We'd like to
    // delete the VNIC manually in `PortInner::drop`, because we _can't_ delete
    // the xde device if we fail to delete the VNIC. See
    // https://github.com/oxidecomputer/opte/issues/178 for more details. This
    // can be changed back to a real VNIC when that is resolved, and the Drop
    // impl below can simplify to just call `drop(self.vnic)`.
    vnic: String,
}

#[cfg(target_os = "illumos")]
impl Drop for PortInner {
    fn drop(&mut self) {
        if let Err(e) =
            crate::dladm::Dladm::delete_vnic(&self.executor, &self.vnic)
        {
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
        executor: &BoxedExecutor,
        name: String,
        ip: IpAddr,
        mac: MacAddr6,
        slot: u8,
        vni: Vni,
        gateway: Gateway,
        vnic: String,
    ) -> Self {
        Self {
            inner: Arc::new(PortInner {
                executor: DebugIgnore(executor.clone()),
                name,
                _ip: ip,
                mac,
                slot,
                vni,
                gateway,
                vnic,
            }),
        }
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

    pub fn vnic_name(&self) -> &str {
        &self.inner.vnic
    }

    pub fn slot(&self) -> u8 {
        self.inner.slot
    }
}
