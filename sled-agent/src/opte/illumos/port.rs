// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A single port on the OPTE virtual switch.

use crate::illumos::dladm::Dladm;
use crate::opte::BoundaryServices;
use crate::opte::Gateway;
use crate::opte::Vni;
use crate::params::SourceNatConfig;
use ipnetwork::IpNetwork;
use macaddr::MacAddr6;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::sync::Arc;

#[derive(Debug)]
struct PortInner {
    // Name of the port as identified by OPTE
    name: String,
    // IP address within the VPC Subnet
    _ip: IpAddr,
    // VPC Subnet
    _subnet: IpNetwork,
    // VPC-private MAC address
    mac: MacAddr6,
    // Emulated PCI slot for the guest NIC, passed to Propolis
    slot: u8,
    // Geneve VNI for the VPC
    vni: Vni,
    // IP address of the hosting sled
    _underlay_ip: Ipv6Addr,
    // The external IP address and port range provided for this port, to allow
    // outbound network connectivity.
    _source_nat: Option<SourceNatConfig>,
    // The external IP addresses provided to this port, to allow _inbound_
    // network connectivity.
    external_ips: Option<Vec<IpAddr>>,
    // Information about the virtual gateway, aka OPTE
    _gateway: Gateway,
    // Information about Boundary Services, for forwarding traffic between sleds
    // or off the rack.
    _boundary_services: BoundaryServices,
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
}

impl Drop for PortInner {
    fn drop(&mut self) {
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
        ip: IpAddr,
        subnet: IpNetwork,
        mac: MacAddr6,
        slot: u8,
        vni: Vni,
        underlay_ip: Ipv6Addr,
        source_nat: Option<SourceNatConfig>,
        external_ips: Option<Vec<IpAddr>>,
        gateway: Gateway,
        boundary_services: BoundaryServices,
        vnic: String,
    ) -> Self {
        Self {
            inner: Arc::new(PortInner {
                name,
                _ip: ip,
                _subnet: subnet,
                mac,
                slot,
                vni: vni,
                _underlay_ip: underlay_ip,
                _source_nat: source_nat,
                external_ips,
                _gateway: gateway,
                _boundary_services: boundary_services,
                vnic,
            }),
        }
    }

    pub fn external_ips(&self) -> &Option<Vec<IpAddr>> {
        &self.inner.external_ips
    }

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
