// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for controlling a single instance.

use crate::common::vlan::VlanID;
use crate::illumos::dladm::{
    PhysicalLink, VNIC_PREFIX_CONTROL, VNIC_PREFIX_GUEST,
};
use omicron_common::api::external::MacAddr;
use std::net::IpAddr;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[cfg(not(test))]
use crate::illumos::dladm::Dladm;
#[cfg(test)]
use crate::illumos::dladm::MockDladm as Dladm;

type Error = crate::illumos::dladm::Error;

fn guest_vnic_name(id: u64) -> String {
    format!("{}{}", VNIC_PREFIX_GUEST, id)
}

pub fn control_vnic_name(id: u64) -> String {
    format!("{}{}", VNIC_PREFIX_CONTROL, id)
}

pub fn interface_name(vnic_name: &str) -> String {
    format!("{}/omicron", vnic_name)
}

/// A shareable wrapper around an atomic counter.
/// May be used to allocate runtime-unique IDs for objects
/// which have naming constraints - such as VNICs.
#[derive(Clone, Debug)]
pub struct IdAllocator {
    value: Arc<AtomicU64>,
}

impl IdAllocator {
    pub fn new() -> Self {
        Self { value: Arc::new(AtomicU64::new(0)) }
    }

    pub fn next(&self) -> u64 {
        self.value.fetch_add(1, Ordering::SeqCst)
    }
}

/// Represents an allocated VNIC on the system.
/// The VNIC is de-allocated when it goes out of scope.
///
/// Note that the "ownership" of the VNIC is based on convention;
/// another process in the global zone could also modify / destroy
/// the VNIC while this object is alive.
#[derive(Debug)]
pub struct Vnic {
    name: String,
    deleted: bool,
}

impl Vnic {
    /// Takes ownership of an existing VNIC.
    pub fn wrap_existing(name: String) -> Self {
        Vnic { name, deleted: false }
    }

    /// Creates a new NIC, intended for usage by the guest.
    pub fn new_guest(
        allocator: &IdAllocator,
        physical_dl: &PhysicalLink,
        mac: Option<MacAddr>,
        ip: IpAddr,
        vlan: Option<VlanID>,
    ) -> Result<Self, Error> {
        use opte_core::{ether, geneve, ip6};
        use opte_core::headers::{IpAddr, IpCidr};
        use opte_core::oxide_net::{overlay, router};

        let name = guest_vnic_name(allocator.next());
        Dladm::create_vnic(physical_dl, &name, mac, vlan)?;
        let ip4 = match ip {
            std::net::IpAddr::V4(v) => v,

            _ => {
                todo!("OPTE supports IPv4 guests only at the moment");
            }
        };
        let ip_cfg = opte_core::ioctl::IpCfg {
            // NOTE: OPTE has it's own Ipv4Addr, thus the into().
            private_ip: ip4.into(),
	    snat: None,
        };
        let req = opte_core::ioctl::AddPortReq {
            link_name: name.clone(),
            ip_cfg,
        };
        let hdl = opteadm::OpteAdm::open()?;
        hdl.add_port(&req)?;
        let ip4b = ip4.octets();
        hdl.set_overlay(&overlay::SetOverlayReq {
            port_name: name.clone(),
            cfg: overlay::OverlayCfg {
                boundary_services: overlay::PhysNet {
                    ether: ether::EtherAddr::from(
                        [0x01, 0x02, 0x03, 0x04, 0x05, 0x06]
                    ),
                    ip: ip6::Ipv6Addr::from(
                        [
                            0x2601, 0x0284, 0x4100, 0xe240,
                            0x0000, 0x0000, 0x7777, 0xABCD
                        ]
                    ),
                    vni: geneve::Vni::new(7777u32).unwrap(),
                },
                vni: geneve::Vni::new(99u32).unwrap(),
                phys_mac_src: ether::EtherAddr::from(mac.unwrap().into_array()),
                // kalm's e1000g0
                phys_mac_dst: ether::EtherAddr::from(
                    [0x54, 0xbe, 0xf7, 0x0b, 0x09, 0xec]
                ),
                // mom's router
                // phys_mac_dst: ether::EtherAddr::from(
                //     [0xB8, 0xF8, 0x53, 0xAF, 0x53, 0x7D]
                // ),
                // steamboat router
                // phys_mac_dst: ether::EtherAddr::from(
                //     [0x78, 0x23, 0xAE, 0x5d, 0x4F, 0x0D]
                // ),
                phys_ip_src: ip6::Ipv6Addr::from(
                    [
                        0x2601, 0x0284, 0x4100, 0xE240,
                        0x0000, 0x0000, u16::from_be_bytes([ip4b[0], ip4b[1]]),
                        u16::from_be_bytes([ip4b[2], ip4b[3]])
                    ]
                ),
            }
        })?;

        hdl.set_v2p(&overlay::SetVirt2PhysReq {
            vip: IpAddr::Ip4(ip4.into()),
            phys: overlay::PhysNet {
                ether: ether::EtherAddr::from(mac.unwrap().into_array()),
                ip: ip6::Ipv6Addr::from(
                    [
                        0x2601, 0x0284, 0x4100, 0xE240,
                        0x0000, 0x0000, u16::from_be_bytes([ip4b[0], ip4b[1]]),
                        u16::from_be_bytes([ip4b[2], ip4b[3]])
                    ]
                ),
                vni: geneve::Vni::new(99u32).unwrap(),
            }
        })?;

        hdl.add_router_entry_ip4(&router::AddRouterEntryIpv4Req {
            port_name: name.clone(),
            dest: "192.168.1.240/28".parse().unwrap(),
            target: router::RouterTarget::VpcSubnet(
                IpCidr::Ip4("192.168.1.240/28".parse().unwrap())
            ),
        })?;

        Ok(Vnic { name, deleted: false })
    }

    /// Creates a new NIC, intended for allowing Propolis to communicate
    // with the control plane.
    pub fn new_control(
        allocator: &IdAllocator,
        physical_dl: &PhysicalLink,
        mac: Option<MacAddr>,
    ) -> Result<Self, Error> {
        let name = control_vnic_name(allocator.next());
        Dladm::create_vnic(physical_dl, &name, mac, None)?;
        Ok(Vnic { name, deleted: false })
    }

    // Deletes a NIC (if it has not already been deleted).
    pub fn delete(&mut self) -> Result<(), Error> {
        if self.deleted {
            Ok(())
        } else {
            self.deleted = true;
            let hdl = opteadm::OpteAdm::open()?;
            hdl.delete_port(&self.name)?;
            Dladm::delete_vnic(&self.name)
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for Vnic {
    fn drop(&mut self) {
        let r = self.delete();
        if let Err(e) = r {
            eprintln!("Failed to delete VNIC: {}", e);
        }
    }
}
