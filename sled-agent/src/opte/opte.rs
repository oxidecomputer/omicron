// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interactions with the Oxide Packet Transformation Engine (OPTE)

use crate::illumos::addrobj;
use crate::illumos::addrobj::AddrObject;
use crate::illumos::dladm;
use crate::illumos::dladm::Dladm;
use crate::illumos::dladm::PhysicalLink;
use crate::illumos::vnic::Vnic;
use crate::illumos::zone::Zones;
use ipnetwork::IpNetwork;
use macaddr::MacAddr6;
use opte::api::IpCidr;
use opte::api::Ipv4Cidr;
use opte::api::Ipv4PrefixLen;
use opte::api::MacAddr;
pub use opte::api::Vni;
use opte::oxide_vpc::api::AddRouterEntryIpv4Req;
use opte::oxide_vpc::api::RouterTarget;
use opte_ioctl::OpteHdl;
use slog::Logger;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failure interacting with the OPTE ioctl(2) interface: {0}")]
    Opte(#[from] opte_ioctl::Error),

    #[error("Failed to wrap OPTE port in a VNIC: {0}")]
    CreateVnic(#[from] dladm::CreateVnicError),

    #[error("Failed to create an IPv6 link-local address for xde underlay devices: {0}")]
    UnderlayDevice(#[from] crate::illumos::ExecutionError),

    #[error(transparent)]
    BadAddrObj(#[from] addrobj::ParseError),
}

#[derive(Debug, Clone)]
pub struct OptePortAllocator {
    value: Arc<AtomicU64>,
}

impl OptePortAllocator {
    pub fn new() -> Self {
        Self { value: Arc::new(AtomicU64::new(0)) }
    }

    fn next(&self) -> String {
        format!("opte{}", self.next_id())
    }

    fn next_id(&self) -> u64 {
        self.value.fetch_add(1, Ordering::SeqCst)
    }

    pub fn new_port(
        &self,
        ip: IpAddr,
        mac: MacAddr6,
        subnet: IpNetwork,
        vni: Vni,
        underlay_ip: Ipv6Addr,
    ) -> Result<OptePort, Error> {
        // TODO-completess: Remove IPv4 restrictions once OPTE supports virtual
        // IPv6 networks.
        let private_ip = match ip {
            IpAddr::V4(ip) => Ok(ip),
            IpAddr::V6(_) => Err(opte_ioctl::Error::InvalidArgument(
                String::from("IPv6 is not yet supported for guest interfaces"),
            )),
        }?;
        let gateway = Gateway::from_subnet(&subnet);
        let gateway_ip = match gateway.ip {
            IpAddr::V4(ip) => Ok(ip),
            IpAddr::V6(_) => Err(opte_ioctl::Error::InvalidArgument(
                String::from("IPv6 is not yet supported for guest interfaces"),
            )),
        }?;
        let boundary_services = BoundaryServices::default();
        let name = self.next();
        let hdl = OpteHdl::open(OpteHdl::DLD_CTL)?;
        hdl.create_xde(
            &name,
            MacAddr::from(mac.into_array()),
            private_ip,
            MacAddr::from(gateway.mac.into_array()),
            gateway_ip,
            boundary_services.ip,
            boundary_services.vni,
            vni,
            underlay_ip,
            /* passthru = */ false,
        )?;

        // Add a router entry for this interface's subnet, directing traffic to the
        // VPC subnet.
        match subnet.network() {
            IpAddr::V4(ip) => {
                let prefix =
                    Ipv4PrefixLen::new(subnet.prefix()).map_err(|e| {
                        opte_ioctl::Error::InvalidArgument(format!(
                            "Invalid IPv4 subnet prefix: {}",
                            e
                        ))
                    })?;
                let cidr = Ipv4Cidr::new(opte::api::Ipv4Addr::from(ip), prefix);
                let route = AddRouterEntryIpv4Req {
                    port_name: name.clone(),
                    dest: cidr,
                    target: RouterTarget::VpcSubnet(IpCidr::Ip4(cidr)),
                };
                hdl.add_router_entry_ip4(&route)?;
            }
            IpAddr::V6(_) => {
                return Err(opte_ioctl::Error::InvalidArgument(String::from(
                    "IPv6 not yet supported",
                ))
                .into());
            }
        }

        // Create a VNIC on top of this device, to hook Viona into.
        //
        // Viona is the illumos MAC provider that implements the VIRTIO
        // specification.It sits on top of a MAC provider, which is responsible
        // for delivering frames to the underlying data link. The guest includes
        // a driver that handles the virtio-net specification on their side,
        // which talks to Viona.
        //
        // In theory, Viona work with any MAC provider. However, there are
        // implicit assumptions, in both Viona _and_ MAC, that require Viona to
        // be built on top of a VNIC specifically. There is probably a good deal
        // of work required to relax that assumption, so in the meantime, we
        // create a superfluous VNIC on the OPTE device, solely so Viona can use
        // it.
        let vnic = {
            let phys = PhysicalLink(name.clone());
            let vnic_name = format!("v{}", name);
            Dladm::create_vnic(
                &phys,
                &vnic_name,
                Some(omicron_common::api::external::MacAddr(mac)),
                None,
            )?;
            Some(Vnic::wrap_existing(vnic_name))
        };

        Ok(OptePort {
            name,
            ip,
            subnet,
            mac,
            vni,
            underlay_ip,
            gateway,
            boundary_services,
            vnic,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BoundaryServices {
    pub ip: Ipv6Addr,
    pub vni: Vni,
}

impl Default for BoundaryServices {
    fn default() -> Self {
        // TODO-completeness: Don't hardcode this.
        //
        // Boundary Services will be started on several Sidecars during rack
        // setup, and those addresses will need to be propagated here.
        const BOUNDARY_SERVICES_ADDR: Ipv6Addr =
            Ipv6Addr::new(0xfd00, 0x99, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01);
        let boundary_services_vni = Vni::new(99_u32).unwrap();

        Self { ip: BOUNDARY_SERVICES_ADDR, vni: boundary_services_vni }
    }
}

/// Information about the gateway for an OPTE port
#[derive(Debug, Clone, Copy)]
pub struct Gateway {
    mac: MacAddr6,
    ip: IpAddr,
}

// The MAC address that OPTE exposes to guest NICs, i.e., the MAC of the virtual
// gateway OPTE operates as for each guest. See
// https://github.com/oxidecomputer/omicron/pull/955#discussion_r856432498 for
// more context on the genesis of this, but this is just a reserved address
// within the "system" portion of the virtual MAC address space.
const OPTE_VIRTUAL_GATEWAY_MAC: MacAddr6 =
    MacAddr6::new(0xa8, 0x40, 0x25, 0xff, 0x77, 0x77);

impl Gateway {
    pub fn from_subnet(subnet: &IpNetwork) -> Self {
        Self {
            mac: OPTE_VIRTUAL_GATEWAY_MAC,

            // See RFD 21, section 2.2 table 1
            ip: subnet
                .iter()
                .nth(1)
                .expect("IP subnet must have at least 1 address"),
        }
    }
}

/// A port on the OPTE "virtual switch", which corresponds to one guest network
/// interface.
#[derive(Debug)]
#[allow(dead_code)]
pub struct OptePort {
    name: String,
    ip: IpAddr,
    subnet: IpNetwork,
    mac: MacAddr6,
    vni: Vni,
    underlay_ip: Ipv6Addr,
    gateway: Gateway,
    boundary_services: BoundaryServices,
    // TODO-correctness: Remove this once we can put Viona directly on top of an
    // OPTE port device.
    //
    // Note that this will always be `Some(_)`. It is wrapped in an optional to
    // ensure we can drop the VNIC before we drop the OPTE port itself.
    vnic: Option<Vnic>,
}

impl OptePort {
    /// Return the VNIC used to link OPTE and Viona.
    // TODO-correctness: Remove this once we can put Viona directly on top of an
    // OPTE port device.
    pub fn vnic(&self) -> &Vnic {
        self.vnic.as_ref().unwrap()
    }
}

impl Drop for OptePort {
    fn drop(&mut self) {
        self.vnic.take();
        if let Ok(hdl) = OpteHdl::open(OpteHdl::DLD_CTL) {
            if hdl.delete_xde(&self.name).is_ok() {
                return;
            }
        }
        eprintln!("WARNING: Failed to delete OPTE port '{}'", self.name);
    }
}

/// Initialize the underlay devices required for the xde kernel module.
///
/// The xde driver needs information about the physical devices out which it can
/// send traffic from the guests.
pub fn initialize_xde_driver(log: &Logger) -> Result<(), Error> {
    let underlay_nics = find_chelsio_links()?;
    info!(log, "using '{:?}' as data links for xde driver", underlay_nics);
    if underlay_nics.len() < 2 {
        return Err(Error::Opte(opte_ioctl::Error::InvalidArgument(
            String::from("There must be at least two underlay NICs"),
        )));
    }
    for nic in &underlay_nics {
        let addrobj = AddrObject::new(&nic.0, "linklocal")?;
        Zones::ensure_has_link_local_v6_address(None, &addrobj)?;
    }
    match OpteHdl::open(OpteHdl::DLD_CTL)?
        .set_xde_underlay(&underlay_nics[0].0, &underlay_nics[1].0)
    {
        Ok(_) => Ok(()),
        // TODO-correctness: xde provides no way to get the current underlay
        // devices we're using, but we'd probably like the further check that
        // those are exactly what we're giving it now.
        Err(opte_ioctl::Error::CommandError(
            _,
            opte::api::OpteError::System { errno: libc::EEXIST, .. },
        )) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

fn find_chelsio_links() -> Result<Vec<PhysicalLink>, Error> {
    // TODO-correctness: This should eventually be determined by a call to
    // `Dladm` to get the real Chelsio links on a Gimlet. These will likely be
    // called `cxgbeN`, but we explicitly call them `netN` to be clear that
    // they're likely VNICs for the time being.
    Ok((0..2).map(|i| PhysicalLink(format!("net{}", i))).collect())
}
