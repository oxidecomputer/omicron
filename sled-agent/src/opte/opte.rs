// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interactions with the Oxide Packet Transformation Engine (OPTE)

use crate::common::underlay;
use crate::illumos::dladm;
use crate::illumos::dladm::Dladm;
use crate::illumos::dladm::PhysicalLink;
use crate::illumos::vnic::Vnic;
use crate::params::ExternalIp;
use ipnetwork::IpNetwork;
use macaddr::MacAddr6;
use opte::api::IpCidr;
use opte::api::Ipv4Cidr;
use opte::api::Ipv4PrefixLen;
use opte::api::MacAddr;
pub use opte::api::Vni;
use opte::oxide_vpc::api::AddRouterEntryIpv4Req;
use opte::oxide_vpc::api::RouterTarget;
use opte::oxide_vpc::api::SNatCfg;
use opte_ioctl::OpteHdl;
use slog::Logger;
use std::fs;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

// Prefix used to identify xde data links.
const XDE_LINK_PREFIX: &str = "opte";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failure interacting with the OPTE ioctl(2) interface: {0}")]
    Opte(#[from] opte_ioctl::Error),

    #[error("Failed to wrap OPTE port in a VNIC: {0}")]
    CreateVnic(#[from] dladm::CreateVnicError),

    #[error("Failed to get VNICs for xde underlay devices: {0}")]
    GetVnic(#[from] underlay::Error),

    #[error(
        "No xde driver configuration file exists at '/kernel/drv/xde.conf'"
    )]
    NoXdeConf,

    #[error(
        "The OS kernel does not support the xde driver. Please update the OS \
        using `./tools/install_opte.sh` to provide kernel bits and the xde \
        driver which are compatible."
    )]
    IncompatibleKernel,

    #[error(transparent)]
    BadAddrObj(#[from] crate::illumos::addrobj::ParseError),

    #[error(transparent)]
    SetLinkpropError(#[from] crate::illumos::dladm::SetLinkpropError),
}

#[derive(Debug, Clone)]
pub struct OptePortAllocator {
    gateway_mac: MacAddr,
    value: Arc<AtomicU64>,
}

impl OptePortAllocator {
    pub fn new(gateway_mac: MacAddr6) -> Self {
        let gateway_mac = MacAddr::from(gateway_mac.into_array());
        Self { gateway_mac, value: Arc::new(AtomicU64::new(0)) }
    }

    fn next(&self) -> String {
        format!("{}{}", XDE_LINK_PREFIX, self.next_id())
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
        external_ip: Option<ExternalIp>,
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
        let vpcsub = match subnet {
            IpNetwork::V4(ip4net) => {
                // We assume that IpNetwork does not allow an invalid prefix.
                Ok(Ipv4Cidr::new(
                    ip4net.ip().into(),
                    Ipv4PrefixLen::new(ip4net.prefix()).unwrap(),
                ))
            }
            IpNetwork::V6(_) => Err(opte_ioctl::Error::InvalidArgument(
                String::from("IPv6 is not yet supported for guest interfaces"),
            )),
        }?;

        // Describe the source NAT for this instance.
        let snat = match external_ip {
            Some(ip) => {
                let public_ip = match ip.ip {
                    IpAddr::V4(ip) => ip.into(),
                    IpAddr::V6(_) => {
                        return Err(opte_ioctl::Error::InvalidArgument(
                            String::from("IPv6 is not yet supported for external addresses")
                        ).into());
                    }
                };
                let ports = ip.first_port..=ip.last_port;
                Some(SNatCfg {
                    public_ip,
                    ports,
                    phys_gw_mac: self.gateway_mac,
                })
            }
            None => None,
        };

        let hdl = OpteHdl::open(OpteHdl::DLD_CTL)?;
        hdl.create_xde(
            &name,
            MacAddr::from(mac.into_array()),
            private_ip,
            vpcsub,
            MacAddr::from(gateway.mac.into_array()),
            gateway_ip,
            boundary_services.ip,
            boundary_services.vni,
            vni,
            underlay_ip,
            snat,
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
            // Safety: We're explicitly creating the VNIC with the prefix
            // `VNIC_PREFIX_GUEST`, so this call must return Some(_).
            Some(Vnic::wrap_existing(vnic_name).unwrap())
        };

        // TODO-remove
        //
        // This is part of the workaround to get external connectivity into
        // instances, without setting up all of boundary services. Rather than
        // encap/decap the guest traffic, OPTE just performs 1-1 NAT between the
        // private IP address of the guest and the external address provided by
        // the control plane. This call here allows the underlay nic, `net0` to
        // advertise as having the guest's MAC address.
        Dladm::set_linkprop(
            underlay::find_chelsio_links()?[0].0.as_str(),
            "secondary-macs",
            &mac.to_string().to_lowercase(),
        )?;

        // TODO-remove
        //
        // This is another part of the workaround, allowing reply traffic from
        // the guest back out. Normally, OPTE would drop such traffic at the
        // router layer, as it has no route for that external IP address. This
        // allows such traffic through.
        //
        // Note that this exact rule will eventually be included, since it's one
        // of the default routing rules in the VPC System Router. However, that
        // will likely be communicated in a different way, or could be modified,
        // and this specific call should be removed in favor of sending the
        // routing rules the control plane provides.
        //
        // This rule sends all traffic that has no better match to the gateway.
        let prefix = Ipv4PrefixLen::new(0).unwrap();
        let dest = Ipv4Cidr::new(Ipv4Addr::UNSPECIFIED.into(), prefix);
        let target = RouterTarget::InternetGateway;
        hdl.add_router_entry_ip4(&AddRouterEntryIpv4Req {
            port_name: name.clone(),
            dest,
            target,
        })?;

        Ok(OptePort {
            name,
            ip,
            subnet,
            mac,
            vni,
            underlay_ip,
            external_ip,
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
    // The external IP information for this port, or None if it has no external
    // connectivity. Only the primary interface has Some(_) here.
    external_ip: Option<ExternalIp>,
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

/// Delete all xde devices on the system.
pub fn delete_all_xde_devices(log: &Logger) -> Result<(), Error> {
    let hdl = OpteHdl::open(OpteHdl::DLD_CTL)?;
    for port_info in hdl.list_ports()?.ports.into_iter() {
        let name = &port_info.name;
        info!(
            log,
            "deleting existing OPTE port and xde device";
            "device_name" => name
        );
        hdl.delete_xde(name)?;
    }
    Ok(())
}

/// Initialize the underlay devices required for the xde kernel module.
///
/// The xde driver needs information about the physical devices out which it can
/// send traffic from the guests.
pub fn initialize_xde_driver(log: &Logger) -> Result<(), Error> {
    const XDE_CONF: &str = "/kernel/drv/xde.conf";
    let xde_conf = Path::new(XDE_CONF);
    if !xde_conf.exists() {
        return Err(Error::NoXdeConf);
    }

    // TODO-remove
    //
    // An additional part of the workaround to connect into instances. This is
    // required to tell OPTE to actually act as a 1-1 NAT when an instance is
    // provided with an external IP address, rather than do its normal job of
    // encapsulating the traffic onto the underlay (such as for delivery to
    // boundary services).
    use_external_ip_workaround(&log, &xde_conf);

    let underlay_nics = underlay::find_nics()?;
    info!(log, "using '{:?}' as data links for xde driver", underlay_nics);
    if underlay_nics.len() < 2 {
        const MESSAGE: &str = concat!(
            "There must be at least two underlay NICs for the xde ",
            "driver to operate. These are currently created by ",
            "`./tools/create_virtual_hardware.sh`. Please ensure that ",
            "script has been run, and that two VNICs named `net{0,1}` ",
            "exist on the system."
        );
        return Err(Error::Opte(opte_ioctl::Error::InvalidArgument(
            String::from(MESSAGE),
        )));
    }
    match OpteHdl::open(OpteHdl::DLD_CTL)?.set_xde_underlay(
        underlay_nics[0].interface(),
        underlay_nics[1].interface(),
    ) {
        Ok(_) => Ok(()),
        // Handle the specific case where the kernel appears to be unaware of
        // xde at all. This implies the developer has not installed the correct
        // helios-netdev kernel bits.
        //
        // TODO-correctness: This error should never occur in the product. Both
        // xde the kernel driver and the kernel bits needed to recognize it will
        // be packaged as part of our OS ramdisk, meaning it should not be
        // possible to get out of sync.
        Err(opte_ioctl::Error::IoctlFailed(_, ref message))
            if message.contains("unexpected errno: 48") =>
        {
            Err(Error::IncompatibleKernel)
        }
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

fn use_external_ip_workaround(log: &Logger, xde_conf: &Path) {
    const NEEDLE: &str = "ext_ip_hack = 0;";
    const NEW_NEEDLE: &str = "ext_ip_hack = 1;";

    // NOTE: This only works in the real sled agent, which is run as root. The
    // file is not world-readable.
    let contents = fs::read_to_string(xde_conf)
        .expect("Failed to read xde configuration file");
    let new = contents.replace(NEEDLE, NEW_NEEDLE);
    if contents == new {
        info!(
            log,
            "xde driver configuration file appears to already use external IP workaround";
            "conf_file" => ?xde_conf,
        );
    } else {
        info!(
            log,
            "updating xde driver configuration file for external IP workaround";
            "conf_file" => ?xde_conf,
        );
        fs::write(xde_conf, &new)
            .expect("Failed to modify xde configuration file");
    }

    // Ensure the driver picks up the updated configuration file, if it's been
    // loaded previously without the workaround.
    std::process::Command::new(crate::illumos::PFEXEC)
        .args(&["update_drv", "xde"])
        .output()
        .expect("Failed to reload xde driver configuration file");
}
