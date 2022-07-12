// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manager for all OPTE ports on a Helios system

use crate::illumos::dladm::Dladm;
use crate::illumos::dladm::PhysicalLink;
use crate::illumos::dladm::VnicSource;
use crate::opte::BoundaryServices;
use crate::opte::Error;
use crate::opte::Gateway;
use crate::opte::Port;
use crate::opte::Vni;
use crate::params::ExternalIp;
use crate::params::NetworkInterface;
use ipnetwork::IpNetwork;
use macaddr::MacAddr6;
use opte::api::IpCidr;
use opte::api::Ipv4Cidr;
use opte::api::Ipv4PrefixLen;
use opte::api::MacAddr;
use opte::oxide_vpc::api::AddRouterEntryIpv4Req;
use opte::oxide_vpc::api::RouterTarget;
use opte::oxide_vpc::api::SNatCfg;
use opte_ioctl::OpteHdl;
use slog::debug;
use slog::info;
use slog::warn;
use slog::Logger;
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use uuid::Uuid;

// Prefix used to identify xde data links.
const XDE_LINK_PREFIX: &str = "opte";

#[derive(Debug)]
struct PortManagerInner {
    log: Logger,

    // Sequential identifier for each port on the system.
    next_port_id: AtomicU64,

    // TODO-remove: This is part of the external IP address workaround
    //
    // See https://github.com/oxidecomputer/omicron/issues/1335
    //
    // We only need to know this while we're setting the secondary MACs of the
    // link to support OPTE's proxy ARP for the guest's IP.
    data_link: PhysicalLink,

    // TODO-remove: This is part of the external IP address workaround.
    //
    // See https://github.com/oxidecomputer/omicron/issues/1335
    //
    // We only need this while OPTE needs to forward traffic to the local
    // gateway. This will be replaced by boundary services.
    gateway_mac: MacAddr6,

    // IP address of the hosting sled on the underlay.
    underlay_ip: Ipv6Addr,

    // Map of instance ID to list of ports.
    //
    // NOTE: By storing all ports in a vector, the ticket mechanism makes the
    // first dropped port cause the whole vector to be dropped. The remaining
    // ports' drop impls will still call remove on this map, but there will no
    // longer be a value with that key.
    ports: Mutex<BTreeMap<Uuid, Vec<Port>>>,
}

impl PortManagerInner {
    fn next_port_name(&self) -> String {
        format!(
            "{}{}",
            XDE_LINK_PREFIX,
            self.next_port_id.fetch_add(1, Ordering::SeqCst)
        )
    }

    // TODO-remove
    //
    // See https://github.com/oxidecomputer/omicron/issues/1335
    //
    // This is part of the workaround to get external connectivity into
    // instances, without setting up all of boundary services. Rather than
    // encap/decap the guest traffic, OPTE just performs 1-1 NAT between the
    // private IP address of the guest and the external address provided by
    // the control plane. This call here allows the underlay nic, `net0` to
    // advertise as having the guest's MAC address.
    fn update_secondary_macs(
        &self,
        ports: &mut MutexGuard<'_, BTreeMap<Uuid, Vec<Port>>>,
    ) -> Result<(), Error> {
        let secondary_macs = ports
            .values()
            .flatten()
            .filter_map(|port| {
                // Only advertise Ports with an external address (primary
                // interface for an instance).
                if port.external_ip().is_some() {
                    Some(port.mac().to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join(",");
        if secondary_macs.is_empty() {
            Dladm::reset_linkprop(self.data_link.name(), "secondary-macs")?;
            debug!(
                self.log,
                "Reset data link secondary MACs link prop for OPTE proxy ARP";
                "link_name" => self.data_link.name(),
            );
        } else {
            Dladm::set_linkprop(
                self.data_link.name(),
                "secondary-macs",
                &secondary_macs,
            )?;
            debug!(
                self.log,
                "Updated data link secondary MACs link prop for OPTE proxy ARP";
                "data_link" => &self.data_link.0,
                "secondary_macs" => ?secondary_macs,
            );
        }
        Ok(())
    }
}

/// The port manager controls all OPTE ports on a single host.
#[derive(Debug, Clone)]
pub struct PortManager {
    inner: Arc<PortManagerInner>,
}

impl PortManager {
    /// Create a new manager, for creating OPTE ports for guest network
    /// interfaces
    pub fn new(
        log: Logger,
        underlay_ip: Ipv6Addr,
        gateway_mac: MacAddr6,
    ) -> Self {
        let data_link = crate::common::underlay::find_chelsio_links()
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let inner = Arc::new(PortManagerInner {
            log,
            next_port_id: AtomicU64::new(0),
            data_link,
            gateway_mac,
            underlay_ip,
            ports: Mutex::new(BTreeMap::new()),
        });

        Self { inner }
    }

    pub fn underlay_ip(&self) -> &Ipv6Addr {
        &self.inner.underlay_ip
    }

    /// Create an OPTE port for the given guest instance.
    pub fn create_port(
        &self,
        instance_id: Uuid,
        nic: &NetworkInterface,
        external_ip: Option<ExternalIp>,
    ) -> Result<Port, Error> {
        // TODO-completess: Remove IPv4 restrictions once OPTE supports virtual
        // IPv6 networks.
        let private_ip = match nic.ip {
            IpAddr::V4(ip) => Ok(ip),
            IpAddr::V6(_) => Err(opte_ioctl::Error::InvalidArgument(
                String::from("IPv6 is not yet supported for guest interfaces"),
            )),
        }?;

        // Argument checking and conversions into OPTE data types.
        let subnet = IpNetwork::from(nic.subnet);
        let mac = *nic.mac;
        let vni = Vni::new(nic.vni).unwrap();
        let (opte_subnet, gateway) = match subnet {
            IpNetwork::V4(net) => (
                Ipv4Cidr::new(
                    net.ip().into(),
                    Ipv4PrefixLen::new(net.prefix()).unwrap(),
                ),
                Gateway::from_subnet(&subnet),
            ),
            IpNetwork::V6(_) => {
                return Err(opte_ioctl::Error::InvalidArgument(String::from(
                    "IPv6 is not yet supported for guest interfaces",
                ))
                .into());
            }
        };
        let gateway_ip = match gateway.ip {
            IpAddr::V4(ip) => Ok(ip),
            IpAddr::V6(_) => Err(opte_ioctl::Error::InvalidArgument(
                String::from("IPv6 is not yet supported for guest interfaces"),
            )),
        }?;
        let boundary_services = BoundaryServices::default();

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
                    phys_gw_mac: MacAddr::from(
                        self.inner.gateway_mac.into_array(),
                    ),
                })
            }
            None => None,
        };

        // Create the xde device.
        //
        // The sequencing here is important. We'd like to make sure things are
        // cleaned up properly, while having a sequence of fallible operations.
        // So we:
        //
        // - create the xde device
        // - create the vnic, cleaning up the xde device if that fails
        // - add both to the Port
        //
        // The Port object's drop implementation will clean up both of those, if
        // any of the remaining fallible operations fail.
        let port_name = self.inner.next_port_name();
        let hdl = OpteHdl::open(OpteHdl::DLD_CTL)?;
        hdl.create_xde(
            &port_name,
            MacAddr::from(mac.into_array()),
            private_ip,
            opte_subnet,
            MacAddr::from(gateway.mac.into_array()),
            gateway_ip,
            boundary_services.ip,
            boundary_services.vni,
            vni,
            self.inner.underlay_ip,
            snat,
            /* passthru = */ false,
        )?;
        debug!(
            self.inner.log,
            "Created xde device for guest port";
            "port_name" => &port_name,
        );

        // Create a VNIC on top of this device, to hook Viona into.
        //
        // Viona is the illumos MAC provider that implements the VIRTIO
        // specification. It sits on top of a MAC provider, which is responsible
        // for delivering frames to the underlying data link. The guest includes
        // a driver that handles the virtio-net specification on their side,
        // which talks to Viona.
        //
        // In theory, Viona works with any MAC provider. However, there are
        // implicit assumptions, in both Viona _and_ MAC, that require Viona to
        // be built on top of a VNIC specifically. There is probably a good deal
        // of work required to relax that assumption, so in the meantime, we
        // create a superfluous VNIC on the OPTE device, solely so Viona can use
        // it.
        let vnic = {
            let phys = PhysicalLink(port_name.clone());
            let vnic_name = format!("v{}", port_name);
            if let Err(e) =
                Dladm::create_vnic(&phys, &vnic_name, Some(nic.mac), None)
            {
                warn!(
                    self.inner.log,
                    "Failed to create overlay VNIC for xde device";
                    "port_name" => port_name.as_str(),
                    "err" => ?e
                );
                if let Err(e) = hdl.delete_xde(&port_name) {
                    warn!(
                        self.inner.log,
                        "Failed to clean up xde device after failure to create overlay VNIC";
                        "err" => ?e
                    );
                }
                return Err(e.into());
            }
            debug!(
                self.inner.log,
                "Created overlay VNIC for xde device";
                "port_name" => &port_name,
                "vnic_name" => &vnic_name,
            );

            // NOTE: We intentionally use a string rather than the Vnic type
            // here. See the notes on the `opte::PortInner::vnic` field.
            vnic_name
        };

        let port = {
            let mut ports = self.inner.ports.lock().unwrap();
            let ticket = PortTicket::new(instance_id, self.inner.clone());
            let port = Port::new(
                ticket,
                port_name.clone(),
                nic.ip,
                subnet,
                mac,
                nic.slot,
                vni,
                self.inner.underlay_ip,
                external_ip,
                gateway,
                boundary_services,
                vnic,
            );
            ports
                .entry(instance_id)
                .or_insert_with(Vec::new)
                .push(port.clone());
            self.inner.update_secondary_macs(&mut ports)?;
            port
        };

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
                    port_name: port_name.clone(),
                    dest: cidr,
                    target: RouterTarget::VpcSubnet(IpCidr::Ip4(cidr)),
                };
                hdl.add_router_entry_ip4(&route)?;
                debug!(
                    self.inner.log,
                    "Added IPv4 VPC Subnet router entry for OPTE port";
                    "port_name" => &port_name,
                    "entry" => ?route,
                );
            }
            IpAddr::V6(_) => {
                return Err(opte_ioctl::Error::InvalidArgument(String::from(
                    "IPv6 not yet supported",
                ))
                .into());
            }
        }

        // TODO-remove
        //
        // See https://github.com/oxidecomputer/omicron/issues/1336
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
        let dest =
            Ipv4Cidr::new(std::net::Ipv4Addr::UNSPECIFIED.into(), prefix);
        let target = RouterTarget::InternetGateway;
        hdl.add_router_entry_ip4(&AddRouterEntryIpv4Req {
            port_name,
            dest,
            target,
        })?;

        info!(
            self.inner.log,
            "Created OPTE port for guest";
            "port" => ?&port,
        );
        Ok(port)
    }
}

#[derive(Clone)]
pub struct PortTicket {
    id: Uuid,
    manager: Option<Arc<PortManagerInner>>,
}

impl std::fmt::Debug for PortTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        const SOME: &str = "Some(_)";
        const NONE: &str = "None";
        f.debug_struct("PortTicket")
            .field("id", &self.id)
            .field(
                "manager",
                if self.manager.is_some() { &SOME } else { &NONE },
            )
            .finish()
    }
}

impl PortTicket {
    fn new(id: Uuid, manager: Arc<PortManagerInner>) -> Self {
        Self { id, manager: Some(manager) }
    }

    pub fn release(&mut self) -> Result<(), Error> {
        if let Some(manager) = self.manager.take() {
            let mut ports = manager.ports.lock().unwrap();
            let n_ports = ports.remove(&self.id).map(|p| p.len()).unwrap_or(0);
            debug!(
                manager.log,
                "Removing OPTE ports from manager";
                "instance_id" => ?self.id,
                "n_ports" => n_ports,
            );
            if let Err(e) = manager.update_secondary_macs(&mut ports) {
                warn!(
                    manager.log,
                    "Failed to update secondary-macs linkprop when \
                    releasing OPTE ports for instance";
                    "instance_id" => ?self.id,
                    "err" => ?e,
                );
                return Err(e);
            } else {
                return Ok(());
            }
        }
        Ok(())
    }
}

impl Drop for PortTicket {
    fn drop(&mut self) {
        // We're ignoring the value since (1) it's already logged and (2) we
        // can't do anything with it anyway.
        let _ = self.release();
    }
}
