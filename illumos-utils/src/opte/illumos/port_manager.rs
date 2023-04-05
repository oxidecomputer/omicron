// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manager for all OPTE ports on a Helios system

use crate::dladm::Dladm;
use crate::dladm::PhysicalLink;
use crate::opte::default_boundary_services;
use crate::opte::opte_firewall_rules;
use crate::opte::params::NetworkInterface;
use crate::opte::params::SetVirtualNetworkInterfaceHost;
use crate::opte::params::SourceNatConfig;
use crate::opte::params::VpcFirewallRule;
use crate::opte::Error;
use crate::opte::Gateway;
use crate::opte::Port;
use crate::opte::Vni;
use ipnetwork::IpNetwork;
use macaddr::MacAddr6;
use opte_ioctl::OpteHdl;
use oxide_vpc::api::AddRouterEntryReq;
use oxide_vpc::api::IpCfg;
use oxide_vpc::api::IpCidr;
use oxide_vpc::api::Ipv4Cfg;
use oxide_vpc::api::Ipv4Cidr;
use oxide_vpc::api::Ipv4PrefixLen;
use oxide_vpc::api::MacAddr;
use oxide_vpc::api::PhysNet;
use oxide_vpc::api::RouterTarget;
use oxide_vpc::api::SNat4Cfg;
use oxide_vpc::api::SetFwRulesReq;
use oxide_vpc::api::SetVirt2PhysReq;
use oxide_vpc::api::VpcCfg;
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
use uuid::Uuid;

// Prefix used to identify xde data links.
const XDE_LINK_PREFIX: &str = "opte";

#[derive(Debug)]
struct PortManagerInner {
    log: Logger,

    // Sequential identifier for each port on the system.
    next_port_id: AtomicU64,

    // TODO-remove: This is part of the external IP address workaround.
    //
    // See https://github.com/oxidecomputer/omicron/issues/1335
    //
    // We only need this while OPTE needs to forward traffic to the local
    // gateway. This will be replaced by boundary services.
    gateway_mac: MacAddr6,

    // IP address of the hosting sled on the underlay.
    underlay_ip: Ipv6Addr,

    // Map of all ports, keyed on the instance Uuid and the port name.
    ports: Mutex<BTreeMap<(Uuid, String), Port>>,
}

impl PortManagerInner {
    fn next_port_name(&self) -> String {
        format!(
            "{}{}",
            XDE_LINK_PREFIX,
            self.next_port_id.fetch_add(1, Ordering::SeqCst)
        )
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
        let inner = Arc::new(PortManagerInner {
            log,
            next_port_id: AtomicU64::new(0),
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
        source_nat: Option<SourceNatConfig>,
        external_ips: Option<Vec<IpAddr>>,
        firewall_rules: &[VpcFirewallRule],
    ) -> Result<(Port, PortTicket), Error> {
        // TODO-completeness: Remove IPv4 restrictions once OPTE supports
        // virtual IPv6 networks.
        let private_ip = match nic.ip {
            IpAddr::V4(ip) => Ok(oxide_vpc::api::Ipv4Addr::from(ip)),
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
            IpAddr::V4(ip) => Ok(oxide_vpc::api::Ipv4Addr::from(ip)),
            IpAddr::V6(_) => Err(opte_ioctl::Error::InvalidArgument(
                String::from("IPv6 is not yet supported for guest interfaces"),
            )),
        }?;
        let boundary_services = default_boundary_services();

        // Describe the source NAT for this instance.
        let snat = match source_nat {
            Some(snat) => {
                let external_ip = match snat.ip {
                    IpAddr::V4(ip) => ip.into(),
                    IpAddr::V6(_) => {
                        return Err(opte_ioctl::Error::InvalidArgument(
                            String::from("IPv6 is not yet supported for external addresses")
                        ).into());
                    }
                };
                let ports = snat.first_port..=snat.last_port;
                Some(SNat4Cfg { external_ip, ports })
            }
            None => None,
        };

        // Describe the external IP addresses for this instance.
        //
        // Note that we're currently only taking the first address, which is all
        // that OPTE supports. The array is guaranteed to be limited by Nexus.
        // See https://github.com/oxidecomputer/omicron/issues/1467
        // See https://github.com/oxidecomputer/opte/issues/196
        let external_ip = match external_ips {
            Some(ref ips) if !ips.is_empty() => {
                match ips[0] {
                    IpAddr::V4(ipv4) => Some(ipv4.into()),
                    IpAddr::V6(_) => {
                        return Err(opte_ioctl::Error::InvalidArgument(
                            String::from("IPv6 is not yet supported for external addresses")
                        ).into());
                    }
                }
            }
            _ => None,
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
        let hdl = OpteHdl::open(OpteHdl::XDE_CTL)?;

        // TODO-completeness: Add support for IPv6.
        let vpc_cfg = VpcCfg {
            ip_cfg: IpCfg::Ipv4(Ipv4Cfg {
                vpc_subnet: opte_subnet,
                private_ip,
                gateway_ip,
                snat,
                external_ips: external_ip,
            }),
            guest_mac: MacAddr::from(mac.into_array()),
            gateway_mac: MacAddr::from(gateway.mac.into_array()),
            vni,
            phys_ip: self.inner.underlay_ip.into(),
            boundary_services,
            // TODO-remove: Part of the external IP hack.
            //
            // NOTE: This value of this flag is irrelevant, since the driver
            // always overwrites it. The field itself is used in the `oxide-vpc`
            // code though, to determine how to set up the ARP layer, which is
            // why it's still here.
            proxy_arp_enable: true,
            phys_gw_mac: Some(MacAddr::from(
                self.inner.gateway_mac.into_array(),
            )),
            // TODO-completeness (#2153): Plumb domain search list
            domain_list: vec![],
        };
        hdl.create_xde(&port_name, vpc_cfg, /* passthru = */ false)?;
        debug!(
            self.inner.log,
            "Created xde device for guest port";
            "port_name" => &port_name,
        );

        // Initialize firewall rules for the new port.
        let mut rules = opte_firewall_rules(firewall_rules, &vni, &mac);

        // TODO-remove: This is part of the external IP hack.
        //
        // We need to allow incoming ARP packets past the firewall layer so
        // that they may be handled properly at the gateway layer.
        rules.push(
            "dir=in priority=65534 protocol=arp action=allow".parse().unwrap(),
        );

        debug!(
            self.inner.log,
            "Setting firewall rules";
            "port_name" => &port_name,
            "rules" => ?&rules,
        );
        hdl.set_fw_rules(&SetFwRulesReq {
            port_name: port_name.clone(),
            rules,
        })?;

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

        let (port, ticket) = {
            let mut ports = self.inner.ports.lock().unwrap();
            let ticket = PortTicket::new(
                instance_id,
                port_name.clone(),
                self.inner.clone(),
            );
            let port = Port::new(
                port_name.clone(),
                nic.ip,
                subnet,
                mac,
                nic.slot,
                vni,
                self.inner.underlay_ip,
                source_nat,
                external_ips,
                gateway,
                boundary_services,
                vnic,
            );
            let old =
                ports.insert((instance_id, port_name.clone()), port.clone());
            assert!(
                old.is_none(),
                "Duplicate OPTE port detected: instance_id = {}, port_name = {}",
                instance_id,
                &port_name,
            );
            (port, ticket)
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
                let cidr =
                    Ipv4Cidr::new(oxide_vpc::api::Ipv4Addr::from(ip), prefix);
                let route = AddRouterEntryReq {
                    port_name: port_name.clone(),
                    dest: cidr.into(),
                    target: RouterTarget::VpcSubnet(IpCidr::Ip4(cidr)),
                };
                hdl.add_router_entry(&route)?;
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
        hdl.add_router_entry(&AddRouterEntryReq {
            port_name,
            dest: dest.into(),
            target,
        })?;

        info!(
            self.inner.log,
            "Created OPTE port for guest";
            "port" => ?&port,
        );
        Ok((port, ticket))
    }

    pub fn firewall_rules_ensure(
        &self,
        rules: &[VpcFirewallRule],
    ) -> Result<(), Error> {
        let hdl = OpteHdl::open(OpteHdl::XDE_CTL)?;
        for ((_, port_name), port) in self.inner.ports.lock().unwrap().iter() {
            let rules = opte_firewall_rules(rules, port.vni(), port.mac());
            let port_name = port_name.clone();
            info!(
                self.inner.log,
                "Setting OPTE firewall rules";
                "port" => ?&port_name,
                "rules" => ?&rules,
            );
            hdl.set_fw_rules(&SetFwRulesReq { port_name, rules })?;
        }
        Ok(())
    }

    pub fn set_virtual_nic_host(
        &self,
        mapping: &SetVirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        let hdl = OpteHdl::open(OpteHdl::XDE_CTL)?;

        hdl.set_v2p(&SetVirt2PhysReq {
            vip: mapping.virtual_ip.into(),
            phys: PhysNet {
                ether: oxide_vpc::api::MacAddr::from(
                    (*mapping.virtual_mac).into_array(),
                ),
                ip: mapping.physical_host_ip.into(),
                vni: Vni::new(mapping.vni).unwrap(),
            },
        })?;

        Ok(())
    }

    pub fn unset_virtual_nic_host(
        &self,
        _mapping: &SetVirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        // TODO requires https://github.com/oxidecomputer/opte/issues/332
        Ok(())
    }
}

pub struct PortTicket {
    id: Uuid,
    port_name: String,
    manager: Option<Arc<PortManagerInner>>,
}

impl std::fmt::Debug for PortTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.manager.is_some() {
            f.debug_struct("PortTicket")
                .field("id", &self.id)
                .field("manager", &"{ .. }")
                .finish()
        } else {
            f.debug_struct("PortTicket").field("id", &self.id).finish()
        }
    }
}

impl PortTicket {
    fn new(
        id: Uuid,
        port_name: String,
        manager: Arc<PortManagerInner>,
    ) -> Self {
        Self { id, port_name, manager: Some(manager) }
    }

    pub fn release(&mut self) -> Result<(), Error> {
        if let Some(manager) = self.manager.take() {
            let mut ports = manager.ports.lock().unwrap();
            ports.remove(&(self.id, self.port_name.clone()));
            debug!(
                manager.log,
                "Removing OPTE port from manager";
                "instance_id" => ?self.id,
                "port_name" => &self.port_name,
            );
            return Ok(());
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
