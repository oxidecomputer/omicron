// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manager for all OPTE ports on a Helios system

use crate::dladm::Dladm;
use crate::dladm::PhysicalLink;
use crate::opte::default_boundary_services;
use crate::opte::opte_firewall_rules;
use crate::opte::params::NetworkInterface;
use crate::opte::params::SourceNatConfig;
use crate::opte::params::VpcFirewallRule;
use crate::opte::Error;
use crate::opte::Gateway;
use crate::opte::Port;
use crate::opte::PortKey;
use crate::opte::PortType;
use crate::opte::Vni;
use ipnetwork::IpNetwork;
use macaddr::MacAddr6;
use opte_ioctl::OpteHdl;
use oxide_vpc::api::AddRouterEntryReq;
use oxide_vpc::api::IpCfg;
use oxide_vpc::api::IpCidr;
use oxide_vpc::api::Ipv4Cfg;
use oxide_vpc::api::Ipv6Cfg;
use oxide_vpc::api::MacAddr;
use oxide_vpc::api::RouterTarget;
use oxide_vpc::api::SNat4Cfg;
use oxide_vpc::api::SNat6Cfg;
use oxide_vpc::api::SetFwRulesReq;
use oxide_vpc::api::VpcCfg;
use slog::debug;
use slog::error;
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

    // Map of all ports.
    ports: Mutex<BTreeMap<PortKey, Port>>,
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

    fn create_port_inner(
        &self,
        port_type: PortType,
        port_name: Option<String>,
        nic: &NetworkInterface,
        source_nat: Option<SourceNatConfig>,
        external_ips: &[IpAddr],
        firewall_rules: &[VpcFirewallRule],
    ) -> Result<(Port, PortTicket), Error> {
        let mac = *nic.mac;
        let vni = Vni::new(nic.vni).unwrap();
        let subnet = IpNetwork::from(nic.subnet);
        let vpc_subnet = IpCidr::from(subnet);
        let gateway = Gateway::from_subnet(&subnet);
        let boundary_services = default_boundary_services();

        // Describe the external IP addresses for this instance/service.
        //
        // Note that we're currently only taking the first address, which is all
        // that OPTE supports. The array is guaranteed to be limited by Nexus.
        // See https://github.com/oxidecomputer/omicron/issues/1467
        // See https://github.com/oxidecomputer/opte/issues/196
        //
        // In the case of a service we explicitly only pass in a single
        // address, see `create_svc_port`.
        let external_ip = external_ips.get(0);

        macro_rules! ip_cfg {
            ($ip:expr, $log_prefix:literal, $ip_t:path, $cidr_t:path,
             $ipcfg_e:path, $ipcfg_t:ident, $snat_t:ident) => {{
                let $cidr_t(vpc_subnet) = vpc_subnet else {
                    error!(
                        self.inner.log,
                        concat!($log_prefix, " subnet");
                        "subnet" => ?vpc_subnet,
                    );
                    return Err(Error::InvalidPortIpConfig);
                };
                let $ip_t(gateway_ip) = gateway.ip else {
                    error!(
                        self.inner.log,
                        concat!($log_prefix, " gateway");
                        "gateway_ip" => ?gateway.ip,
                    );
                    return Err(Error::InvalidPortIpConfig);
                };
                let snat = match source_nat {
                    Some(snat) => {
                        let $ip_t(snat_ip) = snat.ip else {
                            error!(
                                self.inner.log,
                                concat!($log_prefix, " SNAT config");
                                "snat_ip" => ?snat.ip,
                            );
                            return Err(Error::InvalidPortIpConfig);
                        };
                        let ports = snat.first_port..=snat.last_port;
                        Some($snat_t { external_ip: snat_ip.into(), ports })
                    }
                    None => None,
                };
                let external_ip = match external_ip {
                    Some($ip_t(ip)) => Some((*ip).into()),
                    Some(_) => {
                        error!(
                            self.inner.log,
                            concat!($log_prefix, " external IP");
                            "external_ip" => ?external_ip,
                        );
                        return Err(Error::InvalidPortIpConfig);
                    }
                    None => None,
                };

                $ipcfg_e($ipcfg_t {
                    vpc_subnet,
                    private_ip: $ip.into(),
                    gateway_ip: gateway_ip.into(),
                    snat,
                    external_ips: external_ip,
                })
            }}
        }

        let ip_cfg = match nic.ip {
            IpAddr::V4(ip) => ip_cfg!(
                ip,
                "Expected IPv4",
                IpAddr::V4,
                IpCidr::Ip4,
                IpCfg::Ipv4,
                Ipv4Cfg,
                SNat4Cfg
            ),
            IpAddr::V6(ip) => ip_cfg!(
                ip,
                "Expected IPv6",
                IpAddr::V6,
                IpCidr::Ip6,
                IpCfg::Ipv6,
                Ipv6Cfg,
                SNat6Cfg
            ),
        };

        let vpc_cfg = VpcCfg {
            ip_cfg,
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

        let port_name =
            port_name.unwrap_or_else(|| self.inner.next_port_name());

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

        // 1. Create the xde device for the port.
        let hdl = OpteHdl::open(OpteHdl::XDE_CTL)?;
        debug!(
            self.inner.log,
            "Creating xde device";
            "port_name" => &port_name,
            "vpc_cfg" => ?&vpc_cfg,
        );
        hdl.create_xde(&port_name, vpc_cfg, /* passthru= */ false)?;

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

        // 2. Create a VNIC on top of this device, to hook Viona into.
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
                Dladm::create_vnic(&phys, &vnic_name, Some(mac.into()), None)
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

        // 3. Create the Port object.
        let (port, ticket) = {
            let mut ports = self.inner.ports.lock().unwrap();
            let port = Port::new(
                port_name.clone(),
                port_type,
                nic.ip,
                mac,
                nic.slot,
                vni,
                gateway,
                vnic,
            );
            let ticket = PortTicket::new(port.key(), self.inner.clone());
            let old = ports.insert(port.key(), port.clone());
            assert!(
                old.is_none(),
                "Duplicate OPTE port detected: key = {:?}, port_name = {}",
                port.key(),
                port_name,
            );
            (port, ticket)
        };

        // Add a router entry for this interface's subnet, directing traffic to the
        // VPC subnet.
        let route = AddRouterEntryReq {
            port_name: port_name.clone(),
            dest: vpc_subnet.into(),
            target: RouterTarget::VpcSubnet(vpc_subnet.into()),
        };
        hdl.add_router_entry(&route)?;
        debug!(
            self.inner.log,
            "Added IPv4 VPC Subnet router entry for OPTE port";
            "port_name" => &port_name,
            "entry" => ?route,
        );

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
        let dest = match vpc_subnet {
            IpCidr::Ip4(_) => "0.0.0.0/0".parse().unwrap(),
            IpCidr::Ip6(_) => "::/0".parse().unwrap(),
        };
        let route = AddRouterEntryReq {
            port_name: port_name.clone(),
            dest,
            target: RouterTarget::InternetGateway,
        };
        hdl.add_router_entry(&route)?;
        debug!(
            self.inner.log,
            "Added default internet gateway route entry";
            "port_name" => &port_name,
            "route" => ?route,
        );

        info!(
            self.inner.log,
            "Created OPTE port";
            "port_type" => ?port_type,
            "port" => ?&port,
        );

        Ok((port, ticket))
    }

    /// Create an OPTE port for the given guest instance.
    pub fn create_guest_port(
        &self,
        instance_id: Uuid,
        nic: &NetworkInterface,
        source_nat: Option<SourceNatConfig>,
        external_ips: &[IpAddr],
        firewall_rules: &[VpcFirewallRule],
    ) -> Result<(Port, PortTicket), Error> {
        self.create_port_inner(
            PortType::Guest { id: instance_id },
            None,
            nic,
            source_nat,
            external_ips,
            firewall_rules,
        )
    }

    pub fn create_svc_port(
        &self,
        svc_id: Uuid,
        nic: &NetworkInterface,
        external_ip: IpAddr,
    ) -> Result<(Port, PortTicket), Error> {
        let port_name = format!("{}_{}{}", XDE_LINK_PREFIX, nic.name, nic.slot);
        self.create_port_inner(
            PortType::Service { id: svc_id },
            Some(port_name),
            nic,
            None,
            &[external_ip],
            &[],
        )
    }

    pub fn firewall_rules_ensure(
        &self,
        rules: &[VpcFirewallRule],
    ) -> Result<(), Error> {
        let hdl = OpteHdl::open(OpteHdl::XDE_CTL)?;
        for (_, port) in self.inner.ports.lock().unwrap().iter() {
            let mut rules =
                opte_firewall_rules(rules, &port.vni(), &port.mac());
            // TODO-remove: This is part of the external IP hack.
            //
            // We need to allow incoming ARP packets past the firewall layer so
            // that they may be handled properly at the gateway layer.
            rules.push(
                "dir=in priority=65534 protocol=arp action=allow"
                    .parse()
                    .unwrap(),
            );
            let port_name = port.name();
            info!(
                self.inner.log,
                "Setting OPTE firewall rules";
                "port" => ?&(port.key(), port_name),
                "rules" => ?&rules,
            );
            hdl.set_fw_rules(&SetFwRulesReq {
                port_name: port_name.to_string(),
                rules,
            })?;
        }
        Ok(())
    }
}

pub struct PortTicket {
    port_key: PortKey,
    manager: Option<Arc<PortManagerInner>>,
}

impl std::fmt::Debug for PortTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.manager.is_some() {
            f.debug_struct("PortTicket")
                .field("port_key", &self.port_key)
                .field("manager", &"{ .. }")
                .finish()
        } else {
            f.debug_struct("PortTicket")
                .field("port_key", &self.port_key)
                .finish()
        }
    }
}

impl PortTicket {
    fn new(port_key: PortKey, manager: Arc<PortManagerInner>) -> Self {
        Self { port_key, manager: Some(manager) }
    }

    pub fn release(&mut self) -> Result<(), Error> {
        if let Some(manager) = self.manager.take() {
            let mut ports = manager.ports.lock().unwrap();
            ports.remove(&self.port_key);
            debug!(
                manager.log,
                "Removed OPTE port from manager";
                "port_key" => ?self.port_key,
            );
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
