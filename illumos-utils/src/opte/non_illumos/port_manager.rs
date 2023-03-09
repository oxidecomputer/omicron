// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manager for all OPTE ports on a Helios system

use crate::dladm::PhysicalLink;
use crate::opte::params::NetworkInterface;
use crate::opte::params::SourceNatConfig;
use crate::opte::params::VpcFirewallRule;
use crate::opte::Error;
use crate::opte::Gateway;
use crate::opte::Port;
use crate::opte::PortType;
use crate::opte::Vni;
use ipnetwork::IpNetwork;
use ipnetwork::Ipv4Network;
use macaddr::MacAddr6;
use slog::debug;
use slog::info;
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
#[allow(dead_code)]
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

    // Map of all ports, keyed on the instance/service Uuid and the port name.
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
        data_link: PhysicalLink,
        underlay_ip: Ipv6Addr,
        gateway_mac: MacAddr6,
    ) -> Self {
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

    #[allow(clippy::too_many_arguments)]
    fn create_port_inner(
        &self,
        id: Uuid,
        port_type: PortType,
        port_name: Option<String>,
        ip: IpAddr,
        mac: MacAddr6,
        vni: Vni,
        gateway: Gateway,
        _firewall_rules: &[VpcFirewallRule],
        externally_visible: bool,
    ) -> Result<(Port, PortTicket), Error> {
        let port_name =
            port_name.unwrap_or_else(|| self.inner.next_port_name());
        let vnic = format!("v{}", port_name);

        let (port, ticket) = {
            let mut ports = self.inner.ports.lock().unwrap();
            let ticket =
                PortTicket::new(id, port_name.clone(), self.inner.clone());
            let port = Port::new(
                port_name.clone(),
                port_type,
                ip,
                mac,
                vni,
                gateway,
                vnic,
                externally_visible,
            );
            let old = ports.insert((id, port_name.clone()), port.clone());
            assert!(
                old.is_none(),
                "Duplicate OPTE port detected: id = {}, port_name = {}",
                id,
                &port_name,
            );
            (port, ticket)
        };

        info!(
            self.inner.log,
            "Created OPTE port";
            "port_type" => ?port_type,
            "port" => ?&port,
        );

        Ok((port, ticket))
    }

    pub fn create_guest_port(
        &self,
        instance_id: Uuid,
        nic: &NetworkInterface,
        _source_nat: Option<SourceNatConfig>,
        external_ips: Option<Vec<IpAddr>>,
        firewall_rules: &[VpcFirewallRule],
    ) -> Result<(Port, PortTicket), Error> {
        // TODO-completeness: Remove IPv4 restrictions once OPTE supports
        // virtual IPv6 networks.
        let ip = match nic.ip {
            IpAddr::V4(_) => nic.ip,
            IpAddr::V6(_) => {
                return Err(Error::InvalidArgument(String::from(
                    "IPv6 is not yet supported for guest interfaces",
                )))
            }
        };

        // Argument checking and conversions into OPTE data types.
        let subnet = IpNetwork::from(nic.subnet);
        let mac = *nic.mac;
        let vni = Vni::new(nic.vni).unwrap();
        let gateway = match subnet {
            IpNetwork::V4(_) => Gateway::from_subnet(&subnet),
            IpNetwork::V6(_) => {
                return Err(Error::InvalidArgument(String::from(
                    "IPv6 is not yet supported for guest interfaces",
                )));
            }
        };
        let _ = match gateway.ip {
            IpAddr::V4(ip) => Ok(ip),
            IpAddr::V6(_) => Err(Error::InvalidArgument(String::from(
                "IPv6 is not yet supported for guest interfaces",
            ))),
        }?;

        self.create_port_inner(
            instance_id,
            PortType::Guest { slot: nic.slot },
            None,
            ip,
            mac,
            vni,
            gateway,
            firewall_rules,
            external_ips.is_some(),
        )
    }

    pub fn create_svc_port(
        &self,
        svc_id: Uuid,
        svc_name: &str,
        mac: MacAddr6,
        _external_ip: IpAddr,
        vni: u32,
    ) -> Result<(Port, PortTicket), Error> {
        // TODO: Assume each service zone has a single port.
        let port_name = format!("{}_{}0", XDE_LINK_PREFIX, svc_name);

        // TODO: Arbitrary subnet, should come from a "service" VPC.
        let vpc_subnet: Ipv4Network = "10.0.0.0/24".parse().unwrap();
        // Use first non-reserved IP, see RFD 21, section 2.2, table 1.
        let private_ip = vpc_subnet.iter().nth(5).unwrap();
        let gateway = Gateway::from_subnet(&vpc_subnet.into());
        let vni = Vni::new(vni).unwrap();

        self.create_port_inner(
            svc_id,
            PortType::Service,
            Some(port_name),
            private_ip.into(),
            mac,
            vni,
            gateway,
            &[],
            /* externally_visible= */ true,
        )
    }

    pub fn firewall_rules_ensure(
        &self,
        rules: &[VpcFirewallRule],
    ) -> Result<(), Error> {
        info!(self.inner.log, "Ignoring {} firewall rules", rules.len());
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
                "Removing OPTE ports from manager";
                "id" => ?self.id,
                "port_name" => &self.port_name,

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
