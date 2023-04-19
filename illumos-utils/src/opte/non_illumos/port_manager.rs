// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manager for all OPTE ports on a Helios system

use crate::opte::default_boundary_services;
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

    pub fn create_port(
        &self,
        instance_id: Uuid,
        nic: &NetworkInterface,
        source_nat: Option<SourceNatConfig>,
        external_ips: Option<Vec<IpAddr>>,
        _firewall_rules: &[VpcFirewallRule],
    ) -> Result<(Port, PortTicket), Error> {
        // TODO-completeness: Remove IPv4 restrictions once OPTE supports
        // virtual IPv6 networks.
        let _ = match nic.ip {
            IpAddr::V4(ip) => Ok(ip),
            IpAddr::V6(_) => Err(Error::InvalidArgument(String::from(
                "IPv6 is not yet supported for guest interfaces",
            ))),
        }?;

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
        let boundary_services = default_boundary_services();
        let port_name = self.inner.next_port_name();
        let vnic = format!("v{}", port_name);
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
        info!(self.inner.log, "Ignoring {} firewall rules", rules.len());
        Ok(())
    }

    pub fn set_virtual_nic_host(
        &self,
        _mapping: &SetVirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        info!(self.inner.log, "Ignoring virtual NIC mapping");
        Ok(())
    }

    pub fn unset_virtual_nic_host(
        &self,
        _mapping: &SetVirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        info!(self.inner.log, "Ignoring unset of virtual NIC mapping");
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
                "instance_id" => ?self.id,
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
