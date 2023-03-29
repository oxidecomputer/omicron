// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Manager for all OPTE ports on a Helios system

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
use omicron_common::api::external::IpNet;
use slog::debug;
use slog::error;
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

    #[allow(clippy::too_many_arguments)]
    fn create_port_inner(
        &self,
        port_type: PortType,
        port_name: Option<String>,
        nic: &NetworkInterface,
        source_nat: Option<SourceNatConfig>,
        external_ips: &[IpAddr],
        _firewall_rules: &[VpcFirewallRule],
    ) -> Result<(Port, PortTicket), Error> {
        let subnet = IpNetwork::from(nic.subnet);
        let mac = *nic.mac;
        let vni = Vni::new(nic.vni).unwrap();
        let gateway = Gateway::from_subnet(&subnet);

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

        let port_name =
            port_name.unwrap_or_else(|| self.inner.next_port_name());
        let vnic = format!("v{}", port_name);

        let snat_ip = source_nat.map(|sn| sn.ip);
        match (nic.ip, nic.subnet, snat_ip, external_ip) {
            (
                IpAddr::V4(_),
                IpNet::V4(_),
                None | Some(IpAddr::V4(_)),
                None | Some(IpAddr::V4(_)),
            ) => {}
            (
                IpAddr::V6(_),
                IpNet::V6(_),
                None | Some(IpAddr::V6(_)),
                None | Some(IpAddr::V6(_)),
            ) => {}
            _ => {
                error!(
                    self.inner.log,
                    "Mismatched IP configuration";
                    "port_name" => &port_name,
                    "ip" => ?nic.ip,
                    "subnet" => ?nic.subnet,
                    "snat_ip" => ?snat_ip,
                    "external_ip" => ?external_ip,
                );
                return Err(Error::InvalidPortIpConfig);
            }
        }

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
        info!(self.inner.log, "Ignoring {} firewall rules", rules.len());
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
                "Removing OPTE ports from manager";
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
