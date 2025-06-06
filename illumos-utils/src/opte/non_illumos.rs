// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Mock / dummy versions of the OPTE module, for non-illumos platforms

use crate::addrobj::AddrObject;
use omicron_common::api::internal::shared::NetworkInterfaceKind;
use oxide_vpc::api::AddRouterEntryReq;
use oxide_vpc::api::ClearVirt2PhysReq;
use oxide_vpc::api::DelRouterEntryReq;
use oxide_vpc::api::DhcpCfg;
use oxide_vpc::api::Direction;
use oxide_vpc::api::DumpVirt2PhysResp;
use oxide_vpc::api::IpCfg;
use oxide_vpc::api::IpCidr;
use oxide_vpc::api::ListPortsResp;
use oxide_vpc::api::NoResp;
use oxide_vpc::api::PortInfo;
use oxide_vpc::api::RouterClass;
use oxide_vpc::api::RouterTarget;
use oxide_vpc::api::SetExternalIpsReq;
use oxide_vpc::api::SetFwRulesReq;
use oxide_vpc::api::SetVirt2PhysReq;
use oxide_vpc::api::VpcCfg;
use slog::Logger;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::net::IpAddr;
use std::sync::Mutex;
use std::sync::OnceLock;

type OpteError = anyhow::Error;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failure interacting with the dummy OPTE implementation")]
    Opte(#[from] OpteError),

    #[error("Invalid IP configuration for port")]
    InvalidPortIpConfig,

    #[error("Tried to release non-existent port ({0}, {1:?})")]
    ReleaseMissingPort(uuid::Uuid, NetworkInterfaceKind),

    #[error("Tried to update external IPs on non-existent port ({0}, {1:?})")]
    ExternalIpUpdateMissingPort(uuid::Uuid, NetworkInterfaceKind),

    #[error("Could not find Primary NIC")]
    NoPrimaryNic,

    #[error("Can't attach new ephemeral IP {0}, currently have {1}")]
    ImplicitEphemeralIpDetach(IpAddr, IpAddr),

    #[error("No matching NIC found for port {0} at slot {1}.")]
    NoNicforPort(String, u32),
}

pub fn initialize_xde_driver(
    log: &Logger,
    _underlay_nics: &[AddrObject],
) -> Result<(), Error> {
    slog::warn!(log, "`xde` driver is a fiction on non-illumos systems");
    Ok(())
}

pub fn delete_all_xde_devices(log: &Logger) -> Result<(), Error> {
    slog::warn!(log, "`xde` driver is a fiction on non-illumos systems");
    Ok(())
}

#[derive(Debug)]
pub(crate) struct RouteInfo {
    pub dest: IpCidr,
    pub target: RouterTarget,
    pub class: RouterClass,
}

// NOTE: It would be nice to derive this, but `RouterTarget` and `RouterClass`
// are in OPTE, and they don't currently implement this trait.
impl PartialEq for RouteInfo {
    fn eq(&self, other: &Self) -> bool {
        if self.dest != other.dest {
            return false;
        }
        match (self.class, other.class) {
            (RouterClass::System, RouterClass::Custom) => return false,
            (RouterClass::Custom, RouterClass::System) => return false,
            (RouterClass::System, RouterClass::System)
            | (RouterClass::Custom, RouterClass::Custom) => {}
        }
        match (self.target, other.target) {
            (RouterTarget::Drop, RouterTarget::Drop) => true,
            (
                RouterTarget::InternetGateway(id0),
                RouterTarget::InternetGateway(id1),
            ) => id0 == id1,
            (RouterTarget::Ip(ip0), RouterTarget::Ip(ip1)) => ip0 == ip1,
            (
                RouterTarget::VpcSubnet(cidr0),
                RouterTarget::VpcSubnet(cidr1),
            ) => cidr0 == cidr1,
            (RouterTarget::Drop, RouterTarget::InternetGateway(_))
            | (RouterTarget::Drop, RouterTarget::Ip(_))
            | (RouterTarget::Drop, RouterTarget::VpcSubnet(_))
            | (RouterTarget::InternetGateway(_), RouterTarget::Drop)
            | (RouterTarget::InternetGateway(_), RouterTarget::Ip(_))
            | (RouterTarget::InternetGateway(_), RouterTarget::VpcSubnet(_))
            | (RouterTarget::Ip(_), RouterTarget::Drop)
            | (RouterTarget::Ip(_), RouterTarget::InternetGateway(_))
            | (RouterTarget::Ip(_), RouterTarget::VpcSubnet(_))
            | (RouterTarget::VpcSubnet(_), RouterTarget::Drop)
            | (RouterTarget::VpcSubnet(_), RouterTarget::InternetGateway(_))
            | (RouterTarget::VpcSubnet(_), RouterTarget::Ip(_)) => false,
        }
    }
}

impl RouteInfo {
    #[cfg(test)]
    pub fn is_system_default_ipv4_route(&self) -> bool {
        let system_default_route = RouteInfo {
            dest: IpCidr::Ip4(oxide_vpc::api::Ipv4Cidr::new(
                oxide_vpc::api::Ipv4Addr::ANY_ADDR,
                0.try_into().unwrap(),
            )),
            target: RouterTarget::InternetGateway(None),
            class: RouterClass::System,
        };
        *self == system_default_route
    }
}

impl From<&AddRouterEntryReq> for RouteInfo {
    fn from(value: &AddRouterEntryReq) -> Self {
        Self { dest: value.dest, target: value.target, class: value.class }
    }
}
impl From<&DelRouterEntryReq> for RouteInfo {
    fn from(value: &DelRouterEntryReq) -> Self {
        Self { dest: value.dest, target: value.target, class: value.class }
    }
}

/// Data for one OPTE port
#[derive(Debug)]
pub(crate) struct PortData {
    /// The OPTE-layer information
    #[cfg_attr(test, allow(dead_code))]
    pub port: PortInfo,
    /// The routes for this port. This simulates the router layer.
    pub routes: Vec<RouteInfo>,
}

#[derive(Debug)]
pub(crate) struct State {
    pub ports: HashMap<String, PortData>,
    pub underlay_initialized: bool,
}

const NO_RESPONSE: NoResp = NoResp { unused: 99 };
static OPTE_STATE: OnceLock<Mutex<State>> = OnceLock::new();

fn opte_state() -> &'static Mutex<State> {
    OPTE_STATE.get_or_init(|| {
        Mutex::new(State { ports: HashMap::new(), underlay_initialized: false })
    })
}

/// Simulated handle to OPTE
pub struct Handle;

impl Handle {
    /// Create a new handle to the test OPTE kernel module.
    pub fn new() -> Result<Self, OpteError> {
        Ok(Self)
    }

    /// Helper to get the OPTE state.
    #[cfg(test)]
    pub(crate) fn state(&self) -> &'static Mutex<State> {
        opte_state()
    }

    /// Add a new port.
    pub fn create_xde(
        &self,
        name: &str,
        cfg: VpcCfg,
        _: DhcpCfg,
        _: bool,
    ) -> Result<NoResp, OpteError> {
        let name = name.to_string();
        let IpCfg::Ipv4(ip_cfg) = cfg.ip_cfg else {
            unimplemented!("IPv6 support");
        };
        let ephemeral_ip4_addr =
            ip_cfg.external_ips.snat.as_ref().map(|snat| snat.external_ip);
        let port = PortInfo {
            name: name.clone(),
            mac_addr: cfg.guest_mac,
            ip4_addr: Some(ip_cfg.private_ip),
            ephemeral_ip4_addr,
            floating_ip4_addrs: None,
            ip6_addr: None,
            ephemeral_ip6_addr: None,
            floating_ip6_addrs: None,
            state: "created".to_string(),
        };
        let mut state = opte_state().lock().unwrap();
        anyhow::ensure!(
            state.underlay_initialized,
            "Underlay is not initialized"
        );
        match state.ports.entry(name) {
            Entry::Occupied(entry) => {
                anyhow::bail!("Duplicate OPTE port: '{}'", entry.key());
            }
            Entry::Vacant(entry) => {
                entry.insert(PortData { port, routes: Vec::new() });
            }
        }
        Ok(NO_RESPONSE)
    }

    pub fn delete_xde(&self, name: &str) -> Result<NoResp, OpteError> {
        let _ = opte_state().lock().unwrap().ports.remove(name);
        Ok(NO_RESPONSE)
    }

    /// Set new firewall rules.
    pub fn set_firewall_rules(
        &self,
        _: &SetFwRulesReq,
    ) -> Result<NoResp, OpteError> {
        Ok(NO_RESPONSE)
    }

    /// Add a new router entry to OPTE.
    pub fn add_router_entry(
        &self,
        req: &AddRouterEntryReq,
    ) -> Result<NoResp, OpteError> {
        let mut inner = opte_state().lock().unwrap();
        let Some(PortData { routes, .. }) = inner.ports.get_mut(&req.port_name)
        else {
            anyhow::bail!("No such port '{}'", req.port_name);
        };
        routes.push(req.into());
        Ok(NO_RESPONSE)
    }

    /// Allow traffic to / from a CIDR block on a port.
    pub fn allow_cidr(
        &self,
        _: &str,
        _: IpCidr,
        _: Direction,
    ) -> Result<NoResp, OpteError> {
        unimplemented!("Not yet used in tests")
    }

    /// Delete a router entry from a port.
    pub fn del_router_entry(
        &self,
        req: &DelRouterEntryReq,
    ) -> Result<NoResp, OpteError> {
        let mut inner = opte_state().lock().unwrap();
        let Some(PortData { routes, .. }) = inner.ports.get_mut(&req.port_name)
        else {
            anyhow::bail!("No such port '{}'", req.port_name);
        };
        let req = RouteInfo::from(req);
        if let Some(index) = routes.iter().position(|rt| rt == &req) {
            routes.remove(index);
        }
        Ok(NO_RESPONSE)
    }

    /// Set the external IPs in use by a port.
    pub fn set_external_ips(
        &self,
        _: &SetExternalIpsReq,
    ) -> Result<NoResp, OpteError> {
        unimplemented!("Not yet used in tests")
    }

    /// Set a mapping from a virtual NIC to a physical host.
    pub fn set_v2p(&self, _: &SetVirt2PhysReq) -> Result<NoResp, OpteError> {
        unimplemented!("Not yet used in tests")
    }

    /// Dump a mapping from a virtual NIC to a physical host.
    pub fn dump_v2p(&self) -> Result<DumpVirt2PhysResp, OpteError> {
        unimplemented!("Not yet used in tests")
    }

    /// Clear a mapping from a virtual NIC to a physical host.
    pub fn clear_v2p(
        &self,
        _: &ClearVirt2PhysReq,
    ) -> Result<NoResp, OpteError> {
        unimplemented!("Not yet used in tests")
    }

    /// List ports on the current system.
    #[allow(dead_code)]
    pub(crate) fn list_ports(&self) -> Result<ListPortsResp, OpteError> {
        let ports = opte_state()
            .lock()
            .unwrap()
            .ports
            .values()
            .map(|data| {
                let info = &data.port;
                PortInfo {
                    name: info.name.clone(),
                    mac_addr: info.mac_addr,
                    ip4_addr: info.ip4_addr,
                    ephemeral_ip4_addr: info.ephemeral_ip4_addr,
                    floating_ip4_addrs: info.floating_ip4_addrs.clone(),
                    ip6_addr: info.ip6_addr,
                    ephemeral_ip6_addr: info.ephemeral_ip6_addr,
                    floating_ip6_addrs: info.floating_ip6_addrs.clone(),
                    state: info.state.clone(),
                }
            })
            .collect();
        Ok(ListPortsResp { ports })
    }

    /// Set the underlay devices on the system.
    #[allow(dead_code)]
    pub(crate) fn set_xde_underlay(
        &self,
        _: &str,
        _: &str,
    ) -> Result<NoResp, OpteError> {
        let mut state = opte_state().lock().unwrap();
        anyhow::ensure!(
            !state.underlay_initialized,
            "Underlay is already initialized"
        );
        state.underlay_initialized = true;
        Ok(NO_RESPONSE)
    }
}
