// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Mock / empty interface to the Oxide Packet Transformation Engine (OPTE), for
//! building the sled agent on non-illumos systems.

use crate::illumos::vnic::Vnic;
use crate::params::ExternalIp;
use ipnetwork::IpNetwork;
use macaddr::MacAddr6;
use slog::Logger;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub struct Vni(u32);

impl Vni {
    pub fn new<N>(n: N) -> Result<Self, Error>
    where
        N: Into<u32>,
    {
        let x = n.into();
        if x <= 0x00_FF_FF_FF {
            Ok(Self(x))
        } else {
            Err(Error::InvalidArgument(format!("invalid VNI: {}", x)))
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
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
        external_ip: Option<ExternalIp>,
    ) -> Result<OptePort, Error> {
        // TODO-completess: Remove IPv4 restrictions once OPTE supports virtual
        // IPv6 networks.
        if matches!(ip, IpAddr::V6(_)) {
            return Err(Error::InvalidArgument(String::from(
                "IPv6 not yet supported",
            )));
        }
        let gateway = Gateway::from_subnet(&subnet);
        if matches!(gateway.ip, IpAddr::V6(_)) {
            return Err(Error::InvalidArgument(String::from(
                "IPv6 not yet supported",
            )));
        }
        let boundary_services = BoundaryServices::default();
        let name = self.next();
        if matches!(subnet.network(), IpAddr::V6(_)) {
            return Err(Error::InvalidArgument(String::from(
                "IPv6 not yet supported",
            )));
        }
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
            vnic: None,
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
#[allow(dead_code)]
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
    external_ip: Option<ExternalIp>,
    gateway: Gateway,
    boundary_services: BoundaryServices,
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
    }
}

pub fn initialize_xde_driver(log: &Logger) -> Result<(), Error> {
    slog::warn!(log, "`xde` driver is a fiction on non-illumos systems");
    Ok(())
}

pub fn delete_all_xde_devices(log: &Logger) -> Result<(), Error> {
    slog::warn!(log, "`xde` driver is a fiction on non-illumos systems");
    Ok(())
}
