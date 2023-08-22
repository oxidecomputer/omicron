// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Emulates an illumos system

// TODO REMOVE ME
#![allow(dead_code)]
#![allow(unused_mut)]
#![allow(unused_variables)]

use camino::Utf8PathBuf;
use helios_fusion::zpool::ZpoolName;
use helios_fusion::Input;
use helios_fusion::{
    DLADM, IPADM, PFEXEC, ROUTE, SVCADM, SVCCFG, ZFS, ZLOGIN, ZONEADM, ZONECFG,
    ZPOOL,
};
use ipnetwork::IpNetwork;
use std::collections::{HashMap, HashSet};

// Parsing command-line utilities
mod dladm;
mod ipadm;
mod route;
mod svcadm;
mod svccfg;
mod zfs;
mod zoneadm;
mod zonecfg;
mod zpool;

mod parse;

use crate::host::parse::InputExt;

enum LinkType {
    Etherstub,
    Vnic,
}

#[derive(Debug, PartialEq, Eq)]
pub struct LinkName(String);
struct Link {
    ty: LinkType,
    parent: Option<LinkName>,
    properties: HashMap<String, String>,
}

pub struct IpInterfaceName(String);
struct IpInterface {}

enum RouteDestination {
    Default,
    Literal(IpNetwork),
}

struct Route {
    destination: RouteDestination,
    gateway: IpNetwork,
}

#[derive(Debug)]
pub struct ServiceName(String);

struct Service {
    state: smf::SmfState,
    properties: HashMap<smf::PropertyName, smf::PropertyValue>,
}

struct ZoneEnvironment {
    id: u64,
    links: HashMap<LinkName, Link>,
    ip_interfaces: HashMap<IpInterfaceName, IpInterface>,
    routes: Vec<Route>,
    services: HashMap<ServiceName, Service>,
}

impl ZoneEnvironment {
    fn new(id: u64) -> Self {
        Self {
            id,
            links: HashMap::new(),
            ip_interfaces: HashMap::new(),
            routes: vec![],
            services: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct ZoneName(String);

pub struct ZoneConfig {
    state: zone::State,
    brand: String,
    // zonepath
    path: Utf8PathBuf,
    datasets: Vec<zone::Dataset>,
    devices: Vec<zone::Device>,
    nets: Vec<zone::Net>,
    fs: Vec<zone::Fs>,
    // E.g. zone image, overlays, etc.
    layers: Vec<Utf8PathBuf>,
}

struct Zone {
    config: ZoneConfig,
    environment: ZoneEnvironment,
}

struct Host {
    global: ZoneEnvironment,
    zones: HashMap<ZoneName, Zone>,

    // TODO: Is this the right abstraction layer?
    // How do you want to represent zpools & filesystems?
    //
    // TODO: Should filesystems be part of the "ZoneEnvironment" abstraction?
    zpools: HashSet<ZpoolName>,
}

impl Host {
    pub fn new() -> Self {
        Self {
            global: ZoneEnvironment::new(0),
            zones: HashMap::new(),
            zpools: HashSet::new(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum AddrType {
    Dhcp,
    Static(IpNetwork),
    Addrconf,
}

pub struct FilesystemName(String);

enum KnownCommand {
    Coreadm, // TODO
    Dladm(dladm::Command),
    Dumpadm, // TODO
    Ipadm(ipadm::Command),
    Fstyp,    // TODO
    RouteAdm, // TODO
    Route(route::Command),
    Savecore, // TODO
    Svccfg(svccfg::Command),
    Svcadm(svcadm::Command),
    Zfs(zfs::Command),
    Zoneadm(zoneadm::Command),
    Zonecfg(zonecfg::Command),
    Zpool(zpool::Command),
}

struct Command {
    with_pfexec: bool,
    in_zone: Option<ZoneName>,
    cmd: KnownCommand,
}

impl TryFrom<Input> for Command {
    type Error = String;

    fn try_from(mut input: Input) -> Result<Self, Self::Error> {
        let mut with_pfexec = false;
        let mut in_zone = None;

        while input.program == PFEXEC {
            with_pfexec = true;
            input.shift_program()?;
        }
        if input.program == ZLOGIN {
            input.shift_program()?;
            in_zone = Some(ZoneName(input.shift_program()?));
        }

        use KnownCommand::*;
        let cmd = match input.program.as_str() {
            DLADM => Dladm(dladm::Command::try_from(input)?),
            IPADM => Ipadm(ipadm::Command::try_from(input)?),
            ROUTE => Route(route::Command::try_from(input)?),
            SVCCFG => Svccfg(svccfg::Command::try_from(input)?),
            SVCADM => Svcadm(svcadm::Command::try_from(input)?),
            ZFS => Zfs(zfs::Command::try_from(input)?),
            ZONEADM => Zoneadm(zoneadm::Command::try_from(input)?),
            ZONECFG => Zonecfg(zonecfg::Command::try_from(input)?),
            ZPOOL => Zpool(zpool::Command::try_from(input)?),
            _ => return Err(format!("Unknown command: {}", input.program)),
        };

        Ok(Command { with_pfexec, in_zone, cmd })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn empty_state() {
        let host = Host::new();

        assert_eq!(0, host.global.id);
        assert!(host.global.links.is_empty());
        assert!(host.global.ip_interfaces.is_empty());
        assert!(host.global.routes.is_empty());
        assert!(host.global.services.is_empty());
        assert!(host.zones.is_empty());
    }
}
