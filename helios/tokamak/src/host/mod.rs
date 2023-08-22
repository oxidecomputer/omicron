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
use std::str::FromStr;

mod dladm;
mod ipadm;
mod route;
mod svcadm;
mod svccfg;
mod zfs;
mod zoneadm;
mod zonecfg;
mod zpool;

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

#[derive(Debug, PartialEq, Eq)]
pub enum RouteTarget {
    Default,
    DefaultV4,
    DefaultV6,
    ByAddress(IpNetwork),
}

impl RouteTarget {
    fn shift_target(input: &mut Input) -> Result<Self, String> {
        let force_v4 = shift_arg_if(input, "-inet")?;
        let force_v6 = shift_arg_if(input, "-inet6")?;

        let target = match (force_v4, force_v6, shift_arg(input)?.as_str()) {
            (true, true, _) => {
                return Err("Cannot force both v4 and v6".to_string())
            }
            (true, false, "default") => RouteTarget::DefaultV4,
            (false, true, "default") => RouteTarget::DefaultV6,
            (false, false, "default") => RouteTarget::Default,
            (_, _, other) => {
                let net =
                    IpNetwork::from_str(other).map_err(|e| e.to_string())?;
                if force_v4 && !net.is_ipv4() {
                    return Err(format!("{net} is not ipv4"));
                }
                if force_v6 && !net.is_ipv6() {
                    return Err(format!("{net} is not ipv6"));
                }
                RouteTarget::ByAddress(net)
            }
        };
        Ok(target)
    }
}

pub struct FilesystemName(String);

enum KnownCommand {
    Dladm(dladm::Command),
    Ipadm(ipadm::Command),
    Fstyp,
    RouteAdm,
    Route(route::Command),
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
            shift_program(&mut input)?;
        }
        if input.program == ZLOGIN {
            shift_program(&mut input)?;
            in_zone = Some(ZoneName(shift_program(&mut input)?));
        }

        let cmd = match input.program.as_str() {
            DLADM => KnownCommand::Dladm(dladm::Command::try_from(input)?),
            IPADM => KnownCommand::Ipadm(ipadm::Command::try_from(input)?),
            ROUTE => KnownCommand::Route(route::Command::try_from(input)?),
            SVCCFG => KnownCommand::Svccfg(svccfg::Command::try_from(input)?),
            SVCADM => KnownCommand::Svcadm(svcadm::Command::try_from(input)?),
            ZFS => KnownCommand::Zfs(zfs::Command::try_from(input)?),
            ZONEADM => {
                KnownCommand::Zoneadm(zoneadm::Command::try_from(input)?)
            }
            ZONECFG => {
                KnownCommand::Zonecfg(zonecfg::Command::try_from(input)?)
            }
            ZPOOL => KnownCommand::Zpool(zpool::Command::try_from(input)?),
            _ => return Err(format!("Unknown command: {}", input.program)),
        };

        Ok(Command { with_pfexec, in_zone, cmd })
    }
}

// Shifts out the program, putting the subsequent argument in its place.
//
// Returns the prior program value.
pub(crate) fn shift_program(input: &mut Input) -> Result<String, String> {
    let new = input
        .args
        .pop_front()
        .ok_or_else(|| format!("Failed to parse {input}"))?;

    let old = std::mem::replace(&mut input.program, new);

    Ok(old)
}

pub(crate) fn no_args_remaining(input: &Input) -> Result<(), String> {
    if !input.args.is_empty() {
        return Err(format!("Unexpected extra arguments: {input}"));
    }
    Ok(())
}

// Removes the next argument unconditionally.
pub(crate) fn shift_arg(input: &mut Input) -> Result<String, String> {
    Ok(input.args.pop_front().ok_or_else(|| "Missing argument")?)
}

// Removes the next argument, which must equal the provided value.
pub(crate) fn shift_arg_expect(
    input: &mut Input,
    value: &str,
) -> Result<(), String> {
    let v = input.args.pop_front().ok_or_else(|| "Missing argument")?;
    if value != v {
        return Err(format!("Unexpected argument {v} (expected: {value}"));
    }
    Ok(())
}

// Removes the next argument if it equals `value`.
//
// Returns if it was equal.
pub(crate) fn shift_arg_if(
    input: &mut Input,
    value: &str,
) -> Result<bool, String> {
    let eq = input.args.front().ok_or_else(|| "Missing argument")? == value;
    if eq {
        input.args.pop_front();
    }
    Ok(eq)
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
