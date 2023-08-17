// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Emulates an illumos system

// TODO REMOVE ME
#![allow(dead_code)]

use crate::addrobj::AddrObject;
use crate::dladm::DLADM;
use crate::host::input::Input;
use crate::host::PFEXEC;
use crate::zfs::ZFS;
use crate::zone::IPADM;
use crate::zone::SVCADM;
use crate::zone::SVCCFG;
use crate::zone::ZLOGIN;
use crate::zone::ZONEADM;
use crate::zone::ZONECFG;
use crate::zpool::ZPOOL;
use crate::zpool::ZpoolName;
use crate::ROUTE;
use camino::Utf8PathBuf;
use ipnetwork::IpNetwork;
use omicron_common::vlan::VlanID;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

enum LinkType {
    Etherstub,
    Vnic,
}

#[derive(Debug, PartialEq, Eq)]
struct LinkName(String);
struct Link {
    ty: LinkType,
    parent: Option<LinkName>,
    properties: HashMap<String, String>,
}

struct IpInterfaceName(String);
struct IpInterface {}

enum RouteDestination {
    Default,
    Literal(IpNetwork),
}

struct Route {
    destination: RouteDestination,
    gateway: IpNetwork,
}

struct ServiceName(String);

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
struct ZoneName(String);

struct ZoneConfig {
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

#[derive(Debug)]
enum DladmCommand {
    CreateVnic {
        link: LinkName,
        temporary: bool,
        mac: Option<String>,
        vlan: Option<VlanID>,
        name: LinkName,
        properties: HashMap<String, String>,
    },
    CreateEtherstub {
        temporary: bool,
        name: LinkName,
    },
    DeleteEtherstub {
        temporary: bool,
        name: LinkName,
    },
    DeleteVnic {
        temporary: bool,
        name: LinkName,
    },
    ShowEtherstub {
        name: Option<LinkName>,
    },
    ShowLink {
        name: LinkName,
        fields: Vec<String>,
    },
    ShowPhys {
        mac: bool,
        fields: Vec<String>,
        name: Option<LinkName>,
    },
    ShowVnic {
        fields: Option<Vec<String>>,
        name: Option<LinkName>,
    },
    SetLinkprop {
        temporary: bool,
        properties: HashMap<String, String>,
        name: LinkName,
    },
}

impl TryFrom<Input> for DladmCommand {
    type Error = String;

    fn try_from(mut input: Input) -> Result<Self, Self::Error> {
        if input.program != DLADM {
            return Err(format!("Not dladm command: {}", input.program));
        }

        match shift_arg(&mut input)?.as_str() {
            "create-vnic" => {
                let mut link = None;
                let mut temporary = false;
                let mut mac = None;
                let mut vlan = None;
                let mut properties = HashMap::new();
                let name = LinkName(
                    input.args.pop_back().ok_or_else(|| "Missing name")?,
                );

                while !input.args.is_empty() {
                    if shift_arg_if(&mut input, "-t")? {
                        temporary = true;
                    } else if shift_arg_if(&mut input, "-p")? {
                        let props = shift_arg(&mut input)?;
                        let props = props.split(',');
                        for prop in props {
                            let (k, v) =
                                prop.split_once('=').ok_or_else(|| {
                                    format!("Bad property: {prop}")
                                })?;
                            properties.insert(k.to_string(), v.to_string());
                        }
                    } else if shift_arg_if(&mut input, "-m")? {
                        // NOTE: Not yet supporting the keyword-based MACs.
                        mac = Some(shift_arg(&mut input)?);
                    } else if shift_arg_if(&mut input, "-l")? {
                        link = Some(LinkName(shift_arg(&mut input)?));
                    } else if shift_arg_if(&mut input, "-v")? {
                        vlan = Some(
                            VlanID::from_str(&shift_arg(&mut input)?)
                                .map_err(|e| e.to_string())?,
                        );
                    } else {
                        return Err(format!("Invalid arguments {}", input));
                    }
                }

                Ok(Self::CreateVnic {
                    link: link.ok_or_else(|| "Missing link")?,
                    temporary,
                    mac,
                    vlan,
                    name,
                    properties,
                })
            }
            "create-etherstub" => {
                let mut temporary = false;
                let name = LinkName(
                    input.args.pop_back().ok_or_else(|| "Missing name")?,
                );
                while !input.args.is_empty() {
                    if shift_arg_if(&mut input, "-t")? {
                        temporary = true;
                    } else {
                        return Err(format!("Invalid arguments {}", input));
                    }
                }
                Ok(Self::CreateEtherstub { temporary, name })
            }
            "delete-etherstub" => {
                let mut temporary = false;
                let name = LinkName(
                    input.args.pop_back().ok_or_else(|| "Missing name")?,
                );
                while !input.args.is_empty() {
                    if shift_arg_if(&mut input, "-t")? {
                        temporary = true;
                    } else {
                        return Err(format!("Invalid arguments {}", input));
                    }
                }
                Ok(Self::DeleteEtherstub { temporary, name })
            }
            "delete-vnic" => {
                let mut temporary = false;
                let name = LinkName(
                    input.args.pop_back().ok_or_else(|| "Missing name")?,
                );
                while !input.args.is_empty() {
                    if shift_arg_if(&mut input, "-t")? {
                        temporary = true;
                    } else {
                        return Err(format!("Invalid arguments {}", input));
                    }
                }
                Ok(Self::DeleteVnic { temporary, name })
            }
            "show-etherstub" => {
                let name = input.args.pop_back().map(|s| LinkName(s));
                no_args_remaining(&input)?;
                Ok(Self::ShowEtherstub { name })
            }
            "show-link" => {
                let name = LinkName(
                    input.args.pop_back().ok_or_else(|| "Missing name")?,
                );
                if !shift_arg_if(&mut input, "-p")? {
                    return Err(
                        "You should ask for parseable output ('-p')".into()
                    );
                }
                if !shift_arg_if(&mut input, "-o")? {
                    return Err(
                        "You should ask for specific outputs ('-o')".into()
                    );
                }
                let fields = shift_arg(&mut input)?
                    .split(',')
                    .map(|s| s.to_string())
                    .collect();
                no_args_remaining(&input)?;

                Ok(Self::ShowLink { name, fields })
            }
            "show-phys" => {
                let mut mac = false;
                if shift_arg_if(&mut input, "-m")? {
                    mac = true;
                }
                if !shift_arg_if(&mut input, "-p")? {
                    return Err(
                        "You should ask for parseable output ('-p')".into()
                    );
                }
                if !shift_arg_if(&mut input, "-o")? {
                    return Err(
                        "You should ask for specific outputs ('-o')".into()
                    );
                }
                let fields = shift_arg(&mut input)?
                    .split(',')
                    .map(|s| s.to_string())
                    .collect();
                let name = input.args.pop_front().map(|s| LinkName(s));
                no_args_remaining(&input)?;

                Ok(Self::ShowPhys { mac, fields, name })
            }
            "show-vnic" => {
                let mut fields = None;
                if shift_arg_if(&mut input, "-p")? {
                    if !shift_arg_if(&mut input, "-o")? {
                        return Err(
                            "You should ask for specific outputs ('-o')".into(),
                        );
                    }
                    fields = Some(
                        shift_arg(&mut input)?
                            .split(',')
                            .map(|s| s.to_string())
                            .collect(),
                    );
                }

                let name = input.args.pop_front().map(|s| LinkName(s));
                no_args_remaining(&input)?;
                Ok(Self::ShowVnic { fields, name })
            }
            "set-linkprop" => {
                let mut temporary = false;
                let mut properties = HashMap::new();
                let name = LinkName(
                    input.args.pop_back().ok_or_else(|| "Missing name")?,
                );

                while !input.args.is_empty() {
                    if shift_arg_if(&mut input, "-t")? {
                        temporary = true;
                    } else if shift_arg_if(&mut input, "-p")? {
                        let props = shift_arg(&mut input)?;
                        let props = props.split(',');
                        for prop in props {
                            let (k, v) =
                                prop.split_once('=').ok_or_else(|| {
                                    format!("Bad property: {prop}")
                                })?;
                            properties.insert(k.to_string(), v.to_string());
                        }
                    } else {
                        return Err(format!("Invalid arguments {}", input));
                    }
                }

                if properties.is_empty() {
                    return Err("Missing properties".into());
                }

                Ok(Self::SetLinkprop { temporary, properties, name })
            }
            command => Err(format!("Unsupported command: {}", command)),
        }
    }
}

#[derive(Debug, PartialEq)]
enum AddrType {
    Dhcp,
    Static(IpNetwork),
    Addrconf,
}

enum IpadmCommand {
    CreateAddr {
        temporary: bool,
        ty: AddrType,
        addrobj: AddrObject,
    },
    CreateIf {
        temporary: bool,
        name: IpInterfaceName,
    },
    DeleteAddr {
        addrobj: AddrObject,
    },
    DeleteIf {
        name: IpInterfaceName,
    },
    ShowIf {
        properties: Vec<String>,
        name: IpInterfaceName,
    },
    SetIfprop {
        temporary: bool,
        properties: HashMap<String, String>,
        module: String,
        name: IpInterfaceName,
    },
}

impl TryFrom<Input> for IpadmCommand {
    type Error = String;

    fn try_from(mut input: Input) -> Result<Self, Self::Error> {
        if input.program != IPADM {
            return Err(format!("Not ipadm command: {}", input.program));
        }

        match shift_arg(&mut input)?.as_str() {
            "create-addr" => {
                let temporary = shift_arg_if(&mut input, "-t")?;
                shift_arg_expect(&mut input, "-T")?;

                let ty = match shift_arg(&mut input)?.as_str() {
                    "static" => {
                        shift_arg_expect(&mut input, "-a")?;
                        let addr = shift_arg(&mut input)?;
                        AddrType::Static(
                            IpNetwork::from_str(&addr)
                                .map_err(|e| e.to_string())?,
                        )
                    }
                    "dhcp" => AddrType::Dhcp,
                    "addrconf" => AddrType::Addrconf,
                    ty => return Err(format!("Unknown address type {ty}")),
                };
                let addrobj = AddrObject::from_str(&shift_arg(&mut input)?)
                    .map_err(|e| e.to_string())?;
                no_args_remaining(&input)?;
                Ok(IpadmCommand::CreateAddr { temporary, ty, addrobj })
            }
            "create-ip" | "create-if" => {
                let temporary = shift_arg_if(&mut input, "-t")?;
                let name = IpInterfaceName(shift_arg(&mut input)?);
                no_args_remaining(&input)?;
                Ok(IpadmCommand::CreateIf { temporary, name })
            }
            "delete-addr" => {
                let addrobj = AddrObject::from_str(&shift_arg(&mut input)?)
                    .map_err(|e| e.to_string())?;
                no_args_remaining(&input)?;
                Ok(IpadmCommand::DeleteAddr { addrobj })
            }
            "delete-ip" | "delete-if" => {
                let name = IpInterfaceName(shift_arg(&mut input)?);
                no_args_remaining(&input)?;
                Ok(IpadmCommand::DeleteIf { name })
            }
            "show-if" => {
                let name = IpInterfaceName(
                    input.args.pop_back().ok_or_else(|| "Missing name")?,
                );
                let mut properties = vec![];
                while !input.args.is_empty() {
                    if shift_arg_if(&mut input, "-p")? {
                        shift_arg_expect(&mut input, "-o")?;
                        properties = shift_arg(&mut input)?
                            .split(',')
                            .map(|s| s.to_string())
                            .collect();
                    } else {
                        return Err(format!("Unexpected input: {input}"));
                    }
                }

                Ok(IpadmCommand::ShowIf { properties, name })
            }
            "set-ifprop" => {
                let name = IpInterfaceName(
                    input.args.pop_back().ok_or_else(|| "Missing name")?,
                );

                let mut temporary = false;
                let mut properties = HashMap::new();
                let mut module = "ip".to_string();

                while !input.args.is_empty() {
                    if shift_arg_if(&mut input, "-t")? {
                        temporary = true;
                    } else if shift_arg_if(&mut input, "-m")? {
                        module = shift_arg(&mut input)?;
                    } else if shift_arg_if(&mut input, "-p")? {
                        let props = shift_arg(&mut input)?;
                        let props = props.split(',');
                        for prop in props {
                            let (k, v) =
                                prop.split_once('=').ok_or_else(|| {
                                    format!("Bad property: {prop}")
                                })?;
                            properties.insert(k.to_string(), v.to_string());
                        }
                    } else {
                        return Err(format!("Unexpected input: {input}"));
                    }
                }

                Ok(IpadmCommand::SetIfprop {
                    temporary,
                    properties,
                    module,
                    name,
                })
            }
            command => return Err(format!("Unexpected command: {command}")),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum RouteTarget {
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

enum RouteCommand {
    Add {
        destination: RouteTarget,
        gateway: RouteTarget,
        interface: Option<LinkName>,
    },
}

impl TryFrom<Input> for RouteCommand {
    type Error = String;

    fn try_from(mut input: Input) -> Result<Self, Self::Error> {
        if input.program != ROUTE {
            return Err(format!("Not route command: {}", input.program));
        }

        match shift_arg(&mut input)?.as_str() {
            "add" => {
                let destination = RouteTarget::shift_target(&mut input)?;
                let gateway = RouteTarget::shift_target(&mut input)?;

                let interface =
                    if let Ok(true) = shift_arg_if(&mut input, "-ifp") {
                        Some(LinkName(shift_arg(&mut input)?))
                    } else {
                        None
                    };
                no_args_remaining(&input)?;
                Ok(RouteCommand::Add { destination, gateway, interface })
            }
            command => return Err(format!("Unsupported command: {}", command)),
        }
    }
}

enum SvccfgCommand {
    Addpropvalue {
        zone: Option<ZoneName>,
        service: ServiceName,
        key: smf::PropertyName,
        value: smf::PropertyValue,
    },
    Addpg {
        zone: Option<ZoneName>,
        service: ServiceName,
        group: smf::PropertyGroupName,
        group_type: String,
    },
    Delpg {
        zone: Option<ZoneName>,
        service: ServiceName,
        group: smf::PropertyName,
        value: String,
    },
    Delpropvalue {
        zone: Option<ZoneName>,
        service: ServiceName,
        group: smf::PropertyName,
        value: String,
    },
    Import {
        zone: Option<ZoneName>,
        file: Utf8PathBuf,
    },
    Refresh {
        zone: Option<ZoneName>,
        service: ServiceName,
    },
    Setprop {
        zone: Option<ZoneName>,
        service: ServiceName,
        key: smf::PropertyName,
        value: smf::PropertyValue,
    },
}

impl TryFrom<Input> for SvccfgCommand {
    type Error = String;

    fn try_from(mut input: Input) -> Result<Self, Self::Error> {
        if input.program != SVCCFG {
            return Err(format!("Not svccfg command: {}", input.program));
        }

        let zone = if shift_arg_if(&mut input, "-z")? {
            Some(ZoneName(shift_arg(&mut input)?))
        } else {
            None
        };

        let fmri = if shift_arg_if(&mut input, "-s")? {
            Some(ServiceName(shift_arg(&mut input)?))
        } else {
            None
        };

        match shift_arg(&mut input)?.as_str() {
            "addpropvalue" => todo!(),
            "addpg" => todo!(),
            "delpg" => todo!(),
            "delpropvalue" => todo!(),
            "import" => todo!(),
            "refresh" => todo!(),
            "setprop" => todo!(),
            command => return Err(format!("Unexpected command: {command}")),
        }
    }
}

enum SvcadmCommand {
    Enable { zone: Option<ZoneName>, service: ServiceName },
    Disable { zone: Option<ZoneName>, service: ServiceName },
}

impl TryFrom<Input> for SvcadmCommand {
    type Error = String;

    fn try_from(mut input: Input) -> Result<Self, Self::Error> {
        if input.program != SVCADM {
            return Err(format!("Not svcadm command: {}", input.program));
        }
        todo!();
    }
}

enum ZfsCommand {
    Create,
    Destroy,
    Get,
    List,
    Mount,
    Set,
}

impl TryFrom<Input> for ZfsCommand {
    type Error = String;

    fn try_from(mut input: Input) -> Result<Self, Self::Error> {
        if input.program != ZFS {
            return Err(format!("Not zfs command: {}", input.program));
        }
        todo!();
    }
}

enum ZoneadmCommand {
    List,
    Install,
    Boot,
}

impl TryFrom<Input> for ZoneadmCommand {
    type Error = String;

    fn try_from(mut input: Input) -> Result<Self, Self::Error> {
        if input.program != ZONEADM {
            return Err(format!("Not zoneadm command: {}", input.program));
        }
        todo!();
    }
}

enum ZonecfgCommand {
    Create { name: ZoneName, config: ZoneConfig },
    Delete { name: ZoneName },
}

impl TryFrom<Input> for ZonecfgCommand {
    type Error = String;

    fn try_from(mut input: Input) -> Result<Self, Self::Error> {
        if input.program != ZONECFG {
            return Err(format!("Not zonecfg command: {}", input.program));
        }
        shift_arg_expect(&mut input, "-z")?;
        let zone = ZoneName(shift_arg(&mut input)?);
        match shift_arg(&mut input)?.as_str() {
            "create" => {
                shift_arg_expect(&mut input, "-F")?;
                shift_arg_expect(&mut input, "-b")?;

                enum Scope {
                    Global,
                    Dataset(zone::Dataset),
                    Device(zone::Device),
                    Fs(zone::Fs),
                    Net(zone::Net),
                }
                let mut scope = Scope::Global;

                // Globally-scoped Resources
                let mut brand = None;
                let mut path = None;

                // Non-Global Resources
                let mut datasets = vec![];
                let mut devices = vec![];
                let mut nets = vec![];
                let mut fs = vec![];

                while !input.args.is_empty() {
                    shift_arg_expect(&mut input, ";")?;
                    match shift_arg(&mut input)?.as_str() {
                        "set" => {
                            let prop = shift_arg(&mut input)?;
                            let (k, v) =
                                prop.split_once('=').ok_or_else(|| {
                                    format!("Bad property: {prop}")
                                })?;

                            match &mut scope {
                                Scope::Global => {
                                    match k {
                                        "brand" => {
                                            brand = Some(v.to_string());
                                        }
                                        "zonepath" => {
                                            path = Some(Utf8PathBuf::from(v));
                                        }
                                        "autoboot" => {
                                            if v != "false" {
                                                return Err(format!("Unhandled autoboot value: {v}"));
                                            }
                                        }
                                        "ip-type" => {
                                            if v != "exclusive" {
                                                return Err(format!("Unhandled ip-type value: {v}"));
                                            }
                                        }
                                        k => {
                                            return Err(format!(
                                                "Unknown property name: {k}"
                                            ))
                                        }
                                    }
                                }
                                Scope::Dataset(d) => match k {
                                    "name" => d.name = v.to_string(),
                                    k => {
                                        return Err(format!(
                                            "Unknown property name: {k}"
                                        ))
                                    }
                                },
                                Scope::Device(d) => match k {
                                    "match" => d.name = v.to_string(),
                                    k => {
                                        return Err(format!(
                                            "Unknown property name: {k}"
                                        ))
                                    }
                                },
                                Scope::Fs(f) => match k {
                                    "type" => f.ty = v.to_string(),
                                    "dir" => f.dir = v.to_string(),
                                    "special" => f.special = v.to_string(),
                                    "raw" => f.raw = Some(v.to_string()),
                                    "options" => {
                                        f.options = v
                                            .split(',')
                                            .map(|s| s.to_string())
                                            .collect()
                                    }
                                    k => {
                                        return Err(format!(
                                            "Unknown property name: {k}"
                                        ))
                                    }
                                },
                                Scope::Net(n) => match k {
                                    "physical" => n.physical = v.to_string(),
                                    "address" => {
                                        n.address = Some(v.to_string())
                                    }
                                    "allowed-address" => {
                                        n.allowed_address = Some(v.to_string())
                                    }
                                    k => {
                                        return Err(format!(
                                            "Unknown property name: {k}"
                                        ))
                                    }
                                },
                            }
                        }
                        "add" => {
                            if !matches!(scope, Scope::Global) {
                                return Err("Cannot add from non-global scope"
                                    .to_string());
                            }
                            match shift_arg(&mut input)?.as_str() {
                                "dataset" => {
                                    scope =
                                        Scope::Dataset(zone::Dataset::default())
                                }
                                "device" => {
                                    scope =
                                        Scope::Device(zone::Device::default())
                                }
                                "fs" => scope = Scope::Fs(zone::Fs::default()),
                                "net" => {
                                    scope = Scope::Net(zone::Net::default())
                                }
                                scope => {
                                    return Err(format!(
                                        "Unexpected scope: {scope}"
                                    ))
                                }
                            }
                        }
                        "end" => {
                            match scope {
                                Scope::Global => {
                                    return Err(
                                        "Cannot end global scope".to_string()
                                    )
                                }
                                Scope::Dataset(d) => datasets.push(d),
                                Scope::Device(d) => devices.push(d),
                                Scope::Fs(f) => fs.push(f),
                                Scope::Net(n) => nets.push(n),
                            }
                            scope = Scope::Global;
                        }
                        sc => {
                            return Err(format!("Unexpected subcommand: {sc}"))
                        }
                    }
                }

                if !matches!(scope, Scope::Global) {
                    return Err(
                        "Cannot end zonecfg outside global scope".to_string()
                    );
                }

                Ok(ZonecfgCommand::Create {
                    name: zone,
                    config: ZoneConfig {
                        state: zone::State::Configured,
                        brand: brand.ok_or_else(|| "Missing brand")?,
                        path: path.ok_or_else(|| "Missing zonepath")?,
                        datasets,
                        devices,
                        nets,
                        fs,
                        layers: vec![],
                    },
                })
            }
            "delete" => {
                shift_arg_expect(&mut input, "-F")?;
                Ok(ZonecfgCommand::Delete { name: zone })
            }
            command => return Err(format!("Unexpected command: {command}")),
        }
    }
}

enum ZpoolCommand {
    Create,
    Export,
    Import,
    List,
    Set,
}

impl TryFrom<Input> for ZpoolCommand {
    type Error = String;

    fn try_from(mut input: Input) -> Result<Self, Self::Error> {
        if input.program != ZPOOL {
            return Err(format!("Not zpool command: {}", input.program));
        }

        todo!();
    }
}

enum KnownCommand {
    Dladm(DladmCommand),
    Ipadm(IpadmCommand),
    RouteAdm,
    Route(RouteCommand),
    Svccfg(SvccfgCommand),
    Svcadm(SvcadmCommand),
    Zfs(ZfsCommand),
    Zoneadm(ZoneadmCommand),
    Zonecfg(ZonecfgCommand),
    Zpool(ZpoolCommand),
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
            DLADM => KnownCommand::Dladm(DladmCommand::try_from(input)?),
            IPADM => KnownCommand::Ipadm(IpadmCommand::try_from(input)?),
            ROUTE => KnownCommand::Route(RouteCommand::try_from(input)?),
            SVCCFG => KnownCommand::Svccfg(SvccfgCommand::try_from(input)?),
            SVCADM => KnownCommand::Svcadm(SvcadmCommand::try_from(input)?),
            ZFS => KnownCommand::Zfs(ZfsCommand::try_from(input)?),
            ZONEADM => KnownCommand::Zoneadm(ZoneadmCommand::try_from(input)?),
            ZONECFG => KnownCommand::Zonecfg(ZonecfgCommand::try_from(input)?),
            ZPOOL => KnownCommand::Zpool(ZpoolCommand::try_from(input)?),
            _ => return Err(format!("Unknown command: {}", input.program)),
        };

        Ok(Command { with_pfexec, in_zone, cmd })
    }
}

// Shifts out the program, putting the subsequent argument in its place.
//
// Returns the prior program value.
fn shift_program(input: &mut Input) -> Result<String, String> {
    let new = input
        .args
        .pop_front()
        .ok_or_else(|| format!("Failed to parse {input}"))?;

    let old = std::mem::replace(&mut input.program, new);

    Ok(old)
}

fn no_args_remaining(input: &Input) -> Result<(), String> {
    if !input.args.is_empty() {
        return Err(format!("Unexpected extra arguments: {input}"));
    }
    Ok(())
}

// Removes the next argument unconditionally.
fn shift_arg(input: &mut Input) -> Result<String, String> {
    Ok(input.args.pop_front().ok_or_else(|| "Missing argument")?)
}

// Removes the next argument, which must equal the provided value.
fn shift_arg_expect(input: &mut Input, value: &str) -> Result<(), String> {
    let v = input.args.pop_front().ok_or_else(|| "Not enough args")?;
    if value != v {
        return Err(format!("Unexpected argument {v} (expected: {value}"));
    }
    Ok(())
}

// Removes the next argument if it equals `value`.
//
// Returns if it was equal.
fn shift_arg_if(input: &mut Input, value: &str) -> Result<bool, String> {
    let eq = input.args.front().ok_or_else(|| "Not enough args")? == value;
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

    #[test]
    fn dladm_create_vnic() {
        // Valid usage
        let DladmCommand::CreateVnic { link, temporary, mac, vlan, name, properties } = DladmCommand::try_from(
            Input::shell(format!("{DLADM} create-vnic -t -l mylink newlink"))
        ).unwrap() else {
            panic!("Wrong command");
        };
        assert_eq!(link.0, "mylink");
        assert!(temporary);
        assert!(mac.is_none());
        assert!(vlan.is_none());
        assert_eq!(name.0, "newlink");
        assert!(properties.is_empty());

        // Valid usage
        let DladmCommand::CreateVnic { link, temporary, mac, vlan, name, properties } = DladmCommand::try_from(
            Input::shell(format!("{DLADM} create-vnic -l mylink -v 3 -m foobar -p mtu=123 newlink"))
        ).unwrap() else {
            panic!("Wrong command");
        };
        assert_eq!(link.0, "mylink");
        assert!(!temporary);
        assert_eq!(mac.unwrap(), "foobar");
        assert_eq!(vlan.unwrap(), VlanID::new(3).unwrap());
        assert_eq!(name.0, "newlink");
        assert_eq!(
            properties,
            HashMap::from([("mtu".to_string(), "123".to_string())])
        );

        // Missing link
        DladmCommand::try_from(Input::shell(format!(
            "{DLADM} create-vnic newlink"
        )))
        .unwrap_err();

        // Missing name
        DladmCommand::try_from(Input::shell(format!(
            "{DLADM} create-vnic -l mylink"
        )))
        .unwrap_err();

        // Bad properties
        DladmCommand::try_from(Input::shell(format!(
            "{DLADM} create-vnic -l mylink -p foo=bar,baz mylink"
        )))
        .unwrap_err();

        // Unknown argument
        DladmCommand::try_from(Input::shell(format!(
            "{DLADM} create-vnic -l mylink --splorch mylink"
        )))
        .unwrap_err();

        // Missing command
        DladmCommand::try_from(Input::shell(DLADM)).unwrap_err();

        // Not dladm
        DladmCommand::try_from(Input::shell("hello!")).unwrap_err();
    }

    #[test]
    fn dladm_create_etherstub() {
        // Valid usage
        let DladmCommand::CreateEtherstub { temporary, name } = DladmCommand::try_from(
            Input::shell(format!("{DLADM} create-etherstub -t newlink"))
        ).unwrap() else {
            panic!("Wrong command");
        };

        assert!(temporary);
        assert_eq!(name.0, "newlink");

        // Missing link
        DladmCommand::try_from(Input::shell(format!(
            "{DLADM} create-etherstub"
        )))
        .unwrap_err();

        // Invalid argument
        DladmCommand::try_from(Input::shell(format!(
            "{DLADM} create-etherstub --splorch mylink"
        )))
        .unwrap_err();
    }

    #[test]
    fn dladm_delete_etherstub() {
        // Valid usage
        let DladmCommand::DeleteEtherstub { temporary, name } = DladmCommand::try_from(
            Input::shell(format!("{DLADM} delete-etherstub -t newlink"))
        ).unwrap() else {
            panic!("Wrong command");
        };

        assert!(temporary);
        assert_eq!(name.0, "newlink");

        // Missing link
        DladmCommand::try_from(Input::shell(format!(
            "{DLADM} delete-etherstub"
        )))
        .unwrap_err();

        // Invalid argument
        DladmCommand::try_from(Input::shell(format!(
            "{DLADM} delete-etherstub --splorch mylink"
        )))
        .unwrap_err();
    }

    #[test]
    fn dladm_delete_vnic() {
        // Valid usage
        let DladmCommand::DeleteVnic { temporary, name } = DladmCommand::try_from(
            Input::shell(format!("{DLADM} delete-vnic -t newlink"))
        ).unwrap() else {
            panic!("Wrong command");
        };

        assert!(temporary);
        assert_eq!(name.0, "newlink");

        // Missing link
        DladmCommand::try_from(Input::shell(format!("{DLADM} delete-vnic")))
            .unwrap_err();

        // Invalid argument
        DladmCommand::try_from(Input::shell(format!(
            "{DLADM} delete-vnic --splorch mylink"
        )))
        .unwrap_err();
    }

    #[test]
    fn dladm_show_etherstub() {
        // Valid usage
        let DladmCommand::ShowEtherstub { name } = DladmCommand::try_from(
            Input::shell(format!("{DLADM} show-etherstub newlink"))
        ).unwrap() else {
            panic!("Wrong command");
        };
        assert_eq!(name.unwrap().0, "newlink");

        // Valid usage
        let DladmCommand::ShowEtherstub { name } = DladmCommand::try_from(
            Input::shell(format!("{DLADM} show-etherstub"))
        ).unwrap() else {
            panic!("Wrong command");
        };
        assert!(name.is_none());

        // Invalid argument
        DladmCommand::try_from(Input::shell(format!(
            "{DLADM} show-etherstub --splorch mylink"
        )))
        .unwrap_err();
    }

    #[test]
    fn dladm_show_link() {
        // Valid usage
        let DladmCommand::ShowLink { name, fields } = DladmCommand::try_from(
            Input::shell(format!("{DLADM} show-link -p -o LINK,STATE newlink"))
        ).unwrap() else {
            panic!("Wrong command");
        };
        assert_eq!(name.0, "newlink");
        assert_eq!(fields[0], "LINK");
        assert_eq!(fields[1], "STATE");

        // Missing link name
        DladmCommand::try_from(Input::shell(format!("{DLADM} show-link")))
            .unwrap_err();

        // Not asking for output
        DladmCommand::try_from(Input::shell(format!(
            "{DLADM} show-link mylink"
        )))
        .unwrap_err();

        // Not asking for parseable output
        DladmCommand::try_from(Input::shell(format!(
            "{DLADM} show-link -o LINK mylink"
        )))
        .unwrap_err();
    }

    #[test]
    fn dladm_show_phys() {
        // Valid usage
        let DladmCommand::ShowPhys{ mac, fields, name } = DladmCommand::try_from(
            Input::shell(format!("{DLADM} show-phys -p -o LINK"))
        ).unwrap() else {
            panic!("Wrong command");
        };
        assert!(!mac);
        assert_eq!(fields[0], "LINK");
        assert!(name.is_none());

        // Not asking for output
        DladmCommand::try_from(Input::shell(format!(
            "{DLADM} show-phys mylink"
        )))
        .unwrap_err();

        // Not asking for parseable output
        DladmCommand::try_from(Input::shell(format!(
            "{DLADM} show-phys -o LINK mylink"
        )))
        .unwrap_err();
    }

    #[test]
    fn dladm_show_vnic() {
        // Valid usage
        let DladmCommand::ShowVnic{ fields, name } = DladmCommand::try_from(
            Input::shell(format!("{DLADM} show-vnic -p -o LINK"))
        ).unwrap() else {
            panic!("Wrong command");
        };
        assert_eq!(fields.unwrap(), vec!["LINK"]);
        assert!(name.is_none());

        // Valid usage
        let DladmCommand::ShowVnic{ fields, name } = DladmCommand::try_from(
            Input::shell(format!("{DLADM} show-vnic mylink"))
        ).unwrap() else {
            panic!("Wrong command");
        };
        assert!(fields.is_none());
        assert_eq!(name.unwrap().0, "mylink");

        // Not asking for parseable output
        DladmCommand::try_from(Input::shell(format!(
            "{DLADM} show-vnic -o LINK mylink"
        )))
        .unwrap_err();
    }

    #[test]
    fn dladm_set_linkprop() {
        // Valid usage
        let DladmCommand::SetLinkprop { temporary, properties, name } = DladmCommand::try_from(
            Input::shell(format!("{DLADM} set-linkprop -t -p mtu=123 mylink"))
        ).unwrap() else {
            panic!("Wrong command");
        };
        assert!(temporary);
        assert_eq!(
            properties,
            HashMap::from([("mtu".to_string(), "123".to_string())])
        );
        assert_eq!(name.0, "mylink");

        // Missing properties
        DladmCommand::try_from(Input::shell(format!(
            "{DLADM} set-linkprop mylink"
        )))
        .unwrap_err();

        // Bad property
        DladmCommand::try_from(Input::shell(format!(
            "{DLADM} set-linkprop -p bar mylink"
        )))
        .unwrap_err();

        // Missing link
        DladmCommand::try_from(Input::shell(format!(
            "{DLADM} set-linkprop -p foo=bar"
        )))
        .unwrap_err();
    }

    #[test]
    fn zonecfg_create() {
        let ZonecfgCommand::Create { name, config } = ZonecfgCommand::try_from(
            Input::shell(format!(
                "{ZONECFG} -z myzone \
                    create -F -b ; \
                    set brand=omicron1 ; \
                    set zonepath=/zone/myzone ; \
                    set autoboot=false ; \
                    set ip-type=exclusive ; \
                    add net ; \
                    set physical=oxControlService0 ; \
                    end"
            )),
        ).unwrap() else {
            panic!("Wrong command");
        };

        assert_eq!(name.0, "myzone");
        assert_eq!(config.state, zone::State::Configured);
        assert_eq!(config.brand, "omicron1");
        assert_eq!(config.path, Utf8PathBuf::from("/zone/myzone"));
        assert!(config.datasets.is_empty());
        assert_eq!(config.nets[0].physical, "oxControlService0");
        assert!(config.fs.is_empty());
        assert!(config.layers.is_empty());

        // Missing brand
        assert!(ZonecfgCommand::try_from(Input::shell(format!(
            "{ZONECFG} -z myzone \
                    create -F -b ; \
                    set zonepath=/zone/myzone"
        )),)
        .err()
        .unwrap()
        .contains("Missing brand"));

        // Missing zonepath
        assert!(ZonecfgCommand::try_from(Input::shell(format!(
            "{ZONECFG} -z myzone \
                    create -F -b ; \
                    set brand=omicron1"
        )),)
        .err()
        .unwrap()
        .contains("Missing zonepath"));

        // Ending mid-scope
        assert!(ZonecfgCommand::try_from(Input::shell(format!(
            "{ZONECFG} -z myzone \
                    create -F -b ; \
                    set brand=omicron1 ; \
                    set zonepath=/zone/myzone ; \
                    add net ; \
                    set physical=oxControlService0"
        )),)
        .err()
        .unwrap()
        .contains("Cannot end zonecfg outside global scope"));
    }

    #[test]
    fn zonecfg_delete() {
        let ZonecfgCommand::Delete { name } = ZonecfgCommand::try_from(
            Input::shell(format!("{ZONECFG} -z myzone delete -F")),
        ).unwrap() else {
            panic!("Wrong command");
        };
        assert_eq!(name.0, "myzone");
    }

    #[test]
    fn route_add() {
        // Valid command
        let RouteCommand::Add { destination, gateway, interface } =
            RouteCommand::try_from(Input::shell(format!(
                "{ROUTE} add -inet6 fd00::/16 default -ifp mylink"
            )))
            .unwrap();
        assert_eq!(
            destination,
            RouteTarget::ByAddress(IpNetwork::from_str("fd00::/16").unwrap())
        );
        assert_eq!(gateway, RouteTarget::Default);
        assert_eq!(interface.unwrap().0, "mylink");

        // Valid command
        let RouteCommand::Add { destination, gateway, interface } =
            RouteCommand::try_from(Input::shell(format!(
                "{ROUTE} add -inet default 127.0.0.1/8"
            )))
            .unwrap();
        assert_eq!(destination, RouteTarget::DefaultV4);
        assert_eq!(
            gateway,
            RouteTarget::ByAddress(IpNetwork::from_str("127.0.0.1/8").unwrap())
        );
        assert!(interface.is_none());

        // Invalid address family
        RouteCommand::try_from(Input::shell(format!(
            "{ROUTE} add -inet -inet6 default 127.0.0.1/8"
        )))
        .err()
        .unwrap()
        .contains("Cannot force both v4 and v6");

        // Invalid address family
        RouteCommand::try_from(Input::shell(format!(
            "{ROUTE} add -ine6 default -inet6 127.0.0.1/8"
        )))
        .err()
        .unwrap()
        .contains("127.0.0.1/8 is not ipv6");
    }

    #[test]
    fn ipadm_create_addr() {
        // Valid command
        let IpadmCommand::CreateAddr { temporary, ty, addrobj } = IpadmCommand::try_from(
            Input::shell(format!("{IPADM} create-addr -t -T addrconf foo/bar"))
        ).unwrap() else {
            panic!("Wrong command")
        };
        assert!(temporary);
        assert!(matches!(ty, AddrType::Addrconf));
        assert_eq!("foo/bar", addrobj.to_string());

        // Valid command
        let IpadmCommand::CreateAddr { temporary, ty, addrobj } = IpadmCommand::try_from(
            Input::shell(format!("{IPADM} create-addr -T static -a ::/32 foo/bar"))
        ).unwrap() else {
            panic!("Wrong command")
        };
        assert!(!temporary);
        assert_eq!(ty, AddrType::Static(IpNetwork::from_str("::/32").unwrap()));
        assert_eq!("foo/bar", addrobj.to_string());

        // Bad type
        IpadmCommand::try_from(Input::shell(format!(
            "{IPADM} create-addr -T quadratric foo/bar"
        )))
        .err()
        .unwrap()
        .contains("Unknown address type");

        // Missing name
        IpadmCommand::try_from(Input::shell(format!(
            "{IPADM} create-addr -T dhcp"
        )))
        .err()
        .unwrap()
        .contains("Missing argument");

        // Too many arguments
        IpadmCommand::try_from(Input::shell(format!(
            "{IPADM} create-addr -T dhcp foo/bar baz"
        )))
        .err()
        .unwrap()
        .contains("Unexpected extra arguments");

        // Not addrobject
        IpadmCommand::try_from(Input::shell(format!(
            "{IPADM} create-addr -T dhcp foobar"
        )))
        .err()
        .unwrap()
        .contains("Failed to parse addrobj name");
    }

    #[test]
    fn ipadm_create_if() {
        // Valid command
        let IpadmCommand::CreateIf { temporary, name } = IpadmCommand::try_from(
            Input::shell(format!("{IPADM} create-if foobar"))
        ).unwrap() else {
            panic!("Wrong command")
        };
        assert!(!temporary);
        assert_eq!(name.0, "foobar");

        // Too many arguments
        IpadmCommand::try_from(Input::shell(format!(
            "{IPADM} create-if foo bar"
        )))
        .err()
        .unwrap()
        .contains("Unexpected extra arguments");
    }

    #[test]
    fn ipadm_delete_addr() {
        // Valid command
        let IpadmCommand::DeleteAddr { addrobj } = IpadmCommand::try_from(
            Input::shell(format!("{IPADM} delete-addr foo/bar"))
        ).unwrap() else {
            panic!("Wrong command")
        };
        assert_eq!(addrobj.to_string(), "foo/bar");

        // Not addrobject
        IpadmCommand::try_from(Input::shell(format!(
            "{IPADM} delete-addr foobar"
        )))
        .err()
        .unwrap()
        .contains("Failed to parse addobj name");

        // Too many arguments
        IpadmCommand::try_from(Input::shell(format!(
            "{IPADM} delete-addr foo/bar foo/bar"
        )))
        .err()
        .unwrap()
        .contains("Unexpected extra arguments");
    }

    #[test]
    fn ipadm_delete_if() {
        // Valid command
        let IpadmCommand::DeleteIf { name } = IpadmCommand::try_from(
            Input::shell(format!("{IPADM} delete-if foobar"))
        ).unwrap() else {
            panic!("Wrong command")
        };
        assert_eq!(name.0, "foobar");

        // Too many arguments
        IpadmCommand::try_from(Input::shell(format!(
            "{IPADM} delete-if foo bar"
        )))
        .err()
        .unwrap()
        .contains("Unexpected extra arguments");
    }

    #[test]
    fn ipadm_show_if() {
        // Valid command
        let IpadmCommand::ShowIf { properties, name } = IpadmCommand::try_from(
            Input::shell(format!("{IPADM} show-if foobar"))
        ).unwrap() else {
            panic!("Wrong command")
        };
        assert!(properties.is_empty());
        assert_eq!(name.0, "foobar");

        // Valid command
        let IpadmCommand::ShowIf { properties, name } = IpadmCommand::try_from(
            Input::shell(format!("{IPADM} show-if -p -o IFNAME foobar"))
        ).unwrap() else {
            panic!("Wrong command")
        };
        assert_eq!(properties[0], "IFNAME");
        assert_eq!(name.0, "foobar");

        // Non parseable output
        IpadmCommand::try_from(Input::shell(format!(
            "{IPADM} show-if -o IFNAME foobar"
        )))
        .err()
        .unwrap();

        // Not asking for specific field
        IpadmCommand::try_from(Input::shell(format!(
            "{IPADM} show-if -p foobar"
        )))
        .err()
        .unwrap();

        // Too many arguments
        IpadmCommand::try_from(Input::shell(format!(
            "{IPADM} show-if fizz buzz"
        )))
        .err()
        .unwrap()
        .contains("Unexpected input");
    }

    #[test]
    fn ipadm_set_ifprop() {
        // Valid command
        let IpadmCommand::SetIfprop { temporary, properties, module, name } = IpadmCommand::try_from(
            Input::shell(format!("{IPADM} set-ifprop -t -m ipv4 -p mtu=123 foo"))
        ).unwrap() else {
            panic!("Wrong command")
        };

        assert!(temporary);
        assert_eq!(properties["mtu"], "123");
        assert_eq!(module, "ipv4");
        assert_eq!(name.0, "foo");

        // Bad property
        IpadmCommand::try_from(Input::shell(format!(
            "{IPADM} set-ifprop -p blarg foo"
        )))
        .err()
        .unwrap()
        .contains("Bad property: blarg");

        // Too many arguments
        IpadmCommand::try_from(Input::shell(format!(
            "{IPADM} set-ifprop -p mtu=123 foo bar"
        )))
        .err()
        .unwrap()
        .contains("Unexpected input");
    }
}
