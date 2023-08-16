// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Emulates an illumos system

// TODO TODO TODO REMOVE ME
#![allow(dead_code)]

use crate::dladm::DLADM;
use crate::host::input::Input;
use crate::host::PFEXEC;
use crate::zone::IPADM;
use crate::zone::SVCCFG;
use crate::zone::ZLOGIN;
use crate::zone::ZONEADM;
use crate::zone::ZONECFG;
use crate::ROUTE;
use camino::Utf8PathBuf;
use ipnetwork::IpNetwork;
use omicron_common::vlan::VlanID;
use std::collections::HashMap;
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

// TODO: How much is it worth doing this vs just making things self-assembling?
// XXX E.g. is it actually worth emulating svccfg?

struct ZoneName(String);
struct ZoneConfig {
    state: zone::State,
    brand: String,
    // zonepath
    path: Utf8PathBuf,
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
}

impl Host {
    pub fn new() -> Self {
        Self { global: ZoneEnvironment::new(0), zones: HashMap::new() }
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

        match input.args.pop_front().ok_or_else(|| "Missing command")?.as_str()
        {
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
                    if shift_if_eq(&mut input, "-t")? {
                        temporary = true;
                    } else if shift_if_eq(&mut input, "-p")? {
                        let props = shift_arg(&mut input)?;
                        let props = props.split(',');
                        for prop in props {
                            let (k, v) = prop
                                .split_once('=')
                                .ok_or_else(|| "Bad property")?;
                            properties.insert(k.to_string(), v.to_string());
                        }
                    } else if shift_if_eq(&mut input, "-m")? {
                        // NOTE: Not yet supporting the keyword-based MACs.
                        mac = Some(shift_arg(&mut input)?);
                    } else if shift_if_eq(&mut input, "-l")? {
                        link = Some(LinkName(shift_arg(&mut input)?));
                    } else if shift_if_eq(&mut input, "-v")? {
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
                    if shift_if_eq(&mut input, "-t")? {
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
                    if shift_if_eq(&mut input, "-t")? {
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
                    if shift_if_eq(&mut input, "-t")? {
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
                if !shift_if_eq(&mut input, "-p")? {
                    return Err(
                        "You should ask for parseable output ('-p')".into()
                    );
                }
                if !shift_if_eq(&mut input, "-o")? {
                    return Err(
                        "You should ask for specific outputs ('-o')".into()
                    );
                }
                // TODO: Could parse an enum of known properties...
                let fields = shift_arg(&mut input)?
                    .split(',')
                    .map(|s| s.to_string())
                    .collect();
                no_args_remaining(&input)?;

                Ok(Self::ShowLink { name, fields })
            }
            "show-phys" => {
                let mut mac = false;
                if shift_if_eq(&mut input, "-m")? {
                    mac = true;
                }
                if !shift_if_eq(&mut input, "-p")? {
                    return Err(
                        "You should ask for parseable output ('-p')".into()
                    );
                }
                if !shift_if_eq(&mut input, "-o")? {
                    return Err(
                        "You should ask for specific outputs ('-o')".into()
                    );
                }
                // TODO: Could parse an enum of known properties...
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
                if shift_if_eq(&mut input, "-p")? {
                    if !shift_if_eq(&mut input, "-o")? {
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
                    if shift_if_eq(&mut input, "-t")? {
                        temporary = true;
                    } else if shift_if_eq(&mut input, "-p")? {
                        let props = shift_arg(&mut input)?;
                        let props = props.split(',');
                        for prop in props {
                            let (k, v) = prop
                                .split_once('=')
                                .ok_or_else(|| "Bad property")?;
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
            _ => Err(format!("Unsupported command: {}", input.program)),
        }
    }
}

enum IpadmCommand {
    CreateIf,
    DeleteIf,
    ShowIf,
    SetIfprop,
}

enum RouteCommand {
    Add,
}

enum SvccfgCommand {
    Import,
    Refresh,
    Setprop,
}

enum ZoneadmCommand {
    List,
    Install,
    Boot,
}

enum ZonecfgCommand {
    Create,
}

enum KnownCommand {
    Dladm(DladmCommand),
    Ipadm(IpadmCommand),
    RouteAdm,
    Route(RouteCommand),
    Svccfg(SvccfgCommand),
    Zoneadm(ZoneadmCommand),
    Zonecfg(ZonecfgCommand),
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
            IPADM => todo!(),
            ROUTE => todo!(),
            SVCCFG => todo!(),
            ZONEADM => todo!(),
            ZONECFG => todo!(),
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

// Removes the next argument if it equals value.
//
// Returns if it was equal.
fn shift_if_eq(input: &mut Input, value: &str) -> Result<bool, String> {
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
}
