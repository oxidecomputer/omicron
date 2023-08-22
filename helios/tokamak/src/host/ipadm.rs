// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::host::{
    no_args_remaining, shift_arg, shift_arg_expect, shift_arg_if,
};
use crate::host::{AddrType, IpInterfaceName};

use helios_fusion::addrobj::AddrObject;
use helios_fusion::Input;
use helios_fusion::IPADM;
use ipnetwork::IpNetwork;
use std::collections::HashMap;
use std::str::FromStr;

pub(crate) enum Command {
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

impl TryFrom<Input> for Command {
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
                Ok(Command::CreateAddr { temporary, ty, addrobj })
            }
            "create-ip" | "create-if" => {
                let temporary = shift_arg_if(&mut input, "-t")?;
                let name = IpInterfaceName(shift_arg(&mut input)?);
                no_args_remaining(&input)?;
                Ok(Command::CreateIf { temporary, name })
            }
            "delete-addr" => {
                let addrobj = AddrObject::from_str(&shift_arg(&mut input)?)
                    .map_err(|e| e.to_string())?;
                no_args_remaining(&input)?;
                Ok(Command::DeleteAddr { addrobj })
            }
            "delete-ip" | "delete-if" => {
                let name = IpInterfaceName(shift_arg(&mut input)?);
                no_args_remaining(&input)?;
                Ok(Command::DeleteIf { name })
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

                Ok(Command::ShowIf { properties, name })
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

                Ok(Command::SetIfprop { temporary, properties, module, name })
            }
            command => return Err(format!("Unexpected command: {command}")),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn create_addr() {
        // Valid command
        let Command::CreateAddr { temporary, ty, addrobj } = Command::try_from(
            Input::shell(format!("{IPADM} create-addr -t -T addrconf foo/bar"))
        ).unwrap() else {
            panic!("Wrong command")
        };
        assert!(temporary);
        assert!(matches!(ty, AddrType::Addrconf));
        assert_eq!("foo/bar", addrobj.to_string());

        // Valid command
        let Command::CreateAddr { temporary, ty, addrobj } = Command::try_from(
            Input::shell(format!("{IPADM} create-addr -T static -a ::/32 foo/bar"))
        ).unwrap() else {
            panic!("Wrong command")
        };
        assert!(!temporary);
        assert_eq!(ty, AddrType::Static(IpNetwork::from_str("::/32").unwrap()));
        assert_eq!("foo/bar", addrobj.to_string());

        // Bad type
        assert!(Command::try_from(Input::shell(format!(
            "{IPADM} create-addr -T quadratric foo/bar"
        )))
        .err()
        .unwrap()
        .contains("Unknown address type"));

        // Missing name
        assert!(Command::try_from(Input::shell(format!(
            "{IPADM} create-addr -T dhcp"
        )))
        .err()
        .unwrap()
        .contains("Missing argument"));

        // Too many arguments
        assert!(Command::try_from(Input::shell(format!(
            "{IPADM} create-addr -T dhcp foo/bar baz"
        )))
        .err()
        .unwrap()
        .contains("Unexpected extra arguments"));

        // Not addrobject
        assert!(Command::try_from(Input::shell(format!(
            "{IPADM} create-addr -T dhcp foobar"
        )))
        .err()
        .unwrap()
        .contains("Failed to parse addrobj name"));
    }

    #[test]
    fn create_if() {
        // Valid command
        let Command::CreateIf { temporary, name } = Command::try_from(
            Input::shell(format!("{IPADM} create-if foobar"))
        ).unwrap() else {
            panic!("Wrong command")
        };
        assert!(!temporary);
        assert_eq!(name.0, "foobar");

        // Too many arguments
        assert!(Command::try_from(Input::shell(format!(
            "{IPADM} create-if foo bar"
        )))
        .err()
        .unwrap()
        .contains("Unexpected extra arguments"));
    }

    #[test]
    fn delete_addr() {
        // Valid command
        let Command::DeleteAddr { addrobj } = Command::try_from(
            Input::shell(format!("{IPADM} delete-addr foo/bar"))
        ).unwrap() else {
            panic!("Wrong command")
        };
        assert_eq!(addrobj.to_string(), "foo/bar");

        // Not addrobject
        assert!(Command::try_from(Input::shell(format!(
            "{IPADM} delete-addr foobar"
        )))
        .err()
        .unwrap()
        .contains("Failed to parse addrobj name"));

        // Too many arguments
        assert!(Command::try_from(Input::shell(format!(
            "{IPADM} delete-addr foo/bar foo/bar"
        )))
        .err()
        .unwrap()
        .contains("Unexpected extra arguments"));
    }

    #[test]
    fn delete_if() {
        // Valid command
        let Command::DeleteIf { name } = Command::try_from(
            Input::shell(format!("{IPADM} delete-if foobar"))
        ).unwrap() else {
            panic!("Wrong command")
        };
        assert_eq!(name.0, "foobar");

        // Too many arguments
        assert!(Command::try_from(Input::shell(format!(
            "{IPADM} delete-if foo bar"
        )))
        .err()
        .unwrap()
        .contains("Unexpected extra arguments"));
    }

    #[test]
    fn show_if() {
        // Valid command
        let Command::ShowIf { properties, name } = Command::try_from(
            Input::shell(format!("{IPADM} show-if foobar"))
        ).unwrap() else {
            panic!("Wrong command")
        };
        assert!(properties.is_empty());
        assert_eq!(name.0, "foobar");

        // Valid command
        let Command::ShowIf { properties, name } = Command::try_from(
            Input::shell(format!("{IPADM} show-if -p -o IFNAME foobar"))
        ).unwrap() else {
            panic!("Wrong command")
        };
        assert_eq!(properties[0], "IFNAME");
        assert_eq!(name.0, "foobar");

        // Non parsable output
        Command::try_from(Input::shell(format!(
            "{IPADM} show-if -o IFNAME foobar"
        )))
        .err()
        .unwrap();

        // Not asking for specific field
        Command::try_from(Input::shell(format!("{IPADM} show-if -p foobar")))
            .err()
            .unwrap();

        // Too many arguments
        assert!(Command::try_from(Input::shell(format!(
            "{IPADM} show-if fizz buzz"
        )))
        .err()
        .unwrap()
        .contains("Unexpected input"));
    }

    #[test]
    fn set_ifprop() {
        // Valid command
        let Command::SetIfprop { temporary, properties, module, name } = Command::try_from(
            Input::shell(format!("{IPADM} set-ifprop -t -m ipv4 -p mtu=123 foo"))
        ).unwrap() else {
            panic!("Wrong command")
        };

        assert!(temporary);
        assert_eq!(properties["mtu"], "123");
        assert_eq!(module, "ipv4");
        assert_eq!(name.0, "foo");

        // Bad property
        assert!(Command::try_from(Input::shell(format!(
            "{IPADM} set-ifprop -p blarg foo"
        )))
        .err()
        .unwrap()
        .contains("Bad property: blarg"));

        // Too many arguments
        assert!(Command::try_from(Input::shell(format!(
            "{IPADM} set-ifprop -p mtu=123 foo bar"
        )))
        .err()
        .unwrap()
        .contains("Unexpected input"));
    }
}
