// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::host::LinkName;
use crate::host::{no_args_remaining, shift_arg, shift_arg_if};

use helios_fusion::Input;
use helios_fusion::DLADM;
use omicron_common::vlan::VlanID;
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Debug)]
pub(crate) enum Command {
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

impl TryFrom<Input> for Command {
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
                        "You should ask for parsable output ('-p')".into()
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
                        "You should ask for parsable output ('-p')".into()
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn create_vnic() {
        // Valid usage
        let Command::CreateVnic { link, temporary, mac, vlan, name, properties } = Command::try_from(
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
        let Command::CreateVnic { link, temporary, mac, vlan, name, properties } = Command::try_from(
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
        Command::try_from(Input::shell(format!("{DLADM} create-vnic newlink")))
            .unwrap_err();

        // Missing name
        Command::try_from(Input::shell(format!(
            "{DLADM} create-vnic -l mylink"
        )))
        .unwrap_err();

        // Bad properties
        Command::try_from(Input::shell(format!(
            "{DLADM} create-vnic -l mylink -p foo=bar,baz mylink"
        )))
        .unwrap_err();

        // Unknown argument
        Command::try_from(Input::shell(format!(
            "{DLADM} create-vnic -l mylink --splorch mylink"
        )))
        .unwrap_err();

        // Missing command
        Command::try_from(Input::shell(DLADM)).unwrap_err();

        // Not dladm
        Command::try_from(Input::shell("hello!")).unwrap_err();
    }

    #[test]
    fn create_etherstub() {
        // Valid usage
        let Command::CreateEtherstub { temporary, name } = Command::try_from(
            Input::shell(format!("{DLADM} create-etherstub -t newlink"))
        ).unwrap() else {
            panic!("Wrong command");
        };

        assert!(temporary);
        assert_eq!(name.0, "newlink");

        // Missing link
        Command::try_from(Input::shell(format!("{DLADM} create-etherstub")))
            .unwrap_err();

        // Invalid argument
        Command::try_from(Input::shell(format!(
            "{DLADM} create-etherstub --splorch mylink"
        )))
        .unwrap_err();
    }

    #[test]
    fn delete_etherstub() {
        // Valid usage
        let Command::DeleteEtherstub { temporary, name } = Command::try_from(
            Input::shell(format!("{DLADM} delete-etherstub -t newlink"))
        ).unwrap() else {
            panic!("Wrong command");
        };

        assert!(temporary);
        assert_eq!(name.0, "newlink");

        // Missing link
        Command::try_from(Input::shell(format!("{DLADM} delete-etherstub")))
            .unwrap_err();

        // Invalid argument
        Command::try_from(Input::shell(format!(
            "{DLADM} delete-etherstub --splorch mylink"
        )))
        .unwrap_err();
    }

    #[test]
    fn delete_vnic() {
        // Valid usage
        let Command::DeleteVnic { temporary, name } = Command::try_from(
            Input::shell(format!("{DLADM} delete-vnic -t newlink"))
        ).unwrap() else {
            panic!("Wrong command");
        };

        assert!(temporary);
        assert_eq!(name.0, "newlink");

        // Missing link
        Command::try_from(Input::shell(format!("{DLADM} delete-vnic")))
            .unwrap_err();

        // Invalid argument
        Command::try_from(Input::shell(format!(
            "{DLADM} delete-vnic --splorch mylink"
        )))
        .unwrap_err();
    }

    #[test]
    fn show_etherstub() {
        // Valid usage
        let Command::ShowEtherstub { name } = Command::try_from(
            Input::shell(format!("{DLADM} show-etherstub newlink"))
        ).unwrap() else {
            panic!("Wrong command");
        };
        assert_eq!(name.unwrap().0, "newlink");

        // Valid usage
        let Command::ShowEtherstub { name } = Command::try_from(
            Input::shell(format!("{DLADM} show-etherstub"))
        ).unwrap() else {
            panic!("Wrong command");
        };
        assert!(name.is_none());

        // Invalid argument
        Command::try_from(Input::shell(format!(
            "{DLADM} show-etherstub --splorch mylink"
        )))
        .unwrap_err();
    }

    #[test]
    fn show_link() {
        // Valid usage
        let Command::ShowLink { name, fields } = Command::try_from(
            Input::shell(format!("{DLADM} show-link -p -o LINK,STATE newlink"))
        ).unwrap() else {
            panic!("Wrong command");
        };
        assert_eq!(name.0, "newlink");
        assert_eq!(fields[0], "LINK");
        assert_eq!(fields[1], "STATE");

        // Missing link name
        Command::try_from(Input::shell(format!("{DLADM} show-link")))
            .unwrap_err();

        // Not asking for output
        Command::try_from(Input::shell(format!("{DLADM} show-link mylink")))
            .unwrap_err();

        // Not asking for parsable output
        Command::try_from(Input::shell(format!(
            "{DLADM} show-link -o LINK mylink"
        )))
        .unwrap_err();
    }

    #[test]
    fn show_phys() {
        // Valid usage
        let Command::ShowPhys{ mac, fields, name } = Command::try_from(
            Input::shell(format!("{DLADM} show-phys -p -o LINK"))
        ).unwrap() else {
            panic!("Wrong command");
        };
        assert!(!mac);
        assert_eq!(fields[0], "LINK");
        assert!(name.is_none());

        // Not asking for output
        Command::try_from(Input::shell(format!("{DLADM} show-phys mylink")))
            .unwrap_err();

        // Not asking for parsable output
        Command::try_from(Input::shell(format!(
            "{DLADM} show-phys -o LINK mylink"
        )))
        .unwrap_err();
    }

    #[test]
    fn show_vnic() {
        // Valid usage
        let Command::ShowVnic{ fields, name } = Command::try_from(
            Input::shell(format!("{DLADM} show-vnic -p -o LINK"))
        ).unwrap() else {
            panic!("Wrong command");
        };
        assert_eq!(fields.unwrap(), vec!["LINK"]);
        assert!(name.is_none());

        // Valid usage
        let Command::ShowVnic{ fields, name } = Command::try_from(
            Input::shell(format!("{DLADM} show-vnic mylink"))
        ).unwrap() else {
            panic!("Wrong command");
        };
        assert!(fields.is_none());
        assert_eq!(name.unwrap().0, "mylink");

        // Not asking for parsable output
        Command::try_from(Input::shell(format!(
            "{DLADM} show-vnic -o LINK mylink"
        )))
        .unwrap_err();
    }

    #[test]
    fn set_linkprop() {
        // Valid usage
        let Command::SetLinkprop { temporary, properties, name } = Command::try_from(
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
        Command::try_from(Input::shell(format!("{DLADM} set-linkprop mylink")))
            .unwrap_err();

        // Bad property
        Command::try_from(Input::shell(format!(
            "{DLADM} set-linkprop -p bar mylink"
        )))
        .unwrap_err();

        // Missing link
        Command::try_from(Input::shell(format!(
            "{DLADM} set-linkprop -p foo=bar"
        )))
        .unwrap_err();
    }
}
