// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::host::{shift_arg, shift_arg_expect};
use crate::host::{ZoneConfig, ZoneName};

use camino::Utf8PathBuf;
use helios_fusion::Input;
use helios_fusion::ZONECFG;

pub(crate) enum Command {
    Create { name: ZoneName, config: ZoneConfig },
    Delete { name: ZoneName },
}

impl TryFrom<Input> for Command {
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

                Ok(Command::Create {
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
                Ok(Command::Delete { name: zone })
            }
            command => return Err(format!("Unexpected command: {command}")),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn create() {
        let Command::Create { name, config } = Command::try_from(
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
        assert!(Command::try_from(Input::shell(format!(
            "{ZONECFG} -z myzone \
                    create -F -b ; \
                    set zonepath=/zone/myzone"
        )),)
        .err()
        .unwrap()
        .contains("Missing brand"));

        // Missing zonepath
        assert!(Command::try_from(Input::shell(format!(
            "{ZONECFG} -z myzone \
                    create -F -b ; \
                    set brand=omicron1"
        )),)
        .err()
        .unwrap()
        .contains("Missing zonepath"));

        // Ending mid-scope
        assert!(Command::try_from(Input::shell(format!(
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
    fn delete() {
        let Command::Delete { name } = Command::try_from(
            Input::shell(format!("{ZONECFG} -z myzone delete -F")),
        ).unwrap() else {
            panic!("Wrong command");
        };
        assert_eq!(name.0, "myzone");
    }
}
