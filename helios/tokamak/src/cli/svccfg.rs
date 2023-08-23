// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cli::parse::InputParser;
use crate::host::{ServiceName, ZoneName};

use camino::Utf8PathBuf;
use helios_fusion::Input;
use helios_fusion::SVCCFG;
use std::str::FromStr;

pub(crate) enum Command {
    Addpropvalue {
        zone: Option<ZoneName>,
        fmri: ServiceName,
        key: smf::PropertyName,
        ty: Option<String>,
        value: String,
    },
    Addpg {
        zone: Option<ZoneName>,
        fmri: ServiceName,
        group: smf::PropertyGroupName,
        group_type: String,
    },
    Delpg {
        zone: Option<ZoneName>,
        fmri: ServiceName,
        group: smf::PropertyGroupName,
    },
    Delpropvalue {
        zone: Option<ZoneName>,
        fmri: ServiceName,
        name: smf::PropertyName,
        glob: String,
    },
    Import {
        zone: Option<ZoneName>,
        file: Utf8PathBuf,
    },
    Refresh {
        zone: Option<ZoneName>,
        fmri: ServiceName,
    },
    Setprop {
        zone: Option<ZoneName>,
        fmri: ServiceName,
        name: smf::PropertyName,
        value: String,
    },
}

impl TryFrom<Input> for Command {
    type Error = String;

    fn try_from(input: Input) -> Result<Self, Self::Error> {
        if input.program != SVCCFG {
            return Err(format!("Not svccfg command: {}", input.program));
        }

        let mut input = InputParser::new(input);

        let zone = if input.shift_arg_if("-z")? {
            Some(ZoneName(input.shift_arg()?))
        } else {
            None
        };

        let fmri = if input.shift_arg_if("-s")? {
            Some(ServiceName(input.shift_arg()?))
        } else {
            None
        };

        match input.shift_arg()?.as_str() {
            "addpropvalue" => {
                let name = input.shift_arg()?;
                let name = smf::PropertyName::from_str(&name)
                    .map_err(|e| e.to_string())?;

                let type_or_value = input.shift_arg()?;
                let (ty, value) = match input.shift_arg().ok() {
                    Some(value) => {
                        let ty = type_or_value
                            .strip_suffix(':')
                            .ok_or_else(|| {
                                format!("Bad property type: {type_or_value}")
                            })?
                            .to_string();
                        (Some(ty), value)
                    }
                    None => (None, type_or_value),
                };

                let fmri =
                    fmri.ok_or_else(|| "-s option required for addpropvalue")?;

                input.no_args_remaining()?;
                Ok(Command::Addpropvalue { zone, fmri, key: name, ty, value })
            }
            "addpg" => {
                let name = input.shift_arg()?;
                let group = smf::PropertyGroupName::new(&name)
                    .map_err(|e| e.to_string())?;

                let group_type = input.shift_arg()?;
                if let Some(_flags) = input.shift_arg().ok() {
                    return Err(
                        "Parsing of optional flags not implemented".to_string()
                    );
                }
                let fmri =
                    fmri.ok_or_else(|| "-s option required for addpg")?;

                input.no_args_remaining()?;
                Ok(Command::Addpg { zone, fmri, group, group_type })
            }
            "delpg" => {
                let name = input.shift_arg()?;
                let group = smf::PropertyGroupName::new(&name)
                    .map_err(|e| e.to_string())?;
                let fmri =
                    fmri.ok_or_else(|| "-s option required for delpg")?;

                input.no_args_remaining()?;
                Ok(Command::Delpg { zone, fmri, group })
            }
            "delpropvalue" => {
                let name = input.shift_arg()?;
                let name = smf::PropertyName::from_str(&name)
                    .map_err(|e| e.to_string())?;
                let fmri =
                    fmri.ok_or_else(|| "-s option required for delpropvalue")?;
                let glob = input.shift_arg()?;

                input.no_args_remaining()?;
                Ok(Command::Delpropvalue { zone, fmri, name, glob })
            }
            "import" => {
                let file = input.shift_arg()?;
                if let Some(_) = fmri {
                    return Err(
                        "Cannot use '-s' option with import".to_string()
                    );
                }
                input.no_args_remaining()?;
                Ok(Command::Import { zone, file: file.into() })
            }
            "refresh" => {
                let fmri =
                    fmri.ok_or_else(|| "-s option required for refresh")?;
                input.no_args_remaining()?;
                Ok(Command::Refresh { zone, fmri })
            }
            "setprop" => {
                let fmri =
                    fmri.ok_or_else(|| "-s option required for setprop")?;

                // Setprop seems fine accepting args of the form:
                // - name=value
                // - name = value
                // - name = type: value     (NOTE: not yet supported)
                let first_arg = input.shift_arg()?;
                let (name, value) =
                    if let Some((name, value)) = first_arg.split_once('=') {
                        (name.to_string(), value.to_string())
                    } else {
                        let name = first_arg;
                        input.shift_arg_expect("=")?;
                        let value = input.shift_arg()?;
                        (name, value)
                    };

                let name = smf::PropertyName::from_str(&name)
                    .map_err(|e| e.to_string())?;

                input.no_args_remaining()?;
                Ok(Command::Setprop { zone, fmri, name, value })
            }
            command => return Err(format!("Unexpected command: {command}")),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn addpropvalue() {
        let Command::Addpropvalue { zone, fmri, key, ty, value } = Command::try_from(
            Input::shell(format!(
                "{SVCCFG} -z myzone -s svc:/myservice:default addpropvalue foo/bar astring: baz"
            ))
        ).unwrap() else {
            panic!("Wrong command");
        };

        assert_eq!(zone.unwrap().0, "myzone");
        assert_eq!(fmri.0, "svc:/myservice:default");
        assert_eq!(key.to_string(), "foo/bar");
        assert_eq!(ty, Some("astring".to_string()));
        assert_eq!(value, "baz");

        assert!(Command::try_from(Input::shell(format!(
            "{SVCCFG} addpropvalue foo/bar baz"
        )))
        .err()
        .unwrap()
        .contains("-s option required"));

        assert!(Command::try_from(Input::shell(format!(
            "{SVCCFG} -s svc:/mysvc addpropvalue foo/bar astring baz"
        )))
        .err()
        .unwrap()
        .contains("Bad property type"));
    }

    #[test]
    fn addpg() {
        let Command::Addpg { zone, fmri, group, group_type } = Command::try_from(
            Input::shell(format!(
                "{SVCCFG} -z myzone -s svc:/myservice:default addpg foo baz"
            ))
        ).unwrap() else {
            panic!("Wrong command");
        };

        assert_eq!(zone.unwrap().0, "myzone");
        assert_eq!(fmri.0, "svc:/myservice:default");
        assert_eq!(group.to_string(), "foo");
        assert_eq!(group_type, "baz");

        assert!(Command::try_from(Input::shell(format!(
            "{SVCCFG} addpg foo baz"
        )))
        .err()
        .unwrap()
        .contains("-s option required"));

        assert!(Command::try_from(Input::shell(format!(
            "{SVCCFG} addpg foo baz P"
        )))
        .err()
        .unwrap()
        .contains("Parsing of optional flags not implemented"));
    }

    #[test]
    fn delpg() {
        let Command::Delpg { zone, fmri, group } = Command::try_from(
            Input::shell(format!(
                "{SVCCFG} -z myzone -s svc:/myservice:default delpg foo"
            ))
        ).unwrap() else {
            panic!("Wrong command");
        };

        assert_eq!(zone.unwrap().0, "myzone");
        assert_eq!(fmri.0, "svc:/myservice:default");
        assert_eq!(group.to_string(), "foo");

        assert!(Command::try_from(Input::shell(format!("{SVCCFG} delpg foo")))
            .err()
            .unwrap()
            .contains("-s option required"));

        assert!(Command::try_from(Input::shell(format!(
            "{SVCCFG} -s mysvc delpg foo baz"
        )))
        .err()
        .unwrap()
        .contains("Unexpected extra arguments"));
    }

    #[test]
    fn import() {
        let Command::Import { zone, file } = Command::try_from(
            Input::shell(format!(
                "{SVCCFG} -z myzone import myfile"
            ))
        ).unwrap() else {
            panic!("Wrong command");
        };

        assert_eq!(zone.unwrap().0, "myzone");
        assert_eq!(file, "myfile");

        assert!(Command::try_from(Input::shell(format!(
            "{SVCCFG} import myfile myotherfile"
        )))
        .err()
        .unwrap()
        .contains("Unexpected extra arguments"));

        assert!(Command::try_from(Input::shell(format!(
            "{SVCCFG} -s myservice import myfile"
        )))
        .err()
        .unwrap()
        .contains("Cannot use '-s' option with import"));
    }

    #[test]
    fn refresh() {
        let Command::Refresh { zone, fmri } = Command::try_from(
            Input::shell(format!(
                "{SVCCFG} -z myzone -s myservice refresh"
            ))
        ).unwrap() else {
            panic!("Wrong command");
        };

        assert_eq!(zone.unwrap().0, "myzone");
        assert_eq!(fmri.0, "myservice");
    }

    #[test]
    fn setprop() {
        let Command::Setprop { zone, fmri, name, value } = Command::try_from(
            Input::shell(format!(
                "{SVCCFG} -z myzone -s myservice setprop foo/bar=baz"
            ))
        ).unwrap() else {
            panic!("Wrong command");
        };

        assert_eq!(zone.unwrap().0, "myzone");
        assert_eq!(fmri.0, "myservice");
        assert_eq!(name.to_string(), "foo/bar");
        assert_eq!(value, "baz");

        // Try that command again, but with spaces
        let Command::Setprop { zone, fmri, name, value } = Command::try_from(
            Input::shell(format!(
                "{SVCCFG} -z myzone -s myservice setprop foo/bar = baz"
            ))
        ).unwrap() else {
            panic!("Wrong command");
        };
        assert_eq!(zone.unwrap().0, "myzone");
        assert_eq!(fmri.0, "myservice");
        assert_eq!(name.to_string(), "foo/bar");
        assert_eq!(value, "baz");

        // Try that command again, but with quotes
        let Command::Setprop { zone, fmri, name, value } = Command::try_from(
            Input::shell(format!(
                "{SVCCFG} -z myzone -s myservice setprop foo/bar = \"fizz buzz\""
            ))
        ).unwrap() else {
            panic!("Wrong command");
        };
        assert_eq!(zone.unwrap().0, "myzone");
        assert_eq!(fmri.0, "myservice");
        assert_eq!(name.to_string(), "foo/bar");
        assert_eq!(value, "fizz buzz");

        assert!(Command::try_from(
            Input::shell(format!(
                "{SVCCFG} -z myzone -s myservice setprop foo/bar = \"fizz buzz\" blat"
            ))
        ).err().unwrap().contains("Unexpected extra arguments"));
    }
}
