// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cli::parse::InputParser;
use crate::host::ZoneName;

use helios_fusion::Input;
use helios_fusion::ZONEADM;

pub(crate) enum Command {
    Boot {
        name: ZoneName,
    },
    Halt {
        name: ZoneName,
    },
    Install {
        name: ZoneName,
        brand_specific_args: Vec<String>,
    },
    List {
        // Overrides the "list_installed" option
        list_configured: bool,
        list_installed: bool,
    },
    Uninstall {
        name: ZoneName,
        force: bool,
    },
}

impl TryFrom<Input> for Command {
    type Error = String;

    fn try_from(input: Input) -> Result<Self, Self::Error> {
        if input.program != ZONEADM {
            return Err(format!("Not zoneadm command: {}", input.program));
        }

        let mut input = InputParser::new(input);

        let name = if input.shift_arg_if("-z")? {
            Some(ZoneName(input.shift_arg()?))
        } else {
            None
        };

        match input.shift_arg()?.as_str() {
            "boot" => {
                input.no_args_remaining()?;
                let name = name.ok_or_else(|| {
                    "No zone specified, try: zoneadm -z ZONE boot"
                })?;
                Ok(Command::Boot { name })
            }
            "halt" => {
                input.no_args_remaining()?;
                let name = name.ok_or_else(|| {
                    "No zone specified, try: zoneadm -z ZONE halt"
                })?;
                Ok(Command::Halt { name })
            }
            "install" => {
                let brand_specific_args =
                    input.args().into_iter().cloned().collect();
                let name = name.ok_or_else(|| {
                    "No zone specified, try: zoneadm -z ZONE install"
                })?;
                Ok(Command::Install { name, brand_specific_args })
            }
            "list" => {
                let mut list_configured = false;
                let mut list_installed = false;
                let mut parsable = false;

                while !input.args().is_empty() {
                    let arg = input.shift_arg()?;
                    let mut chars = arg.chars();

                    if let Some('-') = chars.next() {
                        while let Some(c) = chars.next() {
                            match c {
                                'c' => list_configured = true,
                                'i' => list_installed = true,
                                'p' => parsable = true,
                                c => {
                                    return Err(format!(
                                        "Unrecognized option '-{c}'"
                                    ))
                                }
                            }
                        }
                    } else {
                        return Err(format!("Unrecognized argument {arg}"));
                    }
                }

                if !parsable {
                    return Err("You should run 'zoneadm list' commands with the '-p' flag enabled".to_string());
                }

                Ok(Command::List { list_configured, list_installed })
            }
            "uninstall" => {
                let name = name.ok_or_else(|| {
                    "No zone specified, try: zoneadm -z ZONE uninstall"
                })?;
                let force = if !input.args().is_empty() {
                    input.shift_arg_if("-F")?
                } else {
                    false
                };
                input.no_args_remaining()?;
                Ok(Command::Uninstall { name, force })
            }
            command => return Err(format!("Unexpected command: {command}")),
        }
    }
}
