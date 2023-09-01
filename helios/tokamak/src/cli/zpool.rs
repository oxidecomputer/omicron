// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cli::parse::InputParser;

use camino::Utf8PathBuf;
use helios_fusion::zpool::ZpoolName;
use helios_fusion::Input;
use helios_fusion::ZPOOL;
use std::str::FromStr;

pub(crate) enum Command {
    Create { pool: ZpoolName, vdev: Utf8PathBuf },
    Export { pool: ZpoolName },
    Import { force: bool, pool: ZpoolName },
    List { properties: Vec<String>, pools: Option<Vec<ZpoolName>> },
    Set { property: String, value: String, pool: ZpoolName },
}

impl TryFrom<Input> for Command {
    type Error = String;

    fn try_from(input: Input) -> Result<Self, Self::Error> {
        if input.program != ZPOOL {
            return Err(format!("Not zpool command: {}", input.program));
        }

        let mut input = InputParser::new(input);

        match input.shift_arg()?.as_str() {
            "create" => {
                let pool = ZpoolName::from_str(&input.shift_arg()?)?;
                let vdev = Utf8PathBuf::from(input.shift_arg()?);
                input.no_args_remaining()?;
                Ok(Command::Create { pool, vdev })
            }
            "export" => {
                let pool = ZpoolName::from_str(&input.shift_arg()?)?;
                input.no_args_remaining()?;
                Ok(Command::Export { pool })
            }
            "import" => {
                let force = input.shift_arg_if("-f")?;
                let pool = ZpoolName::from_str(&input.shift_arg()?)?;
                Ok(Command::Import { force, pool })
            }
            "list" => {
                let mut scripting = false;
                let mut parsable = false;
                let mut properties = vec![];
                let mut pools = None;

                while !input.args().is_empty() {
                    let arg = input.shift_arg()?;
                    let mut chars = arg.chars();
                    // ZFS list lets callers pass in flags in groups, or
                    // separately.
                    if let Some('-') = chars.next() {
                        while let Some(c) = chars.next() {
                            match c {
                                'H' => scripting = true,
                                'p' => parsable = true,
                                'o' => {
                                    if chars.next().is_some() {
                                        return Err("-o should be immediately followed by properties".to_string());
                                    }
                                    properties = input
                                        .shift_arg()?
                                        .split(',')
                                        .map(|s| s.to_string())
                                        .collect();
                                }
                                c => {
                                    return Err(format!(
                                        "Unrecognized option '-{c}'"
                                    ))
                                }
                            }
                        }
                    } else {
                        pools = Some(vec![ZpoolName::from_str(&arg)?]);
                        break;
                    }
                }

                let remaining_pools = input.args();
                if !remaining_pools.is_empty() {
                    pools.get_or_insert(vec![]).extend(
                        remaining_pools
                            .into_iter()
                            .map(|s| ZpoolName::from_str(s))
                            .collect::<Result<Vec<_>, String>>()?,
                    )
                };
                if !scripting || !parsable {
                    return Err("You should run 'zpool list' commands with the '-Hp' flags enabled".to_string());
                }

                if properties.is_empty() {
                    properties = vec!["name".to_string(), "health".to_string()];
                }
                Ok(Command::List { properties, pools })
            }
            "set" => {
                let prop = input.shift_arg()?;
                let (k, v) = prop
                    .split_once('=')
                    .ok_or_else(|| format!("Bad property: {prop}"))?;
                let property = k.to_string();
                let value = v.to_string();

                let pool = ZpoolName::from_str(&input.shift_arg()?)?;
                input.no_args_remaining()?;
                Ok(Command::Set { property, value, pool })
            }
            command => return Err(format!("Unexpected command: {command}")),
        }
    }
}
