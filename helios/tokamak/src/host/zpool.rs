// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::host::{no_args_remaining, shift_arg, shift_arg_if};

use helios_fusion::Input;
use helios_fusion::ZPOOL;

// TODO: Consider using helios_fusion::zpool::ZpoolName here?

pub(crate) enum Command {
    Create { pool: String, vdev: String },
    Export { pool: String },
    Import { force: bool, pool: String },
    List { properties: Vec<String>, pools: Option<Vec<String>> },
    Set { property: String, value: String, pool: String },
}

impl TryFrom<Input> for Command {
    type Error = String;

    fn try_from(mut input: Input) -> Result<Self, Self::Error> {
        if input.program != ZPOOL {
            return Err(format!("Not zpool command: {}", input.program));
        }

        match shift_arg(&mut input)?.as_str() {
            "create" => {
                let pool = shift_arg(&mut input)?;
                let vdev = shift_arg(&mut input)?;
                no_args_remaining(&input)?;
                Ok(Command::Create { pool, vdev })
            }
            "export" => {
                let pool = shift_arg(&mut input)?;
                no_args_remaining(&input)?;
                Ok(Command::Export { pool })
            }
            "import" => {
                let force = shift_arg_if(&mut input, "-f")?;
                let pool = shift_arg(&mut input)?;
                Ok(Command::Import { force, pool })
            }
            "list" => {
                let mut scripting = false;
                let mut parsable = false;
                let mut properties = vec![];
                let mut pools = None;

                while !input.args.is_empty() {
                    let arg = shift_arg(&mut input)?;
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
                                    properties = shift_arg(&mut input)?
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
                        pools = Some(vec![arg]);
                        break;
                    }
                }

                let remaining_pools = std::mem::take(&mut input.args);
                if !remaining_pools.is_empty() {
                    pools
                        .get_or_insert(vec![])
                        .extend(remaining_pools.into_iter());
                };
                if !scripting || !parsable {
                    return Err("You should run 'zpool list' commands with the '-Hp' flags enabled".to_string());
                }
                Ok(Command::List { properties, pools })
            }
            "set" => {
                let prop = shift_arg(&mut input)?;
                let (k, v) = prop
                    .split_once('=')
                    .ok_or_else(|| format!("Bad property: {prop}"))?;
                let property = k.to_string();
                let value = v.to_string();

                let pool = shift_arg(&mut input)?;
                no_args_remaining(&input)?;
                Ok(Command::Set { property, value, pool })
            }
            command => return Err(format!("Unexpected command: {command}")),
        }
    }
}
