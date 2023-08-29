// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cli::parse::InputParser;
use crate::types::dataset;

use helios_fusion::Input;
use helios_fusion::ZFS;
use std::collections::HashMap;

pub(crate) enum Command {
    CreateFilesystem {
        properties: HashMap<dataset::Property, String>,
        name: dataset::Name,
    },
    CreateVolume {
        properties: HashMap<dataset::Property, String>,
        sparse: bool,
        blocksize: Option<u64>,
        size: u64,
        name: dataset::Name,
    },
    Destroy {
        recursive_dependents: bool,
        recursive_children: bool,
        force_unmount: bool,
        name: dataset::Name,
    },
    Get {
        recursive: bool,
        depth: Option<usize>,
        // name, property, value, source
        fields: Vec<String>,
        properties: Vec<dataset::Property>,
        datasets: Option<Vec<dataset::Name>>,
    },
    List {
        recursive: bool,
        depth: Option<usize>,
        properties: Vec<dataset::Property>,
        datasets: Option<Vec<dataset::Name>>,
    },
    Mount {
        load_keys: bool,
        filesystem: dataset::Name,
    },
    Set {
        properties: HashMap<dataset::Property, String>,
        name: dataset::Name,
    },
}

impl TryFrom<Input> for Command {
    type Error = String;

    fn try_from(input: Input) -> Result<Self, Self::Error> {
        if input.program != ZFS {
            return Err(format!("Not zfs command: {}", input.program));
        }

        let mut input = InputParser::new(input);
        match input.shift_arg()?.as_str() {
            "create" => {
                let mut size = None;
                let mut blocksize = None;
                let mut sparse = None;
                let mut properties = HashMap::new();

                while input.args().len() > 1 {
                    // Volume Size (volumes only, required)
                    if input.shift_arg_if("-V")? {
                        let size_str = input.shift_arg()?;

                        let (size_str, multiplier) = if let Some(size_str) =
                            size_str.strip_suffix('G')
                        {
                            (size_str, (1 << 30))
                        } else if let Some(size_str) =
                            size_str.strip_suffix('M')
                        {
                            (size_str, (1 << 20))
                        } else if let Some(size_str) =
                            size_str.strip_suffix('K')
                        {
                            (size_str, (1 << 10))
                        } else {
                            (size_str.as_str(), 1)
                        };

                        size = Some(
                            size_str
                                .parse::<u64>()
                                .map_err(|e| e.to_string())?
                                * multiplier,
                        );
                    // Sparse (volumes only, optional)
                    } else if input.shift_arg_if("-s")? {
                        sparse = Some(true);
                    // Block size (volumes only, optional)
                    } else if input.shift_arg_if("-b")? {
                        blocksize = Some(
                            input
                                .shift_arg()?
                                .parse::<u64>()
                                .map_err(|e| e.to_string())?,
                        );
                    // Properties
                    } else if input.shift_arg_if("-o")? {
                        let prop = input.shift_arg()?;
                        let (k, v) = prop
                            .split_once('=')
                            .ok_or_else(|| format!("Bad property: {prop}"))?;
                        let prop = dataset::Property::try_from(k)
                            .map_err(|e| format!("Unknown property: {e}"))?;

                        properties.insert(prop, v.to_string());
                    } else {
                        let arg = input.shift_arg()?;
                        return Err(format!("Unexpected argument: {arg}"));
                    }
                }
                let name = input.shift_arg()?;
                input.no_args_remaining()?;

                if let Some(size) = size {
                    // Volume
                    let sparse = sparse.unwrap_or(false);
                    let name = dataset::Name::new(name)?;
                    Ok(Command::CreateVolume {
                        properties,
                        sparse,
                        blocksize,
                        size,
                        name,
                    })
                } else {
                    // Filesystem
                    if sparse.is_some() || blocksize.is_some() {
                        return Err("Using volume arguments, but forgot to specify '-V size'?".to_string());
                    }
                    let name = dataset::Name::new(name)?;
                    Ok(Command::CreateFilesystem { properties, name })
                }
            }
            "destroy" => {
                let mut recursive_dependents = false;
                let mut recursive_children = false;
                let mut force_unmount = false;
                let mut name = None;

                while !input.args().is_empty() {
                    let arg = input.shift_arg()?;
                    let mut chars = arg.chars();
                    if let Some('-') = chars.next() {
                        while let Some(c) = chars.next() {
                            match c {
                                'R' => recursive_dependents = true,
                                'r' => recursive_children = true,
                                'f' => force_unmount = true,
                                c => {
                                    return Err(format!(
                                        "Unrecognized option '-{c}'"
                                    ))
                                }
                            }
                        }
                    } else {
                        name = Some(dataset::Name::new(arg)?);
                        input.no_args_remaining()?;
                    }
                }
                let name = name.ok_or_else(|| "Missing name".to_string())?;
                Ok(Command::Destroy {
                    recursive_dependents,
                    recursive_children,
                    force_unmount,
                    name,
                })
            }
            "get" => {
                let mut scripting = false;
                let mut parsable = false;
                let mut recursive = false;
                let mut depth = None;
                let mut fields = ["name", "property", "value", "source"]
                    .map(String::from)
                    .to_vec();
                let mut properties = vec![];

                while !input.args().is_empty() {
                    let arg = input.shift_arg()?;
                    let mut chars = arg.chars();
                    // ZFS list lets callers pass in flags in groups, or
                    // separately.
                    if let Some('-') = chars.next() {
                        while let Some(c) = chars.next() {
                            match c {
                                'r' => recursive = true,
                                'H' => scripting = true,
                                'p' => parsable = true,
                                'd' => {
                                    let depth_raw =
                                        if chars.clone().next().is_some() {
                                            chars.collect::<String>()
                                        } else {
                                            input.shift_arg()?
                                        };
                                    depth = Some(
                                        depth_raw
                                            .parse::<usize>()
                                            .map_err(|e| e.to_string())?,
                                    );
                                    // Convince the compiler we won't use any
                                    // more 'chars', because used them all
                                    // parsing 'depth'.
                                    break;
                                }
                                'o' => {
                                    if chars.next().is_some() {
                                        return Err("-o should be immediately followed by fields".to_string());
                                    }
                                    fields = input
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
                        properties = arg
                            .split(',')
                            .map(|s| {
                                dataset::Property::try_from(s).map_err(|err| {
                                    format!("unknown property: {err}")
                                })
                            })
                            .collect::<Result<Vec<dataset::Property>, String>>(
                            )?;
                        break;
                    }
                }

                let datasets = Some(
                    input
                        .args()
                        .into_iter()
                        .map(|s| dataset::Name::new(s.to_string()))
                        .collect::<Result<Vec<dataset::Name>, _>>()?,
                );
                if !scripting || !parsable {
                    return Err("You should run 'zfs get' commands with the '-Hp' flags enabled".to_string());
                }

                Ok(Command::Get {
                    recursive,
                    depth,
                    fields,
                    properties,
                    datasets,
                })
            }
            "list" => {
                let mut scripting = false;
                let mut parsable = false;
                let mut recursive = false;
                let mut depth = None;
                let mut properties = vec![];
                let mut datasets = None;

                while !input.args().is_empty() {
                    let arg = input.shift_arg()?;
                    let mut chars = arg.chars();
                    // ZFS list lets callers pass in flags in groups, or
                    // separately.
                    if let Some('-') = chars.next() {
                        while let Some(c) = chars.next() {
                            match c {
                                'r' => recursive = true,
                                'H' => scripting = true,
                                'p' => parsable = true,
                                'd' => {
                                    let depth_raw =
                                        if chars.clone().next().is_some() {
                                            chars.collect::<String>()
                                        } else {
                                            input.shift_arg()?
                                        };
                                    depth = Some(
                                        depth_raw
                                            .parse::<usize>()
                                            .map_err(|e| e.to_string())?,
                                    );
                                    // Convince the compiler we won't use any
                                    // more 'chars', because used them all
                                    // parsing 'depth'.
                                    break;
                                }
                                'o' => {
                                    if chars.next().is_some() {
                                        return Err("-o should be immediately followed by properties".to_string());
                                    }
                                    properties = input
                                        .shift_arg()?
                                        .split(',')
                                        .map(|s| {
                                            dataset::Property::try_from(s)
                                                .map_err(|err| format!("unknown property: {err}"))
                                        })
                                        .collect::<Result<Vec<dataset::Property>, String>>()?;
                                }
                                c => {
                                    return Err(format!(
                                        "Unrecognized option '-{c}'"
                                    ))
                                }
                            }
                        }
                    } else {
                        // As soon as non-flag arguments are passed, the rest of
                        // the arguments are treated as datasets.
                        datasets =
                            Some(vec![dataset::Name::new(arg.to_string())?]);
                        break;
                    }
                }

                let remaining_datasets = input.args();
                if !remaining_datasets.is_empty() {
                    datasets.get_or_insert(vec![]).extend(
                        remaining_datasets
                            .into_iter()
                            .map(|d| dataset::Name::new(d.to_string()))
                            .collect::<Result<Vec<_>, _>>()?,
                    );
                };

                if !scripting || !parsable {
                    return Err("You should run 'zfs list' commands with the '-Hp' flags enabled".to_string());
                }

                Ok(Command::List { recursive, depth, properties, datasets })
            }
            "mount" => {
                let load_keys = input.shift_arg_if("-l")?;
                let filesystem = dataset::Name::new(input.shift_arg()?)?;
                input.no_args_remaining()?;
                Ok(Command::Mount { load_keys, filesystem })
            }
            "set" => {
                let mut properties = HashMap::new();

                while input.args().len() > 1 {
                    let prop = input.shift_arg()?;
                    let (k, v) = prop
                        .split_once('=')
                        .ok_or_else(|| format!("Bad property: {prop}"))?;
                    let prop = dataset::Property::try_from(k)
                        .map_err(|e| format!("Unknown property: {e}"))?;
                    properties.insert(prop, v.to_string());
                }
                let name = dataset::Name::new(input.shift_arg()?)?;
                input.no_args_remaining()?;

                Ok(Command::Set { properties, name })
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
        // Create a filesystem
        let Command::CreateFilesystem { properties, name } = Command::try_from(
            Input::shell(format!("{ZFS} create myfilesystem"))
        ).unwrap() else { panic!("wrong command") };
        assert_eq!(properties, HashMap::new());
        assert_eq!(name.as_str(), "myfilesystem");

        // Create a volume
        let Command::CreateVolume { properties, sparse, blocksize, size, name } = Command::try_from(
            Input::shell(format!("{ZFS} create -s -V 1024 -b 512 -o logbias=bar myvolume"))
        ).unwrap() else { panic!("wrong command") };
        assert_eq!(
            properties,
            HashMap::from([(dataset::Property::Logbias, "bar".to_string())])
        );
        assert_eq!(name.as_str(), "myvolume");
        assert!(sparse);
        assert_eq!(size, 1024);
        assert_eq!(blocksize, Some(512));

        // Create a volume (using letter suffix)
        let Command::CreateVolume { properties, sparse, blocksize, size, name } = Command::try_from(
            Input::shell(format!("{ZFS} create -s -V 2G -b 512 -o logbias=bar myvolume"))
        ).unwrap() else { panic!("wrong command") };
        assert_eq!(
            properties,
            HashMap::from([(dataset::Property::Logbias, "bar".to_string())])
        );
        assert_eq!(name.as_str(), "myvolume");
        assert!(sparse);
        assert_eq!(size, 2 << 30);
        assert_eq!(blocksize, Some(512));

        // Create volume (invalid)
        assert!(Command::try_from(Input::shell(format!(
            "{ZFS} create -s -b 512 -o logbias=bar myvolume"
        )))
        .err()
        .unwrap()
        .contains("Using volume arguments, but forgot to specify '-V size'"));
    }

    #[test]
    fn destroy() {
        let Command::Destroy { recursive_dependents, recursive_children, force_unmount, name } =
            Command::try_from(
                Input::shell(format!("{ZFS} destroy -rf foobar"))
            ).unwrap() else { panic!("wrong command") };

        assert!(!recursive_dependents);
        assert!(recursive_children);
        assert!(force_unmount);
        assert_eq!(name.as_str(), "foobar");

        assert!(Command::try_from(Input::shell(format!(
            "{ZFS} destroy -x doit"
        )))
        .err()
        .unwrap()
        .contains("Unrecognized option '-x'"));
    }

    #[test]
    fn get() {
        let Command::Get { recursive, depth, fields, properties, datasets } = Command::try_from(
            Input::shell(format!("{ZFS} get -Hrpd10 -o name,value mounted,available myvolume"))
        ).unwrap() else { panic!("wrong command") };

        assert!(recursive);
        assert_eq!(depth, Some(10));
        assert_eq!(fields, vec!["name", "value"]);
        assert_eq!(
            properties,
            vec![dataset::Property::Mounted, dataset::Property::Available]
        );
        assert_eq!(
            datasets.unwrap(),
            vec![dataset::Name::new("myvolume".to_string()).unwrap()]
        );

        assert!(Command::try_from(Input::shell(format!(
            "{ZFS} get -o name,value mounted,available myvolume"
        )))
        .err()
        .unwrap()
        .contains(
            "You should run 'zfs get' commands with the '-Hp' flags enabled"
        ));
    }

    #[test]
    fn list() {
        let Command::List { recursive, depth, properties, datasets } = Command::try_from(
            Input::shell(format!("{ZFS} list -d 1 -rHpo name myfilesystem"))
        ).unwrap() else { panic!("wrong command") };

        assert!(recursive);
        assert_eq!(depth.unwrap(), 1);
        assert_eq!(properties, vec![dataset::Property::Name]);
        assert_eq!(
            datasets.unwrap(),
            vec![dataset::Name::new("myfilesystem".to_string()).unwrap()]
        );

        assert!(Command::try_from(Input::shell(format!(
            "{ZFS} list name myfilesystem"
        )))
        .err()
        .unwrap()
        .contains(
            "You should run 'zfs list' commands with the '-Hp' flags enabled"
        ));
    }

    #[test]
    fn mount() {
        let Command::Mount { load_keys, filesystem } = Command::try_from(
            Input::shell(format!("{ZFS} mount -l foobar"))
        ).unwrap() else { panic!("wrong command") };

        assert!(load_keys);
        assert_eq!(filesystem.as_str(), "foobar");
    }

    #[test]
    fn set() {
        let Command::Set { properties, name } = Command::try_from(
            Input::shell(format!("{ZFS} set mountpoint=bar logbias=blat myfs"))
        ).unwrap() else { panic!("wrong command") };

        assert_eq!(
            properties,
            HashMap::from([
                (dataset::Property::Mountpoint, "bar".to_string()),
                (dataset::Property::Logbias, "blat".to_string())
            ])
        );
        assert_eq!(name.as_str(), "myfs");
    }
}
