// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A tool to show oxide related log file paths
//!
//! All data is based off of reading the filesystem

use anyhow::Context;
use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;
use std::fs::{read_dir, DirEntry};
use std::io;
use uuid::Uuid;

/// Return a UUID if the `DirEntry` contains a directory that parses into a UUID.
fn get_uuid_dir(result: io::Result<DirEntry>) -> Option<Uuid> {
    let Ok(entry) = result else {
        return None;
    };
    let Ok(file_type) = entry.file_type() else {
        return None;
    };
    if !file_type.is_dir() {
        return None;
    }
    let file_name = entry.file_name();
    let Some(s) = file_name.to_str() else {
        return None;
    };
    if let Ok(uuid) = s.parse() {
        Some(uuid)
    } else {
        None
    }
}

#[derive(Debug)]
pub struct Pools {
    pub internal: Vec<Uuid>,
    pub external: Vec<Uuid>,
}

impl Pools {
    pub fn read() -> anyhow::Result<Pools> {
        let internal = read_dir("/pool/int/")
            .context("Failed to read /pool/int")?
            .filter_map(get_uuid_dir)
            .collect();
        let external = read_dir("/pool/ext/")
            .context("Failed to read /pool/ext")?
            .filter_map(get_uuid_dir)
            .collect();
        Ok(Pools { internal, external })
    }
}

/// Path and metadata about a logfile
/// We use options for metadata as retrieval is fallible
#[derive(Debug, Clone, Eq)]
pub struct LogFile {
    pub path: Utf8PathBuf,
    pub size: Option<u64>,
    pub modified: Option<DateTime<Utc>>,
}

impl PartialEq for LogFile {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}

impl PartialOrd for LogFile {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LogFile {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.path.cmp(&other.path)
    }
}

impl LogFile {
    fn new(path: Utf8PathBuf) -> LogFile {
        LogFile { path, size: None, modified: None }
    }
}

/// All oxide logs for a given service in a given zone
#[derive(Debug, Clone, Default)]
pub struct SvcLogs {
    pub current: Option<LogFile>,
    pub archived: Vec<LogFile>,

    // Logs in non-standard places and logs that aren't necessarily oxide logs
    pub extra: Vec<LogFile>,
}

// These probably don't warrant newtypes. They are just to make the
// keys in maps a bit easier to read.
type ZoneName = String;
type ServiceName = String;

pub struct Paths {
    pub primary: Utf8PathBuf,
    pub debug: Vec<Utf8PathBuf>,
    pub extra: Vec<(&'static str, Utf8PathBuf)>,
}

pub struct Zones {
    pub zones: BTreeMap<ZoneName, Paths>,
}

impl Zones {
    pub fn load() -> Result<Zones, anyhow::Error> {
        let mut zones = BTreeMap::new();

        // Describe where to find logs for the global zone
        zones.insert(
            "global".to_string(),
            Paths {
                primary: Utf8PathBuf::from("/var/svc/log"),
                debug: vec![],
                extra: vec![],
            },
        );

        // Describe where to find logs for the switch zone
        zones.insert(
            "oxz_switch".to_string(),
            Paths {
                primary: Utf8PathBuf::from("/zone/oxz_switch/root/var/svc/log"),
                debug: vec![],
                extra: vec![(
                    "dendrite",
                    "/zone/oxz_switch/root/var/dendrite".into(),
                )],
            },
        );
        // Find the directories containing the primary and extra log files
        // for all zones on external storage pools.
        let pools = Pools::read()?;
        for uuid in &pools.external {
            let zones_path: Utf8PathBuf =
                ["/pool/ext", &uuid.to_string(), "crypt/zone"].iter().collect();
            // Find the zones on the given pool
            let Ok(entries) = read_dir(zones_path.as_path()) else {
                continue;
            };
            for entry in entries {
                let Ok(zone_entry) = entry else {
                    continue;
                };
                let zone = zone_entry.file_name();
                let Some(zone) = zone.to_str() else {
                    // not utf8
                    continue;
                };
                // Add the path to the current logs for the zone
                let mut dir = zones_path.clone();
                dir.push(zone);
                dir.push("root/var/svc/log");
                let mut paths =
                    Paths { primary: dir, debug: vec![], extra: vec![] };

                // Add the path to the extra logs for the zone
                if zone.starts_with("oxz_cockroachdb") {
                    let mut dir = zones_path.clone();
                    dir.push(zone);
                    dir.push("root/data/logs");
                    paths.extra.push(("cockroachdb", dir));
                }

                zones.insert(zone.to_string(), paths);
            }
        }

        // Find the directories containing the debug log files
        for uuid in &pools.external {
            let zones_path: Utf8PathBuf =
                ["/pool/ext", &uuid.to_string(), "crypt/debug"]
                    .iter()
                    .collect();
            // Find the zones on the given pool
            let Ok(entries) = read_dir(zones_path.as_path()) else {
                continue;
            };
            for entry in entries {
                let Ok(zone_entry) = entry else {
                    continue;
                };
                let zone = zone_entry.file_name();
                let Some(zone) = zone.to_str() else {
                    // not utf8
                    continue;
                };
                let mut dir = zones_path.clone();
                dir.push(zone);

                // We only add debug paths if the zones have primary paths
                zones.get_mut(zone).map(|paths| paths.debug.push(dir));
            }
        }

        Ok(Zones { zones })
    }

    pub fn zone_logs(&self, zone: &str) -> BTreeMap<ServiceName, SvcLogs> {
        let mut output = BTreeMap::new();
        let Some(paths) = self.zones.get(zone) else {
            return BTreeMap::new();
        };
        load_svc_logs(paths.primary.clone(), &mut output);
        for dir in paths.debug.clone() {
            load_svc_logs(dir, &mut output);
        }
        for (svc_name, dir) in paths.extra.clone() {
            load_extra_logs(dir, svc_name, &mut output);
        }
        output
    }
}

fn load_svc_logs(dir: Utf8PathBuf, logs: &mut BTreeMap<ServiceName, SvcLogs>) {
    let Ok(entries) = read_dir(dir.as_path()) else {
        return;
    };
    for entry in entries {
        let Ok(entry) = entry else {
            continue;
        };
        let filename = entry.file_name();
        let Some(filename) = filename.to_str() else {
            continue;
        };
        if filename.starts_with("oxide-") {
            let mut path = dir.clone();
            path.push(filename);
            let mut logfile = LogFile::new(path);
            // If we can't find the service name, then skip the log
            let Some((prefix, _suffix)) = filename.split_once(':') else {
                continue;
            };
            // We already look for this prefix above
            let svc_name = prefix.strip_prefix("oxide-").unwrap().to_string();
            if let Ok(metadata) = entry.metadata() {
                if metadata.len() == 0 {
                    // skip 0 size files
                    continue;
                }
                logfile.size = Some(metadata.len());
                if let Ok(modified) = metadata.modified() {
                    logfile.modified = Some(modified.into());
                }
            }

            let is_current = filename.ends_with(".log");

            let svc_logs =
                logs.entry(svc_name.clone()).or_insert(SvcLogs::default());

            if is_current {
                svc_logs.current = Some(logfile.clone());
            } else {
                svc_logs.archived.push(logfile.clone());
            }
        }
    }
}

fn load_extra_logs(
    dir: Utf8PathBuf,
    svc_name: &str,
    logs: &mut BTreeMap<ServiceName, SvcLogs>,
) {
    let Ok(entries) = read_dir(dir.as_path()) else {
        return;
    };

    // We only insert extra files if we have already collected
    // related current and archived files.
    // This should always be the case unless the files are
    // for zones that no longer exist.
    let Some(svc_logs) = logs.get_mut(svc_name) else {
        return;
    };

    for entry in entries {
        let Ok(entry) = entry else {
            continue;
        };
        let filename = entry.file_name();
        let Some(filename) = filename.to_str() else {
            continue;
        };
        let mut path = dir.clone();
        path.push(filename);
        let mut logfile = LogFile::new(path);
        if let Ok(metadata) = entry.metadata() {
            if metadata.len() == 0 {
                // skip 0 size files
                continue;
            }
            logfile.size = Some(metadata.len());
            if let Ok(modified) = metadata.modified() {
                logfile.modified = Some(modified.into());
            }
        }

        svc_logs.extra.push(logfile);
    }
}
