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

/// Filter which logs to search for in a given zone
///
/// Each field in the filter is additive.
///
/// The filter was added to the library and not just the CLI because in some
/// cases searching for archived logs is pretty expensive.
#[derive(Clone, Copy, Debug)]
pub struct Filter {
    pub current: bool,
    pub archived: bool,
    pub extra: bool,
}

/// Path and metadata about a logfile
/// We use options for metadata as retrieval is fallible
#[derive(Debug, Clone, Eq)]
pub struct LogFile {
    pub path: Utf8PathBuf,
    pub size: Option<u64>,
    pub modified: Option<DateTime<Utc>>,
}

impl LogFile {
    pub fn read_metadata(&mut self, entry: DirEntry) {
        if let Ok(metadata) = entry.metadata() {
            self.size = Some(metadata.len());
            if let Ok(modified) = metadata.modified() {
                self.modified = Some(modified.into());
            }
        }
    }
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
                if let Some(paths) = zones.get_mut(zone) {
                    paths.debug.push(dir);
                }
            }
        }

        Ok(Zones { zones })
    }

    /// Return log files organized by service name
    pub fn zone_logs(
        &self,
        zone: &str,
        filter: Filter,
    ) -> BTreeMap<ServiceName, SvcLogs> {
        let mut output = BTreeMap::new();
        let Some(paths) = self.zones.get(zone) else {
            return BTreeMap::new();
        };
        load_svc_logs(paths.primary.clone(), &mut output);

        if filter.archived {
            for dir in paths.debug.clone() {
                load_svc_logs(dir, &mut output);
            }
        }
        if filter.extra {
            for (svc_name, dir) in paths.extra.clone() {
                load_extra_logs(dir, svc_name, &mut output);
            }
        }
        output
    }
}

const OX_SMF_PREFIXES: [&str; 2] = ["oxide-", "system-illumos-"];

/// Return true if the provided file name appears to be a valid log file for an
/// Oxide-managed SMF service.
///
/// Note that this operates on the _file name_. Any leading path components will
/// cause this check to return `false`.
pub fn is_oxide_smf_log_file(filename: impl AsRef<str>) -> bool {
    // Log files are named by the SMF services, with the `/` in the FMRI
    // translated to a `-`.
    let filename = filename.as_ref();
    OX_SMF_PREFIXES
        .iter()
        .any(|prefix| filename.starts_with(prefix) && filename.contains(".log"))
}

// Parse an oxide smf log file name and return the name of the underlying
// service.
//
// If parsing fails for some reason, return `None`.
pub fn oxide_smf_service_name_from_log_file_name(
    filename: &str,
) -> Option<&str> {
    let Some((prefix, _suffix)) = filename.split_once(':') else {
        // No ':' found
        return None;
    };

    for ox_prefix in OX_SMF_PREFIXES {
        if let Some(svc_name) = prefix.strip_prefix(ox_prefix) {
            return Some(svc_name);
        }
    }

    None
}

// Given a directory, find all oxide specific SMF service logs and return them
// mapped to their inferred service name.
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

        // Is this a log file we care about?
        if is_oxide_smf_log_file(filename) {
            let mut path = dir.clone();
            path.push(filename);
            let mut logfile = LogFile::new(path);

            let Some(svc_name) =
                oxide_smf_service_name_from_log_file_name(filename)
            else {
                // parsing failed
                continue;
            };

            logfile.read_metadata(entry);
            if logfile.size == Some(0) {
                // skip 0 size files
                continue;
            }

            let is_current = filename.ends_with(".log");

            let svc_logs =
                logs.entry(svc_name.to_string()).or_insert(SvcLogs::default());

            if is_current {
                svc_logs.current = Some(logfile.clone());
            } else {
                svc_logs.archived.push(logfile.clone());
            }
        }
    }
}

// Load any logs in non-standard paths. We grab all logs in `dir` and
// don't filter based on filename prefix as in `load_svc_logs`.
fn load_extra_logs(
    dir: Utf8PathBuf,
    svc_name: &str,
    logs: &mut BTreeMap<ServiceName, SvcLogs>,
) {
    let Ok(entries) = read_dir(dir.as_path()) else {
        return;
    };

    let svc_logs =
        logs.entry(svc_name.to_string()).or_insert(SvcLogs::default());

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
        logfile.read_metadata(entry);
        if logfile.size == Some(0) {
            // skip 0 size files
            continue;
        }
        svc_logs.extra.push(logfile);
    }
}

#[cfg(test)]
mod tests {
    pub use super::is_oxide_smf_log_file;
    pub use super::oxide_smf_service_name_from_log_file_name;

    #[test]
    fn test_is_oxide_smf_log_file() {
        assert!(is_oxide_smf_log_file("oxide-blah:default.log"));
        assert!(is_oxide_smf_log_file("oxide-blah:default.log.0"));
        assert!(is_oxide_smf_log_file("oxide-blah:default.log.1111"));
        assert!(is_oxide_smf_log_file("system-illumos-blah:default.log"));
        assert!(is_oxide_smf_log_file("system-illumos-blah:default.log.0"));
        assert!(!is_oxide_smf_log_file("not-oxide-blah:default.log"));
        assert!(!is_oxide_smf_log_file("not-system-illumos-blah:default.log"));
        assert!(!is_oxide_smf_log_file("system-blah:default.log"));
    }

    #[test]
    fn test_oxide_smf_service_name_from_log_file_name() {
        assert_eq!(
            Some("blah"),
            oxide_smf_service_name_from_log_file_name("oxide-blah:default.log")
        );
        assert_eq!(
            Some("blah"),
            oxide_smf_service_name_from_log_file_name(
                "oxide-blah:default.log.0"
            )
        );
        assert_eq!(
            Some("blah"),
            oxide_smf_service_name_from_log_file_name(
                "oxide-blah:default.log.1111"
            )
        );
        assert_eq!(
            Some("blah"),
            oxide_smf_service_name_from_log_file_name(
                "system-illumos-blah:default.log"
            )
        );
        assert_eq!(
            Some("blah"),
            oxide_smf_service_name_from_log_file_name(
                "system-illumos-blah:default.log.0"
            )
        );
        assert!(oxide_smf_service_name_from_log_file_name(
            "not-oxide-blah:default.log"
        )
        .is_none());
        assert!(oxide_smf_service_name_from_log_file_name(
            "not-system-illumos-blah:default.log"
        )
        .is_none());
        assert!(oxide_smf_service_name_from_log_file_name(
            "system-blah:default.log"
        )
        .is_none());
    }
}
