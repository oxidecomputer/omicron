// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A tool to show oxide related log file paths
//!
//! All data is based off of reading the filesystem

use anyhow::Context;
use camino::{Utf8DirEntry, Utf8Path, Utf8PathBuf};
use glob::Pattern;
use jiff::Timestamp;
use rayon::prelude::*;
use std::collections::BTreeMap;
use std::io;
use uuid::Uuid;

/// Return a UUID if the `DirEntry` contains a directory that parses into a UUID.
fn get_uuid_dir(result: io::Result<Utf8DirEntry>) -> Option<Uuid> {
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
    file_name.parse().ok()
}

#[derive(Debug)]
pub struct Pools {
    pub internal: Vec<Uuid>,
    pub external: Vec<Uuid>,
}

impl Pools {
    pub fn read() -> anyhow::Result<Pools> {
        let internal = Utf8Path::new("/pool/int/")
            .read_dir_utf8()
            .context("Failed to read /pool/int")?
            .filter_map(get_uuid_dir)
            .collect();
        let external = Utf8Path::new("/pool/ext/")
            .read_dir_utf8()
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
    /// The current logfile for a service.
    /// e.g. `/var/svc/log/oxide-sled-agent:default.log`
    pub current: bool,

    /// Any rotated log files in the default service directory or archived to
    /// a debug directory. e.g. `/var/svc/log/oxide-sled-agent:default.log.0`
    /// or `/pool/ext/021afd19-2f87-4def-9284-ab7add1dd6ae/crypt/debug/global/oxide-sled-agent:default.log.1697509861`
    pub archived: bool,

    /// Any files of special interest for a given service that don't reside in
    /// standard paths or don't follow the naming conventions of SMF service
    /// files. e.g. `/pool/ext/e12f29b8-1ab8-431e-bc96-1c1298947980/crypt/zone/oxz_cockroachdb_8bbea076-ff60-4330-8302-383e18140ef3/root/data/logs/cockroach.log`
    pub extra: bool,

    /// Show a log file even if is has zero size.
    pub show_empty: bool,

    /// Show a log file if its `mtime` falls within this date range.
    pub date_range: Option<DateRange>,
}

/// The range of time a file's `mtime` must be in within to be included.
#[derive(Copy, Clone, Debug)]
pub struct DateRange {
    /// Files with `mtime`s equal to or later than this will be excluded.
    before: Timestamp,
    /// Files with `mtime`s equal to or earlier than this will be excluded.
    after: Timestamp,
}

impl DateRange {
    pub fn new(before: Timestamp, after: Timestamp) -> Self {
        Self { before, after }
    }
}

/// Path and metadata about a logfile
/// We use options for metadata as retrieval is fallible
#[derive(Debug, Clone, Eq)]
pub struct LogFile {
    pub path: Utf8PathBuf,
    pub size: Option<u64>,
    pub modified: Option<Timestamp>,
}

impl LogFile {
    pub fn read_metadata(&mut self, entry: &Utf8DirEntry) {
        if let Ok(metadata) = entry.metadata() {
            self.size = Some(metadata.len());
            if let Ok(modified) = metadata.modified() {
                // An mtime that overflows Timestamp is not accurate, ignore.
                self.modified = modified.try_into().ok();
            }
        }
    }

    pub fn file_name_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.path.file_name().cmp(&other.path.file_name())
    }

    pub fn in_date_range(&self, date_range: &DateRange) -> bool {
        let Some(modified) = self.modified else {
            // We failed to stat the file, it probably doesn't exist anymore.
            // Exclude from output.
            return false;
        };

        if modified < date_range.after || modified > date_range.before {
            return false;
        }

        true
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
    /// The current logfile for a service.
    /// e.g. `/var/svc/log/oxide-sled-agent:default.log`
    pub current: Option<LogFile>,

    /// Any rotated log files in the default service directory or archived to
    /// a debug directory. e.g. `/var/svc/log/oxide-sled-agent:default.log.0`
    /// or `/pool/ext/021afd19-2f87-4def-9284-ab7add1dd6ae/crypt/debug/global/oxide-sled-agent:default.log.1697509861`
    pub archived: Vec<LogFile>,

    /// Any files of special interest for a given service that don't reside in
    /// standard paths or don't follow the naming conventions of SMF service
    /// files. e.g. `/pool/ext/e12f29b8-1ab8-431e-bc96-1c1298947980/crypt/zone/oxz_cockroachdb_8bbea076-ff60-4330-8302-383e18140ef3/root/data/logs/cockroach.log`
    pub extra: Vec<LogFile>,
}

impl SvcLogs {
    /// Sort the archived and extra log files by filename.
    ///
    /// readdir traverses over directories in indeterminate order, so sort by
    /// filename (which is enough to sort by service name and timestamp in most
    /// cases).
    ///
    /// Generally we don't want to sort by full path, because log files may be
    /// scattered across several different directories -- and we care more
    /// about filename than which directory they are in.
    pub fn sort_by_file_name(&mut self) {
        self.archived.sort_unstable_by(LogFile::file_name_cmp);
        self.extra.sort_unstable_by(LogFile::file_name_cmp);
    }
}

// These probably don't warrant newtypes. They are just to make the
// keys in maps a bit easier to read.
type ZoneName = String;
type ServiceName = String;

pub struct Paths {
    /// Links to the location of current and rotated log files for a given service
    pub primary: Utf8PathBuf,

    /// Links to debug directories containing archived log files
    pub debug: Vec<Utf8PathBuf>,

    /// Links to directories containing extra files such as cockroachdb logs
    /// that reside outside our SMF log and debug service log paths.
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
            let Ok(entries) = zones_path.read_dir_utf8() else {
                continue;
            };
            for entry in entries {
                let Ok(zone_entry) = entry else {
                    continue;
                };
                let zone = zone_entry.file_name();

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
            let Ok(entries) = zones_path.read_dir_utf8() else {
                continue;
            };
            for entry in entries {
                let Ok(zone_entry) = entry else {
                    continue;
                };
                let zone = zone_entry.file_name();
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
        // Some rotated files exist in `paths.primary` that we track as
        // 'archived'. These files have not yet been migrated into the debug
        // directory.
        if filter.current || filter.archived {
            load_svc_logs(
                paths.primary.clone(),
                &mut output,
                filter.show_empty,
                filter.date_range,
            );
        }

        if filter.archived {
            for dir in paths.debug.clone() {
                load_svc_logs(
                    dir,
                    &mut output,
                    filter.show_empty,
                    filter.date_range,
                );
            }
        }
        if filter.extra {
            for (svc_name, dir) in paths.extra.clone() {
                load_extra_logs(
                    dir,
                    svc_name,
                    &mut output,
                    filter.show_empty,
                    filter.date_range,
                );
            }
        }

        sort_logs(&mut output);
        output
    }

    /// Return log files for all zones whose names match `zone_pattern`
    pub fn matching_zone_logs(
        &self,
        zone_pattern: &Pattern,
        filter: Filter,
    ) -> Vec<BTreeMap<ServiceName, SvcLogs>> {
        self.zones
            .par_iter()
            .filter(|(zone, _)| zone_pattern.matches(zone))
            .map(|(zone, _)| self.zone_logs(zone, filter))
            .collect()
    }
}

fn sort_logs(output: &mut BTreeMap<String, SvcLogs>) {
    for svc_logs in output.values_mut() {
        svc_logs.sort_by_file_name();
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
fn load_svc_logs(
    dir: Utf8PathBuf,
    logs: &mut BTreeMap<ServiceName, SvcLogs>,
    show_empty: bool,
    date_range: Option<DateRange>,
) {
    let Ok(entries) = dir.read_dir_utf8() else {
        return;
    };
    for entry in entries {
        let Ok(entry) = entry else {
            continue;
        };
        let filename = entry.file_name();

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

            // Stat the file only if necessary.
            if !show_empty || date_range.is_some() {
                logfile.read_metadata(&entry);
            }

            if !show_empty {
                if logfile.size == Some(0) {
                    // skip 0 size files
                    continue;
                }
            }

            if let Some(date_range) = date_range {
                if !logfile.in_date_range(&date_range) {
                    continue;
                }
            }

            let is_current = filename.ends_with(".log");

            let svc_logs = logs.entry(svc_name.to_string()).or_default();

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
    show_empty: bool,
    date_range: Option<DateRange>,
) {
    let Ok(entries) = dir.read_dir_utf8() else {
        return;
    };

    let svc_logs = logs.entry(svc_name.to_string()).or_default();

    for entry in entries {
        let Ok(entry) = entry else {
            continue;
        };
        let filename = entry.file_name();
        let mut path = dir.clone();
        path.push(filename);
        let mut logfile = LogFile::new(path);

        // Stat the file only if necessary.
        if !show_empty || date_range.is_some() {
            logfile.read_metadata(&entry);
        }

        if !show_empty {
            if logfile.size == Some(0) {
                // skip 0 size files
                continue;
            }
        }

        if let Some(date_range) = date_range {
            if !logfile.in_date_range(&date_range) {
                continue;
            }
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
        assert!(
            oxide_smf_service_name_from_log_file_name(
                "not-oxide-blah:default.log"
            )
            .is_none()
        );
        assert!(
            oxide_smf_service_name_from_log_file_name(
                "not-system-illumos-blah:default.log"
            )
            .is_none()
        );
        assert!(
            oxide_smf_service_name_from_log_file_name(
                "system-blah:default.log"
            )
            .is_none()
        );
    }

    #[test]
    fn test_sort_logs() {
        use super::{LogFile, SvcLogs};
        use std::collections::BTreeMap;

        let mut logs = BTreeMap::new();
        logs.insert(
            "blah".to_string(),
            SvcLogs {
                current: None,
                archived: vec![
                    // "foo" comes after "bar", but the sorted order should
                    // have 1600000000 before 1700000000.
                    LogFile {
                        path: "/bar/blah:default.log.1700000000".into(),
                        size: None,
                        modified: None,
                    },
                    LogFile {
                        path: "/foo/blah:default.log.1600000000".into(),
                        size: None,
                        modified: None,
                    },
                ],
                extra: vec![
                    // "foo" comes after "bar", but the sorted order should
                    // have log1 before log2.
                    LogFile {
                        path: "/foo/blah/sub.default.log1".into(),
                        size: None,
                        modified: None,
                    },
                    LogFile {
                        path: "/bar/blah/sub.default.log2".into(),
                        size: None,
                        modified: None,
                    },
                ],
            },
        );

        super::sort_logs(&mut logs);

        let svc_logs = logs.get("blah").unwrap();
        assert_eq!(
            svc_logs.archived[0].path,
            "/foo/blah:default.log.1600000000"
        );
        assert_eq!(
            svc_logs.archived[1].path,
            "/bar/blah:default.log.1700000000"
        );
        assert_eq!(svc_logs.extra[0].path, "/foo/blah/sub.default.log1");
        assert_eq!(svc_logs.extra[1].path, "/bar/blah/sub.default.log2");
    }

    #[test]
    fn test_daterange_filter() {
        use super::{DateRange, LogFile};
        use camino::Utf8PathBuf;
        use jiff::Timestamp;

        let old_log = LogFile {
            path: Utf8PathBuf::from("old"),
            size: None,
            modified: Some("1950-01-01T00:00:00Z".parse().unwrap()),
        };
        let new_log = LogFile {
            path: Utf8PathBuf::from("new"),
            size: None,
            modified: Some("2050-01-01T00:00:00Z".parse().unwrap()),
        };
        let matched_log = LogFile {
            path: Utf8PathBuf::from("just_right"),
            size: None,
            modified: Some("2024-01-01T00:00:00Z".parse().unwrap()),
        };

        let full_date_range = DateRange {
            before: "2025-01-01T23:59:59Z".parse().unwrap(),
            after: "1986-01-01T00:00:01Z".parse().unwrap(),
        };

        assert!(!old_log.in_date_range(&full_date_range));
        assert!(!new_log.in_date_range(&full_date_range));
        assert!(matched_log.in_date_range(&full_date_range));

        // Range if '--after` is not set.
        let min_after_date_range = DateRange {
            before: "2025-01-01T23:59:59Z".parse().unwrap(),
            after: Timestamp::MIN,
        };

        assert!(old_log.in_date_range(&min_after_date_range));
        assert!(!new_log.in_date_range(&min_after_date_range));
        assert!(matched_log.in_date_range(&min_after_date_range));

        // Range if '--before` is not set.
        let max_before_date_range = DateRange {
            before: Timestamp::MAX,
            after: "1986-01-01T00:00:01Z".parse().unwrap(),
        };

        assert!(!old_log.in_date_range(&max_before_date_range));
        assert!(new_log.in_date_range(&max_before_date_range));
        assert!(matched_log.in_date_range(&max_before_date_range));
    }
}
