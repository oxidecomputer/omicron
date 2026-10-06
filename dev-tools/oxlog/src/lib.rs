// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A tool to show oxide related log file paths
//!
//! All data is based off of reading the filesystem
//!
//! # Dating log files
//!
//! Filtering by a [`DateRange`] needs the span of time in which each file's
//! content was written (its [`LogAge`]). A file's own timestamps don't
//! reliably give this span:
//!
//! - logadm(8) rotates SMF logs (and chrony's logs) by copying and then
//!   truncating them, so a rotated file's creation time is when it was
//!   copied, after its content was written;
//! - sled-agent's debug collector copies rotated files again when archiving
//!   them to a debug dataset, which resets their `mtime` to the time of
//!   archival (it records the source's `mtime` in the archived file's name
//!   instead);
//! - truncation leaves a live log's creation time unchanged, so it can be
//!   arbitrarily earlier than the live log's content.
//!
//! Instead, files are dated by their *series*.
//!
//! ## Series
//!
//! In oxlog, a **series** is the set of files that, one after another, have
//! held a single stream of log output. For example:
//!
//! - for one instance of an SMF service in one zone: its live file
//!   `foo.log`, the rotated `foo.log.0` beside it, and the archived
//!   `foo.log.<epoch>` files in each of the zone's debug datasets;
//! - for one CockroachDB log: all of its `<prefix>.<...>.log` files.
//!
//! Which files form a series is decided by the naming rules of whatever
//! writes each directory. A file that doesn't follow those rules is a series
//! of its own.
//!
//! A series' files never overlap in time: each rotation, whether by copying
//! or by starting a new file, begins the new file after the previous file's
//! last write. So, with a series sorted by newest write:
//!
//! - a file's newest write is its `mtime`, except for archived files, whose
//!   names record the `mtime` of the file they were copied from;
//! - a file's oldest write is the newest write of the next-older file in its
//!   series, which is at or before the file's first line;
//! - the oldest file in a series has no known oldest write.

use anyhow::Context;
use camino::{Utf8DirEntry, Utf8Path, Utf8PathBuf};
use glob::Pattern;
use jiff::Timestamp;
use rayon::prelude::*;
use std::collections::{BTreeMap, HashMap};
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

    /// Show a log file if its content may overlap this date range, judged
    /// by its [`LogAge`].
    pub date_range: Option<DateRange>,
}

/// The range of time a file's content must overlap to be included.
/// Both bounds are inclusive.
#[derive(Copy, Clone, Debug)]
pub struct DateRange {
    /// The end of the range: files whose oldest write is after this are
    /// excluded.
    before: Timestamp,
    /// The start of the range: files whose newest write is before this are
    /// excluded.
    after: Timestamp,
}

impl DateRange {
    pub fn new(before: Timestamp, after: Timestamp) -> Self {
        Self { before, after }
    }
}

/// The span of time in which a log file's content may have been written.
///
/// See the [module documentation](crate#dating-log-files) for how this is
/// derived.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogAge {
    /// When the file was last written.
    pub newest_write: Timestamp,
    /// A time at or before the file's first write, or `None` if no bound is
    /// known. This may be earlier than the first write, but not later.
    pub oldest_write: Option<Timestamp>,
}

impl LogAge {
    /// Returns true if the file's content may overlap `date_range`
    /// (inclusive on both ends).
    pub fn overlaps(&self, date_range: &DateRange) -> bool {
        self.oldest_write.is_none_or(|oldest| oldest <= date_range.before)
            && self.newest_write >= date_range.after
    }
}

/// Path and metadata about a logfile
/// We use options for metadata as retrieval is fallible
#[derive(Debug, Clone, Eq)]
pub struct LogFile {
    pub path: Utf8PathBuf,
    pub size: Option<u64>,
    pub modified: Option<Timestamp>,
    /// When the file's content may have been written. Populated whenever the
    /// file's metadata is read, and `None` if its `mtime` could not be read.
    pub age: Option<LogAge>,
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

    /// Returns true if the file's content may overlap `date_range`
    /// (inclusive on both ends). A file without a known [`LogAge`] (e.g.,
    /// one we failed to stat) is excluded.
    pub fn in_date_range(&self, date_range: &DateRange) -> bool {
        self.age.is_some_and(|age| age.overlaps(date_range))
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
        LogFile { path, size: None, modified: None, age: None }
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
    pub extra: Vec<(ExtraLogDir, Utf8PathBuf)>,
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
                    ExtraLogDir::Dendrite,
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
                    paths.extra.push((ExtraLogDir::Cockroachdb, dir));
                }

                // Grab the chrony logs that are not apart of the standard SMF
                // logs.
                if zone.starts_with("oxz_ntp") {
                    let mut dir = zones_path.clone();
                    dir.push(zone);
                    dir.push("root/var/log/chrony");
                    paths.extra.push((ExtraLogDir::Ntp, dir));
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
    ///
    /// Every file's [`LogAge`] is derived from all of the zone's files that
    /// are loaded, so the date range is applied only after every directory
    /// has been read: a series' files may be spread across the zone's primary
    /// directory and several debug datasets.
    pub fn zone_logs(
        &self,
        zone: &str,
        filter: Filter,
    ) -> BTreeMap<ServiceName, SvcLogs> {
        let mut output: BTreeMap<ServiceName, SvcLogs> = BTreeMap::new();
        let Some(paths) = self.zones.get(zone) else {
            return BTreeMap::new();
        };

        // Stat the files only if necessary.
        let read_metadata = !filter.show_empty || filter.date_range.is_some();
        let mut undated = Vec::new();

        // Some rotated files exist in `paths.primary` that we track as
        // 'archived'. These files have not yet been migrated into the debug
        // directory.
        //
        // A directory that is missing or can't be read just has no logs to
        // report.
        if filter.current || filter.archived {
            undated.extend(
                load_svc_logs(
                    &paths.primary,
                    SvcLogDir::Primary,
                    read_metadata,
                )
                .unwrap_or_default(),
            );
        }

        if filter.archived {
            for dir in &paths.debug {
                undated.extend(
                    load_svc_logs(dir, SvcLogDir::Debug, read_metadata)
                        .unwrap_or_default(),
                );
            }
        }
        if filter.extra {
            for &(extra_dir, ref dir) in &paths.extra {
                if let Ok(files) =
                    load_extra_logs(dir, extra_dir, read_metadata)
                {
                    // Report the service even if it has no logs, as long as
                    // its log directory exists.
                    output
                        .entry(extra_dir.service_name().to_string())
                        .or_default();
                    undated.extend(files);
                }
            }
        }

        for DatedLogFile { service, kind, file } in date_files(undated) {
            // Empty files are dated along with the rest of their series:
            // their newest writes still bound their neighbors' oldest writes.
            if !filter.show_empty && file.size == Some(0) {
                continue;
            }
            if let Some(date_range) = &filter.date_range {
                if !file.in_date_range(date_range) {
                    continue;
                }
            }
            let svc_logs = output.entry(service).or_default();
            match kind {
                LogKind::Current => svc_logs.current = Some(file),
                LogKind::Archived => svc_logs.archived.push(file),
                LogKind::Extra => svc_logs.extra.push(file),
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

    /// Return the names of zones with at least one log file matching
    /// `filter`, in sorted order.
    ///
    /// A zone appears here exactly when [`Self::zone_logs`] with the same
    /// filter would return at least one file for it, so callers can use
    /// this to skip zones whose per-zone log retrieval would come back
    /// empty.
    pub fn zones_with_matching_logs(&self, filter: Filter) -> Vec<ZoneName> {
        self.zones
            .par_iter()
            .filter(|(zone, _)| !self.zone_logs(zone, filter).is_empty())
            .map(|(zone, _)| zone.clone())
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

/// Strips a trailing `.<digits>` suffix from `filename`, if it has one.
///
/// Returns the remaining name and the digits.
fn split_numeric_suffix(filename: &str) -> (&str, Option<&str>) {
    match filename.rsplit_once('.') {
        Some((name, suffix))
            if !suffix.is_empty()
                && suffix.bytes().all(|b| b.is_ascii_digit()) =>
        {
            (name, Some(suffix))
        }
        _ => (filename, None),
    }
}

/// Returns the name of the series that an SMF log file belongs to: its live
/// file's name.
///
/// SMF log files are named `<service>.log` (live), `<service>.log.<N>`
/// (rotated by logadm(8)), and `<service>.log.<epoch>` (archived by
/// sled-agent's debug collector). Keying on the full live file name, rather
/// than the service name, keeps the series of separate instances of a
/// service apart.
fn smf_series(filename: &str) -> &str {
    split_numeric_suffix(filename).0
}

/// A directory of log files kept outside SMF's log directories.
///
/// Each directory's files are grouped into [series](crate#series) by the
/// naming rules of whatever names them, not by a guess that applies to every
/// directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExtraLogDir {
    /// CockroachDB's own logs (`/data/logs` in a CockroachDB zone).
    ///
    /// CockroachDB writes several logs here, each as a sequence of files that
    /// it starts anew on each rotation and on each restart. For example (with
    /// the host, `oxzcockroachdb<zone ID>`, abbreviated):
    ///
    /// ```text
    /// cockroach.log                    (symlink to the current file)
    /// cockroach.oxzcockroachdb….root.2026-09-28T20_25_39Z.016211.log
    /// cockroach.oxzcockroachdb….root.2026-09-29T09_44_35Z.006190.log
    /// cockroach-health.log             (symlink to the current file)
    /// cockroach-health.oxzcockroachdb….root.2026-09-22T03_29_59Z.003419.log
    /// cockroach-health.oxzcockroachdb….root.2026-09-22T03_44_19Z.015495.log
    /// goroutine_dump/
    /// ```
    ///
    /// Files are named `<prefix>.<...>.log`, one series per `<prefix>`
    /// (`cockroach`, `cockroach-health`, ...). Only the prefix is relied on:
    /// CockroachDB constructs it to never contain a `.`, and groups its own
    /// files by it (see `FileNamePattern` and `normalizeFileName` in
    /// CockroachDB's `pkg/util/log`). What comes between it and `.log`
    /// (currently the host, user, timestamp, and pid) may change.
    ///
    /// `<prefix>.log` is a symlink to the current file; it takes its target's
    /// age rather than being part of a series. Anything else, like the
    /// `goroutine_dump` directory, is a series of its own.
    Cockroachdb,
    /// chrony's logs (`/var/log/chrony` in an NTP zone), reported under the
    /// `ntp` service.
    ///
    /// logadm(8) rotates them using the template `$file.$secs` and
    /// compression (see `smf/chrony-setup/etc/logadm.d/chrony.logadm.conf`):
    /// `<name>.log` (live) and `<name>.log.<secs>.gz` (rotated), one series
    /// per `<name>.log`.
    Ntp,
    /// dpd's working directory (`/var/dendrite` in the switch zone).
    ///
    /// This holds no logs: dpd sends the Tofino SDE's driver logs to its own
    /// SMF log (as `"unit":"bf-sde"`), not to the zlog file its configuration
    /// here names. So no naming rule is applied, and any file here is a
    /// series of its own.
    //
    // TODO(https://github.com/oxidecomputer/omicron/issues/11390): oxlog
    // likely doesn't need to list this directory at all.
    Dendrite,
}

impl ExtraLogDir {
    /// The service the directory's files are reported under, alongside that
    /// service's SMF logs: the name oxlog derives from its SMF log file (see
    /// [`oxide_smf_service_name_from_log_file_name`]).
    pub fn service_name(self) -> &'static str {
        match self {
            Self::Cockroachdb => "cockroachdb",
            Self::Ntp => "ntp",
            Self::Dendrite => "dendrite",
        }
    }

    /// Returns the name of the series that a file in this directory belongs
    /// to, or `None` if the file doesn't follow the directory's naming rules
    /// and is a series of its own.
    fn series(self, filename: &str) -> Option<&str> {
        match self {
            Self::Cockroachdb => {
                // `<prefix>.<...>.log`. Requiring something between the prefix
                // and `.log` excludes `<prefix>.log`, the current file's
                // symlink.
                let (prefix, rest) = filename.split_once('.')?;
                let middle = rest.strip_suffix(".log")?;
                (!prefix.is_empty() && !middle.is_empty()).then_some(prefix)
            }
            Self::Ntp => {
                let name = filename.strip_suffix(".gz").unwrap_or(filename);
                let (name, _) = split_numeric_suffix(name);
                name.ends_with(".log").then_some(name)
            }
            Self::Dendrite => None,
        }
    }
}

/// Parses a trailing `.<seconds>` suffix of `filename` as a Unix timestamp.
fn epoch_from_filename(filename: &str) -> Option<Timestamp> {
    let (_, epoch) = split_numeric_suffix(filename);
    Timestamp::from_second(epoch?.parse().ok()?).ok()
}

/// Which kind of directory SMF log files are loaded from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SvcLogDir {
    /// A zone's SMF log directory, holding live and rotated files.
    Primary,
    /// A debug dataset's directory of archived files for a zone.
    Debug,
}

/// Which of a service's kinds of logs (see [`SvcLogs`]) a file is reported
/// as. These are the same kinds that [`Filter`] selects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LogKind {
    /// Reported in [`SvcLogs::current`].
    Current,
    /// Reported in [`SvcLogs::archived`]: rotated files, whether still in
    /// the zone or archived to a debug dataset.
    Archived,
    /// Reported in [`SvcLogs::extra`].
    Extra,
}

/// A log file found while loading a zone, before it is dated.
#[derive(Debug)]
struct UndatedLogFile {
    /// The service the file is reported under.
    service: ServiceName,
    kind: LogKind,
    /// The series the file belongs to within `service` (see [`smf_series`]
    /// and [`ExtraLogDir::series`]), or `None` if it is a series of its own.
    series: Option<String>,
    /// For a symlink, the path it points to.
    symlink_target: Option<Utf8PathBuf>,
    /// The file's newest write, if its metadata was read successfully. Not
    /// used for symlinks, which take their target's age.
    newest_write: Option<Timestamp>,
    path: Utf8PathBuf,
    size: Option<u64>,
    modified: Option<Timestamp>,
}

impl UndatedLogFile {
    fn into_dated(self, age: Option<LogAge>) -> DatedLogFile {
        let UndatedLogFile { service, kind, path, size, modified, .. } = self;
        DatedLogFile {
            service,
            kind,
            file: LogFile { path, size, modified, age },
        }
    }
}

/// A log file with its age assigned, ready for date filtering.
#[derive(Debug)]
struct DatedLogFile {
    /// The service the file is reported under.
    service: ServiceName,
    kind: LogKind,
    file: LogFile,
}

/// An SMF log file's newest write, as determined when it is loaded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NewestWrite {
    /// The file couldn't be stat'd (most likely because it was removed after
    /// its directory was read), so it has no age.
    Unknown,
    /// The file's newest write, which can bound the oldest writes of its
    /// neighbors in its series.
    InSeries(Timestamp),
    /// A time at or after the file's newest write. That is safe for dating
    /// the file alone (it can only widen the file's age), but not as a bound
    /// on its neighbors', so the file is a series of its own.
    Alone(Timestamp),
}

/// Determines an SMF log file's newest write from its name and `modified`,
/// the `mtime` from its stat (`None` if the stat failed).
fn svc_log_newest_write(
    svc_log_dir: SvcLogDir,
    filename: &str,
    modified: Option<Timestamp>,
) -> NewestWrite {
    let Some(modified) = modified else {
        return NewestWrite::Unknown;
    };
    match svc_log_dir {
        SvcLogDir::Primary => NewestWrite::InSeries(modified),
        SvcLogDir::Debug => match epoch_from_filename(filename) {
            // The debug collector names archived files `<name>.<epoch>`,
            // where `<epoch>` is the source file's `mtime` when it was
            // archived: the archived file's newest write. (The copy's own
            // `mtime` is the time of archival.)
            Some(epoch) => NewestWrite::InSeries(epoch),
            // Without the source's mtime in its name, only the time of
            // archival is known, which is later than the file's newest write.
            None => NewestWrite::Alone(modified),
        },
    }
}

// Given a directory, find all oxide specific SMF service logs.
//
// Returns an error only if `dir` itself can't be read; entries that can't be
// read are skipped.
fn load_svc_logs(
    dir: &Utf8Path,
    svc_log_dir: SvcLogDir,
    read_metadata: bool,
) -> io::Result<Vec<UndatedLogFile>> {
    let mut undated = Vec::new();
    for entry in dir.read_dir_utf8()? {
        let Ok(entry) = entry else {
            continue;
        };
        let filename = entry.file_name();

        // Is this a log file we care about?
        if !is_oxide_smf_log_file(filename) {
            continue;
        }
        let Some(svc_name) =
            oxide_smf_service_name_from_log_file_name(filename)
        else {
            // parsing failed
            continue;
        };

        let mut file = LogFile::new(dir.join(filename));
        let mut series = Some(smf_series(filename).to_string());
        let mut newest_write = None;
        if read_metadata {
            file.read_metadata(&entry);
            match svc_log_newest_write(svc_log_dir, filename, file.modified) {
                NewestWrite::Unknown => {}
                NewestWrite::InSeries(t) => newest_write = Some(t),
                NewestWrite::Alone(t) => {
                    newest_write = Some(t);
                    series = None;
                }
            }
        }

        let kind = if filename.ends_with(".log") {
            LogKind::Current
        } else {
            LogKind::Archived
        };

        let LogFile { path, size, modified, .. } = file;
        undated.push(UndatedLogFile {
            service: svc_name.to_string(),
            kind,
            series,
            symlink_target: None,
            newest_write,
            path,
            size,
            modified,
        });
    }
    Ok(undated)
}

// Load any logs in non-standard paths. We grab all logs in `dir` and
// don't filter based on filename prefix as in `load_svc_logs`.
//
// Returns an error only if `dir` itself can't be read; entries that can't be
// read are skipped.
fn load_extra_logs(
    dir: &Utf8Path,
    extra_dir: ExtraLogDir,
    read_metadata: bool,
) -> io::Result<Vec<UndatedLogFile>> {
    let mut undated = Vec::new();
    for entry in dir.read_dir_utf8()? {
        let Ok(entry) = entry else {
            continue;
        };
        let filename = entry.file_name();
        let path = dir.join(filename);

        let is_symlink = entry.file_type().is_ok_and(|t| t.is_symlink());
        // A relative symlink target is relative to the symlink's directory
        // (an absolute one replaces `dir` entirely).
        let symlink_target = is_symlink
            .then(|| path.read_link_utf8().ok().map(|target| dir.join(target)))
            .flatten();

        let mut file = LogFile::new(path);
        let mut newest_write = None;
        if read_metadata {
            file.read_metadata(&entry);
            if !is_symlink {
                newest_write = file.modified;
            }
        }

        let LogFile { path, size, modified, .. } = file;
        undated.push(UndatedLogFile {
            service: extra_dir.service_name().to_string(),
            kind: LogKind::Extra,
            series: if is_symlink {
                None
            } else {
                extra_dir.series(filename).map(str::to_string)
            },
            symlink_target,
            newest_write,
            path,
            size,
            modified,
        });
    }
    Ok(undated)
}

/// Dates each file by its series (see the [module
/// documentation](crate#series)), returning one [`DatedLogFile`] per input,
/// in the same order.
///
/// - A file takes the newest write of the next-older file in its series as
///   its oldest write. Ties are skipped: of two files with the same newest
///   write, either could be the older one, so each is bounded by the next
///   strictly older file instead. The oldest file of a series (including a
///   file that is a series of its own) has no known oldest write.
/// - A symlink takes the age of the file it points to. Symlinks are kept out
///   of every series: a symlink shares its target's newest write, and so
///   could otherwise become its own target's next-older file. If the target
///   isn't among `files`, the symlink is dated by the target's `mtime`
///   alone.
/// - A file whose newest write is unknown (e.g., one we failed to stat) has
///   no age.
fn date_files(files: Vec<UndatedLogFile>) -> Vec<DatedLogFile> {
    // Group the files that are in a series by (service, series).
    let mut series: BTreeMap<(&str, &str), Vec<(usize, Timestamp)>> =
        BTreeMap::new();
    for (i, file) in files.iter().enumerate() {
        if let (Some(name), Some(newest_write)) =
            (&file.series, file.newest_write)
        {
            series
                .entry((&file.service, name))
                .or_default()
                .push((i, newest_write));
        }
    }

    let mut ages: Vec<Option<LogAge>> = files
        .iter()
        .map(|file| {
            file.newest_write
                .map(|newest_write| LogAge { newest_write, oldest_write: None })
        })
        .collect();

    for mut members in series.into_values() {
        // Sort the series newest first.
        members
            .sort_by_key(|&(_, newest_write)| std::cmp::Reverse(newest_write));
        for (position, &(i, newest_write)) in members.iter().enumerate() {
            let next_older = members[position + 1..]
                .iter()
                .map(|&(_, older)| older)
                .find(|&older| older < newest_write);
            if let Some(age) = &mut ages[i] {
                age.oldest_write = next_older;
            }
        }
    }

    // Symlinks take their target's age.
    let symlink_ages: Vec<(usize, Option<LogAge>)> = {
        let ages_by_path: HashMap<&Utf8Path, LogAge> = files
            .iter()
            .zip(&ages)
            .filter(|(file, _)| file.symlink_target.is_none())
            .filter_map(|(file, age)| Some((file.path.as_path(), (*age)?)))
            .collect();
        files
            .iter()
            .enumerate()
            .filter_map(|(i, file)| {
                let target = file.symlink_target.as_deref()?;
                // Only date the symlink if its metadata was read.
                file.modified?;
                let age = ages_by_path.get(target).copied().or_else(|| {
                    let modified = target.metadata().ok()?.modified().ok()?;
                    Some(LogAge {
                        newest_write: modified.try_into().ok()?,
                        oldest_write: None,
                    })
                });
                Some((i, age))
            })
            .collect()
    };
    for (i, age) in symlink_ages {
        ages[i] = age;
    }

    files
        .into_iter()
        .zip(ages)
        .map(|(file, age)| file.into_dated(age))
        .collect()
}

#[cfg(test)]
mod tests {
    pub use super::is_oxide_smf_log_file;
    pub use super::oxide_smf_service_name_from_log_file_name;

    #[test]
    fn test_zones_with_matching_logs() {
        use super::{DateRange, Filter, Paths, Zones};
        use jiff::Timestamp;
        use std::collections::BTreeMap;

        let dir = camino_tempfile::tempdir().unwrap();
        let mtime = |secs: u64| {
            std::time::SystemTime::UNIX_EPOCH
                + std::time::Duration::from_secs(secs)
        };

        // Three zones: one whose log was last written long ago, one
        // written recently, and one whose only log is empty.
        let mut zones = BTreeMap::new();
        for (zone, mtime_secs, contents) in [
            ("oxz_old", 1_000, "stale but real data"),
            ("oxz_recent", 2_000_000, "fresh data"),
            ("oxz_empty", 2_000_000, ""),
        ] {
            let logdir = dir.path().join(zone).join("var/svc/log");
            std::fs::create_dir_all(&logdir).unwrap();
            let logfile = logdir.join("oxide-svc:default.log");
            std::fs::write(&logfile, contents).unwrap();
            std::fs::File::open(&logfile)
                .unwrap()
                .set_modified(mtime(mtime_secs))
                .unwrap();
            zones.insert(
                zone.to_string(),
                Paths { primary: logdir, debug: vec![], extra: vec![] },
            );
        }
        let zones = Zones { zones };

        let filter = |date_range| Filter {
            current: true,
            archived: true,
            extra: true,
            show_empty: false,
            date_range,
        };
        let ts = |secs| Timestamp::from_second(secs).unwrap();

        // With no date range, every zone with a non-empty log matches.
        assert_eq!(
            zones.zones_with_matching_logs(filter(None)),
            ["oxz_old", "oxz_recent"]
        );

        // A range covering only the recent mtime excludes the old zone.
        let recent_only = DateRange::new(ts(3_000_000), ts(1_000_000));
        assert_eq!(
            zones.zones_with_matching_logs(filter(Some(recent_only))),
            ["oxz_recent"]
        );

        // A range after every mtime matches no zone at all.
        let after_everything = DateRange::new(ts(4_000_000), ts(3_000_000));
        assert_eq!(
            zones.zones_with_matching_logs(filter(Some(after_everything))),
            Vec::<String>::new()
        );

        // A range predating every mtime still matches each non-empty zone:
        // each zone's only file is the oldest in its series, so it has no
        // known oldest write and may hold content from any earlier time.
        let before_everything = DateRange::new(ts(900), ts(0));
        assert_eq!(
            zones.zones_with_matching_logs(filter(Some(before_everything))),
            ["oxz_old", "oxz_recent"]
        );
    }

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
                        age: None,
                    },
                    LogFile {
                        path: "/foo/blah:default.log.1600000000".into(),
                        size: None,
                        modified: None,
                        age: None,
                    },
                ],
                extra: vec![
                    // "foo" comes after "bar", but the sorted order should
                    // have log1 before log2.
                    LogFile {
                        path: "/foo/blah/sub.default.log1".into(),
                        size: None,
                        modified: None,
                        age: None,
                    },
                    LogFile {
                        path: "/bar/blah/sub.default.log2".into(),
                        size: None,
                        modified: None,
                        age: None,
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

    fn ts(s: &str) -> jiff::Timestamp {
        s.parse().expect("test timestamp should be valid RFC 3339")
    }

    fn range(after: &str, before: &str) -> super::DateRange {
        super::DateRange::new(ts(before), ts(after))
    }

    #[test]
    fn test_log_age_overlaps() {
        use super::LogAge;

        let window = range("2024-01-01T01:00:00Z", "2024-01-01T01:59:00Z");
        let age = |oldest: Option<&str>, newest: &str| LogAge {
            newest_write: ts(newest),
            oldest_write: oldest.map(ts),
        };

        // Content spanning the end of the window overlaps it.
        assert!(
            age(Some("2024-01-01T01:30:00Z"), "2024-01-01T03:00:00Z")
                .overlaps(&window)
        );
        // Content spanning the whole window overlaps it.
        assert!(
            age(Some("2024-01-01T00:00:00Z"), "2024-01-01T03:00:00Z")
                .overlaps(&window)
        );
        // Content entirely before or after the window doesn't.
        assert!(
            !age(Some("2023-01-01T00:00:00Z"), "2024-01-01T00:59:59Z")
                .overlaps(&window)
        );
        assert!(
            !age(Some("2024-01-01T02:00:00Z"), "2024-01-01T03:00:00Z")
                .overlaps(&window)
        );
        // Without an oldest write, only the newest write limits a file.
        assert!(age(None, "2024-01-01T03:00:00Z").overlaps(&window));
        assert!(!age(None, "2024-01-01T00:59:59Z").overlaps(&window));
        // Bounds are inclusive.
        assert!(
            age(Some("2024-01-01T01:59:00Z"), "2024-01-01T03:00:00Z")
                .overlaps(&window)
        );
        assert!(
            age(Some("2023-01-01T00:00:00Z"), "2024-01-01T01:00:00Z")
                .overlaps(&window)
        );
    }

    #[test]
    fn test_smf_series() {
        use super::smf_series;

        for filename in [
            "oxide-nexus:default.log",
            "oxide-nexus:default.log.0",
            "oxide-nexus:default.log.12",
            "oxide-nexus:default.log.1790585101",
        ] {
            assert_eq!(smf_series(filename), "oxide-nexus:default.log");
        }
        // Separate instances of a service are separate series.
        assert_eq!(
            smf_series("oxide-foo:instance2.log.0"),
            "oxide-foo:instance2.log"
        );
    }

    #[test]
    fn test_epoch_from_filename() {
        use super::epoch_from_filename;

        assert_eq!(
            epoch_from_filename("oxide-nexus:default.log.1790585101"),
            Some(jiff::Timestamp::from_second(1790585101).unwrap())
        );
        assert_eq!(epoch_from_filename("oxide-nexus:default.log"), None);
        assert_eq!(epoch_from_filename("oxide-nexus:default.log.x1"), None);
    }

    #[test]
    fn test_extra_log_dir_series() {
        use super::ExtraLogDir;

        // CockroachDB log files, as named on a real system.
        let zone = "oxzcockroachdb8bbea076-ff60-4330-8302-383e18140ef3";
        let crdb = ExtraLogDir::Cockroachdb;
        assert_eq!(
            crdb.series(&format!(
                "cockroach.{zone}.root.2026-09-29T09_44_35Z.006190.log"
            )),
            Some("cockroach")
        );
        assert_eq!(
            crdb.series(&format!(
                "cockroach-health.{zone}.root.2026-09-22T03_29_59Z.003419.log"
            )),
            Some("cockroach-health")
        );
        // Only the prefix matters: what comes between it and `.log` may
        // change in future versions of CockroachDB.
        for filename in [
            "cockroach.host.log",
            "cockroach.host.root.2026-09-29T09_44_35Z.log",
            "cockroach.host.root.2026-09-29T09_44_35Z.006190.extra.log",
        ] {
            assert_eq!(crdb.series(filename), Some("cockroach"), "{filename}");
        }
        // The symlinks to the current files, directories, and anything else
        // that doesn't follow CockroachDB's naming are series of their own.
        for filename in [
            "cockroach.log",
            "cockroach-health.log",
            "goroutine_dump",
            ".host.root.log",
            "cockroach.host.root.txt",
        ] {
            assert_eq!(crdb.series(filename), None, "{filename}");
        }

        // chrony's logs, as rotated by logadm.
        for filename in ["tracking.log", "tracking.log.1790705215.gz"] {
            assert_eq!(
                ExtraLogDir::Ntp.series(filename),
                Some("tracking.log"),
                "{filename}"
            );
        }
        assert_eq!(ExtraLogDir::Ntp.series("chrony.conf"), None);

        // No naming rule is known for dendrite's directory.
        assert_eq!(ExtraLogDir::Dendrite.series("zlog-cfg-cur"), None);
        assert_eq!(ExtraLogDir::Dendrite.series("bf_drivers.log"), None);
    }

    #[test]
    fn test_svc_log_newest_write() {
        use super::{NewestWrite, SvcLogDir, svc_log_newest_write};

        let mtime = ts("2026-09-28T08:04:12Z");
        let epoch = ts("2026-09-28T07:59:59Z");
        let archived = format!("oxide-nexus:default.log.{}", epoch.as_second());

        // In the zone, a file's mtime is its newest write.
        assert_eq!(
            svc_log_newest_write(
                SvcLogDir::Primary,
                "oxide-nexus:default.log.0",
                Some(mtime)
            ),
            NewestWrite::InSeries(mtime)
        );
        // An archived file's name records its newest write; its own mtime is
        // the time of archival.
        assert_eq!(
            svc_log_newest_write(SvcLogDir::Debug, &archived, Some(mtime)),
            NewestWrite::InSeries(epoch)
        );
        // Without one in its name, only the time of archival is known.
        assert_eq!(
            svc_log_newest_write(
                SvcLogDir::Debug,
                "oxide-nexus:default.log",
                Some(mtime)
            ),
            NewestWrite::Alone(mtime)
        );
        // A file that couldn't be stat'd has no newest write, even if its
        // name records one.
        for (dir, filename) in [
            (SvcLogDir::Primary, "oxide-nexus:default.log.0"),
            (SvcLogDir::Debug, archived.as_str()),
            (SvcLogDir::Debug, "oxide-nexus:default.log"),
        ] {
            assert_eq!(
                svc_log_newest_write(dir, filename, None),
                NewestWrite::Unknown,
                "{filename}"
            );
        }
    }

    /// Builds an undated file in `series` with the given newest write.
    fn undated(
        path: &str,
        series: Option<&str>,
        newest_write: &str,
    ) -> super::UndatedLogFile {
        super::UndatedLogFile {
            service: "svc".to_string(),
            kind: super::LogKind::Archived,
            series: series.map(str::to_string),
            symlink_target: None,
            newest_write: Some(ts(newest_write)),
            path: path.into(),
            size: None,
            modified: Some(ts(newest_write)),
        }
    }

    fn oldest_writes(dated: &[super::DatedLogFile]) -> Vec<Option<String>> {
        dated
            .iter()
            .map(|d| d.file.age.unwrap().oldest_write.map(|t| t.to_string()))
            .collect()
    }

    #[test]
    fn test_date_files_series_order() {
        // One service's files, as in the example on
        // https://github.com/oxidecomputer/omicron/issues/11357: a live file,
        // a rotated file, and three archived files, in arbitrary order.
        let series = Some("oxide-nexus:default.log");
        let dated = super::date_files(vec![
            undated("archived-b", series, "2026-09-28T03:59:57Z"),
            undated("live", series, "2026-09-28T12:02:10Z"),
            undated("archived-c", series, "2026-09-27T23:59:58Z"),
            undated("rotated", series, "2026-09-28T11:59:58Z"),
            undated("archived-a", series, "2026-09-28T07:59:59Z"),
        ]);

        assert_eq!(
            oldest_writes(&dated),
            vec![
                Some("2026-09-27T23:59:58Z".to_string()),
                Some("2026-09-28T11:59:58Z".to_string()),
                None,
                Some("2026-09-28T07:59:59Z".to_string()),
                Some("2026-09-28T03:59:57Z".to_string()),
            ]
        );

        // Only the archived file holding 05:00-06:00 overlaps it. Judged by
        // their own timestamps, the live file would be included and that
        // archived file excluded.
        let window = range("2026-09-28T05:00:00Z", "2026-09-28T06:00:00Z");
        let included: Vec<&str> = dated
            .iter()
            .filter(|d| d.file.in_date_range(&window))
            .map(|d| d.file.path.as_str())
            .collect();
        assert_eq!(included, vec!["archived-a"]);
    }

    #[test]
    fn test_date_files_separate_series() {
        // Files of different series never bound each other, even when their
        // times interleave.
        let dated = super::date_files(vec![
            undated("a-new", Some("a"), "2026-09-28T12:00:00Z"),
            undated("b-new", Some("b"), "2026-09-28T11:00:00Z"),
            undated("a-old", Some("a"), "2026-09-28T10:00:00Z"),
            undated("b-old", Some("b"), "2026-09-28T09:00:00Z"),
            // A file that is a series of its own.
            undated("alone", None, "2026-09-28T11:30:00Z"),
        ]);

        assert_eq!(
            oldest_writes(&dated),
            vec![
                Some("2026-09-28T10:00:00Z".to_string()),
                Some("2026-09-28T09:00:00Z".to_string()),
                None,
                None,
                None,
            ]
        );
    }

    #[test]
    fn test_date_files_ties() {
        // Of two files with the same newest write, either could be the older
        // one, so both are bounded by the next strictly older file.
        let series = Some("series");
        let dated = super::date_files(vec![
            undated("tie-1", series, "2026-09-28T12:00:00Z"),
            undated("tie-2", series, "2026-09-28T12:00:00Z"),
            undated("older", series, "2026-09-28T10:00:00Z"),
        ]);

        assert_eq!(
            oldest_writes(&dated),
            vec![
                Some("2026-09-28T10:00:00Z".to_string()),
                Some("2026-09-28T10:00:00Z".to_string()),
                None,
            ]
        );
    }

    #[test]
    fn test_date_files_unknown_newest_write() {
        // A file whose mtime couldn't be read has no age, and doesn't bound
        // its neighbors.
        let series = Some("series");
        let mut unknown = undated("unknown", series, "2026-09-28T11:00:00Z");
        unknown.newest_write = None;
        unknown.modified = None;
        let dated = super::date_files(vec![
            undated("newer", series, "2026-09-28T12:00:00Z"),
            unknown,
            undated("older", series, "2026-09-28T10:00:00Z"),
        ]);

        assert_eq!(
            dated[0].file.age.unwrap().oldest_write,
            Some(ts("2026-09-28T10:00:00Z"))
        );
        assert_eq!(dated[1].file.age, None);
        assert_eq!(dated[2].file.age.unwrap().oldest_write, None);
    }

    /// Creates `path` with some content and the given `mtime`.
    fn create_file(path: &camino::Utf8Path, mtime: &str) {
        let file = std::fs::File::create(path).unwrap();
        std::io::Write::write_all(&mut &file, b"log line\n").unwrap();
        file.set_modified(ts(mtime).into()).unwrap();
    }

    #[test]
    fn test_zone_logs_date_range() {
        use super::{ExtraLogDir, Filter, Paths, Zones};
        use std::collections::BTreeMap;

        let dir = camino_tempfile::tempdir().unwrap();
        let primary = dir.path().join("primary");
        let debug = dir.path().join("debug");
        let crdb = dir.path().join("crdb");
        for d in [&primary, &debug, &crdb] {
            std::fs::create_dir(d).unwrap();
        }

        // An SMF service's series. Archived files' own mtimes are the time of
        // archival; their names hold their newest writes.
        let svc = "oxide-nexus:default.log";
        create_file(&primary.join(svc), "2026-09-28T12:02:10Z");
        create_file(&primary.join(format!("{svc}.0")), "2026-09-28T11:59:58Z");
        for (newest_write, archived_at) in [
            ("2026-09-28T07:59:59Z", "2026-09-28T08:04:12Z"),
            ("2026-09-28T03:59:57Z", "2026-09-28T04:03:40Z"),
            ("2026-09-27T23:59:58Z", "2026-09-28T00:04:05Z"),
        ] {
            let epoch = ts(newest_write).as_second();
            create_file(&debug.join(format!("{svc}.{epoch}")), archived_at);
        }

        // A CockroachDB log, with a symlink to its current file.
        let zone = "oxzcockroachdb8bbea076-ff60-4330-8302-383e18140ef3";
        let crdb_file = |ts: &str, pid: &str| {
            format!("cockroach.{zone}.root.{ts}.{pid}.log")
        };
        let crdb_current = crdb_file("2026-09-28T04_00_00Z", "002000");
        create_file(
            &crdb.join(crdb_file("2026-09-27T00_00_00Z", "001000")),
            "2026-09-28T03:00:00Z",
        );
        create_file(&crdb.join(&crdb_current), "2026-09-28T12:00:00Z");
        std::os::unix::fs::symlink(&crdb_current, crdb.join("cockroach.log"))
            .unwrap();

        let zones = Zones {
            zones: BTreeMap::from([(
                "oxz_test".to_string(),
                Paths {
                    primary: primary.clone(),
                    debug: vec![debug.clone()],
                    extra: vec![(ExtraLogDir::Cockroachdb, crdb.clone())],
                },
            )]),
        };
        let logs_in = |after: &str, before: &str| -> Vec<String> {
            let logs = zones.zone_logs(
                "oxz_test",
                Filter {
                    current: true,
                    archived: true,
                    extra: true,
                    show_empty: false,
                    date_range: Some(range(after, before)),
                },
            );
            let mut files: Vec<String> = logs
                .values()
                .flat_map(|svc_logs| {
                    svc_logs
                        .current
                        .iter()
                        .chain(&svc_logs.archived)
                        .chain(&svc_logs.extra)
                })
                .map(|f| f.path.file_name().unwrap().to_string())
                .collect();
            files.sort();
            files
        };

        // 05:00-06:00 falls within the archived file with a newest write of
        // 07:59:59, and within the current CockroachDB file (and so its
        // symlink).
        let archived =
            format!("{svc}.{}", ts("2026-09-28T07:59:59Z").as_second());
        let mut expected =
            vec![archived, crdb_current.clone(), "cockroach.log".to_string()];
        expected.sort();
        assert_eq!(
            logs_in("2026-09-28T05:00:00Z", "2026-09-28T06:00:00Z"),
            expected
        );

        // 12:01-12:05 holds only the live SMF file: the symlink's own mtime
        // (its creation, now) doesn't matter, and the CockroachDB files end
        // before 12:01.
        assert_eq!(
            logs_in("2026-09-28T12:01:00Z", "2026-09-28T12:05:00Z"),
            vec![svc.to_string()]
        );
    }
}
