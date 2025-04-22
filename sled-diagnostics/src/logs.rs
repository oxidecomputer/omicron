// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled Diagnostics log collection.

use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    io::{BufRead, BufReader, Seek, Write},
};

use camino::{Utf8Path, Utf8PathBuf};
use fs_err::File;
use illumos_utils::zfs::{
    CreateSnapshotError, GetValueError, ListDatasetsError, Snapshot, Zfs,
};
use oxlog::LogFile;
use rand::{Rng, distributions::Alphanumeric, thread_rng};
use slog::Logger;
use zip::{result::ZipError, write::FullFileOptions};

// The name of the snapshot created from the zone root filesystem.
//
// We will attach a unique sufix to ensure concurrently running tasks do not
// attempt to create or delete the same snapshots.
const SLED_DIAGNOSTICS_SNAPSHOT_PREFIX: &'static str = "sled-diagnostics-";

// An extra ZFS user property attached to all sled-diagnostic snapshots.
//
// This is used to ensure that we are not accidentally deleting ZFS objects that
// a user has created, but which happen to be named the same thing.
const SLED_DIAGNOSTICS_ZFS_PROPERTY_NAME: &'static str =
    "oxide:for-sled-diagnostics";
const SLED_DIAGNOSTICS_ZFS_PROPERTY_VALUE: &'static str = "true";

const fn diagnostics_zfs_properties() -> &'static [(&'static str, &'static str)]
{
    &[(SLED_DIAGNOSTICS_ZFS_PROPERTY_NAME, SLED_DIAGNOSTICS_ZFS_PROPERTY_VALUE)]
}

#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("Failed to query oxlog: {0}")]
    OxLog(anyhow::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("ZFS dataset {0} is missing a mountpoint")]
    MissingMountpoint(String),

    #[error("Log file is missing {zone} in path {logfile}")]
    MissingZonePathComponent { zone: String, logfile: Utf8PathBuf },

    #[error(transparent)]
    Snapshot(#[from] CreateSnapshotError),

    #[error(transparent)]
    ZfsGetValue(#[from] GetValueError),

    #[error(transparent)]
    ZfsListDatasets(#[from] ListDatasetsError),

    #[error(transparent)]
    Zip(#[from] ZipError),
}

///A ZFS snapshot that is taken by the `sled-diagnostics` crate and handles
/// snapshot deletion on `Drop`.
#[derive(Debug)]
struct DiagnosticsSnapshot {
    log: Logger,
    /// The ZFS snapshot.
    snapshot: Snapshot,
    /// The mountpoint on disk where this snapshot is mounted.
    snapshot_mountpoint: Utf8PathBuf,
}

impl DiagnosticsSnapshot {
    /// Create a snapshot for a ZFS filesystem
    fn create(logger: &Logger, filesystem: &str) -> Result<Self, LogError> {
        let snap_name = format!(
            "{SLED_DIAGNOSTICS_SNAPSHOT_PREFIX}{}",
            thread_rng()
                .sample_iter(Alphanumeric)
                .take(12)
                .map(char::from)
                .collect::<String>()
        );

        Zfs::create_snapshot(
            filesystem,
            &snap_name,
            diagnostics_zfs_properties(),
        )?;

        let snapshot =
            Snapshot { filesystem: filesystem.to_string(), snap_name };

        debug!(
            logger,
            "created sled-diagnostics ZFS snapshot";
            "snapshot" => %snapshot
        );

        let snapshot_mountpoint =
            DiagnosticsSnapshot::determine_snapshot_mountpoint(
                logger, &snapshot,
            )?;

        Ok(Self { log: logger.clone(), snapshot, snapshot_mountpoint })
    }

    /// Return the full path to the snapshot directory within the filesystem.
    fn snapshot_mountpoint(&self) -> &Utf8PathBuf {
        // We are returning this cached value since a single
        // `DiagnosticsSnapshot` may be used to lookup multiple log files
        // residing within the same snapshot. There is certainly a TOCTOU issue
        // here that also exists when querying the non cached data as well,
        // however we don't expect the crypt/debug dataset or a zone's dataset
        // will get moved around at runtime. We are more likely to to encounter
        // a removed filesystem.
        //
        // TODO: figure out if a removed zone or disk causes us issues. The
        // drop method on the snapshot should just log an error and any log
        // collection will likely result in an IO error.
        &self.snapshot_mountpoint
    }

    // We are opting to use "/etc/mnttab" here rather than calling
    // `Snapshot::full_path` because there are some rough edges we need to work
    // around:
    // - When asking ZFS for the mountpoint on a "zoned" filesystem (aka
    //   delegated dataset) it will return the mountpoint relative to the zone's
    //   root file system.
    // - When asking ZFS for the mountpoint of the root filesystem it will
    //   return "legacy".
    // - An unmounted filesystem will return "none" as the mountpoint.
    fn determine_snapshot_mountpoint(
        logger: &Logger,
        snapshot: &Snapshot,
    ) -> Result<Utf8PathBuf, LogError> {
        let mnttab = BufReader::new(File::open("/etc/mnttab")?);
        let mut mountpoint = None;
        for line in mnttab.lines() {
            let line = line?;
            // "/etc/mnttab" is a read-only file maintained by the kernel that
            // tracks mounted filesystems on the current host. We could use
            // getmntent(3C) but libc currently doesn't have bindings for this,
            // so for now we are just going to parse the entries manually.
            //
            // The file is separated by TABs in the form of:
            // special   mount_point   fstype   options   time
            let mut split_line = line.split('\t');
            if let Some(special) = split_line.next() {
                if special == snapshot.filesystem {
                    if let Some(mp) = split_line.next() {
                        mountpoint = Some(mp.to_string());
                        trace!(
                            logger,
                            "found mountpoint {mp} for dataset {}",
                            snapshot.filesystem
                        )
                    }
                }
            }
        }

        mountpoint
            .ok_or(LogError::MissingMountpoint(snapshot.filesystem.clone()))
            .map(|mp| {
                Utf8PathBuf::from(mp)
                    .join(format!(".zfs/snapshot/{}", snapshot.snap_name))
            })
    }
}

impl Drop for DiagnosticsSnapshot {
    fn drop(&mut self) {
        let _ = Zfs::destroy_snapshot(
            &self.snapshot.filesystem,
            &self.snapshot.snap_name,
        )
        .inspect(|_| {
            debug!(
                self.log,
                "destroyed sled-diagnostics ZFS snapshot";
                "snapshot" => %self.snapshot
            );
        })
        .inspect_err(|e| {
            error!(
                self.log,
                "failed to destroy sled-diagnostics ZFS snapshot";
                "snapshot" => %self.snapshot,
                "error" => ?e,
            );
        });
    }
}

/// A utility type that keeps track fo `DiagnosticsSnapshot`s keyed off of a
/// ZFS dataset name.
struct LogSnapshots {
    inner: HashMap<String, DiagnosticsSnapshot>,
}

impl LogSnapshots {
    fn new() -> Self {
        Self { inner: HashMap::new() }
    }

    /// For a given log file return the corresponding `DiagnosticsSnapshot` or
    /// create a new one if we have not yet created one for the underlying ZFS
    /// dataset backing this particular file.
    fn get_or_create(
        &mut self,
        logger: &Logger,
        logfile: &Utf8Path,
    ) -> Result<&DiagnosticsSnapshot, LogError> {
        let dataset = Zfs::get_dataset_name(logfile.as_str())?;
        let snapshot = match self.inner.entry(dataset.clone()) {
            Entry::Occupied(occupied_entry) => occupied_entry.into_mut(),
            Entry::Vacant(vacant_entry) => vacant_entry
                .insert(DiagnosticsSnapshot::create(logger, dataset.as_str())?),
        };

        Ok(snapshot)
    }
}

/// The type of log file we are operating on.
#[derive(PartialEq, Debug, Clone)]
enum LogType {
    /// Logs that are within the a zone's dataset.
    /// e.g.  `/pool/ext/<UUID>/crypt/zone/<ZONE_NAME>/root/var/log/svc/<LOGFILE>`
    Current,
    /// Logs that have been archived by sled-agent into a debug dataset.
    /// e.g.  `/pool/ext/<UUID>/crypt/debug/<ZONE_NAME>/<LOGFILE>`
    Archive,
    /// Logs that are within a delegated dataset.
    /// e.g.
    ///     dataset: `oxp_<UUID>/crypt/cockroachdb`
    ///     path: `/pool/ext/<UUID>/crypt/zone/<ZONE_NAME>/root/data/`
    Extra,
}

impl std::fmt::Display for LogType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogType::Current => write!(f, "current"),
            LogType::Archive => write!(f, "archive"),
            LogType::Extra => write!(f, "extra"),
        }
    }
}

/// A type managing sled diagnostics log collection snapshots and cleanup.
#[derive(Clone)]
pub struct LogsHandle {
    log: Logger,
}

impl LogsHandle {
    pub fn new(logger: Logger) -> Self {
        Self { log: logger }
    }

    /// Cleanup snapshots that may have been left around due to unknown
    /// circumstances such as a crash.
    pub fn cleanup_snapshots(&self) {
        let diagnostic_snapshots = Zfs::list_snapshots().unwrap().into_iter().filter (|snap| {
           if !snap.snap_name.starts_with(SLED_DIAGNOSTICS_SNAPSHOT_PREFIX) {
               return false
           }

            // Additionally check for the sled-diagnostics property.
            //
            // If we find a dataset that matches our names, but which _does not_
            // have such a property (or has in invalid property), we'll log it
            // but avoid deleting the snapshot.
            let name = snap.to_string();
            let value = match Zfs::get_values(
                &name,
                &[SLED_DIAGNOSTICS_ZFS_PROPERTY_NAME],
                Some(illumos_utils::zfs::PropertySource::Local),
                ) {
                    Ok([value]) => value,
                    Err(e) => {
                        error!(
                            self.log,
                            "Found a ZFS snapshot with a name reserved for
                            sled diagnostics, but which does not have the \
                            sled-diagnostics-specific property. Bailing out, \
                            rather than risking deletion of user data: {e}";
                            "snap_name" => &name,
                            "property" => SLED_DIAGNOSTICS_ZFS_PROPERTY_VALUE
                        );
                        return false;
                    },
                };

            if value != SLED_DIAGNOSTICS_ZFS_PROPERTY_VALUE {
                warn!(
                    self.log,
                    "Found a ZFS snapshot with a name reserved for sled \
                    diagnostics, with an unexpected property value. \
                    Bailing out, rather than risking deletion of user data.";
                    "snap_name" => &name,
                    "property" => SLED_DIAGNOSTICS_ZFS_PROPERTY_NAME,
                    "property_value" => value,
                );
                return false;
            }
            true
        });

        for snapshot in diagnostic_snapshots {
            Zfs::destroy_snapshot(&snapshot.filesystem, &snapshot.snap_name)
                .unwrap();
            debug!(
                self.log,
                "destroyed pre-existing sled-diagnostics snapshot";
                "snapshot" => %snapshot,
            );
        }
    }

    /// Get all of the zones on a sled containing logs.
    pub fn get_zones() -> Result<Vec<String>, LogError> {
        oxlog::Zones::load()
            .map(|z| z.zones.into_keys().collect())
            .map_err(|e| LogError::OxLog(e))
    }

    fn find_log_in_snapshot(
        &self,
        zone: &str,
        log_snapshots: &mut LogSnapshots,
        logfile: &Utf8Path,
    ) -> Result<Utf8PathBuf, LogError> {
        let diagnostics_snapshot =
            log_snapshots.get_or_create(&self.log, logfile)?;

        trace!(
            &self.log,
            "using diagnostics snapshot {} for logfile {}",
            diagnostics_snapshot.snapshot,
            logfile;
        );

        let filepath = match zone {
            "global" => logfile.as_str(),
            _ => {
                logfile
                    .as_str()
                    .split_once(zone)
                    .ok_or(LogError::MissingZonePathComponent {
                        zone: zone.to_string(),
                        logfile: logfile.to_path_buf(),
                    })?
                    .1
            }
        };

        let snapshot_logfile = diagnostics_snapshot
            .snapshot_mountpoint()
            // append the path to the log file itself
            .join(
                filepath
                    .trim_start_matches("/")
                    // Extra logs are often on delegated datasets so we need
                    // to be sure we remove that as well.
                    //
                    // TODO: it would be nice to figure this out at runtime
                    // rather than assume delegated datasets are always at
                    // "/data".
                    .trim_start_matches("root/data/"),
            );
        Ok(snapshot_logfile)
    }

    /// For a given log file:
    /// - Create a snapshot of the underlying dataset if one doesn't yet exist.
    /// - Determine the logs path within the snapshot.
    ///   - In the case of "current" logs, also find all of its rotated
    ///     variants.
    /// - Write the logs contents into the provided zip file based on its zone,
    ///   and service.
    fn process_logs<W: Write + Seek>(
        &self,
        zone: &str,
        service: &str,
        zip: &mut zip::ZipWriter<W>,
        log_snapshots: &mut LogSnapshots,
        logfile: &Utf8Path,
        logtype: LogType,
    ) -> Result<(), LogError> {
        let snapshot_logfile =
            self.find_log_in_snapshot(zone, log_snapshots, logfile)?;

        if logtype == LogType::Current {
            // Since we are processing the current log files in a zone we need
            // to examine the parent directory and find all log files that match
            // the service. e.g. service.log, service.log.1, ...service.log.n
            if let Some(parent) = snapshot_logfile.parent() {
                if let Some(filename) = snapshot_logfile.file_name() {
                    let (files, errors): (Vec<_>, Vec<_>) =
                        parent.read_dir_utf8()?.partition(Result::is_ok);

                    for err in errors.into_iter().map(Result::unwrap_err) {
                        error!(
                            self.log,
                            "Failed to read dir ent while processing \
                            sled-diagnostics current log files";
                            "error" => %err
                        );
                    }

                    // A filter that ensures our logfile matches the correct
                    // pattern where `filename` is the type of log we are
                    // looking for such as `oxide-mg-ddm:default.log`.
                    //
                    // Valid variants are:
                    // - `oxide-mg-ddm:default.log`
                    // - `oxide-mg-ddm:default.log.n`
                    let is_log_file = |path: &Utf8Path, filename: &str| {
                        path.file_name()
                            // Make sure the path starts with our filename or
                            // is an exact match.
                            .filter(|fname| fname.starts_with(filename))
                            .and_then(|fname| Utf8Path::new(fname).extension())
                            // If we found a match make sure that the file ends
                            // in ".log" or a number from log rotation.
                            .map_or(false, |ext| {
                                ext == "log" || ext.parse::<u64>().is_ok()
                            })
                    };

                    for f in files
                        .into_iter()
                        .map(Result::unwrap)
                        .filter(|f| is_log_file(f.path(), filename))
                    {
                        let logfile = f.path();
                        if logfile.is_file() {
                            write_log_to_zip(
                                &self.log,
                                service,
                                zip,
                                LogType::Current,
                                logfile,
                            )?;
                        }
                    }
                }
            }
        } else {
            match snapshot_logfile.is_file() {
                true => {
                    write_log_to_zip(
                        &self.log,
                        service,
                        zip,
                        logtype,
                        &snapshot_logfile,
                    )?;
                }
                false => {
                    error!(
                        self.log,
                        "found log file that is not a file, skipping over it \
                        but this is likely a programming error";
                        "logfile" => %snapshot_logfile,
                    );
                }
            }
        }

        Ok(())
    }

    /// For a given zone find all of its logs for all of its services and write
    /// them to a zip file. Additionally include up to `max_rotated` logs in
    /// the zip file.
    ///
    /// Note that this log retrieval will automatically take and cleanup
    /// necessary zfs snapshots along the way.
    pub fn get_zone_logs<W: Write + Seek>(
        &self,
        zone: &str,
        max_rotated: usize,
        writer: &mut W,
    ) -> Result<(), LogError> {
        // We are opting to use oxlog to find logs rather than using a similar
        // pattern to zone bundles because the sled-diagnostics crate lives
        // outside of sled-agent itself. This means we don't have access to some
        // internal structures like a list of running zones. Instead we operate
        // on all of the log paths that oxlog is capable of discovering via the
        // filesystem directly.
        let zones = oxlog::Zones::load().map_err(|e| LogError::OxLog(e))?;
        let zone_logs = zones.zone_logs(
            zone,
            oxlog::Filter {
                current: true,
                archived: true,
                extra: true,
                // This avoids calling stat on every log file found.
                show_empty: true,
                date_range: None,
            },
        );

        let mut zip = zip::ZipWriter::new(writer);

        // Hold onto log snapshots so that they can be cleaned up on drop.
        let mut log_snapshots = LogSnapshots::new();

        for (service, service_logs) in zone_logs {
            //  - Grab all of the service's SMF logs -
            if let Some(current) = service_logs.current {
                self.process_logs(
                    zone,
                    &service,
                    &mut zip,
                    &mut log_snapshots,
                    &current.path,
                    LogType::Current,
                )?;
            }

            //  - Grab all of the service's archived logs -

            // Oxlog will consider rotated smf logs from `/<ZONE>/var/svc/log/`
            // as "archived", but we are gathering those up as a part of
            // "current" log processing. We only care about logs that have made
            // it explicitly to the debug dataset.
            let mut archived: Vec<_> = service_logs
                .archived
                .into_iter()
                .filter(|log| log.path.as_str().contains("crypt/debug"))
                .map(|log| log.path)
                .collect();

            // Since these logs can be spread out across multiple U.2 devices
            // we need to sort them by timestamp.
            archived.sort_by_key(|log| {
                log.as_str()
                    .rsplit_once(".")
                    .and_then(|(_, date)| date.parse::<u64>().ok())
                    .unwrap_or(0)
            });

            for file in archived.iter().rev().take(max_rotated) {
                self.process_logs(
                    zone,
                    &service,
                    &mut zip,
                    &mut log_snapshots,
                    &file,
                    LogType::Archive,
                )?;
            }

            //  - Grab all of the service's extra logs -

            // Currently we are only grabbing cockroachdb logs. If other
            // services contain valuable logs we should add them here.
            if service == "cockroachdb" {
                let cockroach_extra_logs =
                    sort_cockroach_extra_logs(&service_logs.extra);
                for (_prefix, extra_logs) in cockroach_extra_logs {
                    // We always want the most current log being written to.
                    if let Some(log) = extra_logs.current {
                        self.process_logs(
                            zone,
                            &service,
                            &mut zip,
                            &mut log_snapshots,
                            log,
                            LogType::Extra,
                        )?;
                    }

                    // We clamp the number of rotated logs we grab to 5.
                    for log in extra_logs.rotated.iter().rev().take(max_rotated)
                    {
                        self.process_logs(
                            zone,
                            &service,
                            &mut zip,
                            &mut log_snapshots,
                            log,
                            LogType::Extra,
                        )?;
                    }
                }
            }
        }

        zip.finish()?;

        Ok(())
    }
}

fn write_log_to_zip<W: Write + Seek>(
    logger: &Logger,
    service: &str,
    zip: &mut zip::ZipWriter<W>,
    logtype: LogType,
    snapshot_logfile: &Utf8Path,
) -> Result<(), LogError> {
    let Some(log_name) = snapshot_logfile.file_name() else {
        debug!(
            logger,
            "sled-diagnostics unable to determine filename for logfile";
            "logfile" => %snapshot_logfile,
        );

        return Ok(());
    };

    let mut src = File::open(&snapshot_logfile)?;
    let zip_path = format!("{service}/{logtype}/{log_name}");
    zip.start_file_from_path(
        zip_path,
        FullFileOptions::default()
            .compression_method(zip::CompressionMethod::Zstd)
            .compression_level(Some(3)),
    )?;
    if let Err(e) = std::io::copy(&mut src, zip) {
        // If we fail here the `ZipWriter` is an unknown state and we are forced
        // to bubble up an error.
        zip.abort_file()?;

        // We are only going to log this error and continue on in hopes that we
        // are able to grab as many logs as possible for debugging purposes.
        error!(
            logger,
            "Failed to write service log to zip file: {e}";
            "service" => %service,
            "log" => %snapshot_logfile,
        );
    };

    Ok(())
}

#[derive(Debug, Default, PartialEq)]
struct CockroachExtraLog<'a> {
    current: Option<&'a Utf8Path>,
    rotated: Vec<&'a Utf8Path>,
}
fn sort_cockroach_extra_logs(
    logs: &[LogFile],
) -> HashMap<&str, CockroachExtraLog<'_>> {
    // Known logging paths for cockroachdb:
    // https://www.cockroachlabs.com/docs/stable/logging-overview#logging-destinations
    let cockroach_log_prefix = HashSet::from([
        "cockroach",
        "cockroach-health",
        "cockroach-kv-distribution",
        "cockroach-security",
        "cockroach-sql-audit",
        "cockroach-sql-auth",
        "cockroach-sql-exec",
        "cockroach-sql-slow",
        "cockroach-sql-schema",
        "cockroach-pebble",
        "cockroach-telemetry",
        // Not documented but found on our sleds
        "cockroach-stderr",
    ]);

    let mut interested: HashMap<&str, CockroachExtraLog<'_>> = HashMap::new();
    for log in logs {
        let Some(file_name) = log.path.file_name() else {
            continue;
        };

        // We grab the first part of a log file which is prefixed with the log
        // type so that we gauge our interest.
        // e.g. cockroach.oxzcockroachdb<ZONENAME>.root.2025-04-06T20_30_29Z.010615.log
        let Some((prefix, _)) = file_name.split_once(".") else {
            continue;
        };

        // Log files come in the form of:
        // - cockroach-health.log
        // - cockroach.oxzcockroachdbcf286aa9-fb2d-4285-a3cb-48eee3c1ebeb.root.2025-04-09T23_20_13Z.010615.log
        // We put these in two separate buckets as the first variant is the
        // current active log, while the latter is a log that has been rotated.
        if cockroach_log_prefix.contains(prefix) {
            let entry = interested.entry(prefix).or_default();

            if file_name == format!("{prefix}.log") {
                entry.current = Some(log.path.as_path());
            } else {
                entry.rotated.push(log.path.as_path());
            }
        }
    }

    interested
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use camino::Utf8PathBuf;

    use super::*;

    #[test]
    fn test_sort_cockroach_extra_logs() {
        let logs: Vec<_> = [
            "cockroach-health.log",
            "cockroach-health.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2025-01-31T21_43_26Z.011435.log",
            "cockroach-health.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2025-02-01T01_51_53Z.011486.log",
            "cockroach-stderr.log",
            "cockroach-stderr.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2023-08-30T18_56_19Z.011950.log",
            "cockroach-stderr.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2023-08-31T02_59_24Z.010479.log",
            "cockroach.log",
            "cockroach.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2025-01-31T17_11_45Z.011435.log",
            "cockroach.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2025-02-01T01_51_51Z.011486.log",
            "bogus.log",
            "some/dir"
        ].into_iter().map(|l| {
                oxlog::LogFile { path: Utf8PathBuf::from(l), size: None, modified: None }
            }).collect();

        let mut expected: HashMap<&str, CockroachExtraLog> = HashMap::new();

        // cockroach
        expected.entry("cockroach").or_default().current =
            Some(Utf8Path::new("cockroach.log"));
        expected
            .entry("cockroach")
            .or_default()
            .rotated
            .push(Utf8Path::new("cockroach.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2025-01-31T17_11_45Z.011435.log"));
        expected
            .entry("cockroach")
            .or_default()
            .rotated
            .push(Utf8Path::new("cockroach.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2025-02-01T01_51_51Z.011486.log"));

        // cockroach-health
        expected.entry("cockroach-health").or_default().current =
            Some(Utf8Path::new("cockroach-health.log"));
        expected
            .entry("cockroach-health")
            .or_default()
            .rotated
            .push(Utf8Path::new("cockroach-health.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2025-01-31T21_43_26Z.011435.log"));
        expected
            .entry("cockroach-health")
            .or_default()
            .rotated
            .push(Utf8Path::new("cockroach-health.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2025-02-01T01_51_53Z.011486.log"));

        // cockroach-stderr
        expected.entry("cockroach-stderr").or_default().current =
            Some(Utf8Path::new("cockroach-stderr.log"));
        expected
            .entry("cockroach-stderr")
            .or_default()
            .rotated
            .push(Utf8Path::new("cockroach-stderr.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2023-08-30T18_56_19Z.011950.log"));
        expected
            .entry("cockroach-stderr")
            .or_default()
            .rotated
            .push(Utf8Path::new("cockroach-stderr.oxzcockroachdba3628a56-6f85-43b5-be50-71d8f0e04877.root.2023-08-31T02_59_24Z.010479.log"));

        let extra = sort_cockroach_extra_logs(logs.as_slice());
        assert_eq!(
            extra, expected,
            "cockroachdb extra logs are properly sorted"
        );
    }
}

#[cfg(all(target_os = "illumos", test))]
mod illumos_tests {
    use std::collections::BTreeMap;
    use std::io::Read;

    use super::*;

    use illumos_utils::zfs::ZFS;
    use omicron_common::disk::DatasetConfig;
    use omicron_common::disk::DatasetKind;
    use omicron_common::disk::DatasetName;
    use omicron_common::disk::DatasetsConfig;
    use omicron_common::disk::SharedDatasetConfig;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::DatasetUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use sled_storage::manager_test_harness::StorageManagerTestHarness;
    use tokio::io::AsyncWriteExt;
    use zip::ZipArchive;
    use zip::ZipWriter;

    struct SingleU2StorageHarness {
        storage_test_harness: StorageManagerTestHarness,
        zpool_id: ZpoolUuid,
    }

    impl SingleU2StorageHarness {
        async fn new(log: &Logger) -> Self {
            let mut harness = StorageManagerTestHarness::new(log).await;
            harness.handle().key_manager_ready().await;
            let _raw_internal_disks =
                harness.add_vdevs(&["m2_left.vdev", "m2_right.vdev"]).await;

            let raw_disks = harness.add_vdevs(&["u2_0.vdev"]).await;

            let config = harness.make_config(1, &raw_disks);
            let result = harness
                .handle()
                .omicron_physical_disks_ensure(config.clone())
                .await
                .expect("Failed to ensure disks");
            assert!(!result.has_error(), "{result:?}");

            let zpool_id = config.disks[0].pool_id;
            Self { storage_test_harness: harness, zpool_id }
        }

        async fn configure_dataset(&self, kind: DatasetKind) -> Utf8PathBuf {
            let zpool_name = ZpoolName::new_external(self.zpool_id);
            let dataset_id = DatasetUuid::new_v4();
            let name = DatasetName::new(zpool_name.clone(), kind);
            let mountpoint = name.mountpoint(
                &self.storage_test_harness.handle().mount_config().root,
            );
            let datasets = BTreeMap::from([(
                dataset_id,
                DatasetConfig {
                    id: dataset_id,
                    name: name.clone(),
                    inner: SharedDatasetConfig::default(),
                },
            )]);
            let config = DatasetsConfig { datasets, ..Default::default() };
            let status = self
                .storage_test_harness
                .handle()
                .datasets_ensure(config.clone())
                .await
                .unwrap();
            assert!(!status.has_error(), "{status:?}");

            mountpoint
        }

        async fn cleanup(mut self) {
            self.storage_test_harness.cleanup().await
        }
    }

    // A custom zfs snapshot list that only shows us our view of the world for
    // a particular filesystem to prevent races from other concurrent tests.
    fn list_snapshots(filesystem: &str) -> Vec<Snapshot> {
        let mut command = std::process::Command::new(ZFS);
        let cmd = command.args(&[
            "list", "-H", "-o", "name", "-t", "snapshot", "-r", filesystem,
        ]);
        let output = cmd.output().unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        stdout
            .trim()
            .lines()
            .map(|line| {
                let (filesystem, snap_name) = line.split_once('@').unwrap();
                Snapshot {
                    filesystem: filesystem.to_string(),
                    snap_name: snap_name.to_string(),
                }
            })
            .collect()
    }

    /// Find all sled-diagnostics created snapshots
    fn get_sled_diagnostics_snapshots(filesystem: &str) -> Vec<Snapshot> {
        list_snapshots(filesystem)
            .into_iter()
            .filter(|snap| {
                if !snap.snap_name.starts_with(SLED_DIAGNOSTICS_SNAPSHOT_PREFIX)
                {
                    return false;
                }
                let name = snap.to_string();
                if Zfs::get_values(
                    &name,
                    &[SLED_DIAGNOSTICS_ZFS_PROPERTY_NAME],
                    Some(illumos_utils::zfs::PropertySource::Local),
                )
                .unwrap()
                    == [SLED_DIAGNOSTICS_ZFS_PROPERTY_VALUE]
                {
                    return true;
                };

                false
            })
            .collect()
    }

    #[tokio::test]
    async fn log_snapshots_work() {
        let logctx = test_setup_log("log_snapshots_work");
        let log = &logctx.log;

        // Set up storage
        let harness = SingleU2StorageHarness::new(log).await;

        // Create a new zone dataset
        let mountpoint = harness
            .configure_dataset(DatasetKind::TransientZone {
                name: "oxz_switch".to_string(),
            })
            .await;
        let zfs_filesystem =
            &ZpoolName::new_external(harness.zpool_id).to_string();

        // Make sure an error in this block results in the correct drop ordering
        // for test cleanup
        {
            let mut log_snapshots = LogSnapshots::new();

            // Create a new snapshot
            log_snapshots.get_or_create(&log, &mountpoint).unwrap();
            let snapshots = get_sled_diagnostics_snapshots(zfs_filesystem);
            assert_eq!(snapshots.len(), 1, "single snapshot created");

            // Creating a second snapshot from the same dataset doesn't create a
            // new snapshot
            log_snapshots.get_or_create(&log, &mountpoint).unwrap();
            let snapshots = get_sled_diagnostics_snapshots(zfs_filesystem);
            assert_eq!(snapshots.len(), 1, "duplicate snapshots not taken");

            // Free all of the log_snapshots
            drop(log_snapshots);

            let snapshots = get_sled_diagnostics_snapshots(zfs_filesystem);
            assert!(snapshots.is_empty(), "no snapshots left behind");

            // Simulate a crash leaving behind stale snapshots
            let mut log_snapshots = LogSnapshots::new();
            log_snapshots.get_or_create(&log, &mountpoint).unwrap();

            // Don't run the drop handler for any log_snapshots
            std::mem::forget(log_snapshots);

            let snapshots = get_sled_diagnostics_snapshots(zfs_filesystem);
            assert_eq!(snapshots.len(), 1, "single snapshot created");

            let handle = LogsHandle::new(log.clone());
            handle.cleanup_snapshots();

            let snapshots = get_sled_diagnostics_snapshots(zfs_filesystem);
            assert!(snapshots.is_empty(), "all stale snapshots cleaned up");
        }

        // Cleanup
        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn collect_current_logs() {
        let logctx = test_setup_log("collect_current_logs");
        let log = &logctx.log;

        // Set up storage
        let harness = SingleU2StorageHarness::new(log).await;

        // Create a new zone dataset
        let mountpoint = harness
            .configure_dataset(DatasetKind::TransientZone {
                name: "oxz_switch".to_string(),
            })
            .await;

        let logfile_to_data = [
            ("oxide-mg-ddm:default.log", "very important log data"),
            ("oxide-mg-ddm:default.log.0", "life before death"),
            ("oxide-mg-ddm:default.log.1", "strength before weakness"),
            ("oxide-mg-ddm:default.log.2", "journey before destination"),
        ];

        let logfile_to_data_unwanted = [
            ("oxide-mg-ddm:default.log.foo", "some other file"),
            ("oxide-mg-ddm:otther.log.0", "some other file rotated"),
        ];

        let logdir = mountpoint.join("var/svc/log");
        fs_err::tokio::create_dir_all(&logdir).await.unwrap();

        // Populate some sample logs
        for (name, data) in logfile_to_data {
            let logfile = logdir.join(name);
            let mut logfile_handle =
                fs_err::tokio::File::create_new(&logfile).await.unwrap();
            logfile_handle.write_all(data.as_bytes()).await.unwrap();
        }

        // Populate some file with similar names that should be skipped over
        // upon collection
        for (name, data) in logfile_to_data_unwanted {
            let logfile = logdir.join(name);
            let mut logfile_handle =
                fs_err::tokio::File::create_new(&logfile).await.unwrap();
            logfile_handle.write_all(data.as_bytes()).await.unwrap();
        }

        // Make sure an error in this block results in the correct drop ordering
        // for test cleanup
        {
            let loghandle = LogsHandle::new(log.clone());
            let mut log_snapshots = LogSnapshots::new();

            let zipfile_path = mountpoint.join("test.zip");
            let zipfile = File::create_new(&zipfile_path).unwrap();
            let mut zip = ZipWriter::new(zipfile);

            loghandle
                .process_logs(
                    "oxz_switch",
                    "mg-ddm",
                    &mut zip,
                    &mut log_snapshots,
                    &mountpoint
                        .join(format!("var/svc/log/{}", logfile_to_data[0].0)),
                    LogType::Current,
                )
                .unwrap();

            zip.finish().unwrap();

            // Confirm the zip has our file and data
            let mut archive =
                ZipArchive::new(File::open(zipfile_path).unwrap()).unwrap();
            for (name, data) in logfile_to_data {
                let mut file_in_zip =
                    archive.by_name(&format!("mg-ddm/current/{name}")).unwrap();
                let mut contents = String::new();
                file_in_zip.read_to_string(&mut contents).unwrap();

                assert_eq!(contents.as_str(), data, "log file data matches");
            }

            // Confirm the zip did not pick up the unwanted files
            for (name, _) in logfile_to_data_unwanted {
                let file_in_zip =
                    archive.by_name(&format!("mg-ddm/current/{name}"));
                assert!(file_in_zip.is_err(), "file should not be in zip");
            }
        }

        // Cleanup
        harness.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn log_collection_comes_from_snapshot() {
        let logctx = test_setup_log("log_collection_comes_from_snapshot");
        let log = &logctx.log;

        // Set up storage
        let harness = SingleU2StorageHarness::new(log).await;

        // Create a new zone dataset
        let mountpoint = harness
            .configure_dataset(DatasetKind::TransientZone {
                name: "oxz_switch".to_string(),
            })
            .await;

        let mgddm_log = "oxide-mg-ddm:default.log";
        let data1 = "very important log data";
        let data2 = "changed log data";

        let logdir = mountpoint.join("var/svc/log");
        fs_err::tokio::create_dir_all(&logdir).await.unwrap();

        // Make sure an error in this block results in the correct drop ordering
        // for test cleanup
        {
            // Write the log data before we take a snapshot
            let logfile = logdir.join(mgddm_log);
            let mut logfile_handle =
                fs_err::tokio::File::create_new(&logfile).await.unwrap();
            logfile_handle.write_all(data1.as_bytes()).await.unwrap();

            let loghandle = LogsHandle::new(log.clone());
            let mut log_snapshots = LogSnapshots::new();

            // Create a snapshot first
            log_snapshots.get_or_create(&log, &logfile).unwrap();

            // Change the data on disk by truncating the old file first
            let mut logfile_handle =
                fs_err::tokio::File::create(&logfile).await.unwrap();
            logfile_handle.write_all(data2.as_bytes()).await.unwrap();

            let zipfile_path = mountpoint.join("test.zip");
            let zipfile = File::create_new(&zipfile_path).unwrap();
            let mut zip = ZipWriter::new(zipfile);

            loghandle
                .process_logs(
                    "oxz_switch",
                    "mg-ddm",
                    &mut zip,
                    &mut log_snapshots,
                    &logfile,
                    LogType::Current,
                )
                .unwrap();

            zip.finish().unwrap();

            let mut archive =
                ZipArchive::new(File::open(zipfile_path).unwrap()).unwrap();
            let mut file_in_zip = archive
                .by_name(&format!("mg-ddm/current/{mgddm_log}"))
                .unwrap();
            let mut contents = String::new();
            file_in_zip.read_to_string(&mut contents).unwrap();

            // Confirm we have the data in the snapshot and not the newly
            // written data.
            assert_eq!(contents.as_str(), data1, "log file data matches");
        }

        // Cleanup
        harness.cleanup().await;
        logctx.cleanup_successful();
    }
}
