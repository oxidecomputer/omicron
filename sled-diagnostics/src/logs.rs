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
use once_cell::sync::OnceCell;
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

#[derive(Debug)]
struct SnapshotPermit {
    log: Logger,
    snapshot: Snapshot,
    full_path: OnceCell<Utf8PathBuf>,
}

impl SnapshotPermit {
    fn create(logger: &Logger, filesystem: &str) -> Result<Self, LogError> {
        let snap_name = format!(
            "{SLED_DIAGNOSTICS_SNAPSHOT_PREFIX}{}",
            thread_rng()
                .sample_iter(Alphanumeric)
                .take(6)
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

        Ok(Self { log: logger.clone(), snapshot, full_path: OnceCell::new() })
    }

    /// Return the full path to the snapshot directory within the filesystem.
    fn snapshot_full_path(&self) -> Result<&Utf8PathBuf, LogError> {
        // We are caching this value since a single snapshot permit may be used
        // to lookup multiple log files residing within the same snapshot. There
        // is certainly a TOCTOU issue here that also exists when querying the
        // non cached data as well, however we don't expect the crypt/debug
        // dataset or a zone's dataset will get moved around at runtime. We are
        // more likely to to encounter a removed filesystem.
        //
        // TODO: figure out if a removed zone or disk causes us issues. The drop
        // method on the snapshot should just log an error and any log
        // collection will likely result in an IO error.
        self.full_path.get_or_try_init(|| self.snapshot_full_path_impl())
    }

    // We are opting to use "/etc/mnttab" here rather than calling
    // `Snapshot::full_path` because there are some rough edges we need to work
    // around:
    // - When asking ZFS for the mountpoint on a "zoned" filesystem (aka
    // delegated dataset) it will return the mountpoint relative to the zone's
    // root file system.
    // - When asking ZFS for the mountpoint of the root filesystem it will
    // return "legacy".
    // - An unmounted filesystem will return "none" as the mountpoint.
    fn snapshot_full_path_impl(&self) -> Result<Utf8PathBuf, LogError> {
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
                if special == self.snapshot.filesystem {
                    if let Some(mp) = split_line.next() {
                        mountpoint = Some(mp.to_string());
                        trace!(
                            &self.log,
                            "found mountpoint {mp} for dataset {}",
                            self.snapshot.filesystem
                        )
                    }
                }
            }
        }

        mountpoint
            .ok_or(LogError::MissingMountpoint(
                self.snapshot.filesystem.clone(),
            ))
            .map(|mp| {
                Utf8PathBuf::from(mp)
                    .join(format!(".zfs/snapshot/{}", self.snapshot.snap_name))
            })
    }
}

impl Drop for SnapshotPermit {
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

struct Permits {
    inner: HashMap<String, SnapshotPermit>,
}

impl Permits {
    fn new() -> Self {
        Self { inner: HashMap::new() }
    }

    fn get_or_create(
        &mut self,
        logger: &Logger,
        logfile: &Utf8Path,
    ) -> Result<&SnapshotPermit, LogError> {
        let dataset = Zfs::get_dataset_name(logfile.as_str())?;
        let permit = match self.inner.entry(dataset.clone()) {
            Entry::Occupied(occupied_entry) => occupied_entry.into_mut(),
            Entry::Vacant(vacant_entry) => vacant_entry
                .insert(SnapshotPermit::create(logger, dataset.as_str())?),
        };

        Ok(permit)
    }
}

/// The type of log file we are operating on.
#[derive(PartialEq, Debug, Clone)]
enum LogType {
    /// Logs that are within the a zone's dataset.
    /// e.g.  `/pool/ext/<UUID>/crypt/zone/<ZONE_NAME>/root/var/log/svc/<LOGFILE>`
    Current,
    /// Logs that have been archived by sled-agent into a debug dataset.
    /// e.g.  `/pool/ext/<UUID>/crypt/debug/<ZONE_NAME/<LOGFILE>`
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

/// A type managing support bundle log collection snapshots and cleanup.
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

            // Additionally check for the zone-bundle-specific property.
            //
            // If we find a dataset that matches our names, but which _does not_
            // have such a property (or has in invalid property), we'll log it
            // but avoid deleting the snapshot.
            let name = snap.to_string();
            let Ok([value]) = Zfs::get_values(
                &name,
                &[SLED_DIAGNOSTICS_ZFS_PROPERTY_NAME],
                Some(illumos_utils::zfs::PropertySource::Local),
            ) else {
                warn!(
                    self.log,
                    "Found a ZFS snapshot with a name reserved for zone \
                    bundling, but which does not have the zone-bundle-specific \
                    property. Bailing out, rather than risking deletion of \
                    user data.";
                    "snap_name" => &name,
                    "property" => SLED_DIAGNOSTICS_ZFS_PROPERTY_VALUE
                );
                return false;
            };
            if value != SLED_DIAGNOSTICS_ZFS_PROPERTY_VALUE {
                warn!(
                    self.log,
                    "Found a ZFS snapshot with a name reserved for zone \
                    bundling, with an unexpected property value. \
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
                "destroyed pre-existing zone bundle snapshot";
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
        permits: &mut Permits,
        logfile: &Utf8Path,
    ) -> Result<Utf8PathBuf, LogError> {
        let permit = permits.get_or_create(&self.log, logfile)?;

        trace!(
            &self.log,
            "using permit with snapshot {} for logfile {}",
            permit.snapshot,
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

        let snapshot_logfile = permit
            .snapshot_full_path()?
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
        zip: &mut zip::ZipWriter<&mut W>,
        permits: &mut Permits,
        logfile: &Utf8Path,
        logtype: LogType,
    ) -> Result<(), LogError> {
        let snapshot_logfile =
            self.find_log_in_snapshot(zone, permits, logfile)?;

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

                    for f in files
                        .into_iter()
                        .map(Result::unwrap)
                        .filter(|f| f.path().as_str().contains(filename))
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
            if snapshot_logfile.is_file() {
                write_log_to_zip(
                    &self.log,
                    service,
                    zip,
                    logtype,
                    &snapshot_logfile,
                )?;
            }
        }

        Ok(())
    }

    /// For a given zone find all of its logs for all of its services and write
    /// them to a zip file.
    ///
    /// Note that this log retrieval will automatically take and cleanup
    /// necessary zfs snapshots along the way.
    pub fn get_zone_logs<W: Write + Seek>(
        &self,
        zone: &str,
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

        // Hold onto snapshot permits so that they can be cleaned up on drop.
        let mut permits = Permits::new();

        for (service, service_logs) in zone_logs {
            //  - Grab all of the service's SMF logs -
            if let Some(current) = service_logs.current {
                self.process_logs(
                    zone,
                    &service,
                    &mut zip,
                    &mut permits,
                    &current.path,
                    LogType::Current,
                )?;
            }

            //  - Grab all of the service's archived logs -

            // We only care about logs that have made it to the debug dataset.
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

            // 5 is an arbitrary amount of logs to grab, if we find that we are
            // missing important information while debugging we should bump this
            // value.
            for file in archived.iter().rev().take(5) {
                self.process_logs(
                    zone,
                    &service,
                    &mut zip,
                    &mut permits,
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
                            &mut permits,
                            log,
                            LogType::Extra,
                        )?;
                    }

                    // We clamp the number of rotated logs we grab to 5.
                    for log in extra_logs.rotated.iter().rev().take(5) {
                        self.process_logs(
                            zone,
                            &service,
                            &mut zip,
                            &mut permits,
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
    zip: &mut zip::ZipWriter<&mut W>,
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
        // type so that we gague our interest.
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
            let entry = interested
                .entry(prefix)
                .or_insert(CockroachExtraLog::default());

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
