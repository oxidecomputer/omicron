// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration and implementation for archiving ordinary files as debug data
//! (e.g., log files)

use anyhow::anyhow;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use slog::Logger;
use thiserror::Error;

pub struct Archiver {
    log: Logger,
    what: ArchiveWhat,
    glob_groups: Vec<GlobGroup>,
}

impl Archiver {
    pub fn new(log: Logger, what: ArchiveWhat) -> Archiver {
        Archiver { log, what, glob_groups: Vec::new() }
    }

    pub fn include_zone(&mut self, zone_name: &str, zone_root: &Utf8Path) {
        let zone_name = zone_name.to_owned();
        let smf_log_dir = format!("{}/var/svc/log", zone_root);
        self.glob_groups.push(GlobGroup {
            label: format!("zone {zone_name}: rotated SMF logs"),
            // XXX-dap digits
            glob_pattern: format!("{smf_log_dir}/*.log.*"),
            kind: DebugFileKind::LogRotated(zone_name.clone()),
        });

        let syslog_dir = format!("{}/var/adm", zone_root);
        self.glob_groups.push(GlobGroup {
            label: format!("zone {zone_name}: rotated syslog"),
            // XXX-dap digits
            glob_pattern: format!("{syslog_dir}/messages.*"),
            kind: DebugFileKind::LogRotated(zone_name.clone()),
        });

        match self.what {
            ArchiveWhat::ImmutableOnly => (),
            ArchiveWhat::Everything => {
                self.glob_groups.push(GlobGroup {
                    label: format!("zone {zone_name}: live SMF logs"),
                    glob_pattern: format!("{smf_log_dir}/*.log"),
                    kind: DebugFileKind::LogLive(zone_name.clone()),
                });
                self.glob_groups.push(GlobGroup {
                    label: format!("zone {zone_name}: live syslog"),
                    glob_pattern: format!("{syslog_dir}/messages"),
                    kind: DebugFileKind::LogLive(zone_name.clone()),
                });
            }
        }
    }

    pub fn include_cores_directory(&mut self, cores_dir: &Utf8Path) {
        self.glob_groups.push(GlobGroup {
            label: format!("core files in {cores_dir}"),
            glob_pattern: format!("{cores_dir}/*"),
            kind: DebugFileKind::Core,
        });
    }

    pub fn to_plan(&self) -> ArchivePlan<'_> {
        ArchivePlan::new(&self.log, &self.glob_groups)
    }
}

/// Describes what to archive in this path
#[derive(Debug, Clone, Copy)]
pub enum ArchiveWhat {
    /// Archive only immutable files
    ///
    /// This includes core files and rotated log files, but ignores live log
    /// files, since they are still being written-to.
    ImmutableOnly,

    /// Archive everything, including live log files that may still be written
    /// to
    Everything,
}

/// Basic unit of archival: a group of files of kind `kind` identified by
/// `glob_pattern`
#[derive(Debug, Clone)]
struct GlobGroup {
    label: String,
    glob_pattern: String,
    kind: DebugFileKind,
}

/// Describes what type of debug file this is
///
/// This is used to determine where in the debug dataset the file will be
/// archived and whether the original should be deleted.
#[derive(Debug, Clone)]
enum DebugFileKind {
    /// a rotated (now-immutable) log file in the given zone
    LogRotated(String),
    /// a live log file in the given zone
    LogLive(String),
    /// a user process core dump
    Core,
}

impl DebugFileKind {
    /// Returns whether the original file should be deleted when the file is
    /// archived
    ///
    /// Live log files are never deleted.  Rotated log files and core dumps are.
    fn should_delete_original(&self) -> bool {
        match self {
            DebugFileKind::LogRotated(_) | DebugFileKind::Core => true,
            DebugFileKind::LogLive(_) => false,
        }
    }
}

pub struct ArchiveStep<'a> {
    source: Utf8PathBuf,
    kind: &'a DebugFileKind,
}

impl ArchiveStep<'_> {
    fn destination(&self, debug_dir: &Utf8Path) -> Utf8PathBuf {
        match self.kind {
            DebugFileKind::LogRotated(zone_info) => todo!(),
            DebugFileKind::LogLive(zone_info) => todo!(),
            DebugFileKind::Core => todo!(),
        }
    }
}

pub struct ArchivePlan<'a> {
    log: &'a Logger,
    glob_groups: &'a [GlobGroup],
    steps: Box<dyn Iterator<Item = ArchiveStep<'a>> + Send + Sync + 'a>,
}

impl<'a> ArchivePlan<'a> {
    pub fn new(
        log: &'a Logger,
        glob_groups: &'a [GlobGroup],
    ) -> ArchivePlan<'a> {
        // XXX-dap this really ought to work in a streaming way
        let mut rv = Vec::new();

        for glob_group in glob_groups {
            // XXX-dap warn instead of ignoring these errors with flatten
            let files = match glob::glob(&glob_group.glob_pattern) {
                Ok(files) => files.flatten(),
                Err(error) => {
                    // XXX-dap log error -- this should never happen
                    continue;
                }
            };
            for file in files {
                // XXX-dap unwrap()
                let path = Utf8PathBuf::try_from(file).unwrap();
                rv.push(ArchiveStep { source: path, kind: &glob_group.kind });
            }
        }

        let steps = Box::new(rv.into_iter());
        ArchivePlan { log, glob_groups, steps }
    }

    #[cfg(test)]
    fn into_steps(self) -> Box<dyn Iterator<Item = ArchiveStep<'a>> + 'a> {
        self.steps
    }

    pub async fn execute(
        self,
        debug_dir: &Utf8Path,
    ) -> Result<(), anyhow::Error> {
        let log = self.log;
        let steps = self.steps;
        // XXX-dap logging
        let mut rv = Ok(());
        for step in steps {
            match archive_one(debug_dir, step).await {
                Ok(()) => (),
                Err(error) => {
                    // XXX-dap warning
                    rv = Err(anyhow!("failed to archive some files"));
                }
            }
        }

        rv
    }
}

// XXX-dap copied from copy_sync_and_remove()
pub async fn archive_one(
    debug_dir: &Utf8Path,
    step: ArchiveStep<'_>,
) -> tokio::io::Result<()> {
    let dest = step.destination(debug_dir);
    let source = &step.source;
    let mut dest_f = tokio::fs::File::create(&dest).await?;
    let mut src_f = tokio::fs::File::open(&source).await?;

    tokio::io::copy(&mut src_f, &mut dest_f).await?;

    dest_f.sync_all().await?;

    drop(src_f);
    drop(dest_f);

    if step.kind.should_delete_original() {
        tokio::fs::remove_file(source).await?;
    }

    Ok(())
}
