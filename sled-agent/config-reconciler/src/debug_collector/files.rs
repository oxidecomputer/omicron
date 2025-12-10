// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration and implementation for archiving ordinary files as debug data
//! (e.g., log files)

use camino::Utf8Path;
use camino::Utf8PathBuf;
use slog::Logger;
use tokio::fs::File;

#[derive(Debug, Clone, Copy)]
pub struct ZoneInfo<'a> {
    name: &'a str,
    root: &'a Utf8Path,
}

#[derive(Debug, Clone, Copy)]
pub enum ArchiveWhat {
    ImmutableOnly,
    Everything,
}

#[derive(Debug, Clone)]
pub struct GlobGroup<'a> {
    label: &'static str,
    glob_pattern: String,
    kind: DebugFileKind<'a>,
}

#[derive(Debug, Clone, Copy)]
pub enum DebugFileKind<'a> {
    LogRotated(&'a ZoneInfo<'a>),
    LogLive(&'a ZoneInfo<'a>),
    Core,
}

impl DebugFileKind<'_> {
    fn should_delete_original(&self) -> bool {
        match self {
            DebugFileKind::LogRotated(_) | DebugFileKind::Core => true,
            DebugFileKind::LogLive(zone_info) => false,
        }
    }
}

pub fn plan_archive_zones<'a>(
    zones: &'a [ZoneInfo<'a>],
    what: ArchiveWhat,
) -> impl Iterator<Item = GlobGroup<'a>> {
    let mut rv = Vec::new();

    for zone in zones {
        let smf_log_dir = format!("{}/var/svc/log", zone.root);
        rv.push(GlobGroup {
            label: "rotated SMF logs",
            // XXX-dap digits
            glob_pattern: format!("{smf_log_dir}/*.log.*"),
            kind: DebugFileKind::LogRotated(zone),
        });

        let syslog_dir = format!("{}/var/adm", zone.root);
        rv.push(GlobGroup {
            label: "rotated syslog",
            // XXX-dap digits
            glob_pattern: format!("{syslog_dir}/messages.*"),
            kind: DebugFileKind::LogRotated(zone),
        });

        match what {
            ArchiveWhat::ImmutableOnly => (),
            ArchiveWhat::Everything => {
                rv.push(GlobGroup {
                    label: "live SMF logs",
                    glob_pattern: format!("{smf_log_dir}/*.log"),
                    kind: DebugFileKind::LogLive(zone),
                });
                rv.push(GlobGroup {
                    label: "live syslog",
                    glob_pattern: format!("{syslog_dir}/messages"),
                    kind: DebugFileKind::LogLive(zone),
                });
            }
        }
    }

    rv.into_iter()
}

pub fn plan_archive_cores<'a>(
    core_directories: &'a [&'a Utf8Path],
) -> impl Iterator<Item = GlobGroup<'a>> {
    core_directories.iter().map(|core_dir| GlobGroup {
        label: "core files",
        glob_pattern: format!("{core_dir}/*"),
        kind: DebugFileKind::Core,
    })
}

pub struct ArchiveStep<'a> {
    source: Utf8PathBuf,
    kind: DebugFileKind<'a>,
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

pub fn plan_archive<'a>(
    globs: impl Iterator<Item = GlobGroup<'a>>,
) -> Result<impl Iterator<Item = ArchiveStep<'a>>, glob::PatternError> {
    // XXX-dap this really ought to work in a streaming way
    let mut rv = Vec::new();

    for glob_group in globs {
        // XXX-dap warn instead of ignoring these errors with flatten
        let files = glob::glob(&glob_group.glob_pattern)?.flatten();
        for file in files {
            // XXX-dap unwrap()
            let path = Utf8PathBuf::try_from(file).unwrap();
            rv.push(ArchiveStep { source: path, kind: glob_group.kind });
        }
    }

    Ok(rv.into_iter())
}

pub async fn archive_all<'a>(
    log: &Logger,
    debug_dir: &Utf8Path,
    steps: impl Iterator<Item = ArchiveStep<'a>>,
) {
    // XXX-dap logging
    for step in steps {
        match archive_one(debug_dir, step).await {
            Ok(()) => (),
            Err(error) => {
                // XXX-dap warning
            }
        }
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
