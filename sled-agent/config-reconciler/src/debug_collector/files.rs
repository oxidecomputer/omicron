// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration and implementation for archiving ordinary files as debug data
//! (e.g., log files)

// XXX-dap next steps:
// - add output config
//   - directory
//   - filename
//   - both of these are dynamic, which makes it feel like this should be a
//     trait after all?

use camino::Utf8Path;
use camino::Utf8PathBuf;
use itertools::Itertools;
use slog::Logger;
use slog::debug;
use std::sync::LazyLock;
use tokio::fs::File;

#[derive(Debug, Clone, Copy)]
pub struct ZoneInfo<'a> {
    name: &'a str,
    root: &'a Utf8Path,
}

pub struct AllFilesConfig {
    file_configs: Vec<DebugFileConfig>,
}

impl AllFilesConfig {
    fn new(file_configs: Vec<DebugFileConfig>) -> AllFilesConfig {
        AllFilesConfig { file_configs }
    }

    pub fn start_archive<'a>(
        &'a self,
        log: &'a Logger,
        what: ArchiveWhat,
        zones: &'a [ZoneInfo<'a>],
    ) -> Archiver<'a> {
        Archiver { log: log.clone(), what, zones, configs: self }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ArchiveWhat {
    ImmutableOnly,
    Everything,
}

struct DebugFileConfig {
    name: String,
    path_in_zone: Utf8PathBuf,
    kind: DebugFileKind,
    // XXX-dap trait instead?
    // output_directory: Box<dyn Fn(&Utf8Path, &Utf8Path) -> Utf8PathBuf>,
    // output_file: Box<dyn Fn(&Utf8Path, &Utf8Path) -> File>,
    // XXX-dap cfg(test)
    test_examples: Vec<&'static str>,
    // XXX-dap output pattern
}

impl DebugFileConfig {
    fn archive_steps(
        &self,
        zone: &ZoneInfo<'_>,
        what: ArchiveWhat,
    ) -> Vec<ArchiveStep> {
        let mut rv = Vec::new();

        match self.kind {
            DebugFileKind::RotatedLog(base_pattern) => {
                // Always add a step for rotated logs.
                let zone_logs_path = zone.root.join(&self.path_in_zone);
                // XXX-dap digits
                let rotated_pattern =
                    zone_logs_path.join(format!("{base_pattern}.*"));
                rv.push(ArchiveStep {
                    label: format!("rotated logs for {}", self.name),
                    glob_pattern: rotated_pattern.to_string(),
                    do_delete: true,
                });

                // If this is an "everything" archival, then add a step for the
                // live logs, too.  But don't delete them.
                let live_pattern = zone_logs_path.join(base_pattern);
                rv.push(ArchiveStep {
                    label: format!("live logs for {}", self.name),
                    glob_pattern: live_pattern.to_string(),
                    do_delete: false,
                });
            }
        }

        rv
    }
}

#[derive(Debug, Clone, Copy)]
enum DebugFileKind {
    RotatedLog(&'static str),
}

enum ArchiveWhen {
    Always,
    NonPeriodicOnly,
}

enum DeleteWhen {
    Never,
    AfterArchive,
}

static ARCHIVE_CONFIGS: LazyLock<AllFilesConfig> = LazyLock::new(|| {
    AllFilesConfig::new(vec![
        DebugFileConfig {
            name: String::from("SMF log files"),
            path_in_zone: Utf8PathBuf::from("var/svc/log"),
            kind: DebugFileKind::RotatedLog("*.log"),
            test_examples: vec![
                // service with a live log and two rotated logs
                "svc1.log",
                "svc1.log.1",
                "svc1.log.2",
                // service with just a live log
                "svc2.log",
                // service with only rotated logs
                "svc3.log.0",
                "svc3.log.1",
                // servie with many digits in the rotated log file name
                "svc3.log.12345",
            ],
        },
        DebugFileConfig {
            name: String::from("syslog"),
            path_in_zone: Utf8PathBuf::from("var/adm"),
            kind: DebugFileKind::RotatedLog("messages"),
            test_examples: vec!["messages", "messages.0", "messages.1"],
        },
    ])
});

pub struct Archiver<'a> {
    log: Logger,
    what: ArchiveWhat,
    zones: &'a [ZoneInfo<'a>],
    configs: &'a AllFilesConfig,
}

impl Archiver<'_> {
    pub fn archive_all(self) -> Vec<anyhow::Error> {
        let mut errors = Vec::new();
        let mut archive_steps = self
            .zones
            .iter()
            .cartesian_product(&self.configs.file_configs)
            .flat_map(|(zone, file_config)| {
                file_config.archive_steps(&zone, self.what)
            });

        for step in archive_steps {
            debug!(&self.log, "processing archive step"; &step);
            if let Err(error) = self.archive_one(&step) {
                // XXX-dap log
                errors.push(error);
            }
        }

        errors
    }

    fn archive_one(&self, step: &ArchiveStep) -> Result<(), anyhow::Error> {
        // XXX-dap this is where we do the thing
        todo!();
    }
}

struct ArchiveStep {
    label: String,
    glob_pattern: String,
    do_delete: bool,
}

impl slog::KV for ArchiveStep {
    fn serialize(
        &self,
        record: &slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        todo!() // XXX-dap
    }
}
