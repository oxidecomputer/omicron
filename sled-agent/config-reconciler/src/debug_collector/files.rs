// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration and implementation for archiving ordinary files as debug data
//! (e.g., log files)

// XXX-dap current status:
// - flesh out regular implementation (see XXX-daps, todos)
// - run the existing test in worker.rs to make sure it works
// - write battery of automated tests
// - finish cleanup

use anyhow::anyhow;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use derive_more::AsRef;
use slog::Logger;
use std::sync::LazyLock;
use thiserror::Error;

pub struct Archiver {
    log: Logger,
    what: ArchiveWhat,
    debug_dir: Utf8PathBuf,
    groups: Vec<ArchiveGroup<'static>>,
}

impl Archiver {
    pub fn new(
        log: Logger,
        what: ArchiveWhat,
        debug_dir: &Utf8Path,
    ) -> Archiver {
        Archiver {
            log,
            what,
            debug_dir: debug_dir.to_owned(),
            groups: Vec::new(),
        }
    }

    pub fn include_zone(&mut self, zone_name: &str, zone_root: &Utf8Path) {
        let source = Source {
            input_prefix: zone_root.to_owned(),
            output_prefix: self.debug_dir.join(zone_name),
        };

        // XXX-dap TODO-cleanup
        let mut iter1;
        let mut iter2;
        let rules: &mut dyn Iterator<Item = &Rule> = match self.what {
            ArchiveWhat::ImmutableOnly => {
                iter1 = ZONE_RULES_IMMUTABLE.iter();
                &mut iter1
            }
            ArchiveWhat::Everything => {
                iter2 =
                    ZONE_RULES_IMMUTABLE.iter().chain(ZONE_RULES_LIVE.iter());
                &mut iter2
            }
        };

        for rule in rules {
            self.groups.push(ArchiveGroup { source: source.clone(), rule });
        }
    }

    pub fn include_cores_directory(&mut self, cores_dir: &Utf8Path) {
        let source = Source {
            input_prefix: cores_dir.to_owned(),
            output_prefix: self.debug_dir.clone(), // XXX-dap check this
        };
        self.groups.push(ArchiveGroup { source, rule: &CORES_RULE })
    }

    // XXX-dap error type
    pub async fn execute(self) -> Result<(), anyhow::Error> {
        let mut rv = Vec::new();

        // XXX-dap refactor into subfunctions so we can use `?`
        // XXX-dap refactor so it can be streaming so that we can see all the
        // specific archive calls that pop out?
        for group in self.groups {
            let input_directory = group.input_directory();
            let output_directory = group.output_directory(&self.debug_dir);

            let files =
                match input_directory.read_dir_utf8() {
                    Ok(files) => files,
                    Err(error) => {
                        rv.push(anyhow!(error).context(format!(
                            "processing {:?}",
                            input_directory
                        )));
                        continue;
                    }
                };

            for maybe_file in files {
                let file = match maybe_file {
                    Ok(file) => file,
                    Err(error) => {
                        rv.push(anyhow!(error).context(format!(
                            "processing file in {:?}",
                            input_directory,
                        )));
                        continue;
                    }
                };

                let filename_str = file.file_name();
                let filename =
                    match Filename::try_from(file.file_name().to_owned()) {
                        Ok(filename) => filename,
                        Err(error) => {
                            rv.push(anyhow!(error).context(format!(
                                "processing {filename_str}",
                            )));
                            continue;
                        }
                    };

                if !group.rule.include_file(&filename) {
                    continue;
                }

                let file_metadata = match file.metadata() {
                    Ok(metadata) => metadata,
                    Err(error) => {
                        rv.push(anyhow!(error).context(format!(
                            "processing {filename_str}: reading metadata",
                        )));
                        continue;
                    }
                };

                let output_filename = group
                    .rule
                    .naming
                    .archived_file_name(&filename, &file_metadata);
                let output_path =
                    output_directory.join(output_filename.as_ref());

                if let Err(error) = archive_one(
                    file.path(),
                    &output_path,
                    group.rule.delete_original,
                )
                .await
                {
                    rv.push(anyhow!(error).context(format!(
                        "processing {filename_str}: archiving",
                    )));
                    continue;
                }
            }
        }

        // XXX-dap
        if rv.is_empty() {
            Ok(())
        } else {
            Err(anyhow!("one or more archiving errors"))
        }
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

// XXX-dap copied from copy_sync_and_remove()
pub async fn archive_one(
    source: &Utf8Path,
    dest: &Utf8Path,
    delete_original: bool,
) -> tokio::io::Result<()> {
    let mut dest_f = tokio::fs::File::create(&dest).await?;
    let mut src_f = tokio::fs::File::open(&source).await?;

    tokio::io::copy(&mut src_f, &mut dest_f).await?;

    dest_f.sync_all().await?;

    drop(src_f);
    drop(dest_f);

    if delete_original {
        tokio::fs::remove_file(source).await?;
    }

    Ok(())
}

#[derive(Clone)]
struct Source {
    input_prefix: Utf8PathBuf,
    output_prefix: Utf8PathBuf,
}

struct Rule {
    label: &'static str,
    directory: Utf8PathBuf,
    glob_pattern: glob::Pattern, // XXX-dap consider regex?
    delete_original: bool,
    naming: Box<dyn NamingRule + Send + Sync>,
}

impl Rule {
    fn include_file(&self, filename: &Filename) -> bool {
        self.glob_pattern.matches(filename.as_ref())
    }
}

trait NamingRule {
    fn archived_file_name(
        &self,
        source_file_name: &Filename,
        source_file_metadata: &std::fs::Metadata,
    ) -> Filename;
}

struct ArchiveGroup<'a> {
    source: Source,
    rule: &'a Rule,
}

#[derive(AsRef, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Filename(String);
#[derive(Debug, Error)]
#[error("string is not a valid filename (has slashes or is '.' or '..')")]
struct BadFilename;
impl TryFrom<String> for Filename {
    type Error = BadFilename;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value == "." || value == ".." || value.contains('/') {
            Err(BadFilename)
        } else {
            Ok(Filename(value))
        }
    }
}

impl<'a> ArchiveGroup<'a> {
    fn input_directory(&self) -> Utf8PathBuf {
        self.source.input_prefix.join(&self.rule.directory)
    }

    fn output_directory(&self, debug_dir: &Utf8Path) -> Utf8PathBuf {
        debug_dir.join(&self.source.output_prefix)
    }
}

static VAR_SVC_LOG: &str = "var/svc/log";
static VAR_ADM: &str = "var/adm";
static ZONE_RULES_IMMUTABLE: LazyLock<Vec<Rule>> = LazyLock::new(|| {
    vec![
        Rule {
            label: "rotated SMF log files",
            directory: VAR_SVC_LOG.parse().unwrap(),
            glob_pattern: "*.log.*".parse().unwrap(), // XXX-dap digits
            delete_original: true,
            naming: Box::new(NameRotatedLogFile),
        },
        Rule {
            label: "rotated syslog files",
            directory: VAR_ADM.parse().unwrap(),
            glob_pattern: "messages.*".parse().unwrap(), // XXX-dap digits
            delete_original: true,
            naming: Box::new(NameRotatedLogFile),
        },
    ]
});
static ZONE_RULES_LIVE: LazyLock<Vec<Rule>> = LazyLock::new(|| {
    vec![
        Rule {
            label: "live SMF log files",
            directory: VAR_SVC_LOG.parse().unwrap(),
            glob_pattern: "*.log".parse().unwrap(),
            delete_original: false,
            naming: Box::new(NameLiveLogFile),
        },
        Rule {
            label: "live syslog files",
            directory: VAR_ADM.parse().unwrap(),
            glob_pattern: "*.log".parse().unwrap(),
            delete_original: false,
            naming: Box::new(NameLiveLogFile),
        },
    ]
});

static CORES_RULE: LazyLock<Rule> = LazyLock::new(|| Rule {
    label: "process core files",
    directory: ".".parse().unwrap(),
    glob_pattern: "core.*".parse().unwrap(),
    delete_original: true,
    naming: Box::new(NameIdentity),
});

struct NameRotatedLogFile;
impl NamingRule for NameRotatedLogFile {
    fn archived_file_name(
        &self,
        source_file_name: &Filename,
        source_file_metadata: &std::fs::Metadata,
    ) -> Filename {
        todo!() // XXX-dap
    }
}

struct NameLiveLogFile;
impl NamingRule for NameLiveLogFile {
    fn archived_file_name(
        &self,
        source_file_name: &Filename,
        source_file_metadata: &std::fs::Metadata,
    ) -> Filename {
        todo!() // XXX-dap
    }
}

struct NameIdentity;
impl NamingRule for NameIdentity {
    fn archived_file_name(
        &self,
        source_file_name: &Filename,
        _source_file_metadata: &std::fs::Metadata,
    ) -> Filename {
        source_file_name.clone()
    }
}
