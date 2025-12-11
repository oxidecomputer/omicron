// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration and implementation for archiving ordinary files as debug data
//! (e.g., log files)

// XXX-dap current status:
// - write battery of automated tests
// - finish cleanup

use anyhow::Context;
use anyhow::anyhow;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use derive_more::AsRef;
use slog::Logger;
use slog::debug;
use slog::o;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::fs::Metadata;
use std::sync::LazyLock;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use thiserror::Error;

struct ErrorAccumulator {
    errors: Vec<anyhow::Error>,
}

pub struct ArchivePlanner {
    log: Logger,
    what: ArchiveWhat,
    debug_dir: Utf8PathBuf,
    groups: Vec<ArchiveGroup<'static>>,
    lister: Box<dyn FileLister + Send + Sync>,
    errors: ErrorAccumulator,
}

impl ArchivePlanner {
    pub fn new(
        log: Logger,
        what: ArchiveWhat,
        debug_dir: &Utf8Path,
    ) -> ArchivePlanner {
        let log = log.new(o!(
            "component" => "DebugCollectorArchiver",
            "debug_dir" => debug_dir.to_string(),
            "what" => format!("{what:?}"),
        ));
        debug!(&log, "planning archival");

        ArchivePlanner {
            log,
            what,
            debug_dir: debug_dir.to_owned(),
            groups: Vec::new(),
            lister: Box::new(FilesystemLister),
            errors: ErrorAccumulator { errors: Vec::new() },
        }
    }

    pub fn include_zone(&mut self, zone_name: &str, zone_root: &Utf8Path) {
        debug!(
            &self.log,
            "archiving debug data from zone";
            "zonename" => zone_name,
            "zone_root" => %zone_root,
        );

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
        debug!(
            &self.log,
            "archiving debug data from cores directory";
            "cores_dir" => %cores_dir,
        );

        let source = Source {
            input_prefix: cores_dir.to_owned(),
            output_prefix: self.debug_dir.clone(), // XXX-dap check this
        };
        self.groups.push(ArchiveGroup { source, rule: &CORES_RULE })
    }

    fn into_plan(self) -> ArchivePlan {
        ArchivePlan {
            log: self.log,
            groups: self.groups,
            debug_dir: self.debug_dir,
            lister: self.lister,
            errors: self.errors,
        }
    }

    pub async fn execute(self) -> Result<(), anyhow::Error> {
        if !self.into_plan().execute().await.is_empty() {
            Err(anyhow!("one or more archive steps failed (see logs)"))
        } else {
            Ok(())
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
        lister: &dyn FileLister,
        output_directory: &Utf8Path,
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
            naming: Box::new(NameRotatedLogFile), // XXX-dap
        },
        Rule {
            label: "live syslog files",
            directory: VAR_ADM.parse().unwrap(),
            glob_pattern: "*.log".parse().unwrap(),
            delete_original: false,
            naming: Box::new(NameRotatedLogFile), // XXX-dap
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
        lister: &dyn FileLister,
        output_directory: &Utf8Path,
    ) -> Filename {
        // XXX-dap TODO-doc
        let filename_base = match source_file_name.as_ref().rsplit_once('.') {
            Some((base, _extension)) => base,
            None => source_file_name.as_ref(),
        };

        let mut n = source_file_metadata
            .modified()
            .unwrap_or_else(|_| SystemTime::now())
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let mut rv = format!("{filename_base}.{n}");
        // XXX-dap we should bound this loop and return an error
        // What if we don't have permissions?
        loop {
            let dest = output_directory.join(format!("{filename_base}.{n}"));
            if lister.file_exists(&dest) {
                n += 1;
                rv = format!("{filename_base}.{n}");
            } else {
                break;
            }
        }
        // unwrap(): we started with a valid `Filename` and did not add any
        // slashes here.
        Filename::try_from(rv).unwrap()
    }
}

// XXX-dap
// struct NameLiveLogFile;
// impl NamingRule for NameLiveLogFile {
//     fn archived_file_name(
//         &self,
//         source_file_name: &Filename,
//         source_file_metadata: &std::fs::Metadata,
//         lister: &dyn FileLister,
//         output_directory: &Utf8Path,
//     ) -> Filename {
//         todo!() // XXX-dap
//     }
// }

struct NameIdentity;
impl NamingRule for NameIdentity {
    fn archived_file_name(
        &self,
        source_file_name: &Filename,
        _source_file_metadata: &std::fs::Metadata,
        _lister: &dyn FileLister,
        _output_directory: &Utf8Path,
    ) -> Filename {
        source_file_name.clone()
    }
}

trait FileLister {
    fn list_files(&self, path: &Utf8Path) -> Vec<Filename>;
    fn file_metadata(&self, path: &Utf8Path) -> Option<Metadata>;
    fn file_exists(&self, path: &Utf8Path) -> bool;
}

struct FilesystemLister;
impl FileLister for FilesystemLister {
    fn list_files(&self, path: &Utf8Path) -> Vec<Filename> {
        // XXX-dap need a place to store errors
        let Ok(entry_iter) = path.read_dir_utf8() else {
            return Vec::new();
        };

        entry_iter
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                Filename::try_from(entry.file_name().to_owned()).ok()
            })
            .collect()
    }

    fn file_metadata(&self, path: &Utf8Path) -> Option<Metadata> {
        // XXX-dap need a place to store errors
        path.symlink_metadata().ok()
    }

    fn file_exists(&self, path: &Utf8Path) -> bool {
        path.exists()
    }
}

struct ArchivePlan {
    log: slog::Logger,
    debug_dir: Utf8PathBuf,
    groups: Vec<ArchiveGroup<'static>>,
    lister: Box<dyn FileLister + Send + Sync>,
    errors: ErrorAccumulator,
}

impl ArchivePlan {
    // XXX-dap cfg(test)
    fn to_steps(&self) -> impl Iterator<Item = ArchiveStep<'_>> {
        Self::to_steps_generic(
            &self.log,
            &self.groups,
            &self.debug_dir,
            &*self.lister,
        )
    }

    fn to_steps_generic<'a>(
        log: &Logger,
        groups: &'a [ArchiveGroup<'static>],
        debug_dir: &'a Utf8Path,
        lister: &'a (dyn FileLister + Send + Sync),
    ) -> impl Iterator<Item = ArchiveStep<'a>> {
        groups
            .iter()
            .map(|group| {
                let output_directory = group.output_directory(debug_dir);
                ArchiveStep::Mkdir { output_directory }
            })
            .chain(
                groups
                    .iter()
                    .flat_map(move |group| {
                        let input_directory = group.input_directory();

                        debug!(
                            log,
                            "listing directory";
                            "input_directory" => %input_directory
                        );
                        lister
                            .list_files(&input_directory)
                            .into_iter()
                            .map(move |filename| (group, filename))
                    })
                    .filter(move |(group, filename)| {
                        debug!(
                            log,
                            "checking file";
                            "file" => %filename.as_ref(),
                        );
                        group.rule.include_file(filename)
                    })
                    .filter_map(|(group, filename)| {
                        let input_path =
                            group.input_directory().join(filename.as_ref());
                        lister
                            .file_metadata(&input_path)
                            .map(|metadata| (group, input_path, metadata))
                    })
                    .map(|(group, input_path, metadata)| {
                        let output_directory =
                            group.output_directory(debug_dir);
                        ArchiveStep::ArchiveFile {
                            input_path,
                            metadata,
                            output_directory,
                            lister,
                            namer: &*group.rule.naming,
                            delete_original: group.rule.delete_original,
                        }
                    }),
            )
    }

    async fn execute(self) -> Vec<anyhow::Error> {
        let mut errors = self.errors;
        let log = &self.log;
        let groups = self.groups;
        let debug_dir = self.debug_dir;
        let lister = self.lister;
        for step in Self::to_steps_generic(log, &groups, &debug_dir, &*lister) {
            let result = match step {
                ArchiveStep::Mkdir { output_directory } => {
                    // We assume that the parent of all output directories
                    // already exists. XXX-dap document better
                    debug!(
                        log,
                        "create directory";
                        "directory" => %output_directory
                    );
                    tokio::fs::create_dir(&output_directory)
                        .await
                        .or_else(|error| {
                            if error.kind() == std::io::ErrorKind::AlreadyExists
                            {
                                Ok(())
                            } else {
                                Err(error)
                            }
                        })
                        .with_context(|| format!("mkdir {output_directory:?}"))
                }
                ArchiveStep::ArchiveFile {
                    input_path,
                    delete_original,
                    metadata,
                    output_directory,
                    namer,
                    lister,
                } => {
                    let output_filename = namer.archived_file_name(
                        // XXX-dap
                        &input_path
                            .file_name()
                            .unwrap()
                            .to_owned()
                            .try_into()
                            .unwrap(),
                        &metadata,
                        lister,
                        &output_directory,
                    );
                    let output_path =
                        output_directory.join(output_filename.as_ref());
                    debug!(
                        log,
                        "archive file";
                        "input_path" => %input_path,
                        "output_path" => %output_path,
                        "delete_original" => delete_original,
                    );
                    archive_one(&input_path, &output_path, delete_original)
                        .await
                        .with_context(|| {
                            format!("archive {input_path:?} to {output_path:?}")
                        })
                }
            };

            if let Err(error) = result {
                warn!(
                    log,
                    "error during archival";
                    InlineErrorChain::new(&*error)
                );
                errors.errors.push(error);
            }
        }

        errors.errors
    }
}

enum ArchiveStep<'a> {
    Mkdir {
        output_directory: Utf8PathBuf,
    },
    ArchiveFile {
        input_path: Utf8PathBuf,
        metadata: Metadata,
        output_directory: Utf8PathBuf,
        lister: &'a (dyn FileLister + Send + Sync),
        namer: &'a (dyn NamingRule + Send + Sync),
        delete_original: bool,
    },
}
