// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration and implementation for archiving ordinary files as debug data
//! (e.g., log files)

// XXX-dap current status:
// - working on the test at the end of the file
// - write battery of other automated tests
// - lots of cleanup to do

use anyhow::Context;
use anyhow::anyhow;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use chrono::DateTime;
use chrono::Utc;
use derive_more::AsRef;
use either::Either;
use slog::Logger;
use slog::debug;
use slog::o;
use slog::warn;
use slog_error_chain::InlineErrorChain;
use std::sync::LazyLock;
use thiserror::Error;

struct ErrorAccumulator {
    errors: Vec<anyhow::Error>,
}

pub struct ArchivePlanner<'a> {
    log: Logger,
    what: ArchiveWhat,
    debug_dir: Utf8PathBuf,
    groups: Vec<ArchiveGroup<'static>>,
    lister: &'a (dyn FileLister + Send + Sync),
    errors: ErrorAccumulator,
}

impl ArchivePlanner<'static> {
    pub fn new(
        log: &Logger,
        what: ArchiveWhat,
        debug_dir: &Utf8Path,
    ) -> ArchivePlanner<'static> {
        Self::new_with_lister(log, what, debug_dir, &FilesystemLister)
    }
}

impl<'a> ArchivePlanner<'a> {
    fn new_with_lister(
        log: &Logger,
        what: ArchiveWhat,
        debug_dir: &Utf8Path,
        lister: &'a (dyn FileLister + Send + Sync),
    ) -> ArchivePlanner<'a> {
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
            lister,
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

        let source = Source::for_zone(zone_name, zone_root, &self.debug_dir);
        let rules = match self.what {
            ArchiveWhat::ImmutableOnly => {
                Either::Left(ZONE_RULES_IMMUTABLE.iter())
            }
            ArchiveWhat::Everything => Either::Right(
                ZONE_RULES_IMMUTABLE.iter().chain(ZONE_RULES_LIVE.iter()),
            ),
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

        let source = Source::for_cores_directory(cores_dir, &self.debug_dir);
        self.groups.push(ArchiveGroup { source, rule: &CORES_RULE })
    }

    fn into_plan(self) -> ArchivePlan<'a> {
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

impl Source {
    fn for_zone(
        zone_name: &str,
        zone_root: &Utf8Path,
        output_directory: &Utf8Path,
    ) -> Source {
        Source {
            input_prefix: zone_root.to_owned(),
            output_prefix: output_directory.join(zone_name),
        }
    }

    fn for_cores_directory(
        cores_dir: &Utf8Path,
        output_directory: &Utf8Path,
    ) -> Source {
        Source {
            input_prefix: cores_dir.to_owned(),
            output_prefix: output_directory.to_owned(),
        }
    }
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
        source_file_mtime: Option<DateTime<Utc>>,
        lister: &dyn FileLister,
        output_directory: &Utf8Path,
    ) -> Result<Filename, anyhow::Error>;
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
        source_file_mtime: Option<DateTime<Utc>>,
        lister: &dyn FileLister,
        output_directory: &Utf8Path,
    ) -> Result<Filename, anyhow::Error> {
        // XXX-dap TODO-doc
        let filename_base = match source_file_name.as_ref().rsplit_once('.') {
            Some((base, _extension)) => base,
            None => source_file_name.as_ref(),
        };

        let mtime_as_seconds =
            source_file_mtime.unwrap_or_else(|| Utc::now()).timestamp();
        for i in 0..30 {
            let rv = format!("{filename_base}.{}", mtime_as_seconds + i);
            let dest = output_directory.join(&rv);
            if !lister.file_exists(&dest)? {
                // unwrap(): we started with a valid `Filename` and did not add
                // any slashes here.
                return Ok(Filename::try_from(rv).unwrap());
            }
        }

        // XXX-dap better message
        Err(anyhow!("too many files with the same mtime"))
    }
}

// XXX-dap
// struct NameLiveLogFile;
// impl NamingRule for NameLiveLogFile {
//     fn archived_file_name(
//         &self,
//         source_file_name: &Filename,
//         source_file_mtime: &std::fs::Metadata,
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
        _source_file_mtime: Option<DateTime<Utc>>,
        _lister: &dyn FileLister,
        _output_directory: &Utf8Path,
    ) -> Result<Filename, anyhow::Error> {
        Ok(source_file_name.clone())
    }
}

trait FileLister {
    fn list_files(
        &self,
        path: &Utf8Path,
    ) -> Vec<Result<Filename, anyhow::Error>>;
    fn file_mtime(
        &self,
        path: &Utf8Path,
    ) -> Result<Option<DateTime<Utc>>, anyhow::Error>;
    fn file_exists(&self, path: &Utf8Path) -> Result<bool, anyhow::Error>;
}

struct FilesystemLister;
impl FileLister for FilesystemLister {
    fn list_files(
        &self,
        path: &Utf8Path,
    ) -> Vec<Result<Filename, anyhow::Error>> {
        let entry_iter = match path
            .read_dir_utf8()
            .with_context(|| format!("readdir {path:?}"))
        {
            Ok(entry_iter) => entry_iter,
            Err(error) => return vec![Err(error)],
        };

        entry_iter
            .map(|entry| {
                entry.context("reading directory entry").and_then(|entry| {
                    // It should be impossible for this `try_from()` to fail,
                    // but it's easy enough to handle gracefully.
                    Filename::try_from(entry.file_name().to_owned())
                        .with_context(|| {
                            format!(
                                "processing as a file name: {:?}",
                                entry.file_name(),
                            )
                        })
                })
            })
            .collect()
    }

    fn file_mtime(
        &self,
        path: &Utf8Path,
    ) -> Result<Option<DateTime<Utc>>, anyhow::Error> {
        let metadata = path
            .symlink_metadata()
            .with_context(|| format!("loading metadata for {path:?}"))?;

        Ok(metadata
            .modified()
            // This `ok()` ignores an error fetching the mtime.  We could
            // probably just handle it, since it shouldn't come up.  But this
            // preserves historical behavior.
            .ok()
            .map(|m| m.into()))
    }

    fn file_exists(&self, path: &Utf8Path) -> Result<bool, anyhow::Error> {
        path.try_exists()
            .with_context(|| format!("checking existence of {path:?}"))
    }
}

struct ArchivePlan<'a> {
    log: slog::Logger,
    debug_dir: Utf8PathBuf,
    groups: Vec<ArchiveGroup<'static>>,
    lister: &'a (dyn FileLister + Send + Sync),
    errors: ErrorAccumulator,
}

impl ArchivePlan<'_> {
    #[cfg(test)]
    fn to_steps(
        &self,
    ) -> impl Iterator<Item = Result<ArchiveStep<'_>, anyhow::Error>> {
        Self::to_steps_generic(
            &self.log,
            &self.groups,
            &self.debug_dir,
            self.lister,
        )
    }

    fn to_steps_generic<'a>(
        log: &Logger,
        groups: &'a [ArchiveGroup<'static>],
        debug_dir: &'a Utf8Path,
        lister: &'a (dyn FileLister + Send + Sync),
    ) -> impl Iterator<Item = Result<ArchiveStep<'a>, anyhow::Error>> {
        groups
            .iter()
            .map(|group| {
                let output_directory = group.output_directory(debug_dir);
                Ok(ArchiveStep::Mkdir { output_directory })
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
                        lister.list_files(&input_directory).into_iter().map(
                            move |item| item.map(|filename| (group, filename)),
                        )
                    })
                    .filter(move |entry| match entry {
                        Err(_) => true,
                        Ok((group, filename)) => {
                            debug!(
                                log,
                                "checking file";
                                "file" => %filename.as_ref(),
                            );
                            group.rule.include_file(&filename)
                        }
                    })
                    .filter_map(|entry| match entry {
                        Ok((group, filename)) => {
                            let input_path =
                                group.input_directory().join(filename.as_ref());
                            Some(
                                lister
                                    .file_mtime(&input_path)
                                    .map(|mtime| (group, input_path, mtime)),
                            )
                        }
                        Err(error) => Some(Err(error)),
                    })
                    .map(|entry| {
                        entry.and_then(|(group, input_path, mtime)| {
                            let output_directory =
                                group.output_directory(debug_dir);
                            Ok(ArchiveStep::ArchiveFile {
                                input_path,
                                mtime,
                                output_directory,
                                lister,
                                namer: &*group.rule.naming,
                                delete_original: group.rule.delete_original,
                            })
                        })
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
                Err(error) => Err(error),
                Ok(ArchiveStep::Mkdir { output_directory }) => {
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
                Ok(ArchiveStep::ArchiveFile {
                    input_path,
                    delete_original,
                    mtime,
                    output_directory,
                    namer,
                    lister,
                }) => {
                    match namer.archived_file_name(
                        // XXX-dap
                        &input_path
                            .file_name()
                            .unwrap()
                            .to_owned()
                            .try_into()
                            .unwrap(),
                        mtime,
                        lister,
                        &output_directory,
                    ) {
                        Err(error) => Err(error),
                        Ok(output_filename) => {
                            let output_path =
                                output_directory.join(output_filename.as_ref());
                            debug!(
                                log,
                                "archive file";
                                "input_path" => %input_path,
                                "output_path" => %output_path,
                                "delete_original" => delete_original,
                            );
                            archive_one(
                                &input_path,
                                &output_path,
                                delete_original,
                            )
                            .await
                            .with_context(|| {
                                format!(
                                    "archive {input_path:?} to {output_path:?}"
                                )
                            })
                        }
                    }
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
        mtime: Option<DateTime<Utc>>,
        output_directory: Utf8PathBuf,
        lister: &'a (dyn FileLister + Send + Sync),
        namer: &'a (dyn NamingRule + Send + Sync),
        delete_original: bool,
    },
}

#[cfg(test)]
mod test {
    use super::Filename;
    use crate::debug_collector::files::ArchivePlanner;
    use crate::debug_collector::files::ArchiveStep;
    use crate::debug_collector::files::ArchiveWhat;
    use crate::debug_collector::files::FileLister;
    use anyhow::Context;
    use anyhow::anyhow;
    use camino::Utf8Path;
    use camino::Utf8PathBuf;
    use chrono::DateTime;
    use chrono::Utc;
    use omicron_test_utils::dev::test_setup_log;
    use regex::Regex;
    use std::collections::BTreeSet;
    use std::sync::LazyLock;
    use strum::Display;
    use strum::EnumDiscriminants;
    use strum::EnumIter;
    use strum::IntoDiscriminant;
    use strum::IntoEnumIterator;

    struct TestFile {
        path: Utf8PathBuf,
        kind: TestFileKind,
    }

    #[derive(Debug, Display, EnumIter, EnumDiscriminants)]
    #[strum_discriminants(derive(EnumIter, Ord, PartialOrd))]
    enum TestFileKind {
        KernelCrashDump {
            cores_directory: String,
        },
        ProcessCoreDump {
            cores_directory: String,
        },
        LogSmfRotated {
            zone_name: String,
            zone_root: String,
        },
        LogSmfLive {
            zone_name: String,
            zone_root: String,
        },
        LogSyslogRotated {
            zone_name: String,
            zone_root: String,
        },
        LogSyslogLive {
            zone_name: String,
            zone_root: String,
        },
        GlobalLogSmfRotated,
        GlobalLogSmfLive,
        GlobalLogSyslogRotated,
        GlobalLogSyslogLive,
        /// files we don't especially care about, but are in the test data to
        /// ensure that they don't create a problem
        Ignored,
    }

    impl TestFileKind {
        fn cores_directory(&self) -> Option<&Utf8Path> {
            match self {
                TestFileKind::KernelCrashDump { cores_directory }
                | TestFileKind::ProcessCoreDump { cores_directory } => {
                    Some(Utf8Path::new(cores_directory))
                }
                TestFileKind::LogSmfRotated { .. }
                | TestFileKind::LogSmfLive { .. }
                | TestFileKind::LogSyslogRotated { .. }
                | TestFileKind::LogSyslogLive { .. }
                | TestFileKind::GlobalLogSmfRotated
                | TestFileKind::GlobalLogSmfLive
                | TestFileKind::GlobalLogSyslogRotated
                | TestFileKind::GlobalLogSyslogLive
                | TestFileKind::Ignored => None,
            }
        }

        fn zone_info(&self) -> Option<(&str, &Utf8Path)> {
            match self {
                TestFileKind::KernelCrashDump { .. }
                | TestFileKind::ProcessCoreDump { .. }
                | TestFileKind::Ignored => None,
                TestFileKind::LogSmfRotated { zone_name, zone_root }
                | TestFileKind::LogSmfLive { zone_name, zone_root }
                | TestFileKind::LogSyslogRotated { zone_name, zone_root }
                | TestFileKind::LogSyslogLive { zone_name, zone_root } => {
                    Some((zone_name, Utf8Path::new(zone_root)))
                }
                TestFileKind::GlobalLogSmfRotated
                | TestFileKind::GlobalLogSmfLive
                | TestFileKind::GlobalLogSyslogRotated
                | TestFileKind::GlobalLogSyslogLive => {
                    Some(("global", Utf8Path::new("/")))
                }
            }
        }
    }

    static RE_CORES_DATASET: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new("^(/pool/int/[^/]+/crash)/[^/]+$").unwrap()
    });

    static RE_NONGLOBAL_ZONE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new("^/pool/ext/[^/]+/crypt/zone/([^/]+)/root").unwrap()
    });

    impl TryFrom<&Utf8Path> for TestFileKind {
        type Error = anyhow::Error;

        fn try_from(value: &Utf8Path) -> Result<Self, Self::Error> {
            let s = value.as_str();

            if let Some(m) = RE_CORES_DATASET.find(s) {
                let cores_dataset = m.as_str().to_owned();
                if s.ends_with("bounds") {
                    Ok(TestFileKind::Ignored)
                } else if s.contains("/vmdump.") {
                    Ok(TestFileKind::KernelCrashDump {
                        cores_directory: cores_dataset,
                    })
                } else if s.contains("/core.") {
                    Ok(TestFileKind::ProcessCoreDump {
                        cores_directory: cores_dataset,
                    })
                } else {
                    Err(anyhow!("unknown cores dataset test file kind"))
                }
            } else if let Some(c) = RE_NONGLOBAL_ZONE.captures(s) {
                let (zone_root, [zone_name]) = c.extract();
                let zone_root = zone_root.to_owned();
                let zone_name = zone_name.to_owned();
                if s.ends_with("/messages") {
                    Ok(TestFileKind::LogSyslogLive { zone_name, zone_root })
                } else if s.contains("/messages.") {
                    Ok(TestFileKind::LogSyslogRotated { zone_name, zone_root })
                } else if s.contains("/var/svc/log") {
                    if s.ends_with(".log") {
                        Ok(TestFileKind::LogSmfLive { zone_name, zone_root })
                    } else {
                        Ok(TestFileKind::LogSmfRotated { zone_name, zone_root })
                    }
                } else {
                    Err(anyhow!("unknown non-global zone test file kind"))
                }
            } else {
                if s == "/var/adm/messages" {
                    Ok(TestFileKind::GlobalLogSyslogLive)
                } else if s.starts_with("/var/adm") && s.contains("/messages.")
                {
                    Ok(TestFileKind::GlobalLogSyslogRotated)
                } else if s.starts_with("/var/svc/log") {
                    if s.ends_with(".log") {
                        Ok(TestFileKind::GlobalLogSmfLive)
                    } else {
                        Ok(TestFileKind::GlobalLogSmfRotated)
                    }
                } else {
                    Err(anyhow!("unknown test file kind"))
                }
            }
        }
    }

    fn load_real_paths() -> anyhow::Result<BTreeSet<Utf8PathBuf>> {
        let path = "test-data/debug-files.txt";
        std::fs::read_to_string(&path)
            .with_context(|| format!("read {path:?}"))?
            .lines()
            .enumerate()
            .map(|(i, l)| (i, l.trim()))
            .filter(|(_i, l)| !l.is_empty() && !l.starts_with("#"))
            .map(|(i, l)| {
                Utf8PathBuf::try_from(l).map_err(|_err| {
                    anyhow!("{path:?} line {}: non-UTF8 file path", i + 1)
                })
            })
            .collect()
    }

    fn load_test_files() -> anyhow::Result<Vec<TestFile>> {
        load_real_paths()?
            .into_iter()
            .map(|path| {
                TestFileKind::try_from(path.as_ref())
                    .context("may need to update load_test_files()?")
                    .map(|kind| TestFile { path, kind })
            })
            .collect()
    }

    struct TestLister<'a> {
        files: &'a [TestFile],
    }

    impl<'a> TestLister<'a> {
        fn new(files: &'a [TestFile]) -> Self {
            Self { files }
        }
    }
    impl FileLister for TestLister<'_> {
        fn list_files(
            &self,
            path: &Utf8Path,
        ) -> Vec<Result<Filename, anyhow::Error>> {
            self.files
                .iter()
                .filter_map(|test_file| {
                    let directory = test_file
                        .path
                        .parent()
                        .expect("test file has a parent");
                    (directory == path).then(|| {
                        let filename = test_file
                            .path
                            .file_name()
                            .expect("test file has a filename");
                        Ok(Filename::try_from(filename.to_owned())
                            .expect("filename has no slashes"))
                    })
                })
                .collect()
        }

        fn file_mtime(
            &self,
            _path: &Utf8Path,
        ) -> Result<Option<DateTime<Utc>>, anyhow::Error> {
            // XXX-dap will need to be overridable
            Ok(Some("2025-12-12T16:51:00-07:00".parse().unwrap()))
        }

        fn file_exists(&self, path: &Utf8Path) -> Result<bool, anyhow::Error> {
            Ok(self.files.iter().any(|f| f.path == path))
        }
    }

    /// Test that our test data includes all the kinds of things that we expect.
    /// If you see this test failing, presumably you updated the test data and
    /// you'll need to make sure it's still representative.
    #[test]
    fn test_test_data() {
        // Load the test data and determine what kind of file each one is.
        let files = load_test_files().unwrap();

        // Create a set of all the kinds of test files that we have not seen so
        // far.  We'll remove from this set as we find files of this kind.  Any
        // kinds left over at the end are missing from our test data.
        let mut all_kinds: BTreeSet<_> =
            TestFileKindDiscriminants::iter().collect();
        // We don't care about finding the "ignored" kind.
        all_kinds.remove(&TestFileKindDiscriminants::Ignored);
        for test_file in files {
            println!("{} {}", test_file.kind, test_file.path);
            all_kinds.remove(&test_file.kind.discriminant());
        }

        if !all_kinds.is_empty() {
            panic!("missing file in test data for kinds: {:?}", all_kinds);
        }
    }

    /// Test that every file found on a production system matches at most one
    /// of our configured rules.
    #[test]
    fn test_real_files_match_one_rule() {
        let files = load_test_files().unwrap();

        // Construct sources that correspond with the test data.
        let fake_output_dir = Utf8Path::new("/fake-output-directory");
        let cores_datasets: BTreeSet<_> = files
            .iter()
            .filter_map(|test_file| test_file.kind.cores_directory())
            .collect();
        let zone_infos: BTreeSet<_> = files
            .iter()
            .filter_map(|test_file| test_file.kind.zone_info())
            .collect();

        // Make a test-only file lister for our test data.
        let lister = TestLister::new(&files);

        // Plan an archival pass.
        let logctx = test_setup_log("test_real_files_match_one_rule");
        let log = &logctx.log;
        let mut planner = ArchivePlanner::new_with_lister(
            log,
            ArchiveWhat::Everything,
            fake_output_dir,
            &lister,
        );
        for cores_dir in cores_datasets {
            planner.include_cores_directory(cores_dir);
        }
        for (zone_name, zone_root) in zone_infos {
            planner.include_zone(zone_name, zone_root);
        }
        let plan = planner.into_plan();

        let mut all_files: BTreeSet<_> =
            files.iter().map(|f| &f.path).collect();
        for step in plan.to_steps() {
            let step = step.expect("no errors with test lister");
            // XXX-dap make sure the Mkdirs are tested elsewhere
            // XXX-dap make sure the delete_original flag is right for this file
            // kind
            let ArchiveStep::ArchiveFile { input_path, .. } = step else {
                continue;
            };

            println!("archiving: {input_path}");
            if !all_files.remove(&input_path) {
                panic!(
                    "attempted to archive the same file multiple times \
                     (or it was not in the test dataset): {input_path:?}",
                );
            }
        }

        // XXX-dap there are files here that aren't being archived that should
        // be
        println!("files that were not archived: {}", all_files.len());
        for file in all_files {
            println!("    {file}");
        }

        logctx.cleanup_successful();
    }
}
