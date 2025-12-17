// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration and implementation for archiving ordinary files as debug data
//! (e.g., log files)

// XXX-dap current status:
// - write battery of other automated tests
//   - ArchiveStep::ArchiveFile should have a method to compute the filename and
//     we should verify it
//     - this will also cover the case of a conflict with an existing file
//   - what else?
// - figure out what will happen for file conflicts outside of log files and
//   what to do about it
// - lots of cleanup to do

use anyhow::Context;
use anyhow::anyhow;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use chrono::DateTime;
use chrono::Utc;
use derive_more::AsRef;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
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

        let source = Source {
            input_prefix: zone_root.to_owned(),
            output_prefix: self.debug_dir.join(zone_name),
        };

        let rules =
            ALL_RULES.iter().filter(|r| match (&r.rule_scope, &self.what) {
                (RuleScope::ZoneAlways, _) => true,
                (RuleScope::ZoneMutable, ArchiveWhat::Everything) => true,
                (RuleScope::ZoneMutable, ArchiveWhat::ImmutableOnly) => false,
                (RuleScope::CoresDirectory, _) => false,
            });

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
            output_prefix: self.debug_dir.to_owned(),
        };

        let rules = ALL_RULES.iter().filter(|r| match r.rule_scope {
            RuleScope::CoresDirectory => true,
            RuleScope::ZoneMutable | RuleScope::ZoneAlways => false,
        });

        for rule in rules {
            self.groups.push(ArchiveGroup { source: source.clone(), rule });
        }
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

struct Rule {
    label: &'static str,
    rule_scope: RuleScope,
    directory: Utf8PathBuf,
    glob_pattern: glob::Pattern, // XXX-dap consider regex?
    delete_original: bool,
    // XXX-dap these are all static -- maybe use &'static dyn NamingRule?
    naming: Box<dyn NamingRule + Send + Sync>,
}

impl Rule {
    fn include_file(&self, filename: &Filename) -> bool {
        self.glob_pattern.matches(filename.as_ref())
    }
}

impl IdOrdItem for Rule {
    type Key<'a> = &'static str;
    fn key(&self) -> Self::Key<'_> {
        self.label
    }
    id_upcast!();
}

enum RuleScope {
    // this rule applies to all cores directories
    CoresDirectory,
    // this rule applies to zone roots for "everything" collections, but not
    // "immutable" ones
    ZoneMutable,
    // this rule applies to zone roots always, regardless of whether or not
    // we're collecting immutable data only
    ZoneAlways,
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

static ALL_RULES: LazyLock<IdOrdMap<Rule>> = LazyLock::new(|| {
    let rules = [
        Rule {
            label: "process core files and kernel crash dumps",
            rule_scope: RuleScope::CoresDirectory,
            directory: ".".parse().unwrap(),
            glob_pattern: "*".parse().unwrap(),
            delete_original: true,
            naming: Box::new(NameIdentity),
        },
        Rule {
            label: "live SMF log files",
            rule_scope: RuleScope::ZoneMutable,
            directory: VAR_SVC_LOG.parse().unwrap(),
            glob_pattern: "*.log".parse().unwrap(),
            delete_original: false,
            naming: Box::new(NameLiveLogFile),
        },
        Rule {
            label: "live syslog files",
            rule_scope: RuleScope::ZoneMutable,
            directory: VAR_ADM.parse().unwrap(),
            glob_pattern: "messages".parse().unwrap(),
            delete_original: false,
            naming: Box::new(NameLiveLogFile),
        },
        Rule {
            label: "rotated SMF log files",
            rule_scope: RuleScope::ZoneAlways,
            directory: VAR_SVC_LOG.parse().unwrap(),
            glob_pattern: "*.log.*".parse().unwrap(), // XXX-dap digits
            delete_original: true,
            naming: Box::new(NameRotatedLogFile),
        },
        Rule {
            label: "rotated syslog files",
            rule_scope: RuleScope::ZoneAlways,
            directory: VAR_ADM.parse().unwrap(),
            glob_pattern: "messages.*".parse().unwrap(), // XXX-dap digits
            delete_original: true,
            naming: Box::new(NameRotatedLogFile),
        },
    ];

    // We could do this more concisely with a `collect()` or `IdOrdMap::from`,
    // but those would silently discard duplicates.  We want to detect these and
    // provide a clear error message.
    let mut rv = IdOrdMap::new();
    for rule in rules {
        let label = rule.label;
        if let Err(_) = rv.insert_unique(rule) {
            panic!("found multiple rules with the same label: {:?}", label);
        }
    }

    rv
});

const MAX_COLLIDING_FILENAMES: u16 = 30;
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
        for i in 0..MAX_COLLIDING_FILENAMES {
            let rv =
                format!("{filename_base}.{}", mtime_as_seconds + i64::from(i));
            let dest = output_directory.join(&rv);
            if !lister.file_exists(&dest)? {
                // unwrap(): we started with a valid `Filename` and did not add
                // any slashes here.
                return Ok(Filename::try_from(rv).unwrap());
            }
        }

        // XXX-dap better message
        Err(anyhow!("too many files with colliding names"))
    }
}

struct NameLiveLogFile;
impl NamingRule for NameLiveLogFile {
    fn archived_file_name(
        &self,
        source_file_name: &Filename,
        source_file_mtime: Option<DateTime<Utc>>,
        lister: &dyn FileLister,
        output_directory: &Utf8Path,
    ) -> Result<Filename, anyhow::Error> {
        // XXX-dap should work better
        NameRotatedLogFile.archived_file_name(
            source_file_name,
            source_file_mtime,
            lister,
            output_directory,
        )
    }
}

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
            .filter_map(move |group| {
                let output_directory = group.output_directory(debug_dir);
                if output_directory != debug_dir {
                    Some(Ok(ArchiveStep::Mkdir { output_directory }))
                } else {
                    None
                }
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
                            Ok(ArchiveStep::ArchiveFile(ArchiveFile {
                                input_path,
                                mtime,
                                output_directory,
                                namer: &*group.rule.naming,
                                delete_original: group.rule.delete_original,
                                rule: group.rule.label,
                            }))
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
                    // already exists.  That's because in practice it should be
                    // true: all of the output directories are one level below
                    // the debug dataset itself.  (The test suite verifies
                    // this.)  So if we find at runtime that this isn't true,
                    // that's a bad sign.  Maybe somebody has unmounted the
                    // debug dataset and deleted its mountpoint?  We don't want
                    // to start spewing stuff to the wrong place.  That's why we
                    // don't use create_dir_all() here.
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
                Ok(ArchiveStep::ArchiveFile(archive_file)) => {
                    match archive_file.choose_filename(lister) {
                        Err(error) => Err(error),
                        Ok(output_filename) => {
                            let input_path = &archive_file.input_path;
                            let output_path = archive_file
                                .output_directory
                                .join(output_filename.as_ref());
                            debug!(
                                log,
                                "archive file";
                                "input_path" => %input_path,
                                "output_path" => %output_path,
                                "delete_original" =>
                                    archive_file.delete_original,
                            );
                            archive_one(
                                &input_path,
                                &output_path,
                                archive_file.delete_original,
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
    Mkdir { output_directory: Utf8PathBuf },
    ArchiveFile(ArchiveFile<'a>),
}

#[derive(Clone)]
struct ArchiveFile<'a> {
    input_path: Utf8PathBuf,
    mtime: Option<DateTime<Utc>>,
    output_directory: Utf8PathBuf,
    namer: &'a (dyn NamingRule + Send + Sync),
    delete_original: bool,
    rule: &'static str,
}

impl ArchiveFile<'_> {
    fn choose_filename(
        &self,
        lister: &dyn FileLister,
    ) -> Result<Filename, anyhow::Error> {
        let file_name: Filename = self
            .input_path
            .file_name()
            .ok_or_else(|| {
                // This should be impossible, but it's easy enough to handle
                // gracefully.
                anyhow!(
                    "file for archival has no filename: {:?}",
                    &self.input_path
                )
            })?
            .to_owned()
            .try_into()
            .context("file_name() returned a non-Filename")?;
        self.namer.archived_file_name(
            &file_name,
            self.mtime,
            lister,
            &self.output_directory,
        )
    }
}

#[cfg(test)]
mod test {
    use super::Filename;
    use crate::debug_collector::files::ALL_RULES;
    use crate::debug_collector::files::ArchiveFile;
    use crate::debug_collector::files::ArchivePlan;
    use crate::debug_collector::files::ArchivePlanner;
    use crate::debug_collector::files::ArchiveStep;
    use crate::debug_collector::files::ArchiveWhat;
    use crate::debug_collector::files::FileLister;
    use crate::debug_collector::files::MAX_COLLIDING_FILENAMES;
    use crate::debug_collector::files::NameRotatedLogFile;
    use anyhow::Context;
    use anyhow::anyhow;
    use anyhow::bail;
    use camino::Utf8Path;
    use camino::Utf8PathBuf;
    use chrono::DateTime;
    use chrono::Timelike;
    use chrono::Utc;
    use iddqd::IdOrdItem;
    use iddqd::IdOrdMap;
    use iddqd::id_upcast;
    use omicron_test_utils::dev::test_setup_log;
    use regex::Regex;
    use slog::Logger;
    use slog::debug;
    use slog::info;
    use slog_error_chain::InlineErrorChain;
    use std::collections::BTreeSet;
    use std::sync::LazyLock;
    use std::sync::Mutex;
    use strum::Display;
    use strum::EnumDiscriminants;
    use strum::EnumIter;
    use strum::IntoDiscriminant;
    use strum::IntoEnumIterator;

    #[derive(Clone)]
    struct TestFile {
        path: Utf8PathBuf,
        kind: TestFileKind,
    }

    impl IdOrdItem for TestFile {
        type Key<'a> = &'a Utf8Path;

        fn key(&self) -> Self::Key<'_> {
            &self.path
        }

        id_upcast!();
    }

    #[derive(Clone, Debug, Display, EnumIter, EnumDiscriminants)]
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

            if let Some(c) = RE_CORES_DATASET.captures(s) {
                let (_, [cores_directory]) = c.extract();
                let cores_directory = cores_directory.to_owned();
                if s.ends_with("bounds") {
                    Ok(TestFileKind::Ignored)
                } else if s.contains("/vmdump.") {
                    Ok(TestFileKind::KernelCrashDump { cores_directory })
                } else if s.contains("/core.") {
                    Ok(TestFileKind::ProcessCoreDump { cores_directory })
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

    fn load_test_files() -> anyhow::Result<IdOrdMap<TestFile>> {
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
        files: BTreeSet<&'a Utf8Path>,
        last_listed: Mutex<Option<Utf8PathBuf>>,
        injected_error: Option<&'a Utf8Path>,
    }

    impl<'a> TestLister<'a> {
        fn new_for_test_data(files: &'a IdOrdMap<TestFile>) -> Self {
            Self::new(files.iter().map(|test_file| test_file.path.as_path()))
        }

        fn empty() -> Self {
            Self::new::<_, &'a str>(std::iter::empty())
        }

        fn new<I, P>(files: I) -> Self
        where
            I: IntoIterator<Item = &'a P>,
            P: AsRef<Utf8Path> + ?Sized + 'a,
        {
            Self {
                files: files.into_iter().map(|p| p.as_ref()).collect(),
                last_listed: Mutex::new(None),
                injected_error: None,
            }
        }

        fn inject_error(&mut self, fail_path: &'a Utf8Path) {
            self.injected_error = Some(fail_path);
        }
    }

    impl FileLister for TestLister<'_> {
        fn list_files(
            &self,
            path: &Utf8Path,
        ) -> Vec<Result<Filename, anyhow::Error>> {
            // Keep track of the last path that was listed.
            *self.last_listed.lock().unwrap() = Some(path.to_owned());

            // Inject any errors we've been configured to inject.
            if let Some(fail_path) = self.injected_error {
                if path == fail_path {
                    return vec![Err(anyhow!(
                        "injected error for {fail_path:?}"
                    ))];
                }
            }

            // Create a directory listing from the files in our test data.
            self.files
                .iter()
                .filter_map(|file_path| {
                    let directory =
                        file_path.parent().expect("test file has a parent");
                    (directory == path).then(|| {
                        let filename = file_path
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
            path: &Utf8Path,
        ) -> Result<Option<DateTime<Utc>>, anyhow::Error> {
            if let Some(fail_path) = self.injected_error {
                if path == fail_path {
                    bail!("injected error for {fail_path:?}");
                }
            }
            // XXX-dap will need to be overridable
            Ok(Some("2025-12-12T16:51:00-07:00".parse().unwrap()))
        }

        fn file_exists(&self, path: &Utf8Path) -> Result<bool, anyhow::Error> {
            Ok(self.files.contains(path))
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

    fn test_archive<'a>(
        log: &Logger,
        files: &IdOrdMap<TestFile>,
        output_dir: &Utf8Path,
        what: ArchiveWhat,
        lister: &'a TestLister,
    ) -> ArchivePlan<'a> {
        // Construct sources that correspond with the test data.
        let cores_datasets: BTreeSet<_> = files
            .iter()
            .filter_map(|test_file| test_file.kind.cores_directory())
            .collect();
        let zone_infos: BTreeSet<_> = files
            .iter()
            .filter_map(|test_file| test_file.kind.zone_info())
            .collect();

        // Plan an archival pass.
        let mut planner =
            ArchivePlanner::new_with_lister(log, what, output_dir, lister);

        for cores_dir in cores_datasets {
            debug!(log, "including cores directory"; "cores_dir" => %cores_dir);
            planner.include_cores_directory(cores_dir);
        }

        for (zone_name, zone_root) in zone_infos {
            debug!(
                log,
                "including zone";
                "zone_name" => zone_name,
                "zone_root" => %zone_root,
            );
            planner.include_zone(zone_name, zone_root);
        }

        planner.into_plan()
    }

    /// Fully tests archive planning with a bunch of real-world file paths
    #[test]
    fn test_archiving_basic() {
        // Set up the test.
        let logctx = test_setup_log("test_archiving_basic");
        let log = &logctx.log;

        // Load the test data
        let files = load_test_files().unwrap();

        // Run a simulated archive.
        let fake_output_dir = Utf8Path::new("/fake-output-directory");
        let lister = TestLister::new_for_test_data(&files);
        let plan = test_archive(
            log,
            &files,
            fake_output_dir,
            ArchiveWhat::Everything,
            &lister,
        );

        // Now, walk through the archive plan and verify it.
        let mut directories_created = BTreeSet::new();
        let mut unarchived_files = files.clone();
        let mut rules_unused: BTreeSet<_> =
            ALL_RULES.iter().map(|r| r.label).collect();
        for step in plan.to_steps() {
            let step = step.expect("no errors with test lister");

            match step {
                // For a `mkdir`, verify that the parent directory matches our
                // output directory.  (For more on why, see the code where we
                // process this Mkdir.)  Then record it.  We'll use that to
                // verify that files are always archived into directories that
                // already exist.
                ArchiveStep::Mkdir { output_directory } => {
                    let parent = output_directory
                        .parent()
                        .expect("output directory has a parent");
                    if parent != fake_output_dir {
                        panic!(
                            "archiver created an output directory \
                             ({output_directory:?}) whose parent is not the \
                             fake debug directory ({fake_output_dir:?}).  \
                             This is not currently supported."
                        );
                    }
                    directories_created.insert(output_directory);
                }

                ArchiveStep::ArchiveFile(ArchiveFile {
                    input_path,
                    delete_original,
                    output_directory,
                    rule,
                    ..
                }) => {
                    println!("archiving: {input_path}");

                    // Check that we have not already archived this file.
                    // That would imply that two rules matched the same file,
                    // which would be a bug in the rule definitions.
                    let test_file = unarchived_files
                        .remove(input_path.as_path())
                        .unwrap_or_else(|| {
                            panic!(
                                "attempted to archive the same file multiple \
                                 times (or it was not in the test dataset): \
                                 {input_path:?}",
                            );
                        });

                    // Check that we've correctly determined whether to delete
                    // the original file when archiving it.  This is determined
                    // by the rule that it matched.  We check it here against
                    // what we expect for each kind of file.
                    match &test_file.kind {
                        TestFileKind::KernelCrashDump { .. }
                        | TestFileKind::ProcessCoreDump { .. }
                        | TestFileKind::LogSmfRotated { .. }
                        | TestFileKind::LogSyslogRotated { .. }
                        | TestFileKind::GlobalLogSmfRotated
                        | TestFileKind::GlobalLogSyslogRotated
                        | TestFileKind::Ignored => {
                            assert!(
                                delete_original,
                                "expected to delete original file when \
                                 archiving file of kind {:?}",
                                test_file.kind,
                            );
                        }

                        TestFileKind::LogSmfLive { .. }
                        | TestFileKind::LogSyslogLive { .. }
                        | TestFileKind::GlobalLogSmfLive
                        | TestFileKind::GlobalLogSyslogLive => {
                            assert!(
                                !delete_original,
                                "expected not to delete original file when \
                                 archiving file of kind {:?}",
                                test_file.kind,
                            );
                        }
                    }

                    // The output directory must either match the overall output
                    // directory or else be one of the directories created by a
                    // Mkdir that we've already processed.
                    if output_directory != fake_output_dir
                        && !directories_created.contains(&output_directory)
                    {
                        panic!(
                            "file was archived into a non-existent \
                             directory: {}",
                            test_file.path
                        );
                    }

                    // Mark that we've used this rule.  It's not a problem if
                    // we've already done so.
                    let _ = rules_unused.remove(rule);
                }
            };
        }

        if !rules_unused.is_empty() {
            panic!(
                "one or more rules was not covered by the tests: \
                 {rules_unused:?}"
            );
        }

        println!("files that were not archived: {}", unarchived_files.len());
        for test_file in unarchived_files {
            println!("    {}", test_file.path);
            if !matches!(test_file.kind, TestFileKind::Ignored) {
                panic!(
                    "non-ignored test file was not archived: {:?}",
                    test_file.path
                );
            }
        }

        logctx.cleanup_successful();
    }

    // Tests that when we archive "immutable-only" files:
    // - we do archive the stuff we expect
    // - we don't archive the stuff that we don't expect
    #[test]
    fn test_archiving_immutable_only() {
        // Set up the test.
        let logctx = test_setup_log("test_archiving_immutable_only");
        let log = &logctx.log;

        // Load the test data
        let files = load_test_files().unwrap();

        // Run a simulated archive.
        let fake_output_dir = Utf8Path::new("/fake-output-directory");
        let lister = TestLister::new_for_test_data(&files);
        let plan = test_archive(
            log,
            &files,
            fake_output_dir,
            ArchiveWhat::ImmutableOnly,
            &lister,
        );

        let mut expected_unarchived: BTreeSet<_> = files
            .iter()
            .filter_map(|test_file| {
                let expected = match test_file.kind {
                    TestFileKind::KernelCrashDump { .. }
                    | TestFileKind::ProcessCoreDump { .. }
                    | TestFileKind::LogSmfRotated { .. }
                    | TestFileKind::LogSyslogRotated { .. }
                    | TestFileKind::GlobalLogSmfRotated
                    | TestFileKind::GlobalLogSyslogRotated => true,
                    TestFileKind::LogSmfLive { .. }
                    | TestFileKind::LogSyslogLive { .. }
                    | TestFileKind::GlobalLogSmfLive
                    | TestFileKind::GlobalLogSyslogLive
                    | TestFileKind::Ignored => false,
                };

                expected.then_some(&test_file.path)
            })
            .collect();

        // Check that precisely the expected files were collected.
        // We do not check all the other expected behaviors around archiving
        // here.  That's tested in `test_archive_basic` for all files.
        for step in plan.to_steps() {
            let step = step.expect("no errors with test lister");
            let ArchiveStep::ArchiveFile(archive_file) = step else {
                continue;
            };

            let input_path = archive_file.input_path;
            let test_file = files.get(input_path.as_path()).expect(
                "unexpectedly archived file that was not in the test data",
            );
            if matches!(test_file.kind, TestFileKind::Ignored) {
                // We don't care whether "ignored" files get archived or not.
                continue;
            }

            if !expected_unarchived.remove(&input_path) {
                panic!(
                    "unexpectedly archived file (either it should not have \
                     been at all or it was archived more than once): \
                     {input_path:?}",
                );
            }

            // This is technically checked in the other test, but since it's
            // related to the file being immutable, we may as well check it
            // again here.
            assert!(
                archive_file.delete_original,
                "expected to delete the original when archiving immutable files"
            );
        }

        if !expected_unarchived.is_empty() {
            panic!(
                "did not archive some of the files we expected: {:?}",
                expected_unarchived
            );
        }

        logctx.cleanup_successful();
    }

    /// Verifies that the archive plan streams rather than pre-computing all the
    /// steps it has to do at once
    ///
    /// This property is important for scalability and memory usage.
    #[test]
    fn test_archiving_is_streaming() {
        // Set up the test.
        let logctx = test_setup_log("test_archiving_is_streaming");
        let log = &logctx.log;

        // Load the test data
        let files = load_test_files().unwrap();

        // Begin a simulated archive.
        let fake_output_dir = Utf8Path::new("/fake-output-directory");
        let lister = TestLister::new_for_test_data(&files);
        let plan = test_archive(
            log,
            &files,
            fake_output_dir,
            ArchiveWhat::Everything,
            &lister,
        );

        // Verify that the archiver operates in a streaming way by checking that
        // each archived file is contained in the most-recently-listed
        // directory.  If it's not, then it must have come from some previously
        // listed directory, which means that the archiver should have returned
        // it before listing the next directory.  In other words, that would
        // mean that the archiver read ahead of the directory whose files it's
        // currently archiving, which is the thing we're trying to check
        // doesn't happen.
        for step in plan.to_steps() {
            let step = step.expect("test lister does not produce errors");
            let ArchiveStep::ArchiveFile(archive_file) = &step else {
                continue;
            };

            let last_listed = lister.last_listed.lock().unwrap();
            let last = last_listed
                .as_ref()
                .expect("listed a directory before archiving any files");
            assert!(
                archive_file.input_path.starts_with(last),
                "archived file is not in the most-recently-listed directory",
            );
        }

        logctx.cleanup_successful();
    }

    /// Verifies that failure to list a directory does not affect archiving
    /// other directories
    #[test]
    fn test_directory_list_error() {
        // Set up the test.
        let logctx = test_setup_log("test_directory_list_error");
        let log = &logctx.log;

        // Load the test data
        let files = load_test_files().unwrap();

        // Choose a directory for which to inject an error.
        let fail_dir = files
            .iter()
            .find_map(|test_file| {
                if matches!(&test_file.kind, TestFileKind::Ignored) {
                    None
                } else {
                    let parent = test_file.path.parent().unwrap();
                    Some(Utf8Path::new(parent))
                }
            })
            .expect("at least one non-ignored file in test data");
        info!(
            log,
            "injecting error for directory";
            "directory" => fail_dir.as_str(),
        );

        // Begin a simulated archive.  Configure the lister to inject an error
        // for the directory that we chose.
        let fake_output_dir = Utf8Path::new("/fake-output-directory");
        let mut lister = TestLister::new_for_test_data(&files);
        lister.inject_error(fail_dir);
        let plan = test_archive(
            log,
            &files,
            fake_output_dir,
            ArchiveWhat::Everything,
            &lister,
        );

        // Now walk through the archive plan and make sure:
        // (1) Everything that's not in this directory gets archived.
        // (2) There's an error produced for this directory.
        // (3) Nothing is archived within this directory.
        let mut unarchived_files = files.clone();
        let mut nerrors = 0;
        for step in plan.to_steps() {
            let step = match step {
                Err(error) => {
                    let error = InlineErrorChain::new(&*error);
                    let error_str = error.to_string();
                    debug!(log, "found error"; error);
                    assert!(error_str.contains(fail_dir.as_str()));
                    assert!(error_str.contains("injected error"));
                    nerrors += 1;
                    continue;
                }
                Ok(step) => step,
            };

            let ArchiveStep::ArchiveFile(archive_file) = &step else {
                continue;
            };

            assert!(
                !archive_file.input_path.starts_with(fail_dir),
                "archived file in the directory where we injected an error"
            );

            let _ = unarchived_files
                .remove(archive_file.input_path.as_path())
                .expect("archived file was in list of test files");
        }

        // We should see one error for each time the directory that we chose was
        // listed.  That should always be at least once.  It could be more than
        // once, depending on how rules are configured.  For example, with two
        // rules for syslog (/var/adm/messages.* and /var/adm/messages), there
        // would be two errors for /var/adm.
        assert_ne!(
            nerrors, 0,
            "expected at least one error after injecting one"
        );

        for file in unarchived_files {
            assert!(
                file.path.starts_with(fail_dir),
                "missed file: {:?}",
                file.path
            );
        }

        logctx.cleanup_successful();
    }

    /// Verifies that failure to fetch file details does not affect archiving
    /// other files
    #[test]
    fn test_file_metadata_error() {
        // Set up the test.
        let logctx = test_setup_log("test_file_metadata_error");
        let log = &logctx.log;

        // Load the test data
        let files = load_test_files().unwrap();

        // Find a directory that contains at least two files.  We'll inject an
        // error for one of those files.
        let mut fail_file = None;
        {
            let mut dirs_with_files: BTreeSet<_> = BTreeSet::new();
            for test_file in &files {
                if matches!(&test_file.kind, TestFileKind::Ignored) {
                    continue;
                }
                let file = &test_file.path;
                let dir = Utf8Path::new(
                    file.parent().expect("test file has parent directory"),
                );
                if dirs_with_files.contains(dir) {
                    fail_file = Some(file);
                    break;
                }

                dirs_with_files.insert(dir);
            }
        };
        let Some(fail_file) = fail_file else {
            panic!(
                "test data had no directory with multiple non-ignored files"
            );
        };

        // Begin a simulated archive.  Configure the lister to inject an error
        // on the path that we selected above.
        let fake_output_dir = Utf8Path::new("/fake-output-directory");
        let mut lister = TestLister::new_for_test_data(&files);
        lister.inject_error(fail_file);
        let plan = test_archive(
            log,
            &files,
            fake_output_dir,
            ArchiveWhat::Everything,
            &lister,
        );

        // Run through the archive plan and verify:
        //
        // (1) We get exactly one error and it's for the path we injected an
        //     error for.
        // (2) That file does not get archived.
        // (2) Every other file gets archived.
        let mut unarchived_files = files.clone();
        let mut nerrors = 0;
        for step in plan.to_steps() {
            let step = match step {
                Err(error) => {
                    let error = InlineErrorChain::new(&*error);
                    let error_str = error.to_string();
                    debug!(log, "found error"; error);
                    assert!(error_str.contains(fail_file.as_str()));
                    assert!(error_str.contains("injected error"));
                    nerrors += 1;
                    continue;
                }
                Ok(step) => step,
            };

            let ArchiveStep::ArchiveFile(ArchiveFile { input_path, .. }) =
                &step
            else {
                continue;
            };

            assert!(
                input_path != fail_file,
                "unexpectedly archived file for which we injected an error"
            );

            let _ = unarchived_files
                .remove(input_path.as_path())
                .expect("archived file was in list of test files");
        }

        // There should be exactly one error.
        assert_eq!(
            nerrors, 1,
            "expected exatcly one error after injecting only one error \
             on a file path",
        );

        // There should be exactly one file that was not archived.
        assert_eq!(unarchived_files.len(), 1);
        assert!(unarchived_files.contains_key(fail_file.as_path()));

        logctx.cleanup_successful();
    }

    #[test]
    fn test_naming_logs() {
        // template used for other tests
        let template = ArchiveFile {
            input_path: Utf8Path::new("/nonexistent/one/two.log.0").to_owned(),
            mtime: Some("2025-12-12T16:51:00-07:00".parse().unwrap()),
            output_directory: Utf8Path::new("/nonexistent/out").to_owned(),
            namer: &NameRotatedLogFile,
            delete_original: true,
            rule: "dummy rule",
        };

        let empty_lister = TestLister::empty();

        // ordinary case of a rotated log file name: output filename generated
        // based on input and mtime
        let input = ArchiveFile {
            input_path: Utf8Path::new("/nonexistent/one/two.log.0").to_owned(),
            ..template.clone()
        };
        let filename = input.choose_filename(&empty_lister).unwrap();
        assert_eq!(filename.as_ref(), "two.log.1765583460");

        // ordinary case with a live log file name
        let input = ArchiveFile {
            input_path: Utf8Path::new("/nonexistent/one/two.log").to_owned(),
            ..template.clone()
        };
        let filename = input.choose_filename(&empty_lister).unwrap();
        assert_eq!(filename.as_ref(), "two.1765583460");

        // case: rotated log file, no mtime available
        // (this may never happen in practice)
        //
        // The current mtime should be used instead.
        let input = ArchiveFile {
            input_path: Utf8Path::new("/nonexistent/one/two.log.0").to_owned(),
            mtime: None,
            ..template.clone()
        };
        let before = Utc::now().with_nanosecond(0).unwrap();
        let filename = input.choose_filename(&empty_lister).unwrap();
        let after = Utc::now();
        assert!(before <= after);
        // The resulting filename should be "two.log.MTIME".
        let (prefix, mtime) =
            filename.as_ref().rsplit_once(".").expect("unexpected filename");
        assert_eq!(prefix, "two.log");
        let parsed: DateTime<Utc> = DateTime::from_timestamp(
            mtime.parse().expect("expected Unix timestamp in filename"),
            0,
        )
        .unwrap();
        println!("dap: {before} {parsed} {after}"); // XXX-dap
        assert!(before <= parsed);
        assert!(parsed <= after);

        // case: live log file, no mtime available
        // (this may never happen in practice)
        //
        // The current mtime should be used instead.
        let input = ArchiveFile {
            input_path: Utf8Path::new("/nonexistent/one/two.log").to_owned(),
            mtime: None,
            ..template.clone()
        };
        let before = Utc::now().with_nanosecond(0).unwrap();
        let filename = input.choose_filename(&empty_lister).unwrap();
        let after = Utc::now();
        assert!(before <= after);
        // The resulting filename should be "two.MTIME".
        let (prefix, mtime) =
            filename.as_ref().rsplit_once(".").expect("unexpected filename");
        assert_eq!(prefix, "two");
        let parsed: DateTime<Utc> = DateTime::from_timestamp(
            mtime.parse().expect("expected Unix timestamp in filename"),
            0,
        )
        .unwrap();
        assert!(before <= parsed);
        assert!(parsed <= after);

        // case: the normal output filename already exists
        // expected behavior: the "mtime" in the filename is incremented
        let input = ArchiveFile {
            input_path: Utf8Path::new("/nonexistent/one/two.log.0").to_owned(),
            ..template.clone()
        };
        let lister = TestLister::new(["/nonexistent/out/two.log.1765583460"]);
        let filename = input.choose_filename(&lister).unwrap();
        assert_eq!(filename.as_ref(), "two.log.1765583461");

        // case: several closely-named output filenames also exist
        let lister = TestLister::new([
            "/nonexistent/out/two.log.1765583460",
            "/nonexistent/out/two.log.1765583461",
            "/nonexistent/out/two.log.1765583462",
            "/nonexistent/out/two.log.1765583464",
        ]);
        let filename = input.choose_filename(&lister).unwrap();
        assert_eq!(filename.as_ref(), "two.log.1765583463");

        // case: too many closely-named output files also exist
        let colliding_filenames: Vec<_> = (0..=MAX_COLLIDING_FILENAMES)
            .map(|i| {
                format!(
                    "/nonexistent/out/two.log.{}",
                    1765583460u64 + u64::from(i)
                )
            })
            .collect();
        let lister = TestLister::new(colliding_filenames.iter());
        let error = input.choose_filename(&lister).unwrap_err();
        assert!(
            error.to_string().contains("too many files with colliding names")
        );
    }
}
