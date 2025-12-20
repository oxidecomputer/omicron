// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration and implementation for archiving ordinary files as debug data
//! (e.g., log files)
//!
//! This system is designed so that as much possible is incorporated into the
//! plan so that it can be tested in simulation without extensive dependency
//! injection.  See also [https://mmapped.blog/posts/29-plan-execute](the
//! plan-execute pattern).

use super::ErrorAccumulator;
use super::execution::execute_archive_step;
use super::filesystem::FileLister;
use super::filesystem::Filename;
use super::filesystem::FilesystemLister;
use super::rules::ALL_RULES;
use super::rules::ArchiveGroup;
use super::rules::NamingRule;
use super::rules::RuleScope;
use super::rules::Source;
use anyhow::Context;
use anyhow::anyhow;
use camino::Utf8Path;
use camino::Utf8PathBuf;
use chrono::DateTime;
use chrono::Utc;
use slog::Logger;
use slog::debug;
use slog::o;
use slog::warn;
use slog_error_chain::InlineErrorChain;

/// Describes what kind of archive operation this is, which affects what debug
/// data to collect
#[derive(Debug, Clone, Copy)]
pub enum ArchiveKind {
    /// Periodic archive
    ///
    /// Periodic archives include immutable files like core files and rotated
    /// log files, but they ignore live log files since they're still being
    /// written-to.  Those will get picked up in a subsequent periodic archive
    /// (once rotated) or a final archive for this source.
    Periodic,

    /// Final archive for this source
    ///
    /// The final archive for a given source is our last chance to archive debug
    /// data from it.  It is also generally at rest (or close to it).  So this
    /// includes everything that a periodic archive includes *plus* live log
    /// files.
    Final,
}

/// Used to configure and execute a file archival operation
pub struct ArchivePlanner<'a> {
    log: Logger,
    what: ArchiveKind,
    debug_dir: Utf8PathBuf,
    groups: Vec<ArchiveGroup<'static>>,
    lister: &'a (dyn FileLister + Send + Sync),
    errors: ErrorAccumulator,
}

impl ArchivePlanner<'static> {
    /// Begin an archival operation that will store data into `debug_dir`
    pub fn new(
        log: &Logger,
        what: ArchiveKind,
        debug_dir: &Utf8Path,
    ) -> ArchivePlanner<'static> {
        Self::new_with_lister(log, what, debug_dir, &FilesystemLister)
    }
}

impl<'a> ArchivePlanner<'a> {
    // Used by the tests to inject a custom lister.
    pub(crate) fn new_with_lister(
        log: &Logger,
        what: ArchiveKind,
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

    /// Configure this archive operation to include debug data from the given
    /// illumos zone zone
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
                (RuleScope::ZoneMutable, ArchiveKind::Final) => true,
                (RuleScope::ZoneMutable, ArchiveKind::Periodic) => false,
                (RuleScope::CoresDirectory, _) => false,
            });

        for rule in rules {
            self.groups.push(ArchiveGroup { source: source.clone(), rule });
        }
    }

    /// Configure this archive operation to include debug data from the given
    /// cores directory
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

    /// Returns an `ArchivePlan` that describes more specific steps for
    /// archiving the requested debug data
    pub fn into_plan(self) -> ArchivePlan<'a> {
        ArchivePlan {
            log: self.log,
            groups: self.groups,
            debug_dir: self.debug_dir,
            lister: self.lister,
            errors: self.errors,
        }
    }

    /// Generates an `ArchivePlan` and immediately executes it, archiving the
    /// requested files
    ///
    /// Returns a single `anyhow::Error` if there are any problems archiving any
    /// files.  Details are emitted to the log.  (It's assumed that consumers
    /// don't generally care to act on detailed failures programmatically, just
    /// report them to the log.)
    pub async fn execute(self) -> Result<(), anyhow::Error> {
        if !self.into_plan().execute().await.is_empty() {
            Err(anyhow!("one or more archive steps failed (see logs)"))
        } else {
            Ok(())
        }
    }
}

/// Describes specific steps to carry out an archive operation
///
/// Constructed with [`ArchivePlanner`].
pub(crate) struct ArchivePlan<'a> {
    log: slog::Logger,
    debug_dir: Utf8PathBuf,
    groups: Vec<ArchiveGroup<'static>>,
    lister: &'a (dyn FileLister + Send + Sync),
    errors: ErrorAccumulator, // XXX-dap why is this stored
}

impl ArchivePlan<'_> {
    #[cfg(test)]
    pub(crate) fn to_steps(
        &self,
    ) -> impl Iterator<Item = Result<ArchiveStep<'_>, anyhow::Error>> {
        Self::to_steps_generic(
            &self.log,
            &self.groups,
            &self.debug_dir,
            self.lister,
        )
    }

    pub(crate) fn to_steps_generic<'a>(
        log: &Logger,
        groups: &'a [ArchiveGroup<'static>],
        debug_dir: &'a Utf8Path,
        lister: &'a (dyn FileLister + Send + Sync),
    ) -> impl Iterator<Item = Result<ArchiveStep<'a>, anyhow::Error>> {
        // This gigantic combinator iterates the list of archive steps, which
        // consist of:
        //
        // - an `ArchiveStep::Mkdir` for each output directory we need to create
        // - an `ArchiveStep::ArchiveFile` for each file that we need to archive
        //   (all files matching all the rules that have been applied to the
        //   input sources).
        //
        // In fact, each item that we iterate is a `Result`: it's either one of
        // these archive steps or its an error that was encountered along the
        // way.
        //
        // Being one big expression makes this annoying to read and modify, but
        // it has the useful property that it operates in a streaming way: at no
        // point are all of the files in all of the matching directories read
        // into memory at once.
        groups
            .iter()
            // Start with a `mkdir` for each of the output directories.
            .filter_map(move |group| {
                let output_directory = group.output_directory(debug_dir);
                if output_directory != debug_dir {
                    Some(Ok(ArchiveStep::Mkdir { output_directory }))
                } else {
                    None
                }
            })
            // Chain this with a list of all the files we need to archive.
            .chain(
                groups
                    .iter()
                    .flat_map(move |group| {
                        // Each group essentially identifies one directory that
                        // we need to scan for files to archive.  For each of
                        // these, list the files in the directory.
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
                        // Errors are passed to the end of this pipeline.
                        Err(_) => true,

                        // Files that we found in an input directory are checked
                        // against the corresponding rule to see if we should
                        // include them.
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
                        // Errors are passed to the end of this pipeline.
                        Err(error) => Some(Err(error)),

                        // If we found a matching file, fetch its metadata and
                        // grab the mtime.  This is used for naming the archived
                        // file.
                        Ok((group, filename)) => {
                            let input_path =
                                group.input_directory().join(filename.as_ref());
                            Some(
                                lister
                                    .file_mtime(&input_path)
                                    .map(|mtime| (group, input_path, mtime)),
                            )
                        }
                    })
                    .map(|entry| match entry {
                        // Errors are passed to the end of this pipeline.
                        Err(error) => Err(error),

                        // If we succeeded so far, we have a matching input
                        // file, its mtime and the associated group.  Construct
                        // an archive step describing that we need to archive
                        // this file.
                        Ok((group, input_path, mtime)) => {
                            let output_directory =
                                group.output_directory(debug_dir);
                            Ok(ArchiveStep::ArchiveFile(ArchiveFile {
                                input_path,
                                mtime,
                                output_directory,
                                namer: &*group.rule.naming,
                                delete_original: group.rule.delete_original,
                                #[cfg(test)]
                                rule: group.rule.label,
                            }))
                        }
                    }),
            )
    }

    pub(crate) async fn execute(self) -> Vec<anyhow::Error> {
        let mut errors = self.errors;
        let log = &self.log;
        let groups = self.groups;
        let debug_dir = self.debug_dir;
        let lister = self.lister;
        for step in Self::to_steps_generic(log, &groups, &debug_dir, &*lister) {
            let result = match step {
                Err(error) => Err(error),
                Ok(step) => execute_archive_step(log, step, lister).await,
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

pub(crate) enum ArchiveStep<'a> {
    Mkdir { output_directory: Utf8PathBuf },
    ArchiveFile(ArchiveFile<'a>),
}

#[derive(Clone)]
pub(crate) struct ArchiveFile<'a> {
    pub(crate) input_path: Utf8PathBuf,
    pub(crate) mtime: Option<DateTime<Utc>>,
    pub(crate) output_directory: Utf8PathBuf,
    pub(crate) namer: &'a (dyn NamingRule + Send + Sync),
    pub(crate) delete_original: bool,
    #[cfg(test)]
    pub(crate) rule: &'static str,
}

impl ArchiveFile<'_> {
    pub(crate) fn choose_filename(
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
