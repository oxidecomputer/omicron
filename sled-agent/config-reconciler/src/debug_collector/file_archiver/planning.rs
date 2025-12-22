// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration and implementation for archiving ordinary files as debug data
//! (e.g., log files)
//!
//! This system is designed so that as much possible is incorporated into the
//! plan so that it can be tested in simulation without extensive dependency
//! injection.  See also [the plan-execute
//! pattern](https://mmapped.blog/posts/29-plan-execute).

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
                    .map(|entry| match entry {
                        // Errors are passed to the end of this pipeline.
                        Err(error) => Err(error),

                        // If we found a matching file, fetch its metadata and
                        // grab the mtime.  This is used for naming the archived
                        // file.
                        Ok((group, filename)) => {
                            let input_path =
                                group.input_directory().join(filename.as_ref());
                            lister
                                .file_mtime(&input_path)
                                .map(|mtime| (group, input_path, mtime))
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
                                namer: group.rule.naming,
                                delete_original: group.rule.delete_original,
                                #[cfg(test)]
                                rule: group.rule.label,
                            }))
                        }
                    }),
            )
    }

    pub(crate) async fn execute(self) -> Vec<anyhow::Error> {
        let mut errors = Vec::new();
        let log = &self.log;
        let groups = self.groups;
        let debug_dir = self.debug_dir;
        let lister = self.lister;
        for step in Self::to_steps_generic(log, &groups, &debug_dir, lister) {
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
                errors.push(error);
            }
        }

        errors
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

#[cfg(test)]
mod test {
    use crate::debug_collector::file_archiver;
    use camino::Utf8Path;
    use chrono::DateTime;
    use chrono::Timelike;
    use chrono::Utc;
    use file_archiver::planning::ArchiveFile;
    use file_archiver::planning::ArchiveKind;
    use file_archiver::planning::ArchiveStep;
    use file_archiver::rules::ALL_RULES;
    use file_archiver::rules::MAX_COLLIDING_FILENAMES;
    use file_archiver::rules::NameRotatedLogFile;
    use file_archiver::test_helpers::*;
    use omicron_test_utils::dev::test_setup_log;
    use slog::debug;
    use slog::info;
    use slog_error_chain::InlineErrorChain;
    use std::collections::BTreeSet;

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
            ArchiveKind::Final,
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
            ArchiveKind::Periodic,
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
            ArchiveKind::Final,
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

            let last_listed = lister.last_listed();
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
            ArchiveKind::Final,
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
            ArchiveKind::Final,
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
