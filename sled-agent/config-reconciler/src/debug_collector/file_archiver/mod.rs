// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration and implementation for archiving ordinary files as debug data
//! (e.g., log files)

// XXX-dap current status:
// - fix up imports
// - try to move tests into submodules and fix up visibility
// - clean up XXX-daps
// - self-review and clean up

use derive_more::AsRef;
use thiserror::Error;

mod execution;
mod filesystem;
mod planning;
mod rules;
#[cfg(test)]
mod test_helpers;

pub use planning::ArchiveKind;
pub use planning::ArchivePlanner;

struct ErrorAccumulator {
    errors: Vec<anyhow::Error>,
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

#[cfg(test)]
mod test {
    use super::test_helpers::*;

    use super::Filename;
    use super::planning::ArchiveFile;
    use super::planning::ArchiveKind;
    use super::planning::ArchivePlanner;
    use super::planning::ArchiveStep;
    use super::rules::ALL_RULES;
    use super::rules::MAX_COLLIDING_FILENAMES;
    use super::rules::NameRotatedLogFile;
    use anyhow::Context;
    use camino::Utf8Path;
    use camino_tempfile::Utf8TempDir;
    use chrono::DateTime;
    use chrono::Timelike;
    use chrono::Utc;
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

    #[test]
    fn test_filename() {
        assert_eq!(
            Filename::try_from(String::from("foo")).unwrap().as_ref(),
            "foo"
        );
        assert!(Filename::try_from(String::from(".")).is_err());
        assert!(Filename::try_from(String::from("..")).is_err());
        assert!(Filename::try_from(String::from("foo/bar")).is_err());
        assert!(Filename::try_from(String::from("foo/")).is_err());
        assert!(Filename::try_from(String::from("/bar")).is_err());
    }

    #[tokio::test]
    async fn test_real_archival() {
        // Set up the test.
        let logctx = test_setup_log("test_archiving_basic");
        let log = &logctx.log;

        // Create a temporary directory in which to store some output files.
        let tempdir = Utf8TempDir::new().unwrap();
        info!(log, "temporary directory"; "tempdir" => %tempdir.path());

        // Populate it with a couple of files.
        //
        // Note that all of the interesting cases around generating archive
        // steps are covered elsewhere.  We really only need to smoke check
        // basic behavior here.
        let outdir = tempdir.path().join("out");
        let zone_name = "an-example-zone";
        let zone_root = tempdir.path().join(zone_name);
        let logdir = zone_root.join("var/svc/log");
        let file1_live = logdir.join("svc1.log");
        let file2_rotated = logdir.join("svc1.log.0");
        let file3_rotated = logdir.join("svc2.log.0");
        let coredir = tempdir.path().join("crash");
        let file4_core = coredir.join("core.123");

        let populate_input = |contents: &str| {
            std::fs::create_dir_all(&logdir).unwrap();
            std::fs::create_dir_all(&coredir).unwrap();
            for file in
                [&file1_live, &file2_rotated, &file3_rotated, &file4_core]
            {
                let contents =
                    format!("{}-{contents}", file.file_name().unwrap());
                std::fs::write(&file, contents).unwrap();
            }
        };

        populate_input("first");

        // Compute the expected filenames.  These depend on the mtimes that the
        // files wound up with.
        let expected_filename = |base: &str, input: &Utf8Path| {
            let found_mtime = input.metadata().unwrap().modified().unwrap();
            let mtime: DateTime<Utc> = DateTime::from(found_mtime);
            format!("{base}{}", mtime.timestamp())
        };
        let file1_expected = expected_filename("svc1.", &file1_live);
        let file2_expected = expected_filename("svc1.log.", &file2_rotated);
        let file3_expected = expected_filename("svc2.log.", &file3_rotated);

        // Run a complete archive.
        std::fs::create_dir(&outdir).unwrap();
        let mut planner = ArchivePlanner::new(log, ArchiveKind::Final, &outdir);
        planner.include_cores_directory(&coredir);
        planner.include_zone(zone_name, &zone_root);
        let () = planner.execute().await.expect("successful execution");

        // Check each of the output log files.  This is a little annoying
        // because we don't necessarily know what names they were given, since
        // it depends on the mtime on the input file.
        let verify_logs = |unchanged| {
            for (input_path, expected_filename, deleted_original) in [
                (&file1_live, &file1_expected, false),
                (&file2_rotated, &file2_expected, true),
                (&file3_rotated, &file3_expected, true),
            ] {
                let expected_path =
                    outdir.join(zone_name).join(expected_filename);
                let contents = std::fs::read_to_string(&expected_path)
                    .with_context(|| {
                        format!("read expected output file {expected_path:?}")
                    })
                    .unwrap();
                assert!(contents.starts_with(input_path.file_name().unwrap()));
                assert!(contents.ends_with("-first"));

                if deleted_original {
                    // Check that the original file is gone.
                    assert!(!input_path.exists());
                } else {
                    // The input file should exist.  It may or may not match
                    // what it originally did, depending on what the caller
                    // says.
                    let input_contents = std::fs::read_to_string(&input_path)
                        .with_context(|| {
                            format!("read expected intput file {input_path:?}")
                        })
                        .unwrap();
                    if unchanged {
                        assert_eq!(contents, input_contents);
                    }
                }
            }
        };

        verify_logs(true);

        // Check the output core file, too.
        let file4_output = outdir.join("core.123");
        let contents = std::fs::read_to_string(&file4_output)
            .with_context(|| {
                format!("read expected output file {file4_output:?}")
            })
            .unwrap();
        assert_eq!(contents, "core.123-first");
        assert!(!file4_core.exists());

        // Now, check the behavior for file collisions.
        //
        // First, re-populate the input tree, but with new data so that we can
        // tell when things have been clobbered.
        populate_input("second");

        // Run another archive.
        let mut planner = ArchivePlanner::new(log, ArchiveKind::Final, &outdir);
        planner.include_cores_directory(&coredir);
        planner.include_zone(zone_name, &zone_root);
        let () = planner.execute().await.expect("successful execution");

        // The previously archived log file should still exist, still have the
        // same (original) contents, and the input files should be gone again.
        verify_logs(false);

        // There should now be new versions of the three log files that contain
        // the new contents.
        for result in outdir.join(zone_name).read_dir_utf8().unwrap() {
            let entry = result.unwrap();
            let contents = std::fs::read_to_string(&entry.path())
                .with_context(|| {
                    format!("read expected intput file {:?}", entry.path())
                })
                .unwrap();

            if entry.file_name() == &file1_expected
                || entry.file_name() == &file2_expected
                || entry.file_name() == &file3_expected
            {
                assert!(contents.ends_with("-first"));
            } else {
                assert!(contents.ends_with("-second"));
            }
        }

        // The core file should have been completely overwritten with new
        // contents.
        assert!(!file4_core.exists());
        let contents = std::fs::read_to_string(&file4_output)
            .with_context(|| {
                format!("read expected output file {file4_output:?}")
            })
            .unwrap();
        assert_eq!(contents, "core.123-second");

        logctx.cleanup_successful();
    }
}
