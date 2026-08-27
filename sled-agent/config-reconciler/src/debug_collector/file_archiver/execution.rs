// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Execution of file archival
//!
//! As much as possible, behavior should **not** live here, but in the planning
//! module instead so that it can be tested without touching the filesystem.

use super::filesystem::FileLister;
use super::planning::ArchiveStep;
use anyhow::Context;
use camino::Utf8Path;
use slog::debug;

pub(crate) async fn execute_archive_step<'a>(
    log: &slog::Logger,
    step: ArchiveStep<'a>,
    lister: &'a (dyn FileLister + Send + Sync),
) -> Result<(), anyhow::Error> {
    match step {
        ArchiveStep::Mkdir { output_directory } => {
            // We assume that the parent of all output directories
            // already exists.  That's because in practice, either:
            //
            // 1. the output directory is directly inside the debug dataset
            //    itself, or
            // 2. the parent of this output directory was created by a previous
            //    `Mkdir` step
            //
            // The test suite verifies this.  So if we find at runtime that this
            // isn't true, that's a bad sign.  Maybe somebody has unmounted the
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
                    if error.kind() == std::io::ErrorKind::AlreadyExists {
                        Ok(())
                    } else {
                        Err(error)
                    }
                })
                .with_context(|| format!("mkdir {output_directory:?}"))
        }
        ArchiveStep::ArchiveFile(archive_file) => {
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
                        format!("archive {input_path:?} to {output_path:?}")
                    })
                }
            }
        }
    }
}

async fn archive_one(
    source: &Utf8Path,
    dest: &Utf8Path,
    delete_original: bool,
) -> tokio::io::Result<()> {
    let mut src_f = tokio::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&source)
        .await?;
    let mut dest_f = tokio::fs::File::create(&dest).await?;

    tokio::io::copy(&mut src_f, &mut dest_f).await?;

    dest_f.sync_all().await?;
    if let Some(parent) = dest.parent() {
        let file = tokio::fs::File::open(&parent).await?;
        file.sync_all().await?;
    }

    drop(src_f);
    drop(dest_f);

    if delete_original {
        tokio::fs::remove_file(source).await?;
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::debug_collector::file_archiver;
    use anyhow::Context;
    use camino::Utf8Path;
    use chrono::DateTime;
    use chrono::Utc;
    use file_archiver::planning::ArchiveKind;
    use file_archiver::planning::ArchivePlanner;
    use filetime::FileTime;
    use omicron_test_utils::dev::TestTempDir;
    use omicron_test_utils::dev::test_setup_log;
    use slog::info;

    /// Fixed mtime assigned to all of this test's input files
    const FIXED_MTIME: FileTime = FileTime::from_unix_time(1_700_000_000, 0);

    #[tokio::test]
    async fn test_real_archival() {
        // Set up the test.
        let logctx = test_setup_log("test_archiving_basic");
        let log = &logctx.log;

        // Create a temporary directory in which to store some output files.
        let tempdir = TestTempDir::new(log);
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
        let producer_dir =
            zone_root.join("var/debug_dropbox").join("test-producer");
        let file5_dropbox = producer_dir.join("test-file.dat");
        // A dropbox deposit that's still being staged.  This must never be
        // archived nor deleted.
        let staging_dir = zone_root
            .join(format!(
                "var/debug_dropbox/{}",
                omicron_debug_dropbox::RESERVED_PRODUCER_NAME
            ))
            .join("test-producer");
        let file6_staged = staging_dir.join("partial.dat");

        let populate_input = |contents: &str| {
            std::fs::create_dir_all(&logdir).unwrap();
            std::fs::create_dir_all(&coredir).unwrap();
            std::fs::create_dir_all(&producer_dir).unwrap();
            std::fs::create_dir_all(&staging_dir).unwrap();
            for file in [
                &file1_live,
                &file2_rotated,
                &file3_rotated,
                &file4_core,
                &file5_dropbox,
                &file6_staged,
            ] {
                let contents =
                    format!("{}-{contents}", file.file_name().unwrap());
                std::fs::write(&file, contents).unwrap();
                // The naming rules derive output filenames from the input
                // file's mtime and disambiguate collisions with a counter.  Pin
                // the mtime so that a file rewritten later in this test lands
                // on the same timestamp as before and so exercises the
                // collision path deterministically.
                filetime::set_file_mtime(file, FIXED_MTIME).unwrap();
            }
        };

        populate_input("first");

        // Compute the expected filenames.  These depend on the mtimes that the
        // files wound up with.
        let expected_log_filename = |base: &str, input: &Utf8Path| {
            let found_mtime = input.metadata().unwrap().modified().unwrap();
            let mtime: DateTime<Utc> = DateTime::from(found_mtime);
            format!("{base}{}", mtime.timestamp())
        };
        let file1_expected = expected_log_filename("svc1.", &file1_live);
        let file2_expected = expected_log_filename("svc1.log.", &file2_rotated);
        let file3_expected = expected_log_filename("svc2.log.", &file3_rotated);

        let expected_dropbox_filename = |input: &Utf8Path, counter: u16| {
            let found_mtime = input.metadata().unwrap().modified().unwrap();
            let mtime: DateTime<Utc> = DateTime::from(found_mtime);
            format!("test-file.dat.{}.{counter}", mtime.timestamp())
        };
        let file5_expected = expected_dropbox_filename(&file5_dropbox, 0);

        // Run a complete archive.
        std::fs::create_dir(&outdir).unwrap();
        let mut planner = ArchivePlanner::new(log, ArchiveKind::Final, &outdir);
        planner.include_cores_directory(&coredir);
        planner.include_zone(zone_name, &zone_root);
        let () = planner.execute().await.expect("successful execution");

        // Check each of the output log files.
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

        // Check the dropbox file at the expected nested output path.
        let verify_dropbox = |expected_name: &str, expected_contents: &str| {
            let dropbox_outdir = outdir.join(zone_name).join("debug_dropbox");
            let expected_path =
                dropbox_outdir.join("test-producer").join(expected_name);
            let contents = std::fs::read_to_string(&expected_path)
                .with_context(|| {
                    format!("read expected output file {expected_path:?}")
                })
                .unwrap();
            assert_eq!(contents, expected_contents);
            // The original input file should be gone (delete_original: true).
            assert!(!file5_dropbox.exists());

            // The still-being-staged deposit should have been left alone
            // entirely: neither archived nor deleted.
            assert!(
                file6_staged.exists(),
                "archiver removed a deposit that was still being staged",
            );
            assert!(
                !dropbox_outdir
                    .join(omicron_debug_dropbox::RESERVED_PRODUCER_NAME)
                    .exists(),
                "archiver created an output directory for the dropbox's \
                 staging directory",
            );
        };

        verify_logs(true);
        verify_dropbox(&file5_expected, "test-file.dat-first");

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
        let file5_expected_second =
            expected_dropbox_filename(&file5_dropbox, 1);

        // Run another archive.
        let mut planner = ArchivePlanner::new(log, ArchiveKind::Final, &outdir);
        planner.include_cores_directory(&coredir);
        planner.include_zone(zone_name, &zone_root);
        let () = planner.execute().await.expect("successful execution");

        // The previously archived log file should still exist, still have the
        // same (original) contents, and the input files should be gone again.
        verify_logs(false);
        verify_dropbox(&file5_expected_second, "test-file.dat-second");

        // There should now be new versions of the three log files that contain
        // the new contents.  Directories (e.g., "debug_dropbox") are skipped.
        for result in outdir.join(zone_name).read_dir_utf8().unwrap() {
            let entry = result.unwrap();
            if entry.file_type().unwrap().is_dir() {
                // There are directories here (like `debug_dropbox`), but we're
                // only interested in files.
                continue;
            }
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

        tempdir.cleanup_successful();
        logctx.cleanup_successful();
    }

    /// Contents of files outside the zone tree that must never be archived
    const CANARY_CONTENTS: &str = "canary-contents-must-never-be-archived";

    /// Verifies that directory entries whose type does not match what a rule
    /// expects are neither archived nor removed, nor do they cause archiving to
    /// fail
    ///
    /// A critical case is a symlink where a rule expects a regular file: if the
    /// archiver followed it, it would copy the target's contents into the debug
    /// dataset (which gets shipped off-sled in support bundles) and then unlink
    /// the symlink rather than the file whose contents it copied.  These cases
    /// cannot be expressed with `TestLister`, which models the test data as
    /// paths with no type information, so they are tested here against a real
    /// filesystem.
    #[tokio::test]
    async fn test_entry_type_confusion() {
        let logctx = test_setup_log("test_entry_type_confusion");
        let log = &logctx.log;

        let tempdir = TestTempDir::new(log);
        info!(log, "temporary directory"; "tempdir" => %tempdir.path());

        // The input tree, relative to the temporary directory, looks like
        // this.  "->" marks a symlink; the targets are absolute paths.
        //
        //     outside-dir/                      outside the zone tree
        //         canary.txt
        //     an-example-zone/                  zone root
        //         var/svc/log/
        //             svc.log.0 -> canary.txt
        //         var/debug_dropbox/
        //             stray.dat
        //             producer-link -> outside-dir
        //             producer-a/
        //                 real.dat              the one file to be archived
        //                 link.dat -> canary.txt
        //                 subdir/
        //                     nested.dat
        //
        // Despite the dropbox and logs directories containing symlinks that
        // point at paths under "outside-dir", nothing under `outside-dir`
        // should ever be read or copied into the output directory because
        // symlinks should not be followed.
        let outdir = tempdir.path().join("out");
        let zone_name = "an-example-zone";
        let zone_root = tempdir.path().join(zone_name);
        let dropbox = zone_root.join("var/debug_dropbox");
        let producer_dir = dropbox.join("producer-a");

        let outside_dir = tempdir.path().join("outside-dir");
        let canary = outside_dir.join("canary.txt");

        // Define some paths:
        //
        // - a real deposit that must be archived
        let real_file = producer_dir.join("real.dat");
        // - a directory where we expect to find a regular file, plus a file
        //   nested below it
        let unexpected_subdir = producer_dir.join("subdir");
        let too_deep_file = unexpected_subdir.join("nested.dat");
        // - a regular file in a path where we expect to find a directory
        let stray_file = dropbox.join("stray.dat");

        for (path, contents) in [
            (&canary, CANARY_CONTENTS),
            (&real_file, "real-contents"),
            (&too_deep_file, "nested-contents"),
            (&stray_file, "stray-contents"),
        ] {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, contents).unwrap();
        }

        // Define some paths to symlinks:
        //
        // - a symlink where we expect a regular file.
        let file_symlink = producer_dir.join("link.dat");
        // - a symlink where we expect a directory.
        let producer_symlink = dropbox.join("producer-link");
        // - a symlink matching the rotated SMF log rule
        let log_symlink = zone_root.join("var/svc/log/svc.log.0");

        for (link, target) in [
            (&file_symlink, &canary),
            (&producer_symlink, &outside_dir),
            (&log_symlink, &canary),
        ] {
            std::fs::create_dir_all(link.parent().unwrap()).unwrap();
            std::os::unix::fs::symlink(target, link).unwrap();
            // Verify that the link really resolves to the intended target.
            assert_eq!(
                link.canonicalize_utf8().unwrap(),
                target.canonicalize_utf8().unwrap(),
                "symlink {link} does not resolve to {target}",
            );
        }

        // Now, run a complete archive.
        std::fs::create_dir(&outdir).unwrap();
        let mut planner = ArchivePlanner::new(log, ArchiveKind::Final, &outdir);
        planner.include_zone(zone_name, &zone_root);
        let () = planner.execute().await.expect("successful execution");

        // The real deposit should be the only thing archived from the producer
        // directory, and its original should be gone.
        let dropbox_outdir = outdir.join(zone_name).join("debug_dropbox");
        let mut archived: Vec<_> = dropbox_outdir
            .join("producer-a")
            .read_dir_utf8()
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_owned())
            .collect();
        archived.sort();
        assert_eq!(archived.len(), 1, "unexpected output files: {archived:?}");
        assert!(
            archived[0].starts_with("real.dat."),
            "unexpected output file: {:?}",
            archived[0],
        );
        assert!(!real_file.exists(), "real deposit was not deleted");

        // Each symlink should still be a symlink.  Checking this rather than
        // `exists()` (which follows symlinks) verifies that the link itself was
        // not unlinked.
        for path in [&file_symlink, &producer_symlink, &log_symlink] {
            let metadata = path.symlink_metadata().unwrap();
            assert!(metadata.is_symlink(), "no longer a symlink: {path}");
        }

        // Everything else the archiver declined to handle should be untouched.
        for path in [&canary, &stray_file, &unexpected_subdir, &too_deep_file] {
            assert!(path.exists(), "no longer exists: {path}");
        }
        assert_eq!(std::fs::read_to_string(&canary).unwrap(), CANARY_CONTENTS);

        // No output directory should have been created for an entry that isn't
        // a producer directory, nor for a subdirectory nested below one.
        for path in [
            dropbox_outdir.join("producer-link"),
            dropbox_outdir.join("stray.dat"),
            dropbox_outdir.join("producer-a").join("subdir"),
        ] {
            assert!(!path.exists(), "unexpected output path: {path}");
        }

        // The property that actually matters: nothing from outside the zone
        // tree wound up in the debug dataset.
        for entry in walkdir::WalkDir::new(&outdir) {
            let entry = entry.unwrap();
            if !entry.file_type().is_file() {
                continue;
            }
            let contents = std::fs::read_to_string(entry.path())
                .with_context(|| format!("read {:?}", entry.path()))
                .unwrap();
            assert_ne!(
                contents,
                CANARY_CONTENTS,
                "archiver copied a symlink's target into the debug dataset: \
                 {:?}",
                entry.path(),
            );
        }

        tempdir.cleanup_successful();
        logctx.cleanup_successful();
    }
}
