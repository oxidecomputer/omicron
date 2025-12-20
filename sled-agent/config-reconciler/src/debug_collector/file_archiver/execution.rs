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

#[cfg(test)]
mod test {
    use crate::debug_collector::file_archiver;
    use anyhow::Context;
    use camino::Utf8Path;
    use camino_tempfile::Utf8TempDir;
    use chrono::DateTime;
    use chrono::Utc;
    use file_archiver::planning::ArchiveKind;
    use file_archiver::planning::ArchivePlanner;
    use omicron_test_utils::dev::test_setup_log;
    use slog::info;

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
