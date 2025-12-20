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
