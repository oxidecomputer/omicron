// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collects output from omdb commands

use crate::app::background::tasks::support_bundle::collection::BundleCollection;
use crate::app::background::tasks::support_bundle::step::CollectionStepOutput;
use camino::Utf8Path;
use tokio::process::Command;

/// Run an omdb command and write its output to a file within the bundle.
///
/// This function returns an error if we cannot write to our local filesystem,
/// or cannot run the omdb command at all. However, if the omdb command runs
/// and fails, it returns "Ok()".
///
/// # Arguments
/// * `collection` - The bundle collection context
/// * `dir` - The root directory of the bundle
/// * `args` - The arguments to pass to omdb (e.g., `&["nexus", "background-tasks", "list"]`)
/// * `output_path` - The relative path within the bundle where output should be written
///                   (e.g., `"omdb/nexus/background-tasks/list.txt"`)
async fn run_omdb(
    collection: &BundleCollection,
    dir: &Utf8Path,
    args: &[&str],
    output_path: &str,
) -> anyhow::Result<()> {
    let full_output_path = dir.join(output_path);

    // Create parent directories if they don't exist
    if let Some(parent) = full_output_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // Run the omdb command
    let omdb_path = &collection.omdb_config().bin_path;
    let output =
        Command::new(omdb_path).args(args).output().await.map_err(|e| {
            anyhow::anyhow!(
                "Failed to execute omdb at {:?} with args {:?}: {}",
                omdb_path,
                args,
                e
            )
        })?;

    // Format the output
    let output_text = if output.status.success() {
        String::from_utf8_lossy(&output.stdout).to_string()
    } else {
        // If the command failed, include both stdout and stderr
        format!(
            "Command {} failed with exit code: {:?}\n\nSTDOUT:\n{}\n\nSTDERR:\n{}",
            args.join(" "),
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    };

    tokio::fs::write(full_output_path, output_text).await?;
    Ok(())
}

/// Collect diagnostic output from various omdb commands.
///
/// This function runs multiple omdb queries and stores their output in the bundle.
/// To add more omdb queries, simply add another `run_omdb()` call with the
/// appropriate arguments and output path.
pub async fn collect(
    collection: &BundleCollection,
    dir: &Utf8Path,
) -> anyhow::Result<CollectionStepOutput> {
    // NOTE: We could parallelize these commands, if they take a while.
    //
    // NOTE: These commands issues queries to "some Nexus", as returned by DNS -
    // not necessarily our own Nexus. We may want to include queries to
    // each Nexus instance individually in a future iteration, especially for
    // "nexus-specific" commands.

    // Run a sequence of omdb commands. If any of these commands fail, we'll
    // save the stdout and stderr, and proceed to the next one (note that
    // "run_omdb" does not return an error when the output is not successfull).

    run_omdb(
        collection,
        dir,
        &["nexus", "background-tasks", "list"],
        "omdb/nexus/background-tasks/list.txt",
    )
    .await?;

    run_omdb(
        collection,
        dir,
        &["nexus", "quiesce", "show"],
        "omdb/nexus/quiesce/show.txt",
    )
    .await?;

    run_omdb(
        collection,
        dir,
        &["nexus", "mgs-updates"],
        "omdb/nexus/mgs-updates.txt",
    )
    .await?;

    run_omdb(
        collection,
        dir,
        &["nexus", "update-status"],
        "omdb/nexus/update-status.txt",
    )
    .await?;

    run_omdb(
        collection,
        dir,
        &["db", "saga", "running"],
        "omdb/db/saga/running",
    )
    .await?;

    Ok(CollectionStepOutput::None)
}
