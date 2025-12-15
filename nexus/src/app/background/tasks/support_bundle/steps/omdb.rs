// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collects output from omdb commands

use crate::app::background::tasks::support_bundle::collection::BundleCollection;
use crate::app::background::tasks::support_bundle::step::CollectionStepOutput;
use camino::Utf8Path;
use tokio::process::Command;

pub async fn collect(
    collection: &BundleCollection,
    dir: &Utf8Path,
) -> anyhow::Result<CollectionStepOutput> {
    // Create the omdb/nexus/background-tasks directory
    let omdb_dir = dir.join("omdb/nexus/background-tasks");
    tokio::fs::create_dir_all(&omdb_dir).await?;

    // Run the omdb command
    let omdb_path = &collection.omdb_config().bin_path;
    let output = Command::new(omdb_path)
        .arg("nexus")
        .arg("background-tasks")
        .arg("list")
        .output()
        .await?;

    // Write the output to list.txt
    let output_path = omdb_dir.join("list.txt");
    let output_text = if output.status.success() {
        String::from_utf8_lossy(&output.stdout).to_string()
    } else {
        // If the command failed, include both stdout and stderr
        format!(
            "Command failed with exit code: {:?}\n\nSTDOUT:\n{}\n\nSTDERR:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    };

    tokio::fs::write(output_path, output_text).await?;

    Ok(CollectionStepOutput::None)
}
