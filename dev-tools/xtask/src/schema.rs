// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Schema management utilities

use anyhow::{Context, Result};
use clap::Args;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

#[derive(Args)]
pub struct SchemaArgs {
    #[command(subcommand)]
    cmd: SchemaCommands,
}

#[derive(clap::Subcommand)]
enum SchemaCommands {
    /// Generate previous schema file for testing migration from last version
    GeneratePrevious,
}

pub fn run_cmd(args: SchemaArgs) -> Result<()> {
    match args.cmd {
        SchemaCommands::GeneratePrevious => generate_previous_schema(),
    }
}

fn generate_previous_schema() -> Result<()> {
    println!("Generating previous schema file for migration testing");

    // Find the workspace root
    let workspace_root = find_workspace_root()?;

    // Clean and recreate the previous-schema directory
    let previous_schema_dir =
        workspace_root.join("schema/crdb/previous-schema");

    if previous_schema_dir.exists() {
        println!("Cleaning existing previous-schema directory");
        fs::remove_dir_all(&previous_schema_dir).with_context(|| {
            format!("Failed to remove directory: {:?}", previous_schema_dir)
        })?;
    }

    fs::create_dir_all(&previous_schema_dir).with_context(|| {
        format!("Failed to create directory: {:?}", previous_schema_dir)
    })?;

    // Find the previous schema version (second in git history)
    let git_log_output = Command::new("git")
        .current_dir(&workspace_root)
        .args([
            "log",
            "--oneline",
            "--format=%H %s",
            "-10", // Get recent commits
            "nexus/db-model/src/schema_versions.rs",
        ])
        .output()
        .context("Failed to run git log")?;

    if !git_log_output.status.success() {
        let stderr = String::from_utf8_lossy(&git_log_output.stderr);
        anyhow::bail!("Git log command failed: {}", stderr);
    }

    let commits = String::from_utf8(git_log_output.stdout)?;
    let mut found_versions = Vec::new();

    // Find the first two unique schema versions in git history
    for line in commits.lines() {
        let parts: Vec<&str> = line.splitn(2, ' ').collect();
        if parts.len() < 2 {
            continue;
        }

        let commit_hash = parts[0];
        let commit_message = parts[1];

        if let Some(version) =
            get_schema_version_from_commit(&workspace_root, commit_hash)?
        {
            // Skip if we already found this version (avoid duplicates)
            if !found_versions.iter().any(|(v, _, _)| v == &version) {
                found_versions.push((
                    version,
                    commit_hash.to_string(),
                    commit_message.to_string(),
                ));

                // Stop after finding 2 unique versions (current and previous)
                if found_versions.len() >= 2 {
                    break;
                }
            }
        }
    }

    if found_versions.len() < 2 {
        anyhow::bail!(
            "Could not find previous schema version. Need at least 2 schema versions in git history."
        );
    }

    // Use the second version found (previous version)
    let (previous_version, commit_hash, commit_message) = &found_versions[1];

    println!(
        "Processing previous version {} from commit {}: {}",
        previous_version,
        &commit_hash[..8],
        commit_message
    );

    // Get the dbinit.sql from this commit
    if let Some(dbinit_content) =
        get_dbinit_from_commit(&workspace_root, commit_hash)?
    {
        // Save it to the previous-schema directory
        let version_dir =
            previous_schema_dir.join(&previous_version.to_string());
        fs::create_dir_all(&version_dir).with_context(|| {
            format!("Failed to create version directory: {:?}", version_dir)
        })?;

        let dbinit_file = version_dir.join("dbinit.sql");
        fs::write(&dbinit_file, dbinit_content).with_context(|| {
            format!("Failed to write dbinit.sql to {:?}", dbinit_file)
        })?;

        println!(
            "Saved dbinit.sql for previous version {} in {:?}",
            previous_version, version_dir
        );
    } else {
        anyhow::bail!(
            "Could not find dbinit.sql for previous version {}",
            previous_version
        );
    }
    Ok(())
}

fn find_workspace_root() -> Result<PathBuf> {
    let current_dir =
        std::env::current_dir().context("Failed to get current directory")?;
    current_dir
        .ancestors()
        .find(|p| p.join("Cargo.toml").exists() && p.join(".git").exists())
        .map(|p| p.to_path_buf())
        .ok_or_else(|| anyhow::anyhow!("Could not find workspace root"))
}

fn get_schema_version_from_commit(
    workspace_root: &PathBuf,
    commit_hash: &str,
) -> Result<Option<semver::Version>> {
    let show_output = Command::new("git")
        .current_dir(workspace_root)
        .args([
            "show",
            &format!("{}:nexus/db-model/src/schema_versions.rs", commit_hash),
        ])
        .output()
        .context("Failed to run git show")?;

    if !show_output.status.success() {
        return Ok(None); // File might not exist in this commit
    }

    let file_content = String::from_utf8(show_output.stdout)?;

    // Parse the SCHEMA_VERSION from the file
    for line in file_content.lines() {
        if line.contains("pub const SCHEMA_VERSION")
            && line.contains("Version::new(")
        {
            if let Some(version_part) = line.split("Version::new(").nth(1) {
                if let Some(version_nums) = version_part.split(')').next() {
                    let parts: Vec<&str> = version_nums.split(',').collect();
                    if parts.len() >= 3 {
                        if let (Ok(major), Ok(minor), Ok(patch)) = (
                            parts[0].trim().parse::<u64>(),
                            parts[1].trim().parse::<u64>(),
                            parts[2].trim().parse::<u64>(),
                        ) {
                            return Ok(Some(semver::Version::new(
                                major, minor, patch,
                            )));
                        }
                    }
                }
            }
            break;
        }
    }

    Ok(None)
}

fn get_dbinit_from_commit(
    workspace_root: &PathBuf,
    commit_hash: &str,
) -> Result<Option<String>> {
    let show_output = Command::new("git")
        .current_dir(workspace_root)
        .args(["show", &format!("{}:schema/crdb/dbinit.sql", commit_hash)])
        .output()
        .context("Failed to run git show for dbinit.sql")?;

    if !show_output.status.success() {
        return Ok(None); // dbinit.sql might not exist in this commit
    }

    Ok(Some(String::from_utf8(show_output.stdout)?))
}
