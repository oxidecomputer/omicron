// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Schema management utilities

use anyhow::{Context, Result};
use clap::Args;
use nexus_db_model::KNOWN_VERSIONS;
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

    // Get the previous schema version from KNOWN_VERSIONS
    // KNOWN_VERSIONS is in reverse order (newest first), so index 1 is the previous version
    if KNOWN_VERSIONS.len() < 2 {
        anyhow::bail!(
            "Need at least 2 known versions to find previous version. Current count: {}",
            KNOWN_VERSIONS.len()
        );
    }

    let previous_version = KNOWN_VERSIONS[1].semver();
    println!("Previous version from KNOWN_VERSIONS: {}", previous_version);

    // Find the git commit where this version was the current SCHEMA_VERSION
    let commit_hash =
        find_commit_for_schema_version(&workspace_root, previous_version)?;
    println!(
        "Found commit {} for version {}",
        &commit_hash[..8],
        previous_version
    );

    // Get the dbinit.sql from this commit
    let dbinit_content = get_dbinit_from_commit(&workspace_root, &commit_hash)?;

    // Save it to the previous-schema directory
    let version_dir = previous_schema_dir.join(&previous_version.to_string());
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
    Ok(())
}

fn find_commit_for_schema_version(
    workspace_root: &PathBuf,
    target_version: &semver::Version,
) -> Result<String> {
    // Search through git history to find the commit where this version was current
    let git_log_output = Command::new("git")
        .current_dir(workspace_root)
        .args([
            "log",
            "--oneline",
            "--format=%H",
            "nexus/db-model/src/schema_versions.rs",
        ])
        .output()
        .context("Failed to run git log")?;

    if !git_log_output.status.success() {
        let stderr = String::from_utf8_lossy(&git_log_output.stderr);
        anyhow::bail!("Git log command failed: {}", stderr);
    }

    let commits = String::from_utf8(git_log_output.stdout)?;

    for commit_hash in commits.lines() {
        let commit_hash = commit_hash.trim();
        if commit_hash.is_empty() {
            continue;
        }

        if let Some(version) =
            get_schema_version_from_commit(workspace_root, commit_hash)?
        {
            if version == *target_version {
                return Ok(commit_hash.to_string());
            }
        }
    }

    anyhow::bail!(
        "Could not find git commit for schema version {}",
        target_version
    )
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
) -> Result<String> {
    let show_output = Command::new("git")
        .current_dir(workspace_root)
        .args(["show", &format!("{}:schema/crdb/dbinit.sql", commit_hash)])
        .output()
        .context("Failed to run git show for dbinit.sql")?;

    if !show_output.status.success() {
        anyhow::bail!("dbinit.sql not found in commit");
    }

    Ok(String::from_utf8(show_output.stdout)?)
}
