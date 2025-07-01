// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Schema management utilities

use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Parser, Subcommand};
use nexus_db_model::KNOWN_VERSIONS;
use std::collections::HashSet;
use std::fs;
use std::process::Command;

#[derive(Parser)]
#[command(name = "schema", about = "Schema management utilities")]
struct Args {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate a base schema file from a specific git commit or tag
    GenerateBase {
        /// Git commit hash or tag name to use as the base
        commit_or_tag: String,
    },
    /// Identify old schema migrations that can be removed based on current base
    OldMigrations {
        /// Automatically remove orphaned migration directories
        #[arg(long)]
        remove: bool,
    },
}

fn main() -> Result<()> {
    let args = Args::parse();
    match args.cmd {
        Commands::GenerateBase { commit_or_tag } => {
            generate_base_schema(&commit_or_tag)
        }
        Commands::OldMigrations { remove } => manage_old_migrations(remove),
    }
}

fn find_workspace_root() -> Result<Utf8PathBuf> {
    let current_dir =
        std::env::current_dir().context("Failed to get current directory")?;
    let current_dir = Utf8PathBuf::try_from(current_dir)
        .context("Current directory path is not valid UTF-8")?;

    current_dir
        .ancestors()
        .find(|p| p.join("Cargo.toml").exists() && p.join(".git").exists())
        .map(|p| p.to_path_buf())
        .ok_or_else(|| anyhow::anyhow!("Could not find workspace root"))
}

fn get_schema_version_from_commit(
    workspace_root: &Utf8Path,
    commit_hash: &str,
) -> Result<Option<semver::Version>> {
    // Try the new location first (schema_versions.rs)
    let show_output = Command::new("git")
        .current_dir(workspace_root)
        .args([
            "show",
            &format!("{}:nexus/db-model/src/schema_versions.rs", commit_hash),
        ])
        .output()
        .context("Failed to run git show")?;

    let file_content = if show_output.status.success() {
        String::from_utf8(show_output.stdout)?
    } else {
        // Try the old location (schema.rs) for older commits
        let show_output_old = Command::new("git")
            .current_dir(workspace_root)
            .args([
                "show",
                &format!("{}:nexus/db-model/src/schema.rs", commit_hash),
            ])
            .output()
            .context("Failed to run git show for old schema.rs")?;

        if !show_output_old.status.success() {
            return Ok(None); // File doesn't exist in either location
        }

        String::from_utf8(show_output_old.stdout)?
    };

    // Parse the SCHEMA_VERSION from the file
    for line in file_content.lines() {
        if line.contains("pub const SCHEMA_VERSION")
            && (line.contains("Version::new(")
                || line.contains("SemverVersion::new("))
        {
            // Handle both old format (SemverVersion::new) and new format (Version::new)
            let version_part = if line.contains("Version::new(") {
                line.split("Version::new(").nth(1)
            } else {
                line.split("SemverVersion::new(").nth(1)
            };

            if let Some(version_part) = version_part {
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
    workspace_root: &Utf8Path,
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

fn generate_base_schema(commit_or_tag: &str) -> Result<()> {
    println!("Generating base schema from commit/tag: {}", commit_or_tag);

    let workspace_root = find_workspace_root()?;

    // Resolve the commit/tag to a full commit hash
    let commit_hash =
        resolve_commit_or_tag(workspace_root.as_ref(), commit_or_tag)?;
    println!("Resolved {} to commit {}", commit_or_tag, &commit_hash);

    // Get the dbinit.sql content from that commit
    let dbinit_content = get_dbinit_from_commit(&workspace_root, &commit_hash)?;

    // Get the schema version from that commit to include in the header
    let schema_version =
        get_schema_version_from_commit(&workspace_root, &commit_hash)?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Could not determine schema version from commit {}",
                    commit_hash
                )
            })?;

    // Create the base schema directory
    let base_schema_dir = workspace_root.join("schema/crdb");

    // Create the header comment with generation info
    let header_comment = format!(
        "-- Auto-generated base schema file (earliest supported SQL schema)\n\
         -- Generated from: {}\n\
         -- Commit: {}\n\
         -- Schema version: {}\n\
         -- Generated by: cargo xtask schema generate-base\n\
         -- DO NOT EDIT: This file will be overwritten\n\n",
        commit_or_tag, commit_hash, schema_version
    );

    // Write the base schema file
    let base_file = base_schema_dir.join("dbinit-base.sql");
    let final_content = header_comment + &dbinit_content;

    fs::write(&base_file, final_content).with_context(|| {
        format!("Failed to write base schema to {:?}", base_file)
    })?;

    println!("Generated base schema file: {:?}", base_file);
    println!("Base version: {}", schema_version);

    Ok(())
}

fn manage_old_migrations(remove: bool) -> Result<()> {
    println!("Identifying old migrations that can be removed");

    let workspace_root = find_workspace_root()?;
    let base_file = workspace_root.join("schema/crdb/dbinit-base.sql");

    // Check if base file exists
    if !base_file.exists() {
        anyhow::bail!(
            "Base schema file not found: {:?}\nRun 'cargo xtask schema generate-base' first",
            base_file
        );
    }

    // Read and parse the base file to get the version
    let base_content = fs::read_to_string(&base_file).with_context(|| {
        format!("Failed to read base schema file: {:?}", base_file)
    })?;

    let base_version = parse_base_schema_version(&base_content)?;
    println!("Base schema version: {}", base_version);

    // Find all migrations that are older than the base version (from KNOWN_VERSIONS)
    let mut old_migrations = Vec::new();
    for known_version in KNOWN_VERSIONS.iter() {
        if known_version.semver() < &base_version {
            old_migrations.push(known_version);
        }
    }

    // Find orphaned directories (exist in filesystem but not in KNOWN_VERSIONS)
    let schema_dir = workspace_root.join("schema/crdb");
    let mut orphaned_dirs = find_orphaned_migration_directories(&schema_dir)?;

    // Display results
    if !old_migrations.is_empty() {
        println!(
            "Found {} migrations in KNOWN_VERSIONS older than base version {}:",
            old_migrations.len(),
            base_version
        );
        println!(
            "These entries should be removed from nexus/db-model/src/schema_versions.rs:\n"
        );
        for migration in &old_migrations {
            println!(
                "  {} (schema/crdb/{})",
                migration.semver(),
                migration.relative_path()
            );
        }
        println!("");
    }

    if !orphaned_dirs.is_empty() {
        println!(
            "Found {} orphaned migration directories:",
            orphaned_dirs.len()
        );
        if remove {
            remove_orphaned_directories(&schema_dir, orphaned_dirs)?;
            orphaned_dirs = Vec::new();
        } else {
            println!(
                "These directories exist but have no corresponding KNOWN_VERSIONS entry:\n"
            );
            for dir_name in &orphaned_dirs {
                println!("  schema/crdb/{}/", dir_name);
            }
        }
        println!("");
    }

    if old_migrations.is_empty() && orphaned_dirs.is_empty() {
        println!("OK: No old migrations found");
    } else {
        println!("To clean up old migrations:");
        if !old_migrations.is_empty() {
            println!(
                "  - Remove the listed entries from nexus/db-model/src/schema_versions.rs"
            );
        }
        if !orphaned_dirs.is_empty() {
            println!(
                "  - Delete the orphaned directories (use --remove flag to do this automatically)"
            );
        }
        println!(
            "  - Update EARLIEST_SUPPORTED_VERSION in schema_versions.rs if needed"
        );
        println!("  - Run tests to ensure everything still works");

        anyhow::bail!("Old migration artifacts still exist");
    }

    Ok(())
}

fn remove_orphaned_directories(
    schema_dir: &Utf8Path,
    orphaned_dirs: Vec<String>,
) -> Result<()> {
    println!("Removing orphaned directories...\n");
    for dir_name in &orphaned_dirs {
        let dir_path = schema_dir.join(dir_name);
        match fs::remove_dir_all(&dir_path) {
            Ok(()) => {
                println!("  Removed {dir_path}");
            }
            Err(e) => {
                anyhow::bail!("  Failed to remove {dir_path}: {}", e);
            }
        }
    }
    Ok(())
}

fn find_orphaned_migration_directories(
    schema_dir: &Utf8Path,
) -> Result<Vec<String>> {
    let mut orphaned = Vec::new();

    // Get all directory names from KNOWN_VERSIONS
    let mut known_dirs = HashSet::new();
    for known_version in KNOWN_VERSIONS.iter() {
        known_dirs.insert(known_version.relative_path());
    }

    // Special files to ignore
    let ignore_files = HashSet::from([
        "dbinit.sql",
        "dbinit-base.sql",
        "dbwipe.sql",
        "README.adoc",
    ]);

    // Read the schema directory
    let entries = schema_dir.read_dir_utf8().with_context(|| {
        format!("Failed to read schema directory: {:?}", schema_dir)
    })?;

    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string();

        // Skip files (we only care about directories)
        if entry.file_type()?.is_file() {
            continue;
        }

        // Skip known files and directories
        //
        // These are already files, so this is a little redundant, but this
        // is just future-proofing
        if ignore_files.contains(name.as_str())
            || known_dirs.contains(name.as_str())
        {
            continue;
        }

        // This directory exists and isn't in KNOWN_VERSIONS
        orphaned.push(name);
    }

    orphaned.sort();
    Ok(orphaned)
}

// Given an input "commit" or "tag", returns a "commit".
//
// This helps normalize input arguments.
fn resolve_commit_or_tag(
    workspace_root: &Utf8Path,
    commit_or_tag: &str,
) -> Result<String> {
    let output = Command::new("git")
        .current_dir(workspace_root)
        .args(["rev-parse", commit_or_tag])
        .output()
        .context("Failed to run git rev-parse")?;

    if !output.status.success() {
        anyhow::bail!(
            "Failed to resolve commit/tag '{}': {}",
            commit_or_tag,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let commit_hash = String::from_utf8(output.stdout)?.trim().to_string();

    Ok(commit_hash)
}

fn parse_base_schema_version(content: &str) -> Result<semver::Version> {
    for line in content.lines() {
        if line.starts_with("-- Schema version:") {
            let version_str = line
                .strip_prefix("-- Schema version:")
                .ok_or_else(|| {
                    anyhow::anyhow!("Invalid schema version line format")
                })?
                .trim();
            return semver::Version::parse(version_str).with_context(|| {
                format!("Failed to parse schema version: {}", version_str)
            });
        }
    }
    anyhow::bail!("Could not find schema version in base file header")
}
