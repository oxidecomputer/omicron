// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask setup-agents
//!
//! Materializes per-directory agent instruction files. For every directory
//! that has a README, this creates `CLAUDE.md` and `AGENTS.md` symlinks
//! pointing at that README, so coding agents pick up the same directory-scoped
//! guidance humans read (Claude Code and other agents load these files on
//! demand when they read files in the directory).
//!
//! The root `CLAUDE.md`/`AGENTS.md` are committed and gitignored everywhere
//! else: the root files hold the repo map and tell agents to run this command,
//! and the nested symlinks it generates are throwaway, per-checkout artifacts.
//!
//! The command is idempotent and never clobbers a hand-authored (regular)
//! `CLAUDE.md`/`AGENTS.md`, so directories with bespoke guidance are left
//! untouched.

use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use fs_err as fs;
use std::collections::BTreeMap;
use std::io::{self, IsTerminal, Write};
use std::os::unix::fs::symlink;
use std::process::Command;

#[derive(Parser)]
pub struct Args {
    /// Report what would change without modifying the filesystem.
    #[clap(long)]
    dry_run: bool,

    /// Remove orphaned symlinks without prompting (for non-interactive use).
    #[clap(long)]
    force: bool,
}

/// Agent instruction filenames we generate, each linked to the directory's
/// README.
const AGENT_FILES: &[&str] = &["CLAUDE.md", "AGENTS.md"];

/// README filenames to link to, in order of preference.
const README_NAMES: &[&str] = &["README.adoc", "README.md"];

pub fn run_cmd(args: Args) -> Result<()> {
    let workspace = crate::load_workspace()?;
    let root = workspace.workspace_root;

    let mut changed = 0usize;
    let mut unchanged = 0usize;
    let mut skipped_authored = 0usize;

    let readme_dirs = readme_dirs(&root)?;
    let mut linked = Vec::new();
    let mut relinked = Vec::new();

    for (dir, readme) in &readme_dirs {
        let readme = *readme;
        // If the directory has a README but also a hand-authored (regular)
        // agent file, treat the whole directory as hand-managed.
        if AGENT_FILES.iter().any(|name| is_regular_file(&dir.join(name))) {
            skipped_authored += 1;
            continue;
        }

        // The two agent files in a directory almost always need the same
        // action, so decide per file but report and count per directory.
        let mut to_link = Vec::new();
        let mut to_relink = Vec::new();
        for name in AGENT_FILES {
            let link = dir.join(name);
            match our_symlink_target(&link) {
                // Already pointing at the right README.
                Some(target) if target == readme => {}
                // Our symlink points at the wrong README (renamed); repoint.
                Some(_) => to_relink.push(*name),
                // A foreign file/symlink occupies the name; leave it alone.
                None if link.symlink_metadata().is_ok() => {}
                // No managed link yet.
                None => to_link.push(*name),
            }
        }

        if to_link.is_empty() && to_relink.is_empty() {
            unchanged += 1;
            continue;
        }
        changed += 1;

        if !args.dry_run {
            for name in &to_relink {
                fs::remove_file(dir.join(name))?;
            }
            for name in to_link.iter().chain(&to_relink) {
                let link = dir.join(name);
                symlink(readme, &link)
                    .with_context(|| format!("creating symlink {link}"))?;
            }
        }

        let rel = dir.strip_prefix(&root).unwrap_or(dir.as_path()).to_owned();
        if !to_link.is_empty() {
            linked.push((rel.clone(), readme, to_link));
        }
        if !to_relink.is_empty() {
            relinked.push((rel, readme, to_relink));
        }
    }

    print_link_group(args.dry_run, false, &linked);
    print_link_group(args.dry_run, true, &relinked);

    let orphan_summary = remove_orphans(&orphan_links(&root)?, &root, &args)?;
    let (orphan_count, orphan_label) = match orphan_summary {
        OrphanSummary::Removed(n) => (n, "orphaned removed"),
        OrphanSummary::WouldRemove(n) => (n, "orphaned (would remove)"),
    };

    let suffix = if args.dry_run { " (dry run)" } else { "" };
    println!("\nsetup-agents{suffix}:");
    for (count, label) in [
        (changed, if args.dry_run { "would link" } else { "linked" }),
        (unchanged, "unchanged"),
        (orphan_count, orphan_label),
        (skipped_authored, "hand-authored dirs left alone"),
    ] {
        println!("  {count:>4}  {label}");
    }
    Ok(())
}

/// Handle symlinks whose target README has disappeared. In a dry run, just
/// report them. Otherwise prompt before removing (or remove directly with
/// `--force`); when stdin isn't a TTY and `--force` wasn't given, leave them
/// and explain, so non-interactive callers (agents, CI) never hang or delete
/// unexpectedly.
fn remove_orphans(
    orphans: &[Utf8PathBuf],
    root: &Utf8Path,
    args: &Args,
) -> Result<OrphanSummary> {
    if orphans.is_empty() {
        return Ok(if args.dry_run {
            OrphanSummary::WouldRemove(0)
        } else {
            OrphanSummary::Removed(0)
        });
    }
    let rel =
        |link: &Utf8Path| link.strip_prefix(root).unwrap_or(link).to_owned();

    if args.dry_run {
        for link in orphans {
            println!("would remove orphaned {}", rel(link));
        }
        return Ok(OrphanSummary::WouldRemove(orphans.len()));
    }

    println!(
        "\nFound {} orphaned agent symlink(s) whose README no longer exists:",
        orphans.len()
    );
    for link in orphans {
        println!("  {}", rel(link));
    }

    let do_remove = if args.force {
        true
    } else if io::stdin().is_terminal() {
        prompt_yes_no("Remove them?")?
    } else {
        println!("Not removing (non-interactive). Re-run with --force.");
        false
    };
    if !do_remove {
        return Ok(OrphanSummary::Removed(0));
    }
    for link in orphans {
        fs::remove_file(link)?;
        println!("removed orphaned {}", rel(link));
    }
    Ok(OrphanSummary::Removed(orphans.len()))
}

fn prompt_yes_no(question: &str) -> Result<bool> {
    print!("{question} [y/N] ");
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    Ok(matches!(line.trim().to_ascii_lowercase().as_str(), "y" | "yes"))
}

/// Print a header naming the constant agent filenames once, then list the
/// directories that get them, each with the README it links to. A file is
/// named individually only in the rare case where one of the pair needs an
/// action the other doesn't.
fn print_link_group(
    dry_run: bool,
    relink: bool,
    dirs: &[(Utf8PathBuf, &'static str, Vec<&'static str>)],
) {
    if dirs.is_empty() {
        return;
    }
    let verb = match (dry_run, relink) {
        (true, false) => "would link",
        (true, true) => "would relink",
        (false, false) => "linked",
        (false, true) => "relinked",
    };
    println!("{verb} {} -> README:", AGENT_FILES.join(", "));
    for (rel, readme, names) in dirs {
        if names.len() == AGENT_FILES.len() {
            println!("  {rel}/ ({readme})");
        } else {
            println!("  {rel}/{} ({readme})", names.join(", "));
        }
    }
}

/// Whether `path` exists as a regular file (not a symlink, not a directory).
fn is_regular_file(path: &Utf8Path) -> bool {
    path.symlink_metadata().is_ok_and(|m| m.is_file())
}

/// If `path` is a symlink we manage (it points at one of the README
/// filenames), return that target. Returns `None` for non-symlinks, missing
/// paths, or symlinks pointing elsewhere, which we never touch.
fn our_symlink_target(path: &Utf8Path) -> Option<&'static str> {
    let dest = fs::read_link(path).ok()?;
    let dest = dest.to_str()?;
    README_NAMES.iter().copied().find(|&name| name == dest)
}

fn readme_dirs(root: &Utf8Path) -> Result<Vec<(Utf8PathBuf, &'static str)>> {
    let readmes = git_ls_files(
        root,
        &["--cached", "--others", "--exclude-standard"],
        &[":(glob)**/README.adoc", ":(glob)**/README.md"],
    )?;
    let mut dirs = BTreeMap::new();

    for name in README_NAMES {
        for path in &readmes {
            if path.file_name() != Some(name) {
                continue;
            }
            let Some(parent) = path.parent() else {
                continue;
            };
            if parent.as_str().is_empty() {
                continue;
            }
            dirs.entry(root.join(parent)).or_insert(*name);
        }
    }

    Ok(dirs.into_iter().collect())
}

fn orphan_links(root: &Utf8Path) -> Result<Vec<Utf8PathBuf>> {
    // `--ignored` forces git to descend into ignored directories to enumerate
    // ignored files; without `--directory` it would walk all of `target/`
    // (tens of thousands of files) just to find our handful of gitignored
    // symlinks. `--directory` collapses wholly-untracked directories into a
    // single entry instead of recursing, and those collapsed entries don't
    // match the pathspec below so they drop out. Source directories still
    // contain tracked files, so git keeps descending into them and finds any
    // orphaned symlinks.
    let agent_files = git_ls_files(
        root,
        &["--others", "--ignored", "--exclude-standard", "--directory"],
        &[":(glob)**/CLAUDE.md", ":(glob)**/AGENTS.md"],
    )?;

    Ok(agent_files
        .into_iter()
        .map(|path| root.join(path))
        .filter(|path| match path.parent() {
            Some(parent) => {
                our_symlink_target(path).is_some()
                    && !README_NAMES
                        .iter()
                        .any(|readme| parent.join(readme).is_file())
            }
            None => false,
        })
        .collect())
}

fn git_ls_files(
    root: &Utf8Path,
    options: &[&str],
    pathspecs: &[&str],
) -> Result<Vec<Utf8PathBuf>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .arg("ls-files")
        .arg("-z")
        .args(options)
        .arg("--")
        .args(pathspecs)
        .output()
        .context("running git ls-files")?;

    if !output.status.success() {
        anyhow::bail!(
            "git ls-files failed with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    Ok(output
        .stdout
        .split(|b| *b == b'\0')
        .filter(|path| !path.is_empty())
        .filter_map(|path| std::str::from_utf8(path).ok())
        .map(Utf8PathBuf::from)
        .collect())
}

enum OrphanSummary {
    Removed(usize),
    WouldRemove(usize),
}
