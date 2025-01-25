// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helpers for accessing data from git
//! XXX-dap TODO-doc needs update

use anyhow::{bail, Context};
use camino::{Utf8Path, Utf8PathBuf};
use std::process::Command;

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct GitRevision(String);
NewtypeDebug! { () pub struct GitRevision(String); }
NewtypeDeref! { () pub struct GitRevision(String); }
NewtypeDerefMut! { () pub struct GitRevision(String); }
NewtypeDisplay! { () pub struct GitRevision(String); }
NewtypeFrom! { () pub struct GitRevision(String); }

fn git_start() -> Command {
    Command::new("git")
}

fn do_run(cmd: &mut Command) -> anyhow::Result<String> {
    let label = cmd_label(cmd);
    let output = cmd.output().with_context(|| format!("invoking {:?}", cmd))?;
    let status = output.status;
    let stdout = output.stdout;
    let stderr = output.stderr;
    if status.success() {
        if let Ok(stdout) = String::from_utf8(stdout) {
            return Ok(stdout);
        } else {
            bail!("command succeeded, but output was not UTF-8: {}:\n", label);
        }
    }

    bail!(
        "command failed: {}: {}\n\
        stderr:\n\
        -----\n\
        {}\n\
        -----\n",
        label,
        status,
        String::from_utf8_lossy(&stderr)
    );
}

fn cmd_label(cmd: &Command) -> String {
    format!(
        "{:?} {}",
        cmd.get_program(),
        cmd.get_args()
            .map(|a| format!("{:?}", a))
            .collect::<Vec<_>>()
            .join(" ")
    )
}

pub fn git_merge_base_head(
    revision: &GitRevision,
) -> anyhow::Result<GitRevision> {
    let mut cmd = git_start();
    cmd.arg("merge-base").arg("HEAD").arg(revision.as_str());
    let label = cmd_label(&cmd);
    let stdout = do_run(&mut cmd)?;
    let stdout = stdout.trim();
    if stdout.contains(" ") || stdout.contains("\n") {
        bail!("unexpected output from {} (contains whitespace)", label);
    }
    Ok(GitRevision::from(stdout.to_owned()))
}

pub fn git_ls_tree(
    revision: &GitRevision,
    directory: &Utf8Path,
) -> anyhow::Result<Vec<Utf8PathBuf>> {
    let mut cmd = git_start();
    cmd.arg("ls-tree")
        .arg("-r")
        .arg("-z")
        .arg("--name-only")
        .arg("--full-tree")
        .arg(revision.as_str())
        .arg(&directory);
    let label = cmd_label(&cmd);
    let stdout = do_run(&mut cmd)?;
    Ok(stdout
        .trim()
        .split("\0")
        .filter(|s| !s.is_empty())
        .map(|path| {
            let found_path = Utf8PathBuf::from(path);
            let Ok(relative) = found_path.strip_prefix(directory) else {
                bail!(
                "git ls-tree unexpectedly returned a path that did not start \
                 with {:?}: {:?} (cmd: {})",
                directory,
                found_path,
                label,
            );
            };
            Ok(relative.to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?)
}

pub fn git_show_file(
    revision: &GitRevision,
    path: &Utf8Path,
) -> anyhow::Result<Vec<u8>> {
    let mut cmd = git_start();
    cmd.arg("show").arg(format!("{}:{}", revision, path));
    let stdout = do_run(&mut cmd)?;
    Ok(stdout.into_bytes())
}
