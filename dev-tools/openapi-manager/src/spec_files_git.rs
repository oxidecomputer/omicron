// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Working with OpenAPI specification files from Git history
//! XXX-dap TODO-doc needs update
//! XXX-dap rename to Blessed and put git stuff elsewhere?

use crate::{
    apis::{ApiIdent, ManagedApis},
    spec_files_generic::{ApiSpecFile, ApiSpecFilesBuilder},
};
use anyhow::{anyhow, bail, Context};
use camino::{Utf8Path, Utf8PathBuf};
use std::{collections::BTreeMap, process::Command};

/// Container for "blessed" OpenAPI spec files, found in Git
///
/// Most validation is not done at this point.
/// XXX-dap be more specific about what has and has not been validated at this
/// point.
// XXX-dap actually, maybe the thing to do here is to have one type with a
// sentinel generic type paramter, like SpecFileContainer<Local>.
#[derive(Debug)]
pub struct BlessedFiles {
    pub spec_files:
        BTreeMap<ApiIdent, BTreeMap<semver::Version, Vec<BlessedApiSpecFile>>>,
    pub errors: Vec<anyhow::Error>,
    pub warnings: Vec<anyhow::Error>,
}

pub struct BlessedApiSpecFile(ApiSpecFile);
NewtypeDebug! { () pub struct BlessedApiSpecFile(ApiSpecFile); }
NewtypeDeref! { () pub struct BlessedApiSpecFile(ApiSpecFile); }
NewtypeDerefMut! { () pub struct BlessedApiSpecFile(ApiSpecFile); }
NewtypeFrom! { () pub struct BlessedApiSpecFile(ApiSpecFile); }

impl BlessedFiles {
    pub fn load_from_parent_branch(
        branch: &GitRevision,
        directory: &Utf8Path,
        apis: &ManagedApis,
    ) -> anyhow::Result<BlessedFiles> {
        let revision = git_merge_base_head(branch)?;
        Self::load_from_git_revision(&revision, directory, apis)
    }

    pub fn load_from_git_revision(
        commit: &GitRevision,
        directory: &Utf8Path,
        apis: &ManagedApis,
    ) -> anyhow::Result<BlessedFiles> {
        // XXX-dap how do we ensure this is a relative path from the root of the
        // workspace
        let files_found = git_ls_tree(&commit, directory)?;
        todo!();
    }
}

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

fn git_merge_base_head(revision: &GitRevision) -> anyhow::Result<GitRevision> {
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

fn git_ls_tree(
    revision: &GitRevision,
    directory: &Utf8Path,
) -> anyhow::Result<Vec<Utf8PathBuf>> {
    let mut cmd = git_start();
    cmd.arg("ls-tree")
        .arg("-r")
        .arg("-z")
        .arg("--name-only")
        .arg(revision.as_str())
        .arg(&directory);
    let stdout = do_run(&mut cmd)?;
    Ok(stdout.trim().split("\0").map(Utf8PathBuf::from).collect())
}
