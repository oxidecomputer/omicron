// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Working with the "blessed" OpenAPI specification files
//! XXX-dap TODO-doc needs update

use crate::{
    apis::{ApiIdent, ManagedApis},
    git::{git_ls_tree, git_merge_base_head, git_show_file, GitRevision},
    spec_files_generic::{
        ApiFiles, ApiLoad, ApiSpecFile, ApiSpecFilesBuilder, AsRawFiles,
    },
};
use anyhow::{anyhow, bail};
use camino::Utf8Path;
use std::{collections::BTreeMap, ops::Deref};

pub struct BlessedApiSpecFile(ApiSpecFile);
NewtypeDebug! { () pub struct BlessedApiSpecFile(ApiSpecFile); }
NewtypeDeref! { () pub struct BlessedApiSpecFile(ApiSpecFile); }
NewtypeDerefMut! { () pub struct BlessedApiSpecFile(ApiSpecFile); }
NewtypeFrom! { () pub struct BlessedApiSpecFile(ApiSpecFile); }

impl ApiLoad for BlessedApiSpecFile {
    const MISCONFIGURATIONS_ALLOWED: bool = true;

    fn make_item(raw: ApiSpecFile) -> Self {
        BlessedApiSpecFile(raw)
    }

    fn try_extend(&mut self, item: ApiSpecFile) -> anyhow::Result<()> {
        // This should be impossible.
        bail!(
            "found more than one blessed OpenAPI document for a given \
             API version: at least {} and {}",
            self.spec_file_name(),
            item.spec_file_name()
        );
    }
}

impl AsRawFiles for BlessedApiSpecFile {
    fn as_raw_files<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = &'a ApiSpecFile> + 'a> {
        Box::new(std::iter::once(self.deref()))
    }
}

/// Container for "blessed" OpenAPI spec files, found in Git
///
/// Most validation is not done at this point.
/// XXX-dap be more specific about what has and has not been validated at this
/// point.
#[derive(Debug)]
pub struct BlessedFiles {
    pub spec_files: BTreeMap<ApiIdent, ApiFiles<BlessedApiSpecFile>>,
    pub errors: Vec<anyhow::Error>,
    pub warnings: Vec<anyhow::Error>,
}

impl<'a> From<ApiSpecFilesBuilder<'a, BlessedApiSpecFile>> for BlessedFiles {
    fn from(api_files: ApiSpecFilesBuilder<'a, BlessedApiSpecFile>) -> Self {
        let (spec_files, errors, warnings) = api_files.into_parts();
        BlessedFiles { spec_files, errors, warnings }
    }
}

impl BlessedFiles {
    pub fn load_from_git_parent_branch(
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
        let mut api_files: ApiSpecFilesBuilder<BlessedApiSpecFile> =
            ApiSpecFilesBuilder::new(apis);
        let files_found = git_ls_tree(&commit, directory)?;
        for f in files_found {
            // We should be looking at either a single-component path
            // ("api.json") or a file inside one level of directory hierarhcy
            // ("api/api-1.2.3-hash.json").  Figure out which case we're in.
            let parts: Vec<_> = f.iter().collect();
            if parts.is_empty() || parts.len() > 2 {
                api_files.load_warning(anyhow!(
                    "path {:?}: can't understand this path name",
                    f
                ));
                continue;
            }

            // Read the contents.
            let contents = git_show_file(commit, &directory.join(&f))?;
            if parts.len() == 1 {
                if let Some(file_name) = api_files.lockstep_file_name(parts[0])
                {
                    api_files.load_contents(file_name, contents);
                }
            } else if parts.len() == 2 {
                if let Some(ident) = api_files.versioned_directory(parts[0]) {
                    if parts[1] == format!("{}-latest.json", ident) {
                        // This is the "latest" symlink.  We could dereference
                        // it and report it here, but it's not relevant for
                        // anything this tool does.
                        continue;
                    }

                    if let Some(file_name) =
                        api_files.versioned_file_name(&ident, parts[1])
                    {
                        api_files.load_contents(file_name, contents);
                    }
                }
            }
        }

        Ok(BlessedFiles::from(api_files))
    }
}
