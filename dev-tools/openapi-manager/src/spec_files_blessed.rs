// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Working with the "blessed" OpenAPI specification files
//! XXX-dap TODO-doc needs update

use crate::{
    apis::{ApiIdent, ManagedApis},
    git::{git_ls_tree, git_merge_base_head, git_show_file, GitRevision},
    iter_only::iter_only,
    spec_files_generic::{
        ApiFiles, ApiSpecFile, ApiSpecFilesBuilder, AsRawFiles,
    },
};
use anyhow::{anyhow, Context};
use camino::Utf8Path;
use std::{collections::BTreeMap, ops::Deref};

pub struct BlessedApiSpecFile(ApiSpecFile);
NewtypeDebug! { () pub struct BlessedApiSpecFile(ApiSpecFile); }
NewtypeDeref! { () pub struct BlessedApiSpecFile(ApiSpecFile); }
NewtypeDerefMut! { () pub struct BlessedApiSpecFile(ApiSpecFile); }
NewtypeFrom! { () pub struct BlessedApiSpecFile(ApiSpecFile); }

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

impl<'a> TryFrom<ApiSpecFilesBuilder<'a>> for BlessedFiles {
    type Error = anyhow::Error;

    fn try_from(api_files: ApiSpecFilesBuilder<'a>) -> anyhow::Result<Self> {
        let (raw_spec_files, errors, warnings) = api_files.into_parts();
        let spec_files = raw_spec_files
            .into_iter()
            .map(|(v, list)| {
                let file = BlessedApiSpecFile::from(
                        iter_only(list.into_iter()).context(
                            "list of blessed OpenAPI documents for an API",
                        )?,
                    );
                Ok((
                    v,
                    BlessedApiSpecFile::from(
                        iter_only(list.into_iter()).context(
                            "list of blessed OpenAPI documents for an API",
                        )?,
                    ),
                ))
            })
            .collect::<Result<BTreeMap<_, _>, anyhow::Error>>()?;
        Ok(BlessedFiles { spec_files, errors, warnings })
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
        let mut api_files = ApiSpecFilesBuilder::new(apis);
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
                if let Some(file_name) =
                    api_files.lockstep_file_name(parts[0], true)
                {
                    api_files.load_contents(file_name, contents);
                }
            } else if parts.len() == 2 {
                if let Some(ident) =
                    api_files.versioned_directory(parts[0], true)
                {
                    if parts[1] == format!("{}-latest.json", ident) {
                        // This is the "latest" symlink.  We could dereference
                        // it and report it here, but it's not relevant for
                        // anything this tool does.
                        continue;
                    }

                    if let Some(file_name) =
                        api_files.versioned_file_name(&ident, parts[1], true)
                    {
                        api_files.load_contents(file_name, contents);
                    }
                }
            }
        }

        BlessedFiles::try_from(api_files)
    }
}
