// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Working with the "blessed" OpenAPI specification files
//! XXX-dap TODO-doc needs update

use crate::{
    apis::{ApiIdent, ManagedApis},
    git::{git_ls_tree, git_merge_base_head, git_show_file, GitRevision},
    spec_files_generic::{ApiSpecFile, ApiSpecFilesBuilder},
};
use anyhow::anyhow;
use camino::Utf8Path;
use std::collections::BTreeMap;

/// Container for "blessed" OpenAPI spec files, found in Git
///
/// Most validation is not done at this point.
/// XXX-dap be more specific about what has and has not been validated at this
/// point.
// XXX-dap actually, maybe the thing to do here is to have one type with a
// sentinel generic type paramter, like SpecFileContainer<Local>.
// XXX-dap alternatively: maybe the thing to do has have ApiSpecFilesBuilder
// accept a config option which is either whether to allow more than one, or to
// have it be parametrized across a "thing" that can accept a found spec (which
// would either be Vec<X> or ... X?  That way, it would produce an error during
// loading for BlessedFiles and GeneratedFiles if we found more than one
// matching file *and* these structures' types could reflect that there's
// exactly one.
// XXX-dap however, we should probably decide on the question of whether to
// include checksums in local file names first because if not, this will all get
// a lot simpler and they *can* use the same type as LocalFiles
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
                api_files.load_error(anyhow!(
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

impl<'a> From<ApiSpecFilesBuilder<'a>> for BlessedFiles {
    fn from(api_files: ApiSpecFilesBuilder<'a>) -> Self {
        let (spec_files, errors, warnings) = api_files.into_parts();
        BlessedFiles { spec_files, errors, warnings }
    }
}
