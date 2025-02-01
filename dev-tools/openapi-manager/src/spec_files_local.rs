// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Working with OpenAPI specification files in the repository
//! XXX-dap TODO-doc needs update

use crate::{
    apis::{ApiIdent, ManagedApis},
    spec_files_generic::{ApiSpecFile, ApiSpecFilesBuilder},
};
use anyhow::{anyhow, Context};
use camino::Utf8Path;
use std::collections::BTreeMap;

/// Container for OpenAPI spec files found in the local filesystem
///
/// Most validation is not done at this point.
// XXX-dap see comments on BlessedFiles
#[derive(Debug)]
pub struct LocalFiles {
    pub spec_files:
        BTreeMap<ApiIdent, BTreeMap<semver::Version, Vec<LocalApiSpecFile>>>,
    pub errors: Vec<anyhow::Error>,
    pub warnings: Vec<anyhow::Error>,
}

impl LocalFiles {
    // XXX-dap goofy that this can return a thing with errors or an error
    // itself.  but there are different layers of error here:
    // - error traversing the directory (that's what this returned error means)
    // - error with individual items found
    // - things that were skipped, etc.
    pub fn load_from_directory(
        dir: &Utf8Path,
        apis: &ManagedApis,
    ) -> anyhow::Result<LocalFiles> {
        let api_files = walk_local_directory(dir, apis)?;
        Ok(Self::from(api_files))
    }
}

impl<'a> From<ApiSpecFilesBuilder<'a>> for LocalFiles {
    fn from(api_files: ApiSpecFilesBuilder) -> Self {
        let (spec_files, errors, warnings) = api_files.into_parts();
        LocalFiles { spec_files, errors, warnings }
    }
}

pub struct LocalApiSpecFile(ApiSpecFile);
NewtypeDebug! { () pub struct LocalApiSpecFile(ApiSpecFile); }
NewtypeDeref! { () pub struct LocalApiSpecFile(ApiSpecFile); }
NewtypeDerefMut! { () pub struct LocalApiSpecFile(ApiSpecFile); }
NewtypeFrom! { () pub struct LocalApiSpecFile(ApiSpecFile); }

pub fn walk_local_directory<'a>(
    dir: &'_ Utf8Path,
    apis: &'a ManagedApis,
) -> anyhow::Result<ApiSpecFilesBuilder<'a>> {
    let mut api_files = ApiSpecFilesBuilder::new(apis);
    let entry_iter =
        dir.read_dir_utf8().with_context(|| format!("readdir {:?}", dir))?;
    for maybe_entry in entry_iter {
        let entry =
            maybe_entry.with_context(|| format!("readdir {:?} entry", dir))?;

        // If this entry is a file, then we'd expect it to be the JSON file
        // for one of our lockstep APIs.  Check and see.
        let path = entry.path();
        let file_name = entry.file_name();
        let file_type = entry
            .file_type()
            .with_context(|| format!("file type of {:?}", path))?;
        if file_type.is_file() {
            match fs_err::read(path) {
                Ok(contents) => {
                    if let Some(file_name) =
                        api_files.lockstep_file_name(file_name)
                    {
                        api_files.load_contents(file_name, contents);
                    }
                }
                Err(error) => {
                    api_files.load_error(anyhow!(error));
                }
            };
        } else if file_type.is_dir() {
            load_versioned_directory(&mut api_files, path, file_name);
        } else {
            // This is not something the tool cares about, but it's not
            // obviously a problem, either.
            api_files.load_warning(anyhow!(
                "ignored (not a file or directory): {:?}",
                path
            ));
        };
    }

    Ok(api_files)
}

fn load_versioned_directory(
    api_files: &mut ApiSpecFilesBuilder,
    path: &Utf8Path,
    basename: &str,
) {
    let Some(ident) = api_files.versioned_directory(basename) else {
        return;
    };

    let entries = match path
        .read_dir_utf8()
        .and_then(|entry_iter| entry_iter.collect::<Result<Vec<_>, _>>())
    {
        Ok(entries) => entries,
        Err(error) => {
            api_files.load_error(
                anyhow!(error).context(format!("readdir {:?}", path)),
            );
            return;
        }
    };

    for entry in entries {
        let file_name = entry.file_name();
        let Some(file_name) = api_files.versioned_file_name(&ident, file_name)
        else {
            continue;
        };

        let contents = match fs_err::read(&entry.path()) {
            Ok(contents) => contents,
            Err(error) => {
                api_files.load_error(anyhow!(error));
                continue;
            }
        };

        api_files.load_contents(file_name, contents);
    }
}
