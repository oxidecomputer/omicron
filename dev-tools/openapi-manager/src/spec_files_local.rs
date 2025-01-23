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
/// XXX-dap be more specific about what has and has not been validated at this
/// point.
// XXX-dap move to a separate module?
// XXX-dap actually, maybe the thing to do here is to have one type with a
// sentinel generic type paramter, like SpecFileContainer<Local>.
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
        let mut api_files = ApiSpecFilesBuilder::new(apis);
        let entry_iter = dir
            .read_dir_utf8()
            .with_context(|| format!("readdir {:?}", dir))?;
        for maybe_entry in entry_iter {
            let entry = maybe_entry
                .with_context(|| format!("readdir {:?} entry", dir))?;

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
                LocalFiles::load_versioned_directory(
                    &mut api_files,
                    path,
                    file_name,
                );
            } else {
                // This is not something the tool cares about, but it's not
                // obviously a problem, either.
                api_files.load_warning(anyhow!(
                    "ignored (not a file or directory): {:?}",
                    path
                ));
            };
        }

        let (spec_files, errors, warnings) = api_files.into_parts();
        Ok(LocalFiles { spec_files, errors, warnings })
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
            let Some(file_name) =
                api_files.versioned_file_name(&ident, file_name)
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
}

#[derive(Debug)]
pub struct LocalApiSpecFile(pub ApiSpecFile); // XXX-dap make non-pub
impl From<ApiSpecFile> for LocalApiSpecFile {
    fn from(value: ApiSpecFile) -> Self {
        LocalApiSpecFile(value)
    }
}
