// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Working with OpenAPI specification files in the repository
//! XXX-dap TODO-doc needs update

use crate::{
    apis::{ApiIdent, ManagedApis},
    spec_files_generic::{
        ApiFiles, ApiSpecFile, ApiSpecFilesBuilder, AsRawFiles,
    },
};
use anyhow::{anyhow, Context};
use camino::Utf8Path;
use std::{collections::BTreeMap, ops::Deref};

/// Container for OpenAPI spec files found in the local filesystem
///
/// Most validation is not done at this point.
// XXX-dap see comments on BlessedFiles
#[derive(Debug)]
pub struct LocalFiles {
    pub spec_files: BTreeMap<ApiIdent, ApiFiles<Vec<LocalApiSpecFile>>>,
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
        let api_files = walk_local_directory(dir, apis, false)?;
        Self::try_from(api_files)
    }
}

impl<'a> TryFrom<ApiSpecFilesBuilder<'a>> for LocalFiles {
    type Error = anyhow::Error;

    fn try_from(api_files: ApiSpecFilesBuilder) -> anyhow::Result<Self> {
        let (spec_files, errors, warnings) = api_files.into_parts()?;
        Ok(LocalFiles { spec_files, errors, warnings })
    }
}

pub struct LocalApiSpecFile(ApiSpecFile);
NewtypeDebug! { () pub struct LocalApiSpecFile(ApiSpecFile); }
NewtypeDeref! { () pub struct LocalApiSpecFile(ApiSpecFile); }
NewtypeDerefMut! { () pub struct LocalApiSpecFile(ApiSpecFile); }
NewtypeFrom! { () pub struct LocalApiSpecFile(ApiSpecFile); }

impl AsRawFiles for Vec<LocalApiSpecFile> {
    fn as_raw_files<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = &'a ApiSpecFile> + 'a> {
        Box::new(self.iter().map(|t| t.deref()))
    }
}

// impl TryFrom<Vec<ApiSpecFile>> for Vec<LocalApiSpecFile> {
//     type Error = anyhow::Error;
//
//     fn try_from(value: Vec<ApiSpecFile>) -> Result<Self, Self::Error> {
//         Ok(value.into_iter().map(LocalApiSpecFile::from).collect())
//     }
// }

pub fn walk_local_directory<'a>(
    dir: &'_ Utf8Path,
    apis: &'a ManagedApis,
    misconfigurations_okay: bool,
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
                    if let Some(file_name) = api_files
                        .lockstep_file_name(file_name, misconfigurations_okay)
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
    let Some(ident) = api_files.versioned_directory(basename, false) else {
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

        // XXX-dap use helper function that does rsplitn and checks both parts?
        // see also blessed.rs
        if file_name == format!("{}-latest.json", ident) {
            // We should be looking at a symlink.
            let symlink = match entry.path().read_link_utf8() {
                Ok(s) => s,
                Err(error) => {
                    api_files.load_error(anyhow!(error).context(format!(
                        "read what should be a symlink {:?}",
                        entry.path()
                    )));
                    continue;
                }
            };

            // XXX-dap this error message will be confusing because the user
            // won't know why we're looking at this path
            if let Some(v) =
                api_files.versioned_file_name(&ident, symlink.as_str(), false)
            {
                api_files.load_latest_link(&ident, v, false);
            }
            continue;
        }

        let Some(file_name) =
            api_files.versioned_file_name(&ident, file_name, false)
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
