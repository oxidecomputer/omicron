// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Working with OpenAPI specification files in the repository

use crate::apis::{ApiIdent, ManagedApis};
use anyhow::{anyhow, bail, Context};
use camino::{Utf8Path, Utf8PathBuf};
use openapiv3::OpenAPI;
use std::{collections::BTreeMap, ops::Deref};

/// Container for all the OpenAPI spec files found
///
/// Most validation is not done at this point.
struct AllApiSpecFiles {
    api_files: BTreeMap<ApiIdent, Vec<ApiSpecFile>>,
    warnings: Vec<anyhow::Error>,
}

impl AllApiSpecFiles {
    pub fn load_from_directory(
        dir: &Utf8Path,
        apis: &ManagedApis,
    ) -> anyhow::Result<AllApiSpecFiles> {
        let mut api_files = BTreeMap::new();
        let mut warnings = Vec::new();
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
            let loaded = if file_type.is_file() {
                Some(
                    AllApiSpecFiles::load_lockstep_file(apis, path, file_name)
                        .map(|v| vec![v]),
                )
            } else if file_type.is_dir() {
                Some(AllApiSpecFiles::load_versioned_directory(
                    apis, path, file_name,
                ))
            } else {
                None
            };

            match loaded {
                Some(Ok(files)) => {
                    for file in files {
                        api_files
                            .entry(file.name.ident.clone())
                            .or_insert_with(Vec::new)
                            .push(file);
                    }
                }
                Some(Err(error)) => {
                    warnings.push(error);
                }
                None => {
                    warnings
                        .push(anyhow!("not a file or directory: {:?}", path));
                }
            };
        }

        Ok(AllApiSpecFiles { api_files, warnings })
    }

    fn load_lockstep_file(
        apis: &ManagedApis,
        path: &Utf8Path,
        basename: &str,
    ) -> anyhow::Result<ApiSpecFile> {
        let file_name = ApiSpecFileName::new_lockstep(basename)
            .with_context(|| format!("path {:?}", path))?;
        let ident = &file_name.ident;
        let api = apis
            .api(ident)
            .ok_or_else(|| anyhow!("found file for unknown API: {:?}", path))?;
        if !api.is_lockstep() {
            bail!("found lockstep file for non-lockstep API: {:?}", path);
        }

        ApiSpecFile::load(file_name, path)
    }

    fn load_versioned_directory(
        apis: &ManagedApis,
        path: &Utf8Path,
        basename: &str,
        // XXX-dap either use a builder or an accumulator of both warnings and
        // spec files
    ) -> anyhow::Result<Vec<ApiSpecFile>> {
        let ident = ApiIdent::from(basename.to_owned());
        let api = apis.api(&ident).ok_or_else(|| {
            anyhow!("found directory for unknown API: {:?}", path)
        })?;
        if !api.is_versioned() {
            bail!(
                "found versioned directory for non-versioned API: {:?}",
                path
            );
        }

        let mut rv = Vec::new();
        let entry_iter = path
            .read_dir_utf8()
            .with_context(|| format!("readdir {:?}", path))?;
        for maybe_entry in entry_iter {
            let entry = maybe_entry
                .with_context(|| format!("readdir {:?} entry", path))?;
            let file_name = entry.file_name();
            let file_name = ApiSpecFileName::new_versioned(basename, file_name)
                .with_context(|| format!("path {:?}", entry.path()))?;
            rv.push(ApiSpecFile::load(file_name, path)?);
        }

        Ok(rv)
    }

    pub fn apis(&self) -> impl Iterator<Item = &ApiIdent> + '_ {
        self.api_files.keys()
    }

    pub fn api_spec_files(
        &self,
        ident: &ApiIdent,
    ) -> Option<impl Iterator<Item = &ApiSpecFile> + '_> {
        self.api_files.get(ident).map(|files| files.iter())
    }
}

/// Describes the path to an OpenAPI document file
// XXX-dap spec -> document?
struct ApiSpecFileName {
    ident: ApiIdent,
    kind: ApiSpecFileNameKind,
}

impl ApiSpecFileName {
    /// Attempts to parse the given file basename as an ApiSpecFileName of kind
    /// `Versioned`.  These look like:
    ///
    ///     ident-SEMVER-CHECKSUM.json
    ///
    /// However, the `ident-` must have already been stripped off by the caller.
    fn new_versioned(
        ident: &str,
        basename: &str,
    ) -> anyhow::Result<ApiSpecFileName> {
        let expected_prefix = format!("{}-", ident);
        let suffix = basename.strip_prefix(ident).ok_or_else(|| {
            anyhow!(
                "versioned API document filename did not start with {:?}",
                expected_prefix
            )
        })?;

        let middle = suffix.strip_suffix(".json").ok_or_else(|| {
            anyhow!("versioned API document filename did not end in .json")
        })?;

        let (version_str, sum) = middle.rsplit_once("-").ok_or_else(|| {
            anyhow!(
                "extracting version and checksum from versioned API \
                 document filename"
            )
        })?;

        let version: semver::Version =
            version_str.parse().with_context(|| {
                format!("version string {:?} is not a semver", version_str)
            })?;

        // Dropshot does not support pre-release strings and we don't either.
        // This could probably be made to work, but it's easier to constrain
        // things for now and relax it later.
        if !version.pre.is_empty() {
            bail!(
                "version string {:?} has a prerelease field (not supported)",
                version_str
            );
        }

        if !version.build.is_empty() {
            bail!(
                "version string {:?} has a build field (not supported)",
                version_str
            );
        }

        Ok(ApiSpecFileName {
            ident: ApiIdent::from(ident.to_string()),
            kind: ApiSpecFileNameKind::Versioned {
                version,
                sum: sum.to_string(),
            },
        })
    }

    /// Attempts to parse the given file basename as an ApiSpecFileName of kind
    /// `Lockstep`
    fn new_lockstep(basename: &str) -> anyhow::Result<ApiSpecFileName> {
        let ident = basename.strip_suffix(".json").ok_or_else(|| {
            anyhow!("lockstep API document filename did not end in .json")
        })?;
        Ok(ApiSpecFileName {
            ident: ApiIdent::from(ident.to_string()),
            kind: ApiSpecFileNameKind::Lockstep,
        })
    }

    /// Returns the path of this file relative to the root of the OpenAPI specs
    fn path(&self) -> Utf8PathBuf {
        match &self.kind {
            ApiSpecFileNameKind::Lockstep => {
                Utf8PathBuf::from_iter([self.basename()])
            }
            ApiSpecFileNameKind::Versioned { .. } => Utf8PathBuf::from_iter([
                self.ident.deref().clone(),
                self.basename(),
            ]),
        }
    }

    fn basename(&self) -> String {
        match &self.kind {
            ApiSpecFileNameKind::Lockstep => format!("{}.json", self.ident),
            ApiSpecFileNameKind::Versioned { version, sum } => {
                // XXX-dap the version number must not contain dashes
                format!("{}-{}-{}.json", self.ident, version, sum)
            }
        }
    }
}

/// Describes how this API's specification file is named
enum ApiSpecFileNameKind {
    Lockstep,
    Versioned { version: semver::Version, sum: String },
}

/// Describes an OpenAPI document found on disk
struct ApiSpecFile {
    name: ApiSpecFileName,
    contents: OpenAPI,
}

impl ApiSpecFile {
    fn load(
        name: ApiSpecFileName,
        path: &Utf8Path,
    ) -> anyhow::Result<ApiSpecFile> {
        let contents = todo!(); // XXX-dap read file and parse it
        Ok(ApiSpecFile { name, contents })
    }
}
