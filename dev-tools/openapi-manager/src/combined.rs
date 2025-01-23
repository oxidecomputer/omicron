// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Holistic view of API information available, based on both Rust-defined
//! configuration and the OpenAPI documents present

use crate::apis::{ApiIdent, ManagedApi, ManagedApis};
use crate::spec::Environment;
use crate::spec_files::{ApiSpecFile, ApiSpecFileName, LocalFiles};
use crate::validation::{validate_generated_openapi_document, DocumentSummary};
use anyhow::Context;
use camino::{Utf8Path, Utf8PathBuf};
use std::collections::BTreeMap;
use std::fmt::Display;
use thiserror::Error;

pub struct CombinedApis {
    apis: BTreeMap<ApiIdent, ManagedApi>,
    versions: BTreeMap<ApiIdent, BTreeMap<semver::Version, ApiSpecFileName>>,
    loaded_spec_files: BTreeMap<ApiSpecFileName, ApiSpecFile>,
    problems: Vec<Problem>,
}

impl CombinedApis {
    pub fn load(
        dir: &Utf8Path,
    ) -> anyhow::Result<(CombinedApis, Vec<anyhow::Error>)> {
        let managed_apis = ManagedApis::all()?;
        // XXX-dap
        let all_spec_files = todo!();
        let warnings = todo!();
        // let (all_spec_files, warnings) =
        //     LocalFiles::load_from_directory(dir, &managed_apis)?;
        Ok((CombinedApis::new(managed_apis, all_spec_files), warnings))
    }

    pub fn new(
        api_list: ManagedApis,
        all_spec_files: LocalFiles,
    ) -> CombinedApis {
        let apis = api_list.into_map();
        let all_spec_files: BTreeMap<ApiIdent, Vec<ApiSpecFile>> = todo!(); // all_spec_files.into_map(); // XXX-dap

        let mut versions = BTreeMap::new();
        let mut loaded_spec_files = BTreeMap::new();
        let mut problems = Vec::new();

        // First, group the specification files that we found for each managed
        // API by version.  We'll identify several possible problems here,
        // including:
        //
        // - spec files with no associated API
        // - spec files for a version of an API that does not exist
        // - duplicate spec files for a given API version
        //   XXX-dap this is now impossible
        for (api_ident, spec_files) in all_spec_files {
            let Some(managed_api) = apis.get(&api_ident) else {
                // unwrap(): there should always be at least one spec file for
                // each API in AllApiSpecFiles or else we wouldn't have known
                // about this API to begin with.
                problems.push(Problem::SpecFileUnknownApi {
                    spec_file_name: spec_files
                        .first()
                        .unwrap()
                        .spec_file_name()
                        .clone(),
                });
                continue;
            };

            let mut api_versions = BTreeMap::new();

            for file in spec_files {
                let version = file.version();
                let this_file_name = file.spec_file_name().clone();

                if !managed_api.has_version(version) {
                    problems.push(Problem::SpecFileUnknownVersion {
                        spec_file_name: this_file_name,
                    });
                    continue;
                }

                // It should be impossible to run into the same filename twice.
                let version = version.clone();
                assert!(loaded_spec_files
                    .insert(this_file_name.clone(), file)
                    .is_none());

                if let Some(other_file_name) =
                    api_versions.insert(version.clone(), this_file_name.clone())
                {
                    problems.push(Problem::DuplicateSpecFiles {
                        version: version.clone(),
                        spec_file_names: DisplayableVec(vec![
                            this_file_name.clone(),
                            other_file_name.clone(),
                        ]),
                    });
                }
            }

            // It should be impossible to see the same identifier twice because
            // `AllApiSpecFiles` should not be reporting duplicates.
            assert!(versions.insert(api_ident, api_versions).is_none());
        }

        // Now go through the managed API versions and look for any missing spec
        // files.
        for (api_ident, managed_api) in &apis {
            let api_versions = versions.get(api_ident);
            for api_version in managed_api.iter_versions_semver() {
                if api_versions.and_then(|vs| vs.get(api_version)).is_none() {
                    problems.push(Problem::MissingSpecFile {
                        api_ident: api_ident.clone(),
                        version: api_version.clone(),
                    });
                };
            }
        }

        CombinedApis { apis, versions, loaded_spec_files, problems }
    }

    pub fn problems(&self) -> &[Problem] {
        self.problems.as_slice()
    }

    pub fn apis(&self) -> impl Iterator<Item = CombinedApi<'_>> + '_ {
        self.apis.iter().map(|(ident, managed_api)| {
            // unwrap(): we verified this during construction.
            // XXX-dap this and other unwraps are not necessarily safe if we
            // encountered Problems during constructor.  May want to make these
            // errors per-API and do something different here?
            let versions = self.versions.get(ident).unwrap();
            CombinedApi {
                managed_api,
                versions,
                spec_files: &self.loaded_spec_files,
            }
        })
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct DisplayableVec<T>(pub Vec<T>);
impl<T> Display for DisplayableVec<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // slice::join would require the use of unstable Rust.
        let mut iter = self.0.iter();
        if let Some(item) = iter.next() {
            write!(f, "{item}")?;
        }

        for item in iter {
            write!(f, ", {item}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum Problem {
    #[error(
        "found extra OpenAPI document (no such API defined in Rust): \
         {spec_file_name}"
    )]
    SpecFileUnknownApi { spec_file_name: ApiSpecFileName },

    #[error(
        "found extra OpenAPI document \
         (no corresponding version defined in Rust): {spec_file_name}"
    )]
    SpecFileUnknownVersion { spec_file_name: ApiSpecFileName },

    #[error(
        "missing OpenAPI document for Rust-defined API {api_ident} \
         version {version}"
    )]
    MissingSpecFile { api_ident: ApiIdent, version: semver::Version },

    #[error(
        "multiple OpenAPI documents have the same version ({version}): \
         {spec_file_names}"
    )]
    DuplicateSpecFiles {
        version: semver::Version,
        spec_file_names: DisplayableVec<ApiSpecFileName>,
    },
}

pub struct CombinedApi<'a> {
    managed_api: &'a ManagedApi,
    versions: &'a BTreeMap<semver::Version, ApiSpecFileName>,
    spec_files: &'a BTreeMap<ApiSpecFileName, ApiSpecFile>,
}

impl<'a> CombinedApi<'a> {
    pub fn api(&self) -> &ManagedApi {
        self.managed_api
    }

    pub fn versions(&self) -> impl Iterator<Item = CombinedApiVersion> + '_ {
        self.versions.iter().map(|(version, spec_file_name)| {
            // unwrap(): we verified during construction that we have all these
            // files.
            let spec_file = self.spec_files.get(spec_file_name).unwrap();
            CombinedApiVersion {
                managed_api: self.managed_api,
                version,
                spec_file_name,
                spec_file,
            }
        })
    }
}

pub struct CombinedApiVersion<'a> {
    managed_api: &'a ManagedApi,
    version: &'a semver::Version,
    spec_file_name: &'a ApiSpecFileName,
    spec_file: &'a ApiSpecFile,
}

impl<'a> CombinedApiVersion<'a> {
    pub fn semver(&self) -> &semver::Version {
        self.version
    }

    pub fn check(&self, env: &Environment) -> anyhow::Result<SpecCheckStatus> {
        let freshly_generated =
            self.managed_api.generate_spec_bytes(self.version)?;
        let (openapi, validation_result) = validate_generated_openapi_document(
            self.managed_api,
            &freshly_generated,
        )?;
        // XXX-dap where should DocumentSummary live
        let summary = DocumentSummary::new(&openapi);

        let api_spec = ApiSpecFile::for_contents(
            self.spec_file_name.clone(),
            openapi,
            freshly_generated,
        )?;

        // XXX-dap
        // What we want to do here is check:
        // (1) the OpenAPI doc against what we loaded earlier
        // (2) each "extra file" against what's on disk (may not be present)
        // and generate a CheckStatus for each one
        let openapi_doc = if api_spec.contents() == self.spec_file.contents() {
            CheckStatus::Fresh
        } else if self.managed_api.is_versioned() {
            CheckStatus::Stale(CheckStale::New)
        } else {
            CheckStatus::Stale(CheckStale::Modified {
                full_path: self.spec_file_name.path(),
                actual: api_spec.contents().to_vec(),
                expected: self.spec_file.contents().to_vec(),
            })
        };

        let extra_files = validation_result
            .extra_files
            .into_iter()
            .map(|(path, contents)| {
                let full_path = env.workspace_root.join(&path);
                let status = check_file(full_path, contents)?;
                Ok((path, status))
            })
            .collect::<anyhow::Result<_>>()?;

        Ok(SpecCheckStatus { summary, openapi_doc, extra_files })
    }
}

/// Check a file against expected contents.
// XXX-dap non-pub
pub(crate) fn check_file(
    full_path: Utf8PathBuf,
    contents: Vec<u8>,
) -> anyhow::Result<CheckStatus> {
    let existing_contents =
        read_opt(&full_path).context("failed to read contents on disk")?;

    match existing_contents {
        Some(existing_contents) if existing_contents == contents => {
            Ok(CheckStatus::Fresh)
        }
        Some(existing_contents) => {
            Ok(CheckStatus::Stale(CheckStale::Modified {
                full_path,
                actual: existing_contents,
                expected: contents,
            }))
        }
        None => Ok(CheckStatus::Stale(CheckStale::New)),
    }
}

// XXX-dap non-pub
pub(crate) fn read_opt(path: &Utf8Path) -> std::io::Result<Option<Vec<u8>>> {
    match fs_err::read(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => return Err(err),
    }
}

// XXX-dap move this to validation.rs and call that check.rs?
#[derive(Debug)]
#[must_use]
pub(crate) struct SpecCheckStatus {
    pub(crate) summary: DocumentSummary,
    pub(crate) openapi_doc: CheckStatus,
    pub(crate) extra_files: Vec<(Utf8PathBuf, CheckStatus)>,
}

impl SpecCheckStatus {
    pub(crate) fn total_errors(&self) -> usize {
        self.iter_errors().count()
    }

    pub(crate) fn extra_files_len(&self) -> usize {
        self.extra_files.len()
    }

    pub(crate) fn iter_errors(
        &self,
    ) -> impl Iterator<Item = (ApiSpecFileWhich<'_>, &CheckStale)> {
        std::iter::once((ApiSpecFileWhich::Openapi, &self.openapi_doc))
            .chain(self.extra_files.iter().map(|(file_name, status)| {
                (ApiSpecFileWhich::Extra(file_name), status)
            }))
            .filter_map(|(spec_file, status)| {
                if let CheckStatus::Stale(e) = status {
                    Some((spec_file, e))
                } else {
                    None
                }
            })
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ApiSpecFileWhich<'a> {
    Openapi,
    Extra(&'a Utf8Path),
}

#[derive(Debug)]
#[must_use]
pub(crate) enum CheckStatus {
    Fresh,
    Stale(CheckStale),
}

#[derive(Debug)]
#[must_use]
pub(crate) enum CheckStale {
    Modified { full_path: Utf8PathBuf, actual: Vec<u8>, expected: Vec<u8> },
    New,
}
