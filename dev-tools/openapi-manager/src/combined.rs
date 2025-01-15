// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Holistic view of API information available, based on both Rust-defined
//! configuration and the OpenAPI documents present

use crate::apis::{ApiIdent, ManagedApi, ManagedApis};
use crate::spec_files::{AllApiSpecFiles, ApiSpecFile, ApiSpecFileName};
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
    pub fn new(
        api_list: ManagedApis,
        all_spec_files: AllApiSpecFiles,
    ) -> CombinedApis {
        let apis = api_list.into_map();
        let all_spec_files = all_spec_files.into_map();

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
            for api_version in managed_api.iter_versions() {
                if api_versions.and_then(|vs| vs.get(api_version)).is_some() {
                    problems.push(Problem::MissingSpecFile {
                        api_ident: api_ident.clone(),
                        version: api_version.clone(),
                    });
                };
            }
        }

        CombinedApis { apis, versions, loaded_spec_files, problems }
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
