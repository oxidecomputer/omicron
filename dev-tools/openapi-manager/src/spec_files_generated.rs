// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Generated OpenAPI spec files

use crate::{
    apis::{ApiIdent, ManagedApis},
    spec_files_generic::{
        ApiFiles, ApiSpecFile, ApiSpecFileName, ApiSpecFilesBuilder,
    },
};
use std::collections::BTreeMap;

/// Container for OpenAPI spec files generated by the code in this repo
///
/// Most validation is not done at this point.
// XXX-dap see comments on BlessedFiles
#[derive(Debug)]
pub struct GeneratedFiles {
    pub spec_files: BTreeMap<ApiIdent, ApiFiles<GeneratedApiSpecFile>>,
    pub errors: Vec<anyhow::Error>,
    pub warnings: Vec<anyhow::Error>,
}

pub struct GeneratedApiSpecFile(ApiSpecFile);
NewtypeDebug! { () pub struct GeneratedApiSpecFile(ApiSpecFile); }
NewtypeDeref! { () pub struct GeneratedApiSpecFile(ApiSpecFile); }
NewtypeDerefMut! { () pub struct GeneratedApiSpecFile(ApiSpecFile); }
NewtypeFrom! { () pub struct GeneratedApiSpecFile(ApiSpecFile); }

impl GeneratedFiles {
    pub fn generate(apis: &ManagedApis) -> anyhow::Result<GeneratedFiles> {
        let mut api_files = ApiSpecFilesBuilder::new(apis);

        for api in apis.iter_apis() {
            if api.is_lockstep() {
                for version in api.iter_versions_semver() {
                    let contents = api.generate_spec_bytes(version)?;
                    let file_name = ApiSpecFileName::for_lockstep(api);
                    api_files.load_contents(file_name, contents);
                }
            } else {
                // unwrap(): this returns `Some` for versioned APIs.
                let supported_versions = api.iter_versioned_versions().unwrap();
                let mut latest = None;
                for supported_version in supported_versions {
                    let version = supported_version.semver();
                    let contents = api.generate_spec_bytes(version)?;
                    let file_name = ApiSpecFileName::for_versioned(
                        api,
                        version.clone(),
                        &contents,
                    );
                    latest = Some(file_name.clone());
                    api_files.load_contents(file_name, contents);
                }

                // unwrap(): there must have been at least one version
                api_files.load_latest_link(
                    api.ident(),
                    latest.expect("at least one version of supported API"),
                    false,
                );
            }
        }

        Ok(Self::from(api_files))
    }
}

impl<'a> From<ApiSpecFilesBuilder<'a>> for GeneratedFiles {
    fn from(api_files: ApiSpecFilesBuilder<'a>) -> Self {
        let (spec_files, errors, warnings) = api_files.into_parts();
        GeneratedFiles { spec_files, errors, warnings }
    }
}
