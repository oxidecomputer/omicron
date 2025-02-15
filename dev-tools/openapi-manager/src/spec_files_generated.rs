// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Newtype and collection to represent OpenAPI documents generated from the
//! API definitions

// XXX-dap consider centralizing the load-time error reporting
// XXX-dap then consider making these type defs instead of structs

use crate::{
    apis::{ApiIdent, ManagedApis},
    spec_files_generic::{
        ApiFiles, ApiLoad, ApiSpecFile, ApiSpecFileName, ApiSpecFilesBuilder,
        AsRawFiles,
    },
};
use anyhow::bail;
use std::{collections::BTreeMap, ops::Deref};

/// Newtype wrapper around [`ApiSpecFile`] to describe OpenAPI documents
/// generated from API definitions
///
/// This includes documents for lockstep APIs and versioned APIs, for both
/// blessed and locally-added versions.
pub struct GeneratedApiSpecFile(ApiSpecFile);
NewtypeDebug! { () pub struct GeneratedApiSpecFile(ApiSpecFile); }
NewtypeDeref! { () pub struct GeneratedApiSpecFile(ApiSpecFile); }
NewtypeDerefMut! { () pub struct GeneratedApiSpecFile(ApiSpecFile); }
NewtypeFrom! { () pub struct GeneratedApiSpecFile(ApiSpecFile); }

// Trait impls that allow us to use `ApiFiles<GeneratedApiSpecFile>`
//
// Note that this is NOT a `Vec` because it's NOT allowed to have more than one
// GeneratedApiSpecFile for a given version.

impl ApiLoad for GeneratedApiSpecFile {
    const MISCONFIGURATIONS_ALLOWED: bool = false;

    fn make_item(raw: ApiSpecFile) -> Self {
        GeneratedApiSpecFile(raw)
    }

    fn try_extend(&mut self, item: ApiSpecFile) -> anyhow::Result<()> {
        // This should be impossible.
        bail!(
            "found more than one generated OpenAPI document for a given \
             API version: at least {} and {}",
            self.spec_file_name(),
            item.spec_file_name()
        );
    }
}

impl AsRawFiles for GeneratedApiSpecFile {
    fn as_raw_files<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = &'a ApiSpecFile> + 'a> {
        Box::new(std::iter::once(self.deref()))
    }
}

/// Container for OpenAPI spec files generated from API definitions
///
/// **Be sure to check for load errors and warnings before using this
/// structure.**
///
/// For more on what's been validated at this point, see
/// [`ApiSpecFilesBuilder`].
#[derive(Debug)]
pub struct GeneratedFiles {
    pub spec_files: BTreeMap<ApiIdent, ApiFiles<GeneratedApiSpecFile>>,

    /// load failures indicating that the loaded information is wrong or
    /// incomplete
    pub errors: Vec<anyhow::Error>,

    /// load-time failures that should not affect the validity of the loaded
    /// data
    pub warnings: Vec<anyhow::Error>,
}

impl GeneratedFiles {
    /// Generate OpenAPI documents for all supported versions of all managed
    /// APIs
    pub fn generate(apis: &ManagedApis) -> anyhow::Result<GeneratedFiles> {
        let mut api_files: ApiSpecFilesBuilder<GeneratedApiSpecFile> =
            ApiSpecFilesBuilder::new(apis);

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
                );
            }
        }

        Ok(Self::from(api_files))
    }
}

impl<'a> From<ApiSpecFilesBuilder<'a, GeneratedApiSpecFile>>
    for GeneratedFiles
{
    fn from(api_files: ApiSpecFilesBuilder<'a, GeneratedApiSpecFile>) -> Self {
        let (spec_files, errors, warnings) = api_files.into_parts();
        GeneratedFiles { spec_files, errors, warnings }
    }
}
