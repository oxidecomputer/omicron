// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Working with OpenAPI specification files in the repository

use camino::Utf8Path;
use openapiv3::OpenAPI;
use std::collections::BTreeMap;
use crate::apis::{ApiIdent, ManagedApis};

/// Container for all the OpenAPI spec files found
///
/// Most validation is not done at this point.
struct AllApiSpecFiles {
    apis: BTreeMap<ApiIdent, Vec<ApiSpecFile>>,
    warnings: Vec<anyhow::Error>,
}

impl AllApiSpecFiles {
    pub fn load_from_directory(
        dir: &Utf8Path,
        apis: &ManagedApis,
    ) -> anyhow::Result<AllApiSpecFiles> {
        // XXX-dap
        // - walk the directory tree, which is expected to look in a particular
        // way
        // - parse file names
        // - parse file contents
        // - produce warnings if we can't parse anything
        // XXX-dap one question is whether we should take as *configuration*
        // whether we expect a given API to be lockstep or versioned so that we
        // don't guess at the parsing.  I kind of feel like it should work this
        // way.
        todo!();
    }

    pub fn apis(&self) -> impl Iterator<Item = &ApiIdent> + '_ {
        self.apis.keys()
    }

    pub fn api_spec_files(
        &self,
        ident: &ApiIdent,
    ) -> Option<impl Iterator<Item = &ApiSpecFile> + '_> {
        self.apis.get(ident).map(|files| files.iter())
    }
}

/// Describes the path to an OpenAPI document file
// XXX-dap spec -> document?
struct ApiSpecFileName {
    ident: ApiIdent,
    kind: ApiSpecFileNameKind,
}

impl ApiSpecFileName {
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
