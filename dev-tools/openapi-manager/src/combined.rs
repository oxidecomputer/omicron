// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Holistic view of API information available, based on both Rust-defined
//! configuration and the OpenAPI documents present

use crate::apis::{ApiIdent, ManagedApi};
use crate::spec_files::{ApiSpecFile, ApiSpecFileName};
use std::collections::BTreeMap;
use std::fmt::Display;
use thiserror::Error;

pub struct CombinedApis {
    apis: BTreeMap<ApiIdent, ManagedApi>,
    versions: BTreeMap<ApiIdent, BTreeMap<semver::Version, ApiSpecFileName>>,
    loaded_spec_files: BTreeMap<ApiSpecFileName, ApiSpecFile>,
    problems: Vec<Problem>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct DisplayableVec<T>(Vec<T>);
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
        "found extra OpenAPI document \
         (no corresponding version defined in Rust): {spec_file_name}"
    )]
    ExtraSpecFile { spec_file_name: ApiSpecFileName },

    #[error("missing OpenAPI document for version {version} defined in Rust")]
    MissingSpecFile { version: semver::Version },

    #[error(
        "multiple OpenAPI documents have the same version ({version}): \
         {spec_file_names}"
    )]
    DuplicateSpecFiles {
        version: semver::Version,
        spec_file_names: DisplayableVec<ApiSpecFileName>,
    },
}
