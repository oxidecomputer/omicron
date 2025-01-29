// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Holistic view of API information available, based on both Rust-defined
//! configuration and the OpenAPI documents present

// XXX-dap delete me

use crate::apis::{ApiIdent, ManagedApi, ManagedApis};
use crate::resolved::DisplayableVec;
pub(crate) use crate::resolved::{check_file, read_opt};
pub(crate) use crate::resolved::{CheckStale, CheckStatus};
use crate::spec::Environment;
use crate::spec_files_generic::{ApiSpecFile, ApiSpecFileName};
use crate::spec_files_local::LocalFiles;
use crate::validation::{validate_generated_openapi_document, DocumentSummary};
use anyhow::Context;
use camino::{Utf8Path, Utf8PathBuf};

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
