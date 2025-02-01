// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::apis::ApiBoundary;
use crate::apis::ManagedApi;
use camino::Utf8PathBuf;
use openapi_manager_types::ValidationBackend;
use openapi_manager_types::ValidationContext;
use openapiv3::OpenAPI;

pub fn validate_generated_openapi_document(
    api: &ManagedApi,
    openapi_doc: &OpenAPI,
) -> anyhow::Result<ValidationResult> {
    // Check for lint errors.
    let errors = match api.boundary() {
        ApiBoundary::Internal => openapi_lint::validate(&openapi_doc),
        ApiBoundary::External => openapi_lint::validate_external(&openapi_doc),
    };
    if !errors.is_empty() {
        return Err(anyhow::anyhow!("{}", errors.join("\n\n")));
    }

    // Perform any additional API-specific validation.
    let mut validation_context =
        ValidationContextImpl { errors: Vec::new(), files: Vec::new() };
    api.extra_validation(
        &openapi_doc,
        ValidationContext::new(&mut validation_context),
    );

    if !validation_context.errors.is_empty() {
        return Err(anyhow::anyhow!(
            "OpenAPI document extended validation failed:\n{}",
            validation_context
                .errors
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("\n")
        ));
    }

    Ok(ValidationResult { extra_files: validation_context.files })
}

#[derive(Debug)]
#[must_use]
pub(crate) struct DocumentSummary {
    pub(crate) path_count: usize,
    // None if data is missing.
    pub(crate) schema_count: Option<usize>,
}

impl DocumentSummary {
    pub(crate) fn new(doc: &OpenAPI) -> Self {
        Self {
            path_count: doc.paths.paths.len(),
            schema_count: doc
                .components
                .as_ref()
                .map_or(None, |c| Some(c.schemas.len())),
        }
    }
}

#[derive(Debug)]
#[must_use]
pub struct ValidationResult {
    // Extra files recorded by the validation context.
    // XXX-dap remove pub when I remove spec.rs
    pub(crate) extra_files: Vec<(Utf8PathBuf, Vec<u8>)>,
}

struct ValidationContextImpl {
    errors: Vec<anyhow::Error>,
    files: Vec<(Utf8PathBuf, Vec<u8>)>,
}

impl ValidationBackend for ValidationContextImpl {
    fn report_error(&mut self, error: anyhow::Error) {
        self.errors.push(error);
    }

    fn record_file_contents(&mut self, path: Utf8PathBuf, contents: Vec<u8>) {
        self.files.push((path, contents));
    }
}
