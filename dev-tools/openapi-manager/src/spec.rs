// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{fmt, io::Write};

use anyhow::{Context, Result};
use atomicwrites::AtomicFile;
use camino::{Utf8Path, Utf8PathBuf};
use dropshot::{ApiDescription, ApiDescriptionBuildErrors, StubContext};
use fs_err as fs;
use openapiv3::OpenAPI;

/// All APIs managed by openapi-manager.
pub fn all_apis() -> Vec<ApiSpec> {
    vec![
        ApiSpec {
            title: "Installinator API".to_string(),
            version: "0.0.1".to_string(),
            description: "API for installinator to fetch artifacts \
                and report progress"
                .to_string(),
            boundary: ApiBoundary::Internal,
            api_description:
                installinator_api::installinator_api::stub_api_description,
            filename: "installinator.json".to_string(),
            extra_validation: None,
        },
        ApiSpec {
            title: "Nexus internal API".to_string(),
            version: "0.0.1".to_string(),
            description: "Nexus internal API".to_string(),
            boundary: ApiBoundary::Internal,
            api_description:
                nexus_internal_api::nexus_internal_api_mod::stub_api_description,
            filename: "nexus-internal.json".to_string(),
            extra_validation: None,
        },
        // Add your APIs here! Please keep this list sorted by filename.
    ]
}

pub struct ApiSpec {
    /// The title.
    pub title: String,

    /// The version.
    pub version: String,

    /// The description string.
    pub description: String,

    /// Whether this API is internal or external.
    pub boundary: ApiBoundary,

    /// The API description function, typically a reference to
    /// `stub_api_description`.
    pub api_description:
        fn() -> Result<ApiDescription<StubContext>, ApiDescriptionBuildErrors>,

    /// The JSON filename to write the API description to.
    pub filename: String,

    /// Extra validation to perform on the OpenAPI spec, if any.
    pub extra_validation: Option<fn(&OpenAPI) -> anyhow::Result<()>>,
}

impl ApiSpec {
    pub(crate) fn overwrite(
        &self,
        dir: &Utf8Path,
    ) -> Result<(OverwriteStatus, DocumentSummary)> {
        let contents = self.to_json_bytes()?;

        let summary = self
            .validate_json(&contents)
            .context("OpenAPI document validation failed")?;

        let full_path = dir.join(&self.filename);
        let status = overwrite_file(&full_path, &contents)?;

        Ok((status, summary))
    }

    pub(crate) fn check(&self, dir: &Utf8Path) -> Result<CheckStatus> {
        let contents = self.to_json_bytes()?;
        let summary = self
            .validate_json(&contents)
            .context("OpenAPI document validation failed")?;

        let full_path = dir.join(&self.filename);
        let existing_contents =
            read_opt(&full_path).context("failed to read contents on disk")?;

        match existing_contents {
            Some(existing_contents) if existing_contents == contents => {
                Ok(CheckStatus::Ok(summary))
            }
            Some(existing_contents) => Ok(CheckStatus::Stale {
                full_path,
                actual: existing_contents,
                expected: contents,
            }),
            None => Ok(CheckStatus::Missing),
        }
    }

    pub(crate) fn to_openapi_doc(&self) -> Result<OpenAPI> {
        // It's a bit weird to first convert to bytes and then back to OpenAPI,
        // but this is the easiest way to do so (currently, Dropshot doesn't
        // return the OpenAPI type directly). It is also consistent with the
        // other code paths.
        let contents = self.to_json_bytes()?;
        contents_to_openapi(&contents)
    }

    fn to_json_bytes(&self) -> Result<Vec<u8>> {
        let description = (self.api_description)().map_err(|error| {
            // ApiDescriptionBuildError is actually a list of errors so it
            // doesn't implement std::error::Error itself. Its Display
            // impl formats the errors appropriately.
            anyhow::anyhow!("{}", error)
        })?;
        let mut openapi_def = description.openapi(&self.title, &self.version);
        openapi_def
            .description(&self.description)
            .contact_url("https://oxide.computer")
            .contact_email("api@oxide.computer");

        // Use write because it's the most reliable way to get the canonical
        // JSON order. The `json` method returns a serde_json::Value which may
        // or may not have preserve_order enabled.
        let mut contents = Vec::new();
        openapi_def.write(&mut contents)?;
        Ok(contents)
    }

    fn validate_json(&self, contents: &[u8]) -> Result<DocumentSummary> {
        let openapi_doc = contents_to_openapi(contents)
            .context("JSON returned by ApiDescription is not valid OpenAPI")?;

        // Check for lint errors.
        let errors = match self.boundary {
            ApiBoundary::Internal => openapi_lint::validate(&openapi_doc),
            ApiBoundary::External => {
                openapi_lint::validate_external(&openapi_doc)
            }
        };
        if !errors.is_empty() {
            return Err(anyhow::anyhow!("{}", errors.join("\n\n")));
        }

        if let Some(extra_validation) = self.extra_validation {
            extra_validation(&openapi_doc)?;
        }

        Ok(DocumentSummary::new(&openapi_doc))
    }
}

fn contents_to_openapi(contents: &[u8]) -> Result<OpenAPI> {
    serde_json::from_slice(&contents)
        .context("JSON returned by ApiDescription is not valid OpenAPI")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApiBoundary {
    Internal,
    #[allow(dead_code)] // Remove this when we start managing an external API.
    External,
}

impl fmt::Display for ApiBoundary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiBoundary::Internal => write!(f, "internal"),
            ApiBoundary::External => write!(f, "external"),
        }
    }
}

#[derive(Debug)]
#[must_use]
pub(crate) enum OverwriteStatus {
    Updated,
    Unchanged,
}

#[derive(Debug)]
#[must_use]
pub(crate) enum CheckStatus {
    Ok(DocumentSummary),
    Stale { full_path: Utf8PathBuf, actual: Vec<u8>, expected: Vec<u8> },
    Missing,
}

#[derive(Debug)]
#[must_use]
pub(crate) struct DocumentSummary {
    pub(crate) path_count: usize,
    // None if data is missing.
    pub(crate) schema_count: Option<usize>,
}

impl DocumentSummary {
    fn new(doc: &OpenAPI) -> Self {
        Self {
            path_count: doc.paths.paths.len(),
            schema_count: doc
                .components
                .as_ref()
                .map_or(None, |c| Some(c.schemas.len())),
        }
    }
}

pub(crate) fn openapi_dir(dir: Option<Utf8PathBuf>) -> Result<Utf8PathBuf> {
    match dir {
        Some(dir) => Ok(dir.canonicalize_utf8().with_context(|| {
            format!("failed to canonicalize directory: {}", dir)
        })?),
        None => find_openapi_dir().context("failed to find openapi directory"),
    }
}

pub(crate) fn find_openapi_dir() -> Result<Utf8PathBuf> {
    let mut root = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // This crate is two levels down from the root of omicron, so go up twice.
    root.pop();
    root.pop();

    root.push("openapi");
    let root = root.canonicalize_utf8().with_context(|| {
        format!("failed to canonicalize openapi directory: {}", root)
    })?;

    if !root.is_dir() {
        anyhow::bail!("openapi root is not a directory: {}", root);
    }

    Ok(root)
}

/// Overwrite a file with new contents, if the contents are different.
///
/// The file is left unchanged if the contents are the same. That's to avoid
/// mtime-based recompilations.
fn overwrite_file(path: &Utf8Path, contents: &[u8]) -> Result<OverwriteStatus> {
    // Only overwrite the file if the contents are actually different.
    let existing_contents =
        read_opt(path).context("failed to read contents on disk")?;

    // None means the file doesn't exist, in which case we always want to write
    // the new contents.
    if existing_contents.as_deref() == Some(contents) {
        return Ok(OverwriteStatus::Unchanged);
    }

    AtomicFile::new(path, atomicwrites::OverwriteBehavior::AllowOverwrite)
        .write(|f| f.write_all(contents))
        .with_context(|| format!("failed to write to `{}`", path))?;

    Ok(OverwriteStatus::Updated)
}

fn read_opt(path: &Utf8Path) -> std::io::Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => return Err(err),
    }
}
