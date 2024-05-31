// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::io::Write;

use anyhow::{Context, Result};
use atomicwrites::AtomicFile;
use camino::{Utf8Path, Utf8PathBuf};
use dropshot::{ApiDescription, StubContext};
use fs_err as fs;
use nexus_internal_api::NexusInternalApiFactory;
use openapiv3::OpenAPI;

/// All APIs managed by apigen.
pub fn all_apis() -> Vec<ApiSpec> {
    vec![ApiSpec {
        title: "Nexus internal API".to_string(),
        version: "0.0.1".to_string(),
        description: "Nexus internal API".to_string(),
        api_description: NexusInternalApiFactory::stub_api_description()
            .expect("Nexus internal API generated successfully"),
        filename: "nexus-internal.json".to_string(),
    }]
}

pub struct ApiSpec {
    /// The title.
    pub title: String,

    /// The version.
    pub version: String,

    /// The description string.
    pub description: String,

    /// The API description.
    pub api_description: ApiDescription<StubContext>,

    /// The JSON filename to write the API description to.
    pub filename: String,
}

impl ApiSpec {
    pub(crate) fn overwrite(&self, dir: &Utf8Path) -> Result<OverwriteStatus> {
        let contents = self.to_json_bytes()?;
        validate_json(&contents).with_context(|| {
            format!("failed to validate JSON for `{}`", self.filename)
        })?;

        let full_path = dir.join(&self.filename);
        overwrite_file(&full_path, &contents)
    }

    pub(crate) fn check(&self, dir: &Utf8Path) -> Result<CheckStatus> {
        let contents = self.to_json_bytes()?;
        validate_json(&contents).with_context(|| {
            format!("failed to validate JSON for `{}`", self.filename)
        })?;

        let full_path = dir.join(&self.filename);
        let existing_contents =
            read_opt(&full_path).context("failed to read contents on disk")?;

        match existing_contents {
            Some(existing_contents) if existing_contents == contents => {
                Ok(CheckStatus::Ok)
            }
            Some(existing_contents) => Ok(CheckStatus::Mismatch {
                full_path,
                actual: existing_contents,
                expected: contents,
            }),
            None => Ok(CheckStatus::Missing { full_path }),
        }
    }

    pub(crate) fn to_json_bytes(&self) -> Result<Vec<u8>> {
        let mut description =
            self.api_description.openapi(&self.title, &self.version);
        // Use write because it's the most reliable way to get the canonical
        // JSON order. The `json` method returns a serde_json::Value which may
        // or may not have preserve_order enabled.
        let mut contents = Vec::new();
        description
            .description(&self.description)
            .contact_url("https://oxide.computer")
            .contact_email("api@oxide.computer")
            .write(&mut contents)?;
        Ok(contents)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub(crate) enum OverwriteStatus {
    Unchanged,
    Updated,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use]
pub(crate) enum CheckStatus {
    Ok,
    Mismatch { full_path: Utf8PathBuf, actual: Vec<u8>, expected: Vec<u8> },
    Missing { full_path: Utf8PathBuf },
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
    // This is two levels down, so go up twice.
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

fn validate_json(json: &[u8]) -> Result<()> {
    let spec = serde_json::from_slice::<OpenAPI>(json)
        .context("JSON returned by ApiDescription is not valid OpenAPI")?;

    // Check for lint errors.
    let errors = openapi_lint::validate(&spec);
    if !errors.is_empty() {
        return Err(anyhow::anyhow!("{}", errors.join("\n\n")));
    }

    Ok(())
}

/// Overwrite a file with new contents, if the contents are different.
///
/// The file is left unchanged if the contents are the same. That's to avoid
/// mtime-based recompilations.
fn overwrite_file(path: &Utf8Path, contents: &[u8]) -> Result<OverwriteStatus> {
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
    // Only overwrite the file if the contents are actually different.
    match fs::read(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => return Err(err),
    }
}
