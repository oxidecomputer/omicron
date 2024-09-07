// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{fmt, io::Write};

use anyhow::{Context, Result};
use atomicwrites::AtomicFile;
use camino::{Utf8Path, Utf8PathBuf};
use dropshot::{ApiDescription, ApiDescriptionBuildErrors, StubContext};
use fs_err as fs;
use openapi_manager_types::{ValidationBackend, ValidationContext};
use openapiv3::OpenAPI;

/// All APIs managed by openapi-manager.
pub fn all_apis() -> Vec<ApiSpec> {
    vec![
        ApiSpec {
            title: "Bootstrap Agent API",
            version: "0.0.1",
            description: "Per-sled API for setup and teardown",
            boundary: ApiBoundary::Internal,
            api_description:
                bootstrap_agent_api::bootstrap_agent_api_mod::stub_api_description,
            filename: "bootstrap-agent.json",
            extra_validation: None,
        },
        ApiSpec {
            title: "ClickHouse Cluster Admin API",
            version: "0.0.1",
            description: "API for interacting with the Oxide \
                control plane's ClickHouse cluster",
            boundary: ApiBoundary::Internal,
            api_description:
                clickhouse_admin_api::clickhouse_admin_api_mod::stub_api_description,
            filename: "clickhouse-admin.json",
            extra_validation: None,
        },
        ApiSpec {
            title: "CockroachDB Cluster Admin API",
            version: "0.0.1",
            description: "API for interacting with the Oxide \
                control plane's CockroachDB cluster",
            boundary: ApiBoundary::Internal,
            api_description:
                cockroach_admin_api::cockroach_admin_api_mod::stub_api_description,
            filename: "cockroach-admin.json",
            extra_validation: None,
        },
        ApiSpec {
            title: "Oxide Management Gateway Service API",
            version: "0.0.1",
            description: "API for interacting with the Oxide \
                control plane's gateway service",
            boundary: ApiBoundary::Internal,
            api_description:
                gateway_api::gateway_api_mod::stub_api_description,
            filename: "gateway.json",
            extra_validation: None,
        },
        ApiSpec {
            title: "Internal DNS",
            version: "0.0.1",
            description: "API for the internal DNS server",
            boundary: ApiBoundary::Internal,
            api_description:
                dns_server_api::dns_server_api_mod::stub_api_description,
            filename: "dns-server.json",
            extra_validation: None,
        },
        ApiSpec {
            title: "Installinator API",
            version: "0.0.1",
            description: "API for installinator to fetch artifacts \
                and report progress",
            boundary: ApiBoundary::Internal,
            api_description:
                installinator_api::installinator_api_mod::stub_api_description,
            filename: "installinator.json",
            extra_validation: None,
        },
        ApiSpec {
            title: "Oxide Region API",
            version: "20240821.0",
            description: "API for interacting with the Oxide control plane",
            boundary: ApiBoundary::External,
            api_description:
                nexus_external_api::nexus_external_api_mod::stub_api_description,
            filename: "nexus.json",
            extra_validation: Some(nexus_external_api::validate_api),
        },
        ApiSpec {
            title: "Nexus internal API",
            version: "0.0.1",
            description: "Nexus internal API",
            boundary: ApiBoundary::Internal,
            api_description:
                nexus_internal_api::nexus_internal_api_mod::stub_api_description,
            filename: "nexus-internal.json",
            extra_validation: None,
        },
        ApiSpec {
            title: "Oxide Oximeter API",
            version: "0.0.1",
            description: "API for interacting with oximeter",
            boundary: ApiBoundary::Internal,
            api_description:
                oximeter_api::oximeter_api_mod::stub_api_description,
            filename: "oximeter.json",
            extra_validation: None,
        },
        ApiSpec {
            title: "Oxide Sled Agent API",
            version: "0.0.1",
            description: "API for interacting with individual sleds",
            boundary: ApiBoundary::Internal,
            api_description:
                sled_agent_api::sled_agent_api_mod::stub_api_description,
            filename: "sled-agent.json",
            extra_validation: None,
        },
        ApiSpec {
            title: "Oxide Technician Port Control Service",
            version: "0.0.1",
            description: "API for use by the technician port TUI: wicket",
            boundary: ApiBoundary::Internal,
            api_description: wicketd_api::wicketd_api_mod::stub_api_description,
            filename: "wicketd.json",
            extra_validation: None,
        },
        // Add your APIs here! Please keep this list sorted by filename.
    ]
}

pub struct ApiSpec {
    /// The title.
    pub title: &'static str,

    /// The version.
    pub version: &'static str,

    /// The description string.
    pub description: &'static str,

    /// Whether this API is internal or external.
    pub boundary: ApiBoundary,

    /// The API description function, typically a reference to
    /// `stub_api_description`.
    pub api_description:
        fn() -> Result<ApiDescription<StubContext>, ApiDescriptionBuildErrors>,

    /// The JSON filename to write the API description to.
    pub filename: &'static str,

    /// Extra validation to perform on the OpenAPI spec, if any.
    pub extra_validation: Option<fn(&OpenAPI, ValidationContext<'_>)>,
}

impl ApiSpec {
    pub(crate) fn overwrite(
        &self,
        env: &Environment,
    ) -> Result<SpecOverwriteStatus> {
        let contents = self.to_json_bytes()?;

        let (summary, validation_result) = self
            .validate_json(&contents)
            .context("OpenAPI document validation failed")?;

        let full_path = env.openapi_dir.join(&self.filename);
        let openapi_doc_status = overwrite_file(&full_path, &contents)?;

        let extra_files = validation_result
            .extra_files
            .into_iter()
            .map(|(path, contents)| {
                let full_path = env.workspace_root.join(&path);
                let status = overwrite_file(&full_path, &contents)?;
                Ok((path, status))
            })
            .collect::<Result<_>>()?;

        Ok(SpecOverwriteStatus {
            summary,
            openapi_doc: openapi_doc_status,
            extra_files,
        })
    }

    pub(crate) fn check(&self, env: &Environment) -> Result<SpecCheckStatus> {
        let contents = self.to_json_bytes()?;
        let (summary, validation_result) = self
            .validate_json(&contents)
            .context("OpenAPI document validation failed")?;

        let full_path = env.openapi_dir.join(&self.filename);
        let openapi_doc_status = check_file(full_path, contents)?;

        let extra_files = validation_result
            .extra_files
            .into_iter()
            .map(|(path, contents)| {
                let full_path = env.workspace_root.join(&path);
                let status = check_file(full_path, contents)?;
                Ok((path, status))
            })
            .collect::<Result<_>>()?;

        Ok(SpecCheckStatus {
            summary,
            openapi_doc: openapi_doc_status,
            extra_files,
        })
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

    fn validate_json(
        &self,
        contents: &[u8],
    ) -> Result<(DocumentSummary, ValidationResult)> {
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

        let extra_files = if let Some(extra_validation) = self.extra_validation
        {
            let mut validation_context =
                ValidationContextImpl { errors: Vec::new(), files: Vec::new() };
            extra_validation(
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

            validation_context.files
        } else {
            Vec::new()
        };

        Ok((
            DocumentSummary::new(&openapi_doc),
            ValidationResult { extra_files },
        ))
    }
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
pub(crate) struct SpecOverwriteStatus {
    pub(crate) summary: DocumentSummary,
    openapi_doc: OverwriteStatus,
    extra_files: Vec<(Utf8PathBuf, OverwriteStatus)>,
}

impl SpecOverwriteStatus {
    pub(crate) fn updated_count(&self) -> usize {
        self.iter()
            .filter(|(_, status)| matches!(status, OverwriteStatus::Updated))
            .count()
    }

    fn iter(
        &self,
    ) -> impl Iterator<Item = (ApiSpecFile<'_>, &OverwriteStatus)> {
        std::iter::once((ApiSpecFile::Openapi, &self.openapi_doc)).chain(
            self.extra_files.iter().map(|(file_name, status)| {
                (ApiSpecFile::Extra(file_name), status)
            }),
        )
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
    ) -> impl Iterator<Item = (ApiSpecFile<'_>, &CheckStale)> {
        std::iter::once((ApiSpecFile::Openapi, &self.openapi_doc))
            .chain(self.extra_files.iter().map(|(file_name, status)| {
                (ApiSpecFile::Extra(file_name), status)
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
pub(crate) enum ApiSpecFile<'a> {
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

#[derive(Debug)]
#[must_use]
struct ValidationResult {
    // Extra files recorded by the validation context.
    extra_files: Vec<(Utf8PathBuf, Vec<u8>)>,
}

pub(crate) struct Environment {
    pub(crate) workspace_root: Utf8PathBuf,
    pub(crate) openapi_dir: Utf8PathBuf,
}

impl Environment {
    pub(crate) fn new(openapi_dir: Option<Utf8PathBuf>) -> Result<Self> {
        let mut root = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        // This crate is two levels down from the root of omicron, so go up twice.
        root.pop();
        root.pop();

        let workspace_root = root.canonicalize_utf8().with_context(|| {
            format!("failed to canonicalize workspace root: {}", root)
        })?;

        let openapi_dir =
            openapi_dir.unwrap_or_else(|| workspace_root.join("openapi"));
        let openapi_dir =
            openapi_dir.canonicalize_utf8().with_context(|| {
                format!(
                    "failed to canonicalize openapi directory: {}",
                    openapi_dir
                )
            })?;

        if !openapi_dir.is_dir() {
            anyhow::bail!("openapi root is not a directory: {}", root);
        }

        Ok(Self { workspace_root, openapi_dir })
    }
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

/// Check a file against expected contents.
fn check_file(
    full_path: Utf8PathBuf,
    contents: Vec<u8>,
) -> Result<CheckStatus> {
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

fn read_opt(path: &Utf8Path) -> std::io::Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => return Err(err),
    }
}
