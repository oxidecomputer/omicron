// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// XXX-dap delete me

use std::io::Write;

use crate::apis::{ApiBoundary, Versions};
use crate::resolved::{check_file, read_opt};
pub(crate) use crate::resolved::{CheckStale, CheckStatus};
pub(crate) use crate::validation::DocumentSummary;
use crate::validation::{ValidationContextImpl, ValidationResult};
use anyhow::{Context, Result};
use atomicwrites::AtomicFile;
use camino::{Utf8Path, Utf8PathBuf};
use dropshot::{ApiDescription, ApiDescriptionBuildErrors, StubContext};
use openapi_manager_types::ValidationContext;
use openapiv3::OpenAPI;

/// All APIs managed by openapi-manager.
// TODO The metadata here overlaps with metadata in api-manifest.toml.
pub fn all_apis() -> Vec<ApiSpec> {
    vec![
        ApiSpec {
            title: "Bootstrap Agent API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "Per-sled API for setup and teardown",
            boundary: ApiBoundary::Internal,
            api_description:
                bootstrap_agent_api::bootstrap_agent_api_mod::stub_api_description,
            file_stem: "bootstrap-agent",
            extra_validation: None,
        },
        ApiSpec {
            title: "ClickHouse Cluster Admin Keeper API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "API for interacting with the Oxide \
                control plane's ClickHouse cluster keepers",
            boundary: ApiBoundary::Internal,
            api_description:
                clickhouse_admin_api::clickhouse_admin_keeper_api_mod::stub_api_description,
            file_stem: "clickhouse-admin-keeper",
            extra_validation: None,
        },
        ApiSpec {
            title: "ClickHouse Cluster Admin Server API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "API for interacting with the Oxide \
                control plane's ClickHouse cluster replica servers",
            boundary: ApiBoundary::Internal,
            api_description:
                clickhouse_admin_api::clickhouse_admin_server_api_mod::stub_api_description,
            file_stem: "clickhouse-admin-server",
            extra_validation: None,
        },
        ApiSpec {
            title: "ClickHouse Single-Node Admin Server API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "API for interacting with the Oxide \
                control plane's single-node ClickHouse database",
            boundary: ApiBoundary::Internal,
            api_description:
                clickhouse_admin_api::clickhouse_admin_single_api_mod::stub_api_description,
            file_stem: "clickhouse-admin-single",
            extra_validation: None,
        },
        ApiSpec {
            title: "CockroachDB Cluster Admin API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "API for interacting with the Oxide \
                control plane's CockroachDB cluster",
            boundary: ApiBoundary::Internal,
            api_description:
                cockroach_admin_api::cockroach_admin_api_mod::stub_api_description,
            file_stem: "cockroach-admin",
            extra_validation: None,
        },
        ApiSpec {
            title: "Oxide Management Gateway Service API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "API for interacting with the Oxide \
                control plane's gateway service",
            boundary: ApiBoundary::Internal,
            api_description:
                gateway_api::gateway_api_mod::stub_api_description,
            file_stem: "gateway",
            extra_validation: None,
        },
        ApiSpec {
            title: "Internal DNS",
            versions: Versions::new_versioned(
                dns_server_api::supported_versions()
            ),
            description: "API for the internal DNS server",
            boundary: ApiBoundary::Internal,
            api_description:
                dns_server_api::dns_server_api_mod::stub_api_description,
            file_stem: "dns-server",
            extra_validation: None,
        },
        ApiSpec {
            title: "Installinator API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "API for installinator to fetch artifacts \
                and report progress",
            boundary: ApiBoundary::Internal,
            api_description:
                installinator_api::installinator_api_mod::stub_api_description,
            file_stem: "installinator",
            extra_validation: None,
        },
        ApiSpec {
            title: "Oxide Region API",
            versions: Versions::Lockstep { version: semver::Version::new(20241204,0,0) },
            description: "API for interacting with the Oxide control plane",
            boundary: ApiBoundary::External,
            api_description:
                nexus_external_api::nexus_external_api_mod::stub_api_description,
            file_stem: "nexus",
            extra_validation: Some(nexus_external_api::validate_api),
        },
        ApiSpec {
            title: "Nexus internal API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "Nexus internal API",
            boundary: ApiBoundary::Internal,
            api_description:
                nexus_internal_api::nexus_internal_api_mod::stub_api_description,
            file_stem: "nexus-internal",
            extra_validation: None,
        },
        ApiSpec {
            title: "Oxide Oximeter API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "API for interacting with oximeter",
            boundary: ApiBoundary::Internal,
            api_description:
                oximeter_api::oximeter_api_mod::stub_api_description,
            file_stem: "oximeter",
            extra_validation: None,
        },
        ApiSpec {
            title: "Oxide TUF Repo Depot API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "API for fetching update artifacts",
            boundary: ApiBoundary::Internal,
            api_description: repo_depot_api::repo_depot_api_mod::stub_api_description,
            file_stem: "repo-depot",
            extra_validation: None,
        },
        ApiSpec {
            title: "Oxide Sled Agent API",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "API for interacting with individual sleds",
            boundary: ApiBoundary::Internal,
            api_description:
                sled_agent_api::sled_agent_api_mod::stub_api_description,
            file_stem: "sled-agent",
            extra_validation: None,
        },
        ApiSpec {
            title: "Oxide Technician Port Control Service",
            versions: Versions::Lockstep { version: semver::Version::new(0,0,1) },
            description: "API for use by the technician port TUI: wicket",
            boundary: ApiBoundary::Internal,
            api_description: wicketd_api::wicketd_api_mod::stub_api_description,
            file_stem: "wicketd",
            extra_validation: None,
        },
        // Add your APIs here! Please keep this list sorted by filename.
    ]
}

pub struct ApiSpec {
    /// The title.
    pub title: &'static str,

    /// Supported version(s) of this API
    pub(crate) versions: Versions,

    /// The description string.
    pub description: &'static str,

    /// Whether this API is internal or external.
    pub boundary: ApiBoundary,

    /// The API description function, typically a reference to
    /// `stub_api_description`.
    pub api_description:
        fn() -> Result<ApiDescription<StubContext>, ApiDescriptionBuildErrors>,

    /// The spec-specific part of the filename for API descriptions
    pub(crate) file_stem: &'static str,

    /// Extra validation to perform on the OpenAPI spec, if any.
    pub extra_validation: Option<fn(&OpenAPI, ValidationContext<'_>)>,
}

impl ApiSpec {
    pub(crate) fn overwrite(
        &self,
        env: &Environment,
    ) -> Result<SpecOverwriteStatus> {
        todo!(); // XXX-dap
                 // let contents = self.to_json_bytes()?;

        // let (summary, validation_result) = self
        //     .validate_json(&contents)
        //     .context("OpenAPI document validation failed")?;

        // let full_path = env.openapi_dir.join(self.latest_file_name());
        // let openapi_doc_status = overwrite_file(&full_path, &contents)?;

        // let extra_files = validation_result
        //     .extra_files
        //     .into_iter()
        //     .map(|(path, contents)| {
        //         let full_path = env.workspace_root.join(&path);
        //         let status = overwrite_file(&full_path, &contents)?;
        //         Ok((path, status))
        //     })
        //     .collect::<Result<_>>()?;

        // Ok(SpecOverwriteStatus {
        //     summary,
        //     openapi_doc: openapi_doc_status,
        //     extra_files,
        // })
    }

    pub(crate) fn file_stem(&self) -> &str {
        &self.file_stem
    }

    pub fn is_versioned(&self) -> bool {
        self.versions.is_versioned()
    }

    pub fn versions(&self) -> impl Iterator<Item = ApiSpecVersion<'_>> {
        self.versions
            .iter_versions_semvers()
            .map(|v| ApiSpecVersion { spec: self, version: v })
    }
}

pub struct ApiSpecVersion<'a> {
    pub spec: &'a ApiSpec,
    pub version: &'a semver::Version,
}

impl<'a> ApiSpecVersion<'a> {
    pub(crate) fn file_name(&self, contents: &[u8]) -> String {
        let label = "dummy-label"; // XXX-dap
        match self.spec.versions {
            Versions::Lockstep { .. } => {
                format!("{}.json", &self.spec.file_stem)
            }
            Versions::Versioned { .. } => {
                format!("{}-{}.json", &self.spec.file_stem, label)
            }
        }
    }

    pub(crate) fn check(&self, env: &Environment) -> Result<SpecCheckStatus> {
        let contents = self.to_json_bytes()?;
        let (summary, validation_result) = self
            .validate_json(&contents)
            .context("OpenAPI document validation failed")?;

        let full_path = env.openapi_dir().join(self.file_name(&contents));
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

    fn validate_json(
        &self,
        contents: &[u8],
    ) -> Result<(DocumentSummary, ValidationResult)> {
        let openapi_doc = contents_to_openapi(contents)
            .context("JSON returned by ApiDescription is not valid OpenAPI")?;

        // Check for lint errors.
        let errors = match self.spec.boundary {
            ApiBoundary::Internal => openapi_lint::validate(&openapi_doc),
            ApiBoundary::External => {
                openapi_lint::validate_external(&openapi_doc)
            }
        };
        if !errors.is_empty() {
            return Err(anyhow::anyhow!("{}", errors.join("\n\n")));
        }

        let extra_files = if let Some(extra_validation) =
            self.spec.extra_validation
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

    pub(crate) fn to_openapi_doc(&self) -> Result<OpenAPI> {
        // It's a bit weird to first convert to bytes and then back to OpenAPI,
        // but this is the easiest way to do so (currently, Dropshot doesn't
        // return the OpenAPI type directly). It is also consistent with the
        // other code paths.
        let contents = self.to_json_bytes()?;
        contents_to_openapi(&contents)
    }

    pub(crate) fn to_json_bytes(&self) -> Result<Vec<u8>> {
        let spec = self.spec;
        let description = (spec.api_description)().map_err(|error| {
            // ApiDescriptionBuildError is actually a list of errors so it
            // doesn't implement std::error::Error itself. Its Display
            // impl formats the errors appropriately.
            anyhow::anyhow!("{}", error)
        })?;
        let mut openapi_def =
            description.openapi(&spec.title, self.version.clone());
        openapi_def
            .description(&spec.description)
            .contact_url("https://oxide.computer")
            .contact_email("api@oxide.computer");

        // Use write because it's the most reliable way to get the canonical
        // JSON order. The `json` method returns a serde_json::Value which may
        // or may not have preserve_order enabled.
        let mut contents = Vec::new();
        openapi_def.write(&mut contents)?;
        Ok(contents)
    }
}

fn contents_to_openapi(contents: &[u8]) -> Result<OpenAPI> {
    serde_json::from_slice(&contents)
        .context("JSON returned by ApiDescription is not valid OpenAPI")
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
    ) -> impl Iterator<Item = (ApiSpecFileWhich<'_>, &OverwriteStatus)> {
        std::iter::once((ApiSpecFileWhich::Openapi, &self.openapi_doc)).chain(
            self.extra_files.iter().map(|(file_name, status)| {
                (ApiSpecFileWhich::Extra(file_name), status)
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

pub(crate) use crate::environment::Environment;

/// Overwrite a file with new contents, if the contents are different.
///
/// The file is left unchanged if the contents are the same. That's to avoid
/// mtime-based recompilations.
// XXX-dap remove pub
pub fn overwrite_file(
    path: &Utf8Path,
    contents: &[u8],
) -> Result<OverwriteStatus> {
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

// XXX-dap move this to validation.rs and call that check.rs?
// XXX-dap actually: it seems like we might want to put this into Resolution
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
