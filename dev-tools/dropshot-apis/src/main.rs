// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::process::ExitCode;

use anyhow::{Context, anyhow};
use bootstrap_agent_api::*;
use camino::Utf8PathBuf;
use clap::Parser;
use clickhouse_admin_api::*;
use cockroach_admin_api::*;
use dns_server_api::*;
use dropshot_api_manager::{Environment, ManagedApiConfig, ManagedApis};
use dropshot_api_manager_types::{
    ManagedApiMetadata, ValidationContext, Versions,
};
use gateway_api::*;
use installinator_api::*;
use nexus_external_api::*;
use nexus_internal_api::*;
use nexus_lockstep_api::*;
use ntp_admin_api::*;
use openapiv3::OpenAPI;
use oximeter_api::*;
use repo_depot_api::*;
use serde::{Deserialize, Serialize};
use sled_agent_api::*;
use wicketd_api::*;

fn environment() -> anyhow::Result<Environment> {
    // The workspace root is two levels up from this crate's directory.
    let workspace_root = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let env = Environment::new(
        // This is the command used to run the OpenAPI manager.
        "cargo xtask openapi",
        workspace_root,
        // This is the location within the workspace root where the OpenAPI
        // documents are stored.
        "openapi",
    )?;
    Ok(env)
}

/// All APIs managed by the OpenAPI manager.
// TODO The metadata here overlaps with metadata in api-manifest.toml.
fn all_apis() -> anyhow::Result<ManagedApis> {
    let apis = vec![
        ManagedApiConfig {
            title: "Bootstrap Agent API",
            versions: Versions::new_lockstep(semver::Version::new(0, 0, 1)),
            metadata: ManagedApiMetadata {
                description: Some("Per-sled API for setup and teardown"),
                contact_url: Some("https://oxide.computer"),
                contact_email: Some("api@oxide.computer"),
                extra: to_value(ApiBoundary::Internal),
            },
            api_description: bootstrap_agent_api_mod::stub_api_description,
            ident: "bootstrap-agent",
            extra_validation: None,
        },
        ManagedApiConfig {
            title: "ClickHouse Cluster Admin Keeper API",
            versions: Versions::new_versioned(
                clickhouse_admin_api::supported_versions(),
            ),
            metadata: ManagedApiMetadata {
                description: Some(
                    "API for interacting with the Oxide \
                     control plane's ClickHouse cluster keepers",
                ),
                contact_url: Some("https://oxide.computer"),
                contact_email: Some("api@oxide.computer"),
                extra: to_value(ApiBoundary::Internal),
            },
            api_description:
                clickhouse_admin_keeper_api_mod::stub_api_description,
            ident: "clickhouse-admin-keeper",
            extra_validation: None,
        },
        ManagedApiConfig {
            title: "ClickHouse Cluster Admin Server API",
            versions: Versions::new_versioned(
                clickhouse_admin_api::supported_versions(),
            ),
            metadata: ManagedApiMetadata {
                description: Some(
                    "API for interacting with the Oxide \
                    control plane's ClickHouse cluster replica servers",
                ),
                contact_url: Some("https://oxide.computer"),
                contact_email: Some("api@oxide.computer"),
                extra: to_value(ApiBoundary::Internal),
            },
            api_description:
                clickhouse_admin_server_api_mod::stub_api_description,
            ident: "clickhouse-admin-server",
            extra_validation: None,
        },
        ManagedApiConfig {
            title: "ClickHouse Single-Node Admin Server API",
            versions: Versions::new_versioned(
                clickhouse_admin_api::supported_versions(),
            ),
            metadata: ManagedApiMetadata {
                description: Some(
                    "API for interacting with the Oxide \
                    control plane's single-node ClickHouse database",
                ),
                contact_url: Some("https://oxide.computer"),
                contact_email: Some("api@oxide.computer"),
                extra: to_value(ApiBoundary::Internal),
            },
            api_description:
                clickhouse_admin_single_api_mod::stub_api_description,
            ident: "clickhouse-admin-single",
            extra_validation: None,
        },
        ManagedApiConfig {
            title: "CockroachDB Cluster Admin API",
            versions: Versions::new_versioned(
                cockroach_admin_api::supported_versions(),
            ),
            metadata: ManagedApiMetadata {
                description: Some(
                    "API for interacting with the Oxide \
                    control plane's CockroachDB cluster",
                ),
                contact_url: Some("https://oxide.computer"),
                contact_email: Some("api@oxide.computer"),
                extra: to_value(ApiBoundary::Internal),
            },
            api_description: cockroach_admin_api_mod::stub_api_description,
            ident: "cockroach-admin",
            extra_validation: None,
        },
        ManagedApiConfig {
            title: "Internal DNS",
            versions: Versions::new_versioned(
                dns_server_api::supported_versions(),
            ),
            metadata: ManagedApiMetadata {
                description: Some("API for the internal DNS server"),
                contact_url: Some("https://oxide.computer"),
                contact_email: Some("api@oxide.computer"),
                extra: to_value(ApiBoundary::Internal),
            },
            api_description: dns_server_api_mod::stub_api_description,
            ident: "dns-server",
            extra_validation: None,
        },
        ManagedApiConfig {
            title: "Oxide Management Gateway Service API",
            versions: Versions::new_versioned(gateway_api::supported_versions()),
            metadata: ManagedApiMetadata {
                description: Some(
                    "API for interacting with the Oxide \
                    control plane's gateway service",
                ),
                contact_url: Some("https://oxide.computer"),
                contact_email: Some("api@oxide.computer"),
                extra: to_value(ApiBoundary::Internal),
            },
            api_description: gateway_api_mod::stub_api_description,
            ident: "gateway",
            extra_validation: None,
        },
        ManagedApiConfig {
            title: "Installinator API",
            versions: Versions::new_lockstep(semver::Version::new(0, 0, 1)),
            metadata: ManagedApiMetadata {
                description: Some(
                    "API for installinator to fetch artifacts \
                     and report progress",
                ),
                contact_url: Some("https://oxide.computer"),
                contact_email: Some("api@oxide.computer"),
                extra: to_value(ApiBoundary::Internal),
            },
            api_description: installinator_api_mod::stub_api_description,
            ident: "installinator",
            extra_validation: None,
        },
        ManagedApiConfig {
            title: "Oxide Region API",
            versions: Versions::new_lockstep(semver::Version::new(
                20251008, 0, 0,
            )),
            metadata: ManagedApiMetadata {
                description: Some(
                    "API for interacting with the Oxide control plane",
                ),
                contact_url: Some("https://oxide.computer"),
                contact_email: Some("api@oxide.computer"),
                extra: to_value(ApiBoundary::External),
            },
            api_description: nexus_external_api_mod::stub_api_description,
            ident: "nexus",
            extra_validation: Some(nexus_external_api::validate_api),
        },
        ManagedApiConfig {
            title: "Nexus internal API",
            versions: Versions::new_lockstep(semver::Version::new(0, 0, 1)),
            metadata: ManagedApiMetadata {
                description: Some("Nexus internal API"),
                contact_url: Some("https://oxide.computer"),
                contact_email: Some("api@oxide.computer"),
                extra: to_value(ApiBoundary::Internal),
            },
            api_description: nexus_internal_api_mod::stub_api_description,
            ident: "nexus-internal",
            extra_validation: None,
        },
        ManagedApiConfig {
            title: "Nexus lockstep API",
            versions: Versions::new_lockstep(semver::Version::new(0, 0, 1)),
            metadata: ManagedApiMetadata {
                description: Some("Nexus lockstep internal API"),
                contact_url: Some("https://oxide.computer"),
                contact_email: Some("api@oxide.computer"),
                extra: to_value(ApiBoundary::Internal),
            },
            api_description: nexus_lockstep_api_mod::stub_api_description,
            ident: "nexus-lockstep",
            extra_validation: None,
        },
        ManagedApiConfig {
            title: "NTP Admin API",
            versions: Versions::new_versioned(
                ntp_admin_api::supported_versions(),
            ),
            metadata: ManagedApiMetadata {
                description: Some("API for interacting with NTP"),
                contact_url: Some("https://oxide.computer"),
                contact_email: Some("api@oxide.computer"),
                extra: to_value(ApiBoundary::Internal),
            },
            api_description: ntp_admin_api_mod::stub_api_description,
            ident: "ntp-admin",
            extra_validation: None,
        },
        ManagedApiConfig {
            title: "Oxide Oximeter API",
            versions: Versions::new_versioned(
                oximeter_api::supported_versions(),
            ),
            metadata: ManagedApiMetadata {
                description: Some("API for interacting with oximeter"),
                contact_url: Some("https://oxide.computer"),
                contact_email: Some("api@oxide.computer"),
                extra: to_value(ApiBoundary::Internal),
            },
            api_description: oximeter_api_mod::stub_api_description,
            ident: "oximeter",
            extra_validation: None,
        },
        ManagedApiConfig {
            title: "Oxide TUF Repo Depot API",
            versions: Versions::new_lockstep(semver::Version::new(0, 0, 1)),
            metadata: ManagedApiMetadata {
                description: Some("API for fetching update artifacts"),
                contact_url: Some("https://oxide.computer"),
                contact_email: Some("api@oxide.computer"),
                extra: to_value(ApiBoundary::Internal),
            },
            api_description: repo_depot_api_mod::stub_api_description,
            ident: "repo-depot",
            extra_validation: None,
        },
        ManagedApiConfig {
            title: "Oxide Sled Agent API",
            versions: Versions::new_versioned(
                sled_agent_api::supported_versions(),
            ),
            metadata: ManagedApiMetadata {
                description: Some("API for interacting with individual sleds"),
                contact_url: Some("https://oxide.computer"),
                contact_email: Some("api@oxide.computer"),
                extra: to_value(ApiBoundary::Internal),
            },
            api_description: sled_agent_api_mod::stub_api_description,
            ident: "sled-agent",
            extra_validation: None,
        },
        ManagedApiConfig {
            title: "Oxide Technician Port Control Service",
            versions: Versions::new_lockstep(semver::Version::new(0, 0, 1)),
            metadata: ManagedApiMetadata {
                description: Some(
                    "API for use by the technician port TUI: wicket",
                ),
                contact_url: Some("https://oxide.computer"),
                contact_email: Some("api@oxide.computer"),
                extra: to_value(ApiBoundary::Internal),
            },
            api_description: wicketd_api_mod::stub_api_description,
            ident: "wicketd",
            extra_validation: None,
        },
    ];

    let apis = ManagedApis::new(apis)
        .context("error creating ManagedApis")?
        .with_validation(validate);
    Ok(apis)
}

/// A bit of extra metadata that can be supplied to each API.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ApiExtra {
    boundary: ApiBoundary,
}

fn to_value(boundary: ApiBoundary) -> serde_json::Value {
    serde_json::to_value(ApiExtra { boundary }).unwrap()
}

/// This is some example data that is used in the `validate` function below.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ApiBoundary {
    Internal,
    External,
}

fn validate(doc: &OpenAPI, mut cx: ValidationContext<'_>) {
    let extra = serde_json::from_value::<ApiExtra>(cx.metadata().extra.clone())
        .unwrap();

    let errors = match extra.boundary {
        ApiBoundary::Internal => openapi_lint::validate(doc),
        ApiBoundary::External => openapi_lint::validate_external(doc),
    };
    for error in errors {
        cx.report_error(anyhow!(error));
    }
}

fn main() -> anyhow::Result<ExitCode> {
    let app = dropshot_api_manager::App::parse();
    let env = environment()?;
    let apis = all_apis()?;

    Ok(app.exec(&env, &apis))
}

#[cfg(test)]
mod tests {
    use dropshot_api_manager::test_util::check_apis_up_to_date;

    use super::*;

    #[test]
    fn test_apis_up_to_date() -> anyhow::Result<ExitCode> {
        let env = environment()?;
        let apis = all_apis()?;

        let result = check_apis_up_to_date(&env, &apis)?;
        Ok(result.to_exit_code())
    }
}
