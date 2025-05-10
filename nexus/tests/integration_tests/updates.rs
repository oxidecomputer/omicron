// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TODO(iliana):
// - refactor `test_update_end_to_end` into a test setup function
// - test that an unknown artifact returns 404, not 500
// - tests around target names and artifact names that contain dangerous paths like `../`

use anyhow::{Context, Result, ensure};
use camino::Utf8Path;
use camino_tempfile::{Builder, Utf8TempPath};
use clap::Parser;
use dropshot::test_util::LogContext;
use http::{Method, StatusCode};
use nexus_config::UpdatesConfig;
use nexus_db_queries::context::OpContext;
use nexus_test_utils::background::run_tuf_artifact_replication_step;
use nexus_test_utils::background::wait_tuf_artifact_replication_step;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::{load_test_config, test_setup, test_setup_with_config};
use omicron_common::api::external::{
    TufRepoGetResponse, TufRepoInsertResponse, TufRepoInsertStatus,
};
use omicron_sled_agent::sim;
use pretty_assertions::assert_eq;
use semver::Version;
use serde::Deserialize;
use std::collections::HashSet;
use std::fmt::Debug;
use std::io::Write;
use tufaceous_artifact::KnownArtifactKind;
use tufaceous_lib::assemble::{DeserializedManifest, ManifestTweak};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_repo_upload_unconfigured() -> Result<()> {
    let mut config = load_test_config();
    let logctx = LogContext::new("test_update_uninitialized", &config.pkg.log);
    let cptestctx = test_setup_with_config::<omicron_nexus::Server>(
        "test_update_uninitialized",
        &mut config,
        sim::SimMode::Explicit,
        None,
        0,
    )
    .await;
    let client = &cptestctx.external_client;

    // Build a fake TUF repo
    let archive_path = make_archive(&logctx.log).await?;

    // Attempt to upload the repository to Nexus. This should fail with a 500
    // error because the updates system is not configured.
    {
        make_upload_request(
            client,
            &archive_path,
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .execute()
        .await
        .context("repository upload should have failed with 500 error")?;
    }

    // The artifact replication background task should have nothing to do.
    let status =
        run_tuf_artifact_replication_step(&cptestctx.internal_client).await;
    assert_eq!(
        status.last_run_counters.put_ok + status.last_run_counters.copy_ok,
        0
    );
    assert_eq!(status.local_repos, 0);

    // Attempt to fetch a repository description from Nexus. This should also
    // fail with a 500 error.
    {
        make_get_request(
            client,
            "1.0.0".parse().unwrap(),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .execute()
        .await
        .context("repository fetch should have failed with 500 error")?;
    }

    cptestctx.teardown().await;
    logctx.cleanup_successful();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_repo_upload() -> Result<()> {
    let mut config = load_test_config();
    config.pkg.updates = Some(UpdatesConfig {
        // XXX: This is currently not used by the update system, but
        // trusted_root will become meaningful in the future.
        trusted_root: "does-not-exist.json".into(),
    });
    let logctx = LogContext::new("test_update_end_to_end", &config.pkg.log);
    let cptestctx = test_setup_with_config::<omicron_nexus::Server>(
        "test_update_end_to_end",
        &mut config,
        sim::SimMode::Explicit,
        None,
        3, // 4 total sled agents
    )
    .await;
    let client = &cptestctx.external_client;

    // The initial generation number should be 1.
    let datastore = cptestctx.server.server_context().nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    assert_eq!(
        datastore.tuf_get_generation(&opctx).await.unwrap(),
        1u32.into()
    );

    // Build a fake TUF repo
    let archive_path = make_archive(&logctx.log).await?;

    // Upload the repository to Nexus.
    let mut initial_description = {
        let response =
            make_upload_request(client, &archive_path, StatusCode::OK)
                .execute()
                .await
                .context("error uploading repository")?;

        let response =
            serde_json::from_slice::<TufRepoInsertResponse>(&response.body)
                .context("error deserializing response body")?;
        assert_eq!(response.status, TufRepoInsertStatus::Inserted);
        response.recorded
    };
    let unique_sha256_count = initial_description
        .artifacts
        .iter()
        .map(|artifact| artifact.hash)
        .collect::<HashSet<_>>()
        .len();
    // The repository description should have `Zone` artifacts instead of the
    // composite `ControlPlane` artifact.
    assert_eq!(
        initial_description
            .artifacts
            .iter()
            .filter_map(|artifact| {
                if artifact.id.kind == KnownArtifactKind::Zone.into() {
                    Some(&artifact.id.name)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>(),
        ["zone1", "zone2"]
    );
    assert!(!initial_description.artifacts.iter().any(|artifact| {
        artifact.id.kind == KnownArtifactKind::ControlPlane.into()
    }));
    // The generation number should now be 2.
    assert_eq!(
        datastore.tuf_get_generation(&opctx).await.unwrap(),
        2u32.into()
    );

    // The artifact replication background task should have been activated, and
    // we should see a local repo and successful PUTs.
    let status =
        wait_tuf_artifact_replication_step(&cptestctx.internal_client).await;
    eprintln!("{status:?}");
    assert_eq!(status.generation, 2u32.into());
    assert_eq!(status.last_run_counters.put_config_ok, 4);
    assert_eq!(status.last_run_counters.list_ok, 4);
    assert_eq!(status.last_run_counters.put_ok, 3 * unique_sha256_count);
    assert_eq!(status.last_run_counters.copy_ok, unique_sha256_count);
    // The local repo is not deleted until the next task run.
    assert_eq!(status.local_repos, 1);

    // Wait for all the copy requests to complete.
    futures::future::join_all(cptestctx.sled_agents.iter().map(|sled_agent| {
        sled_agent.sled_agent().artifact_store().wait_for_copy_tasks()
    }))
    .await;

    // Run the replication background task again; the local repos should be
    // dropped.
    let status =
        run_tuf_artifact_replication_step(&cptestctx.internal_client).await;
    eprintln!("{status:?}");
    assert_eq!(status.last_run_counters.put_config_ok, 4);
    assert_eq!(status.last_run_counters.list_ok, 4);
    assert_eq!(status.last_run_counters.sum(), 8);
    assert_eq!(status.local_repos, 0);

    // Upload the repository to Nexus again. This should return a 200 with an
    // `AlreadyExists` status.
    let mut reupload_description = {
        let response =
            make_upload_request(client, &archive_path, StatusCode::OK)
                .execute()
                .await
                .context("error uploading repository a second time")?;

        let response =
            serde_json::from_slice::<TufRepoInsertResponse>(&response.body)
                .context("error deserializing response body")?;
        assert_eq!(response.status, TufRepoInsertStatus::AlreadyExists);
        response.recorded
    };

    initial_description.sort_artifacts();
    reupload_description.sort_artifacts();

    assert_eq!(
        initial_description, reupload_description,
        "initial description matches reupload"
    );

    // We didn't insert a new repo, so the generation number should still be 2.
    assert_eq!(
        datastore.tuf_get_generation(&opctx).await.unwrap(),
        2u32.into()
    );

    // Now get the repository that was just uploaded.
    let mut get_description = {
        let response = make_get_request(
            client,
            "1.0.0".parse().unwrap(), // this is the system version of the fake manifest
            StatusCode::OK,
        )
        .execute()
        .await
        .context("error fetching repository")?;

        let response =
            serde_json::from_slice::<TufRepoGetResponse>(&response.body)
                .context("error deserializing response body")?;
        response.description
    };

    get_description.sort_artifacts();

    assert_eq!(
        initial_description, get_description,
        "initial description matches fetched description"
    );

    // Upload a new repository with the same system version but a different
    // version for one of the components. This will produce a different hash,
    // which should return an error.
    {
        let tweaks = &[ManifestTweak::ArtifactVersion {
            kind: KnownArtifactKind::GimletSp,
            version: "2.0.0".parse().unwrap(),
        }];
        let archive_path = make_tweaked_archive(&logctx.log, tweaks).await?;

        let response = make_upload_request(
            client,
            &archive_path,
            StatusCode::CONFLICT,
        )
        .execute()
        .await
        .context(
            "error uploading repository with different artifact version \
             but same system version",
        )?;
        assert_error_message_contains(
            &response.body,
            "Uploaded repository with system version 1.0.0 has SHA256 hash",
        )?;
    }

    // Upload a new repository with a different system version and different
    // contents (but same version) for an artifact.
    {
        let tweaks = &[
            ManifestTweak::SystemVersion("2.0.0".parse().unwrap()),
            ManifestTweak::ArtifactSize {
                kind: KnownArtifactKind::ControlPlane,
                size_delta: 1024,
            },
        ];
        let archive_path = make_tweaked_archive(&logctx.log, tweaks).await?;

        let response =
            make_upload_request(client, &archive_path, StatusCode::CONFLICT)
                .execute()
                .await
                .context(
                    "error uploading repository with artifact \
                     containing different hash for same version",
                )?;
        assert_error_message_contains(
            &response.body,
            "Uploaded artifacts don't match existing artifacts with same IDs:",
        )?;
        assert_error_message_contains(
            &response.body,
            "For artifact (name: zone1, version: 1.0.0, kind: zone), uploaded \
             SHA256 hash",
        )?;
    }

    // Upload a new repository with the same *hash* but a different artifact
    // version.
    {
        let tweaks = &[
            ManifestTweak::SystemVersion("2.0.0".parse().unwrap()),
            ManifestTweak::ArtifactVersion {
                kind: KnownArtifactKind::GimletSp,
                version: "2.0.0".parse().unwrap(),
            },
            ManifestTweak::ArtifactDataVersion {
                kind: KnownArtifactKind::GimletSp,
                // 1.0.0 is the original version in the fake manifest.
                data_version: Some("1.0.0".parse().unwrap()),
            },
        ];

        let archive_path = make_tweaked_archive(&logctx.log, tweaks).await?;

        let response =
            make_upload_request(client, &archive_path, StatusCode::CONFLICT)
                .execute()
                .await
                .context(
                    "error uploading repository with artifact \
                     containing different hash for same version",
                )?;
        assert_error_message_contains(
            &response.body,
            "Uploaded artifacts don't match existing artifacts with same IDs:",
        )?;
        assert_error_message_contains(
            &response.body,
            "For artifact (kind: gimlet_sp, hash: ",
        )?;
        assert_error_message_contains(
            &response.body,
            "uploaded name fake-gimlet-sp and version 2.0.0, but \
             existing artifact has name fake-gimlet-sp and version 1.0.0.",
        )?;
    }

    // Upload a new repository with a different system version but no other
    // changes. This should be accepted.
    {
        let tweaks = &[ManifestTweak::SystemVersion("2.0.0".parse().unwrap())];
        let archive_path = make_tweaked_archive(&logctx.log, tweaks).await?;

        let response =
            make_upload_request(client, &archive_path, StatusCode::OK)
                .execute()
                .await
                .context("error uploading repository with different system version (should succeed)")?;

        let response =
            serde_json::from_slice::<TufRepoInsertResponse>(&response.body)
                .context("error deserializing response body")?;
        assert_eq!(response.status, TufRepoInsertStatus::Inserted);
    }
    // No artifacts changed, so the generation number should still be 2...
    assert_eq!(
        datastore.tuf_get_generation(&opctx).await.unwrap(),
        2u32.into()
    );
    // ... and the task should have nothing to do and should immediately
    // delete the local artifacts.
    let status =
        wait_tuf_artifact_replication_step(&cptestctx.internal_client).await;
    eprintln!("{status:?}");
    assert_eq!(status.generation, 2u32.into());
    assert_eq!(status.last_run_counters.list_ok, 4);
    assert_eq!(status.last_run_counters.put_ok, 0);
    assert_eq!(status.last_run_counters.copy_ok, 0);
    assert_eq!(status.local_repos, 0);

    cptestctx.teardown().await;
    logctx.cleanup_successful();

    Ok(())
}

async fn make_archive(log: &slog::Logger) -> anyhow::Result<Utf8TempPath> {
    make_tweaked_archive(log, &[]).await
}

async fn make_tweaked_archive(
    log: &slog::Logger,
    tweaks: &[ManifestTweak],
) -> anyhow::Result<Utf8TempPath> {
    let manifest = DeserializedManifest::tweaked_fake(tweaks);
    let mut manifest_file = Builder::new()
        .prefix("manifest")
        .suffix(".toml")
        .tempfile()
        .context("error creating temp file for manifest")?;
    let manifest_to_toml = manifest.to_toml()?;
    manifest_file.write_all(manifest_to_toml.as_bytes())?;

    let archive_path = Builder::new()
        .prefix("archive")
        .suffix(".zip")
        .tempfile()
        .context("error creating temp file for archive")?
        .into_temp_path();

    let args = tufaceous::Args::try_parse_from([
        "tufaceous",
        "assemble",
        manifest_file.path().as_str(),
        archive_path.as_str(),
    ])
    .context("error parsing args")?;

    args.exec(log).await.context("error executing assemble command")?;

    Ok(archive_path)
}

fn make_upload_request<'a>(
    client: &'a dropshot::test_util::ClientTestContext,
    archive_path: &'a Utf8Path,
    expected_status: StatusCode,
) -> NexusRequest<'a> {
    let file_name =
        archive_path.file_name().expect("archive_path must have a file name");
    let request = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::PUT,
            &format!("/v1/system/update/repository?file_name={}", file_name),
        )
        .body_file(Some(archive_path))
        .expect_status(Some(expected_status)),
    )
    .authn_as(AuthnMode::PrivilegedUser);
    request
}

fn make_get_request(
    client: &dropshot::test_util::ClientTestContext,
    system_version: Version,
    expected_status: StatusCode,
) -> NexusRequest<'_> {
    let request = NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::GET,
            &format!("/v1/system/update/repository/{system_version}"),
        )
        .expect_status(Some(expected_status)),
    )
    .authn_as(AuthnMode::PrivilegedUser);
    request
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    message: String,
}

// XXX: maybe replace this with a more detailed error code
fn assert_error_message_contains(
    body: &[u8],
    needle: &str,
) -> anyhow::Result<()> {
    let body: ErrorBody =
        serde_json::from_slice(body).context("body is not valid JSON")?;
    ensure!(
        body.message.contains(needle),
        "expected body to contain {:?}, but it was {:?}",
        needle,
        body
    );
    Ok(())
}

// =^..^=   =^..^=   =^..^=   =^..^=   =^..^=   =^..^=   =^..^=   =^..^=

// Tests that ".." paths are disallowed by dropshot.
#[tokio::test]
async fn test_download_with_dots_fails() {
    let cptestctx =
        test_setup::<omicron_nexus::Server>("test_download_with_dots_fails", 0)
            .await;
    let client = &cptestctx.internal_client;

    let filename = "hey/can/you/look/../../../../up/the/directory/tree";
    let artifact_get_url = format!("/artifacts/{}", filename);

    NexusRequest::expect_failure(
        client,
        StatusCode::BAD_REQUEST,
        Method::GET,
        &artifact_get_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap();

    cptestctx.teardown().await;
}
