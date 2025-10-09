// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, Result, ensure};
use camino::Utf8Path;
use camino_tempfile::{Builder, Utf8TempPath};
use chrono::{DateTime, Duration, Timelike, Utc};
use dropshot::ResultsPage;
use http::{Method, StatusCode};
use nexus_db_model::SemverVersion;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pub_test_utils::helpers::insert_test_tuf_repo;
use nexus_test_utils::background::activate_background_task;
use nexus_test_utils::background::run_tuf_artifact_replication_step;
use nexus_test_utils::background::wait_tuf_artifact_replication_step;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::object_get;
use nexus_test_utils::resource_helpers::object_get_error;
use nexus_test_utils::resource_helpers::objects_list_page_authz;
use nexus_test_utils::test_setup;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::views;
use nexus_types::external_api::views::{TufRepoUpload, TufRepoUploadStatus};
use omicron_common::api::external::TufArtifactMeta;
use pretty_assertions::assert_eq;
use semver::Version;
use serde::Deserialize;
use std::collections::HashSet;
use std::fmt::Debug;
use tough::editor::signed::SignedRole;
use tough::schema::Root;
use tufaceous_artifact::ArtifactVersion;
use tufaceous_artifact::KnownArtifactKind;
use tufaceous_lib::Key;
use tufaceous_lib::assemble::{ArtifactManifest, OmicronRepoAssembler};
use tufaceous_lib::assemble::{DeserializedManifest, ManifestTweak};

use crate::integration_tests::target_release::set_target_release;

const TRUST_ROOTS_URL: &str = "/v1/system/update/trust-roots";

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

/// Get artifacts for a repository using the datastore directly, sorted by ID
async fn get_repo_artifacts(
    cptestctx: &ControlPlaneTestContext,
    version: &str,
) -> Vec<TufArtifactMeta> {
    let datastore = cptestctx.server.server_context().nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.new(o!()), datastore.clone());
    let system_version = SemverVersion::from(
        version.parse::<Version>().expect("version should parse"),
    );

    let repo = datastore
        .tuf_repo_get_by_version(&opctx, system_version)
        .await
        .expect("should get repo by version");

    let artifacts = datastore
        .tuf_list_repo_artifacts(&opctx, repo.id.into())
        .await
        .expect("should get artifacts");

    let mut result: Vec<TufArtifactMeta> = artifacts
        .into_iter()
        .map(|artifact| artifact.into_external())
        .collect();

    // Sort artifacts by their ID for consistent comparison
    result.sort_by(|a, b| a.id.cmp(&b.id));
    result
}

pub struct TestTrustRoot {
    pub key: Key,
    pub expiry: DateTime<Utc>,
    pub root_role: SignedRole<Root>,
}

impl TestTrustRoot {
    pub async fn generate() -> Result<TestTrustRoot> {
        let key = Key::generate_ed25519()?;
        let expiry = Utc::now()
            .with_nanosecond(0)
            .expect("0 is less than 2,000,000,000")
            + Duration::weeks(1);
        let root_role =
            tufaceous_lib::root::new_root(vec![key.clone()], expiry).await?;
        Ok(TestTrustRoot { key, expiry, root_role })
    }

    pub fn to_upload_request<'a>(
        &'a self,
        client: &'a dropshot::test_util::ClientTestContext,
        expected_status: StatusCode,
    ) -> NexusRequest<'a> {
        let request =
            RequestBuilder::new(client, Method::POST, TRUST_ROOTS_URL)
                .body(Some(self.root_role.signed()))
                .expect_status(Some(expected_status));
        NexusRequest::new(request).authn_as(AuthnMode::PrivilegedUser)
    }

    pub async fn assemble_repo(
        &self,
        log: &slog::Logger,
        tweaks: &[ManifestTweak],
    ) -> Result<TestRepo> {
        let archive_path = Builder::new()
            .prefix("archive")
            .suffix(".zip")
            .tempfile()
            .context("error creating temp file for archive")?
            .into_temp_path();

        let manifest = ArtifactManifest::from_deserialized(
            Utf8Path::new(""),
            DeserializedManifest::tweaked_fake(tweaks),
        )?;
        let mut assembler = OmicronRepoAssembler::new(
            log,
            manifest,
            vec![self.key.clone()],
            self.expiry,
            true,
            archive_path.to_path_buf(),
        );
        assembler.set_root_role(self.root_role.clone());
        assembler.build().await?;
        Ok(TestRepo(archive_path))
    }
}

pub struct TestRepo(pub Utf8TempPath);

impl TestRepo {
    /// Generate a `NexusRequest` to upload this repo.
    ///
    /// Prefer `into_upload_request` to ensure the temporary file is deleted.
    pub fn to_upload_request<'a>(
        &self,
        client: &'a dropshot::test_util::ClientTestContext,
        expected_status: StatusCode,
    ) -> NexusRequest<'a> {
        let url = format!(
            "/v1/system/update/repositories?file_name={}",
            self.0.file_name().expect("archive path must have a file name")
        );
        let request = RequestBuilder::new(client, Method::PUT, &url)
            .body_file(Some(&self.0))
            .expect_status(Some(expected_status));
        NexusRequest::new(request).authn_as(AuthnMode::PrivilegedUser)
    }

    /// Generate a `NexusRequest` to upload this repo, and then delete the
    /// temporary file.
    ///
    /// Panics if we fail to delete the temporary file.
    pub fn into_upload_request(
        self,
        client: &dropshot::test_util::ClientTestContext,
        expected_status: StatusCode,
    ) -> NexusRequest<'_> {
        let request = self.to_upload_request(client, expected_status);
        self.0.close().unwrap();
        request
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_repo_upload_unconfigured() -> Result<()> {
    let cptestctx =
        test_setup::<omicron_nexus::Server>("test_update_uninitialized", 0)
            .await;
    let client = &cptestctx.external_client;
    let logctx = &cptestctx.logctx;

    // Generate a trust root, but _don't_ upload it to Nexus.
    let trust_root = TestTrustRoot::generate().await?;
    // Build a fake TUF repo and attempt to upload it to Nexus. This should fail
    // with a 400 error because we did not upload a trusted root role.
    trust_root
        .assemble_repo(&logctx.log, &[])
        .await?
        .into_upload_request(client, StatusCode::BAD_REQUEST)
        .execute()
        .await?;

    // The artifact replication background task should have nothing to do.
    let status =
        run_tuf_artifact_replication_step(&cptestctx.lockstep_client).await;
    assert_eq!(
        status.last_run_counters.put_ok + status.last_run_counters.copy_ok,
        0
    );
    assert_eq!(status.local_repos, 0);

    // Attempt to fetch a repository description from Nexus. This should fail
    // with a 404 error.
    object_get_error(
        client,
        "/v1/system/update/repositories/1.0.0",
        StatusCode::NOT_FOUND,
    )
    .await;

    cptestctx.teardown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_repo_upload() -> Result<()> {
    let cptestctx = test_setup::<omicron_nexus::Server>(
        "test_update_end_to_end",
        3, // 4 total sled agents
    )
    .await;
    let client = &cptestctx.external_client;
    let logctx = &cptestctx.logctx;

    // The initial generation number should be 1.
    let datastore = cptestctx.server.server_context().nexus.datastore();
    let opctx = OpContext::for_tests(logctx.log.new(o!()), datastore.clone());
    assert_eq!(
        datastore.tuf_get_generation(&opctx).await.unwrap(),
        1u32.into()
    );

    let trust_root = TestTrustRoot::generate().await?;
    trust_root.to_upload_request(client, StatusCode::CREATED).execute().await?;

    // Build a fake TUF repo
    let repo = trust_root.assemble_repo(&logctx.log, &[]).await?;

    // Generate a repository and upload it to Nexus.
    let initial_repo = {
        let response = repo
            .to_upload_request(client, StatusCode::OK)
            .execute()
            .await
            .context("error uploading repository")?;

        let response = serde_json::from_slice::<TufRepoUpload>(&response.body)
            .context("error deserializing response body")?;
        assert_eq!(response.status, TufRepoUploadStatus::Inserted);
        response.repo
    };

    // Get artifacts using the datastore directly
    let initial_artifacts = get_repo_artifacts(&cptestctx, "1.0.0").await;
    let unique_sha256_count = initial_artifacts
        .iter()
        .map(|artifact| artifact.hash)
        .collect::<HashSet<_>>()
        .len();
    // The repository description should have `Zone` artifacts instead of the
    // composite `ControlPlane` artifact.
    let zone_names: HashSet<&str> = initial_artifacts
        .iter()
        .filter_map(|artifact| {
            if artifact.id.kind == KnownArtifactKind::Zone.into() {
                Some(artifact.id.name.as_str())
            } else {
                None
            }
        })
        .collect();
    let expected_zones: HashSet<&str> =
        ["zone-1", "zone-2"].into_iter().collect();
    assert_eq!(zone_names, expected_zones);
    assert!(!initial_artifacts.iter().any(|artifact| {
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
        wait_tuf_artifact_replication_step(&cptestctx.lockstep_client).await;
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
        run_tuf_artifact_replication_step(&cptestctx.lockstep_client).await;
    eprintln!("{status:?}");
    assert_eq!(status.last_run_counters.put_config_ok, 4);
    assert_eq!(status.last_run_counters.list_ok, 4);
    assert_eq!(status.last_run_counters.sum(), 8);
    assert_eq!(status.local_repos, 0);

    // Upload the repository to Nexus again. This should return a 200 with an
    // `AlreadyExists` status.
    let reupload_description = {
        let response = repo
            .into_upload_request(client, StatusCode::OK)
            .execute()
            .await
            .context("error uploading repository a second time")?;

        let response = serde_json::from_slice::<TufRepoUpload>(&response.body)
            .context("error deserializing response body")?;
        assert_eq!(response.status, TufRepoUploadStatus::AlreadyExists);
        response.repo
    };

    // Get artifacts again and compare them
    let reupload_artifacts = get_repo_artifacts(&cptestctx, "1.0.0").await;

    assert_eq!(
        initial_artifacts, reupload_artifacts,
        "initial artifacts match reupload artifacts"
    );

    // Also verify that the repo metadata (without artifacts) matches
    assert_eq!(
        initial_repo.hash, reupload_description.hash,
        "repo hash matches"
    );
    assert_eq!(
        initial_repo.system_version, reupload_description.system_version,
        "system version matches"
    );

    // We didn't insert a new repo, so the generation number should still be 2.
    assert_eq!(
        datastore.tuf_get_generation(&opctx).await.unwrap(),
        2u32.into()
    );

    // Now get the repository that was just uploaded.
    let repo = object_get::<views::TufRepo>(
        client,
        "/v1/system/update/repositories/1.0.0",
    )
    .await;

    // Compare just the repo metadata (not artifacts)
    assert_eq!(initial_repo.hash, repo.hash, "repo hash matches");
    assert_eq!(
        initial_repo.system_version, repo.system_version,
        "system version matches"
    );

    // Upload a new repository with the same system version but a different
    // version for one of the components. This will produce a different hash,
    // which should return an error.
    {
        let tweaks = &[ManifestTweak::ArtifactVersion {
            kind: KnownArtifactKind::GimletSp,
            version: "2.0.0".parse().unwrap(),
        }];
        let response = trust_root
            .assemble_repo(&logctx.log, tweaks)
            .await?
            .into_upload_request(client, StatusCode::CONFLICT)
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
        let response = trust_root
            .assemble_repo(&logctx.log, tweaks)
            .await?
            .into_upload_request(client, StatusCode::CONFLICT)
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
            "For artifact (name: zone-1, version: 1.0.0, kind: zone), uploaded \
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

        let response = trust_root
            .assemble_repo(&logctx.log, tweaks)
            .await?
            .into_upload_request(client, StatusCode::CONFLICT)
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
    let initial_installinator_doc_hash = {
        let tweaks = &[ManifestTweak::SystemVersion("2.0.0".parse().unwrap())];
        let response = trust_root
            .assemble_repo(&logctx.log, tweaks)
            .await?
            .into_upload_request(client, StatusCode::OK)
            .execute()
            .await
            .context(
                "error uploading repository with different system version \
                (should succeed)",
            )?;

        let response = serde_json::from_slice::<TufRepoUpload>(&response.body)
            .context("error deserializing response body")?;
        assert_eq!(response.status, TufRepoUploadStatus::Inserted);

        // Get artifacts for the 2.0.0 repository
        let artifacts_2_0_0 = get_repo_artifacts(&cptestctx, "2.0.0").await;

        // The artifacts should be exactly the same as the 1.0.0 repo we
        // uploaded, other than the installinator document (which will have
        // system version 2.0.0).
        let mut installinator_doc_1 = None;
        let filtered_artifacts_1 = initial_artifacts
            .iter()
            .filter(|artifact| {
                if artifact.id.kind
                    == KnownArtifactKind::InstallinatorDocument.into()
                {
                    installinator_doc_1 = Some(*artifact);
                    false
                } else {
                    true
                }
            })
            .collect::<Vec<_>>();
        let mut installinator_doc_2 = None;
        let filtered_artifacts_2 = artifacts_2_0_0
            .iter()
            .filter(|artifact| {
                if artifact.id.kind
                    == KnownArtifactKind::InstallinatorDocument.into()
                {
                    installinator_doc_2 = Some(*artifact);
                    false
                } else {
                    true
                }
            })
            .collect::<Vec<_>>();

        let installinator_doc_1 = installinator_doc_1
            .expect("should have found installinator document in 1.0.0");
        assert_eq!(installinator_doc_1.id.version, "1.0.0".parse().unwrap());
        let installinator_doc_2 = installinator_doc_2
            .expect("should have found installinator document in 2.0.0");
        assert_eq!(installinator_doc_2.id.version, "2.0.0".parse().unwrap());

        assert_eq!(
            filtered_artifacts_1, filtered_artifacts_2,
            "artifacts for 1.0.0 and 2.0.0 should match"
        );

        // Now get the repository that was just uploaded.
        let get_repo = object_get::<views::TufRepo>(
            client,
            "/v1/system/update/repositories/2.0.0",
        )
        .await;

        // Validate the repo metadata
        assert_eq!(get_repo.system_version.to_string(), "2.0.0");

        installinator_doc_1.hash.to_string()
    };
    // The installinator document changed, so the generation number is bumped to
    // 3.
    assert_eq!(
        datastore.tuf_get_generation(&opctx).await.unwrap(),
        3u32.into()
    );
    // ... and the task will have one artifact to replicate.
    let status =
        wait_tuf_artifact_replication_step(&cptestctx.lockstep_client).await;
    eprintln!("{status:?}");
    assert_eq!(status.generation, 3u32.into());
    assert_eq!(status.last_run_counters.list_ok, 4);
    assert_eq!(status.last_run_counters.put_ok, 3);
    assert_eq!(status.last_run_counters.copy_ok, 1);
    assert_eq!(status.local_repos, 1);
    // Run the replication background task again; the local repos should be
    // dropped.
    let status =
        run_tuf_artifact_replication_step(&cptestctx.lockstep_client).await;
    eprintln!("{status:?}");
    assert_eq!(status.last_run_counters.put_config_ok, 4);
    assert_eq!(status.last_run_counters.list_ok, 4);
    assert_eq!(status.last_run_counters.sum(), 8);
    assert_eq!(status.local_repos, 0);

    // Verify the initial installinator document is present on all sled-agents.
    for sled_agent in &cptestctx.sled_agents {
        for dir in sled_agent.sled_agent().artifact_store().storage_paths() {
            let path = dir.join(&initial_installinator_doc_hash);
            assert!(path.exists(), "{path} does not exist");
        }
    }
    // Collect watchers for all of the sled-agent artifact delete reconcilers.
    let mut delete_watchers = cptestctx
        .sled_agents
        .iter()
        .map(|sled_agent| {
            sled_agent.sled_agent().artifact_store().subscribe_delete_done()
        })
        .collect::<Vec<_>>();
    // Manually prune the first repo.
    let initial_repo = datastore
        .tuf_repo_get_by_version(
            &opctx,
            "1.0.0".parse::<Version>().unwrap().into(),
        )
        .await?;
    let recent_releases =
        datastore.target_release_fetch_recent_distinct(&opctx, 3).await?;
    datastore
        .tuf_repo_mark_pruned(
            &opctx,
            status.generation,
            &recent_releases,
            initial_repo.id(),
        )
        .await
        .unwrap();
    // Marking a repository as pruned bumps the generation number.
    assert_eq!(
        datastore.tuf_get_generation(&opctx).await.unwrap(),
        4u32.into()
    );
    // Run the replication background task; we should see new configs be put.
    let status =
        run_tuf_artifact_replication_step(&cptestctx.lockstep_client).await;
    eprintln!("{status:?}");
    assert_eq!(status.last_run_counters.put_config_ok, 4);
    assert_eq!(status.last_run_counters.list_ok, 4);
    assert_eq!(status.last_run_counters.sum(), 8);
    assert_eq!(status.generation, 4u32.into());
    // Wait for the delete reconciler to finish on all sled agents.
    futures::future::join_all(
        delete_watchers.iter_mut().map(|watcher| watcher.changed()),
    )
    .await;
    // Verify the installinator document from the initial repo is deleted.
    for sled_agent in &cptestctx.sled_agents {
        for dir in sled_agent.sled_agent().artifact_store().storage_paths() {
            let path = dir.join(&initial_installinator_doc_hash);
            assert!(!path.exists(), "{path} was not deleted");
        }
    }

    cptestctx.teardown().await;
    Ok(())
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

#[nexus_test]
async fn test_trust_root_operations(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let trust_root =
        TestTrustRoot::generate().await.expect("trust root generation failed");

    // POST /v1/system/update/trust-roots
    let trust_root_view: views::UpdatesTrustRoot = trust_root
        .to_upload_request(client, StatusCode::CREATED)
        .execute()
        .await
        .expect("trust root add failed")
        .parsed_body()
        .expect("failed to parse add response");

    // GET /v1/system/update/trust-roots
    let request = RequestBuilder::new(client, Method::GET, TRUST_ROOTS_URL)
        .expect_status(Some(StatusCode::OK));
    let response: ResultsPage<views::UpdatesTrustRoot> =
        NexusRequest::new(request)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("trust root list failed")
            .parsed_body()
            .expect("failed to parse list response");
    assert_eq!(response.items, std::slice::from_ref(&trust_root_view.clone()));

    // GET /v1/system/update/trust-roots/{id}
    let id_url = format!("{TRUST_ROOTS_URL}/{}", trust_root_view.id);
    let request = RequestBuilder::new(client, Method::GET, &id_url)
        .expect_status(Some(StatusCode::OK));
    let response: views::UpdatesTrustRoot = NexusRequest::new(request)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("trust root get failed")
        .parsed_body()
        .expect("failed to parse get response");
    assert_eq!(response, trust_root_view);

    // DELETE /v1/system/update/trust-roots/{id}
    let request = RequestBuilder::new(client, Method::DELETE, &id_url)
        .expect_status(Some(StatusCode::NO_CONTENT));
    NexusRequest::new(request)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("trust root delete failed");
    let request = RequestBuilder::new(client, Method::GET, TRUST_ROOTS_URL)
        .expect_status(Some(StatusCode::OK));
    let response: ResultsPage<views::UpdatesTrustRoot> =
        NexusRequest::new(request)
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .expect("trust root list after delete failed")
            .parsed_body()
            .expect("failed to parse list after delete response");
    assert!(response.items.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_update_status() -> Result<()> {
    let cptestctx =
        test_setup::<omicron_nexus::Server>("test_update_uninitialized", 0)
            .await;
    let client = &cptestctx.external_client;
    let logctx = &cptestctx.logctx;

    // initial status
    let status: views::UpdateStatus =
        object_get(client, "/v1/system/update/status").await;
    assert_eq!(status.target_release.0, None);
    // does not start suspended because the DB migration initialized the
    // target_release table with a row with gen 1, and the initial target
    // blueprint also has gen 1
    assert!(!status.suspended);

    let counts = status.components_by_release_version;
    assert_eq!(counts.get("install dataset").unwrap(), &7);
    assert_eq!(counts.get("unknown").unwrap(), &15);

    // hold onto this to compare it to later values
    let time_last_step_planned = status.time_last_step_planned;

    // Upload a fake TUF repo and set it as the target release
    let trust_root = TestTrustRoot::generate().await?;
    trust_root.to_upload_request(client, StatusCode::CREATED).execute().await?;
    trust_root
        .assemble_repo(&logctx.log, &[])
        .await?
        .to_upload_request(client, StatusCode::OK)
        .execute()
        .await?;
    let v1 = Version::new(1, 0, 0);
    set_target_release(client, &v1).await?;

    let status: views::UpdateStatus =
        object_get(client, "/v1/system/update/status").await;
    assert_eq!(status.target_release.0.unwrap().version, v1);
    assert!(!status.suspended, "should not be suspended after setting v1");

    // blueprint time doesn't change
    assert_eq!(time_last_step_planned, status.time_last_step_planned);

    let counts = status.components_by_release_version;
    assert_eq!(counts.get("install dataset").unwrap(), &7);
    assert_eq!(counts.get("unknown").unwrap(), &15);

    // do it again so there are two, so both versions are associated with tuf repos
    let v2 = Version::new(2, 0, 0);
    let tweaks = &[
        ManifestTweak::SystemVersion(v2.clone()),
        ManifestTweak::ArtifactVersion {
            kind: KnownArtifactKind::SwitchRotBootloader,
            version: ArtifactVersion::new("non-semver-2").unwrap(),
        },
    ];
    let trust_root = TestTrustRoot::generate().await?;
    trust_root.to_upload_request(client, StatusCode::CREATED).execute().await?;
    trust_root
        .assemble_repo(&logctx.log, tweaks)
        .await?
        .to_upload_request(client, StatusCode::OK)
        .execute()
        .await?;
    set_target_release(client, &v2).await?;

    let status: views::UpdateStatus =
        object_get(client, "/v1/system/update/status").await;

    assert_eq!(status.target_release.0.unwrap().version, v2);
    assert!(!status.suspended, "should not be suspended after setting v2");

    // blueprint time doesn't change
    assert_eq!(time_last_step_planned, status.time_last_step_planned);

    let counts = status.components_by_release_version;
    assert_eq!(counts.get("install dataset").unwrap(), &7);
    assert_eq!(counts.get("unknown").unwrap(), &15);

    cptestctx.teardown().await;
    Ok(())
}

#[nexus_test]
async fn test_repo_prune(cptestctx: &ControlPlaneTestContext) {
    let logctx = &cptestctx.logctx;
    let datastore = cptestctx.server.server_context().nexus.datastore();
    let opctx = OpContext::for_tests(logctx.log.new(o!()), datastore.clone());

    // Wait for one activation of the task to avoid racing with it.
    let client = &cptestctx.lockstep_client;
    activate_background_task(client, "tuf_repo_pruner").await;

    // Insert four TUF repos.
    let repo1id = insert_test_tuf_repo(&opctx, datastore, 1).await;
    let repo2id = insert_test_tuf_repo(&opctx, datastore, 2).await;
    let repo3id = insert_test_tuf_repo(&opctx, datastore, 3).await;
    let repo4id = insert_test_tuf_repo(&opctx, datastore, 4).await;

    // Immediately, all four repos ought to be visible.
    let repos = datastore
        .tuf_list_repos_unpruned_batched(&opctx)
        .await
        .expect("listing repos");
    assert_eq!(repos.len(), 4);
    assert!(repos.iter().any(|r| r.id() == repo1id));
    assert!(repos.iter().any(|r| r.id() == repo2id));
    assert!(repos.iter().any(|r| r.id() == repo3id));
    assert!(repos.iter().any(|r| r.id() == repo4id));

    // Activate the task again and wait for it to complete.  Exactly one of
    // the repos should be pruned.
    activate_background_task(client, "tuf_repo_pruner").await;
    let repos = datastore
        .tuf_list_repos_unpruned_batched(&opctx)
        .await
        .expect("listing repos");
    assert_eq!(repos.len(), 3);
    assert!(!repos.iter().any(|r| r.id() == repo1id));
    assert!(repos.iter().any(|r| r.id() == repo2id));
    assert!(repos.iter().any(|r| r.id() == repo3id));
    assert!(repos.iter().any(|r| r.id() == repo4id));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_repo_list() -> Result<()> {
    let cptestctx = test_setup::<omicron_nexus::Server>(
        "test_update_repo_list",
        3, // 4 total sled agents
    )
    .await;
    let client = &cptestctx.external_client;
    let logctx = &cptestctx.logctx;

    // Initially, list should be empty
    let initial_list: ResultsPage<views::TufRepo> =
        objects_list_page_authz(client, "/v1/system/update/repositories").await;

    assert_eq!(initial_list.items.len(), 0);
    assert!(initial_list.next_page.is_none());

    // Add a trust root
    let trust_root = TestTrustRoot::generate().await?;
    trust_root.to_upload_request(client, StatusCode::CREATED).execute().await?;

    // Upload first repository (system version 1.0.0)
    let repo1 = trust_root.assemble_repo(&logctx.log, &[]).await?;
    let upload_response1 = repo1
        .into_upload_request(client, StatusCode::OK)
        .execute()
        .await
        .context("error uploading first repository")?;
    let response1 =
        serde_json::from_slice::<TufRepoUpload>(&upload_response1.body)
            .context("error deserializing first response body")?;
    assert_eq!(response1.status, TufRepoUploadStatus::Inserted);

    // Upload second repository (system version 2.0.0)
    let tweaks = &[ManifestTweak::SystemVersion("2.0.0".parse().unwrap())];
    let repo2 = trust_root.assemble_repo(&logctx.log, tweaks).await?;
    let upload_response2 = repo2
        .into_upload_request(client, StatusCode::OK)
        .execute()
        .await
        .context("error uploading second repository")?;
    let response2 =
        serde_json::from_slice::<TufRepoUpload>(&upload_response2.body)
            .context("error deserializing second response body")?;
    assert_eq!(response2.status, TufRepoUploadStatus::Inserted);

    // Upload third repository (system version 3.0.0)
    let tweaks = &[ManifestTweak::SystemVersion("3.0.0".parse().unwrap())];
    let repo3 = trust_root.assemble_repo(&logctx.log, tweaks).await?;
    let upload_response3 = repo3
        .into_upload_request(client, StatusCode::OK)
        .execute()
        .await
        .context("error uploading third repository")?;
    let response3 =
        serde_json::from_slice::<TufRepoUpload>(&upload_response3.body)
            .context("error deserializing third response body")?;
    assert_eq!(response3.status, TufRepoUploadStatus::Inserted);

    // List repositories - should return all 3, ordered by system version (newest first)
    let list: ResultsPage<views::TufRepo> =
        objects_list_page_authz(client, "/v1/system/update/repositories").await;

    assert_eq!(list.items.len(), 3);

    // Repositories should be ordered by system version descending (newest first)
    let system_versions: Vec<String> =
        list.items.iter().map(|item| item.system_version.to_string()).collect();
    assert_eq!(system_versions, vec!["3.0.0", "2.0.0", "1.0.0"]);

    // Verify that each response contains the correct system version
    for (i, item) in list.items.iter().enumerate() {
        let expected_version = match i {
            0 => "3.0.0",
            1 => "2.0.0",
            2 => "1.0.0",
            _ => panic!("unexpected index"),
        };
        assert_eq!(item.system_version.to_string(), expected_version);
    }

    // Request ascending order and expect the versions oldest-first
    let ascending_list: ResultsPage<views::TufRepo> = objects_list_page_authz(
        client,
        "/v1/system/update/repositories?sort_by=version_ascending",
    )
    .await;

    assert_eq!(ascending_list.items.len(), 3);

    let ascending_versions: Vec<String> = ascending_list
        .items
        .iter()
        .map(|item| item.system_version.to_string())
        .collect();
    assert_eq!(ascending_versions, vec!["1.0.0", "2.0.0", "3.0.0"]);

    // Test pagination by setting a small limit
    let paginated_list = objects_list_page_authz::<views::TufRepo>(
        client,
        "/v1/system/update/repositories?limit=2",
    )
    .await;

    assert_eq!(paginated_list.items.len(), 2);
    assert!(paginated_list.next_page.is_some());

    // First two items should be 3.0.0 and 2.0.0 (newest first)
    let paginated_versions: Vec<String> = paginated_list
        .items
        .iter()
        .map(|item| item.system_version.to_string())
        .collect();
    assert_eq!(paginated_versions, vec!["3.0.0", "2.0.0"]);

    // Fetch the next page via the returned page token and expect the remaining repo
    let next_page_url = format!(
        "/v1/system/update/repositories?limit=2&page_token={}",
        paginated_list.next_page.clone().expect("expected next page token"),
    );
    let next_page: ResultsPage<views::TufRepo> =
        objects_list_page_authz(client, &next_page_url).await;
    assert_eq!(next_page.items.len(), 1);
    assert_eq!(next_page.items[0].system_version.to_string(), "1.0.0");

    // Test filtering out pruned repos

    // Mark the 1.0.0 repo as pruned (use datastore methods since there's no API
    // for it)
    let datastore = cptestctx.server.server_context().nexus.datastore();
    let opctx = OpContext::for_tests(logctx.log.new(o!()), datastore.clone());

    let v1 = "1.0.0".parse::<Version>().unwrap();
    let repo_to_prune =
        datastore.tuf_repo_get_by_version(&opctx, v1.into()).await?;

    let generation = datastore.tuf_get_generation(&opctx).await?;
    let recent_releases =
        datastore.target_release_fetch_recent_distinct(&opctx, 3).await?;

    datastore
        .tuf_repo_mark_pruned(
            &opctx,
            generation,
            &recent_releases,
            repo_to_prune.id(),
        )
        .await?;

    // List repositories again - the pruned repo should not appear
    let list_after_prune: ResultsPage<views::TufRepo> =
        objects_list_page_authz(client, "/v1/system/update/repositories").await;

    assert_eq!(list_after_prune.items.len(), 2);
    let versions_after_prune: Vec<String> = list_after_prune
        .items
        .iter()
        .map(|item| item.system_version.to_string())
        .collect();
    assert_eq!(versions_after_prune, vec!["3.0.0", "2.0.0"]);

    cptestctx.teardown().await;
    Ok(())
}
