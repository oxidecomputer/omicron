// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{Context, Result, ensure};
use camino::Utf8Path;
use camino_tempfile::{Builder, Utf8TempPath};
use chrono::{DateTime, Duration, Timelike, Utc};
use dropshot::ResultsPage;
use http::{Method, StatusCode};
use nexus_db_queries::context::OpContext;
use nexus_test_utils::background::run_tuf_artifact_replication_step;
use nexus_test_utils::background::wait_tuf_artifact_replication_step;
use nexus_test_utils::http_testing::{AuthnMode, NexusRequest, RequestBuilder};
use nexus_test_utils::test_setup;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::views::UpdatesTrustRoot;
use omicron_common::api::external::{
    TufRepoGetResponse, TufRepoInsertResponse, TufRepoInsertStatus,
};
use pretty_assertions::assert_eq;
use semver::Version;
use serde::Deserialize;
use std::collections::HashSet;
use std::fmt::Debug;
use tough::editor::signed::SignedRole;
use tough::schema::Root;
use tufaceous_artifact::KnownArtifactKind;
use tufaceous_lib::Key;
use tufaceous_lib::assemble::{ArtifactManifest, OmicronRepoAssembler};
use tufaceous_lib::assemble::{DeserializedManifest, ManifestTweak};

const TRUST_ROOTS_URL: &str = "/v1/system/update/trust-roots";

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

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
            "/v1/system/update/repository?file_name={}",
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
        run_tuf_artifact_replication_step(&cptestctx.internal_client).await;
    assert_eq!(
        status.last_run_counters.put_ok + status.last_run_counters.copy_ok,
        0
    );
    assert_eq!(status.local_repos, 0);

    // Attempt to fetch a repository description from Nexus. This should fail
    // with a 404 error.
    {
        make_get_request(
            client,
            "1.0.0".parse().unwrap(),
            StatusCode::NOT_FOUND,
        )
        .execute()
        .await
        .context("repository fetch should have failed with 404 error")?;
    }

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
    let mut initial_description = {
        let response = repo
            .to_upload_request(client, StatusCode::OK)
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
        ["zone-1", "zone-2"]
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
        let response = repo
            .into_upload_request(client, StatusCode::OK)
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
    {
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

        let response =
            serde_json::from_slice::<TufRepoInsertResponse>(&response.body)
                .context("error deserializing response body")?;
        assert_eq!(response.status, TufRepoInsertStatus::Inserted);
        let mut description = response.recorded;
        description.sort_artifacts();

        // The artifacts should be exactly the same as the 1.0.0 repo we
        // uploaded, other than the installinator document (which will have
        // system version 2.0.0).
        let mut installinator_doc_1 = None;
        let filtered_artifacts_1 = initial_description
            .artifacts
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
        let filtered_artifacts_2 = description
            .artifacts
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

        // Now get the repository that was just uploaded and make sure the
        // artifact list is the same.
        let response: TufRepoGetResponse =
            make_get_request(client, "2.0.0".parse().unwrap(), StatusCode::OK)
                .execute()
                .await
                .context("error fetching repository")?
                .parsed_body()?;
        let mut get_description = response.description;
        get_description.sort_artifacts();

        assert_eq!(
            description, get_description,
            "initial description matches fetched description"
        );
    }
    // The installinator document changed, so the generation number is bumped to
    // 3.
    assert_eq!(
        datastore.tuf_get_generation(&opctx).await.unwrap(),
        3u32.into()
    );
    // ... and the task will have one artifact to replicate.
    let status =
        wait_tuf_artifact_replication_step(&cptestctx.internal_client).await;
    eprintln!("{status:?}");
    assert_eq!(status.generation, 3u32.into());
    assert_eq!(status.last_run_counters.list_ok, 4);
    assert_eq!(status.last_run_counters.put_ok, 3);
    assert_eq!(status.last_run_counters.copy_ok, 1);
    assert_eq!(status.local_repos, 1);

    cptestctx.teardown().await;
    Ok(())
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

#[nexus_test]
async fn test_trust_root_operations(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;
    let trust_root =
        TestTrustRoot::generate().await.expect("trust root generation failed");

    // POST /v1/system/update/trust-roots
    let trust_root_view: UpdatesTrustRoot = trust_root
        .to_upload_request(client, StatusCode::CREATED)
        .execute()
        .await
        .expect("trust root add failed")
        .parsed_body()
        .expect("failed to parse add response");

    // GET /v1/system/update/trust-roots
    let request = RequestBuilder::new(client, Method::GET, TRUST_ROOTS_URL)
        .expect_status(Some(StatusCode::OK));
    let response: ResultsPage<UpdatesTrustRoot> = NexusRequest::new(request)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("trust root list failed")
        .parsed_body()
        .expect("failed to parse list response");
    assert_eq!(response.items, &[trust_root_view.clone()]);

    // GET /v1/system/update/trust-roots/{id}
    let id_url = format!("{TRUST_ROOTS_URL}/{}", trust_root_view.id);
    let request = RequestBuilder::new(client, Method::GET, &id_url)
        .expect_status(Some(StatusCode::OK));
    let response: UpdatesTrustRoot = NexusRequest::new(request)
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
    let response: ResultsPage<UpdatesTrustRoot> = NexusRequest::new(request)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .expect("trust root list after delete failed")
        .parsed_body()
        .expect("failed to parse list after delete response");
    assert!(response.items.is_empty());
}
