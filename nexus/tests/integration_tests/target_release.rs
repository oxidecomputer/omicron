// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Get/set the target release via the external API.

use anyhow::Context as _;
use anyhow::Result;
use chrono::Utc;
use dropshot::HttpErrorResponseBody;
use dropshot::test_util::ClientTestContext;
use http::StatusCode;
use http::method::Method;
use nexus_db_queries::authn;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_lockstep_client::types::BlueprintTargetSet;
use nexus_test_interface::NexusServer;
use nexus_test_utils::ControlPlaneTestContext;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::TestResponse;
use nexus_test_utils::http_testing::{NexusRequest, RequestBuilder};
use nexus_test_utils::resource_helpers::object_get;
use nexus_types::deployment::BlueprintArtifactVersion;
use nexus_types::deployment::BlueprintHostPhase2DesiredContents;
use nexus_types::deployment::BlueprintZoneImageSource;
use nexus_types::external_api::params::SetTargetReleaseParams;
use nexus_types::external_api::views;
use omicron_uuid_kinds::{BlueprintUuid, GenericUuid};
use semver::Version;
use std::sync::Arc;
use tufaceous_artifact::ArtifactKind;
use tufaceous_artifact::{ArtifactVersion, KnownArtifactKind};
use tufaceous_lib::assemble::ManifestTweak;

use crate::integration_tests::updates::TestTrustRoot;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_set_target_release() -> Result<()> {
    let ctx =
        nexus_test_utils::ControlPlaneBuilder::new("get_set_target_release")
            .start::<omicron_nexus::Server>()
            .await;
    let client = &ctx.external_client;
    let logctx = &ctx.logctx;

    // There is no target release before one has ever been specified
    let status: views::UpdateStatus =
        object_get(client, "/v1/system/update/status").await;
    assert_eq!(status.target_release.0, None);

    // Attempting to set an invalid system version should fail.
    let system_version = Version::new(0, 0, 0);
    NexusRequest::object_put(
        client,
        "/v1/system/update/target-release",
        Some(&SetTargetReleaseParams { system_version }),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect_err("invalid TUF repo");

    let trust_root = TestTrustRoot::generate().await?;
    trust_root.to_upload_request(client, StatusCode::CREATED).execute().await?;

    // Adding a fake (tufaceous) repo and then setting it as the
    // target release should succeed.
    let version_1_0_0 = {
        let before = Utc::now();
        let system_version = Version::new(1, 0, 0);
        let response: views::TufRepoUpload = trust_root
            .assemble_repo(&logctx.log, &[])
            .await?
            .into_upload_request(client, StatusCode::OK)
            .execute()
            .await?
            .parsed_body()?;
        assert_eq!(system_version, response.repo.system_version);

        set_target_release_for_update_with_expected_status(
            client,
            &system_version,
            StatusCode::NO_CONTENT,
        )
        .await?;

        let status: views::UpdateStatus =
            object_get(client, "/v1/system/update/status").await;

        let target_release = status.target_release.0.unwrap();
        let after = Utc::now();
        assert!(target_release.time_requested >= before);
        assert!(target_release.time_requested <= after);
        assert_eq!(target_release.version, system_version);
        system_version
    };

    // Fake completing an update to 1.0.0.
    install_target_blueprint_with_update_complete(&ctx, &version_1_0_0).await?;

    // Adding a repo with non-semver artifact versions should be ok, too.
    {
        let before = Utc::now();
        let system_version = Version::new(2, 0, 0);
        let tweaks = &[
            ManifestTweak::SystemVersion(system_version.clone()),
            ManifestTweak::ArtifactVersion {
                kind: KnownArtifactKind::SwitchRotBootloader,
                version: ArtifactVersion::new("non-semver-2").unwrap(),
            },
        ];
        let response: views::TufRepoUpload = trust_root
            .assemble_repo(&logctx.log, tweaks)
            .await?
            .into_upload_request(client, StatusCode::OK)
            .execute()
            .await?
            .parsed_body()?;
        assert_eq!(system_version, response.repo.system_version);

        set_target_release_for_update_with_expected_status(
            client,
            &system_version,
            StatusCode::NO_CONTENT,
        )
        .await?;

        let status: views::UpdateStatus =
            object_get(client, "/v1/system/update/status").await;

        let target_release = status.target_release.0.unwrap();
        let after = Utc::now();
        assert!(target_release.time_requested >= before);
        assert!(target_release.time_requested <= after);
        assert_eq!(target_release.version, system_version);
    }

    // Attempting to downgrade to an earlier system version (2.0.0 → 1.0.0)
    // should not be allowed.
    let response = set_target_release_for_update_with_expected_status(
        client,
        &version_1_0_0,
        StatusCode::BAD_REQUEST,
    )
    .await?;
    let err = response.parsed_body::<HttpErrorResponseBody>().unwrap();
    assert!(
        err.message.contains("cannot downgrade"),
        "unexpected error message: {}",
        err.message
    );

    // We also shouldn't be able to upgrade to a subsequent version, because we
    // haven't completed the update to 2.0.0 yet (the current blueprint is still
    // the one we installed above to complete the update to 1.0.0).
    let response = set_target_release_for_update_with_expected_status(
        client,
        &Version::new(3, 0, 0),
        StatusCode::BAD_REQUEST,
    )
    .await?;
    let err = response.parsed_body::<HttpErrorResponseBody>().unwrap();
    assert!(
        err.message.contains("previous update is still in progress"),
        "unexpected error message: {}",
        err.message
    );

    ctx.teardown().await;
    Ok(())
}

/// Create a `Blueprint` with all host OS and zone image sources set to match
/// artifacts from the TUF repo with the given `system_version`.
///
/// This emulates completion of a system update sufficiently to start another
/// subsequent update. A TUF repo matching `system_version` must already exist
/// in the database.
pub async fn install_target_blueprint_with_update_complete<N: NexusServer>(
    ctx: &ControlPlaneTestContext<N>,
    system_version: &Version,
) -> Result<(), anyhow::Error> {
    let lockstep_client = ctx.lockstep_client();
    let datastore = ctx.server.datastore();
    let log = &ctx.logctx.log;
    let opctx = OpContext::for_background(
        log.clone(),
        Arc::new(authz::Authz::new(log)),
        authn::Context::internal_api(),
        datastore.clone(),
    );

    // Get the TUF repo at `system_version`.
    let tuf_repo = datastore
        .tuf_repo_get_by_version(&opctx, system_version.clone().into())
        .await
        .with_context(|| {
            format!("getting TUF repo with version {system_version}")
        })?;
    let tuf_repo_artifacts =
        datastore.tuf_list_repo_artifacts(&opctx, tuf_repo.id()).await?;

    // Get the hashes of host phase 2 and some zone. (We don't have to match up
    // particular zone kinds; we can use one zone artifact for all zones. This
    // seems a little fishy, but we rely on the planner to do the right thing.)
    let host_phase_2_artifact_hash = tuf_repo_artifacts
        .iter()
        .find_map(|a| {
            if a.kind == ArtifactKind::HOST_PHASE_2.as_str() {
                Some(a.sha256)
            } else {
                None
            }
        })
        .context("no host phase 2 artifact in TUF repo")?;
    let zone_artifact_hash = tuf_repo_artifacts
        .iter()
        .find_map(|a| {
            if a.kind == KnownArtifactKind::Zone.to_string() {
                Some(a.sha256)
            } else {
                None
            }
        })
        .context("no zone artifact in TUF repo")?;

    // Get the current blueprint.
    let target_id =
        lockstep_client.blueprint_target_view().await?.into_inner().target_id;
    let mut blueprint = lockstep_client
        .blueprint_view(target_id.as_untyped_uuid())
        .await?
        .into_inner();

    // Create a child blueprint by mutating the IDs in place; update metadata
    // for easier manual debugging if needed.
    blueprint.parent_blueprint_id = Some(blueprint.id);
    blueprint.id = BlueprintUuid::new_v4();
    blueprint.time_created = Utc::now();
    blueprint.creator = "get_set_target_release test".to_string();
    blueprint.comment = "manual update to 1.0.0".to_string();

    // Modify all the OS and zone sources to point to the 1.0.0 TUF repo.
    let bp_artifact_version = BlueprintArtifactVersion::Available {
        version: ArtifactVersion::new(system_version.to_string()).unwrap(),
    };
    for sled in blueprint.sleds.values_mut() {
        // We should only need to set the "active" slot, but we don't track that
        // in the blueprint. Set both.
        sled.host_phase_2.slot_a =
            BlueprintHostPhase2DesiredContents::Artifact {
                version: bp_artifact_version.clone(),
                hash: *host_phase_2_artifact_hash,
            };
        sled.host_phase_2.slot_b =
            BlueprintHostPhase2DesiredContents::Artifact {
                version: bp_artifact_version.clone(),
                hash: *host_phase_2_artifact_hash,
            };

        for mut zone in sled.zones.iter_mut() {
            zone.image_source = BlueprintZoneImageSource::Artifact {
                version: bp_artifact_version.clone(),
                hash: *zone_artifact_hash,
            };
        }
    }

    // Import this blueprint and make it the new target, reflecting a
    // completed update to `system_version`.
    lockstep_client.blueprint_import(&blueprint).await?;
    lockstep_client
        .blueprint_target_set(&BlueprintTargetSet {
            enabled: false,
            target_id: blueprint.id,
        })
        .await?;

    Ok(())
}

pub async fn set_target_release_for_update_with_expected_status(
    client: &ClientTestContext,
    system_version: &Version,
    expected_status: StatusCode,
) -> Result<TestResponse, anyhow::Error> {
    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::PUT,
            "/v1/system/update/target-release",
        )
        .body(Some(&SetTargetReleaseParams {
            system_version: system_version.clone(),
        }))
        .expect_status(Some(expected_status)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
}

pub async fn set_target_release_for_mupdate_recovery(
    client: &ClientTestContext,
    system_version: &Version,
) -> Result<(), anyhow::Error> {
    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::PUT,
            "/v1/system/update/target-release/recovery",
        )
        .body(Some(&SetTargetReleaseParams {
            system_version: system_version.clone(),
        }))
        .expect_status(Some(StatusCode::NO_CONTENT)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .map(|_| ())
}
