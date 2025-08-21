// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Get/set the target release via the external API.

use anyhow::Result;
use chrono::Utc;
use dropshot::test_util::ClientTestContext;
use http::StatusCode;
use http::method::Method;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::{NexusRequest, RequestBuilder};
use nexus_test_utils::test_setup;
use nexus_types::external_api::params::SetTargetReleaseParams;
use nexus_types::external_api::views::{TargetRelease, TargetReleaseSource};
use omicron_common::api::external::TufRepoInsertResponse;
use semver::Version;
use tufaceous_artifact::{ArtifactVersion, KnownArtifactKind};
use tufaceous_lib::assemble::ManifestTweak;

use crate::integration_tests::updates::TestTrustRoot;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_set_target_release() -> Result<()> {
    let ctx =
        test_setup::<omicron_nexus::Server>("get_set_target_release", 0).await;
    let client = &ctx.external_client;
    let logctx = &ctx.logctx;

    // There should always be a target release.
    let target_release: TargetRelease =
        NexusRequest::object_get(client, "/v1/system/update/target-release")
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert!(target_release.time_requested < Utc::now());
    assert_eq!(target_release.release_source, TargetReleaseSource::Unspecified);

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
    {
        let before = Utc::now();
        let system_version = Version::new(1, 0, 0);
        let response: TufRepoInsertResponse = trust_root
            .assemble_repo(&logctx.log, &[])
            .await?
            .into_upload_request(client, StatusCode::OK)
            .execute()
            .await?
            .parsed_body()?;
        assert_eq!(system_version, response.recorded.repo.system_version);

        let target_release =
            set_target_release(client, &system_version).await?;
        let after = Utc::now();
        assert!(target_release.time_requested >= before);
        assert!(target_release.time_requested <= after);
        assert_eq!(
            target_release.release_source,
            TargetReleaseSource::SystemVersion { version: system_version },
        );
    }

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
        let response: TufRepoInsertResponse = trust_root
            .assemble_repo(&logctx.log, tweaks)
            .await?
            .into_upload_request(client, StatusCode::OK)
            .execute()
            .await?
            .parsed_body()?;
        assert_eq!(system_version, response.recorded.repo.system_version);

        let target_release =
            set_target_release(client, &system_version).await?;
        let after = Utc::now();
        assert!(target_release.time_requested >= before);
        assert!(target_release.time_requested <= after);
        assert_eq!(
            target_release.release_source,
            TargetReleaseSource::SystemVersion { version: system_version },
        );
    }

    // Attempting to downgrade to an earlier system version (2.0.0 → 1.0.0)
    // should not be allowed.
    set_target_release(client, &Version::new(1, 0, 0))
        .await
        .expect_err("shouldn't be able to downgrade system");

    ctx.teardown().await;
    Ok(())
}

pub async fn set_target_release(
    client: &ClientTestContext,
    system_version: &Version,
) -> Result<TargetRelease> {
    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::PUT,
            "/v1/system/update/target-release",
        )
        .body(Some(&SetTargetReleaseParams {
            system_version: system_version.clone(),
        }))
        .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .map(|response| response.parsed_body().unwrap())
}
