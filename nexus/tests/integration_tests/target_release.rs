// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Get/set the target release via the external API.

use anyhow::Result;
use camino::Utf8Path;
use camino_tempfile::Utf8TempDir;
use chrono::Utc;
use clap::Parser as _;
use dropshot::test_util::{ClientTestContext, LogContext};
use http::StatusCode;
use http::method::Method;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::{NexusRequest, RequestBuilder};
use nexus_test_utils::load_test_config;
use nexus_test_utils::test_setup_with_config;
use nexus_types::external_api::params::SetTargetReleaseParams;
use nexus_types::external_api::views::{TargetRelease, TargetReleaseSource};
use omicron_common::api::external::TufRepoInsertResponse;
use omicron_sled_agent::sim;
use semver::Version;
use tufaceous_lib::Key;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_set_target_release() -> Result<()> {
    let mut config = load_test_config();
    let ctx = test_setup_with_config::<omicron_nexus::Server>(
        "test_update_uninitialized",
        &mut config,
        sim::SimMode::Explicit,
        None,
        0,
    )
    .await;
    let client = &ctx.external_client;
    let logctx = LogContext::new("get_set_target_release", &config.pkg.log);

    // There should always be a target release.
    let target_release: TargetRelease =
        NexusRequest::object_get(client, "/v1/system/update/target-release")
            .authn_as(AuthnMode::PrivilegedUser)
            .execute()
            .await
            .unwrap()
            .parsed_body()
            .unwrap();
    assert_eq!(target_release.generation, 1);
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

    let temp = Utf8TempDir::new().unwrap();
    let key = Key::generate_ed25519().unwrap();

    // Adding a fake (tufaceous) repo and then setting it as the
    // target release should succeed.
    {
        let before = Utc::now();
        let system_version = Version::new(1, 0, 0);
        let path = temp.path().join("repo-1.0.0.zip");
        tufaceous::Args::try_parse_from([
            "tufaceous",
            "assemble",
            "--key",
            &key.to_string(),
            "../update-common/manifests/fake.toml",
            path.as_str(),
        ])
        .expect("can't parse tufaceous args")
        .exec(&logctx.log)
        .await
        .expect("can't assemble TUF repo");

        assert_eq!(
            system_version,
            upload_tuf_repo(client, &path).await?.recorded.repo.system_version
        );

        let target_release =
            set_target_release(client, system_version.clone()).await?;
        let after = Utc::now();
        assert_eq!(target_release.generation, 2);
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
        let path = temp.path().join("repo-2.0.0.zip");
        tufaceous::Args::try_parse_from([
            "tufaceous",
            "assemble",
            "--key",
            &key.to_string(),
            "../update-common/manifests/fake-non-semver.toml",
            "--allow-non-semver",
            path.as_str(),
        ])
        .expect("can't parse tufaceous args")
        .exec(&logctx.log)
        .await
        .expect("can't assemble TUF repo");

        assert_eq!(
            system_version,
            upload_tuf_repo(client, &path).await?.recorded.repo.system_version
        );

        let target_release =
            set_target_release(client, system_version.clone()).await?;
        let after = Utc::now();
        assert_eq!(target_release.generation, 3);
        assert!(target_release.time_requested >= before);
        assert!(target_release.time_requested <= after);
        assert_eq!(
            target_release.release_source,
            TargetReleaseSource::SystemVersion { version: system_version },
        );
    }

    // Attempting to downgrade to an earlier system version (2.0.0 → 1.0.0)
    // should not be allowed.
    set_target_release(client, Version::new(1, 0, 0))
        .await
        .expect_err("shouldn't be able to downgrade system");

    ctx.teardown().await;
    logctx.cleanup_successful();
    Ok(())
}

async fn upload_tuf_repo(
    client: &ClientTestContext,
    path: &Utf8Path,
) -> Result<TufRepoInsertResponse> {
    NexusRequest::new(
        RequestBuilder::new(
            client,
            http::Method::PUT,
            "/v1/system/update/repository?file_name=/tmp/foo.zip",
        )
        .body_file(Some(path))
        .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .map(|response| response.parsed_body().unwrap())
}

async fn set_target_release(
    client: &ClientTestContext,
    system_version: Version,
) -> Result<TargetRelease> {
    NexusRequest::new(
        RequestBuilder::new(
            client,
            Method::PUT,
            "/v1/system/update/target-release",
        )
        .body(Some(&SetTargetReleaseParams { system_version }))
        .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .map(|response| response.parsed_body().unwrap())
}
