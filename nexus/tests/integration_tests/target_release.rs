// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Get/set the target release via the external API.

use camino_tempfile::Utf8TempDir;
use chrono::Utc;
use clap::Parser as _;
use dropshot::test_util::LogContext;
use nexus_config::UpdatesConfig;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::{NexusRequest, RequestBuilder};
use nexus_test_utils::load_test_config;
use nexus_test_utils::test_setup_with_config;
use nexus_types::external_api::params::SetTargetReleaseParams;
use nexus_types::external_api::shared::{TargetRelease, TargetReleaseSource};
use omicron_common::api::external::TufRepoInsertResponse;
use omicron_sled_agent::sim;
use semver::Version;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_set_target_release() -> anyhow::Result<()> {
    let mut config = load_test_config();
    config.pkg.updates = Some(UpdatesConfig {
        // XXX: This is currently not used by the update system, but
        // trusted_root will become meaningful in the future.
        trusted_root: "does-not-exist.json".into(),
    });
    let ctx = test_setup_with_config::<omicron_nexus::Server>(
        "test_update_uninitialized",
        &mut config,
        sim::SimMode::Explicit,
        None,
        0,
    )
    .await;
    let client = &ctx.external_client;

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
    let version = Version::new(0, 0, 0);
    NexusRequest::objects_post(
        client,
        "/v1/system/update/target-release",
        &SetTargetReleaseParams {
            release_source: TargetReleaseSource::SystemVersion(version),
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .expect_err("invalid TUF repo");

    // Adding a fake (tufaceous) repo and then setting it as the
    // target release should succeed.
    let before = Utc::now();
    let version = Version::new(1, 0, 0);
    let logctx = LogContext::new("get_set_target_release", &config.pkg.log);
    let temp = Utf8TempDir::new().unwrap();
    let path = temp.path().join("repo.zip");
    tufaceous::Args::try_parse_from([
        "tufaceous",
        "assemble",
        "../tufaceous/manifests/fake.toml",
        path.as_str(),
    ])
    .expect("can't parse tufaceous args")
    .exec(&logctx.log)
    .await
    .expect("can't assemble TUF repo");

    assert_eq!(
        version,
        NexusRequest::new(
            RequestBuilder::new(
                client,
                http::Method::PUT,
                "/v1/system/update/repository?file_name=/tmp/foo.zip",
            )
            .body_file(Some(&path))
            .expect_status(Some(http::StatusCode::OK)),
        )
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .unwrap()
        .parsed_body::<TufRepoInsertResponse>()
        .unwrap()
        .recorded
        .repo
        .system_version
    );

    let target_release: TargetRelease = NexusRequest::objects_post(
        client,
        "/v1/system/update/target-release",
        &SetTargetReleaseParams {
            release_source: TargetReleaseSource::SystemVersion(version.clone()),
        },
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .unwrap()
    .parsed_body()
    .unwrap();
    let after = Utc::now();
    assert_eq!(target_release.generation, 2);
    assert!(target_release.time_requested >= before);
    assert!(target_release.time_requested <= after);
    assert_eq!(
        target_release.release_source,
        TargetReleaseSource::SystemVersion(version),
    );

    ctx.teardown().await;
    logctx.cleanup_successful();
    Ok(())
}
