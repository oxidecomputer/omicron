// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tests Support Bundles

use anyhow::Context;
use anyhow::Result;
use dropshot::HttpErrorResponseBody;
use dropshot::test_util::ClientTestContext;
use http::StatusCode;
use http::method::Method;
use nexus_client::types::LastResult;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::shared::SupportBundleInfo;
use nexus_types::external_api::shared::SupportBundleState;
use nexus_types::internal_api::background::SupportBundleCleanupReport;
use nexus_types::internal_api::background::SupportBundleCollectionReport;
use omicron_common::api::internal::shared::DatasetKind;
use omicron_uuid_kinds::SupportBundleUuid;
use serde::Deserialize;
use std::io::Cursor;
use zip::read::ZipArchive;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;
type DiskTestBuilder<'a> = nexus_test_utils::resource_helpers::DiskTestBuilder<
    'a,
    omicron_nexus::Server,
>;

// -- HTTP methods --
//
// The following are a set of helper functions to access Support Bundle APIs
// through the public interface.

const BUNDLES_URL: &str = "/experimental/v1/system/support-bundles";

async fn expect_not_found(
    client: &ClientTestContext,
    bundle_id: SupportBundleUuid,
    bundle_url: &str,
    method: Method,
) -> Result<()> {
    let response = NexusRequest::expect_failure(
        client,
        StatusCode::NOT_FOUND,
        method.clone(),
        &bundle_url,
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .context("Failed to execute request and get response")?;

    // HEAD requests should not return bodies
    if method == Method::HEAD {
        return Ok(());
    }

    let error: HttpErrorResponseBody =
        response.parsed_body().context("Failed to parse response")?;

    let expected =
        format!("not found: support-bundle with id \"{}\"", bundle_id);
    if error.message != expected {
        anyhow::bail!(
            "Unexpected error: {} (wanted {})",
            error.message,
            expected
        );
    }
    Ok(())
}

async fn bundles_list(
    client: &ClientTestContext,
) -> Result<Vec<SupportBundleInfo>> {
    Ok(NexusRequest::iter_collection_authn(client, BUNDLES_URL, "", None)
        .await
        .context("failed to list bundles")?
        .all_items)
}

async fn bundle_get(
    client: &ClientTestContext,
    id: SupportBundleUuid,
) -> Result<SupportBundleInfo> {
    let url = format!("{BUNDLES_URL}/{id}");
    NexusRequest::object_get(client, &url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .with_context(|| format!("failed to make \"GET\" request to {url}"))?
        .parsed_body()
}

async fn bundle_get_expect_fail(
    client: &ClientTestContext,
    id: SupportBundleUuid,
    expected_status: StatusCode,
    expected_message: &str,
) -> Result<()> {
    let url = format!("{BUNDLES_URL}/{id}");
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::GET, &url)
            .expect_status(Some(expected_status)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .context("should have failed to GET bundle")?
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .context("failed to response error from bundle GET")?;

    if error.message != expected_message {
        anyhow::bail!(
            "Unexpected error: {} (wanted {})",
            error.message,
            expected_message
        );
    }
    Ok(())
}

async fn bundle_delete(
    client: &ClientTestContext,
    id: SupportBundleUuid,
) -> Result<()> {
    let url = format!("{BUNDLES_URL}/{id}");
    NexusRequest::object_delete(client, &url)
        .authn_as(AuthnMode::PrivilegedUser)
        .execute()
        .await
        .with_context(|| {
            format!("failed to make \"DELETE\" request to {url}")
        })?;
    Ok(())
}

async fn bundle_create(
    client: &ClientTestContext,
) -> Result<SupportBundleInfo> {
    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, BUNDLES_URL)
            .expect_status(Some(StatusCode::CREATED)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .context("failed to request bundle creation")?
    .parsed_body()
    .context("failed to parse 'create bundle' response")
}

async fn bundle_create_expect_fail(
    client: &ClientTestContext,
    expected_status: StatusCode,
    expected_message: &str,
) -> Result<()> {
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, BUNDLES_URL)
            .expect_status(Some(expected_status)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .context("should have failed to create bundle")?
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .context("failed to response error from bundle creation")?;

    if error.message != expected_message {
        anyhow::bail!(
            "Unexpected error: {} (wanted {})",
            error.message,
            expected_message
        );
    }
    Ok(())
}

async fn bundle_download(
    client: &ClientTestContext,
    id: SupportBundleUuid,
) -> Result<bytes::Bytes> {
    let url = format!("{BUNDLES_URL}/{id}/download");
    let body = NexusRequest::new(
        RequestBuilder::new(client, Method::GET, &url)
            .expect_status(Some(StatusCode::OK))
            .expect_range_requestable(),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .context("failed to request bundle download")?
    .body;

    Ok(body)
}

async fn bundle_download_expect_fail(
    client: &ClientTestContext,
    id: SupportBundleUuid,
    expected_status: StatusCode,
    expected_message: &str,
) -> Result<()> {
    let url = format!("{BUNDLES_URL}/{id}/download");
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::GET, &url)
            .expect_status(Some(expected_status)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .context("failed to request bundle download")?
    .parsed_body::<dropshot::HttpErrorResponseBody>()
    .context("failed to response error from bundle download")?;

    if error.message != expected_message {
        anyhow::bail!(
            "Unexpected error: {} (wanted {})",
            error.message,
            expected_message
        );
    }
    Ok(())
}

// -- Background Task --
//
// The following logic helps us trigger and observe the output of the support
// bundle background task.

#[derive(Deserialize)]
struct TaskOutput {
    cleanup_err: Option<String>,
    collection_err: Option<String>,
    cleanup_report: Option<SupportBundleCleanupReport>,
    collection_report: Option<SupportBundleCollectionReport>,
}

async fn activate_bundle_collection_background_task(
    cptestctx: &ControlPlaneTestContext,
) -> TaskOutput {
    use nexus_test_utils::background::activate_background_task;

    let task = activate_background_task(
        &cptestctx.internal_client,
        "support_bundle_collector",
    )
    .await;

    let LastResult::Completed(result) = task.last else {
        panic!("Task did not complete");
    };
    serde_json::from_value(result.details).expect(
        "Should have been able to deserialize TaskOutput from background task",
    )
}

// Test accessing support bundle interfaces when the bundle does not exist,
// and when no U.2s exist on which to store support bundles.
#[nexus_test]
async fn test_support_bundle_not_found(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let id = SupportBundleUuid::new_v4();

    expect_not_found(
        &client,
        id,
        &format!("{BUNDLES_URL}/{id}"),
        Method::DELETE,
    )
    .await
    .unwrap();

    expect_not_found(&client, id, &format!("{BUNDLES_URL}/{id}"), Method::GET)
        .await
        .unwrap();

    expect_not_found(
        &client,
        id,
        &format!("{BUNDLES_URL}/{id}/download"),
        Method::GET,
    )
    .await
    .unwrap();

    expect_not_found(
        &client,
        id,
        &format!("{BUNDLES_URL}/{id}/download/single-file"),
        Method::GET,
    )
    .await
    .unwrap();

    expect_not_found(
        &client,
        id,
        &format!("{BUNDLES_URL}/{id}/download"),
        Method::HEAD,
    )
    .await
    .unwrap();

    expect_not_found(
        &client,
        id,
        &format!("{BUNDLES_URL}/{id}/download/single-file"),
        Method::HEAD,
    )
    .await
    .unwrap();

    expect_not_found(
        &client,
        id,
        &format!("{BUNDLES_URL}/{id}/download/index"),
        Method::HEAD,
    )
    .await
    .unwrap();

    assert!(bundles_list(&client).await.unwrap().is_empty());

    bundle_create_expect_fail(
        &client,
        StatusCode::INSUFFICIENT_STORAGE,
        "Insufficient capacity: Current policy limits support bundle creation to 'one per external disk', and no disks are available. You must delete old support bundles before new ones can be created",
    ).await.unwrap();
}

// Test the create, read, and deletion operations on a bundle.
#[nexus_test]
async fn test_support_bundle_lifecycle(cptestctx: &ControlPlaneTestContext) {
    let client = &cptestctx.external_client;

    let disk_test =
        DiskTestBuilder::new(&cptestctx).with_zpool_count(1).build().await;

    // Validate our test setup: We should see a single Debug dataset
    // in our disk test.
    let mut debug_dataset_count = 0;
    for zpool in disk_test.zpools() {
        for dataset in &zpool.datasets {
            if matches!(dataset.kind, DatasetKind::Debug) {
                debug_dataset_count += 1;
            }
        }
    }
    assert_eq!(debug_dataset_count, 1);

    // We should see no bundles before we start creation.
    assert!(bundles_list(&client).await.unwrap().is_empty());
    let bundle = bundle_create(&client).await.unwrap();

    assert_eq!(bundle.reason_for_creation, "Created by external API");
    assert_eq!(bundle.reason_for_failure, None);
    assert_eq!(bundle.state, SupportBundleState::Collecting);

    let bundles = bundles_list(&client).await.unwrap();
    assert_eq!(bundles.len(), 1);
    assert_eq!(bundles[0].id, bundle.id);
    assert_eq!(bundle_get(&client, bundle.id).await.unwrap().id, bundle.id);

    // We can't collect a second bundle because the debug dataset already fully
    // occupied.
    //
    // We'll retry this at the end of the test, and see that we can create
    // another bundle when the first is cleared.
    bundle_create_expect_fail(
        &client,
        StatusCode::INSUFFICIENT_STORAGE,
        "Insufficient capacity: Current policy limits support bundle creation to 'one per external disk', and no disks are available. You must delete old support bundles before new ones can be created",
    ).await.unwrap();

    // The bundle is "Collecting", not "Active", so we can't download it yet.
    bundle_download_expect_fail(
        &client,
        bundle.id,
        StatusCode::BAD_REQUEST,
        "Cannot download bundle in non-active state",
    )
    .await
    .unwrap();

    // If we prompt the background task to run, the bundle should transition to
    // "Active".
    let output = activate_bundle_collection_background_task(&cptestctx).await;
    assert_eq!(output.cleanup_err, None);
    assert_eq!(output.collection_err, None);
    assert_eq!(
        output.cleanup_report,
        Some(SupportBundleCleanupReport { ..Default::default() })
    );
    assert_eq!(
        output.collection_report,
        Some(SupportBundleCollectionReport {
            bundle: bundle.id,
            listed_in_service_sleds: true,
            activated_in_db_ok: true,
        })
    );
    let bundle = bundle_get(&client, bundle.id).await.unwrap();
    assert_eq!(bundle.state, SupportBundleState::Active);

    // Now we should be able to download the bundle
    let contents = bundle_download(&client, bundle.id).await.unwrap();
    let archive = ZipArchive::new(Cursor::new(&contents)).unwrap();
    let mut names = archive.file_names();
    assert_eq!(names.next(), Some("bundle_id.txt"));
    assert_eq!(names.next(), Some("rack/"));
    // There's much more data in the bundle, but validating it isn't the point
    // of this test, which cares more about bundle lifecycle.

    // We are also able to delete the bundle
    bundle_delete(&client, bundle.id).await.unwrap();
    let observed = bundle_get(&client, bundle.id).await.unwrap();
    assert_eq!(observed.state, SupportBundleState::Destroying);

    // We cannot download anything after bundle deletion starts
    bundle_download_expect_fail(
        &client,
        bundle.id,
        StatusCode::BAD_REQUEST,
        "Cannot download bundle in non-active state",
    )
    .await
    .unwrap();

    // If we prompt the background task to run, the bundle will be cleaned up.
    let output = activate_bundle_collection_background_task(&cptestctx).await;
    assert_eq!(output.cleanup_err, None);
    assert_eq!(output.collection_err, None);
    assert_eq!(
        output.cleanup_report,
        Some(SupportBundleCleanupReport {
            sled_bundles_deleted_ok: 1,
            db_destroying_bundles_removed: 1,
            ..Default::default()
        })
    );
    assert_eq!(output.collection_report, None);

    // The bundle is now fully deleted, so it should no longer appear.
    bundle_get_expect_fail(
        &client,
        bundle.id,
        StatusCode::NOT_FOUND,
        &format!("not found: support-bundle with id \"{}\"", bundle.id),
    )
    .await
    .unwrap();

    // We can now create a second bundle, as the first has been freed.
    let second_bundle = bundle_create(&client).await.unwrap();

    assert_ne!(
        second_bundle.id, bundle.id,
        "We should have made a distinct bundle"
    );
    assert_eq!(second_bundle.reason_for_creation, "Created by external API");
    assert_eq!(second_bundle.state, SupportBundleState::Collecting);
}
