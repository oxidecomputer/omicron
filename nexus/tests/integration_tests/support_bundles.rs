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
use nexus_types::internal_api::background::SupportBundleEreportStatus;
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
    bundle_create_with_comment(client, None).await
}

async fn bundle_create_with_comment(
    client: &ClientTestContext,
    user_comment: Option<String>,
) -> Result<SupportBundleInfo> {
    use nexus_types::external_api::params::SupportBundleCreate;

    let create_params = SupportBundleCreate { user_comment };

    NexusRequest::new(
        RequestBuilder::new(client, Method::POST, BUNDLES_URL)
            .body(Some(&create_params))
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
    use nexus_types::external_api::params::SupportBundleCreate;

    let create_params = SupportBundleCreate { user_comment: None };
    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, BUNDLES_URL)
            .body(Some(&create_params))
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
            .expect_range_requestable("application/zip"),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .context("failed to request bundle download")?
    .body;

    Ok(body)
}

async fn bundle_download_head(
    client: &ClientTestContext,
    id: SupportBundleUuid,
) -> Result<usize> {
    let url = format!("{BUNDLES_URL}/{id}/download");
    let len = NexusRequest::new(
        RequestBuilder::new(client, Method::HEAD, &url)
            .expect_status(Some(StatusCode::OK))
            .expect_range_requestable("application/zip"),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .context("failed to request bundle download")?
    .headers
    .get(http::header::CONTENT_LENGTH)
    .context("Missing content length response header")?
    .to_str()
    .context("Failed to convert content length to string")?
    .parse()
    .context("Failed to parse content length")?;

    Ok(len)
}

async fn bundle_download_range(
    client: &ClientTestContext,
    id: SupportBundleUuid,
    value: &str,
    expected_content_range: &str,
) -> Result<bytes::Bytes> {
    let url = format!("{BUNDLES_URL}/{id}/download");
    let body = NexusRequest::new(
        RequestBuilder::new(client, Method::GET, &url)
            .header(http::header::RANGE, value)
            .expect_status(Some(StatusCode::PARTIAL_CONTENT))
            .expect_response_header(
                http::header::CONTENT_RANGE,
                expected_content_range,
            )
            .expect_range_requestable("application/zip"),
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

async fn bundle_update_comment(
    client: &ClientTestContext,
    id: SupportBundleUuid,
    comment: Option<String>,
) -> Result<SupportBundleInfo> {
    use nexus_types::external_api::params::SupportBundleUpdate;

    let url = format!("{BUNDLES_URL}/{id}");
    let update = SupportBundleUpdate { user_comment: comment };

    NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &url)
            .body(Some(&update))
            .expect_status(Some(StatusCode::OK)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .context("failed to update bundle comment")?
    .parsed_body()
    .context("failed to parse 'update bundle comment' response")
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
        let _dataset = zpool.debug_dataset();
        debug_dataset_count += 1;
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
            listed_sps: true,
            activated_in_db_ok: true,
            host_ereports: SupportBundleEreportStatus::Collected {
                n_collected: 0
            },
            sp_ereports: SupportBundleEreportStatus::Collected {
                n_collected: 0
            }
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
    assert!(names.any(|n| n == "sp_task_dumps/"));
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

// Test range requests on a bundle
#[nexus_test]
async fn test_support_bundle_range_requests(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let disk_test =
        DiskTestBuilder::new(&cptestctx).with_zpool_count(1).build().await;

    // Validate our test setup: We should see a single Debug dataset
    // in our disk test.
    let mut debug_dataset_count = 0;
    for zpool in disk_test.zpools() {
        let _dataset = zpool.debug_dataset();
        debug_dataset_count += 1;
    }
    assert_eq!(debug_dataset_count, 1);

    let bundle = bundle_create(&client).await.unwrap();
    assert_eq!(bundle.state, SupportBundleState::Collecting);

    // Finish collection, activate the bundle.
    let output = activate_bundle_collection_background_task(&cptestctx).await;
    assert_eq!(output.collection_err, None);
    assert_eq!(
        output.collection_report,
        Some(SupportBundleCollectionReport {
            bundle: bundle.id,
            listed_in_service_sleds: true,
            listed_sps: true,
            activated_in_db_ok: true,
            host_ereports: SupportBundleEreportStatus::Collected {
                n_collected: 0
            },
            sp_ereports: SupportBundleEreportStatus::Collected {
                n_collected: 0
            }
        })
    );
    let bundle = bundle_get(&client, bundle.id).await.unwrap();
    assert_eq!(bundle.state, SupportBundleState::Active);

    // Download the bundle without using range requests.
    let full_contents = bundle_download(&client, bundle.id).await.unwrap();
    let len = full_contents.len();

    // HEAD the bundle length
    let head_len = bundle_download_head(&client, bundle.id).await.unwrap();
    assert_eq!(
        len, head_len,
        "Length from 'download bundle' vs 'HEAD bundle' did not match"
    );

    // Download portions of the bundle using range requests.
    let (rr1_start, rr1_end) = (0, len / 2);
    let (rr2_start, rr2_end) = (len / 2 + 1, len - 1);
    let rr_header1 = format!("bytes={rr1_start}-{rr1_end}");
    let rr_header2 = format!("bytes={rr2_start}-{rr2_end}");
    let first_half = bundle_download_range(
        &client,
        bundle.id,
        &rr_header1,
        &format!("bytes {rr1_start}-{rr1_end}/{len}"),
    )
    .await
    .unwrap();
    assert_eq!(first_half, full_contents[..first_half.len()]);

    let second_half = bundle_download_range(
        &client,
        bundle.id,
        &rr_header2,
        &format!("bytes {rr2_start}-{rr2_end}/{len}"),
    )
    .await
    .unwrap();
    assert_eq!(second_half, full_contents[first_half.len()..]);
}

// Test that support bundle listing returns bundles ordered by creation time
#[nexus_test]
async fn test_support_bundle_list_time_ordering(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    // Create a disk test with multiple zpools to allow multiple bundles
    let _disk_test =
        DiskTestBuilder::new(&cptestctx).with_zpool_count(3).build().await;

    // Create multiple bundles with delays to ensure different creation times
    let mut bundle_ids = Vec::new();

    for _ in 0..3 {
        let bundle = bundle_create(&client).await.unwrap();
        bundle_ids.push(bundle.id);

        // Small delay to ensure different creation times
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    // List all bundles
    let bundles = bundles_list(&client).await.unwrap();
    assert_eq!(bundles.len(), 3, "Should have created 3 bundles");

    // Verify bundles are ordered by creation time (ascending)
    for i in 0..bundles.len() - 1 {
        assert!(
            bundles[i].time_created <= bundles[i + 1].time_created,
            "Bundles should be ordered by creation time (ascending). Bundle at index {} has time {:?}, but bundle at index {} has time {:?}",
            i,
            bundles[i].time_created,
            i + 1,
            bundles[i + 1].time_created
        );
    }

    // Verify that all our created bundles are present
    let returned_ids: Vec<_> = bundles.iter().map(|b| b.id).collect();
    for bundle_id in &bundle_ids {
        assert!(
            returned_ids.contains(bundle_id),
            "Bundle ID {:?} should be in the returned list",
            bundle_id
        );
    }
}

// Test updating bundle comments
#[nexus_test]
async fn test_support_bundle_update_comment(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let _disk_test =
        DiskTestBuilder::new(&cptestctx).with_zpool_count(1).build().await;

    // Create a bundle
    let bundle = bundle_create(&client).await.unwrap();
    assert_eq!(bundle.user_comment, None);

    // Update the comment
    let comment = Some("Test comment".to_string());
    let updated_bundle =
        bundle_update_comment(&client, bundle.id, comment.clone())
            .await
            .unwrap();
    assert_eq!(updated_bundle.user_comment, comment);

    // Update with a different comment
    let new_comment = Some("Updated comment".to_string());
    let updated_bundle =
        bundle_update_comment(&client, bundle.id, new_comment.clone())
            .await
            .unwrap();
    assert_eq!(updated_bundle.user_comment, new_comment);

    // Clear the comment
    let updated_bundle =
        bundle_update_comment(&client, bundle.id, None).await.unwrap();
    assert_eq!(updated_bundle.user_comment, None);

    // Test maximum length validation (4096 bytes)
    let max_comment = "a".repeat(4096);
    let updated_bundle =
        bundle_update_comment(&client, bundle.id, Some(max_comment.clone()))
            .await
            .unwrap();
    assert_eq!(updated_bundle.user_comment, Some(max_comment));

    // Test exceeding maximum length (4097 bytes)
    let too_long_comment = "a".repeat(4097);
    let url = format!("{BUNDLES_URL}/{}", bundle.id);
    let update = nexus_types::external_api::params::SupportBundleUpdate {
        user_comment: Some(too_long_comment),
    };

    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::PUT, &url)
            .body(Some(&update))
            .expect_status(Some(StatusCode::BAD_REQUEST)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .context("failed to update bundle comment")
    .unwrap()
    .parsed_body::<HttpErrorResponseBody>()
    .context("failed to parse error response")
    .unwrap();

    assert!(error.message.contains("cannot exceed 4096 bytes"));

    // Clean up
    bundle_delete(&client, bundle.id).await.unwrap();
}

// Test creating bundles with comments
#[nexus_test]
async fn test_support_bundle_create_with_comment(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;

    let _disk_test =
        DiskTestBuilder::new(&cptestctx).with_zpool_count(3).build().await;

    // Create a bundle without comment
    let bundle_no_comment =
        bundle_create_with_comment(&client, None).await.unwrap();
    assert_eq!(bundle_no_comment.user_comment, None);

    // Create a bundle with comment
    let comment = Some("Test comment during creation".to_string());
    let bundle_with_comment =
        bundle_create_with_comment(&client, comment.clone()).await.unwrap();
    assert_eq!(bundle_with_comment.user_comment, comment);

    // Create a bundle with empty comment
    let empty_comment = Some("".to_string());
    let bundle_empty_comment =
        bundle_create_with_comment(&client, empty_comment.clone())
            .await
            .unwrap();
    assert_eq!(bundle_empty_comment.user_comment, empty_comment);

    // Clean up
    bundle_delete(&client, bundle_no_comment.id).await.unwrap();
    bundle_delete(&client, bundle_with_comment.id).await.unwrap();
    bundle_delete(&client, bundle_empty_comment.id).await.unwrap();
}
