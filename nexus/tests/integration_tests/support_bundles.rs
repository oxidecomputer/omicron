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
use nexus_db_model::SupportBundleState as DbSupportBundleState;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::SupportBundleCreateParams;
use nexus_lockstep_client::types::LastResult;
use nexus_test_utils::http_testing::AuthnMode;
use nexus_test_utils::http_testing::NexusRequest;
use nexus_test_utils::http_testing::RequestBuilder;
use nexus_test_utils_macros::nexus_test;
use nexus_types::external_api::support_bundle::SupportBundleCreate;
use nexus_types::external_api::support_bundle::SupportBundleData;
use nexus_types::external_api::support_bundle::SupportBundleDataSelection;
use nexus_types::external_api::support_bundle::SupportBundleEreports;
use nexus_types::external_api::support_bundle::SupportBundleHostInfo;
use nexus_types::external_api::support_bundle::SupportBundleInfo;
use nexus_types::external_api::support_bundle::SupportBundleSledSelection;
use nexus_types::external_api::support_bundle::SupportBundleState;
use nexus_types::external_api::support_bundle::SupportBundleView;
use nexus_types::internal_api::background::SupportBundleActivationReport;
use nexus_types::internal_api::background::SupportBundleCleanupReport;
use nexus_types::internal_api::background::SupportBundleCollectionStep;
use nexus_types::internal_api::background::SupportBundleCollectionStepStatus;
use nexus_types::internal_api::background::SupportBundleEreportStatus;
use nexus_types::support_bundle::BundleDataSelection;
use nexus_types::support_bundle::BundleTimeRange;
use nexus_types::support_bundle::BundleZoneType;
use omicron_common::api::external::LookupType;
use omicron_sled_agent::sim::SimLogEntry;
use omicron_uuid_kinds::SledUuid;
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

const BUNDLES_URL: &str = "/v1/system/support-bundles";

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
    Ok(bundle_view(client, id).await?.bundle)
}

async fn bundle_view(
    client: &ClientTestContext,
    id: SupportBundleUuid,
) -> Result<SupportBundleView> {
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
    let create_params =
        SupportBundleCreate { user_comment, data_selection: None };

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
    let create_params =
        SupportBundleCreate { user_comment: None, data_selection: None };
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

async fn bundle_create_with_selection(
    client: &ClientTestContext,
    data_selection: SupportBundleDataSelection,
) -> Result<SupportBundleInfo> {
    let create_params = SupportBundleCreate {
        user_comment: None,
        data_selection: Some(data_selection),
    };

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
}

/// Requests a bundle expected to be rejected, returning the error message.
async fn bundle_create_with_selection_expect_fail(
    client: &ClientTestContext,
    data_selection: SupportBundleDataSelection,
    expected_status: StatusCode,
) -> Result<String> {
    let create_params = SupportBundleCreate {
        user_comment: None,
        data_selection: Some(data_selection),
    };

    let error = NexusRequest::new(
        RequestBuilder::new(client, Method::POST, BUNDLES_URL)
            .body(Some(&create_params))
            .expect_status(Some(expected_status)),
    )
    .authn_as(AuthnMode::PrivilegedUser)
    .execute()
    .await
    .context("should have failed to create bundle")?
    .parsed_body::<HttpErrorResponseBody>()
    .context("failed to parse error from bundle creation")?;

    Ok(error.message)
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
    use nexus_types::external_api::support_bundle::SupportBundleUpdate;

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
    collection_report: Option<SupportBundleActivationReport>,
}

async fn activate_bundle_collection_background_task(
    cptestctx: &ControlPlaneTestContext,
) -> TaskOutput {
    use nexus_test_utils::background::activate_background_task;

    let task = activate_background_task(
        &cptestctx.lockstep_client,
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

fn assert_ereport_details_eq(
    collection: &nexus_types::internal_api::background::SupportBundleCollectionReport,
    expected: SupportBundleEreportStatus,
) {
    let step = collection
        .steps
        .iter()
        .find(|s| s.name == SupportBundleCollectionStep::STEP_EREPORTS)
        .expect("should have ereports step");
    assert_eq!(step.status, SupportBundleCollectionStepStatus::Ok);
    let details: SupportBundleEreportStatus = serde_json::from_value(
        step.details.clone().expect("ereports step should have details"),
    )
    .expect("ereports step details should deserialize");
    assert_eq!(details, expected);
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

    let report = output.collection_report.as_ref().expect("Missing report");
    assert_eq!(report.collection.bundle, bundle.id);
    assert!(report.activated_in_db_ok);
    // This assertion expects 0 ereports in the database. This depends on
    // the sp_ereport_ingester background task being disabled in the test
    // config (config.test.toml). If that task runs before bundle collection,
    // it will ingest ereports from the simulated SPs into the database,
    // causing this assertion to fail nondeterministically.
    assert_ereport_details_eq(
        &report.collection,
        SupportBundleEreportStatus {
            n_collected: 0,
            n_found: 0,
            errors: Vec::new(),
        },
    );

    // Verify that steps were recorded with reasonable timing data
    assert!(
        !report.collection.steps.is_empty(),
        "Should have recorded some steps"
    );
    for step in &report.collection.steps {
        assert!(
            step.end >= step.start,
            "Step '{}' end time should be >= start time",
            step.name
        );
    }

    // Verify that we successfully spawned steps to query sleds and SPs
    let step_names: Vec<_> =
        report.collection.steps.iter().map(|s| s.name.as_str()).collect();
    assert!(
        step_names.contains(&SupportBundleCollectionStep::STEP_SPAWN_SLEDS),
        "Should have attempted to list in-service sleds"
    );
    assert!(
        step_names.contains(&SupportBundleCollectionStep::STEP_SPAWN_SP_DUMPS),
        "Should have attempted to list service processors"
    );

    let bundle = bundle_get(&client, bundle.id).await.unwrap();
    assert_eq!(bundle.state, SupportBundleState::Active);

    // Now we should be able to download the bundle
    let contents = bundle_download(&client, bundle.id).await.unwrap();
    let archive = ZipArchive::new(Cursor::new(&contents)).unwrap();
    let mut names = archive.file_names();
    assert_eq!(names.next(), Some("bundle_id.txt"));
    assert_eq!(names.next(), Some("meta/"));
    assert_eq!(names.next(), Some("meta/reason_for_creation.txt"));
    assert_eq!(names.next(), Some("meta/report.json"));
    assert_eq!(names.next(), Some("meta/trace.json"));
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

// Test that the bundle-wide time range bounds which zone logs are collected
// from a sled.
#[nexus_test]
async fn test_support_bundle_zone_log_time_range(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let _disk_test =
        DiskTestBuilder::new(&cptestctx).with_zpool_count(1).build().await;

    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.clone(), datastore.clone());

    // Inject synthetic zone logs into the first simulated sled-agent at
    // three ages: 30 minutes, 6 hours, and 30 days.
    const ZONE: &str = "oxz_fake_test_zone";
    let now = chrono::Utc::now();
    let sled_agent = cptestctx.sled_agents[0].sled_agent();
    for (filename, age) in [
        ("fake-svc.log.30-minutes-old", chrono::Duration::minutes(30)),
        ("fake-svc.log.6-hours-old", chrono::Duration::hours(6)),
        ("fake-svc.log.30-days-old", chrono::Duration::days(30)),
    ] {
        sled_agent.insert_support_log(
            ZONE,
            SimLogEntry {
                filename: filename.to_string(),
                contents: b"totally fake log data".to_vec(),
                mtime: now - age,
            },
        );
    }

    // Creates a bundle with the given window, collects it, and returns the
    // names of the collected zone-log files.
    async fn collect_zone_logs_with_range(
        cptestctx: &ControlPlaneTestContext,
        client: &ClientTestContext,
        opctx: &OpContext,
        range: BundleTimeRange,
    ) -> Vec<String> {
        let nexus = &cptestctx.server.server_context().nexus;
        let bundle = nexus
            .datastore()
            .support_bundle_create(
                opctx,
                SupportBundleCreateParams {
                    reason: "Testing zone-log time-range filtering",
                    nexus_id: nexus.id(),
                    user_comment: None,
                    data_selection: BundleDataSelection::new()
                        .with_all_sleds()
                        .with_time_range(range),
                },
            )
            .await
            .expect("Couldn't allocate a support bundle");

        let output =
            activate_bundle_collection_background_task(&cptestctx).await;
        assert_eq!(output.collection_err, None);
        let report = output.collection_report.as_ref().expect("Missing report");
        assert_eq!(report.collection.bundle, bundle.id.into());
        assert!(report.activated_in_db_ok);

        let contents = bundle_download(client, bundle.id.into()).await.unwrap();
        let archive = ZipArchive::new(Cursor::new(&contents)).unwrap();
        let log_prefix = format!("logs/{ZONE}/");
        let logs = archive
            .file_names()
            .filter(|name| name.contains(&log_prefix) && !name.ends_with('/'))
            .map(String::from)
            .collect();

        // Delete the bundle (and run the cleanup pass) so the next
        // collection has a free debug dataset to land on.
        bundle_delete(client, bundle.id.into()).await.unwrap();
        let output =
            activate_bundle_collection_background_task(&cptestctx).await;
        assert_eq!(output.cleanup_err, None);

        logs
    }

    // A 24-hour window includes the 30-minute and 6-hour logs, but not the
    // 30-day log.
    let logs = collect_zone_logs_with_range(
        &cptestctx,
        client,
        &opctx,
        BundleTimeRange::new(Some(now - chrono::Duration::hours(24)), None)
            .unwrap(),
    )
    .await;
    assert_eq!(logs.len(), 2, "expected 2 in-window logs, got: {logs:?}");
    assert!(logs.iter().any(|l| l.ends_with("fake-svc.log.30-minutes-old")));
    assert!(logs.iter().any(|l| l.ends_with("fake-svc.log.6-hours-old")));

    // A window that ends a day ago includes only the 30-day log, exercising
    // the end bound.
    let logs = collect_zone_logs_with_range(
        &cptestctx,
        client,
        &opctx,
        BundleTimeRange::new(
            Some(now - chrono::Duration::days(60)),
            Some(now - chrono::Duration::days(1)),
        )
        .unwrap(),
    )
    .await;
    assert_eq!(logs.len(), 1, "expected 1 in-window log, got: {logs:?}");
    assert!(logs.iter().any(|l| l.ends_with("fake-svc.log.30-days-old")));

    // A window with no bounds does not collect unbounded history: bundle
    // creation fills in the default lookback as the start bound, so the
    // 30-day log stays excluded.
    let logs = collect_zone_logs_with_range(
        &cptestctx,
        client,
        &opctx,
        BundleTimeRange::new(None, None).unwrap(),
    )
    .await;
    assert_eq!(logs.len(), 2, "expected 2 in-lookback logs, got: {logs:?}");
    assert!(logs.iter().any(|l| l.ends_with("fake-svc.log.30-minutes-old")));
    assert!(logs.iter().any(|l| l.ends_with("fake-svc.log.6-hours-old")));
}

// Test that the zone-type selection bounds which zones' logs are collected
// from a sled.
#[nexus_test]
async fn test_support_bundle_zone_type_filtering(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let _disk_test =
        DiskTestBuilder::new(&cptestctx).with_zpool_count(1).build().await;

    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.clone(), datastore.clone());

    // Inject a recent synthetic log into the first simulated sled-agent for
    // one zone of each name shape: Omicron zones, the global zone, an
    // instance zone, and a name that classifies to no known zone type.
    const ZONES: [&str; 5] = [
        "oxz_nexus_00000000-0000-0000-0000-000000000001",
        "oxz_crucible_00000000-0000-0000-0000-000000000002",
        "oxz_propolis-server_00000000-0000-0000-0000-000000000003",
        "global",
        "oxz_fake_test_zone",
    ];
    let now = chrono::Utc::now();
    let sled_agent = cptestctx.sled_agents[0].sled_agent();
    for zone in ZONES {
        sled_agent.insert_support_log(
            zone,
            SimLogEntry {
                filename: "fake-svc.log.0".to_string(),
                contents: b"totally fake log data".to_vec(),
                mtime: now - chrono::Duration::minutes(30),
            },
        );
    }

    // Creates a bundle with the given selection, collects it, and returns
    // the zones with any entry under logs/ in the collected archive.
    async fn collect_zones_with_selection(
        cptestctx: &ControlPlaneTestContext,
        client: &ClientTestContext,
        opctx: &OpContext,
        data_selection: BundleDataSelection,
    ) -> Vec<&'static str> {
        let nexus = &cptestctx.server.server_context().nexus;
        let bundle = nexus
            .datastore()
            .support_bundle_create(
                opctx,
                SupportBundleCreateParams {
                    reason: "Testing zone-type filtering",
                    nexus_id: nexus.id(),
                    user_comment: None,
                    data_selection,
                },
            )
            .await
            .expect("Couldn't allocate a support bundle");

        let output =
            activate_bundle_collection_background_task(&cptestctx).await;
        assert_eq!(output.collection_err, None);
        let report = output.collection_report.as_ref().expect("Missing report");
        assert_eq!(report.collection.bundle, bundle.id.into());
        assert!(report.activated_in_db_ok);

        let contents = bundle_download(client, bundle.id.into()).await.unwrap();
        let archive = ZipArchive::new(Cursor::new(&contents)).unwrap();
        let names: Vec<String> =
            archive.file_names().map(String::from).collect();
        let zones = ZONES
            .into_iter()
            .filter(|zone| {
                let needle = format!("logs/{zone}");
                names.iter().any(|name| name.contains(&needle))
            })
            .collect();

        // Delete the bundle (and run the cleanup pass) so the next
        // collection has a free debug dataset to land on.
        bundle_delete(client, bundle.id.into()).await.unwrap();
        let output =
            activate_bundle_collection_background_task(&cptestctx).await;
        assert_eq!(output.cleanup_err, None);

        zones
    }

    // With no zone-type selection, every zone is collected, including the
    // one that classifies to no known type.
    let zones = collect_zones_with_selection(
        &cptestctx,
        client,
        &opctx,
        BundleDataSelection::new().with_all_sleds(),
    )
    .await;
    assert_eq!(zones, ZONES, "expected every zone to be collected");

    // Selecting specific types collects exactly the matching zones. The
    // unclassifiable zone does not match any type.
    let zones = collect_zones_with_selection(
        &cptestctx,
        client,
        &opctx,
        BundleDataSelection::new().with_all_sleds().with_zone_types([
            BundleZoneType::Nexus,
            BundleZoneType::Propolis,
            BundleZoneType::Global,
        ]),
    )
    .await;
    assert_eq!(
        zones,
        [
            "oxz_nexus_00000000-0000-0000-0000-000000000001",
            "oxz_propolis-server_00000000-0000-0000-0000-000000000003",
            "global",
        ],
        "expected only the zones of the selected types"
    );

    // Selecting no types collects logs from no zones at all.
    let zones = collect_zones_with_selection(
        &cptestctx,
        client,
        &opctx,
        BundleDataSelection::new().with_all_sleds().with_zone_types([]),
    )
    .await;
    assert_eq!(zones, [] as [&str; 0], "expected no zone logs at all");
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
    let report = output.collection_report.as_ref().expect("Missing report");
    assert_eq!(report.collection.bundle, bundle.id);
    assert!(report.activated_in_db_ok);
    // See comment above — this depends on sp_ereport_ingester being disabled
    // in config.test.toml.
    assert_ereport_details_eq(
        &report.collection,
        SupportBundleEreportStatus {
            n_collected: 0,
            n_found: 0,
            errors: Vec::new(),
        },
    );

    // Verify that steps were recorded with reasonable timing data
    assert!(
        !report.collection.steps.is_empty(),
        "Should have recorded some steps"
    );
    for step in &report.collection.steps {
        assert!(
            step.end >= step.start,
            "Step '{}' end time should be >= start time",
            step.name
        );
    }

    // Verify that we successfully spawned steps to query sleds and SPs
    let step_names: Vec<_> =
        report.collection.steps.iter().map(|s| s.name.as_str()).collect();
    assert!(
        step_names.contains(&SupportBundleCollectionStep::STEP_SPAWN_SLEDS),
        "Should have attempted to list in-service sleds"
    );
    assert!(
        step_names.contains(&SupportBundleCollectionStep::STEP_SPAWN_SP_DUMPS),
        "Should have attempted to list service processors"
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
    let update =
        nexus_types::external_api::support_bundle::SupportBundleUpdate {
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

fn authz_support_bundle_from_id(id: SupportBundleUuid) -> authz::SupportBundle {
    authz::SupportBundle::new(authz::FLEET, id, LookupType::by_id(id))
}

// Test that bundles in "Failed" state can be deleted.
//
// This is a regression test for https://github.com/oxidecomputer/omicron/issues/9558
// where bundles that had transitioned to the Failed state could not be deleted
// via the API because the delete operation incorrectly tried to transition
// Failed -> Destroying, which is not a valid state transition.
#[nexus_test]
async fn test_support_bundle_delete_failed_bundle(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let nexus = &cptestctx.server.server_context().nexus;
    let datastore = nexus.datastore();
    let opctx =
        OpContext::for_tests(cptestctx.logctx.log.clone(), datastore.clone());

    let _disk_test =
        DiskTestBuilder::new(&cptestctx).with_zpool_count(1).build().await;

    // Create a bundle via the external API
    let bundle = bundle_create(&client).await.unwrap();
    assert_eq!(bundle.state, SupportBundleState::Collecting);

    // Mark the bundle as "Failing" via the datastore.
    // This simulates what happens when a Nexus managing the bundle is expunged
    // or the underlying storage is removed.
    let authz_bundle = authz_support_bundle_from_id(bundle.id);
    datastore
        .support_bundle_update(
            &opctx,
            &authz_bundle,
            DbSupportBundleState::Failing,
        )
        .await
        .expect("Should be able to mark bundle as failing");

    // Run the background task to clean up storage and transition Failing -> Failed
    let output = activate_bundle_collection_background_task(&cptestctx).await;
    assert_eq!(output.cleanup_err, None);
    assert_eq!(
        output.cleanup_report,
        Some(SupportBundleCleanupReport {
            // The bundle hadn't been collected yet, so sled agent returns not found
            sled_bundles_deleted_not_found: 1,
            // The bundle transitioned from Failing to Failed
            db_failing_bundles_updated: 1,
            ..Default::default()
        })
    );

    // Verify the bundle is now in Failed state via the external API
    let bundle = bundle_get(&client, bundle.id).await.unwrap();
    assert_eq!(
        bundle.state,
        SupportBundleState::Failed,
        "Bundle should be in Failed state after cleanup"
    );

    // This is the key assertion: we should be able to delete a Failed bundle.
    // Before the fix for #9558, this would fail with:
    // "Cannot update support bundle state from Failed to Destroying"
    bundle_delete(&client, bundle.id).await.unwrap();

    // For Failed bundles, the storage has already been cleaned up by the
    // background task, so deletion removes the database record immediately
    // (no need for a subsequent background task run).
    bundle_get_expect_fail(
        &client,
        bundle.id,
        StatusCode::NOT_FOUND,
        &format!("not found: support-bundle with id \"{}\"", bundle.id),
    )
    .await
    .unwrap();

    // Verify bundle is not in the list
    let bundles = bundles_list(&client).await.unwrap();
    assert!(
        !bundles.iter().any(|b| b.id == bundle.id),
        "Deleted bundle should not appear in bundle list"
    );
}

/// Returns the categories a viewed selection reports, as a set of names, so
/// tests can compare against what they asked for.
fn viewed_categories(
    selection: &SupportBundleDataSelection,
) -> Vec<&'static str> {
    let SupportBundleData::Explicit {
        reconfigurator,
        sled_cubby_info,
        sp_dumps,
        host_info,
        ereports,
    } = &selection.data
    else {
        panic!("a stored selection is always reported explicitly");
    };

    let mut categories = Vec::new();
    if *reconfigurator {
        categories.push("reconfigurator");
    }
    if *sled_cubby_info {
        categories.push("sled_cubby_info");
    }
    if *sp_dumps {
        categories.push("sp_dumps");
    }
    if host_info.is_some() {
        categories.push("host_info");
    }
    if ereports.is_some() {
        categories.push("ereports");
    }
    categories
}

// Test that a bundle created without a data selection collects everything,
// and that the view reports the default lookback Nexus stamped for it.
#[nexus_test]
async fn test_support_bundle_default_data_selection(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let _disk_test =
        DiskTestBuilder::new(&cptestctx).with_zpool_count(2).build().await;

    // CockroachDB stores timestamps at microsecond precision, so bracket
    // creation with the same truncated clock Nexus stamps with.
    let before = omicron_common::now_db_precision();
    let bundle = bundle_create(&client).await.unwrap();
    let after = omicron_common::now_db_precision();

    let selection =
        bundle_view(&client, bundle.id).await.unwrap().data_selection;
    assert_eq!(
        viewed_categories(&selection),
        [
            "reconfigurator",
            "sled_cubby_info",
            "sp_dumps",
            "host_info",
            "ereports"
        ],
    );
    assert_eq!(
        selection.data,
        SupportBundleData::Explicit {
            reconfigurator: true,
            sled_cubby_info: true,
            sp_dumps: true,
            host_info: Some(SupportBundleHostInfo {
                sleds: SupportBundleSledSelection::All
            }),
            ereports: Some(SupportBundleEreports {
                only_serials: vec![],
                only_classes: vec![],
            }),
        },
    );

    // Nexus stamps a start bound seven days back, so an omitted window does
    // not collect unbounded log history.
    let lookback = chrono::Duration::days(7);
    let start = selection.start_time.expect("creation stamps a start bound");
    assert!(
        start >= before - lookback && start <= after - lookback,
        "stamped start {start} is not seven days before creation \
         ({before} to {after})",
    );
    assert_eq!(selection.end_time, None);

    // Asking for everything explicitly is the same thing.
    let explicit = bundle_create_with_selection(
        &client,
        SupportBundleDataSelection {
            data: SupportBundleData::All,
            start_time: None,
            end_time: None,
        },
    )
    .await
    .unwrap();
    let explicit =
        bundle_view(&client, explicit.id).await.unwrap().data_selection;
    assert_eq!(explicit.data, selection.data);
}

// Test that an explicit data selection round-trips through creation and
// back out of the view endpoint.
#[nexus_test]
async fn test_support_bundle_explicit_data_selection(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let _disk_test =
        DiskTestBuilder::new(&cptestctx).with_zpool_count(3).build().await;

    let sled_id = cptestctx.all_sled_agents().next().unwrap().sled_agent.id;

    // A subset of categories, each with its own settings.
    let requested = SupportBundleData::Explicit {
        reconfigurator: true,
        sled_cubby_info: false,
        sp_dumps: false,
        host_info: Some(SupportBundleHostInfo {
            sleds: SupportBundleSledSelection::Specific {
                sleds: vec![sled_id],
            },
        }),
        ereports: Some(SupportBundleEreports {
            only_serials: vec!["BRM-FAKE-0".to_string()],
            only_classes: vec!["fake.class".to_string()],
        }),
    };
    // Microsecond precision: CockroachDB truncates anything finer, so a
    // timestamp with nanoseconds would not come back as it was sent.
    let now = omicron_common::now_db_precision();
    let start = now - chrono::Duration::days(2);
    let end = now - chrono::Duration::hours(1);

    let bundle = bundle_create_with_selection(
        &client,
        SupportBundleDataSelection {
            data: requested.clone(),
            start_time: Some(start),
            end_time: Some(end),
        },
    )
    .await
    .unwrap();

    let selection =
        bundle_view(&client, bundle.id).await.unwrap().data_selection;
    assert_eq!(
        viewed_categories(&selection),
        ["reconfigurator", "host_info", "ereports"]
    );
    assert_eq!(selection.data, requested);
    assert_eq!(selection.start_time, Some(start));
    assert_eq!(selection.end_time, Some(end));

    // An explicit selection naming nothing collects nothing. The bundle is
    // still created; that is the caller's business.
    let empty = SupportBundleData::Explicit {
        reconfigurator: false,
        sled_cubby_info: false,
        sp_dumps: false,
        host_info: None,
        ereports: None,
    };
    let bundle = bundle_create_with_selection(
        &client,
        SupportBundleDataSelection {
            data: empty.clone(),
            start_time: None,
            end_time: None,
        },
    )
    .await
    .unwrap();

    let selection =
        bundle_view(&client, bundle.id).await.unwrap().data_selection;
    assert_eq!(viewed_categories(&selection), Vec::<&str>::new());
    assert_eq!(selection.data, empty);
}

// Test the data selections that creation rejects.
#[nexus_test]
async fn test_support_bundle_data_selection_bad_request(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let _disk_test =
        DiskTestBuilder::new(&cptestctx).with_zpool_count(1).build().await;

    // A window whose start is after its end.
    let now = omicron_common::now_db_precision();
    let message = bundle_create_with_selection_expect_fail(
        &client,
        SupportBundleDataSelection {
            data: SupportBundleData::All,
            start_time: Some(now),
            end_time: Some(now - chrono::Duration::hours(1)),
        },
        StatusCode::BAD_REQUEST,
    )
    .await
    .unwrap();
    assert!(
        message.contains("must not be later than"),
        "unexpected message: {message}"
    );

    // A sled that does not exist. Without this check, a mistyped UUID would
    // produce a bundle that quietly collects nothing from that sled.
    let missing_sled = SledUuid::new_v4();
    let message = bundle_create_with_selection_expect_fail(
        &client,
        SupportBundleDataSelection {
            data: SupportBundleData::Explicit {
                reconfigurator: false,
                sled_cubby_info: false,
                sp_dumps: false,
                host_info: Some(SupportBundleHostInfo {
                    sleds: SupportBundleSledSelection::Specific {
                        sleds: vec![missing_sled],
                    },
                }),
                ereports: None,
            },
            start_time: None,
            end_time: None,
        },
        StatusCode::BAD_REQUEST,
    )
    .await
    .unwrap();
    assert!(
        message.contains(&format!("sled {missing_sled} does not exist")),
        "unexpected message: {message}"
    );
}

// Test that viewing a bundle with no time range row succeeds, reporting no
// bounds. Bundles collected before time ranges were recorded have no such
// row: the schema migration only creates rows for bundles still awaiting
// collection.
#[nexus_test]
async fn test_support_bundle_view_without_time_range_row(
    cptestctx: &ControlPlaneTestContext,
) {
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;
    use omicron_uuid_kinds::GenericUuid;

    let client = &cptestctx.external_client;
    let _disk_test =
        DiskTestBuilder::new(&cptestctx).with_zpool_count(1).build().await;

    let bundle = bundle_create(&client).await.unwrap();

    // Simulate such a bundle by deleting the row creation stamped.
    let datastore = cptestctx.server.server_context().nexus.datastore();
    let conn = datastore.pool_connection_for_tests().await.unwrap();
    {
        use nexus_db_schema::schema::support_bundle_data_selection_time_range::dsl;
        diesel::delete(
            dsl::support_bundle_data_selection_time_range
                .filter(dsl::bundle_id.eq(bundle.id.into_untyped_uuid())),
        )
        .execute_async(&*conn)
        .await
        .expect("Should be able to delete time range row");
    }

    let selection =
        bundle_view(&client, bundle.id).await.unwrap().data_selection;
    assert_eq!(selection.start_time, None);
    assert_eq!(selection.end_time, None);
    assert_eq!(
        viewed_categories(&selection),
        [
            "reconfigurator",
            "sled_cubby_info",
            "sp_dumps",
            "host_info",
            "ereports"
        ],
    );
}

// Test that an unprivileged caller is turned away by the authorization
// check, whatever the selection looks like. Validating the selection looks
// sleds up, so an unauthorized request must be rejected before that runs,
// rather than surfacing a lookup failure as a 400.
#[nexus_test]
async fn test_support_bundle_create_unauthorized_with_selection(
    cptestctx: &ControlPlaneTestContext,
) {
    let client = &cptestctx.external_client;
    let _disk_test =
        DiskTestBuilder::new(&cptestctx).with_zpool_count(1).build().await;
    let sled_id = cptestctx.all_sled_agents().next().unwrap().sled_agent.id;

    for (label, sleds) in [
        ("no selection", None),
        ("an existing sled", Some(vec![sled_id])),
        ("a sled that does not exist", Some(vec![SledUuid::new_v4()])),
    ] {
        let create_params = SupportBundleCreate {
            user_comment: None,
            data_selection: sleds.map(|sleds| SupportBundleDataSelection {
                data: SupportBundleData::Explicit {
                    reconfigurator: false,
                    sled_cubby_info: false,
                    sp_dumps: false,
                    host_info: Some(SupportBundleHostInfo {
                        sleds: SupportBundleSledSelection::Specific { sleds },
                    }),
                    ereports: None,
                },
                start_time: None,
                end_time: None,
            }),
        };

        NexusRequest::new(
            RequestBuilder::new(client, Method::POST, BUNDLES_URL)
                .body(Some(&create_params))
                .expect_status(Some(StatusCode::FORBIDDEN)),
        )
        .authn_as(AuthnMode::UnprivilegedUser)
        .execute()
        .await
        .unwrap_or_else(|e| {
            panic!("creating a bundle naming {label} should be forbidden: {e}")
        });
    }
}
