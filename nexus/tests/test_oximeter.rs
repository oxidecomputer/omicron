//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//

//! Integration tests for oximeter collectors and producers.

pub mod common;

use omicron_test_utils::dev::poll::{wait_for_condition, CondCheckError};
use oximeter_db::DbWrite;
use std::net;
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
async fn test_oximeter_database_records() {
    let context = common::test_setup("test_oximeter_database_records").await;
    let db = &context.database;

    // Get a handle to the DB, for various tests
    let conn = db.connect().await.unwrap();

    // Verify that the Oximeter instance lives in the DB.
    let result = conn
        .query("SELECT * FROM omicron.public.oximeter;", &[])
        .await
        .unwrap();
    assert_eq!(
        result.len(),
        1,
        "Expected a single Oximeter instance in the database"
    );
    let actual_id = result[0].get::<&str, Uuid>("id");
    assert_eq!(
        actual_id,
        common::OXIMETER_UUID.parse().unwrap(),
        "Oximeter ID does not match the ID returned from the database"
    );

    // Verify that the producer lives in the DB.
    let result = conn
        .query("SELECT * FROM omicron.public.metric_producer;", &[])
        .await
        .unwrap();
    assert_eq!(
        result.len(),
        1,
        "Expected a single metric producer instance in the database"
    );
    let actual_id = result[0].get::<&str, Uuid>("id");
    assert_eq!(
        actual_id,
        common::PRODUCER_UUID.parse().unwrap(),
        "Producer ID does not match the ID returned from the database"
    );
    let actual_oximeter_id = result[0].get::<&str, Uuid>("oximeter_id");
    assert_eq!(
        actual_oximeter_id,
        common::OXIMETER_UUID.parse().unwrap(),
        "Producer's oximeter ID returned from the database does not match the expected ID"
    );

    context.teardown().await;
}

#[tokio::test]
async fn test_oximeter_reregistration() {
    let mut context = common::test_setup("test_oximeter_reregistration").await;
    let db = &context.database;
    let producer_id = common::PRODUCER_UUID.parse().unwrap();
    let oximeter_id = common::OXIMETER_UUID.parse().unwrap();

    // Get a handle to the DB, for various tests
    let conn = db.connect().await.unwrap();

    // Helper to get a record for a single metric producer
    let get_record = || async {
        let result = conn
            .query("SELECT * FROM omicron.public.metric_producer;", &[])
            .await
            .unwrap();
        assert_eq!(
            result.len(),
            1,
            "Expected a single metric producer instance in the database"
        );
        let actual_id = result[0].get::<&str, Uuid>("id");
        assert_eq!(
            actual_id, producer_id,
            "Producer ID does not match the ID returned from the database"
        );
        result
    };

    // Get the original time modified, for comparison later.
    let original_time_modified = {
        let result = get_record().await;
        result[0].get::<&str, chrono::DateTime<chrono::Utc>>("time_modified")
    };

    // ClickHouse client for verifying collection.
    let ch_address = net::SocketAddrV6::new(
        "::1".parse().unwrap(),
        context.clickhouse.port(),
        0,
        0,
    );
    let client =
        oximeter_db::Client::new(ch_address.into(), context.logctx.log.clone());
    client.init_db().await.expect("Failed to initialize timeseries database");

    // Helper to retrieve the timeseries from ClickHouse
    let timeseries_name = "integration_target:integration_metric";
    let retrieve_timeseries = || async {
        match client
            .filter_timeseries_with(timeseries_name, &[], None, None)
            .await
        {
            Ok(maybe_series) => {
                if maybe_series.is_empty() {
                    Err(CondCheckError::NotYet)
                } else {
                    Ok(maybe_series)
                }
            }
            Err(oximeter_db::Error::QueryError(_)) => {
                Err(CondCheckError::NotYet)
            }
            Err(e) => Err(CondCheckError::from(e)),
        }
    };

    // Timeouts for checks
    const POLL_INTERVAL: Duration = Duration::from_millis(100);
    const POLL_DURATION: Duration = Duration::from_secs(5);

    // We must have at exactly one timeseries, with at least one sample.
    let timeseries =
        wait_for_condition(retrieve_timeseries, &POLL_INTERVAL, &POLL_DURATION)
            .await
            .expect("Failed to retrieve timeseries");
    assert_eq!(timeseries.len(), 1);
    assert_eq!(timeseries[0].timeseries_name, timeseries_name);
    assert!(!timeseries[0].measurements.is_empty());
    let timeseries = timeseries.into_iter().next().unwrap();

    // Drop the producer.
    //
    // At this point, Oximeter will still be collecting, so we poll for data and verify that
    // there's no new data in ClickHouse.
    drop(context.producer);
    let new_timeseries =
        retrieve_timeseries().await.expect("Failed to retrieve timeseries");
    assert_eq!(new_timeseries.len(), 1);
    let new_timeseries = new_timeseries.into_iter().next().unwrap();
    assert_eq!(timeseries, new_timeseries);

    // Restart the producer, and verify that we have _more_ data than before
    // Set up a test metric producer server
    context.producer = common::start_producer_server(
        context.server.http_server_internal.local_addr(),
        common::PRODUCER_UUID.parse().unwrap(),
    )
    .await
    .expect("Failed to restart metric producer server");

    // Run the verification in a loop using wait_for_condition, waiting until there is more data,
    // or failing the test otherwise.
    let timeseries = new_timeseries;
    let new_timeseries = wait_for_condition(
        || async {
            match retrieve_timeseries().await {
                Ok(new_timeseries) => {
                    if new_timeseries[0].measurements.len()
                        > timeseries.measurements.len()
                    {
                        Ok(new_timeseries)
                    } else {
                        Err(CondCheckError::NotYet)
                    }
                }
                e => e,
            }
        },
        &POLL_INTERVAL,
        &POLL_DURATION,
    )
    .await
    .expect("There should be new data after restarting the producer");
    assert_eq!(new_timeseries.len(), 1);
    let new_timeseries = new_timeseries.into_iter().next().unwrap();
    assert_eq!(timeseries.timeseries_name, new_timeseries.timeseries_name);
    assert_eq!(timeseries.target, new_timeseries.target);
    assert_eq!(timeseries.metric, new_timeseries.metric);
    assert!(timeseries.measurements.len() < new_timeseries.measurements.len());
    assert_eq!(
        timeseries.measurements,
        new_timeseries.measurements[..timeseries.measurements.len()]
    );
    let timeseries = new_timeseries;

    // Also verify that the producer's port is still correct, because we've updated it.
    // Note that it's _probably_ not the case that the port is the same as the original, but it is
    // possible. We can verify that the modification time has been changed.
    let (new_port, new_time_modified) = {
        let result = get_record().await;
        (
            result[0].get::<&str, i32>("port") as u16,
            result[0]
                .get::<&str, chrono::DateTime<chrono::Utc>>("time_modified"),
        )
    };
    assert_eq!(new_port, context.producer.address().port());
    assert!(
        new_time_modified > original_time_modified,
        "Expected the modification time of the producer record to have changed"
    );

    // Verify that the time modified has changed as well.

    // Drop oximeter and verify that we've still got the same data, because there's no collector
    // running.
    drop(context.oximeter);
    let new_timeseries =
        retrieve_timeseries().await.expect("Failed to retrieve timeseries");
    assert_eq!(new_timeseries.len(), 1);
    let new_timeseries = new_timeseries.into_iter().next().unwrap();
    assert_eq!(timeseries, new_timeseries);
    let timeseries = new_timeseries;

    // Restart oximeter again, and verify that we have even more new data.
    context.oximeter = common::start_oximeter(
        context.server.http_server_internal.local_addr(),
        context.clickhouse.port(),
        oximeter_id,
    )
    .await
    .unwrap();

    // Poll until there is more data.
    let new_timeseries = wait_for_condition(
        || async {
            match retrieve_timeseries().await {
                Ok(new_timeseries) => {
                    if new_timeseries[0].measurements.len()
                        > timeseries.measurements.len()
                    {
                        Ok(new_timeseries)
                    } else {
                        Err(CondCheckError::NotYet)
                    }
                }
                e => e,
            }
        },
        &POLL_INTERVAL,
        &POLL_DURATION,
    )
    .await
    .expect("There should be new data after restarting the oximeter collector");
    assert_eq!(new_timeseries.len(), 1);
    let new_timeseries = new_timeseries.into_iter().next().unwrap();
    assert_eq!(timeseries.timeseries_name, new_timeseries.timeseries_name);
    assert_eq!(timeseries.target, new_timeseries.target);
    assert_eq!(timeseries.metric, new_timeseries.metric);
    assert!(timeseries.measurements.len() < new_timeseries.measurements.len());
    assert_eq!(
        timeseries.measurements,
        new_timeseries.measurements[..timeseries.measurements.len()]
    );
}
