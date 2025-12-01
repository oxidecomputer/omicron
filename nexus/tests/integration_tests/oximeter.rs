// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration tests for oximeter collectors and producers.

use nexus_test_interface::NexusServer;
use nexus_test_utils::wait_for_producer;
use nexus_test_utils_macros::nexus_test;
use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
use oximeter_db::DbWrite;
use std::time::Duration;
use uuid::Uuid;

type ControlPlaneTestContext =
    nexus_test_utils::ControlPlaneTestContext<omicron_nexus::Server>;

#[nexus_test]
async fn test_oximeter_database_records(context: &ControlPlaneTestContext) {
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
        nexus_test_utils::OXIMETER_UUID.parse::<Uuid>().unwrap(),
        "Oximeter ID does not match the ID returned from the database"
    );

    // Kind of silly, but let's wait until the producer is actually registered
    // with Oximeter.
    let producer_id: Uuid = nexus_test_utils::PRODUCER_UUID.parse().unwrap();
    wait_for_producer(&context.oximeter, producer_id).await;

    // Verify that the producer lives in the DB.
    let results = conn
        .query("SELECT * FROM omicron.public.metric_producer;", &[])
        .await
        .unwrap();
    assert!(
        !result.is_empty(),
        "Expected at least 1 metric producer instance in the database"
    );
    let actual_oximeter_id = results
        .iter()
        .find_map(|row| {
            let id = row.get::<&str, Uuid>("id");
            if id == producer_id {
                Some(row.get::<&str, Uuid>("oximeter_id"))
            } else {
                None
            }
        })
        .expect("The database doesn't contain a record of our producer");
    assert_eq!(
        actual_oximeter_id,
        nexus_test_utils::OXIMETER_UUID.parse::<Uuid>().unwrap(),
        "Producer's oximeter ID returned from the database does not match the expected ID"
    );
}

#[tokio::test]
async fn test_oximeter_reregistration() {
    let mut context = nexus_test_utils::test_setup::<omicron_nexus::Server>(
        "test_oximeter_reregistration",
        0,
    )
    .await;
    let db = &context.database;
    let producer_id: Uuid = nexus_test_utils::PRODUCER_UUID.parse().unwrap();
    let oximeter_id: Uuid = nexus_test_utils::OXIMETER_UUID.parse().unwrap();

    // Get a handle to the DB, for various tests
    let conn = db.connect().await.unwrap();

    // Helper to get the record for our test metric producer
    let get_record = || async {
        let result = conn
            .query("SELECT * FROM omicron.public.metric_producer;", &[])
            .await
            .unwrap();

        // There may be multiple producers in the DB, since Nexus and the
        // simulated sled agent register their own. We just care about the
        // actual integration test producer here.
        result
            .into_iter()
            .find(|row| row.get::<&str, Uuid>("id") == producer_id)
            .ok_or_else(|| CondCheckError::<()>::NotYet)
    };

    // Get the original time modified, for comparison later.
    //
    // Note that the record may not show up right away, so we'll wait for it
    // here.
    const PRODUCER_POLL_INTERVAL: Duration = Duration::from_secs(1);
    const PRODUCER_POLL_DURATION: Duration = Duration::from_secs(60);
    let row = wait_for_condition(
        get_record,
        &PRODUCER_POLL_INTERVAL,
        &PRODUCER_POLL_DURATION,
    )
    .await
    .expect("Integration test producer is not in the database");
    let original_time_modified =
        row.get::<&str, chrono::DateTime<chrono::Utc>>("time_modified");

    // ClickHouse client for verifying collection.
    let native_address = context.clickhouse.native_address().into();
    let client = oximeter_db::Client::new(native_address, &context.logctx.log);
    client
        .init_single_node_db()
        .await
        .expect("Failed to initialize timeseries database");

    // Helper to retrieve the timeseries from ClickHouse
    let timeseries_name = "integration_target:integration_metric";
    let retrieve_timeseries = || async {
        match client
            .select_timeseries_with(
                timeseries_name,
                &[],
                None,
                None,
                None,
                None,
            )
            .await
        {
            Ok(maybe_series) => {
                if maybe_series.is_empty() {
                    Err(CondCheckError::NotYet)
                } else {
                    Ok(maybe_series)
                }
            }
            Err(oximeter_db::Error::TimeseriesNotFound(_)) => {
                Err(CondCheckError::NotYet)
            }
            Err(e) => Err(CondCheckError::from(e)),
        }
    };

    // Timeouts for checks
    const POLL_INTERVAL: Duration = Duration::from_millis(100);
    const POLL_DURATION: Duration = Duration::from_secs(60);

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
    //
    // Specifically, we grab a timestamp before dropping the producer, and assert that any data in
    // the timeseries is from before that timestamp. Note that there is still techincally a race
    // here, between when the timestamp is taken and we drop the producer. This is likely to be
    // very small, but it definitely exists.
    let time_producer_dropped = chrono::Utc::now();
    drop(context.producer);
    let new_timeseries =
        retrieve_timeseries().await.expect("Failed to retrieve timeseries");
    assert_eq!(new_timeseries.len(), 1);
    let new_timeseries = new_timeseries.into_iter().next().unwrap();

    // The test here is a bit complicated, so deserves some explanation.
    //
    // We'd like to just assert that the old and new timeseries are equal. That's not quite right,
    // since data may be somewhere on its way to ClickHouse before we drop the producer, e.g. in
    // the collector's memory. So we first check that the original timeseries is a prefix of the
    // new timeseries. Then we check that any remaining measurements are no later than the
    // timestamp immediately prior to dropping the producer.
    #[track_caller]
    fn check_following_timeseries(
        timeseries: &oximeter_db::Timeseries,
        new_timeseries: &oximeter_db::Timeseries,
        drop_time: chrono::DateTime<chrono::Utc>,
    ) {
        assert_eq!(timeseries.timeseries_name, new_timeseries.timeseries_name);
        assert_eq!(timeseries.target, new_timeseries.target);
        assert!(
            timeseries.measurements.len() <= new_timeseries.measurements.len(),
            "New timeseries should have at least as many measurements as the original"
        );
        let n_measurements = timeseries.measurements.len();
        assert_eq!(
            timeseries.measurements,
            &new_timeseries.measurements[..n_measurements],
            "Original timeseries measurements should be a prefix of the new ones"
        );
        for meas in &new_timeseries.measurements[n_measurements..] {
            assert!(
                meas.timestamp() <= drop_time,
                "Any new measurements should have timestamps after {}",
                drop_time
            );
        }
    }

    check_following_timeseries(
        &timeseries,
        &new_timeseries,
        time_producer_dropped,
    );

    // Restart the producer, and verify that we have _more_ data than before
    // Set up a test metric producer server
    context.producer = nexus_test_utils::start_producer_server(
        context.server.get_http_server_internal_address(),
        nexus_test_utils::PRODUCER_UUID.parse().unwrap(),
    )
    .expect("Failed to restart metric producer server");
    nexus_test_utils::register_test_producer(&context.producer)
        .expect("Failed to register producer");

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
        let row =
            get_record().await.expect("Expected the producer record to exist");
        (
            row.get::<&str, i32>("port") as u16,
            row.get::<&str, chrono::DateTime<chrono::Utc>>("time_modified"),
        )
    };
    assert_eq!(new_port, context.producer.address().port());
    assert!(
        new_time_modified > original_time_modified,
        "Expected the modification time of the producer record to have changed"
    );

    // Drop oximeter and verify that we've still got the same data, because there's no collector
    // running.
    let time_oximeter_dropped = chrono::Utc::now();
    drop(context.oximeter);
    let new_timeseries =
        retrieve_timeseries().await.expect("Failed to retrieve timeseries");
    assert_eq!(new_timeseries.len(), 1);
    let new_timeseries = new_timeseries.into_iter().next().unwrap();
    check_following_timeseries(
        &timeseries,
        &new_timeseries,
        time_oximeter_dropped,
    );
    let timeseries = new_timeseries;

    // Restart oximeter again, and verify that we have even more new data.
    context.oximeter = nexus_test_utils::start_oximeter(
        context.logctx.log.new(o!("component" => "oximeter")),
        context.server.get_http_server_internal_address(),
        context.clickhouse.native_address().port(),
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
    context.teardown().await;
}
