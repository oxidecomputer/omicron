// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for Oximeter collectors and producers.

use crate::db::raw_query_builder::{QueryBuilder, TypedSqlQuery};
use diesel::sql_types;
use ipnetwork::IpNetwork;
use nexus_db_model::{ProducerKind, ProducerKindEnum, SqlU16};
use omicron_common::api::internal;
use uuid::Uuid;

/// Upsert a metric producer.
///
/// If the producer is being inserted for the first time, a random Oximeter will
/// be chosen from among all non-expunged entries in the `oximeter` table.
///
/// If this query succeeds but returns 0 rows inserted/updated, there are no
/// non-expunged `Oximeter` instances to choose.
pub fn upsert_producer(
    producer: &internal::nexus::ProducerEndpoint,
) -> TypedSqlQuery<()> {
    let builder = QueryBuilder::new();

    // Choose a random non-expunged Oximeter instance.
    let builder = builder.sql(
        r#"
        WITH chosen_oximeter AS (
          SELECT
            id AS oximeter_id
          FROM oximeter
          WHERE time_expunged IS NULL
          ORDER BY random()
          LIMIT 1
        )
    "#,
    );

    // Build the INSERT for new producers...
    let builder = builder.sql(
        r#"
        INSERT INTO metric_producer (
            id,
            time_created,
            time_modified,
            kind,
            ip,
            port,
            interval,
            oximeter_id
        )
    "#,
    );

    // ... by querying our chosen oximeter ID and the values from `producer`.
    let builder = builder
        .sql("SELECT ")
        .param()
        .bind::<sql_types::Uuid, _>(producer.id)
        .sql(", now()") // time_created
        .sql(", now()") // time_modified
        .sql(", ")
        .param()
        .bind::<ProducerKindEnum, ProducerKind>(producer.kind.into())
        .sql(", ")
        .param()
        .bind::<sql_types::Inet, IpNetwork>(producer.address.ip().into())
        .sql(", ")
        .param()
        .bind::<sql_types::Int4, SqlU16>(producer.address.port().into())
        .sql(", ")
        .param()
        .bind::<sql_types::Float, _>(producer.interval.as_secs_f32())
        .sql(", oximeter_id FROM chosen_oximeter");

    // If the producer already exists, update a subset of the fields,
    // intentionally omitting `time_created` and `oximeter_id`.
    let builder = builder.sql(
        r#"
        ON CONFLICT (id)
        DO UPDATE SET
          time_modified = now(),
          kind = excluded.kind,
          ip = excluded.ip,
          port = excluded.port,
          interval = excluded.interval
    "#,
    );

    builder.query()
}

/// For a given Oximeter instance (which is presumably no longer running),
/// reassign any producers assigned to it to a different Oximeter. Each
/// assignment is randomly chosen from among the non-expunged Oximeter instances
/// recorded in the `oximeter` table.
pub fn reassign_producers_query(oximeter_id: Uuid) -> TypedSqlQuery<()> {
    let builder = QueryBuilder::new();

    // Find all non-expunged Oximeter instances.
    let builder = builder.sql(
        "\
        WITH available_oximeters AS ( \
          SELECT ARRAY( \
            SELECT id FROM oximeter WHERE time_expunged IS NULL
          ) AS ids \
        ), ",
    );

    // Create a mapping of producer ID <-> new, random, non-expunged Oximeter ID
    // for every producer assigned to `oximeter_id`. If the `ids` array from the
    // previous expression is empty, every `new_id` column in this expression
    // will be NULL. We'll catch that in the update below.
    let builder = builder
        .sql(
            "\
            new_assignments AS ( \
              SELECT
                metric_producer.id AS producer_id,
                ids[1 + floor(random() * array_length(ids, 1)::float)::int]
                  AS new_id
              FROM metric_producer
              LEFT JOIN available_oximeters ON true
              WHERE oximeter_id = ",
        )
        .param()
        .sql(")")
        .bind::<sql_types::Uuid, _>(oximeter_id);

    // Actually perform the update. If the `new_id` column from the previous
    // step is `NULL` (because there aren't any non-expunged Oximeter
    // instances), this will fail the `NOT NULL` constraint on the oximeter_id
    // column.
    let builder = builder
        .sql(
            "\
            UPDATE metric_producer SET oximeter_id = ( \
              SELECT new_id FROM new_assignments \
              WHERE new_assignments.producer_id = metric_producer.id \
            ) WHERE oximeter_id = ",
        )
        .param()
        .bind::<sql_types::Uuid, _>(oximeter_id);

    builder.query()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::explain::ExplainableAsync;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;
    use std::time::Duration;
    use uuid::Uuid;

    // These tests are a bit of a "change detector", but it's here to help with
    // debugging too. If you change these query, it can be useful to see exactly
    // how the output SQL has been altered.
    #[tokio::test]
    async fn expectorate_query_upsert_producer() {
        let producer = internal::nexus::ProducerEndpoint {
            id: Uuid::nil(),
            kind: ProducerKind::SledAgent.into(),
            address: "[::1]:0".parse().unwrap(),
            interval: Duration::from_secs(30),
        };

        let query = upsert_producer(&producer);

        expectorate_query_contents(
            &query,
            "tests/output/oximeter_upsert_producer.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_query_reassign_producers() {
        let oximeter_id = Uuid::nil();

        let query = reassign_producers_query(oximeter_id);

        expectorate_query_contents(
            &query,
            "tests/output/oximeter_reassign_producers.sql",
        )
        .await;
    }

    // Explain the SQL queries to ensure that they create valid SQL strings.
    #[tokio::test]
    async fn explainable_upsert_producer() {
        let logctx = dev::test_setup_log("explainable_upsert_producer");
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool = crate::db::Pool::new_single_host(&logctx.log, &cfg);
        let conn = pool.claim().await.unwrap();

        let producer = internal::nexus::ProducerEndpoint {
            id: Uuid::nil(),
            kind: ProducerKind::SledAgent.into(),
            address: "[::1]:0".parse().unwrap(),
            interval: Duration::from_secs(30),
        };

        let query = upsert_producer(&producer);
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn explainable_reassign_producers() {
        let logctx = dev::test_setup_log("explainable_reassign_producers");
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool = crate::db::Pool::new_single_host(&logctx.log, &cfg);
        let conn = pool.claim().await.unwrap();

        let oximeter_id = Uuid::nil();

        let query = reassign_producers_query(oximeter_id);
        let _ = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
