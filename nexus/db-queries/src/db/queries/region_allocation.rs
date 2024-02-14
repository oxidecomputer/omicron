// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for provisioning regions.

use crate::db::datastore::REGION_REDUNDANCY_THRESHOLD;
use crate::db::true_or_cast_error::matches_sentinel;
use const_format::concatcp;
use diesel::pg::Pg;
use diesel::result::Error as DieselError;
use diesel::sql_types;
use omicron_common::api::external;
use omicron_common::nexus_config::RegionAllocationStrategy;
use std::cell::Cell;

const NOT_ENOUGH_DATASETS_SENTINEL: &'static str = "Not enough datasets";
const NOT_ENOUGH_ZPOOL_SPACE_SENTINEL: &'static str = "Not enough space";
const NOT_ENOUGH_UNIQUE_ZPOOLS_SENTINEL: &'static str =
    "Not enough unique zpools selected";

/// Translates a generic pool error to an external error based
/// on messages which may be emitted during region provisioning.
pub fn from_diesel(e: DieselError) -> external::Error {
    use crate::db::error;

    let sentinels = [
        NOT_ENOUGH_DATASETS_SENTINEL,
        NOT_ENOUGH_ZPOOL_SPACE_SENTINEL,
        NOT_ENOUGH_UNIQUE_ZPOOLS_SENTINEL,
    ];
    if let Some(sentinel) = matches_sentinel(&e, &sentinels) {
        let external_message = "Not enough storage";
        match sentinel {
            NOT_ENOUGH_DATASETS_SENTINEL => {
                return external::Error::insufficient_capacity(
                    external_message,
                    "Not enough datasets to allocate disks",
                );
            }
            NOT_ENOUGH_ZPOOL_SPACE_SENTINEL => {
                return external::Error::insufficient_capacity(
                    external_message,
                    "Not enough zpool space to allocate disks. There may not be enough disks with space for the requested region. You may also see this if your rack is in a degraded state, or you're running the default multi-rack topology configuration in a 1-sled development environment.",
                );
            }
            NOT_ENOUGH_UNIQUE_ZPOOLS_SENTINEL => {
                return external::Error::insufficient_capacity(
                    external_message,
                    "Not enough unique zpools selected while allocating disks",
                );
            }
            // Fall-through to the generic error conversion.
            _ => {}
        }
    }

    error::public_error_from_diesel(e, error::ErrorHandler::Server)
}

struct BindParamCounter(Cell<i32>);
impl BindParamCounter {
    fn new() -> Self {
        Self(0.into())
    }
    fn next(&self) -> i32 {
        self.0.set(self.0.get() + 1);
        self.0.get()
    }
}

trait SqlQueryBinds {
    fn add_bind(self, bind_counter: &BindParamCounter) -> Self;
}

impl<'a, Query> SqlQueryBinds
    for diesel::query_builder::BoxedSqlQuery<'a, Pg, Query>
{
    fn add_bind(self, bind_counter: &BindParamCounter) -> Self {
        self.sql("$").sql(bind_counter.next().to_string())
    }
}

// A small wrapper around [diesel::query_builder::BoxedSqlQuery] which
// assists with counting bind parameters and recommends avoiding the usage of
// any non-static strings in query construction.
struct QueryBuilder {
    query: diesel::query_builder::BoxedSqlQuery<'static, Pg, diesel::query_builder::SqlQuery>,
    bind_counter: BindParamCounter,
}

impl QueryBuilder {
    fn new() -> Self {
        Self {
            query: diesel::sql_query("").into_boxed(),
            bind_counter: BindParamCounter::new(),
        }
    }

    // Identifies that a bind parameter should exist in this location within
    // the SQL string.
    fn param(self) -> Self {
        Self {
            query: self.query.sql("$").sql(self.bind_counter.next().to_string()),
            bind_counter: self.bind_counter,
        }
    }

    // Slightly more strict than the "sql" method of Diesel's SqlQuery.
    // Only permits "&'static str" intentionally to limit susceptibility to
    // SQL injection.
    fn sql(self, s: &'static str) -> Self {
        Self {
            query: self.query.sql(s),
            bind_counter: self.bind_counter,
        }
    }

    fn bind<BindSt, Value>(self, b: Value) -> Self
    where
        Pg: sql_types::HasSqlType<BindSt>,
        Value: diesel::serialize::ToSql<BindSt, Pg> + Send + 'static,
        BindSt: Send + 'static
    {
        Self {
            query: self.query.bind(b),
            bind_counter: self.bind_counter,
        }
    }
}

pub fn allocation_query(
    volume_id: uuid::Uuid,
    block_size: u64,
    blocks_per_extent: u64,
    extent_count: u64,
    allocation_strategy: &RegionAllocationStrategy,
) -> diesel::query_builder::BoxedSqlQuery<
    'static,
    Pg,
    diesel::query_builder::SqlQuery,
> {
    let (seed, distinct_sleds) = {
        let (input_seed, distinct_sleds) = match allocation_strategy {
            RegionAllocationStrategy::Random { seed } => (seed, false),
            RegionAllocationStrategy::RandomWithDistinctSleds { seed } => {
                (seed, true)
            }
        };
        (
            input_seed.map_or_else(
                || {
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                },
                |seed| seed as u128,
            ),
            distinct_sleds,
        )
    };

    let seed = seed.to_le_bytes().to_vec();

    let size_delta = block_size * blocks_per_extent * extent_count;
    let redunancy: i64 = i64::try_from(REGION_REDUNDANCY_THRESHOLD).unwrap();


    let builder = QueryBuilder::new().sql(
    // Find all old regions associated with a particular volume
"WITH
  old_regions AS
    (SELECT
      region.id,
      region.time_created,
      region.time_modified,
      region.dataset_id,
      region.volume_id,
      region.block_size,
      region.blocks_per_extent,
      region.extent_count
    FROM region WHERE (region.volume_id = ").param().sql(")),")
    .bind::<sql_types::Uuid, _>(volume_id)

    // Calculates the old size being used by zpools under consideration as targets for region
    // allocation.
    .sql("
  old_zpool_usage AS
    (SELECT
      dataset.pool_id,
      sum(dataset.size_used) AS size_used
    FROM dataset WHERE ((dataset.size_used IS NOT NULL) AND (dataset.time_deleted IS NULL)) GROUP BY dataset.pool_id),");

    // Identifies zpools with enough space for region allocation.
    //
    // NOTE: This changes the format of the underlying SQL query, as it uses
    // distinct bind parameters depending on the conditional branch.
    if distinct_sleds {
        builder.sql("
  candidate_zpools AS
    (SELECT
      old_zpool_usage.pool_id
    FROM (old_zpool_usage INNER JOIN (zpool INNER JOIN sled ON (zpool.sled_id = sled.id)) ON (zpool.id = old_zpool_usage.pool_id))
    WHERE (((old_zpool_usage.size_used + ").param().sql(" ) <= total_size) AND (sled.provision_state = 'provisionable'))),")
        .bind::<sql_types::BigInt, _>(size_delta as i64)
    } else {
        builder.sql("
  candidate_zpools AS
    (SELECT DISTINCT ON (zpool.sled_id)
      old_zpool_usage.pool_id
    FROM (old_zpool_usage INNER JOIN (zpool INNER JOIN sled ON (zpool.sled_id = sled.id)) ON (zpool.id = old_zpool_usage.pool_id))
    WHERE (((old_zpool_usage.size_used + ").param().sql(" ) <= total_size) AND (sled.provision_state = 'provisionable'))
    ORDER BY zpool.sled_id, md5((CAST(zpool.id as BYTEA) || ").param().sql("))),")
        .bind::<sql_types::BigInt, _>(size_delta as i64)
        .bind::<sql_types::Bytea, _>(seed.clone())
    }
    // Find datasets which could be used for provisioning regions.
    //
    // We only consider datasets which are already allocated as "Crucible".
    // This implicitly distinguishes between "M.2s" and "U.2s" -- Nexus needs to
    // determine during dataset provisioning which devices should be considered for
    // usage as Crucible storage.
    //
    // We select only one dataset from each zpool.
    .sql(
"
  candidate_datasets AS
    (SELECT DISTINCT ON (dataset.pool_id)
      dataset.id,
      dataset.pool_id
    FROM (dataset INNER JOIN candidate_zpools ON (dataset.pool_id = candidate_zpools.pool_id))
    WHERE (((dataset.time_deleted IS NULL) AND (dataset.size_used IS NOT NULL)) AND (dataset.kind = 'crucible'))
    ORDER BY dataset.pool_id, md5((CAST(dataset.id as BYTEA) || ").param().sql("))),")
    .bind::<sql_types::Bytea, _>(seed.clone())
    // We order by md5 to shuffle the ordering of the datasets.
    // md5 has a uniform output distribution so it does the job.
    .sql(
"
  shuffled_candidate_datasets AS
    (SELECT
      candidate_datasets.id,
      candidate_datasets.pool_id
    FROM candidate_datasets
    ORDER BY md5((CAST(candidate_datasets.id as BYTEA) || ").param().sql(")) LIMIT ").param().sql("),")
    .bind::<sql_types::Bytea, _>(seed)
    .bind::<sql_types::BigInt, _>(redunancy)
    // Create the regions-to-be-inserted for the volume.
    .sql(
"
  candidate_regions AS
    (SELECT
      gen_random_uuid() AS id,
      now() AS time_created,
      now() AS time_modified,
      shuffled_candidate_datasets.id AS dataset_id,
      ").param().sql(" AS volume_id,
      ").param().sql(" AS block_size,
      ").param().sql(" AS blocks_per_extent,
      ").param().sql(" AS extent_count
    FROM shuffled_candidate_datasets),")
    .bind::<sql_types::Uuid, _>(volume_id)
    .bind::<sql_types::BigInt, _>(block_size as i64)
    .bind::<sql_types::BigInt, _>(blocks_per_extent as i64)
    .bind::<sql_types::BigInt, _>(extent_count as i64)
    // A subquery which summarizes the changes we intend to make, showing:
    //
    // 1. Which datasets will have size adjustments
    // 2. Which pools those datasets belong to
    // 3. The delta in size-used
    .sql(
"
  proposed_dataset_changes AS
    (SELECT
      candidate_regions.dataset_id AS id,
      dataset.pool_id AS pool_id,
      ((candidate_regions.block_size * candidate_regions.blocks_per_extent) * candidate_regions.extent_count) AS size_used_delta
    FROM (candidate_regions INNER JOIN dataset ON (dataset.id = candidate_regions.dataset_id))),")
    // Confirms whether or not the insertion and updates should
    // occur.
    //
    // This subquery additionally exits the CTE early with an error if either:
    // 1. Not enough datasets exist to provision regions with our required
    //    redundancy, or
    // 2. Not enough space exists on zpools to perform the provisioning.
    //
    // We want to ensure that we do not allocate on two datasets in the same
    // zpool, for two reasons
    // - Data redundancy: If a drive fails it should only take one of the 3
    //   regions with it
    // - Risk of overallocation: We only check that each zpool as enough
    //   room for one region, so we should not allocate more than one region
    //   to it.
    //
    // Selecting two datasets on the same zpool will not initially be
    // possible, as at the time of writing each zpool only has one dataset.
    // Additionally, we intend to modify the allocation strategy to select
    // from 3 distinct sleds, removing the possibility entirely. But, if we
    // introduce a change that adds another crucible dataset to zpools
    // before we improve the allocation strategy, this check will make sure
    // we don't violate drive redundancy, and generate an error instead.
    .sql("
  do_insert AS
    (SELECT
      (((((SELECT COUNT(*) FROM old_regions LIMIT 1) < ").param().sql(") AND CAST(IF(((SELECT COUNT(*) FROM candidate_zpools LIMIT 1) >= ").param().sql(concatcp!("), 'TRUE', '", NOT_ENOUGH_ZPOOL_SPACE_SENTINEL, "') AS BOOL)) AND CAST(IF(((SELECT COUNT(*) FROM candidate_regions LIMIT 1) >= ")).param().sql(concatcp!("), 'TRUE', '", NOT_ENOUGH_DATASETS_SENTINEL, "') AS BOOL)) AND CAST(IF(((SELECT COUNT(DISTINCT dataset.pool_id) FROM (candidate_regions INNER JOIN dataset ON (candidate_regions.dataset_id = dataset.id)) LIMIT 1) >= ")).param().sql(concatcp!("), 'TRUE', '", NOT_ENOUGH_UNIQUE_ZPOOLS_SENTINEL, "') AS BOOL)) AS insert),"))
    .bind::<sql_types::BigInt, _>(redunancy)
    .bind::<sql_types::BigInt, _>(redunancy)
    .bind::<sql_types::BigInt, _>(redunancy)
    .bind::<sql_types::BigInt, _>(redunancy)
    .sql(
"
  inserted_regions AS
    (INSERT INTO region
      (id, time_created, time_modified, dataset_id, volume_id, block_size, blocks_per_extent, extent_count)
    SELECT
      candidate_regions.id,
      candidate_regions.time_created,
      candidate_regions.time_modified,
      candidate_regions.dataset_id,
      candidate_regions.volume_id,
      candidate_regions.block_size,
      candidate_regions.blocks_per_extent,
      candidate_regions.extent_count
    FROM candidate_regions
    WHERE
      (SELECT do_insert.insert FROM do_insert LIMIT 1)
    RETURNING
      region.id,
      region.time_created,
      region.time_modified,
      region.dataset_id,
      region.volume_id,
      region.block_size,
      region.blocks_per_extent,
      region.extent_count
    ),
  updated_datasets AS
    (UPDATE dataset SET
      size_used = (dataset.size_used + (SELECT proposed_dataset_changes.size_used_delta FROM proposed_dataset_changes WHERE (proposed_dataset_changes.id = dataset.id) LIMIT 1))
    WHERE (
      (dataset.id = ANY(SELECT proposed_dataset_changes.id FROM proposed_dataset_changes)) AND
      (SELECT do_insert.insert FROM do_insert LIMIT 1))
    RETURNING
      dataset.id,
      dataset.time_created,
      dataset.time_modified,
      dataset.time_deleted,
      dataset.rcgen,
      dataset.pool_id,
      dataset.ip,
      dataset.port,
      dataset.kind,
      dataset.size_used
    )
(SELECT
  dataset.id,
  dataset.time_created,
  dataset.time_modified,
  dataset.time_deleted,
  dataset.rcgen,
  dataset.pool_id,
  dataset.ip,
  dataset.port,
  dataset.kind,
  dataset.size_used,
  old_regions.id,
  old_regions.time_created,
  old_regions.time_modified,
  old_regions.dataset_id,
  old_regions.volume_id,
  old_regions.block_size,
  old_regions.blocks_per_extent,
  old_regions.extent_count
FROM
  (old_regions INNER JOIN dataset ON (old_regions.dataset_id = dataset.id))
) UNION
(SELECT
  updated_datasets.id,
  updated_datasets.time_created,
  updated_datasets.time_modified,
  updated_datasets.time_deleted,
  updated_datasets.rcgen,
  updated_datasets.pool_id,
  updated_datasets.ip,
  updated_datasets.port,
  updated_datasets.kind,
  updated_datasets.size_used,
  inserted_regions.id,
  inserted_regions.time_created,
  inserted_regions.time_modified,
  inserted_regions.dataset_id,
  inserted_regions.volume_id,
  inserted_regions.block_size,
  inserted_regions.blocks_per_extent,
  inserted_regions.extent_count
FROM (inserted_regions INNER JOIN updated_datasets ON (inserted_regions.dataset_id = updated_datasets.id)))"
    ).query
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::explain::ExplainableAsync;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;
    use uuid::Uuid;

    // Explain the possible forms of the SQL query to ensure that it
    // creates a valid SQL string.
    #[tokio::test]
    async fn explainable() {
        let logctx = dev::test_setup_log("explainable");
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool = crate::db::Pool::new(&logctx.log, &cfg);
        let conn = pool.pool().get().await.unwrap();

        let volume_id = Uuid::new_v4();
        let block_size = 512;
        let blocks_per_extent = 4;
        let extent_count = 8;

        // First structure: Explain the query with "RandomWithDistinctSleds"

        let region_allocate = allocation_query(
            volume_id,
            block_size,
            blocks_per_extent,
            extent_count,
            &RegionAllocationStrategy::RandomWithDistinctSleds { seed: None },
        );
        let _ = region_allocate
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        // Second structure: Explain the query with "Random"

        let region_allocate = allocation_query(
            volume_id,
            block_size,
            blocks_per_extent,
            extent_count,
            &RegionAllocationStrategy::Random { seed: None },
        );
        let _ = region_allocate
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
