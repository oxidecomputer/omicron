// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for provisioning regions.

use crate::db::column_walker::AllColumnsOf;
use crate::db::model::{Dataset, Region};
use crate::db::raw_query_builder::{QueryBuilder, TypedSqlQuery};
use crate::db::schema;
use crate::db::true_or_cast_error::matches_sentinel;
use const_format::concatcp;
use diesel::pg::Pg;
use diesel::result::Error as DieselError;
use diesel::sql_types;
use nexus_config::RegionAllocationStrategy;
use omicron_common::api::external;

type AllColumnsOfRegion = AllColumnsOf<schema::region::table>;
type AllColumnsOfDataset = AllColumnsOf<schema::dataset::table>;

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

type SelectableSql<T> = <
    <T as diesel::Selectable<Pg>>::SelectExpression as diesel::Expression
>::SqlType;

pub fn allocation_query(
    volume_id: uuid::Uuid,
    block_size: u64,
    blocks_per_extent: u64,
    extent_count: u64,
    allocation_strategy: &RegionAllocationStrategy,
    redundancy: usize,
) -> TypedSqlQuery<(SelectableSql<Dataset>, SelectableSql<Region>)> {
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
                |seed| u128::from(seed),
            ),
            distinct_sleds,
        )
    };

    let seed = seed.to_le_bytes().to_vec();

    let size_delta = block_size * blocks_per_extent * extent_count;
    let redundancy: i64 = i64::try_from(redundancy).unwrap();

    let builder = QueryBuilder::new().sql(
    // Find all old regions associated with a particular volume
"WITH
  old_regions AS (
    SELECT ").sql(AllColumnsOfRegion::with_prefix("region")).sql("
    FROM region WHERE (region.volume_id = ").param().sql(")),")
    .bind::<sql_types::Uuid, _>(volume_id)

    // Calculates the old size being used by zpools under consideration as targets for region
    // allocation.
    .sql("
  old_zpool_usage AS (
    SELECT
      dataset.pool_id,
      sum(dataset.size_used) AS size_used
    FROM dataset WHERE ((dataset.size_used IS NOT NULL) AND (dataset.time_deleted IS NULL)) GROUP BY dataset.pool_id),")

    // Any zpool already have this volume's existing regions?
    .sql("
  existing_zpools AS (
    SELECT
      dataset.pool_id
    FROM
      dataset INNER JOIN old_regions ON (old_regions.dataset_id = dataset.id)
  ),")

    // Identifies zpools with enough space for region allocation, that are not
    // currently used by this Volume's existing regions.
    //
    // NOTE: 'distinct_sleds' changes the format of the underlying SQL query, as it uses
    // distinct bind parameters depending on the conditional branch.
    .sql("
  candidate_zpools AS (");
    let builder = if distinct_sleds {
        builder.sql("SELECT DISTINCT ON (zpool.sled_id) ")
    } else {
        builder.sql("SELECT ")
    };
    let builder = builder.sql("
        old_zpool_usage.pool_id
    FROM
        old_zpool_usage
        INNER JOIN
        (zpool INNER JOIN sled ON (zpool.sled_id = sled.id)) ON (zpool.id = old_zpool_usage.pool_id)
        INNER JOIN
        physical_disk ON (zpool.physical_disk_id = physical_disk.id)
    WHERE (
      (old_zpool_usage.size_used + ").param().sql(" ) <=
         (SELECT total_size FROM omicron.public.inv_zpool WHERE
          inv_zpool.id = old_zpool_usage.pool_id
          ORDER BY inv_zpool.time_collected DESC LIMIT 1)
      AND sled.sled_policy = 'in_service'
      AND sled.sled_state = 'active'
      AND physical_disk.disk_policy = 'in_service'
      AND physical_disk.disk_state = 'active'
      AND NOT(zpool.id = ANY(SELECT existing_zpools.pool_id FROM existing_zpools))
    )"
    ).bind::<sql_types::BigInt, _>(size_delta as i64);

    let builder = if distinct_sleds {
        builder
            .sql("ORDER BY zpool.sled_id, md5((CAST(zpool.id as BYTEA) || ")
            .param()
            .sql("))")
            .bind::<sql_types::Bytea, _>(seed.clone())
    } else {
        builder
    }
    .sql("),");

    // Find datasets which could be used for provisioning regions.
    //
    // We only consider datasets which are already allocated as "Crucible".
    // This implicitly distinguishes between "M.2s" and "U.2s" -- Nexus needs to
    // determine during dataset provisioning which devices should be considered for
    // usage as Crucible storage.
    //
    // We select only one dataset from each zpool.
    builder.sql("
  candidate_datasets AS (
    SELECT DISTINCT ON (dataset.pool_id)
      dataset.id,
      dataset.pool_id
    FROM (dataset INNER JOIN candidate_zpools ON (dataset.pool_id = candidate_zpools.pool_id))
    WHERE (
      ((dataset.time_deleted IS NULL) AND
      (dataset.size_used IS NOT NULL)) AND
      (dataset.kind = 'crucible')
    )
    ORDER BY dataset.pool_id, md5((CAST(dataset.id as BYTEA) || ").param().sql("))
  ),")
    .bind::<sql_types::Bytea, _>(seed.clone())

    // We order by md5 to shuffle the ordering of the datasets.
    // md5 has a uniform output distribution so it does the job.
    .sql("
  shuffled_candidate_datasets AS (
    SELECT
      candidate_datasets.id,
      candidate_datasets.pool_id
    FROM candidate_datasets
    ORDER BY md5((CAST(candidate_datasets.id as BYTEA) || ").param().sql(")) LIMIT ").param().sql("
  ),")
    .bind::<sql_types::Bytea, _>(seed)
    .bind::<sql_types::BigInt, _>(redundancy)

    // Create the regions-to-be-inserted for the volume.
    .sql("
  candidate_regions AS (
    SELECT
      gen_random_uuid() AS id,
      now() AS time_created,
      now() AS time_modified,
      shuffled_candidate_datasets.id AS dataset_id,
      ").param().sql(" AS volume_id,
      ").param().sql(" AS block_size,
      ").param().sql(" AS blocks_per_extent,
      ").param().sql(" AS extent_count
    FROM shuffled_candidate_datasets")
  // Only select the *additional* number of candidate regions for the required
  // redundancy level
  .sql("
    LIMIT (").param().sql(" - (
      SELECT COUNT(*) FROM old_regions
    ))
  ),")
    .bind::<sql_types::Uuid, _>(volume_id)
    .bind::<sql_types::BigInt, _>(block_size as i64)
    .bind::<sql_types::BigInt, _>(blocks_per_extent as i64)
    .bind::<sql_types::BigInt, _>(extent_count as i64)
    .bind::<sql_types::BigInt, _>(redundancy)

    // A subquery which summarizes the changes we intend to make, showing:
    //
    // 1. Which datasets will have size adjustments
    // 2. Which pools those datasets belong to
    // 3. The delta in size-used
    .sql("
  proposed_dataset_changes AS (
    SELECT
      candidate_regions.dataset_id AS id,
      dataset.pool_id AS pool_id,
      ((candidate_regions.block_size * candidate_regions.blocks_per_extent) * candidate_regions.extent_count) AS size_used_delta
    FROM (candidate_regions INNER JOIN dataset ON (dataset.id = candidate_regions.dataset_id))
  ),")

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
    // Additionally, provide a configuration option ("distinct_sleds") to modify
    // the allocation strategy to select from 3 distinct sleds, removing the
    // possibility entirely. But, if we introduce a change that adds another
    // crucible dataset to zpools before we improve the allocation strategy,
    // this check will make sure we don't violate drive redundancy, and generate
    // an error instead.
    .sql("
  do_insert AS (
    SELECT (((")
    // There's regions not allocated yet
    .sql("
        ((SELECT COUNT(*) FROM old_regions LIMIT 1) < ").param().sql(") AND")
    // Enough filtered candidate zpools + existing zpools to meet redundancy
    .sql("
        CAST(IF(((
          (
            (SELECT COUNT(*) FROM candidate_zpools LIMIT 1) +
            (SELECT COUNT(*) FROM existing_zpools LIMIT 1)
          )
        ) >= ").param().sql(concatcp!("), 'TRUE', '", NOT_ENOUGH_ZPOOL_SPACE_SENTINEL, "') AS BOOL)) AND"))
    // Enough candidate regions + existing regions to meet redundancy
    .sql("
        CAST(IF(((
          (
            (SELECT COUNT(*) FROM candidate_regions LIMIT 1) +
            (SELECT COUNT(*) FROM old_regions LIMIT 1)
          )
        ) >= ").param().sql(concatcp!("), 'TRUE', '", NOT_ENOUGH_DATASETS_SENTINEL, "') AS BOOL)) AND"))
    // Enough unique zpools (looking at both existing and new) to meet redundancy
    .sql("
        CAST(IF(((
         (
           SELECT
             COUNT(DISTINCT pool_id)
           FROM
            (
              (
               SELECT
                 dataset.pool_id
               FROM
                 candidate_regions
                   INNER JOIN dataset ON (candidate_regions.dataset_id = dataset.id)
              )
              UNION
              (
               SELECT
                 dataset.pool_id
               FROM
                 old_regions
                   INNER JOIN dataset ON (old_regions.dataset_id = dataset.id)
              )
            )
           LIMIT 1
         )
        ) >= ").param().sql(concatcp!("), 'TRUE', '", NOT_ENOUGH_UNIQUE_ZPOOLS_SENTINEL, "') AS BOOL)
     ) AS insert
   ),"))
    .bind::<sql_types::BigInt, _>(redundancy)
    .bind::<sql_types::BigInt, _>(redundancy)
    .bind::<sql_types::BigInt, _>(redundancy)
    .bind::<sql_types::BigInt, _>(redundancy)

    .sql("
  inserted_regions AS (
    INSERT INTO region
      (id, time_created, time_modified, dataset_id, volume_id, block_size, blocks_per_extent, extent_count)
    SELECT ").sql(AllColumnsOfRegion::with_prefix("candidate_regions")).sql("
    FROM candidate_regions
    WHERE
      (SELECT do_insert.insert FROM do_insert LIMIT 1)
    RETURNING ").sql(AllColumnsOfRegion::with_prefix("region")).sql("
  ),
  updated_datasets AS (
    UPDATE dataset SET
      size_used = (dataset.size_used + (SELECT proposed_dataset_changes.size_used_delta FROM proposed_dataset_changes WHERE (proposed_dataset_changes.id = dataset.id) LIMIT 1))
    WHERE (
      (dataset.id = ANY(SELECT proposed_dataset_changes.id FROM proposed_dataset_changes)) AND
      (SELECT do_insert.insert FROM do_insert LIMIT 1))
    RETURNING ").sql(AllColumnsOfDataset::with_prefix("dataset")).sql("
  )
(
  SELECT ")
    .sql(AllColumnsOfDataset::with_prefix("dataset"))
    .sql(", ")
    .sql(AllColumnsOfRegion::with_prefix("old_regions")).sql("
  FROM
    (old_regions INNER JOIN dataset ON (old_regions.dataset_id = dataset.id))
)
UNION
(
  SELECT ")
    .sql(AllColumnsOfDataset::with_prefix("updated_datasets"))
    .sql(", ")
    .sql(AllColumnsOfRegion::with_prefix("inserted_regions")).sql("
  FROM (inserted_regions INNER JOIN updated_datasets ON (inserted_regions.dataset_id = updated_datasets.id))
)"
    ).query()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::datastore::REGION_REDUNDANCY_THRESHOLD;
    use crate::db::explain::ExplainableAsync;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;
    use uuid::Uuid;

    // This test is a bit of a "change detector", but it's here to help with
    // debugging too. If you change this query, it can be useful to see exactly
    // how the output SQL has been altered.
    #[tokio::test]
    async fn expectorate_query() {
        let volume_id = Uuid::nil();
        let block_size = 512;
        let blocks_per_extent = 4;
        let extent_count = 8;

        // First structure: "RandomWithDistinctSleds"

        let region_allocate = allocation_query(
            volume_id,
            block_size,
            blocks_per_extent,
            extent_count,
            &RegionAllocationStrategy::RandomWithDistinctSleds {
                seed: Some(1),
            },
            REGION_REDUNDANCY_THRESHOLD,
        );
        expectorate_query_contents(
            &region_allocate,
            "tests/output/region_allocate_distinct_sleds.sql",
        )
        .await;

        // Second structure: "Random"

        let region_allocate = allocation_query(
            volume_id,
            block_size,
            blocks_per_extent,
            extent_count,
            &RegionAllocationStrategy::Random { seed: Some(1) },
            REGION_REDUNDANCY_THRESHOLD,
        );
        expectorate_query_contents(
            &region_allocate,
            "tests/output/region_allocate_random_sleds.sql",
        )
        .await;
    }

    // Explain the possible forms of the SQL query to ensure that it
    // creates a valid SQL string.
    #[tokio::test]
    async fn explainable() {
        let logctx = dev::test_setup_log("explainable");
        let log = logctx.log.new(o!());
        let mut db = test_setup_database(&log).await;
        let cfg = crate::db::Config { url: db.pg_config().clone() };
        let pool = crate::db::Pool::new_qorb_single_host_blocking(&cfg).await;
        let conn = pool.claim().await.unwrap();

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
            REGION_REDUNDANCY_THRESHOLD,
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
            REGION_REDUNDANCY_THRESHOLD,
        );
        let _ = region_allocate
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
