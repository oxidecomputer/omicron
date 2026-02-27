// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for provisioning regions.

use crate::db::column_walker::AllColumnsOf;
use crate::db::model::{CrucibleDataset, Region, RegionReservationPercent};
use crate::db::raw_query_builder::{QueryBuilder, TypedSqlQuery};
use crate::db::true_or_cast_error::matches_sentinel;
use const_format::concatcp;
use diesel::pg::Pg;
use diesel::result::Error as DieselError;
use diesel::sql_types;
use nexus_config::RegionAllocationStrategy;
use nexus_db_schema::enums::RegionReservationPercentEnum;
use nexus_db_schema::schema;
use omicron_common::api::external;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::VolumeUuid;

type AllColumnsOfRegion = AllColumnsOf<schema::region::table>;
type AllColumnsOfCrucibleDataset =
    AllColumnsOf<schema::crucible_dataset::table>;

const NOT_ENOUGH_DATASETS_SENTINEL: &'static str = "Not enough datasets";
const NOT_ENOUGH_ZPOOL_SPACE_SENTINEL: &'static str = "Not enough space";
const NOT_ENOUGH_UNIQUE_ZPOOLS_SENTINEL: &'static str =
    "Not enough unique zpools selected";

/// Translates a generic pool error to an external error based
/// on messages which may be emitted during region provisioning.
pub fn from_diesel(e: DieselError) -> external::Error {
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
                    "Not enough zpool space to allocate disks. There may not \
                    be enough disks with space for the requested region. You \
                    may also see this if your rack is in a degraded state, or \
                    you're running the default multi-rack topology \
                    configuration in a 1-sled development environment.",
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

    nexus_db_errors::public_error_from_diesel(
        e,
        nexus_db_errors::ErrorHandler::Server,
    )
}

type SelectableSql<T> = <
    <T as diesel::Selectable<Pg>>::SelectExpression as diesel::Expression
>::SqlType;

/// Parameters for the region(s) being allocated
#[derive(Debug, Clone, Copy)]
pub struct RegionParameters {
    pub block_size: u64,
    pub blocks_per_extent: u64,
    pub extent_count: u64,

    /// True if the region will be filled with a Clone operation and is meant to
    /// be read-only.
    pub read_only: bool,
}

type AllocationQuery =
    TypedSqlQuery<(SelectableSql<CrucibleDataset>, SelectableSql<Region>)>;

/// Currently the largest region that can be allocated matches the largest disk
/// that can be requested, but separate this constant so that when
/// MAX_DISK_SIZE_BYTES is increased the region allocation query will still use
/// this as a maximum size.
pub const MAX_REGION_SIZE_BYTES: u64 = 1098437885952; // 1023 * (1 << 30);

#[derive(Debug)]
pub enum AllocationQueryError {
    /// Region size multiplication overflowed u64
    RegionSizeOverflow,

    /// Requested region size larger than maximum
    RequestedRegionOverMaxSize { request: u64, maximum: u64 },

    /// Requested size not divisible by reservation factor
    RequestedRegionNotDivisibleByFactor { request: i64, factor: i64 },

    /// Adding the overhead to the requested size overflowed
    RequestedRegionOverheadOverflow { request: i64, overhead: i64 },

    /// Converting from u64 to i64 truncated
    RequestedRegionSizeTruncated { request: u64, e: String },
}

impl From<AllocationQueryError> for external::Error {
    fn from(e: AllocationQueryError) -> external::Error {
        match e {
            AllocationQueryError::RegionSizeOverflow => {
                external::Error::invalid_value(
                    "region allocation",
                    "region size overflowed u64",
                )
            }

            AllocationQueryError::RequestedRegionOverMaxSize {
                request,
                maximum,
            } => external::Error::invalid_value(
                "region allocation",
                format!("region size {request} over maximum {maximum}"),
            ),

            AllocationQueryError::RequestedRegionNotDivisibleByFactor {
                request,
                factor,
            } => external::Error::invalid_value(
                "region allocation",
                format!("region size {request} not divisible by {factor}"),
            ),

            AllocationQueryError::RequestedRegionOverheadOverflow {
                request,
                overhead,
            } => external::Error::invalid_value(
                "region allocation",
                format!(
                    "adding {overhead} to region size {request} overflowed"
                ),
            ),

            AllocationQueryError::RequestedRegionSizeTruncated {
                request,
                e,
            } => external::Error::internal_error(&format!(
                "converting {request} to i64 failed! {e}"
            )),
        }
    }
}

/// For a given volume, idempotently allocate enough regions (according to some
/// allocation strategy) to meet some redundancy level. This should only be used
/// for the region set that is in the top level of the Volume (not the deeper
/// layers of the hierarchy). If that volume has region snapshots in the region
/// set, a `snapshot_id` should be supplied matching those entries.
///
/// Depending on the call site, it may not safe for multiple callers to call
/// this function concurrently for the same volume id. Care is required!
pub fn allocation_query(
    volume_id: VolumeUuid,
    snapshot_id: Option<uuid::Uuid>,
    params: RegionParameters,
    allocation_strategy: &RegionAllocationStrategy,
    redundancy: usize,
) -> Result<AllocationQuery, AllocationQueryError> {
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

    // Ensure that the multiplication doesn't overflow.
    let requested_size: u64 = params
        .block_size
        .checked_mul(params.blocks_per_extent)
        .ok_or(AllocationQueryError::RegionSizeOverflow)?
        .checked_mul(params.extent_count)
        .ok_or(AllocationQueryError::RegionSizeOverflow)?;

    if requested_size > MAX_REGION_SIZE_BYTES {
        return Err(AllocationQueryError::RequestedRegionOverMaxSize {
            request: requested_size,
            maximum: MAX_REGION_SIZE_BYTES,
        });
    }

    // After the above check, cast from u64 to i64. The value is low enough
    // (after the check above) that try_into should always return Ok.
    let requested_size: i64 = match requested_size.try_into() {
        Ok(v) => v,
        Err(e) => {
            return Err(AllocationQueryError::RequestedRegionSizeTruncated {
                request: requested_size,
                e: e.to_string(),
            });
        }
    };

    let reservation_percent = RegionReservationPercent::TwentyFive;

    let size_delta: i64 = match reservation_percent {
        RegionReservationPercent::TwentyFive => {
            // Check first that the requested region size is divisible by this.
            // This should basically never fail because all block sizes are
            // divisible by 4.
            if requested_size % 4 != 0 {
                return Err(
                    AllocationQueryError::RequestedRegionNotDivisibleByFactor {
                        request: requested_size,
                        factor: 4,
                    },
                );
            }

            let overhead: i64 = requested_size.checked_div(4).ok_or(
                AllocationQueryError::RequestedRegionNotDivisibleByFactor {
                    request: requested_size,
                    factor: 4,
                },
            )?;

            requested_size.checked_add(overhead).ok_or(
                AllocationQueryError::RequestedRegionOverheadOverflow {
                    request: requested_size,
                    overhead,
                },
            )?
        }
    };

    let redundancy: i64 = i64::try_from(redundancy).unwrap();

    let mut builder = QueryBuilder::new();

    builder
        .sql(
            // Find all old regions associated with a particular volume
            "WITH
  old_regions AS (
    SELECT ",
        )
        .sql(AllColumnsOfRegion::with_prefix("region"))
        .sql(
            "
    FROM region WHERE (region.volume_id = ",
        )
        .param()
        .sql(")),")
        .bind::<sql_types::Uuid, _>(*volume_id.as_untyped_uuid())
        // Calculates the old size being used by zpools under consideration as
        // targets for region allocation.
        //
        // Account for the local storage dataset rendezvous tables not having been
        // created yet (or for the integration tests, where blueprint execution is
        // currently disabled) by performing a LEFT JOIN on pool_id and a coalesce
        // for the size_used column.
        .sql(
            "
  old_zpool_usage AS (
    SELECT
      crucible_dataset.pool_id,
      (
       sum(crucible_dataset.size_used) +
       sum(coalesce(rendezvous_local_storage_dataset.size_used, 0)) +
       sum(coalesce(rendezvous_local_storage_unencrypted_dataset.size_used, 0))
      ) AS size_used
    FROM
      crucible_dataset
    LEFT JOIN rendezvous_local_storage_dataset
      ON
        crucible_dataset.pool_id = rendezvous_local_storage_dataset.pool_id AND
        rendezvous_local_storage_dataset.time_tombstoned is NULL
    LEFT JOIN rendezvous_local_storage_unencrypted_dataset
      ON
        crucible_dataset.pool_id = rendezvous_local_storage_unencrypted_dataset.pool_id AND
        rendezvous_local_storage_unencrypted_dataset.time_tombstoned is NULL
    WHERE
      crucible_dataset.size_used IS NOT NULL AND
      crucible_dataset.time_deleted IS NULL
    GROUP BY crucible_dataset.pool_id),",
        );

    if let Some(snapshot_id) = snapshot_id {
        // Any zpool already have this volume's existing regions, or host the
        // snapshot volume's regions?
        builder.sql("
      existing_zpools AS ((
        SELECT
          crucible_dataset.pool_id
        FROM
          crucible_dataset INNER JOIN old_regions ON (old_regions.dataset_id = crucible_dataset.id)
      ) UNION (
       select crucible_dataset.pool_id from
 crucible_dataset inner join region_snapshot on (region_snapshot.dataset_id = crucible_dataset.id)
 where region_snapshot.snapshot_id = ").param().sql(")),")
        .bind::<sql_types::Uuid, _>(snapshot_id);
    } else {
        // Any zpool already have this volume's existing regions?
        builder.sql("
      existing_zpools AS (
        SELECT
          crucible_dataset.pool_id
        FROM
          crucible_dataset INNER JOIN old_regions ON (old_regions.dataset_id = crucible_dataset.id)
      ),");
    }

    // If `distinct_sleds` is selected, then take note of the sleds used by
    // existing allocations, and filter those out later. This step is required
    // when taking an existing allocation of regions and increasing the
    // redundancy in order to _not_ allocate to sleds already used.

    if distinct_sleds {
        builder.sql(
            "
        existing_sleds AS (
          SELECT
            zpool.sled_id as id
          FROM
            zpool
          WHERE
            zpool.id = ANY(SELECT pool_id FROM existing_zpools)
        ),",
        );
    }

    // Identifies zpools with enough space for region allocation, that are not
    // currently used by this Volume's existing regions.
    //
    // NOTE: 'distinct_sleds' changes the format of the underlying SQL query, as it uses
    // distinct bind parameters depending on the conditional branch.
    builder.sql(
        "
  candidate_zpools AS (",
    );
    if distinct_sleds {
        builder.sql("SELECT DISTINCT ON (zpool.sled_id) ")
    } else {
        builder.sql("SELECT ")
    };
    builder.sql("
        old_zpool_usage.pool_id
    FROM
        old_zpool_usage
        INNER JOIN
        (zpool INNER JOIN sled ON (zpool.sled_id = sled.id)) ON (zpool.id = old_zpool_usage.pool_id)
        INNER JOIN
        physical_disk ON (zpool.physical_disk_id = physical_disk.id)
        INNER JOIN
        crucible_dataset ON (crucible_dataset.pool_id = zpool.id)
    WHERE (
      (old_zpool_usage.size_used + ").param().sql(" + zpool.control_plane_storage_buffer) <=
         (SELECT total_size FROM omicron.public.inv_zpool WHERE
          inv_zpool.id = old_zpool_usage.pool_id
          ORDER BY inv_zpool.time_collected DESC LIMIT 1)
      AND sled.sled_policy = 'in_service'
      AND sled.sled_state = 'active'
      AND physical_disk.disk_policy = 'in_service'
      AND physical_disk.disk_state = 'active'
      AND NOT(zpool.id = ANY(SELECT existing_zpools.pool_id FROM existing_zpools))
      AND (crucible_dataset.time_deleted is NULL)
      AND (crucible_dataset.no_provision = false)
    "
    ).bind::<sql_types::BigInt, _>(size_delta);

    if distinct_sleds {
        builder
            .sql("AND NOT(sled.id = ANY(SELECT existing_sleds.id FROM existing_sleds)))
            ORDER BY zpool.sled_id, md5((CAST(zpool.id as BYTEA) || ")
            .param()
            .sql("))")
            .bind::<sql_types::Bytea, _>(seed.clone())
    } else {
        builder.sql(")")
    }
    .sql("),");

    // Find datasets which could be used for provisioning regions.
    //
    // We select only one dataset from each zpool.
    builder.sql("
  candidate_datasets AS (
    SELECT DISTINCT ON (crucible_dataset.pool_id)
      crucible_dataset.id,
      crucible_dataset.pool_id
    FROM (crucible_dataset INNER JOIN candidate_zpools ON (crucible_dataset.pool_id = candidate_zpools.pool_id))
    WHERE (crucible_dataset.time_deleted IS NULL) AND (crucible_dataset.no_provision = false)
    ORDER BY crucible_dataset.pool_id, md5((CAST(crucible_dataset.id as BYTEA) || ").param().sql("))
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
      ").param().sql(" AS extent_count,
      NULL AS port,
      ").param().sql(" AS read_only,
      FALSE as deleting,
      ").param().sql(" AS reservation_percent
    FROM shuffled_candidate_datasets")
  // Only select the *additional* number of candidate regions for the required
  // redundancy level
  .sql("
    LIMIT (").param().sql(" - (
      SELECT COUNT(*) FROM old_regions
    ))
  ),")
    .bind::<sql_types::Uuid, _>(*volume_id.as_untyped_uuid())
    .bind::<sql_types::BigInt, _>(params.block_size as i64)
    .bind::<sql_types::BigInt, _>(params.blocks_per_extent as i64)
    .bind::<sql_types::BigInt, _>(params.extent_count as i64)
    .bind::<sql_types::Bool, _>(params.read_only)
    .bind::<RegionReservationPercentEnum, _>(reservation_percent)
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
      crucible_dataset.pool_id AS pool_id,
      ").param().sql(" AS size_used_delta
    FROM (candidate_regions INNER JOIN crucible_dataset ON (crucible_dataset.id = candidate_regions.dataset_id))
  ),")
    .bind::<sql_types::BigInt, _>(size_delta)

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
                 crucible_dataset.pool_id
               FROM
                 candidate_regions
                   INNER JOIN crucible_dataset ON (candidate_regions.dataset_id = crucible_dataset.id)
              )
              UNION
              (
               SELECT
                 crucible_dataset.pool_id
               FROM
                 old_regions
                   INNER JOIN crucible_dataset ON (old_regions.dataset_id = crucible_dataset.id)
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
      (id, time_created, time_modified, dataset_id, volume_id, block_size, blocks_per_extent, extent_count, port, read_only, deleting, reservation_percent)
    SELECT ").sql(AllColumnsOfRegion::with_prefix("candidate_regions")).sql("
    FROM candidate_regions
    WHERE
      (SELECT do_insert.insert FROM do_insert LIMIT 1)
    RETURNING ").sql(AllColumnsOfRegion::with_prefix("region")).sql("
  ),
  updated_datasets AS (
    UPDATE crucible_dataset SET
      size_used = (crucible_dataset.size_used + (SELECT proposed_dataset_changes.size_used_delta FROM proposed_dataset_changes WHERE (proposed_dataset_changes.id = crucible_dataset.id) LIMIT 1))
    WHERE (
      (crucible_dataset.id = ANY(SELECT proposed_dataset_changes.id FROM proposed_dataset_changes)) AND
      (SELECT do_insert.insert FROM do_insert LIMIT 1))
    RETURNING ").sql(AllColumnsOfCrucibleDataset::with_prefix("crucible_dataset")).sql("
  )
(
  SELECT ")
    .sql(AllColumnsOfCrucibleDataset::with_prefix("crucible_dataset"))
    .sql(", ")
    .sql(AllColumnsOfRegion::with_prefix("old_regions")).sql("
  FROM
    (old_regions INNER JOIN crucible_dataset ON (old_regions.dataset_id = crucible_dataset.id))
)
UNION
(
  SELECT ")
    .sql(AllColumnsOfCrucibleDataset::with_prefix("updated_datasets"))
    .sql(", ")
    .sql(AllColumnsOfRegion::with_prefix("inserted_regions")).sql("
  FROM (inserted_regions INNER JOIN updated_datasets ON (inserted_regions.dataset_id = updated_datasets.id))
)"
    );

    Ok(builder.query())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::context::OpContext;
    use crate::db;
    use crate::db::datastore::DataStore;
    use crate::db::datastore::REGION_REDUNDANCY_THRESHOLD;
    use crate::db::datastore::RegionAllocationFor;
    use crate::db::datastore::RegionAllocationParameters;
    use crate::db::explain::ExplainableAsync;
    use crate::db::model::ByteCount;
    use crate::db::model::Generation;
    use crate::db::model::to_db_typed_uuid;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use chrono::Utc;
    use illumos_utils::zpool::ZpoolHealth;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::BlueprintUuid;
    use omicron_uuid_kinds::CollectionUuid;
    use omicron_uuid_kinds::DatasetUuid;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::SledUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use std::net::SocketAddrV6;
    use uuid::Uuid;

    // This test is a bit of a "change detector", but it's here to help with
    // debugging too. If you change this query, it can be useful to see exactly
    // how the output SQL has been altered.
    #[tokio::test]
    async fn expectorate_query() {
        let volume_id = VolumeUuid::nil();
        let params = RegionParameters {
            block_size: 512,
            blocks_per_extent: 4,
            extent_count: 8,
            read_only: false,
        };

        // Start with snapshot_id = None

        let snapshot_id = None;

        // First structure: "RandomWithDistinctSleds"

        let region_allocate = allocation_query(
            volume_id,
            snapshot_id,
            params,
            &RegionAllocationStrategy::RandomWithDistinctSleds {
                seed: Some(1),
            },
            REGION_REDUNDANCY_THRESHOLD,
        )
        .unwrap();

        expectorate_query_contents(
            &region_allocate,
            "tests/output/region_allocate_distinct_sleds.sql",
        )
        .await;

        // Second structure: "Random"

        let region_allocate = allocation_query(
            volume_id,
            snapshot_id,
            params,
            &RegionAllocationStrategy::Random { seed: Some(1) },
            REGION_REDUNDANCY_THRESHOLD,
        )
        .unwrap();
        expectorate_query_contents(
            &region_allocate,
            "tests/output/region_allocate_random_sleds.sql",
        )
        .await;

        // Next, put a value in for snapshot_id

        let snapshot_id = Some(Uuid::new_v4());

        // First structure: "RandomWithDistinctSleds"

        let region_allocate = allocation_query(
            volume_id,
            snapshot_id,
            params,
            &RegionAllocationStrategy::RandomWithDistinctSleds {
                seed: Some(1),
            },
            REGION_REDUNDANCY_THRESHOLD,
        )
        .unwrap();
        expectorate_query_contents(
            &region_allocate,
            "tests/output/region_allocate_with_snapshot_distinct_sleds.sql",
        )
        .await;

        // Second structure: "Random"

        let region_allocate = allocation_query(
            volume_id,
            snapshot_id,
            params,
            &RegionAllocationStrategy::Random { seed: Some(1) },
            REGION_REDUNDANCY_THRESHOLD,
        )
        .unwrap();
        expectorate_query_contents(
            &region_allocate,
            "tests/output/region_allocate_with_snapshot_random_sleds.sql",
        )
        .await;
    }

    // Explain the possible forms of the SQL query to ensure that it
    // creates a valid SQL string.
    #[tokio::test]
    async fn explainable() {
        let logctx = dev::test_setup_log("explainable");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let volume_id = VolumeUuid::new_v4();
        let params = RegionParameters {
            block_size: 512,
            blocks_per_extent: 4,
            extent_count: 8,
            read_only: false,
        };

        // First structure: Explain the query with "RandomWithDistinctSleds"

        let region_allocate = allocation_query(
            volume_id,
            None,
            params,
            &RegionAllocationStrategy::RandomWithDistinctSleds { seed: None },
            REGION_REDUNDANCY_THRESHOLD,
        )
        .unwrap();
        let _ = region_allocate
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        // Second structure: Explain the query with "Random"

        let region_allocate = allocation_query(
            volume_id,
            None,
            params,
            &RegionAllocationStrategy::Random { seed: None },
            REGION_REDUNDANCY_THRESHOLD,
        )
        .unwrap();
        let _ = region_allocate
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[test]
    fn allocation_query_region_size_overflow() {
        let volume_id = VolumeUuid::nil();
        let snapshot_id = None;

        let params = RegionParameters {
            block_size: 512,
            blocks_per_extent: 4294967296,
            extent_count: 8388609, // should cause an overflow!
            read_only: false,
        };

        let Err(e) = allocation_query(
            volume_id,
            snapshot_id,
            params,
            &RegionAllocationStrategy::RandomWithDistinctSleds {
                seed: Some(1),
            },
            REGION_REDUNDANCY_THRESHOLD,
        ) else {
            panic!("expected error");
        };

        assert!(matches!(e, AllocationQueryError::RegionSizeOverflow));
    }

    #[test]
    fn allocation_query_region_size_too_large() {
        let volume_id = VolumeUuid::nil();
        let snapshot_id = None;

        let params = RegionParameters {
            block_size: 512,
            blocks_per_extent: 8388608, // 2^32 / 512
            extent_count: 256,          // 255 would be ok, 256 is too large
            read_only: false,
        };

        let Err(e) = allocation_query(
            volume_id,
            snapshot_id,
            params,
            &RegionAllocationStrategy::RandomWithDistinctSleds {
                seed: Some(1),
            },
            REGION_REDUNDANCY_THRESHOLD,
        ) else {
            panic!("expected error!");
        };

        assert!(matches!(
            e,
            AllocationQueryError::RequestedRegionOverMaxSize {
                request: 1099511627776u64,
                maximum: MAX_REGION_SIZE_BYTES,
            }
        ));
    }

    struct RegionAllocationTest {
        sleds: Vec<RegionAllocationTestSled>,
    }

    struct RegionAllocationTestSled {
        sled_id: SledUuid,
        sled_serial: String,
        u2s: Vec<RegionAllocationTestSledU2>,
    }

    struct RegionAllocationTestSledU2 {
        physical_disk_id: PhysicalDiskUuid,
        physical_disk_serial: String,

        zpool_id: ZpoolUuid,
        control_plane_storage_buffer: external::ByteCount,

        inventory_total_size: external::ByteCount,

        crucible_dataset_id: DatasetUuid,
        crucible_dataset_addr: SocketAddrV6,

        local_storage_dataset:
            Option<RegionAllocationTestSledLocalStorageDataset>,
    }

    struct RegionAllocationTestSledLocalStorageDataset {
        id: DatasetUuid,
        size_used: i64,
    }

    /// Configuration for a U.2 disk, produced by U2Builder.
    struct U2BuilderConfig {
        control_plane_storage_buffer_gib: u32,
        inventory_total_size_gib: u32,
        /// None = no local storage dataset, Some(n) = local storage with n GiB used
        local_storage_size_used_gib: Option<u32>,
    }

    /// Builder for configuring a single U.2 disk.
    ///
    /// Defaults:
    /// - control_plane_storage_buffer: 250 GiB
    /// - inventory_total_size: 1024 GiB
    /// - local storage: None (no local storage dataset)
    struct U2Builder {
        control_plane_storage_buffer_gib: u32,
        inventory_total_size_gib: u32,
        local_storage_size_used_gib: Option<u32>,
    }

    impl U2Builder {
        fn new() -> Self {
            Self {
                control_plane_storage_buffer_gib: 250,
                inventory_total_size_gib: 1024,
                local_storage_size_used_gib: None,
            }
        }

        fn control_plane_storage_buffer_gib(mut self, gib: u32) -> Self {
            self.control_plane_storage_buffer_gib = gib;
            self
        }

        fn inventory_total_size_gib(mut self, gib: u32) -> Self {
            self.inventory_total_size_gib = gib;
            self
        }

        /// Configure this U.2 to have a local storage dataset with the
        /// specified amount used.
        fn local_storage_size_used_gib(mut self, gib: u32) -> Self {
            self.local_storage_size_used_gib = Some(gib);
            self
        }

        fn build(self) -> U2BuilderConfig {
            U2BuilderConfig {
                control_plane_storage_buffer_gib: self
                    .control_plane_storage_buffer_gib,
                inventory_total_size_gib: self.inventory_total_size_gib,
                local_storage_size_used_gib: self.local_storage_size_used_gib,
            }
        }
    }

    /// Builder for configuring a single sled with its U.2 disks.
    struct SledBuilder {
        u2s: Vec<U2BuilderConfig>,
    }

    impl SledBuilder {
        fn new() -> Self {
            Self { u2s: Vec::new() }
        }

        /// Add a U.2 disk to this sled.
        fn add_u2(mut self, u2: U2Builder) -> Self {
            self.u2s.push(u2.build());
            self
        }
    }

    /// Builder for constructing `RegionAllocationTest` configurations.
    ///
    /// Auto-generates:
    /// - Sled IDs, serials, disk IDs, dataset IDs
    /// - Crucible dataset addresses based on sled/disk indices
    struct RegionAllocationTestBuilder {
        sleds: Vec<SledBuilder>,
    }

    impl RegionAllocationTestBuilder {
        fn new() -> Self {
            Self { sleds: Vec::new() }
        }

        /// Add a sled with its U.2 configuration.
        fn add_sled(mut self, sled: SledBuilder) -> Self {
            self.sleds.push(sled);
            self
        }

        fn build(self) -> RegionAllocationTest {
            let sleds = self
                .sleds
                .into_iter()
                .enumerate()
                .map(|(sled_idx, sled_builder)| {
                    let u2s = sled_builder
                        .u2s
                        .into_iter()
                        .enumerate()
                        .map(|(u2_idx, u2_config)| {
                            // Generate address based on sled/disk indices:
                            // [fd00:1122:3344:{sled+1}{disk+1}1::1]:12345
                            let addr_str = format!(
                                "[fd00:1122:3344:{}{:02}1::1]:12345",
                                sled_idx + 1,
                                u2_idx + 1
                            );

                            let local_storage_dataset =
                                u2_config.local_storage_size_used_gib.map(
                                    |gib| {
                                        RegionAllocationTestSledLocalStorageDataset {
                                        id: DatasetUuid::new_v4(),
                                        size_used: i64::from(
                                            external::ByteCount::from_gibibytes_u32(
                                                gib,
                                            ),
                                        ),
                                    }
                                    },
                                );

                            RegionAllocationTestSledU2 {
                                physical_disk_id: PhysicalDiskUuid::new_v4(),
                                physical_disk_serial: format!(
                                    "phys{}_{}",
                                    sled_idx, u2_idx
                                ),
                                zpool_id: ZpoolUuid::new_v4(),
                                control_plane_storage_buffer:
                                    external::ByteCount::from_gibibytes_u32(
                                        u2_config.control_plane_storage_buffer_gib,
                                    ),
                                inventory_total_size:
                                    external::ByteCount::from_gibibytes_u32(
                                        u2_config.inventory_total_size_gib,
                                    ),
                                crucible_dataset_id: DatasetUuid::new_v4(),
                                crucible_dataset_addr: addr_str.parse().unwrap(),
                                local_storage_dataset,
                            }
                        })
                        .collect();

                    RegionAllocationTestSled {
                        sled_id: SledUuid::new_v4(),
                        sled_serial: format!("sled_{}", sled_idx),
                        u2s,
                    }
                })
                .collect();

            RegionAllocationTest { sleds }
        }
    }

    const GIB: u64 = 1024 * 1024 * 1024;

    /// Calculate the raw disk size in bytes from region parameters.
    /// This is the size WITHOUT the reservation overhead.
    fn disk_size_from_region_params(
        block_size: u64,
        blocks_per_extent: u64,
        extent_count: u64,
    ) -> u64 {
        block_size * blocks_per_extent * extent_count
    }

    /// Calculate the disk size WITH the reservation overhead.
    /// This mirrors the production calculation in `Region::reserved_size()`.
    ///
    /// Note: This function explicitly matches on `RegionReservationPercent`
    /// to ensure tests fail to compile if new variants are added.
    fn disk_size_with_reservation(disk_size_without_reservation: u64) -> u64 {
        // Use the same reservation percent as allocation_query()
        let reservation_percent = RegionReservationPercent::TwentyFive;

        let overhead = match reservation_percent {
            RegionReservationPercent::TwentyFive => {
                disk_size_without_reservation / 4
            } // If new variants are added, this match will fail to compile,
              // forcing the test author to update this function.
        };

        disk_size_without_reservation + overhead
    }

    /// Calculate available space on a zpool given its configuration.
    /// All parameters are in GiB for clarity.
    fn available_space_gib(
        inventory_total_size_gib: u32,
        control_plane_storage_buffer_gib: u32,
        local_storage_used_gib: u32,
    ) -> u64 {
        let available_gib = inventory_total_size_gib
            - control_plane_storage_buffer_gib
            - local_storage_used_gib;
        u64::from(available_gib) * GIB
    }

    async fn setup_region_allocation_test(
        opctx: &OpContext,
        datastore: &DataStore,
        config: &RegionAllocationTest,
    ) {
        for sled_config in &config.sleds {
            let sled = db::model::SledUpdate::new(
                sled_config.sled_id,
                "[::1]:0".parse().unwrap(),
                0,
                db::model::SledBaseboard {
                    serial_number: sled_config.sled_serial.clone(),
                    part_number: "test-pn".to_string(),
                    revision: 0,
                },
                db::model::SledSystemHardware {
                    is_scrimlet: false,
                    usable_hardware_threads: 128,
                    usable_physical_ram: (64 << 30).try_into().unwrap(),
                    reservoir_size: (16 << 30).try_into().unwrap(),
                    cpu_family: db::model::SledCpuFamily::AmdMilan,
                },
                Uuid::new_v4(),
                Generation::new(),
            );

            datastore.sled_upsert(sled).await.expect("failed to upsert sled");

            for u2 in &sled_config.u2s {
                let physical_disk = db::model::PhysicalDisk::new(
                    u2.physical_disk_id,
                    String::from("vendor"),
                    u2.physical_disk_serial.clone(),
                    String::from("model"),
                    db::model::PhysicalDiskKind::U2,
                    sled_config.sled_id,
                );

                datastore
                    .physical_disk_insert(opctx, physical_disk)
                    .await
                    .unwrap();

                let zpool = db::model::Zpool::new(
                    u2.zpool_id,
                    sled_config.sled_id,
                    u2.physical_disk_id,
                    u2.control_plane_storage_buffer.into(),
                );

                datastore
                    .zpool_insert(opctx, zpool)
                    .await
                    .expect("failed to upsert zpool");

                add_inventory_row_for_zpool(
                    datastore,
                    u2.zpool_id,
                    sled_config.sled_id,
                    u2.inventory_total_size.into(),
                    ZpoolHealth::Online,
                )
                .await;

                add_crucible_dataset(
                    datastore,
                    u2.crucible_dataset_id,
                    u2.zpool_id,
                    u2.crucible_dataset_addr,
                )
                .await;

                if let Some(local_storage_dataset) = &u2.local_storage_dataset {
                    add_local_storage_dataset(
                        opctx,
                        datastore,
                        local_storage_dataset.id,
                        u2.zpool_id,
                        local_storage_dataset.size_used,
                    )
                    .await;
                }
            }
        }
    }

    async fn add_inventory_row_for_zpool(
        datastore: &DataStore,
        zpool_id: ZpoolUuid,
        sled_id: SledUuid,
        total_size: ByteCount,
        health: ZpoolHealth,
    ) {
        use nexus_db_schema::schema::inv_zpool::dsl;

        let inv_collection_id = CollectionUuid::new_v4();
        let time_collected = Utc::now();
        let inv_pool = nexus_db_model::InvZpool {
            inv_collection_id: inv_collection_id.into(),
            time_collected,
            id: zpool_id.into(),
            sled_id: to_db_typed_uuid(sled_id),
            total_size,
            health: health.into(),
        };

        diesel::insert_into(dsl::inv_zpool)
            .values(inv_pool)
            .execute_async(
                &*datastore.pool_connection_for_tests().await.unwrap(),
            )
            .await
            .unwrap();
    }

    async fn add_crucible_dataset(
        datastore: &DataStore,
        dataset_id: DatasetUuid,
        pool_id: ZpoolUuid,
        addr: SocketAddrV6,
    ) -> DatasetUuid {
        let dataset =
            db::model::CrucibleDataset::new(dataset_id, pool_id, addr);

        datastore.crucible_dataset_upsert(dataset).await.unwrap();

        dataset_id
    }

    async fn add_local_storage_dataset(
        opctx: &OpContext,
        datastore: &DataStore,
        dataset_id: DatasetUuid,
        pool_id: ZpoolUuid,
        size_used: i64,
    ) -> DatasetUuid {
        let mut dataset = db::model::RendezvousLocalStorageDataset::new(
            dataset_id,
            pool_id,
            BlueprintUuid::new_v4(),
        );

        dataset.size_used = size_used;

        datastore
            .local_storage_dataset_insert_if_not_exists(opctx, dataset)
            .await
            .unwrap();

        dataset_id
    }

    #[tokio::test]
    async fn region_allocation_normal() {
        let logctx = dev::test_setup_log("region_allocation_normal");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // U2 configuration
        let inventory_total_size_gib: u32 = 1024;
        let control_plane_storage_buffer_gib: u32 = 250;
        let local_storage_used_gib: u32 = 0;

        let config = RegionAllocationTestBuilder::new()
            .add_sled(
                SledBuilder::new().add_u2(
                    U2Builder::new()
                        .control_plane_storage_buffer_gib(
                            control_plane_storage_buffer_gib,
                        )
                        .inventory_total_size_gib(inventory_total_size_gib)
                        .local_storage_size_used_gib(local_storage_used_gib),
                ),
            )
            .add_sled(
                SledBuilder::new().add_u2(
                    U2Builder::new()
                        .control_plane_storage_buffer_gib(
                            control_plane_storage_buffer_gib,
                        )
                        .inventory_total_size_gib(inventory_total_size_gib)
                        .local_storage_size_used_gib(local_storage_used_gib),
                ),
            )
            .add_sled(
                SledBuilder::new().add_u2(
                    U2Builder::new()
                        .control_plane_storage_buffer_gib(
                            control_plane_storage_buffer_gib,
                        )
                        .inventory_total_size_gib(inventory_total_size_gib)
                        .local_storage_size_used_gib(local_storage_used_gib),
                ),
            )
            .build();

        setup_region_allocation_test(&opctx, datastore, &config).await;

        // Region parameters
        let block_size: u64 = 512;
        let blocks_per_extent: u64 = 131072;
        let extent_count: u64 = 7680;

        // Calculate sizes
        let disk_size_without_reservation = disk_size_from_region_params(
            block_size,
            blocks_per_extent,
            extent_count,
        );
        let reserved_size =
            disk_size_with_reservation(disk_size_without_reservation);
        let available_space = available_space_gib(
            inventory_total_size_gib,
            control_plane_storage_buffer_gib,
            local_storage_used_gib,
        );

        // Verify test setup: this test expects allocation to SUCCEED
        assert!(
            reserved_size <= available_space,
            "Test setup error: reserved_size ({} GiB) should fit in available_space ({} GiB)",
            reserved_size / GIB,
            available_space / GIB,
        );

        datastore
            .arbitrary_region_allocate(
                opctx,
                RegionAllocationFor::DiskVolume {
                    volume_id: VolumeUuid::new_v4(),
                },
                RegionAllocationParameters::FromRaw {
                    block_size,
                    blocks_per_extent,
                    extent_count,
                },
                &RegionAllocationStrategy::RandomWithDistinctSleds {
                    seed: None,
                },
                3, // num_regions_required
            )
            .await
            .expect("allocation should succeed: reserved size fits in available space");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Ensure region allocations work even without a local storage dataset row
    #[tokio::test]
    async fn region_allocation_normal_no_local_storage_dataset() {
        let logctx = dev::test_setup_log(
            "region_allocation_normal_no_local_storage_dataset",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // U2 configuration
        let inventory_total_size_gib: u32 = 1024;
        let control_plane_storage_buffer_gib: u32 = 250;

        let config = RegionAllocationTestBuilder::new()
            .add_sled(
                SledBuilder::new().add_u2(
                    U2Builder::new()
                        .control_plane_storage_buffer_gib(
                            control_plane_storage_buffer_gib,
                        )
                        .inventory_total_size_gib(inventory_total_size_gib),
                ),
            )
            .add_sled(
                SledBuilder::new().add_u2(
                    U2Builder::new()
                        .control_plane_storage_buffer_gib(
                            control_plane_storage_buffer_gib,
                        )
                        .inventory_total_size_gib(inventory_total_size_gib),
                ),
            )
            .add_sled(
                SledBuilder::new().add_u2(
                    U2Builder::new()
                        .control_plane_storage_buffer_gib(
                            control_plane_storage_buffer_gib,
                        )
                        .inventory_total_size_gib(inventory_total_size_gib),
                ),
            )
            .build();

        setup_region_allocation_test(&opctx, datastore, &config).await;

        // Region parameters
        let block_size: u64 = 512;
        let blocks_per_extent: u64 = 131072;
        let extent_count: u64 = 7680;

        // Calculate sizes (no local storage, so 0 for that parameter)
        let disk_size_without_reservation = disk_size_from_region_params(
            block_size,
            blocks_per_extent,
            extent_count,
        );
        let reserved_size =
            disk_size_with_reservation(disk_size_without_reservation);
        let available_space = available_space_gib(
            inventory_total_size_gib,
            control_plane_storage_buffer_gib,
            0, // no local storage dataset
        );

        // Verify test setup: this test expects allocation to SUCCEED
        assert!(
            reserved_size <= available_space,
            "Test setup error: reserved_size ({} GiB) should fit in available_space ({} GiB)",
            reserved_size / GIB,
            available_space / GIB,
        );

        datastore
            .arbitrary_region_allocate(
                opctx,
                RegionAllocationFor::DiskVolume {
                    volume_id: VolumeUuid::new_v4(),
                },
                RegionAllocationParameters::FromRaw {
                    block_size,
                    blocks_per_extent,
                    extent_count,
                },
                &RegionAllocationStrategy::RandomWithDistinctSleds {
                    seed: None,
                },
                3, // num_regions_required
            )
            .await
            .expect(
                "allocation should succeed without local storage dataset row",
            );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Ensure the control plane storage buffer is respected.
    /// This test also verifies that the reservation overhead is applied,
    /// because the raw disk size would fit, but the reserved size does not.
    #[tokio::test]
    async fn region_allocation_fail_control_plane_storage_buffer() {
        let logctx = dev::test_setup_log(
            "region_allocation_fail_control_plane_storage_buffer",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // U2 configuration
        let inventory_total_size_gib: u32 = 1024;
        let control_plane_storage_buffer_gib: u32 = 250;
        let local_storage_used_gib: u32 = 0;

        let config = RegionAllocationTestBuilder::new()
            .add_sled(
                SledBuilder::new().add_u2(
                    U2Builder::new()
                        .control_plane_storage_buffer_gib(
                            control_plane_storage_buffer_gib,
                        )
                        .inventory_total_size_gib(inventory_total_size_gib)
                        .local_storage_size_used_gib(local_storage_used_gib),
                ),
            )
            .add_sled(
                SledBuilder::new().add_u2(
                    U2Builder::new()
                        .control_plane_storage_buffer_gib(
                            control_plane_storage_buffer_gib,
                        )
                        .inventory_total_size_gib(inventory_total_size_gib)
                        .local_storage_size_used_gib(local_storage_used_gib),
                ),
            )
            .add_sled(
                SledBuilder::new().add_u2(
                    U2Builder::new()
                        .control_plane_storage_buffer_gib(
                            control_plane_storage_buffer_gib,
                        )
                        .inventory_total_size_gib(inventory_total_size_gib)
                        .local_storage_size_used_gib(local_storage_used_gib),
                ),
            )
            .build();

        setup_region_allocation_test(&opctx, datastore, &config).await;

        // Region parameters
        let block_size: u64 = 512;
        let blocks_per_extent: u64 = 131072;
        let extent_count: u64 = 10240;

        // Calculate sizes
        let disk_size_without_reservation = disk_size_from_region_params(
            block_size,
            blocks_per_extent,
            extent_count,
        );
        let reserved_size =
            disk_size_with_reservation(disk_size_without_reservation);
        let available_space = available_space_gib(
            inventory_total_size_gib,
            control_plane_storage_buffer_gib,
            local_storage_used_gib,
        );

        // Verify test setup: disk without reservation fits, but with
        // reservation does NOT
        assert!(
            disk_size_without_reservation <= available_space,
            "Test setup error: disk_size_without_reservation ({} GiB) should fit \
             (this tests that reservation overhead matters)",
            disk_size_without_reservation / GIB,
        );
        assert!(
            reserved_size > available_space,
            "Test setup error: reserved_size ({} GiB) should NOT fit in \
             available_space ({} GiB)",
            reserved_size / GIB,
            available_space / GIB,
        );

        let err = datastore
            .arbitrary_region_allocate(
                opctx,
                RegionAllocationFor::DiskVolume {
                    volume_id: VolumeUuid::new_v4(),
                },
                RegionAllocationParameters::FromRaw {
                    block_size,
                    blocks_per_extent,
                    extent_count,
                },
                &RegionAllocationStrategy::RandomWithDistinctSleds {
                    seed: None,
                },
                3, // num_regions_required
            )
            .await
            .expect_err(
                "allocation should fail: reserved size exceeds available space",
            );

        assert!(
            matches!(
                &err,
                external::Error::InsufficientCapacity { message }
                    if message.external_message() == "Not enough storage"
            ),
            "expected InsufficientCapacity with 'Not enough storage', got: {err:?}"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Ensure the size_used consumed by local storage is respected
    #[tokio::test]
    async fn region_allocation_fail_local_storage_dataset() {
        let logctx =
            dev::test_setup_log("region_allocation_fail_local_storage_dataset");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // U2 configuration
        let inventory_total_size_gib: u32 = 1024;
        let control_plane_storage_buffer_gib: u32 = 250;
        // Sled 0 has local storage consuming 500 GiB, sleds 1 and 2 have 0
        let local_storage_used_gib_sled0: u32 = 500;
        let local_storage_used_gib_other: u32 = 0;

        let config = RegionAllocationTestBuilder::new()
            .add_sled(
                SledBuilder::new().add_u2(
                    U2Builder::new()
                        .control_plane_storage_buffer_gib(
                            control_plane_storage_buffer_gib,
                        )
                        .inventory_total_size_gib(inventory_total_size_gib)
                        .local_storage_size_used_gib(
                            local_storage_used_gib_sled0,
                        ),
                ),
            )
            .add_sled(
                SledBuilder::new().add_u2(
                    U2Builder::new()
                        .control_plane_storage_buffer_gib(
                            control_plane_storage_buffer_gib,
                        )
                        .inventory_total_size_gib(inventory_total_size_gib)
                        .local_storage_size_used_gib(
                            local_storage_used_gib_other,
                        ),
                ),
            )
            .add_sled(
                SledBuilder::new().add_u2(
                    U2Builder::new()
                        .control_plane_storage_buffer_gib(
                            control_plane_storage_buffer_gib,
                        )
                        .inventory_total_size_gib(inventory_total_size_gib)
                        .local_storage_size_used_gib(
                            local_storage_used_gib_other,
                        ),
                ),
            )
            .build();

        setup_region_allocation_test(&opctx, datastore, &config).await;

        // Region parameters
        let block_size: u64 = 512;
        let blocks_per_extent: u64 = 131072;
        let extent_count: u64 = 4800;

        // Calculate sizes
        let disk_size_without_reservation = disk_size_from_region_params(
            block_size,
            blocks_per_extent,
            extent_count,
        );
        let reserved_size =
            disk_size_with_reservation(disk_size_without_reservation);
        let available_space_sled0 = available_space_gib(
            inventory_total_size_gib,
            control_plane_storage_buffer_gib,
            local_storage_used_gib_sled0,
        );
        let available_space_other = available_space_gib(
            inventory_total_size_gib,
            control_plane_storage_buffer_gib,
            local_storage_used_gib_other,
        );

        // Verify test setup: reserved_size does NOT fit on sled 0 but DOES fit
        // on sleds 1 and 2
        assert!(
            reserved_size > available_space_sled0,
            "Test setup error: reserved_size ({} GiB) should NOT fit on sled 0 \
             (available: {} GiB)",
            reserved_size / GIB,
            available_space_sled0 / GIB,
        );
        assert!(
            reserved_size <= available_space_other,
            "Test setup error: reserved_size ({} GiB) should fit on sleds 1 and 2 \
             (available: {} GiB)",
            reserved_size / GIB,
            available_space_other / GIB,
        );

        // Allocation with 3 regions should FAIL (only 2 sleds have enough space)
        let err = datastore
            .arbitrary_region_allocate(
                opctx,
                RegionAllocationFor::DiskVolume {
                    volume_id: VolumeUuid::new_v4(),
                },
                RegionAllocationParameters::FromRaw {
                    block_size,
                    blocks_per_extent,
                    extent_count,
                },
                &RegionAllocationStrategy::RandomWithDistinctSleds {
                    seed: None,
                },
                3, // num_regions_required
            )
            .await
            .expect_err(
                "allocation should fail: only 2 sleds have enough space for 3 regions",
            );

        assert!(
            matches!(
                &err,
                external::Error::InsufficientCapacity { message }
                    if message.external_message() == "Not enough storage"
            ),
            "expected InsufficientCapacity with 'Not enough storage', got: {err:?}"
        );

        // Allocation with 2 regions should SUCCEED
        datastore
            .arbitrary_region_allocate(
                opctx,
                RegionAllocationFor::DiskVolume {
                    volume_id: VolumeUuid::new_v4(),
                },
                RegionAllocationParameters::FromRaw {
                    block_size,
                    blocks_per_extent,
                    extent_count,
                },
                &RegionAllocationStrategy::RandomWithDistinctSleds {
                    seed: None,
                },
                2, // num_regions_required
            )
            .await
            .expect("allocation should succeed: 2 sleds have enough space");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Ensure the size_used consumed by local storage is respected EXACTLY.
    /// This test verifies boundary conditions: N extents fit, but N+1 don't.
    #[tokio::test]
    async fn region_allocation_barely_pass_local_storage_dataset() {
        let logctx = dev::test_setup_log(
            "region_allocation_barely_pass_local_storage_dataset",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // U2 configuration
        let inventory_total_size_gib: u32 = 1024;
        let control_plane_storage_buffer_gib: u32 = 250;
        // Sled 0 has 500 GiB local storage, making it the bottleneck
        let local_storage_used_gib_sled0: u32 = 500;
        let local_storage_used_gib_other: u32 = 0;

        let config = RegionAllocationTestBuilder::new()
            .add_sled(
                SledBuilder::new().add_u2(
                    U2Builder::new()
                        .control_plane_storage_buffer_gib(
                            control_plane_storage_buffer_gib,
                        )
                        .inventory_total_size_gib(inventory_total_size_gib)
                        .local_storage_size_used_gib(
                            local_storage_used_gib_sled0,
                        ),
                ),
            )
            .add_sled(
                SledBuilder::new().add_u2(
                    U2Builder::new()
                        .control_plane_storage_buffer_gib(
                            control_plane_storage_buffer_gib,
                        )
                        .inventory_total_size_gib(inventory_total_size_gib)
                        .local_storage_size_used_gib(
                            local_storage_used_gib_other,
                        ),
                ),
            )
            .add_sled(
                SledBuilder::new().add_u2(
                    U2Builder::new()
                        .control_plane_storage_buffer_gib(
                            control_plane_storage_buffer_gib,
                        )
                        .inventory_total_size_gib(inventory_total_size_gib)
                        .local_storage_size_used_gib(
                            local_storage_used_gib_other,
                        ),
                ),
            )
            .build();

        setup_region_allocation_test(&opctx, datastore, &config).await;

        // Region parameters
        let block_size: u64 = 512;
        let blocks_per_extent: u64 = 131072;
        // Test boundary: 3507 extents should fit, 3508 should not
        let extent_count_fits: u64 = 3507;
        let extent_count_fails: u64 = 3508;

        // Calculate sizes for sled 0 (the bottleneck)
        let available_space_sled0 = available_space_gib(
            inventory_total_size_gib,
            control_plane_storage_buffer_gib,
            local_storage_used_gib_sled0,
        );

        let reserved_size_fits =
            disk_size_with_reservation(disk_size_from_region_params(
                block_size,
                blocks_per_extent,
                extent_count_fits,
            ));
        let reserved_size_fails =
            disk_size_with_reservation(disk_size_from_region_params(
                block_size,
                blocks_per_extent,
                extent_count_fails,
            ));

        // Verify test setup: exactly at the boundary
        assert!(
            reserved_size_fits <= available_space_sled0,
            "Test setup error: extent_count={} should fit (reserved: {} bytes, \
             available: {} bytes)",
            extent_count_fits,
            reserved_size_fits,
            available_space_sled0,
        );
        assert!(
            reserved_size_fails > available_space_sled0,
            "Test setup error: extent_count={} should NOT fit (reserved: {} bytes, \
             available: {} bytes)",
            extent_count_fails,
            reserved_size_fails,
            available_space_sled0,
        );

        // Allocation with extent_count_fits should SUCCEED
        let datasets_and_regions = datastore
            .arbitrary_region_allocate(
                opctx,
                RegionAllocationFor::DiskVolume {
                    volume_id: VolumeUuid::new_v4(),
                },
                RegionAllocationParameters::FromRaw {
                    block_size,
                    blocks_per_extent,
                    extent_count: extent_count_fits,
                },
                &RegionAllocationStrategy::RandomWithDistinctSleds {
                    seed: None,
                },
                3, // num_regions_required
            )
            .await
            .expect("allocation should succeed at boundary: extent_count_fits just fits");

        // Delete the regions so we can try again with one more extent
        let region_ids = datasets_and_regions
            .iter()
            .map(|(_dataset, region)| region.id())
            .collect();

        datastore
            .regions_hard_delete(&logctx.log, region_ids)
            .await
            .expect("region cleanup should succeed");

        // Allocation with extent_count_fails should FAIL
        let err = datastore
            .arbitrary_region_allocate(
                opctx,
                RegionAllocationFor::DiskVolume {
                    volume_id: VolumeUuid::new_v4(),
                },
                RegionAllocationParameters::FromRaw {
                    block_size,
                    blocks_per_extent,
                    extent_count: extent_count_fails,
                },
                &RegionAllocationStrategy::RandomWithDistinctSleds {
                    seed: None,
                },
                3, // num_regions_required
            )
            .await
            .expect_err(
                "allocation should fail at boundary: extent_count_fails exceeds available space",
            );

        assert!(
            matches!(
                &err,
                external::Error::InsufficientCapacity { message }
                    if message.external_message() == "Not enough storage"
            ),
            "expected InsufficientCapacity with 'Not enough storage', got: {err:?}"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
