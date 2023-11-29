// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for provisioning regions.

use crate::db::alias::ExpressionAlias;
use crate::db::cast_uuid_as_bytea::CastUuidToBytea;
use crate::db::datastore::REGION_REDUNDANCY_THRESHOLD;
use crate::db::model::{Dataset, DatasetKind, Region};
use crate::db::pool::DbConnection;
use crate::db::subquery::{AsQuerySource, Cte, CteBuilder, CteQuery};
use crate::db::true_or_cast_error::{matches_sentinel, TrueOrCastError};
use db_macros::Subquery;
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, Query, QueryFragment, QueryId};
use diesel::result::Error as DieselError;
use diesel::PgBinaryExpressionMethods;
use diesel::{
    sql_types, BoolExpressionMethods, Column, CombineDsl, ExpressionMethods,
    Insertable, IntoSql, JoinOnDsl, NullableExpressionMethods, QueryDsl,
    RunQueryDsl,
};
use nexus_db_model::queries::region_allocation::{
    candidate_datasets, candidate_regions, candidate_zpools, cockroach_md5,
    do_insert, inserted_regions, old_regions, old_zpool_usage,
    proposed_dataset_changes, shuffled_candidate_datasets, updated_datasets,
};
use nexus_db_model::schema;
use omicron_common::api::external;
use omicron_common::nexus_config::RegionAllocationStrategy;

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
        match sentinel {
            NOT_ENOUGH_DATASETS_SENTINEL => {
                return external::Error::unavail_external(
                    "Not enough datasets to allocate disks",
                );
            }
            NOT_ENOUGH_ZPOOL_SPACE_SENTINEL => {
                return external::Error::unavail_external(
                    "Not enough zpool space to allocate disks. There may not be enough disks with space for the requested region. You may also see this if your rack is in a degraded state, or you're running the default multi-rack topology configuration in a 1-sled development environment.",
                );
            }
            NOT_ENOUGH_UNIQUE_ZPOOLS_SENTINEL => {
                return external::Error::unavail_external(
                    "Not enough unique zpools selected while allocating disks",
                );
            }
            // Fall-through to the generic error conversion.
            _ => {}
        }
    }

    error::public_error_from_diesel(e, error::ErrorHandler::Server)
}

/// A subquery to find all old regions associated with a particular volume.
#[derive(Subquery, QueryId)]
#[subquery(name = old_regions)]
struct OldRegions {
    query: Box<dyn CteQuery<SqlType = schema::region::SqlType>>,
}

impl OldRegions {
    fn new(volume_id: uuid::Uuid) -> Self {
        use crate::db::schema::region::dsl;
        Self {
            query: Box::new(dsl::region.filter(dsl::volume_id.eq(volume_id))),
        }
    }
}

/// A subquery to find datasets which could be used for provisioning regions.
///
/// We only consider datasets which are already allocated as "Crucible".
/// This implicitly distinguishes between "M.2s" and "U.2s" -- Nexus needs to
/// determine during dataset provisioning which devices should be considered for
/// usage as Crucible storage.
///
/// We select only one dataset from each zpool.
#[derive(Subquery, QueryId)]
#[subquery(name = candidate_datasets)]
struct CandidateDatasets {
    query: Box<dyn CteQuery<SqlType = candidate_datasets::SqlType>>,
}

impl CandidateDatasets {
    fn new(candidate_zpools: &CandidateZpools, seed: u128) -> Self {
        use crate::db::schema::dataset::dsl as dataset_dsl;
        use candidate_zpools::dsl as candidate_zpool_dsl;

        let seed_bytes = seed.to_le_bytes();

        let query: Box<dyn CteQuery<SqlType = candidate_datasets::SqlType>> =
            Box::new(
                dataset_dsl::dataset
                    .inner_join(candidate_zpools.query_source().on(
                        dataset_dsl::pool_id.eq(candidate_zpool_dsl::pool_id),
                    ))
                    .filter(dataset_dsl::time_deleted.is_null())
                    .filter(dataset_dsl::size_used.is_not_null())
                    .filter(dataset_dsl::kind.eq(DatasetKind::Crucible))
                    .distinct_on(dataset_dsl::pool_id)
                    .order_by((
                        dataset_dsl::pool_id,
                        cockroach_md5::dsl::md5(
                            CastUuidToBytea::new(dataset_dsl::id)
                                .concat(seed_bytes.to_vec()),
                        ),
                    ))
                    .select((dataset_dsl::id, dataset_dsl::pool_id)),
            );
        Self { query }
    }
}

/// Shuffle the candidate datasets, and select REGION_REDUNDANCY_THRESHOLD
/// regions from it.
#[derive(Subquery, QueryId)]
#[subquery(name = shuffled_candidate_datasets)]
struct ShuffledCandidateDatasets {
    query: Box<dyn CteQuery<SqlType = shuffled_candidate_datasets::SqlType>>,
}

impl ShuffledCandidateDatasets {
    fn new(candidate_datasets: &CandidateDatasets, seed: u128) -> Self {
        use candidate_datasets::dsl as candidate_datasets_dsl;

        let seed_bytes = seed.to_le_bytes();

        let query: Box<dyn CteQuery<SqlType = candidate_datasets::SqlType>> =
            Box::new(
                candidate_datasets
                    .query_source()
                    // We order by md5 to shuffle the ordering of the datasets.
                    // md5 has a uniform output distribution so it does the job.
                    .order(cockroach_md5::dsl::md5(
                        CastUuidToBytea::new(candidate_datasets_dsl::id)
                            .concat(seed_bytes.to_vec()),
                    ))
                    .select((
                        candidate_datasets_dsl::id,
                        candidate_datasets_dsl::pool_id,
                    ))
                    .limit(REGION_REDUNDANCY_THRESHOLD.try_into().unwrap()),
            );
        Self { query }
    }
}

/// A subquery to create the regions-to-be-inserted for the volume.
#[derive(Subquery, QueryId)]
#[subquery(name = candidate_regions)]
struct CandidateRegions {
    query: Box<dyn CteQuery<SqlType = schema::region::SqlType>>,
}

diesel::sql_function!(fn gen_random_uuid() -> Uuid);
diesel::sql_function!(fn now() -> Timestamptz);

impl CandidateRegions {
    fn new(
        shuffled_candidate_datasets: &ShuffledCandidateDatasets,
        volume_id: uuid::Uuid,
        block_size: u64,
        blocks_per_extent: u64,
        extent_count: u64,
    ) -> Self {
        use schema::region;
        use shuffled_candidate_datasets::dsl as shuffled_candidate_datasets_dsl;

        let volume_id = volume_id.into_sql::<sql_types::Uuid>();
        let block_size = (block_size as i64).into_sql::<sql_types::BigInt>();
        let blocks_per_extent =
            (blocks_per_extent as i64).into_sql::<sql_types::BigInt>();
        let extent_count =
            (extent_count as i64).into_sql::<sql_types::BigInt>();
        Self {
            query: Box::new(shuffled_candidate_datasets.query_source().select(
                (
                    ExpressionAlias::new::<region::id>(gen_random_uuid()),
                    ExpressionAlias::new::<region::time_created>(now()),
                    ExpressionAlias::new::<region::time_modified>(now()),
                    ExpressionAlias::new::<region::dataset_id>(
                        shuffled_candidate_datasets_dsl::id,
                    ),
                    ExpressionAlias::new::<region::volume_id>(volume_id),
                    ExpressionAlias::new::<region::block_size>(block_size),
                    ExpressionAlias::new::<region::blocks_per_extent>(
                        blocks_per_extent,
                    ),
                    ExpressionAlias::new::<region::extent_count>(extent_count),
                ),
            )),
        }
    }
}

/// A subquery which summarizes the changes we intend to make, showing:
///
/// 1. Which datasets will have size adjustments
/// 2. Which pools those datasets belong to
/// 3. The delta in size-used
#[derive(Subquery, QueryId)]
#[subquery(name = proposed_dataset_changes)]
struct ProposedChanges {
    query: Box<dyn CteQuery<SqlType = proposed_dataset_changes::SqlType>>,
}

impl ProposedChanges {
    fn new(candidate_regions: &CandidateRegions) -> Self {
        use crate::db::schema::dataset::dsl as dataset_dsl;
        use candidate_regions::dsl as candidate_regions_dsl;
        Self {
            query: Box::new(
                candidate_regions.query_source()
                    .inner_join(
                        dataset_dsl::dataset.on(dataset_dsl::id.eq(candidate_regions_dsl::dataset_id))
                    )
                    .select((
                        ExpressionAlias::new::<proposed_dataset_changes::id>(candidate_regions_dsl::dataset_id),
                        ExpressionAlias::new::<proposed_dataset_changes::pool_id>(dataset_dsl::pool_id),
                        ExpressionAlias::new::<proposed_dataset_changes::size_used_delta>(
                            candidate_regions_dsl::block_size *
                            candidate_regions_dsl::blocks_per_extent *
                            candidate_regions_dsl::extent_count
                        ),
                    ))
            ),
        }
    }
}

/// A subquery which calculates the old size being used by zpools
/// under consideration as targets for region allocation.
#[derive(Subquery, QueryId)]
#[subquery(name = old_zpool_usage)]
struct OldPoolUsage {
    query: Box<dyn CteQuery<SqlType = old_zpool_usage::SqlType>>,
}

impl OldPoolUsage {
    fn new() -> Self {
        use crate::db::schema::dataset::dsl as dataset_dsl;
        Self {
            query: Box::new(
                dataset_dsl::dataset
                    .group_by(dataset_dsl::pool_id)
                    .filter(dataset_dsl::size_used.is_not_null())
                    .filter(dataset_dsl::time_deleted.is_null())
                    .select((
                        dataset_dsl::pool_id,
                        ExpressionAlias::new::<old_zpool_usage::size_used>(
                            diesel::dsl::sum(dataset_dsl::size_used)
                                .assume_not_null(),
                        ),
                    )),
            ),
        }
    }
}

/// A subquery which identifies zpools with enough space for a region allocation.
#[derive(Subquery, QueryId)]
#[subquery(name = candidate_zpools)]
struct CandidateZpools {
    query: Box<dyn CteQuery<SqlType = candidate_zpools::SqlType>>,
}

impl CandidateZpools {
    fn new(
        old_zpool_usage: &OldPoolUsage,
        zpool_size_delta: u64,
        seed: u128,
        distinct_sleds: bool,
    ) -> Self {
        use schema::zpool::dsl as zpool_dsl;

        // Why are we using raw `diesel::dsl::sql` here?
        //
        // When SQL performs the "SUM" operation on "bigint" type, the result
        // is promoted to "numeric" (see: old_zpool_usage::dsl::size_used).
        //
        // However, we'd like to compare that value with a different value
        // (zpool_dsl::total_size) which is still a "bigint". This comparison
        // is safe (after all, we basically want to promote "total_size" to a
        // Numeric too) but Diesel demands that the input and output SQL types
        // of expression methods like ".le" match exactly.
        //
        // For similar reasons, we use `diesel::dsl::sql` with zpool_size_delta.
        // We would like to add it, but diesel only permits us to `to_sql()` it
        // into a BigInt, not a Numeric. I welcome a better solution.
        let it_will_fit = (old_zpool_usage::dsl::size_used
            + diesel::dsl::sql(&zpool_size_delta.to_string()))
        .le(diesel::dsl::sql(zpool_dsl::total_size::NAME));

        let with_zpool = zpool_dsl::zpool
            .on(zpool_dsl::id.eq(old_zpool_usage::dsl::pool_id));

        let base_query = old_zpool_usage
            .query_source()
            .inner_join(with_zpool)
            .filter(it_will_fit)
            .select((old_zpool_usage::dsl::pool_id,));

        let query = if distinct_sleds {
            let seed_bytes = seed.to_le_bytes();

            let query: Box<dyn CteQuery<SqlType = candidate_zpools::SqlType>> =
                Box::new(
                    base_query
                        .order_by((
                            zpool_dsl::sled_id,
                            cockroach_md5::dsl::md5(
                                CastUuidToBytea::new(zpool_dsl::id)
                                    .concat(seed_bytes.to_vec()),
                            ),
                        ))
                        .distinct_on(zpool_dsl::sled_id),
                );

            query
        } else {
            let query: Box<dyn CteQuery<SqlType = candidate_zpools::SqlType>> =
                Box::new(base_query);

            query
        };

        Self { query }
    }
}

diesel::sql_function! {
    #[aggregate]
    fn bool_and(b: sql_types::Bool) -> sql_types::Bool;
}

/// A subquery which confirms whether or not the insertion and updates should
/// occur.
///
/// This subquery additionally exits the CTE early with an error if either:
/// 1. Not enough datasets exist to provision regions with our required
///    redundancy, or
/// 2. Not enough space exists on zpools to perform the provisioning.
#[derive(Subquery, QueryId)]
#[subquery(name = do_insert)]
struct DoInsert {
    query: Box<dyn CteQuery<SqlType = do_insert::SqlType>>,
}

impl DoInsert {
    fn new(
        old_regions: &OldRegions,
        candidate_regions: &CandidateRegions,
        candidate_zpools: &CandidateZpools,
    ) -> Self {
        let redundancy = REGION_REDUNDANCY_THRESHOLD as i64;
        let not_allocated_yet = old_regions
            .query_source()
            .count()
            .single_value()
            .assume_not_null()
            .lt(redundancy);

        let enough_candidate_zpools = candidate_zpools
            .query_source()
            .count()
            .single_value()
            .assume_not_null()
            .ge(redundancy);

        let enough_candidate_regions = candidate_regions
            .query_source()
            .count()
            .single_value()
            .assume_not_null()
            .ge(redundancy);

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
        use crate::db::schema::dataset::dsl as dataset_dsl;
        use candidate_regions::dsl as candidate_dsl;
        let enough_unique_candidate_zpools = candidate_regions
            .query_source()
            .inner_join(
                dataset_dsl::dataset
                    .on(candidate_dsl::dataset_id.eq(dataset_dsl::id)),
            )
            .select(diesel::dsl::count_distinct(dataset_dsl::pool_id))
            .single_value()
            .assume_not_null()
            .ge(redundancy);

        Self {
            query: Box::new(diesel::select((ExpressionAlias::new::<
                do_insert::insert,
            >(
                not_allocated_yet
                    .and(TrueOrCastError::new(
                        enough_candidate_zpools,
                        NOT_ENOUGH_ZPOOL_SPACE_SENTINEL,
                    ))
                    .and(TrueOrCastError::new(
                        enough_candidate_regions,
                        NOT_ENOUGH_DATASETS_SENTINEL,
                    ))
                    .and(TrueOrCastError::new(
                        enough_unique_candidate_zpools,
                        NOT_ENOUGH_UNIQUE_ZPOOLS_SENTINEL,
                    )),
            ),))),
        }
    }
}

/// A subquery which actually inserts the regions.
#[derive(Subquery, QueryId)]
#[subquery(name = inserted_regions)]
struct InsertRegions {
    query: Box<dyn CteQuery<SqlType = schema::region::SqlType>>,
}

impl InsertRegions {
    fn new(do_insert: &DoInsert, candidate_regions: &CandidateRegions) -> Self {
        use crate::db::schema::region;

        Self {
            query: Box::new(
                candidate_regions
                    .query_source()
                    .select(candidate_regions::all_columns)
                    .filter(
                        do_insert
                            .query_source()
                            .select(do_insert::insert)
                            .single_value()
                            .assume_not_null(),
                    )
                    .insert_into(region::table)
                    .returning(region::all_columns),
            ),
        }
    }
}

/// A subquery which updates dataset size usage based on inserted regions.
#[derive(Subquery, QueryId)]
#[subquery(name = updated_datasets)]
struct UpdateDatasets {
    query: Box<dyn CteQuery<SqlType = updated_datasets::SqlType>>,
}

impl UpdateDatasets {
    fn new(
        do_insert: &DoInsert,
        proposed_dataset_changes: &ProposedChanges,
    ) -> Self {
        use crate::db::schema::dataset::dsl as dataset_dsl;

        let datasets_with_updates = proposed_dataset_changes
            .query_source()
            .select(proposed_dataset_changes::columns::id)
            .into_boxed();

        Self {
            query: Box::new(
                diesel::update(
                    dataset_dsl::dataset.filter(
                        dataset_dsl::id.eq_any(datasets_with_updates)
                    )
                )
                .filter(
                    do_insert.query_source()
                        .select(do_insert::insert)
                        .single_value()
                        .assume_not_null()
                )
                .set(
                    dataset_dsl::size_used.eq(
                        dataset_dsl::size_used + proposed_dataset_changes.query_source()
                            .filter(proposed_dataset_changes::columns::id.eq(dataset_dsl::id))
                            .select(proposed_dataset_changes::columns::size_used_delta)
                            .single_value()
                    )
                )
                .returning(crate::db::schema::dataset::all_columns)
            )
        }
    }
}

/// Constructs a CTE for allocating new regions, and updating the datasets to
/// which those regions belong.
#[derive(QueryId)]
pub struct RegionAllocate {
    cte: Cte,
}

impl RegionAllocate {
    pub fn new(
        volume_id: uuid::Uuid,
        block_size: u64,
        blocks_per_extent: u64,
        extent_count: u64,
        allocation_strategy: &RegionAllocationStrategy,
    ) -> Self {
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

        let size_delta = block_size * blocks_per_extent * extent_count;

        let old_regions = OldRegions::new(volume_id);

        let old_pool_usage = OldPoolUsage::new();
        let candidate_zpools = CandidateZpools::new(
            &old_pool_usage,
            size_delta,
            seed,
            distinct_sleds,
        );

        let candidate_datasets =
            CandidateDatasets::new(&candidate_zpools, seed);

        let shuffled_candidate_datasets =
            ShuffledCandidateDatasets::new(&candidate_datasets, seed);

        let candidate_regions = CandidateRegions::new(
            &shuffled_candidate_datasets,
            volume_id,
            block_size,
            blocks_per_extent,
            extent_count,
        );
        let proposed_changes = ProposedChanges::new(&candidate_regions);
        let do_insert =
            DoInsert::new(&old_regions, &candidate_regions, &candidate_zpools);
        let insert_regions = InsertRegions::new(&do_insert, &candidate_regions);
        let updated_datasets =
            UpdateDatasets::new(&do_insert, &proposed_changes);

        // Gather together all "(dataset, region)" rows for all regions which
        // are allocated to the volume.
        //
        // This roughly translates to:
        //
        // old_regions INNER JOIN old_datasets
        // UNION
        // new_regions INNER JOIN updated_datasets
        //
        // Note that we cannot simply JOIN the old + new regions, and query for
        // their associated datasets: doing so would return the pre-UPDATE
        // values of datasets that are updated by this CTE.
        let final_select = Box::new(
            old_regions
                .query_source()
                .inner_join(
                    crate::db::schema::dataset::dsl::dataset
                        .on(old_regions::dataset_id
                            .eq(crate::db::schema::dataset::dsl::id)),
                )
                .select((
                    crate::db::schema::dataset::all_columns,
                    old_regions::all_columns,
                ))
                .union(
                    insert_regions
                        .query_source()
                        .inner_join(
                            updated_datasets::dsl::updated_datasets
                                .on(inserted_regions::dataset_id
                                    .eq(updated_datasets::id)),
                        )
                        .select((
                            updated_datasets::all_columns,
                            inserted_regions::all_columns,
                        )),
                ),
        );

        let cte = CteBuilder::new()
            .add_subquery(old_regions)
            .add_subquery(old_pool_usage)
            .add_subquery(candidate_zpools)
            .add_subquery(candidate_datasets)
            .add_subquery(shuffled_candidate_datasets)
            .add_subquery(candidate_regions)
            .add_subquery(proposed_changes)
            .add_subquery(do_insert)
            .add_subquery(insert_regions)
            .add_subquery(updated_datasets)
            .build(final_select);

        Self { cte }
    }
}

impl QueryFragment<Pg> for RegionAllocate {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.unsafe_to_cache_prepared();

        self.cte.walk_ast(out.reborrow())?;
        Ok(())
    }
}

type SelectableSql<T> = <
    <T as diesel::Selectable<Pg>>::SelectExpression as diesel::Expression
>::SqlType;

impl Query for RegionAllocate {
    type SqlType = (SelectableSql<Dataset>, SelectableSql<Region>);
}

impl RunQueryDsl<DbConnection> for RegionAllocate {}
