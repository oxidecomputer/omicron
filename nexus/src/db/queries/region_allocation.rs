// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for provisioning regions.

use crate::db::alias::ExpressionAlias;
use crate::db::datastore::REGION_REDUNDANCY_THRESHOLD;
use crate::db::model::DatasetKind;
use crate::db::model::Region;
use crate::db::pool::DbConnection;
use crate::db::subquery::{
    AsQuerySource, Cte, CteBuilder, CteQuery, TrueOrCastError,
};
use crate::subquery;
use db_macros::Subquery;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::Query;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::sql_types;
use diesel::BoolExpressionMethods;
use diesel::Column;
use diesel::CombineDsl;
use diesel::ExpressionMethods;
use diesel::Insertable;
use diesel::IntoSql;
use diesel::JoinOnDsl;
use diesel::NullableExpressionMethods;
use diesel::QueryDsl;
use diesel::RunQueryDsl;
use nexus_db_model::schema;
use nexus_db_model::subquery::candidate_datasets;
use nexus_db_model::subquery::candidate_regions;
use nexus_db_model::subquery::candidate_zpools;
use nexus_db_model::subquery::do_insert;
use nexus_db_model::subquery::inserted_regions;
use nexus_db_model::subquery::old_regions;
use nexus_db_model::subquery::old_zpool_usage;
use nexus_db_model::subquery::proposed_dataset_changes;
use nexus_db_model::subquery::proposed_datasets_fit;
use nexus_db_model::subquery::updated_datasets;
use nexus_db_model::subquery::zpool_size_delta;
use nexus_db_model::ByteCount;
use omicron_common::api::external;

const NOT_ENOUGH_DATASETS_SENTINEL: &'static str = "Not enough datasets";
const NOT_ENOUGH_ZPOOL_SPACE_SENTINEL: &'static str = "Not enough space";

fn bool_parse_error(sentinel: &'static str) -> String {
    format!("could not parse \"{sentinel}\" as type bool: invalid bool value")
}

pub fn from_pool(e: async_bb8_diesel::PoolError) -> external::Error {
    use crate::db::error;
    use async_bb8_diesel::ConnectionError;
    use async_bb8_diesel::PoolError;
    use diesel::result::DatabaseErrorKind;
    use diesel::result::Error;

    match e {
        // Catch the specific errors designed to communicate the failures we
        // want to distinguish
        PoolError::Connection(ConnectionError::Query(
            Error::DatabaseError(DatabaseErrorKind::Unknown, ref info),
        )) => {
            if info.message() == bool_parse_error(NOT_ENOUGH_DATASETS_SENTINEL)
            {
                return external::Error::unavail(
                    "Not enough datasets to allocate disks",
                );
            } else if info.message()
                == bool_parse_error(NOT_ENOUGH_ZPOOL_SPACE_SENTINEL)
            {
                return external::Error::unavail(
                    "Not enough zpool space to allocate disks",
                );
            }
        }
        // Any other error at all is a bug
        _ => {}
    }

    error::public_error_from_diesel_pool(e, error::ErrorHandler::Server)
}

/// A subquery to find all old regions.
#[derive(Subquery)]
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

#[derive(Subquery)]
#[subquery(name = candidate_datasets)]
struct CandidateDatasets {
    query: Box<dyn CteQuery<SqlType = candidate_datasets::SqlType>>,
}

impl CandidateDatasets {
    fn new() -> Self {
        use crate::db::schema::dataset::dsl as dataset_dsl;

        Self {
            query: Box::new(
                dataset_dsl::dataset
                    .filter(dataset_dsl::time_deleted.is_null())
                    .filter(dataset_dsl::size_used.is_not_null())
                    .filter(dataset_dsl::kind.eq(DatasetKind::Crucible))
                    .order(dataset_dsl::size_used.asc())
                    .limit(REGION_REDUNDANCY_THRESHOLD.try_into().unwrap())
                    .select((dataset_dsl::id, dataset_dsl::pool_id)),
            ),
        }
    }
}

#[derive(Subquery)]
#[subquery(name = candidate_zpools)]
struct CandidateZpools {
    query: Box<dyn CteQuery<SqlType = candidate_zpools::SqlType>>,
}

impl CandidateZpools {
    fn new(candidate_datasets: &CandidateDatasets) -> Self {
        use crate::db::schema::zpool::dsl as zpool_dsl;
        use candidate_datasets::dsl as candidate_datasets_dsl;

        Self {
            query: Box::new(
                zpool_dsl::zpool
                    .inner_join(
                        candidate_datasets
                            .query_source()
                            .on(candidate_datasets_dsl::pool_id
                                .eq(zpool_dsl::id)),
                    )
                    .select((zpool_dsl::id, zpool_dsl::total_size)),
            ),
        }
    }
}

#[derive(Subquery)]
#[subquery(name = candidate_regions)]
struct CandidateRegions {
    query: Box<dyn CteQuery<SqlType = schema::region::SqlType>>,
}

diesel::sql_function!(fn gen_random_uuid() -> Uuid);
diesel::sql_function!(fn now() -> Timestamptz);

impl CandidateRegions {
    fn new(
        candidate_datasets: &CandidateDatasets,
        volume_id: uuid::Uuid,
        block_size: ByteCount,
        blocks_per_extent: i64,
        extent_count: i64,
    ) -> Self {
        use candidate_datasets::dsl as candidate_datasets_dsl;
        use schema::region;

        let volume_id = volume_id.into_sql::<sql_types::Uuid>();
        // TODO
        let block_size = block_size.into_sql::<sql_types::BigInt>();
        let blocks_per_extent =
            blocks_per_extent.into_sql::<sql_types::BigInt>();
        let extent_count = extent_count.into_sql::<sql_types::BigInt>();
        Self {
            query: Box::new(candidate_datasets.query_source().select((
                ExpressionAlias::new::<region::id>(gen_random_uuid()),
                ExpressionAlias::new::<region::time_created>(now()),
                ExpressionAlias::new::<region::time_modified>(now()),
                ExpressionAlias::new::<region::dataset_id>(
                    candidate_datasets_dsl::id,
                ),
                ExpressionAlias::new::<region::volume_id>(volume_id),
                ExpressionAlias::new::<region::block_size>(block_size),
                ExpressionAlias::new::<region::blocks_per_extent>(
                    blocks_per_extent,
                ),
                ExpressionAlias::new::<region::extent_count>(extent_count),
            ))),
        }
    }
}

#[derive(Subquery)]
#[subquery(name = proposed_dataset_changes)]
struct ProposedChanges {
    query: Box<dyn CteQuery<SqlType = proposed_dataset_changes::SqlType>>,
}

impl ProposedChanges {
    fn new(candidate_regions: &CandidateRegions) -> Self {
        use crate::db::schema::dataset::dsl as dataset_dsl;
        use crate::db::schema::zpool::dsl as zpool_dsl;
        use candidate_regions::dsl as candidate_regions_dsl;
        Self {
            query: Box::new(
                candidate_regions.query_source()
                    .left_join(
                        dataset_dsl::dataset.on(dataset_dsl::id.eq(candidate_regions_dsl::dataset_id))
                    )
                    .left_join(
                        zpool_dsl::zpool.on(zpool_dsl::id.eq(dataset_dsl::pool_id))
                    )
                    .select((
                        ExpressionAlias::new::<proposed_dataset_changes::id>(candidate_regions_dsl::dataset_id),
                        ExpressionAlias::new::<proposed_dataset_changes::pool_id>(dataset_dsl::pool_id.nullable().assume_not_null()),
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

#[derive(Subquery)]
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
                    .inner_join(
                        candidate_zpools::dsl::candidate_zpools
                            .on(dataset_dsl::pool_id
                                .eq(candidate_zpools::dsl::id)),
                    )
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

#[derive(Subquery)]
#[subquery(name = zpool_size_delta)]
struct ZpoolSizeDelta {
    query: Box<dyn CteQuery<SqlType = zpool_size_delta::SqlType>>,
}

impl ZpoolSizeDelta {
    fn new(proposed_changes: &ProposedChanges) -> Self {
        Self {
            query: Box::new(
                proposed_changes.query_source()
                    .group_by(proposed_dataset_changes::dsl::pool_id)
                    .select((
                        ExpressionAlias::new::<zpool_size_delta::pool_id>(proposed_dataset_changes::dsl::pool_id),
                        ExpressionAlias::new::<zpool_size_delta::size_used_delta>(diesel::dsl::sum(proposed_dataset_changes::dsl::size_used_delta).assume_not_null()),
                    ))
            )
        }
    }
}

#[derive(Subquery)]
#[subquery(name = proposed_datasets_fit)]
struct ProposedDatasetsFit {
    query: Box<dyn CteQuery<SqlType = proposed_datasets_fit::SqlType>>,
}

impl ProposedDatasetsFit {
    fn new(
        old_zpool_usage: &OldPoolUsage,
        zpool_size_delta: &ZpoolSizeDelta,
    ) -> Self {
        use schema::zpool::dsl as zpool_dsl;

        Self {
            query:
                Box::new(
                    old_zpool_usage
                        .query_source()
                        .inner_join(zpool_dsl::zpool.on(
                            zpool_dsl::id.eq(old_zpool_usage::dsl::pool_id),
                        ))
                        .inner_join(
                            zpool_size_delta
                                .query_source()
                                .on(zpool_size_delta::dsl::pool_id
                                    .eq(old_zpool_usage::dsl::pool_id)),
                        )
                        .select((ExpressionAlias::new::<
                            proposed_datasets_fit::dsl::fits,
                        >(
                            (old_zpool_usage::dsl::size_used
                                + zpool_size_delta::dsl::size_used_delta)
                                .lt(diesel::dsl::sql(
                                    zpool_dsl::total_size::NAME,
                                )),
                        ),)),
                ),
        }
    }
}

diesel::sql_function! {
    #[aggregate]
    fn bool_and(b: sql_types::Bool) -> sql_types::Bool;
}

#[derive(Subquery)]
#[subquery(name = do_insert)]
struct DoInsert {
    query: Box<dyn CteQuery<SqlType = do_insert::SqlType>>,
}

impl DoInsert {
    fn new(
        old_regions: &OldRegions,
        candidate_regions: &CandidateRegions,
        proposed_datasets_fit: &ProposedDatasetsFit,
    ) -> Self {
        let redundancy = REGION_REDUNDANCY_THRESHOLD as i64;
        let not_allocated_yet = old_regions
            .query_source()
            .count()
            .single_value()
            .assume_not_null()
            .lt(redundancy);
        let enough_candidates = candidate_regions
            .query_source()
            .count()
            .single_value()
            .assume_not_null()
            .ge(redundancy);
        let proposals_fit = proposed_datasets_fit
            .query_source()
            .select(bool_and(proposed_datasets_fit::dsl::fits))
            .single_value()
            .assume_not_null();

        Self {
            query: Box::new(diesel::select((ExpressionAlias::new::<
                do_insert::insert,
            >(
                not_allocated_yet
                    .and(TrueOrCastError::new(
                        enough_candidates,
                        NOT_ENOUGH_DATASETS_SENTINEL,
                    ))
                    .and(TrueOrCastError::new(
                        proposals_fit,
                        NOT_ENOUGH_ZPOOL_SPACE_SENTINEL,
                    )),
            ),))),
        }
    }
}

#[derive(Subquery)]
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

#[derive(Subquery)]
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
                .returning((dataset_dsl::id,))
            )
        }
    }
}

/// TODO: Document
pub struct RegionAllocate {
    cte: Cte,
}

impl RegionAllocate {
    pub fn new(
        volume_id: uuid::Uuid,
        block_size: ByteCount,
        blocks_per_extent: i64,
        extent_count: i64,
    ) -> Self {
        let old_regions = OldRegions::new(volume_id);
        let candidate_datasets = CandidateDatasets::new();
        let candidate_zpools = CandidateZpools::new(&candidate_datasets);
        let candidate_regions = CandidateRegions::new(
            &candidate_datasets,
            volume_id,
            block_size,
            blocks_per_extent,
            extent_count,
        );
        let proposed_changes = ProposedChanges::new(&candidate_regions);
        let old_pool_usage = OldPoolUsage::new();
        let zpool_size_delta = ZpoolSizeDelta::new(&proposed_changes);
        let proposed_datasets_fit =
            ProposedDatasetsFit::new(&old_pool_usage, &zpool_size_delta);
        let do_insert = DoInsert::new(
            &old_regions,
            &candidate_regions,
            &proposed_datasets_fit,
        );
        let insert_regions = InsertRegions::new(&do_insert, &candidate_regions);
        let updated_datasets =
            UpdateDatasets::new(&do_insert, &proposed_changes);

        let final_select = Box::new(
            old_regions.query_source().select(old_regions::all_columns).union(
                insert_regions
                    .query_source()
                    .select(inserted_regions::all_columns),
            ),
        );

        let cte = CteBuilder::new()
            .add_subquery(old_regions)
            .add_subquery(candidate_datasets)
            .add_subquery(candidate_zpools)
            .add_subquery(candidate_regions)
            .add_subquery(proposed_changes)
            .add_subquery(old_pool_usage)
            .add_subquery(zpool_size_delta)
            .add_subquery(proposed_datasets_fit)
            .add_subquery(do_insert)
            .add_subquery(insert_regions)
            .add_subquery(updated_datasets)
            .build(final_select);

        Self { cte }
    }
}

// TODO:
// We could probably make this generic over the Cte "build" method, enforce the
// type there, and auto-impl:
// - QueryId
// - QueryFragment
// - Query
//
// If we know what the SqlType is supposed to be.
impl QueryId for RegionAllocate {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
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

impl Query for RegionAllocate {
    type SqlType = <
        <Region as diesel::Selectable<Pg>>::SelectExpression as diesel::Expression
    >::SqlType;
}

impl RunQueryDsl<DbConnection> for RegionAllocate {}

#[cfg(test)]
mod tests {
    use crate::context::OpContext;
    use crate::db::datastore::DataStore;
    use diesel::pg::Pg;
    use dropshot::test_util::LogContext;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;
    use omicron_test_utils::dev::db::CockroachInstance;
    use std::sync::Arc;
    use uuid::Uuid;

    use super::RegionAllocate;

    struct TestContext {
        logctx: LogContext,
        opctx: OpContext,
        db: CockroachInstance,
        db_datastore: Arc<DataStore>,
    }

    impl TestContext {
        async fn new(test_name: &str) -> Self {
            let logctx = dev::test_setup_log(test_name);
            let log = logctx.log.new(o!());
            let db = test_setup_database(&log).await;
            crate::db::datastore::datastore_test(&logctx, &db).await;
            let cfg = crate::db::Config { url: db.pg_config().clone() };
            let pool = Arc::new(crate::db::Pool::new(&cfg));
            let db_datastore =
                Arc::new(crate::db::DataStore::new(Arc::clone(&pool)));
            let opctx =
                OpContext::for_tests(log.new(o!()), db_datastore.clone());
            Self { logctx, opctx, db, db_datastore }
        }

        async fn success(mut self) {
            self.db.cleanup().await.unwrap();
            self.logctx.cleanup_successful();
        }
    }

    #[tokio::test]
    async fn test_query_output() {
        let context = TestContext::new("test_query_output").await;

        let volume_id = Uuid::new_v4();
        let query = RegionAllocate::new(
            volume_id,
            nexus_db_model::BlockSize::Traditional.into(),
            456,
            789,
        );
        pretty_assertions::assert_eq!(
            diesel::debug_query::<Pg, _>(&query).to_string(),
            format!(
            "WITH \
                \"old_regions\" AS (\
                    SELECT \
                        \"region\".\"id\", \
                        \"region\".\"time_created\", \
                        \"region\".\"time_modified\", \
                        \"region\".\"dataset_id\", \
                        \"region\".\"volume_id\", \
                        \"region\".\"block_size\", \
                        \"region\".\"blocks_per_extent\", \
                        \"region\".\"extent_count\" \
                    FROM \"region\" \
                    WHERE \
                        (\"region\".\"volume_id\" = $1)\
                ), \
                \"candidate_datasets\" AS (\
                    SELECT \
                        \"dataset\".\"id\", \
                        \"dataset\".\"pool_id\" \
                    FROM \"dataset\" \
                    WHERE \
                        (((\"dataset\".\"time_deleted\" IS NULL) AND (\"dataset\".\"size_used\" IS NOT NULL)) AND (\"dataset\".\"kind\" = $2)) \
                    ORDER BY \"dataset\".\"size_used\" ASC  LIMIT $3\
                ), \
                \"candidate_zpools\" AS (SELECT \"zpool\".\"id\" FROM (\"zpool\" INNER JOIN \"candidate_datasets\" ON (\"candidate_datasets\".\"pool_id\" = \"zpool\".\"id\"))), \
                \"candidate_regions\" AS (SELECT gen_random_uuid() AS id, now() AS time_created, now() AS time_modified, \"candidate_datasets\".\"id\" AS dataset_id, $4 AS volume_id, $5 AS block_size, $6 AS blocks_per_extent, $7 AS extent_count FROM \"candidate_datasets\"), \
                \"proposed_dataset_changes\" AS (SELECT \"candidate_regions\".\"dataset_id\" AS id, ((\"candidate_regions\".\"block_size\" * \"candidate_regions\".\"blocks_per_extent\") * \"candidate_regions\".\"extent_count\") AS size_used_delta FROM (\"candidate_regions\" LEFT OUTER JOIN \"dataset\" ON (\"dataset\".\"id\" = \"candidate_regions\".\"dataset_id\"))), \
                \"do_insert\" AS (SELECT (((SELECT COUNT(*) FROM \"old_regions\" LIMIT $8) < $9) AND ((SELECT COUNT(*) FROM \"candidate_regions\" LIMIT $10) >= $11)) AS insert), \
                \"inserted_regions\" AS (INSERT INTO \"region\" (\"id\", \"time_created\", \"time_modified\", \"dataset_id\", \"volume_id\", \"block_size\", \"blocks_per_extent\", \"extent_count\") SELECT \"candidate_regions\".\"id\", \"candidate_regions\".\"time_created\", \"candidate_regions\".\"time_modified\", \"candidate_regions\".\"dataset_id\", \"candidate_regions\".\"volume_id\", \"candidate_regions\".\"block_size\", \"candidate_regions\".\"blocks_per_extent\", \"candidate_regions\".\"extent_count\" FROM \"candidate_regions\" WHERE (SELECT \"do_insert\".\"insert\" FROM \"do_insert\" LIMIT $12) RETURNING \"region\".\"id\", \"region\".\"time_created\", \"region\".\"time_modified\", \"region\".\"dataset_id\", \"region\".\"volume_id\", \"region\".\"block_size\", \"region\".\"blocks_per_extent\", \"region\".\"extent_count\"), \
                \"updated_datasets\" AS (UPDATE \"dataset\" SET \"size_used\" = (\"dataset\".\"size_used\" + (SELECT \"proposed_dataset_changes\".\"size_used_delta\" FROM \"proposed_dataset_changes\" WHERE (\"proposed_dataset_changes\".\"id\" = \"dataset\".\"id\") LIMIT $13)) WHERE ((\"dataset\".\"id\" = ANY(SELECT \"proposed_dataset_changes\".\"id\" FROM \"proposed_dataset_changes\")) AND (SELECT \"do_insert\".\"insert\" FROM \"do_insert\" LIMIT $14)) RETURNING \"dataset\".\"id\") \
                (SELECT \"old_regions\".\"id\", \"old_regions\".\"time_created\", \"old_regions\".\"time_modified\", \"old_regions\".\"dataset_id\", \"old_regions\".\"volume_id\", \"old_regions\".\"block_size\", \"old_regions\".\"blocks_per_extent\", \"old_regions\".\"extent_count\" FROM \"old_regions\") UNION (SELECT \"inserted_regions\".\"id\", \"inserted_regions\".\"time_created\", \"inserted_regions\".\"time_modified\", \"inserted_regions\".\"dataset_id\", \"inserted_regions\".\"volume_id\", \"inserted_regions\".\"block_size\", \"inserted_regions\".\"blocks_per_extent\", \"inserted_regions\".\"extent_count\" FROM \"inserted_regions\") -- binds: [eb55547f-3322-4930-9986-db752d157cd0, Crucible, 3, eb55547f-3322-4930-9986-db752d157cd0, ByteCount(ByteCount(512)), 456, 789, 1, 2, 1, 2, 1, 1, 1]"
             ),
         );

        context.success().await;
    }
}
