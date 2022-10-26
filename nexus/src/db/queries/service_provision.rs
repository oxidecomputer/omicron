// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for provisioning services.

use crate::db::alias::ExpressionAlias;
use crate::db::model::queries::service_provision::{
    candidate_services, candidate_sleds, inserted_services, new_internal_ips,
    new_service_count, old_service_count, previously_allocated_services,
    sled_allocation_pool,
};
use crate::db::model::Service;
use crate::db::model::ServiceKind;
use crate::db::pool::DbConnection;
use crate::db::schema;
use crate::db::subquery::{AsQuerySource, Cte, CteBuilder, CteQuery};
use chrono::DateTime;
use chrono::Utc;
use db_macros::Subquery;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::Query;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::sql_types;
use diesel::CombineDsl;
use diesel::ExpressionMethods;
use diesel::Insertable;
use diesel::IntoSql;
use diesel::JoinOnDsl;
use diesel::NullableExpressionMethods;
use diesel::QueryDsl;
use diesel::RunQueryDsl;

/// A subquery to find all sleds that could run services.
#[derive(Subquery)]
#[subquery(name = sled_allocation_pool)]
struct SledAllocationPool {
    query: Box<dyn CteQuery<SqlType = sled_allocation_pool::SqlType>>,
}

impl SledAllocationPool {
    fn new(rack_id: uuid::Uuid) -> Self {
        use crate::db::schema::sled::dsl;
        Self {
            query: Box::new(
                dsl::sled
                    .filter(dsl::time_deleted.is_null())
                    .filter(dsl::rack_id.eq(rack_id))
                    .select((dsl::id,)),
            ),
        }
    }
}

/// A subquery to find all services of a particular type which have already been
/// allocated.
#[derive(Subquery)]
#[subquery(name = previously_allocated_services)]
struct PreviouslyAllocatedServices {
    query: Box<dyn CteQuery<SqlType = schema::service::SqlType>>,
}

impl PreviouslyAllocatedServices {
    fn new(allocation_pool: &SledAllocationPool, kind: ServiceKind) -> Self {
        use crate::db::schema::service::dsl as service_dsl;
        use sled_allocation_pool::dsl as alloc_pool_dsl;

        let select_from_pool = allocation_pool
            .query_source()
            .select(alloc_pool_dsl::id)
            .into_boxed();
        Self {
            query: Box::new(
                service_dsl::service
                    .filter(service_dsl::kind.eq(kind))
                    .filter(service_dsl::sled_id.eq_any(select_from_pool)),
            ),
        }
    }
}

/// A subquery to find the number of old services.
#[derive(Subquery)]
#[subquery(name = old_service_count)]
struct OldServiceCount {
    query: Box<dyn CteQuery<SqlType = old_service_count::SqlType>>,
}

impl OldServiceCount {
    fn new(
        previously_allocated_services: &PreviouslyAllocatedServices,
    ) -> Self {
        Self {
            query: Box::new(
                previously_allocated_services
                    .query_source()
                    .select((diesel::dsl::count_star(),)),
            ),
        }
    }
}

/// A subquery to find the number of additional services which should be
/// provisioned.
#[derive(Subquery)]
#[subquery(name = new_service_count)]
struct NewServiceCount {
    query: Box<dyn CteQuery<SqlType = new_service_count::SqlType>>,
}

diesel::sql_function!(fn greatest(a: sql_types::BigInt, b: sql_types::BigInt) -> sql_types::BigInt);

impl NewServiceCount {
    fn new(redundancy: i32, old_service_count: &OldServiceCount) -> Self {
        let old_count = old_service_count
            .query_source()
            .select(old_service_count::dsl::count)
            .single_value()
            .assume_not_null();
        Self {
            query: Box::new(diesel::select(ExpressionAlias::new::<
                new_service_count::dsl::count,
            >((greatest(
                (redundancy as i64).into_sql::<sql_types::BigInt>(),
                old_count,
            ) - old_count,)))),
        }
    }
}

/// A subquery to find new sleds to host the proposed services.
#[derive(Subquery)]
#[subquery(name = candidate_sleds)]
struct CandidateSleds {
    query: Box<dyn CteQuery<SqlType = candidate_sleds::SqlType>>,
}

impl CandidateSleds {
    fn new(
        sled_allocation_pool: &SledAllocationPool,
        previously_allocated_services: &PreviouslyAllocatedServices,
        _new_service_count: &NewServiceCount,
    ) -> Self {
        let select_from_previously_allocated = previously_allocated_services
            .query_source()
            .select(previously_allocated_services::dsl::sled_id)
            .into_boxed();

        let mut select_stmt = sled_allocation_pool
            .query_source()
            .filter(
                sled_allocation_pool::dsl::id
                    .ne_all(select_from_previously_allocated),
            )
            .select((sled_allocation_pool::dsl::id,))
            .into_boxed();

        // TODO: I'd really prefer to just pass the 'new_service_count' as the
        // `.limit(...)` here, but the API cannot currently operate on an
        // expression.
        //
        // See: https://github.com/diesel-rs/diesel/discussions/3328 for further
        // discussion.
        select_stmt.limit_offset =
            diesel::query_builder::BoxedLimitOffsetClause {
                limit: Some(Box::new(diesel::dsl::sql::<sql_types::BigInt>(
                    " LIMIT SELECT * FROM new_service_count",
                ))),
                offset: select_stmt.limit_offset.offset,
            };

        Self { query: Box::new(select_stmt) }
    }
}

/// A subquery to provision internal IPs for all the new services.
#[derive(Subquery)]
#[subquery(name = new_internal_ips)]
struct NewInternalIps {
    query: Box<dyn CteQuery<SqlType = new_internal_ips::SqlType>>,
}

impl NewInternalIps {
    fn new(candidate_sleds: &CandidateSleds) -> Self {
        use crate::db::schema::sled::dsl as sled_dsl;
        use candidate_sleds::dsl as candidate_sleds_dsl;

        let select_from_candidate_sleds = candidate_sleds
            .query_source()
            .select(candidate_sleds_dsl::id)
            .into_boxed();
        Self {
            query: Box::new(
                diesel::update(
                    sled_dsl::sled.filter(
                        sled_dsl::id.eq_any(select_from_candidate_sleds),
                    ),
                )
                .set(
                    sled_dsl::last_used_address
                        .eq(sled_dsl::last_used_address + 1),
                )
                .returning((sled_dsl::id, sled_dsl::last_used_address)),
            ),
        }
    }
}

/// A subquery to create the new services which should be inserted.
#[derive(Subquery)]
#[subquery(name = candidate_services)]
struct CandidateServices {
    query: Box<dyn CteQuery<SqlType = schema::service::SqlType>>,
}

diesel::sql_function!(fn gen_random_uuid() -> Uuid);
diesel::sql_function!(fn now() -> Timestamptz);

impl CandidateServices {
    fn new(
        candidate_sleds: &CandidateSleds,
        new_internal_ips: &NewInternalIps,
        kind: ServiceKind,
    ) -> Self {
        use candidate_sleds::dsl as candidate_sleds_dsl;
        use new_internal_ips::dsl as new_internal_ips_dsl;
        use schema::service::dsl as service_dsl;

        let kind = kind.into_sql::<crate::db::model::ServiceKindEnum>();
        Self {
            query:
                Box::new(
                    candidate_sleds
                        .query_source()
                        .inner_join(
                            new_internal_ips
                                .query_source()
                                .on(candidate_sleds_dsl::id
                                    .eq(new_internal_ips_dsl::id)),
                        )
                        .select((
                            ExpressionAlias::new::<service_dsl::id>(
                                gen_random_uuid(),
                            ),
                            ExpressionAlias::new::<service_dsl::time_created>(
                                now(),
                            ),
                            ExpressionAlias::new::<service_dsl::time_modified>(
                                now(),
                            ),
                            ExpressionAlias::new::<service_dsl::sled_id>(
                                candidate_sleds_dsl::id,
                            ),
                            ExpressionAlias::new::<service_dsl::ip>(
                                new_internal_ips_dsl::last_used_address,
                            ),
                            ExpressionAlias::new::<service_dsl::kind>(kind),
                        )),
                ),
        }
    }
}

/// A subquery to insert the new services.
#[derive(Subquery)]
#[subquery(name = inserted_services)]
struct InsertServices {
    query: Box<dyn CteQuery<SqlType = schema::service::SqlType>>,
}

impl InsertServices {
    fn new(candidate: &CandidateServices) -> Self {
        use crate::db::schema::service;

        Self {
            query: Box::new(
                candidate
                    .query_source()
                    .select(candidate_services::all_columns)
                    .insert_into(service::table)
                    .returning(service::all_columns),
            ),
        }
    }
}

/// Provision services of a particular type within a rack.
///
/// TODO: Document
pub struct ServiceProvision {
    now: DateTime<Utc>,

    cte: Cte,
}

impl ServiceProvision {
    pub fn new(
        redundancy: i32,
        rack_id: uuid::Uuid,
        kind: ServiceKind,
    ) -> Self {
        let now = Utc::now();
        let sled_allocation_pool = SledAllocationPool::new(rack_id);
        let previously_allocated_services =
            PreviouslyAllocatedServices::new(&sled_allocation_pool, kind);
        let old_service_count =
            OldServiceCount::new(&previously_allocated_services);
        let new_service_count =
            NewServiceCount::new(redundancy, &old_service_count);
        let candidate_sleds = CandidateSleds::new(
            &sled_allocation_pool,
            &previously_allocated_services,
            &new_service_count,
        );
        let new_internal_ips = NewInternalIps::new(&candidate_sleds);
        let candidate_services =
            CandidateServices::new(&candidate_sleds, &new_internal_ips, kind);
        let inserted_services = InsertServices::new(&candidate_services);

        let final_select = Box::new(
            previously_allocated_services
                .query_source()
                .select(previously_allocated_services::all_columns)
                .union(
                    inserted_services
                        .query_source()
                        .select(inserted_services::all_columns),
                ),
        );

        let cte = CteBuilder::new()
            .add_subquery(sled_allocation_pool)
            .add_subquery(previously_allocated_services)
            .add_subquery(old_service_count)
            .add_subquery(new_service_count)
            .add_subquery(candidate_sleds)
            .add_subquery(new_internal_ips)
            .add_subquery(candidate_services)
            .add_subquery(inserted_services)
            .build(final_select);

        Self { now, cte }
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
impl QueryId for ServiceProvision {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for ServiceProvision {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.unsafe_to_cache_prepared();

        self.cte.walk_ast(out.reborrow())?;
        Ok(())
    }
}

impl Query for ServiceProvision {
    type SqlType = <<Service as
        diesel::Selectable<Pg>>::SelectExpression as diesel::Expression>::SqlType;
}

impl RunQueryDsl<DbConnection> for ServiceProvision {}

#[cfg(test)]
mod tests {
    use crate::context::OpContext;
    use crate::db::datastore::DataStore;
    use diesel::pg::Pg;
    use dropshot::test_util::LogContext;
    use nexus_test_utils::db::test_setup_database;
    use nexus_test_utils::RACK_UUID;
    use omicron_test_utils::dev;
    use omicron_test_utils::dev::db::CockroachInstance;
    use std::sync::Arc;
    use uuid::Uuid;

    use super::ServiceProvision;

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

        let redundancy = 3;
        let query = ServiceProvision::new(
            redundancy,
            Uuid::parse_str(RACK_UUID).unwrap(),
            crate::db::model::ServiceKind::Nexus,
        );

        pretty_assertions::assert_eq!(
            diesel::debug_query::<Pg, _>(&query).to_string(),
            format!(
            "WITH \
             \"sled_allocation_pool\" AS (\
                SELECT \
                    \"sled\".\"id\" \
                FROM \"sled\" \
                WHERE (\
                    (\"sled\".\"time_deleted\" IS NULL) AND \
                    (\"sled\".\"rack_id\" = $1)\
                )\
            ), \
            \"previously_allocated_services\" AS (\
                SELECT \
                    \"service\".\"id\", \
                    \"service\".\"time_created\", \
                    \"service\".\"time_modified\", \
                    \"service\".\"sled_id\", \
                    \"service\".\"ip\", \
                    \"service\".\"kind\" \
                FROM \"service\" \
                WHERE (\
                    (\"service\".\"kind\" = $2) AND \
                    (\"service\".\"sled_id\" = \
                        ANY(SELECT \"sled_allocation_pool\".\"id\" FROM \"sled_allocation_pool\")\
                    )\
                )\
            ), \
            \"old_service_count\" AS (\
                SELECT COUNT(*) FROM \"previously_allocated_services\"\
            ), \
            \"new_service_count\" AS (\
                SELECT (\
                    greatest(\
                        $3, \
                        (SELECT \"old_service_count\".\"count\" FROM \"old_service_count\" LIMIT $4)\
                    ) - (SELECT \"old_service_count\".\"count\" FROM \"old_service_count\" LIMIT $5)\
                ) \
                AS count\
            ), \
            \"candidate_sleds\" AS (\
                SELECT \
                    \"sled_allocation_pool\".\"id\" \
                FROM \"sled_allocation_pool\" \
                WHERE (\
                    \"sled_allocation_pool\".\"id\" != ALL(\
                        SELECT \
                            \"previously_allocated_services\".\"sled_id\" \
                        FROM \"previously_allocated_services\"\
                    )\
                ) \
                LIMIT SELECT * FROM new_service_count\
            ), \
            \"new_internal_ips\" AS (\
                UPDATE \
                    \"sled\" \
                SET \
                    \"last_used_address\" = (\"sled\".\"last_used_address\" + $6) \
                WHERE \
                    (\"sled\".\"id\" = ANY(SELECT \"candidate_sleds\".\"id\" FROM \"candidate_sleds\")) \
                RETURNING \
                    \"sled\".\"id\", \
                    \"sled\".\"last_used_address\"\
            ), \
            \"candidate_services\" AS (\
                SELECT \
                    gen_random_uuid() AS id, \
                    now() AS time_created, \
                    now() AS time_modified, \
                    \"candidate_sleds\".\"id\" AS sled_id, \
                    \"new_internal_ips\".\"last_used_address\" AS ip, \
                    $7 AS kind \
                FROM (\
                    \"candidate_sleds\" \
                INNER JOIN \
                    \"new_internal_ips\" \
                ON (\
                    \"candidate_sleds\".\"id\" = \"new_internal_ips\".\"id\"\
                ))\
            ), \
            \"inserted_services\" AS (\
                INSERT INTO \"service\" \
                    (\"id\", \"time_created\", \"time_modified\", \"sled_id\", \"ip\", \"kind\") \
                SELECT \
                    \"candidate_services\".\"id\", \
                    \"candidate_services\".\"time_created\", \
                    \"candidate_services\".\"time_modified\", \
                    \"candidate_services\".\"sled_id\", \
                    \"candidate_services\".\"ip\", \
                    \"candidate_services\".\"kind\" \
                FROM \"candidate_services\" \
                RETURNING \
                    \"service\".\"id\", \
                    \"service\".\"time_created\", \
                    \"service\".\"time_modified\", \
                    \"service\".\"sled_id\", \
                    \"service\".\"ip\", \"service\".\"kind\"\
            ) \
            (\
                SELECT \
                    \"previously_allocated_services\".\"id\", \
                    \"previously_allocated_services\".\"time_created\", \
                    \"previously_allocated_services\".\"time_modified\", \
                    \"previously_allocated_services\".\"sled_id\", \
                    \"previously_allocated_services\".\"ip\", \
                    \"previously_allocated_services\".\"kind\" \
                FROM \"previously_allocated_services\"\
            ) UNION \
            (\
                SELECT \
                    \"inserted_services\".\"id\", \
                    \"inserted_services\".\"time_created\", \
                    \"inserted_services\".\"time_modified\", \
                    \"inserted_services\".\"sled_id\", \
                    \"inserted_services\".\"ip\", \
                    \"inserted_services\".\"kind\" \
                FROM \"inserted_services\"\
            ) -- binds: [{RACK_UUID}, Nexus, {redundancy}, 1, 1, 1, Nexus]",
            ),
        );

        context.success().await;
    }
}
