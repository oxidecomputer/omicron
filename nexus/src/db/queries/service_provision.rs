// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for provisioning services.

use crate::db::model::Service;
use crate::db::model::ServiceKind;
use crate::db::pool::DbConnection;
use crate::db::schema;
use chrono::DateTime;
use chrono::Utc;
use diesel::CombineDsl;
use diesel::Expression;
use diesel::ExpressionMethods;
use diesel::IntoSql;
use diesel::Insertable;
use diesel::JoinOnDsl;
use diesel::NullableExpressionMethods;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
// use diesel::query_builder::AsQuery;
use diesel::query_builder::Query;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::sql_types;
use diesel::QueryDsl;
use diesel::RunQueryDsl;
use diesel::SelectableExpression;

trait CteQuery<ST>: Query<SqlType = ST> + QueryFragment<Pg> {}

impl<T, ST> CteQuery<ST> for T
where T: Query<SqlType = ST> + QueryFragment<Pg> {}

// TODO: What macro am I going to build to make this generation easier?
//
// - Must create a `diesel::table` which wraps the output type.
//   - This could have a helper to match the shape of an existing table.
// - Should automatically implement our version of "HasTable"
// - Should automatically implement SubQuery
//
// INPUT:
// - macro_rules generation for `subquery`
//  - Implied: Associated table name
//
// - On struct: Associated table name
//
// OUTPUT:
// - Output `table!` macro
// - Impl `HasTable` for associated object
// - Impl `SubQuery`

/// Represents a sub-query within a CTE.
///
/// For an expression like:
///
/// ```sql
/// WITH
///     foo as ...,
///     bar as ...,
/// SELECT * FROM bar;
/// ```
///
/// This trait represents one of the sub-query arms, such as "foo as ..." or
/// "bar as ...".
trait SubQuery {
    fn name(&self) -> &'static str;
    fn query(&self) -> &dyn QueryFragment<Pg>;
}

// TODO: Do you actually want to grab the *table*?
//
// You want something that allows selection, calling 'count', filtering,
// joining, etc.
trait HasTable
where
//    Self::Table: AsQuery,
{
    type Table;
    fn table(&self) -> Self::Table;
}

/// A thin wrapper around a [`SubQuery`].
///
/// Used to avoid orphan rules while creating blanket implementations.
struct CteSubquery(Box<dyn SubQuery>);

impl QueryId for CteSubquery {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for CteSubquery {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.unsafe_to_cache_prepared();

        out.push_sql(self.0.name());
        out.push_sql(" AS (");
        self.0.query().walk_ast(out.reborrow())?;
        out.push_sql(")");
        Ok(())
    }
}

struct CteBuilder {
    subqueries: Vec<CteSubquery>,
}

impl CteBuilder {
    fn new() -> Self {
        Self {
            subqueries: vec![],
        }
    }

    fn add_subquery<Q: SubQuery + 'static>(mut self, subquery: Q) -> Self {
        self.subqueries.push(
            CteSubquery(Box::new(subquery))
        );
        self
    }

    // TODO: It would be nice if this could be typed?
    // It's not necessarily a SubQuery, but it's probably a "Query" object
    // with a particular SQL type.
    fn build(self, statement: Box<dyn QueryFragment<Pg>>) -> Cte {
        Cte {
            subqueries: self.subqueries,
            statement
        }
    }
}

struct Cte {
    subqueries: Vec<CteSubquery>,
    statement: Box<dyn QueryFragment<Pg>>,
}

impl QueryFragment<Pg> for Cte {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.unsafe_to_cache_prepared();

        out.push_sql("WITH ");
        for (pos, query) in self.subqueries.iter().enumerate() {
            query.walk_ast(out.reborrow())?;
            if pos == self.subqueries.len() - 1 {
                out.push_sql(" ");
            } else {
                out.push_sql(", ");
            }
        }
        self.statement.walk_ast(out.reborrow())?;
        Ok(())
    }
}

/// Allows an [`diesel::Expression`] to be referenced by a new name.
///
/// This generates an "<expression> AS <name>" SQL fragment.
///
///
/// For example:
///
/// ```ignore
/// diesel::sql_function!(fn gen_random_uuid() -> Uuid);
///
/// let query = sleds.select(
///     (
///         ExpressionAlias::<schema::services::dsl::id>(gen_random_uuid()),
///         ExpressionAlias::<schema::services::dsl::sled_id>(gen_random_uuid()),
///     ),
/// );
/// ```
///
/// Produces the following SQL:
///
/// ```sql
/// SELECT
///   gen_random_uuid() as id,
///   gen_random_uuid() as sled_id,
/// FROM sleds
/// ```
// TODO: This is currently used within SELECT statements, though it could
// also be used in INSERT / UPDATE / DELETE statements, to force a subquery
// to have a particular name. This would likely involve an invasive change
// within Diesel itself.
#[derive(diesel::expression::ValidGrouping, diesel::query_builder::QueryId)]
struct ExpressionAlias<E> {
    expr: E,
    name: &'static str,
}

impl<E> ExpressionAlias<E>
where
    E: Expression
{
    fn new<C: diesel::Column>(expr: E) -> Self {
        Self {
            expr,
            name: C::NAME,
        }
    }
}

impl <E> Expression for ExpressionAlias<E>
where
    E: Expression
{
    type SqlType = E::SqlType;
}

impl <E, QS> diesel::AppearsOnTable<QS> for ExpressionAlias<E>
where
    E: diesel::AppearsOnTable<QS>
{}

impl <E, T> SelectableExpression<T> for ExpressionAlias<E>
where
    E: SelectableExpression<T>
{}

impl<E> QueryFragment<Pg> for ExpressionAlias<E>
where
    E: QueryFragment<Pg>
{
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        self.expr.walk_ast(out.reborrow())?;
        out.push_sql(" AS ");
        out.push_sql(&self.name);
        Ok(())
    }
}

// ----------------------------- //
// Above should be for a generic CTE builder
// Below should be for service provisioning
// ----------------------------- //

// TODO: I want this to be as lightweight to make as possible!
struct SledAllocationPool {
    query: Box<dyn CteQuery<sled_allocation_pool::SqlType>>,
}

impl SledAllocationPool {
    fn new() -> Self {
        use crate::db::schema::sled::dsl;
        Self {
            query: Box::new(
                dsl::sled
                    .filter(dsl::time_deleted.is_null())
                    // TODO: Filter by rack?
                    .select((dsl::id,))
            )
        }
    }
}

impl HasTable for SledAllocationPool {
    type Table = sled_allocation_pool::dsl::sled_allocation_pool;
    fn table(&self) -> Self::Table {
        use diesel::internal::table_macro::StaticQueryFragment;
        // TODO: Converting this to a compile-time check would be nicer.
        //
        // TODO: Even better, don't have "name()" at all... force the ExpressionAlias
        // to be the intermediate "table" name.
        assert_eq!(self.name(), Self::Table::STATIC_COMPONENT.0);

        sled_allocation_pool::dsl::sled_allocation_pool
    }
}

impl SubQuery for SledAllocationPool {
    fn name(&self) -> &'static str {
        "sled_allocation_pool"
    }

    fn query(&self) -> &dyn QueryFragment<Pg> {
        &self.query
    }
}

// TODO: We actually want a trimmed down version of this.
// It's generating too much; we don't want to be able to insert/delete/update
// this table; it's basically an ExpressionAlias.
// We *also* do not want the Primary Key.
//
// However, being able to select columns by name is a critical feature
// that we can't easily do without a similar-looking macro.
diesel::table! {
    sled_allocation_pool {
        id -> Uuid,
    }
}

// TODO:
// - How do we avoid re-typing UUID?
// - What can be made generic?

struct PreviouslyAllocatedServices {
    query: Box<dyn CteQuery<previously_allocated_services::SqlType>>,
}

impl PreviouslyAllocatedServices {
    fn new(allocation_pool: &SledAllocationPool) -> Self {
        use crate::db::schema::service::dsl as service_dsl;
        use sled_allocation_pool::dsl as alloc_pool_dsl;

        let select_from_pool = allocation_pool.table().select(alloc_pool_dsl::id).into_boxed();
        Self {
            query: Box::new(
                service_dsl::service
                    .filter(service_dsl::kind.eq(ServiceKind::Nexus))
                    .filter(service_dsl::sled_id.eq_any(select_from_pool))
            )
        }
    }
}

impl HasTable for PreviouslyAllocatedServices {
    type Table = previously_allocated_services::dsl::previously_allocated_services;
    fn table(&self) -> Self::Table {
        use diesel::internal::table_macro::StaticQueryFragment;
        assert_eq!(self.name(), Self::Table::STATIC_COMPONENT.0);
        previously_allocated_services::dsl::previously_allocated_services
    }
}

impl SubQuery for PreviouslyAllocatedServices {
    fn name(&self) -> &'static str {
        "previously_allocated_services"
    }

    fn query(&self) -> &dyn QueryFragment<Pg> {
        &self.query
    }
}

diesel::table! {
    previously_allocated_services {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,

        sled_id -> Uuid,
        ip -> Inet,
        kind -> crate::db::model::ServiceKindEnum,
    }
}

struct OldServiceCount {
    query: Box<dyn CteQuery<sql_types::BigInt>>,
}

impl OldServiceCount {
    fn new(previously_allocated_services: &PreviouslyAllocatedServices) -> Self {
        Self {
            query: Box::new(
                previously_allocated_services.table().count()
            )
        }
    }
}

impl HasTable for OldServiceCount {
    type Table = old_service_count::dsl::old_service_count;
    fn table(&self) -> Self::Table {
        use diesel::internal::table_macro::StaticQueryFragment;
        assert_eq!(self.name(), Self::Table::STATIC_COMPONENT.0);
        old_service_count::dsl::old_service_count
    }
}

impl SubQuery for OldServiceCount {
    fn name(&self) -> &'static str {
        "old_service_count"
    }

    fn query(&self) -> &dyn QueryFragment<Pg> {
        &self.query
    }
}

diesel::table! {
    old_service_count (count) {
        count -> Int8,
    }
}

struct NewServiceCount {
    query: Box<dyn CteQuery<sql_types::BigInt>>,
}

diesel::sql_function!(fn greatest(a: sql_types::BigInt, b: sql_types::BigInt) -> sql_types::BigInt);

impl NewServiceCount {
    fn new(redundancy: i32, old_service_count: &OldServiceCount) -> Self {
        let old_count = old_service_count.table()
            .select(old_service_count::dsl::count)
            .single_value()
            .assume_not_null();
        Self {
            query: Box::new(
                diesel::select(
                    greatest(
                        (redundancy as i64).into_sql::<sql_types::BigInt>(),
                        old_count,
                    ) - old_count
                )
            )
        }
    }
}

impl SubQuery for NewServiceCount {
    fn name(&self) -> &'static str {
        "new_service_count"
    }

    fn query(&self) -> &dyn QueryFragment<Pg> {
        &self.query
    }
}

struct CandidateSleds {
    query: Box<dyn CteQuery<sql_types::Uuid>>
}

impl CandidateSleds {
    fn new(
        sled_allocation_pool: &SledAllocationPool,
        previously_allocated_services: &PreviouslyAllocatedServices,
        _new_service_count: &NewServiceCount,
    ) -> Self {

        let select_from_previously_allocated = previously_allocated_services.table()
            .select(previously_allocated_services::dsl::sled_id)
            .into_boxed();

        let mut select_stmt = sled_allocation_pool
            .table()
            .filter(sled_allocation_pool::dsl::id.ne_all(select_from_previously_allocated))
            .select(sled_allocation_pool::dsl::id)
            .into_boxed();

        // TODO: I'd really prefer to just pass the 'new_service_count' as the
        // `.limit(...)` here, but the API cannot currently operate on an
        // expression.
        //
        // See: https://github.com/diesel-rs/diesel/discussions/3328 for further
        // discussion.
        select_stmt.limit_offset = diesel::query_builder::BoxedLimitOffsetClause {
            limit: Some(Box::new(diesel::dsl::sql::<sql_types::BigInt>(" LIMIT SELECT * FROM new_service_count"))),
            offset: select_stmt.limit_offset.offset,
        };

        Self {
            query: Box::new(
                select_stmt
            )
        }
    }
}

impl HasTable for CandidateSleds {
    type Table = candidate_sleds::dsl::candidate_sleds;
    fn table(&self) -> Self::Table {
        use diesel::internal::table_macro::StaticQueryFragment;
        assert_eq!(self.name(), Self::Table::STATIC_COMPONENT.0);
        candidate_sleds::dsl::candidate_sleds
    }
}

impl SubQuery for CandidateSleds {
    fn name(&self) -> &'static str {
        "candidate_sleds"
    }

    fn query(&self) -> &dyn QueryFragment<Pg> {
        &self.query
    }
}

diesel::table! {
    candidate_sleds {
        id -> Uuid,
    }
}

struct NewInternalIps {
    query: Box<dyn CteQuery<new_internal_ips::SqlType>>,
}

impl NewInternalIps {
    fn new(candidate_sleds: &CandidateSleds) -> Self {
        use crate::db::schema::sled::dsl as sled_dsl;
        use candidate_sleds::dsl as candidate_sleds_dsl;

        let select_from_candidate_sleds = candidate_sleds.table().select(candidate_sleds_dsl::id).into_boxed();
        Self {

            query: Box::new(
                diesel::update(sled_dsl::sled.filter(sled_dsl::id.eq_any(select_from_candidate_sleds)))
                    .set(sled_dsl::last_used_address.eq(sled_dsl::last_used_address + 1))
                    .returning((sled_dsl::id, sled_dsl::last_used_address))
            )
        }
    }
}

impl HasTable for NewInternalIps {
    type Table = new_internal_ips::dsl::new_internal_ips;
    fn table(&self) -> Self::Table {
        use diesel::internal::table_macro::StaticQueryFragment;
        assert_eq!(self.name(), Self::Table::STATIC_COMPONENT.0);
        new_internal_ips::dsl::new_internal_ips
    }
}

impl SubQuery for NewInternalIps {
    fn name(&self) -> &'static str {
        "new_internal_ips"
    }

    fn query(&self) -> &dyn QueryFragment<Pg> {
        &self.query
    }
}

diesel::table! {
    new_internal_ips {
        id -> Uuid,
        last_used_address -> Inet,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    candidate_sleds,
    new_internal_ips,
);

struct CandidateServices {
    query: Box<dyn CteQuery<candidate_services::SqlType>>,
}

diesel::sql_function!(fn gen_random_uuid() -> Uuid);
diesel::sql_function!(fn now() -> Timestamptz);

impl CandidateServices {
    fn new(candidate_sleds: &CandidateSleds, new_internal_ips: &NewInternalIps) -> Self {
        use candidate_sleds::dsl as candidate_sleds_dsl;
        use new_internal_ips::dsl as new_internal_ips_dsl;
        use schema::service::dsl as service_dsl;

        Self {
            query: Box::new(
                candidate_sleds.table().inner_join(
                    new_internal_ips.table().on(
                        candidate_sleds_dsl::id.eq(new_internal_ips_dsl::id)
                    )
                ).select(
                    (
                        ExpressionAlias::new::<service_dsl::id>(gen_random_uuid()),
                        ExpressionAlias::new::<service_dsl::time_created>(now()),
                        ExpressionAlias::new::<service_dsl::time_modified>(now()),
                        ExpressionAlias::new::<service_dsl::sled_id>(candidate_sleds_dsl::id),
                        ExpressionAlias::new::<service_dsl::ip>(new_internal_ips_dsl::last_used_address),
                        ExpressionAlias::new::<service_dsl::kind>(ServiceKind::Nexus.into_sql::<crate::db::model::ServiceKindEnum>()),
                    ),
                )
            )
        }
    }
}

impl HasTable for CandidateServices {
    type Table = candidate_services::dsl::candidate_services;
    fn table(&self) -> Self::Table {
        use diesel::internal::table_macro::StaticQueryFragment;
        assert_eq!(self.name(), Self::Table::STATIC_COMPONENT.0);
        candidate_services::dsl::candidate_services
    }
}

impl SubQuery for CandidateServices {
    fn name(&self) -> &'static str {
        "candidate_services"
    }

    fn query(&self) -> &dyn QueryFragment<Pg> {
        &self.query
    }
}

diesel::table! {
    candidate_services {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,

        sled_id -> Uuid,
        ip -> Inet,
        kind -> crate::db::model::ServiceKindEnum,
    }
}

struct InsertServices {
    query: Box<dyn CteQuery<schema::service::SqlType>>,
}

impl InsertServices {
    fn new(candidate: &CandidateServices) -> Self {
        use crate::db::schema::service;

        Self {
            query: Box::new(
                candidate.table().select(
                    candidate_services::all_columns,
                ).insert_into(
                    service::table
                ).returning(
                    service::all_columns
                )
            )
        }
    }
}

impl HasTable for InsertServices {
    type Table = inserted_services::dsl::inserted_services;
    fn table(&self) -> Self::Table {
        use diesel::internal::table_macro::StaticQueryFragment;
        assert_eq!(self.name(), Self::Table::STATIC_COMPONENT.0);
        inserted_services::dsl::inserted_services
    }
}

impl SubQuery for InsertServices {
    fn name(&self) -> &'static str {
        "inserted_services"
    }

    fn query(&self) -> &dyn QueryFragment<Pg> {
        &self.query
    }
}

diesel::table! {
    inserted_services {
        id -> Uuid,
        time_created -> Timestamptz,
        time_modified -> Timestamptz,

        sled_id -> Uuid,
        ip -> Inet,
        kind -> crate::db::model::ServiceKindEnum,
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
    pub fn new(redundancy: i32) -> Self {
        let now = Utc::now();
        let sled_allocation_pool = SledAllocationPool::new();
        let previously_allocated_services = PreviouslyAllocatedServices::new(&sled_allocation_pool);
        let old_service_count = OldServiceCount::new(&previously_allocated_services);
        let new_service_count = NewServiceCount::new(redundancy, &old_service_count);
        let candidate_sleds = CandidateSleds::new(
            &sled_allocation_pool,
            &previously_allocated_services,
            &new_service_count
        );
        let new_internal_ips = NewInternalIps::new(&candidate_sleds);
        let candidate_services = CandidateServices::new(&candidate_sleds, &new_internal_ips);
        let inserted_services = InsertServices::new(&candidate_services);

        let final_select = Box::new(
            previously_allocated_services
                .table()
                .select(previously_allocated_services::all_columns)
                .union(
                    inserted_services.table().select(
                        inserted_services::all_columns
                    )
                )
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

        Self {
            now,
            cte,
        }
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
    use crate::db::identity::Resource;
    use crate::db::model::Name;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use diesel::pg::Pg;
    use dropshot::test_util::LogContext;
    use nexus_test_utils::db::test_setup_database;
    use nexus_test_utils::RACK_UUID;
    use omicron_common::api::external::Error;
    use omicron_common::api::external::IdentityMetadataCreateParams;
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
    async fn test_foobar() {
        let context = TestContext::new(
            "test_foobar",
        )
        .await;

        let query = ServiceProvision::new(3);

        let stringified = diesel::debug_query::<Pg, _>(&query).to_string();

        assert_eq!(
            stringified,
            "WITH \
            sled_allocation_pool AS (\
                SELECT \
                    \"sled\".\"id\" \
                FROM \"sled\" \
                WHERE (\
                    \"sled\".\"time_deleted\" IS NULL\
                )\
            ), \
            previously_allocated_services AS (\
                SELECT \
                    \"service\".\"id\", \
                    \"service\".\"time_created\", \
                    \"service\".\"time_modified\", \
                    \"service\".\"sled_id\", \
                    \"service\".\"ip\", \
                    \"service\".\"kind\" \
                FROM \"service\" \
                WHERE (\
                    (\"service\".\"kind\" = $1) AND \
                    (\"service\".\"sled_id\" = \
                        ANY(SELECT \"sled_allocation_pool\".\"id\" FROM \"sled_allocation_pool\")\
                    )\
                )\
            ), \
            old_service_count AS (\
                SELECT COUNT(*) FROM \"previously_allocated_services\"\
            ), \
            new_service_count AS (\
                SELECT (\
                    greatest(\
                        $2, \
                        (SELECT \"old_service_count\".\"count\" FROM \"old_service_count\" LIMIT $3)\
                    ) - (SELECT \"old_service_count\".\"count\" FROM \"old_service_count\" LIMIT $4)\
                )\
            ), \
            candidate_sleds AS (\
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
            new_internal_ips AS (\
                UPDATE \
                    \"sled\" \
                SET \
                    \"last_used_address\" = (\"sled\".\"last_used_address\" + $5) \
                WHERE \
                    (\"sled\".\"id\" = ANY(SELECT \"candidate_sleds\".\"id\" FROM \"candidate_sleds\")) \
                RETURNING \
                    \"sled\".\"id\", \
                    \"sled\".\"last_used_address\"\
            ), \
            candidate_services AS (\
                SELECT \
                    gen_random_uuid() AS id, \
                    now() AS time_created, \
                    now() AS time_modified, \
                    \"candidate_sleds\".\"id\" AS sled_id, \
                    \"new_internal_ips\".\"last_used_address\" AS ip, \
                    $6 AS kind \
                FROM (\
                    \"candidate_sleds\" \
                INNER JOIN \
                    \"new_internal_ips\" \
                ON (\
                    \"candidate_sleds\".\"id\" = \"new_internal_ips\".\"id\"
                ))
            ),
            inserted_services AS (\
                INSERT INTO \
                    \"service\" \
                (\"id\", \"time_created\", \"time_modified\", \"sled_id\", \"ip\", \"kind\") SELECT \"candidate_services\".\"id\", \"candidate_services\".\"time_created\", \"candidate_services\".\"time_modified\", \"candidate_services\".\"sled_id\", \"candidate_services\".\"ip\", \"candidate_services\".\"kind\" FROM \"candidate_services\" RETURNING \"service\".\"id\", \"service\".\"time_created\", \"service\".\"time_modified\", \"service\".\"sled_id\", \"service\".\"ip\", \"service\".\"kind\")
            SELECT \"inserted_services\".\"id\", \"inserted_services\".\"time_created\", \"inserted_services\".\"time_modified\", \"inserted_services\".\"sled_id\", \"inserted_services\".\"ip\", \"inserted_services\".\"kind\" FROM \"inserted_services\"
            ) -- binds: [Nexus, 3, 1, Nexus]",
        );

        context.success().await;
    }
}
