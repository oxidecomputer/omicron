// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for provisioning services.

use crate::db::model::Service;
use crate::db::model::ServiceKind;
use crate::db::model::Sled;
use crate::db::pool::DbConnection;
use crate::db::schema;
use chrono::DateTime;
use chrono::Utc;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::AsQuery;
use diesel::query_builder::Query;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::sql_types;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::RunQueryDsl;

type FromClause<T> =
    diesel::internal::table_macro::StaticQueryFragmentInstance<T>;
type ServiceFromClause = FromClause<schema::service::table>;
const SERVICE_FROM_CLAUSE: ServiceFromClause = ServiceFromClause::new();

trait CteQuery<ST>: Query<SqlType = ST> + QueryFragment<Pg> {}

impl<T, ST> CteQuery<ST> for T
where
    T: Query<SqlType = ST> + QueryFragment<Pg>
{}

/*
trait CteQueryClone<ST> {
    fn clone_box(&self) -> Box<dyn CteQuery<SqlType = ST>>;
}

impl<T, ST> CteQueryClone<ST> for T
where
    T: 'static + CteQuery<SqlType = ST> + Clone,
{
    fn clone_box(&self) -> Box<dyn CteQuery<SqlType = ST>> {
        Box::new(self.clone())
    }
}

impl<ST> Clone for Box<dyn CteQuery<SqlType = ST>> {
    fn clone(&self) -> Box<dyn CteQuery<SqlType = ST>> {
        self.clone_box()
    }
}
*/

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

trait AsTable
where
    Self::Table: AsQuery,
{
    type Table;
    fn as_table(&self) -> Self::Table;
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

trait SameType {}
impl<T> SameType for (T, T) {}
fn same_type<A, B>() where (A, B): SameType {}

// ----------------------------- //
// Above should be for a generic CTE builder
// Below should be for service provisioning
// ----------------------------- //

// TODO: I want this to be as lightweight to make as possible!
struct SledAllocationPoolSubquery {
    // TODO: How do we bridge the gap of this CteQuery type to the
    // table?
    query: Box<dyn CteQuery<(sql_types::Uuid,)>>,
}

impl SledAllocationPoolSubquery {
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

impl AsTable for SledAllocationPoolSubquery {
    type Table = sled_allocation_pool::dsl::sled_allocation_pool;
    fn as_table(&self) -> Self::Table {
        use diesel::internal::table_macro::StaticQueryFragment;

        // TODO: This should either be auto-generated, or checked more
        // uniformally.
        same_type::<(sql_types::Uuid,), sled_allocation_pool::SqlType>();

        // TODO: Converting this to a compile-time check would be nicer.
        //
        // TODO: Even better, don't have "name()" at all... force the alias
        // to be the intermediate "table" name.
        assert_eq!(self.name(), sled_allocation_pool::dsl::sled_allocation_pool::STATIC_COMPONENT.0);

        sled_allocation_pool::dsl::sled_allocation_pool
    }
}

impl SubQuery for SledAllocationPoolSubquery {
    fn name(&self) -> &'static str {
        "sled_allocation_pool"
    }

    fn query(&self) -> &dyn QueryFragment<Pg> {
        &self.query
    }
}

// TODO: We actually want a trimmed down version of this.
// It's generating too much; we don't want to be able to insert/delete/update
// this table; it's basically an alias.
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
    query: Box<dyn CteQuery<crate::db::schema::service::SqlType>>,
}

impl PreviouslyAllocatedServices {
    fn new(allocation_pool: &SledAllocationPoolSubquery) -> Self {
        use crate::db::schema::service::dsl as service_dsl;
        use sled_allocation_pool::dsl as alloc_pool_dsl;

        let select_from_pool = allocation_pool.as_table().select(alloc_pool_dsl::id).into_boxed();
        Self {
            query: Box::new(
                service_dsl::service
                    .filter(service_dsl::kind.eq(ServiceKind::Nexus))
                    .filter(service_dsl::sled_id.eq_any(select_from_pool))
            )
        }
    }
}

impl AsTable for PreviouslyAllocatedServices {
    type Table = previously_allocated_services::dsl::previously_allocated_services;
    fn as_table(&self) -> Self::Table {
        use diesel::internal::table_macro::StaticQueryFragment;
        assert_eq!(self.name(), previously_allocated_services::dsl::previously_allocated_services::STATIC_COMPONENT.0);
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
                previously_allocated_services.as_table().count()
            )
        }
    }
}

impl AsTable for OldServiceCount {
    type Table = old_service_count::dsl::old_service_count;
    fn as_table(&self) -> Self::Table {
        use diesel::internal::table_macro::StaticQueryFragment;
        assert_eq!(self.name(), old_service_count::dsl::old_service_count::STATIC_COMPONENT.0);
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
    redundancy: i32,
}

impl QueryId for NewServiceCount {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl NewServiceCount {
    fn new(redundancy: i32, _old_service_count: &OldServiceCount) -> Self {
        Self {
            redundancy,
        }
    }
}

impl SubQuery for NewServiceCount {
    fn name(&self) -> &'static str {
        "new_service_count"
    }

    fn query(&self) -> &dyn QueryFragment<Pg> {
        self
    }
}

// NOTE: This CTE arm is raw SQL because the "GREATEST" function is not
// supported by Diesel.
impl QueryFragment<Pg> for NewServiceCount {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.unsafe_to_cache_prepared();
        out.push_sql("SELECT GREATEST(");
        out.push_bind_param::<sql_types::Integer, i32>(&self.redundancy)?;
        out.push_sql(", (SELECT * FROM old_service_count)) - (SELECT * FROM old_service_count)");
        Ok(())
    }
}

impl Query for NewServiceCount {
    type SqlType = sql_types::BigInt;
}

struct CandidateSleds {}

impl CandidateSleds {
    fn new() -> Self {
        Self {}
    }
}

impl QueryId for CandidateSleds {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl AsTable for CandidateSleds {
    type Table = candidate_sleds::dsl::candidate_sleds;
    fn as_table(&self) -> Self::Table {
        use diesel::internal::table_macro::StaticQueryFragment;
        assert_eq!(self.name(), candidate_sleds::dsl::candidate_sleds::STATIC_COMPONENT.0);
        candidate_sleds::dsl::candidate_sleds
    }
}

impl SubQuery for CandidateSleds {
    fn name(&self) -> &'static str {
        "candidate_sleds"
    }

    fn query(&self) -> &dyn QueryFragment<Pg> {
        self
    }
}

// NOTE: This CTE arm is raw SQL because the "LIMIT" expression cannot
// include sub-queries in Diesel.
impl QueryFragment<Pg> for CandidateSleds {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.unsafe_to_cache_prepared();

        out.push_sql(
            "SELECT \
                \"sled_allocation_pool\".\"id\" \
            FROM \"sled_allocation_pool\" \
            WHERE \
                \"sled_allocation_pool\".\"id\" NOT IN \
                    (SELECT \"sled_id\" FROM \"previously_allocated_services\") \
            LIMIT (SELECT * FROM \"new_service_count\")"
        );
        Ok(())
    }
}

impl Query for CandidateSleds {
    type SqlType = sql_types::Uuid;
}

diesel::table! {
    candidate_sleds {
        id -> Uuid,
    }
}

struct NewInternalIps {
    query: Box<dyn CteQuery<(sql_types::Uuid, sql_types::Inet)>>,
}

impl NewInternalIps {
    fn new(candidate_sleds: &CandidateSleds) -> Self {
        use crate::db::schema::sled::dsl as sled_dsl;
        use candidate_sleds::dsl as candidate_sleds_dsl;

        let select_from_candidate_sleds = candidate_sleds.as_table().select(candidate_sleds_dsl::id).into_boxed();
        Self {

            query: Box::new(
                diesel::update(sled_dsl::sled.filter(sled_dsl::id.eq_any(select_from_candidate_sleds)))
                    .set(sled_dsl::last_used_address.eq(sled_dsl::last_used_address + 1))
                    .returning((sled_dsl::id, sled_dsl::last_used_address))
            )
        }
    }
}

impl AsTable for NewInternalIps {
    type Table = new_internal_ips::dsl::new_internal_ips;
    fn as_table(&self) -> Self::Table {
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
        let sled_allocation_pool = SledAllocationPoolSubquery::new();
        let previously_allocated_services = PreviouslyAllocatedServices::new(&sled_allocation_pool);
        let old_service_count = OldServiceCount::new(&previously_allocated_services);
        let new_service_count = NewServiceCount::new(redundancy, &old_service_count);
        let candidate_sleds = CandidateSleds::new();
        let new_internal_ips = NewInternalIps::new(&candidate_sleds);

        // TODO: Reference prior subquery?
        use crate::db::schema::sled::dsl;
        let final_select = Box::new(dsl::sled.filter(dsl::time_deleted.is_null()));

        let cte = CteBuilder::new()
            .add_subquery(sled_allocation_pool)
            .add_subquery(previously_allocated_services)
            .add_subquery(old_service_count)
            .add_subquery(new_service_count)
            .add_subquery(candidate_sleds)
            .add_subquery(new_internal_ips)
            .build(final_select);

        Self {
            now,
            cte,
        }
    }
}


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
                SELECT GREATEST($2, (SELECT * FROM old_service_count)) - (SELECT * FROM old_service_count)\
            ), \
            candidate_sleds AS (\
                SELECT \
                    \"sled_allocation_pool\".\"id\" \
                FROM \"sled_allocation_pool\" \
                WHERE \
                    \"sled_allocation_pool\".\"id\" NOT IN \
                        (SELECT \"sled_id\" FROM \"previously_allocated_services\") \
                LIMIT (SELECT * FROM \"new_service_count\")\
            ), \
            new_internal_ips AS (\
                UPDATE \
                    \"sled\" \
                SET \
                    \"last_used_address\" = (\"sled\".\"last_used_address\" + $3) \
                WHERE \
                    (\"sled\".\"id\" = ANY(SELECT \"candidate_sleds\".\"id\" FROM \"candidate_sleds\")) \
                RETURNING \"sled\".\"id\", \"sled\".\"last_used_address\"\
            ) \
            SELECT \
                \"sled\".\"id\", \
                \"sled\".\"time_created\", \
                \"sled\".\"time_modified\", \
                \"sled\".\"time_deleted\", \
                \"sled\".\"rcgen\", \
                \"sled\".\"rack_id\", \
                \"sled\".\"is_scrimlet\", \
                \"sled\".\"ip\", \
                \"sled\".\"port\", \
                \"sled\".\"last_used_address\" \
            FROM \"sled\" \
            WHERE (\
                \"sled\".\"time_deleted\" IS NULL\
            ) -- binds: [Nexus, 3, 1]",
        );

        context.success().await;
    }
}
