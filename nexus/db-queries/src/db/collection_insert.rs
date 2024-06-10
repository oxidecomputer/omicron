// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CTE implementation for inserting a row representing a child resource of a
//! collection. This atomically
//! 1) checks if the collection exists and is not soft deleted, and fails
//!    otherwise
//! 2) updates the collection's child resource generation number
//! 3) inserts the child resource row

use super::pool::DbConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::associations::HasTable;
use diesel::helper_types::*;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::query_dsl::methods as query_methods;
use diesel::query_source::Table;
use diesel::result::Error as DieselError;
use diesel::sql_types::SingleValue;
use nexus_db_model::DatastoreCollectionConfig;
use std::fmt::Debug;
use std::marker::PhantomData;

/// Extension trait adding behavior to types that implement
/// `nexus_db_model::DatastoreCollection`.
pub trait DatastoreCollection<ResourceType>:
    DatastoreCollectionConfig<ResourceType>
{
    /// Create a statement for inserting a resource into the given collection.
    ///
    /// The ISR type is the same type as the second generic argument to
    /// InsertStatement, and should generally be inferred rather than explicitly
    /// specified.
    ///
    /// CAUTION: The API does not currently enforce that `key` matches the value
    /// of the collection id within the inserted row.
    fn insert_resource<ISR>(
        key: Self::CollectionId,
        // Note that InsertStatement's fourth argument defaults to Ret =
        // NoReturningClause. This enforces that the given input statement does
        // not have a RETURNING clause.
        insert: InsertStatement<ResourceTable<ResourceType, Self>, ISR>,
    ) -> InsertIntoCollectionStatement<ResourceType, ISR, Self>
    where
        (
            <Self::GenerationNumberColumn as Column>::Table,
            <Self::CollectionTimeDeletedColumn as Column>::Table,
        ): TypesAreSame,
        Self: Sized,
        // Enables the "table()" method.
        CollectionTable<ResourceType, Self>: HasTable<Table = CollectionTable<ResourceType, Self>>
            + 'static
            + Send
            + Table
            // Allows calling ".into_boxed()" on the table.
            + query_methods::BoxedDsl<
                'static,
                Pg,
                Output = BoxedDslOutput<CollectionTable<ResourceType, Self>>,
            >,
        // Allows treating "filter_subquery" as a boxed "dyn QueryFragment<Pg>".
        <CollectionTable<ResourceType, Self> as QuerySource>::FromClause:
            QueryFragment<Pg> + Send,
        // Allows sending "filter_subquery" between threads.
        <CollectionTable<ResourceType, Self> as AsQuery>::SqlType: Send,
        // Allows calling ".filter()" on the boxed table.
        BoxedQuery<CollectionTable<ResourceType, Self>>:
            query_methods::FilterDsl<
                    Eq<
                        CollectionPrimaryKey<ResourceType, Self>,
                        CollectionId<ResourceType, Self>,
                    >,
                    Output = BoxedQuery<CollectionTable<ResourceType, Self>>,
                > + query_methods::FilterDsl<
                    IsNull<CollectionTimeDeletedColumn<ResourceType, Self>>,
                    Output = BoxedQuery<CollectionTable<ResourceType, Self>>,
                >,
        // Allows using "key" in in ".eq(...)".
        CollectionId<ResourceType, Self>: diesel::expression::AsExpression<
            SerializedCollectionPrimaryKey<ResourceType, Self>,
        >,
        <CollectionPrimaryKey<ResourceType, Self> as Expression>::SqlType:
            SingleValue,
        // Allows calling "is_null()" on the time deleted column.
        CollectionTimeDeletedColumn<ResourceType, Self>: ExpressionMethods,
        // Necessary for output type (`InsertIntoCollectionStatement`).
        ResourceType: Selectable<Pg>,
    {
        let filter_subquery = Box::new(
            <CollectionTable<ResourceType, Self> as HasTable>::table()
                .into_boxed()
                .filter(
                    <CollectionTable<ResourceType, Self> as HasTable>::table()
                        .primary_key()
                        .eq(key),
                )
                .filter(Self::CollectionTimeDeletedColumn::default().is_null()),
        );

        let from_clause =
            <CollectionTable<ResourceType, Self> as HasTable>::table()
                .from_clause();
        let returning_clause = ResourceType::as_returning();
        InsertIntoCollectionStatement {
            insert_statement: insert,
            filter_subquery,
            from_clause,
            returning_clause,
            query_type: PhantomData,
        }
    }
}

impl<T, R> DatastoreCollection<R> for T where T: DatastoreCollectionConfig<R> {}

/// Utility type to make trait bounds below easier to read.
type CollectionId<ResourceType, C> =
    <C as DatastoreCollectionConfig<ResourceType>>::CollectionId;
/// The table representing the collection. The resource references
/// this table.
type CollectionTable<ResourceType, C> = <<C as DatastoreCollectionConfig<
    ResourceType,
>>::GenerationNumberColumn as Column>::Table;
/// The table representing the resource. This table contains an
/// ID acting as a foreign key into the collection table.
type ResourceTable<ResourceType, C> = <<C as DatastoreCollectionConfig<
    ResourceType,
>>::CollectionIdColumn as Column>::Table;
type CollectionTimeDeletedColumn<ResourceType, C> =
    <C as DatastoreCollectionConfig<ResourceType>>::CollectionTimeDeletedColumn;
type GenerationNumberColumn<ResourceType, C> =
    <C as DatastoreCollectionConfig<ResourceType>>::GenerationNumberColumn;

// Trick to check that columns come from the same table
pub trait TypesAreSame {}
impl<T> TypesAreSame for (T, T) {}

/// The CTE described in the module docs
#[must_use = "Queries must be executed"]
pub struct InsertIntoCollectionStatement<ResourceType, ISR, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreCollectionConfig<ResourceType>,
{
    insert_statement: InsertStatement<ResourceTable<ResourceType, C>, ISR>,
    filter_subquery: Box<dyn QueryFragment<Pg> + Send>,
    from_clause: <CollectionTable<ResourceType, C> as QuerySource>::FromClause,
    returning_clause: AsSelect<ResourceType, Pg>,
    query_type: PhantomData<ResourceType>,
}

impl<ResourceType, ISR, C> QueryId
    for InsertIntoCollectionStatement<ResourceType, ISR, C>
where
    C: DatastoreCollectionConfig<ResourceType>,
    ResourceType: Selectable<Pg>,
{
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

/// Result of [`InsertIntoCollectionStatement`] when executed asynchronously
pub type AsyncInsertIntoCollectionResult<Q> = Result<Q, AsyncInsertError>;

/// Errors returned by [`InsertIntoCollectionStatement`].
#[derive(Debug)]
pub enum AsyncInsertError {
    /// The collection that the query was inserting into does not exist
    CollectionNotFound,
    /// Other database error
    DatabaseError(DieselError),
}

impl<ResourceType, ISR, C> InsertIntoCollectionStatement<ResourceType, ISR, C>
where
    ResourceType: 'static + Debug + Send + Selectable<Pg>,
    C: 'static + DatastoreCollectionConfig<ResourceType> + Send,
    CollectionId<ResourceType, C>: 'static + PartialEq + Send,
    ResourceTable<ResourceType, C>: 'static + Table + Send + Copy + Debug,
    ISR: 'static + Send,
    InsertIntoCollectionStatement<ResourceType, ISR, C>: Send,
{
    /// Issues the CTE asynchronously and parses the result.
    ///
    /// The three outcomes are:
    /// - Ok(new row)
    /// - Error(collection not found)
    /// - Error(other diesel error)
    pub async fn insert_and_get_result_async(
        self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> AsyncInsertIntoCollectionResult<ResourceType>
    where
        // We require this bound to ensure that "Self" is runnable as query.
        Self: query_methods::LoadQuery<'static, DbConnection, ResourceType>,
    {
        self.get_result_async::<ResourceType>(conn)
            .await
            .map_err(|e| Self::translate_async_error(e))
    }

    /// Issues the CTE asynchronously and parses the result.
    ///
    /// The four outcomes are:
    /// - Ok(Some(new row))
    /// - Ok(None)
    /// - Error(collection not found)
    /// - Error(other diesel error)
    pub async fn insert_and_get_optional_result_async(
        self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> AsyncInsertIntoCollectionResult<Option<ResourceType>>
    where
        // We require this bound to ensure that "Self" is runnable as query.
        Self: query_methods::LoadQuery<'static, DbConnection, ResourceType>,
    {
        self.get_result_async::<ResourceType>(conn)
            .await
            .optional()
            .map_err(|e| Self::translate_async_error(e))
    }

    /// Issues the CTE asynchronously and parses the result.
    ///
    /// The three outcomes are:
    /// - Ok(Vec of new rows)
    /// - Error(collection not found)
    /// - Error(other diesel error)
    pub async fn insert_and_get_results_async(
        self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> AsyncInsertIntoCollectionResult<Vec<ResourceType>>
    where
        // We require this bound to ensure that "Self" is runnable as query.
        Self: query_methods::LoadQuery<'static, DbConnection, ResourceType>,
    {
        self.get_results_async::<ResourceType>(conn)
            .await
            .map_err(|e| Self::translate_async_error(e))
    }

    /// Check for the intentional division by zero error
    fn error_is_division_by_zero(err: &diesel::result::Error) -> bool {
        match err {
            // See
            // https://rfd.shared.oxide.computer/rfd/0192#_dueling_administrators
            // for a full explanation of why we're checking for this. In
            // summary, the CTE generates a division by zero intentionally
            // if the collection doesn't exist in the database.
            diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::Unknown,
                info,
            ) if info.message() == "division by zero" => true,
            _ => false,
        }
    }

    /// Translate from diesel errors into AsyncInsertError, handling the
    /// intentional division-by-zero error in the CTE.
    fn translate_async_error(err: DieselError) -> AsyncInsertError {
        if Self::error_is_division_by_zero(&err) {
            AsyncInsertError::CollectionNotFound
        } else {
            AsyncInsertError::DatabaseError(err)
        }
    }
}

type SelectableSqlType<Q> =
    <<Q as diesel::Selectable<Pg>>::SelectExpression as Expression>::SqlType;

impl<ResourceType, ISR, C> Query
    for InsertIntoCollectionStatement<ResourceType, ISR, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreCollectionConfig<ResourceType>,
{
    type SqlType = SelectableSqlType<ResourceType>;
}

impl<ResourceType, ISR, C> RunQueryDsl<DbConnection>
    for InsertIntoCollectionStatement<ResourceType, ISR, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreCollectionConfig<ResourceType>,
{
}

// Representation of Primary Key in Rust.
type CollectionPrimaryKey<ResourceType, C> =
    <CollectionTable<ResourceType, C> as Table>::PrimaryKey;
// Representation of Primary Key in SQL.
type SerializedCollectionPrimaryKey<ResourceType, C> =
    <CollectionPrimaryKey<ResourceType, C> as diesel::Expression>::SqlType;

type TableSqlType<T> = <T as AsQuery>::SqlType;

type BoxedQuery<T> = diesel::helper_types::IntoBoxed<'static, T, Pg>;
type BoxedDslOutput<T> = diesel::internal::table_macro::BoxedSelectStatement<
    'static,
    TableSqlType<T>,
    diesel::internal::table_macro::FromClause<T>,
    Pg,
>;

/// This implementation uses the following CTE:
///
/// ```text
/// // WITH found_row AS MATERIALIZED (
/// //          SELECT <PK> FROM C WHERE <PK> = <value> AND
/// //              <time_deleted> IS NULL FOR UPDATE),
/// //      dummy AS MATERIALIZED (
/// //          SELECT IF(EXISTS(SELECT <PK> FROM found_row), TRUE,
/// //              CAST(1/0 AS BOOL))),
/// //      updated_row AS MATERIALIZED (
/// //          UPDATE C SET <generation number> = <generation_number> + 1 WHERE
/// //              <PK> IN (SELECT <PK> FROM found_row) RETURNING 1),
/// //      inserted_row AS (<user provided insert statement>
/// //          RETURNING <ResourceType.as_returning()>)
/// //  SELECT * FROM inserted_row;
/// ```
///
/// This CTE is equivalent in desired behavior to the one specified in
/// [RFD 192](https://rfd.shared.oxide.computer/rfd/0192#_dueling_administrators).
///
/// The general idea is that the first clause of the CTE (the "dummy" table)
/// will generate a divison-by-zero error and rollback the transaction if the
/// target collection is not found in its table. It simultaneously locks the
/// row for update, to allow us to subsequently use the "updated_row" query to
/// increase the child-resource generation count for the collection. In the same
/// transaction, it performs the provided insert query, which should
/// insert a new resource into its table with its collection id column set
/// to the collection we just checked for.
///
/// NOTE: It is important that the WHERE clauses on the SELECT and UPDATE
/// against the collection table must match, or else we will not get the desired
/// behavior described in RFD 192.
/// NOTE: It is important that the WITH clauses have MATERIALIZED, since under
/// some conditions, clauses may be inlined (and potentially eliminated by
/// consequence of being unused). At the time of writing this, this happens
/// for the "dummy" table, preventing the division-by-zero error from occuring.
/// The MATERIALIZED keyword forces the queries that are not referenced
/// to be materialized instead.
impl<ResourceType, ISR, C> QueryFragment<Pg>
    for InsertIntoCollectionStatement<ResourceType, ISR, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreCollectionConfig<ResourceType>,
    CollectionPrimaryKey<ResourceType, C>: diesel::Column,
    // Necessary to "walk_ast" over "select.from_clause".
    <CollectionTable<ResourceType, C> as QuerySource>::FromClause:
        QueryFragment<Pg>,
    // Necessary to "walk_ast" over "self.insert_statement".
    InsertStatement<ResourceTable<ResourceType, C>, ISR>: QueryFragment<Pg>,
    // Necessary to "walk_ast" over "self.returning_clause".
    AsSelect<ResourceType, Pg>: QueryFragment<Pg>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_sql("WITH found_row AS MATERIALIZED (");
        self.filter_subquery.walk_ast(out.reborrow())?;
        // Manually add the FOR_UPDATE, since .for_update() is incompatible with
        // BoxedQuery
        out.push_sql(" FOR UPDATE), ");
        out.push_sql(
            "dummy AS MATERIALIZED (\
                SELECT IF(EXISTS(SELECT ",
        );
        out.push_identifier(CollectionPrimaryKey::<ResourceType, C>::NAME)?;
        out.push_sql(" FROM found_row), TRUE, CAST(1/0 AS BOOL))), ");

        // Write the update manually instead of with the dsl, to avoid the
        // explosion in complexity of type traits
        out.push_sql("updated_row AS MATERIALIZED (UPDATE ");
        self.from_clause.walk_ast(out.reborrow())?;
        out.push_sql(" SET ");
        out.push_identifier(GenerationNumberColumn::<ResourceType, C>::NAME)?;
        out.push_sql(" = ");
        out.push_identifier(GenerationNumberColumn::<ResourceType, C>::NAME)?;
        out.push_sql(" + 1 WHERE ");
        out.push_identifier(CollectionPrimaryKey::<ResourceType, C>::NAME)?;
        out.push_sql(" IN (SELECT ");
        // We must include "RETURNING 1" since all CTE clauses must return
        // something
        out.push_identifier(CollectionPrimaryKey::<ResourceType, C>::NAME)?;
        out.push_sql(" FROM found_row) RETURNING 1), ");

        out.push_sql("inserted_row AS (");
        // TODO: Check or force the insert_statement to have
        //       C::CollectionIdColumn set
        self.insert_statement.walk_ast(out.reborrow())?;
        out.push_sql(" RETURNING ");
        // We manually write the RETURNING clause here because the wrapper type
        // used for InsertStatement's Ret generic is private to diesel and so we
        // cannot express it.
        self.returning_clause.walk_ast(out.reborrow())?;

        out.push_sql(") SELECT * FROM inserted_row");
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::{self, identity::Resource as IdentityResource};
    use async_bb8_diesel::{AsyncRunQueryDsl, AsyncSimpleConnection};
    use chrono::{DateTime, Utc};
    use db_macros::Resource;
    use diesel::expression_methods::ExpressionMethods;
    use diesel::pg::Pg;
    use diesel::QueryDsl;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;

    table! {
        test_schema.collection (id) {
            id -> Uuid,
            name -> Text,
            description -> Text,
            time_created -> Timestamptz,
            time_modified -> Timestamptz,
            time_deleted -> Nullable<Timestamptz>,
            rcgen -> Int8,
        }
    }

    table! {
        test_schema.resource (id) {
            id -> Uuid,
            name -> Text,
            description -> Text,
            time_created -> Timestamptz,
            time_modified -> Timestamptz,
            time_deleted -> Nullable<Timestamptz>,
            collection_id -> Uuid,
        }
    }

    async fn setup_db(
        pool: &crate::db::Pool,
    ) -> crate::db::datastore::DataStoreConnection {
        let connection = pool.claim().await.unwrap();
        (*connection)
            .batch_execute_async(
                "CREATE SCHEMA IF NOT EXISTS test_schema; \
                 CREATE TABLE IF NOT EXISTS test_schema.collection ( \
                     id UUID PRIMARY KEY, \
                     name STRING(63) NOT NULL, \
                     description STRING(512) NOT NULL, \
                     time_created TIMESTAMPTZ NOT NULL, \
                     time_modified TIMESTAMPTZ NOT NULL, \
                     time_deleted TIMESTAMPTZ, \
                     rcgen INT NOT NULL); \
                 CREATE TABLE IF NOT EXISTS test_schema.resource( \
                     id UUID PRIMARY KEY, \
                     name STRING(63) NOT NULL, \
                     description STRING(512) NOT NULL, \
                     time_created TIMESTAMPTZ NOT NULL, \
                     time_modified TIMESTAMPTZ NOT NULL, \
                     time_deleted TIMESTAMPTZ, \
                     collection_id UUID NOT NULL); \
                 TRUNCATE test_schema.collection; \
                 TRUNCATE test_schema.resource",
            )
            .await
            .unwrap();
        connection
    }

    /// Describes an organization within the database.
    #[derive(Queryable, Insertable, Debug, Resource, Selectable)]
    #[diesel(table_name = resource)]
    struct Resource {
        #[diesel(embed)]
        pub identity: ResourceIdentity,

        pub collection_id: uuid::Uuid,
    }

    struct Collection;
    impl DatastoreCollectionConfig<Resource> for Collection {
        type CollectionId = uuid::Uuid;
        type GenerationNumberColumn = collection::dsl::rcgen;
        type CollectionIdColumn = resource::dsl::collection_id;
        type CollectionTimeDeletedColumn = collection::dsl::time_deleted;
    }

    #[test]
    fn test_verify_query() {
        let collection_id =
            uuid::Uuid::parse_str("223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d0")
                .unwrap();
        let resource_id =
            uuid::Uuid::parse_str("223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d8")
                .unwrap();
        let create_time = DateTime::from_timestamp(0, 0).unwrap();
        let modify_time = DateTime::from_timestamp(1, 0).unwrap();
        let insert = Collection::insert_resource(
            collection_id,
            diesel::insert_into(resource::table).values(vec![(
                resource::dsl::id.eq(resource_id),
                resource::dsl::name.eq("test"),
                resource::dsl::description.eq("desc"),
                resource::dsl::time_created.eq(create_time),
                resource::dsl::time_modified.eq(modify_time),
                resource::dsl::collection_id.eq(collection_id),
            )]),
        );
        let query = diesel::debug_query::<Pg, _>(&insert).to_string();

        let expected_query = "WITH \
             found_row AS MATERIALIZED (SELECT \
                     \"test_schema\".\"collection\".\"id\", \
                     \"test_schema\".\"collection\".\"name\", \
                     \"test_schema\".\"collection\".\"description\", \
                     \"test_schema\".\"collection\".\"time_created\", \
                     \"test_schema\".\"collection\".\"time_modified\", \
                     \"test_schema\".\"collection\".\"time_deleted\", \
                     \"test_schema\".\"collection\".\"rcgen\" \
                     FROM \"test_schema\".\"collection\" WHERE (\
                     (\"test_schema\".\"collection\".\"id\" = $1) AND \
                     (\"test_schema\".\"collection\".\"time_deleted\" IS NULL)\
                     ) FOR UPDATE), \
             dummy AS MATERIALIZED (SELECT IF(\
                 EXISTS(SELECT \"id\" FROM found_row), \
                 TRUE, CAST(1/0 AS BOOL))), \
             updated_row AS MATERIALIZED (UPDATE \
                 \"test_schema\".\"collection\" SET \"rcgen\" = \"rcgen\" + 1 \
                 WHERE \"id\" IN (SELECT \"id\" FROM found_row) RETURNING 1), \
             inserted_row AS (INSERT INTO \"test_schema\".\"resource\" \
                 (\"id\", \"name\", \"description\", \"time_created\", \
                  \"time_modified\", \"collection_id\") \
                 VALUES ($2, $3, $4, $5, $6, $7) \
                 RETURNING \"test_schema\".\"resource\".\"id\", \
                           \"test_schema\".\"resource\".\"name\", \
                           \"test_schema\".\"resource\".\"description\", \
                           \"test_schema\".\"resource\".\"time_created\", \
                           \"test_schema\".\"resource\".\"time_modified\", \
                           \"test_schema\".\"resource\".\"time_deleted\", \
                           \"test_schema\".\"resource\".\"collection_id\") \
            SELECT * FROM inserted_row \
        -- binds: [223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d0, \
                   223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d8, \
                   \"test\", \
                   \"desc\", \
                   1970-01-01T00:00:00Z, \
                   1970-01-01T00:00:01Z, \
                   223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d0]";

        assert_eq!(query, expected_query);
    }

    #[tokio::test]
    async fn test_collection_not_present() {
        let logctx = dev::test_setup_log("test_collection_not_present");
        let mut db = test_setup_database(&logctx.log).await;
        let cfg = db::Config { url: db.pg_config().clone() };
        let pool = db::Pool::new_qorb_single_host_blocking(&cfg).await;

        let conn = setup_db(&pool).await;

        let collection_id = uuid::Uuid::new_v4();
        let resource_id = uuid::Uuid::new_v4();
        let insert = Collection::insert_resource(
            collection_id,
            diesel::insert_into(resource::table).values((
                resource::dsl::id.eq(resource_id),
                resource::dsl::name.eq("test"),
                resource::dsl::description.eq("desc"),
                resource::dsl::time_created.eq(Utc::now()),
                resource::dsl::time_modified.eq(Utc::now()),
                resource::dsl::collection_id.eq(collection_id),
            )),
        )
        .insert_and_get_result_async(&conn)
        .await;
        assert!(matches!(insert, Err(AsyncInsertError::CollectionNotFound)));

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_collection_present() {
        let logctx = dev::test_setup_log("test_collection_present");
        let mut db = test_setup_database(&logctx.log).await;
        let cfg = db::Config { url: db.pg_config().clone() };
        let pool = db::Pool::new_qorb_single_host_blocking(&cfg).await;

        let conn = setup_db(&pool).await;

        let collection_id = uuid::Uuid::new_v4();
        let resource_id = uuid::Uuid::new_v4();

        // Insert the collection so it's present later
        diesel::insert_into(collection::table)
            .values(vec![(
                collection::dsl::id.eq(collection_id),
                collection::dsl::name.eq("test"),
                collection::dsl::description.eq("desc"),
                collection::dsl::time_created.eq(Utc::now()),
                collection::dsl::time_modified.eq(Utc::now()),
                collection::dsl::rcgen.eq(1),
            )])
            .execute_async(&*conn)
            .await
            .unwrap();

        let create_time = DateTime::from_timestamp(0, 0).unwrap();
        let modify_time = DateTime::from_timestamp(1, 0).unwrap();
        let resource = Collection::insert_resource(
            collection_id,
            diesel::insert_into(resource::table).values(vec![(
                resource::dsl::id.eq(resource_id),
                resource::dsl::name.eq("test"),
                resource::dsl::description.eq("desc"),
                resource::dsl::time_created.eq(create_time),
                resource::dsl::time_modified.eq(modify_time),
                resource::dsl::collection_id.eq(collection_id),
            )]),
        )
        .insert_and_get_result_async(&conn)
        .await
        .unwrap();
        assert_eq!(resource.id(), resource_id);
        assert_eq!(resource.name().as_str(), "test");
        assert_eq!(resource.description(), "desc");
        assert_eq!(resource.time_created(), create_time);
        assert_eq!(resource.time_modified(), modify_time);
        assert_eq!(resource.collection_id, collection_id);

        let collection_rcgen = collection::table
            .find(collection_id)
            .select(collection::dsl::rcgen)
            .first_async::<i64>(&*conn)
            .await
            .unwrap();

        // Make sure rcgen got incremented
        assert_eq!(collection_rcgen, 2);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
