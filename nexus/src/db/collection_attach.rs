// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CTE implementation for updating a row representing a child resource of a
//! collection. This atomically
//! 1) Checks if the collection exists and is not soft deleted, and fails
//!    otherwise
//! 2) Updates the collection's child resource generation number
//! 3) Updates the child resource row

use super::pool::DbConnection;
use async_bb8_diesel::{
    AsyncRunQueryDsl, ConnectionError, ConnectionManager, PoolError,
};
use diesel::associations::HasTable;
use diesel::helper_types::*;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::query_dsl::methods as query_methods;
use diesel::query_source::Table;
use diesel::sql_types::SingleValue;
use std::fmt::Debug;
use std::marker::PhantomData;

/// Trait to be implemented by structs representing an attachable collection.
///
/// For example, since Instances have a one-to-many relationship with
/// Disks, the Instance datatype should implement this trait.
/// ```
/// # use diesel::prelude::*;
/// # use omicron_nexus::db::collection_attach::DatastoreAttachTarget;
/// # use omicron_nexus::db::model::Generation;
/// #
/// # table! {
/// #     test_schema.instance (id) {
/// #         id -> Uuid,
/// #         time_deleted -> Nullable<Timestamptz>,
/// #         rcgen -> Int8,
/// #     }
/// # }
/// #
/// # table! {
/// #     test_schema.disk (id) {
/// #         id -> Uuid,
/// #         time_deleted -> Nullable<Timestamptz>,
/// #         rcgen -> Int8,
/// #         instance_id -> Nullable<Uuid>,
/// #     }
/// # }
///
/// #[derive(Queryable, Debug, Selectable)]
/// #[diesel(table_name = disk)]
/// struct Disk {
///     pub id: uuid::Uuid,
///     pub time_deleted: Option<chrono::DateTime<chrono::Utc>>,
///     pub rcgen: Generation,
///     pub instance_id: Option<uuid::Uuid>,
/// }
///
/// #[derive(Queryable, Debug, Selectable)]
/// #[diesel(table_name = instance)]
/// struct Instance {
///     pub id: uuid::Uuid,
///     pub time_deleted: Option<chrono::DateTime<chrono::Utc>>,
///     pub rcgen: Generation,
/// }
///
/// impl DatastoreAttachTarget<Disk> for Instance {
///     // Type of Instance::identity::id.
///     type CollectionId = uuid::Uuid;
///
///     type ParentGenerationColumn = instance::dsl::rcgen;
///     type ParentTimeDeletedColumn = instance::dsl::time_deleted;
///
///     type ChildCollectionIdColumn = disk::dsl::instance_id;
/// }
/// ```
pub trait DatastoreAttachTarget<ResourceType> {
    /// The Rust type of the collection id (typically Uuid for us)
    type CollectionId: Copy + Debug;

    /// The column in the CollectionTable that acts as a generation number.
    /// This is the "child-resource-generation-number" in RFD 192.
    type ParentGenerationColumn: Column + Default;

    /// The time deleted column in the CollectionTable
    // We enforce that this column comes from the same table as
    // ParentGenerationColumn when defining attach_resource() below.
    type ParentTimeDeletedColumn: Column + Default;

    /// The column in the ResourceType that acts as a foreign key into
    /// the CollectionTable
    type ChildCollectionIdColumn: Column;

    /// Create a statement for attaching a resource to the given collection.
    ///
    /// The U, V types are the same type as the 3rd and 4th generic arguments to
    /// UpdateStatement, and should generally be inferred rather than explicitly
    /// specified.
    ///
    /// CAUTION: The API does not currently enforce that `key` matches the value
    /// of the collection id within the attached row.
    fn attach_resource<U, V>(
        key: Self::CollectionId,
        // TODO: I'd like to be able to add some filters on the parent type too.
        // For example, checking the instance state.

        // Note that UpdateStatement's fourth argument defaults to Ret =
        // NoReturningClause. This enforces that the given input statement does
        // not have a RETURNING clause.
        update: UpdateStatement<ResourceTable<ResourceType, Self>, U, V>,
    ) -> AttachToCollectionStatement<ResourceType, U, V, Self>
    where
        (
            <Self::ParentGenerationColumn as Column>::Table,
            <Self::ParentTimeDeletedColumn as Column>::Table,
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
                    IsNull<ParentTimeDeletedColumn<ResourceType, Self>>,
                    Output = BoxedQuery<CollectionTable<ResourceType, Self>>,
                >,
        // Allows using "key" in in ".eq(...)".
        CollectionId<ResourceType, Self>: diesel::expression::AsExpression<
            SerializedCollectionPrimaryKey<ResourceType, Self>,
        >,
        <CollectionPrimaryKey<ResourceType, Self> as Expression>::SqlType:
            SingleValue,
        // Allows calling "is_null()" on the time deleted column.
        ParentTimeDeletedColumn<ResourceType, Self>: ExpressionMethods,
        // Necessary for output type (`AttachToCollectionStatement`).
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
                .filter(Self::ParentTimeDeletedColumn::default().is_null()),
        );

        let from_clause =
            <CollectionTable<ResourceType, Self> as HasTable>::table()
                .from_clause();
        let returning_clause = ResourceType::as_returning();
        AttachToCollectionStatement {
            update_statement: update,
            filter_subquery,
            from_clause,
            returning_clause,
            query_type: PhantomData,
        }
    }
}

/// Utility type to make trait bounds below easier to read.
type CollectionId<ResourceType, C> =
    <C as DatastoreAttachTarget<ResourceType>>::CollectionId;
/// The table representing the collection. The resource references
/// this table.
type CollectionTable<ResourceType, C> = <<C as DatastoreAttachTarget<
    ResourceType,
>>::ParentGenerationColumn as Column>::Table;
/// The table representing the resource. This table contains an
/// ID acting as a foreign key into the collection table.
type ResourceTable<ResourceType, C> = <<C as DatastoreAttachTarget<
    ResourceType,
>>::ChildCollectionIdColumn as Column>::Table;
type ParentTimeDeletedColumn<ResourceType, C> =
    <C as DatastoreAttachTarget<ResourceType>>::ParentTimeDeletedColumn;
type ParentGenerationColumn<ResourceType, C> =
    <C as DatastoreAttachTarget<ResourceType>>::ParentGenerationColumn;

// Trick to check that columns come from the same table
pub trait TypesAreSame {}
impl<T> TypesAreSame for (T, T) {}

/// The CTE described in the module docs
#[must_use = "Queries must be executed"]
pub struct AttachToCollectionStatement<ResourceType, U, V, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreAttachTarget<ResourceType>,
{
    update_statement: UpdateStatement<ResourceTable<ResourceType, C>, U, V>,
    filter_subquery: Box<dyn QueryFragment<Pg> + Send>,
    from_clause: <CollectionTable<ResourceType, C> as QuerySource>::FromClause,
    returning_clause: AsSelect<ResourceType, Pg>,
    query_type: PhantomData<ResourceType>,
}

impl<ResourceType, U, V, C> QueryId
    for AttachToCollectionStatement<ResourceType, U, V, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreAttachTarget<ResourceType>,
{
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

/// Result of [`AttachToCollectionStatement`] when executed asynchronously
pub type AsyncAttachToCollectionResult<Q> = Result<Q, AsyncInsertError>;

/// Result of [`AttachToCollectionStatement`] when executed synchronously
pub type SyncAttachToCollectionResult<Q> = Result<Q, SyncInsertError>;

/// Errors returned by [`AttachToCollectionStatement`].
#[derive(Debug)]
pub enum AsyncInsertError {
    /// The collection that the query was inserting into does not exist
    CollectionNotFound,
    /// Other database error
    DatabaseError(PoolError),
}

/// Errors returned by [`AttachToCollectionStatement`].
#[derive(Debug)]
pub enum SyncInsertError {
    /// The collection that the query was inserting into does not exist
    CollectionNotFound,
    /// Other database error
    DatabaseError(diesel::result::Error),
}

impl<ResourceType, U, V, C> AttachToCollectionStatement<ResourceType, U, V, C>
where
    ResourceType: 'static + Debug + Send + Selectable<Pg>,
    C: 'static + DatastoreAttachTarget<ResourceType> + Send,
    CollectionId<ResourceType, C>: 'static + PartialEq + Send,
    ResourceTable<ResourceType, C>: 'static + Table + Send + Copy + Debug,
    U: 'static + Send,
    V: 'static + Send,
    AttachToCollectionStatement<ResourceType, U, V, C>: Send,
{
    /// Issues the CTE asynchronously and parses the result.
    ///
    /// The three outcomes are:
    /// - Ok(new row)
    /// - Error(collection not found)
    /// - Error(other diesel error)
    pub async fn attach_and_get_result_async(
        self,
        pool: &bb8::Pool<ConnectionManager<DbConnection>>,
    ) -> AsyncAttachToCollectionResult<ResourceType>
    where
        // We require this bound to ensure that "Self" is runnable as query.
        Self: query_methods::LoadQuery<'static, DbConnection, ResourceType>,
    {
        self.get_result_async::<ResourceType>(pool)
            .await
            .map_err(Self::translate_async_error)
    }

    /// Issues the CTE asynchronously and parses the result.
    ///
    /// The three outcomes are:
    /// - Ok(Vec of new rows)
    /// - Error(collection not found)
    /// - Error(other diesel error)
    pub async fn attach_and_get_results_async(
        self,
        pool: &bb8::Pool<ConnectionManager<DbConnection>>,
    ) -> AsyncAttachToCollectionResult<Vec<ResourceType>>
    where
        // We require this bound to ensure that "Self" is runnable as query.
        Self: query_methods::LoadQuery<'static, DbConnection, ResourceType>,
    {
        self.get_results_async::<ResourceType>(pool)
            .await
            .map_err(Self::translate_async_error)
    }

    /// Issues the CTE synchronously and parses the result.
    ///
    /// The three outcomes are:
    /// - Ok(new row)
    /// - Error(collection not found)
    /// - Error(other diesel error)
    pub fn attach_and_get_result(
        self,
        conn: &mut DbConnection,
    ) -> SyncAttachToCollectionResult<ResourceType>
    where
        // We require this bound to ensure that "Self" is runnable as query.
        Self: query_methods::LoadQuery<'static, DbConnection, ResourceType>,
    {
        self.get_result::<ResourceType>(conn)
            .map_err(Self::translate_sync_error)
    }

    /// Issues the CTE synchronously and parses the result.
    ///
    /// The three outcomes are:
    /// - Ok(Vec of new rows)
    /// - Error(collection not found)
    /// - Error(other diesel error)
    pub fn attach_and_get_results(
        self,
        conn: &mut DbConnection,
    ) -> SyncAttachToCollectionResult<Vec<ResourceType>>
    where
        // We require this bound to ensure that "Self" is runnable as query.
        Self: query_methods::LoadQuery<'static, DbConnection, ResourceType>,
    {
        self.get_results::<ResourceType>(conn)
            .map_err(Self::translate_sync_error)
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
    fn translate_async_error(err: PoolError) -> AsyncInsertError {
        match err {
            PoolError::Connection(ConnectionError::Query(err))
                if Self::error_is_division_by_zero(&err) =>
            {
                AsyncInsertError::CollectionNotFound
            }
            other => AsyncInsertError::DatabaseError(other),
        }
    }

    /// Translate from diesel errors into SyncInsertError, handling the
    /// intentional division-by-zero error in the CTE.
    fn translate_sync_error(err: diesel::result::Error) -> SyncInsertError {
        if Self::error_is_division_by_zero(&err) {
            SyncInsertError::CollectionNotFound
        } else {
            SyncInsertError::DatabaseError(err)
        }
    }
}

type SelectableSqlType<Q> =
    <<Q as diesel::Selectable<Pg>>::SelectExpression as Expression>::SqlType;

impl<ResourceType, U, V, C> Query
    for AttachToCollectionStatement<ResourceType, U, V, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreAttachTarget<ResourceType>,
{
    type SqlType = SelectableSqlType<ResourceType>;
}

impl<ResourceType, U, V, C> RunQueryDsl<DbConnection>
    for AttachToCollectionStatement<ResourceType, U, V, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreAttachTarget<ResourceType>,
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
/// //      updated_parent_row AS MATERIALIZED (
/// //          UPDATE C SET <generation number> = <generation_number> + 1 WHERE
/// //              <PK> IN (SELECT <PK> FROM found_row) RETURNING 1),
/// //      updated_resource_row AS (<user provided update statement>
/// //          RETURNING <ResourceType.as_returning()>)
/// //  SELECT * FROM updated_resource_row;
/// ```
///
/// This CTE is equivalent in desired behavior to the one specified in
/// [RFD 192](https://rfd.shared.oxide.computer/rfd/0192#_dueling_administrators).
///
/// The general idea is that the first clause of the CTE (the "dummy" table)
/// will generate a divison-by-zero error and rollback the transaction if the
/// target collection is not found in its table. It simultaneously locks the
/// row for update, to allow us to subsequently use the "updated_parent_row" query to
/// increase the child-resource generation count for the collection. In the same
/// transaction, it performs the provided update statement, which should
/// update the child resource, referencing the collection ID to the parent
/// collection we just checked for.
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
impl<ResourceType, U, V, C> QueryFragment<Pg>
    for AttachToCollectionStatement<ResourceType, U, V, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreAttachTarget<ResourceType>,
    CollectionPrimaryKey<ResourceType, C>: diesel::Column,
    // Necessary to "walk_ast" over "select.from_clause".
    <CollectionTable<ResourceType, C> as QuerySource>::FromClause:
        QueryFragment<Pg>,
    // Necessary to "walk_ast" over "self.update_statement".
    UpdateStatement<ResourceTable<ResourceType, C>, U, V>: QueryFragment<Pg>,
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
        out.push_sql("updated_parent_row AS MATERIALIZED (UPDATE ");
        self.from_clause.walk_ast(out.reborrow())?;
        out.push_sql(" SET ");
        out.push_identifier(ParentGenerationColumn::<ResourceType, C>::NAME)?;
        out.push_sql(" = ");
        out.push_identifier(ParentGenerationColumn::<ResourceType, C>::NAME)?;
        out.push_sql(" + 1 WHERE ");
        out.push_identifier(CollectionPrimaryKey::<ResourceType, C>::NAME)?;
        out.push_sql(" IN (SELECT ");
        // We must include "RETURNING 1" since all CTE clauses must return
        // something
        out.push_identifier(CollectionPrimaryKey::<ResourceType, C>::NAME)?;
        out.push_sql(" FROM found_row) RETURNING 1), ");

        out.push_sql("updated_resource_row AS (");
        // TODO: Check or force the update_statement to have
        //       C::ChildCollectionIdColumn set
        self.update_statement.walk_ast(out.reborrow())?;
        out.push_sql(" RETURNING ");
        // We manually write the RETURNING clause here because the wrapper type
        // used for UpdateStatement's Ret generic is private to diesel and so we
        // cannot express it.
        self.returning_clause.walk_ast(out.reborrow())?;

        out.push_sql(") SELECT * FROM updated_resource_row");
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{AsyncInsertError, DatastoreAttachTarget, SyncInsertError};
    use crate::db::{
        self, error::TransactionError, identity::Resource as IdentityResource,
    };
    use async_bb8_diesel::{
        AsyncConnection, AsyncRunQueryDsl, AsyncSimpleConnection,
    };
    use chrono::{DateTime, NaiveDateTime, Utc};
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
            rcgen -> Int8,
            collection_id -> Nullable<Uuid>,
        }
    }

    async fn setup_db(pool: &crate::db::Pool) {
        let connection = pool.pool().get().await.unwrap();
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
                     rcgen INT NOT NULL, \
                     collection_id UUID); \
                 TRUNCATE test_schema.collection; \
                 TRUNCATE test_schema.resource",
            )
            .await
            .unwrap();
    }

    /// Describes a resource within the database.
    #[derive(Queryable, Insertable, Debug, Resource, Selectable)]
    #[diesel(table_name = resource)]
    struct Resource {
        #[diesel(embed)]
        pub identity: ResourceIdentity,

        pub rcgen: i64,
        pub collection_id: Option<uuid::Uuid>,
    }

    struct Collection;
    impl DatastoreAttachTarget<Resource> for Collection {
        type CollectionId = uuid::Uuid;

        type ParentGenerationColumn = collection::dsl::rcgen;
        type ParentTimeDeletedColumn = collection::dsl::time_deleted;

        type ChildCollectionIdColumn = resource::dsl::collection_id;
    }

    #[test]
    fn test_verify_query() {
        let collection_id =
            uuid::Uuid::parse_str("223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d0")
                .unwrap();
        let resource_id =
            uuid::Uuid::parse_str("223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d8")
                .unwrap();
        let attach = Collection::attach_resource(
            collection_id,
            diesel::update(resource::table.filter(resource::dsl::id.eq(resource_id)))
                .set(resource::dsl::collection_id.eq(collection_id))
        );
        let query = diesel::debug_query::<Pg, _>(&attach).to_string();

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
             updated_parent_row AS MATERIALIZED (UPDATE \
                 \"test_schema\".\"collection\" SET \"rcgen\" = \"rcgen\" + 1 \
                 WHERE \"id\" IN (SELECT \"id\" FROM found_row) RETURNING 1), \
             updated_resource_row AS (UPDATE \"test_schema\".\"resource\" \
                 SET \"collection_id\" = $2 \
                 WHERE (\"test_schema\".\"resource\".\"id\" = $3) \
                 RETURNING \"test_schema\".\"resource\".\"id\", \
                           \"test_schema\".\"resource\".\"name\", \
                           \"test_schema\".\"resource\".\"description\", \
                           \"test_schema\".\"resource\".\"time_created\", \
                           \"test_schema\".\"resource\".\"time_modified\", \
                           \"test_schema\".\"resource\".\"time_deleted\", \
                           \"test_schema\".\"resource\".\"rcgen\", \
                           \"test_schema\".\"resource\".\"collection_id\") \
            SELECT * FROM updated_resource_row \
        -- binds: [223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d0, \
                   223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d0, \
                   223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d8]";

        assert_eq!(query, expected_query);
    }

    #[tokio::test]
    async fn test_collection_not_present() {
        let logctx = dev::test_setup_log("test_collection_not_present");
        let mut db = test_setup_database(&logctx.log).await;
        let cfg = db::Config { url: db.pg_config().clone() };
        let pool = db::Pool::new(&cfg);

        setup_db(&pool).await;

        let collection_id = uuid::Uuid::new_v4();
        let resource_id = uuid::Uuid::new_v4();
        let attach = Collection::attach_resource(
            collection_id,
            diesel::update(resource::table.filter(resource::dsl::id.eq(resource_id)))
                .set(resource::dsl::collection_id.eq(collection_id))
        )
        .attach_and_get_result_async(pool.pool())
        .await;
        assert!(matches!(attach, Err(AsyncInsertError::CollectionNotFound)));

        let attach_query = Collection::attach_resource(
            collection_id,
            diesel::update(resource::table.filter(resource::dsl::id.eq(resource_id)))
                .set(resource::dsl::collection_id.eq(collection_id))
        );

        #[derive(Debug)]
        enum CollectionError {
            NotFound,
        }
        type TxnError = TransactionError<CollectionError>;

        let result = pool
            .pool()
            .transaction(move |conn| {
                attach_query.attach_and_get_result(conn).map_err(|e| match e {
                    SyncInsertError::CollectionNotFound => {
                        TxnError::CustomError(CollectionError::NotFound)
                    }
                    SyncInsertError::DatabaseError(e) => TxnError::from(e),
                })
            })
            .await;

        assert!(matches!(
            result,
            Err(TxnError::CustomError(CollectionError::NotFound))
        ));

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_collection_present() {
        let logctx = dev::test_setup_log("test_collection_present");
        let mut db = test_setup_database(&logctx.log).await;
        let cfg = db::Config { url: db.pg_config().clone() };
        let pool = db::Pool::new(&cfg);

        setup_db(&pool).await;

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
            .execute_async(pool.pool())
            .await
            .unwrap();

        // Insert the resource so it's present later
        let insert_time =
            DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc);
        diesel::insert_into(resource::table)
            .values(vec![(
                resource::dsl::id.eq(resource_id),
                resource::dsl::name.eq("test"),
                resource::dsl::description.eq("desc"),
                resource::dsl::time_created.eq(insert_time),
                resource::dsl::time_modified.eq(insert_time),
                resource::dsl::rcgen.eq(1),
                resource::dsl::collection_id.eq(Option::<uuid::Uuid>::None),
            )])
            .execute_async(pool.pool())
            .await
            .unwrap();

        // Attempt to attach the resource.
        let update_time =
            DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(1, 0), Utc);
        let resource = Collection::attach_resource(
            collection_id,
            diesel::update(
                resource::table
                    .filter(resource::dsl::id.eq(resource_id))
                    .filter(resource::dsl::time_deleted.is_null())
            ).set((
                resource::dsl::collection_id.eq(collection_id),
                resource::dsl::time_modified.eq(update_time),
                resource::dsl::rcgen.eq(resource::dsl::rcgen + 1),
            ))
        )
        .attach_and_get_result_async(pool.pool())
        .await
        .unwrap();
        assert_eq!(resource.id(), resource_id);
        assert_eq!(resource.name().as_str(), "test");
        assert_eq!(resource.description(), "desc");
        assert_eq!(resource.time_created(), insert_time);
        assert_eq!(resource.time_modified(), update_time);
        assert_eq!(resource.collection_id.unwrap(), collection_id);
        assert_eq!(resource.rcgen, 2);

        let collection_rcgen = collection::table
            .find(collection_id)
            .select(collection::dsl::rcgen)
            .first_async::<i64>(pool.pool())
            .await
            .unwrap();

        // Make sure rcgen got incremented
        assert_eq!(collection_rcgen, 2);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
