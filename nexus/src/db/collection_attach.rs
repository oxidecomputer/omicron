// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CTE for attaching a resource to a collection.
//!
//! This atomically:
//! - Checks if the collection exists and is not soft deleted
//! - Updates the collection's resource generation number
//! - Updates the resource row

use super::pool::DbConnection;
use async_bb8_diesel::{
    AsyncRunQueryDsl, ConnectionManager, PoolError,
};
use diesel::associations::HasTable;
use diesel::expression::Expression;
use diesel::helper_types::*;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::query_dsl::methods as query_methods;
use diesel::query_source::Table;
use diesel::sql_types::{BigInt, Nullable, SingleValue};
use std::fmt::Debug;
use std::marker::PhantomData;

/// The table representing the collection. The resource references
/// this table.
type CollectionTable<ResourceType, C> = <<C as DatastoreAttachTarget<
    ResourceType,
>>::CollectionGenerationColumn as Column>::Table;
/// The table representing the resource. This table contains an
/// ID acting as a foreign key into the collection table.
type ResourceTable<ResourceType, C> = <<C as DatastoreAttachTarget<
    ResourceType,
>>::ResourceCollectionIdColumn as Column>::Table;
type CollectionGenerationColumn<ResourceType, C> =
    <C as DatastoreAttachTarget<ResourceType>>::CollectionGenerationColumn;
type CollectionIdColumn<ResourceType, C> =
    <C as DatastoreAttachTarget<ResourceType>>::CollectionIdColumn;
type ResourceIdColumn<ResourceType, C> =
    <C as DatastoreAttachTarget<ResourceType>>::ResourceIdColumn;

/// Trick to check that columns come from the same table
pub trait TypesAreSame {}
impl<T> TypesAreSame for (T, T) {}

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
/// #         instance_id -> Nullable<Uuid>,
/// #     }
/// # }
///
/// #[derive(Queryable, Debug, Selectable)]
/// #[diesel(table_name = disk)]
/// struct Disk {
///     pub id: uuid::Uuid,
///     pub time_deleted: Option<chrono::DateTime<chrono::Utc>>,
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
///     // Type of instance::id and disk::id.
///     type Id = uuid::Uuid;
///
///     type CollectionIdColumn = instance::dsl::id;
///     type CollectionGenerationColumn = instance::dsl::rcgen;
///     type CollectionTimeDeletedColumn = instance::dsl::time_deleted;
///
///     type ResourceIdColumn = disk::dsl::id;
///     type ResourceCollectionIdColumn = disk::dsl::instance_id;
///     type ResourceTimeDeletedColumn = disk::dsl::time_deleted;
/// }
/// ```
pub trait DatastoreAttachTarget<ResourceType> : Selectable<Pg> {
    /// The Rust type of the collection and resource ids (typically Uuid).
    type Id: Copy + Debug + PartialEq + Send + 'static;

    type CollectionIdColumn: Column;

    /// The column in the CollectionTable that acts as a generation number.
    /// This is the "resource-generation-number" in RFD 192.
    type CollectionGenerationColumn: Column + Default + Expression<SqlType = BigInt>;

    /// The time deleted column in the CollectionTable
    // We enforce that this column comes from the same table as
    // CollectionGenerationColumn when defining attach_resource() below.
    type CollectionTimeDeletedColumn: Column + Default;

    type ResourceIdColumn: Column;

    /// The column in the ResourceType that acts as a foreign key into
    /// the CollectionTable
    type ResourceCollectionIdColumn: Column + Default;

    /// The time deleted column in the ResourceTable
    type ResourceTimeDeletedColumn: Column + Default;

    /// Create a statement for attaching a resource to the given collection.
    ///
    /// The U, V types are the same type as the 3rd and 4th generic arguments to
    /// UpdateStatement, and should generally be inferred rather than explicitly
    /// specified.
    ///
    /// CAUTION: The API does not currently enforce that `key` matches the value
    /// of the collection id within the attached row.
    fn attach_resource<U, V>(
        collection_id: Self::Id,
        resource_id: Self::Id,

        collection_query: BoxedQuery<CollectionTable<ResourceType, Self>>,
        resource_query: BoxedQuery<ResourceTable<ResourceType, Self>>,

        max_attached_resources: usize,

        // Note that UpdateStatement's fourth argument defaults to Ret =
        // NoReturningClause. This enforces that the given input statement does
        // not have a RETURNING clause.
        update: UpdateStatement<ResourceTable<ResourceType, Self>, U, V>,
    ) -> AttachToCollectionStatement<ResourceType, U, V, Self>
    where
        // TODO: More of this?
        (
            <Self::CollectionGenerationColumn as Column>::Table,
            <Self::CollectionTimeDeletedColumn as Column>::Table,
        ): TypesAreSame,
        (
            <Self::ResourceCollectionIdColumn as Column>::Table,
            <Self::ResourceTimeDeletedColumn as Column>::Table,
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
        // Enables the "table()" method.
        ResourceTable<ResourceType, Self>: HasTable<Table = ResourceTable<ResourceType, Self>>
            + 'static
            + Send
            + Table
            // Allows calling ".into_boxed()" on the table.
            + query_methods::BoxedDsl<
                'static,
                Pg,
                Output = BoxedDslOutput<ResourceTable<ResourceType, Self>>,
            >,
        // Allows treating "collection_exists_query" as a boxed "dyn QueryFragment<Pg>".
        <CollectionTable<ResourceType, Self> as QuerySource>::FromClause:
            QueryFragment<Pg> + Send,
        // Allows treating "resource_exists_query" as a boxed "dyn QueryFragment<Pg>".
        <ResourceTable<ResourceType, Self> as QuerySource>::FromClause:
            QueryFragment<Pg> + Send,
        // Allows sending "collection_exists_query" between threads.
        <CollectionTable<ResourceType, Self> as AsQuery>::SqlType: Send,
        // Allows sending "resource_exists_query" between threads.
        <ResourceTable<ResourceType, Self> as AsQuery>::SqlType: Send,
        // Allows calling ".filter()" on the boxed table.
        BoxedQuery<CollectionTable<ResourceType, Self>>:
            query_methods::FilterDsl<
                    Eq<
                        CollectionPrimaryKey<ResourceType, Self>,
                        Self::Id,
                    >,
                    Output = BoxedQuery<CollectionTable<ResourceType, Self>>,
                > + query_methods::FilterDsl<
                    IsNull<Self::CollectionTimeDeletedColumn>,
                    Output = BoxedQuery<CollectionTable<ResourceType, Self>>,
                >,
        BoxedQuery<ResourceTable<ResourceType, Self>>:
            query_methods::FilterDsl<
                    Eq<
                        ResourcePrimaryKey<ResourceType, Self>,
                        Self::Id,
                    >,
                    Output = BoxedQuery<ResourceTable<ResourceType, Self>>,
            > + query_methods::FilterDsl<
                    Eq<
                        Self::ResourceCollectionIdColumn,
                        Self::Id,
                    >,
                    Output = BoxedQuery<ResourceTable<ResourceType, Self>>,
            > + query_methods::FilterDsl<
                IsNull<Self::ResourceTimeDeletedColumn>,
                Output = BoxedQuery<ResourceTable<ResourceType, Self>>,
            >,

        // Allows using "id" in expressions (e.g. ".eq(...)") with...
        Self::Id: diesel::expression::AsExpression<
            // ... The Collection table's PK
            SerializedCollectionPrimaryKey<ResourceType, Self>,
        > + diesel::expression::AsExpression<
            // ... The Resource table's PK
            SerializedResourcePrimaryKey<ResourceType, Self>,
        > + diesel::expression::AsExpression<
            // ... The Resource table's FK to the Collection table
            SerializedResourceForeignKey<ResourceType, Self>
        >,
        <CollectionPrimaryKey<ResourceType, Self> as Expression>::SqlType:
            SingleValue,
        <ResourcePrimaryKey<ResourceType, Self> as Expression>::SqlType:
            SingleValue,

        // Allows calling "is_null()" on the time deleted column.
        Self::CollectionTimeDeletedColumn: ExpressionMethods,
        Self::ResourceTimeDeletedColumn: ExpressionMethods,
        Self::ResourceCollectionIdColumn: ExpressionMethods,

        Self::CollectionGenerationColumn: ExpressionMethods,
        // Necessary for output type (`AttachToCollectionStatement`).
        ResourceType: Selectable<Pg>,

        // XXX ?
        <Self::CollectionGenerationColumn as Expression>::SqlType:
            SingleValue,
        <Self::ResourceCollectionIdColumn as Expression>::SqlType:
            SingleValue,
    {
        let collection_table = || {
            <CollectionTable<ResourceType, Self> as HasTable>::table()
        };
        let resource_table = || {
            <ResourceTable<ResourceType, Self> as HasTable>::table()
        };

        let collection_exists_query = Box::new(
            collection_table()
                .into_boxed()
                .filter(collection_table().primary_key().eq(collection_id))
                .filter(Self::CollectionTimeDeletedColumn::default().is_null())
        );
        let resource_exists_query = Box::new(
            resource_table()
                .into_boxed()
                .filter(resource_table().primary_key().eq(resource_id))
                .filter(Self::ResourceTimeDeletedColumn::default().is_null())
        );

        let resource_count_query = Box::new(
            resource_table()
                .into_boxed()
                .filter(Self::ResourceCollectionIdColumn::default().eq(collection_id))
                .filter(Self::ResourceTimeDeletedColumn::default().is_null())
                .count()
        );

        let collection_query = Box::new(
            collection_query
                .filter(collection_table().primary_key().eq(collection_id))
                .filter(Self::CollectionTimeDeletedColumn::default().is_null())
        );

        let resource_query = Box::new(
            resource_query
                .filter(resource_table().primary_key().eq(resource_id))
                .filter(Self::ResourceTimeDeletedColumn::default().is_null())
        );

        let collection_from_clause = collection_table().from_clause();
        let collection_returning_clause = Self::as_returning();
        let resource_returning_clause = ResourceType::as_returning();
        AttachToCollectionStatement {
            collection_exists_query,
            resource_exists_query,
            resource_count_query,
            collection_query,
            resource_query,
            max_attached_resources,
            update_resource_statement: update,
            collection_from_clause,
            collection_returning_clause,
            resource_returning_clause,
            query_type: PhantomData,
        }
    }
}

/// The CTE described in the module docs
#[must_use = "Queries must be executed"]
pub struct AttachToCollectionStatement<ResourceType, U, V, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreAttachTarget<ResourceType>,
{
    // Query which answers: "Does the collection exist?"
    collection_exists_query: Box<dyn QueryFragment<Pg> + Send>,
    // Query which answers: "Does the resource exist?"
    resource_exists_query: Box<dyn QueryFragment<Pg> + Send>,
    // Query which answers: "How many resources are associated with the
    // collection?"
    resource_count_query: Box<dyn QueryFragment<Pg> + Send>,
    // A (mostly) user-provided query for validating the collection.
    collection_query: Box<dyn QueryFragment<Pg> + Send>,
    // A (mostly) user-provided query for validating the resource.
    resource_query: Box<dyn QueryFragment<Pg> + Send>,
    // The maximum number of resources which may be attached to the collection.
    max_attached_resources: usize,

    // Update statement for the resource.
    update_resource_statement: UpdateStatement<ResourceTable<ResourceType, C>, U, V>,
    collection_from_clause: <CollectionTable<ResourceType, C> as QuerySource>::FromClause,
    collection_returning_clause: AsSelect<C, Pg>,
    resource_returning_clause: AsSelect<ResourceType, Pg>,
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
pub type AsyncAttachToCollectionResult<ResourceType, C> = Result<ResourceType, AttachError<ResourceType, C>>;

/*
/// Result of [`AttachToCollectionStatement`] when executed synchronously
pub type SyncAttachToCollectionResult<Q> = Result<Q, SyncAttachError>;
*/

/// Errors returned by [`AttachToCollectionStatement`].
#[derive(Debug)]
pub enum AttachError<ResourceType, C> {
    /// The collection that the query was inserting into does not exist
    CollectionNotFound,
    /// The resource being attached does not exist
    ResourceNotFound,
    /// Too many resources are currently attached to the collection
    TooManyAttached,
    /// Although the resource and collection exist, the update did not occur
    ///
    /// The unchanged resource and collection are returned as a part of this
    /// error; it is the responsibility of the caller to determine which
    /// condition was not met.
    NoUpdate(ResourceType, C),
    /// Other database error
    DatabaseError(PoolError),
}

/*
/// Errors returned by [`AttachToCollectionStatement`].
#[derive(Debug)]
pub enum SyncAttachError {
    /// The collection that the query was inserting into does not exist
    CollectionNotFound,
    /// Other database error
    DatabaseError(diesel::result::Error),
}
*/

pub type RawOutput<ResourceType, C> = (i64, Option<C>, Option<ResourceType>, Option<C>, Option<ResourceType>);

impl<ResourceType, U, V, C> AttachToCollectionStatement<ResourceType, U, V, C>
where
    ResourceType: 'static + Debug + Send + Selectable<Pg>,
    C: 'static + Debug + DatastoreAttachTarget<ResourceType> + Send,
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
    ) -> AsyncAttachToCollectionResult<ResourceType, C>
    where
        // We require this bound to ensure that "Self" is runnable as query.
        Self: query_methods::LoadQuery<'static, DbConnection, RawOutput<ResourceType, C>>,
    {
        let capacity = self.max_attached_resources;
        self.get_result_async::<RawOutput<ResourceType, C>>(pool)
            .await
            // If the database returns an error, propagate it right away.
            .map_err(|e| {
                eprintln!("ERROR from DB - not parsing result");
                AttachError::DatabaseError(e)
            })
            // Otherwise, parse the output to determine if the CTE succeeded.
            .and_then(|r| Self::parse_result(r, capacity))
    }

    /*

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
            .map(parse_result)
    }

    */

    fn parse_result(
        result: RawOutput<ResourceType, C>,
        capacity: usize,
    ) -> Result<ResourceType, AttachError<ResourceType, C>> {
        eprintln!("Parsing DB result: {:?}", result);

        let (
            attached_count,
            collection_before_update,
            resource_before_update,
            collection_after_update,
            resource_after_update
        ) = result;

        // TODO: avoid unwrap here
        if attached_count >= capacity.try_into().unwrap() {
            return Err(AttachError::TooManyAttached);
        }

        let collection_before_update =
            collection_before_update.ok_or_else(|| AttachError::CollectionNotFound)?;

        let resource_before_update =
            resource_before_update.ok_or_else(|| AttachError::ResourceNotFound)?;

        match (collection_after_update, resource_after_update) {
            (Some(_), Some(resource)) => Ok(resource),
            (None, None) => Err(AttachError::NoUpdate(resource_before_update, collection_before_update)),
            _ => panic!("Partial update applied - This is a CTE bug"),
        }
    }

    /*
    /// Translate from diesel errors into AttachError, handling the
    /// intentional division-by-zero error in the CTE.
    fn translate_async_error(err: PoolError) -> AttachError {
        match err {
            PoolError::Connection(ConnectionError::Query(err))
                if Self::error_is_division_by_zero(&err) =>
            {
                AttachError::CollectionNotFound
            }
            other => AttachError::DatabaseError(other),
        }
    }
    */

    /*
    /// Translate from diesel errors into SyncAttachError, handling the
    /// intentional division-by-zero error in the CTE.
    fn translate_sync_error(err: diesel::result::Error) -> SyncAttachError {
        if Self::error_is_division_by_zero(&err) {
            SyncAttachError::CollectionNotFound
        } else {
            SyncAttachError::DatabaseError(err)
        }
    }
    */
}

type SelectableSqlType<Q> =
    <<Q as diesel::Selectable<Pg>>::SelectExpression as Expression>::SqlType;

impl<ResourceType, U, V, C> Query
    for AttachToCollectionStatement<ResourceType, U, V, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreAttachTarget<ResourceType>,
{
    type SqlType = (
        // The number of resources attached to the collection before update.
        BigInt,
        // If the collection exists, the value before update.
        Nullable<SelectableSqlType<C>>,
        // If the resource exists, the value before update.
        Nullable<SelectableSqlType<ResourceType>>,
        // If the collection was updated, the new value.
        Nullable<SelectableSqlType<C>>,
        // If the resource was updated, the new value.
        Nullable<SelectableSqlType<ResourceType>>,
    );
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
type ResourcePrimaryKey<ResourceType, C> =
    <ResourceTable<ResourceType, C> as Table>::PrimaryKey;
type ResourceForeignKey<ResourceType, C> = <C as DatastoreAttachTarget<
    ResourceType,
>>::ResourceCollectionIdColumn;

// Representation of Primary Key in SQL.
type SerializedCollectionPrimaryKey<ResourceType, C> =
    <CollectionPrimaryKey<ResourceType, C> as diesel::Expression>::SqlType;
type SerializedResourcePrimaryKey<ResourceType, C> =
    <ResourcePrimaryKey<ResourceType, C> as diesel::Expression>::SqlType;
type SerializedResourceForeignKey<ResourceType, C> =
    <ResourceForeignKey<ResourceType, C> as diesel::Expression>::SqlType;

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
/// // WITH
/// //      /* Look up the collection - Check for existence only! */
/// //      collection_by_id AS (
/// //          SELECT * FROM C
/// //          WHERE <PK> = <VALUE> AND <time_deleted> IS NULL
/// //          FOR UPDATE
/// //      ),
/// //      /* Look up the resource - Check for existence only! */
/// //      resource_by_id AS (
/// //          SELECT * FROM R
/// //          WHERE <PK> = <VALUE> AND <time_deleted> IS NULL
/// //          FOR UPDATE
/// //      ),
/// //      /* Count the number of attached resources */
/// //      resource_count AS (
/// //          SELECT COUNT(*) FROM R
/// //          WHERE <FK> = <VALUE> AND <time_deleted> IS NULL
/// //      ),
/// //      /* Look up the collection - Check for additional constraints */
/// //      collection_info AS (
/// //          SELECT * FROM C
/// //          WHERE <PK> = <VALUE> AND <time_deleted> IS NULL AND
/// //              <Additional user-supplied constraints>
/// //          FOR UPDATE
/// //      ),
/// //      /* Look up the resource - Check for additional constraints */
/// //      resource_info AS (
/// //          SELECT * FROM R
/// //          WHERE <PK> = <VALUE> AND <time_deleted> IS NULL AND
/// //              <Additional user-supplied constraints>
/// //          FOR UPDATE
/// //      ),
/// //      /* Make a decision on whether or not to apply ANY updates */
/// //      do_update AS (
/// //          SELECT IF(
/// //              EXISTS(SELECT id FROM collection_info) AND
/// //              EXISTS(SELECT id FROM resource_info) AND
/// //              (SELECT * FROM resource_count) < <CAPACITY>,
/// //          TRUE, FALSE),
/// //      ),
/// //      /* Update the generation number of the collection row */
/// //      updated_collection AS (
/// //          UPDATE C SET <generation number> = <generation_number> + 1
/// //          WHERE <PK> IN (SELECT <PK> FROM collection_info) AND (SELECT * FROM do_update)
/// //          RETURNING *
/// //      ),
/// //      /* Update the resource */
/// //      updated_resource AS (
/// //          UPDATE R SET <User-supplied Update>
/// //          WHERE <PK> IN (SELECT <PK> FROM resource_info) AND (SELECT * FROM do_update)
/// //          RETURNING *
/// //      )
/// //  SELECT * FROM
/// //      (SELECT * FROM resource_count)
/// //      LEFT JOIN (SELECT * FROM collection_by_id) ON TRUE
/// //      LEFT JOIN (SELECT * FROM resource_by_id) ON TRUE
/// //      LEFT JOIN (SELECT * FROM updated_collection) ON TRUE
/// //      LEFT JOIN (SELECT * FROM resource) ON TRUE;
/// ```
///
/// This CTE is similar in desired behavior to the one specified in
/// [RFD 192](https://rfd.shared.oxide.computer/rfd/0192#_dueling_administrators),
/// but tuned to the case of modifying an associated resource.
///
/// The general idea is that the first clause of the CTE (the "dummy" table)
/// will generate a division-by-zero error and rollback the transaction if the
/// target collection is not found in its table. It simultaneously locks the
/// row for update, to allow us to subsequently use the "updated_collection_row" query to
/// increase the resource generation count for the collection. In the same
/// transaction, it performs the provided update statement, which should
/// update the resource, referencing the collection ID to the collection
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
    // Necessary to "walk_ast" over "select.collection_from_clause".
    <CollectionTable<ResourceType, C> as QuerySource>::FromClause:
        QueryFragment<Pg>,
    // Necessary to "walk_ast" over "self.update_resource_statement".
    UpdateStatement<ResourceTable<ResourceType, C>, U, V>: QueryFragment<Pg>,
    // Necessary to "walk_ast" over "self.resource_returning_clause".
    AsSelect<ResourceType, Pg>: QueryFragment<Pg>,
    // Necessary to "walk_ast" over "self.collection_returning_clause".
    AsSelect<C, Pg>: QueryFragment<Pg>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();
        out.push_sql("WITH collection_by_id AS (");
        self.collection_exists_query.walk_ast(out.reborrow())?;
        out.push_sql(" FOR UPDATE), ");

        out.push_sql("resource_by_id AS (");
        self.resource_exists_query.walk_ast(out.reborrow())?;
        out.push_sql(" FOR UPDATE), ");

        out.push_sql("resource_count AS (");
        self.resource_count_query.walk_ast(out.reborrow())?;
        out.push_sql("), ");

        out.push_sql("collection_info AS (");
        self.collection_query.walk_ast(out.reborrow())?;
        out.push_sql(" FOR UPDATE), ");

        out.push_sql("resource_info AS (");
        self.resource_query.walk_ast(out.reborrow())?;
        out.push_sql(" FOR UPDATE), ");

        out.push_sql("do_update AS (SELECT IF(EXISTS(SELECT ");
        out.push_identifier(CollectionIdColumn::<ResourceType, C>::NAME)?;
        out.push_sql(" FROM collection_info) AND EXISTS(SELECT ");
        out.push_identifier(ResourceIdColumn::<ResourceType, C>::NAME)?;
        out.push_sql(
            &format!(" FROM resource_info) AND (SELECT * FROM resource_count) < {}, TRUE,FALSE)), ",
            self.max_attached_resources)
        );

        out.push_sql("updated_collection AS (UPDATE ");
        self.collection_from_clause.walk_ast(out.reborrow())?;
        out.push_sql(" SET ");
        out.push_identifier(CollectionGenerationColumn::<ResourceType, C>::NAME)?;
        out.push_sql(" = ");
        out.push_identifier(CollectionGenerationColumn::<ResourceType, C>::NAME)?;
        out.push_sql(" + 1 WHERE ");
        out.push_identifier(CollectionPrimaryKey::<ResourceType, C>::NAME)?;
        out.push_sql(" IN (SELECT ");
        out.push_identifier(CollectionPrimaryKey::<ResourceType, C>::NAME)?;
        out.push_sql(" FROM collection_info) AND (SELECT * FROM do_update) RETURNING ");
        // TODO: You don't actually need to return anything here. We only care
        // about the inserted resource...
        self.collection_returning_clause.walk_ast(out.reborrow())?;
        out.push_sql("), ");

        out.push_sql("updated_resource AS (");
        // TODO: Check or force the update_resource_statement to have
        //       C::ResourceCollectionIdColumn set
        self.update_resource_statement.walk_ast(out.reborrow())?;
        // TODO: Is this safe? There must be a WHERE clause for this to work.
        out.push_sql(" AND (SELECT * FROM do_update)");
        out.push_sql(" RETURNING ");
        self.resource_returning_clause.walk_ast(out.reborrow())?;
        out.push_sql(") ");

        // Why do all these LEFT JOINs here? In short, to ensure that we are
        // always returning a constant number of columns.
        //
        // Diesel parses output "one column at a time", mapping to structs or
        // tuples. For example, when deserializing an "Option<(A, B, C)>" object,
        // Diesel checks nullability of the "A", "B", and "C" columns.
        // If any of those columns unexpectedly return NULL, the entire object is
        // treated as "None".
        //
        // In summary:
        // - Without the LEFT JOINs, we'd occassionally be returning "zero
        // rows", which would make the output entirely unparseable.
        // - If we used an operation like COALESCE (which attempts to map the
        // result of an expression to either "NULL" or a single tuple column),
        // Diesel struggles to map the result back to a structure.
        //
        // By returning a static number of columns, each component of the
        // "RawOutput" tuple can be parsed, regardless of nullability, without
        // preventing later portions of the result from being parsed.
        out.push_sql("SELECT * FROM \
            (SELECT * FROM resource_count) \
            LEFT JOIN (SELECT * FROM collection_by_id) ON TRUE \
            LEFT JOIN (SELECT * FROM resource_by_id) ON TRUE \
            LEFT JOIN (SELECT * FROM updated_collection) ON TRUE \
            LEFT JOIN (SELECT * FROM updated_resource) ON TRUE;");

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{AttachError, DatastoreAttachTarget};
    use crate::db::{
        self, error::TransactionError, identity::Resource as IdentityResource,
        model::Generation,
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
                     collection_id UUID); \
                 CREATE INDEX IF NOT EXISTS collection_index ON test_schema.resource ( \
                     collection_id \
                 ) WHERE collection_id IS NOT NULL AND time_deleted IS NULL; \
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
        pub collection_id: Option<uuid::Uuid>,
    }

    #[derive(Queryable, Insertable, Debug, Resource, Selectable)]
    #[diesel(table_name = collection)]
    struct Collection {
        #[diesel(embed)]
        pub identity: CollectionIdentity,
        pub rcgen: i64,
    }

    impl DatastoreAttachTarget<Resource> for Collection {
        type Id = uuid::Uuid;

        type CollectionIdColumn = collection::dsl::id;
        type CollectionGenerationColumn = collection::dsl::rcgen;
        type CollectionTimeDeletedColumn = collection::dsl::time_deleted;

        type ResourceIdColumn = resource::dsl::id;
        type ResourceCollectionIdColumn = resource::dsl::collection_id;
        type ResourceTimeDeletedColumn = resource::dsl::time_deleted;
    }

    #[test]
    fn test_verify_query() {
        let collection_id =
            uuid::Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc")
                .unwrap();
        let resource_id =
            uuid::Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
                .unwrap();
        let attach = Collection::attach_resource(
            collection_id,
            resource_id,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            12345,
            diesel::update(resource::table.filter(resource::dsl::id.eq(resource_id)))
                .set(resource::dsl::collection_id.eq(collection_id))
        );
        let query = diesel::debug_query::<Pg, _>(&attach).to_string();

        let expected_query = "WITH \
            collection_by_id AS (\
                SELECT \
                    \"test_schema\".\"collection\".\"id\", \
                    \"test_schema\".\"collection\".\"name\", \
                    \"test_schema\".\"collection\".\"description\", \
                    \"test_schema\".\"collection\".\"time_created\", \
                    \"test_schema\".\"collection\".\"time_modified\", \
                    \"test_schema\".\"collection\".\"time_deleted\", \
                    \"test_schema\".\"collection\".\"rcgen\" \
                FROM \"test_schema\".\"collection\" \
                WHERE (\
                    (\"test_schema\".\"collection\".\"id\" = $1) AND \
                    (\"test_schema\".\"collection\".\"time_deleted\" IS NULL)\
                ) FOR UPDATE\
            ), \
            resource_by_id AS (\
                SELECT \
                    \"test_schema\".\"resource\".\"id\", \
                    \"test_schema\".\"resource\".\"name\", \
                    \"test_schema\".\"resource\".\"description\", \
                    \"test_schema\".\"resource\".\"time_created\", \
                    \"test_schema\".\"resource\".\"time_modified\", \
                    \"test_schema\".\"resource\".\"time_deleted\", \
                    \"test_schema\".\"resource\".\"collection_id\" \
                FROM \"test_schema\".\"resource\" \
                WHERE (\
                    (\"test_schema\".\"resource\".\"id\" = $2) AND \
                    (\"test_schema\".\"resource\".\"time_deleted\" IS NULL)\
                ) FOR UPDATE\
            ), \
            resource_count AS (\
                SELECT COUNT(*) \
                FROM \"test_schema\".\"resource\" \
                WHERE (\
                    (\"test_schema\".\"resource\".\"collection_id\" = $3) AND \
                    (\"test_schema\".\"resource\".\"time_deleted\" IS NULL)\
                )\
            ), \
            collection_info AS (\
                SELECT \
                    \"test_schema\".\"collection\".\"id\", \
                    \"test_schema\".\"collection\".\"name\", \
                    \"test_schema\".\"collection\".\"description\", \
                    \"test_schema\".\"collection\".\"time_created\", \
                    \"test_schema\".\"collection\".\"time_modified\", \
                    \"test_schema\".\"collection\".\"time_deleted\", \
                    \"test_schema\".\"collection\".\"rcgen\" \
                FROM \"test_schema\".\"collection\" \
                WHERE (\
                    (\"test_schema\".\"collection\".\"id\" = $4) AND \
                    (\"test_schema\".\"collection\".\"time_deleted\" IS NULL)\
                ) FOR UPDATE\
            ), \
            resource_info AS (\
                SELECT \
                    \"test_schema\".\"resource\".\"id\", \
                    \"test_schema\".\"resource\".\"name\", \
                    \"test_schema\".\"resource\".\"description\", \
                    \"test_schema\".\"resource\".\"time_created\", \
                    \"test_schema\".\"resource\".\"time_modified\", \
                    \"test_schema\".\"resource\".\"time_deleted\", \
                    \"test_schema\".\"resource\".\"collection_id\" \
                FROM \"test_schema\".\"resource\" \
                WHERE (\
                    (\"test_schema\".\"resource\".\"id\" = $5) AND \
                    (\"test_schema\".\"resource\".\"time_deleted\" IS NULL)\
                ) FOR UPDATE\
            ), \
            do_update AS (\
                SELECT IF(\
                    EXISTS(SELECT \"id\" FROM collection_info) AND \
                    EXISTS(SELECT \"id\" FROM resource_info) AND \
                    (SELECT * FROM resource_count) < 12345, \
                TRUE,\
                FALSE)\
            ), \
            updated_collection AS (\
                UPDATE \
                    \"test_schema\".\"collection\" \
                SET \
                    \"rcgen\" = \"rcgen\" + 1 \
                WHERE \
                    \"id\" IN (SELECT \"id\" FROM collection_info) AND \
                    (SELECT * FROM do_update) \
                RETURNING \
                    \"test_schema\".\"collection\".\"id\", \
                    \"test_schema\".\"collection\".\"name\", \
                    \"test_schema\".\"collection\".\"description\", \
                    \"test_schema\".\"collection\".\"time_created\", \
                    \"test_schema\".\"collection\".\"time_modified\", \
                    \"test_schema\".\"collection\".\"time_deleted\", \
                    \"test_schema\".\"collection\".\"rcgen\"\
            ), \
            updated_resource AS (\
                UPDATE \
                    \"test_schema\".\"resource\" \
                SET \
                    \"collection_id\" = $6 \
                WHERE \
                    (\"test_schema\".\"resource\".\"id\" = $7) AND \
                    (SELECT * FROM do_update) \
                RETURNING \
                    \"test_schema\".\"resource\".\"id\", \
                    \"test_schema\".\"resource\".\"name\", \
                    \"test_schema\".\"resource\".\"description\", \
                    \"test_schema\".\"resource\".\"time_created\", \
                    \"test_schema\".\"resource\".\"time_modified\", \
                    \"test_schema\".\"resource\".\"time_deleted\", \
                    \"test_schema\".\"resource\".\"collection_id\"\
                ) \
            SELECT * FROM \
                (SELECT * FROM resource_count) \
                LEFT JOIN (SELECT * FROM collection_by_id) ON TRUE \
                LEFT JOIN (SELECT * FROM resource_by_id) ON TRUE \
                LEFT JOIN (SELECT * FROM updated_collection) ON TRUE \
                LEFT JOIN (SELECT * FROM updated_resource) ON TRUE; -- binds: [cccccccc-cccc-cccc-cccc-cccccccccccc, aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, cccccccc-cccc-cccc-cccc-cccccccccccc, cccccccc-cccc-cccc-cccc-cccccccccccc, aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, cccccccc-cccc-cccc-cccc-cccccccccccc, aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa]";
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
            resource_id,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            12345,
            diesel::update(resource::table.filter(resource::dsl::id.eq(resource_id)))
                .set(resource::dsl::collection_id.eq(collection_id))
        )
        .attach_and_get_result_async(pool.pool())
        .await;

        eprintln!("!!!! result: {:?}", attach);

        assert!(matches!(attach, Err(AttachError::CollectionNotFound)));

        /*

        let attach_query = Collection::attach_resource(
            collection_id,
            resource_id,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            12345,
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
                    SyncAttachError::CollectionNotFound => {
                        TxnError::CustomError(CollectionError::NotFound)
                    }
                    SyncAttachError::DatabaseError(e) => TxnError::from(e),
                })
            })
            .await;

        assert!(matches!(
            result,
            Err(TxnError::CustomError(CollectionError::NotFound))
        ));

        */

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    /*
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
            Generation::new(),
            diesel::update(
                resource::table
                    .filter(resource::dsl::id.eq(resource_id))
                    .filter(resource::dsl::time_deleted.is_null())
            ).set((
                resource::dsl::collection_id.eq(collection_id),
                resource::dsl::time_modified.eq(update_time),
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

    */
}
