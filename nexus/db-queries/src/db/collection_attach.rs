// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CTE for attaching a resource to a collection.
//!
//! This atomically:
//! - Checks if the collection exists and is not soft deleted
//! - Checks if the resource exists and is not soft deleted
//! - Validates conditions on both the collection and resource
//! - Ensures the number of attached resources does not exceed
//! a provided threshold
//! - Updates the resource row

use super::cte_utils::{
    BoxableTable, BoxableUpdateStatement, BoxedQuery, ExprSqlType, FilterBy,
    QueryFromClause, QuerySqlType, TableDefaultWhereClause,
};
use crate::db::raw_query_builder::TypedSqlQuery;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::associations::HasTable;
use diesel::expression::{AsExpression, Expression};
use diesel::helper_types::*;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::query_dsl::methods as query_methods;
use diesel::query_source::Table;
use diesel::result::Error as DieselError;
use diesel::sql_types::{BigInt, Nullable, SingleValue};
use nexus_db_lookup::DbConnection;
use nexus_db_model::DatastoreAttachTargetConfig;
use std::fmt::Debug;

/// A collection of type aliases particularly relevant to collection-based CTEs.
pub(crate) mod aliases {
    use super::{
        Column, DatastoreAttachTargetConfig, Table, TableDefaultWhereClause,
    };

    /// The table representing the collection. The resource references
    /// this table.
    pub type CollectionTable<ResourceType, C> = <<C as DatastoreAttachTargetConfig<
        ResourceType,
    >>::CollectionIdColumn as Column>::Table;
    /// The table representing the resource. This table contains an
    /// ID acting as a foreign key into the collection table.
    pub type ResourceTable<ResourceType, C> = <<C as DatastoreAttachTargetConfig<
        ResourceType,
    >>::ResourceIdColumn as Column>::Table;

    /// The default WHERE clause of the collection table.
    pub type CollectionTableDefaultWhereClause<ResourceType, C> =
        TableDefaultWhereClause<CollectionTable<ResourceType, C>>;
    /// The default WHERE clause of the resource table.
    pub type ResourceTableDefaultWhereClause<ResourceType, C> =
        TableDefaultWhereClause<ResourceTable<ResourceType, C>>;

    pub type CollectionIdColumn<ResourceType, C> =
        <C as DatastoreAttachTargetConfig<ResourceType>>::CollectionIdColumn;
    pub type ResourceIdColumn<ResourceType, C> =
        <C as DatastoreAttachTargetConfig<ResourceType>>::ResourceIdColumn;

    /// Representation of Primary Key in Rust.
    pub type CollectionPrimaryKey<ResourceType, C> =
        <CollectionTable<ResourceType, C> as Table>::PrimaryKey;
    pub type ResourcePrimaryKey<ResourceType, C> =
        <ResourceTable<ResourceType, C> as Table>::PrimaryKey;
    pub type ResourceForeignKey<ResourceType, C> =
        <C as DatastoreAttachTargetConfig<ResourceType>>::ResourceCollectionIdColumn;

    /// Representation of Primary Key in SQL.
    pub type SerializedCollectionPrimaryKey<ResourceType, C> =
        <CollectionPrimaryKey<ResourceType, C> as diesel::Expression>::SqlType;
    pub type SerializedResourcePrimaryKey<ResourceType, C> =
        <ResourcePrimaryKey<ResourceType, C> as diesel::Expression>::SqlType;
    pub type SerializedResourceForeignKey<ResourceType, C> =
        <ResourceForeignKey<ResourceType, C> as diesel::Expression>::SqlType;
}

use aliases::*;

/// Extension trait adding behavior to types that implement
/// `nexus_db_model::DatastoreAttachTarget`.
pub trait DatastoreAttachTarget<ResourceType>:
    DatastoreAttachTargetConfig<ResourceType>
{
    /// Creates a statement for attaching a resource to the given collection.
    ///
    /// This statement allows callers to atomically check the state of a
    /// collection and a resource while attaching a resource to a collection.
    ///
    /// - `collection_id`: Primary key of the collection being inserted into.
    /// - `resource_id`: Primary key of the resource being attached.
    /// - `collection_query`: An optional query for collection state. The
    /// CTE will automatically filter this query to `collection_id`, and
    /// validate that the "time deleted" column is NULL.
    /// - `resource_query`: An optional query for the resource state. The
    /// CTE will automatically filter this query to `resource_id`,
    /// validate that the "time deleted" column is NULL, and validate that the
    /// "collection_id" column is NULL.
    /// - `max_attached_resources`: The maximum number of non-deleted
    /// resources which are permitted to have their "collection_id" column
    /// set to the value of `collection_id`. If attaching `resource_id` would
    /// cross this threshold, the update is aborted.
    /// - `update`: An update statement, identifying how the resource object
    /// should be modified to be attached.
    ///
    /// The V type refers to the "update target" of the UpdateStatement,
    /// and should generally be inferred rather than explicitly specified.
    fn attach_resource<V>(
        collection_id: Self::Id,
        resource_id: Self::Id,

        collection_query: BoxedQuery<CollectionTable<ResourceType, Self>>,
        resource_query: BoxedQuery<ResourceTable<ResourceType, Self>>,

        max_attached_resources: u32,

        // We are intentionally picky about this update statement:
        // - The second argument - the WHERE clause - must match the default
        // for the table. This encourages the "resource_query" filter to be
        // used instead, and makes it possible for the CTE to modify the
        // filter here (ensuring "resource_id" is selected).
        // - Additionally, UpdateStatement's fourth argument defaults to Ret =
        // NoReturningClause. This enforces that the given input statement does
        // not have a RETURNING clause, and also lets the CTE control this
        // value.
        update: UpdateStatement<
            ResourceTable<ResourceType, Self>,
            ResourceTableDefaultWhereClause<ResourceType, Self>,
            V,
        >,
    ) -> AttachToCollectionStatement<ResourceType, V, Self>
    where
        // Treat the collection and resource as boxed tables.
        CollectionTable<ResourceType, Self>: BoxableTable,
        ResourceTable<ResourceType, Self>: BoxableTable,
        // Allows treating "collection_exists_query" as a boxed "dyn QueryFragment<Pg>".
        QueryFromClause<CollectionTable<ResourceType, Self>>:
            QueryFragment<Pg> + Send,
        // Allows treating "resource_exists_query" as a boxed "dyn QueryFragment<Pg>".
        QueryFromClause<ResourceTable<ResourceType, Self>>:
            QueryFragment<Pg> + Send,
        // Allows sending "collection_exists_query" between threads.
        QuerySqlType<CollectionTable<ResourceType, Self>>: Send,
        // Allows sending "resource_exists_query" between threads.
        QuerySqlType<ResourceTable<ResourceType, Self>>: Send,
        // Allows calling ".filter()" on the boxed collection table.
        BoxedQuery<CollectionTable<ResourceType, Self>>: FilterBy<Eq<CollectionPrimaryKey<ResourceType, Self>, Self::Id>>
            + FilterBy<IsNull<Self::CollectionTimeDeletedColumn>>,
        // Allows calling ".filter()" on the boxed resource table.
        BoxedQuery<ResourceTable<ResourceType, Self>>: FilterBy<Eq<ResourcePrimaryKey<ResourceType, Self>, Self::Id>>
            + FilterBy<Eq<Self::ResourceCollectionIdColumn, Self::Id>>
            + FilterBy<IsNull<Self::ResourceCollectionIdColumn>>
            + FilterBy<IsNull<Self::ResourceTimeDeletedColumn>>,
        // Allows calling "update.into_boxed()"
        UpdateStatement<
            ResourceTable<ResourceType, Self>,
            ResourceTableDefaultWhereClause<ResourceType, Self>,
            V,
        >: BoxableUpdateStatement<ResourceTable<ResourceType, Self>, V>,
        // Allows calling
        // ".filter(resource_table().primary_key().eq(resource_id)" on the
        // boxed update statement.
        BoxedUpdateStatement<'static, Pg, ResourceTable<ResourceType, Self>, V>:
            FilterBy<Eq<ResourcePrimaryKey<ResourceType, Self>, Self::Id>>,
        // Allows using "id" in expressions (e.g. ".eq(...)") with...
        Self::Id: AsExpression<
                // ... The Collection table's PK
                SerializedCollectionPrimaryKey<ResourceType, Self>,
            > + AsExpression<
                // ... The Resource table's PK
                SerializedResourcePrimaryKey<ResourceType, Self>,
            > + AsExpression<
                // ... The Resource table's FK to the Collection table
                SerializedResourceForeignKey<ResourceType, Self>,
            >,
        ExprSqlType<CollectionPrimaryKey<ResourceType, Self>>: SingleValue,
        ExprSqlType<ResourcePrimaryKey<ResourceType, Self>>: SingleValue,
        ExprSqlType<Self::ResourceCollectionIdColumn>: SingleValue,
        // Necessary to actually select the resource in the output type.
        ResourceType: Selectable<Pg>,
    {
        let collection_table =
            || <CollectionTable<ResourceType, Self> as HasTable>::table();
        let resource_table =
            || <ResourceTable<ResourceType, Self> as HasTable>::table();

        // Create new queries to determine if the collection and resources
        // already exist.
        let collection_exists_query = Box::new(
            collection_table()
                .into_boxed()
                .filter(collection_table().primary_key().eq(collection_id))
                .filter(Self::CollectionTimeDeletedColumn::default().is_null()),
        );
        let resource_exists_query = Box::new(
            resource_table()
                .into_boxed()
                .filter(resource_table().primary_key().eq(resource_id))
                .filter(Self::ResourceTimeDeletedColumn::default().is_null()),
        );

        // Additionally, construct a new query to count the number of
        // already attached resources.
        let resource_count_query = Box::new(
            resource_table()
                .into_boxed()
                .filter(
                    Self::ResourceCollectionIdColumn::default()
                        .eq(collection_id),
                )
                .filter(Self::ResourceTimeDeletedColumn::default().is_null())
                .count(),
        );

        // For the queries which decide whether or not we'll perform the update,
        // extend the user-provided arguments.
        //
        // We force these queries to:
        // - Check against the primary key of the target objects
        // - Ensure the objects are not deleted
        // - (for the resource) Ensure it is not already attached
        // - (for the update) Ensure that only the resource with "resource_id"
        // is modified.
        let collection_query = Box::new(
            collection_query
                .filter(collection_table().primary_key().eq(collection_id))
                .filter(Self::CollectionTimeDeletedColumn::default().is_null()),
        );
        let resource_query = if Self::ALLOW_FROM_ATTACHED {
            Box::new(
                resource_query
                    .filter(resource_table().primary_key().eq(resource_id))
                    .filter(
                        Self::ResourceTimeDeletedColumn::default().is_null(),
                    ),
            )
        } else {
            Box::new(
                resource_query
                    .filter(resource_table().primary_key().eq(resource_id))
                    .filter(
                        Self::ResourceTimeDeletedColumn::default().is_null(),
                    )
                    .filter(
                        Self::ResourceCollectionIdColumn::default().is_null(),
                    ),
            )
        };

        let update_resource_statement = update
            .into_boxed()
            .filter(resource_table().primary_key().eq(resource_id));

        let resource_returning_clause = ResourceType::as_returning();
        AttachToCollectionStatement {
            collection_exists_query,
            resource_exists_query,
            resource_count_query,
            collection_query,
            resource_query,
            max_attached_resources,
            update_resource_statement,
            resource_returning_clause,
        }
    }
}

impl<T, R> DatastoreAttachTarget<R> for T where T: DatastoreAttachTargetConfig<R>
{}

/// A database object which may have one or more [`Resource`]s attached to it.
pub struct Collection {
    table_name: &'static str,
    id_column: &'static str,
    time_deleted_column: &'static str,
}

impl Collection {
    /// Describes a collection, within which a [`Resource`] will be inserted.
    ///
    /// - `table_name`: Name of the collection table
    /// - `id_column`: Name of the primary key column
    /// - `time_deleted_column`: Name of the column indicating if the collection
    /// has been soft-deleted.
    pub const fn new(
        table_name: &'static str,
        id_column: &'static str,
        time_deleted_column: &'static str,
    ) -> Self {
        Self { table_name, id_column, time_deleted_column }
    }
}

/// A database object which may belong to one or more [`Collection`]s.
pub struct Resource {
    table_name: &'static str,
    id_column: &'static str,
    collection_id_column: &'static str,
    time_deleted_column: &'static str,
}

impl Resource {
    /// Describes a resource, which may be inserted within a [`Collection`].
    ///
    /// - `table_name`: Name of the collection table
    /// - `id_column`: Name of the primary key column
    /// - `collection_id_column`: Name of the column referencing the collection
    /// by primary key.
    /// - `time_deleted_column`: Name of the column indicating if the collection
    /// has been soft-deleted.
    pub const fn new(
        table_name: &'static str,
        id_column: &'static str,
        collection_id_column: &'static str,
        time_deleted_column: &'static str,
    ) -> Self {
        Self {
            table_name,
            id_column,
            collection_id_column,
            time_deleted_column,
        }
    }
}

/// Creates a statement for attaching a [`Resource`] object to a [`Collection`].
///
/// This query allows callers to atomically check the state of a
/// collection and a resource while attaching a resource to a collection.
///
/// This provides a two-phase API:
/// - [`AttachQueryTemplate::new`] - Creates a query with table/column names
/// - [`AttachQueryTemplate::execute`] - Executes with runtime values (UUIDs, max, SET clause)
pub struct AttachQueryTemplate {
    collection: Collection,
    resource: Resource,
    allow_from_attached: bool,
}

impl AttachQueryTemplate {
    /// Creates a new attach query template with the schema information.
    ///
    /// This captures all the table and column names that define the structure
    /// of the attach operation. Call [`Self::execute`] with runtime parameters to
    /// build the actual query.
    ///
    /// `allow_from_attached` answers the question: "Can we attach a resource to
    /// this collection, even if it's currently attached to a different
    /// collection"?
    pub const fn new(
        collection: Collection,
        resource: Resource,
        allow_from_attached: bool,
    ) -> Self {
        Self { collection, resource, allow_from_attached }
    }

    /// Builds the attach query with runtime parameters.
    ///
    /// Takes the UUIDs, capacity limit, and a closure to build the SET clause.
    /// The closure receives a mutable reference to a QueryBuilder and should
    /// construct the SET clause for the UPDATE statement (without the "SET" keyword).
    ///
    /// Optional filter closures can be provided to add additional WHERE conditions
    /// for collection and resource validation beyond just the ID and time_deleted checks.
    ///
    /// Example:
    /// ```ignore
    /// template.build(
    ///     collection_id,
    ///     resource_id,
    ///     max_resources,
    ///     |builder| {
    ///         builder.sql("column1 = ");
    ///         builder.param().bind::<sql_types::Uuid, _>(value1);
    ///         builder.sql(", column2 = ");
    ///         builder.param().bind::<sql_types::Text, _>(value2);
    ///     },
    ///     Some(|builder| {
    ///         builder.sql(" AND state = ");
    ///         builder.param().bind::<sql_types::Text, _>("active");
    ///     }),
    ///     None,
    /// )
    /// ```
    pub fn build<ResourceType, CollectionType>(
        &self,
        collection_id: uuid::Uuid,
        resource_id: uuid::Uuid,
        max_attached_resources: u32,
        build_set_clause: impl FnOnce(
            &mut crate::db::raw_query_builder::QueryBuilder,
        ),
        collection_filter: Option<
            impl FnOnce(&mut crate::db::raw_query_builder::QueryBuilder),
        >,
        resource_filter: Option<
            impl FnOnce(&mut crate::db::raw_query_builder::QueryBuilder),
        >,
    ) -> AttachQuery<ResourceType, CollectionType>
    where
        ResourceType: Selectable<Pg> + Send + 'static,
        CollectionType: Selectable<Pg> + Send + 'static,
    {
        use crate::db::raw_query_builder::QueryBuilder;
        use diesel::sql_types;

        let mut builder = QueryBuilder::new();

        // Build the CTE query structure matching the QueryFragment implementation

        // WITH collection_by_id AS (...)
        builder.sql("WITH collection_by_id AS (");
        builder.sql("SELECT * FROM ");
        builder.sql(self.collection.table_name);
        builder.sql(" WHERE ");
        builder.sql(self.collection.id_column);
        builder.sql(" = ");
        builder.param().bind::<sql_types::Uuid, _>(collection_id);
        builder.sql(" AND ");
        builder.sql(self.collection.time_deleted_column);
        builder.sql(" IS NULL");
        if let Some(filter) = collection_filter {
            filter(&mut builder);
        }
        builder.sql(" FOR UPDATE), ");

        // resource_by_id AS (...)
        builder.sql("resource_by_id AS (");
        builder.sql("SELECT * FROM ");
        builder.sql(self.resource.table_name);
        builder.sql(" WHERE ");
        builder.sql(self.resource.id_column);
        builder.sql(" = ");
        builder.param().bind::<sql_types::Uuid, _>(resource_id);
        builder.sql(" AND ");
        builder.sql(self.resource.time_deleted_column);
        builder.sql(" IS NULL");
        if let Some(filter) = resource_filter {
            filter(&mut builder);
        }
        builder.sql(" FOR UPDATE), ");

        // resource_count AS (...)
        builder.sql("resource_count AS (");
        builder.sql("SELECT COUNT(*) FROM ");
        builder.sql(self.resource.table_name);
        builder.sql(" WHERE ");
        builder.sql(self.resource.collection_id_column);
        builder.sql(" = ");
        builder.param().bind::<sql_types::Uuid, _>(collection_id);
        builder.sql(" AND ");
        builder.sql(self.resource.time_deleted_column);
        builder.sql(" IS NULL), ");

        // collection_info AS (...)
        builder.sql("collection_info AS (");
        builder.sql("SELECT * FROM ");
        builder.sql(self.collection.table_name);
        builder.sql(" WHERE ");
        builder.sql(self.collection.id_column);
        builder.sql(" = ");
        builder.param().bind::<sql_types::Uuid, _>(collection_id);
        builder.sql(" AND ");
        builder.sql(self.collection.time_deleted_column);
        builder.sql(" IS NULL FOR UPDATE), ");

        // resource_info AS (...)
        builder.sql("resource_info AS (");
        builder.sql("SELECT * FROM ");
        builder.sql(self.resource.table_name);
        builder.sql(" WHERE ");
        builder.sql(self.resource.id_column);
        builder.sql(" = ");
        builder.param().bind::<sql_types::Uuid, _>(resource_id);
        builder.sql(" AND ");
        builder.sql(self.resource.time_deleted_column);
        builder.sql(" IS NULL");
        if !self.allow_from_attached {
            builder.sql(" AND ");
            builder.sql(self.resource.collection_id_column);
            builder.sql(" IS NULL");
        }
        builder.sql(" FOR UPDATE), ");

        // do_update AS (...)
        builder.sql("do_update AS (SELECT IF(EXISTS(SELECT ");
        builder.sql(self.collection.id_column);
        builder.sql(" FROM collection_info) AND EXISTS(SELECT ");
        builder.sql(self.resource.id_column);
        builder
            .sql(" FROM resource_info) AND (SELECT * FROM resource_count) < ");
        builder.sql(crate::db::raw_query_builder::TrustedStr::from_u32(
            max_attached_resources,
        ));
        builder.sql(", TRUE, FALSE)), ");

        // updated_resource AS (...)
        builder.sql("updated_resource AS (UPDATE ");
        builder.sql(self.resource.table_name);
        builder.sql(" SET ");

        // Let the caller build the SET clause
        build_set_clause(&mut builder);

        builder.sql(" WHERE ");
        builder.sql(self.resource.id_column);
        builder.sql(" = ");
        builder.param().bind::<sql_types::Uuid, _>(resource_id);
        builder.sql(" AND (SELECT * FROM do_update) RETURNING *) ");

        builder.sql(
            "SELECT * FROM \
            (SELECT * FROM resource_count) \
            LEFT JOIN (SELECT * FROM collection_by_id) ON TRUE \
            LEFT JOIN (SELECT * FROM resource_by_id) ON TRUE \
            LEFT JOIN (SELECT * FROM updated_resource) ON TRUE",
        );

        AttachQuery { query: builder.query() }
    }
}

pub type RawSqlOutput<ResourceType, CollectionType> = (
    BigInt,
    Nullable<SelectableSqlType<CollectionType>>,
    Nullable<SelectableSqlType<ResourceType>>,
    Nullable<SelectableSqlType<ResourceType>>,
);

/// A query built by AttachQueryTemplate that can be executed to attach
/// a resource to a collection.
///
/// This wraps the underlying TypedSqlQuery and provides an execution
/// interface similar to AttachToCollectionStatement.
pub struct AttachQuery<ResourceType, CollectionType>
where
    ResourceType: Selectable<Pg> + Send + 'static,
    CollectionType: Selectable<Pg> + Send + 'static,
{
    query: crate::db::raw_query_builder::TypedSqlQuery<
        RawSqlOutput<ResourceType, CollectionType>,
    >,
}

impl<ResourceType, CollectionType> AttachQuery<ResourceType, CollectionType>
where
    ResourceType: Send + 'static + Selectable<Pg>,
    CollectionType: Send + 'static + Selectable<Pg>,
{
    /// Executes the attach query and parses the result.
    ///
    /// Returns the collection and updated resource on success, or an
    /// AttachError describing why the operation failed.
    pub async fn attach_and_get_result_async(
        self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> AsyncAttachToCollectionResult<ResourceType, CollectionType>
    where
        TypedSqlQuery<RawSqlOutput<ResourceType, CollectionType>>:
            query_methods::LoadQuery<
                    'static,
                    DbConnection,
                    RawOutput<ResourceType, CollectionType>,
                >,
    {
        use async_bb8_diesel::AsyncRunQueryDsl;

        // Execute the query and get the raw tuple result
        let (
            attached_count,
            collection_before_update,
            resource_before_update,
            resource_after_update,
        ): (
            i64,
            Option<CollectionType>,
            Option<ResourceType>,
            Option<ResourceType>,
        ) = self
            .query
            .get_result_async::<RawOutput<ResourceType, CollectionType>>(conn)
            .await
            .map_err(|e| AttachError::DatabaseError(e))?;

        // Parse the result using the same logic as AttachToCollectionStatement
        let collection_before_update = collection_before_update
            .ok_or_else(|| AttachError::CollectionNotFound)?;

        let resource_before_update = resource_before_update
            .ok_or_else(|| AttachError::ResourceNotFound)?;

        match resource_after_update {
            Some(resource) => Ok((collection_before_update, resource)),
            None => Err(AttachError::NoUpdate {
                attached_count,
                resource: resource_before_update,
                collection: collection_before_update,
            }),
        }
    }
}

/// The CTE described in the module docs
#[must_use = "Queries must be executed"]
pub struct AttachToCollectionStatement<ResourceType, V, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreAttachTargetConfig<ResourceType>,
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
    max_attached_resources: u32,

    // Update statement for the resource.
    update_resource_statement:
        BoxedUpdateStatement<'static, Pg, ResourceTable<ResourceType, C>, V>,
    // Describes what should be returned after UPDATE-ing the resource.
    resource_returning_clause: AsSelect<ResourceType, Pg>,
}

impl<ResourceType, V, C> QueryId
    for AttachToCollectionStatement<ResourceType, V, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreAttachTargetConfig<ResourceType>,
{
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

/// Result of [`AttachToCollectionStatement`] when executed asynchronously
pub type AsyncAttachToCollectionResult<ResourceType, CollectionType> = Result<
    (CollectionType, ResourceType),
    AttachError<ResourceType, CollectionType, DieselError>,
>;

/// Errors returned by [`AttachToCollectionStatement`].
#[derive(Debug)]
pub enum AttachError<ResourceType, C, E> {
    /// The collection that the query was inserting into does not exist
    CollectionNotFound,
    /// The resource being attached does not exist
    ResourceNotFound,
    /// Although the resource and collection exist, the update did not occur
    ///
    /// The unchanged resource and collection are returned as a part of this
    /// error; it is the responsibility of the caller to determine which
    /// condition was not met.
    NoUpdate { attached_count: i64, resource: ResourceType, collection: C },
    /// Other database error
    DatabaseError(E),
}

/// Describes the type returned from the actual CTE, which is parsed
/// and interpreted before propagating it to users of the Rust API.
pub type RawOutput<ResourceType, C> =
    (i64, Option<C>, Option<ResourceType>, Option<ResourceType>);

impl<ResourceType, V, C> AttachToCollectionStatement<ResourceType, V, C>
where
    ResourceType: 'static + Debug + Send + Selectable<Pg>,
    C: 'static + Debug + DatastoreAttachTargetConfig<ResourceType> + Send,
    ResourceTable<ResourceType, C>: 'static + Table + Send + Copy + Debug,
    V: 'static + Send,
    AttachToCollectionStatement<ResourceType, V, C>: Send,
{
    /// Issues the CTE asynchronously and parses the result.
    pub async fn attach_and_get_result_async(
        self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> AsyncAttachToCollectionResult<ResourceType, C>
    where
        // We require this bound to ensure that "Self" is runnable as query.
        Self: query_methods::LoadQuery<
                'static,
                DbConnection,
                RawOutput<ResourceType, C>,
            >,
    {
        self.get_result_async::<RawOutput<ResourceType, C>>(conn)
            .await
            // If the database returns an error, propagate it right away.
            .map_err(|e| AttachError::DatabaseError(e))
            // Otherwise, parse the output to determine if the CTE succeeded.
            .and_then(Self::parse_result)
    }

    fn parse_result<E>(
        result: RawOutput<ResourceType, C>,
    ) -> Result<(C, ResourceType), AttachError<ResourceType, C, E>> {
        let (
            attached_count,
            collection_before_update,
            resource_before_update,
            resource_after_update,
        ) = result;

        let collection_before_update = collection_before_update
            .ok_or_else(|| AttachError::CollectionNotFound)?;

        let resource_before_update = resource_before_update
            .ok_or_else(|| AttachError::ResourceNotFound)?;

        match resource_after_update {
            Some(resource) => Ok((collection_before_update, resource)),
            None => Err(AttachError::NoUpdate {
                attached_count,
                resource: resource_before_update,
                collection: collection_before_update,
            }),
        }
    }
}

type SelectableSqlType<Q> =
    <<Q as diesel::Selectable<Pg>>::SelectExpression as Expression>::SqlType;

impl<ResourceType, V, C> Query
    for AttachToCollectionStatement<ResourceType, V, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreAttachTargetConfig<ResourceType>,
{
    type SqlType = (
        // The number of resources attached to the collection before update.
        BigInt,
        // If the collection exists, the value before update.
        Nullable<SelectableSqlType<C>>,
        // If the resource exists, the value before update.
        Nullable<SelectableSqlType<ResourceType>>,
        // If the resource was updated, the new value.
        Nullable<SelectableSqlType<ResourceType>>,
    );
}

impl<ResourceType, V, C> RunQueryDsl<DbConnection>
    for AttachToCollectionStatement<ResourceType, V, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreAttachTargetConfig<ResourceType>,
{
}

/// This implementation uses a CTE which attempts to do the following:
///
/// 1. (collection_by_id, resource_by_id): Identify if the collection and
///    resource objects exist at all.
/// 2. (resource_count): Identify if the number of resources already attached to
///    the collection exceeds a threshold.
/// 3. (collection_info, resource_info): Checks for arbitrary user-provided
///    constraints on the collection and resource objects.
/// 4. (do_update): IFF all previous checks succeeded, make a decision to perfom
///    an update.
/// 5. (updated_resource): Apply user-provided updates on the resource -
///    presumably, setting the collection ID value.
///
/// This is implemented as follows:
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
/// //              <FK> IS NULL AND <Additional user-supplied constraints>
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
/// //      LEFT JOIN (SELECT * FROM updated_resource) ON TRUE;
/// ```
impl<ResourceType, V, C> QueryFragment<Pg>
    for AttachToCollectionStatement<ResourceType, V, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreAttachTargetConfig<ResourceType>,
    CollectionPrimaryKey<ResourceType, C>: diesel::Column,
    // Necessary to "walk_ast" over "self.update_resource_statement".
    BoxedUpdateStatement<'static, Pg, ResourceTable<ResourceType, C>, V>:
        QueryFragment<Pg>,
    // Necessary to "walk_ast" over "self.resource_returning_clause".
    AsSelect<ResourceType, Pg>: QueryFragment<Pg>,
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

        out.push_sql("updated_resource AS (");
        self.update_resource_statement.walk_ast(out.reborrow())?;
        // NOTE: It is safe to start with "AND" - we forced the update statement
        // to have a WHERE clause on the primary key of the resource.
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
        out.push_sql(
            "SELECT * FROM \
            (SELECT * FROM resource_count) \
            LEFT JOIN (SELECT * FROM collection_by_id) ON TRUE \
            LEFT JOIN (SELECT * FROM resource_by_id) ON TRUE \
            LEFT JOIN (SELECT * FROM updated_resource) ON TRUE;",
        );

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::identity::Resource as IdentityResource;
    use crate::db::pub_test_utils::TestDatabase;
    use async_bb8_diesel::{AsyncRunQueryDsl, AsyncSimpleConnection};
    use chrono::Utc;
    use db_macros::Resource;
    use diesel::QueryDsl;
    use diesel::SelectableHelper;
    use diesel::expression_methods::ExpressionMethods;
    use diesel::pg::Pg;
    use nexus_db_lookup::DataStoreConnection;
    use omicron_common::api::external::{IdentityMetadataCreateParams, Name};
    use omicron_test_utils::dev;
    use uuid::Uuid;

    table! {
        test_schema.collection (id) {
            id -> Uuid,
            name -> Text,
            description -> Text,
            time_created -> Timestamptz,
            time_modified -> Timestamptz,
            time_deleted -> Nullable<Timestamptz>,
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

    async fn setup_db(pool: &crate::db::Pool) -> DataStoreConnection {
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
                     time_deleted TIMESTAMPTZ); \
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
        connection
    }

    /// Describes a resource within the database.
    #[derive(
        Clone, Queryable, Insertable, Debug, Resource, Selectable, PartialEq,
    )]
    #[diesel(table_name = resource)]
    struct Resource {
        #[diesel(embed)]
        pub identity: ResourceIdentity,
        pub collection_id: Option<uuid::Uuid>,
    }

    #[derive(
        Clone, Queryable, Insertable, Debug, Resource, Selectable, PartialEq,
    )]
    #[diesel(table_name = collection)]
    struct Collection {
        #[diesel(embed)]
        pub identity: CollectionIdentity,
    }

    impl DatastoreAttachTargetConfig<Resource> for Collection {
        type Id = uuid::Uuid;

        type CollectionIdColumn = collection::dsl::id;
        type CollectionTimeDeletedColumn = collection::dsl::time_deleted;

        type ResourceIdColumn = resource::dsl::id;
        type ResourceCollectionIdColumn = resource::dsl::collection_id;
        type ResourceTimeDeletedColumn = resource::dsl::time_deleted;
    }

    async fn insert_collection(
        id: Uuid,
        name: &str,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Collection {
        let create_params = IdentityMetadataCreateParams {
            name: Name::try_from(name.to_string()).unwrap(),
            description: "description".to_string(),
        };
        let c =
            Collection { identity: CollectionIdentity::new(id, create_params) };

        diesel::insert_into(collection::table)
            .values(c)
            .execute_async(conn)
            .await
            .unwrap();

        get_collection(id, &conn).await
    }

    async fn get_collection(
        id: Uuid,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Collection {
        collection::table
            .find(id)
            .select(Collection::as_select())
            .first_async(conn)
            .await
            .unwrap()
    }

    async fn insert_resource(
        id: Uuid,
        name: &str,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Resource {
        let create_params = IdentityMetadataCreateParams {
            name: Name::try_from(name.to_string()).unwrap(),
            description: "description".to_string(),
        };
        let r = Resource {
            identity: ResourceIdentity::new(id, create_params),
            collection_id: None,
        };

        diesel::insert_into(resource::table)
            .values(r)
            .execute_async(conn)
            .await
            .unwrap();

        get_resource(id, conn).await
    }

    async fn get_resource(
        id: Uuid,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Resource {
        resource::table
            .find(id)
            .select(Resource::as_select())
            .first_async(conn)
            .await
            .unwrap()
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
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(collection_id)),
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
                    \"test_schema\".\"collection\".\"time_deleted\" \
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
                    \"test_schema\".\"collection\".\"time_deleted\" \
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
                WHERE ((\
                    (\"test_schema\".\"resource\".\"id\" = $5) AND \
                    (\"test_schema\".\"resource\".\"time_deleted\" IS NULL)) AND \
                    (\"test_schema\".\"resource\".\"collection_id\" IS NULL)\
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
                LEFT JOIN (SELECT * FROM updated_resource) ON TRUE; -- binds: [cccccccc-cccc-cccc-cccc-cccccccccccc, aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, cccccccc-cccc-cccc-cccc-cccccccccccc, cccccccc-cccc-cccc-cccc-cccccccccccc, aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, cccccccc-cccc-cccc-cccc-cccccccccccc, aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa]";
        assert_eq!(query, expected_query);
    }

    #[tokio::test]
    async fn test_attach_missing_collection_fails() {
        let logctx =
            dev::test_setup_log("test_attach_missing_collection_fails");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = setup_db(pool).await;

        let collection_id = uuid::Uuid::new_v4();
        let resource_id = uuid::Uuid::new_v4();
        let attach = Collection::attach_resource(
            collection_id,
            resource_id,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            10,
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(collection_id)),
        )
        .attach_and_get_result_async(&conn)
        .await;

        assert!(matches!(attach, Err(AttachError::CollectionNotFound)));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_attach_missing_resource_fails() {
        let logctx = dev::test_setup_log("test_attach_missing_resource_fails");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();

        let conn = setup_db(&pool).await;

        let collection_id = uuid::Uuid::new_v4();
        let resource_id = uuid::Uuid::new_v4();

        // Create the collection
        let collection =
            insert_collection(collection_id, "collection", &conn).await;

        // Attempt to attach - even though the resource does not exist.
        let attach = Collection::attach_resource(
            collection_id,
            resource_id,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            10,
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(collection_id)),
        )
        .attach_and_get_result_async(&conn)
        .await;

        assert!(matches!(attach, Err(AttachError::ResourceNotFound)));
        // The collection should remain unchanged.
        assert_eq!(collection, get_collection(collection_id, &conn).await);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_attach_once() {
        let logctx = dev::test_setup_log("test_attach_once");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();

        let conn = setup_db(&pool).await;

        let collection_id = uuid::Uuid::new_v4();
        let resource_id = uuid::Uuid::new_v4();

        // Create the collection and resource.
        let _collection =
            insert_collection(collection_id, "collection", &conn).await;
        let _resource = insert_resource(resource_id, "resource", &conn).await;

        // Attach the resource to the collection.
        let attach = Collection::attach_resource(
            collection_id,
            resource_id,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            10,
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(collection_id)),
        )
        .attach_and_get_result_async(&conn)
        .await;

        // "attach_and_get_result_async" should return the "attached" resource.
        let (returned_collection, returned_resource) =
            attach.expect("Attach should have worked");
        assert_eq!(
            returned_resource.collection_id.expect("Expected a collection ID"),
            collection_id
        );
        // The returned value should be the latest value in the DB.
        assert_eq!(
            returned_collection,
            get_collection(collection_id, &conn).await
        );
        assert_eq!(returned_resource, get_resource(resource_id, &conn).await);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_attach_once_synchronous() {
        let logctx = dev::test_setup_log("test_attach_once_synchronous");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();

        let conn = setup_db(&pool).await;

        let collection_id = uuid::Uuid::new_v4();
        let resource_id = uuid::Uuid::new_v4();

        // Create the collection and resource.
        let _collection =
            insert_collection(collection_id, "collection", &conn).await;
        let _resource = insert_resource(resource_id, "resource", &conn).await;

        // Attach the resource to the collection.
        let attach_query = Collection::attach_resource(
            collection_id,
            resource_id,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            10,
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(collection_id)),
        );

        // "attach_and_get_result" should return the "attached" resource.
        let (returned_collection, returned_resource) = attach_query
            .attach_and_get_result_async(&conn)
            .await
            .expect("Attach should have worked");

        assert_eq!(
            returned_resource.collection_id.expect("Expected a collection ID"),
            collection_id
        );
        // The returned values should be the latest value in the DB.
        assert_eq!(
            returned_collection,
            get_collection(collection_id, &conn).await
        );
        assert_eq!(returned_resource, get_resource(resource_id, &conn).await);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_attach_multiple_times() {
        let logctx = dev::test_setup_log("test_attach_multiple_times");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = setup_db(pool).await;

        const RESOURCE_COUNT: u32 = 5;

        let collection_id = uuid::Uuid::new_v4();

        // Create the collection.
        let _collection =
            insert_collection(collection_id, "collection", &conn).await;

        // Create each resource, attaching them to the collection.
        for i in 0..RESOURCE_COUNT {
            let resource_id = uuid::Uuid::new_v4();
            insert_resource(resource_id, &format!("resource{}", i), &conn)
                .await;

            // Attach the resource to the collection.
            let attach = Collection::attach_resource(
                collection_id,
                resource_id,
                collection::table.into_boxed(),
                resource::table.into_boxed(),
                RESOURCE_COUNT,
                diesel::update(resource::table)
                    .set(resource::dsl::collection_id.eq(collection_id)),
            )
            .attach_and_get_result_async(&conn)
            .await;

            // "attach_and_get_result_async" should return the "attached" resource.
            let (_, returned_resource) =
                attach.expect("Attach should have worked");
            assert_eq!(
                returned_resource
                    .collection_id
                    .expect("Expected a collection ID"),
                collection_id
            );
            // The returned resource value should be the latest value in the DB.
            assert_eq!(
                returned_resource,
                get_resource(resource_id, &conn).await
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_attach_beyond_capacity_fails() {
        let logctx = dev::test_setup_log("test_attach_beyond_capacity_fails");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = setup_db(pool).await;

        let collection_id = uuid::Uuid::new_v4();

        // Attach a resource to a collection, as usual.
        let _collection =
            insert_collection(collection_id, "collection", &conn).await;
        let resource_id1 = uuid::Uuid::new_v4();
        let _resource = insert_resource(resource_id1, "resource1", &conn).await;
        let attach = Collection::attach_resource(
            collection_id,
            resource_id1,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            1,
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(collection_id)),
        )
        .attach_and_get_result_async(&conn)
        .await;
        assert_eq!(
            attach.expect("Attach should have worked").1.id(),
            resource_id1
        );

        // Let's try attaching a second resource, now that we're at capacity.
        let resource_id2 = uuid::Uuid::new_v4();
        let _resource = insert_resource(resource_id2, "resource2", &conn).await;
        let attach = Collection::attach_resource(
            collection_id,
            resource_id2,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            1,
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(collection_id)),
        )
        .attach_and_get_result_async(&conn)
        .await;

        let err = attach.expect_err("Should have failed to attach");
        match err {
            AttachError::NoUpdate { attached_count, resource, collection } => {
                assert_eq!(attached_count, 1);
                assert_eq!(resource, get_resource(resource_id2, &conn).await);
                assert_eq!(
                    collection,
                    get_collection(collection_id, &conn).await
                );
            }
            _ => panic!("Unexpected error: {:?}", err),
        };

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_attach_while_already_attached() {
        let logctx = dev::test_setup_log("test_attach_while_already_attached");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = setup_db(pool).await;

        let collection_id = uuid::Uuid::new_v4();

        // Attach a resource to a collection, as usual.
        let _collection =
            insert_collection(collection_id, "collection", &conn).await;
        let resource_id = uuid::Uuid::new_v4();
        let _resource = insert_resource(resource_id, "resource", &conn).await;
        let attach = Collection::attach_resource(
            collection_id,
            resource_id,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            10,
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(collection_id)),
        )
        .attach_and_get_result_async(&conn)
        .await;
        assert_eq!(
            attach.expect("Attach should have worked").1.id(),
            resource_id
        );

        // Try attaching when well below the capacity.
        let attach = Collection::attach_resource(
            collection_id,
            resource_id,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            10,
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(collection_id)),
        )
        .attach_and_get_result_async(&conn)
        .await;
        let err = attach.expect_err("Should have failed to attach");

        // A caller should be able to inspect this result, see that the count of
        // attached devices is below capacity, and that resource.collection_id
        // is already set. This should provide enough context to identify "the
        // resource is already attached".
        match err {
            AttachError::NoUpdate { attached_count, resource, collection } => {
                assert_eq!(attached_count, 1);
                assert_eq!(
                    *resource
                        .collection_id
                        .as_ref()
                        .expect("Should already be attached"),
                    collection_id
                );
                assert_eq!(resource, get_resource(resource_id, &conn).await);
                assert_eq!(
                    collection,
                    get_collection(collection_id, &conn).await
                );
            }
            _ => panic!("Unexpected error: {:?}", err),
        };

        // Let's try attaching the same resource again - while at capacity.
        let attach = Collection::attach_resource(
            collection_id,
            resource_id,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            1,
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(collection_id)),
        )
        .attach_and_get_result_async(&conn)
        .await;
        let err = attach.expect_err("Should have failed to attach");
        // Even when at capacity, the same information should be propagated back
        // to the caller.
        match err {
            AttachError::NoUpdate { attached_count, resource, collection } => {
                assert_eq!(attached_count, 1);
                assert_eq!(
                    *resource
                        .collection_id
                        .as_ref()
                        .expect("Should already be attached"),
                    collection_id
                );
                assert_eq!(resource, get_resource(resource_id, &conn).await);
                assert_eq!(
                    collection,
                    get_collection(collection_id, &conn).await
                );
            }
            _ => panic!("Unexpected error: {:?}", err),
        };

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_attach_with_filters() {
        let logctx = dev::test_setup_log("test_attach_once");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = setup_db(pool).await;

        let collection_id = uuid::Uuid::new_v4();
        let resource_id = uuid::Uuid::new_v4();

        // Create the collection and resource.
        let _collection =
            insert_collection(collection_id, "collection", &conn).await;
        let _resource = insert_resource(resource_id, "resource", &conn).await;

        // Attach the resource to the collection.
        //
        // Note that we are also filtering for specific conditions on the
        // collection and resource - admittedly, just the name, but this could
        // also be used to check the state of a disk, instance, etc.
        let attach = Collection::attach_resource(
            collection_id,
            resource_id,
            collection::table
                .filter(collection::name.eq("collection"))
                .into_boxed(),
            resource::table.filter(resource::name.eq("resource")).into_boxed(),
            10,
            // When actually performing the update, update the collection ID
            // as well as an auxiliary field - the description.
            //
            // This provides an example of how one could attach an ID and update
            // the state of a resource simultaneously.
            diesel::update(resource::table).set((
                resource::dsl::collection_id.eq(collection_id),
                resource::dsl::description.eq("new description".to_string()),
            )),
        )
        .attach_and_get_result_async(&conn)
        .await;

        let (_, returned_resource) = attach.expect("Attach should have worked");
        assert_eq!(
            returned_resource.collection_id.expect("Expected a collection ID"),
            collection_id
        );
        assert_eq!(returned_resource, get_resource(resource_id, &conn).await);
        assert_eq!(returned_resource.description(), "new description");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_attach_deleted_resource_fails() {
        let logctx = dev::test_setup_log("test_attach_deleted_resource_fails");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = setup_db(pool).await;

        let collection_id = uuid::Uuid::new_v4();
        let resource_id = uuid::Uuid::new_v4();

        // Create the collection and resource.
        let _collection =
            insert_collection(collection_id, "collection", &conn).await;
        let _resource = insert_resource(resource_id, "resource", &conn).await;

        // Immediately soft-delete the resource.
        diesel::update(
            resource::table.filter(resource::dsl::id.eq(resource_id)),
        )
        .set(resource::dsl::time_deleted.eq(Utc::now()))
        .execute_async(&*conn)
        .await
        .unwrap();

        // Attach the resource to the collection. Observe a failure which is
        // indistinguishable from the resource not existing.
        let attach = Collection::attach_resource(
            collection_id,
            resource_id,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            10,
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(collection_id)),
        )
        .attach_and_get_result_async(&conn)
        .await;
        assert!(matches!(attach, Err(AttachError::ResourceNotFound)));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_attach_without_update_filter() {
        let logctx = dev::test_setup_log("test_attach_without_update_filter");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = setup_db(pool).await;

        let collection_id = uuid::Uuid::new_v4();

        // Create the collection and some resources.
        let _collection =
            insert_collection(collection_id, "collection", &conn).await;
        let resource_id1 = uuid::Uuid::new_v4();
        let resource_id2 = uuid::Uuid::new_v4();
        let _resource1 =
            insert_resource(resource_id1, "resource1", &conn).await;
        let _resource2 =
            insert_resource(resource_id2, "resource2", &conn).await;

        // Attach the resource to the collection.
        //
        // NOTE: In the update statement, we aren't filtering by resource ID,
        // even though we explicitly have two "live" resources".
        let attach = Collection::attach_resource(
            collection_id,
            resource_id1,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            10,
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(collection_id)),
        )
        .attach_and_get_result_async(&conn)
        .await;

        let (_, returned_resource) = attach.expect("Attach should have worked");
        assert_eq!(returned_resource.id(), resource_id1);

        // Note that only "resource1" should be attached.
        // "resource2" should have automatically been filtered away from the
        // update statement, regardless of user input.
        assert_eq!(
            get_resource(resource_id1, &conn).await.collection_id.unwrap(),
            collection_id
        );
        assert!(
            get_resource(resource_id2, &conn).await.collection_id.is_none()
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
