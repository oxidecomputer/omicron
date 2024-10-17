// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CTE for detaching multiple resources from a collection.
//!
//! This atomically:
//! - Checks if the collection exists and is not soft deleted
//! - Validates conditions on both the collection and resources
//! - Updates the collection row
//! - Updates the resource rows

use super::collection_attach::aliases::*;
use super::cte_utils::{
    BoxableTable, BoxableUpdateStatement, BoxedQuery, ExprSqlType, FilterBy,
    QueryFromClause, QuerySqlType,
};
use super::pool::DbConnection;
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
use diesel::sql_types::{Nullable, SingleValue};
use nexus_db_model::DatastoreAttachTargetConfig;
use std::fmt::Debug;

/// Trait to be implemented by structs representing a detachable collection.
///
/// A blanket implementation is provided for traits that implement
/// [`crate::db::collection_attach::DatastoreAttachTarget`].
pub trait DatastoreDetachManyTarget<ResourceType>:
    DatastoreAttachTargetConfig<ResourceType>
{
    /// Creates a statement for detaching a resource from the given collection.
    ///
    /// This statement allows callers to atomically check the state of a
    /// collection and a resource while detaching a resource.
    ///
    /// - `collection_id`: Primary key of the collection being removed from.
    /// - `collection_query`: An optional query for collection state. The
    /// CTE will automatically filter this query to `collection_id`, and
    /// validate that the "time deleted" column is NULL.
    /// - `resource_query`: An optional query for the resource state. The
    /// CTE will automatically filter this query to non-deleted resources.
    /// validate that the "time deleted" column is NULL, and validate that the
    /// "collection_id" column points to `collection_id`.
    /// - `update_collection`: An update statement, identifying how the
    /// collection object should be modified as associated resources are
    /// detached.
    /// - `update_resource`: An update statement, identifying how the resource
    /// objects should be modified to be detached
    ///
    /// The VC, VR types refer to the "update target" of the UpdateStatements,
    /// and should generally be inferred rather than explicitly specified.
    fn detach_resources<VC, VR>(
        collection_id: Self::Id,

        collection_query: BoxedQuery<CollectionTable<ResourceType, Self>>,
        resource_query: BoxedQuery<ResourceTable<ResourceType, Self>>,

        update_collection: UpdateStatement<
            CollectionTable<ResourceType, Self>,
            CollectionTableDefaultWhereClause<ResourceType, Self>,
            VC,
        >,
        update_resource: UpdateStatement<
            ResourceTable<ResourceType, Self>,
            ResourceTableDefaultWhereClause<ResourceType, Self>,
            VR,
        >,
    ) -> DetachManyFromCollectionStatement<ResourceType, VC, VR, Self>
    where
        // Treat the collection and resource as boxed tables.
        CollectionTable<ResourceType, Self>: BoxableTable,
        ResourceTable<ResourceType, Self>: BoxableTable,

        // Allows treating "collection_exists_query" as a boxed "dyn QueryFragment<Pg>".
        QueryFromClause<CollectionTable<ResourceType, Self>>:
            QueryFragment<Pg> + Send,
        QuerySqlType<CollectionTable<ResourceType, Self>>: Send,
        // Allows treating "resource_exists_query" as a boxed "dyn QueryFragment<Pg>".
        QueryFromClause<ResourceTable<ResourceType, Self>>:
            QueryFragment<Pg> + Send,
        QuerySqlType<ResourceTable<ResourceType, Self>>: Send,

        // Allows calling ".filter()" on the boxed collection table.
        BoxedQuery<CollectionTable<ResourceType, Self>>: FilterBy<Eq<CollectionPrimaryKey<ResourceType, Self>, Self::Id>>
            + FilterBy<IsNull<Self::CollectionTimeDeletedColumn>>,
        // Allows calling ".filter()" on the boxed resource table.
        BoxedQuery<ResourceTable<ResourceType, Self>>: FilterBy<Eq<Self::ResourceCollectionIdColumn, Self::Id>>
            + FilterBy<IsNull<Self::ResourceTimeDeletedColumn>>,

        // Allows calling "update.into_boxed()"
        UpdateStatement<
            CollectionTable<ResourceType, Self>,
            CollectionTableDefaultWhereClause<ResourceType, Self>,
            VC,
        >: BoxableUpdateStatement<CollectionTable<ResourceType, Self>, VC>,
        UpdateStatement<
            ResourceTable<ResourceType, Self>,
            ResourceTableDefaultWhereClause<ResourceType, Self>,
            VR,
        >: BoxableUpdateStatement<ResourceTable<ResourceType, Self>, VR>,

        // Allows calling
        // ".filter(collection_table().primary_key().eq(collection_id)" on the
        // boxed update statement.
        BoxedUpdateStatement<
            'static,
            Pg,
            CollectionTable<ResourceType, Self>,
            VC,
        >: FilterBy<Eq<CollectionPrimaryKey<ResourceType, Self>, Self::Id>>,
        // Allows calling
        // ".filter(Self::ResourceTimeDeletedColumn::default().is_null())"
        BoxedUpdateStatement<
            'static,
            Pg,
            ResourceTable<ResourceType, Self>,
            VR,
        >: FilterBy<Eq<Self::ResourceCollectionIdColumn, Self::Id>>
            + FilterBy<IsNull<Self::ResourceTimeDeletedColumn>>,

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

        ResourceType: Selectable<Pg>,
    {
        let collection_table =
            || <CollectionTable<ResourceType, Self> as HasTable>::table();

        // Create new queries to determine if the collection exists.
        let collection_exists_query = Box::new(
            collection_table()
                .into_boxed()
                .filter(collection_table().primary_key().eq(collection_id))
                .filter(Self::CollectionTimeDeletedColumn::default().is_null()),
        );

        // For the queries which decide whether or not we'll perform the update,
        // extend the user-provided arguments.
        //
        // We force these queries to:
        // - Check against the primary key of the target objects
        // - Ensure the objects are not deleted
        // - (for the resources) Ensure they are attached
        // - (for the update) Ensure that only the collection with "collection_id"
        // is modified.
        let collection_query = Box::new(
            collection_query
                .filter(collection_table().primary_key().eq(collection_id))
                .filter(Self::CollectionTimeDeletedColumn::default().is_null()),
        );
        let resource_query = Box::new(
            resource_query
                .filter(Self::ResourceTimeDeletedColumn::default().is_null())
                .filter(
                    Self::ResourceCollectionIdColumn::default()
                        .eq(collection_id),
                ),
        );

        let update_collection_statement = update_collection
            .into_boxed()
            .filter(collection_table().primary_key().eq(collection_id));

        let update_resource_statement = update_resource
            .into_boxed()
            .filter(Self::ResourceTimeDeletedColumn::default().is_null())
            .filter(
                Self::ResourceCollectionIdColumn::default().eq(collection_id),
            );

        let collection_returning_clause = Self::as_returning();
        DetachManyFromCollectionStatement {
            collection_exists_query,
            collection_query,
            resource_query,
            update_collection_statement,
            update_resource_statement,
            collection_returning_clause,
        }
    }
}

impl<T, ResourceType> DatastoreDetachManyTarget<ResourceType> for T where
    T: DatastoreAttachTargetConfig<ResourceType>
{
}

/// The CTE described in the module docs
#[must_use = "Queries must be executed"]
pub struct DetachManyFromCollectionStatement<ResourceType, VC, VR, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreDetachManyTarget<ResourceType>,
{
    // Query which answers: "Does the collection exist?"
    collection_exists_query: Box<dyn QueryFragment<Pg> + Send>,
    // A (mostly) user-provided query for validating the collection.
    collection_query: Box<dyn QueryFragment<Pg> + Send>,
    // A (mostly) user-provided query for validating the resource.
    resource_query: Box<dyn QueryFragment<Pg> + Send>,

    // Update statement for the collection.
    update_collection_statement:
        BoxedUpdateStatement<'static, Pg, CollectionTable<ResourceType, C>, VC>,
    // Update statement for the resource.
    update_resource_statement:
        BoxedUpdateStatement<'static, Pg, ResourceTable<ResourceType, C>, VR>,
    // Describes what should be returned after UPDATE-ing the resource.
    collection_returning_clause: AsSelect<C, Pg>,
}

impl<ResourceType, VC, VR, C> QueryId
    for DetachManyFromCollectionStatement<ResourceType, VC, VR, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreDetachManyTarget<ResourceType>,
{
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

/// Result of [`DetachManyFromCollectionStatement`] when executed asynchronously
pub type AsyncDetachManyFromCollectionResult<C> =
    Result<C, DetachManyError<C, DieselError>>;

/// Errors returned by [`DetachManyFromCollectionStatement`].
#[derive(Debug)]
pub enum DetachManyError<C, E> {
    /// The collection that the query was removing from does not exist
    CollectionNotFound,
    /// Although the collection exists, the update did not occur
    ///
    /// The unchanged resource and collection are returned as a part of this
    /// error; it is the responsibility of the caller to determine which
    /// condition was not met.
    NoUpdate { collection: C },
    /// Other database error
    DatabaseError(E),
}

/// Describes the type returned from the actual CTE, which is parsed
/// and interpreted before propagating it to users of the Rust API.
pub type RawOutput<C> = (i64, Option<C>, Option<C>);

impl<ResourceType, VC, VR, C>
    DetachManyFromCollectionStatement<ResourceType, VC, VR, C>
where
    ResourceType: 'static + Debug + Send + Selectable<Pg>,
    C: 'static + Debug + DatastoreDetachManyTarget<ResourceType> + Send,
    ResourceTable<ResourceType, C>: 'static + Table + Send + Copy + Debug,
    VC: 'static + Send,
    VR: 'static + Send,
    DetachManyFromCollectionStatement<ResourceType, VC, VR, C>: Send,
{
    /// Issues the CTE asynchronously and parses the result.
    pub async fn detach_and_get_result_async(
        self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> AsyncDetachManyFromCollectionResult<C>
    where
        // We require this bound to ensure that "Self" is runnable as query.
        Self: query_methods::LoadQuery<'static, DbConnection, RawOutput<C>>,
    {
        self.get_result_async::<RawOutput<C>>(conn)
            .await
            // If the database returns an error, propagate it right away.
            .map_err(|e| DetachManyError::DatabaseError(e))
            // Otherwise, parse the output to determine if the CTE succeeded.
            .and_then(Self::parse_result)
    }

    fn parse_result<E>(
        result: RawOutput<C>,
    ) -> Result<C, DetachManyError<C, E>> {
        let (_, collection_before_update, collection_after_update) = result;

        let collection_before_update = collection_before_update
            .ok_or_else(|| DetachManyError::CollectionNotFound)?;

        match collection_after_update {
            Some(collection) => Ok(collection),
            None => Err(DetachManyError::NoUpdate {
                collection: collection_before_update,
            }),
        }
    }
}

type SelectableSqlType<Q> =
    <<Q as diesel::Selectable<Pg>>::SelectExpression as Expression>::SqlType;

impl<ResourceType, VC, VR, C> Query
    for DetachManyFromCollectionStatement<ResourceType, VC, VR, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreDetachManyTarget<ResourceType>,
{
    type SqlType = (
        // Ignored "SELECT 1" value
        diesel::sql_types::BigInt,
        // If the collection exists, the value before update.
        Nullable<SelectableSqlType<C>>,
        // If the collection was updated, the new value.
        Nullable<SelectableSqlType<C>>,
    );
}

impl<ResourceType, VC, VR, C> RunQueryDsl<DbConnection>
    for DetachManyFromCollectionStatement<ResourceType, VC, VR, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreDetachManyTarget<ResourceType>,
{
}

/// This implementation uses a CTE which attempts to do the following:
///
/// 1. (collection_by_id): Identify if the collection exists at all.
/// 2. (collection_info, resource_info): Checks for arbitrary user-provided
///    constraints on the collection and resource objects.
/// 3. (do_update): IFF all previous checks succeeded, make a decision to perfom
///    an update.
/// 4. (updated_collection, updated_resource): Apply user-provided updates on
///    the collection and resource - presumably, setting the collection ID
///    value.
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
/// //          WHERE <time_deleted> IS NULL AND
/// //              <FK> = <VALUE> AND <Additional user-supplied constraints>
/// //          FOR UPDATE
/// //      ),
/// //      /* Make a decision on whether or not to apply ANY updates */
/// //      do_update AS (
/// //          SELECT IF(
/// //              EXISTS(SELECT id FROM collection_info)
/// //          TRUE, FALSE),
/// //      ),
/// //      /* Update the collection */
/// //      updated_collection AS (
/// //          UPDATE C SET <User-supplied Update>
/// //          WHERE <PK> IN (SELECT <PK> FROM collection_info) AND (SELECT * FROM do_update)
/// //          RETURNING *
/// //      ),
/// //      /* Update the resource */
/// //      updated_resource AS (
/// //          UPDATE R SET <User-supplied Update>
/// //          WHERE (id IN (SELECT id FROM resource_info)) AND (SELECT * FROM do_update)
/// //          RETURNING 1
/// //      )
/// //  SELECT * FROM
/// //      (SELECT 1)
/// //      LEFT JOIN (SELECT * FROM collection_by_id) ON TRUE
/// //      LEFT JOIN (SELECT * FROM updated_collection) ON TRUE;
/// ```
impl<ResourceType, VC, VR, C> QueryFragment<Pg>
    for DetachManyFromCollectionStatement<ResourceType, VC, VR, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreDetachManyTarget<ResourceType>,
    ResourcePrimaryKey<ResourceType, C>: diesel::Column,
    // Necessary to "walk_ast" over "self.update_collection_statement".
    BoxedUpdateStatement<'static, Pg, CollectionTable<ResourceType, C>, VC>:
        QueryFragment<Pg>,
    // Necessary to "walk_ast" over "self.update_resource_statement".
    BoxedUpdateStatement<'static, Pg, ResourceTable<ResourceType, C>, VR>:
        QueryFragment<Pg>,
    // Necessary to "walk_ast" over "self.collection_returning_clause".
    AsSelect<C, Pg>: QueryFragment<Pg>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();
        out.push_sql("WITH collection_by_id AS (");
        self.collection_exists_query.walk_ast(out.reborrow())?;
        out.push_sql(" FOR UPDATE), ");

        out.push_sql("collection_info AS (");
        self.collection_query.walk_ast(out.reborrow())?;
        out.push_sql(" FOR UPDATE), ");

        out.push_sql("resource_info AS (");
        self.resource_query.walk_ast(out.reborrow())?;
        out.push_sql(" FOR UPDATE), ");

        out.push_sql("do_update AS (SELECT IF(EXISTS(SELECT ");
        out.push_identifier(CollectionIdColumn::<ResourceType, C>::NAME)?;
        out.push_sql(" FROM collection_info), TRUE,FALSE)), ");

        out.push_sql("updated_collection AS (");
        self.update_collection_statement.walk_ast(out.reborrow())?;
        // NOTE: It is safe to start with "AND" - we forced the update statement
        // to have a WHERE clause on the primary key of the resource.
        out.push_sql(" AND (SELECT * FROM do_update)");
        out.push_sql(" RETURNING ");
        self.collection_returning_clause.walk_ast(out.reborrow())?;
        out.push_sql("), ");

        out.push_sql("updated_resource AS (");
        self.update_resource_statement.walk_ast(out.reborrow())?;
        // NOTE: It is safe to start with "AND" - we forced the update statement
        // to have a WHERE clause on the time deleted column.
        out.push_sql(" AND (");
        out.push_identifier(ResourcePrimaryKey::<ResourceType, C>::NAME)?;
        out.push_sql(" IN (SELECT ");
        out.push_identifier(ResourcePrimaryKey::<ResourceType, C>::NAME)?;
        out.push_sql(" FROM resource_info))");
        out.push_sql(" AND (SELECT * FROM do_update) RETURNING 1) ");

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
            (SELECT 1) \
            LEFT JOIN (SELECT * FROM collection_by_id) ON TRUE \
            LEFT JOIN (SELECT * FROM updated_collection) ON TRUE;",
        );

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::collection_attach::DatastoreAttachTarget;
    use crate::db::datastore::pub_test_utils::TestDatabase;
    use crate::db::identity::Resource as IdentityResource;
    use async_bb8_diesel::{AsyncRunQueryDsl, AsyncSimpleConnection};
    use chrono::Utc;
    use db_macros::Resource;
    use diesel::expression_methods::ExpressionMethods;
    use diesel::pg::Pg;
    use diesel::QueryDsl;
    use diesel::SelectableHelper;
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

        get_collection(id, conn).await
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

    async fn attach_resource(
        collection_id: Uuid,
        resource_id: Uuid,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) {
        Collection::attach_resource(
            collection_id,
            resource_id,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            100,
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(collection_id)),
        )
        .attach_and_get_result_async(conn)
        .await
        .unwrap();
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
        let _resource_id =
            uuid::Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
                .unwrap();
        let detach = Collection::detach_resources(
            collection_id,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            diesel::update(collection::table)
                .set(collection::dsl::description.eq("Updated desc")),
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(Option::<Uuid>::None)),
        );
        let query = diesel::debug_query::<Pg, _>(&detach).to_string();

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
                    (\"test_schema\".\"collection\".\"id\" = $2) AND \
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
                    (\"test_schema\".\"resource\".\"time_deleted\" IS NULL) AND \
                    (\"test_schema\".\"resource\".\"collection_id\" = $3)\
                ) FOR UPDATE\
            ), \
            do_update AS (\
                SELECT IF(\
                    EXISTS(SELECT \"id\" FROM collection_info), \
                TRUE,\
                FALSE)\
            ), \
            updated_collection AS (\
                UPDATE \
                    \"test_schema\".\"collection\" \
                SET \
                    \"description\" = $4 \
                WHERE \
                    (\"test_schema\".\"collection\".\"id\" = $5) AND \
                    (SELECT * FROM do_update) \
                RETURNING \
                    \"test_schema\".\"collection\".\"id\", \
                    \"test_schema\".\"collection\".\"name\", \
                    \"test_schema\".\"collection\".\"description\", \
                    \"test_schema\".\"collection\".\"time_created\", \
                    \"test_schema\".\"collection\".\"time_modified\", \
                    \"test_schema\".\"collection\".\"time_deleted\"\
            ), \
            updated_resource AS (\
                UPDATE \
                    \"test_schema\".\"resource\" \
                SET \
                    \"collection_id\" = $6 \
                WHERE \
                    ((\"test_schema\".\"resource\".\"time_deleted\" IS NULL) AND \
                     (\"test_schema\".\"resource\".\"collection_id\" = $7)) AND \
                    (\"id\" IN (SELECT \"id\" FROM resource_info)) AND \
                    (SELECT * FROM do_update) \
                RETURNING 1\
            ) \
            SELECT * FROM \
                (SELECT 1) \
                LEFT JOIN (SELECT * FROM collection_by_id) ON TRUE \
                LEFT JOIN (SELECT * FROM updated_collection) ON TRUE; -- binds: [cccccccc-cccc-cccc-cccc-cccccccccccc, cccccccc-cccc-cccc-cccc-cccccccccccc, cccccccc-cccc-cccc-cccc-cccccccccccc, \"Updated desc\", cccccccc-cccc-cccc-cccc-cccccccccccc, None, cccccccc-cccc-cccc-cccc-cccccccccccc]";
        assert_eq!(query, expected_query);
    }

    #[tokio::test]
    async fn test_detach_missing_collection_fails() {
        let logctx =
            dev::test_setup_log("test_detach_missing_collection_fails");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = setup_db(pool).await;

        let collection_id = uuid::Uuid::new_v4();
        let _resource_id = uuid::Uuid::new_v4();
        let detach = Collection::detach_resources(
            collection_id,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            diesel::update(collection::table)
                .set(collection::dsl::description.eq("Updated desc")),
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(Option::<Uuid>::None)),
        )
        .detach_and_get_result_async(&conn)
        .await;

        assert!(matches!(detach, Err(DetachManyError::CollectionNotFound)));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_detach_missing_resource_succeeds() {
        let logctx =
            dev::test_setup_log("test_detach_missing_resource_succeeds");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = setup_db(pool).await;

        let collection_id = uuid::Uuid::new_v4();
        let _resource_id = uuid::Uuid::new_v4();

        // Create the collection
        let _collection =
            insert_collection(collection_id, "collection", &conn).await;

        // Attempt to detach - even though the resource does not exist.
        let detach = Collection::detach_resources(
            collection_id,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            diesel::update(collection::table)
                .set(collection::dsl::description.eq("Updated desc")),
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(Option::<Uuid>::None)),
        )
        .detach_and_get_result_async(&conn)
        .await;

        let returned_collection = detach.expect("Detach should have worked");
        assert_eq!(returned_collection.description(), "Updated desc");
        // The collection should still be updated.
        assert_eq!(
            returned_collection,
            get_collection(collection_id, &conn).await
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_detach_once() {
        let logctx = dev::test_setup_log("test_detach_once");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = setup_db(pool).await;

        let collection_id = uuid::Uuid::new_v4();
        let resource_id = uuid::Uuid::new_v4();

        // Create the collection and resource. Attach them.
        let _collection =
            insert_collection(collection_id, "collection", &conn).await;
        let _resource = insert_resource(resource_id, "resource", &conn).await;
        attach_resource(collection_id, resource_id, &conn).await;

        // Detach the resource from the collection.
        let detach = Collection::detach_resources(
            collection_id,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            diesel::update(collection::table)
                .set(collection::dsl::description.eq("Updated desc")),
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(Option::<Uuid>::None)),
        )
        .detach_and_get_result_async(&conn)
        .await;

        // "detach_and_get_result_async" should return the updated collection.
        let returned_collection = detach.expect("Detach should have worked");
        // The returned value should be the latest value in the DB.
        assert_eq!(
            returned_collection,
            get_collection(collection_id, &conn).await
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_detach_once_synchronous() {
        let logctx = dev::test_setup_log("test_detach_once_synchronous");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = setup_db(pool).await;

        let collection_id = uuid::Uuid::new_v4();
        let resource_id = uuid::Uuid::new_v4();

        // Create the collection and resource.
        let _collection =
            insert_collection(collection_id, "collection", &conn).await;
        let _resource = insert_resource(resource_id, "resource", &conn).await;
        attach_resource(collection_id, resource_id, &conn).await;

        // Detach the resource from the collection.
        let detach_query = Collection::detach_resources(
            collection_id,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            diesel::update(collection::table)
                .set(collection::dsl::description.eq("Updated desc")),
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(Option::<Uuid>::None)),
        );

        // "detach_and_get_result" should return the "detached" resource.
        let returned_collection = detach_query
            .detach_and_get_result_async(&conn)
            .await
            .expect("Detach should have worked");

        // The returned values should be the latest value in the DB.
        assert_eq!(
            returned_collection,
            get_collection(collection_id, &conn).await
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_detach_while_already_detached() {
        let logctx = dev::test_setup_log("test_detach_while_already_detached");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = setup_db(pool).await;

        let collection_id = uuid::Uuid::new_v4();

        let _collection =
            insert_collection(collection_id, "collection", &conn).await;
        let resource_id = uuid::Uuid::new_v4();
        let _resource = insert_resource(resource_id, "resource", &conn).await;
        attach_resource(collection_id, resource_id, &conn).await;

        // Detach a resource from a collection, as usual.
        let detach = Collection::detach_resources(
            collection_id,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            diesel::update(collection::table)
                .set(collection::dsl::description.eq("Updated desc")),
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(Option::<Uuid>::None)),
        )
        .detach_and_get_result_async(&conn)
        .await;
        assert_eq!(
            detach.expect("Detach should have worked").description(),
            "Updated desc"
        );

        // Try detaching once more. This one won't detach anything, but
        // we still expect it to succeed.
        let detach = Collection::detach_resources(
            collection_id,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            diesel::update(collection::table)
                .set(collection::dsl::description.eq("... and again!")),
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(Option::<Uuid>::None)),
        )
        .detach_and_get_result_async(&conn)
        .await;
        assert_eq!(
            detach.expect("Detach should have worked").description(),
            "... and again!"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_detach_filter_collection() {
        let logctx = dev::test_setup_log("test_detach_filter_collection");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = setup_db(pool).await;

        let collection_id = uuid::Uuid::new_v4();

        let _collection =
            insert_collection(collection_id, "collection", &conn).await;
        let resource_id = uuid::Uuid::new_v4();
        let _resource = insert_resource(resource_id, "resource", &conn).await;
        attach_resource(collection_id, resource_id, &conn).await;

        // Detach a resource from a collection, but do so with a picky filter
        // on the collectipon.
        let detach = Collection::detach_resources(
            collection_id,
            collection::table
                .into_boxed()
                .filter(collection::dsl::name.eq("This name will not match")),
            resource::table.into_boxed(),
            diesel::update(collection::table)
                .set(collection::dsl::description.eq("Updated desc")),
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(Option::<Uuid>::None)),
        )
        .detach_and_get_result_async(&conn)
        .await;

        let err = detach.expect_err("Expected this detach to fail");

        // A caller should be able to inspect this result; the collection
        // exists but has a different name than requested.
        match err {
            DetachManyError::NoUpdate { collection } => {
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
    async fn test_detach_deleted_resource() {
        let logctx = dev::test_setup_log("test_detach_deleted_resource");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = setup_db(pool).await;

        let collection_id = uuid::Uuid::new_v4();
        let resource_id = uuid::Uuid::new_v4();

        // Create the collection and resource.
        let _collection =
            insert_collection(collection_id, "collection", &conn).await;
        let _resource = insert_resource(resource_id, "resource", &conn).await;
        attach_resource(collection_id, resource_id, &conn).await;

        // Immediately soft-delete the resource.
        diesel::update(
            resource::table.filter(resource::dsl::id.eq(resource_id)),
        )
        .set(resource::dsl::time_deleted.eq(Utc::now()))
        .execute_async(&*conn)
        .await
        .unwrap();

        // Detach the resource from the collection. Observe a failure which is
        // indistinguishable from the resource not existing.
        let detach = Collection::detach_resources(
            collection_id,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            diesel::update(collection::table)
                .set(collection::dsl::description.eq("Updated desc")),
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(collection_id)),
        )
        .detach_and_get_result_async(&conn)
        .await;

        assert_eq!(
            detach.expect("Detach should have worked").description(),
            "Updated desc"
        );
        assert_eq!(
            get_resource(resource_id, &conn)
                .await
                .collection_id
                .as_ref()
                .expect("Should be deleted, but still attached"),
            &collection_id,
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_detach_many() {
        let logctx = dev::test_setup_log("test_detach_many");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = setup_db(pool).await;

        // Create the collection and some resources.
        let collection_id1 = uuid::Uuid::new_v4();
        let _collection1 =
            insert_collection(collection_id1, "collection", &conn).await;
        let resource_id1 = uuid::Uuid::new_v4();
        let resource_id2 = uuid::Uuid::new_v4();
        let _resource1 =
            insert_resource(resource_id1, "resource1", &conn).await;
        attach_resource(collection_id1, resource_id1, &conn).await;
        let _resource2 =
            insert_resource(resource_id2, "resource2", &conn).await;
        attach_resource(collection_id1, resource_id2, &conn).await;

        // Create a separate collection with a resource.
        //
        // We will check that this resource is untouched after operating
        // on "collection_id1".
        let collection_id2 = uuid::Uuid::new_v4();
        let _collection2 =
            insert_collection(collection_id2, "collection2", &conn).await;
        let resource_id3 = uuid::Uuid::new_v4();
        let _resource3 =
            insert_resource(resource_id3, "resource3", &conn).await;
        attach_resource(collection_id2, resource_id3, &conn).await;

        // Detach the resource from the collection.
        let detach = Collection::detach_resources(
            collection_id1,
            collection::table.into_boxed(),
            resource::table.into_boxed(),
            diesel::update(collection::table)
                .set(collection::dsl::description.eq("Updated desc")),
            diesel::update(resource::table)
                .set(resource::dsl::collection_id.eq(Option::<Uuid>::None)),
        )
        .detach_and_get_result_async(&conn)
        .await;

        let returned_resource = detach.expect("Detach should have worked");
        assert_eq!(returned_resource.id(), collection_id1);
        assert_eq!(returned_resource.description(), "Updated desc");

        // Note that only "resource1" and "resource2" should be detached.
        assert!(get_resource(resource_id1, &conn)
            .await
            .collection_id
            .is_none());
        assert!(get_resource(resource_id2, &conn)
            .await
            .collection_id
            .is_none());

        // "resource3" should have been left alone.
        assert_eq!(
            get_resource(resource_id3, &conn)
                .await
                .collection_id
                .as_ref()
                .expect("Should still be attached"),
            &collection_id2
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
