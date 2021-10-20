//! CTE implementation for inserting a row representing a child resource of a
//! collection. This atomically
//! 1) checks if the collection exists and is not soft deleted, and fails
//!    otherwise
//! 2) updates the collection's child resource generation number
//! 3) inserts the child resource row

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

/// Trait to be implemented by any structs representing a collection.
/// For example, since Organizations have a one-to-many relationship with
/// Projects, the Organization datatype should implement this trait.
/// ```
/// # use diesel::prelude::*;
/// # use omicron_nexus::db::collection_insert::DatastoreCollection;
/// # use omicron_nexus::db::model::Generation;
/// #
/// # table! {
/// #     test_schema.organization (id) {
/// #         id -> Uuid,
/// #         time_deleted -> Nullable<Timestamptz>,
/// #         rcgen -> Int8,
/// #     }
/// # }
/// #
/// # table! {
/// #     test_schema.project (id) {
/// #         id -> Uuid,
/// #         time_deleted -> Nullable<Timestamptz>,
/// #         organization_id -> Uuid,
/// #     }
/// # }
///
/// #[derive(Queryable, Insertable, Debug, Selectable)]
/// #[table_name = "project"]
/// struct Project {
///     pub id: uuid::Uuid,
///     pub time_deleted: Option<chrono::DateTime<chrono::Utc>>,
///     pub organization_id: uuid::Uuid,
/// }
///
/// #[derive(Queryable, Insertable, Debug, Selectable)]
/// #[table_name = "organization"]
/// struct Organization {
///     pub id: uuid::Uuid,
///     pub time_deleted: Option<chrono::DateTime<chrono::Utc>>,
///     pub rcgen: Generation,
/// }
///
/// impl DatastoreCollection<Project> for Organization {
///     // Type of Organization::identity::id and Project::organization_id
///     type CollectionId = uuid::Uuid;
///
///     type GenerationNumberColumn = organization::dsl::rcgen;
///     type CollectionTimeDeletedColumn = organization::dsl::time_deleted;
///
///     type CollectionIdColumn = project::dsl::organization_id;
/// }
/// ```
pub trait DatastoreCollection<ResourceType> {
    /// The Rust type of the collection id (typically Uuid for us)
    type CollectionId: Copy + Debug;

    /// The column in the CollectionTable that acts as a generation number.
    /// This is the "child-resource-generation-number" in RFD 192.
    type GenerationNumberColumn: Column + Default;

    /// The time deleted column in the CollectionTable
    // We enforce that this column comes from the same table as
    // GenerationNumberColumn when defining insert_resource() below.
    type CollectionTimeDeletedColumn: Column + Default;

    /// The column in the ResourceTable that acts as a foreign key into
    /// the CollectionTable
    type CollectionIdColumn: Column;

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
        ResourceTable<ResourceType, Self>: Copy + Debug,
        ResourceType: Selectable<Pg>,
    {
        InsertIntoCollectionStatement {
            insert_statement: insert,
            key,
            query_type: PhantomData,
        }
    }
}

/// Utility type to make trait bounds below easier to read.
type CollectionId<ResourceType, C> =
    <C as DatastoreCollection<ResourceType>>::CollectionId;
type CollectionTable<ResourceType, C> = <<C as DatastoreCollection<
    ResourceType,
>>::GenerationNumberColumn as Column>::Table;
type ResourceTable<ResourceType, C> = <<C as DatastoreCollection<
    ResourceType,
>>::CollectionIdColumn as Column>::Table;
type CollectionTimeDeletedColumn<ResourceType, C> =
    <C as DatastoreCollection<ResourceType>>::CollectionTimeDeletedColumn;
type GenerationNumberColumn<ResourceType, C> =
    <C as DatastoreCollection<ResourceType>>::GenerationNumberColumn;

// Trick to check that columns come from the same table
pub trait TypesAreSame {}
impl<T> TypesAreSame for (T, T) {}

/// The CTE described in the module docs
#[derive(Debug, Clone, Copy)]
#[must_use = "Queries must be executed"]
pub struct InsertIntoCollectionStatement<ResourceType, ISR, C>
where
    C: DatastoreCollection<ResourceType>,
    ResourceTable<ResourceType, C>: Copy + Debug,
{
    insert_statement: InsertStatement<ResourceTable<ResourceType, C>, ISR>,
    key: CollectionId<ResourceType, C>,
    query_type: PhantomData<ResourceType>,
}

impl<ResourceType, ISR, C> QueryId
    for InsertIntoCollectionStatement<ResourceType, ISR, C>
where
    C: DatastoreCollection<ResourceType>,
    ResourceTable<ResourceType, C>: Copy + Debug,
{
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

/// Result of [`InsertIntoCollectionStatement`].
pub type InsertIntoCollectionResult<Q> = Result<Q, InsertError>;

/// Errors returned by [`InsertIntoCollectionStatement`].
#[derive(Debug)]
pub enum InsertError {
    /// The collection that the query was inserting into does not exist
    CollectionNotFound,
    /// Other database error
    DatabaseError(PoolError),
}

impl<ResourceType, ISR, C> InsertIntoCollectionStatement<ResourceType, ISR, C>
where
    ResourceType: 'static + Debug + Send,
    C: 'static + DatastoreCollection<ResourceType> + Send,
    CollectionId<ResourceType, C>: 'static + PartialEq + Send,
    ResourceTable<ResourceType, C>: 'static + Table + Send + Copy + Debug,
    ISR: 'static + Send,
    InsertIntoCollectionStatement<ResourceType, ISR, C>: Send,
{
    /// Issues the CTE and parses the result.
    ///
    /// The three outcomes are:
    /// - Ok(Row was inserted)
    /// - Error(collection not found)
    /// - Error(other diesel error)
    pub async fn insert_and_get_result_async(
        self,
        pool: &bb8::Pool<ConnectionManager<PgConnection>>,
    ) -> InsertIntoCollectionResult<ResourceType>
    where
        // We require this bound to ensure that "Self" is runnable as query.
        Self: query_methods::LoadQuery<PgConnection, ResourceType>,
    {
        match self.get_result_async::<ResourceType>(pool).await {
            Ok(row) => Ok(row),
            Err(PoolError::Connection(ConnectionError::Query(
                diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::Unknown,
                    info,
                ),
            ))) if info.message() == "division by zero" => {
                // See
                // https://rfd.shared.oxide.computer/rfd/0192#_dueling_administrators
                // for a full explanation of why we're checking for this. In
                // summary, the CTE generates a division by zero intentionally
                // if the collection doesn't exist in the database.
                Err(InsertError::CollectionNotFound)
            }
            Err(other) => Err(InsertError::DatabaseError(other)),
        }
    }
}

type SelectableSqlType<Q> =
    <<Q as diesel::Selectable<Pg>>::SelectExpression as Expression>::SqlType;

impl<ResourceType, ISR, C> Query
    for InsertIntoCollectionStatement<ResourceType, ISR, C>
where
    ResourceType: Selectable<Pg>,
    C: DatastoreCollection<ResourceType>,
    ResourceTable<ResourceType, C>: Copy + Debug,
{
    type SqlType = SelectableSqlType<ResourceType>;
}

impl<ResourceType, ISR, C> RunQueryDsl<PgConnection>
    for InsertIntoCollectionStatement<ResourceType, ISR, C>
where
    ResourceTable<ResourceType, C>: Table + Copy + Debug,
    C: DatastoreCollection<ResourceType>,
{
}

// Representation of Primary Key in Rust.
type CollectionPrimaryKey<ResourceType, C> =
    <CollectionTable<ResourceType, C> as Table>::PrimaryKey;
// Representation of Primary Key in SQL.
type SerializedCollectionPrimaryKey<ResourceType, C> =
    <CollectionPrimaryKey<ResourceType, C> as diesel::Expression>::SqlType;

type TableSqlType<T> = <T as AsQuery>::SqlType;
type BoxedQuery<T> = BoxedSelectStatement<'static, TableSqlType<T>, T, Pg>;

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
/// //              <PK> = <value> AND <time_deleted> IS NULL RETURNING 1),
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
    <ResourceType as Selectable<Pg>>::SelectExpression: QueryFragment<Pg>,
    C: DatastoreCollection<ResourceType>,
    CollectionTable<ResourceType, C>: HasTable<Table = CollectionTable<ResourceType, C>>
        + Table
        + IntoUpdateTarget
        + query_methods::BoxedDsl<
            'static,
            Pg,
            Output = BoxedQuery<CollectionTable<ResourceType, C>>,
        >,
    ResourceTable<ResourceType, C>: Copy + Debug,
    <CollectionPrimaryKey<ResourceType, C> as Expression>::SqlType: SingleValue,
    CollectionPrimaryKey<ResourceType, C>: diesel::Column,
    CollectionTimeDeletedColumn<ResourceType, C>: ExpressionMethods,
    CollectionId<ResourceType, C>: diesel::expression::AsExpression<
            SerializedCollectionPrimaryKey<ResourceType, C>,
        > + diesel::serialize::ToSql<
            SerializedCollectionPrimaryKey<ResourceType, C>,
            Pg,
        >,
    <CollectionTable<ResourceType, C> as diesel::QuerySource>::FromClause:
        QueryFragment<Pg>,
    InsertStatement<ResourceTable<ResourceType, C>, ISR>: QueryFragment<Pg>,
    BoxedQuery<CollectionTable<ResourceType, C>>: query_methods::FilterDsl<
        Eq<
            CollectionPrimaryKey<ResourceType, C>,
            CollectionId<ResourceType, C>,
        >,
        Output = BoxedQuery<CollectionTable<ResourceType, C>>,
    >,
    BoxedQuery<CollectionTable<ResourceType, C>>: query_methods::FilterDsl<
        IsNull<CollectionTimeDeletedColumn<ResourceType, C>>,
        Output = BoxedQuery<CollectionTable<ResourceType, C>>,
    >,
    BoxedQuery<CollectionTable<ResourceType, C>>: QueryFragment<Pg>,
    Pg: diesel::sql_types::HasSqlType<
        SerializedCollectionPrimaryKey<ResourceType, C>,
    >,
    <ResourceTable<ResourceType, C> as Table>::AllColumns: QueryFragment<Pg>,
{
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        let subquery = CollectionTable::<ResourceType, C>::table()
            .into_boxed()
            .filter(
                CollectionTable::<ResourceType, C>::table()
                    .primary_key()
                    .eq(self.key),
            )
            .filter(C::CollectionTimeDeletedColumn::default().is_null());
        out.push_sql("WITH found_row AS MATERIALIZED (");
        subquery.walk_ast(out.reborrow())?;
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
        CollectionTable::<ResourceType, C>::table()
            .from_clause()
            .walk_ast(out.reborrow())?;
        out.push_sql(" SET ");
        out.push_identifier(GenerationNumberColumn::<ResourceType, C>::NAME)?;
        out.push_sql(" = ");
        out.push_identifier(GenerationNumberColumn::<ResourceType, C>::NAME)?;
        out.push_sql(" + 1 WHERE ");
        out.push_identifier(CollectionPrimaryKey::<ResourceType, C>::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<SerializedCollectionPrimaryKey<ResourceType, C>, _>(
            &self.key,
        )?;
        out.push_sql(" AND ");
        out.push_identifier(
            CollectionTimeDeletedColumn::<ResourceType, C>::NAME,
        )?;
        // We must include "RETURNING 1" since all CTE clauses must return
        // something
        out.push_sql(" IS NULL RETURNING 1), ");

        out.push_sql("inserted_row AS (");
        // TODO: Check or force the insert_statement to have
        //       C::CollectionIdColumn set
        self.insert_statement.walk_ast(out.reborrow())?;
        out.push_sql(" RETURNING ");
        // We manually write the RETURNING clause here because the wrapper type
        // used for InsertStatement's Ret generic is private to diesel and so we
        // cannot express it.
        ResourceType::as_returning().walk_ast(out.reborrow())?;

        out.push_sql(") SELECT * FROM inserted_row");
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{DatastoreCollection, InsertError};
    use crate::db;
    use crate::db::identity::Resource as IdentityResource;
    use async_bb8_diesel::{AsyncRunQueryDsl, AsyncSimpleConnection};
    use chrono::{DateTime, NaiveDateTime, Utc};
    use db_macros::Resource;
    use diesel::expression_methods::ExpressionMethods;
    use diesel::pg::Pg;
    use diesel::QueryDsl;
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
                     collection_id UUID NOT NULL); \
                 TRUNCATE test_schema.collection; \
                 TRUNCATE test_schema.resource",
            )
            .await
            .unwrap();
    }

    /// Describes an organization within the database.
    #[derive(Queryable, Insertable, Debug, Resource, Selectable)]
    #[table_name = "resource"]
    struct Resource {
        #[diesel(embed)]
        pub identity: ResourceIdentity,

        pub collection_id: uuid::Uuid,
    }

    struct Collection;
    impl DatastoreCollection<Resource> for Collection {
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
        let create_time =
            DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc);
        let modify_time =
            DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(1, 0), Utc);
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
                 WHERE \"id\" = $2 AND \"time_deleted\" IS NULL RETURNING 1), \
             inserted_row AS (INSERT INTO \"test_schema\".\"resource\" \
                 (\"id\", \"name\", \"description\", \"time_created\", \
                  \"time_modified\", \"collection_id\") \
                 VALUES ($3, $4, $5, $6, $7, $8) \
                 RETURNING \"test_schema\".\"resource\".\"id\", \
                           \"test_schema\".\"resource\".\"name\", \
                           \"test_schema\".\"resource\".\"description\", \
                           \"test_schema\".\"resource\".\"time_created\", \
                           \"test_schema\".\"resource\".\"time_modified\", \
                           \"test_schema\".\"resource\".\"time_deleted\", \
                           \"test_schema\".\"resource\".\"collection_id\") \
            SELECT * FROM inserted_row \
        -- binds: [223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d0, \
                   223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d0, \
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
        let mut db = dev::test_setup_database(&logctx.log).await;
        let cfg = db::Config { url: db.pg_config().clone() };
        let pool = db::Pool::new(&cfg);

        setup_db(&pool).await;

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
        .insert_and_get_result_async(pool.pool())
        .await;
        assert!(matches!(insert, Err(InsertError::CollectionNotFound)));

        db.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_collection_present() {
        let logctx = dev::test_setup_log("test_collection_present");
        let mut db = dev::test_setup_database(&logctx.log).await;
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

        let create_time =
            DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc);
        let modify_time =
            DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(1, 0), Utc);
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
        .insert_and_get_result_async(pool.pool())
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
            .first_async::<i64>(pool.pool())
            .await
            .unwrap();

        // Make sure rcgen got incremented
        assert_eq!(collection_rcgen, 2);

        db.cleanup().await.unwrap();
    }
}
