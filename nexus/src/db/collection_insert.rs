//! CTE implementation for inserting a row representing a child resource of a
//! collection. This atomically
//! 1) checks if the collection exists, and fails otherwise
//! 2) updates the collection's child resource generation number
//! 3) inserts the child resource row

use async_bb8_diesel::{
    AsyncRunQueryDsl, ConnectionError, ConnectionManager, PoolError,
};
use diesel::associations::HasTable;
use diesel::deserialize::FromSqlRow;
use diesel::helper_types::*;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::query_dsl::methods as query_methods;
use diesel::query_source::Table;
use diesel::sql_types::SingleValue;
use std::fmt::Debug;
use std::marker::PhantomData;

/// A simple wrapper type for Diesel's [`InsertStatement`], which
/// allows referencing generics with names (and extending usage
/// without re-stating those generic parameters everywhere).
pub trait InsertStatementExt<C: DatastoreCollection> {
    type Records;
    type Operator;
    type Returning;

    fn statement(
        self,
    ) -> InsertStatement<
        // Force the insert statement to have the appropriate target table
        C::ResourceTable,
        Self::Records,
        Self::Operator,
        Self::Returning,
    >;
}

impl<C, T, U, Op, Ret> InsertStatementExt<C> for InsertStatement<T, U, Op, Ret>
where
    C: DatastoreCollection<ResourceTable = T>,
{
    type Records = U;
    type Operator = Op;
    type Returning = Ret;

    fn statement(self) -> InsertStatement<C::ResourceTable, U, Op, Ret> {
        self
    }
}

/// Trait to be implemented by any structs representing a collection.
/// For example, since Organizations have a one-to-many relationship with
/// Projects, the Organization datatype should implement this trait.
pub trait DatastoreCollection {
    /// The Rust type of the collection id (typically Uuid for us)
    type CollectionId: Copy + Debug;

    /// The diesel SQL table of the collection
    type CollectionTable: Table;
    /// The diesel SQL table of the resources
    type ResourceTable: Table;

    /// The column in the CollectionTable that acts as a generation number.
    /// This is the "child-resource-generation-number" in RFD 192.
    type GenerationNumberColumn: Column;

    /// The column in the ResourceTable that acts as a foreign key into
    /// the CollectionTable
    type CollectionIdColumn: Column;

    /// The time deleted column in the CollectionTable
    type CollectionTimeDeletedColumn: Column;

    /// The time deleted column in the CollectionTable
    fn generation_number_column() -> Self::GenerationNumberColumn;

    /// The time deleted column in the CollectionTable
    fn collection_time_deleted_column() -> Self::CollectionTimeDeletedColumn;
}

/// Utility type to make trait bounds below easier to read.
type CollectionId<C> = <C as DatastoreCollection>::CollectionId;

/// Wrapper around [`diesel::insert`] for a Table, which creates the CTE
/// described in the module docs.
///
/// IS: [`InsertStatement`] which we are extending.
/// C: Collection type.
pub trait InsertIntoCollection<IS> {
    /// Nests the existing insert statement in a CTE which
    /// checks if the containing collection exists (by ID),
    fn insert_into_collection<C: DatastoreCollection, Q>(
        self,
        key: CollectionId<C>,
    ) -> InsertIntoCollectionStatement<IS, C, Q>
    where
        IS: InsertStatementExt<C>,
        Q: FromSqlRow<
                <InsertIntoCollectionStatement<IS, C, Q> as AsQuery>::SqlType,
                Pg,
            > + Selectable<Pg>;
}

impl<IS> InsertIntoCollection<IS> for IS {
    fn insert_into_collection<C: DatastoreCollection, Q>(
        self,
        key: CollectionId<C>,
    ) -> InsertIntoCollectionStatement<IS, C, Q>
    where
        IS: InsertStatementExt<C>,
        Q: FromSqlRow<
                <InsertIntoCollectionStatement<IS, C, Q> as AsQuery>::SqlType,
                Pg,
            > + Selectable<Pg>,
    {
        InsertIntoCollectionStatement {
            insert_statement: self.statement(),
            key,
            query_type: PhantomData,
        }
    }
}

/// The CTE described in the module docs
#[derive(Debug, Clone, Copy)]
#[must_use = "Queries must be executed"]
pub struct InsertIntoCollectionStatement<IS, C, Q>
where
    IS: InsertStatementExt<C>,
    C: DatastoreCollection,
{
    insert_statement: InsertStatement<
        C::ResourceTable,
        IS::Records,
        IS::Operator,
        IS::Returning,
    >,
    key: CollectionId<C>,
    query_type: PhantomData<Q>,
}

impl<IS, C, Q> QueryId for InsertIntoCollectionStatement<IS, C, Q>
where
    IS: InsertStatementExt<C>,
    C: DatastoreCollection,
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

impl<IS, C, Q> InsertIntoCollectionStatement<IS, C, Q>
where
    IS: 'static + InsertStatementExt<C> + Send,
    C: 'static + DatastoreCollection + Send,
    CollectionId<C>: 'static + PartialEq + Send,
    ResourceTable<C>: 'static + Table + Send,
    IS::Records: 'static + Send,
    Q: 'static + Debug + Send,
    InsertIntoCollectionStatement<IS, C, Q>: Send,
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
    ) -> InsertIntoCollectionResult<Q>
    where
        // We require this bound to ensure that "Self" is runnable as query.
        Self: query_methods::LoadQuery<PgConnection, Q>,
    {
        match self.get_result_async::<Q>(pool).await {
            Ok(row) => Ok(row),
            Err(PoolError::Connection(ConnectionError::Query(
                diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::Unknown,
                    info,
                ),
            ))) if info.message() == "division by zero" => {
                Err(InsertError::CollectionNotFound)
            }
            Err(other) => Err(InsertError::DatabaseError(other)),
        }
    }
}

type SelectableSqlType<Q> =
    <<Q as diesel::Selectable<Pg>>::SelectExpression as Expression>::SqlType;

impl<IS, C, Q> Query for InsertIntoCollectionStatement<IS, C, Q>
where
    IS: InsertStatementExt<C>,
    Q: Selectable<Pg>,
    C: DatastoreCollection,
{
    type SqlType = SelectableSqlType<Q>;
}

impl<IS, C, Q> RunQueryDsl<PgConnection>
    for InsertIntoCollectionStatement<IS, C, Q>
where
    IS: InsertStatementExt<C>,
    ResourceTable<C>: Table,
    C: DatastoreCollection,
{
}

type CollectionTable<C> = <C as DatastoreCollection>::CollectionTable;
type ResourceTable<C> = <C as DatastoreCollection>::ResourceTable;
// Representation of Primary Key in Rust.
type CollectionPrimaryKey<C> = <CollectionTable<C> as Table>::PrimaryKey;
type ResourcePrimaryKey<C> = <ResourceTable<C> as Table>::PrimaryKey;
// Representation of Primary Key in SQL.
type SerializedCollectionPrimaryKey<C> =
    <CollectionPrimaryKey<C> as diesel::Expression>::SqlType;
type SerializedResourcePrimaryKey<C> =
    <ResourcePrimaryKey<C> as diesel::Expression>::SqlType;

type CollectionTimeDeletedColumn<C> =
    <C as DatastoreCollection>::CollectionTimeDeletedColumn;
type GenerationNumberColumn<C> =
    <C as DatastoreCollection>::GenerationNumberColumn;
type SerializedGenerationNumberColumn<C> =
    <GenerationNumberColumn<C> as diesel::Expression>::SqlType;

type TableSqlType<T> = <T as AsQuery>::SqlType;
type BoxedQuery<T> = BoxedSelectStatement<'static, TableSqlType<T>, T, Pg>;

/// This implementation uses the following CTE:
///
/// ```text
/// // WITH dummy AS (SELECT IF(EXISTS(SELECT <PK> FROM C WHERE <PK> = <value>
/// //                                 AND time_deleted IS NULL FOR UPDATE),
///                             TRUE, CAST(1/0 AS BOOL))),
/// //      updated_row AS (UPDATE C SET <generation number> =
/// //                      <generation_number> + 1 WHERE <PK> = <value> AND
/// //                      time_deleted IS NULL RETURNING 1),
/// // <user provided insert statement>
/// ```
impl<IS, C, Q> QueryFragment<Pg> for InsertIntoCollectionStatement<IS, C, Q>
where
    IS: InsertStatementExt<C>,
    C: DatastoreCollection,
    CollectionTable<C>: HasTable<Table = CollectionTable<C>>
        + Table
        + IntoUpdateTarget
        + query_methods::BoxedDsl<
            'static,
            Pg,
            Output = BoxedQuery<CollectionTable<C>>,
        >,
    InsertStatement<ResourceTable<C>, IS::Records, IS::Operator, IS::Returning>:
        QueryFragment<Pg>,
    <CollectionPrimaryKey<C> as Expression>::SqlType: SingleValue,
    CollectionPrimaryKey<C>: diesel::Column,
    CollectionTimeDeletedColumn<C>: ExpressionMethods,
    CollectionId<C>:
        diesel::expression::AsExpression<SerializedCollectionPrimaryKey<C>>,
    <CollectionTable<C> as diesel::QuerySource>::FromClause: QueryFragment<Pg>,
    BoxedQuery<CollectionTable<C>>: query_methods::FilterDsl<
        Eq<CollectionPrimaryKey<C>, CollectionId<C>>,
        Output = BoxedQuery<CollectionTable<C>>,
    >,
    BoxedQuery<CollectionTable<C>>: query_methods::FilterDsl<
        IsNull<CollectionTimeDeletedColumn<C>>,
        Output = BoxedQuery<CollectionTable<C>>,
    >,
    BoxedQuery<CollectionTable<C>>: QueryFragment<Pg>,
    Pg: diesel::sql_types::HasSqlType<SerializedCollectionPrimaryKey<C>>,
    CollectionId<C>:
        diesel::serialize::ToSql<SerializedCollectionPrimaryKey<C>, Pg>,
    <ResourceTable<C> as Table>::AllColumns: QueryFragment<Pg>,
{
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.push_sql("WITH dummy AS (SELECT IF(EXISTS(");
        let subquery = C::CollectionTable::table()
            .into_boxed()
            .filter(C::CollectionTable::table().primary_key().eq(self.key))
            .filter(C::collection_time_deleted_column().is_null());
        subquery.walk_ast(out.reborrow())?;
        // Manually add the FOR_UPDATE, since .for_update() is incompatible with
        // BoxedQuery
        out.push_sql(" FOR UPDATE), TRUE, CAST(1/0 AS BOOL))),");

        // Write the update manually instead of with the dsl, to avoid the
        // explosion in complexity of type traits
        out.push_sql("updated_row AS (UPDATE ");
        C::CollectionTable::table().from_clause().walk_ast(out.reborrow())?;
        out.push_sql(" SET ");
        out.push_identifier(GenerationNumberColumn::<C>::NAME)?;
        out.push_sql(" = ");
        out.push_identifier(GenerationNumberColumn::<C>::NAME)?;
        out.push_sql(" + 1 WHERE ");
        out.push_identifier(CollectionPrimaryKey::<C>::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<SerializedCollectionPrimaryKey<C>, _>(&self.key)?;
        out.push_sql(" AND ");
        out.push_identifier(CollectionTimeDeletedColumn::<C>::NAME)?;
        out.push_sql(" IS NULL RETURNING 1)");

        // TODO: Check or force the insert_statement to have
        //       C::CollectionIdColumn set
        self.insert_statement.walk_ast(out.reborrow())?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{DatastoreCollection, InsertError, InsertIntoCollection};
    use crate::db;
    use async_bb8_diesel::AsyncRunQueryDsl;
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
        diesel::sql_query("CREATE SCHEMA IF NOT EXISTS test_schema")
            .execute_async(pool.pool())
            .await
            .unwrap();

        diesel::sql_query(
            "CREATE TABLE IF NOT EXISTS test_schema.collection (
                id UUID PRIMARY KEY,
                name STRING(63) NOT NULL,
                description STRING(512) NOT NULL,
                time_created TIMESTAMPTZ NOT NULL,
                time_modified TIMESTAMPTZ NOT NULL,
                time_deleted TIMESTAMPTZ,
                rcgen INT NOT NULL)",
        )
        .execute_async(pool.pool())
        .await
        .unwrap();

        diesel::sql_query(
            "CREATE TABLE IF NOT EXISTS test_schema.resource(
                id UUID PRIMARY KEY,
                name STRING(63) NOT NULL,
                description STRING(512) NOT NULL,
                time_created TIMESTAMPTZ NOT NULL,
                time_modified TIMESTAMPTZ NOT NULL,
                time_deleted TIMESTAMPTZ,
                collection_id UUID NOT NULL)",
        )
        .execute_async(pool.pool())
        .await
        .unwrap();

        diesel::sql_query("TRUNCATE test_schema.collection")
            .execute_async(pool.pool())
            .await
            .unwrap();

        diesel::sql_query("TRUNCATE test_schema.resource")
            .execute_async(pool.pool())
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
    impl DatastoreCollection for Collection {
        type CollectionId = uuid::Uuid;
        type CollectionTable = collection::table;
        type ResourceTable = resource::table;
        type GenerationNumberColumn = collection::dsl::rcgen;
        type CollectionIdColumn = resource::dsl::collection_id;
        type CollectionTimeDeletedColumn = collection::dsl::time_deleted;
        fn generation_number_column() -> Self::GenerationNumberColumn {
            collection::dsl::rcgen
        }
        fn collection_time_deleted_column() -> Self::CollectionTimeDeletedColumn
        {
            collection::dsl::time_deleted
        }
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
        let insert = diesel::insert_into(resource::table)
            .values(vec![(
                resource::dsl::id.eq(resource_id),
                resource::dsl::name.eq("test"),
                resource::dsl::description.eq("desc"),
                resource::dsl::time_created.eq(create_time),
                resource::dsl::time_modified.eq(modify_time),
                resource::dsl::collection_id.eq(collection_id),
            )])
            .returning(resource::all_columns)
            .insert_into_collection::<Collection, Resource>(collection_id);
        let query = diesel::debug_query::<Pg, _>(&insert).to_string();

        let expected_query = "WITH dummy AS (SELECT IF(EXISTS(SELECT \"test_schema\".\"collection\".\"id\", \"test_schema\".\"collection\".\"name\", \"test_schema\".\"collection\".\"description\", \"test_schema\".\"collection\".\"time_created\", \"test_schema\".\"collection\".\"time_modified\", \"test_schema\".\"collection\".\"time_deleted\", \"test_schema\".\"collection\".\"rcgen\" FROM \"test_schema\".\"collection\" WHERE ((\"test_schema\".\"collection\".\"id\" = $1) AND (\"test_schema\".\"collection\".\"time_deleted\" IS NULL)) FOR UPDATE), TRUE, CAST(1/0 AS BOOL))),updated_row AS (UPDATE \"test_schema\".\"collection\" SET \"rcgen\" = \"rcgen\" + 1 WHERE \"id\" = $2 AND \"time_deleted\" IS NULL RETURNING 1)INSERT INTO \"test_schema\".\"resource\" (\"id\", \"name\", \"description\", \"time_created\", \"time_modified\", \"collection_id\") VALUES ($3, $4, $5, $6, $7, $8) RETURNING \"test_schema\".\"resource\".\"id\", \"test_schema\".\"resource\".\"name\", \"test_schema\".\"resource\".\"description\", \"test_schema\".\"resource\".\"time_created\", \"test_schema\".\"resource\".\"time_modified\", \"test_schema\".\"resource\".\"time_deleted\", \"test_schema\".\"resource\".\"collection_id\" -- binds: [223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d0, 223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d0, 223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d8, \"test\", \"desc\", 1970-01-01T00:00:00Z, 1970-01-01T00:00:01Z, 223cb7f7-0d3a-4a4e-a5e1-ad38ecb785d0]";

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
        let insert = diesel::insert_into(resource::table)
            .values(vec![(
                resource::dsl::id.eq(resource_id),
                resource::dsl::name.eq("test"),
                resource::dsl::description.eq("desc"),
                resource::dsl::time_created.eq(Utc::now()),
                resource::dsl::time_modified.eq(Utc::now()),
                resource::dsl::collection_id.eq(collection_id),
            )])
            .returning(resource::all_columns)
            .insert_into_collection::<Collection, Resource>(collection_id)
            .insert_and_get_result_async(pool.pool())
            .await;
        match insert.unwrap_err() {
            InsertError::CollectionNotFound => (),
            err => panic!("Unexpected error: {:?}", err),
        }

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

        diesel::insert_into(resource::table)
            .values(vec![(
                resource::dsl::id.eq(resource_id),
                resource::dsl::name.eq("test"),
                resource::dsl::description.eq("desc"),
                resource::dsl::time_created.eq(Utc::now()),
                resource::dsl::time_modified.eq(Utc::now()),
                resource::dsl::collection_id.eq(collection_id),
            )])
            .returning(resource::all_columns)
            .insert_into_collection::<Collection, Resource>(collection_id)
            .insert_and_get_result_async(pool.pool())
            .await
            .unwrap();

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
