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

/// A simple wrapper type for Diesel's [`InsertStatement`], which
/// allows referencing generics with names (and extending usage
/// without re-stating those generic parameters everywhere).
/// NOTE: This only waraps insert statements without a returning clause.
pub trait InsertStatementExt<ResType, C: DatastoreCollection<ResType>> {
    type Records;

    fn statement(
        self,
    ) -> InsertStatement<
        // Force the insert statement to have the appropriate target table
        ResourceTable<ResType, C>,
        Self::Records,
    >;
}

impl<ResType, C: DatastoreCollection<ResType>, U> InsertStatementExt<ResType, C>
    for InsertStatement<ResourceTable<ResType, C>, U>
{
    type Records = U;

    fn statement(self) -> InsertStatement<ResourceTable<ResType, C>, U> {
        self
    }
}

/// Trait to be implemented by any structs representing a collection.
/// For example, since Organizations have a one-to-many relationship with
/// Projects, the Organization datatype should implement this trait.
pub trait DatastoreCollection<ResourceType> {
    /// The Rust type of the collection id (typically Uuid for us)
    type CollectionId: Copy + Debug;

    /// The column in the CollectionTable that acts as a generation number.
    /// This is the "child-resource-generation-number" in RFD 192.
    type GenerationNumberColumn: Column;

    /// The column in the ResourceTable that acts as a foreign key into
    /// the CollectionTable
    type CollectionIdColumn: Column;

    /// The time deleted column in the CollectionTable
    // We enforce that this column comes from the same table as
    // GenerationNumberColumn when defining insert_resource() below.
    type CollectionTimeDeletedColumn: Column;

    /// The generation number column in the CollectionTable
    fn generation_number_column() -> Self::GenerationNumberColumn;

    /// The time deleted column in the CollectionTable
    fn collection_time_deleted_column() -> Self::CollectionTimeDeletedColumn;

    fn insert_resource<IS>(
        key: Self::CollectionId,
        insert: IS,
    ) -> InsertIntoCollectionStatement<ResourceType, IS, Self>
    where
        (
            <Self::GenerationNumberColumn as Column>::Table,
            <Self::CollectionTimeDeletedColumn as Column>::Table,
        ): TypesAreSame,
        Self: Sized,
        IS: InsertStatementExt<ResourceType, Self>,
        ResourceTable<ResourceType, Self>: Copy + Debug,
        ResourceType: Selectable<Pg>,
    {
        InsertIntoCollectionStatement {
            insert_statement: insert.statement(),
            key,
            query_type: PhantomData,
        }
    }
}

/// Utility type to make trait bounds below easier to read.
type CollectionId<ResType, C> =
    <C as DatastoreCollection<ResType>>::CollectionId;
type CollectionTable<ResType, C> =
    <<C as DatastoreCollection<ResType>>::GenerationNumberColumn as Column>::Table;
type ResourceTable<ResType, C> =
    <<C as DatastoreCollection<ResType>>::CollectionIdColumn as Column>::Table;
type CollectionTimeDeletedColumn<ResType, C> =
    <C as DatastoreCollection<ResType>>::CollectionTimeDeletedColumn;
type GenerationNumberColumn<ResType, C> =
    <C as DatastoreCollection<ResType>>::GenerationNumberColumn;

// Trick to check that columns come from the same table
pub trait TypesAreSame {}
impl<T> TypesAreSame for (T, T) {}

/// The CTE described in the module docs
#[derive(Debug, Clone, Copy)]
#[must_use = "Queries must be executed"]
pub struct InsertIntoCollectionStatement<ResType, IS, C>
where
    IS: InsertStatementExt<ResType, C>,
    C: DatastoreCollection<ResType>,
    ResourceTable<ResType, C>: Copy + Debug,
{
    insert_statement: InsertStatement<ResourceTable<ResType, C>, IS::Records>,
    key: CollectionId<ResType, C>,
    query_type: PhantomData<ResType>,
}

impl<ResType, IS, C> QueryId for InsertIntoCollectionStatement<ResType, IS, C>
where
    IS: InsertStatementExt<ResType, C>,
    C: DatastoreCollection<ResType>,
    ResourceTable<ResType, C>: Copy + Debug,
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

impl<ResType, IS, C> InsertIntoCollectionStatement<ResType, IS, C>
where
    ResType: 'static + Debug + Send,
    IS: 'static + InsertStatementExt<ResType, C> + Send,
    C: 'static + DatastoreCollection<ResType> + Send,
    CollectionId<ResType, C>: 'static + PartialEq + Send,
    ResourceTable<ResType, C>: 'static + Table + Send + Copy + Debug,
    IS::Records: 'static + Send,
    InsertIntoCollectionStatement<ResType, IS, C>: Send,
{
    /// Issues the CTE and parses the result.
    ///
    /// The three outcomes are:
    /// - Ok(Row was inserted)
    /// - Error(collection not found)
    /// - Error(other diesel error)
    // TODO: Remove this allow(dead_code) once we have a caller. Since it and
    // several types it uses are only used in tests right now, the compiler
    // produces a bunch of warnings here.
    #[allow(dead_code)]
    pub async fn insert_and_get_result_async(
        self,
        pool: &bb8::Pool<ConnectionManager<PgConnection>>,
    ) -> InsertIntoCollectionResult<ResType>
    where
        // We require this bound to ensure that "Self" is runnable as query.
        Self: query_methods::LoadQuery<PgConnection, ResType>,
    {
        match self.get_result_async::<ResType>(pool).await {
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

impl<ResType, IS, C> Query for InsertIntoCollectionStatement<ResType, IS, C>
where
    IS: InsertStatementExt<ResType, C>,
    ResType: Selectable<Pg>,
    C: DatastoreCollection<ResType>,
    ResourceTable<ResType, C>: Copy + Debug,
{
    type SqlType = SelectableSqlType<ResType>;
}

impl<ResType, IS, C> RunQueryDsl<PgConnection>
    for InsertIntoCollectionStatement<ResType, IS, C>
where
    IS: InsertStatementExt<ResType, C>,
    ResourceTable<ResType, C>: Table + Copy + Debug,
    C: DatastoreCollection<ResType>,
{
}

// Representation of Primary Key in Rust.
type CollectionPrimaryKey<ResType, C> =
    <CollectionTable<ResType, C> as Table>::PrimaryKey;
// Representation of Primary Key in SQL.
type SerializedCollectionPrimaryKey<ResType, C> =
    <CollectionPrimaryKey<ResType, C> as diesel::Expression>::SqlType;

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
impl<ResType, IS, C> QueryFragment<Pg>
    for InsertIntoCollectionStatement<ResType, IS, C>
where
    ResType: Selectable<Pg>,
    <ResType as Selectable<Pg>>::SelectExpression: QueryFragment<Pg>,
    IS: InsertStatementExt<ResType, C>,
    C: DatastoreCollection<ResType>,
    CollectionTable<ResType, C>: HasTable<Table = CollectionTable<ResType, C>>
        + Table
        + IntoUpdateTarget
        + query_methods::BoxedDsl<
            'static,
            Pg,
            Output = BoxedQuery<CollectionTable<ResType, C>>,
        >,
    ResourceTable<ResType, C>: Copy + Debug,
    <CollectionPrimaryKey<ResType, C> as Expression>::SqlType: SingleValue,
    CollectionPrimaryKey<ResType, C>: diesel::Column,
    CollectionTimeDeletedColumn<ResType, C>: ExpressionMethods,
    CollectionId<ResType, C>: diesel::expression::AsExpression<
            SerializedCollectionPrimaryKey<ResType, C>,
        > + diesel::serialize::ToSql<SerializedCollectionPrimaryKey<ResType, C>, Pg>,
    <CollectionTable<ResType, C> as diesel::QuerySource>::FromClause:
        QueryFragment<Pg>,
    InsertStatement<ResourceTable<ResType, C>, IS::Records>: QueryFragment<Pg>,
    BoxedQuery<CollectionTable<ResType, C>>: query_methods::FilterDsl<
        Eq<CollectionPrimaryKey<ResType, C>, CollectionId<ResType, C>>,
        Output = BoxedQuery<CollectionTable<ResType, C>>,
    >,
    BoxedQuery<CollectionTable<ResType, C>>: query_methods::FilterDsl<
        IsNull<CollectionTimeDeletedColumn<ResType, C>>,
        Output = BoxedQuery<CollectionTable<ResType, C>>,
    >,
    BoxedQuery<CollectionTable<ResType, C>>: QueryFragment<Pg>,
    Pg: diesel::sql_types::HasSqlType<
        SerializedCollectionPrimaryKey<ResType, C>,
    >,
    <ResourceTable<ResType, C> as Table>::AllColumns: QueryFragment<Pg>,
{
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.push_sql("WITH dummy AS (SELECT IF(EXISTS(");
        let subquery = CollectionTable::<ResType, C>::table()
            .into_boxed()
            .filter(
                CollectionTable::<ResType, C>::table()
                    .primary_key()
                    .eq(self.key),
            )
            .filter(C::collection_time_deleted_column().is_null());
        subquery.walk_ast(out.reborrow())?;
        // Manually add the FOR_UPDATE, since .for_update() is incompatible with
        // BoxedQuery
        out.push_sql(" FOR UPDATE), TRUE, CAST(1/0 AS BOOL))),");

        // Write the update manually instead of with the dsl, to avoid the
        // explosion in complexity of type traits
        out.push_sql("updated_row AS (UPDATE ");
        CollectionTable::<ResType, C>::table()
            .from_clause()
            .walk_ast(out.reborrow())?;
        out.push_sql(" SET ");
        out.push_identifier(GenerationNumberColumn::<ResType, C>::NAME)?;
        out.push_sql(" = ");
        out.push_identifier(GenerationNumberColumn::<ResType, C>::NAME)?;
        out.push_sql(" + 1 WHERE ");
        out.push_identifier(CollectionPrimaryKey::<ResType, C>::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<SerializedCollectionPrimaryKey<ResType, C>, _>(
            &self.key,
        )?;
        out.push_sql(" AND ");
        out.push_identifier(CollectionTimeDeletedColumn::<ResType, C>::NAME)?;
        out.push_sql(" IS NULL RETURNING 1)");

        // TODO: Check or force the insert_statement to have
        //       C::CollectionIdColumn set
        self.insert_statement.walk_ast(out.reborrow())?;
        out.push_sql(" RETURNING ");
        // We manually write the RETURNING clause here because the wrapper type
        // used for InsertStatement's Ret generic is private to diesel and so we
        // cannot express it.
        ResType::as_returning().walk_ast(out.reborrow())?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{DatastoreCollection, InsertError};
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
    impl DatastoreCollection<Resource> for Collection {
        type CollectionId = uuid::Uuid;
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

        Collection::insert_resource(
            collection_id,
            diesel::insert_into(resource::table).values(vec![(
                resource::dsl::id.eq(resource_id),
                resource::dsl::name.eq("test"),
                resource::dsl::description.eq("desc"),
                resource::dsl::time_created.eq(Utc::now()),
                resource::dsl::time_modified.eq(Utc::now()),
                resource::dsl::collection_id.eq(collection_id),
            )]),
        )
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
