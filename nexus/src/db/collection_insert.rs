//! CTE implementation for inserting a row representing a child resource of a
//! collection. This atomically
//! 1) checks if the collection exists, and fails otherwise
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
use diesel::query_dsl::methods::LoadQuery;
use diesel::query_source::Table;
use std::fmt::Debug;
use std::marker::PhantomData;

/// A simple wrapper type for Diesel's [`InsertStatement`], which
/// allows referencing generics with names (and extending usage
/// without re-stating those generic parameters everywhere).
pub trait InsertStatementExt {
    type Table;
    type Records;
    type Operator;
    type Returning;

    fn statement(
        self,
    ) -> InsertStatement<
        Self::Table,
        Self::Records,
        Self::Operator,
        Self::Returning,
    >;
}

impl<T, U, Op, Ret> InsertStatementExt for InsertStatement<T, U, Op, Ret> {
    type Table = T;
    type Records = U;
    type Operator = Op;
    type Returning = Ret;

    fn statement(self) -> InsertStatement<T, U, Op, Ret> {
        self
    }
}

/// Trait to be implemented by any structs representing a collection.
/// For example, since Organizations have a one-to-many relationship with
/// Projects, the Organization datatype should implement this trait.
pub trait DatastoreCollection {
    /// The type of the collection
    type CollectionTable: Table;
    /// The type of the collection
    type ResourceTable: Table;

    /// The column in the CollectionTable that acts as a generation number.
    /// This is the "child-resource-generation-number" in RFD 192.
    type GenerationNumberColumn: Column;

    /// The column in the ResourceTable that acts as a foreign key into
    /// the CollectionTable
    type CollectionIdColumn: Column;
}

/// Utility type to make trait bounds below easier to read.
type CollectionTableId<C> =
    <<C as DatastoreCollection>::CollectionTable as Identifiable>::Id;

/// Wrapper around [`diesel::insert`] for a Table, which creates the CTE
/// described in the module docs.
///
/// IS: [`InsertStatement`] which we are extending.
/// C: Collection type.
pub trait InsertIntoCollection<IS, C>
where
    IS: InsertStatementExt,
    C: DatastoreCollection,
    CollectionTableId<C>: Copy + Debug,
{
    /// Nests the existing insert statement in a CTE which
    /// identifies if the containing collection exists (by ID),
    fn insert_into_collection<Q>(
        self,
        key: CollectionTableId<C>,
    ) -> InsertIntoCollectionStatement<IS, C, Q>;
}

impl<IS, C> InsertIntoCollection<IS, C> for IS
where
    IS: InsertStatementExt,
    C: DatastoreCollection,
    CollectionTableId<C>: Copy + Debug,
{
    fn insert_into_collection<Q>(
        self,
        key: <C::CollectionTable as Identifiable>::Id,
    ) -> InsertIntoCollectionStatement<IS, C, Q> {
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
    IS: InsertStatementExt,
    C: DatastoreCollection,
    CollectionTableId<C>: Copy + Debug,
{
    insert_statement:
        InsertStatement<IS::Table, IS::Records, IS::Operator, IS::Returning>,
    key: CollectionTableId<C>,
    query_type: PhantomData<Q>,
}

impl<IS, C, Q> QueryId for InsertIntoCollectionStatement<IS, C, Q>
where
    IS: InsertStatementExt,
    C: DatastoreCollection,
    CollectionTableId<C>: Copy + Debug,
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
    IS: 'static + InsertStatementExt + Send,
    C: 'static + DatastoreCollection + Send,
    CollectionTableId<C>: 'static + PartialEq + Send + Debug + Copy,
    IS::Table: 'static + Table + Send,
    IS::Records: 'static + Send,
    Q: 'static + Debug + Send,
{
    /// Issues the CTE and parses the result.
    ///
    /// The three outcomes are:
    /// - Ok(Row was inserted)
    /// - Error(collection not found)
    /// - Error(other diesel error)
    pub async fn execute(
        self,
        pool: &bb8::Pool<ConnectionManager<PgConnection>>,
    ) -> InsertIntoCollectionResult<Q>
    where
        // We require this bound to ensure that "Self" is runnable as query.
        Self: LoadQuery<PgConnection, Q>,
    {
        match self.get_result_async::<Q>(pool).await {
            Ok(row) => Ok(row),
            Err(PoolError::Connection(ConnectionError::Query(
                diesel::result::Error::DatabaseError(kind, info),
            ))) => {
                // TODO: Figure out what the 1/0 generates
                println!("{:?} {:?}", kind, info);
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
    IS: InsertStatementExt,
    Q: Selectable<Pg>,
    C: DatastoreCollection,
    CollectionTableId<C>: Copy + Debug,
{
    type SqlType = SelectableSqlType<Q>;
}

impl<IS, C, Q> RunQueryDsl<PgConnection>
    for InsertIntoCollectionStatement<IS, C, Q>
where
    IS: InsertStatementExt,
    IS::Table: Table,
    C: DatastoreCollection,
    CollectionTableId<C>: Copy + Debug,
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

/// This implementation uses the following CTE:
///
/// ```text
/// // WITH found_row AS (SELECT <PK> FROM C WHERE <PK> = <value> AND
/// //                    time_deleted IS NULL FOR UPDATE),
/// //      dummy AS (SELECT IF(EXISTS(found_row), TRUE, 1/0)),
/// //      updated_row AS (UPDATE C SET <generation number> =
/// //                      <generation_number> + 1 WHERE <PK> = <value> AND
/// //                      time_deleted IS NULL),
/// //      inserted_row AS (<user provided insert statement>)
/// // SELECT * from inserted_row;
/// ```
impl<IS, C, Q> QueryFragment<Pg> for InsertIntoCollectionStatement<IS, C, Q>
where
    IS: InsertStatementExt,
    C: DatastoreCollection,
    CollectionTable<C>: HasTable<Table = CollectionTable<C>>
        + Table
        + diesel::query_dsl::methods::FindDsl<CollectionTableId<C>>,
    CollectionTableId<C>: Copy + Debug,
    Find<CollectionTable<C>, CollectionTableId<C>>: QueryFragment<Pg>,
    InsertStatement<IS::Table, IS::Records, IS::Operator, IS::Returning>:
        QueryFragment<Pg>,
{
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.push_sql("WITH found_row AS (");
        let subquery = C::CollectionTable::table()
            .find(self.key)
            // TODO: Lift time_deleted out of here
            .filter(C::CollectionTable::time_deleted.is_null())
            .for_update();
        subquery.walk_ast(out.reborrow())?;

        out.push_sql("), dummy AS (SELECT IF(EXISTS(found_row), TRUE, 1/0)),");

        out.push_sql("updated_row AS (");
        let subquery = diesel::update(C::CollectionTable::table())
            .filter(C::CollectionTable::table().primary_key().eq(self.key))
            .filter(C::CollectionTable::time_deleted.is_null())
            .set(C::GenerationNumberColumn.eq(C::GenerationNumberColumn + 1))
            .for_update();

        out.push_sql("), inserted_row AS (");
        // TODO: Check or force the insert_statement to have
        // C::CollectionIdColumn set
        self.insert_statement.walk_ast(out.reborrow())?;
        out.push_sql(") ");

        out.push_sql("SELECT ");
        C::ResourceTable::all_columns().walk_ast(out.reborrow())?;
        out.push_sql(" from inserted_row");
        Ok(())
    }
}
