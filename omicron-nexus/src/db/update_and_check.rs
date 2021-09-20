//! CTE implementation for "UPDATE with extended return status".

use async_bb8_diesel::{AsyncRunQueryDsl, DieselConnectionManager};
use diesel::associations::HasTable;
use diesel::helper_types::*;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::query_dsl::methods::LoadQuery;
use diesel::query_source::Table;
use diesel::sql_types::Nullable;
use std::marker::PhantomData;

/// Wrapper around [`diesel::update`] for a Table, which allows
/// callers to distinguish between "not found", "found but not updated", and
/// "updated".
///
/// T: Table on which the UpdateAndCheck should be applied.
/// K: Primary Key type.
/// U: Where clause of the update statement.
/// V: Changeset to be applied to the update statement.
/// Q: Return type of query
pub trait UpdateAndCheck<T, K, U, V> {
    /// Nests the existing update statement in a CTE which
    /// identifies if the row exists (by ID), even if the row
    /// cannot be successfully updated.
    fn check_if_exists<Q>(
        self,
        key: K,
    ) -> UpdateAndQueryStatement<T, K, U, V, Q>;
}

// UpdateStatement has four generic parameters:
// - T: Table which is being updated
// - U: Where clause
// - V: Changeset to be applied (default = SetNotCalled)
// - Ret: Returning clause (default = NoReturningClause)
//
// As currently implemented, we only define "UpdateAndCheck" for
// UpdateStatements using the default "Ret" value. This means
// the UpdateAndCheck methods can only be invoked for update statements
// to which a "returning" clause has not yet been added.
//
// This allows our implementation of the  CTE to overwrite
// the return behavior of the SQL statement.
impl<T, K, U, V> UpdateAndCheck<T, K, U, V> for UpdateStatement<T, U, V> {
    fn check_if_exists<Q>(
        self,
        key: K,
    ) -> UpdateAndQueryStatement<T, K, U, V, Q> {
        UpdateAndQueryStatement {
            update_statement: self,
            key,
            query_type: PhantomData,
        }
    }
}

/// An UPDATE statement which can be combined (via a CTE)
/// with other statements to also SELECT a row.
#[derive(Debug, Clone, Copy)]
#[must_use = "Queries must be executed"]
pub struct UpdateAndQueryStatement<T, K, U, V, Q> {
    update_statement: UpdateStatement<T, U, V>,
    key: K,
    query_type: PhantomData<Q>,
}

impl<T, K, U, V, Q> QueryId for UpdateAndQueryStatement<T, K, U, V, Q> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

/// Result of [`UpdateAndQueryStatement`].
#[derive(Copy, Clone, PartialEq, Debug)]
pub struct UpdateAndQueryResult<Q> {
    pub status: UpdateStatus,
    pub found: Q,
}

/// Status of [`UpdateAndQueryResult`].
#[derive(Copy, Clone, PartialEq, Debug)]
pub enum UpdateStatus {
    /// The row exists and was updated.
    Updated,
    /// The row exists, but was not updated.
    NotUpdatedButExists,
}

// Representation of Primary Key in Rust.
type PrimaryKey<T> = <T as diesel::Table>::PrimaryKey;
// Representation of Primary Key in SQL.
type SerializedPrimaryKey<T> = <PrimaryKey<T> as diesel::Expression>::SqlType;

impl<T, K, U, V, Q> UpdateAndQueryStatement<T, K, U, V, Q>
where
    K: 'static + PartialEq + Send,
    T: 'static + Table + Send,
    U: 'static + Send,
    V: 'static + Send,
    Q: std::fmt::Debug + Send + 'static,
{
    /// Issues the CTE and parses the result.
    ///
    /// The three outcomes are:
    /// - Ok(Row exists and was updated)
    /// - Ok(Row exists, but was not updated)
    /// - Error (row doesn't exist, or other diesel error)
    pub async fn execute_and_check(
        self,
        pool: &bb8::Pool<DieselConnectionManager<PgConnection>>,
    ) -> Result<UpdateAndQueryResult<Q>, diesel::result::Error>
    where
        // We require this bound to ensure that "Self" is runnable as query.
        Self: LoadQuery<PgConnection, (Option<K>, Option<K>, Q)>,
    {
        let (id0, id1, found) =
            self.get_result_async::<(Option<K>, Option<K>, Q)>(pool).await?;
        let status = if id0 == id1 {
            UpdateStatus::Updated
        } else {
            UpdateStatus::NotUpdatedButExists
        };

        Ok(UpdateAndQueryResult { status, found })
    }
}

type SelectableSqlType<Q> =
    <<Q as diesel::Selectable<Pg>>::SelectExpression as Expression>::SqlType;

impl<T, K, U, V, Q> Query for UpdateAndQueryStatement<T, K, U, V, Q>
where
    T: Table,
    Q: Selectable<Pg>,
{
    type SqlType = (
        Nullable<SerializedPrimaryKey<T>>,
        Nullable<SerializedPrimaryKey<T>>,
        SelectableSqlType<Q>,
    );
}

impl<T, K, U, V, Q> RunQueryDsl<PgConnection>
    for UpdateAndQueryStatement<T, K, U, V, Q>
where
    T: Table,
{
}

/// This implementation uses the following CTE:
///
/// ```text
/// // WITH found   AS (SELECT <primary key> FROM T WHERE <primary key = value>)
/// //      updated AS (UPDATE T SET <constraints> RETURNING *)
/// // SELECT
/// //      found.<primary key>
/// //      updated.<primary key>
/// //      found.*
/// // FROM
/// //      found
/// // LEFT JOIN
/// //      updated
/// // ON
/// //      found.<primary_key> = updated.<primary_key>;
/// ```
impl<T, K, U, V, Q> QueryFragment<Pg> for UpdateAndQueryStatement<T, K, U, V, Q>
where
    T: HasTable<Table = T> + Table + diesel::query_dsl::methods::FindDsl<K>,
    K: Copy,
    Find<T, K>: QueryFragment<Pg>,
    PrimaryKey<T>: diesel::Column,
    UpdateStatement<T, U, V>: QueryFragment<Pg>,
{
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.push_sql("WITH found AS (");
        let subquery = T::table().find(self.key);
        subquery.walk_ast(out.reborrow())?;
        out.push_sql("), updated AS (");
        self.update_statement.walk_ast(out.reborrow())?;
        // TODO: Only need primary? Or would we actually want
        // to pass the returned rows back through the result?
        out.push_sql(" RETURNING *) ");

        out.push_sql("SELECT");

        let name = <T::PrimaryKey as Column>::NAME;
        out.push_sql(" found.");
        out.push_identifier(name)?;
        out.push_sql(", updated.");
        out.push_identifier(name)?;
        // TODO: I'd prefer to list all columns explicitly. But how?
        // The types exist within Table::AllColumns, and each one
        // has a name as "<C as Column>::Name".
        // But Table::AllColumns is a tuple, which makes iteration
        // a pain.
        //
        // TODO: Technically, we're repeating the PK here.
        out.push_sql(", found.*");

        out.push_sql(" FROM found LEFT JOIN updated ON");
        out.push_sql(" found.");
        out.push_identifier(name)?;
        out.push_sql(" = ");
        out.push_sql("updated.");
        out.push_identifier(name)?;

        Ok(())
    }
}
