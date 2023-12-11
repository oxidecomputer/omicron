// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CTE implementation for "UPDATE with extended return status".

use super::column_walker::ColumnWalker;
use super::pool::DbConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::associations::HasTable;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::query_dsl::methods::LoadQuery;
use diesel::query_source::Table;
use diesel::result::Error as DieselError;
use diesel::sql_types::Nullable;
use diesel::QuerySource;
use std::marker::PhantomData;

/// A simple wrapper type for Diesel's [`UpdateStatement`], which
/// allows referencing generics with names (and extending usage
/// without re-stating those generic parameters everywhere).
pub trait UpdateStatementExt {
    type Table: Table + QuerySource;
    type WhereClause;
    type Changeset;

    fn statement(
        self,
    ) -> UpdateStatement<Self::Table, Self::WhereClause, Self::Changeset>;
}

impl<T, U, V> UpdateStatementExt for UpdateStatement<T, U, V>
where
    T: Table + QuerySource,
{
    type Table = T;
    type WhereClause = U;
    type Changeset = V;

    fn statement(self) -> UpdateStatement<T, U, V> {
        self
    }
}

/// Wrapper around [`diesel::update`] for a Table, which allows
/// callers to distinguish between "not found", "found but not updated", and
/// "updated".
///
/// US: [`UpdateStatement`] which we are extending.
/// K: Primary Key type.
pub trait UpdateAndCheck<US, K>
where
    US: UpdateStatementExt,
{
    /// Nests the existing update statement in a CTE which
    /// identifies if the row exists (by ID), even if the row
    /// cannot be successfully updated.
    fn check_if_exists<Q>(self, key: K) -> UpdateAndQueryStatement<US, K, Q>;
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
// This allows our implementation of the CTE to overwrite
// the return behavior of the SQL statement.
impl<US, K> UpdateAndCheck<US, K> for US
where
    US: UpdateStatementExt,
    US::Table: HasTable<Table = US::Table>
        + Table
        + diesel::query_dsl::methods::FindDsl<K>,
    <US::Table as diesel::query_dsl::methods::FindDsl<K>>::Output:
        QueryFragment<Pg> + Send + 'static,
    K: 'static + Copy + Send,
{
    fn check_if_exists<Q>(self, key: K) -> UpdateAndQueryStatement<US, K, Q> {
        let find_subquery = Box::new(US::Table::table().find(key));
        UpdateAndQueryStatement {
            update_statement: self.statement(),
            find_subquery,
            key_type: PhantomData,
            query_type: PhantomData,
        }
    }
}

/// An UPDATE statement which can be combined (via a CTE)
/// with other statements to also SELECT a row.
#[must_use = "Queries must be executed"]
pub struct UpdateAndQueryStatement<US, K, Q>
where
    US: UpdateStatementExt,
{
    update_statement:
        UpdateStatement<US::Table, US::WhereClause, US::Changeset>,
    find_subquery: Box<dyn QueryFragment<Pg> + Send>,
    key_type: PhantomData<K>,
    query_type: PhantomData<Q>,
}

impl<US, K, Q> QueryId for UpdateAndQueryStatement<US, K, Q>
where
    US: UpdateStatementExt,
{
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

// Representation of an UpdateStatement's table.
type UpdateTable<US> = <US as UpdateStatementExt>::Table;
// Representation of Primary Key in Rust.
type PrimaryKey<US> = <UpdateTable<US> as diesel::Table>::PrimaryKey;
// Representation of Primary Key in SQL.
type SerializedPrimaryKey<US> = <PrimaryKey<US> as diesel::Expression>::SqlType;

impl<US, K, Q> UpdateAndQueryStatement<US, K, Q>
where
    Self: Send,
    US: 'static + UpdateStatementExt,
    K: 'static + Copy + PartialEq + Send,
    US::Table: 'static + Table + Send,
    US::WhereClause: 'static + Send,
    US::Changeset: 'static + Send,
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
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<UpdateAndQueryResult<Q>, DieselError>
    where
        // We require this bound to ensure that "Self" is runnable as query.
        Self: LoadQuery<'static, DbConnection, (Option<K>, Option<K>, Q)>,
    {
        let (id0, id1, found) =
            self.get_result_async::<(Option<K>, Option<K>, Q)>(conn).await?;
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

impl<US, K, Q> Query for UpdateAndQueryStatement<US, K, Q>
where
    US: UpdateStatementExt,
    US::Table: Table,
    Q: Selectable<Pg>,
{
    type SqlType = (
        Nullable<SerializedPrimaryKey<US>>,
        Nullable<SerializedPrimaryKey<US>>,
        SelectableSqlType<Q>,
    );
}

impl<US, K, Q> RunQueryDsl<DbConnection> for UpdateAndQueryStatement<US, K, Q>
where
    US: UpdateStatementExt,
    US::Table: Table,
{
}

/// This implementation uses the following CTE:
///
/// ```text
/// // WITH found   AS (SELECT <primary key> FROM T WHERE <primary key = value>)
/// //      updated AS (UPDATE T SET <constraints> RETURNING <primary key>)
/// // SELECT
/// //      found.<primary key>
/// //      updated.<primary key>
/// //      found.<all columns>
/// // FROM
/// //      found
/// // LEFT JOIN
/// //      updated
/// // ON
/// //      found.<primary_key> = updated.<primary_key>;
/// ```
impl<US, K, Q> QueryFragment<Pg> for UpdateAndQueryStatement<US, K, Q>
where
    US: UpdateStatementExt,
    US::Table: HasTable<Table = US::Table> + Table,
    ColumnWalker<<<US as UpdateStatementExt>::Table as Table>::AllColumns>:
        IntoIterator<Item = &'static str>,
    PrimaryKey<US>: diesel::Column,
    UpdateStatement<US::Table, US::WhereClause, US::Changeset>:
        QueryFragment<Pg>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        let primary_key = <PrimaryKey<US> as Column>::NAME;

        out.push_sql("WITH found AS (");
        self.find_subquery.walk_ast(out.reborrow())?;
        out.push_sql("), updated AS (");
        self.update_statement.walk_ast(out.reborrow())?;
        out.push_sql(" RETURNING ");
        out.push_identifier(primary_key)?;
        out.push_sql(") ");

        out.push_sql("SELECT");

        out.push_sql(" found.");
        out.push_identifier(primary_key)?;
        out.push_sql(", updated.");
        out.push_identifier(primary_key)?;

        // List all the "found" columns explicitly.
        // This admittedly repeats the primary key, but that keeps the query
        // "simple" since it returns all columns in the same order as
        // AllColumns.
        let all_columns = ColumnWalker::<
            <<US as UpdateStatementExt>::Table as Table>::AllColumns,
        >::new();
        for column in all_columns.into_iter() {
            out.push_sql(", found.");
            out.push_identifier(column)?;
        }

        out.push_sql(" FROM found LEFT JOIN updated ON");
        out.push_sql(" found.");
        out.push_identifier(primary_key)?;
        out.push_sql(" = ");
        out.push_sql("updated.");
        out.push_identifier(primary_key)?;

        Ok(())
    }
}
