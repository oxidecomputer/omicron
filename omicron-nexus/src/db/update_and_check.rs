use diesel::associations::HasTable;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::query_source::Table;
use diesel::sql_types;

/// Wrapper around [`diesel::update`] for a Table, which allows
/// callers to distinguish between "not found", "found but not updated", and
/// "updated".
///
/// T: Table on which the UpdateAndCheck should be applied.
/// K: Primary Key type.
/// U: Where clause of the update statement.
/// V: Changeset to be applied to the update statement.
pub trait UpdateAndCheck<T, K, U, V> {
    /// Nests the existing update statement in a CTE which
    /// identifies if the row exists (by ID), even if the row
    /// cannot be successfully updated.
    fn check_if_exists(self, key: K) -> UpdateAndQueryStatement<T, K, U, V>;
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
impl<T, K, U, V> UpdateAndCheck<T, K, U, V> for UpdateStatement<T, U, V> {
    fn check_if_exists(self, key: K) -> UpdateAndQueryStatement<T, K, U, V> {
        UpdateAndQueryStatement { update_statement: self, key }
    }
}

/// An UPDATE statement which can be combined (via a CTE)
/// with other statements to also SELECT a row.
#[derive(Debug, Clone, Copy)]
#[must_use = "Queries must be executed"]
pub struct UpdateAndQueryStatement<T, K, U, V> {
    update_statement: UpdateStatement<T, U, V>,
    key: K,
}

impl<T, K, U, V> QueryId for UpdateAndQueryStatement<T, K, U, V> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

/// Result of [`UpdateAndQueryStatement`].
#[derive(Copy, Clone, PartialEq, Debug)]
pub enum UpdateAndQueryResult {
    /// The row exists and was updated.
    Updated,
    /// The row exists, but was not updated.
    NotUpdatedButExists,
}

// Representation of Primary Key in Rust.
type PrimaryKey<T> = <T as diesel::Table>::PrimaryKey;
// Representation of Primary Key in SQL.
type SerializedPrimaryKey<T> = <PrimaryKey<T> as diesel::Expression>::SqlType;

impl<T, K, U, V> UpdateAndQueryStatement<T, K, U, V>
where
    // Bounds to compare primary keys and ensure that they're queryable:
    K: PartialEq + diesel::Queryable<SerializedPrimaryKey<T>, diesel::pg::Pg>,
    // Bounds which ensure an impl of LoadQuery exists:
    Pg: sql_types::HasSqlType<SerializedPrimaryKey<T>>,
    <Self as AsQuery>::Query: QueryFragment<Pg>,
    // Bound to implement QueryFragment:
    T: Table,
    SerializedPrimaryKey<T>: sql_types::NotNull,
{
    /// Issues the CTE and parses the result.
    ///
    /// The three outcomes are:
    /// - Ok(Row exists and was updated)
    /// - Ok(Row exists, but was not updated)
    /// - Error (row doesn't exist, or other diesel error)
    pub fn execute_and_check(
        self,
        conn: &PgConnection,
    ) -> Result<UpdateAndQueryResult, diesel::result::Error> {
        let (id0, id1) = self.get_result::<(Option<K>, Option<K>)>(conn)?;
        if id0 == id1 {
            Ok(UpdateAndQueryResult::Updated)
        } else {
            Ok(UpdateAndQueryResult::NotUpdatedButExists)
        }
    }
}

impl<T, K, U, V> Query for UpdateAndQueryStatement<T, K, U, V>
where
    T: Table,
    SerializedPrimaryKey<T>: sql_types::NotNull,
{
    type SqlType = (
        sql_types::Nullable<SerializedPrimaryKey<T>>,
        sql_types::Nullable<SerializedPrimaryKey<T>>,
    );
}

impl<T, K, U, V> RunQueryDsl<PgConnection>
    for UpdateAndQueryStatement<T, K, U, V>
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
/// //        found.<primary key> AS found_<primary_key>,
/// //        updated.<primary key> AS updated_<primary_key>
/// // FROM
/// //        found
/// // LEFT JOIN
/// //        updated
/// // ON
/// //        found.<primary_key> = updated.<primary_key>;
/// ```
impl<T, K, U, V> QueryFragment<Pg> for UpdateAndQueryStatement<T, K, U, V>
where
    T: HasTable<Table = T>
        + Table
        + diesel::query_dsl::methods::FindDsl<K>
        + Copy,
    K: Copy,
    <T as diesel::query_dsl::methods::FindDsl<K>>::Output: QueryFragment<Pg>,
    <T as Table>::PrimaryKey: diesel::Column,
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

        out.push_sql(" FROM found LEFT JOIN updated ON");
        out.push_sql(" found.");
        out.push_identifier(name)?;
        out.push_sql(" = ");
        out.push_sql("updated.");
        out.push_identifier(name)?;

        Ok(())
    }
}
