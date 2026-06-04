// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Generic stale-execution-guarded inserts for resources created by fault
//! management.
//!
//! The caller is expected to be a subtask of fm_rendezvous, executing either an
//! [`AlertRequest`] or [`SupportBundleRequest`] in a sitrep. Given some
//! unconditional Diesel `INSERT` statement for the requested alert or support
//! bundle, the goal here is to wrap the statement such that it's conditional
//! on a couple things:
//!
//!   - The generation number in `GENERATION_COLUMN` on the sitrep being
//!     executed must be equal to the generation of the latest sitrep in
//!     the database. This guards against stale execution.
//!
//!   - There is no entry in `MARKER_TABLE` for the requested resource id
//!     indicating that the resource was previously created. This guards
//!     against resurrection of resources that were already deleted.
//!
//! See detailed comments on trait [`SitrepGuardedResource`] and struct
//! [`SitrepGuardedInsert`] for how this policy is implemented.
//!
//! [`AlertRequest`]: nexus_types::fm::case::AlertRequest
//! [`SupportBundleRequest`]: nexus_types::fm::case::SupportBundleRequest

use crate::db::true_or_cast_error::matches_sentinel;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::Column;
use diesel::QueryResult;
use diesel::associations::HasTable;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::Query;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::query_source::QuerySource;
use diesel::result::Error as DieselError;
use diesel::sql_types;
use nexus_db_lookup::DbConnection;
use nexus_db_model::Generation;
use uuid::Uuid;

/// Trait supplying the types injected into [`SitrepGuardedInsert`]'s emitted
/// SQL. This codifies the expectation that each guarded resource `R` will have
/// some generation column on `fm_sitrep`, and some "marker" table (distinct
/// from the resource) for tracking resource creation.
///
/// Example:
///
/// ```ignore
/// use schema::fm_sitrep::dsl::my_resource_generation;
/// use schema::rendezvous_my_resource_created::dsl::my_resource_id;
///
/// impl SitrepGuardedResource for MyResource {
///     type GenerationColumn = my_resource_generation;
///     type MarkerIdColumn = my_resource_id;
/// }
/// ```
///
pub trait SitrepGuardedResource {
    /// Column on `fm_sitrep` carrying this resource's generation counter.
    type GenerationColumn: Column;
    /// Resource-id column in the marker table.
    type MarkerIdColumn: Column;
}

/// The marker table for resource `R`: the table owning its
/// [`SitrepGuardedResource::MarkerIdColumn`].
type MarkerTable<R> =
    <<R as SitrepGuardedResource>::MarkerIdColumn as Column>::Table;

/// CTE-wrapped stale-execution-guarded INSERT.
///
/// Type parameters:
/// - `R` is the type of the resource being inserted, which must implement
///   [`SitrepGuardedResource`] (see [`Self::walk_ast`] below).
/// - `ISR` is the wrapped `INSERT` -- typically a Diesel-built
///   `InsertStatement<...>`, inferred at the call site. It must include a
///   `RETURNING` clause projecting to `R`.
#[must_use = "Queries must be executed"]
pub struct SitrepGuardedInsert<R, ISR>
where
    R: SitrepGuardedResource,
{
    /// UUID of the resource being inserted. Used as the lookup key in the
    /// `prior_marker_guard` CTE and as the row value written by the
    /// `new_marker` CTE.
    resource_id: Uuid,

    /// Resource generation in the sitrep currently being executed by
    /// fm_rendezvous. The `stale_guard` CTE requires this to equal the latest
    /// sitrep's value of [`SitrepGuardedResource::GenerationColumn`].
    expected_generation: Generation,

    /// Caller-built INSERT for the resource row itself, nested into the
    /// `new_resource` CTE via [`QueryFragment::walk_ast`].
    resource_insert: ISR,

    /// The marker table's `FROM` clause, built once in [`Self::new`] from
    /// [`MarkerTable<R>`]. Stored as a field here, rather than reconstructed
    /// inside [`Self::walk_ast`] so it correctly shares `self`'s lifetime when
    /// walked.
    marker_from_clause: <MarkerTable<R> as QuerySource>::FromClause,
}

impl<R, ISR> SitrepGuardedInsert<R, ISR>
where
    R: SitrepGuardedResource,
    MarkerTable<R>: HasTable<Table = MarkerTable<R>>,
{
    /// Build a guarded insert for `resource_id` at `expected_generation`.
    ///
    /// The wrapped `resource_insert` statement should be built with
    /// `ON CONFLICT DO NOTHING`. This combinator distinguishes "freshly
    /// created" from "already exists" purely by whether the inner INSERT's
    /// `RETURNING` produced a row: [`Self::execute_async`] maps an empty result
    /// to [`SitrepGuardedInsertOutcome::AlreadyExists`]. An inner INSERT that
    /// errored (rather than silently no-op'd) on a primary-key conflict would
    /// turn the "row already exists" outcome into a hard error.
    pub fn new(
        resource_id: Uuid,
        expected_generation: Generation,
        resource_insert: ISR,
    ) -> Self {
        let marker_from_clause =
            <MarkerTable<R> as HasTable>::table().from_clause();
        Self {
            resource_id,
            expected_generation,
            resource_insert,
            marker_from_clause,
        }
    }
}

impl<R, ISR> QueryId for SitrepGuardedInsert<R, ISR>
where
    R: SitrepGuardedResource,
{
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

/// The combinator's projection mirrors the inner `INSERT`'s `RETURNING` clause.
/// See [`Self::walk_ast`]'s outer `SELECT nr.* FROM new_resource nr`.
impl<R, ISR> Query for SitrepGuardedInsert<R, ISR>
where
    R: SitrepGuardedResource,
    ISR: Query,
{
    type SqlType = ISR::SqlType;
}

/// Sentinel string emitted by the `stale_guard` CTE on generation mismatch.
/// Decoded back to [`SitrepGuardedInsertOutcome::StaleSitrep`].
const STALE_GENERATION_SENTINEL: &str = "stale generation";

/// Sentinel string emitted by the `prior_marker_guard` CTE when a creation
/// marker already exists for this resource id. Decoded back to
/// [`SitrepGuardedInsertOutcome::AlreadyExists`].
const ALREADY_EXISTS_SENTINEL: &str = "marker already exists";

impl<R, ISR> QueryFragment<Pg> for SitrepGuardedInsert<R, ISR>
where
    R: SitrepGuardedResource,
    ISR: QueryFragment<Pg>,
    <MarkerTable<R> as QuerySource>::FromClause: QueryFragment<Pg>,
{
    /// Executes a statement of the form:
    ///
    /// ```sql
    /// WITH
    ///   latest_history AS MATERIALIZED (...),
    ///   stale_guard AS MATERIALIZED (...),
    ///   prior_marker_guard AS MATERIALIZED (...),
    ///   new_resource AS (...),
    ///   new_marker AS (...)
    /// SELECT nr.* FROM new_resource nr
    /// ```
    ///
    /// The individual CTEs here are:
    ///
    /// 1. `latest_history`: the most-recent `fm_sitrep_history` row.
    /// 2. `stale_guard`: compare `expected_generation` against the generation
    ///    on the latest sitrep. If it doesn't match, it aborts with a
    ///    non-retryable DB error that maps to
    ///    [`SitrepGuardedInsertOutcome::StaleSitrep`].
    /// 3. `prior_marker_guard`: checks whether a creation marker already exists
    ///    for this resource id. If it does, aborts with a non-retryable DB
    ///    error that maps to [`SitrepGuardedInsertOutcome::AlreadyExists`].
    /// 4. `new_resource`: the caller's resource INSERT.
    /// 5. `new_marker`: the marker INSERT, emitted inline by the
    ///    combinator using R's schema types. This only runs when
    ///    `new_resource` actually produced a row.
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        // Column names injected into the CTEs below.
        let marker_id_column = <R::MarkerIdColumn as Column>::NAME;
        let generation_column = <R::GenerationColumn as Column>::NAME;

        // CTE 1: latest_history.
        out.push_sql(
            "WITH latest_history AS MATERIALIZED (\
                SELECT sitrep_id FROM omicron.public.fm_sitrep_history \
                 ORDER BY version DESC LIMIT 1\
             )",
        );

        // CTE 2: stale_guard.
        //
        // A note on why Diesel has made us sad today: We'd have preferred to
        // use `TrueOrCastError` here in `stale_guard` and `prior_marker_guard`
        // below, rather than copy-pasting the `CAST(IF(...))` shape. We
        // couldn't make it work.
        //
        // The crux of the problem was the signature of Diesel's
        // `QueryFragment::walk_ast`, which ties the lifetimes of its `&'b self`
        // and `out: AstPass<'_, 'b, Pg>` parameters to a single `'b`. If we try
        // to write something here like:
        //
        // ```rust
        // let guard = TrueOrCastError::new(...);
        // guard.walk_ast(out.reborrow());
        // ```
        //
        // ... the borrow checker would complain that `guard` doesn't have the
        // same lifetime as `self`. We could certainly do this, lifting the
        // guard clauses into fields of `SitrepGuardedInsert` to give them the
        // correct lifetimes, but that would be way less readable than just
        // writing the guards inline here.
        out.push_sql(
            ", stale_guard AS MATERIALIZED (SELECT CAST(IF(\
                ((SELECT s.",
        );
        out.push_identifier(generation_column)?;
        out.push_sql(
            " FROM omicron.public.fm_sitrep s \
                  JOIN latest_history lh ON lh.sitrep_id = s.id) = ",
        );
        out.push_bind_param::<sql_types::BigInt, _>(&self.expected_generation)?;
        out.push_sql("), 'TRUE', '");
        out.push_sql(STALE_GENERATION_SENTINEL);
        out.push_sql("') AS BOOL) AS ok)");

        // CTE 3: prior_marker_guard.
        out.push_sql(
            ", prior_marker_guard AS MATERIALIZED (SELECT CAST(IF(\
                NOT EXISTS (SELECT 1 FROM ",
        );
        self.marker_from_clause.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(marker_id_column)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, _>(&self.resource_id)?;
        out.push_sql("), 'TRUE', '");
        out.push_sql(ALREADY_EXISTS_SENTINEL);
        out.push_sql("') AS BOOL) AS ok)");

        // CTE 4: new_resource, the caller's INSERT statement.
        //
        // Note that unlike the guard CTEs, we're able to call walk_ast without
        // lifetime issues here because resource_insert is a field of self.
        out.push_sql(", new_resource AS (");
        self.resource_insert.walk_ast(out.reborrow())?;
        out.push_sql(")");

        // CTE 5: new_marker.
        //
        // The `WHERE EXISTS (SELECT 1 FROM new_resource)` predicate gates the
        // marker INSERT on the inner resource INSERT having produced a row.
        out.push_sql(", new_marker AS (INSERT INTO ");
        self.marker_from_clause.walk_ast(out.reborrow())?;
        out.push_sql(" (");
        out.push_identifier(marker_id_column)?;
        out.push_sql(", ");
        // The generation column name is hardcoded here rather than carried as a
        // `SitrepGuardedResource` associated type. We expect all marker tables
        // to use the same column name for consistency.
        out.push_identifier("created_at_generation")?;
        out.push_sql(") SELECT ");
        out.push_bind_param::<sql_types::Uuid, _>(&self.resource_id)?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::BigInt, _>(&self.expected_generation)?;
        out.push_sql(
            " WHERE EXISTS (SELECT 1 FROM new_resource) ON CONFLICT (",
        );
        out.push_identifier(marker_id_column)?;
        out.push_sql(") DO NOTHING RETURNING ");
        out.push_identifier(marker_id_column)?;
        out.push_sql(")");

        // Outer SELECT projects the inner INSERT's `RETURNING` columns through,
        // so callers receive the inserted row.
        out.push_sql(" SELECT nr.* FROM new_resource nr");

        Ok(())
    }
}

impl<R, ISR> diesel::RunQueryDsl<DbConnection> for SitrepGuardedInsert<R, ISR> where
    R: SitrepGuardedResource
{
}

/// Outcome of a [`SitrepGuardedInsert`] attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SitrepGuardedInsertOutcome<R> {
    /// Both guards passed and the resource `INSERT` produced a row; the inner
    /// statement's `RETURNING` clause is projected through and surfaced here.
    /// The marker `INSERT` also wrote its row atomically.
    Created(R),
    /// The resource already exists. Either `prior_marker_guard`
    /// short-circuited because a marker for this resource id was present, or
    /// both guards passed but the inner INSERT `ON CONFLICT DO NOTHING` matched
    /// a pre-existing row.
    AlreadyExists,
    /// The executor's expected generation does not match the current generation
    /// on the latest sitrep; nothing was inserted.
    StaleSitrep,
}

impl<R, ISR> SitrepGuardedInsert<R, ISR>
where
    R: SitrepGuardedResource + Send + 'static,
    ISR: 'static,
    Self:
        Send + diesel::query_dsl::methods::LoadQuery<'static, DbConnection, R>,
{
    pub async fn execute_async(
        self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<SitrepGuardedInsertOutcome<R>, DieselError> {
        match self.get_result_async::<R>(conn).await {
            Ok(row) => Ok(SitrepGuardedInsertOutcome::Created(row)),
            // Both guards passed, but the outer SELECT returned zero rows. This
            // is unlikely: the only way to reach this state is if the guards
            // both passed but the wrapped `INSERT`'s `ON CONFLICT DO NOTHING`
            // did nothing because there was a preexisting row with the same
            // UUID.
            Err(DieselError::NotFound) => {
                Ok(SitrepGuardedInsertOutcome::AlreadyExists)
            }
            Err(e)
                if matches_sentinel(&e, &[STALE_GENERATION_SENTINEL])
                    .is_some() =>
            {
                Ok(SitrepGuardedInsertOutcome::StaleSitrep)
            }
            Err(e)
                if matches_sentinel(&e, &[ALREADY_EXISTS_SENTINEL])
                    .is_some() =>
            {
                Ok(SitrepGuardedInsertOutcome::AlreadyExists)
            }
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use diesel::prelude::*;

    // Dummy schema standing in for a real resource. These tables exist only in
    // this test module (not in the real schema) so we can exercise the typed
    // `SitrepGuardedResource` path without a concrete resource, which lands in
    // later PRs.
    table! {
        test_schema.dummy_sitrep (id) {
            id -> Uuid,
            dummy_generation -> Int8,
        }
    }
    table! {
        test_schema.dummy_marker (dummy_id) {
            dummy_id -> Uuid,
            created_at_generation -> Int8,
        }
    }
    table! {
        test_schema.dummy_resource (id) {
            id -> Uuid,
            name -> Text,
        }
    }

    struct DummyResource;
    impl SitrepGuardedResource for DummyResource {
        type GenerationColumn = dummy_sitrep::dsl::dummy_generation;
        type MarkerIdColumn = dummy_marker::dsl::dummy_id;
    }

    // Snapshots the SQL the combinator generates, so accidental changes to the
    // hand-assembled query are caught in review.
    #[tokio::test]
    async fn expectorate_sitrep_guarded_insert() {
        let resource_id =
            "11111111-1111-1111-1111-111111111111".parse().unwrap();
        let insert = diesel::insert_into(dummy_resource::table)
            .values((
                dummy_resource::dsl::id.eq(resource_id),
                dummy_resource::dsl::name.eq("test"),
            ))
            .on_conflict(dummy_resource::dsl::id)
            .do_nothing()
            .returning(dummy_resource::dsl::id);
        let query = SitrepGuardedInsert::<DummyResource, _>::new(
            resource_id,
            Generation::try_from(3).unwrap(),
            insert,
        );
        expectorate_query_contents(
            &query,
            "tests/output/sitrep_guarded_insert.sql",
        )
        .await;
    }
}
