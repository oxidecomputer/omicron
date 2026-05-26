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
use diesel::QueryResult;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::Query;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::result::Error as DieselError;
use diesel::sql_types;
use nexus_db_lookup::DbConnection;
use std::marker::PhantomData;
use uuid::Uuid;

/// Trait supplying the static SQL identifiers injected into
/// [`SitrepGuardedInsert`]'s emitted SQL. This codifies the expectation that
/// each guarded resource `R` will have some generation column on `fm_sitrep`,
/// and some "marker" table (distinct from the resource) for tracking resource
/// creation.
///
/// Example:
///
/// ```ignore
/// impl SitrepGuardedResource for nexus_db_model::Alert {
///    const GENERATION_COLUMN: &'static str = "alert_generation";
///    const MARKER_TABLE: &'static str = "rendezvous_alert_created";
///    const MARKER_ID_COLUMN: &'static str = "alert_id";
/// }
/// ```
///
/// All three constants here are hard-coded `&'static str`s, never user input,
/// and safe to inject directly as text.
pub trait SitrepGuardedResource {
    /// Column on `fm_sitrep` carrying this resource's generation counter.
    const GENERATION_COLUMN: &'static str;
    /// Creation table tracked alongside the resource.
    const MARKER_TABLE: &'static str;
    /// Resource-id column in the marker table.
    const MARKER_ID_COLUMN: &'static str;
}

/// CTE-wrapped stale-execution-guarded INSERT.
///
/// Type parameters:
/// - `R` is the type of the resource being inserted, which must implement
///   [`SitrepGuardedResource`] (see [`Self::walk_ast`] below).
/// - `ISR` is the wrapped `INSERT` -- typically a Diesel-built
///   `InsertStatement<...>`, inferred at the call site. It must include a
///   `RETURNING` clause projecting to `R`.
#[must_use = "Queries must be executed"]
pub struct SitrepGuardedInsert<R, ISR> {
    /// UUID of the resource being inserted. Used as the lookup key in the
    /// `prior_marker_guard` CTE and as the row value written by the
    /// `new_marker` CTE.
    resource_id: Uuid,

    /// Resource generation in the sitrep currently being executed by
    /// fm_rendezvous. The `stale_guard` CTE requires this to equal the latest
    /// sitrep's value of [`SitrepGuardedResource::GENERATION_COLUMN`].
    expected_generation: i64,

    /// Caller-built INSERT for the resource row itself, nested into the
    /// `new_resource` CTE via [`QueryFragment::walk_ast`].
    resource_insert: ISR,

    /// Phantom data field carrying the `R` type parameter to make rustc happy.
    _phantom: PhantomData<R>,
}

impl<R, ISR> SitrepGuardedInsert<R, ISR> {
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
        expected_generation: i64,
        resource_insert: ISR,
    ) -> Self {
        Self {
            resource_id,
            expected_generation,
            resource_insert,
            _phantom: PhantomData,
        }
    }
}

/// Opt out of the prepared-statement cache. Because [`Self::walk_ast`] emits
/// SQL with identifiers chosen from `R`'s `&'static str` consts, two distinct
/// `R`s map to two different SQL strings even though they share a Rust type.
/// Setting `HAS_STATIC_QUERY_ID = false` is what disables caching;
/// [`Self::walk_ast`] does not need to call `out.unsafe_to_cache_prepared()`.
impl<R, ISR> QueryId for SitrepGuardedInsert<R, ISR> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

/// The combinator's projection mirrors the inner `INSERT`'s `RETURNING` clause.
/// See [`Self::walk_ast`]'s outer `SELECT nr.* FROM new_resource nr`.
impl<R, ISR> Query for SitrepGuardedInsert<R, ISR>
where
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
    ///    combinator using R's `&'static str` consts. This only runs when
    ///    `new_resource` actually produced a row.
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
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
        out.push_sql(R::GENERATION_COLUMN);
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
                NOT EXISTS (SELECT 1 FROM omicron.public.",
        );
        out.push_sql(R::MARKER_TABLE);
        out.push_sql(" WHERE ");
        out.push_sql(R::MARKER_ID_COLUMN);
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
        out.push_sql(", new_marker AS (INSERT INTO omicron.public.");
        out.push_sql(R::MARKER_TABLE);
        out.push_sql(" (");
        out.push_sql(R::MARKER_ID_COLUMN);
        out.push_sql(", created_at_generation) SELECT ");
        out.push_bind_param::<sql_types::Uuid, _>(&self.resource_id)?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::BigInt, _>(&self.expected_generation)?;
        out.push_sql(
            " WHERE EXISTS (SELECT 1 FROM new_resource) ON CONFLICT (",
        );
        out.push_sql(R::MARKER_ID_COLUMN);
        out.push_sql(") DO NOTHING RETURNING ");
        out.push_sql(R::MARKER_ID_COLUMN);
        out.push_sql(")");

        // Outer SELECT projects the inner INSERT's `RETURNING` columns through,
        // so callers receive the inserted row.
        out.push_sql(" SELECT nr.* FROM new_resource nr");

        Ok(())
    }
}

impl<R, ISR> diesel::RunQueryDsl<DbConnection> for SitrepGuardedInsert<R, ISR> {}

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
    R: Send + 'static,
    ISR: Send + 'static,
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
            Err(e) => {
                if matches_sentinel(&e, &[STALE_GENERATION_SENTINEL]).is_some()
                {
                    Ok(SitrepGuardedInsertOutcome::StaleSitrep)
                } else if matches_sentinel(&e, &[ALREADY_EXISTS_SENTINEL])
                    .is_some()
                {
                    Ok(SitrepGuardedInsertOutcome::AlreadyExists)
                } else {
                    Err(e)
                }
            }
        }
    }
}
