// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Resources created by FM rendezvous, described in terms of their Diesel
//! schema types.
//!
//! When executing a sitrep, FM rendezvous can create resources (currently
//! alerts and support bundles) in the database. To prevent wrongly creating
//! resources that have already been deleted by the user/operator, inserts are
//! guarded by a "creation marker" and a CTE that understands the semantics of
//! that marker (see [`crate::db::sitrep_guard`]).
//!
//! The [`FmRendezvousResource`] trait gathers the Diesel schema types that the
//! guard CTE needs for a given resource. Implementations are provided for the
//! two supported resource types, `Alert` and `SupportBundle`.

use diesel::Column;
use diesel::associations::HasTable;
use diesel::pg::Pg;
use diesel::query_builder::QueryFragment;
use diesel::query_source::QuerySource;
use nexus_db_schema::schema;

/// The creation marker table corresponding to some [`FmRendezvousResource`]
/// `R`.
///
/// This is the table that defines the column referenced by the
/// [`FmRendezvousResource::IdColumn`] associated type.
pub type MarkerTable<R> =
    <<R as FmRendezvousResource>::IdColumn as Column>::Table;

/// A resource created by FM rendezvous, comprising the Diesel schema types
/// required to generically construct useful queries / CTEs that operate on that
/// resource.
///
/// Each resource type is backed by three pieces of schema:
///
/// - The resource's own table (e.g. `alert`, `support_bundle`), holding the
///   resource rows themselves. This trait doesn't name that table; queries
///   that create the resource take a caller-built `INSERT` into it as input.
///
/// - A per-resource-type generation column on the `fm_sitrep` table
///   ([`Self::GenerationColumn`], e.g. `fm_sitrep.alert_generation`), bumped
///   each time a sitrep's request set for this resource type changes.
///   Comparing the executing sitrep's value against the latest sitrep's
///   detects a rendezvous task executing a stale sitrep.
///
/// - A creation marker table (e.g. `rendezvous_alert_created`), recording
///   each resource that FM rendezvous has ever created: the resource's id
///   ([`Self::IdColumn`], the marker table's primary key) and the generation
///   at which it was created. The marker is written atomically with the
///   resource row and outlives it: if the resource is deleted while some
///   sitrep still requests it, the marker's presence prevents the resource
///   from being re-created.
///
/// The common required trait bounds for building these queries are captured
/// here, in the trait's `where` clause, so we don't have to repeat them at
/// every use site.
pub trait FmRendezvousResource
where
    // `MarkerTable<R>` is a `Table` (see above), but disappointingly, `Table`
    // isn't `HasTable`! We need this bound to be able to call
    // `MarkerTable::table()`...
    MarkerTable<Self>: HasTable<Table = MarkerTable<Self>>,
    // ... Furthermore, we need to use the marker table in a `FROM` clause, so
    // it must be `QuerySource`. To be spliced into the query by `walk_ast`, the
    // `FROM` clause must also be `QueryFragment<Pg>`. And finally, the built
    // query is held across `.await`, so it must be `Send`.
    <MarkerTable<Self> as QuerySource>::FromClause: QueryFragment<Pg> + Send,
{
    /// The column on `fm_sitrep` carrying this resource's generation counter
    /// (e.g. `fm_sitrep::alert_generation` for `Alert`). Read by
    /// [`SitrepGuardedInsert`] to guard against stale execution.
    ///
    /// [`SitrepGuardedInsert`]: crate::db::sitrep_guard::SitrepGuardedInsert
    type GenerationColumn: Column<Table = schema::fm_sitrep::table>;

    /// The id column in the creation marker table
    /// (e.g. `rendezvous_alert_created::alert_id` for `Alert`).
    type IdColumn: Column;
}

impl FmRendezvousResource for nexus_db_model::Alert {
    type GenerationColumn = schema::fm_sitrep::dsl::alert_generation;
    type IdColumn = schema::rendezvous_alert_created::dsl::alert_id;
}

impl FmRendezvousResource for nexus_db_model::SupportBundle {
    type GenerationColumn = schema::fm_sitrep::dsl::support_bundle_generation;
    type IdColumn =
        schema::rendezvous_support_bundle_created::dsl::support_bundle_id;
}

/// Test fixtures for the insert (`sitrep_guard`) test suite: a synthetic
/// [`FmRendezvousResource`] and the throwaway schema its query references.
///
/// The dummy schema lets the suite exercise the generic, typed query path
/// against a stand-in resource, decoupled from the specifics of any real
/// resource.
#[cfg(test)]
pub(crate) mod test_utils {
    use super::FmRendezvousResource;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use async_bb8_diesel::AsyncSimpleConnection;
    use diesel::prelude::*;
    use nexus_db_lookup::DbConnection;
    use uuid::Uuid;

    // Schema for a dummy resource and its creation marker.
    table! {
        test_schema.dummy_resource (id) {
            id -> Uuid,
            name -> Text,
        }
    }
    table! {
        test_schema.dummy_marker (dummy_id) {
            dummy_id -> Uuid,
            created_at_generation -> Int8,
        }
    }

    // The dummy resource's generation column lives on the *real* `fm_sitrep`
    // table, not a dummy table. But of course the real `fm_sitrep`'s `table!`
    // doesn't declare it, so we have to manually define a Diesel `Column` for
    // it, equivalent to what `table!` would have generated.
    #[allow(non_camel_case_types)]
    #[derive(Debug, Clone, Copy)]
    pub(crate) struct dummy_generation;

    impl diesel::Expression for dummy_generation {
        type SqlType = diesel::sql_types::BigInt;
    }

    impl diesel::Column for dummy_generation {
        type Table = nexus_db_schema::schema::fm_sitrep::table;
        const NAME: &'static str = "dummy_generation";
    }

    // Model for the dummy resource.
    #[derive(Queryable, Selectable, Debug)]
    #[diesel(table_name = dummy_resource)]
    pub(crate) struct DummyResource {
        pub id: Uuid,
        pub name: String,
    }

    impl FmRendezvousResource for DummyResource {
        type GenerationColumn = dummy_generation;
        type IdColumn = dummy_marker::dsl::dummy_id;
    }

    /// Creates the tables the guarded-insert SQL references but which the real
    /// schema doesn't have: the dummy resource and marker tables, plus a
    /// `dummy_generation` column on the real `fm_sitrep` table.
    ///
    /// Mutating the real `fm_sitrep` is safe because each test runs against its
    /// own fresh `TestDatabase`; the added column never escapes into production
    /// or another test.
    pub(crate) async fn setup_dummy_schema(
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) {
        conn.batch_execute_async(
            "
             CREATE SCHEMA IF NOT EXISTS test_schema; \
             CREATE TABLE IF NOT EXISTS test_schema.dummy_resource ( \
                 id UUID PRIMARY KEY, \
                 name TEXT NOT NULL \
             ); \
             CREATE TABLE IF NOT EXISTS test_schema.dummy_marker ( \
                 dummy_id UUID PRIMARY KEY, \
                 created_at_generation INT8 NOT NULL \
             ); \
             ALTER TABLE omicron.public.fm_sitrep \
                 ADD COLUMN IF NOT EXISTS dummy_generation INT8;",
        )
        .await
        .unwrap();
    }

    /// Insert a row into the dummy marker table at the given
    /// `created_at_generation`.
    pub(crate) async fn insert_dummy_marker(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        id: Uuid,
        created_at_generation: i64,
    ) {
        diesel::insert_into(dummy_marker::table)
            .values((
                dummy_marker::dsl::dummy_id.eq(id),
                dummy_marker::dsl::created_at_generation
                    .eq(created_at_generation),
            ))
            .execute_async(conn)
            .await
            .unwrap();
    }

    /// The `created_at_generation` recorded in the marker row for `id`, if
    /// any (`None` means no marker row exists).
    pub(crate) async fn marker_generation(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        id: Uuid,
    ) -> Option<i64> {
        dummy_marker::table
            .filter(dummy_marker::dsl::dummy_id.eq(id))
            .select(dummy_marker::dsl::created_at_generation)
            .first_async::<i64>(conn)
            .await
            .optional()
            .unwrap()
    }
}
