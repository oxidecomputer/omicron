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
//! that marker (see [`crate::db::sitrep_guard`]). Once no sitrep can request a
//! resource again, its marker is garbage-collected (see
//! `crate::db::datastore::fm_rendezvous_gc`).
//!
//! The [`FmRendezvousResource`] trait gathers the Diesel schema types that the
//! guard CTE and the GC sweep need for a given resource. Implementations are
//! provided for the two supported resource types, `Alert` and `SupportBundle`.

use diesel::Column;
use diesel::associations::HasTable;
use diesel::pg::Pg;
use diesel::query_builder::QueryFragment;
use diesel::query_source::QuerySource;
use nexus_db_schema::schema;

/// The creation marker table corresponding to some [`FmRendezvousResource`]
/// `R`. This is the table that defines the column referenced by the
/// [`FmRendezvousResource::IdColumn`] associated type.
pub type MarkerTable<R> =
    <<R as FmRendezvousResource>::IdColumn as Column>::Table;

/// A resource created by FM rendezvous, comprising the Diesel schema types
/// required to generically construct useful queries / CTEs that operate on that
/// resource.
///
/// Each resource type is backed by four pieces of schema:
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
/// - A request table ([`Self::RequestTable`], e.g. `fm_alert_request`),
///   holding the set of resources of this type requested by each sitrep,
///   keyed by the requested resource's id and the sitrep's id. The GC query
///   joins it to test whether a marked resource is still requested by the
///   sitrep being executed.
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
    // Same as the marker `FROM` clause above, but for the request table, which
    // only the GC query splices in. (`RequestTable` carries its own `HasTable`
    // bound inline, since it's a named associated type rather than an alias.)
    <Self::RequestTable as QuerySource>::FromClause: QueryFragment<Pg> + Send,
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

    /// The `fm_*_request` table (e.g. `fm_alert_request`) joined by the GC query
    /// to test whether a marked resource is still requested by the sitrep being
    /// executed. Its `id` and `sitrep_id` columns are referenced by name.
    type RequestTable: HasTable<Table = Self::RequestTable> + QuerySource;
}

impl FmRendezvousResource for nexus_db_model::Alert {
    type GenerationColumn = schema::fm_sitrep::dsl::alert_generation;
    type IdColumn = schema::rendezvous_alert_created::dsl::alert_id;
    type RequestTable = schema::fm_alert_request::table;
}

impl FmRendezvousResource for nexus_db_model::SupportBundle {
    type GenerationColumn = schema::fm_sitrep::dsl::support_bundle_generation;
    type IdColumn =
        schema::rendezvous_support_bundle_created::dsl::support_bundle_id;
    type RequestTable = schema::fm_support_bundle_request::table;
}

/// Test fixtures shared by the insert (`sitrep_guard`) and GC
/// (`fm_rendezvous_gc`) test suites: a synthetic [`FmRendezvousResource`] and
/// the throwaway schema its queries reference.
///
/// The dummy schema lets both suites exercise the generic, typed query paths
/// against a stand-in resource, decoupled from the specifics of any real
/// resource.
#[cfg(test)]
pub(crate) mod test_utils {
    use super::FmRendezvousResource;
    use crate::context::OpContext;
    use crate::db::DataStore;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use async_bb8_diesel::AsyncSimpleConnection;
    use chrono::Utc;
    use diesel::prelude::*;
    use iddqd::IdOrdMap;
    use nexus_db_lookup::DbConnection;
    use nexus_types::fm::Sitrep;
    use nexus_types::fm::SitrepMetadata;
    use omicron_common::api::external;
    use omicron_uuid_kinds::CollectionUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::SitrepUuid;
    use uuid::Uuid;

    // Schema for a dummy resource, its creation marker, and its request table.
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
    table! {
        test_schema.dummy_request (sitrep_id, id) {
            id -> Uuid,
            sitrep_id -> Uuid,
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
        type RequestTable = dummy_request::table;
    }

    /// Creates the tables the guarded-insert and GC SQL references but which the
    /// real schema doesn't have: the dummy resource, marker, and request tables,
    /// plus a `dummy_generation` column on the real `fm_sitrep` table.
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
             CREATE TABLE IF NOT EXISTS test_schema.dummy_request ( \
                 id UUID NOT NULL, \
                 sitrep_id UUID NOT NULL, \
                 PRIMARY KEY (sitrep_id, id) \
             ); \
             ALTER TABLE omicron.public.fm_sitrep \
                 ADD COLUMN IF NOT EXISTS dummy_generation INT8;",
        )
        .await
        .unwrap();
    }

    /// Inserts a current sitrep with a given `dummy_generation`, returning its
    /// ID so further sitreps can be chained onto it via `parent`.
    pub(crate) async fn insert_current_sitrep(
        datastore: &DataStore,
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        parent: Option<SitrepUuid>,
        generation: i64,
    ) -> SitrepUuid {
        let sitrep_id = SitrepUuid::new_v4();
        let sitrep = Sitrep {
            metadata: SitrepMetadata {
                id: sitrep_id,
                parent_sitrep_id: parent,
                inv_collection_id: CollectionUuid::new_v4(),
                next_inv_min_time_started: Utc::now(),
                creator_id: OmicronZoneUuid::new_v4(),
                comment: "rendezvous resource test sitrep".to_string(),
                time_created: Utc::now(),
                alert_generation: external::Generation::new(),
                support_bundle_generation: external::Generation::new(),
            },
            cases: IdOrdMap::new(),
            ereports_by_id: IdOrdMap::new(),
        };
        datastore.fm_sitrep_insert(opctx, sitrep, None).await.unwrap();

        // `SitrepMetadata` doesn't have a `dummy_generation` field, so we have
        // to update it manually.
        conn.batch_execute_async(&format!(
            "UPDATE omicron.public.fm_sitrep \
                 SET dummy_generation = {generation} WHERE id = '{sitrep_id}'"
        ))
        .await
        .unwrap();

        sitrep_id
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
