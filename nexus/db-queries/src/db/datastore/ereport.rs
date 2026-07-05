// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods for ereports.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::datastore::RunnableQuery;
use crate::db::model::Ereport;
use crate::db::model::EreporterRestart;
use crate::db::model::EreporterType;
use crate::db::model::SpMgsSlot;
use crate::db::model::SpType;
use crate::db::model::SqlU16;
use crate::db::model::ereport::DbEna;
use crate::db::pagination::{paginated, paginated_multicolumn};
use crate::db::raw_query_builder::QueryBuilder;
use crate::db::raw_query_builder::TypedSqlQuery;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::DateTime;
use chrono::Utc;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::sql_types;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_schema::schema::ereport::dsl;
use nexus_db_schema::schema::ereporter_restart::dsl as restart_dsl;
use nexus_types::fm::ereport as fm;
use nexus_types::fm::ereport::EreportFilters;
use nexus_types::fm::ereport::EreportId;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::EreporterRestartUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SitrepUuid;
use omicron_uuid_kinds::SledUuid;
use uuid::Uuid;

type EreportIdTuple = (Uuid, DbEna);

impl DataStore {
    /// Fetch an ereport by its restart ID and ENA.
    ///
    /// This function queries both the service-processor and host OS ereport
    /// tables, and returns a `NotFound` error if neither table contains an
    /// ereport with the requested ID.
    pub async fn ereport_fetch(
        &self,
        opctx: &OpContext,
        id: fm::EreportId,
    ) -> LookupResult<Ereport> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        self.ereport_fetch_on_conn(&conn, id).await
    }

    pub(crate) async fn ereport_fetch_on_conn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        id: fm::EreportId,
    ) -> LookupResult<Ereport> {
        let restart_id = id.restart_id.into_untyped_uuid();
        let ena = DbEna::from(id.ena);

        let ereport = dsl::ereport
            .filter(dsl::restart_id.eq(restart_id))
            .filter(dsl::ena.eq(ena))
            .filter(dsl::time_deleted.is_null())
            .select(Ereport::as_select())
            .first_async(conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .ok_or_else(|| {
                Error::non_resourcetype_not_found(format!("ereport {id}"))
            })?;
        Ok(ereport)
    }

    pub async fn ereport_fetch_matching(
        &self,
        opctx: &OpContext,
        filters: &EreportFilters,
        pagparams: &DataPageParams<'_, (Uuid, DbEna)>,
    ) -> ListResultVec<Ereport> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        let query = Self::ereport_fetch_matching_query(filters, pagparams);
        query
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    fn ereport_fetch_matching_query(
        filters: &EreportFilters,
        pagparams: &DataPageParams<'_, (Uuid, DbEna)>,
    ) -> impl RunnableQuery<Ereport> + use<> {
        let mut query = paginated_multicolumn(
            dsl::ereport,
            (dsl::restart_id, dsl::ena),
            pagparams,
        )
        .filter(dsl::time_deleted.is_null())
        .select(Ereport::as_select());

        if let Some(start) = filters.start_time() {
            query = query.filter(dsl::time_collected.ge(start));
        }

        if let Some(end) = filters.end_time() {
            query = query.filter(dsl::time_collected.le(end));
        }

        if !filters.only_serials().is_empty() {
            query = query.filter(
                dsl::serial_number.eq_any(filters.only_serials().to_vec()),
            );
        }

        if !filters.only_classes().is_empty() {
            query = query
                .filter(dsl::class.eq_any(filters.only_classes().to_vec()));
        }

        query
    }

    /// List ereports from the reporter with the given restart ID.
    pub async fn ereport_list_by_restart(
        &self,
        opctx: &OpContext,
        restart_id: EreporterRestartUuid,
        pagparams: &DataPageParams<'_, DbEna>,
    ) -> ListResultVec<Ereport> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        paginated(dsl::ereport, dsl::ena, pagparams)
            .filter(dsl::restart_id.eq(restart_id.into_untyped_uuid()))
            .filter(dsl::time_deleted.is_null())
            .select(Ereport::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Lists ereporter restarts, paginated by restart ID.
    pub async fn ereporter_restart_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<EreporterRestart> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        paginated(restart_dsl::ereporter_restart, restart_dsl::id, pagparams)
            .select(EreporterRestart::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn latest_ereport_id(
        &self,
        opctx: &OpContext,
        reporter: fm::Reporter,
    ) -> Result<Option<EreportId>, Error> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let result = match reporter {
            fm::Reporter::Sp { sp_type, slot } => {
                let sp_type = sp_type.into();
                let slot = SpMgsSlot::from(SqlU16::new(slot));
                Self::sp_latest_ereport_id_query(sp_type, slot)
                    .get_result_async(&*conn)
                    .await
            }
            fm::Reporter::HostOs { sled, .. } => {
                Self::host_latest_ereport_id_query(sled)
                    .get_result_async(&*conn)
                    .await
            }
        };
        let id = result
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .map(|(restart_id, DbEna(ena))| EreportId {
                restart_id: EreporterRestartUuid::from_untyped_uuid(restart_id),
                ena,
            });
        Ok(id)
    }

    fn sp_latest_ereport_id_query(
        sp_type: SpType,
        slot: SpMgsSlot,
    ) -> impl RunnableQuery<EreportIdTuple> {
        dsl::ereport
            .filter(
                dsl::slot_type
                    .eq(sp_type)
                    .and(dsl::slot.eq(slot))
                    .and(dsl::time_deleted.is_null()),
            )
            .order_by((dsl::time_collected.desc(), dsl::ena.desc()))
            .limit(1)
            .select((dsl::restart_id, dsl::ena))
    }

    fn host_latest_ereport_id_query(
        sled_id: SledUuid,
    ) -> impl RunnableQuery<EreportIdTuple> {
        dsl::ereport
            .filter(
                dsl::sled_id
                    .eq(sled_id.into_untyped_uuid())
                    .and(dsl::time_deleted.is_null()),
            )
            .order_by((dsl::time_collected.desc(), dsl::ena.desc()))
            .limit(1)
            .select((dsl::restart_id, dsl::ena))
    }

    async fn latest_ena_for_restart_on_conn(
        &self,
        restart_id: EreporterRestartUuid,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<Option<EreportId>, Error> {
        let ena = dsl::ereport
            .filter(dsl::restart_id.eq(restart_id.into_untyped_uuid()))
            .order_by(dsl::ena.desc())
            .limit(1)
            .select(dsl::ena)
            .first_async(conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(ena.map(|DbEna(ena)| EreportId { restart_id, ena }))
    }

    /// Inserts the provided tranche of `ereports` into the `ereport` table, and
    /// potentially updates the `ereporter_restart` table if the restart ID of
    /// the inserted ereports has not been seen before.
    ///
    /// # Returns
    ///
    /// This function returns a tuple containing the number of new `ereport`
    /// rows that were created, along with the newest [`EreportId`] for the same
    /// reporter index as the inserted ereports.
    ///
    /// The returned ereport ID is intended to provide the caller with the
    /// latest ENA to use in a subsequent request to ingest ereports from the
    /// same reporter. In some cases, it may:
    ///
    /// - be a newer ENA than the highest ENA in the inserted ereports, if
    ///   additional ereports were inserted concurrently
    /// - have a different restart ID than the one provided, if ereports from
    ///   a newer restart of that reporter were inserted concurrently
    // Since all the arguments to this function are newtypes with pretty clear
    // meanings (e.g. `rack_id`, `restart_id`, and `collector_id` are all
    // different typed UUIDs), I'm not convinced that factoring stuff out into a
    // struct like `EreportTrancheMetadata` or whatever would actually make this
    // any clearer --- it feels like just adding noise to me. So ignore the
    // warning.
    #[allow(clippy::too_many_arguments)]
    pub async fn ereports_insert(
        &self,
        opctx: &OpContext,
        restart_id: EreporterRestartUuid,
        time_collected: DateTime<Utc>,
        collector_id: OmicronZoneUuid,
        rack_id: RackUuid,
        reporter: fm::Reporter,
        ereports: impl IntoIterator<Item = (fm::Ena, fm::EreportData)>,
    ) -> CreateResult<(usize, Option<EreportId>)> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        let ereports = ereports
            .into_iter()
            .map(|(ena, data)| {
                let id = EreportId { restart_id, ena };
                Ereport::new(id, time_collected, collector_id, data, reporter)
            })
            .collect::<Vec<_>>();
        let n_ereports = ereports.len();
        let created = Self::ereports_insert_query(
            restart_id,
            time_collected,
            rack_id,
            reporter,
            ereports,
        )
        .execute_async(&*conn)
        .await
        .map_err(|e| {
            public_error_from_diesel(e, ErrorHandler::Server).internal_context(
                format!(
                    "failed to insert {n_ereports} ereports from {reporter} \
                     (restart {restart_id})",
                ),
            )
        })?;
        let latest = self
            .latest_ena_for_restart_on_conn(restart_id, &conn)
            .await
            .map_err(|e| {
                e.internal_context(format!(
                    "failed to get latest ENA for restart {restart_id}"
                ))
            })?;
        Ok((created, latest))
    }

    /// Returns a query for inserting ereports into the database and updating
    /// the restart history table if the restart ID of the provided ereports is
    /// not already present.
    ///
    /// The two operations of inserting the ereports into the `ereport` table
    /// and inserting an entry into the `ereporter_restart` table for the
    /// restart ID are performed in a single atomic CTE. It's necessary for
    /// these two operations to be done atomically, because if they are
    /// non-atomic, it is possible for a Nexus to die, or go out to lunch for a
    /// bit, between inserting the ereport records and creating the
    /// `ereporter_restart` record for a tranche of ereports from a not-yet-seen
    /// restart ID. If this happens, another Nexus could insert some more
    /// ereports from that restart ID, and try to put something in the restart
    /// ID table with a timestamp that is later than the actual first time
    /// ereports from that ID were observed.
    ///
    /// The SQL generated here is output to
    /// `tests/output/ereports_insert_host.sql` and
    /// `tests/output/ereports_insert_sp.sql`, for host-OS and SP ereports,
    /// respectively.
    fn ereports_insert_query(
        restart_id: EreporterRestartUuid,
        time_collected: chrono::DateTime<chrono::Utc>,
        rack_id: RackUuid,
        reporter: fm::Reporter,
        ereports: Vec<Ereport>,
    ) -> impl RunnableQuery<i64> {
        /// This is basically just a big pile of ceremony for combining two
        /// little Diesel queries into a CTE...
        struct EreportInsertQuery<IE, IR> {
            insert_ereports: IE,
            insert_reporter: IR,
            slot: Option<SqlU16>,
        }

        impl<IE, IR> QueryId for EreportInsertQuery<IE, IR> {
            type QueryId = ();
            const HAS_STATIC_QUERY_ID: bool = false;
        }

        impl<IE, IR> Query for EreportInsertQuery<IE, IR> {
            type SqlType = sql_types::BigInt;
        }

        impl<IE, IR> diesel::RunQueryDsl<DbConnection> for EreportInsertQuery<IE, IR> {}

        impl<IE, IR> QueryFragment<Pg> for EreportInsertQuery<IE, IR>
        where
            IE: QueryFragment<Pg>,
            IR: QueryFragment<Pg>,
        {
            fn walk_ast<'b>(
                &'b self,
                mut out: AstPass<'_, 'b, Pg>,
            ) -> QueryResult<()> {
                out.push_sql("WITH inserted_ereports AS ( ");
                self.insert_ereports.walk_ast(out.reborrow())?;

                out.push_sql("), inserted_reporter AS (");
                self.insert_reporter.walk_ast(out.reborrow())?;
                out.push_sql(" ON CONFLICT (id) DO ");
                // If we have a slot number, update it so that a previously-null
                // slot number is filled in; if we do not, do nothing on
                // conflict so a previously non-NULL slot is not clobbered.
                if let Some(ref slot) = self.slot {
                    out.push_sql("UPDATE SET \"slot\" = ");
                    out.push_bind_param::<sql_types::Int4, _>(slot)?;
                } else {
                    out.push_sql("NOTHING");
                }
                // We don't actually need this, but `WITH` clauses have to
                // return something, sooo....
                out.push_sql(" RETURNING id) ");
                out.push_sql("SELECT count(*) FROM inserted_ereports");
                Ok(())
            }
        }

        let (reporter, slot_type, slot) = match reporter {
            fm::Reporter::HostOs { slot, .. } => {
                (EreporterType::Host, SpType::Sled, slot.map(SqlU16::from))
            }
            fm::Reporter::Sp { sp_type, slot } => {
                let sp_type = sp_type.into();
                let slot = SqlU16::from(slot);
                (EreporterType::Sp, sp_type, Some(slot))
            }
        };

        // The query fragment to insert ereports into the `ereport` table.
        let insert_ereports = diesel::insert_into(dsl::ereport)
            .values(ereports)
            // Some or all of the ereports collected in this batch may already
            // exist in the database because they were ingested by another
            // Nexus. If the same ENAs exist for this restart ID, that's fine;
            // don't overwrite them.
            .on_conflict((dsl::restart_id, dsl::ena))
            .do_nothing()
            .returning(dsl::ena);

        // Query fragment to insert the reporter restart entry into the
        // ereporter restart table, or update the existing entry's slot column
        // if one exists and the slot column is null. The null behavior will be
        // added by the `walk_ast()` method on `EreporterInsertQuery`, because
        // it depends on whether or not there is a slot number to insert, and I
        // couldn't figure out how to get diesel to let me type erase an INSERT
        // statement that may have one of multiple ON CONFLICT clauses...
        let insert_reporter = diesel::insert_into(
            restart_dsl::ereporter_restart,
        )
        .values(crate::db::model::EreporterRestart {
            id: restart_id.into(),
            time_first_seen: time_collected,
            reporter,
            slot_type,
            slot: slot.map(SpMgsSlot::from),
            rack_id: rack_id.into(),
        });
        EreportInsertQuery { insert_ereports, insert_reporter, slot }
    }

    /// Lists ereports which have not been marked as **definitely seen**
    /// (included in a committed sitrep) in the database, restricted to
    /// ereports whose `class` is one of `classes`, paginated by the reporter
    /// restart ID and ENA.
    ///
    /// Note that this filters based only on whether they have been marked in
    /// the database. Because marking seen ereports occurs asynchronously from
    /// committing sitreps as part of FM rendezvous, ereports returned by this
    /// query may have already been seen. These ereports must be filtered out at
    /// a higher level based on the contents of the current sitrep when
    /// determining which ereports are *actually* new.
    ///
    /// Ereports with `class IS NULL` are intentionally never returned: the
    /// SQL filter is `class = ANY($1::text[])`, which never matches NULL.
    /// Callers (e.g. fm_analysis preparation) deliberately key off
    /// `nexus_fm::diagnosis::known_ereport_classes` so that the loader
    /// only surfaces ereports that FM analysis can consume; see that
    /// function's documentation for the policy and rationale.
    pub async fn ereports_list_unmarked(
        &self,
        opctx: &OpContext,
        classes: &[&str],
        pagparams: &DataPageParams<'_, (Uuid, DbEna)>,
    ) -> ListResultVec<Ereport> {
        // TODO(eliza): ereports should probably have their own resource type someday...
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        // An empty class set means no diagnosis engine consumes any ereport
        // class. There's no value in paging through CRDB to find out
        // there's nothing to load.
        if classes.is_empty() {
            return Ok(Vec::new());
        }
        Self::ereports_list_unmarked_query(classes, pagparams)
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Diesel query that counts ereports per class across the entire (non
    /// soft-deleted) ereport table.
    ///
    /// Backed by the `lookup_ereports_by_class` partial index (predicate
    /// `time_deleted IS NULL`). The query is a *full index scan* over that
    /// partial index — bounded by the predicate — but never a full table
    /// scan of the primary key. The `explain_ereport_class_totals_query`
    /// test asserts this.
    pub fn ereport_class_totals_query()
    -> impl RunnableQuery<(Option<String>, i64)> + use<> {
        use diesel::dsl::count_star;
        dsl::ereport
            .group_by(dsl::class)
            .filter(dsl::time_deleted.is_null())
            .select((dsl::class, count_star()))
    }

    /// Diesel query that counts ereports per class, restricted to ereports
    /// that have not yet been marked as seen in any committed sitrep.
    ///
    /// Backed by the `lookup_unmarked_ereports_by_class` partial index
    /// (predicate `marked_seen_in IS NULL AND time_deleted IS NULL`).
    /// Bounded by the partial-index predicate; never a full table scan.
    /// The `explain_ereport_unmarked_class_totals_query` test asserts this.
    pub fn ereport_unmarked_class_totals_query()
    -> impl RunnableQuery<(Option<String>, i64)> + use<> {
        use diesel::dsl::count_star;
        dsl::ereport
            .group_by(dsl::class)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::marked_seen_in.is_null())
            .select((dsl::class, count_star()))
    }

    fn ereports_list_unmarked_query(
        classes: &[&str],
        pagparams: &DataPageParams<'_, (Uuid, DbEna)>,
    ) -> impl RunnableQuery<Ereport> + use<> {
        // NULL-class ereports are intentionally excluded: `class = ANY(...)`
        // never matches NULL. See `nexus_fm::diagnosis::known_ereport_classes`
        // for the policy.
        let classes: Vec<String> =
            classes.iter().map(|c| (*c).to_string()).collect();
        paginated_multicolumn(
            dsl::ereport,
            (dsl::restart_id, dsl::ena),
            pagparams,
        )
        .filter(dsl::marked_seen_in.is_null())
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::class.eq_any(classes))
        .select(Ereport::as_select())
    }

    /// Mark the ereports with the given ereport IDs as having definitely been
    /// processed as of the provided sitrep ID.
    ///
    /// If any ereports have already been marked as seen with another sitrep ID,
    /// they are unmodified. Otherwise, this query sets the `marked_seen_in`
    /// column to the provided sitrep ID.
    ///
    /// Returns the number of rows updated, which may be less than the number of
    /// ereport IDs provided if some were already marked as seen.
    pub async fn ereports_mark_seen(
        &self,
        opctx: &OpContext,
        sitrep_id: SitrepUuid,
        ereport_ids: impl IntoIterator<Item = EreportId>,
    ) -> Result<usize, Error> {
        // TODO(eliza): ereprots should probably be an authz resource someday, i
        // guess...
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        Self::ereports_mark_seen_query(sitrep_id, ereport_ids)
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    fn ereports_mark_seen_query(
        sitrep_id: SitrepUuid,
        ereport_ids: impl IntoIterator<Item = EreportId>,
    ) -> TypedSqlQuery<sql_types::BigInt> {
        // Untuple the ereport IDs into separate vecs of `restart_id`s and
        // `ena`s, which we will pass as separate bind parameters in the SQL
        // query and then re-tuple back into one array of pairs using `unnest`
        // when the query is executed.
        //
        // This bit is kindas screwy: unfortunately, Postgres serialization
        // does not support bind parameters which are arrays of tuples, so
        // we must bind two separate arrays. Trust me on this one.
        let mut restart_ids = Vec::new();
        let mut enas = Vec::new();
        for EreportId { restart_id, ena } in ereport_ids {
            restart_ids.push(restart_id.into_untyped_uuid());
            enas.push(DbEna::from(ena));
        }

        // Raw SQL is necessary here as `diesel`'s `.eq_any` doesn't work with
        // arrays of tuples (likely due to the weird `unnest` thing being
        // required to serialize the bind parameter properly).
        //
        // The SQL generated here is output to
        // `tests/output/ereports_mark_seen.sql`
        let mut builder = QueryBuilder::new();
        builder
            .sql(
                "UPDATE omicron.public.ereport \
                 SET marked_seen_in = ",
            )
            .param()
            .bind::<sql_types::Uuid, _>(sitrep_id.into_untyped_uuid())
            .sql(
                " WHERE (restart_id, ena) IN (\
                    SELECT unnest (",
            )
            // Pretend it's just one array please?
            .param()
            .bind::<sql_types::Array<sql_types::Uuid>, _>(restart_ids)
            .sql("), unnest (")
            .param()
            .bind::<sql_types::Array<sql_types::BigInt>, _>(enas)
            // Idempotency bit...
            .sql("))  AND marked_seen_in IS NULL");
        builder.query::<sql_types::BigInt>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::explain::ExplainableAsync;
    use crate::db::pagination::Paginator;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::pub_test_utils::explain::assert_uses_partial_index_only;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use dropshot::PaginationOrder;
    use ereport_types::Ena;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::RackUuid;
    use std::collections::BTreeMap;
    use std::num::NonZeroU32;
    use std::time::Duration;

    #[tokio::test]
    async fn explain_sp_latest_ereport_id() {
        let logctx = dev::test_setup_log("explain_sp_latest_ereport_id");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let query = DataStore::sp_latest_ereport_id_query(
            SpType::Sled,
            SpMgsSlot::from(SqlU16::new(1)),
        );
        let explanation = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        eprintln!("{explanation}");

        assert!(
            !explanation.contains("FULL SCAN"),
            "Found an unexpected FULL SCAN: {}",
            explanation
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn explain_host_latest_ereport_id() {
        let logctx = dev::test_setup_log("explain_host_latest_ereport_id");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let query = DataStore::host_latest_ereport_id_query(SledUuid::nil());
        let explanation = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        eprintln!("{explanation}");

        assert!(
            !explanation.contains("FULL SCAN"),
            "Found an unexpected FULL SCAN: {}",
            explanation
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn explain_ereport_fetch_matching_default() {
        explain_fetch_matching_query(
            "explain_ereport_fetch_matching_default",
            EreportFilters::default(),
        )
        .await
    }

    #[tokio::test]
    async fn explain_ereport_fetch_matching_only_serials() {
        explain_fetch_matching_query(
            "explain_ereport_fetch_matching_only_serials",
            EreportFilters::new().with_serials(["BRM6900420", "BRM5555555"]),
        )
        .await
    }

    #[tokio::test]
    async fn explain_ereport_fetch_matching_serials_and_classes() {
        explain_fetch_matching_query(
            "explain_ereport_fetch_matching_serials_and_classes",
            EreportFilters::new()
                .with_serials(["BRM6900420", "BRM5555555"])
                .with_classes([
                    "my.cool.ereport",
                    "hw.frobulator.fault.frobulation_failed",
                ]),
        )
        .await
    }

    #[tokio::test]
    async fn explain_ereport_fetch_matching_only_time() {
        explain_fetch_matching_query(
            "explain_ereport_fetch_matching_only_time",
            EreportFilters::new()
                .with_end_time(chrono::Utc::now())
                .expect("no start time set"),
        )
        .await
    }

    #[tokio::test]
    async fn explain_ereport_fetch_matching_time_and_serials() {
        explain_fetch_matching_query(
            "explain_ereport_fetch_matching_only_time",
            EreportFilters::new()
                .with_serials(["BRM6900420", "BRM5555555"])
                .with_end_time(chrono::Utc::now())
                .expect("no start time set"),
        )
        .await
    }

    async fn explain_fetch_matching_query(
        test_name: &str,
        filters: EreportFilters,
    ) {
        let logctx = dev::test_setup_log(test_name);
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let pagparams = DataPageParams {
            marker: None,
            direction: PaginationOrder::Ascending,
            limit: NonZeroU32::new(100).unwrap(),
        };
        eprintln!("--- filters: {filters:#?}\n");

        let query =
            DataStore::ereport_fetch_matching_query(&filters, &pagparams);

        let explanation = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        eprintln!("--- explanation: {explanation}");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn expectorate_ereports_mark_seen() {
        let query = DataStore::ereports_mark_seen_query(
            SitrepUuid::new_v4(),
            vec![
                EreportId {
                    restart_id: EreporterRestartUuid::new_v4(),
                    ena: fm::Ena(2),
                },
                EreportId {
                    restart_id: EreporterRestartUuid::new_v4(),
                    ena: fm::Ena(3),
                },
            ],
        );
        expectorate_query_contents(
            &query,
            "tests/output/ereports_mark_seen.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn explain_ereports_mark_seen_query() {
        let logctx = dev::test_setup_log("explain_ereports_mark_seen_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let query = DataStore::ereports_mark_seen_query(
            SitrepUuid::new_v4(),
            vec![
                EreportId {
                    restart_id: EreporterRestartUuid::new_v4(),
                    ena: fm::Ena(2),
                },
                EreportId {
                    restart_id: EreporterRestartUuid::new_v4(),
                    ena: fm::Ena(3),
                },
            ],
        );

        let explanation = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        eprintln!("{explanation}");
        assert!(
            !explanation.contains("FULL SCAN"),
            "Found an unexpected FULL SCAN: {}",
            explanation
        );

        // Okay, what happens if we don't actually ask for any ereports to be
        // marked?
        eprintln!(" --- empty vec time ---");
        let query =
            DataStore::ereports_mark_seen_query(SitrepUuid::new_v4(), vec![]);
        let explanation = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        eprintln!("{explanation}");
        assert!(
            !explanation.contains("FULL SCAN"),
            "Found an unexpected FULL SCAN: {}",
            explanation
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn expectorate_ereports_list_unmarked() {
        let pagparams = DataPageParams {
            marker: None,
            direction: PaginationOrder::Ascending,
            limit: NonZeroU32::new(100).unwrap(),
        };
        let classes: &[&str] = &[
            "ereport.positronic-brain.example",
            "ereport.spinning-blades.example",
        ];
        let query =
            DataStore::ereports_list_unmarked_query(classes, &pagparams);
        expectorate_query_contents(
            &query,
            "tests/output/ereports_list_unmarked.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn explain_ereports_list_unmarked_query() {
        let logctx =
            dev::test_setup_log("explain_ereports_list_unmarked_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let pagparams = DataPageParams {
            marker: None,
            direction: PaginationOrder::Ascending,
            limit: NonZeroU32::new(100).unwrap(),
        };
        let classes: &[&str] = &[
            "ereport.positronic-brain.example",
            "ereport.spinning-blades.example",
        ];
        let query =
            DataStore::ereports_list_unmarked_query(classes, &pagparams);
        let explanation = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        assert_uses_partial_index_only(
            &explanation,
            "lookup_unmarked_ereports_by_class",
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn explain_ereport_class_totals_query() {
        let logctx = dev::test_setup_log("explain_ereport_class_totals_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let query = DataStore::ereport_class_totals_query();
        let explanation = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        assert_uses_partial_index_only(
            &explanation,
            "lookup_ereports_by_class",
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn explain_ereport_unmarked_class_totals_query() {
        let logctx =
            dev::test_setup_log("explain_ereport_unmarked_class_totals_query");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let query = DataStore::ereport_unmarked_class_totals_query();
        let explanation = query
            .explain_async(&conn)
            .await
            .expect("Failed to explain query - is it valid SQL?");

        assert_uses_partial_index_only(
            &explanation,
            "lookup_unmarked_ereports_by_class",
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn expectorate_ereports_insert_sp() {
        let restart_id = EreporterRestartUuid::nil();
        let collector_id = OmicronZoneUuid::nil();
        let rack_id = RackUuid::nil();
        let reporter =
            fm::Reporter::Sp { sp_type: SpType::Sled.into(), slot: 16 };
        let query = DataStore::ereports_insert_query(
            restart_id,
            DateTime::<Utc>::MIN_UTC,
            rack_id,
            reporter,
            vec![
                Ereport {
                    restart_id: restart_id.into(),
                    ena: Ena(2).into(),
                    time_collected: DateTime::<Utc>::MIN_UTC,
                    collector_id: collector_id.into(),
                    part_number: Some("my cool CPN".to_string()),
                    serial_number: Some("my cool serial".to_string()),
                    class: Some("my cool ereport".to_string()),
                    report: serde_json::json!({}),
                    marked_seen_in: None,
                    time_deleted: None,
                    reporter: reporter.into(),
                },
                Ereport {
                    restart_id: restart_id.into(),
                    ena: Ena(3).into(),
                    time_collected: DateTime::<Utc>::MIN_UTC,
                    collector_id: collector_id.into(),
                    part_number: Some("my cool CPN".to_string()),
                    serial_number: Some("my cool serial".to_string()),
                    class: Some("my other ereport".to_string()),
                    report: serde_json::json!({}),
                    marked_seen_in: None,
                    time_deleted: None,
                    reporter: reporter.into(),
                },
            ],
        );
        expectorate_query_contents(
            &query,
            "tests/output/ereports_insert_sp.sql",
        )
        .await;
    }

    #[tokio::test]
    async fn expectorate_ereports_insert_host() {
        let restart_id = EreporterRestartUuid::nil();
        let collector_id = OmicronZoneUuid::nil();
        let rack_id = RackUuid::nil();
        let reporter =
            fm::Reporter::HostOs { slot: Some(16), sled: SledUuid::nil() };
        let query = DataStore::ereports_insert_query(
            restart_id,
            DateTime::<Utc>::MIN_UTC,
            rack_id,
            reporter,
            vec![
                Ereport {
                    restart_id: restart_id.into(),
                    ena: Ena(2).into(),
                    time_collected: DateTime::<Utc>::MIN_UTC,
                    collector_id: collector_id.into(),
                    part_number: Some("my cool CPN".to_string()),
                    serial_number: Some("my cool serial".to_string()),
                    class: Some("my cool ereport".to_string()),
                    report: serde_json::json!({}),
                    marked_seen_in: None,
                    time_deleted: None,
                    reporter: reporter.into(),
                },
                Ereport {
                    restart_id: restart_id.into(),
                    ena: Ena(3).into(),
                    time_collected: DateTime::<Utc>::MIN_UTC,
                    collector_id: collector_id.into(),
                    part_number: Some("my cool CPN".to_string()),
                    serial_number: Some("my cool serial".to_string()),
                    class: Some("my other ereport".to_string()),
                    report: serde_json::json!({}),
                    marked_seen_in: None,
                    time_deleted: None,
                    reporter: reporter.into(),
                },
            ],
        );
        expectorate_query_contents(
            &query,
            "tests/output/ereports_insert_host.sql",
        )
        .await;
    }

    // This test tests that the `ereport_fetch_matching` queries succeed with
    // filters that only select ereports over a time range, and the default (no
    // filters).
    //
    // We test these cases because we are concerned that they may fail due to
    // performing a full-table scan. Unfortunately, we cannot easily check this
    // using an `EXPLAIN` test, as these queries may perform a FULL SCAN over an
    // *index*, which is permissable. It's annoying to properly parse the
    // EXPLAIN output to determine if a FULL SCAN is performed over an index or
    // a full table, so rather than asserting they don't do a full scan that
    // way, we just perform a query and make sure it returns something.
    #[tokio::test]
    async fn test_ereport_fetch_matching() {
        let logctx = dev::test_setup_log("test_ereport_fetch_matching");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let restart_id = EreporterRestartUuid::new_v4();
        let id = fm::EreportId { restart_id, ena: ereport_types::Ena(2) };
        let time_collected = Utc::now();
        let collector_id = OmicronZoneUuid::new_v4();
        let rack_id = RackUuid::new_v4();
        let ereport = fm::EreportData {
            part_number: Some("my cool CPN".to_string()),
            serial_number: Some("my cool serial".to_string()),
            class: Some("my cool ereport".to_string()),
            report: serde_json::json!({}),
        };
        datastore
            .ereports_insert(
                &opctx,
                restart_id,
                time_collected,
                collector_id,
                rack_id,
                fm::Reporter::Sp {
                    sp_type: nexus_types::inventory::SpType::Sled,
                    slot: 19,
                },
                vec![(id.ena, ereport.clone())],
            )
            .await
            .expect("insert should succeed");

        #[track_caller]
        fn check_results(
            found_ereports: Vec<Ereport>,
            expected_id: &fm::EreportId,
            collector_id: OmicronZoneUuid,
            expected: &fm::EreportData,
        ) {
            assert_eq!(found_ereports.len(), 1);
            assert_eq!(&found_ereports[0].id(), expected_id);
            assert_eq!(found_ereports[0].collector_id, collector_id.into());
            assert_eq!(&found_ereports[0].part_number, &expected.part_number);
            assert_eq!(
                &found_ereports[0].serial_number,
                &expected.serial_number
            );
            assert_eq!(&found_ereports[0].class, &expected.class);
            assert_eq!(&found_ereports[0].report, &expected.report);
        }

        let pagparams = DataPageParams {
            marker: None,
            direction: PaginationOrder::Ascending,
            limit: NonZeroU32::new(100).unwrap(),
        };

        let found_default = datastore
            .ereport_fetch_matching(opctx, &Default::default(), &pagparams)
            .await
            .expect("fetch matching with default filters should succeed");
        check_results(dbg!(found_default), &id, collector_id, &ereport);

        let found_by_time_range = datastore
            .ereport_fetch_matching(
                opctx,
                &EreportFilters::new()
                    .with_start_time(time_collected - Duration::from_secs(600))
                    .expect("no end time set"),
                &pagparams,
            )
            .await
            .expect("fetch matching with time range filters should succeed");
        check_results(dbg!(found_by_time_range), &id, collector_id, &ereport);

        let found_by_serial = datastore
            .ereport_fetch_matching(
                opctx,
                &EreportFilters::new().with_serials(["my cool serial"]),
                &pagparams,
            )
            .await
            .expect("fetch matching with serial number filters should succeed");
        check_results(dbg!(found_by_serial), &id, collector_id, &ereport);

        let found_by_class = datastore
            .ereport_fetch_matching(
                opctx,
                &EreportFilters::new().with_classes(["my cool ereport"]),
                &pagparams,
            )
            .await
            .expect("fetch matching with class filters should succeed");
        check_results(dbg!(found_by_class), &id, collector_id, &ereport);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// `ereports_list_unmarked` filters by class. Verifies:
    /// - Only ereports with `class IN (filter)` are returned.
    /// - Ereports with NULL class are never returned, even when no class
    ///   filter is specifically targeting them.
    /// - An empty `classes` slice returns empty without a DB roundtrip.
    #[tokio::test]
    async fn test_ereports_list_unmarked_class_filter() {
        let logctx =
            dev::test_setup_log("test_ereports_list_unmarked_class_filter");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Insert three ereports under one reporter: classes "alpha", "beta",
        // and NULL.
        let restart_id = EreporterRestartUuid::new_v4();
        let collector_id = OmicronZoneUuid::new_v4();
        let rack_id = RackUuid::new_v4();
        let make = |ena: u64, class: Option<&str>| {
            (
                ereport_types::Ena(ena),
                fm::EreportData {
                    part_number: Some("CPN".to_string()),
                    serial_number: Some("SN".to_string()),
                    class: class.map(str::to_string),
                    report: serde_json::json!({}),
                },
            )
        };
        datastore
            .ereports_insert(
                &opctx,
                restart_id,
                Utc::now(),
                collector_id,
                rack_id,
                fm::Reporter::Sp {
                    sp_type: nexus_types::inventory::SpType::Sled,
                    slot: 0,
                },
                vec![
                    make(1, Some("alpha")),
                    make(2, Some("beta")),
                    make(3, None),
                ],
            )
            .await
            .expect("insert should succeed");

        let pagparams = DataPageParams {
            marker: None,
            direction: PaginationOrder::Ascending,
            limit: NonZeroU32::new(100).unwrap(),
        };

        // Filter for "alpha" only — should return only ENA 1.
        let alpha_only = datastore
            .ereports_list_unmarked(opctx, &["alpha"], &pagparams)
            .await
            .expect("alpha-only query should succeed");
        let enas: Vec<u64> = alpha_only.iter().map(|e| e.ena.0.0).collect();
        assert_eq!(enas, vec![1], "alpha-only should match only ENA 1");

        // Filter for "alpha" + "beta" — should return ENAs 1 and 2 but
        // NEVER the NULL-class ereport (ENA 3).
        let alpha_beta = datastore
            .ereports_list_unmarked(opctx, &["alpha", "beta"], &pagparams)
            .await
            .expect("alpha+beta query should succeed");
        let mut enas: Vec<u64> = alpha_beta.iter().map(|e| e.ena.0.0).collect();
        enas.sort();
        assert_eq!(
            enas,
            vec![1, 2],
            "alpha+beta should match ENAs 1 and 2; NULL-class ENA 3 must \
             be excluded by the class filter"
        );

        // Filter for a class that doesn't exist — should return empty.
        let nope = datastore
            .ereports_list_unmarked(opctx, &["nonexistent.class"], &pagparams)
            .await
            .expect("nonexistent-class query should succeed");
        assert!(nope.is_empty(), "nonexistent class should match nothing");

        // Empty classes — should return empty without a DB roundtrip.
        let empty = datastore
            .ereports_list_unmarked(opctx, &[], &pagparams)
            .await
            .expect("empty-classes query should succeed");
        assert!(
            empty.is_empty(),
            "empty classes list must short-circuit to no results"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    async fn fetch_restarts(
        datastore: &DataStore,
        opctx: &OpContext,
    ) -> BTreeMap<EreporterRestartUuid, EreporterRestart> {
        // use a very small batch size in tests so that we are also exercising
        // pagination.
        const BATCH_SIZE: NonZeroU32 = match NonZeroU32::new(2) {
            Some(b) => b,
            None => panic!("2 is nonzero"),
        };

        let mut restarts = BTreeMap::new();
        let mut paginator =
            Paginator::new(BATCH_SIZE, dropshot::PaginationOrder::Ascending);
        while let Some(p) = paginator.next() {
            let batch = datastore
                .ereporter_restart_list(opctx, &p.current_pagparams())
                .await
                .expect("listing ereporter restarts should succeed");
            paginator =
                p.found_batch(&batch[..], &|r| r.id.into_untyped_uuid());
            for r in batch {
                let id = r.id.into();
                if let Some(_) = restarts.insert(id, r) {
                    unreachable!(
                        "duplicate ereporter restart ID {id}; this should be \
                         impossible as it is the primary key"
                    )
                }
            }
        }

        restarts
    }

    #[tokio::test]
    async fn test_ereporter_restarts() {
        fn mk_ereport(ena: u64) -> (fm::Ena, fm::EreportData) {
            (
                Ena(ena),
                fm::EreportData {
                    part_number: None,
                    serial_number: None,
                    class: None,
                    report: serde_json::json!({}),
                },
            )
        }

        let logctx = dev::test_setup_log("test_ereporter_restarts");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let collector_id = OmicronZoneUuid::new_v4();
        let rack_id = RackUuid::new_v4();
        let t0 = Utc::now();

        let timestamp = move |secs: usize| -> DateTime<Utc> {
            let t1 = omicron_common::timestamp_db_precision(t0);
            let t2 = t1 + Duration::from_secs(secs as u64);
            let t2 = omicron_common::timestamp_db_precision(t2);
            assert_ne!(
                t1, t2,
                "two timestamps intended to be distinct would be equal after \
                truncating to database precision (advanced by {secs} seconds)"
            );
            t2
        };
        // For host OS reporters, the sled's physical location may not be known
        // immediately, so we may insert ereports with a NULL slot number. We
        // wish to ensure that, if subsequent ereports are inserted for the same
        // restart ID with a non-NULL slot number, the previous slot number is
        // updated to the new value, but the first seen timestamp is not
        // changed.
        let host0_restart_id = EreporterRestartUuid::new_v4();
        let host0_first_seen = timestamp(100);
        let host0_slot = 7;
        let sled = SledUuid::new_v4();
        datastore
            .ereports_insert(
                opctx,
                host0_restart_id,
                host0_first_seen,
                collector_id,
                rack_id,
                fm::Reporter::HostOs { sled, slot: None },
                vec![mk_ereport(1)],
            )
            .await
            .unwrap();
        {
            let time_collected = timestamp(200);
            datastore
                .ereports_insert(
                    opctx,
                    host0_restart_id,
                    time_collected,
                    collector_id,
                    rack_id,
                    fm::Reporter::HostOs { sled, slot: Some(host0_slot) },
                    vec![mk_ereport(2), mk_ereport(3)],
                )
                .await
                .unwrap();
        }

        // Let's also test that if we insert ereports from a reporter with a
        // non-NULL slot number, and then insert ereports from the same reporter
        // with a NULL slot number, the original slot number is preserved. This
        // shouldn't happen in practice, but we should make sure the behavior
        // here is correct anyway.
        let host1_restart_id = EreporterRestartUuid::new_v4();
        let host1_first_seen = timestamp(150);
        let host1_slot = 3;
        let sled = SledUuid::new_v4();
        datastore
            .ereports_insert(
                opctx,
                host1_restart_id,
                host1_first_seen,
                collector_id,
                rack_id,
                fm::Reporter::HostOs { sled, slot: Some(host1_slot) },
                vec![mk_ereport(1), mk_ereport(2)],
            )
            .await
            .unwrap();
        datastore
            .ereports_insert(
                opctx,
                host1_restart_id,
                timestamp(250),
                collector_id,
                rack_id,
                fm::Reporter::HostOs { sled, slot: None },
                vec![mk_ereport(3)],
            )
            .await
            .unwrap();

        // Finally, let's do some SP ereports. These are mostly uncomplicated,
        // since the slot number will always be present.
        let sp0_restart_id0 = EreporterRestartUuid::new_v4();
        let sp0_first_seen = timestamp(300);
        let sp0_slot = 1;
        datastore
            .ereports_insert(
                opctx,
                sp0_restart_id0,
                sp0_first_seen,
                collector_id,
                rack_id,
                fm::Reporter::Sp {
                    sp_type: SpType::Switch.into(),
                    slot: sp0_slot,
                },
                vec![mk_ereport(1), mk_ereport(2)],
            )
            .await
            .unwrap();
        {
            let time_collected = timestamp(350);
            datastore
                .ereports_insert(
                    opctx,
                    sp0_restart_id0,
                    time_collected,
                    collector_id,
                    rack_id,
                    fm::Reporter::Sp {
                        sp_type: SpType::Switch.into(),
                        slot: sp0_slot,
                    },
                    vec![mk_ereport(3)],
                )
                .await
                .unwrap();
        }
        // And a subsequent restart of the same SP slot.
        let sp0_restart_id1 = EreporterRestartUuid::new_v4();
        let sp0_restart1_first_seen = timestamp(360);
        datastore
            .ereports_insert(
                opctx,
                sp0_restart_id1,
                sp0_restart1_first_seen,
                collector_id,
                rack_id,
                fm::Reporter::Sp {
                    sp_type: SpType::Switch.into(),
                    slot: sp0_slot,
                },
                vec![mk_ereport(1), mk_ereport(2)],
            )
            .await
            .unwrap();

        let mut restarts = fetch_restarts(datastore, opctx).await;

        // host 0 restart 0
        let Some(host0) = restarts.remove(&host0_restart_id) else {
            panic!(
                "expected host 0 restart 0 ({host0_restart_id}) to be present"
            );
        };
        assert_eq!(host0.reporter, EreporterType::Host);
        assert_eq!(host0.slot_type, SpType::Sled);
        assert_eq!(
            host0.slot_number(),
            Some(host0_slot),
            "a NULL slot should be filled in once it is known",
        );
        assert_eq!(
            host0.time_first_seen, host0_first_seen,
            "time_first_seen comes from the first insert and is not updated",
        );

        // host 1 restart 0
        let Some(host1) = restarts.remove(&host1_restart_id) else {
            panic!(
                "expected host 1 restart 0 ({host1_restart_id}) to be present"
            );
        };
        assert_eq!(host1.reporter, EreporterType::Host);
        assert_eq!(host1.slot_type, SpType::Sled);
        assert_eq!(
            host1.slot_number(),
            Some(host1_slot),
            "a NULL slot should be filled in once it is known",
        );
        assert_eq!(
            host1.time_first_seen, host1_first_seen,
            "time_first_seen comes from the first insert and is not updated",
        );

        // SP 0 restart 0
        let Some(sp0_restart_0) = restarts.remove(&sp0_restart_id0) else {
            panic!("expected sp 0 restart 0 ({sp0_restart_id0}) to be present");
        };
        assert_eq!(sp0_restart_0.reporter, EreporterType::Sp);
        assert_eq!(sp0_restart_0.slot_type, SpType::Switch);
        assert_eq!(sp0_restart_0.slot_number(), Some(sp0_slot));
        assert_eq!(sp0_restart_0.time_first_seen, sp0_first_seen);

        // SP 0 restart 1
        let Some(sp0_restart_1) = restarts.remove(&sp0_restart_id1) else {
            panic!("expected sp 0 restart 1 ({sp0_restart_id1}) to be present");
        };
        assert_eq!(sp0_restart_1.reporter, EreporterType::Sp);
        assert_eq!(sp0_restart_1.slot_type, SpType::Switch);
        assert_eq!(sp0_restart_1.slot_number(), Some(1));
        assert_eq!(sp0_restart_1.time_first_seen, sp0_restart1_first_seen);

        assert!(
            restarts.is_empty(),
            "unexpected restart entries remaining: {restarts:?}"
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
