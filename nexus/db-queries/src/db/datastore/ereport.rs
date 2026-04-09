// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods for ereports.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::datastore::RunnableQuery;
use crate::db::model::Ereport;
use crate::db::model::SpMgsSlot;
use crate::db::model::SpType;
use crate::db::model::SqlU16;
use crate::db::model::SqlU32;
use crate::db::model::ereport as model;
use crate::db::model::ereport::DbEna;
use crate::db::pagination::{paginated, paginated_multicolumn};
use crate::db::raw_query_builder::QueryBuilder;
use crate::db::raw_query_builder::TypedSqlQuery;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::DateTime;
use chrono::Utc;
use diesel::AggregateExpressionMethods;
use diesel::dsl::{count, min};
use diesel::prelude::*;
use diesel::sql_types;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_schema::schema::ereport::dsl;
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
use omicron_uuid_kinds::SitrepUuid;
use omicron_uuid_kinds::SledUuid;
use uuid::Uuid;

type EreportIdTuple = (Uuid, DbEna);

#[derive(Clone, Debug)]
pub struct EreporterRestartBySerial {
    pub id: EreporterRestartUuid,
    pub first_seen_at: DateTime<Utc>,
    pub reporter_kind: fm::Reporter,
    pub ereports: u32,
}

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

    /// List unique ereporter restarts for the given serial number.
    pub async fn ereporter_restart_list_by_serial(
        &self,
        opctx: &OpContext,
        serial: String,
    ) -> ListResultVec<EreporterRestartBySerial> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        let conn = &*self.pool_connection_authorized(opctx).await?;
        let rows = Self::restart_list_by_serial_query(serial.clone())
            .load_async::<(Uuid, model::Reporter, Option<DateTime<Utc>>, SqlU32)>(
                conn,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        let restarts = rows.into_iter().map(|(restart_id, reporter, first_seen, ereports)| {
            let first_seen_at = first_seen.expect(
                "`min(time_collected)` should never  return `NULL`, since the \
                 `time_collected` column is not nullable, and the `SELECT` clause \
                  should return nothing if the result set is empty"
            );
            EreporterRestartBySerial {
                id: EreporterRestartUuid::from_untyped_uuid(restart_id),
                reporter_kind: reporter.try_into().unwrap(),
                first_seen_at,
                ereports: ereports.into(),
            }
        }).collect();

        Ok(restarts)
    }

    fn restart_list_by_serial_query(
        serial: String,
    ) -> impl RunnableQuery<(Uuid, model::Reporter, Option<DateTime<Utc>>, SqlU32)>
    {
        dsl::ereport
            .filter(dsl::serial_number.eq(serial.clone()))
            .filter(dsl::time_deleted.is_null())
            .group_by((
                dsl::restart_id,
                dsl::reporter,
                dsl::slot_type,
                dsl::slot,
                dsl::sled_id,
            ))
            .select((
                dsl::restart_id,
                model::Reporter::as_select(),
                min(dsl::time_collected),
                count(dsl::ena).aggregate_distinct(),
            ))
            .order_by(dsl::restart_id)
    }

    pub async fn latest_ereport_id(
        &self,
        opctx: &OpContext,
        reporter: fm::Reporter,
    ) -> Result<Option<EreportId>, Error> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        self.latest_ereport_id_on_conn(
            &*self.pool_connection_authorized(opctx).await?,
            reporter,
        )
        .await
    }

    async fn latest_ereport_id_on_conn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        reporter: fm::Reporter,
    ) -> Result<Option<EreportId>, Error> {
        let result = match reporter {
            fm::Reporter::Sp { sp_type, slot } => {
                let sp_type = sp_type.into();
                let slot = SpMgsSlot::from(SqlU16::new(slot));
                Self::sp_latest_ereport_id_query(sp_type, slot)
                    .get_result_async(conn)
                    .await
            }
            fm::Reporter::HostOs { sled, .. } => {
                Self::host_latest_ereport_id_query(sled)
                    .get_result_async(conn)
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

    pub async fn ereports_insert(
        &self,
        opctx: &OpContext,
        reporter: fm::Reporter,
        ereports: impl IntoIterator<Item = fm::EreportData>,
    ) -> CreateResult<(usize, Option<EreportId>)> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let ereports = ereports
            .into_iter()
            .map(|data| Ereport::new(data, reporter))
            .collect::<Vec<_>>();
        let created = diesel::insert_into(dsl::ereport)
            .values(ereports)
            .on_conflict((dsl::restart_id, dsl::ena))
            .do_nothing()
            .execute_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(e, ErrorHandler::Server)
                    .internal_context("failed to insert ereports")
            })?;
        let latest = self
            .latest_ereport_id_on_conn(&conn, reporter)
            .await
            .map_err(|e| {
                e.internal_context(format!(
                    "failed to refresh latest ereport ID for {reporter}"
                ))
            })?;
        Ok((created, latest))
    }

    /// Lists ereports which have not been marked as **definitely seen**
    /// (included in a committed sitrep) in the database, paginated by the
    /// reporter restart ID and ENA.
    ///
    /// Note that this filters based only on whether they have been marked in
    /// the database. Because marking seen ereports occurs asynchronously from
    /// committing sitreps as part of FM rendezvous, ereports returned by this
    /// query may have already been seen. These ereports must be filtered out at
    /// a higher level based on the contents of the current sitrep when
    /// determining which ereports are *actually* new.
    pub async fn ereports_list_unmarked(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, (Uuid, DbEna)>,
    ) -> ListResultVec<Ereport> {
        // TODO(eliza): ereports should probably have their own resource type someday...
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        Self::ereports_list_unmarked_query(pagparams)
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    fn ereports_list_unmarked_query(
        pagparams: &DataPageParams<'_, (Uuid, DbEna)>,
    ) -> impl RunnableQuery<Ereport> + use<> {
        paginated_multicolumn(
            dsl::ereport,
            (dsl::restart_id, dsl::ena),
            pagparams,
        )
        .filter(dsl::marked_seen_in.is_null())
        .filter(dsl::time_deleted.is_null())
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
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use dropshot::PaginationOrder;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::OmicronZoneUuid;
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
    async fn explain_restart_list_by_serial() {
        let logctx = dev::test_setup_log("explain_restart_list_by_serial");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let query = DataStore::restart_list_by_serial_query(String::new());
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
        let query = DataStore::ereports_list_unmarked_query(&pagparams);
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
        let query = DataStore::ereports_list_unmarked_query(&pagparams);
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

        let id = fm::EreportId {
            restart_id: EreporterRestartUuid::new_v4(),
            ena: ereport_types::Ena(2),
        };
        let ereport = fm::EreportData {
            id,
            time_collected: Utc::now(),
            collector_id: OmicronZoneUuid::new_v4(),
            part_number: Some("my cool CPN".to_string()),
            serial_number: Some("my cool serial".to_string()),
            class: Some("my cool ereport".to_string()),
            report: serde_json::json!({}),
        };
        datastore
            .ereports_insert(
                &opctx,
                fm::Reporter::Sp {
                    sp_type: nexus_types::inventory::SpType::Sled,
                    slot: 19,
                },
                vec![ereport.clone()],
            )
            .await
            .expect("insert should succeed");

        #[track_caller]
        fn check_results(
            found_ereports: Vec<Ereport>,
            expected_id: &fm::EreportId,
            expected: &fm::EreportData,
        ) {
            assert_eq!(found_ereports.len(), 1);
            assert_eq!(&found_ereports[0].id(), expected_id);
            assert_eq!(
                found_ereports[0].collector_id,
                expected.collector_id.into()
            );
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
        check_results(dbg!(found_default), &id, &ereport);

        let found_by_time_range = datastore
            .ereport_fetch_matching(
                opctx,
                &EreportFilters::new()
                    .with_start_time(
                        ereport.time_collected - Duration::from_secs(600),
                    )
                    .expect("no end time set"),
                &pagparams,
            )
            .await
            .expect("fetch matching with time range filters should succeed");
        check_results(dbg!(found_by_time_range), &id, &ereport);

        let found_by_serial = datastore
            .ereport_fetch_matching(
                opctx,
                &EreportFilters::new().with_serials(["my cool serial"]),
                &pagparams,
            )
            .await
            .expect("fetch matching with serial number filters should succeed");
        check_results(dbg!(found_by_serial), &id, &ereport);

        let found_by_class = datastore
            .ereport_fetch_matching(
                opctx,
                &EreportFilters::new().with_classes(["my cool ereport"]),
                &pagparams,
            )
            .await
            .expect("fetch matching with class filters should succeed");
        check_results(dbg!(found_by_class), &id, &ereport);

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
