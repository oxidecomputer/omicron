// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods for ereports.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::datastore::RunnableQuery;
use crate::db::model::DbEna;
use crate::db::model::Ereport;
use crate::db::model::HostEreport;
use crate::db::model::Reporter;
use crate::db::model::SpEreport;
use crate::db::model::SpMgsSlot;
use crate::db::model::SpType;
use crate::db::model::SqlU16;
use crate::db::model::SqlU32;
use crate::db::pagination::{paginated, paginated_multicolumn};
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::DateTime;
use chrono::Utc;
use diesel::dsl::{count_distinct, min};
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_schema::schema::host_ereport::dsl as host_dsl;
use nexus_db_schema::schema::sp_ereport::dsl as sp_dsl;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::EreporterRestartUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use uuid::Uuid;

type EreportIdTuple = (Uuid, DbEna);

#[derive(Clone, Debug)]
pub struct EreporterRestartBySerial {
    pub id: EreporterRestartUuid,
    pub first_seen_at: DateTime<Utc>,
    pub reporter_kind: Reporter,
    pub ereports: u32,
}

/// A set of filters for fetching ereports.
#[derive(Clone, Debug, Default)]
pub struct EreportFilters {
    /// If present, include only ereports that were collected at the specified
    /// timestamp or later.
    ///
    /// If `end_time` is also present, this value *must* be earlier than
    /// `end_time`.
    pub start_time: Option<DateTime<Utc>>,
    /// If present, include only ereports that were collected at the specified
    /// timestamp or before.
    ///
    /// If `start_time` is also present, this value *must* be later than
    /// `start_time`.
    pub end_time: Option<DateTime<Utc>>,
    /// If this list is non-empty, include only ereports that were reported by
    /// systems with the provided serial numbers.
    pub only_serials: Vec<String>,
    /// If this list is non-empty, include only ereports with the provided class
    /// strings.
    // TODO(eliza): globbing could be nice to add here eventually...
    pub only_classes: Vec<String>,
}

impl EreportFilters {
    fn check_time_range(&self) -> Result<(), Error> {
        if let (Some(start), Some(end)) = (self.start_time, self.end_time) {
            if start > end {
                return Err(Error::invalid_request(
                    "start time must be before end time",
                ));
            }
        }

        Ok(())
    }
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
        id: ereport_types::EreportId,
    ) -> LookupResult<Ereport> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let restart_id = id.restart_id.into_untyped_uuid();
        let ena = DbEna::from(id.ena);

        if let Some(report) = sp_dsl::sp_ereport
            .filter(sp_dsl::restart_id.eq(restart_id))
            .filter(sp_dsl::ena.eq(ena))
            .filter(sp_dsl::time_deleted.is_null())
            .select(SpEreport::as_select())
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
        {
            return Ok(report.into());
        }

        if let Some(report) = host_dsl::host_ereport
            .filter(host_dsl::restart_id.eq(restart_id))
            .filter(host_dsl::ena.eq(ena))
            .filter(host_dsl::time_deleted.is_null())
            .select(HostEreport::as_select())
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
        {
            return Ok(report.into());
        }

        Err(Error::non_resourcetype_not_found(format!("ereport {id}")))
    }

    pub async fn host_ereports_fetch_matching(
        &self,
        opctx: &OpContext,
        filters: &EreportFilters,
        pagparams: &DataPageParams<'_, (Uuid, DbEna)>,
    ) -> ListResultVec<HostEreport> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        filters.check_time_range()?;

        let mut query = paginated_multicolumn(
            host_dsl::host_ereport,
            (host_dsl::restart_id, host_dsl::ena),
            pagparams,
        )
        .filter(host_dsl::time_deleted.is_null())
        .select(HostEreport::as_select());

        if let Some(start) = filters.start_time {
            query = query.filter(host_dsl::time_collected.ge(start));
        }

        if let Some(end) = filters.end_time {
            query = query.filter(host_dsl::time_collected.le(end));
        }

        if !filters.only_serials.is_empty() {
            query = query.filter(
                host_dsl::sled_serial.eq_any(filters.only_serials.clone()),
            );
        }

        if !filters.only_classes.is_empty() {
            query = query
                .filter(host_dsl::class.eq_any(filters.only_classes.clone()));
        }

        query
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn sp_ereports_fetch_matching(
        &self,
        opctx: &OpContext,
        filters: &EreportFilters,
        pagparams: &DataPageParams<'_, (Uuid, DbEna)>,
    ) -> ListResultVec<SpEreport> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        filters.check_time_range()?;

        let mut query = paginated_multicolumn(
            sp_dsl::sp_ereport,
            (sp_dsl::restart_id, sp_dsl::ena),
            pagparams,
        )
        .filter(sp_dsl::time_deleted.is_null())
        .select(SpEreport::as_select());

        if let Some(start) = filters.start_time {
            query = query.filter(sp_dsl::time_collected.ge(start));
        }

        if let Some(end) = filters.end_time {
            query = query.filter(sp_dsl::time_collected.le(end));
        }

        if !filters.only_serials.is_empty() {
            query = query.filter(
                sp_dsl::serial_number.eq_any(filters.only_serials.clone()),
            );
        }

        if !filters.only_classes.is_empty() {
            query = query
                .filter(sp_dsl::class.eq_any(filters.only_classes.clone()));
        }

        query
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List ereports from the SP with the given restart ID.
    pub async fn sp_ereport_list_by_restart(
        &self,
        opctx: &OpContext,
        restart_id: EreporterRestartUuid,
        pagparams: &DataPageParams<'_, DbEna>,
    ) -> ListResultVec<SpEreport> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        paginated(sp_dsl::sp_ereport, sp_dsl::ena, pagparams)
            .filter(sp_dsl::restart_id.eq(restart_id.into_untyped_uuid()))
            .filter(sp_dsl::time_deleted.is_null())
            .select(SpEreport::as_select())
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
        let sp_rows = Self::sp_restart_list_by_serial_query(serial.clone())
            .load_async::<(Uuid, SpType, SqlU16, Option<DateTime<Utc>>, SqlU32)>(
                conn,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        let host_os_rows =
            Self::host_restart_list_by_serial_query(serial.clone())
                .load_async::<(Uuid, Uuid, Option<DateTime<Utc>>, SqlU32)>(conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                        .internal_context("listing SP ereports")
                })?;

        const FIRST_SEEN_NOT_NULL: &str = "`min(time_collected)` should never \
            return `NULL`, since the `time_collected` column is not nullable, \
            and the `SELECT` clause should return nothing if the result set \
            is empty";

        let sp_reporters = sp_rows.into_iter().map(
            |(restart_id, sp_type, sp_slot, first_seen, ereports)| {
                EreporterRestartBySerial {
                    id: EreporterRestartUuid::from_untyped_uuid(restart_id),
                    reporter_kind: Reporter::Sp {
                        sp_type: sp_type.into(),
                        slot: sp_slot.into(),
                    },
                    first_seen_at: first_seen.expect(FIRST_SEEN_NOT_NULL),
                    ereports: ereports.into(),
                }
            },
        );
        let host_reporters = host_os_rows.into_iter().map(
            |(restart_id, sled_id, first_seen, ereports)| {
                EreporterRestartBySerial {
                    id: EreporterRestartUuid::from_untyped_uuid(restart_id),
                    reporter_kind: Reporter::HostOs {
                        sled: SledUuid::from_untyped_uuid(sled_id),
                    },
                    first_seen_at: first_seen.expect(FIRST_SEEN_NOT_NULL),
                    ereports: ereports.into(),
                }
            },
        );
        Ok(sp_reporters.chain(host_reporters).collect::<Vec<_>>())
    }

    fn sp_restart_list_by_serial_query(
        serial: String,
    ) -> impl RunnableQuery<(Uuid, SpType, SqlU16, Option<DateTime<Utc>>, SqlU32)>
    {
        sp_dsl::sp_ereport
            .filter(
                sp_dsl::serial_number
                    .eq(serial.clone())
                    .and(sp_dsl::time_deleted.is_null()),
            )
            .group_by((sp_dsl::restart_id, sp_dsl::sp_slot, sp_dsl::sp_type))
            .select((
                sp_dsl::restart_id,
                sp_dsl::sp_type,
                sp_dsl::sp_slot,
                min(sp_dsl::time_collected),
                count_distinct(sp_dsl::ena),
            ))
            .order_by(sp_dsl::restart_id)
    }

    fn host_restart_list_by_serial_query(
        serial: String,
    ) -> impl RunnableQuery<(Uuid, Uuid, Option<DateTime<Utc>>, SqlU32)> {
        host_dsl::host_ereport
            .filter(
                host_dsl::sled_serial
                    .eq(serial)
                    .and(host_dsl::time_deleted.is_null()),
            )
            .group_by((host_dsl::restart_id, host_dsl::sled_id))
            .select((
                host_dsl::restart_id,
                host_dsl::sled_id,
                min(host_dsl::time_collected),
                count_distinct(host_dsl::ena),
            ))
            .order_by(host_dsl::restart_id)
    }

    pub async fn sp_latest_ereport_id(
        &self,
        opctx: &OpContext,
        sp_type: impl Into<SpType>,
        slot: u16,
    ) -> Result<Option<ereport_types::EreportId>, Error> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        self.sp_latest_ereport_id_on_conn(
            &*self.pool_connection_authorized(opctx).await?,
            sp_type,
            slot,
        )
        .await
    }

    async fn sp_latest_ereport_id_on_conn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        sp_type: impl Into<SpType>,
        slot: u16,
    ) -> Result<Option<ereport_types::EreportId>, Error> {
        let sp_type = sp_type.into();
        let slot = SpMgsSlot::from(SqlU16::new(slot));
        let id = Self::sp_latest_ereport_id_query(sp_type, slot)
            .get_result_async(conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .map(id_from_tuple);
        Ok(id)
    }

    fn sp_latest_ereport_id_query(
        sp_type: SpType,
        slot: SpMgsSlot,
    ) -> impl RunnableQuery<EreportIdTuple> {
        sp_dsl::sp_ereport
            .filter(
                sp_dsl::sp_type
                    .eq(sp_type)
                    .and(sp_dsl::sp_slot.eq(slot))
                    .and(sp_dsl::time_deleted.is_null()),
            )
            .order_by((sp_dsl::time_collected.desc(), sp_dsl::ena.desc()))
            .limit(1)
            .select((sp_dsl::restart_id, sp_dsl::ena))
    }

    pub async fn host_latest_ereport_id(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
    ) -> Result<Option<ereport_types::EreportId>, Error> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        self.host_latest_ereport_id_on_conn(
            &*self.pool_connection_authorized(opctx).await?,
            sled_id,
        )
        .await
    }

    async fn host_latest_ereport_id_on_conn(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        sled_id: SledUuid,
    ) -> Result<Option<ereport_types::EreportId>, Error> {
        let id = Self::host_latest_ereport_id_query(sled_id)
            .get_result_async(conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?
            .map(id_from_tuple);
        Ok(id)
    }

    fn host_latest_ereport_id_query(
        sled_id: SledUuid,
    ) -> impl RunnableQuery<EreportIdTuple> {
        host_dsl::host_ereport
            .filter(
                host_dsl::sled_id
                    .eq(sled_id.into_untyped_uuid())
                    .and(host_dsl::time_deleted.is_null()),
            )
            .order_by((host_dsl::time_collected.desc(), host_dsl::ena.desc()))
            .limit(1)
            .select((host_dsl::restart_id, host_dsl::ena))
    }

    pub async fn sp_ereports_insert(
        &self,
        opctx: &OpContext,
        sp_type: impl Into<SpType>,
        slot: u16,
        ereports: Vec<SpEreport>,
    ) -> CreateResult<(usize, Option<ereport_types::EreportId>)> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let created = diesel::insert_into(sp_dsl::sp_ereport)
            .values(ereports)
            .on_conflict((sp_dsl::restart_id, sp_dsl::ena))
            .do_nothing()
            .execute_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(e, ErrorHandler::Server)
                    .internal_context("failed to insert ereports")
            })?;
        let sp_type = sp_type.into();
        let latest = self
            .sp_latest_ereport_id_on_conn(&conn, sp_type, slot)
            .await
            .map_err(|e| {
                e.internal_context(format!(
                    "failed to refresh latest ereport ID for {sp_type:?} {slot}"
                ))
            })?;
        Ok((created, latest))
    }

    pub async fn host_ereports_insert(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
        ereports: Vec<HostEreport>,
    ) -> CreateResult<(usize, Option<ereport_types::EreportId>)> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let created = diesel::insert_into(host_dsl::host_ereport)
            .values(ereports)
            .on_conflict((host_dsl::restart_id, host_dsl::ena))
            .do_nothing()
            .execute_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(e, ErrorHandler::Server)
                    .internal_context("failed to insert ereports")
            })?;
        let latest = self
            .host_latest_ereport_id_on_conn(&conn, sled_id)
            .await
            .map_err(|e| {
                e.internal_context(format!(
                    "failed to refresh latest ereport ID for {sled_id}"
                ))
            })?;
        Ok((created, latest))
    }
}

fn id_from_tuple(
    (restart_id, DbEna(ena)): EreportIdTuple,
) -> ereport_types::EreportId {
    ereport_types::EreportId {
        restart_id: EreporterRestartUuid::from_untyped_uuid(restart_id),
        ena,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::explain::ExplainableAsync;
    use crate::db::pub_test_utils::TestDatabase;
    use omicron_test_utils::dev;

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
    async fn explain_host_restart_list_by_serial() {
        let logctx = dev::test_setup_log("explain_host_restart_list_by_serial");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let query = DataStore::host_restart_list_by_serial_query(String::new());
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
    async fn explain_sp_restart_list_by_serial() {
        let logctx = dev::test_setup_log("explain_sp_restart_list_by_serial");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        let query = DataStore::sp_restart_list_by_serial_query(String::new());
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
}
