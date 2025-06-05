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
use crate::db::model::SpEreport;
use crate::db::model::SpMgsSlot;
use crate::db::model::SpType;
use crate::db::model::SqlU16;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_schema::schema::host_ereport::dsl as host_dsl;
use nexus_db_schema::schema::sp_ereport::dsl as sp_dsl;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::EreporterRestartUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use uuid::Uuid;

type EreportIdTuple = (Uuid, DbEna);

impl DataStore {
    // pub async fn sp_ereport_list_by_serial(
    //     &self,
    //     opctx: &OpContext,
    //     serial: String,
    //     time_range: impl RangeBounds<DateTime<Utc>>,
    //     pagparams:
    // ) -> ListResultVec<SpEreport> {
    //     todo!()
    // }

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
            .filter(sp_dsl::sp_type.eq(sp_type).and(sp_dsl::sp_slot.eq(slot)))
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
        let id = Self::host_latest_ereport_id_query(sled_id)
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
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
            .filter(host_dsl::sled_id.eq(sled_id.into_untyped_uuid()))
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
            .sp_latest_ereport_id_on_conn(&*conn, sp_type, slot)
            .await
            .map_err(|e| {
                e.internal_context(format!(
                    "failed to refresh latest ereport ID for {sp_type:?} {slot}"
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
    use crate::db::raw_query_builder::expectorate_query_contents;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn expectorate_sp_latest_ereport_id() {
        let query = DataStore::sp_latest_ereport_id_query(
            SpType::Sled,
            SpMgsSlot::from(SqlU16::new(1)),
        );

        expectorate_query_contents(
            &query,
            "tests/output/sp_latest_ereport_id.sql",
        )
        .await;
    }

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
    async fn expectorate_host_latest_ereport_id() {
        let query = DataStore::host_latest_ereport_id_query(SledUuid::nil());

        expectorate_query_contents(
            &query,
            "tests/output/host_latest_ereport_id.sql",
        )
        .await;
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
}
