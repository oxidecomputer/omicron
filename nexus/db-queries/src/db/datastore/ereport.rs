// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods for ereports.

use super::DataStore;
use crate::context::OpContext;
use crate::db::datastore::RunnableQuery;
use crate::db::model::DbEna;
use crate::db::model::EreportMetadata;
use crate::db::model::SpMgsSlot;
use crate::db::model::SpType;
use crate::db::model::SqlU16;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_schema::schema::host_ereport::dsl as host_dsl;
use nexus_db_schema::schema::sp_ereport::dsl as sp_dsl;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::EreporterRestartUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use uuid::Uuid;

type EreportIdTuple = (Uuid, DbEna);

impl DataStore {
    pub async fn sp_latest_ereport_id(
        &self,
        opctx: &OpContext,
        sp_type: impl Into<SpType>,
        slot: u16,
    ) -> Result<Option<ereport_types::EreportId>, Error> {
        let sp_type = sp_type.into();
        let slot = SpMgsSlot::from(SqlU16::new(slot));
        let id = Self::sp_latest_ereport_id_query(sp_type, slot)
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
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
            .order_by(sp_dsl::time_collected.desc())
            .limit(1)
            .select((sp_dsl::restart_id, sp_dsl::ena))
    }

    pub async fn host_latest_ereport_id(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
    ) -> Result<Option<ereport_types::EreportId>, Error> {
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
            .order_by(host_dsl::time_collected.desc())
            .limit(1)
            .select((host_dsl::restart_id, host_dsl::ena))
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
