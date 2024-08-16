// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use ipnetwork::IpNetwork;
use nexus_db_model::BfdSession;
use nexus_db_model::SqlU32;
use nexus_types::external_api::params;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::{
    CreateResult, DeleteResult, ListResultVec,
};
use uuid::Uuid;

impl DataStore {
    pub async fn bfd_session_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<BfdSession> {
        use db::schema::bfd_session::dsl;
        let conn = self.pool_connection_authorized(opctx).await?;
        paginated(dsl::bfd_session, dsl::id, pagparams)
            .select(BfdSession::as_select())
            .filter(dsl::time_deleted.is_null())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn bfd_session_create(
        &self,
        opctx: &OpContext,
        config: &params::BfdSessionEnable,
    ) -> CreateResult<BfdSession> {
        use db::schema::bfd_session::dsl;
        let conn = self.pool_connection_authorized(opctx).await?;

        let session = BfdSession {
            id: Uuid::new_v4(),
            local: config.local.map(Into::into),
            remote: config.remote.into(),
            detection_threshold: SqlU32::new(config.detection_threshold.into()),
            required_rx: SqlU32::new(
                config.required_rx.try_into().unwrap_or(u32::MAX),
            ),
            switch: config.switch.to_string(),
            mode: config.mode.into(),
            time_created: chrono::Utc::now(),
            time_modified: chrono::Utc::now(),
            time_deleted: None,
        };

        diesel::insert_into(dsl::bfd_session)
            .values(session)
            .returning(BfdSession::as_returning())
            .get_result_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn bfd_session_delete(
        &self,
        opctx: &OpContext,
        config: &params::BfdSessionDisable,
    ) -> DeleteResult {
        use db::schema::bfd_session::dsl;
        let conn = self.pool_connection_authorized(opctx).await?;

        diesel::update(dsl::bfd_session)
            .filter(dsl::remote.eq(IpNetwork::from(config.remote)))
            .filter(dsl::switch.eq(config.switch.to_string()))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(chrono::Utc::now()))
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
            .map(|_| ())
    }
}
