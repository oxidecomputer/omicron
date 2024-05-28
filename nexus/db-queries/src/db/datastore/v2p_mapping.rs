// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::error::{public_error_from_diesel, ErrorHandler};
use crate::db::model::V2PMappingView;
use crate::db::pagination::paginated;
use crate::db::pagination::Paginator;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::{QueryDsl, SelectableHelper};
use omicron_common::api::external::ListResultVec;

impl DataStore {
    pub async fn v2p_mappings(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<V2PMappingView> {
        use db::schema::v2p_mapping_view::dsl;

        opctx.check_complex_operations_allowed()?;

        let mut mappings = Vec::new();
        let mut paginator = Paginator::new(SQL_BATCH_SIZE);
        while let Some(p) = paginator.next() {
            let batch = paginated(
                dsl::v2p_mapping_view,
                dsl::nic_id,
                &p.current_pagparams(),
            )
            .select(V2PMappingView::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

            paginator = p.found_batch(&batch, &|mapping| mapping.nic_id);
            mappings.extend(batch);
        }

        Ok(mappings)
    }
}
