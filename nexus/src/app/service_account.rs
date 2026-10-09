// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_db_queries::context::OpContext;
use nexus_types::external_api::service_account as api;
use omicron_common::api::external::Error;
use omicron_common::api::external::http_pagination::PaginatedBy;

impl super::Nexus {
    pub(crate) async fn service_account_create(
        &self,
        opctx: &OpContext,
        scope: api::ServiceAccountScope,
        parent: api::ServiceAccountParentSelector,
        params: api::ServiceAccountCreate,
    ) -> Result<api::ServiceAccount, Error> {
        self.db_datastore
            .service_account_create(opctx, scope, parent, params)
            .await
    }

    pub(crate) async fn service_account_list(
        &self,
        opctx: &OpContext,
        scope: api::ServiceAccountScope,
        parent: api::ServiceAccountParentSelector,
        pagparams: &PaginatedBy<'_>,
    ) -> Result<Vec<api::ServiceAccount>, Error> {
        self.db_datastore
            .service_account_list(opctx, scope, parent, pagparams)
            .await
    }

    pub(crate) async fn service_account_view(
        &self,
        opctx: &OpContext,
        path: api::ServiceAccountPath,
        parent: api::ServiceAccountParentSelector,
    ) -> Result<api::ServiceAccount, Error> {
        self.db_datastore.service_account_view(opctx, path, parent).await
    }

    pub(crate) async fn service_account_update(
        &self,
        opctx: &OpContext,
        path: api::ServiceAccountPath,
        parent: api::ServiceAccountParentSelector,
        params: api::ServiceAccountUpdate,
    ) -> Result<api::ServiceAccount, Error> {
        self.db_datastore
            .service_account_update(opctx, path, parent, params)
            .await
    }

    pub(crate) async fn service_account_delete(
        &self,
        opctx: &OpContext,
        path: api::ServiceAccountPath,
        parent: api::ServiceAccountParentSelector,
    ) -> Result<(), Error> {
        self.db_datastore.service_account_delete(opctx, path, parent).await
    }
}
