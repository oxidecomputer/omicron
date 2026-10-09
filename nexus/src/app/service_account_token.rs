// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_db_queries::context::OpContext;
use nexus_types::external_api::{
    service_account as account, service_account_token as api,
};
use omicron_common::api::external::{DataPageParams, Error};
use uuid::Uuid;

impl super::Nexus {
    pub(crate) async fn service_account_token_create(
        &self,
        opctx: &OpContext,
        path: account::ServiceAccountPath,
        parent: account::ServiceAccountParentSelector,
        params: api::ServiceAccountTokenCreate,
    ) -> Result<api::ServiceAccountTokenGrant, Error> {
        self.db_datastore
            .service_account_token_create(opctx, path, parent, params)
            .await
            .map(Into::into)
    }

    pub(crate) async fn service_account_token_list(
        &self,
        opctx: &OpContext,
        path: account::ServiceAccountPath,
        parent: account::ServiceAccountParentSelector,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> Result<Vec<api::ServiceAccountToken>, Error> {
        self.db_datastore
            .service_account_token_list(opctx, path, parent, pagparams)
            .await
    }

    pub(crate) async fn service_account_token_view(
        &self,
        opctx: &OpContext,
        path: api::ServiceAccountTokenPath,
        parent: account::ServiceAccountParentSelector,
    ) -> Result<api::ServiceAccountToken, Error> {
        self.db_datastore.service_account_token_view(opctx, path, parent).await
    }

    pub(crate) async fn service_account_token_delete(
        &self,
        opctx: &OpContext,
        path: api::ServiceAccountTokenPath,
        parent: account::ServiceAccountParentSelector,
    ) -> Result<(), Error> {
        self.db_datastore
            .service_account_token_delete(opctx, path, parent)
            .await
    }
}
