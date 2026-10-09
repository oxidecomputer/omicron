// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::{ErrorHandler, public_error_from_diesel};
use nexus_db_model::ServiceAccountToken;
use nexus_db_schema::schema::service_account_token::dsl;
use nexus_types::external_api::{
    service_account as account, service_account_token as api,
};
use nexus_types::identity::Resource;
use omicron_common::api::external::{
    DataPageParams, Error, LookupType, ResourceType,
};
use uuid::Uuid;

impl DataStore {
    pub async fn service_account_token_list(
        &self,
        opctx: &OpContext,
        path: account::ServiceAccountPath,
        parent: account::ServiceAccountParentSelector,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> Result<Vec<api::ServiceAccountToken>, Error> {
        let account = self
            .service_account_lookup_for(
                opctx,
                path,
                parent,
                authz::Action::ListChildren,
            )
            .await?;
        let tokens = paginated(dsl::service_account_token, dsl::id, pagparams)
            .filter(dsl::service_account_id.eq(account.id()))
            .filter(dsl::time_deleted.is_null())
            .select(ServiceAccountToken::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(tokens.into_iter().map(Into::into).collect())
    }

    pub async fn service_account_token_view(
        &self,
        opctx: &OpContext,
        path: api::ServiceAccountTokenPath,
        parent: account::ServiceAccountParentSelector,
    ) -> Result<api::ServiceAccountToken, Error> {
        let account = self
            .service_account_lookup_for(
                opctx,
                path.account_path(),
                parent,
                authz::Action::Read,
            )
            .await?;
        let token = dsl::service_account_token
            .filter(dsl::id.eq(path.token_id))
            .filter(dsl::service_account_id.eq(account.id()))
            .filter(dsl::time_deleted.is_null())
            .select(ServiceAccountToken::as_select())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::ServiceAccountToken,
                        LookupType::ById(path.token_id),
                    ),
                )
            })?;
        Ok(token.into())
    }

    pub async fn service_account_token_delete(
        &self,
        opctx: &OpContext,
        path: api::ServiceAccountTokenPath,
        parent: account::ServiceAccountParentSelector,
    ) -> Result<(), Error> {
        let account = self
            .service_account_lookup_for(
                opctx,
                path.account_path(),
                parent,
                authz::Action::Modify,
            )
            .await?;
        let deleted = diesel::update(dsl::service_account_token)
            .filter(dsl::id.eq(path.token_id))
            .filter(dsl::service_account_id.eq(account.id()))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(Utc::now()))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        if deleted == 0 {
            return Err(Error::not_found_by_id(
                ResourceType::ServiceAccountToken,
                &path.token_id,
            ));
        }
        Ok(())
    }
}
