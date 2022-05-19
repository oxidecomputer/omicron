// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Client authentication and token granting.

use crate::context::OpContext;
use crate::db::model::{ClientAuthentication, ClientToken};
use crate::external_api::client_api::TokenResponse;
use omicron_common::api::external::{CreateResult, LookupResult};
use uuid::Uuid;

impl super::Nexus {
    pub async fn client_authenticate(
        &self,
        opctx: &OpContext,
        client_id: Uuid,
    ) -> CreateResult<ClientAuthentication> {
        // TODO-security: validate `client_id`

        let client_authn = ClientAuthentication::new(client_id);
        self.db_datastore.client_authenticate(opctx, client_authn).await
    }

    pub async fn client_verify(
        &self,
        opctx: &OpContext,
        user_code: String,
    ) -> LookupResult<ClientAuthentication> {
        self.db_datastore.client_verify(opctx, user_code).await
    }

    pub async fn client_verified(
        &self,
        opctx: &OpContext,
        user_code: String,
        silo_user_id: Uuid,
    ) -> CreateResult<ClientToken> {
        let client_authn = self.client_verify(opctx, user_code).await?;
        let client_id = client_authn.client_id;
        let device_code = client_authn.device_code;
        let token = ClientToken::new(client_id, device_code, silo_user_id);
        self.db_datastore.client_grant_token(opctx, token).await
    }

    pub async fn client_get_token(
        &self,
        opctx: &OpContext,
        client_id: Uuid,
        device_code: String,
    ) -> CreateResult<TokenResponse> {
        match self
            .db_datastore
            .client_get_token(opctx, client_id, device_code)
            .await
        {
            Ok(token) => Ok(TokenResponse::Granted(token)),
            Err(_) => Ok(TokenResponse::Pending),
            // TODO: TokenResponse::Denied
        }
    }
}
