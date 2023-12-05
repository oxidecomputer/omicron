// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to OAuth 2.0 Device Authorization Grants.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::error::TransactionError;
use crate::db::model::DeviceAccessToken;
use crate::db::model::DeviceAuthRequest;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

impl DataStore {
    /// Start a device authorization grant flow by recording the request
    /// and initial response parameters.
    pub async fn device_auth_request_create(
        &self,
        opctx: &OpContext,
        auth_request: DeviceAuthRequest,
    ) -> CreateResult<DeviceAuthRequest> {
        opctx
            .authorize(
                authz::Action::CreateChild,
                &authz::DEVICE_AUTH_REQUEST_LIST,
            )
            .await?;

        use db::schema::device_auth_request::dsl;
        diesel::insert_into(dsl::device_auth_request)
            .values(auth_request)
            .returning(DeviceAuthRequest::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Remove the device authorization request and create a new device
    /// access token record. The token may already be expired if the flow
    /// was not completed in time.
    pub async fn device_access_token_create(
        &self,
        opctx: &OpContext,
        authz_request: &authz::DeviceAuthRequest,
        authz_user: &authz::SiloUser,
        access_token: DeviceAccessToken,
    ) -> CreateResult<DeviceAccessToken> {
        assert_eq!(authz_user.id(), access_token.silo_user_id);
        opctx.authorize(authz::Action::Delete, authz_request).await?;
        opctx.authorize(authz::Action::CreateChild, authz_user).await?;

        use db::schema::device_auth_request::dsl as request_dsl;
        let delete_request = diesel::delete(request_dsl::device_auth_request)
            .filter(request_dsl::user_code.eq(authz_request.id()));

        use db::schema::device_access_token::dsl as token_dsl;
        let insert_token = diesel::insert_into(token_dsl::device_access_token)
            .values(access_token)
            .returning(DeviceAccessToken::as_returning());

        #[derive(Debug)]
        enum TokenGrantError {
            RequestNotFound,
            TooManyRequests,
        }
        type TxnError = TransactionError<TokenGrantError>;

        self.pool_connection_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                match delete_request.execute_async(&conn).await? {
                    0 => {
                        Err(TxnError::CustomError(TokenGrantError::RequestNotFound))
                    }
                    1 => Ok(insert_token.get_result_async(&conn).await?),
                    _ => Err(TxnError::CustomError(
                        TokenGrantError::TooManyRequests,
                    )),
                }
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(TokenGrantError::RequestNotFound) => {
                    Error::ObjectNotFound {
                        type_name: ResourceType::DeviceAuthRequest,
                        lookup_type: LookupType::ByCompositeId(
                            authz_request.id(),
                        ),
                    }
                }
                TxnError::CustomError(TokenGrantError::TooManyRequests) => {
                    Error::internal_error("unexpectedly found multiple device auth requests for the same user code")
                }
                TxnError::Database(e) => {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    /// Look up a granted device access token.
    /// Note: since this lookup is not by a primary key or name,
    /// (though it does use a unique index), it does not fit the
    /// usual lookup machinery pattern. It therefore does include
    /// any authz checks. However, the device code is a single-use
    /// high-entropy random token, and so should not be guessable
    /// by an attacker.
    pub async fn device_access_token_fetch(
        &self,
        opctx: &OpContext,
        client_id: Uuid,
        device_code: String,
    ) -> LookupResult<DeviceAccessToken> {
        use db::schema::device_access_token::dsl;
        dsl::device_access_token
            .filter(dsl::client_id.eq(client_id))
            .filter(dsl::device_code.eq(device_code))
            .select(DeviceAccessToken::as_select())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::DeviceAccessToken,
                        LookupType::ByCompositeId(
                            "client_id, device_code".to_string(),
                        ),
                    ),
                )
            })
    }
}
