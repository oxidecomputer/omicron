// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to OAuth 2.0 Device Authorization Grants.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::model::DeviceAccessToken;
use crate::db::model::DeviceAuthRequest;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_schema::schema::device_access_token;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_uuid_kinds::GenericUuid;
use uuid::Uuid;

impl DataStore {
    pub async fn device_token_lookup_by_token(
        &self,
        opctx: &OpContext,
        token: String,
    ) -> LookupResult<(authz::DeviceAccessToken, DeviceAccessToken)> {
        let db_token = device_access_token::table
            .filter(device_access_token::token.eq(token))
            .select(DeviceAccessToken::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|_e| Error::ObjectNotFound {
                type_name: ResourceType::DeviceAccessToken,
                lookup_type: LookupType::ByOther("access token".to_string()),
            })?;

        // we have to construct the authz resource after the lookup because we don't
        // have its ID on hand until then
        let authz_token = authz::DeviceAccessToken::new(
            authz::FLEET,
            db_token.id(),
            LookupType::ById(db_token.id().into_untyped_uuid()),
        );

        // This check might seem superfluous, but (for now at least) only the
        // fleet external authenticator user can read a token, so this is
        // essentially checking that the opctx comes from that user.
        opctx.authorize(authz::Action::Read, &authz_token).await?;

        Ok((authz_token, db_token))
    }

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

        use nexus_db_schema::schema::device_auth_request::dsl;
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

        use nexus_db_schema::schema::device_auth_request::dsl as request_dsl;
        let delete_request = diesel::delete(request_dsl::device_auth_request)
            .filter(request_dsl::user_code.eq(authz_request.id()));

        use nexus_db_schema::schema::device_access_token::dsl as token_dsl;
        let insert_token = diesel::insert_into(token_dsl::device_access_token)
            .values(access_token)
            .returning(DeviceAccessToken::as_returning());

        #[derive(Debug)]
        enum TokenGrantError {
            RequestNotFound,
            TooManyRequests,
        }

        let err = nexus_db_errors::OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("device_access_token_create")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let insert_token = insert_token.clone();
                let delete_request = delete_request.clone();
                async move {
                    match delete_request.execute_async(&conn).await? {
                        0 => Err(err.bail(TokenGrantError::RequestNotFound)),
                        1 => Ok(insert_token.get_result_async(&conn).await?),
                        _ => Err(err.bail(TokenGrantError::TooManyRequests)),
                    }
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        TokenGrantError::RequestNotFound => {
                            Error::ObjectNotFound {
                                type_name: ResourceType::DeviceAuthRequest,
                                lookup_type: LookupType::ByCompositeId(
                                    authz_request.id(),
                                ),
                            }
                        }
                        TokenGrantError::TooManyRequests => {
                            Error::internal_error("unexpectedly found multiple device auth requests for the same user code")
                        }
                    }
                } else {
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
        use nexus_db_schema::schema::device_access_token::dsl;
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

    pub async fn device_access_tokens_list(
        &self,
        opctx: &OpContext,
        authz_user: &authz::SiloUser,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<DeviceAccessToken> {
        // TODO: this authz check can't be right can it? or at least, we
        // should probably handle this explicitly at the policy level
        opctx.authorize(authz::Action::ListChildren, authz_user).await?;

        use nexus_db_schema::schema::device_access_token::dsl;
        paginated(dsl::device_access_token, dsl::id, &pagparams)
            .filter(dsl::silo_user_id.eq(authz_user.id()))
            // we don't have time_deleted on tokens. unfortunately this is not
            // indexed well. maybe it can be!
            .filter(
                dsl::time_expires
                    .is_null()
                    .or(dsl::time_expires.gt(Utc::now())),
            )
            // TODO: what if we used a different model struct here so we're not
            // pulling less out of the DB and it's harder to accidentally return
            // the token itself
            .select(DeviceAccessToken::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn device_access_token_delete(
        &self,
        opctx: &OpContext,
        authz_user: &authz::SiloUser,
        token_id: Uuid,
    ) -> Result<(), Error> {
        // TODO: surely this is the wrong permission
        opctx.authorize(authz::Action::Modify, authz_user).await?;

        use nexus_db_schema::schema::device_access_token::dsl;
        let num_deleted = diesel::delete(dsl::device_access_token)
            .filter(dsl::id.eq(token_id))
            .filter(dsl::silo_user_id.eq(authz_user.id()))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if num_deleted == 0 {
            return Err(Error::not_found_by_id(
                ResourceType::DeviceAccessToken,
                &token_id,
            ));
        }

        Ok(())
    }
}
