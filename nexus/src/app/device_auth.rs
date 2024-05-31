// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! OAuth 2.0 Device Authorization Grant
//!
//! This is an OAuth 2.0 "flow" that allows devices or clients with
//! restricted internet access or that lack a full-featured browser to
//! obtain an access token. For our use-case, the client will usually
//! be the Oxide CLI or an API client library. Such a client will not
//! typically be able to complete e.g., a SAML login flow directly.
//!
//! The protocol is completely described in RFC 8628, which augments
//! the general OAuth 2.0 framework specified in RFC 6749. Slightly
//! simplified, the overall flow looks like this:
//!
//! 1. The client contacts the server to request a token. The client
//!    provides a globally unique `client_id` to identify itself, but
//!    *not* a user identification; at this point, the server does not
//!    know *who* (which actor or user) is requesting a token.
//! 2. The server responds with a short, simple URL to a login page
//!    and a short, fresh `user_code` to enter or verify on login.
//!    It also generates and sends a globally unique `device_code`,
//!    which the user never sees or uses directly.
//! 3. The client starts polling the server for a token, supplying
//!    its `client_id` and the `device_code` the server sent in step 2.
//! 4. The user visits the login page in their browser, either manually
//!    or by having the client open a browser window if possible. In the
//!    automatic case, it may supply the `user_code` as a query parameter.
//! 5. The user logs in using their configured IdP, then enters or verifies
//!    the `user_code`.
//! 6. On successful login, the server responds to the poll started
//!    in step 3 with a freshly granted access token.
//!
//! Note that in this flow, there are actually two distinct sets of
//! connections made to the server: by the client itself, and by the
//! browser on behalf of the client. It is important to keep this distinction
//! in mind when working on this code.
//!
//! Also note that the kind of token granted at the end of a successful
//! flow is not specified by the standard. It may be a long-lived token,
//! or a refresh token which may in turn be used to request a short-lived
//! access token; it may also be structured (like a JWT) or random.
//! In the current implementation, we use long-lived random tokens,
//! but that may change in the future.

use crate::external_api::device_auth::DeviceAccessTokenResponse;
use nexus_db_queries::authn::{Actor, Reason};
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::model::{DeviceAccessToken, DeviceAuthRequest};
use nexus_db_queries::{authz, db};

use omicron_common::api::external::{CreateResult, Error};

use chrono::Utc;
use std::sync::Arc;
use uuid::Uuid;

/// Application level support for OAuth 2.0 Device Authorization
#[derive(Clone)]
pub struct DeviceAuth {
    datastore: Arc<db::DataStore>,
}

impl DeviceAuth {
    pub fn new(datastore: Arc<db::DataStore>) -> DeviceAuth {
        DeviceAuth { datastore }
    }

    /// Start a device authorization grant flow.
    /// Corresponds to steps 1 & 2 in the flow description above.
    pub(crate) async fn device_auth_request_create(
        &self,
        opctx: &OpContext,
        client_id: Uuid,
    ) -> CreateResult<DeviceAuthRequest> {
        // TODO-correctness: the `user_code` generated for a new request
        // is used as a primary key, but may potentially collide with an
        // existing outstanding request. So we should retry some (small)
        // number of times if inserting the new request fails.
        let auth_request = DeviceAuthRequest::new(client_id);
        self.datastore.device_auth_request_create(opctx, auth_request).await
    }

    /// Verify a device authorization grant, and delete the authorization
    /// request so that at most one token will be granted per request.
    /// Invoked in response to a request from the browser, not the client.
    /// Corresponds to step 5 in the flow description above.
    pub(crate) async fn device_auth_request_verify(
        &self,
        opctx: &OpContext,
        user_code: String,
        silo_user_id: Uuid,
    ) -> CreateResult<DeviceAccessToken> {
        let (.., authz_request, db_request) =
            LookupPath::new(opctx, &self.datastore)
                .device_auth_request(&user_code)
                .fetch()
                .await?;

        let (.., authz_user) = LookupPath::new(opctx, &self.datastore)
            .silo_user_id(silo_user_id)
            .lookup_for(authz::Action::CreateChild)
            .await?;
        assert_eq!(authz_user.id(), silo_user_id);

        // Create an access token record.
        let token = DeviceAccessToken::new(
            db_request.client_id,
            db_request.device_code,
            db_request.time_created,
            silo_user_id,
        );

        if db_request.time_expires < Utc::now() {
            // Store the expired token anyway so that the client
            // can get a proper "denied" message on its next poll.
            let token = token.expires(db_request.time_expires);
            self.datastore
                .device_access_token_create(
                    opctx,
                    &authz_request,
                    &authz_user,
                    token,
                )
                .await?;
            Err(Error::invalid_request("device authorization request expired"))
        } else {
            self.datastore
                .device_access_token_create(
                    opctx,
                    &authz_request,
                    &authz_user,
                    token,
                )
                .await
        }
    }

    /// Look up a possibly-not-yet-granted device access token.
    /// Corresponds to steps 3 & 6 in the flow description above.
    pub(crate) async fn device_access_token_fetch(
        &self,
        opctx: &OpContext,
        client_id: Uuid,
        device_code: String,
    ) -> CreateResult<DeviceAccessTokenResponse> {
        use DeviceAccessTokenResponse::*;
        match self
            .datastore
            .device_access_token_fetch(opctx, client_id, device_code)
            .await
        {
            Ok(token) => Ok(Granted(token)),
            Err(_) => Ok(Pending),
            // TODO: TokenResponse::Denied
        }
    }

    /// Look up the actor for which a token was granted.
    /// Corresponds to a request *after* completing the flow above.
    pub(crate) async fn device_access_token_actor(
        &self,
        opctx: &OpContext,
        token: String,
    ) -> Result<Actor, Reason> {
        let (.., db_access_token) = LookupPath::new(opctx, &self.datastore)
            .device_access_token(&token)
            .fetch()
            .await
            .map_err(|e| match e {
                Error::ObjectNotFound { .. } => Reason::UnknownActor {
                    actor: "from device access token".to_string(),
                },
                e => Reason::UnknownError { source: e },
            })?;

        let silo_user_id = db_access_token.silo_user_id;
        let (.., db_silo_user) = LookupPath::new(opctx, &self.datastore)
            .silo_user_id(silo_user_id)
            .fetch()
            .await
            .map_err(|e| match e {
                Error::ObjectNotFound { .. } => {
                    Reason::UnknownActor { actor: silo_user_id.to_string() }
                }
                e => Reason::UnknownError { source: e },
            })?;
        let silo_id = db_silo_user.silo_id;

        Ok(Actor::SiloUser { silo_user_id, silo_id })
    }
}
