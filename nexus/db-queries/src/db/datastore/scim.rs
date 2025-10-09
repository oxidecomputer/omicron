// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to SCIM

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::model::ScimClientBearerToken;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use rand::{RngCore, SeedableRng, rngs::StdRng};
use uuid::Uuid;

// XXX this is the same as generate_session_token!
fn generate_scim_client_bearer_token() -> String {
    let mut rng = StdRng::from_os_rng();
    let mut random_bytes: [u8; 20] = [0; 20];
    rng.fill_bytes(&mut random_bytes);
    hex::encode(random_bytes)
}

impl DataStore {
    // SCIM tokens

    pub async fn scim_idp_get_tokens(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
    ) -> ListResultVec<ScimClientBearerToken> {
        opctx.authorize(authz::Action::ListChildren, authz_silo).await?;

        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::scim_client_bearer_token::dsl;
        let tokens = dsl::scim_client_bearer_token
            .filter(dsl::silo_id.eq(authz_silo.id()))
            .filter(dsl::time_deleted.is_null())
            .select(ScimClientBearerToken::as_select())
            .load_async::<ScimClientBearerToken>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(tokens)
    }

    pub async fn scim_idp_create_token(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
    ) -> CreateResult<ScimClientBearerToken> {
        opctx.authorize(authz::Action::CreateChild, authz_silo).await?;

        let conn = self.pool_connection_authorized(opctx).await?;

        let new_token = ScimClientBearerToken {
            id: Uuid::new_v4(),
            time_created: Utc::now(),
            time_deleted: None,
            // TODO: allow setting an expiry? have a silo default?
            time_expires: None,
            silo_id: authz_silo.id(),
            bearer_token: generate_scim_client_bearer_token(),
        };

        use nexus_db_schema::schema::scim_client_bearer_token::dsl;
        diesel::insert_into(dsl::scim_client_bearer_token)
            .values(new_token.clone())
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(new_token)
    }

    pub async fn scim_idp_get_token_by_id(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        id: Uuid,
    ) -> LookupResult<Option<ScimClientBearerToken>> {
        opctx.authorize(authz::Action::ListChildren, authz_silo).await?;

        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::scim_client_bearer_token::dsl;
        let token = dsl::scim_client_bearer_token
            .filter(dsl::silo_id.eq(authz_silo.id()))
            .filter(dsl::id.eq(id))
            .filter(dsl::time_deleted.is_null())
            .select(ScimClientBearerToken::as_select())
            .first_async::<ScimClientBearerToken>(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(token)
    }

    pub async fn scim_idp_delete_token_by_id(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        id: Uuid,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, authz_silo).await?;

        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::scim_client_bearer_token::dsl;
        diesel::update(dsl::scim_client_bearer_token)
            .filter(dsl::silo_id.eq(authz_silo.id()))
            .filter(dsl::id.eq(id))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(Utc::now()))
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    /// SCIM clients should _not_ authenticate to an Actor in the traditional
    /// sense: they shouldn't have permission on any resources under a Silo,
    /// only enough to CRUD Silo users and groups.
    pub async fn scim_idp_lookup_token_by_bearer(
        &self,
        bearer_token: String,
    ) -> LookupResult<Option<ScimClientBearerToken>> {
        let conn = self.pool_connection_unauthorized().await?;

        use nexus_db_schema::schema::scim_client_bearer_token::dsl;
        let maybe_token = dsl::scim_client_bearer_token
            .filter(dsl::bearer_token.eq(bearer_token))
            .filter(dsl::time_deleted.is_null())
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(maybe_token)
    }
}
