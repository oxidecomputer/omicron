// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods for the POC OIDC signing key.
//!
//! POC: there is at most one live (`time_deleted IS NULL`) signing key. The
//! mint path reads that key from here per request.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::model::OidcSigningKey;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;

impl DataStore {
    /// Store a new OIDC signing key. Fails with a conflict if a live key with
    /// the same `kid` already exists; callers should additionally ensure that
    /// at most one live key exists at a time (see [`Self::oidc_signing_key_live`]).
    pub async fn oidc_signing_key_create(
        &self,
        opctx: &OpContext,
        key: OidcSigningKey,
    ) -> CreateResult<OidcSigningKey> {
        use nexus_db_schema::schema::oidc_signing_key::dsl;

        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        diesel::insert_into(dsl::oidc_signing_key)
            .values(key)
            .returning(OidcSigningKey::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Fetch the single live (non-deleted) OIDC signing key, if one exists.
    pub async fn oidc_signing_key_live(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<Option<OidcSigningKey>> {
        use nexus_db_schema::schema::oidc_signing_key::dsl;

        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        dsl::oidc_signing_key
            .filter(dsl::time_deleted.is_null())
            .order(dsl::time_created.desc())
            .select(OidcSigningKey::as_select())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List all live (non-deleted) OIDC signing keys.
    pub async fn oidc_signing_key_list(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<OidcSigningKey> {
        use nexus_db_schema::schema::oidc_signing_key::dsl;

        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        dsl::oidc_signing_key
            .filter(dsl::time_deleted.is_null())
            .order(dsl::time_created.desc())
            .select(OidcSigningKey::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Soft-delete the live OIDC signing key with the given `kid`.
    pub async fn oidc_signing_key_delete(
        &self,
        opctx: &OpContext,
        kid: &str,
    ) -> DeleteResult {
        use nexus_db_schema::schema::oidc_signing_key::dsl;

        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        let now = Utc::now();
        diesel::update(dsl::oidc_signing_key)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::kid.eq(kid.to_string()))
            .set(dsl::time_deleted.eq(now))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }
}
