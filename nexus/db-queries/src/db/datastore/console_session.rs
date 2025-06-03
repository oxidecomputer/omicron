// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to [`ConsoleSession`]s.

use super::DataStore;
use crate::authn;
use crate::authz;
use crate::context::OpContext;
use crate::db::model::ConsoleSession;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_lookup::LookupPath;
use nexus_db_schema::schema::console_session;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_uuid_kinds::GenericUuid;

impl DataStore {
    /// Look up session by token. The token is a kind of password, so simply
    /// having the token _is_ in a sense the primary authz check here.
    ///
    /// We need to define this lookup function manually because `token` is not
    /// the primary key on the session (sessions have IDs), so we can't use the
    /// automatically-generated lookup methods we use for IDs or names.
    pub async fn session_lookup_by_token(
        &self,
        opctx: &OpContext,
        token: String,
    ) -> LookupResult<(authz::ConsoleSession, ConsoleSession)> {
        let db_session = console_session::table
            .filter(console_session::token.eq(token))
            .select(ConsoleSession::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|_e| Error::ObjectNotFound {
                type_name: ResourceType::ConsoleSession,
                lookup_type: LookupType::ByOther("session token".to_string()),
            })?;

        // we have to construct the authz resource after the lookup because we don't
        // have its ID on hand until then
        let authz_session = authz::ConsoleSession::new(
            authz::FLEET,
            db_session.id(),
            LookupType::ById(db_session.id().into_untyped_uuid()),
        );

        // This check might seem superfluous, but (for now at least) only the
        // fleet external authenticator user can read a session, so this is
        // essentially checking that the opctx comes from that user.
        opctx.authorize(authz::Action::Read, &authz_session).await?;

        Ok((authz_session, db_session))
    }

    // TODO-correctness: fix session method errors. the map_errs turn all errors
    // into 500s, most notably (and most frequently) session not found. they
    // don't end up as 500 in the http response because they get turned into a
    // 4xx error by calling code, the session cookie authn scheme. this is
    // necessary for now in order to avoid the possibility of leaking out a
    // too-friendly 404 to the client. once datastore has its own error type and
    // the conversion to serializable user-facing errors happens elsewhere (see
    // issue #347) these methods can safely return more accurate errors, and
    // showing/hiding that info as appropriate will be handled higher up
    // TODO-correctness this may apply at the Nexus level as well.

    pub async fn session_create(
        &self,
        opctx: &OpContext,
        session: ConsoleSession,
    ) -> CreateResult<ConsoleSession> {
        opctx
            .authorize(authz::Action::CreateChild, &authz::CONSOLE_SESSION_LIST)
            .await?;

        use nexus_db_schema::schema::console_session::dsl;

        diesel::insert_into(dsl::console_session)
            .values(session)
            .returning(ConsoleSession::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error creating session: {:?}",
                    e
                ))
            })
    }

    pub async fn session_update_last_used(
        &self,
        opctx: &OpContext,
        authz_session: &authz::ConsoleSession,
    ) -> UpdateResult<authn::ConsoleSessionWithSiloId> {
        opctx.authorize(authz::Action::Modify, authz_session).await?;

        use nexus_db_schema::schema::console_session::dsl;
        let console_session = diesel::update(dsl::console_session)
            .filter(dsl::id.eq(authz_session.id().into_untyped_uuid()))
            .set((dsl::time_last_used.eq(Utc::now()),))
            .returning(ConsoleSession::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error renewing session: {:?}",
                    e
                ))
            })?;

        let (.., db_silo_user) = LookupPath::new(opctx, self)
            .silo_user_id(console_session.silo_user_id)
            .fetch()
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error fetching silo id: {:?}",
                    e
                ))
            })?;

        Ok(authn::ConsoleSessionWithSiloId {
            console_session,
            silo_id: db_silo_user.silo_id,
        })
    }

    // putting "hard" in the name because we don't do this with any other model
    pub async fn session_hard_delete(
        &self,
        opctx: &OpContext,
        authz_session: &authz::ConsoleSession,
    ) -> DeleteResult {
        // We don't do a typical authz check here.  Instead, knowing that every
        // user is allowed to delete their own session, the query below filters
        // on the session's silo_user_id matching the current actor's id.
        //
        // We could instead model this more like other authz checks.  That would
        // involve fetching the session record from the database, storing the
        // associated silo_user_id into the `authz::ConsoleSession`, and having
        // an Oso rule saying you can delete a session whose associated silo
        // user matches the authenticated actor.  This would be a fair bit more
        // complicated and more work at runtime work than what we're doing here.
        // The tradeoff is that we're effectively encoding policy here, but it
        // seems worth it in this case.
        let actor = opctx
            .authn
            .actor_required()
            .internal_context("deleting current user's session")?;

        // This check shouldn't be required in that there should be no overlap
        // between silo user ids and other types of identity ids.  But it's easy
        // to check, and if we add another type of Actor, we'll be forced here
        // to consider if they should be able to have console sessions and log
        // out of them.
        let silo_user_id = actor
            .silo_user_id()
            .ok_or_else(|| Error::invalid_request("not a Silo user"))?;

        use nexus_db_schema::schema::console_session::dsl;
        diesel::delete(dsl::console_session)
            .filter(dsl::silo_user_id.eq(silo_user_id))
            .filter(dsl::id.eq(authz_session.id().into_untyped_uuid()))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|_rows_deleted| ())
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error deleting session: {:?}",
                    e
                ))
            })
    }

    pub async fn session_hard_delete_by_token(
        &self,
        opctx: &OpContext,
        token: String,
    ) -> DeleteResult {
        // we don't do an authz check here because the possession of
        // the token is the check
        use nexus_db_schema::schema::console_session;
        diesel::delete(console_session::table)
            .filter(console_session::token.eq(token))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|_rows_deleted| ())
            .map_err(|e| {
                Error::internal_error(&format!(
                    "error deleting session by token: {:?}",
                    e
                ))
            })
    }
}
