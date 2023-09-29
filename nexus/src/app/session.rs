// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Console session management.

use hex;
use nexus_db_queries::authn;
use nexus_db_queries::authn::Reason;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::lookup::LookupPath;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::UpdateResult;
use rand::{rngs::StdRng, RngCore, SeedableRng};
use uuid::Uuid;

fn generate_session_token() -> String {
    // TODO: "If getrandom is unable to provide secure entropy this method will panic."
    // Should we explicitly handle that?
    // TODO: store generator somewhere so we don't reseed every time
    let mut rng = StdRng::from_entropy();
    // OWASP recommends at least 64 bits of entropy, OAuth 2 spec 128 minimum, 160 recommended
    // 20 bytes = 160 bits of entropy
    // TODO: the size should be a constant somewhere, maybe even in config?
    let mut random_bytes: [u8; 20] = [0; 20];
    rng.fill_bytes(&mut random_bytes);
    hex::encode(random_bytes)
}

impl super::Nexus {
    async fn login_allowed(
        &self,
        opctx: &OpContext,
        silo_user_id: Uuid,
    ) -> Result<bool, Error> {
        // Was this silo user deleted?
        let fetch_result = LookupPath::new(opctx, &self.db_datastore)
            .silo_user_id(silo_user_id)
            .fetch()
            .await;

        match fetch_result {
            Err(e) => {
                match e {
                    Error::ObjectNotFound { type_name: _, lookup_type: _ } => {
                        // if the silo user was deleted, they're not allowed to
                        // log in :)
                        return Ok(false);
                    }

                    _ => {
                        return Err(e);
                    }
                }
            }

            Ok(_) => {
                // they're allowed
            }
        }

        Ok(true)
    }

    pub(crate) async fn session_create(
        &self,
        opctx: &OpContext,
        user_id: Uuid,
    ) -> CreateResult<db::model::ConsoleSession> {
        if !self.login_allowed(opctx, user_id).await? {
            return Err(Error::Unauthenticated {
                internal_message: "User not allowed to login".to_string(),
            });
        }

        let session =
            db::model::ConsoleSession::new(generate_session_token(), user_id);

        self.db_datastore.session_create(opctx, session).await
    }

    pub(crate) async fn session_fetch(
        &self,
        opctx: &OpContext,
        token: String,
    ) -> LookupResult<authn::ConsoleSessionWithSiloId> {
        let (.., db_console_session) =
            LookupPath::new(opctx, &self.db_datastore)
                .console_session_token(&token)
                .fetch()
                .await?;

        let (.., db_silo_user) = LookupPath::new(opctx, &self.db_datastore)
            .silo_user_id(db_console_session.silo_user_id)
            .fetch()
            .await?;

        Ok(authn::ConsoleSessionWithSiloId {
            console_session: db_console_session,
            silo_id: db_silo_user.silo_id,
        })
    }

    /// Updates last_used to now.
    pub(crate) async fn session_update_last_used(
        &self,
        opctx: &OpContext,
        token: &str,
    ) -> UpdateResult<authn::ConsoleSessionWithSiloId> {
        let authz_session = authz::ConsoleSession::new(
            authz::FLEET,
            token.to_string(),
            LookupType::ByCompositeId(token.to_string()),
        );
        self.db_datastore.session_update_last_used(opctx, &authz_session).await
    }

    pub(crate) async fn session_hard_delete(
        &self,
        opctx: &OpContext,
        token: &str,
    ) -> DeleteResult {
        let authz_session = authz::ConsoleSession::new(
            authz::FLEET,
            token.to_string(),
            LookupType::ByCompositeId(token.to_string()),
        );
        self.db_datastore.session_hard_delete(opctx, &authz_session).await
    }

    pub(crate) async fn lookup_silo_for_authn(
        &self,
        opctx: &OpContext,
        silo_user_id: Uuid,
    ) -> Result<Uuid, Reason> {
        let (.., db_silo_user) = LookupPath::new(opctx, &self.db_datastore)
            .silo_user_id(silo_user_id)
            .fetch()
            .await
            .map_err(|error| match error {
                Error::ObjectNotFound { .. } => {
                    Reason::UnknownActor { actor: silo_user_id.to_string() }
                }
                Error::ObjectAlreadyExists { .. }
                | Error::InvalidRequest { .. }
                | Error::Unauthenticated { .. }
                | Error::InvalidValue { .. }
                | Error::Forbidden
                | Error::InternalError { .. }
                | Error::ServiceUnavailable { .. }
                | Error::MethodNotAllowed { .. }
                | Error::TypeVersionMismatch { .. }
                | Error::Conflict { .. } => {
                    Reason::UnknownError { source: error }
                }
            })?;
        Ok(db_silo_user.silo_id)
    }
}
