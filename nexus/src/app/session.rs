// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Console session management.

use hex;
use nexus_db_lookup::LookupPath;
use nexus_db_queries::authn;
use nexus_db_queries::authn::Reason;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::identity::Asset;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::UpdateResult;
use omicron_uuid_kinds::ConsoleSessionUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SiloUserUuid;
use rand::{RngCore, SeedableRng, rngs::StdRng};
use uuid::Uuid;

fn generate_session_token() -> String {
    // TODO: "If getrandom is unable to provide secure entropy this method will panic."
    // Should we explicitly handle that?
    // TODO: store generator somewhere so we don't reseed every time
    let mut rng = StdRng::from_os_rng();
    // OWASP recommends at least 64 bits of entropy, OAuth 2 spec 128 minimum, 160 recommended
    // 20 bytes = 160 bits of entropy
    // TODO: the size should be a constant somewhere, maybe even in config?
    let mut random_bytes: [u8; 20] = [0; 20];
    rng.fill_bytes(&mut random_bytes);
    hex::encode(random_bytes)
}

impl super::Nexus {
    pub(crate) async fn session_create(
        &self,
        opctx: &OpContext,
        user: &db::model::SiloUser,
    ) -> CreateResult<db::model::ConsoleSession> {
        let session =
            db::model::ConsoleSession::new(generate_session_token(), user.id());
        self.db_datastore.session_create(opctx, session).await
    }

    pub(crate) async fn session_fetch(
        &self,
        opctx: &OpContext,
        token: String,
    ) -> LookupResult<authn::ConsoleSessionWithSiloId> {
        let (.., db_session) =
            self.db_datastore.session_lookup_by_token(&opctx, token).await?;

        let (.., db_silo_user) = LookupPath::new(opctx, &self.db_datastore)
            .silo_user_id(db_session.silo_user_id())
            .fetch()
            .await?;

        Ok(authn::ConsoleSessionWithSiloId {
            console_session: db_session,
            silo_id: db_silo_user.silo_id,
            silo_name: "default".parse().unwrap(),
        })
    }

    /// Updates last_used to now.
    pub(crate) async fn session_update_last_used(
        &self,
        opctx: &OpContext,
        id: ConsoleSessionUuid,
    ) -> UpdateResult<authn::ConsoleSessionWithSiloId> {
        let authz_session = authz::ConsoleSession::new(
            authz::FLEET,
            id,
            LookupType::ById(id.into_untyped_uuid()),
        );
        self.db_datastore.session_update_last_used(opctx, &authz_session).await
    }

    pub(crate) async fn session_hard_delete_by_token(
        &self,
        opctx: &OpContext,
        token: String,
    ) -> DeleteResult {
        self.db_datastore.session_hard_delete_by_token(opctx, token).await
    }

    pub(crate) async fn lookup_silo_for_authn(
        &self,
        opctx: &OpContext,
        silo_user_id: SiloUserUuid,
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
                | Error::InsufficientCapacity { .. }
                | Error::TypeVersionMismatch { .. }
                | Error::Conflict { .. }
                | Error::NotFound { .. }
                | Error::Gone => Reason::UnknownError { source: error },
            })?;
        Ok(db_silo_user.silo_id)
    }
}
