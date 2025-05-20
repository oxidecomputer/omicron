use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_model::SiloSettings;
use nexus_db_model::SiloSettingsUpdate;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;

// Directly modeled on settings query functions.

impl DataStore {
    /// Creates new settings for a silo. This is grouped with silo creation
    /// and shouldn't be called outside of that flow.
    ///
    /// An authz check _cannot_ be performed here because the authz initialization
    /// isn't complete and will lead to a db deadlock.
    ///
    /// See <https://github.com/oxidecomputer/omicron/blob/07eb7dafc20e35e44edf429fcbb759cbb33edd5f/nexus/db-queries/src/db/datastore/rack.rs#L407-L410>
    pub async fn silo_settings_create(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        authz_silo: &authz::Silo,
        settings: SiloSettings,
    ) -> Result<(), Error> {
        let silo_id = authz_silo.id();
        use nexus_db_schema::schema::silo_settings;

        diesel::insert_into(silo_settings::table)
            .values(settings)
            .execute_async(conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::SiloSettings,
                        &silo_id.to_string(),
                    ),
                )
            })
            .map(|_| ())
    }

    pub async fn silo_settings_delete(
        &self,
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        authz_silo: &authz::Silo,
    ) -> DeleteResult {
        // Given that the settings right now are somewhat of an extension of the
        // Silo we just check for delete permission on the silo itself.
        opctx.authorize(authz::Action::Delete, authz_silo).await?;

        use nexus_db_schema::schema::silo_settings;
        diesel::delete(silo_settings::table)
            .filter(silo_settings::silo_id.eq(authz_silo.id()))
            .execute_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    pub async fn silo_settings_update(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        updates: SiloSettingsUpdate,
    ) -> UpdateResult<SiloSettings> {
        opctx.authorize(authz::Action::Modify, authz_silo).await?;
        use nexus_db_schema::schema::silo_settings::dsl;
        let silo_id = authz_silo.id();
        diesel::update(dsl::silo_settings)
            .filter(dsl::silo_id.eq(silo_id))
            .set(updates)
            .returning(SiloSettings::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::SiloSettings,
                        &silo_id.to_string(),
                    ),
                )
            })
    }

    pub async fn silo_settings_view(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
    ) -> Result<SiloSettings, Error> {
        // Works for everyone when making a token because everyone can read
        // their own silo. Operators looking at silo settings will have silo
        // read on all silos.
        opctx.authorize(authz::Action::Read, authz_silo).await?;

        use nexus_db_schema::schema::silo_settings::dsl;
        dsl::silo_settings
            .filter(dsl::silo_id.eq(authz_silo.id()))
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
