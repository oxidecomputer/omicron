// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Datastore access for fleet-wide networking settings.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_model::SystemNetworkingSettings;
use nexus_db_model::SystemNetworkingSettingsUpdate;
use omicron_common::api::external::Error;
use omicron_common::api::external::UpdateResult;

impl DataStore {
    /// Read the singleton fleet networking settings row.
    pub async fn system_networking_settings_view(
        &self,
        opctx: &OpContext,
    ) -> Result<SystemNetworkingSettings, Error> {
        /// Any user context can read system network settings, as they
        /// must be consulted for some features like jumbo frames.
        use nexus_db_schema::schema::system_networking_settings::dsl;
        dsl::system_networking_settings
            .filter(dsl::singleton.eq(true))
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Update the singleton fleet networking settings row. Fields left as
    /// `None` in the supplied update are not modified.
    pub async fn system_networking_settings_update(
        &self,
        opctx: &OpContext,
        external_jumbo_frames_opt_in_enabled: bool,
    ) -> UpdateResult<SystemNetworkingSettings> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        let updates = SystemNetworkingSettingsUpdate {
            external_jumbo_frames_opt_in_enabled,
        };

        use nexus_db_schema::schema::system_networking_settings::dsl;
        diesel::update(dsl::system_networking_settings)
            .filter(dsl::singleton.eq(true))
            .set(updates)
            .returning(SystemNetworkingSettings::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
