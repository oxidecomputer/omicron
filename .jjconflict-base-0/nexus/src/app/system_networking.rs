// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fleet-wide networking settings.

use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_types::external_api::system_networking;
use omicron_common::api::external::Error;

impl super::Nexus {
    pub(crate) async fn system_networking_settings_view(
        &self,
        opctx: &OpContext,
    ) -> Result<system_networking::SystemNetworkingSettings, Error> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let settings =
            self.db_datastore.system_networking_settings_view(opctx).await?;
        Ok(system_networking::SystemNetworkingSettings {
            external_jumbo_frames_opt_in_enabled: settings
                .external_jumbo_frames_opt_in_enabled,
        })
    }

    pub(crate) async fn system_networking_settings_update(
        &self,
        opctx: &OpContext,
        params: &system_networking::SystemNetworkingSettingsUpdate,
    ) -> Result<system_networking::SystemNetworkingSettings, Error> {
        let settings = self
            .db_datastore
            .system_networking_settings_update(
                opctx,
                params.external_jumbo_frames_opt_in_enabled,
            )
            .await?;
        Ok(system_networking::SystemNetworkingSettings {
            external_jumbo_frames_opt_in_enabled: settings
                .external_jumbo_frames_opt_in_enabled,
        })
    }
}
