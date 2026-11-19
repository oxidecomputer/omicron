// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `wicket` SSH authentication control

use nexus_auth::context::OpContext;
use nexus_db_queries::db::datastore::WicketSshAuthGenerationDescription;
use nexus_types::external_api::wicket_ssh_auth::WicketSshAuth;
use nexus_types::external_api::wicket_ssh_auth::WicketSshAuthCreate;
use nexus_types::external_api::wicket_ssh_auth::WicketSshAuthStatus;
use nexus_types::external_api::wicket_ssh_auth::WicketSshAuthStatusSled;
use nexus_types::external_api::wicket_ssh_auth::WicketSshAuthStatusSledLatest;
use nexus_types::external_api::wicket_ssh_auth::WicketSshAuthStatusTimeCollected;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupResult;
use omicron_uuid_kinds::GenericUuid;

impl super::Nexus {
    pub async fn wicket_ssh_auth_insert(
        &self,
        opctx: &OpContext,
        params: WicketSshAuthCreate,
    ) -> CreateResult<WicketSshAuth> {
        self.datastore().wicket_ssh_auth_insert(opctx, params).await
    }

    pub async fn wicket_ssh_auth_view(
        &self,
        opctx: &OpContext,
        generation: WicketSshAuthGenerationDescription,
    ) -> LookupResult<WicketSshAuth> {
        self.datastore().wicket_ssh_auth_read(opctx, generation).await
    }

    pub async fn wicket_ssh_auth_status(
        &self,
        opctx: &OpContext,
    ) -> Result<WicketSshAuthStatus, Error> {
        // Load the current wicket SSH auth setting; this also performs an authz
        // check on opctx. If it's okay to read this setting, it's okay to do
        // the rest of this method (check inventory and report replication
        // status).
        let current_wicket_ssh_auth = self
            .datastore()
            .wicket_ssh_auth_read(
                opctx,
                WicketSshAuthGenerationDescription::Latest,
            )
            .await?;

        let Some(inventory) = self.inventory_load_rx().borrow().clone() else {
            return Err(Error::unavail("No inventory collection found"));
        };

        // Get the list of sleds from the current target blueprint; this way we
        // can report "unknown" for sleds that aren't in the latest inventory
        // collection at all.
        let blueprint = self.blueprint_current_loaded()?.blueprint;

        let sleds = blueprint
            .active_sled_configs()
            .map(|(sled_id, bp_sled_config)| {
                let latest = match inventory.sled_agents.get(&sled_id) {
                    None => WicketSshAuthStatusSledLatest::Unknown,
                    Some(inv_sled_agent) => {
                        WicketSshAuthStatusSledLatest::Known {
                            persisted_generation: inv_sled_agent
                                .ledgered_sled_config
                                .as_ref()
                                .map(|config| {
                                    config.wicket_ssh_auth.generation
                                }),
                            switch_zone_status: inv_sled_agent
                                .wicket_ssh_auth_status
                                .clone(),
                        }
                    }
                };

                let status = WicketSshAuthStatusSled {
                    configured_generation: bp_sled_config
                        .wicket_ssh_auth
                        .generation,
                    latest,
                };

                (sled_id.into_untyped_uuid(), status)
            })
            .collect();

        Ok(WicketSshAuthStatus {
            current_generation: current_wicket_ssh_auth.generation,
            status_time_collected: WicketSshAuthStatusTimeCollected {
                started: inventory.time_started,
                done: inventory.time_done,
            },
            sleds,
        })
    }
}
