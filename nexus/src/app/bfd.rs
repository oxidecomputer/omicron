// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::app::background::BackgroundTasks;
use crate::external_api::params;
use internal_dns::resolver::Resolver;
use mg_admin_client::types::BfdPeerState;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_types::external_api::shared::{BfdState, BfdStatus};
use omicron_common::api::{external::Error, internal::shared::SwitchLocation};
use slog::Logger;
use std::sync::Arc;

/// Application level operations for bidirectional forwarding detection
pub struct Bfd {
    log: Logger,
    datastore: Arc<db::DataStore>,
    background_tasks: Arc<BackgroundTasks>,
    internal_resolver: Resolver,
}

impl Bfd {
    pub fn new(
        log: Logger,
        datastore: Arc<db::DataStore>,
        background_tasks: Arc<BackgroundTasks>,
        internal_resolver: Resolver,
    ) -> Bfd {
        Bfd { log, datastore, background_tasks, internal_resolver }
    }

    async fn mg_client_for_switch_location(
        &self,
        switch: SwitchLocation,
    ) -> Result<mg_admin_client::Client, Error> {
        let mg_client: mg_admin_client::Client =
            super::mg_clients(&self.internal_resolver, &self.log)
                .await
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "failed to get mg clients: {e}"
                    ))
                })?
                .get(&switch)
                .ok_or_else(|| {
                    Error::not_found_by_name(
                        omicron_common::api::external::ResourceType::Switch,
                        &switch.to_string().parse().unwrap(),
                    )
                })?
                .clone();

        Ok(mg_client)
    }

    pub async fn bfd_enable(
        &self,
        opctx: &OpContext,
        session: params::BfdSessionEnable,
    ) -> Result<(), Error> {
        // add the bfd session to the db and trigger the bfd manager to handle
        // the reset
        self.datastore.bfd_session_create(opctx, &session).await?;
        self.background_tasks
            .driver
            .activate(&self.background_tasks.bfd_manager);
        // for timely propagation to bootstore
        self.background_tasks
            .driver
            .activate(&self.background_tasks.task_switch_port_settings_manager);
        Ok(())
    }

    pub async fn bfd_disable(
        &self,
        opctx: &OpContext,
        session: params::BfdSessionDisable,
    ) -> Result<(), Error> {
        // remove the bfd session from the db and trigger the bfd manager to
        // handle the reset
        self.datastore.bfd_session_delete(opctx, &session).await?;
        self.background_tasks
            .driver
            .activate(&self.background_tasks.bfd_manager);
        // for timely propagation to bootstore
        self.background_tasks
            .driver
            .activate(&self.background_tasks.task_switch_port_settings_manager);
        Ok(())
    }

    pub async fn bfd_status(
        &self,
        _opctx: &OpContext,
    ) -> Result<Vec<BfdStatus>, Error> {
        // ask each rack switch about all its BFD sessions. This will need to
        // be updated for multirack.
        let mut result = Vec::new();
        for s in &[SwitchLocation::Switch0, SwitchLocation::Switch1] {
            let mg_client = self.mg_client_for_switch_location(*s).await?;
            let status = mg_client
                .get_bfd_peers()
                .await
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "maghemite get bfd peers: {e}"
                    ))
                })?
                .into_inner();

            for info in status.iter() {
                result.push(BfdStatus {
                    peer: info.config.peer,
                    state: match info.state {
                        BfdPeerState::Up => BfdState::Up,
                        BfdPeerState::Down => BfdState::Down,
                        BfdPeerState::Init => BfdState::Init,
                        BfdPeerState::AdminDown => BfdState::AdminDown,
                    },
                    switch: s.to_string().parse().unwrap(),
                    local: Some(info.config.listen),
                    detection_threshold: info.config.detection_threshold,
                    required_rx: info.config.required_rx,
                    mode: match info.config.mode {
                        mg_admin_client::types::SessionMode::SingleHop => {
                            omicron_common::api::external::BfdMode::SingleHop
                        }
                        mg_admin_client::types::SessionMode::MultiHop => {
                            omicron_common::api::external::BfdMode::MultiHop
                        }
                    },
                })
            }
        }
        Ok(result)
    }
}
