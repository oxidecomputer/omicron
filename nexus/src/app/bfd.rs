// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use mg_admin_client::types::BfdPeerState;
use nexus_db_queries::context::OpContext;
use nexus_types::external_api::bfd;
use nexus_types::external_api::networking;
use omicron_common::api::external::Error;
use sled_agent_types::early_networking::BfdMode;
use sled_agent_types::early_networking::SwitchSlot;

impl super::Nexus {
    pub async fn bfd_enable(
        &self,
        opctx: &OpContext,
        session: networking::BfdSessionEnable,
    ) -> Result<(), Error> {
        // add the bfd session to the db and trigger the bfd manager to handle
        // the reset
        self.datastore().bfd_session_create(opctx, &session).await?;
        self.background_tasks.activate(&self.background_tasks.task_bfd_manager);
        // for timely propagation to bootstore
        self.background_tasks
            .activate(&self.background_tasks.task_switch_port_settings_manager);
        Ok(())
    }

    pub async fn bfd_disable(
        &self,
        opctx: &OpContext,
        session: networking::BfdSessionDisable,
    ) -> Result<(), Error> {
        // remove the bfd session from the db and trigger the bfd manager to
        // handle the reset
        self.datastore().bfd_session_delete(opctx, &session).await?;
        self.background_tasks.activate(&self.background_tasks.task_bfd_manager);
        // for timely propagation to bootstore
        self.background_tasks
            .activate(&self.background_tasks.task_switch_port_settings_manager);
        Ok(())
    }

    pub async fn bfd_status(
        &self,
        _opctx: &OpContext,
    ) -> Result<networking::SwitchResults<bfd::BfdPeerStatuses>, Error> {
        // ask each rack switch about all its BFD sessions. This will need to
        // be updated for multirack.
        let mg_clients = self.mg_clients().await.map_err(|err| {
            Error::internal_error(&format!("failed to get mg clients: {err}"))
        })?;
        let query = |switch_slot| {
            let mg_clients = &mg_clients;
            async move {
                // If we only have one scrimlet, we won't have an entry in
                // `mg_clients` for one of the switch locations. Log that, but
                // continue so we can still report status from whichever switch we
                // do have.
                let Some(mg_client) = mg_clients.get(&switch_slot) else {
                    warn!(
                        self.log, "no mgd client found for switch slot";
                        "switch-slot" => ?switch_slot,
                    );
                    return networking::SwitchResult::Unavailable {
                        reason:
                            networking::SwitchUnavailableReason::MgdUnresolved,
                    };
                };
                match mg_client.get_bfd_peers().await {
                    Ok(status) => networking::SwitchResult::Available {
                        value: bfd::BfdPeerStatuses(
                            status
                                .into_inner()
                                .iter()
                                .map(|info| {
                                    bfd::BfdPeerStatus {
                    peer: info.config.peer,
                    state: match info.state {
                        BfdPeerState::Up => bfd::BfdState::Up,
                        BfdPeerState::Down => bfd::BfdState::Down,
                        BfdPeerState::Init => bfd::BfdState::Init,
                        BfdPeerState::AdminDown => bfd::BfdState::AdminDown,
                    },
                    local: Some(info.config.listen),
                    detection_threshold: info.config.detection_threshold.into(),
                    required_rx: info.config.required_rx,
                    mode: match info.config.mode {
                        mg_admin_client::types::SessionMode::SingleHop => {
                            BfdMode::SingleHop
                        }
                        mg_admin_client::types::SessionMode::MultiHop => {
                            BfdMode::MultiHop
                        }
                    },
                        }
                                })
                                .collect(),
                        ),
                    },
                    Err(err) => {
                        error!(
                            self.log, "failed to get BFD peers";
                            "switch-slot" => ?switch_slot,
                            "error" => %err,
                        );
                        networking::SwitchResult::Unavailable {
                            reason:
                                networking::SwitchUnavailableReason::QueryFailed,
                        }
                    }
                }
            }
        };
        Ok(networking::SwitchResults {
            switch0: query(SwitchSlot::Switch0).await,
            switch1: query(SwitchSlot::Switch1).await,
        })
    }
}
