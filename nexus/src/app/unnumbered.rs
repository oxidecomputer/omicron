// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_db_queries::context::OpContext;
use nexus_types::external_api::networking::{
    SwitchUnnumberedInterface, SwitchUnnumberedManagerState,
};
use omicron_common::api::external::Error;
use sled_agent_types::early_networking::SwitchSlot;

fn maghemite_interface_name(interface_name: &str) -> String {
    format!("tfport{interface_name}_0")
}

impl super::Nexus {
    pub async fn bgp_unnumbered_manager_status(
        &self,
        _optctx: &OpContext,
    ) -> Result<Vec<SwitchUnnumberedManagerState>, Error> {
        // Ask each switch about the BGP unnumbered interfaces it manages.
        let mg_clients = self.mg_clients().await.map_err(|err| {
            Error::internal_error(&format!("failed to get mg clients: {err}"))
        })?;
        let mut result = Vec::new();
        for switch_slot in [SwitchSlot::Switch0, SwitchSlot::Switch1] {
            // Log an error if we only have one scrimlet, but keep going.
            // We still want to return anything we're able to collect.
            let Some(mg_client) = mg_clients.get(&switch_slot) else {
                warn!(
                    self.log, "no mgd client found for switch slot";
                    "switch-slot" => ?switch_slot,
                );
                continue;
            };
            let status = mg_client
                .get_bgp_unnumbered_manager_state()
                .await
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "maghemite get BGP unnumbered manager state: {e}"
                    ))
                })?
                .into_inner();

            result.push(SwitchUnnumberedManagerState {
                switch_slot,
                state: status.into(),
            });
        }
        Ok(result)
    }

    pub async fn bgp_unnumbered_interfaces(
        &self,
        _optctx: &OpContext,
    ) -> Result<Vec<SwitchUnnumberedInterface>, Error> {
        // Ask each switch about the BGP unnumbered interfaces it manages.
        let mg_clients = self.mg_clients().await.map_err(|err| {
            Error::internal_error(&format!("failed to get mg clients: {err}"))
        })?;
        let mut result = Vec::new();
        for switch_slot in [SwitchSlot::Switch0, SwitchSlot::Switch1] {
            // Log an error if we only have one scrimlet, but keep going.
            // We still want to return anything we're able to collect.
            let Some(mg_client) = mg_clients.get(&switch_slot) else {
                warn!(
                    self.log, "no mgd client found for switch slot";
                    "switch-slot" => ?switch_slot,
                );
                continue;
            };
            let interfaces = mg_client
                .get_bgp_unnumbered_interfaces()
                .await
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "maghemite get BGP unnumbered interfaces: {e}"
                    ))
                })?
                .into_inner();

            for interface in interfaces {
                result.push(SwitchUnnumberedInterface {
                    switch_slot,
                    interface: interface.into(),
                });
            }
        }
        Ok(result)
    }

    pub async fn bgp_unnumbered_interface(
        &self,
        _optctx: &OpContext,
        switch_slot: SwitchSlot,
        interface_name: String,
    ) -> Result<SwitchUnnumberedInterface, Error> {
        let mg_clients = self.mg_clients().await.map_err(|err| {
            Error::internal_error(&format!("failed to get mg clients: {err}"))
        })?;
        let mg_client = mg_clients.get(&switch_slot).ok_or_else(|| {
            Error::internal_error(&format!(
                "no mgd client found for switch slot {switch_slot:?}"
            ))
        })?;

        let interface_name = maghemite_interface_name(&interface_name);
        let interface = mg_client
            .get_bgp_unnumbered_interface_detail(&interface_name)
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "maghemite get BGP unnumbered interface detail: {e}"
                ))
            })?
            .into_inner();

        Ok(SwitchUnnumberedInterface {
            switch_slot,
            interface: interface.into(),
        })
    }
}
