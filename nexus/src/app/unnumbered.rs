// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_db_queries::context::OpContext;
use nexus_types::external_api::networking::{
    SwitchError, SwitchResult, SwitchResults, SwitchUnnumberedInterface,
    UnnumberedInterfaces, UnnumberedManagerState,
};
use omicron_common::api::external::Error;
use omicron_common::tfport::TfportInterfaceName;
use sled_agent_types::early_networking::SwitchSlot;

impl super::Nexus {
    pub async fn bgp_unnumbered_manager_status(
        &self,
        _opctx: &OpContext,
    ) -> Result<SwitchResults<UnnumberedManagerState>, Error> {
        // Ask each switch about the BGP unnumbered interfaces it manages.
        let mg_clients = self.mg_clients().await.map_err(|err| {
            Error::internal_error(&format!("failed to get mg clients: {err}"))
        })?;
        let query = |switch_slot| {
            let mg_clients = &mg_clients;
            async move {
                // Log an error if we only have one scrimlet, but keep going.
                // We still want to return anything we're able to collect.
                let Some(mg_client) = mg_clients.get(&switch_slot) else {
                    warn!(
                        self.log, "no mgd client found for switch slot";
                        "switch-slot" => ?switch_slot,
                    );
                    return SwitchResult::Err {
                        error: SwitchError::MgdUnresolved,
                    };
                };
                match mg_client.get_bgp_unnumbered_manager_state().await {
                    Ok(status) => {
                        SwitchResult::Ok { value: status.into_inner().into() }
                    }
                    Err(err) => {
                        error!(
                            self.log,
                            "failed to get BGP unnumbered manager state";
                            "switch-slot" => ?switch_slot,
                            "error" => %err,
                        );
                        SwitchResult::Err { error: err.into() }
                    }
                }
            }
        };
        Ok(SwitchResults {
            switch0: query(SwitchSlot::Switch0).await,
            switch1: query(SwitchSlot::Switch1).await,
        })
    }

    pub async fn bgp_unnumbered_interfaces(
        &self,
        _opctx: &OpContext,
    ) -> Result<SwitchResults<UnnumberedInterfaces>, Error> {
        // Ask each switch about the BGP unnumbered interfaces it manages.
        let mg_clients = self.mg_clients().await.map_err(|err| {
            Error::internal_error(&format!("failed to get mg clients: {err}"))
        })?;
        let query = |switch_slot| {
            let mg_clients = &mg_clients;
            async move {
                // Log an error if we only have one scrimlet, but keep going.
                // We still want to return anything we're able to collect.
                let Some(mg_client) = mg_clients.get(&switch_slot) else {
                    warn!(
                        self.log, "no mgd client found for switch slot";
                        "switch-slot" => ?switch_slot,
                    );
                    return SwitchResult::Err {
                        error: SwitchError::MgdUnresolved,
                    };
                };
                match mg_client.get_bgp_unnumbered_interfaces().await {
                    Ok(interfaces) => SwitchResult::Ok {
                        value: UnnumberedInterfaces(
                            interfaces
                                .into_inner()
                                .into_iter()
                                .map(Into::into)
                                .collect(),
                        ),
                    },
                    Err(err) => {
                        error!(
                            self.log, "failed to get BGP unnumbered interfaces";
                            "switch-slot" => ?switch_slot,
                            "error" => %err,
                        );
                        SwitchResult::Err { error: err.into() }
                    }
                }
            }
        };
        Ok(SwitchResults {
            switch0: query(SwitchSlot::Switch0).await,
            switch1: query(SwitchSlot::Switch1).await,
        })
    }

    pub async fn bgp_unnumbered_interface(
        &self,
        _opctx: &OpContext,
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

        let interface_name =
            TfportInterfaceName::from_port_name(&interface_name)
                .map_err(|err| Error::invalid_request(&err.to_string()))?;
        let interface = mg_client
            .get_bgp_unnumbered_interface_detail(interface_name.as_str())
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
