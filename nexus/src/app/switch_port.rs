// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::external_api::params;
use crate::external_api::shared::SwitchLinkState;
use db::datastore::SwitchPortSettingsCombinedResult;
use dpd_client::types::LinkId;
use dpd_client::types::PortId;
use http::StatusCode;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::UpdatePrecondition;
use nexus_db_queries::db::model::{SwitchPort, SwitchPortSettings};
use omicron_common::api::external::SwitchLocation;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{
    self, CreateResult, DataPageParams, DeleteResult, Error, ListResultVec,
    LookupResult, Name, NameOrId, UpdateResult,
};
use std::sync::Arc;
use uuid::Uuid;

impl super::Nexus {
    pub(crate) async fn switch_port_settings_post(
        self: &Arc<Self>,
        opctx: &OpContext,
        params: params::SwitchPortSettingsCreate,
    ) -> CreateResult<SwitchPortSettingsCombinedResult> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        Self::switch_port_settings_validate(&params)?;

        //TODO race conditions on exists check versus update/create.
        //     Normally I would use a DB lock here, but not sure what
        //     the Omicron way of doing things here is.

        match self
            .db_datastore
            .switch_port_settings_exist(
                opctx,
                params.identity.name.clone().into(),
            )
            .await
        {
            Ok(id) => {
                info!(self.log, "updating port settings {id}");
                self.switch_port_settings_update(opctx, id, params).await
            }
            Err(_) => {
                info!(self.log, "creating new switch port settings");
                self.switch_port_settings_create(opctx, params, None).await
            }
        }
    }

    // TODO: more validation wanted
    fn switch_port_settings_validate(
        params: &params::SwitchPortSettingsCreate,
    ) -> CreateResult<()> {
        for x in &params.bgp_peers {
            for p in x.peers.iter() {
                if let Some(ref key) = p.md5_auth_key {
                    if key.len() > 80 {
                        return Err(Error::invalid_value(
                            "md5_auth_key",
                            format!(
                                "md5 auth key for {} is longer than 80 characters",
                                p.addr
                            ),
                        ));
                    }
                    for c in key.chars() {
                        if !c.is_ascii() || c.is_ascii_control() {
                            return Err(Error::invalid_value(
                                "md5_auth_key",
                                format!(
                                    "md5 auth key for {} must be printable ascii",
                                    p.addr
                                ),
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn switch_port_settings_create(
        self: &Arc<Self>,
        opctx: &OpContext,
        params: params::SwitchPortSettingsCreate,
        id: Option<Uuid>,
    ) -> CreateResult<SwitchPortSettingsCombinedResult> {
        self.db_datastore.switch_port_settings_create(opctx, &params, id).await
    }

    pub(crate) async fn switch_port_settings_update(
        self: &Arc<Self>,
        opctx: &OpContext,
        switch_port_settings_id: Uuid,
        new_settings: params::SwitchPortSettingsCreate,
    ) -> CreateResult<SwitchPortSettingsCombinedResult> {
        let result = self
            .db_datastore
            .switch_port_settings_update(
                opctx,
                &new_settings,
                switch_port_settings_id,
            )
            .await?;

        let ports = self
            .db_datastore
            .switch_ports_using_settings(opctx, switch_port_settings_id)
            .await?;

        for (switch_port_id, _switch_port_name) in ports.into_iter() {
            self.set_switch_port_settings_id(
                &opctx,
                switch_port_id,
                Some(switch_port_settings_id),
                UpdatePrecondition::DontCare,
            )
            .await?;
        }

        // eagerly propagate changes via rpw
        self.background_tasks
            .activate(&self.background_tasks.task_switch_port_settings_manager);

        Ok(result)
    }

    pub(crate) async fn switch_port_settings_delete(
        &self,
        opctx: &OpContext,
        params: &params::SwitchPortSettingsSelector,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        self.db_datastore.switch_port_settings_delete(opctx, params).await
    }

    pub(crate) async fn switch_port_settings_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<SwitchPortSettings> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        self.db_datastore.switch_port_settings_list(opctx, pagparams).await
    }

    pub(crate) async fn switch_port_settings_get(
        &self,
        opctx: &OpContext,
        name_or_id: &NameOrId,
    ) -> LookupResult<SwitchPortSettingsCombinedResult> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.db_datastore.switch_port_settings_get(opctx, name_or_id).await
    }

    async fn switch_port_create(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        switch_location: Name,
        port: Name,
    ) -> CreateResult<SwitchPort> {
        self.db_datastore
            .switch_port_create(
                opctx,
                rack_id,
                switch_location.into(),
                port.into(),
            )
            .await
    }

    pub(crate) async fn switch_port_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<SwitchPort> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.db_datastore.switch_port_list(opctx, pagparams).await
    }

    pub(crate) async fn set_switch_port_settings_id(
        &self,
        opctx: &OpContext,
        switch_port_id: Uuid,
        port_settings_id: Option<Uuid>,
        current_id: UpdatePrecondition<Uuid>,
    ) -> UpdateResult<()> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        self.db_datastore
            .switch_port_set_settings_id(
                opctx,
                switch_port_id,
                port_settings_id,
                current_id,
            )
            .await
    }

    pub(crate) async fn switch_port_apply_settings(
        self: &Arc<Self>,
        opctx: &OpContext,
        port: &Name,
        selector: &params::SwitchPortSelector,
        settings: &params::SwitchPortApplySettings,
    ) -> UpdateResult<()> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let switch_port_id = self
            .db_datastore
            .switch_port_get_id(
                opctx,
                selector.rack_id,
                selector.switch_location.clone().into(),
                port.clone().into(),
            )
            .await?;

        let switch_port_settings_id = match &settings.port_settings {
            NameOrId::Id(id) => *id,
            NameOrId::Name(name) => {
                self.db_datastore
                    .switch_port_settings_get_id(opctx, name.clone().into())
                    .await?
            }
        };

        self.set_switch_port_settings_id(
            &opctx,
            switch_port_id,
            Some(switch_port_settings_id),
            UpdatePrecondition::DontCare,
        )
        .await?;

        // eagerly propagate changes via rpw
        self.background_tasks
            .activate(&self.background_tasks.task_switch_port_settings_manager);

        Ok(())
    }

    pub(crate) async fn switch_port_clear_settings(
        self: &Arc<Self>,
        opctx: &OpContext,
        port: &Name,
        params: &params::SwitchPortSelector,
    ) -> UpdateResult<()> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let switch_port_id = self
            .db_datastore
            .switch_port_get_id(
                opctx,
                params.rack_id,
                params.switch_location.clone().into(),
                port.clone().into(),
            )
            .await?;

        // update the switch port settings association
        self.set_switch_port_settings_id(
            &opctx,
            switch_port_id,
            None,
            UpdatePrecondition::DontCare,
        )
        .await?;

        // eagerly propagate changes via rpw
        self.background_tasks
            .activate(&self.background_tasks.task_switch_port_settings_manager);

        Ok(())
    }

    pub(crate) async fn populate_switch_ports(
        &self,
        opctx: &OpContext,
        ports: &[Name],
        switch: Name,
    ) -> CreateResult<()> {
        for port in ports {
            match self
                .switch_port_create(
                    opctx,
                    self.rack_id,
                    switch.clone(),
                    port.clone(),
                )
                .await
            {
                Ok(_) => {}
                // ignore ObjectAlreadyExists but pass through other errors
                Err(external::Error::ObjectAlreadyExists { .. }) => {}
                Err(e) => return Err(e),
            };
        }

        Ok(())
    }

    pub(crate) async fn switch_port_status(
        &self,
        opctx: &OpContext,
        switch: Name,
        port: Name,
    ) -> Result<SwitchLinkState, Error> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        let loc: SwitchLocation = switch.as_str().parse().map_err(|e| {
            Error::invalid_request(&format!(
                "invalid switch name {switch}: {e}"
            ))
        })?;

        let port_id = PortId::Qsfp(port.as_str().parse().map_err(|e| {
            Error::invalid_request(&format!("invalid port name: {port} {e}"))
        })?);

        // no breakout support yet, link id always 0
        let link_id = LinkId(0);

        let dpd_clients = self.dpd_clients().await.map_err(|e| {
            Error::internal_error(&format!("dpd clients get: {e}"))
        })?;

        let dpd = dpd_clients.get(&loc).ok_or(Error::internal_error(
            &format!("no client for switch {switch}"),
        ))?;

        let status = dpd
            .link_get(&port_id, &link_id)
            .await
            .map_err(|e| {
                Error::internal_error(&format!(
                    "failed to get port status for {port} {e}"
                ))
            })?
            .into_inner();

        let monitors = match dpd.transceiver_monitors_get(&port_id).await {
            Ok(resp) => Some(resp.into_inner()),
            Err(e) => {
                if let Some(StatusCode::NOT_FOUND) = e.status() {
                    None
                } else {
                    return Err(Error::internal_error(&format!(
                        "failed to get txr monitors for {port} {e}"
                    )));
                }
            }
        };

        let link_json = serde_json::to_value(status).map_err(|e| {
            Error::internal_error(&format!(
                "failed to marshal link info to json: {e}"
            ))
        })?;
        let monitors_json = match monitors {
            Some(x) => Some(serde_json::to_value(x).map_err(|e| {
                Error::internal_error(&format!(
                    "failed to marshal monitors to json: {e}"
                ))
            })?),
            None => None,
        };
        Ok(SwitchLinkState::new(link_json, monitors_json))
    }
}

pub(crate) async fn list_switch_ports_with_uplinks(
    datastore: &DataStore,
    opctx: &OpContext,
) -> ListResultVec<SwitchPort> {
    opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
    datastore.switch_ports_with_uplinks(opctx).await
}
