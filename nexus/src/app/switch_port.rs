// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::app::sagas;
use crate::authn;
use crate::authz;
use crate::db;
use crate::db::datastore::UpdatePrecondition;
use crate::db::model::{SwitchPort, SwitchPortSettings};
use crate::external_api::params;
use db::datastore::SwitchPortSettingsCombinedResult;
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{
    self, CreateResult, DataPageParams, DeleteResult, ListResultVec,
    LookupResult, Name, NameOrId, UpdateResult,
};
use std::sync::Arc;
use uuid::Uuid;

impl super::Nexus {
    pub async fn switch_port_settings_create(
        &self,
        opctx: &OpContext,
        params: params::SwitchPortSettingsCreate,
    ) -> CreateResult<SwitchPortSettingsCombinedResult> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        self.db_datastore.switch_port_settings_create(opctx, &params).await
    }

    pub async fn switch_port_settings_delete(
        &self,
        opctx: &OpContext,
        params: &params::SwitchPortSettingsSelector,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        self.db_datastore.switch_port_settings_delete(opctx, params).await
    }

    pub async fn switch_port_settings_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<SwitchPortSettings> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        self.db_datastore.switch_port_settings_list(opctx, pagparams).await
    }

    pub async fn switch_port_settings_get(
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

    pub async fn switch_port_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<SwitchPort> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.db_datastore.switch_port_list(opctx, pagparams).await
    }

    pub async fn get_switch_port(
        &self,
        opctx: &OpContext,
        params: uuid::Uuid,
    ) -> LookupResult<SwitchPort> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.db_datastore.switch_port_get(opctx, params).await
    }

    pub async fn list_switch_ports_with_uplinks(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<SwitchPort> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.db_datastore.switch_ports_with_uplinks(opctx).await
    }

    pub async fn set_switch_port_settings_id(
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

    pub async fn switch_port_apply_settings(
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

        let saga_params = sagas::switch_port_settings_apply::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            switch_port_id,
            switch_port_settings_id,
            switch_port_name: port.to_string(),
        };

        self.execute_saga::<sagas::switch_port_settings_apply::SagaSwitchPortSettingsApply>(
            saga_params,
        )
        .await?;

        Ok(())
    }

    pub async fn switch_port_clear_settings(
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

        let saga_params = sagas::switch_port_settings_clear::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            switch_port_id,
            port_name: port.to_string(),
        };

        self.execute_saga::<sagas::switch_port_settings_clear::SagaSwitchPortSettingsClear>(
            saga_params,
        )
        .await?;

        Ok(())
    }

    pub async fn populate_switch_ports(
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
}
