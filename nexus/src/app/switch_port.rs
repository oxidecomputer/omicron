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
use nexus_db_queries::db::lookup;
use nexus_db_queries::db::lookup::LookupPath;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{
    self, CreateResult, DataPageParams, DeleteResult, ListResultVec,
    LookupResult, Name, NameOrId, UpdateResult,
};
use std::sync::Arc;
use uuid::Uuid;

impl super::Nexus {
    pub fn switch_port_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        switch_port_selector: params::SwitchPortSelector,
    ) -> LookupResult<lookup::SwitchPort<'a>> {
        Ok(LookupPath::new(opctx, &self.db_datastore).switch_port(
            switch_port_selector.switch_id,
            switch_port_selector.port.into(),
        ))
    }
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
        switch_id: Uuid,
        port: Name,
    ) -> CreateResult<SwitchPort> {
        self.db_datastore
            .switch_port_create(opctx, switch_id, port.into())
            .await
    }

    pub async fn switch_port_list(
        &self,
        opctx: &OpContext,
        switch: lookup::Switch<'_>,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<SwitchPort> {
        let (.., authz_switch) =
            switch.lookup_for(authz::Action::ListChildren).await?;
        self.db_datastore
            .switch_port_list(opctx, &authz_switch, pagparams)
            .await
    }

    pub async fn get_switch_port(
        &self,
        opctx: &OpContext,
        params: uuid::Uuid,
    ) -> LookupResult<SwitchPort> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.db_datastore.switch_port_get(opctx, params).await
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
        switch_port_lookup: &lookup::SwitchPort<'_>,
        port: &Name,
        settings: &params::SwitchPortApplySettings,
    ) -> UpdateResult<()> {
        let (.., authz_switch_port) =
            switch_port_lookup.lookup_for(authz::Action::Modify).await?;

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
            switch_port_id: authz_switch_port.id(),
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
        switch_port_lookup: &lookup::SwitchPort<'_>,
        port: &Name,
    ) -> UpdateResult<()> {
        let (.., authz_switch_port) =
            switch_port_lookup.lookup_for(authz::Action::Modify).await?;

        let saga_params = sagas::switch_port_settings_clear::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            switch_port_id: authz_switch_port.id(),
            port_name: port.to_string(),
        };

        self.execute_saga::<sagas::switch_port_settings_clear::SagaSwitchPortSettingsClear>(
            saga_params,
        )
        .await?;

        Ok(())
    }

    // TODO Discover switch ports via Dendrite
    // https://github.com/oxidecomputer/omicron/issues/3069
    pub async fn populate_switch_ports(
        &self,
        opctx: &OpContext,
        switch_id: Uuid,
        port_count: u8,
    ) -> CreateResult<()> {
        for i in 0..port_count {
            match self
                .switch_port_create(
                    opctx,
                    switch_id,
                    // TODO https://github.com/oxidecomputer/omicron/issues/3014
                    // Populate ports for multiple switches
                    format!("qsfp{}", i).parse().unwrap(),
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
