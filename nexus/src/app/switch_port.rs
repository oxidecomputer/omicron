// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::app::sagas;
use crate::external_api::params;
use db::datastore::SwitchPortSettingsCombinedResult;
use ipnetwork::IpNetwork;
use nexus_db_queries::authn;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::datastore::UpdatePrecondition;
use nexus_db_queries::db::model::{SwitchPort, SwitchPortSettings};
use nexus_types::identity::Resource;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{
    self, CreateResult, DataPageParams, DeleteResult, ListResultVec,
    LookupResult, Name, NameOrId, UpdateResult,
};
use sled_agent_client::types::BgpConfig;
use sled_agent_client::types::BgpPeerConfig;
use sled_agent_client::types::{
    EarlyNetworkConfig, PortConfigV1, RackNetworkConfig, RouteConfig,
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

        //TODO(ry) race conditions on exists check versus update/create.
        //         Normally I would use a DB lock here, but not sure what
        //         the Omicron way of doing things here is.

        match self
            .db_datastore
            .switch_port_settings_exist(
                opctx,
                params.identity.name.clone().into(),
            )
            .await
        {
            Ok(id) => self.switch_port_settings_update(opctx, id, params).await,
            Err(_) => self.switch_port_settings_create(opctx, params).await,
        }
    }

    pub async fn switch_port_settings_create(
        self: &Arc<Self>,
        opctx: &OpContext,
        params: params::SwitchPortSettingsCreate,
    ) -> CreateResult<SwitchPortSettingsCombinedResult> {
        self.db_datastore.switch_port_settings_create(opctx, &params).await
    }

    pub(crate) async fn switch_port_settings_update(
        self: &Arc<Self>,
        opctx: &OpContext,
        switch_port_settings_id: Uuid,
        new_settings: params::SwitchPortSettingsCreate,
    ) -> CreateResult<SwitchPortSettingsCombinedResult> {
        // delete old settings
        self.switch_port_settings_delete(
            opctx,
            &params::SwitchPortSettingsSelector {
                port_settings: Some(NameOrId::Id(switch_port_settings_id)),
            },
        )
        .await?;

        // create new settings
        let result = self
            .switch_port_settings_create(opctx, new_settings.clone())
            .await?;

        // run the port settings apply saga for each port referencing the
        // updated settings

        let ports = self
            .db_datastore
            .switch_ports_using_settings(opctx, switch_port_settings_id)
            .await?;

        for (switch_port_id, switch_port_name) in ports.into_iter() {
            let saga_params = sagas::switch_port_settings_apply::Params {
                serialized_authn: authn::saga::Serialized::for_opctx(opctx),
                switch_port_id,
                switch_port_settings_id: result.settings.id(),
                switch_port_name: switch_port_name.to_string(),
            };

            self.execute_saga::<
                sagas::switch_port_settings_apply::SagaSwitchPortSettingsApply
                >(
                    saga_params,
                )
                .await?;
        }

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

    pub(crate) async fn get_switch_port(
        &self,
        opctx: &OpContext,
        params: uuid::Uuid,
    ) -> LookupResult<SwitchPort> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.db_datastore.switch_port_get(opctx, params).await
    }

    pub(crate) async fn list_switch_ports_with_uplinks(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<SwitchPort> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        self.db_datastore.switch_ports_with_uplinks(opctx).await
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

        let saga_params = sagas::switch_port_settings_apply::Params {
            serialized_authn: authn::saga::Serialized::for_opctx(opctx),
            switch_port_id,
            switch_port_settings_id,
            switch_port_name: port.to_string(),
        };

        self.execute_saga::<
            sagas::switch_port_settings_apply::SagaSwitchPortSettingsApply
        >(
            saga_params,
        )
        .await?;

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

    // TODO it would likely be better to do this as a one shot db query.
    pub(crate) async fn active_port_settings(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<Vec<(SwitchPort, SwitchPortSettingsCombinedResult)>> {
        let mut ports = Vec::new();
        let port_list =
            self.switch_port_list(opctx, &DataPageParams::max_page()).await?;

        for p in port_list {
            if let Some(id) = p.port_settings_id {
                ports.push((
                    p.clone(),
                    self.switch_port_settings_get(opctx, &id.into()).await?,
                ));
            }
        }

        LookupResult::Ok(ports)
    }

    pub(crate) async fn compute_bootstore_network_config(
        &self,
        opctx: &OpContext,
        current: &EarlyNetworkConfig,
    ) -> LookupResult<EarlyNetworkConfig> {
        let mut rack_net_config = match &current.rack_network_config {
            Some(cfg) => {
                RackNetworkConfig {
                    infra_ip_first: cfg.infra_ip_first,
                    infra_ip_last: cfg.infra_ip_last,
                    ports: Vec::new(), // To be filled in from db
                    bgp: Vec::new(),   // To be filled in from db
                }
            }
            None => {
                return LookupResult::Err(
                    external::Error::ServiceUnavailable {
                        internal_message:
                            "bootstore network config not initialized yet"
                                .to_string(),
                    },
                );
            }
        };

        let db_ports = self.active_port_settings(opctx).await?;

        for (port, info) in &db_ports {
            let mut peer_info = Vec::new();
            for p in &info.bgp_peers {
                let bgp_config =
                    self.bgp_config_get(&opctx, p.bgp_config_id.into()).await?;
                let announcements = self
                    .bgp_announce_list(
                        &opctx,
                        &params::BgpAnnounceSetSelector {
                            name_or_id: p.bgp_announce_set_id.into(),
                        },
                    )
                    .await?;
                let addr = match p.addr {
                    ipnetwork::IpNetwork::V4(addr) => addr,
                    ipnetwork::IpNetwork::V6(_) => continue, //TODO v6
                };
                peer_info.push((p, bgp_config.asn.0, addr.ip()));
                rack_net_config.bgp.push(BgpConfig {
                    asn: bgp_config.asn.0,
                    originate: announcements
                        .iter()
                        .filter_map(|a| match a.network {
                            IpNetwork::V4(net) => Some(net.into()),
                            //TODO v6
                            _ => None,
                        })
                        .collect(),
                });
            }

            let p = PortConfigV1 {
                routes: info
                    .routes
                    .iter()
                    .map(|r| RouteConfig {
                        destination: r.dst,
                        nexthop: r.gw.ip(),
                    })
                    .collect(),
                addresses: info.addresses.iter().map(|a| a.address).collect(),
                bgp_peers: peer_info
                    .iter()
                    .map(|(_p, asn, addr)| BgpPeerConfig {
                        addr: *addr,
                        asn: *asn,
                        port: port.port_name.clone(),
                    })
                    .collect(),
                switch: port.switch_location.parse().unwrap(),
                port: port.port_name.clone(),
                //TODO hardcode
                uplink_port_fec:
                    omicron_common::api::internal::shared::PortFec::None,
                //TODO hardcode
                uplink_port_speed:
                    omicron_common::api::internal::shared::PortSpeed::Speed100G,
            };

            rack_net_config.ports.push(p);
        }

        let result = EarlyNetworkConfig {
            generation: current.generation,
            rack_subnet: current.rack_subnet,
            ntp_servers: current.ntp_servers.clone(), //TODO update from db
            rack_network_config: Some(rack_net_config),
        };

        LookupResult::Ok(result)
    }
}
