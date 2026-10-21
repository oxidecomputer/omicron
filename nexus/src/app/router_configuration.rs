// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Router configurations

use crate::app::authz;
use nexus_db_lookup::LookupPath;
use nexus_db_lookup::lookup;
use nexus_db_model::{
    RouterConfiguration, RouterConfigurationBfdPeer,
    RouterConfigurationBgpPeer, RouterConfigurationStaticRoute,
};
use nexus_db_queries::context::OpContext;
use nexus_types::external_api::networking;
use nexus_types::identity::Resource;
use nexus_types::router_configuration::is_builtin_router_configuration_id;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{
    CreateResult, DeleteResult, Error, ListResultVec, LookupResult, Name,
    NameOrId, UpdateResult,
};
use omicron_uuid_kinds::{GenericUuid, RouterConfigurationUuid};
use sled_agent_client::types::RouterListEntry;
use sled_agent_types::early_networking::SwitchSlot;
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

/// Maximum number of router configurations that may be assigned to a silo.
///
/// This bounds the size of the transaction that replaces a silo's set of
/// router configurations, like `MAX_ROLE_ASSIGNMENTS_PER_RESOURCE` does for
/// policy updates.
const MAX_ROUTER_CONFIGURATIONS_PER_SILO: usize = 64;

impl super::Nexus {
    /// Router configurations reach mgd via the bootstore, which the
    /// switch-port-settings task writes.
    fn activate_router_configuration_propagation(&self) {
        self.background_tasks
            .activate(&self.background_tasks.task_switch_port_settings_manager);
    }

    pub fn router_configuration_lookup<'a>(
        &'a self,
        opctx: &'a OpContext,
        name_or_id: NameOrId,
    ) -> LookupResult<lookup::RouterConfiguration<'a>> {
        match name_or_id {
            NameOrId::Id(id) => Ok(LookupPath::new(opctx, &self.db_datastore)
                .router_configuration_id(
                    RouterConfigurationUuid::from_untyped_uuid(id),
                )),
            NameOrId::Name(name) => {
                Ok(LookupPath::new(opctx, &self.db_datastore)
                    .router_configuration_name_owned(name.into()))
            }
        }
    }

    /// Build the external view of a router configuration, including the BGP
    /// peer, static route and BFD peer entries stored in separate tables.
    async fn router_configuration_assemble(
        &self,
        opctx: &OpContext,
        authz_configuration: &authz::RouterConfiguration,
        db_configuration: RouterConfiguration,
    ) -> Result<networking::RouterConfiguration, Error> {
        let _ = authz_configuration;
        Ok(self
            .router_configurations_assemble(opctx, vec![db_configuration])
            .await?
            .remove(0))
    }

    async fn router_configurations_assemble(
        &self,
        opctx: &OpContext,
        configurations: Vec<RouterConfiguration>,
    ) -> ListResultVec<networking::RouterConfiguration> {
        let ids: Vec<_> = configurations.iter().map(|c| c.id()).collect();
        let (blocks, bgp_peers, routes, bfd_peers) = futures::try_join!(
            self.db_datastore
                .router_configuration_switch_list_batch(opctx, &ids),
            self.db_datastore
                .router_configuration_bgp_peer_list_batch(opctx, &ids),
            self.db_datastore
                .router_configuration_static_route_list_batch(opctx, &ids),
            self.db_datastore
                .router_configuration_bfd_peer_list_batch(opctx, &ids),
        )?;
        let mut switches =
            HashMap::<_, networking::RouterConfigurationSwitch>::new();
        for block in blocks {
            switches.insert(
                (block.router_configuration_id, block.switch),
                block.try_into()?,
            );
        }
        for peer in bgp_peers {
            if let Some(block) =
                switches.get_mut(&(peer.router_configuration_id, peer.switch))
            {
                block.bgp_peers.push(peer.try_into()?);
            }
        }
        for route in routes {
            if let Some(block) =
                switches.get_mut(&(route.router_configuration_id, route.switch))
            {
                block.routes.push(route.into());
            }
        }
        for peer in bfd_peers {
            if let Some(block) =
                switches.get_mut(&(peer.router_configuration_id, peer.switch))
            {
                block.bfd_peers.push(peer.try_into()?);
            }
        }
        configurations
            .into_iter()
            .map(|config| {
                let id = config.id();
                let mut view: networking::RouterConfiguration =
                    config.try_into()?;
                view.switches = switches
                    .iter()
                    .filter(|((parent, _), _)| {
                        parent.into_untyped_uuid() == id.into_untyped_uuid()
                    })
                    .map(|(_, block)| block.clone())
                    .collect();
                view.switches.sort_by_key(|block| block.switch);
                Ok(view)
            })
            .collect()
    }

    pub async fn router_configuration_create(
        &self,
        opctx: &OpContext,
        params: &networking::RouterConfigurationCreate,
    ) -> CreateResult<networking::RouterConfiguration> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        if nexus_types::router_configuration::is_reserved_router_configuration_name(
            &params.identity.name,
        ) {
            return Err(Error::invalid_request(&format!(
                "the name \"{}\" is reserved",
                params.identity.name,
            )));
        }
        let unique: BTreeSet<_> = params.switches.iter().collect();
        if unique.len() != params.switches.len() {
            return Err(Error::invalid_request("duplicate switch blocks"));
        }
        let db_configuration = self
            .db_datastore
            .router_configuration_create(
                opctx,
                RouterConfiguration::new(params),
                &params.switches,
            )
            .await?;
        self.activate_router_configuration_propagation();
        Ok(self
            .router_configurations_assemble(opctx, vec![db_configuration])
            .await?
            .remove(0))
    }

    pub async fn router_configuration_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<networking::RouterConfiguration> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let db_configurations = self
            .db_datastore
            .router_configuration_list(opctx, pagparams)
            .await?;
        self.router_configurations_assemble(opctx, db_configurations).await
    }

    pub async fn router_configuration_view(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
    ) -> LookupResult<networking::RouterConfiguration> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let (.., authz_configuration, db_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .fetch()
            .await?;
        self.router_configuration_assemble(
            opctx,
            &authz_configuration,
            db_configuration,
        )
        .await
    }

    pub async fn router_configuration_update(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        update: networking::RouterConfigurationUpdate,
    ) -> UpdateResult<networking::RouterConfiguration> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        use nexus_types::router_configuration::is_reserved_router_configuration_name;

        let builtin = is_builtin_router_configuration_id(
            &authz_configuration.id().into_untyped_uuid(),
        );
        if let Some(new_name) = &update.identity.name {
            if builtin && !is_reserved_router_configuration_name(new_name) {
                return Err(Error::invalid_request(
                    "built-in router configurations cannot be renamed",
                ));
            }
            if !builtin && is_reserved_router_configuration_name(new_name) {
                return Err(Error::invalid_request(&format!(
                    "the name \"{new_name}\" is reserved",
                )));
            }
        }
        let db_configuration = self
            .db_datastore
            .router_configuration_update(
                opctx,
                &authz_configuration,
                update.into(),
            )
            .await?;
        self.activate_router_configuration_propagation();
        self.router_configuration_assemble(
            opctx,
            &authz_configuration,
            db_configuration,
        )
        .await
    }

    pub async fn router_configuration_delete(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Delete)
            .await?;
        if nexus_types::router_configuration::is_builtin_router_configuration_id(
            &authz_configuration.id().into_untyped_uuid(),
        ) {
            return Err(Error::invalid_request(
                "built-in router configurations cannot be deleted",
            ));
        }
        self.db_datastore
            .router_configuration_delete(opctx, &authz_configuration)
            .await?;
        self.activate_router_configuration_propagation();
        Ok(())
    }

    pub async fn router_configuration_switch_create(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        switch: SwitchSlot,
    ) -> CreateResult<networking::RouterConfigurationSwitch> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        if is_builtin_router_configuration_id(
            &authz_configuration.id().into_untyped_uuid(),
        ) {
            return Err(Error::invalid_request(
                "built-in router configurations cannot add or remove switch blocks",
            ));
        }
        let block = self
            .db_datastore
            .router_configuration_switch_create(
                opctx,
                &authz_configuration,
                switch.into(),
            )
            .await?;
        self.activate_router_configuration_propagation();
        block.try_into()
    }

    pub async fn router_configuration_switch_delete(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        switch: SwitchSlot,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        if is_builtin_router_configuration_id(
            &authz_configuration.id().into_untyped_uuid(),
        ) {
            return Err(Error::invalid_request(
                "built-in router configurations cannot add or remove switch blocks",
            ));
        }
        self.db_datastore
            .router_configuration_switch_delete(
                opctx,
                &authz_configuration,
                switch.into(),
            )
            .await?;
        self.activate_router_configuration_propagation();
        Ok(())
    }

    pub async fn router_configuration_bgp_config_view(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        switch: SwitchSlot,
    ) -> LookupResult<networking::RouterConfigurationBgpConfig> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Read)
            .await?;
        self.db_datastore.router_configuration_switch_view(opctx, &authz_configuration, switch.into()).await?
            .bgp_config()?.ok_or_else(|| Error::non_resourcetype_not_found(
                format!("router configuration {switch:?} block has no BGP configuration")))
    }

    pub async fn router_configuration_bgp_config_set(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        switch: SwitchSlot,
        config: networking::RouterConfigurationBgpConfigSet,
    ) -> UpdateResult<networking::RouterConfigurationBgpConfig> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        let (.., authz_announce_set) = self
            .bgp_announce_set_lookup(opctx, config.bgp_announce_set.clone())?
            .lookup_for(authz::Action::Read)
            .await?;
        let db_configuration = self
            .db_datastore
            .router_configuration_bgp_config_set(
                opctx,
                &authz_configuration,
                switch.into(),
                config.asn,
                config.max_paths,
                authz_announce_set.id(),
            )
            .await?;
        self.activate_router_configuration_propagation();
        db_configuration.bgp_config()?.ok_or_else(|| {
            Error::internal_error(
                "BGP configuration missing after it was just set",
            )
        })
    }

    pub async fn router_configuration_bgp_config_delete(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        switch: SwitchSlot,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .router_configuration_bgp_config_delete(
                opctx,
                &authz_configuration,
                switch.into(),
            )
            .await?;
        self.activate_router_configuration_propagation();
        Ok(())
    }

    pub async fn router_configuration_bgp_peer_list(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        switch: SwitchSlot,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<networking::RouterConfigurationBgpPeer> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Read)
            .await?;
        self.db_datastore
            .router_configuration_switch_view(
                opctx,
                &authz_configuration,
                switch.into(),
            )
            .await?;

        self.db_datastore
            .router_configuration_bgp_peer_list(
                opctx,
                &authz_configuration,
                switch.into(),
                pagparams,
            )
            .await?
            .into_iter()
            .map(TryInto::try_into)
            .collect()
    }

    pub async fn router_configuration_bgp_peer_create(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        switch: SwitchSlot,
        peer: networking::RouterConfigurationBgpPeer,
    ) -> CreateResult<networking::RouterConfigurationBgpPeer> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        let db_peer = self
            .db_datastore
            .router_configuration_bgp_peer_create(
                opctx,
                &authz_configuration,
                switch.into(),
                RouterConfigurationBgpPeer::new(
                    authz_configuration.id(),
                    switch.into(),
                    peer,
                )?,
            )
            .await?;
        self.activate_router_configuration_propagation();
        db_peer.try_into()
    }

    pub async fn router_configuration_bgp_peer_view(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        switch: SwitchSlot,
        peer: &Name,
    ) -> LookupResult<networking::RouterConfigurationBgpPeer> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Read)
            .await?;
        self.db_datastore
            .router_configuration_bgp_peer_view(
                opctx,
                &authz_configuration,
                switch.into(),
                peer,
            )
            .await?
            .try_into()
    }

    pub async fn router_configuration_bgp_peer_update(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        switch: SwitchSlot,
        peer_name: &Name,
        peer: networking::RouterConfigurationBgpPeer,
    ) -> UpdateResult<networking::RouterConfigurationBgpPeer> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        let db_peer = self
            .db_datastore
            .router_configuration_bgp_peer_update(
                opctx,
                &authz_configuration,
                switch.into(),
                peer_name,
                RouterConfigurationBgpPeer::new(
                    authz_configuration.id(),
                    switch.into(),
                    peer,
                )?,
            )
            .await?;
        self.activate_router_configuration_propagation();
        db_peer.try_into()
    }

    pub async fn router_configuration_bgp_peer_delete(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        switch: SwitchSlot,
        peer: &Name,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .router_configuration_bgp_peer_delete(
                opctx,
                &authz_configuration,
                switch.into(),
                peer,
            )
            .await?;
        self.activate_router_configuration_propagation();
        Ok(())
    }

    pub async fn router_configuration_static_route_list(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        switch: SwitchSlot,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<networking::StaticRoute> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Read)
            .await?;
        self.db_datastore
            .router_configuration_switch_view(
                opctx,
                &authz_configuration,
                switch.into(),
            )
            .await?;

        Ok(self
            .db_datastore
            .router_configuration_static_route_list(
                opctx,
                &authz_configuration,
                switch.into(),
                pagparams,
            )
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub async fn router_configuration_static_route_create(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        switch: SwitchSlot,
        route: networking::StaticRoute,
    ) -> CreateResult<networking::StaticRoute> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        let db_route = self
            .db_datastore
            .router_configuration_static_route_create(
                opctx,
                &authz_configuration,
                switch.into(),
                RouterConfigurationStaticRoute::new(
                    authz_configuration.id(),
                    switch.into(),
                    route,
                )?,
            )
            .await?;
        self.activate_router_configuration_propagation();
        Ok(db_route.into())
    }

    pub async fn router_configuration_static_route_view(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        switch: SwitchSlot,
        route: &Name,
    ) -> LookupResult<networking::StaticRoute> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Read)
            .await?;
        Ok(self
            .db_datastore
            .router_configuration_static_route_view(
                opctx,
                &authz_configuration,
                switch.into(),
                route,
            )
            .await?
            .into())
    }

    pub async fn router_configuration_static_route_update(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        switch: SwitchSlot,
        route_name: &Name,
        route: networking::StaticRoute,
    ) -> UpdateResult<networking::StaticRoute> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        let db_route = self
            .db_datastore
            .router_configuration_static_route_update(
                opctx,
                &authz_configuration,
                switch.into(),
                route_name,
                RouterConfigurationStaticRoute::new(
                    authz_configuration.id(),
                    switch.into(),
                    route,
                )?,
            )
            .await?;
        self.activate_router_configuration_propagation();
        Ok(db_route.into())
    }

    pub async fn router_configuration_static_route_delete(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        switch: SwitchSlot,
        route: &Name,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .router_configuration_static_route_delete(
                opctx,
                &authz_configuration,
                switch.into(),
                route,
            )
            .await?;
        self.activate_router_configuration_propagation();
        Ok(())
    }

    pub async fn router_configuration_bfd_peer_list(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        switch: SwitchSlot,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<networking::BfdPeer> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Read)
            .await?;
        self.db_datastore
            .router_configuration_switch_view(
                opctx,
                &authz_configuration,
                switch.into(),
            )
            .await?;

        self.db_datastore
            .router_configuration_bfd_peer_list(
                opctx,
                &authz_configuration,
                switch.into(),
                pagparams,
            )
            .await?
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()
    }

    pub async fn router_configuration_bfd_peer_create(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        switch: SwitchSlot,
        peer: networking::BfdPeer,
    ) -> CreateResult<networking::BfdPeer> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        let db_peer = self
            .db_datastore
            .router_configuration_bfd_peer_create(
                opctx,
                &authz_configuration,
                switch.into(),
                RouterConfigurationBfdPeer::new(
                    authz_configuration.id(),
                    switch.into(),
                    peer,
                )?,
            )
            .await?;
        self.activate_router_configuration_propagation();
        db_peer.try_into()
    }

    pub async fn router_configuration_bfd_peer_view(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        switch: SwitchSlot,
        peer: &Name,
    ) -> LookupResult<networking::BfdPeer> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Read)
            .await?;
        self.db_datastore
            .router_configuration_bfd_peer_view(
                opctx,
                &authz_configuration,
                switch.into(),
                peer,
            )
            .await?
            .try_into()
    }

    pub async fn router_configuration_bfd_peer_update(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        switch: SwitchSlot,
        peer_name: &Name,
        peer: networking::BfdPeer,
    ) -> UpdateResult<networking::BfdPeer> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        let db_peer = self
            .db_datastore
            .router_configuration_bfd_peer_update(
                opctx,
                &authz_configuration,
                switch.into(),
                peer_name,
                RouterConfigurationBfdPeer::new(
                    authz_configuration.id(),
                    switch.into(),
                    peer,
                )?,
            )
            .await?;
        self.activate_router_configuration_propagation();
        db_peer.try_into()
    }

    pub async fn router_configuration_bfd_peer_delete(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        switch: SwitchSlot,
        peer: &Name,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .router_configuration_bfd_peer_delete(
                opctx,
                &authz_configuration,
                switch.into(),
                peer,
            )
            .await?;
        self.activate_router_configuration_propagation();
        Ok(())
    }

    pub async fn silo_router_configurations_view(
        &self,
        opctx: &OpContext,
        silo: NameOrId,
    ) -> LookupResult<networking::SiloRouterConfigurations> {
        let (.., authz_silo) = self
            .silo_lookup(opctx, silo)?
            .lookup_for(authz::Action::Read)
            .await?;

        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        let configurations = self
            .db_datastore
            .silo_router_configurations_list(opctx, &authz_silo)
            .await?
            .into_iter()
            .map(|link| networking::SiloRouterConfiguration {
                router_configuration_id: RouterConfigurationUuid::from(
                    link.router_configuration_id,
                )
                .into_untyped_uuid(),
                priority: *link.priority,
            })
            .collect();

        Ok(networking::SiloRouterConfigurations { configurations })
    }

    pub async fn silo_router_configurations_update(
        &self,
        opctx: &OpContext,
        silo: NameOrId,
        update: networking::SiloRouterConfigurationsUpdate,
    ) -> UpdateResult<networking::SiloRouterConfigurations> {
        let (.., authz_silo) = self
            .silo_lookup(opctx, silo)?
            .lookup_for(authz::Action::Modify)
            .await?;

        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        if update.configurations.len() > MAX_ROUTER_CONFIGURATIONS_PER_SILO {
            return Err(Error::invalid_request(format!(
                "at most {MAX_ROUTER_CONFIGURATIONS_PER_SILO} router \
                 configurations may be assigned to a silo"
            )));
        }

        let mut priorities = BTreeSet::new();
        let mut configuration_ids = BTreeSet::new();
        let mut links = Vec::new();
        let mut configurations = Vec::new();
        for entry in &update.configurations {
            if !priorities.insert(entry.priority) {
                return Err(Error::invalid_request(format!(
                    "priority {} is used by more than one router \
                     configuration",
                    entry.priority
                )));
            }
            let (.., authz_configuration, db_configuration) = self
                .router_configuration_lookup(
                    opctx,
                    entry.router_configuration.clone(),
                )?
                .fetch()
                .await?;
            if !configuration_ids.insert(authz_configuration.id()) {
                return Err(Error::invalid_request(format!(
                    "router configuration \"{}\" appears more than once",
                    db_configuration.name()
                )));
            }
            links.push(nexus_db_model::SiloRouterConfiguration::new(
                authz_silo.id(),
                authz_configuration.id(),
                entry.priority,
            ));
            configurations.push(networking::SiloRouterConfiguration {
                router_configuration_id: authz_configuration
                    .id()
                    .into_untyped_uuid(),
                priority: entry.priority,
            });
        }

        self.db_datastore
            .silo_router_configurations_replace(opctx, &authz_silo, links)
            .await?;

        self.background_tasks
            .activate(&self.background_tasks.task_router_list_manager);

        configurations.sort_by_key(|c| c.priority);
        Ok(networking::SiloRouterConfigurations { configurations })
    }

    pub async fn control_plane_router_configurations_view(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<networking::ControlPlaneRouterConfigurations> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        let entries = self
            .db_datastore
            .control_plane_router_configurations_list(opctx)
            .await?;

        let configurations = entries
            .into_iter()
            .map(|entry| networking::ControlPlaneRouterConfiguration {
                router_configuration_id: entry
                    .router_configuration_id
                    .into_untyped_uuid(),
                priority: *entry.priority,
            })
            .collect();

        Ok(networking::ControlPlaneRouterConfigurations { configurations })
    }

    pub async fn control_plane_router_configurations_update(
        &self,
        opctx: &OpContext,
        update: networking::ControlPlaneRouterConfigurationsUpdate,
    ) -> UpdateResult<networking::ControlPlaneRouterConfigurations> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        if update.configurations.len() > MAX_ROUTER_CONFIGURATIONS_PER_SILO {
            return Err(Error::invalid_request(format!(
                "at most {MAX_ROUTER_CONFIGURATIONS_PER_SILO} router \
                 configurations may be assigned to the control plane"
            )));
        }

        let mut priorities = BTreeSet::new();
        let mut configuration_ids = BTreeSet::new();
        let mut links = Vec::new();
        let mut configurations = Vec::new();
        for entry in &update.configurations {
            if !priorities.insert(entry.priority) {
                return Err(Error::invalid_request(format!(
                    "priority {} is used by more than one router \
                     configuration",
                    entry.priority
                )));
            }
            let (.., authz_configuration, db_configuration) = self
                .router_configuration_lookup(
                    opctx,
                    entry.router_configuration.clone(),
                )?
                .fetch()
                .await?;
            if !configuration_ids.insert(authz_configuration.id()) {
                return Err(Error::invalid_request(format!(
                    "router configuration \"{}\" appears more than once",
                    db_configuration.name()
                )));
            }
            links.push(nexus_db_model::ControlPlaneRouterConfiguration::new(
                authz_configuration.id(),
                entry.priority,
            ));
            configurations.push(networking::ControlPlaneRouterConfiguration {
                router_configuration_id: authz_configuration
                    .id()
                    .into_untyped_uuid(),
                priority: entry.priority,
            });
        }

        self.db_datastore
            .control_plane_router_configurations_replace(opctx, links)
            .await?;

        self.background_tasks
            .activate(&self.background_tasks.task_router_list_manager);
        // The control-plane list also rides the bootstore, for service ports
        // created before nexus is reachable.
        self.activate_router_configuration_propagation();

        configurations.sort_by_key(|c| c.priority);
        Ok(networking::ControlPlaneRouterConfigurations { configurations })
    }
}

/// Resolve (priority, router-configuration id) links into the router list
/// pushed to OPTE ports, sorted by priority. The built-in default
/// configuration stands for the daemon-owned default router (`None` in
/// OPTE).
pub(crate) fn router_list_from_links(
    links: impl IntoIterator<Item = (u16, Uuid)>,
) -> Vec<RouterListEntry> {
    let mut list: Vec<_> = links
        .into_iter()
        .map(|(priority, rc_id)| RouterListEntry {
            priority,
            router_id: if is_builtin_router_configuration_id(&rc_id) {
                None
            } else {
                Some(rc_id)
            },
        })
        .collect();
    list.sort_by_key(|e| e.priority);
    list
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_types::router_configuration::DEFAULT_ROUTER_CONFIGURATION_ID;

    #[test]
    fn builtin_link_is_the_default_router() {
        let named = Uuid::new_v4();
        let list = router_list_from_links(vec![
            (100, DEFAULT_ROUTER_CONFIGURATION_ID),
            (50, named),
        ]);
        assert_eq!(
            list,
            vec![
                RouterListEntry { priority: 50, router_id: Some(named) },
                RouterListEntry { priority: 100, router_id: None },
            ]
        );
    }

    #[test]
    fn no_links_means_an_empty_list() {
        assert_eq!(router_list_from_links(vec![]), Vec::new());
    }
}
