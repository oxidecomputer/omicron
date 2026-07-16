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
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{
    CreateResult, DeleteResult, Error, ListResultVec, LookupResult, Name,
    NameOrId, UpdateResult,
};
use omicron_uuid_kinds::{GenericUuid, RouterConfigurationUuid};
use std::collections::HashMap;

impl super::Nexus {
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
        let mut view: networking::RouterConfiguration =
            db_configuration.try_into()?;
        view.bgp_peers = self
            .db_datastore
            .router_configuration_bgp_peer_list(opctx, authz_configuration)
            .await?
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?;
        view.routes = self
            .db_datastore
            .router_configuration_static_route_list(opctx, authz_configuration)
            .await?
            .into_iter()
            .map(Into::into)
            .collect();
        view.bfd_peers = self
            .db_datastore
            .router_configuration_bfd_peer_list(opctx, authz_configuration)
            .await?
            .into_iter()
            .map(Into::into)
            .collect();
        Ok(view)
    }

    pub async fn router_configuration_create(
        &self,
        opctx: &OpContext,
        params: &networking::RouterConfigurationCreate,
    ) -> CreateResult<networking::RouterConfiguration> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let db_configuration = self
            .db_datastore
            .router_configuration_create(
                opctx,
                RouterConfiguration::new(params),
            )
            .await?;
        // A just-created router configuration has no entries.
        db_configuration.try_into()
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
        let ids: Vec<RouterConfigurationUuid> =
            db_configurations.iter().map(|c| c.id()).collect();

        let mut bgp_peers: HashMap<RouterConfigurationUuid, Vec<_>> =
            HashMap::new();
        for peer in self
            .db_datastore
            .router_configuration_bgp_peer_list_batch(opctx, &ids)
            .await?
        {
            bgp_peers
                .entry(peer.router_configuration_id.into())
                .or_default()
                .push(networking::RouterConfigurationBgpPeer::try_from(peer)?);
        }
        let mut routes: HashMap<RouterConfigurationUuid, Vec<_>> =
            HashMap::new();
        for route in self
            .db_datastore
            .router_configuration_static_route_list_batch(opctx, &ids)
            .await?
        {
            routes
                .entry(route.router_configuration_id.into())
                .or_default()
                .push(networking::StaticRoute::from(route));
        }
        let mut bfd_peers: HashMap<RouterConfigurationUuid, Vec<_>> =
            HashMap::new();
        for peer in self
            .db_datastore
            .router_configuration_bfd_peer_list_batch(opctx, &ids)
            .await?
        {
            bfd_peers
                .entry(peer.router_configuration_id.into())
                .or_default()
                .push(networking::BfdPeer::from(peer));
        }

        db_configurations
            .into_iter()
            .map(|db_configuration| {
                let id = db_configuration.id();
                let mut view: networking::RouterConfiguration =
                    db_configuration.try_into()?;
                view.bgp_peers = bgp_peers.remove(&id).unwrap_or_default();
                view.routes = routes.remove(&id).unwrap_or_default();
                view.bfd_peers = bfd_peers.remove(&id).unwrap_or_default();
                Ok(view)
            })
            .collect()
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
        let db_configuration = self
            .db_datastore
            .router_configuration_update(
                opctx,
                &authz_configuration,
                update.into(),
            )
            .await?;
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
        self.db_datastore
            .router_configuration_delete(opctx, &authz_configuration)
            .await
    }

    pub async fn router_configuration_bgp_config_view(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
    ) -> LookupResult<networking::RouterConfigurationBgpConfig> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let (.., db_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .fetch()
            .await?;
        db_configuration.bgp_config()?.ok_or_else(|| {
            Error::non_resourcetype_not_found(format!(
                "router configuration \"{}\" has no BGP configuration",
                db_configuration.name(),
            ))
        })
    }

    pub async fn router_configuration_bgp_config_set(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
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
                config.asn,
                config.max_paths,
                authz_announce_set.id(),
            )
            .await?;
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
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .router_configuration_bgp_config_delete(opctx, &authz_configuration)
            .await
    }

    pub async fn router_configuration_bgp_peer_list(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
    ) -> ListResultVec<networking::RouterConfigurationBgpPeer> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Read)
            .await?;
        self.db_datastore
            .router_configuration_bgp_peer_list(opctx, &authz_configuration)
            .await?
            .into_iter()
            .map(TryInto::try_into)
            .collect()
    }

    pub async fn router_configuration_bgp_peer_create(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        peer: networking::RouterConfigurationBgpPeer,
    ) -> CreateResult<networking::RouterConfigurationBgpPeer> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .router_configuration_bgp_peer_create(
                opctx,
                RouterConfigurationBgpPeer::new(authz_configuration.id(), peer),
            )
            .await?
            .try_into()
    }

    pub async fn router_configuration_bgp_peer_view(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
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
                peer,
            )
            .await?
            .try_into()
    }

    pub async fn router_configuration_bgp_peer_update(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        peer_name: &Name,
        peer: networking::RouterConfigurationBgpPeer,
    ) -> UpdateResult<networking::RouterConfigurationBgpPeer> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        self.db_datastore
            .router_configuration_bgp_peer_update(
                opctx,
                &authz_configuration,
                peer_name,
                RouterConfigurationBgpPeer::new(authz_configuration.id(), peer),
            )
            .await?
            .try_into()
    }

    pub async fn router_configuration_bgp_peer_delete(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
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
                peer,
            )
            .await
    }

    pub async fn router_configuration_static_route_list(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
    ) -> ListResultVec<networking::StaticRoute> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Read)
            .await?;
        Ok(self
            .db_datastore
            .router_configuration_static_route_list(opctx, &authz_configuration)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub async fn router_configuration_static_route_create(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        route: networking::StaticRoute,
    ) -> CreateResult<networking::StaticRoute> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        Ok(self
            .db_datastore
            .router_configuration_static_route_create(
                opctx,
                RouterConfigurationStaticRoute::new(
                    authz_configuration.id(),
                    route,
                ),
            )
            .await?
            .into())
    }

    pub async fn router_configuration_static_route_view(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
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
                route,
            )
            .await?
            .into())
    }

    pub async fn router_configuration_static_route_update(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        route_name: &Name,
        route: networking::StaticRoute,
    ) -> UpdateResult<networking::StaticRoute> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        Ok(self
            .db_datastore
            .router_configuration_static_route_update(
                opctx,
                &authz_configuration,
                route_name,
                RouterConfigurationStaticRoute::new(
                    authz_configuration.id(),
                    route,
                ),
            )
            .await?
            .into())
    }

    pub async fn router_configuration_static_route_delete(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
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
                route,
            )
            .await
    }

    pub async fn router_configuration_bfd_peer_list(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
    ) -> ListResultVec<networking::BfdPeer> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Read)
            .await?;
        Ok(self
            .db_datastore
            .router_configuration_bfd_peer_list(opctx, &authz_configuration)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    pub async fn router_configuration_bfd_peer_create(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        peer: networking::BfdPeer,
    ) -> CreateResult<networking::BfdPeer> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        Ok(self
            .db_datastore
            .router_configuration_bfd_peer_create(
                opctx,
                RouterConfigurationBfdPeer::new(authz_configuration.id(), peer),
            )
            .await?
            .into())
    }

    pub async fn router_configuration_bfd_peer_view(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        peer: &Name,
    ) -> LookupResult<networking::BfdPeer> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Read)
            .await?;
        Ok(self
            .db_datastore
            .router_configuration_bfd_peer_view(
                opctx,
                &authz_configuration,
                peer,
            )
            .await?
            .into())
    }

    pub async fn router_configuration_bfd_peer_update(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        peer_name: &Name,
        peer: networking::BfdPeer,
    ) -> UpdateResult<networking::BfdPeer> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let (.., authz_configuration) = self
            .router_configuration_lookup(opctx, name_or_id)?
            .lookup_for(authz::Action::Modify)
            .await?;
        Ok(self
            .db_datastore
            .router_configuration_bfd_peer_update(
                opctx,
                &authz_configuration,
                peer_name,
                RouterConfigurationBfdPeer::new(authz_configuration.id(), peer),
            )
            .await?
            .into())
    }

    pub async fn router_configuration_bfd_peer_delete(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
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
                peer,
            )
            .await
    }
}
