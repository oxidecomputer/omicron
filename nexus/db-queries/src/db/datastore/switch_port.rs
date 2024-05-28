// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::address_lot::{
    ReserveBlockError, ReserveBlockTxnError,
};
use crate::db::datastore::UpdatePrecondition;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::{
    LldpServiceConfig, Name, SwitchInterfaceConfig, SwitchPort,
    SwitchPortAddressConfig, SwitchPortBgpPeerConfig, SwitchPortConfig,
    SwitchPortLinkConfig, SwitchPortRouteConfig, SwitchPortSettings,
    SwitchPortSettingsGroup, SwitchPortSettingsGroups,
    SwitchVlanInterfaceConfig,
};
use crate::db::pagination::paginated;
use crate::transaction_retry::OptionalError;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::{
    CombineDsl, ExpressionMethods, JoinOnDsl, NullableExpressionMethods,
    QueryDsl, SelectableHelper,
};
use nexus_types::external_api::params;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{
    self, CreateResult, DataPageParams, DeleteResult, Error, ListResultVec,
    LookupResult, NameOrId, ResourceType, UpdateResult,
};
use ref_cast::RefCast;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SwitchPortSettingsCombinedResult {
    pub settings: SwitchPortSettings,
    pub groups: Vec<SwitchPortSettingsGroups>,
    pub port: SwitchPortConfig,
    pub links: Vec<SwitchPortLinkConfig>,
    pub link_lldp: Vec<LldpServiceConfig>,
    pub interfaces: Vec<SwitchInterfaceConfig>,
    pub vlan_interfaces: Vec<SwitchVlanInterfaceConfig>,
    pub routes: Vec<SwitchPortRouteConfig>,
    pub bgp_peers: Vec<SwitchPortBgpPeerConfig>,
    pub addresses: Vec<SwitchPortAddressConfig>,
}

impl SwitchPortSettingsCombinedResult {
    fn new(settings: SwitchPortSettings, port: SwitchPortConfig) -> Self {
        SwitchPortSettingsCombinedResult {
            settings,
            port,
            groups: Vec::new(),
            links: Vec::new(),
            link_lldp: Vec::new(),
            interfaces: Vec::new(),
            vlan_interfaces: Vec::new(),
            routes: Vec::new(),
            bgp_peers: Vec::new(),
            addresses: Vec::new(),
        }
    }
}

impl Into<external::SwitchPortSettingsView>
    for SwitchPortSettingsCombinedResult
{
    fn into(self) -> external::SwitchPortSettingsView {
        external::SwitchPortSettingsView {
            settings: self.settings.into(),
            port: self.port.into(),
            groups: self.groups.into_iter().map(Into::into).collect(),
            links: self.links.into_iter().map(Into::into).collect(),
            link_lldp: self.link_lldp.into_iter().map(Into::into).collect(),
            interfaces: self.interfaces.into_iter().map(Into::into).collect(),
            vlan_interfaces: self
                .vlan_interfaces
                .into_iter()
                .map(Into::into)
                .collect(),
            routes: self.routes.into_iter().map(Into::into).collect(),
            bgp_peers: self.bgp_peers.into_iter().map(Into::into).collect(),
            addresses: self.addresses.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SwitchPortSettingsGroupCreateResult {
    pub group: SwitchPortSettingsGroup,
    pub settings: SwitchPortSettingsCombinedResult,
}

impl DataStore {
    pub async fn switch_port_settings_exist(
        &self,
        opctx: &OpContext,
        name: Name,
    ) -> LookupResult<Uuid> {
        use db::schema::switch_port_settings::{
            self, dsl as port_settings_dsl,
        };

        let pool = self.pool_connection_authorized(opctx).await?;

        port_settings_dsl::switch_port_settings
            .filter(switch_port_settings::time_deleted.is_null())
            .filter(switch_port_settings::name.eq(name))
            .select(switch_port_settings::id)
            .limit(1)
            .first_async::<Uuid>(&*pool)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn switch_ports_using_settings(
        &self,
        opctx: &OpContext,
        switch_port_settings_id: Uuid,
    ) -> LookupResult<Vec<(Uuid, Name)>> {
        use db::schema::switch_port::{self, dsl};

        let pool = self.pool_connection_authorized(opctx).await?;

        dsl::switch_port
            .filter(switch_port::port_settings_id.eq(switch_port_settings_id))
            .select((switch_port::id, switch_port::port_name))
            .load_async(&*pool)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn switch_port_settings_create(
        &self,
        opctx: &OpContext,
        params: &params::SwitchPortSettingsCreate,
        id: Option<Uuid>,
    ) -> CreateResult<SwitchPortSettingsCombinedResult> {
        use db::schema::{
            address_lot::dsl as address_lot_dsl,
            //XXX ANNOUNCE bgp_announce_set::dsl as bgp_announce_set_dsl,
            bgp_config::dsl as bgp_config_dsl,
            lldp_service_config::dsl as lldp_config_dsl,
            switch_port_settings::dsl as port_settings_dsl,
            switch_port_settings_address_config::dsl as address_config_dsl,
            switch_port_settings_bgp_peer_config::dsl as bgp_peer_dsl,
            switch_port_settings_interface_config::dsl as interface_config_dsl,
            switch_port_settings_link_config::dsl as link_config_dsl,
            switch_port_settings_port_config::dsl as port_config_dsl,
            switch_port_settings_route_config::dsl as route_config_dsl,
            switch_vlan_interface_config::dsl as vlan_config_dsl,
        };

        #[derive(Debug)]
        enum SwitchPortSettingsCreateError {
            AddressLotNotFound,
            //XXX ANNOUNCE BgpAnnounceSetNotFound,
            BgpConfigNotFound,
            ReserveBlock(ReserveBlockError),
        }
        type SpsCreateError = SwitchPortSettingsCreateError;

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        // TODO https://github.com/oxidecomputer/omicron/issues/2811
        // Audit external networking database transaction usage
        self.transaction_retry_wrapper("switch_port_settings_create")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    // create the top level port settings object
                    let port_settings = match id {
                        Some(id) => SwitchPortSettings::with_id(id, &params.identity),
                        None => SwitchPortSettings::new(&params.identity),
                    };
                    //let port_settings = SwitchPortSettings::new(&params.identity);
                    let db_port_settings: SwitchPortSettings =
                        diesel::insert_into(port_settings_dsl::switch_port_settings)
                            .values(port_settings)
                            .returning(SwitchPortSettings::as_returning())
                            .get_result_async(&conn)
                            .await?;

                    let psid = db_port_settings.identity.id;

                    // add the port config
                    let port_config = SwitchPortConfig::new(
                        psid,
                        params.port_config.geometry.into(),
                    );

                    let db_port_config: SwitchPortConfig =
                        diesel::insert_into(port_config_dsl::switch_port_settings_port_config)
                            .values(port_config)
                            .returning(SwitchPortConfig::as_returning())
                            .get_result_async(&conn)
                            .await?;

                    let mut result = SwitchPortSettingsCombinedResult{
                        settings: db_port_settings,
                        groups: Vec::new(),
                        port: db_port_config,
                        links: Vec::new(),
                        link_lldp: Vec::new(),
                        interfaces: Vec::new(),
                        vlan_interfaces: Vec::new(),
                        routes: Vec::new(),
                        bgp_peers: Vec::new(),
                        addresses: Vec::new(),
                    };

                    //TODO validate link configs consistent with port geometry.
                    // - https://github.com/oxidecomputer/omicron/issues/2816

                    let mut lldp_config = Vec::with_capacity(params.links.len());
                    let mut link_config = Vec::with_capacity(params.links.len());

                    for (link_name, c) in &params.links {
                        let lldp_config_id = match c.lldp.lldp_config {
                            Some(_) => todo!(), // TODO actual lldp support
                            None => None,
                        };
                        let lldp_svc_config =
                            LldpServiceConfig::new(c.lldp.enabled, lldp_config_id);

                        lldp_config.push(lldp_svc_config.clone());
                        link_config.push(SwitchPortLinkConfig::new(
                            psid,
                            lldp_svc_config.id,
                            link_name.clone(),
                            c.mtu,
                            c.fec.into(),
                            c.speed.into(),
                            c.autoneg,
                        ));
                    }
                    result.link_lldp =
                        diesel::insert_into(lldp_config_dsl::lldp_service_config)
                        .values(lldp_config.clone())
                        .returning(LldpServiceConfig::as_returning())
                        .get_results_async(&conn)
                        .await?;
                    result.links =
                        diesel::insert_into(
                            link_config_dsl::switch_port_settings_link_config)
                        .values(link_config)
                        .returning(SwitchPortLinkConfig::as_returning())
                        .get_results_async(&conn)
                        .await?;

                    let mut interface_config = Vec::with_capacity(params.interfaces.len());
                    let mut vlan_interface_config = Vec::new();
                    for (interface_name, i) in &params.interfaces {
                        let ifx_config = SwitchInterfaceConfig::new(
                            psid,
                            interface_name.clone(),
                            i.v6_enabled,
                            i.kind.into(),
                        );
                        interface_config.push(ifx_config.clone());
                        if let params::SwitchInterfaceKind::Vlan(vlan_if) = i.kind {
                            vlan_interface_config.push(SwitchVlanInterfaceConfig::new(
                                ifx_config.id,
                                vlan_if.vid,
                            ));
                        }
                    }
                    result.interfaces =
                        diesel::insert_into(
                            interface_config_dsl::switch_port_settings_interface_config)
                        .values(interface_config)
                        .returning(SwitchInterfaceConfig::as_returning())
                        .get_results_async(&conn)
                        .await?;
                    result.vlan_interfaces =
                        diesel::insert_into(vlan_config_dsl::switch_vlan_interface_config)
                        .values(vlan_interface_config)
                        .returning(SwitchVlanInterfaceConfig::as_returning())
                        .get_results_async(&conn)
                        .await?;

                    let mut route_config = Vec::with_capacity(params.routes.len());

                    for (interface_name, r) in &params.routes {
                        for route in &r.routes {
                            route_config.push(SwitchPortRouteConfig::new(
                                psid,
                                interface_name.clone(),
                                route.dst.into(),
                                route.gw.into(),
                                route.vid.map(Into::into),
                            ));
                        }
                    }
                    result.routes =
                        diesel::insert_into(
                            route_config_dsl::switch_port_settings_route_config)
                        .values(route_config)
                        .returning(SwitchPortRouteConfig::as_returning())
                        .get_results_async(&conn)
                        .await?;

                    let mut bgp_peer_config = Vec::new();
                    for (interface_name, peer_config) in &params.bgp_peers {
                        for p in &peer_config.peers {
                            use db::schema::bgp_config;
                            let bgp_config_id = match &p.bgp_config {
                                NameOrId::Id(id) => *id,
                                NameOrId::Name(name) => {
                                    let name = name.to_string();
                                    bgp_config_dsl::bgp_config
                                        .filter(bgp_config::time_deleted.is_null())
                                        .filter(bgp_config::name.eq(name))
                                        .select(bgp_config::id)
                                        .limit(1)
                                        .first_async::<Uuid>(&conn)
                                        .await
                                        .map_err(|diesel_error| {
                                            err.bail_retryable_or(
                                                diesel_error,
                                                SwitchPortSettingsCreateError::BgpConfigNotFound
                                            )
                                        })?
                                }
                            };

                            bgp_peer_config.push(SwitchPortBgpPeerConfig::new(
                                psid,
                                bgp_config_id,
                                interface_name.clone(),
                                p.addr.into(),
                                p.hold_time.into(),
                                p.idle_hold_time.into(),
                                p.delay_open.into(),
                                p.connect_retry.into(),
                                p.keepalive.into(),
                            ));

                        }
                    }
                    result.bgp_peers =
                        diesel::insert_into(
                            bgp_peer_dsl::switch_port_settings_bgp_peer_config)
                        .values(bgp_peer_config)
                        .returning(SwitchPortBgpPeerConfig::as_returning())
                        .get_results_async(&conn)
                        .await?;

                    let mut address_config = Vec::new();
                    use db::schema::address_lot;
                    for (interface_name, a) in &params.addresses {
                        for address in &a.addresses {
                            let address_lot_id = match &address.address_lot {
                                NameOrId::Id(id) => *id,
                                NameOrId::Name(name) => {
                                    let name = name.to_string();
                                    address_lot_dsl::address_lot
                                    .filter(address_lot::time_deleted.is_null())
                                    .filter(address_lot::name.eq(name))
                                    .select(address_lot::id)
                                    .limit(1)
                                    .first_async::<Uuid>(&conn)
                                    .await
                                    .map_err(|diesel_error| {
                                        err.bail_retryable_or(
                                            diesel_error,
                                            SwitchPortSettingsCreateError::AddressLotNotFound
                                        )
                                    })?
                                }
                            };
                            // TODO: Reduce DB round trips needed for reserving ip blocks
                            // https://github.com/oxidecomputer/omicron/issues/3060
                            let (block, rsvd_block) =
                                crate::db::datastore::address_lot::try_reserve_block(
                                    address_lot_id,
                                    address.address.ip().into(),
                                    // TODO: Should we allow anycast addresses for switch_ports?
                                    // anycast
                                    false,
                                    &conn
                                )
                                .await
                                .map_err(|e| match e {
                                    ReserveBlockTxnError::CustomError(e) => {
                                        err.bail(SwitchPortSettingsCreateError::ReserveBlock(e))
                                    }
                                    ReserveBlockTxnError::Database(e) => e,
                                })?;

                            address_config.push(SwitchPortAddressConfig::new(
                                psid,
                                block.id,
                                rsvd_block.id,
                                address.address.into(),
                                interface_name.clone(),
                            ));

                        }
                    }
                    result.addresses =
                        diesel::insert_into(
                            address_config_dsl::switch_port_settings_address_config)
                        .values(address_config)
                        .returning(SwitchPortAddressConfig::as_returning())
                        .get_results_async(&conn)
                        .await?;

                    Ok(result)
                }
            }
        )
        .await
        .map_err(|e| {
            if let Some(err) = err.take() {
                match err {
                    SpsCreateError::AddressLotNotFound => {
                        Error::invalid_request("AddressLot not found")
                    }
                    SpsCreateError::BgpConfigNotFound => {
                        Error::invalid_request("BGP config not found")
                    }
                    SwitchPortSettingsCreateError::ReserveBlock(
                        ReserveBlockError::AddressUnavailable
                    ) => Error::invalid_request("address unavailable"),
                    SwitchPortSettingsCreateError::ReserveBlock(
                        ReserveBlockError::AddressNotInLot
                    ) => Error::invalid_request("address not in lot"),
                }
            } else {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::SwitchPortSettings,
                        params.identity.name.as_str(),
                    ),
                )
            }
        })
    }

    pub async fn switch_port_settings_delete(
        &self,
        opctx: &OpContext,
        params: &params::SwitchPortSettingsSelector,
    ) -> DeleteResult {
        use db::schema::switch_port_settings::dsl as port_settings_dsl;

        #[derive(Debug)]
        enum SwitchPortSettingsDeleteError {
            SwitchPortSettingsNotFound,
        }

        let conn = self.pool_connection_authorized(opctx).await?;

        let selector = match &params.port_settings {
            None => return Err(Error::invalid_request("name or id required")),
            Some(name_or_id) => name_or_id,
        };

        let err = OptionalError::new();

        // TODO https://github.com/oxidecomputer/omicron/issues/2811
        // Audit external networking database transaction usage
        self.transaction_retry_wrapper("switch_port_settings_delete")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                use db::schema::switch_port_settings;
                let id = match selector {
                    NameOrId::Id(id) => *id,
                    NameOrId::Name(name) => {
                        let name = name.to_string();
                        port_settings_dsl::switch_port_settings
                            .filter(switch_port_settings::time_deleted.is_null())
                            .filter(switch_port_settings::name.eq(name))
                            .select(switch_port_settings::id)
                            .limit(1)
                            .first_async::<Uuid>(&conn)
                            .await
                            .map_err(|diesel_error| {
                                err.bail_retryable_or(
                                    diesel_error,
                                    SwitchPortSettingsDeleteError::SwitchPortSettingsNotFound
                                )
                            })?
                    }
                };

                // delete the top level port settings object
                diesel::delete(port_settings_dsl::switch_port_settings)
                    .filter(switch_port_settings::id.eq(id))
                    .execute_async(&conn)
                    .await?;

                // delete the port config object
                use db::schema::switch_port_settings_port_config::{
                    self as sps_port_config, dsl as port_config_dsl,
                };
                diesel::delete(port_config_dsl::switch_port_settings_port_config)
                    .filter(sps_port_config::port_settings_id.eq(id))
                    .execute_async(&conn)
                    .await?;

                // delete the link configs
                use db::schema::switch_port_settings_link_config::{
                    self as sps_link_config, dsl as link_config_dsl,
                };
                let links: Vec<SwitchPortLinkConfig> =
                    diesel::delete(
                        link_config_dsl::switch_port_settings_link_config
                    )
                    .filter(
                        sps_link_config::port_settings_id.eq(id)
                    )
                    .returning(SwitchPortLinkConfig::as_returning())
                    .get_results_async(&conn)
                    .await?;

                // delete lldp configs
                use db::schema::lldp_service_config::{self, dsl as lldp_config_dsl};
                let lldp_svc_ids: Vec<Uuid> = links
                    .iter()
                    .map(|link| link.lldp_service_config_id)
                    .collect();
                diesel::delete(lldp_config_dsl::lldp_service_config)
                    .filter(lldp_service_config::id.eq_any(lldp_svc_ids))
                    .execute_async(&conn)
                    .await?;

                // delete interface configs
                use db::schema::switch_port_settings_interface_config::{
                    self as sps_interface_config, dsl as interface_config_dsl,
                };

                let interfaces: Vec<SwitchInterfaceConfig> =
                    diesel::delete(
                        interface_config_dsl::switch_port_settings_interface_config
                    )
                    .filter(
                        sps_interface_config::port_settings_id.eq(id)
                    )
                    .returning(SwitchInterfaceConfig::as_returning())
                    .get_results_async(&conn)
                    .await?;

                // delete any vlan interfaces
                use db::schema::switch_vlan_interface_config::{
                    self, dsl as vlan_config_dsl,
                };
                let interface_ids: Vec<Uuid> = interfaces
                    .iter()
                    .map(|interface| interface.id)
                    .collect();

                diesel::delete(vlan_config_dsl::switch_vlan_interface_config)
                    .filter(
                        switch_vlan_interface_config::interface_config_id.eq_any(
                            interface_ids
                        )
                    )
                    .execute_async(&conn)
                    .await?;

                // delete route configs
                use db::schema::switch_port_settings_route_config;
                use db::schema::switch_port_settings_route_config::dsl
                    as route_config_dsl;

                diesel::delete(
                    route_config_dsl::switch_port_settings_route_config
                )
                .filter(switch_port_settings_route_config::port_settings_id.eq(id))
                .execute_async(&conn)
                .await?;

                // delete bgp configurations
                use db::schema::switch_port_settings_bgp_peer_config as bgp_peer;
                use db::schema::switch_port_settings_bgp_peer_config::dsl
                    as bgp_peer_dsl;

                diesel::delete(bgp_peer_dsl::switch_port_settings_bgp_peer_config)
                    .filter(bgp_peer::port_settings_id.eq(id))
                    .execute_async(&conn)
                    .await?;

                // delete address configs
                use db::schema::switch_port_settings_address_config::{
                    self as address_config, dsl as address_config_dsl,
                };

                let port_settings_addrs = diesel::delete(
                    address_config_dsl::switch_port_settings_address_config,
                )
                .filter(address_config::port_settings_id.eq(id))
                .returning(SwitchPortAddressConfig::as_returning())
                .get_results_async(&conn)
                .await?;

                use db::schema::address_lot_rsvd_block::dsl as rsvd_block_dsl;

                for ps in &port_settings_addrs {
                    diesel::delete(rsvd_block_dsl::address_lot_rsvd_block)
                        .filter(rsvd_block_dsl::id.eq(ps.rsvd_address_lot_block_id))
                        .execute_async(&conn)
                        .await?;
                }

                Ok(())
            }
        })
        .await
        .map_err(|e| {
            if let Some(err) = err.take() {
                match err {
                    SwitchPortSettingsDeleteError::SwitchPortSettingsNotFound => {
                        Error::invalid_request("port settings not found")
                    }
                }
            } else {
                let name = match &params.port_settings {
                    Some(name_or_id) => name_or_id.to_string(),
                    None => String::new(),
                };
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::SwitchPortSettings,
                        &name,
                    ),
                )
            }
        })
    }

    pub async fn switch_port_settings_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<SwitchPortSettings> {
        use db::schema::switch_port_settings::dsl;

        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::switch_port_settings, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::switch_port_settings,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::time_deleted.is_null())
        .select(SwitchPortSettings::as_select())
        .load_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn switch_port_settings_get(
        &self,
        opctx: &OpContext,
        name_or_id: &NameOrId,
    ) -> LookupResult<SwitchPortSettingsCombinedResult> {
        #[derive(Debug)]
        enum SwitchPortSettingsGetError {
            NotFound(external::Name),
        }

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        // TODO https://github.com/oxidecomputer/omicron/issues/2811
        // Audit external networking database transaction usage
        self.transaction_retry_wrapper("switch_port_settings_get")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                // get the top level port settings object
                use db::schema::switch_port_settings::{
                    self, dsl as port_settings_dsl,
                };

                let id = match name_or_id {
                    NameOrId::Id(id) => *id,
                    NameOrId::Name(name) => {
                        let name_str = name.to_string();
                        port_settings_dsl::switch_port_settings
                            .filter(switch_port_settings::time_deleted.is_null())
                            .filter(switch_port_settings::name.eq(name_str))
                            .select(switch_port_settings::id)
                            .limit(1)
                            .first_async::<Uuid>(&conn)
                            .await
                            .map_err(|diesel_error| {
                                err.bail_retryable_or_else(diesel_error, |_| {
                                    SwitchPortSettingsGetError::NotFound(
                                        name.clone(),
                                    )
                                })
                            })?
                    }
                };

                let settings: SwitchPortSettings =
                    port_settings_dsl::switch_port_settings
                        .filter(switch_port_settings::time_deleted.is_null())
                        .filter(switch_port_settings::id.eq(id))
                        .select(SwitchPortSettings::as_select())
                        .limit(1)
                        .first_async::<SwitchPortSettings>(&conn)
                        .await?;

                // get the port config
                use db::schema::switch_port_settings_port_config::{
                    self as port_config, dsl as port_config_dsl,
                };
                let port: SwitchPortConfig =
                    port_config_dsl::switch_port_settings_port_config
                        .filter(port_config::port_settings_id.eq(id))
                        .select(SwitchPortConfig::as_select())
                        .limit(1)
                        .first_async::<SwitchPortConfig>(&conn)
                        .await?;

                // initialize result
                let mut result =
                    SwitchPortSettingsCombinedResult::new(settings, port);

                // get the link configs
                use db::schema::switch_port_settings_link_config::{
                    self as link_config, dsl as link_config_dsl,
                };

                result.links = link_config_dsl::switch_port_settings_link_config
                    .filter(link_config::port_settings_id.eq(id))
                    .select(SwitchPortLinkConfig::as_select())
                    .load_async::<SwitchPortLinkConfig>(&conn)
                    .await?;

                let lldp_svc_ids: Vec<Uuid> = result
                    .links
                    .iter()
                    .map(|link| link.lldp_service_config_id)
                    .collect();

                use db::schema::lldp_service_config as lldp_config;
                use db::schema::lldp_service_config::dsl as lldp_dsl;
                result.link_lldp = lldp_dsl::lldp_service_config
                    .filter(lldp_config::id.eq_any(lldp_svc_ids))
                    .select(LldpServiceConfig::as_select())
                    .limit(1)
                    .load_async::<LldpServiceConfig>(&conn)
                    .await?;

                // get the interface configs
                use db::schema::switch_port_settings_interface_config::{
                    self as interface_config, dsl as interface_config_dsl,
                };

                result.interfaces =
                    interface_config_dsl::switch_port_settings_interface_config
                        .filter(interface_config::port_settings_id.eq(id))
                        .select(SwitchInterfaceConfig::as_select())
                        .load_async::<SwitchInterfaceConfig>(&conn)
                        .await?;

                use db::schema::switch_vlan_interface_config as vlan_config;
                use db::schema::switch_vlan_interface_config::dsl as vlan_dsl;
                let interface_ids: Vec<Uuid> = result
                    .interfaces
                    .iter()
                    .map(|interface| interface.id)
                    .collect();

                result.vlan_interfaces = vlan_dsl::switch_vlan_interface_config
                    .filter(vlan_config::interface_config_id.eq_any(interface_ids))
                    .select(SwitchVlanInterfaceConfig::as_select())
                    .load_async::<SwitchVlanInterfaceConfig>(&conn)
                    .await?;

                // get the route configs
                use db::schema::switch_port_settings_route_config::{
                    self as route_config, dsl as route_config_dsl,
                };

                result.routes = route_config_dsl::switch_port_settings_route_config
                    .filter(route_config::port_settings_id.eq(id))
                    .select(SwitchPortRouteConfig::as_select())
                    .load_async::<SwitchPortRouteConfig>(&conn)
                    .await?;

                // get the bgp peer configs
                use db::schema::switch_port_settings_bgp_peer_config::{
                    self as bgp_peer, dsl as bgp_peer_dsl,
                };

                result.bgp_peers =
                    bgp_peer_dsl::switch_port_settings_bgp_peer_config
                        .filter(bgp_peer::port_settings_id.eq(id))
                        .select(SwitchPortBgpPeerConfig::as_select())
                        .load_async::<SwitchPortBgpPeerConfig>(&conn)
                        .await?;

                // get the address configs
                use db::schema::switch_port_settings_address_config::{
                    self as address_config, dsl as address_config_dsl,
                };

                result.addresses =
                    address_config_dsl::switch_port_settings_address_config
                        .filter(address_config::port_settings_id.eq(id))
                        .select(SwitchPortAddressConfig::as_select())
                        .load_async::<SwitchPortAddressConfig>(&conn)
                        .await?;

                Ok(result)
            }
        })
        .await
        .map_err(|e| {
            if let Some(err) = err.take() {
                match err {
                    SwitchPortSettingsGetError::NotFound(name) => {
                        Error::not_found_by_name(
                            ResourceType::SwitchPortSettings,
                            &name,
                        )
                    }
                }
            } else {
                let name = name_or_id.to_string();
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::SwitchPortSettings,
                        &name,
                    ),
                )
            }
        })
    }

    // switch ports

    pub async fn switch_port_create(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        switch_location: Name,
        port: Name,
    ) -> CreateResult<SwitchPort> {
        #[derive(Debug)]
        enum SwitchPortCreateError {
            RackNotFound,
        }

        let err = OptionalError::new();

        let conn = self.pool_connection_authorized(opctx).await?;
        let switch_port = SwitchPort::new(
            rack_id,
            switch_location.to_string(),
            port.to_string(),
        );

        // TODO https://github.com/oxidecomputer/omicron/issues/2811
        // Audit external networking database transaction usage
        self.transaction_retry_wrapper("switch_port_create")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let switch_port = switch_port.clone();
                async move {
                    use db::schema::rack;
                    use db::schema::rack::dsl as rack_dsl;
                    rack_dsl::rack
                        .filter(rack::id.eq(rack_id))
                        .select(rack::id)
                        .limit(1)
                        .first_async::<Uuid>(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or(
                                e,
                                SwitchPortCreateError::RackNotFound,
                            )
                        })?;

                    // insert switch port
                    use db::schema::switch_port::dsl as switch_port_dsl;
                    let db_switch_port: SwitchPort =
                        diesel::insert_into(switch_port_dsl::switch_port)
                            .values(switch_port)
                            .returning(SwitchPort::as_returning())
                            .get_result_async(&conn)
                            .await?;

                    Ok(db_switch_port)
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        SwitchPortCreateError::RackNotFound => {
                            Error::invalid_request("rack not found")
                        }
                    }
                } else {
                    public_error_from_diesel(
                        e,
                        ErrorHandler::Conflict(
                            ResourceType::SwitchPort,
                            &format!(
                                "{}/{}/{}",
                                rack_id, &switch_location, &port,
                            ),
                        ),
                    )
                }
            })
    }

    pub async fn switch_port_delete(
        &self,
        opctx: &OpContext,
        portname: &external::Name,
        params: &params::SwitchPortSelector,
    ) -> DeleteResult {
        #[derive(Debug)]
        enum SwitchPortDeleteError {
            NotFound,
            ActiveSettings,
        }

        let err = OptionalError::new();

        let conn = self.pool_connection_authorized(opctx).await?;

        // TODO https://github.com/oxidecomputer/omicron/issues/2811
        // Audit external networking database transaction usage
        self.transaction_retry_wrapper("switch_port_delete")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    use db::schema::switch_port;
                    use db::schema::switch_port::dsl as switch_port_dsl;

                    let switch_location = params.switch_location.to_string();
                    let port_name = portname.to_string();
                    let port: SwitchPort = switch_port_dsl::switch_port
                        .filter(switch_port::rack_id.eq(params.rack_id))
                        .filter(
                            switch_port::switch_location
                                .eq(switch_location.clone()),
                        )
                        .filter(switch_port::port_name.eq(port_name.clone()))
                        .select(SwitchPort::as_select())
                        .limit(1)
                        .first_async::<SwitchPort>(&conn)
                        .await
                        .map_err(|diesel_error| {
                            err.bail_retryable_or(
                                diesel_error,
                                SwitchPortDeleteError::NotFound,
                            )
                        })?;

                    if port.port_settings_id.is_some() {
                        return Err(
                            err.bail(SwitchPortDeleteError::ActiveSettings)
                        );
                    }

                    diesel::delete(switch_port_dsl::switch_port)
                        .filter(switch_port::id.eq(port.id))
                        .execute_async(&conn)
                        .await?;

                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        SwitchPortDeleteError::NotFound => {
                            let name = &portname.clone();
                            Error::not_found_by_name(
                                ResourceType::SwitchPort,
                                name,
                            )
                        }
                        SwitchPortDeleteError::ActiveSettings => {
                            Error::invalid_request(
                                "must clear port settings first",
                            )
                        }
                    }
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    pub async fn switch_port_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<SwitchPort> {
        use db::schema::switch_port::dsl;

        paginated(dsl::switch_port, dsl::id, pagparams)
            .select(SwitchPort::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn switch_port_get(
        &self,
        opctx: &OpContext,
        id: uuid::Uuid,
    ) -> LookupResult<SwitchPort> {
        use db::schema::switch_port;
        use db::schema::switch_port::dsl as switch_port_dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        switch_port_dsl::switch_port
            .filter(switch_port::id.eq(id))
            .select(SwitchPort::as_select())
            .limit(1)
            .first_async::<SwitchPort>(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn switch_port_set_settings_id(
        &self,
        opctx: &OpContext,
        switch_port_id: Uuid,
        port_settings_id: Option<Uuid>,
        current: UpdatePrecondition<Uuid>,
    ) -> UpdateResult<()> {
        use db::schema::switch_port;
        use db::schema::switch_port::dsl as switch_port_dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        match current {
            UpdatePrecondition::DontCare => {
                diesel::update(switch_port_dsl::switch_port)
                    .filter(switch_port::id.eq(switch_port_id))
                    .set(switch_port::port_settings_id.eq(port_settings_id))
                    .execute_async(&*conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?;
            }
            UpdatePrecondition::Null => {
                diesel::update(switch_port_dsl::switch_port)
                    .filter(switch_port::id.eq(switch_port_id))
                    .filter(switch_port::port_settings_id.is_null())
                    .set(switch_port::port_settings_id.eq(port_settings_id))
                    .execute_async(&*conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?;
            }
            UpdatePrecondition::Value(current_id) => {
                diesel::update(switch_port_dsl::switch_port)
                    .filter(switch_port::id.eq(switch_port_id))
                    .filter(switch_port::port_settings_id.eq(current_id))
                    .set(switch_port::port_settings_id.eq(port_settings_id))
                    .execute_async(&*conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?;
            }
        }

        Ok(())
    }

    pub async fn switch_port_get_id(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        switch_location: Name,
        port_name: Name,
    ) -> LookupResult<Uuid> {
        use db::schema::switch_port;
        use db::schema::switch_port::dsl as switch_port_dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        let id: Uuid = switch_port_dsl::switch_port
            .filter(switch_port::rack_id.eq(rack_id))
            .filter(
                switch_port::switch_location.eq(switch_location.to_string()),
            )
            .filter(switch_port::port_name.eq(port_name.to_string()))
            .select(switch_port::id)
            .limit(1)
            .first_async::<Uuid>(&*conn)
            .await
            .map_err(|_| {
                Error::not_found_by_name(ResourceType::SwitchPort, &port_name)
            })?;

        Ok(id)
    }

    pub async fn switch_port_settings_get_id(
        &self,
        opctx: &OpContext,
        name: Name,
    ) -> LookupResult<Uuid> {
        use db::schema::switch_port_settings;
        use db::schema::switch_port_settings::dsl as port_settings_dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        let db_name = name.to_string();
        let id = port_settings_dsl::switch_port_settings
            .filter(switch_port_settings::time_deleted.is_null())
            .filter(switch_port_settings::name.eq(db_name))
            .select(switch_port_settings::id)
            .limit(1)
            .first_async::<Uuid>(&*conn)
            .await
            .map_err(|_| {
                Error::not_found_by_name(
                    ResourceType::SwitchPortSettings,
                    &name,
                )
            })?;

        Ok(id)
    }

    pub async fn switch_ports_with_uplinks(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<SwitchPort> {
        use db::schema::{
            switch_port::dsl as switch_port_dsl,
            switch_port_settings_bgp_peer_config::dsl as bgp_peer_config_dsl,
            switch_port_settings_route_config::dsl as route_config_dsl,
        };

        switch_port_dsl::switch_port
            .filter(switch_port_dsl::port_settings_id.is_not_null())
            .inner_join(
                route_config_dsl::switch_port_settings_route_config
                    .on(switch_port_dsl::port_settings_id
                        .eq(route_config_dsl::port_settings_id.nullable())),
            )
            .select(SwitchPort::as_select())
            // TODO: #3592 Correctness
            // In single rack deployments there are only 64 ports. We'll need
            // pagination in the future, or maybe a way to constrain the query to
            // a rack?
            .limit(64)
            .union(
                switch_port_dsl::switch_port
                    .filter(switch_port_dsl::port_settings_id.is_not_null())
                    .inner_join(
                        bgp_peer_config_dsl::switch_port_settings_bgp_peer_config
                            .on(switch_port_dsl::port_settings_id
                                .eq(bgp_peer_config_dsl::port_settings_id.nullable()),
                        ),
                    )
                    .select(SwitchPort::as_select())
                    .limit(64),
            )
            .load_async::<SwitchPort>(
                &*self.pool_connection_authorized(opctx).await?,
            )
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

#[cfg(test)]
mod test {
    use crate::db::datastore::test_utils::datastore_test;
    use crate::db::datastore::UpdatePrecondition;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::external_api::params::{
        BgpAnnounceSetCreate, BgpConfigCreate, BgpPeer, BgpPeerConfig,
        SwitchPortConfigCreate, SwitchPortGeometry, SwitchPortSettingsCreate,
    };
    use omicron_common::api::external::{
        IdentityMetadataCreateParams, Name, NameOrId,
    };
    use omicron_test_utils::dev;
    use std::collections::HashMap;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_bgp_boundary_switches() {
        let logctx = dev::test_setup_log("test_bgp_boundary_switches");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let rack_id: Uuid =
            nexus_test_utils::RACK_UUID.parse().expect("parse uuid");
        let switch0: Name = "switch0".parse().expect("parse switch location");
        let qsfp0: Name = "qsfp0".parse().expect("parse qsfp0");

        let port_result = datastore
            .switch_port_create(&opctx, rack_id, switch0.into(), qsfp0.into())
            .await
            .expect("switch port create");

        let announce_set = BgpAnnounceSetCreate {
            identity: IdentityMetadataCreateParams {
                name: "test-announce-set".parse().unwrap(),
                description: "test bgp announce set".into(),
            },
            announcement: Vec::new(),
        };

        datastore.bgp_create_announce_set(&opctx, &announce_set).await.unwrap();

        let bgp_config = BgpConfigCreate {
            identity: IdentityMetadataCreateParams {
                name: "test-bgp-config".parse().unwrap(),
                description: "test bgp config".into(),
            },
            asn: 47,
            bgp_announce_set_id: NameOrId::Name(
                "test-announce-set".parse().unwrap(),
            ),
            vrf: None,
        };

        datastore.bgp_config_set(&opctx, &bgp_config).await.unwrap();

        let settings = SwitchPortSettingsCreate {
            identity: IdentityMetadataCreateParams {
                name: "test-settings".parse().unwrap(),
                description: "test settings".into(),
            },
            port_config: SwitchPortConfigCreate {
                geometry: SwitchPortGeometry::Qsfp28x1,
            },
            groups: Vec::new(),
            links: HashMap::new(),
            interfaces: HashMap::new(),
            routes: HashMap::new(),
            bgp_peers: HashMap::from([(
                "phy0".into(),
                BgpPeerConfig {
                    peers: vec![BgpPeer {
                        bgp_announce_set: NameOrId::Name(
                            "test-announce-set".parse().unwrap(),
                        ),
                        bgp_config: NameOrId::Name(
                            "test-bgp-config".parse().unwrap(),
                        ),
                        interface_name: "qsfp0".into(),
                        addr: "192.168.1.1".parse().unwrap(),
                        hold_time: 0,
                        idle_hold_time: 0,
                        delay_open: 0,
                        connect_retry: 0,
                        keepalive: 0,
                    }],
                },
            )]),
            addresses: HashMap::new(),
        };

        let settings_result = datastore
            .switch_port_settings_create(&opctx, &settings, None)
            .await
            .unwrap();

        datastore
            .switch_port_set_settings_id(
                &opctx,
                port_result.id,
                Some(settings_result.settings.identity.id),
                UpdatePrecondition::DontCare,
            )
            .await
            .unwrap();

        let uplink_ports =
            datastore.switch_ports_with_uplinks(&opctx).await.unwrap();

        assert_eq!(uplink_ports.len(), 1);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
