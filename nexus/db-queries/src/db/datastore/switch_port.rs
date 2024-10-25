// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;
use std::net::IpAddr;

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
    LldpLinkConfig, Name, SwitchInterfaceConfig, SwitchPort,
    SwitchPortAddressConfig, SwitchPortBgpPeerConfig, SwitchPortConfig,
    SwitchPortLinkConfig, SwitchPortRouteConfig, SwitchPortSettings,
    SwitchPortSettingsGroup, SwitchPortSettingsGroups,
    SwitchVlanInterfaceConfig,
};
use crate::db::pagination::paginated;
use crate::transaction_retry::OptionalError;
use async_bb8_diesel::{AsyncRunQueryDsl, Connection};
use diesel::CombineDsl;
use diesel::{
    ExpressionMethods, JoinOnDsl, NullableExpressionMethods, OptionalExtension,
    PgConnection, QueryDsl, SelectableHelper,
};
use diesel_dtrace::DTraceConnection;
use ipnetwork::IpNetwork;
use nexus_db_model::{
    BgpConfig, SqlU16, SqlU32, SqlU8, SwitchPortBgpPeerConfigAllowExport,
    SwitchPortBgpPeerConfigAllowImport, SwitchPortBgpPeerConfigCommunity,
    SwitchPortGeometry,
};
use nexus_types::external_api::params::{
    self, AllowedPrefixAddRemove, BgpCommunityAddRemove,
};
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{
    self, BgpPeer, BgpPeerRemove, CreateResult, DataPageParams, DeleteResult,
    Error, ImportExportPolicy, ListResultVec, LookupResult, NameOrId,
    ResourceType, SwitchLocation, UpdateResult,
};
use ref_cast::RefCast;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BgpPeerConfig {
    pub port_settings_id: Uuid,
    pub bgp_config_id: Uuid,
    pub interface_name: String,
    pub addr: IpNetwork,
    pub hold_time: SqlU32,
    pub idle_hold_time: SqlU32,
    pub delay_open: SqlU32,
    pub connect_retry: SqlU32,
    pub keepalive: SqlU32,
    pub remote_asn: Option<SqlU32>,
    pub min_ttl: Option<SqlU8>,
    pub md5_auth_key: Option<String>,
    pub multi_exit_discriminator: Option<SqlU32>,
    pub local_pref: Option<SqlU32>,
    pub enforce_first_as: bool,
    pub allowed_import: ImportExportPolicy,
    pub allowed_export: ImportExportPolicy,
    pub communities: Vec<u32>,
    pub vlan_id: Option<SqlU16>,
}

impl Into<external::BgpPeerCombined> for BgpPeerConfig {
    fn into(self) -> external::BgpPeerCombined {
        external::BgpPeerCombined {
            bgp_config: self.bgp_config_id.into(),
            interface_name: self.interface_name.clone(),
            addr: self.addr.ip(),
            hold_time: self.hold_time.into(),
            idle_hold_time: self.idle_hold_time.into(),
            delay_open: self.delay_open.into(),
            connect_retry: self.connect_retry.into(),
            keepalive: self.keepalive.into(),
            remote_asn: self.remote_asn.map(Into::into),
            min_ttl: self.min_ttl.map(Into::into),
            md5_auth_key: self.md5_auth_key.clone(),
            multi_exit_discriminator: self
                .multi_exit_discriminator
                .map(Into::into),
            communities: self.communities,
            local_pref: self.local_pref.map(Into::into),
            enforce_first_as: self.enforce_first_as,
            allowed_import: self.allowed_import,
            allowed_export: self.allowed_export,
            vlan_id: self.vlan_id.map(Into::into),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SwitchPortSettingsCombinedResult {
    pub settings: SwitchPortSettings,
    pub groups: Vec<SwitchPortSettingsGroups>,
    pub port: SwitchPortConfig,
    pub links: Vec<SwitchPortLinkConfig>,
    pub link_lldp: Vec<LldpLinkConfig>,
    pub interfaces: Vec<SwitchInterfaceConfig>,
    pub vlan_interfaces: Vec<SwitchVlanInterfaceConfig>,
    pub routes: Vec<SwitchPortRouteConfig>,
    pub bgp_peers: Vec<BgpPeerConfig>,
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
            .filter(switch_port_settings::name.eq(name.clone()))
            .select(switch_port_settings::id)
            .limit(1)
            .first_async::<Uuid>(&*pool)
            .await
            .map_err(|e| {
                let msg = "failed to lookup switch port settings by name";
                error!(opctx.log, "{msg}"; "error" => ?e);

                match e {
                    diesel::result::Error::NotFound => {
                        Error::not_found_by_name(
                            ResourceType::SwitchPortSettings,
                            &name,
                        )
                    }
                    _ => Error::internal_error(msg),
                }
            })
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
        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        // TODO https://github.com/oxidecomputer/omicron/issues/2811
        // Audit external networking database transaction usage
        self.transaction_retry_wrapper("switch_port_settings_create")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    do_switch_port_settings_create(&conn, id, params, err).await
                }
            })
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
                            ReserveBlockError::AddressUnavailable,
                        ) => Error::invalid_request("address unavailable"),
                        SwitchPortSettingsCreateError::ReserveBlock(
                            ReserveBlockError::AddressNotInLot,
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
        params: &NameOrId,
    ) -> DeleteResult {
        let conn = self.pool_connection_authorized(opctx).await?;

        let err = OptionalError::new();

        // TODO https://github.com/oxidecomputer/omicron/issues/2811
        // Audit external networking database transaction usage
        self.transaction_retry_wrapper("switch_port_settings_delete")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    do_switch_port_settings_delete(&conn, &params, err).await
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
                    let name = match params {
                        NameOrId::Id(id) => id.to_string(),
                        NameOrId::Name(name) => name.to_string(),
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

    pub async fn switch_port_settings_update(
        &self,
        opctx: &OpContext,
        params: &params::SwitchPortSettingsCreate,
        id: Uuid,
    ) -> UpdateResult<SwitchPortSettingsCombinedResult> {
        let delete_err = OptionalError::new();
        let create_err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        // TODO https://github.com/oxidecomputer/omicron/issues/2811
        // Audit external networking database transaction usage
        self.transaction_retry_wrapper("switch_port_settings_update")
            .transaction(&conn, |conn| {
                let delete_err = delete_err.clone();
                let create_err = create_err.clone();
                let selector = NameOrId::Id(id);
                async move {
                    do_switch_port_settings_delete(&conn, &selector, delete_err).await?;
                    do_switch_port_settings_create(&conn, Some(id), params, create_err).await
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = delete_err.take() {
                    match err {
                        SwitchPortSettingsDeleteError::SwitchPortSettingsNotFound => {
                            Error::invalid_request("port settings not found")
                        }
                    }
                }
                else if let Some(err) = create_err.take() {
                    match err {
                        SpsCreateError::AddressLotNotFound => {
                            Error::invalid_request("AddressLot not found")
                        }
                        SpsCreateError::BgpConfigNotFound => {
                            Error::invalid_request("BGP config not found")
                        }
                        SwitchPortSettingsCreateError::ReserveBlock(
                            ReserveBlockError::AddressUnavailable,
                        ) => Error::invalid_request("address unavailable"),
                        SwitchPortSettingsCreateError::ReserveBlock(
                            ReserveBlockError::AddressNotInLot,
                        ) => Error::invalid_request("address not in lot"),

                    }
                }
                else {
                    public_error_from_diesel(e, ErrorHandler::Server)
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
                    use db::schema::{
                        switch_port_settings_bgp_peer_config_allow_import::dsl as allow_import_dsl,
                        switch_port_settings_bgp_peer_config_allow_export::dsl as allow_export_dsl,
                        switch_port_settings_bgp_peer_config_communities::dsl as bgp_communities_dsl,
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

                    let lldp_link_ids: Vec<Uuid> = result
                        .links
                        .iter()
                        .filter_map(|link| link.lldp_link_config_id)
                        .collect();

                    use db::schema::lldp_link_config;
                    result.link_lldp = lldp_link_config::dsl::lldp_link_config
                        .filter(lldp_link_config::id.eq_any(lldp_link_ids))
                        .select(LldpLinkConfig::as_select())
                        .limit(1)
                        .load_async::<LldpLinkConfig>(&conn)
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

                    let peers: Vec<SwitchPortBgpPeerConfig> =
                        bgp_peer_dsl::switch_port_settings_bgp_peer_config
                        .filter(bgp_peer::port_settings_id.eq(id))
                        .select(SwitchPortBgpPeerConfig::as_select())
                        .load_async::<SwitchPortBgpPeerConfig>(&conn)
                        .await?;

                    for p in peers.iter() {
                        let allowed_import: ImportExportPolicy = if p.allow_import_list_active {
                            let db_list: Vec<SwitchPortBgpPeerConfigAllowImport> =
                                allow_import_dsl::switch_port_settings_bgp_peer_config_allow_import
                                .filter(allow_import_dsl::port_settings_id.eq(id))
                                .filter(allow_import_dsl::interface_name.eq(p.interface_name.clone()))
                                .filter(allow_import_dsl::addr.eq(p.addr))
                                .select(SwitchPortBgpPeerConfigAllowImport::as_select())
                                .load_async::<SwitchPortBgpPeerConfigAllowImport>(&conn)
                                .await?;

                            ImportExportPolicy::Allow(db_list
                                                      .into_iter()
                                                      .map(|x| x.prefix.into())
                                                      .collect()
                            )
                        } else {
                            ImportExportPolicy::NoFiltering
                        };

                        let allowed_export: ImportExportPolicy = if p.allow_export_list_active {
                            let db_list: Vec<SwitchPortBgpPeerConfigAllowExport> =
                                allow_export_dsl::switch_port_settings_bgp_peer_config_allow_export
                                .filter(allow_export_dsl::port_settings_id.eq(id))
                                .filter(allow_export_dsl::interface_name.eq(p.interface_name.clone()))
                                .filter(allow_export_dsl::addr.eq(p.addr))
                                .select(SwitchPortBgpPeerConfigAllowExport::as_select())
                                .load_async::<SwitchPortBgpPeerConfigAllowExport>(&conn)
                                .await?;

                            ImportExportPolicy::Allow(db_list
                                                      .into_iter()
                                                      .map(|x| x.prefix.into())
                                                      .collect()
                            )
                        } else {
                            ImportExportPolicy::NoFiltering
                        };

                        let communities: Vec<SwitchPortBgpPeerConfigCommunity> =
                            bgp_communities_dsl::switch_port_settings_bgp_peer_config_communities
                            .filter(bgp_communities_dsl::port_settings_id.eq(id))
                            .filter(bgp_communities_dsl::interface_name.eq(p.interface_name.clone()))
                            .filter(bgp_communities_dsl::addr.eq(p.addr))
                            .select(SwitchPortBgpPeerConfigCommunity::as_select())
                            .load_async::<SwitchPortBgpPeerConfigCommunity>(&conn)
                            .await?;

                        let view = BgpPeerConfig {
                            port_settings_id: p.port_settings_id,
                            bgp_config_id: p.bgp_config_id,
                            interface_name: p.interface_name.clone(),
                            addr: p.addr,
                            hold_time: p.hold_time,
                            idle_hold_time: p.idle_hold_time,
                            delay_open: p.delay_open,
                            connect_retry: p.connect_retry,
                            keepalive: p.keepalive,
                            remote_asn: p.remote_asn,
                            min_ttl: p.min_ttl,
                            md5_auth_key: p.md5_auth_key.clone(),
                            multi_exit_discriminator: p.multi_exit_discriminator,
                            local_pref: p.local_pref,
                            enforce_first_as: p.enforce_first_as,
                            vlan_id: p.vlan_id,
                            communities: communities.into_iter().map(|c| c.community.0).collect(),
                            allowed_import,
                            allowed_export,
                        };

                        result.bgp_peers.push(view);
                    }

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

    pub async fn switch_port_configuration_geometry_get(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
    ) -> LookupResult<SwitchPortConfig> {
        use db::schema::switch_port_settings as port_settings;
        use db::schema::switch_port_settings::dsl as port_settings_dsl;
        use db::schema::switch_port_settings_port_config::dsl;

        let dataset = port_settings_dsl::switch_port_settings.inner_join(
            dsl::switch_port_settings_port_config
                .on(dsl::port_settings_id.eq(port_settings_dsl::id)),
        );

        let query = match name_or_id {
            NameOrId::Id(id) => {
                // find port config using port settings id
                dataset.filter(port_settings::id.eq(id)).into_boxed()
            }
            NameOrId::Name(name) => {
                // find port config using port settings name
                dataset
                    .filter(port_settings::name.eq(name.to_string()))
                    .into_boxed()
            }
        };

        let geometry: SwitchPortConfig = query
            .select(SwitchPortConfig::as_select())
            .limit(1)
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(geometry)
    }

    pub async fn switch_port_configuration_geometry_set(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        new_geometry: SwitchPortGeometry,
    ) -> CreateResult<SwitchPortConfig> {
        use db::schema::switch_port_settings_port_config::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("switch_port_configuration_geometry_set")
            .transaction(&conn, |conn| {
                let identity = name_or_id.clone();
                async move {
                    // we query for the parent record instead of trusting that the
                    // uuid is valid, since we don't have true referential integrity between
                    // the tables
                    let port_settings_id =
                        switch_port_configuration_id(&conn, identity).await?;

                    let port_config = SwitchPortConfig {
                        port_settings_id,
                        geometry: new_geometry,
                    };

                    // create or update geometry
                    diesel::insert_into(dsl::switch_port_settings_port_config)
                        .values(port_config.clone())
                        .on_conflict(dsl::port_settings_id)
                        .do_update()
                        .set(dsl::geometry.eq(new_geometry))
                        .execute_async(&conn)
                        .await?;

                    Ok(port_config)
                }
            })
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn switch_port_configuration_link_list(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
    ) -> ListResultVec<SwitchPortLinkConfig> {
        use db::schema::switch_port_settings as port_settings;
        use db::schema::switch_port_settings::dsl as port_settings_dsl;
        use db::schema::switch_port_settings_link_config::dsl as link_dsl;

        let dataset = port_settings_dsl::switch_port_settings.inner_join(
            link_dsl::switch_port_settings_link_config
                .on(link_dsl::port_settings_id.eq(port_settings_dsl::id)),
        );

        let query = match name_or_id {
            NameOrId::Id(id) => {
                // find port config using port settings id
                dataset.filter(port_settings::id.eq(id)).into_boxed()
            }
            NameOrId::Name(name) => {
                // find port config using port settings name
                dataset
                    .filter(port_settings::name.eq(name.to_string()))
                    .into_boxed()
            }
        };

        let configs: Vec<SwitchPortLinkConfig> = query
            .select(SwitchPortLinkConfig::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(configs)
    }

    pub async fn switch_port_configuration_link_create(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        new_settings: params::NamedLinkConfigCreate,
    ) -> CreateResult<SwitchPortLinkConfig> {
        use db::schema::lldp_link_config::dsl as lldp_link_dsl;
        use db::schema::switch_port_settings_link_config::dsl as link_dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        let config = self
            .transaction_retry_wrapper("switch_port_configuration_link_create")
            .transaction(&conn, |conn| {
                let identity = name_or_id.clone();
                let new_settings = new_settings.clone();

                async move {
                    // fetch id of parent record
                    let parent_id =
                        switch_port_configuration_id(&conn, identity).await?;

                    let lldp_link_config_id = match new_settings.lldp_config {
                        Some(name_or_id) => {
                            let config_id =
                                lldp_configuration_id(&conn, name_or_id)
                                    .await?;
                            Ok::<Uuid, diesel::result::Error>(config_id)
                        }
                        None => {
                            let values = LldpLinkConfig::new(
                                false, None, None, None, None, None, None,
                            );
                            let lldp_config = diesel::insert_into(
                                lldp_link_dsl::lldp_link_config,
                            )
                            .values(values)
                            .returning(LldpLinkConfig::as_returning())
                            .get_result_async(&conn)
                            .await?;
                            Ok(lldp_config.id)
                        }
                    }?;

                    let link_config = SwitchPortLinkConfig {
                        port_settings_id: parent_id,
                        lldp_link_config_id: Some(lldp_link_config_id),
                        link_name: new_settings.name.to_string(),
                        mtu: new_settings.mtu.into(),
                        fec: new_settings.fec.into(),
                        speed: new_settings.speed.into(),
                        autoneg: new_settings.autoneg,
                    };

                    let link = diesel::insert_into(
                        link_dsl::switch_port_settings_link_config,
                    )
                    .values(link_config.clone())
                    .returning(SwitchPortLinkConfig::as_returning())
                    .get_result_async(&conn)
                    .await?;

                    Ok(link)
                }
            })
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(config)
    }

    pub async fn switch_port_configuration_link_view(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        link: Name,
    ) -> LookupResult<SwitchPortLinkConfig> {
        use db::schema::switch_port_settings as port_settings;
        use db::schema::switch_port_settings::dsl as port_settings_dsl;
        use db::schema::switch_port_settings_link_config::dsl as link_dsl;

        let dataset = port_settings_dsl::switch_port_settings.inner_join(
            link_dsl::switch_port_settings_link_config
                .on(link_dsl::port_settings_id.eq(port_settings_dsl::id)),
        );

        let query = match name_or_id {
            NameOrId::Id(id) => {
                // find port config using port settings id
                dataset.filter(port_settings::id.eq(id)).into_boxed()
            }
            NameOrId::Name(name) => {
                // find port config using port settings name
                dataset
                    .filter(port_settings::name.eq(name.to_string()))
                    .into_boxed()
            }
        };

        let config: SwitchPortLinkConfig = query
            .filter(link_dsl::link_name.eq(link.to_string()))
            .select(SwitchPortLinkConfig::as_select())
            .limit(1)
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(config)
    }

    pub async fn switch_port_configuration_link_delete(
        &self,
        opctx: &OpContext,
        name_or_id: NameOrId,
        link: Name,
    ) -> DeleteResult {
        use db::schema::lldp_link_config::dsl as lldp_link_dsl;
        use db::schema::switch_port_settings_link_config::dsl as link_dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("switch_port_configuration_link_delete")
            .transaction(&conn, |conn| {
                let identity = name_or_id.clone();
                let link_name = link.clone();

                async move {
                    // fetch id of parent record
                    let parent_id =
                        switch_port_configuration_id(&conn, identity).await?;

                    // delete child record
                    let config = diesel::delete(
                        link_dsl::switch_port_settings_link_config,
                    )
                    .filter(link_dsl::port_settings_id.eq(parent_id))
                    .filter(link_dsl::link_name.eq(link_name.to_string()))
                    .returning(SwitchPortLinkConfig::as_returning())
                    .get_result_async(&conn)
                    .await?;

                    // delete lldp service configuration
                    if let Some(lldp_config_id) = config.lldp_link_config_id {
                        diesel::delete(lldp_link_dsl::lldp_link_config)
                            .filter(lldp_link_dsl::id.eq(lldp_config_id))
                            .execute_async(&conn)
                            .await?;
                    }

                    Ok(())
                }
            })
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(())
    }

    pub async fn switch_port_configuration_address_list(
        &self,
        opctx: &OpContext,
        configuration: NameOrId,
    ) -> ListResultVec<SwitchPortAddressConfig> {
        use db::schema::switch_port_settings as port_settings;
        use db::schema::switch_port_settings_address_config as address_config;

        let dataset =
            port_settings::table
                .inner_join(address_config::table.on(
                    address_config::port_settings_id.eq(port_settings::id),
                ));

        let query = match configuration {
            NameOrId::Id(id) => {
                // find port config using port settings id
                dataset.filter(port_settings::id.eq(id)).into_boxed()
            }
            NameOrId::Name(name) => {
                // find port config using port settings name
                dataset
                    .filter(port_settings::name.eq(name.to_string()))
                    .into_boxed()
            }
        };

        let configs: Vec<SwitchPortAddressConfig> = query
            .select(SwitchPortAddressConfig::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e: diesel::result::Error| {
                let msg =
                    "error while looking up interface address configuration";
                match e {
                    diesel::result::Error::NotFound => {
                        Error::non_resourcetype_not_found(
                            "could not find address configuration for switch port configuration: {configuration}, interface: {interface}"
                        )},
                    _ => Error::internal_error(msg),
                }
            })?;

        Ok(configs)
    }

    pub async fn switch_port_configuration_address_add(
        &self,
        opctx: &OpContext,
        configuration: NameOrId,
        address: params::AddressAddRemove,
    ) -> CreateResult<SwitchPortAddressConfig> {
        use db::schema::address_lot;
        use db::schema::switch_port_settings_address_config as address_config;

        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        self.transaction_retry_wrapper(
            "switch_port_configuration_interface_address_add",
        )
            .transaction(&conn, |conn| {
                let parent_configuration = configuration.clone();
                let new_settings = address.clone();
                let err = err.clone();

                async move {

                    // resolve id of port_settings record
                    let port_settings_id = switch_port_configuration_id(&conn, parent_configuration.clone())
                        .await
                        .map_err(|e: diesel::result::Error| {
                            match e {
                                diesel::result::Error::NotFound => {
                                    err.bail(
                                        Error::non_resourcetype_not_found(
                                            format!("unable to lookup configuration with identifier {parent_configuration}")
                                        )
                                    )
                                },
                                _ => {
                                    err.bail(Error::internal_error(
                                        "error while looking up configuration for interface address"
                                    ))
                                },
                            }
                        })?;

                    // resolve id of referenced address lot
                    let address_lot_id = match new_settings.address_lot {

                        NameOrId::Id(id) => {
                            // verify id is valid
                            address_lot::table
                                .filter(address_lot::time_deleted.is_null())
                                .filter(address_lot::id.eq(id))
                                .select(address_lot::id)
                                .limit(1)
                                .first_async::<Uuid>(&conn)
                                .await
                                .map_err(|e: diesel::result::Error| {
                                    match e {
                                        diesel::result::Error::NotFound => {
                                            err.bail(Error::not_found_by_id(ResourceType::AddressLot, &id))
                                        },
                                        _ => {
                                            let message = "error while looking up address lot for interface address";
                                            error!(opctx.log, "{message}"; "error" => ?e);
                                            err.bail(Error::internal_error(message))
                                        },
                                    }
                                })
                        },

                        NameOrId::Name(name) => {
                            address_lot::table
                                .filter(address_lot::time_deleted.is_null())
                                .filter(address_lot::name.eq(name.to_string()))
                                .select(address_lot::id)
                                .limit(1)
                                .first_async::<Uuid>(&conn)
                                .await
                                .map_err(|e: diesel::result::Error| {
                                    match e {
                                        diesel::result::Error::NotFound => {
                                            err.bail(Error::not_found_by_name(ResourceType::AddressLot, &name))
                                        },
                                        _ => {
                                            let message = "error while looking up address lot for interface address";
                                            error!(opctx.log, "{message}"; "error" => ?e);
                                            err.bail(Error::internal_error(message))
                                        },
                                    }
                                })
                        }
                    }?;

                    // create a reservation
                    let (block, rsvd_block) =
                        crate::db::datastore::address_lot::try_reserve_block(
                            address_lot_id,
                            new_settings.address.addr().into(),
                            // TODO: Should we allow anycast addresses for switch_ports?
                            // anycast
                            false,
                            &conn,
                        )
                        .await
                        .map_err(|e| match e {
                            ReserveBlockTxnError::CustomError(e) => {
                                let message = match e {
                                    ReserveBlockError::AddressUnavailable => "address unavailable",
                                    ReserveBlockError::AddressNotInLot => "address not in lot",
                                };
                                err.bail(Error::conflict(message))
                            }
                            ReserveBlockTxnError::Database(e) => {
                                let message = "error while reserving address";
                                error!(opctx.log, "{message}"; "error" => ?e);
                                err.bail(Error::internal_error(message))
                            },
                        })?;

                    let address_config = SwitchPortAddressConfig {
                        port_settings_id,
                        address_lot_block_id: block.id,
                        rsvd_address_lot_block_id: rsvd_block.id,
                        address: new_settings.address.into(),
                        interface_name: new_settings.interface.to_string(),
                        vlan_id: new_settings.vlan_id.map(|i| i.into()),
                    };

                    let address = diesel::insert_into(
                        address_config::table,
                    )
                        .values(address_config)
                        .returning(SwitchPortAddressConfig::as_returning())
                        .get_result_async(&conn)
                        .await?;

                    Ok(address)
                }
            })
            .await
            .map_err(|e| {
                let message = "switch_port_configuration_interface_address_add failed";
                match err.take() {
                    Some(external_error) => {
                        error!(opctx.log, "{message}"; "error" => ?external_error);
                        external_error
                    },
                    None => {
                        error!(opctx.log, "{message}"; "error" => ?e);
                        Error::internal_error("error while adding address to interface")
                    },
                }
            })
    }

    pub async fn switch_port_configuration_address_remove(
        &self,
        opctx: &OpContext,
        configuration: NameOrId,
        address: params::AddressAddRemove,
    ) -> DeleteResult {
        use db::schema::switch_port_settings_address_config as address_config;

        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        self.transaction_retry_wrapper(
            "switch_port_configuration_interface_address_remove",
        )
            .transaction(&conn, |conn| {
                let parent_configuration = configuration.clone();
                let settings_to_remove = address.clone();
                let err = err.clone();

                async move {

                    // resolve id of port_settings record
                    let port_settings_id = switch_port_configuration_id(&conn, parent_configuration.clone())
                        .await
                        .map_err(|e: diesel::result::Error| {
                            match e {
                                diesel::result::Error::NotFound => {
                                    err.bail(
                                        Error::non_resourcetype_not_found(
                                            format!("unable to lookup configuration with identifier {parent_configuration}")
                                        )
                                    )
                                },
                                _ => {
                                    err.bail(Error::internal_error(
                                        "error while looking up configuration for interface address"
                                    ))
                                },
                            }
                        })?;

                    // find address config
                    let found_address_config = address_config::table
                        .filter(address_config::address.eq(IpNetwork::from(settings_to_remove.address)))
                        .filter(address_config::port_settings_id.eq(port_settings_id))
                        .filter(address_config::interface_name.eq(settings_to_remove.interface.clone().to_string()))
                        .select(SwitchPortAddressConfig::as_select())
                        .limit(1)
                        .first_async::<SwitchPortAddressConfig>(&conn)
                        .await
                        .map_err(|e: diesel::result::Error| {
                            match e {
                                diesel::result::Error::NotFound => {
                                    err.bail(Error::non_resourcetype_not_found("unable to find requested address config"))
                                },
                                _ => {
                                    let message = "error while looking up address lot for interface address";
                                    error!(opctx.log, "{message}"; "error" => ?e);
                                    err.bail(Error::internal_error(message))
                                },
                            }
                        })?;

                    // delete reservation
                    use db::schema::address_lot_rsvd_block as rsvd_block;
                    diesel::delete(rsvd_block::table)
                        .filter(rsvd_block::id.eq(found_address_config.rsvd_address_lot_block_id))
                        .execute_async(&conn)
                        .await?;

                    // delete address config
                    diesel::delete(address_config::table)
                        .filter(address_config::address.eq(IpNetwork::from(settings_to_remove.address)))
                        .filter(address_config::port_settings_id.eq(port_settings_id))
                        .filter(address_config::interface_name.eq(settings_to_remove.interface.to_string()))
                        .execute_async(&conn)
                        .await?;

                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                let message = "switch_port_configuration_interface_address_remove failed";
                match err.take() {
                    Some(external_error) => {
                        error!(opctx.log, "{message}"; "error" => ?external_error);
                        external_error
                    },
                    None => {
                        error!(opctx.log, "{message}"; "error" => ?e);
                        Error::internal_error("error while removing address from interface")
                    },
                }
            })
    }

    pub async fn switch_port_configuration_route_list(
        &self,
        opctx: &OpContext,
        configuration: NameOrId,
    ) -> ListResultVec<SwitchPortRouteConfig> {
        use db::schema::switch_port_settings as port_settings;
        use db::schema::switch_port_settings_route_config as route_config;

        let dataset = port_settings::table.inner_join(
            route_config::table
                .on(route_config::port_settings_id.eq(port_settings::id)),
        );

        let query = match configuration {
            NameOrId::Id(id) => {
                // find port config using port settings id
                dataset.filter(port_settings::id.eq(id)).into_boxed()
            }
            NameOrId::Name(name) => {
                // find port config using port settings name
                dataset
                    .filter(port_settings::name.eq(name.to_string()))
                    .into_boxed()
            }
        };

        let configs: Vec<SwitchPortRouteConfig> = query
            .select(SwitchPortRouteConfig::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e: diesel::result::Error| {
                let msg =
                    "error while looking up interface route configuration";
                match e {
                    diesel::result::Error::NotFound => {
                        Error::non_resourcetype_not_found(
                            "could not find route configuration for switch port configuration: {configuration}, interface: {interface}"
                        )},
                    _ => Error::internal_error(msg),
                }
            })?;

        Ok(configs)
    }

    pub async fn switch_port_configuration_route_add(
        &self,
        opctx: &OpContext,
        configuration: NameOrId,
        route: params::RouteAddRemove,
    ) -> CreateResult<SwitchPortRouteConfig> {
        use db::schema::switch_port_settings_route_config as route_config;

        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        self.transaction_retry_wrapper(
            "switch_port_configuration_interface_route_add",
        )
            .transaction(&conn, |conn| {
                let parent_configuration = configuration.clone();
                let new_settings = route.clone();
                let err = err.clone();

                async move {
                    // resolve id of port_settings record
                    let port_settings_id = switch_port_configuration_id(&conn, parent_configuration.clone())
                        .await
                        .map_err(|e: diesel::result::Error| {
                            match e {
                                diesel::result::Error::NotFound => {
                                    err.bail(
                                        Error::non_resourcetype_not_found(
                                            format!("unable to lookup configuration with identifier {parent_configuration}")
                                        )
                                    )
                                },
                                _ => {
                                    err.bail(Error::internal_error(
                                        "error while looking up configuration for interface route"
                                    ))
                                },
                            }
                        })?;

                    let route_config = SwitchPortRouteConfig{
                        port_settings_id,
                        interface_name: new_settings.interface.to_string(),
                        dst: new_settings.dst.into(),
                        gw: new_settings.gw.into(),
                        vid: new_settings.vid.map(Into::into),
                        rib_priority: new_settings.rib_priority.map(Into::into),
                    };

                    let config = diesel::insert_into(route_config::table)
                        .values(route_config)
                        .returning(SwitchPortRouteConfig::as_returning())
                        .get_result_async(&conn)
                        .await
                        .map_err(|e: diesel::result::Error| {
                            let message = "error while adding route to interface";
                            match e {
                                diesel::result::Error::DatabaseError(kind, _) => {
                                    match kind {
                                        diesel::result::DatabaseErrorKind::UniqueViolation => {
                                            err.bail(Error::conflict("route configuration conflicts with an existing route"))
                                        },
                                        diesel::result::DatabaseErrorKind::NotNullViolation => {
                                            err.bail(Error::invalid_request("a required field is not populated"))
                                        },
                                        _ => err.bail(Error::internal_error(message)),
                                    }
                                },
                                _ => err.bail(Error::internal_error(message)),
                            }
                        })?;

                    Ok(config)
                }
            })
            .await
            .map_err(|e| {
                let message =
                    "switch_port_configuration_interface_route_add failed";
                match err.take() {
                    Some(external_error) => {
                        error!(opctx.log, "{message}"; "error" => ?external_error);
                        external_error
                    }
                    None => {
                        error!(opctx.log, "{message}"; "error" => ?e);
                        Error::internal_error(
                            "error while adding route to interface",
                        )
                    }
                }
            })
    }

    pub async fn switch_port_configuration_route_remove(
        &self,
        opctx: &OpContext,
        configuration: NameOrId,
        route: params::RouteAddRemove,
    ) -> DeleteResult {
        use db::schema::switch_port_settings_route_config as route_config;

        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        self.transaction_retry_wrapper(
            "switch_port_configuration_interface_route_remove",
        )
            .transaction(&conn, |conn| {
                let parent_configuration = configuration.clone();
                let settings_to_remove = route.clone();
                let err = err.clone();

                async move {
                    // resolve id of port_settings record
                    let port_settings_id = switch_port_configuration_id(&conn, parent_configuration.clone())
                        .await
                        .map_err(|e: diesel::result::Error| {
                            match e {
                                diesel::result::Error::NotFound => {
                                    err.bail(
                                        Error::non_resourcetype_not_found(
                                            format!("unable to lookup configuration with identifier {parent_configuration}")
                                        )
                                    )
                                },
                                _ => {
                                    err.bail(Error::internal_error(
                                        "error while looking up configuration for interface route"
                                    ))
                                },
                            }
                        })?;

                    // delete route config
                    // PRIMARY KEY (port_settings_id, interface_name, dst, gw)
                    diesel::delete(route_config::table)
                        .filter(route_config::dst.eq(IpNetwork::from(settings_to_remove.dst)))
                        .filter(route_config::gw.eq(IpNetwork::from(settings_to_remove.gw)))
                        .filter(route_config::port_settings_id.eq(port_settings_id))
                        .filter(route_config::interface_name.eq(settings_to_remove.interface.to_string()))
                        .execute_async(&conn)
                        .await?;

                    Ok(())

                }
            })
            .await
            .map_err(|e| {
                let message =
                    "switch_port_configuration_interface_route_remove failed";
                match err.take() {
                    Some(external_error) => {
                        error!(opctx.log, "{message}"; "error" => ?external_error);
                        external_error
                    }
                    None => {
                        error!(opctx.log, "{message}"; "error" => ?e);
                        Error::internal_error(
                            "error while removing route from interface",
                        )
                    }
                }
            })
    }

    pub async fn switch_port_configuration_bgp_peer_list(
        &self,
        opctx: &OpContext,
        configuration: NameOrId,
    ) -> ListResultVec<SwitchPortBgpPeerConfig> {
        use db::schema::switch_port_settings as port_settings;
        use db::schema::switch_port_settings_bgp_peer_config as bgp_peer_config;

        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        self.transaction_retry_wrapper(
            "switch_port_configuration_bgp_peer_list",
        )
            .transaction(&conn, |conn| {
                let parent_configuration = configuration.clone();
                let err = err.clone();

                async move {

                    let dataset = port_settings::table.inner_join(
                        bgp_peer_config::table
                            .on(bgp_peer_config::port_settings_id.eq(port_settings::id)),
                    );

                    let query = match parent_configuration {
                        NameOrId::Id(id) => {
                            // find port config using port settings id
                            dataset.filter(port_settings::id.eq(id)).into_boxed()
                        }
                        NameOrId::Name(name) => {
                            // find port config using port settings name
                            dataset
                                .filter(port_settings::name.eq(name.to_string()))
                                .into_boxed()
                        }
                    };

                    let peers: Vec<SwitchPortBgpPeerConfig> = query
                        .select(SwitchPortBgpPeerConfig::as_select())
                        .load_async(&conn)
                        .await
                        .map_err(|e: diesel::result::Error| {
                            let message = "error while looking up interface bgp peer configuration";
                            error!(opctx.log, "{message}"; "error" => ?e);
                            match e {
                                diesel::result::Error::NotFound => {
                                    err.bail(Error::non_resourcetype_not_found(
                                        "could not find bgp peer configuration for switch port configuration: {configuration}, interface: {interface}"
                                    ))},
                                _ => err.bail(Error::internal_error(message)),
                            }
                        })?;


                    Ok(peers)
                }
            })
            .await
            .map_err(|e| {
                let message =
                    "switch_port_configuration_bgp_peer_list failed";
                match err.take() {
                    Some(external_error) => {
                        error!(opctx.log, "{message}"; "error" => ?external_error);
                        external_error
                    }
                    None => {
                        error!(opctx.log, "{message}"; "error" => ?e);
                        Error::internal_error(
                            "error while listing peers for interface configuration",
                        )
                    }
                }
            })
    }

    pub async fn switch_port_configuration_bgp_peer_add(
        &self,
        opctx: &OpContext,
        configuration: NameOrId,
        bgp_peer: BgpPeer,
    ) -> CreateResult<SwitchPortBgpPeerConfig> {
        use db::schema::bgp_config;
        use db::schema::switch_port_settings_bgp_peer_config as bgp_peer_config;

        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        self.transaction_retry_wrapper(
            "switch_port_configuration_interface_bgp_peer_add",
        )
            .transaction(&conn, |conn| {
                let parent_configuration = configuration.clone();
                let new_settings = bgp_peer.clone();
                let err = err.clone();

                async move {
                    // resolve id of port_settings record
                    let port_settings_id = switch_port_configuration_id(&conn, parent_configuration.clone())
                        .await
                        .map_err(|e: diesel::result::Error| {
                            match e {
                                diesel::result::Error::NotFound => {
                                    err.bail(
                                        Error::non_resourcetype_not_found(
                                            format!("unable to lookup configuration with identifier {parent_configuration}")
                                        )
                                    )
                                },
                                _ => {
                                    err.bail(Error::internal_error(
                                        "error while looking up configuration for interface bgp peer"
                                    ))
                                },
                            }
                        })?;

                    // resolve id of referenced bgp configuration
                    let bgp_config_id = match new_settings.bgp_config {

                        NameOrId::Id(id) => {
                            // verify id is valid
                            bgp_config::table
                                .filter(bgp_config::time_deleted.is_null())
                                .filter(bgp_config::id.eq(id))
                                .select(bgp_config::id)
                                .limit(1)
                                .first_async::<Uuid>(&conn)
                                .await
                                .map_err(|e: diesel::result::Error| {
                                    match e {
                                        diesel::result::Error::NotFound => {
                                            err.bail(Error::not_found_by_id(ResourceType::BgpConfig, &id))
                                        },
                                        _ => {
                                            let message = "error while looking up bgp config for bgp peer";
                                            error!(opctx.log, "{message}"; "error" => ?e);
                                            err.bail(Error::internal_error(message))
                                        },
                                    }
                                })
                        },

                        NameOrId::Name(name) => {
                            bgp_config::table
                                .filter(bgp_config::time_deleted.is_null())
                                .filter(bgp_config::name.eq(name.to_string()))
                                .select(bgp_config::id)
                                .limit(1)
                                .first_async::<Uuid>(&conn)
                                .await
                                .map_err(|e: diesel::result::Error| {
                                    match e {
                                        diesel::result::Error::NotFound => {
                                            err.bail(Error::not_found_by_name(ResourceType::BgpConfig, &name))
                                        },
                                        _ => {
                                            let message = "error while looking up bgp config for bgp peer";
                                            error!(opctx.log, "{message}"; "error" => ?e);
                                            err.bail(Error::internal_error(message))
                                        },
                                    }
                                })
                        }
                    }?;

                    let bgp_peer_config = SwitchPortBgpPeerConfig {
                        port_settings_id,
                        bgp_config_id,
                        interface_name: new_settings.interface_name,
                        addr: new_settings.addr.into(),
                        hold_time: new_settings.hold_time.into(),
                        idle_hold_time: new_settings.idle_hold_time.into(),
                        delay_open: new_settings.delay_open.into(),
                        connect_retry: new_settings.connect_retry.into(),
                        keepalive: new_settings.keepalive.into(),
                        remote_asn: new_settings.remote_asn.map(Into::into),
                        min_ttl: new_settings.min_ttl.map(Into::into),
                        md5_auth_key: new_settings.md5_auth_key,
                        multi_exit_discriminator: new_settings.multi_exit_discriminator.map(Into::into),
                        local_pref: new_settings.local_pref.map(Into::into),
                        enforce_first_as: new_settings.enforce_first_as,
                        allow_import_list_active: new_settings.allow_import_list_active,
                        allow_export_list_active: new_settings.allow_import_list_active,
                        vlan_id: new_settings.vlan_id.map(Into::into),
                    };

                    let peer = diesel::insert_into(bgp_peer_config::table)
                        .values(bgp_peer_config)
                        .returning(SwitchPortBgpPeerConfig::as_returning())
                        .get_result_async(&conn)
                        .await
                        .map_err(|e: diesel::result::Error| {
                            let message = "error while adding bgp peer to interface";
                            match e {
                                diesel::result::Error::DatabaseError(kind, _) => {
                                    match kind {
                                        diesel::result::DatabaseErrorKind::UniqueViolation => {
                                            err.bail(Error::conflict("bgp peer configuration conflicts with an existing configuration"))
                                        },
                                        diesel::result::DatabaseErrorKind::NotNullViolation => {
                                            err.bail(Error::invalid_request("a required field is not populated"))
                                        },
                                        _ => err.bail(Error::internal_error(message)),
                                    }
                                },
                                _ => err.bail(Error::internal_error(message)),
                            }
                        })?;

                    Ok(peer)
                }
            })
            .await
            .map_err(|e| {
                let message =
                    "switch_port_configuration_interface_bgp_peer_add failed";
                match err.take() {
                    Some(external_error) => {
                        error!(opctx.log, "{message}"; "error" => ?external_error);
                        external_error
                    }
                    None => {
                        error!(opctx.log, "{message}"; "error" => ?e);
                        Error::internal_error(
                            "error while adding bgp peer to interface",
                        )
                    }
                }
            })
    }

    pub async fn switch_port_configuration_bgp_peer_remove(
        &self,
        opctx: &OpContext,
        configuration: NameOrId,
        bgp_peer: BgpPeerRemove,
    ) -> DeleteResult {
        use db::schema::switch_port_settings_bgp_peer_config as bgp_peer_config;

        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        self.transaction_retry_wrapper(
            "switch_port_configuration_interface_bgp_peer_remove",
        )
            .transaction(&conn, |conn| {
                let parent_configuration = configuration.clone();
                let settings_to_remove = bgp_peer.clone();
                let err = err.clone();

                async move {
                    // resolve id of port_settings record
                    let port_settings_id = switch_port_configuration_id(&conn, parent_configuration.clone())
                        .await
                        .map_err(|e: diesel::result::Error| {
                            match e {
                                diesel::result::Error::NotFound => {
                                    err.bail(
                                        Error::non_resourcetype_not_found(
                                            format!("unable to lookup configuration with identifier {parent_configuration}")
                                        )
                                    )
                                },
                                _ => {
                                    err.bail(Error::internal_error(
                                        "error while looking up configuration for interface bgp peer"
                                    ))
                                },
                            }
                        })?;

                    // delete allowed import / export
                    // PRIMARY KEY (port_settings_id, interface_name, addr, prefix)
                    use db::schema::switch_port_settings_bgp_peer_config_allow_import as allow_import;
                    diesel::delete(allow_import::table)
                        .filter(allow_import::port_settings_id.eq(port_settings_id))
                        .filter(allow_import::interface_name.eq(settings_to_remove.interface_name.clone()))
                        .filter(allow_import::addr.eq(IpNetwork::from(settings_to_remove.addr)))
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            let message = "error while deleting import list for bgp peer";
                            error!(opctx.log, "{message}"; "error" => ?e);
                            err.bail(Error::internal_error(message))
                        })?;

                    use db::schema::switch_port_settings_bgp_peer_config_allow_export as allow_export;
                    diesel::delete(allow_export::table)
                        .filter(allow_export::port_settings_id.eq(port_settings_id))
                        .filter(allow_export::interface_name.eq(settings_to_remove.interface_name.clone()))
                        .filter(allow_export::addr.eq(IpNetwork::from(settings_to_remove.addr)))
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            let message = "error while deleting export list for bgp peer";
                            error!(opctx.log, "{message}"; "error" => ?e);
                            err.bail(Error::internal_error(message))
                        })?;

                    // delete communities
                    // PRIMARY KEY (port_settings_id, interface_name, addr, community)
                    use db::schema::switch_port_settings_bgp_peer_config_communities as peer_communities;
                    diesel::delete(peer_communities::table)
                        .filter(peer_communities::port_settings_id.eq(port_settings_id))
                        .filter(peer_communities::interface_name.eq(settings_to_remove.interface_name.clone()))
                        .filter(peer_communities::addr.eq(IpNetwork::from(settings_to_remove.addr)))
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            let message = "error while deleting communities for bgp peer";
                            error!(opctx.log, "{message}"; "error" => ?e);
                            err.bail(Error::internal_error(message))
                        })?;

                    // delete bgp peer config
                    // PRIMARY KEY (port_settings_id, interface_name, addr)
                    diesel::delete(bgp_peer_config::table)
                        .filter(bgp_peer_config::addr.eq(IpNetwork::from(settings_to_remove.addr)))
                        .filter(bgp_peer_config::port_settings_id.eq(port_settings_id))
                        .filter(bgp_peer_config::interface_name.eq(settings_to_remove.interface_name))
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            let message = "error while deleting bgp peer";
                            error!(opctx.log, "{message}"; "error" => ?e);
                            err.bail(Error::internal_error(message))
                        })?;

                    Ok(())

                }
            })
            .await
            .map_err(|e| {
                let message =
                    "switch_port_configuration_interface_bgp_peer_remove failed";
                match err.take() {
                    Some(external_error) => {
                        error!(opctx.log, "{message}"; "error" => ?external_error);
                        external_error
                    }
                    None => {
                        error!(opctx.log, "{message}"; "error" => ?e);
                        Error::internal_error(
                            "error while removing bgp peer from interface",
                        )
                    }
                }
            })
    }

    pub async fn switch_port_configuration_bgp_peer_allow_import_list(
        &self,
        opctx: &OpContext,
        configuration: NameOrId,
        bgp_peer: IpAddr,
    ) -> ListResultVec<SwitchPortBgpPeerConfigAllowImport> {
        use db::schema::switch_port_settings as port_settings;
        use db::schema::switch_port_settings_bgp_peer_config_allow_import as allow_import;

        let dataset = port_settings::table.inner_join(
            allow_import::table
                .on(allow_import::port_settings_id.eq(port_settings::id)),
        );

        let query = match configuration {
            NameOrId::Id(id) => {
                // find port config using port settings id
                dataset.filter(port_settings::id.eq(id)).into_boxed()
            }
            NameOrId::Name(name) => {
                // find port config using port settings name
                dataset
                    .filter(port_settings::name.eq(name.to_string()))
                    .into_boxed()
            }
        };

        query
            .filter(allow_import::addr.eq(IpNetwork::from(bgp_peer)))
            .select(SwitchPortBgpPeerConfigAllowImport::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e: diesel::result::Error| {
                let msg = "error while looking up bgp peer allowed import list";
                error!(opctx.log, "{msg}"; "error" => ?e);
                Error::internal_error(msg)
            })
    }

    pub async fn switch_port_configuration_bgp_peer_allow_import_add(
        &self,
        opctx: &OpContext,
        configuration: NameOrId,
        bgp_peer: IpAddr,
        prefix: AllowedPrefixAddRemove,
    ) -> CreateResult<SwitchPortBgpPeerConfigAllowImport> {
        use db::schema::switch_port_settings_bgp_peer_config_allow_import as allow_import;

        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        self.transaction_retry_wrapper(
            "switch_port_configuration_bgp_peer_allow_import_add",
        ).transaction(&conn, |conn| {
                let parent_configuration = configuration.clone();
                let new_settings = prefix.clone();
                let err = err.clone();

                async move {

                    // resolve id of port_settings record
                    let port_settings_id = switch_port_configuration_id(&conn, parent_configuration.clone())
                        .await
                        .map_err(|e: diesel::result::Error| {
                            match e {
                                diesel::result::Error::NotFound => {
                                    err.bail(
                                        Error::non_resourcetype_not_found(
                                            format!("unable to lookup configuration with identifier {parent_configuration}")
                                        )
                                    )
                                },
                                _ => {
                                    err.bail(Error::internal_error(
                                        "error while looking up switch port configuration for bgp peer"
                                    ))
                                },
                            }
                        })?;

                    let allow_import_config = SwitchPortBgpPeerConfigAllowImport {
                        port_settings_id,
                        interface_name: new_settings.interface.to_string(),
                        addr: bgp_peer.into(),
                        prefix: new_settings.prefix.into(),
                    };

                    let found_config = allow_import::table
                        .filter(allow_import::port_settings_id.eq(allow_import_config.port_settings_id))
                        .filter(allow_import::interface_name.eq(allow_import_config.interface_name.clone()))
                        .filter(allow_import::addr.eq(allow_import_config.addr))
                        .filter(allow_import::prefix.eq(allow_import_config.prefix))
                        .select(SwitchPortBgpPeerConfigAllowImport::as_select())
                        .get_result_async(&conn)
                        .await
                        .optional()?;

                    if let Some(config) = found_config {
                        return Ok(config)
                    }

                    let config = diesel::insert_into(allow_import::table)
                        .values(allow_import_config)
                        .returning(SwitchPortBgpPeerConfigAllowImport::as_returning())
                        .get_result_async(&conn)
                        .await?;

                    Ok(config)
                }
            })
            .await
            .map_err(|e| {
                let message = "switch_port_configuration_bgp_peer_allow_import_add failed";
                match err.take() {
                    Some(external_error) => {
                        error!(opctx.log, "{message}"; "error" => ?external_error);
                        external_error
                    },
                    None => {
                        error!(opctx.log, "{message}"; "error" => ?e);
                        Error::internal_error("error while adding entry to allowed import list")
                    },
                }
            })
    }

    pub async fn switch_port_configuration_bgp_peer_allow_import_remove(
        &self,
        opctx: &OpContext,
        configuration: NameOrId,
        bgp_peer: IpAddr,
        prefix: AllowedPrefixAddRemove,
    ) -> DeleteResult {
        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        self.transaction_retry_wrapper(
            "switch_port_configuration_bgp_peer_allow_import_remove",
        )
            .transaction(&conn, |conn| {
                let parent_configuration = configuration.clone();
                let settings_to_remove = prefix.clone();
                let err = err.clone();

                async move {
                    // resolve id of port_settings record
                    let port_settings_id = switch_port_configuration_id(&conn, parent_configuration.clone())
                        .await
                        .map_err(|e: diesel::result::Error| {
                            match e {
                                diesel::result::Error::NotFound => {
                                    err.bail(
                                        Error::non_resourcetype_not_found(
                                            format!("unable to lookup configuration with identifier {parent_configuration}")
                                        )
                                    )
                                },
                                _ => {
                                    err.bail(Error::internal_error(
                                        "error while looking up configuration for interface bgp peer"
                                    ))
                                },
                            }
                        })?;

                    // delete allowed import
                    // PRIMARY KEY (port_settings_id, interface_name, addr, prefix)
                    use db::schema::switch_port_settings_bgp_peer_config_allow_import as allow_import;
                    diesel::delete(allow_import::table)
                        .filter(allow_import::port_settings_id.eq(port_settings_id))
                        .filter(allow_import::interface_name.eq(settings_to_remove.interface.to_string()))
                        .filter(allow_import::addr.eq(IpNetwork::from(bgp_peer)))
                        .filter(allow_import::prefix.eq(IpNetwork::from(settings_to_remove.prefix)))
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            let message = "error while deleting import list entry for bgp peer";
                            error!(opctx.log, "{message}"; "error" => ?e);
                            err.bail(Error::internal_error(message))
                        })?;
                    Ok(())

                }
            })
            .await
            .map_err(|e| {
                let message =
                    "switch_port_configuration_bgp_peer_allow_import_remove failed";
                match err.take() {
                    Some(external_error) => {
                        error!(opctx.log, "{message}"; "error" => ?external_error);
                        external_error
                    }
                    None => {
                        error!(opctx.log, "{message}"; "error" => ?e);
                        Error::internal_error(
                            "error while removing entry from allow import list",
                        )
                    }
                }
            })
    }

    pub async fn switch_port_configuration_bgp_peer_allow_export_list(
        &self,
        opctx: &OpContext,
        configuration: NameOrId,
        bgp_peer: IpAddr,
    ) -> ListResultVec<SwitchPortBgpPeerConfigAllowExport> {
        use db::schema::switch_port_settings as port_settings;
        use db::schema::switch_port_settings_bgp_peer_config_allow_export as allow_export;

        let dataset = port_settings::table.inner_join(
            allow_export::table
                .on(allow_export::port_settings_id.eq(port_settings::id)),
        );

        let query = match configuration {
            NameOrId::Id(id) => {
                // find port config using port settings id
                dataset.filter(port_settings::id.eq(id)).into_boxed()
            }
            NameOrId::Name(name) => {
                // find port config using port settings name
                dataset
                    .filter(port_settings::name.eq(name.to_string()))
                    .into_boxed()
            }
        };

        query
            .filter(allow_export::addr.eq(IpNetwork::from(bgp_peer)))
            .select(SwitchPortBgpPeerConfigAllowExport::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e: diesel::result::Error| {
                let msg = "error while looking up bgp peer allowed export list";
                error!(opctx.log, "{msg}"; "error" => ?e);
                Error::internal_error(msg)
            })
    }

    pub async fn switch_port_configuration_bgp_peer_allow_export_add(
        &self,
        opctx: &OpContext,
        configuration: NameOrId,
        bgp_peer: IpAddr,
        prefix: AllowedPrefixAddRemove,
    ) -> CreateResult<SwitchPortBgpPeerConfigAllowExport> {
        use db::schema::switch_port_settings_bgp_peer_config_allow_export as allow_export;

        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        self.transaction_retry_wrapper(
            "switch_port_configuration_bgp_peer_allow_export_add",
        )
            .transaction(&conn, |conn| {
                let parent_configuration = configuration.clone();
                let new_settings = prefix.clone();
                let err = err.clone();

                async move {

                    // resolve id of port_settings record
                    let port_settings_id = switch_port_configuration_id(&conn, parent_configuration.clone())
                        .await
                        .map_err(|e: diesel::result::Error| {
                            match e {
                                diesel::result::Error::NotFound => {
                                    err.bail(
                                        Error::non_resourcetype_not_found(
                                            format!("unable to lookup configuration with identifier {parent_configuration}")
                                        )
                                    )
                                },
                                _ => {
                                    err.bail(Error::internal_error(
                                        "error while looking up switch port configuration for bgp peer"
                                    ))
                                },
                            }
                        })?;

                    let allow_export_config = SwitchPortBgpPeerConfigAllowExport {
                        port_settings_id,
                        interface_name: new_settings.interface.to_string(),
                        addr: bgp_peer.into(),
                        prefix: new_settings.prefix.into(),
                    };

                    let found_config = allow_export::table
                        .filter(allow_export::port_settings_id.eq(allow_export_config.port_settings_id))
                        .filter(allow_export::interface_name.eq(allow_export_config.interface_name.clone()))
                        .filter(allow_export::addr.eq(allow_export_config.addr))
                        .filter(allow_export::prefix.eq(allow_export_config.prefix))
                        .select(SwitchPortBgpPeerConfigAllowExport::as_select())
                        .get_result_async(&conn)
                        .await
                        .optional()?;

                    if let Some(config) = found_config {
                        return Ok(config)
                    }

                    let config = diesel::insert_into(allow_export::table)
                        .values(allow_export_config)
                        .returning(SwitchPortBgpPeerConfigAllowExport::as_returning())
                        .get_result_async(&conn)
                        .await?;

                    Ok(config)
                }
            })
            .await
            .map_err(|e| {
                let message = "switch_port_configuration_bgp_peer_allow_export_add failed";
                match err.take() {
                    Some(external_error) => {
                        error!(opctx.log, "{message}"; "error" => ?external_error);
                        external_error
                    },
                    None => {
                        error!(opctx.log, "{message}"; "error" => ?e);
                        Error::internal_error("error while adding entry to allowed export list")
                    },
                }
            })
    }

    pub async fn switch_port_configuration_bgp_peer_allow_export_remove(
        &self,
        opctx: &OpContext,
        configuration: NameOrId,
        bgp_peer: IpAddr,
        prefix: AllowedPrefixAddRemove,
    ) -> DeleteResult {
        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        self.transaction_retry_wrapper(
            "switch_port_configuration_bgp_peer_allow_export_remove",
        )
            .transaction(&conn, |conn| {
                let parent_configuration = configuration.clone();
                let settings_to_remove = prefix.clone();
                let err = err.clone();

                async move {
                    // resolve id of port_settings record
                    let port_settings_id = switch_port_configuration_id(&conn, parent_configuration.clone())
                        .await
                        .map_err(|e: diesel::result::Error| {
                            match e {
                                diesel::result::Error::NotFound => {
                                    err.bail(
                                        Error::non_resourcetype_not_found(
                                            format!("unable to lookup configuration with identifier {parent_configuration}")
                                        )
                                    )
                                },
                                _ => {
                                    err.bail(Error::internal_error(
                                        "error while looking up configuration for interface bgp peer"
                                    ))
                                },
                            }
                        })?;

                    // delete allowed export
                    // PRIMARY KEY (port_settings_id, interface_name, addr, prefix)
                    use db::schema::switch_port_settings_bgp_peer_config_allow_export as allow_export;
                    diesel::delete(allow_export::table)
                        .filter(allow_export::port_settings_id.eq(port_settings_id))
                        .filter(allow_export::interface_name.eq(settings_to_remove.interface.to_string()))
                        .filter(allow_export::addr.eq(IpNetwork::from(bgp_peer)))
                        .filter(allow_export::prefix.eq(IpNetwork::from(settings_to_remove.prefix)))
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            let message = "error while deleting export list entry for bgp peer";
                            error!(opctx.log, "{message}"; "error" => ?e);
                            err.bail(Error::internal_error(message))
                        })?;
                    Ok(())

                }
            })
            .await
            .map_err(|e| {
                let message =
                    "switch_port_configuration_bgp_peer_allow_export_remove failed";
                match err.take() {
                    Some(external_error) => {
                        error!(opctx.log, "{message}"; "error" => ?external_error);
                        external_error
                    }
                    None => {
                        error!(opctx.log, "{message}"; "error" => ?e);
                        Error::internal_error(
                            "error while removing entry from allow export list",
                        )
                    }
                }
            })
    }

    pub async fn switch_port_configuration_bgp_peer_community_list(
        &self,
        opctx: &OpContext,
        configuration: NameOrId,
        bgp_peer: IpAddr,
    ) -> ListResultVec<SwitchPortBgpPeerConfigCommunity> {
        use db::schema::switch_port_settings as port_settings;
        use db::schema::switch_port_settings_bgp_peer_config_communities as communities;

        let dataset = port_settings::table.inner_join(
            communities::table
                .on(communities::port_settings_id.eq(port_settings::id)),
        );

        let query = match configuration {
            NameOrId::Id(id) => {
                // find port config using port settings id
                dataset.filter(port_settings::id.eq(id)).into_boxed()
            }
            NameOrId::Name(name) => {
                // find port config using port settings name
                dataset
                    .filter(port_settings::name.eq(name.to_string()))
                    .into_boxed()
            }
        };

        query
            .filter(communities::addr.eq(IpNetwork::from(bgp_peer)))
            .select(SwitchPortBgpPeerConfigCommunity::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e: diesel::result::Error| {
                let msg = "error while looking up bgp peer community list";
                error!(opctx.log, "{msg}"; "error" => ?e);
                Error::internal_error(msg)
            })
    }

    pub async fn switch_port_configuration_bgp_peer_community_add(
        &self,
        opctx: &OpContext,
        configuration: NameOrId,
        bgp_peer: IpAddr,
        community: BgpCommunityAddRemove,
    ) -> CreateResult<SwitchPortBgpPeerConfigCommunity> {
        use db::schema::switch_port_settings_bgp_peer_config_communities as communities;

        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        self.transaction_retry_wrapper(
            "switch_port_configuration_bgp_peer_community_add",
        )
            .transaction(&conn, |conn| {
                let parent_configuration = configuration.clone();
                let new_settings = community.clone();
                let err = err.clone();

                async move {

                    // resolve id of port_settings record
                    let port_settings_id = switch_port_configuration_id(&conn, parent_configuration.clone())
                        .await
                        .map_err(|e: diesel::result::Error| {
                            match e {
                                diesel::result::Error::NotFound => {
                                    err.bail(
                                        Error::non_resourcetype_not_found(
                                            format!("unable to lookup configuration with identifier {parent_configuration}")
                                        )
                                    )
                                },
                                _ => {
                                    err.bail(Error::internal_error(
                                        "error while looking up switch port configuration for bgp peer"
                                    ))
                                },
                            }
                        })?;

                    let community_config = SwitchPortBgpPeerConfigCommunity {
                        port_settings_id,
                        interface_name: new_settings.interface.to_string(),
                        addr: bgp_peer.into(),
                        community: new_settings.community.into(),
                    };

                    let found_config = communities::table
                        .filter(communities::port_settings_id.eq(community_config.port_settings_id))
                        .filter(communities::interface_name.eq(community_config.interface_name.clone()))
                        .filter(communities::addr.eq(community_config.addr))
                        .filter(communities::community.eq(community_config.community))
                        .select(SwitchPortBgpPeerConfigCommunity::as_select())
                        .get_result_async(&conn)
                        .await
                        .optional()?;

                    if let Some(config) = found_config {
                        return Ok(config)
                    }

                    let config = diesel::insert_into(communities::table)
                        .values(community_config)
                        .returning(SwitchPortBgpPeerConfigCommunity::as_returning())
                        .get_result_async(&conn)
                        .await?;

                    Ok(config)
                }
            })
            .await
            .map_err(|e| {
                let message = "switch_port_configuration_bgp_peer_community_add failed";
                match err.take() {
                    Some(external_error) => {
                        error!(opctx.log, "{message}"; "error" => ?external_error);
                        external_error
                    },
                    None => {
                        error!(opctx.log, "{message}"; "error" => ?e);
                        Error::internal_error("error while adding community to bgp peer")
                    },
                }
            })
    }

    pub async fn switch_port_configuration_bgp_peer_community_remove(
        &self,
        opctx: &OpContext,
        configuration: NameOrId,
        bgp_peer: IpAddr,
        community: BgpCommunityAddRemove,
    ) -> DeleteResult {
        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        self.transaction_retry_wrapper(
            "switch_port_configuration_bgp_peer_community_remove",
        )
            .transaction(&conn, |conn| {
                let parent_configuration = configuration.clone();
                let settings_to_remove = community.clone();
                let err = err.clone();

                async move {
                    // resolve id of port_settings record
                    let port_settings_id = switch_port_configuration_id(&conn, parent_configuration.clone())
                        .await
                        .map_err(|e: diesel::result::Error| {
                            match e {
                                diesel::result::Error::NotFound => {
                                    err.bail(
                                        Error::non_resourcetype_not_found(
                                            format!("unable to lookup configuration with identifier {parent_configuration}")
                                        )
                                    )
                                },
                                _ => {
                                    err.bail(Error::internal_error(
                                        "error while looking up configuration for interface bgp peer"
                                    ))
                                },
                            }
                        })?;

                    // delete communities
                    // PRIMARY KEY (port_settings_id, interface_name, addr, community)
                    use db::schema::switch_port_settings_bgp_peer_config_communities as peer_communities;
                    diesel::delete(peer_communities::table)
                        .filter(peer_communities::port_settings_id.eq(port_settings_id))
                        .filter(peer_communities::interface_name.eq(settings_to_remove.interface.to_string()))
                        .filter(peer_communities::addr.eq(IpNetwork::from(bgp_peer)))
                        .filter(peer_communities::community.eq(SqlU32::from(settings_to_remove.community)))
                        .execute_async(&conn)
                        .await
                        .map_err(|e| {
                            let message = "error while deleting community entry for bgp peer";
                            error!(opctx.log, "{message}"; "error" => ?e);
                            err.bail(Error::internal_error(message))
                        })?;

                    Ok(())

                }
            })
            .await
            .map_err(|e| {
                let message =
                    "switch_port_configuration_bgp_peer_community_remove failed";
                match err.take() {
                    Some(external_error) => {
                        error!(opctx.log, "{message}"; "error" => ?external_error);
                        external_error
                    }
                    None => {
                        error!(opctx.log, "{message}"; "error" => ?e);
                        Error::internal_error(
                            "error while removing entry from community list",
                        )
                    }
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
        use db::schema::bgp_config::dsl as bgp_config_dsl;
        use db::schema::switch_port;
        use db::schema::switch_port::dsl as switch_port_dsl;
        use db::schema::switch_port_settings_bgp_peer_config::dsl as bgp_peer_dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        let err = OptionalError::new();
        self.transaction_retry_wrapper("switch_port_set_settings_id")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    // TODO: remove once per-switch-multi-asn support is added
                    // Bail if user attempts to assign multiple ASNs to a switch via switch port settings
                    // This is a temporary measure until multi-asn-per-switch is supported.

                    // what switch are we adding a configuration to?
                    let switch = switch_port_dsl::switch_port
                        .filter(switch_port_dsl::id.eq(switch_port_id))
                        .select(switch_port_dsl::switch_location)
                        .limit(1)
                        .first_async::<String>(&conn)
                        .await
                        .map_err(|e: diesel::result::Error| {
                            let msg = "failed to look up switch port by id";
                            error!(opctx.log, "{msg}"; "error" => ?e);
                            match e {
                                diesel::result::Error::NotFound => {
                                    err.bail(Error::not_found_by_id(
                                        ResourceType::SwitchPort,
                                        &switch_port_id,
                                    ))
                                }
                                _ => err.bail(Error::internal_error(msg)),
                            }
                        })?;

                    // if we're setting a port settings id (and therefore activating a configuration
                    // on a port) we need to make sure there aren't any conflicting bgp configurations
                    if let Some(psid) = port_settings_id {
                        let bgp_config: Option<BgpConfig> =
                            match bgp_peer_dsl::switch_port_settings_bgp_peer_config
                            .inner_join(
                                bgp_config_dsl::bgp_config
                                    .on(bgp_peer_dsl::bgp_config_id
                                        .eq(bgp_config_dsl::id)),
                            )
                            .filter(
                                bgp_peer_dsl::port_settings_id
                                    .eq(psid),
                            )
                            .select(BgpConfig::as_select())
                            .limit(1)
                            .first_async::<BgpConfig>(&conn)
                            .await {
                                Ok(v) => Ok(Some(v)),
                                Err(e) => {
                                    let msg = "failed to check if bgp peer exists in switch port settings";
                                    error!(opctx.log, "{msg}"; "error" => ?e);
                                    match e {
                                        diesel::result::Error::NotFound => {
                                            Ok(None)
                                        }
                                        _ => Err(err.bail(Error::internal_error(msg))),
                                    }
                                }
                            }?;

                        // find all port settings for the targeted switch
                        // switch port
                        // inner join bgp peer on port settings id
                        // inner join bgp config on bgp config id
                        // filter switch location eq switch
                        // filter port settings id not null
                        // filter asn doesn't equal our asn

                        if let Some(config) = bgp_config {
                            let conflicting_bgp_configs: Vec<BgpConfig> = switch_port_dsl::switch_port
                                .inner_join(
                                    bgp_peer_dsl::switch_port_settings_bgp_peer_config
                                        .on(bgp_peer_dsl::port_settings_id
                                            .nullable()
                                            .eq(switch_port_dsl::port_settings_id)),
                                )
                                .inner_join(bgp_config_dsl::bgp_config.on(
                                    bgp_peer_dsl::bgp_config_id.eq(bgp_config_dsl::id),
                                ))
                                .filter(switch_port_dsl::switch_location.eq(switch))
                                .filter(switch_port_dsl::port_settings_id.is_not_null())
                                .filter(bgp_config_dsl::asn.ne(config.asn))
                                .select(BgpConfig::as_select())
                                .load_async(&conn)
                                .await?;

                            if !conflicting_bgp_configs.is_empty() {
                                return Err(err.bail(Error::conflict("a different asn is already configured on this switch")));
                            }
                        }

                    }

                    // perform the requested update
                    match current {
                        UpdatePrecondition::DontCare => {
                            diesel::update(switch_port_dsl::switch_port)
                                .filter(switch_port::id.eq(switch_port_id))
                                .set(
                                    switch_port::port_settings_id
                                        .eq(port_settings_id),
                                )
                                .execute_async(&conn)
                                .await
                        }
                        UpdatePrecondition::Null => {
                            diesel::update(switch_port_dsl::switch_port)
                                .filter(switch_port::id.eq(switch_port_id))
                                .filter(switch_port::port_settings_id.is_null())
                                .set(
                                    switch_port::port_settings_id
                                        .eq(port_settings_id),
                                )
                                .execute_async(&conn)
                                .await
                        }
                        UpdatePrecondition::Value(current_id) => {
                            diesel::update(switch_port_dsl::switch_port)
                                .filter(switch_port::id.eq(switch_port_id))
                                .filter(
                                    switch_port::port_settings_id
                                        .eq(current_id),
                                )
                                .set(
                                    switch_port::port_settings_id
                                        .eq(port_settings_id),
                                )
                                .execute_async(&conn)
                                .await
                        }
                    }
                }
            })
            .await
            .map_err(|e| {
                let msg = "switch_port_set_settings_id failed";
                if let Some(err) = err.take() {
                    error!(opctx.log, "{msg}"; "error" => ?err);
                    err
                } else {
                    error!(opctx.log, "{msg}"; "error" => ?e);
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })?;

        Ok(())
    }

    pub async fn switch_port_get_active_configuration(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        switch_location: SwitchLocation,
        port_name: Name,
    ) -> LookupResult<Option<SwitchPortSettings>> {
        use db::schema::switch_port;
        use db::schema::switch_port_settings;

        let conn = self.pool_connection_authorized(opctx).await?;

        let active_configuration = switch_port::table
            .inner_join(
                switch_port_settings::table.on(switch_port_settings::id
                    .nullable()
                    .eq(switch_port::port_settings_id)),
            )
            .filter(switch_port::rack_id.eq(rack_id))
            .filter(
                switch_port::switch_location.eq(switch_location.to_string()),
            )
            .filter(switch_port::port_name.eq(port_name.to_string()))
            .select(SwitchPortSettings::as_select())
            .get_result_async::<SwitchPortSettings>(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(active_configuration)
    }

    pub async fn switch_port_get_id(
        &self,
        opctx: &OpContext,
        rack_id: Uuid,
        switch_location: SwitchLocation,
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
            .union_all(
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

#[derive(Debug)]
enum SwitchPortSettingsCreateError {
    AddressLotNotFound,
    BgpConfigNotFound,
    ReserveBlock(ReserveBlockError),
}
type SpsCreateError = SwitchPortSettingsCreateError;

async fn do_switch_port_settings_create(
    conn: &Connection<DTraceConnection<PgConnection>>,
    id: Option<Uuid>,
    params: &params::SwitchPortSettingsCreate,
    err: OptionalError<SwitchPortSettingsCreateError>,
) -> Result<SwitchPortSettingsCombinedResult, diesel::result::Error> {
    use db::schema::{
        address_lot::dsl as address_lot_dsl, bgp_config::dsl as bgp_config_dsl,
        lldp_link_config::dsl as lldp_link_config_dsl,
        switch_port_settings::dsl as port_settings_dsl,
        switch_port_settings_address_config::dsl as address_config_dsl,
        switch_port_settings_bgp_peer_config::dsl as bgp_peer_dsl,
        switch_port_settings_bgp_peer_config_allow_export::dsl as allow_export_dsl,
        switch_port_settings_bgp_peer_config_allow_import::dsl as allow_import_dsl,
        switch_port_settings_bgp_peer_config_communities::dsl as bgp_communities_dsl,
        switch_port_settings_interface_config::dsl as interface_config_dsl,
        switch_port_settings_link_config::dsl as link_config_dsl,
        switch_port_settings_port_config::dsl as port_config_dsl,
        switch_port_settings_route_config::dsl as route_config_dsl,
        switch_vlan_interface_config::dsl as vlan_config_dsl,
    };

    // create the top level port settings object
    let port_settings = match id {
        Some(id) => SwitchPortSettings::with_id(id, &params.identity),
        None => SwitchPortSettings::new(&params.identity),
    };
    //let port_settings = SwitchPortSettings::new(&params.identity);
    let db_port_settings: SwitchPortSettings =
        diesel::insert_into(port_settings_dsl::switch_port_settings)
            .values(port_settings.clone())
            .returning(SwitchPortSettings::as_returning())
            .get_result_async(conn)
            .await?;

    let psid = db_port_settings.identity.id;

    // add the port config
    let port_config =
        SwitchPortConfig::new(psid, params.port_config.geometry.into());

    let db_port_config: SwitchPortConfig =
        diesel::insert_into(port_config_dsl::switch_port_settings_port_config)
            .values(port_config)
            .returning(SwitchPortConfig::as_returning())
            .get_result_async(conn)
            .await?;

    let mut result = SwitchPortSettingsCombinedResult {
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
        let lldp_link_config = LldpLinkConfig::new(
            c.lldp.enabled,
            c.lldp.link_name.clone(),
            c.lldp.link_description.clone(),
            c.lldp.chassis_id.clone(),
            c.lldp.system_name.clone(),
            c.lldp.system_description.clone(),
            c.lldp.management_ip.map(|a| a.into()),
        );
        let lldp_config_id = lldp_link_config.id;
        lldp_config.push(lldp_link_config);

        link_config.push(SwitchPortLinkConfig::new(
            psid,
            lldp_config_id,
            link_name.clone(),
            c.mtu,
            c.fec.into(),
            c.speed.into(),
            c.autoneg,
        ));
    }
    result.link_lldp =
        diesel::insert_into(lldp_link_config_dsl::lldp_link_config)
            .values(lldp_config.clone())
            .returning(LldpLinkConfig::as_returning())
            .get_results_async(conn)
            .await?;

    result.links =
        diesel::insert_into(link_config_dsl::switch_port_settings_link_config)
            .values(link_config)
            .returning(SwitchPortLinkConfig::as_returning())
            .get_results_async(conn)
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
    result.interfaces = diesel::insert_into(
        interface_config_dsl::switch_port_settings_interface_config,
    )
    .values(interface_config)
    .returning(SwitchInterfaceConfig::as_returning())
    .get_results_async(conn)
    .await?;
    result.vlan_interfaces =
        diesel::insert_into(vlan_config_dsl::switch_vlan_interface_config)
            .values(vlan_interface_config)
            .returning(SwitchVlanInterfaceConfig::as_returning())
            .get_results_async(conn)
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
                route.rib_priority.map(Into::into),
            ));
        }
    }
    result.routes = diesel::insert_into(
        route_config_dsl::switch_port_settings_route_config,
    )
    .values(route_config)
    .returning(SwitchPortRouteConfig::as_returning())
    .get_results_async(conn)
    .await?;

    let mut peer_by_addr: BTreeMap<IpAddr, &external::BgpPeerCombined> =
        BTreeMap::new();

    let mut bgp_peer_config = Vec::new();
    for (interface_name, peer_config) in &params.bgp_peers {
        for p in &peer_config.peers {
            peer_by_addr.insert(p.addr, &p);
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
                        .first_async::<Uuid>(conn)
                        .await
                        .map_err(|diesel_error| {
                            err.bail_retryable_or(
                                diesel_error,
                                SwitchPortSettingsCreateError::BgpConfigNotFound
                            )
                        })?
                }
            };

            if let ImportExportPolicy::Allow(list) = &p.allowed_import {
                let id = port_settings.identity.id;
                let to_insert: Vec<SwitchPortBgpPeerConfigAllowImport> = list
                    .clone()
                    .into_iter()
                    .map(|x| SwitchPortBgpPeerConfigAllowImport {
                        port_settings_id: id,
                        interface_name: interface_name.clone(),
                        addr: p.addr.into(),
                        prefix: x.into(),
                    })
                    .collect();

                diesel::insert_into(allow_import_dsl::switch_port_settings_bgp_peer_config_allow_import)
                    .values(to_insert)
                    .execute_async(conn)
                    .await?;
            }

            if let ImportExportPolicy::Allow(list) = &p.allowed_export {
                let id = port_settings.identity.id;
                let to_insert: Vec<SwitchPortBgpPeerConfigAllowExport> = list
                    .clone()
                    .into_iter()
                    .map(|x| SwitchPortBgpPeerConfigAllowExport {
                        port_settings_id: id,
                        interface_name: interface_name.clone(),
                        addr: p.addr.into(),
                        prefix: x.into(),
                    })
                    .collect();

                diesel::insert_into(allow_export_dsl::switch_port_settings_bgp_peer_config_allow_export)
                    .values(to_insert)
                    .execute_async(conn)
                    .await?;
            }

            if !p.communities.is_empty() {
                let id = port_settings.identity.id;
                let to_insert: Vec<SwitchPortBgpPeerConfigCommunity> = p
                    .communities
                    .clone()
                    .into_iter()
                    .map(|x| SwitchPortBgpPeerConfigCommunity {
                        port_settings_id: id,
                        interface_name: interface_name.clone(),
                        addr: p.addr.into(),
                        community: x.into(),
                    })
                    .collect();

                diesel::insert_into(bgp_communities_dsl::switch_port_settings_bgp_peer_config_communities)
                    .values(to_insert)
                    .execute_async(conn)
                    .await?;
            }

            bgp_peer_config.push(SwitchPortBgpPeerConfig::new(
                psid,
                bgp_config_id,
                interface_name.clone(),
                p,
            ));
        }
    }

    let db_bgp_peers: Vec<SwitchPortBgpPeerConfig> =
        diesel::insert_into(bgp_peer_dsl::switch_port_settings_bgp_peer_config)
            .values(bgp_peer_config)
            .returning(SwitchPortBgpPeerConfig::as_returning())
            .get_results_async(conn)
            .await?;

    for p in db_bgp_peers.into_iter() {
        let view = BgpPeerConfig {
            port_settings_id: p.port_settings_id,
            bgp_config_id: p.bgp_config_id,
            interface_name: p.interface_name,
            addr: p.addr,
            hold_time: p.hold_time,
            idle_hold_time: p.idle_hold_time,
            delay_open: p.delay_open,
            connect_retry: p.connect_retry,
            keepalive: p.keepalive,
            remote_asn: p.remote_asn,
            min_ttl: p.min_ttl,
            md5_auth_key: p.md5_auth_key,
            multi_exit_discriminator: p.multi_exit_discriminator,
            local_pref: p.local_pref,
            enforce_first_as: p.enforce_first_as,
            vlan_id: p.vlan_id,
            allowed_import: peer_by_addr
                .get(&p.addr.ip())
                .map(|x| x.allowed_import.clone())
                .unwrap_or(ImportExportPolicy::NoFiltering)
                .clone(),
            allowed_export: peer_by_addr
                .get(&p.addr.ip())
                .map(|x| x.allowed_export.clone())
                .unwrap_or(ImportExportPolicy::NoFiltering)
                .clone(),
            communities: peer_by_addr
                .get(&p.addr.ip())
                .map(|x| x.communities.clone())
                .unwrap_or(Vec::new())
                .clone(),
        };
        result.bgp_peers.push(view);
    }

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
                        .first_async::<Uuid>(conn)
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
                    address.address.addr().into(),
                    // TODO: Should we allow anycast addresses for switch_ports?
                    // anycast
                    false,
                    &conn,
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
                address.vlan_id,
            ));
        }
    }
    result.addresses = diesel::insert_into(
        address_config_dsl::switch_port_settings_address_config,
    )
    .values(address_config)
    .returning(SwitchPortAddressConfig::as_returning())
    .get_results_async(conn)
    .await?;

    Ok(result)
}

#[derive(Debug)]
enum SwitchPortSettingsDeleteError {
    SwitchPortSettingsNotFound,
}

async fn do_switch_port_settings_delete(
    conn: &Connection<DTraceConnection<PgConnection>>,
    selector: &NameOrId,
    err: OptionalError<SwitchPortSettingsDeleteError>,
) -> Result<(), diesel::result::Error> {
    use db::schema::switch_port_settings;
    use db::schema::switch_port_settings::dsl as port_settings_dsl;
    let id = match selector {
        NameOrId::Id(id) => *id,
        NameOrId::Name(name) => {
            let name = name.to_string();
            port_settings_dsl::switch_port_settings
                .filter(switch_port_settings::time_deleted.is_null())
                .filter(switch_port_settings::name.eq(name))
                .select(switch_port_settings::id)
                .limit(1)
                .first_async::<Uuid>(conn)
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
        .execute_async(conn)
        .await?;

    // delete the port config object
    use db::schema::switch_port_settings_port_config::{
        self as sps_port_config, dsl as port_config_dsl,
    };
    diesel::delete(port_config_dsl::switch_port_settings_port_config)
        .filter(sps_port_config::port_settings_id.eq(id))
        .execute_async(conn)
        .await?;

    // delete the link configs
    use db::schema::switch_port_settings_link_config::{
        self as sps_link_config, dsl as link_config_dsl,
    };
    let links: Vec<SwitchPortLinkConfig> =
        diesel::delete(link_config_dsl::switch_port_settings_link_config)
            .filter(sps_link_config::port_settings_id.eq(id))
            .returning(SwitchPortLinkConfig::as_returning())
            .get_results_async(conn)
            .await?;

    // delete lldp configs
    use db::schema::lldp_link_config;
    let lldp_link_ids: Vec<Uuid> =
        links.iter().filter_map(|link| link.lldp_link_config_id).collect();

    diesel::delete(lldp_link_config::dsl::lldp_link_config)
        .filter(lldp_link_config::id.eq_any(lldp_link_ids))
        .execute_async(conn)
        .await?;

    // delete interface configs
    use db::schema::switch_port_settings_interface_config::{
        self as sps_interface_config, dsl as interface_config_dsl,
    };

    let interfaces: Vec<SwitchInterfaceConfig> = diesel::delete(
        interface_config_dsl::switch_port_settings_interface_config,
    )
    .filter(sps_interface_config::port_settings_id.eq(id))
    .returning(SwitchInterfaceConfig::as_returning())
    .get_results_async(conn)
    .await?;

    // delete any vlan interfaces
    use db::schema::switch_vlan_interface_config::{
        self, dsl as vlan_config_dsl,
    };
    let interface_ids: Vec<Uuid> =
        interfaces.iter().map(|interface| interface.id).collect();

    diesel::delete(vlan_config_dsl::switch_vlan_interface_config)
        .filter(
            switch_vlan_interface_config::interface_config_id
                .eq_any(interface_ids),
        )
        .execute_async(conn)
        .await?;

    // delete route configs
    use db::schema::switch_port_settings_route_config;
    use db::schema::switch_port_settings_route_config::dsl as route_config_dsl;

    diesel::delete(route_config_dsl::switch_port_settings_route_config)
        .filter(switch_port_settings_route_config::port_settings_id.eq(id))
        .execute_async(conn)
        .await?;

    // delete bgp configurations
    use db::schema::switch_port_settings_bgp_peer_config as bgp_peer;
    use db::schema::switch_port_settings_bgp_peer_config::dsl as bgp_peer_dsl;

    diesel::delete(bgp_peer_dsl::switch_port_settings_bgp_peer_config)
        .filter(bgp_peer::port_settings_id.eq(id))
        .execute_async(conn)
        .await?;

    // delete allowed exports
    use db::schema::switch_port_settings_bgp_peer_config_allow_export as allow_export;
    use db::schema::switch_port_settings_bgp_peer_config_allow_export::dsl as allow_export_dsl;
    diesel::delete(
        allow_export_dsl::switch_port_settings_bgp_peer_config_allow_export,
    )
    .filter(allow_export::port_settings_id.eq(id))
    .execute_async(conn)
    .await?;

    // delete allowed imports
    use db::schema::switch_port_settings_bgp_peer_config_allow_import as allow_import;
    use db::schema::switch_port_settings_bgp_peer_config_allow_import::dsl as allow_import_dsl;
    diesel::delete(
        allow_import_dsl::switch_port_settings_bgp_peer_config_allow_import,
    )
    .filter(allow_import::port_settings_id.eq(id))
    .execute_async(conn)
    .await?;

    // delete communities
    use db::schema::switch_port_settings_bgp_peer_config_communities as bgp_communities;
    use db::schema::switch_port_settings_bgp_peer_config_communities::dsl as bgp_communities_dsl;
    diesel::delete(
        bgp_communities_dsl::switch_port_settings_bgp_peer_config_communities,
    )
    .filter(bgp_communities::port_settings_id.eq(id))
    .execute_async(conn)
    .await?;

    // delete address configs
    use db::schema::switch_port_settings_address_config::{
        self as address_config, dsl as address_config_dsl,
    };

    let port_settings_addrs =
        diesel::delete(address_config_dsl::switch_port_settings_address_config)
            .filter(address_config::port_settings_id.eq(id))
            .returning(SwitchPortAddressConfig::as_returning())
            .get_results_async(conn)
            .await?;

    use db::schema::address_lot_rsvd_block::dsl as rsvd_block_dsl;

    for ps in &port_settings_addrs {
        diesel::delete(rsvd_block_dsl::address_lot_rsvd_block)
            .filter(rsvd_block_dsl::id.eq(ps.rsvd_address_lot_block_id))
            .execute_async(conn)
            .await?;
    }

    Ok(())
}

// TODO: refactor to emit more detailed errors
async fn switch_port_configuration_id(
    conn: &async_bb8_diesel::Connection<DTraceConnection<diesel::PgConnection>>,
    name_or_id: NameOrId,
) -> diesel::result::QueryResult<Uuid> {
    use db::schema::switch_port_settings as port_settings;
    use db::schema::switch_port_settings::dsl as port_settings_dsl;

    let dataset = port_settings_dsl::switch_port_settings;

    let query = match name_or_id {
        NameOrId::Id(id) => {
            // find port config using port settings id
            dataset.filter(port_settings::id.eq(id)).into_boxed()
        }
        NameOrId::Name(name) => {
            // find port config using port settings name
            dataset
                .filter(port_settings::name.eq(name.to_string()))
                .into_boxed()
        }
    };

    // get settings id
    query.select(port_settings_dsl::id).limit(1).first_async(conn).await
}

async fn lldp_configuration_id(
    conn: &async_bb8_diesel::Connection<DTraceConnection<diesel::PgConnection>>,
    name_or_id: NameOrId,
) -> diesel::result::QueryResult<Uuid> {
    use db::schema::lldp_link_config;
    use db::schema::lldp_link_config::dsl as lldp_link_config_dsl;

    let dataset = lldp_link_config_dsl::lldp_link_config;

    let query = match name_or_id {
        NameOrId::Id(id) => {
            // find port config using port settings id
            dataset.filter(lldp_link_config::id.eq(id)).into_boxed()
        }
        NameOrId::Name(name) => {
            // find port config using port settings name
            dataset
                .filter(lldp_link_config::link_name.eq(name.to_string()))
                .into_boxed()
        }
    };

    // get settings id
    query.select(lldp_link_config_dsl::id).limit(1).first_async(conn).await
}

#[cfg(test)]
mod test {
    use crate::db::datastore::pub_test_utils::TestDatabase;
    use crate::db::datastore::UpdatePrecondition;
    use nexus_types::external_api::params::{
        BgpAnnounceSetCreate, BgpConfigCreate, BgpPeerConfig,
        SwitchPortConfigCreate, SwitchPortGeometry, SwitchPortSettingsCreate,
    };
    use omicron_common::api::external::{
        BgpPeerCombined, IdentityMetadataCreateParams, ImportExportPolicy,
        Name, NameOrId,
    };
    use omicron_test_utils::dev;
    use std::collections::HashMap;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_bgp_boundary_switches() {
        let logctx = dev::test_setup_log("test_bgp_boundary_switches");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

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
            checker: None,
            shaper: None,
        };

        datastore.bgp_config_create(&opctx, &bgp_config).await.unwrap();

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
                    peers: vec![BgpPeerCombined {
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
                        remote_asn: None,
                        min_ttl: None,
                        md5_auth_key: None,
                        multi_exit_discriminator: None,
                        communities: Vec::new(),
                        local_pref: None,
                        enforce_first_as: false,
                        allowed_export: ImportExportPolicy::NoFiltering,
                        allowed_import: ImportExportPolicy::NoFiltering,
                        vlan_id: None,
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

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
