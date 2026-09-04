// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on router configurations.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::model::ApplySledFilterExt;
use crate::db::model::Name;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use crate::db::pagination::paginated_multicolumn;
use crate::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::AsyncSimpleConnection;
use chrono::Utc;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use diesel::{JoinOnDsl, NullableExpressionMethods};
use nexus_db_errors::OptionalError;
use nexus_db_errors::{ErrorHandler, public_error_from_diesel};
use nexus_db_lookup::DbConnection;
use nexus_db_model::{
    ControlPlaneRouterConfiguration, DbTypedUuid, NetworkInterfaceKind,
    RouterConfiguration, RouterConfigurationBfdPeer,
    RouterConfigurationBgpPeer, RouterConfigurationStaticRoute,
    RouterConfigurationUpdate, SiloRouterConfiguration, Sled, SqlU8, SqlU32,
    to_db_typed_uuid,
};
use nexus_types::deployment::SledFilter;
use nexus_types::identity::Resource;
use omicron_common::api::external;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{
    CreateResult, DeleteResult, Error, ListResultVec, LookupResult,
    ResourceType, UpdateResult,
};
use omicron_uuid_kinds::{
    BgpAnnounceSetKind, BgpAnnounceSetUuid, RouterConfigurationKind,
    RouterConfigurationUuid,
};
use ref_cast::RefCast;
use sled_agent_types::early_networking::MaxPathConfig;

impl DataStore {
    /// Load the built-in per-switch default router configurations
    /// (idempotent).
    pub async fn load_builtin_router_configuration(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::router_configuration::dsl;

        opctx.authorize(authz::Action::Modify, &authz::DATABASE).await?;

        let conn = self.pool_connection_authorized(opctx).await?;
        for builtin in [
            &nexus_db_fixed_data::router_configuration::DEFAULT_SWITCH0_ROUTER_CONFIGURATION,
            &nexus_db_fixed_data::router_configuration::DEFAULT_SWITCH1_ROUTER_CONFIGURATION,
        ] {
            diesel::insert_into(dsl::router_configuration)
                .values((**builtin).clone())
                .on_conflict(dsl::id)
                .do_nothing()
                .execute_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
        }

        info!(opctx.log, "created built-in default router configurations");
        Ok(())
    }

    pub async fn router_configuration_create(
        &self,
        opctx: &OpContext,
        config: RouterConfiguration,
    ) -> CreateResult<RouterConfiguration> {
        use nexus_db_schema::schema::router_configuration::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        let name = config.name().to_string();
        diesel::insert_into(dsl::router_configuration)
            .values(config)
            .returning(RouterConfiguration::as_returning())
            .get_result_async(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::RouterConfiguration,
                        &name,
                    ),
                )
            })
    }

    pub async fn router_configuration_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<RouterConfiguration> {
        use nexus_db_schema::schema::router_configuration::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::router_configuration, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::router_configuration,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::time_deleted.is_null())
        .select(RouterConfiguration::as_select())
        .load_async(&*conn)
        .await
        .map_err(|e| {
            error!(opctx.log, "router_configuration_list failed"; "error" => ?e);
            public_error_from_diesel(e, ErrorHandler::Server)
        })
    }

    pub async fn router_configuration_update(
        &self,
        opctx: &OpContext,
        authz_configuration: &authz::RouterConfiguration,
        update: RouterConfigurationUpdate,
    ) -> UpdateResult<RouterConfiguration> {
        use nexus_db_schema::schema::router_configuration::dsl;

        diesel::update(dsl::router_configuration)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(to_db_typed_uuid(authz_configuration.id())))
            .set(update)
            .returning(RouterConfiguration::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                error!(opctx.log, "router_configuration_update failed"; "error" => ?e);
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_configuration),
                )
            })
    }

    pub async fn router_configuration_delete(
        &self,
        opctx: &OpContext,
        authz_configuration: &authz::RouterConfiguration,
    ) -> DeleteResult {
        use nexus_db_schema::schema::control_plane_router_configuration::dsl as cp_link_dsl;
        use nexus_db_schema::schema::router_configuration::dsl;
        use nexus_db_schema::schema::router_configuration_bfd_peer::dsl as bfd_peer_dsl;
        use nexus_db_schema::schema::router_configuration_bgp_peer::dsl as bgp_peer_dsl;
        use nexus_db_schema::schema::router_configuration_static_route::dsl as static_route_dsl;
        use nexus_db_schema::schema::silo_router_configuration::dsl as silo_link_dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        let id = to_db_typed_uuid(authz_configuration.id());
        let err = OptionalError::new();
        self.transaction_retry_wrapper("router_configuration_delete")
            .transaction(&conn, async |conn| {
                // Check if the router configuration is in use by any silos
                let in_use = diesel::dsl::select(diesel::dsl::exists(
                    silo_link_dsl::silo_router_configuration
                        .filter(silo_link_dsl::router_configuration_id.eq(id)),
                ))
                .get_result_async::<bool>(&conn)
                .await?;

                if in_use {
                    return Err(err.bail(Error::conflict(
                        "router configuration is in use by one or more \
                            silos and cannot be deleted",
                    )));
                }

                let cp_in_use = diesel::dsl::select(diesel::dsl::exists(
                    cp_link_dsl::control_plane_router_configuration.filter(
                        cp_link_dsl::router_configuration_id.eq(Some(id)),
                    ),
                ))
                .get_result_async::<bool>(&conn)
                .await?;

                if cp_in_use {
                    return Err(err.bail(Error::conflict(
                        "router configuration is in use by the control \
                            plane and cannot be deleted",
                    )));
                }

                diesel::update(dsl::router_configuration)
                    .filter(dsl::time_deleted.is_null())
                    .filter(dsl::id.eq(id))
                    .set(dsl::time_deleted.eq(Utc::now()))
                    .execute_async(&conn)
                    .await?;
                diesel::delete(bgp_peer_dsl::router_configuration_bgp_peer)
                    .filter(bgp_peer_dsl::router_configuration_id.eq(id))
                    .execute_async(&conn)
                    .await?;
                diesel::delete(
                    static_route_dsl::router_configuration_static_route,
                )
                .filter(static_route_dsl::router_configuration_id.eq(id))
                .execute_async(&conn)
                .await?;
                diesel::delete(bfd_peer_dsl::router_configuration_bfd_peer)
                    .filter(bfd_peer_dsl::router_configuration_id.eq(id))
                    .execute_async(&conn)
                    .await?;
                Ok(())
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return err;
                }
                error!(opctx.log, "router_configuration_delete failed"; "error" => ?e);
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_configuration),
                )
            })?;
        Ok(())
    }

    /// Create or replace the BGP configuration on a router configuration.
    pub async fn router_configuration_bgp_config_set(
        &self,
        opctx: &OpContext,
        authz_configuration: &authz::RouterConfiguration,
        asn: u32,
        max_paths: MaxPathConfig,
        bgp_announce_set_id: BgpAnnounceSetUuid,
    ) -> UpdateResult<RouterConfiguration> {
        use nexus_db_schema::schema::router_configuration::dsl;

        diesel::update(dsl::router_configuration)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(to_db_typed_uuid(authz_configuration.id())))
            .set((
                dsl::bgp_asn.eq(Some(SqlU32::from(asn))),
                dsl::bgp_max_paths.eq(Some(SqlU8::from(max_paths.as_u8()))),
                dsl::bgp_announce_set_id
                    .eq(Some(to_db_typed_uuid(bgp_announce_set_id))),
                dsl::time_modified.eq(Utc::now()),
            ))
            .returning(RouterConfiguration::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                error!(opctx.log, "router_configuration_bgp_config_set failed"; "error" => ?e);
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_configuration),
                )
            })
    }

    /// Remove the BGP configuration on a router configuration.
    ///
    /// This is idempotent: removing the BGP configuration from a router
    /// configuration that doesn't have one is not an error.
    pub async fn router_configuration_bgp_config_delete(
        &self,
        opctx: &OpContext,
        authz_configuration: &authz::RouterConfiguration,
    ) -> DeleteResult {
        use nexus_db_schema::schema::router_configuration::dsl;

        diesel::update(dsl::router_configuration)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(to_db_typed_uuid(authz_configuration.id())))
            .set((
                dsl::bgp_asn.eq(Option::<SqlU32>::None),
                dsl::bgp_max_paths.eq(Option::<SqlU8>::None),
                dsl::bgp_announce_set_id
                    .eq(Option::<DbTypedUuid<BgpAnnounceSetKind>>::None),
                dsl::time_modified.eq(Utc::now()),
            ))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                error!(opctx.log, "router_configuration_bgp_config_delete failed"; "error" => ?e);
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_configuration),
                )
            })?;
        Ok(())
    }

    /// Fetch the BGP peers of several router configurations at once.
    pub async fn router_configuration_bgp_peer_list_batch(
        &self,
        opctx: &OpContext,
        ids: &[RouterConfigurationUuid],
    ) -> ListResultVec<RouterConfigurationBgpPeer> {
        use nexus_db_schema::schema::router_configuration_bgp_peer::dsl;

        let ids: Vec<DbTypedUuid<RouterConfigurationKind>> =
            ids.iter().map(|id| to_db_typed_uuid(*id)).collect();
        dsl::router_configuration_bgp_peer
            .filter(dsl::router_configuration_id.eq_any(ids))
            .order_by((dsl::router_configuration_id.asc(), dsl::name.asc()))
            .select(RouterConfigurationBgpPeer::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Fetch the static routes of several router configurations at once.
    pub async fn router_configuration_static_route_list_batch(
        &self,
        opctx: &OpContext,
        ids: &[RouterConfigurationUuid],
    ) -> ListResultVec<RouterConfigurationStaticRoute> {
        use nexus_db_schema::schema::router_configuration_static_route::dsl;

        let ids: Vec<DbTypedUuid<RouterConfigurationKind>> =
            ids.iter().map(|id| to_db_typed_uuid(*id)).collect();
        dsl::router_configuration_static_route
            .filter(dsl::router_configuration_id.eq_any(ids))
            .order_by((dsl::router_configuration_id.asc(), dsl::name.asc()))
            .select(RouterConfigurationStaticRoute::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Fetch the BFD peers of several router configurations at once.
    pub async fn router_configuration_bfd_peer_list_batch(
        &self,
        opctx: &OpContext,
        ids: &[RouterConfigurationUuid],
    ) -> ListResultVec<RouterConfigurationBfdPeer> {
        use nexus_db_schema::schema::router_configuration_bfd_peer::dsl;

        let ids: Vec<DbTypedUuid<RouterConfigurationKind>> =
            ids.iter().map(|id| to_db_typed_uuid(*id)).collect();
        dsl::router_configuration_bfd_peer
            .filter(dsl::router_configuration_id.eq_any(ids))
            .order_by((dsl::router_configuration_id.asc(), dsl::name.asc()))
            .select(RouterConfigurationBfdPeer::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn router_configuration_bgp_peer_list(
        &self,
        opctx: &OpContext,
        authz_configuration: &authz::RouterConfiguration,
    ) -> ListResultVec<RouterConfigurationBgpPeer> {
        use nexus_db_schema::schema::router_configuration_bgp_peer::dsl;

        dsl::router_configuration_bgp_peer
            .filter(
                dsl::router_configuration_id
                    .eq(to_db_typed_uuid(authz_configuration.id())),
            )
            .order_by(dsl::name.asc())
            .select(RouterConfigurationBgpPeer::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                error!(opctx.log, "router_configuration_bgp_peer_list failed"; "error" => ?e);
                public_error_from_diesel(e, ErrorHandler::Server)
            })
    }

    pub async fn router_configuration_bgp_peer_create(
        &self,
        opctx: &OpContext,
        peer: RouterConfigurationBgpPeer,
    ) -> CreateResult<RouterConfigurationBgpPeer> {
        use nexus_db_schema::schema::router_configuration_bgp_peer::dsl;

        let name = peer.name.to_string();
        diesel::insert_into(dsl::router_configuration_bgp_peer)
            .values(peer)
            .returning(RouterConfigurationBgpPeer::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::RouterConfigurationBgpPeer,
                        &name,
                    ),
                )
            })
    }

    pub async fn router_configuration_bgp_peer_view(
        &self,
        opctx: &OpContext,
        authz_configuration: &authz::RouterConfiguration,
        name: &external::Name,
    ) -> LookupResult<RouterConfigurationBgpPeer> {
        use nexus_db_schema::schema::router_configuration_bgp_peer::dsl;

        dsl::router_configuration_bgp_peer
            .filter(
                dsl::router_configuration_id
                    .eq(to_db_typed_uuid(authz_configuration.id())),
            )
            .filter(dsl::name.eq(name.to_string()))
            .select(RouterConfigurationBgpPeer::as_select())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| match e {
                diesel::result::Error::NotFound => Error::not_found_by_name(
                    ResourceType::RouterConfigurationBgpPeer,
                    name,
                ),
                _ => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    pub async fn router_configuration_bgp_peer_update(
        &self,
        opctx: &OpContext,
        authz_configuration: &authz::RouterConfiguration,
        name: &external::Name,
        peer: RouterConfigurationBgpPeer,
    ) -> UpdateResult<RouterConfigurationBgpPeer> {
        use nexus_db_schema::schema::router_configuration_bgp_peer::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();
        let new_name = peer.name.to_string();
        self.transaction_retry_wrapper("router_configuration_bgp_peer_update")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let peer = peer.clone();
                async move {
                    let deleted =
                        diesel::delete(dsl::router_configuration_bgp_peer)
                            .filter(
                                dsl::router_configuration_id.eq(
                                    to_db_typed_uuid(authz_configuration.id()),
                                ),
                            )
                            .filter(dsl::name.eq(name.to_string()))
                            .execute_async(&conn)
                            .await?;
                    if deleted == 0 {
                        return Err(err.bail(Error::not_found_by_name(
                            ResourceType::RouterConfigurationBgpPeer,
                            name,
                        )));
                    }
                    diesel::insert_into(dsl::router_configuration_bgp_peer)
                        .values(peer)
                        .returning(RouterConfigurationBgpPeer::as_returning())
                        .get_result_async(&conn)
                        .await
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    err
                } else {
                    public_error_from_diesel(
                        e,
                        ErrorHandler::Conflict(
                            ResourceType::RouterConfigurationBgpPeer,
                            &new_name,
                        ),
                    )
                }
            })
    }

    pub async fn router_configuration_bgp_peer_delete(
        &self,
        opctx: &OpContext,
        authz_configuration: &authz::RouterConfiguration,
        name: &external::Name,
    ) -> DeleteResult {
        use nexus_db_schema::schema::router_configuration_bgp_peer::dsl;

        let deleted = diesel::delete(dsl::router_configuration_bgp_peer)
            .filter(
                dsl::router_configuration_id
                    .eq(to_db_typed_uuid(authz_configuration.id())),
            )
            .filter(dsl::name.eq(name.to_string()))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        if deleted == 0 {
            return Err(Error::not_found_by_name(
                ResourceType::RouterConfigurationBgpPeer,
                name,
            ));
        }
        Ok(())
    }

    pub async fn router_configuration_static_route_list(
        &self,
        opctx: &OpContext,
        authz_configuration: &authz::RouterConfiguration,
    ) -> ListResultVec<RouterConfigurationStaticRoute> {
        use nexus_db_schema::schema::router_configuration_static_route::dsl;

        dsl::router_configuration_static_route
            .filter(
                dsl::router_configuration_id
                    .eq(to_db_typed_uuid(authz_configuration.id())),
            )
            .order_by(dsl::name.asc())
            .select(RouterConfigurationStaticRoute::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                error!(opctx.log, "router_configuration_static_route_list failed"; "error" => ?e);
                public_error_from_diesel(e, ErrorHandler::Server)
            })
    }

    pub async fn router_configuration_static_route_create(
        &self,
        opctx: &OpContext,
        route: RouterConfigurationStaticRoute,
    ) -> CreateResult<RouterConfigurationStaticRoute> {
        use nexus_db_schema::schema::router_configuration_static_route::dsl;

        let name = route.name.to_string();
        diesel::insert_into(dsl::router_configuration_static_route)
            .values(route)
            .returning(RouterConfigurationStaticRoute::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::RouterConfigurationStaticRoute,
                        &name,
                    ),
                )
            })
    }

    pub async fn router_configuration_static_route_view(
        &self,
        opctx: &OpContext,
        authz_configuration: &authz::RouterConfiguration,
        name: &external::Name,
    ) -> LookupResult<RouterConfigurationStaticRoute> {
        use nexus_db_schema::schema::router_configuration_static_route::dsl;

        dsl::router_configuration_static_route
            .filter(
                dsl::router_configuration_id
                    .eq(to_db_typed_uuid(authz_configuration.id())),
            )
            .filter(dsl::name.eq(name.to_string()))
            .select(RouterConfigurationStaticRoute::as_select())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| match e {
                diesel::result::Error::NotFound => Error::not_found_by_name(
                    ResourceType::RouterConfigurationStaticRoute,
                    name,
                ),
                _ => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    pub async fn router_configuration_static_route_update(
        &self,
        opctx: &OpContext,
        authz_configuration: &authz::RouterConfiguration,
        name: &external::Name,
        route: RouterConfigurationStaticRoute,
    ) -> UpdateResult<RouterConfigurationStaticRoute> {
        use nexus_db_schema::schema::router_configuration_static_route::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();
        let new_name = route.name.to_string();
        self.transaction_retry_wrapper(
            "router_configuration_static_route_update",
        )
        .transaction(&conn, |conn| {
            let err = err.clone();
            let route = route.clone();
            async move {
                let deleted =
                    diesel::delete(dsl::router_configuration_static_route)
                        .filter(
                            dsl::router_configuration_id
                                .eq(to_db_typed_uuid(authz_configuration.id())),
                        )
                        .filter(dsl::name.eq(name.to_string()))
                        .execute_async(&conn)
                        .await?;
                if deleted == 0 {
                    return Err(err.bail(Error::not_found_by_name(
                        ResourceType::RouterConfigurationStaticRoute,
                        name,
                    )));
                }
                diesel::insert_into(dsl::router_configuration_static_route)
                    .values(route)
                    .returning(RouterConfigurationStaticRoute::as_returning())
                    .get_result_async(&conn)
                    .await
            }
        })
        .await
        .map_err(|e| {
            if let Some(err) = err.take() {
                err
            } else {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::RouterConfigurationStaticRoute,
                        &new_name,
                    ),
                )
            }
        })
    }

    pub async fn router_configuration_static_route_delete(
        &self,
        opctx: &OpContext,
        authz_configuration: &authz::RouterConfiguration,
        name: &external::Name,
    ) -> DeleteResult {
        use nexus_db_schema::schema::router_configuration_static_route::dsl;

        let deleted = diesel::delete(dsl::router_configuration_static_route)
            .filter(
                dsl::router_configuration_id
                    .eq(to_db_typed_uuid(authz_configuration.id())),
            )
            .filter(dsl::name.eq(name.to_string()))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        if deleted == 0 {
            return Err(Error::not_found_by_name(
                ResourceType::RouterConfigurationStaticRoute,
                name,
            ));
        }
        Ok(())
    }

    pub async fn router_configuration_bfd_peer_list(
        &self,
        opctx: &OpContext,
        authz_configuration: &authz::RouterConfiguration,
    ) -> ListResultVec<RouterConfigurationBfdPeer> {
        use nexus_db_schema::schema::router_configuration_bfd_peer::dsl;

        dsl::router_configuration_bfd_peer
            .filter(
                dsl::router_configuration_id
                    .eq(to_db_typed_uuid(authz_configuration.id())),
            )
            .order_by(dsl::name.asc())
            .select(RouterConfigurationBfdPeer::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                error!(opctx.log, "router_configuration_bfd_peer_list failed"; "error" => ?e);
                public_error_from_diesel(e, ErrorHandler::Server)
            })
    }

    pub async fn router_configuration_bfd_peer_create(
        &self,
        opctx: &OpContext,
        peer: RouterConfigurationBfdPeer,
    ) -> CreateResult<RouterConfigurationBfdPeer> {
        use nexus_db_schema::schema::router_configuration_bfd_peer::dsl;

        let name = peer.name.to_string();
        diesel::insert_into(dsl::router_configuration_bfd_peer)
            .values(peer)
            .returning(RouterConfigurationBfdPeer::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::RouterConfigurationBfdPeer,
                        &name,
                    ),
                )
            })
    }

    pub async fn router_configuration_bfd_peer_view(
        &self,
        opctx: &OpContext,
        authz_configuration: &authz::RouterConfiguration,
        name: &external::Name,
    ) -> LookupResult<RouterConfigurationBfdPeer> {
        use nexus_db_schema::schema::router_configuration_bfd_peer::dsl;

        dsl::router_configuration_bfd_peer
            .filter(
                dsl::router_configuration_id
                    .eq(to_db_typed_uuid(authz_configuration.id())),
            )
            .filter(dsl::name.eq(name.to_string()))
            .select(RouterConfigurationBfdPeer::as_select())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| match e {
                diesel::result::Error::NotFound => Error::not_found_by_name(
                    ResourceType::RouterConfigurationBfdPeer,
                    name,
                ),
                _ => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    pub async fn router_configuration_bfd_peer_update(
        &self,
        opctx: &OpContext,
        authz_configuration: &authz::RouterConfiguration,
        name: &external::Name,
        peer: RouterConfigurationBfdPeer,
    ) -> UpdateResult<RouterConfigurationBfdPeer> {
        use nexus_db_schema::schema::router_configuration_bfd_peer::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();
        let new_name = peer.name.to_string();
        self.transaction_retry_wrapper("router_configuration_bfd_peer_update")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let peer = peer.clone();
                async move {
                    let deleted =
                        diesel::delete(dsl::router_configuration_bfd_peer)
                            .filter(
                                dsl::router_configuration_id.eq(
                                    to_db_typed_uuid(authz_configuration.id()),
                                ),
                            )
                            .filter(dsl::name.eq(name.to_string()))
                            .execute_async(&conn)
                            .await?;
                    if deleted == 0 {
                        return Err(err.bail(Error::not_found_by_name(
                            ResourceType::RouterConfigurationBfdPeer,
                            name,
                        )));
                    }
                    diesel::insert_into(dsl::router_configuration_bfd_peer)
                        .values(peer)
                        .returning(RouterConfigurationBfdPeer::as_returning())
                        .get_result_async(&conn)
                        .await
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    err
                } else {
                    public_error_from_diesel(
                        e,
                        ErrorHandler::Conflict(
                            ResourceType::RouterConfigurationBfdPeer,
                            &new_name,
                        ),
                    )
                }
            })
    }

    pub async fn router_configuration_bfd_peer_delete(
        &self,
        opctx: &OpContext,
        authz_configuration: &authz::RouterConfiguration,
        name: &external::Name,
    ) -> DeleteResult {
        use nexus_db_schema::schema::router_configuration_bfd_peer::dsl;

        let deleted = diesel::delete(dsl::router_configuration_bfd_peer)
            .filter(
                dsl::router_configuration_id
                    .eq(to_db_typed_uuid(authz_configuration.id())),
            )
            .filter(dsl::name.eq(name.to_string()))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        if deleted == 0 {
            return Err(Error::not_found_by_name(
                ResourceType::RouterConfigurationBfdPeer,
                name,
            ));
        }
        Ok(())
    }

    /// List the router configurations used by a silo, in ascending priority
    /// order.
    pub async fn silo_router_configurations_list(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
    ) -> ListResultVec<SiloRouterConfiguration> {
        use nexus_db_schema::schema::silo_router_configuration::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        dsl::silo_router_configuration
            .filter(dsl::silo_id.eq(authz_silo.id()))
            .order_by(dsl::priority.asc())
            .select(SiloRouterConfiguration::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| {
                error!(opctx.log, "silo_router_configurations_list failed"; "error" => ?e);
                public_error_from_diesel(e, ErrorHandler::Server)
            })
    }

    /// Unlink all router configurations used by a silo, as part of silo
    /// deletion.
    pub async fn silo_router_configurations_delete(
        &self,
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        authz_silo: &authz::Silo,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_silo).await?;

        use nexus_db_schema::schema::silo_router_configuration::dsl;
        diesel::delete(dsl::silo_router_configuration)
            .filter(dsl::silo_id.eq(authz_silo.id()))
            .execute_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    /// Replace the full set of router configurations used by a silo.
    ///
    /// All current routing configurations associated with the silo are replaced
    /// in a single transaction. The transaction checks that every referenced
    /// router configuration still exists.
    pub async fn silo_router_configurations_replace(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        links: Vec<SiloRouterConfiguration>,
    ) -> ListResultVec<SiloRouterConfiguration> {
        use nexus_db_schema::schema::router_configuration::dsl as rc_dsl;
        use nexus_db_schema::schema::silo_router_configuration::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        let silo_id = authz_silo.id();
        let configuration_ids: Vec<_> =
            links.iter().map(|link| link.router_configuration_id).collect();
        let err = OptionalError::new();
        self.transaction_retry_wrapper("silo_router_configurations_replace")
            .transaction(&conn, async |conn| {
                // Check that all referenced router configurations are still live
                let live_count = rc_dsl::router_configuration
                    .filter(rc_dsl::time_deleted.is_null())
                    .filter(rc_dsl::id.eq_any(configuration_ids.clone()))
                    .count()
                    .get_result_async::<i64>(&conn)
                    .await?;
                if live_count != configuration_ids.len() as i64 {
                    return Err(err.bail(Error::conflict(
                        "a referenced router configuration was deleted \
                         concurrently",
                    )));
                }

                diesel::delete(dsl::silo_router_configuration)
                    .filter(dsl::silo_id.eq(silo_id))
                    .execute_async(&conn)
                    .await?;
                diesel::insert_into(dsl::silo_router_configuration)
                    .values(links.clone())
                    .returning(SiloRouterConfiguration::as_returning())
                    .get_results_async(&conn)
                    .await
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return err;
                }
                error!(opctx.log, "silo_router_configurations_replace failed"; "error" => ?e);
                public_error_from_diesel(e, ErrorHandler::Server)
            })
    }

    /// List every silo-router-configuration link across all silos, for the
    /// router-list reconciler.
    pub async fn silo_router_configurations_list_all(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<SiloRouterConfiguration> {
        use nexus_db_schema::schema::silo_router_configuration::dsl;

        opctx.check_complex_operations_allowed()?;

        let conn = self.pool_connection_authorized(opctx).await?;
        let mut links = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch = paginated_multicolumn(
                dsl::silo_router_configuration,
                (dsl::silo_id, dsl::priority),
                &p.current_pagparams(),
            )
            .select(SiloRouterConfiguration::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| {
                error!(opctx.log, "silo_router_configurations_list_all failed"; "error" => ?e);
                public_error_from_diesel(e, ErrorHandler::Server)
            })?;
            paginator =
                p.found_batch(&batch, &|l| (l.silo_id, i32::from(*l.priority)));
            links.extend(batch);
        }
        Ok(links)
    }

    /// List the control-plane router-configuration list, in ascending
    /// priority order.
    ///
    /// No rows means "never configured"; a single row with a NULL
    /// `router_configuration_id` means "explicitly configured empty".
    pub async fn control_plane_router_configurations_list(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<ControlPlaneRouterConfiguration> {
        use nexus_db_schema::schema::control_plane_router_configuration::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        let mut entries = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch = paginated(
                dsl::control_plane_router_configuration,
                dsl::priority,
                &p.current_pagparams(),
            )
            .select(ControlPlaneRouterConfiguration::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| {
                error!(opctx.log, "control_plane_router_configurations_list failed"; "error" => ?e);
                public_error_from_diesel(e, ErrorHandler::Server)
            })?;
            paginator = p.found_batch(&batch, &|l| i32::from(*l.priority));
            entries.extend(batch);
        }
        Ok(entries)
    }

    /// Replace the full control-plane router-configuration list.
    ///
    /// An empty `links` is recorded as the explicit-empty marker row so it
    /// stays distinguishable from "never configured".
    pub async fn control_plane_router_configurations_replace(
        &self,
        opctx: &OpContext,
        links: Vec<ControlPlaneRouterConfiguration>,
    ) -> ListResultVec<ControlPlaneRouterConfiguration> {
        use nexus_db_schema::schema::control_plane_router_configuration::dsl;
        use nexus_db_schema::schema::router_configuration::dsl as rc_dsl;

        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        let conn = self.pool_connection_authorized(opctx).await?;
        let links = if links.is_empty() {
            vec![ControlPlaneRouterConfiguration::empty_marker()]
        } else {
            links
        };
        let configuration_ids: Vec<_> = links
            .iter()
            .filter_map(|link| link.router_configuration_id)
            .collect();
        let err = OptionalError::new();
        self.transaction_retry_wrapper(
            "control_plane_router_configurations_replace",
        )
        .transaction(&conn, async |conn| {
            // The DELETE below has no WHERE clause; the table is bounded by
            // MAX_ROUTER_CONFIGURATIONS_PER_SILO-sized updates, so a full
            // scan is fine (SET LOCAL, scoped to this transaction).
            conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

            // Check that all referenced router configurations are still live
            let live_count = rc_dsl::router_configuration
                .filter(rc_dsl::time_deleted.is_null())
                .filter(rc_dsl::id.eq_any(configuration_ids.clone()))
                .count()
                .get_result_async::<i64>(&conn)
                .await?;
            if live_count != configuration_ids.len() as i64 {
                return Err(err.bail(Error::conflict(
                    "a referenced router configuration was deleted \
                     concurrently",
                )));
            }

            diesel::delete(dsl::control_plane_router_configuration)
                .execute_async(&conn)
                .await?;
            diesel::insert_into(dsl::control_plane_router_configuration)
                .values(links.clone())
                .returning(ControlPlaneRouterConfiguration::as_returning())
                .get_results_async(&conn)
                .await
        })
        .await
        .map_err(|e| {
            if let Some(err) = err.take() {
                return err;
            }
            error!(opctx.log, "control_plane_router_configurations_replace failed"; "error" => ?e);
            public_error_from_diesel(e, ErrorHandler::Server)
        })
    }

    /// List every guest NIC that backs an OPTE port on an in-service sled,
    /// with the silo its instance belongs to: `(nic_id, silo_id, sled)`.
    pub async fn port_router_list_targets(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<(uuid::Uuid, uuid::Uuid, Sled)> {
        use nexus_db_schema::schema::instance::dsl as instance_dsl;
        use nexus_db_schema::schema::network_interface;
        use nexus_db_schema::schema::network_interface::dsl as nic_dsl;
        use nexus_db_schema::schema::probe::dsl as probe_dsl;
        use nexus_db_schema::schema::project::dsl as project_dsl;
        use nexus_db_schema::schema::sled::dsl as sled_dsl;
        use nexus_db_schema::schema::vmm::dsl as vmm_dsl;

        opctx.check_complex_operations_allowed()?;

        let mut targets = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch: Vec<(uuid::Uuid, uuid::Uuid, Sled)> = paginated(
                nic_dsl::network_interface,
                nic_dsl::id,
                &p.current_pagparams(),
            )
            .inner_join(
                instance_dsl::instance
                    .on(nic_dsl::parent_id.eq(instance_dsl::id)),
            )
            .inner_join(
                project_dsl::project
                    .on(project_dsl::id.eq(instance_dsl::project_id)),
            )
            .inner_join(vmm_dsl::vmm.on(
                vmm_dsl::id.nullable().eq(instance_dsl::active_propolis_id),
            ))
            .inner_join(sled_dsl::sled.on(sled_dsl::id.eq(vmm_dsl::sled_id)))
            .filter(network_interface::time_deleted.is_null())
            .filter(network_interface::kind.eq(NetworkInterfaceKind::Instance))
            .sled_filter(SledFilter::VpcRouting)
            .select((nic_dsl::id, project_dsl::silo_id, Sled::as_select()))
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
            paginator = p.found_batch(&batch, &|(nic_id, _, _)| *nic_id);
            targets.extend(batch);
        }

        // Probe NICs: parented to a probe, which pins a sled directly.
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch: Vec<(uuid::Uuid, uuid::Uuid, Sled)> = paginated(
                nic_dsl::network_interface,
                nic_dsl::id,
                &p.current_pagparams(),
            )
            .inner_join(
                probe_dsl::probe.on(nic_dsl::parent_id.eq(probe_dsl::id)),
            )
            .inner_join(
                project_dsl::project
                    .on(project_dsl::id.eq(probe_dsl::project_id)),
            )
            .inner_join(sled_dsl::sled.on(sled_dsl::id.eq(probe_dsl::sled)))
            .filter(network_interface::time_deleted.is_null())
            .filter(network_interface::kind.eq(NetworkInterfaceKind::Probe))
            .filter(probe_dsl::time_deleted.is_null())
            .sled_filter(SledFilter::VpcRouting)
            .select((nic_dsl::id, project_dsl::silo_id, Sled::as_select()))
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
            paginator = p.found_batch(&batch, &|(nic_id, _, _)| *nic_id);
            targets.extend(batch);
        }
        Ok(targets)
    }

    /// List every live service NIC id
    pub async fn service_nic_ids(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<uuid::Uuid> {
        use nexus_db_schema::schema::network_interface;
        use nexus_db_schema::schema::network_interface::dsl as nic_dsl;

        opctx.check_complex_operations_allowed()?;

        let mut ids = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch: Vec<uuid::Uuid> = paginated(
                nic_dsl::network_interface,
                nic_dsl::id,
                &p.current_pagparams(),
            )
            .filter(network_interface::time_deleted.is_null())
            .filter(network_interface::kind.eq(NetworkInterfaceKind::Service))
            .select(nic_dsl::id)
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
            paginator = p.found_batch(&batch, &|id| *id);
            ids.extend(batch);
        }
        Ok(ids)
    }
}
