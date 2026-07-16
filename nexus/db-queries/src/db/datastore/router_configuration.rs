// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on router configurations.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::model::Name;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use nexus_db_errors::OptionalError;
use nexus_db_errors::{ErrorHandler, public_error_from_diesel};
use nexus_db_model::{
    DbTypedUuid, RouterConfiguration, RouterConfigurationBfdPeer,
    RouterConfigurationBgpPeer, RouterConfigurationStaticRoute,
    RouterConfigurationUpdate, SqlU8, SqlU32, to_db_typed_uuid,
};
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
        use nexus_db_schema::schema::router_configuration::dsl;
        use nexus_db_schema::schema::router_configuration_bfd_peer::dsl as bfd_peer_dsl;
        use nexus_db_schema::schema::router_configuration_bgp_peer::dsl as bgp_peer_dsl;
        use nexus_db_schema::schema::router_configuration_static_route::dsl as static_route_dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        let id = to_db_typed_uuid(authz_configuration.id());
        self.transaction_retry_wrapper("router_configuration_delete")
            .transaction(&conn, |conn| async move {
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
                error!(opctx.log, "router_configuration_delete failed"; "error" => ?e);
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_configuration),
                )
            })?;
        Ok(())
    }

    /// Create or replace the BGP configuration stored inline on a router
    /// configuration.
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

    /// Remove the BGP configuration stored inline on a router configuration.
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
}
