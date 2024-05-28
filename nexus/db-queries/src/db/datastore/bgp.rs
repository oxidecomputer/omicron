// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::context::OpContext;
use crate::db;
use crate::db::error::{public_error_from_diesel, ErrorHandler};
use crate::db::model::{BgpAnnounceSet, BgpAnnouncement, BgpConfig, Name};
use crate::db::pagination::paginated;
use crate::transaction_retry::OptionalError;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use ipnetwork::IpNetwork;
use nexus_db_model::BgpPeerView;
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{
    CreateResult, DeleteResult, Error, ListResultVec, LookupResult, NameOrId,
    ResourceType, SwitchLocation,
};
use ref_cast::RefCast;
use uuid::Uuid;

impl DataStore {
    pub async fn bgp_config_set(
        &self,
        opctx: &OpContext,
        config: &params::BgpConfigCreate,
    ) -> CreateResult<BgpConfig> {
        use db::schema::bgp_config::dsl;
        use db::schema::{
            bgp_announce_set, bgp_announce_set::dsl as announce_set_dsl,
        };
        use diesel::sql_types;
        use diesel::IntoSql;

        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("bgp_config_set")
            .transaction(&conn, |conn| async move {
                let announce_set_id: Uuid = match &config.bgp_announce_set_id {
                    NameOrId::Name(name) => {
                        announce_set_dsl::bgp_announce_set
                            .filter(bgp_announce_set::time_deleted.is_null())
                            .filter(bgp_announce_set::name.eq(name.to_string()))
                            .select(bgp_announce_set::id)
                            .limit(1)
                            .first_async::<Uuid>(&conn)
                            .await?
                    }
                    NameOrId::Id(id) => *id,
                };

                let config =
                    BgpConfig::from_config_create(config, announce_set_id);

                let matching_entry_subquery = dsl::bgp_config
                    .filter(dsl::name.eq(Name::from(config.name().clone())))
                    .filter(dsl::time_deleted.is_null())
                    .select(dsl::name);

                // SELECT exactly the values we're trying to INSERT, but only
                // if it does not already exist.
                let new_entry_subquery = diesel::dsl::select((
                    config.id().into_sql::<sql_types::Uuid>(),
                    config.name().to_string().into_sql::<sql_types::Text>(),
                    config
                        .description()
                        .to_string()
                        .into_sql::<sql_types::Text>(),
                    config.asn.into_sql::<sql_types::BigInt>(),
                    config.bgp_announce_set_id.into_sql::<sql_types::Uuid>(),
                    config
                        .vrf
                        .clone()
                        .into_sql::<sql_types::Nullable<sql_types::Text>>(),
                    Utc::now().into_sql::<sql_types::Timestamptz>(),
                    Utc::now().into_sql::<sql_types::Timestamptz>(),
                ))
                .filter(diesel::dsl::not(diesel::dsl::exists(
                    matching_entry_subquery,
                )));

                diesel::insert_into(dsl::bgp_config)
                    .values(new_entry_subquery)
                    .into_columns((
                        dsl::id,
                        dsl::name,
                        dsl::description,
                        dsl::asn,
                        dsl::bgp_announce_set_id,
                        dsl::vrf,
                        dsl::time_created,
                        dsl::time_modified,
                    ))
                    .execute_async(&conn)
                    .await?;

                dsl::bgp_config
                    .filter(dsl::name.eq(Name::from(config.name().clone())))
                    .filter(dsl::time_deleted.is_null())
                    .select(BgpConfig::as_select())
                    .limit(1)
                    .first_async(&conn)
                    .await
            })
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn bgp_config_delete(
        &self,
        opctx: &OpContext,
        sel: &params::BgpConfigSelector,
    ) -> DeleteResult {
        use db::schema::bgp_config;
        use db::schema::bgp_config::dsl as bgp_config_dsl;

        use db::schema::switch_port_settings_bgp_peer_config as sps_bgp_peer_config;
        use db::schema::switch_port_settings_bgp_peer_config::dsl as sps_bgp_peer_config_dsl;

        #[derive(Debug)]
        enum BgpConfigDeleteError {
            ConfigInUse,
        }

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("bgp_config_delete")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    let name_or_id = sel.name_or_id.clone();

                    let id: Uuid = match name_or_id {
                        NameOrId::Id(id) => id,
                        NameOrId::Name(name) => {
                            bgp_config_dsl::bgp_config
                                .filter(bgp_config::name.eq(name.to_string()))
                                .select(bgp_config::id)
                                .limit(1)
                                .first_async::<Uuid>(&conn)
                                .await?
                        }
                    };

                    let count =
                        sps_bgp_peer_config_dsl::switch_port_settings_bgp_peer_config
                            .filter(sps_bgp_peer_config::bgp_config_id.eq(id))
                            .count()
                            .execute_async(&conn)
                            .await?;

                    if count > 0 {
                        return Err(err.bail(BgpConfigDeleteError::ConfigInUse));
                    }

                    diesel::update(bgp_config_dsl::bgp_config)
                        .filter(bgp_config_dsl::id.eq(id))
                        .set(bgp_config_dsl::time_deleted.eq(Utc::now()))
                        .execute_async(&conn)
                        .await?;

                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        BgpConfigDeleteError::ConfigInUse => {
                            Error::invalid_request("BGP config in use")
                        }
                    }
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    pub async fn bgp_config_get(
        &self,
        opctx: &OpContext,
        name_or_id: &NameOrId,
    ) -> LookupResult<BgpConfig> {
        use db::schema::bgp_config;
        use db::schema::bgp_config::dsl;
        let conn = self.pool_connection_authorized(opctx).await?;

        let name_or_id = name_or_id.clone();

        let config = match name_or_id {
            NameOrId::Name(name) => dsl::bgp_config
                .filter(bgp_config::name.eq(name.to_string()))
                .select(BgpConfig::as_select())
                .limit(1)
                .first_async::<BgpConfig>(&*conn)
                .await
                .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server)),
            NameOrId::Id(id) => dsl::bgp_config
                .filter(bgp_config::id.eq(id))
                .select(BgpConfig::as_select())
                .limit(1)
                .first_async::<BgpConfig>(&*conn)
                .await
                .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server)),
        }?;

        Ok(config)
    }

    pub async fn bgp_config_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<BgpConfig> {
        use db::schema::bgp_config::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::bgp_config, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::bgp_config,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::time_deleted.is_null())
        .select(BgpConfig::as_select())
        .load_async(&*conn)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn bgp_announce_list(
        &self,
        opctx: &OpContext,
        sel: &params::BgpAnnounceSetSelector,
    ) -> ListResultVec<BgpAnnouncement> {
        use db::schema::{
            bgp_announce_set, bgp_announce_set::dsl as announce_set_dsl,
            bgp_announcement::dsl as announce_dsl,
        };

        #[derive(Debug)]
        enum BgpAnnounceListError {
            AnnounceSetNotFound(Name),
        }

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("bgp_announce_list")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    let name_or_id = sel.name_or_id.clone();

                    let announce_id: Uuid = match name_or_id {
                        NameOrId::Id(id) => id,
                        NameOrId::Name(name) => {
                            announce_set_dsl::bgp_announce_set
                                .filter(
                                    bgp_announce_set::time_deleted.is_null(),
                                )
                                .filter(
                                    bgp_announce_set::name.eq(name.to_string()),
                                )
                                .select(bgp_announce_set::id)
                                .limit(1)
                                .first_async::<Uuid>(&conn)
                                .await
                                .map_err(|e| {
                                    err.bail_retryable_or(
                                e,
                                BgpAnnounceListError::AnnounceSetNotFound(
                                    Name::from(name.clone()),
                                )
                            )
                                })?
                        }
                    };

                    let result = announce_dsl::bgp_announcement
                        .filter(announce_dsl::announce_set_id.eq(announce_id))
                        .select(BgpAnnouncement::as_select())
                        .load_async(&conn)
                        .await?;

                    Ok(result)
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        BgpAnnounceListError::AnnounceSetNotFound(name) => {
                            Error::not_found_by_name(
                                ResourceType::BgpAnnounceSet,
                                &name,
                            )
                        }
                    }
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    pub async fn bgp_create_announce_set(
        &self,
        opctx: &OpContext,
        announce: &params::BgpAnnounceSetCreate,
    ) -> CreateResult<(BgpAnnounceSet, Vec<BgpAnnouncement>)> {
        use db::schema::bgp_announce_set::dsl as announce_set_dsl;
        use db::schema::bgp_announcement::dsl as bgp_announcement_dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("bgp_create_announce_set")
            .transaction(&conn, |conn| async move {
                let bas: BgpAnnounceSet = announce.clone().into();

                let found_as: Option<BgpAnnounceSet> =
                    announce_set_dsl::bgp_announce_set
                        .filter(
                            announce_set_dsl::name
                                .eq(Name::from(bas.name().clone())),
                        )
                        .filter(announce_set_dsl::time_deleted.is_null())
                        .select(BgpAnnounceSet::as_select())
                        .limit(1)
                        .first_async(&conn)
                        .await
                        .ok();

                let db_as = match found_as {
                    Some(v) => v,
                    None => {
                        diesel::insert_into(announce_set_dsl::bgp_announce_set)
                            .values(bas.clone())
                            .returning(BgpAnnounceSet::as_returning())
                            .get_result_async::<BgpAnnounceSet>(&conn)
                            .await?
                    }
                };

                let mut db_annoucements = Vec::new();
                for a in &announce.announcement {
                    let an = BgpAnnouncement {
                        announce_set_id: db_as.id(),
                        address_lot_block_id: bas.identity.id,
                        network: a.network.into(),
                    };

                    let found_an: Option<BgpAnnouncement> =
                        bgp_announcement_dsl::bgp_announcement
                            .filter(
                                bgp_announcement_dsl::announce_set_id
                                    .eq(db_as.id()),
                            )
                            .filter(
                                bgp_announcement_dsl::network
                                    .eq(IpNetwork::from(a.network)),
                            )
                            .select(BgpAnnouncement::as_select())
                            .limit(1)
                            .first_async(&conn)
                            .await
                            .ok();

                    let an = match found_an {
                        Some(v) => v,
                        None => {
                            diesel::insert_into(
                                bgp_announcement_dsl::bgp_announcement,
                            )
                            .values(an.clone())
                            .returning(BgpAnnouncement::as_returning())
                            .get_result_async::<BgpAnnouncement>(&conn)
                            .await?
                        }
                    };

                    db_annoucements.push(an);
                }

                Ok((db_as, db_annoucements))
            })
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn bgp_delete_announce_set(
        &self,
        opctx: &OpContext,
        sel: &params::BgpAnnounceSetSelector,
    ) -> DeleteResult {
        use db::schema::bgp_announce_set;
        use db::schema::bgp_announce_set::dsl as announce_set_dsl;
        use db::schema::bgp_announcement::dsl as bgp_announcement_dsl;

        use db::schema::bgp_config;
        use db::schema::bgp_config::dsl as bgp_config_dsl;

        #[derive(Debug)]
        enum BgpAnnounceSetDeleteError {
            AnnounceSetInUse,
        }

        let conn = self.pool_connection_authorized(opctx).await?;
        let name_or_id = sel.name_or_id.clone();

        let err = OptionalError::new();
        self.transaction_retry_wrapper("bgp_delete_announce_set")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let name_or_id = name_or_id.clone();
                async move {
                    let id: Uuid = match name_or_id {
                        NameOrId::Name(name) => {
                            announce_set_dsl::bgp_announce_set
                                .filter(
                                    bgp_announce_set::name.eq(name.to_string()),
                                )
                                .select(bgp_announce_set::id)
                                .limit(1)
                                .first_async::<Uuid>(&conn)
                                .await?
                        }
                        NameOrId::Id(id) => id,
                    };

                    let count = bgp_config_dsl::bgp_config
                        .filter(bgp_config::bgp_announce_set_id.eq(id))
                        .count()
                        .execute_async(&conn)
                        .await?;

                    if count > 0 {
                        return Err(err.bail(
                            BgpAnnounceSetDeleteError::AnnounceSetInUse,
                        ));
                    }

                    diesel::update(announce_set_dsl::bgp_announce_set)
                        .filter(announce_set_dsl::id.eq(id))
                        .set(announce_set_dsl::time_deleted.eq(Utc::now()))
                        .execute_async(&conn)
                        .await?;

                    diesel::delete(bgp_announcement_dsl::bgp_announcement)
                        .filter(bgp_announcement_dsl::announce_set_id.eq(id))
                        .execute_async(&conn)
                        .await?;

                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    match err {
                        BgpAnnounceSetDeleteError::AnnounceSetInUse => {
                            Error::invalid_request("BGP announce set in use")
                        }
                    }
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    pub async fn bgp_peer_configs(
        &self,
        opctx: &OpContext,
        switch: SwitchLocation,
        port: String,
    ) -> ListResultVec<BgpPeerView> {
        use db::schema::bgp_peer_view::dsl;

        let results = dsl::bgp_peer_view
            .filter(dsl::switch_location.eq(switch.to_string()))
            .filter(dsl::port_name.eq(port))
            .select(BgpPeerView::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(results)
    }
}
