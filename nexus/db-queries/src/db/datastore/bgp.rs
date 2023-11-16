use super::DataStore;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel;
use crate::db::error::retryable;
use crate::db::error::ErrorHandler;
use crate::db::model::Name;
use crate::db::model::{BgpAnnounceSet, BgpAnnouncement, BgpConfig};
use crate::db::pagination::paginated;
use crate::transaction_retry::RetryHelper;
use async_bb8_diesel::{AsyncConnection, AsyncRunQueryDsl};
use chrono::Utc;
use diesel::result::Error as DieselError;
use diesel::{ExpressionMethods, QueryDsl, SelectableHelper};
use nexus_types::external_api::params;
use nexus_types::identity::Resource;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::{
    CreateResult, DeleteResult, Error, ListResultVec, LookupResult, NameOrId,
    ResourceType,
};
use ref_cast::RefCast;
use std::sync::{Arc, OnceLock};
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
        let pool = self.pool_connection_authorized(opctx).await?;

        let retry_helper = RetryHelper::new(
            &self.transaction_retry_producer,
            "bgp_config_set",
        );
        pool.transaction_async_with_retry(
            |conn| async move {
                let id: Uuid = match &config.bgp_announce_set_id {
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

                let config = BgpConfig::from_config_create(config, id);

                let result = diesel::insert_into(dsl::bgp_config)
                    .values(config.clone())
                    .returning(BgpConfig::as_returning())
                    .get_result_async(&conn)
                    .await?;
                Ok(result)
            },
            retry_helper.as_callback(),
        )
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

        let err = Arc::new(OnceLock::new());
        let retry_helper = RetryHelper::new(
            &self.transaction_retry_producer,
            "bgp_config_delete",
        );
        let pool = self.pool_connection_authorized(opctx).await?;
        pool.transaction_async_with_retry(|conn| {
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
                    err.set(BgpConfigDeleteError::ConfigInUse).unwrap();
                    return Err(DieselError::RollbackTransaction);
                }

                diesel::update(bgp_config_dsl::bgp_config)
                    .filter(bgp_config_dsl::id.eq(id))
                    .set(bgp_config_dsl::time_deleted.eq(Utc::now()))
                    .execute_async(&conn)
                    .await?;

                Ok(())
            }
        }, retry_helper.as_callback())
        .await
        .map_err(|e| {
            if let Some(err) = err.get() {
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
        let pool = self.pool_connection_authorized(opctx).await?;

        let name_or_id = name_or_id.clone();

        let config = match name_or_id {
            NameOrId::Name(name) => dsl::bgp_config
                .filter(bgp_config::name.eq(name.to_string()))
                .select(BgpConfig::as_select())
                .limit(1)
                .first_async::<BgpConfig>(&*pool)
                .await
                .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server)),
            NameOrId::Id(id) => dsl::bgp_config
                .filter(bgp_config::id.eq(id))
                .select(BgpConfig::as_select())
                .limit(1)
                .first_async::<BgpConfig>(&*pool)
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

        let pool = self.pool_connection_authorized(opctx).await?;

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
        .load_async(&*pool)
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

        let err = Arc::new(OnceLock::new());
        let retry_helper = RetryHelper::new(
            &self.transaction_retry_producer,
            "bgp_announce_list",
        );
        let pool = self.pool_connection_authorized(opctx).await?;
        pool.transaction_async_with_retry(
            |conn| {
                let err = err.clone();
                async move {
                    let name_or_id = sel.name_or_id.clone();

                    let announce_id: Uuid = match name_or_id {
                    NameOrId::Id(id) => id,
                    NameOrId::Name(name) => announce_set_dsl::bgp_announce_set
                        .filter(bgp_announce_set::time_deleted.is_null())
                        .filter(bgp_announce_set::name.eq(name.to_string()))
                        .select(bgp_announce_set::id)
                        .limit(1)
                        .first_async::<Uuid>(&conn)
                        .await
                        .map_err(|e| {
                            if retryable(&e) {
                                return e;
                            }
                            err.set(BgpAnnounceListError::AnnounceSetNotFound(
                                Name::from(name.clone()),
                            )).unwrap();
                            DieselError::RollbackTransaction
                        })?,
                };

                    let result = announce_dsl::bgp_announcement
                        .filter(announce_dsl::announce_set_id.eq(announce_id))
                        .select(BgpAnnouncement::as_select())
                        .load_async(&conn)
                        .await?;

                    Ok(result)
                }
            },
            retry_helper.as_callback(),
        )
        .await
        .map_err(|e| {
            if let Some(err) = err.get() {
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

        let retry_helper = RetryHelper::new(
            &self.transaction_retry_producer,
            "bgp_create_announce_set",
        );
        let pool = self.pool_connection_authorized(opctx).await?;
        pool.transaction_async_with_retry(
            |conn| async move {
                let bas: BgpAnnounceSet = announce.clone().into();

                let db_as: BgpAnnounceSet =
                    diesel::insert_into(announce_set_dsl::bgp_announce_set)
                        .values(bas.clone())
                        .returning(BgpAnnounceSet::as_returning())
                        .get_result_async::<BgpAnnounceSet>(&conn)
                        .await?;

                let mut db_annoucements = Vec::new();
                for a in &announce.announcement {
                    let an = BgpAnnouncement {
                        announce_set_id: db_as.id(),
                        address_lot_block_id: bas.identity.id,
                        network: a.network.into(),
                    };
                    let an = diesel::insert_into(
                        bgp_announcement_dsl::bgp_announcement,
                    )
                    .values(an.clone())
                    .returning(BgpAnnouncement::as_returning())
                    .get_result_async::<BgpAnnouncement>(&conn)
                    .await?;
                    db_annoucements.push(an);
                }

                Ok((db_as, db_annoucements))
            },
            retry_helper.as_callback(),
        )
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

        let pool = self.pool_connection_authorized(opctx).await?;
        let name_or_id = sel.name_or_id.clone();

        let err = Arc::new(OnceLock::new());
        let retry_helper = RetryHelper::new(
            &self.transaction_retry_producer,
            "bgp_delete_announce_set",
        );
        pool.transaction_async_with_retry(
            |conn| {
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
                        err.set(BgpAnnounceSetDeleteError::AnnounceSetInUse)
                            .unwrap();
                        return Err(DieselError::RollbackTransaction);
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
            },
            retry_helper.as_callback(),
        )
        .await
        .map_err(|e| {
            if let Some(err) = err.get() {
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
}
