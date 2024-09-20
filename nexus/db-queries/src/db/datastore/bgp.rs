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
use nexus_db_model::{
    BgpPeerView, SwitchPortBgpPeerConfigAllowExport,
    SwitchPortBgpPeerConfigAllowImport, SwitchPortBgpPeerConfigCommunity,
};
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
    pub async fn bgp_config_create(
        &self,
        opctx: &OpContext,
        config: &params::BgpConfigCreate,
    ) -> CreateResult<BgpConfig> {
        use db::schema::bgp_config::dsl;
        use db::schema::{
            bgp_announce_set, bgp_announce_set::dsl as announce_set_dsl,
        };

        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();
        self.transaction_retry_wrapper("bgp_config_create")
            .transaction(&conn, |conn| {

                let err = err.clone();
                async move {
                    let announce_set_id = match config.bgp_announce_set_id.clone() {
                        // Resolve Name to UUID
                        NameOrId::Name(name) => announce_set_dsl::bgp_announce_set
                            .filter(bgp_announce_set::time_deleted.is_null())
                            .filter(bgp_announce_set::name.eq(name.to_string()))
                            .select(bgp_announce_set::id)
                            .limit(1)
                            .first_async::<Uuid>(&conn)
                            .await
                            .map_err(|e| {
                                let msg = "failed to lookup announce set by name";
                                error!(opctx.log, "{msg}"; "error" => ?e);

                                match e {
                                    diesel::result::Error::NotFound => {
                                        err.bail(Error::not_found_by_name(
                                            ResourceType::BgpAnnounceSet,
                                            &name,
                                        ))
                                    }
                                    _ => err.bail(Error::internal_error(msg)),

                                }
                            }),

                        // We cannot assume that the provided UUID is actually real.
                        // Lookup the parent record by UUID to verify that it is valid.
                        NameOrId::Id(id) => announce_set_dsl::bgp_announce_set
                            .filter(bgp_announce_set::time_deleted.is_null())
                            .filter(bgp_announce_set::id.eq(id))
                            .select(bgp_announce_set::id)
                            .limit(1)
                            .first_async::<Uuid>(&conn)
                            .await
                            .map_err(|e| {
                                let msg = "failed to lookup announce set by id";
                                error!(opctx.log, "{msg}"; "error" => ?e);

                                match e {
                                    diesel::result::Error::NotFound => {
                                        err.bail(Error::not_found_by_id(
                                            ResourceType::BgpAnnounceSet,
                                            &id,
                                        ))
                                    }
                                    _ => err.bail(Error::internal_error(msg)),

                                }
                            }),
                    }?;

                    let config =
                        BgpConfig::from_config_create(config, announce_set_id);

                    // Idempotency:
                    // Check to see if an exact match for the config already exists
                    let query = dsl::bgp_config
                        .filter(dsl::name.eq(config.name().to_string()))
                        .filter(dsl::asn.eq(config.asn))
                        .filter(dsl::bgp_announce_set_id.eq(config.bgp_announce_set_id))
                        .into_boxed();

                    let query = match config.vrf.clone() {
                        Some(v) => query.filter(dsl::vrf.eq(v)),
                        None => query.filter(dsl::vrf.is_null()),
                    };

                    let query = match config.shaper.clone() {
                        Some(v) => query.filter(dsl::shaper.eq(v)),
                        None => query.filter(dsl::shaper.is_null()),
                    };

                    let query = match config.checker.clone() {
                        Some(v) => query.filter(dsl::checker.eq(v)),
                        None => query.filter(dsl::checker.is_null()),
                    };

                    let matching_config = match query
                        .filter(dsl::time_deleted.is_null())
                        .select(BgpConfig::as_select())
                        .first_async::<BgpConfig>(&conn)
                        .await {
                            Ok(v)  => Ok(Some(v)),
                            Err(e) => {
                                match e {
                                    diesel::result::Error::NotFound => {
                                        info!(opctx.log, "no matching bgp config found");
                                        Ok(None)
                                    }
                                    _ => {
                                        let msg = "error while checking if bgp config exists";
                                        error!(opctx.log, "{msg}"; "error" => ?e);
                                        Err(err.bail(Error::internal_error(msg)))
                                    }
                                }
                            }
                        }?;

                    // If so, we're done!
                    if let Some(existing_config) = matching_config {
                        return Ok(existing_config);
                    }

                    // TODO: remove once per-switch-multi-asn support is added
                    // Bail if a conflicting config for this ASN already exists.
                    // This is a temporary measure until multi-asn-per-switch is supported.
                    let configs_with_asn: Vec<BgpConfig> = dsl::bgp_config
                        .filter(dsl::asn.eq(config.asn))
                        .filter(dsl::time_deleted.is_null())
                        .select(BgpConfig::as_select())
                        .load_async(&conn)
                        .await?;

                    if !configs_with_asn.is_empty() {
                        error!(
                            opctx.log,
                            "different config for asn already exists";
                            "asn" => ?config.asn,
                            "requested_config" => ?config,
                            "conflicting_configs" => ?configs_with_asn
                        );
                        return Err(err.bail(Error::conflict("cannot have more than one configuration per ASN")));
                    }

                    diesel::insert_into(dsl::bgp_config)
                        .values(config.clone())
                        .returning(BgpConfig::as_returning())
                        .get_result_async(&conn)
                        .await
                        .map_err(|e | {
                            let msg = "failed to insert bgp config";
                            error!(opctx.log, "{msg}"; "error" => ?e);

                            match e {
                                diesel::result::Error::DatabaseError(kind, _) => {
                                    match kind {
                                        diesel::result::DatabaseErrorKind::UniqueViolation => {
                                            err.bail(Error::conflict("a field that must be unique conflicts with an existing record"))
                                        },
                                        // technically we don't use Foreign Keys but it doesn't hurt to match on them
                                        // instead of returning a 500 by default in the event that we do switch to Foreign Keys
                                        diesel::result::DatabaseErrorKind::ForeignKeyViolation => {
                                            err.bail(Error::conflict("an id field references an object that does not exist"))
                                        }
                                        diesel::result::DatabaseErrorKind::NotNullViolation => {
                                            err.bail(Error::invalid_request("a required field was not provided"))
                                        }
                                        diesel::result::DatabaseErrorKind::CheckViolation => {
                                            err.bail(Error::invalid_request("one or more fields are not valid values"))
                                        },
                                        _ => err.bail(Error::internal_error(msg)),
                                    }
                                }
                                _ => err.bail(Error::internal_error(msg)),
                            }
                        })
                }
            })
            .await
            .map_err(|e|{
                let msg = "bgp_config_create failed";
                if let Some(err) = err.take() {
                    error!(opctx.log, "{msg}"; "error" => ?err);
                    err
                } else {
                    // The transaction handler errors along with any errors emitted via "?"
                    // will fall through to here. These errors should truly be 500s
                    // because they are an internal hiccup that likely was not triggered by
                    // user input.
                    error!(opctx.log, "{msg}"; "error" => ?e);
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
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

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("bgp_config_delete")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    let name_or_id = sel.name_or_id.clone();

                    let id: Uuid = match name_or_id {
                        NameOrId::Id(id) => bgp_config_dsl::bgp_config
                            .filter(bgp_config::id.eq(id))
                            .select(bgp_config::id)
                            .filter(bgp_config::time_deleted.is_null())
                            .limit(1)
                            .first_async::<Uuid>(&conn)
                            .await
                            .map_err(|e| {
                                let msg = "failed to lookup bgp config by id";
                                error!(opctx.log, "{msg}"; "error" => ?e);

                                match e {
                                    diesel::result::Error::NotFound => {
                                        err.bail(Error::not_found_by_id(
                                            ResourceType::BgpConfig,
                                            &id,
                                        ))
                                    }
                                    _ => err.bail(Error::internal_error(msg)),

                                }
                            }),
                        NameOrId::Name(name) =>
                            bgp_config_dsl::bgp_config
                            .filter(bgp_config::name.eq(name.to_string()))
                            .filter(bgp_config::time_deleted.is_null())
                            .select(bgp_config::id)
                            .limit(1)
                            .first_async::<Uuid>(&conn)
                            .await
                            .map_err(|e| {
                                let msg = "failed to lookup bgp config by name";
                                error!(opctx.log, "{msg}"; "error" => ?e);

                                match e {
                                    diesel::result::Error::NotFound => {
                                        err.bail(Error::not_found_by_name(
                                            ResourceType::BgpConfig,
                                            &name,
                                        ))
                                    }
                                    _ => err.bail(Error::internal_error(msg)),

                                }
                            }),
                    }?;

                    let count: i64 =
                        sps_bgp_peer_config_dsl::switch_port_settings_bgp_peer_config
                        .filter(sps_bgp_peer_config::bgp_config_id.eq(id))
                        .select(sps_bgp_peer_config::bgp_config_id)
                        .count()
                        .get_result_async(&conn)
                        .await?;

                    if count > 0 {
                        return Err(err.bail(Error::conflict("BGP Config is in use and cannot be deleted")));
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
                let msg = "bgp_config_delete failed";
                if let Some(err) = err.take() {
                    error!(opctx.log, "{msg}"; "error" => ?err);
                    err
                } else {
                    error!(opctx.log, "{msg}"; "error" => ?e);
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

        match name_or_id {
            NameOrId::Name(name) => dsl::bgp_config
                .filter(bgp_config::name.eq(name.to_string()))
                .select(BgpConfig::as_select())
                .limit(1)
                .first_async::<BgpConfig>(&*conn)
                .await
                .map_err(|e| {
                    let msg = "failed to lookup bgp config by name";
                    error!(opctx.log, "{msg}"; "error" => ?e);

                    match e {
                        diesel::result::Error::NotFound => {
                            Error::not_found_by_name(
                                ResourceType::BgpConfig,
                                &name,
                            )
                        }
                        _ => Error::internal_error(msg),
                    }
                }),
            NameOrId::Id(id) => dsl::bgp_config
                .filter(bgp_config::id.eq(id))
                .select(BgpConfig::as_select())
                .limit(1)
                .first_async::<BgpConfig>(&*conn)
                .await
                .map_err(|e| {
                    let msg = "failed to lookup bgp config by id";
                    error!(opctx.log, "{msg}"; "error" => ?e);

                    match e {
                        diesel::result::Error::NotFound => {
                            Error::not_found_by_id(ResourceType::BgpConfig, &id)
                        }
                        _ => Error::internal_error(msg),
                    }
                }),
        }
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
        .map_err(|e| {
            error!(opctx.log, "bgp_config_list failed"; "error" => ?e);
            public_error_from_diesel(e, ErrorHandler::Server)
        })
    }

    pub async fn bgp_announce_set_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<BgpAnnounceSet> {
        use db::schema::bgp_announce_set::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::bgp_announce_set, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::bgp_announce_set,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::time_deleted.is_null())
        .select(BgpAnnounceSet::as_select())
        .load_async(&*conn)
        .await
        .map_err(|e| {
            error!(opctx.log, "bgp_announce_set_list failed"; "error" => ?e);
            public_error_from_diesel(e, ErrorHandler::Server)
        })
    }

    pub async fn bgp_announcement_list(
        &self,
        opctx: &OpContext,
        sel: &params::BgpAnnounceSetSelector,
    ) -> ListResultVec<BgpAnnouncement> {
        use db::schema::{
            bgp_announce_set, bgp_announce_set::dsl as announce_set_dsl,
            bgp_announcement::dsl as announce_dsl,
        };

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("bgp_announce_list")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    let name_or_id = sel.announce_set.clone();

                    let announce_id: Uuid = match name_or_id {
                        NameOrId::Id(id) => announce_set_dsl::bgp_announce_set
                            .filter(bgp_announce_set::time_deleted.is_null())
                            .filter(bgp_announce_set::id.eq(id))
                            .select(bgp_announce_set::id)
                            .limit(1)
                            .first_async::<Uuid>(&conn)
                            .await
                            .map_err(|e| {
                                let msg = "failed to lookup announce set by id";
                                error!(opctx.log, "{msg}"; "error" => ?e);

                                match e {
                                    diesel::result::Error::NotFound => err
                                        .bail(Error::not_found_by_id(
                                            ResourceType::BgpAnnounceSet,
                                            &id,
                                        )),
                                    _ => err.bail(Error::internal_error(msg)),
                                }
                            }),
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
                                    let msg =
                                        "failed to lookup announce set by name";
                                    error!(opctx.log, "{msg}"; "error" => ?e);

                                    match e {
                                        diesel::result::Error::NotFound => err
                                            .bail(Error::not_found_by_name(
                                                ResourceType::BgpAnnounceSet,
                                                &name,
                                            )),
                                        _ => {
                                            err.bail(Error::internal_error(msg))
                                        }
                                    }
                                })
                        }
                    }?;

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
                error!(opctx.log, "bgp_announce_list failed"; "error" => ?e);
                if let Some(err) = err.take() {
                    err
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    // TODO: it seems this logic actually performs a find OR create for an announce set, and then replaces its child announcements.
    // This might be changed in omicron#6016 to an api that creates an announce set then allows adding / removal of announcements
    // to match how our other APIs work.
    pub async fn bgp_update_announce_set(
        &self,
        opctx: &OpContext,
        announce: &params::BgpAnnounceSetCreate,
    ) -> CreateResult<(BgpAnnounceSet, Vec<BgpAnnouncement>)> {
        use db::schema::bgp_announce_set::dsl as announce_set_dsl;
        use db::schema::bgp_announcement::dsl as bgp_announcement_dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("bgp_update_announce_set")
            .transaction(&conn, |conn| async move {
                let bas: BgpAnnounceSet = announce.clone().into();

                // ensure the announce set exists
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

                // clear existing announcements
                diesel::delete(bgp_announcement_dsl::bgp_announcement)
                    .filter(
                        bgp_announcement_dsl::announce_set_id.eq(db_as.id()),
                    )
                    .execute_async(&conn)
                    .await?;

                // repopulate announcements
                let mut db_annoucements = Vec::new();
                for a in &announce.announcement {
                    let an = BgpAnnouncement {
                        announce_set_id: db_as.id(),
                        address_lot_block_id: bas.identity.id,
                        network: a.network.into(),
                    };
                    let db_an = diesel::insert_into(
                        bgp_announcement_dsl::bgp_announcement,
                    )
                    .values(an.clone())
                    .returning(BgpAnnouncement::as_returning())
                    .get_result_async::<BgpAnnouncement>(&conn)
                    .await?;

                    db_annoucements.push(db_an);
                }

                Ok((db_as, db_annoucements))
            })
            .await
            .map_err(|e| {
                let msg = "bgp_update_announce_set failed";
                error!(opctx.log, "{msg}"; "error" => ?e);
                public_error_from_diesel(e, ErrorHandler::Server)
            })
    }

    // TODO: it seems this logic actually performs a create OR update of an announce set and its child announcements
    // (for example, it will add missing announcements). This might be changed in omicron#6016 to an api that creates an announce set
    // then allows adding / removal of announcements to match how our other APIs work.
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
            .map_err(|e| {
                let msg = "bgp_create_announce_set failed";
                error!(opctx.log, "{msg}"; "error" => ?e);
                public_error_from_diesel(e, ErrorHandler::Server)
            })
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

        let conn = self.pool_connection_authorized(opctx).await?;
        let name_or_id = sel.announce_set.clone();

        let err = OptionalError::new();
        self.transaction_retry_wrapper("bgp_delete_announce_set")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let name_or_id = name_or_id.clone();
                async move {
                    let id: Uuid = match name_or_id {
                        NameOrId::Id(id) => announce_set_dsl::bgp_announce_set
                            .filter(bgp_announce_set::time_deleted.is_null())
                            .filter(bgp_announce_set::id.eq(id))
                            .select(bgp_announce_set::id)
                            .limit(1)
                            .first_async::<Uuid>(&conn)
                            .await
                            .map_err(|e| {
                                let msg = "failed to lookup announce set by id";
                                error!(opctx.log, "{msg}"; "error" => ?e);

                                match e {
                                    diesel::result::Error::NotFound => err
                                        .bail(Error::not_found_by_id(
                                            ResourceType::BgpAnnounceSet,
                                            &id,
                                        )),
                                    _ => err.bail(Error::internal_error(msg)),
                                }
                            }),
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
                                    let msg =
                                        "failed to lookup announce set by name";
                                    error!(opctx.log, "{msg}"; "error" => ?e);

                                    match e {
                                        diesel::result::Error::NotFound => err
                                            .bail(Error::not_found_by_name(
                                                ResourceType::BgpAnnounceSet,
                                                &name,
                                            )),
                                        _ => {
                                            err.bail(Error::internal_error(msg))
                                        }
                                    }
                                })
                        }
                    }?;

                    let count: i64 = bgp_config_dsl::bgp_config
                        .filter(bgp_config::bgp_announce_set_id.eq(id))
                        .filter(bgp_config::time_deleted.is_null())
                        .count()
                        .get_result_async(&conn)
                        .await?;

                    if count > 0 {
                        return Err(
                            err.bail(Error::conflict("announce set in use"))
                        );
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
                let msg = "bgp_delete_announce_set failed";
                if let Some(err) = err.take() {
                    error!(opctx.log, "{msg}"; "error" => ?err);
                    err
                } else {
                    error!(opctx.log, "{msg}"; "error" => ?e);
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
            .map_err(|e| {
                let msg = "bgp_peer_configs failed";
                error!(opctx.log, "{msg}"; "error" => ?e);
                public_error_from_diesel(e, ErrorHandler::Server)
            })?;

        Ok(results)
    }

    pub async fn communities_for_peer(
        &self,
        opctx: &OpContext,
        port_settings_id: Uuid,
        interface_name: &str,
        addr: IpNetwork,
    ) -> ListResultVec<SwitchPortBgpPeerConfigCommunity> {
        use db::schema::switch_port_settings_bgp_peer_config_communities::dsl;

        let results = dsl::switch_port_settings_bgp_peer_config_communities
            .filter(dsl::port_settings_id.eq(port_settings_id))
            .filter(dsl::interface_name.eq(interface_name.to_owned()))
            .filter(dsl::addr.eq(addr))
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                let msg = "communities_for_peer failed";
                error!(opctx.log, "{msg}"; "error" => ?e);
                public_error_from_diesel(e, ErrorHandler::Server)
            })?;

        Ok(results)
    }

    pub async fn allow_export_for_peer(
        &self,
        opctx: &OpContext,
        port_settings_id: Uuid,
        interface_name: &str,
        addr: IpNetwork,
    ) -> LookupResult<Option<Vec<SwitchPortBgpPeerConfigAllowExport>>> {
        use db::schema::switch_port_settings_bgp_peer_config as db_peer;
        use db::schema::switch_port_settings_bgp_peer_config::dsl as peer_dsl;
        use db::schema::switch_port_settings_bgp_peer_config_allow_export as db_allow;
        use db::schema::switch_port_settings_bgp_peer_config_allow_export::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();
        self.transaction_retry_wrapper("bgp_allow_export_for_peer")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    let active = peer_dsl::switch_port_settings_bgp_peer_config
                        .filter(db_peer::port_settings_id.eq(port_settings_id))
                        .filter(db_peer::addr.eq(addr))
                        .select(db_peer::allow_export_list_active)
                        .limit(1)
                        .first_async::<bool>(&conn)
                        .await
                        .map_err(|e| {
                            let msg = "failed to lookup export settings for peer";
                            error!(opctx.log, "{msg}"; "error" => ?e);

                            match e {
                                diesel::result::Error::NotFound => {
                                    let not_found_msg = format!("peer with {addr} not found for port settings {port_settings_id}");
                                    err.bail(Error::non_resourcetype_not_found(not_found_msg))
                                },
                                _ => err.bail(Error::internal_error(msg)),
                            }
                        })?;

                    if !active {
                        return Ok(None);
                    }

                    let list =
                        dsl::switch_port_settings_bgp_peer_config_allow_export
                        .filter(
                            db_allow::port_settings_id.eq(port_settings_id),
                        )
                        .filter(
                            db_allow::interface_name
                                .eq(interface_name.to_owned()),
                        )
                        .filter(db_allow::addr.eq(addr))
                        .load_async(&conn)
                        .await?;

                    Ok(Some(list))
                }
            })
            .await
            .map_err(|e| {
                let msg = "allow_export_for_peer failed";
                if let Some(err) = err.take() {
                    error!(opctx.log, "{msg}"; "error" => ?err);
                    err
                } else {
                    error!(opctx.log, "{msg}"; "error" => ?e);
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    pub async fn allow_import_for_peer(
        &self,
        opctx: &OpContext,
        port_settings_id: Uuid,
        interface_name: &str,
        addr: IpNetwork,
    ) -> LookupResult<Option<Vec<SwitchPortBgpPeerConfigAllowImport>>> {
        use db::schema::switch_port_settings_bgp_peer_config as db_peer;
        use db::schema::switch_port_settings_bgp_peer_config::dsl as peer_dsl;
        use db::schema::switch_port_settings_bgp_peer_config_allow_import as db_allow;
        use db::schema::switch_port_settings_bgp_peer_config_allow_import::dsl;

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        self
            .transaction_retry_wrapper("bgp_allow_import_for_peer")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    let active = peer_dsl::switch_port_settings_bgp_peer_config
                        .filter(db_peer::port_settings_id.eq(port_settings_id))
                        .filter(db_peer::addr.eq(addr))
                        .select(db_peer::allow_import_list_active)
                        .limit(1)
                        .first_async::<bool>(&conn)
                        .await
                        .map_err(|e| {
                            let msg = "failed to lookup import settings for peer";
                            error!(opctx.log, "{msg}"; "error" => ?e);

                            match e {
                                diesel::result::Error::NotFound => {
                                    let not_found_msg = format!("peer with {addr} not found for port settings {port_settings_id}");
                                    err.bail(Error::non_resourcetype_not_found(not_found_msg))
                                },
                                _ => err.bail(Error::internal_error(msg)),
                            }
                        })?;

                    if !active {
                        return Ok(None);
                    }

                    let list =
                        dsl::switch_port_settings_bgp_peer_config_allow_import
                        .filter(
                            db_allow::port_settings_id.eq(port_settings_id),
                        )
                        .filter(
                            db_allow::interface_name
                                .eq(interface_name.to_owned()),
                        )
                        .filter(db_allow::addr.eq(addr))
                        .load_async(&conn)
                        .await?;

                    Ok(Some(list))
                }
            })
            .await
            .map_err(|e| {
                let msg = "allow_import_for_peer failed";
                if let Some(err) = err.take() {
                    error!(opctx.log, "{msg}"; "error" => ?err);
                    err
                } else {
                    error!(opctx.log, "{msg}"; "error" => ?e);
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::datastore::test_utils::datastore_test;
    use nexus_test_utils::db::test_setup_database;
    use omicron_common::api::external::IdentityMetadataCreateParams;
    use omicron_common::api::external::Name;
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn test_delete_bgp_config_delete_by_name() {
        let logctx = dev::test_setup_log("test_delete_bgp_config_by_name");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let config_name: Name = "testconfig47".parse().unwrap();
        let announce_name: Name = "testannounce47".parse().unwrap();

        datastore
            .bgp_create_announce_set(
                &opctx,
                &params::BgpAnnounceSetCreate {
                    identity: IdentityMetadataCreateParams {
                        name: announce_name.clone(),
                        description: String::from("a test announce set"),
                    },
                    announcement: Vec::default(),
                },
            )
            .await
            .expect("create bgp announce set");

        datastore
            .bgp_config_create(
                &opctx,
                &params::BgpConfigCreate {
                    identity: IdentityMetadataCreateParams {
                        name: config_name.clone(),
                        description: String::from("a test config"),
                    },
                    asn: 47,
                    bgp_announce_set_id: NameOrId::Name(announce_name.clone()),
                    vrf: None,
                    shaper: None,
                    checker: None,
                },
            )
            .await
            .expect("create bgp config");

        datastore
            .bgp_config_delete(
                &opctx,
                &params::BgpConfigSelector {
                    name_or_id: NameOrId::Name(config_name),
                },
            )
            .await
            .expect("delete bgp config by name");

        datastore
            .bgp_delete_announce_set(
                &opctx,
                &params::BgpAnnounceSetSelector {
                    announce_set: NameOrId::Name(announce_name),
                },
            )
            .await
            .expect("delete announce set by name");

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
