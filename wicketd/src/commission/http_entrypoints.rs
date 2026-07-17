// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::sync::Arc;

use bootstrap_agent_lockstep_client::ClientInfo as _;
use dropshot::{
    ApiDescription, HttpError, HttpResponseOk, HttpResponseUpdatedNoContent,
    Query, RequestContext, StreamingBody, TypedBody,
};
use iddqd::IdOrdMap;
use omicron_uuid_kinds::RackInitUuid;
use wicket_common::inventory::SledInventory;
use wicketd_commission_api::{
    WicketdCommissionApi, wicketd_commission_api_mod,
};
use wicketd_commission_types::inventory::{
    BootstrapSled, LocationInfo, SpIdentifier, SpInfo, SpInventoryParams,
};
use wicketd_commission_types::rack_setup::{
    CertificateUploadResponse, PutRecoveryUserPasswordHash,
    PutRssUserConfigInsensitive, RackOperationStatus,
};
use wicketd_commission_types::update::{
    ClearUpdateStateParams, RepositoryDescription, SpUpdateProgressEntry,
    StartUpdateParams,
};

use super::conversions;
use super::progress;
use crate::ServerContext;
use crate::http_helpers::{
    ba_lockstep_client, ba_lockstep_error_to_http, http_error_with_message,
    inventory_err_to_http, inventory_unavailable, mgs_inventory_or_unavail,
    shutdown_to_http, start_update,
};
use crate::mgs::GetInventoryResponse as MgsInventoryResponse;

type CommissionApiDescription = ApiDescription<Arc<ServerContext>>;

pub fn api() -> CommissionApiDescription {
    wicketd_commission_api_mod::api_description::<WicketdCommissionApiImpl>()
        .expect("registered commission entrypoints")
}

pub enum WicketdCommissionApiImpl {}

impl WicketdCommissionApi for WicketdCommissionApiImpl {
    type Context = Arc<ServerContext>;

    async fn get_sp_inventory(
        rqctx: RequestContext<Self::Context>,
        query: Query<SpInventoryParams>,
    ) -> Result<HttpResponseOk<IdOrdMap<SpInfo>>, HttpError> {
        let ctx = rqctx.context();
        let force_refresh = query.into_inner().force_refresh;

        let response = if force_refresh {
            let cached = ctx
                .mgs_handle
                .get_cached_inventory()
                .await
                .map_err(shutdown_to_http)?;
            // TODO-RAINCLAUDE: if the discovery read is Unavailable, fail
            // TODO-RAINCLAUDE: rather than degrade to a plain cached read, which
            // TODO-RAINCLAUDE: could return unrefreshed 200 data if the cache
            // TODO-RAINCLAUDE: populates between the two awaits.
            let ids = match cached {
                MgsInventoryResponse::Response { inventory, .. } => {
                    inventory.sps.iter().map(|sp| sp.id).collect()
                }
                MgsInventoryResponse::Unavailable => {
                    return Err(inventory_unavailable());
                }
            };
            ctx.mgs_handle
                .get_inventory_refreshing_sps(ids)
                .await
                .map_err(inventory_err_to_http)?
        } else {
            ctx.mgs_handle
                .get_cached_inventory()
                .await
                .map_err(shutdown_to_http)?
        };

        match response {
            MgsInventoryResponse::Response { inventory, .. } => {
                let mut sps = IdOrdMap::new();
                for sp in inventory.sps {
                    let sp = conversions::sp_info_to_v1(sp);
                    // TODO-RAINCLAUDE: MGS inventory keys SPs uniquely by id, so
                    // TODO-RAINCLAUDE: a duplicate is an invariant violation; surface
                    // TODO-RAINCLAUDE: it as a 500 naming the id rather than dropping it.
                    sps.insert_unique(sp).map_err(|err| {
                        let id = err.new_item().id;
                        http_error_with_message(
                            dropshot::ErrorStatusCode::INTERNAL_SERVER_ERROR,
                            Some("DuplicateSpId".to_string()),
                            format!(
                                "duplicate service processor id in inventory: \
                                 {id:?}"
                            ),
                        )
                    })?;
                }
                Ok(HttpResponseOk(sps))
            }
            MgsInventoryResponse::Unavailable => Err(inventory_unavailable()),
        }
    }

    async fn get_location(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<LocationInfo>, HttpError> {
        let ctx = rqctx.context();
        let inventory = mgs_inventory_or_unavail(&ctx.mgs_handle).await?;

        let switch_id = ctx.local_switch_id().await.ok_or_else(|| {
            http_error_with_message(
                dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
                Some("UnknownSwitchSlot".to_string()),
                "local switch slot not yet determined".to_string(),
            )
        })?;

        let switch_serial = inventory
            .sps
            .iter()
            .find(|sp| sp.id == switch_id)
            .and_then(|sp| sp.state.as_ref())
            .map(|state| state.serial_number.clone());

        let sled_serial =
            ctx.baseboard.as_ref().map(|b| b.identifier().to_string());

        Ok(HttpResponseOk(LocationInfo {
            switch_slot: switch_id.slot,
            switch_serial,
            sled_serial,
        }))
    }

    async fn get_bootstrap_sleds(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<IdOrdMap<BootstrapSled>>, HttpError> {
        let ctx = rqctx.context();
        let inventory = mgs_inventory_or_unavail(&ctx.mgs_handle).await?;

        let ddm_discovered_sleds = ctx.bootstrap_peers.sleds();
        let sled_inventory =
            SledInventory::new(&inventory, &ddm_discovered_sleds, &ctx.log);

        let mut sleds = IdOrdMap::new();
        for desc in sled_inventory.sleds {
            let sled = BootstrapSled {
                slot: desc.id.slot,
                identifier: desc.baseboard.identifier().to_string(),
                ip: desc.bootstrap_ip,
            };
            // TODO-RAINCLAUDE: DDM discovery keys sleds uniquely by baseboard
            // TODO-RAINCLAUDE: serial, so a duplicate is an invariant violation;
            // TODO-RAINCLAUDE: surface it as a 500 naming the id rather than
            // TODO-RAINCLAUDE: dropping it.
            sleds.insert_unique(sled).map_err(|err| {
                let id = &err.new_item().identifier;
                http_error_with_message(
                    dropshot::ErrorStatusCode::INTERNAL_SERVER_ERROR,
                    Some("DuplicateBootstrapSled".to_string()),
                    format!("duplicate bootstrap sled identifier: {id:?}"),
                )
            })?;
        }

        Ok(HttpResponseOk(sleds))
    }

    async fn get_repository(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RepositoryDescription>, HttpError> {
        let ctx = rqctx.context();
        let system_version = ctx.update_tracker.system_version().await;
        Ok(HttpResponseOk(RepositoryDescription { system_version }))
    }

    async fn put_repository(
        rqctx: RequestContext<Self::Context>,
        body: StreamingBody,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ctx = rqctx.context();
        ctx.update_tracker.put_repository(body.into_stream()).await?;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn get_update_progress(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<IdOrdMap<SpUpdateProgressEntry>>, HttpError>
    {
        let ctx = rqctx.context();
        let event_reports = ctx.update_tracker.event_reports().await;

        let mut entries = IdOrdMap::new();
        for (sp_type, slots) in event_reports {
            for (slot, report) in slots {
                let entry = SpUpdateProgressEntry {
                    sp: SpIdentifier { typ: sp_type, slot },
                    progress: progress::sp_update_progress(report),
                };
                // TODO-RAINCLAUDE: event reports are keyed by SP internally, so
                // TODO-RAINCLAUDE: a duplicate is an invariant violation; surface
                // TODO-RAINCLAUDE: it as a 500 naming the id rather than dropping
                // TODO-RAINCLAUDE: it.
                entries.insert_unique(entry).map_err(|err| {
                    let id = err.new_item().sp;
                    http_error_with_message(
                        dropshot::ErrorStatusCode::INTERNAL_SERVER_ERROR,
                        Some("DuplicateSpUpdateProgress".to_string()),
                        format!(
                            "duplicate service processor id in update \
                             progress: {id:?}"
                        ),
                    )
                })?;
            }
        }

        Ok(HttpResponseOk(entries))
    }

    async fn post_start_update(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<StartUpdateParams>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ctx = rqctx.context();
        let log = &rqctx.log;
        let params = params.into_inner();

        let options =
            conversions::start_update_options_to_internal(params.options);

        start_update(ctx, log, params.targets, options).await?;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn post_clear_update_state(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<ClearUpdateStateParams>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ctx = rqctx.context();
        let targets = params.into_inner().targets;

        if targets.is_empty() {
            return Err(HttpError::for_bad_request(
                None,
                "No update targets specified".into(),
            ));
        }

        ctx.update_tracker
            .clear_update_state(targets)
            .await
            .map_err(|err| err.to_http_error())?;
        Ok(HttpResponseUpdatedNoContent())
    }

    async fn get_rack_setup_state(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RackOperationStatus>, HttpError> {
        let ctx = rqctx.context();

        let client = ba_lockstep_client(ctx)?;

        let op_status = client
            .rack_initialization_status()
            .await
            .map_err(|err| ba_lockstep_error_to_http(err, "rack setup"))?
            .into_inner();

        Ok(HttpResponseOk(conversions::rack_operation_status_to_v1(op_status)))
    }

    async fn put_rss_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<PutRssUserConfigInsensitive>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ctx = rqctx.context();

        let config = body.into_inner();

        let inventory = mgs_inventory_or_unavail(&ctx.mgs_handle).await?;

        let mut guard = ctx.rss_or_multirack_join_config.lock().unwrap();
        let rss_config = guard.rss_config_mut_or_default();

        let ddm_discovered_sleds = ctx.bootstrap_peers.sleds();
        rss_config
            .update(
                config,
                ctx.baseboard.as_ref(),
                &inventory,
                &ddm_discovered_sleds,
                &ctx.log,
            )
            .map_err(|err| HttpError::for_bad_request(None, err))?;

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn delete_rss_config(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ctx = rqctx.context();

        let mut guard = ctx.rss_or_multirack_join_config.lock().unwrap();
        let rss_config = guard.rss_config_mut_or_conflict(
            "cannot delete RSS config when not preparing for RSS",
        )?;

        *rss_config = Default::default();

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn post_rss_config_cert(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<String>,
    ) -> Result<HttpResponseOk<CertificateUploadResponse>, HttpError> {
        let ctx = rqctx.context();

        let mut guard = ctx.rss_or_multirack_join_config.lock().unwrap();
        let rss_config = guard.rss_config_mut_or_conflict(
            "cannot post certificates when not preparing for RSS",
        )?;

        let response = rss_config
            .push_cert(body.into_inner())
            .map_err(|err| HttpError::for_bad_request(None, err))?;

        Ok(HttpResponseOk(response))
    }

    async fn post_rss_config_key(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<String>,
    ) -> Result<HttpResponseOk<CertificateUploadResponse>, HttpError> {
        let ctx = rqctx.context();

        let mut guard = ctx.rss_or_multirack_join_config.lock().unwrap();
        let rss_config = guard.rss_config_mut_or_conflict(
            "cannot post private keys when not preparing for RSS",
        )?;

        let response = rss_config
            .push_key(body.into_inner())
            .map_err(|err| HttpError::for_bad_request(None, err))?;

        Ok(HttpResponseOk(response))
    }

    async fn put_rss_config_recovery_user_password_hash(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<PutRecoveryUserPasswordHash>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ctx = rqctx.context();

        let hash =
            conversions::password_hash_to_internal(body.into_inner().hash)
                .map_err(|err| HttpError::for_bad_request(None, err))?;

        let mut guard = ctx.rss_or_multirack_join_config.lock().unwrap();
        let rss_config = guard.rss_config_mut_or_conflict(
            "cannot put recovery user password when not preparing for RSS",
        )?;

        rss_config.set_recovery_user_password_hash(hash);

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn post_run_rack_setup(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RackInitUuid>, HttpError> {
        let ctx = rqctx.context();
        let log = &rqctx.log;

        let client = ba_lockstep_client(ctx)?;

        let request = {
            let mut guard = ctx.rss_or_multirack_join_config.lock().unwrap();
            let rss_config = guard.rss_config_mut_or_conflict(
                "cannot run rack setup when not preparing for RSS",
            )?;
            rss_config.start_rss_request(&ctx.bootstrap_peers, log).map_err(
                |err| HttpError::for_bad_request(None, format!("{err:#}")),
            )?
        };

        slog::info!(
            ctx.log,
            "Sending RSS initialize request to {}",
            client.baseurl()
        );

        let init_id = client
            .rack_initialize(&request)
            .await
            .map_err(|err| ba_lockstep_error_to_http(err, "rack setup"))?
            .into_inner();

        Ok(HttpResponseOk(init_id))
    }
}
