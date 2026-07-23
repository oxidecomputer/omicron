// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::sync::Arc;
use std::time::Duration;

use bootstrap_agent_lockstep_client::ClientInfo as _;
use dropshot::{
    ApiDescription, HttpError, HttpResponseOk, HttpResponseUpdatedNoContent,
    Path, RequestContext, StreamingBody, TypedBody,
};
use iddqd::IdOrdMap;
use omicron_uuid_kinds::RackInitUuid;
use wicketd_commission_api::{
    WicketdCommissionApi, wicketd_commission_api_mod,
};
use wicketd_commission_types::inventory::{
    GetBootstrapSledsResponse, LocationInfo, SpIdentifier, SpInventory,
    SpInventoryParams, SwitchSlot,
};
use wicketd_commission_types::rack_setup::{
    BgpAuthKey, BgpAuthKeyPath, CertificatePem, CertificateUploadResponse,
    PrivateKeyPem, PutRecoveryUserPasswordHash, PutRssUserConfigInsensitive,
    RackOperationStatus, SetBgpAuthKeyStatus,
};
use wicketd_commission_types::update::{
    ClearUpdateStateParams, ClearUpdateStateResponse, RepositoryDescription,
    SpUpdateProgress, StartUpdateParams,
};

use super::conversions;
use super::progress;
use crate::ServerContext;
use crate::helpers::SpIdentifierDisplay;
use crate::http_helpers::{
    ba_lockstep_client, ba_lockstep_error_to_http, http_error_with_message,
    mgs_inventory_or_unavail, shutdown_to_http, start_update,
};
use crate::mgs::{
    GetInventoryResponse as MgsInventoryResponse, MgsHandle, ShutdownInProgress,
};

/// How long to wait for a forced SP refresh.
///
/// This is comfortably above the ~1s it takes to do a normal refresh, and below
/// Progenitor's default 15s client timeout.
const SP_REFRESH_TIMEOUT: Duration = Duration::from_secs(10);

async fn cached_inventory_or_timeout(
    mgs_handle: &MgsHandle,
    timeout: Duration,
) -> Result<MgsInventoryResponse, HttpError> {
    match tokio::time::timeout(timeout, mgs_handle.get_cached_inventory()).await
    {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(err @ ShutdownInProgress)) => Err(shutdown_to_http(err)),
        Err(_elapsed) => Err(http_error_with_message(
            dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
            Some("MgsInventoryTimeout".to_string()),
            format!(
                "timed out after {}s waiting for MGS inventory; MGS may be \
                 down or wicketd may not yet have established contact with it \
                 (see wicketd logs for details)",
                timeout.as_secs(),
            ),
        )),
    }
}

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
        params: TypedBody<SpInventoryParams>,
    ) -> Result<HttpResponseOk<SpInventory>, HttpError> {
        let ctx = rqctx.context();
        let force_refresh = params.into_inner().force_refresh;

        let response = if force_refresh.is_empty() {
            cached_inventory_or_timeout(&ctx.mgs_handle, SP_REFRESH_TIMEOUT)
                .await?
        } else {
            tokio::time::timeout(
                SP_REFRESH_TIMEOUT,
                ctx.mgs_handle.get_inventory_refreshing_sps(
                    force_refresh.iter().copied().collect(),
                ),
            )
            .await
            .map_err(|_elapsed| {
                http_error_with_message(
                    dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
                    Some("SpRefreshTimeout".to_string()),
                    format!(
                        "timed out after {}s waiting for refreshed state of \
                         [{}]; the SPs may be unresponsive or MGS may be down \
                         (see wicketd logs for details)",
                        SP_REFRESH_TIMEOUT.as_secs(),
                        force_refresh
                            .iter()
                            .map(|id| SpIdentifierDisplay(*id).to_string())
                            .collect::<Vec<_>>()
                            .join(", "),
                    ),
                )
            })?
            .map_err(|err| err.to_http_error())?
        };

        let MgsInventoryResponse {
            sps,
            last_ignition_fetch_error,
            mgs_last_seen,
        } = response;

        // Iterate the manager's records directly rather than the
        // (frozen wire) projection, so an SP that has only ever failed
        // to respond — an error-only record, absent from the frozen
        // projection — is still visible to the commission API.
        let sp_infos = IdOrdMap::from_iter_unique(
            sps.iter().map(conversions::sp_info_to_ct),
        )
        .expect(
            "the manager's SP records are keyed by SpIdentifier, so \
             the projected SpInfos have unique ids",
        );
        let transceivers = conversions::transceivers_to_ct(
            ctx.transceiver_handle.get_transceivers(),
        );
        let ignition_fetch_error = last_ignition_fetch_error
            .as_ref()
            .map(conversions::fetch_error_to_ct);
        Ok(HttpResponseOk(SpInventory {
            mgs_last_seen,
            sps: sp_infos,
            ignition_fetch_error,
            transceivers,
        }))
    }

    async fn get_location(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<LocationInfo>, HttpError> {
        let ctx = rqctx.context();
        let response =
            cached_inventory_or_timeout(&ctx.mgs_handle, SP_REFRESH_TIMEOUT)
                .await?;

        let switch_id =
            ctx.local_switch_id().await.map_err(|err| err.to_http_error())?;

        let switch_serial = response
            .sps
            .get(&switch_id)
            .and_then(|record| record.data.as_ref())
            .map(|data| data.state.serial_number.clone());

        let sled_serial =
            ctx.baseboard.as_ref().map(|b| b.identifier().to_string());

        let switch_slot = match switch_id.slot {
            0 => SwitchSlot::Switch0,
            1 => SwitchSlot::Switch1,
            other => {
                return Err(http_error_with_message(
                    dropshot::ErrorStatusCode::INTERNAL_SERVER_ERROR,
                    None,
                    format!(
                        "wicketd derived an invalid local switch slot \
                         ({other}); expected 0 or 1"
                    ),
                ));
            }
        };

        Ok(HttpResponseOk(LocationInfo {
            switch_slot,
            switch_serial,
            sled_serial,
        }))
    }

    async fn get_bootstrap_sleds(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<GetBootstrapSledsResponse>, HttpError> {
        let ctx = rqctx.context();
        let response =
            cached_inventory_or_timeout(&ctx.mgs_handle, SP_REFRESH_TIMEOUT)
                .await?;

        let ddm_discovered_sleds = ctx.bootstrap_peers.sleds();
        let bootstrap_sleds = conversions::bootstrap_sleds_to_ct(
            &response.sps,
            &ddm_discovered_sleds,
        );

        Ok(HttpResponseOk(bootstrap_sleds))
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
    ) -> Result<HttpResponseOk<IdOrdMap<SpUpdateProgress>>, HttpError> {
        let ctx = rqctx.context();
        let event_reports = ctx.update_tracker.event_reports().await;

        // event_reports is keyed by (sp_type, slot), so the derived
        // SpIdentifiers are unique by construction.
        //
        // TODO: once rkdeploy is on the published API, we can make
        // `event_reports` be an IdOrdMap and make this much simpler.
        let mut entries = IdOrdMap::new();
        for (sp_type, slots) in event_reports {
            for (slot, report) in slots {
                entries
                    .insert_unique(progress::sp_update_progress(
                        SpIdentifier { typ: sp_type, slot },
                        report,
                    ))
                    .expect(
                        "event_reports is keyed by (sp_type, slot), so SP ids \
                         are unique",
                    );
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
    ) -> Result<HttpResponseOk<ClearUpdateStateResponse>, HttpError> {
        let ctx = rqctx.context();
        let targets = params.into_inner().targets;

        let response = ctx
            .update_tracker
            .clear_update_state(targets)
            .await
            .map_err(|err| err.to_http_error())?;
        Ok(HttpResponseOk(response))
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

        Ok(HttpResponseOk(conversions::rack_operation_status_to_ct(op_status)))
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
        body: TypedBody<CertificatePem>,
    ) -> Result<HttpResponseOk<CertificateUploadResponse>, HttpError> {
        let ctx = rqctx.context();

        let mut guard = ctx.rss_or_multirack_join_config.lock().unwrap();
        let rss_config = guard.rss_config_mut_or_conflict(
            "cannot post certificates when not preparing for RSS",
        )?;

        let response = rss_config
            .push_cert(body.into_inner().0)
            .map_err(|err| HttpError::for_bad_request(None, err))?;

        Ok(HttpResponseOk(response))
    }

    async fn post_rss_config_key(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<PrivateKeyPem>,
    ) -> Result<HttpResponseOk<CertificateUploadResponse>, HttpError> {
        let ctx = rqctx.context();

        let mut guard = ctx.rss_or_multirack_join_config.lock().unwrap();
        let rss_config = guard.rss_config_mut_or_conflict(
            "cannot post private keys when not preparing for RSS",
        )?;

        let response = rss_config
            .push_key(body.into_inner().0)
            .map_err(|err| HttpError::for_bad_request(None, err))?;

        Ok(HttpResponseOk(response))
    }

    async fn put_bgp_auth_key(
        rqctx: RequestContext<Self::Context>,
        path: Path<BgpAuthKeyPath>,
        body: TypedBody<BgpAuthKey>,
    ) -> Result<HttpResponseOk<SetBgpAuthKeyStatus>, HttpError> {
        let ctx = rqctx.context();
        let BgpAuthKeyPath { key_id } = path.into_inner();

        let mut guard = ctx.rss_or_multirack_join_config.lock().unwrap();
        let status = guard
            .set_bgp_auth_key(key_id, body.into_inner())
            .map_err(|err| HttpError::for_bad_request(None, err.to_string()))?;

        Ok(HttpResponseOk(status))
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
