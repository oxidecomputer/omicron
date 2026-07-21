// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for wicketd

use crate::SmfConfigValues;
use crate::context::CommonConfigContainer;
use crate::context::RssOrMultirackJoinConfig;
use crate::http_helpers::ba_lockstep_client;
use crate::http_helpers::ba_lockstep_error_to_http;
use crate::http_helpers::mgs_inventory_or_unavail;
use crate::http_helpers::start_update;
use crate::mgs::GetInventoryResponse as GetMgsInventoryResponse;
use crate::multirack_config::CurrentMultirackJoinConfig;
use crate::transceivers::GetTransceiversResponse;
use bootstrap_agent_lockstep_client::ClientInfo as _;
use bootstrap_agent_lockstep_types::RackOperationStatus;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::RequestContext;
use dropshot::StreamingBody;
use dropshot::TypedBody;
use internal_dns_resolver::Resolver;
use omicron_uuid_kinds::RackInitUuid;
use omicron_uuid_kinds::RackResetUuid;
use sled_agent_types::early_networking::SwitchSlot;
use sled_hardware_types::Baseboard;
use slog::o;
use std::sync::Arc;
use wicket_common::inventory::MgsV1InventorySnapshot;
use wicket_common::inventory::RackV1Inventory;
use wicket_common::inventory::SpIdentifier;
use wicket_common::inventory::SpType;
use wicket_common::inventory::TransceiverInventorySnapshot;
use wicket_common::multirack_setup::CurrentMultirackJoinUserConfig;
use wicket_common::multirack_setup::MultirackJoinConfigBaseUserInput;
use wicket_common::rack_setup::GetBgpAuthKeyInfoResponse;
use wicket_common::rack_update::AbortUpdateOptions;
use wicket_common::update_events::EventReport;
use wicketd_api::*;
use wicketd_commission_types::rack_setup::CertificateUploadResponse;
use wicketd_commission_types::rack_setup::PutRssUserConfigInsensitive;
use wicketd_commission_types::update::ClearUpdateStateResponse;

use crate::ServerContext;

type WicketdApiDescription = ApiDescription<Arc<ServerContext>>;

/// Return a description of the wicketd api for use in generating an OpenAPI spec
pub fn api() -> WicketdApiDescription {
    wicketd_api_mod::api_description::<WicketdApiImpl>()
        .expect("failed to register entrypoints")
}

pub enum WicketdApiImpl {}

impl WicketdApi for WicketdApiImpl {
    type Context = Arc<ServerContext>;

    async fn get_bootstrap_sleds(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<BootstrapSledIps>, HttpError> {
        let ctx = rqctx.context();

        let sleds = ctx
            .bootstrap_peers
            .sleds()
            .into_iter()
            .map(|(baseboard, ip)| BootstrapSledIp { baseboard, ip })
            .collect();

        Ok(HttpResponseOk(BootstrapSledIps { sleds }))
    }

    async fn get_rss_config(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<CurrentRssUserConfig>, HttpError> {
        let ctx = rqctx.context();

        // We can't run RSS if we don't have an inventory from MGS yet; we always
        // need to fill in the bootstrap sleds first.
        let inventory = mgs_inventory_or_unavail(&ctx.mgs_handle).await?;

        let mut config = ctx.rss_or_multirack_join_config.lock().unwrap();
        let rss_config = config.rss_config_mut_or_conflict(
            "cannot get RSS config when not preparing for RSS",
        )?;

        let ddm_discovered_sleds = &ctx.bootstrap_peers.sleds();
        let config =
            rss_config.get_latest(&inventory, &ddm_discovered_sleds, &ctx.log);

        Ok(HttpResponseOk(config.into()))
    }

    async fn get_multirack_join_config(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<CurrentMultirackJoinUserConfig>, HttpError> {
        let ctx = rqctx.context();

        // We can't join a multirack cluster if we don't have an inventory from
        // MGS yet; we always need to fill in the bootstrap sleds first.
        let inventory = mgs_inventory_or_unavail(&ctx.mgs_handle).await?;

        let mut config = ctx.rss_or_multirack_join_config.lock().unwrap();
        let join_config =
            config.multirack_join_config_mut().ok_or_else(|| {
                HttpError::for_not_found(
                    None,
                    "multirack join config not found".to_string(),
                )
            })?;

        let ddm_discovered_sleds = &ctx.bootstrap_peers.sleds();
        let config =
            join_config.get_latest(&inventory, &ddm_discovered_sleds, &ctx.log);

        Ok(HttpResponseOk(config.into()))
    }

    async fn put_rss_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<PutRssUserConfigInsensitive>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ctx = rqctx.context();

        // We can't run RSS if we don't have an inventory from MGS yet; we always
        // need to fill in the bootstrap sleds first.
        let inventory = mgs_inventory_or_unavail(&ctx.mgs_handle).await?;

        let mut config = ctx.rss_or_multirack_join_config.lock().unwrap();

        // Overwrite any non-rss config
        let rss_config = config.rss_config_mut_or_default();

        let ddm_discovered_sleds = &ctx.bootstrap_peers.sleds();
        rss_config
            .update(
                body.into_inner(),
                ctx.baseboard.as_ref(),
                &inventory,
                &ddm_discovered_sleds,
                &ctx.log,
            )
            .map_err(|err| HttpError::for_bad_request(None, err))?;

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn put_multirack_join_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<MultirackJoinConfigBaseUserInput>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ctx = rqctx.context();

        // We can't join a multirack cluster if we don't have an inventory from
        // MGS yet; we always need to fill in the bootstrap sleds first.
        let inventory = mgs_inventory_or_unavail(&ctx.mgs_handle).await?;

        let ddm_discovered_sleds = &ctx.bootstrap_peers.sleds();
        let mut config = ctx.rss_or_multirack_join_config.lock().unwrap();

        // We don't have a default (empty) version of a `join_config` like we do
        // with an `rss_config` so we have two different paths here.
        if let Some(join_config) = config.multirack_join_config_mut() {
            join_config
                .update(
                    body.into_inner(),
                    ctx.baseboard.as_ref(),
                    &inventory,
                    &ddm_discovered_sleds,
                    &ctx.log,
                )
                .map_err(|err| HttpError::for_bad_request(None, err))?;
        } else {
            // Overwrite any non-multirack-join config
            *config = RssOrMultirackJoinConfig::MultirackJoin(
                CurrentMultirackJoinConfig::new_with_inventory_and_peers(
                    ctx.baseboard.as_ref(),
                    body.into_inner(),
                    &inventory,
                    &ddm_discovered_sleds,
                    &ctx.log,
                )
                .map_err(|err| HttpError::for_bad_request(None, err))?,
            );
        }

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn post_rss_config_cert(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<String>,
    ) -> Result<HttpResponseOk<CertificateUploadResponse>, HttpError> {
        let ctx = rqctx.context();

        let mut config = ctx.rss_or_multirack_join_config.lock().unwrap();

        let rss_config = config.rss_config_mut_or_conflict(
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

        let mut config = ctx.rss_or_multirack_join_config.lock().unwrap();
        let rss_config = config.rss_config_mut_or_conflict(
            "cannot post private keys when not preparing for RSS",
        )?;

        let response = rss_config
            .push_key(body.into_inner())
            .map_err(|err| HttpError::for_bad_request(None, err))?;

        Ok(HttpResponseOk(response))
    }

    async fn get_bgp_auth_key_info(
        rqctx: RequestContext<Self::Context>,
        // A bit weird for a GET request to have a TypedBody, but there's no other
        // nice way to transmit this information as a batch.
        params: TypedBody<GetBgpAuthKeyParams>,
    ) -> Result<HttpResponseOk<GetBgpAuthKeyInfoResponse>, HttpError> {
        let ctx = rqctx.context();
        let params = params.into_inner();

        let config = ctx.rss_or_multirack_join_config.lock().unwrap();
        config
            .check_bgp_auth_keys_valid(&params.check_valid)
            .map_err(|err| HttpError::for_bad_request(None, err.to_string()))?;
        let data = config.get_bgp_auth_key_data();

        Ok(HttpResponseOk(GetBgpAuthKeyInfoResponse { data }))
    }

    async fn put_bgp_auth_key(
        rqctx: RequestContext<Self::Context>,
        params: Path<PutBgpAuthKeyParams>,
        body: TypedBody<PutBgpAuthKeyBody>,
    ) -> Result<HttpResponseOk<PutBgpAuthKeyResponse>, HttpError> {
        let ctx = rqctx.context();
        let params = params.into_inner();

        let mut config = ctx.rss_or_multirack_join_config.lock().unwrap();
        let status = config
            .set_bgp_auth_key(params.key_id, body.into_inner().key)
            .map_err(|err| HttpError::for_bad_request(None, err.to_string()))?;

        Ok(HttpResponseOk(PutBgpAuthKeyResponse { status }))
    }

    async fn put_rss_config_recovery_user_password_hash(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<PutRssRecoveryUserPasswordHash>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ctx = rqctx.context();

        let mut config = ctx.rss_or_multirack_join_config.lock().unwrap();

        let rss_config = config.rss_config_mut_or_conflict(
            "cannot put recovery user password when not preparing for RSS",
        )?;

        rss_config.set_recovery_user_password_hash(body.into_inner().hash);

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn delete_rss_config(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ctx = rqctx.context();

        let mut config = ctx.rss_or_multirack_join_config.lock().unwrap();
        let rss_config = config.rss_config_mut_or_conflict(
            "cannot delete RSS config when not preparing for RSS",
        )?;

        *rss_config = Default::default();

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
            .map_err(|err| ba_lockstep_error_to_http(err, "rack setup state"))?
            .into_inner();

        Ok(HttpResponseOk(op_status))
    }

    async fn post_run_rack_setup(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RackInitUuid>, HttpError> {
        let ctx = rqctx.context();
        let log = &rqctx.log;

        let client = ba_lockstep_client(ctx)?;

        let request = {
            let mut config = ctx.rss_or_multirack_join_config.lock().unwrap();

            let rss_config = config.rss_config_mut_or_conflict(
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

    async fn post_run_rack_reset(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RackResetUuid>, HttpError> {
        let ctx = rqctx.context();

        let client = ba_lockstep_client(ctx)?;

        slog::info!(
            ctx.log,
            "Sending RSS reset request to {}",
            client.baseurl()
        );

        let reset_id = client
            .rack_reset()
            .await
            .map_err(|err| ba_lockstep_error_to_http(err, "rack reset"))?
            .into_inner();

        Ok(HttpResponseOk(reset_id))
    }

    async fn get_inventory(
        rqctx: RequestContext<Self::Context>,
        body_params: TypedBody<GetInventoryParams>,
    ) -> Result<HttpResponseOk<GetInventoryResponse>, HttpError> {
        let GetInventoryParams { force_refresh } = body_params.into_inner();

        // Fetch the MGS-specific inventory first.
        let maybe_mgs_inventory = match rqctx
            .context()
            .mgs_handle
            .get_inventory_refreshing_sps(force_refresh)
            .await
        {
            Ok(GetMgsInventoryResponse::Response {
                inventory,
                mgs_last_seen,
            }) => Some((inventory, mgs_last_seen)),
            Ok(GetMgsInventoryResponse::Unavailable) => None,
            Err(err) => {
                return Err(err.to_http_error());
            }
        };

        // Fetch the transceiver information from the SP.
        let maybe_transceiver_inventory =
            match rqctx.context().transceiver_handle.get_transceivers() {
                GetTransceiversResponse::Response { transceivers } => {
                    // transceivers tracks the last_seen for each switch
                    // independently. But the (currently frozen) wicketd API
                    // wire shape only has a single last_seen field. So we must
                    // pick: min or max? We choose max here, so that if one of
                    // the fetch tasks is wedged, the timestamp indicates that.
                    //
                    // TODO: clean this up (report per-switch last_seen) once
                    // rkdeploy is on the stable commissioning API.
                    let last_seen = transceivers
                        .iter()
                        .map(|switch| switch.updated_at.elapsed())
                        .max();
                    last_seen.map(|last_seen| {
                        // The (currently frozen) wicketd API is a HashMap, so
                        // collect into that.
                        //
                        // TODO: switch to IdOrdMap once rkdeploy is on the
                        // stable commissioning API.
                        let inventory = transceivers
                            .into_iter()
                            .map(|switch| (switch.switch, switch.transceivers))
                            .collect();
                        (inventory, last_seen)
                    })
                }
                GetTransceiversResponse::Unavailable => None,
            };

        // Return 503 if both MGS and transceiver inventory are missing,
        // otherwise return what we can.
        if maybe_mgs_inventory.is_none()
            && maybe_transceiver_inventory.is_none()
        {
            return Err(HttpError::for_unavail(
                None,
                "Rack inventory not yet available".into(),
            ));
        }
        let mgs = maybe_mgs_inventory.map(|(inventory, last_seen)| {
            MgsV1InventorySnapshot { inventory, last_seen }
        });
        let transceivers =
            maybe_transceiver_inventory.map(|(inventory, last_seen)| {
                TransceiverInventorySnapshot { inventory, last_seen }
            });
        let inventory = RackV1Inventory { mgs, transceivers };
        Ok(HttpResponseOk(GetInventoryResponse::Response { inventory }))
    }

    async fn put_repository(
        rqctx: RequestContext<Self::Context>,
        body: StreamingBody,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let rqctx = rqctx.context();

        rqctx.update_tracker.put_repository(body.into_stream()).await?;

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn get_artifacts_and_event_reports(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<GetArtifactsAndEventReportsResponse>, HttpError>
    {
        let response =
            rqctx.context().update_tracker.artifacts_and_event_reports().await;
        Ok(HttpResponseOk(response))
    }

    async fn get_baseboard(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<GetBaseboardResponse>, HttpError> {
        let rqctx = rqctx.context();
        Ok(HttpResponseOk(GetBaseboardResponse {
            baseboard: rqctx.baseboard.clone(),
        }))
    }

    async fn get_location(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<GetLocationResponse>, HttpError> {
        let rqctx = rqctx.context();
        let inventory = mgs_inventory_or_unavail(&rqctx.mgs_handle).await?;

        // We don't error out in get_location on the local switch ID not being
        // available, so discard the error here (it's already logged in
        // local_switch_id).
        let switch_id = rqctx.local_switch_id().await.ok();
        let sled_baseboard = rqctx.baseboard.clone();

        let mut switch_baseboard = None;
        let mut sled_id = None;

        // Safety: `inventory_or_unavail` returns an error if there is no
        // MGS-derived inventory, so option is always `Some(_)`.
        for sp in &inventory.sps {
            if Some(sp.id) == switch_id {
                switch_baseboard = sp.state.as_ref().map(|state| {
                    // TODO-correctness `new_gimlet` isn't the right name: this is a
                    // sidecar baseboard.
                    Baseboard::new_gimlet(
                        state.serial_number.clone(),
                        state.model.clone(),
                        state.revision,
                    )
                });
            } else if let (Some(sled_baseboard), Some(state)) =
                (sled_baseboard.as_ref(), sp.state.as_ref())
            {
                if sled_baseboard.identifier() == state.serial_number
                    && sled_baseboard.model() == state.model
                    && sled_baseboard.revision() == state.revision
                {
                    sled_id = Some(sp.id);
                }
            }
        }

        Ok(HttpResponseOk(GetLocationResponse {
            sled_id,
            sled_baseboard,
            switch_baseboard,
            switch_id,
        }))
    }

    async fn post_start_update(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<StartUpdateParams>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let log = &rqctx.log;
        let rqctx = rqctx.context();
        let params = params.into_inner();

        start_update(rqctx, log, params.targets, params.options).await?;
        Ok(HttpResponseUpdatedNoContent {})
    }

    async fn get_update_sp(
        rqctx: RequestContext<Self::Context>,
        target: Path<SpIdentifier>,
    ) -> Result<HttpResponseOk<EventReport>, HttpError> {
        let event_report = rqctx
            .context()
            .update_tracker
            .event_report(target.into_inner())
            .await;
        Ok(HttpResponseOk(event_report))
    }

    async fn post_abort_update(
        rqctx: RequestContext<Self::Context>,
        target: Path<SpIdentifier>,
        opts: TypedBody<AbortUpdateOptions>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let log = &rqctx.log;
        let target = target.into_inner();

        let opts = opts.into_inner();
        if let Some(test_error) = opts.test_error {
            return Err(test_error
                .into_http_error(log, "aborting update")
                .await);
        }

        match rqctx
            .context()
            .update_tracker
            .abort_update(target, opts.message)
            .await
        {
            Ok(()) => Ok(HttpResponseUpdatedNoContent {}),
            Err(err) => Err(err.to_http_error()),
        }
    }

    async fn post_clear_update_state(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<ClearUpdateStateParams>,
    ) -> Result<HttpResponseOk<ClearUpdateStateResponse>, HttpError> {
        let log = &rqctx.log;
        let rqctx = rqctx.context();
        let params = params.into_inner();

        if let Some(test_error) = params.options.test_error {
            return Err(test_error
                .into_http_error(log, "clearing update state")
                .await);
        }

        match rqctx.update_tracker.clear_update_state(params.targets).await {
            Ok(response) => Ok(HttpResponseOk(response)),
            Err(err) => Err(err.to_http_error()),
        }
    }

    async fn post_ignition_command(
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpIgnitionCommand>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = rqctx.context();
        let PathSpIgnitionCommand { type_, slot, command } = path.into_inner();

        apictx
            .mgs_client
            .ignition_command(&type_, slot, command)
            .await
            .map_err(http_error_from_client_error)?;

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn post_start_preflight_uplink_check(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<PreflightUplinkCheckOptions>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let rqctx = rqctx.context();
        let options = body.into_inner();

        let our_switch_slot = match rqctx.local_switch_id().await {
            Ok(SpIdentifier { slot, typ: SpType::Switch }) => match slot {
                0 => SwitchSlot::Switch0,
                1 => SwitchSlot::Switch1,
                _ => {
                    return Err(HttpError::for_internal_error(format!(
                        "unexpected switch slot {slot}"
                    )));
                }
            },
            Ok(other) => {
                return Err(HttpError::for_internal_error(format!(
                    "unexpected switch SP identifier {other:?}"
                )));
            }
            Err(err) => {
                return Err(err.to_http_error());
            }
        };

        let (network_config, dns_servers, ntp_servers) = {
            let mut config = rqctx.rss_or_multirack_join_config.lock().unwrap();

            let rss_config = config.rss_config_mut_or_conflict(
                "cannot run preflight when not preparing for RSS",
            )?;
            let network_config = rss_config
                .user_specified_rack_network_config()
                .cloned()
                .ok_or_else(|| {
                    HttpError::for_bad_request(
                        None,
                        "uplink preflight check requires setting \
                     the uplink config for RSS"
                            .to_string(),
                    )
                })?;

            (
                network_config,
                rss_config.dns_servers().to_vec(),
                rss_config.ntp_servers().to_vec(),
            )
        };

        match rqctx
            .preflight_checker
            .uplink_start(
                network_config,
                dns_servers,
                ntp_servers,
                our_switch_slot,
                options.dns_name_to_query,
            )
            .await
        {
            Ok(()) => Ok(HttpResponseUpdatedNoContent {}),
            Err(err) => Err(HttpError::for_client_error(
                None,
                dropshot::ClientErrorStatusCode::TOO_MANY_REQUESTS,
                err.to_string(),
            )),
        }
    }

    async fn get_preflight_uplink_report(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<wicket_common::preflight_check::EventReport>,
        HttpError,
    > {
        let rqctx = rqctx.context();

        match rqctx.preflight_checker.uplink_event_report() {
        Some(report) => Ok(HttpResponseOk(report)),
        None => Err(HttpError::for_bad_request(
            None,
            "no preflight uplink report available - have you started a check?"
                .to_string(),
        )),
    }
    }

    async fn post_reload_config(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let smf_values = SmfConfigValues::read_current().map_err(|err| {
            HttpError::for_unavail(
                None,
                format!("failed to read SMF values: {err}"),
            )
        })?;

        let rqctx = rqctx.context();

        // We do not allow a config reload to change our bound address; return an
        // error if the caller is attempting to do so.
        if rqctx.bind_address != smf_values.address {
            return Err(HttpError::for_bad_request(
                None,
                "listening address cannot be reconfigured".to_string(),
            ));
        }

        if let Some(rack_subnet) = smf_values.rack_subnet {
            let resolver = Resolver::new_from_subnet(
                rqctx.log.new(o!("component" => "InternalDnsResolver")),
                rack_subnet,
            )
            .map_err(|err| {
                HttpError::for_unavail(
                    None,
                    format!("failed to create internal DNS resolver: {err}"),
                )
            })?;

            *rqctx.internal_dns_resolver.lock().unwrap() = Some(resolver);
        }

        Ok(HttpResponseUpdatedNoContent())
    }
}

fn http_error_from_client_error(
    err: gateway_client::Error<gateway_client::types::Error>,
) -> HttpError {
    // Most errors have a status code; the only one that definitely doesn't is
    // `Error::InvalidRequest`, for which we'll use `BAD_REQUEST`.
    let status_code = err
        .status()
        .map(|status| {
            status.try_into().expect("status code must be a client error")
        })
        .unwrap_or(dropshot::ErrorStatusCode::BAD_REQUEST);

    let message = format!("request to MGS failed: {err}");

    HttpError {
        status_code,
        error_code: None,
        external_message: message.clone(),
        internal_message: message,
        headers: None,
    }
}
