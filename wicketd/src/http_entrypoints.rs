// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for wicketd

use crate::SmfConfigValues;
use crate::helpers::SpIdentifierDisplay;
use crate::helpers::sps_to_string;
use crate::mgs::GetInventoryError as GetMgsInventoryError;
use crate::mgs::GetInventoryResponse as GetMgsInventoryResponse;
use crate::mgs::MgsHandle;
use crate::mgs::ShutdownInProgress;
use crate::transceivers::GetTransceiversResponse;
use crate::transceivers::Handle as TransceiverHandle;
use bootstrap_agent_client::types::RackOperationStatus;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::RequestContext;
use dropshot::StreamingBody;
use dropshot::TypedBody;
use internal_dns_resolver::Resolver;
use omicron_common::api::internal::shared::SwitchLocation;
use omicron_uuid_kinds::RackInitUuid;
use omicron_uuid_kinds::RackResetUuid;
use sled_hardware_types::Baseboard;
use slog::o;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use wicket_common::WICKETD_TIMEOUT;
use wicket_common::inventory::MgsV1InventorySnapshot;
use wicket_common::inventory::RackV1Inventory;
use wicket_common::inventory::SpIdentifier;
use wicket_common::inventory::SpType;
use wicket_common::inventory::TransceiverInventorySnapshot;
use wicket_common::rack_setup::GetBgpAuthKeyInfoResponse;
use wicket_common::rack_setup::PutRssUserConfigInsensitive;
use wicket_common::rack_update::AbortUpdateOptions;
use wicket_common::rack_update::ClearUpdateStateResponse;
use wicket_common::update_events::EventReport;
use wicketd_api::*;

use crate::ServerContext;

type WicketdApiDescription = ApiDescription<ServerContext>;

/// Return a description of the wicketd api for use in generating an OpenAPI spec
pub fn api() -> WicketdApiDescription {
    wicketd_api_mod::api_description::<WicketdApiImpl>()
        .expect("failed to register entrypoints")
}

pub enum WicketdApiImpl {}

impl WicketdApi for WicketdApiImpl {
    type Context = ServerContext;

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
        let inventory =
            mgs_inventory_or_unavail(&ctx.mgs_handle, &ctx.transceiver_handle)
                .await?;

        let mut config = ctx.rss_config.lock().unwrap();
        let inventory = inventory
            .mgs
            .expect("verified by `inventory_or_unavail`")
            .inventory;
        config.update_with_inventory_and_bootstrap_peers(
            &inventory,
            &ctx.bootstrap_peers,
            &ctx.log,
        );

        Ok(HttpResponseOk((&*config).into()))
    }

    async fn put_rss_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<PutRssUserConfigInsensitive>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ctx = rqctx.context();

        // We can't run RSS if we don't have an inventory from MGS yet; we always
        // need to fill in the bootstrap sleds first.
        let inventory =
            mgs_inventory_or_unavail(&ctx.mgs_handle, &ctx.transceiver_handle)
                .await?;

        let mut config = ctx.rss_config.lock().unwrap();
        let inventory = inventory
            .mgs
            .expect("verified by `inventory_or_unavail`")
            .inventory;
        config.update_with_inventory_and_bootstrap_peers(
            &inventory,
            &ctx.bootstrap_peers,
            &ctx.log,
        );
        config
            .update(body.into_inner(), ctx.baseboard.as_ref())
            .map_err(|err| HttpError::for_bad_request(None, err))?;

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn post_rss_config_cert(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<String>,
    ) -> Result<HttpResponseOk<CertificateUploadResponse>, HttpError> {
        let ctx = rqctx.context();

        let mut config = ctx.rss_config.lock().unwrap();
        let response = config
            .push_cert(body.into_inner())
            .map_err(|err| HttpError::for_bad_request(None, err))?;

        Ok(HttpResponseOk(response))
    }

    async fn post_rss_config_key(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<String>,
    ) -> Result<HttpResponseOk<CertificateUploadResponse>, HttpError> {
        let ctx = rqctx.context();

        let mut config = ctx.rss_config.lock().unwrap();
        let response = config
            .push_key(body.into_inner())
            .map_err(|err| HttpError::for_bad_request(None, err))?;

        Ok(HttpResponseOk(response))
    }

    async fn get_bgp_auth_key_info(
        rqctx: RequestContext<ServerContext>,
        // A bit weird for a GET request to have a TypedBody, but there's no other
        // nice way to transmit this information as a batch.
        params: TypedBody<GetBgpAuthKeyParams>,
    ) -> Result<HttpResponseOk<GetBgpAuthKeyInfoResponse>, HttpError> {
        let ctx = rqctx.context();
        let params = params.into_inner();

        let config = ctx.rss_config.lock().unwrap();
        config
            .check_bgp_auth_keys_valid(&params.check_valid)
            .map_err(|err| HttpError::for_bad_request(None, err.to_string()))?;
        let data = config.get_bgp_auth_key_data();

        Ok(HttpResponseOk(GetBgpAuthKeyInfoResponse { data }))
    }

    async fn put_bgp_auth_key(
        rqctx: RequestContext<ServerContext>,
        params: Path<PutBgpAuthKeyParams>,
        body: TypedBody<PutBgpAuthKeyBody>,
    ) -> Result<HttpResponseOk<PutBgpAuthKeyResponse>, HttpError> {
        let ctx = rqctx.context();
        let params = params.into_inner();

        let mut config = ctx.rss_config.lock().unwrap();
        let status = config
            .set_bgp_auth_key(params.key_id, body.into_inner().key)
            .map_err(|err| HttpError::for_bad_request(None, err.to_string()))?;

        Ok(HttpResponseOk(PutBgpAuthKeyResponse { status }))
    }

    async fn put_rss_config_recovery_user_password_hash(
        rqctx: RequestContext<ServerContext>,
        body: TypedBody<PutRssRecoveryUserPasswordHash>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ctx = rqctx.context();

        let mut config = ctx.rss_config.lock().unwrap();
        config.set_recovery_user_password_hash(body.into_inner().hash);

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn delete_rss_config(
        rqctx: RequestContext<ServerContext>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ctx = rqctx.context();

        let mut config = ctx.rss_config.lock().unwrap();
        *config = Default::default();

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn get_rack_setup_state(
        rqctx: RequestContext<ServerContext>,
    ) -> Result<HttpResponseOk<RackOperationStatus>, HttpError> {
        let ctx = rqctx.context();

        let sled_agent_addr = ctx.bootstrap_agent_addr().map_err(|err| {
            HttpError::for_bad_request(None, format!("{err:#}"))
        })?;

        let client = bootstrap_agent_client::Client::new(
            &format!("http://{}", sled_agent_addr),
            ctx.log.new(slog::o!("component" => "bootstrap client")),
        );

        let op_status = client
            .rack_initialization_status()
            .await
            .map_err(|err| {
                use bootstrap_agent_client::Error as BaError;
                match err {
                    BaError::CommunicationError(err) => {
                        let message =
                            format!("Failed to send rack setup request: {err}");
                        HttpError {
                            status_code:
                                dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
                            error_code: None,
                            external_message: message.clone(),
                            internal_message: message,
                            headers: None,
                        }
                    }
                    other => HttpError::for_bad_request(
                        None,
                        format!("Rack setup request failed: {other}"),
                    ),
                }
            })?
            .into_inner();

        Ok(HttpResponseOk(op_status))
    }

    async fn post_run_rack_setup(
        rqctx: RequestContext<ServerContext>,
    ) -> Result<HttpResponseOk<RackInitUuid>, HttpError> {
        let ctx = rqctx.context();
        let log = &rqctx.log;

        let sled_agent_addr = ctx.bootstrap_agent_addr().map_err(|err| {
            HttpError::for_bad_request(None, format!("{err:#}"))
        })?;

        let request = {
            let mut config = ctx.rss_config.lock().unwrap();
            config.start_rss_request(&ctx.bootstrap_peers, log).map_err(
                |err| HttpError::for_bad_request(None, format!("{err:#}")),
            )?
        };

        slog::info!(
            ctx.log,
            "Sending RSS initialize request to {}",
            sled_agent_addr
        );
        let client = bootstrap_agent_client::Client::new(
            &format!("http://{}", sled_agent_addr),
            ctx.log.new(slog::o!("component" => "bootstrap client")),
        );

        let init_id = client
            .rack_initialize(&request)
            .await
            .map_err(|err| {
                use bootstrap_agent_client::Error as BaError;
                match err {
                    BaError::CommunicationError(err) => {
                        let message =
                            format!("Failed to send rack setup request: {err}");
                        HttpError {
                            status_code:
                                dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
                            error_code: None,
                            external_message: message.clone(),
                            internal_message: message,
                            headers: None,
                        }
                    }
                    other => HttpError::for_bad_request(
                        None,
                        format!("Rack setup request failed: {other}"),
                    ),
                }
            })?
            .into_inner();

        Ok(HttpResponseOk(init_id))
    }

    async fn post_run_rack_reset(
        rqctx: RequestContext<ServerContext>,
    ) -> Result<HttpResponseOk<RackResetUuid>, HttpError> {
        let ctx = rqctx.context();

        let sled_agent_addr = ctx.bootstrap_agent_addr().map_err(|err| {
            HttpError::for_bad_request(None, format!("{err:#}"))
        })?;

        slog::info!(
            ctx.log,
            "Sending RSS reset request to {}",
            sled_agent_addr
        );
        let client = bootstrap_agent_client::Client::new(
            &format!("http://{}", sled_agent_addr),
            ctx.log.new(slog::o!("component" => "bootstrap client")),
        );

        let reset_id = client
            .rack_reset()
            .await
            .map_err(|err| {
                use bootstrap_agent_client::Error as BaError;
                match err {
                    BaError::CommunicationError(err) => {
                        let message =
                            format!("Failed to send rack reset request: {err}");
                        HttpError {
                            status_code:
                                dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
                            error_code: None,
                            external_message: message.clone(),
                            internal_message: message,
                            headers: None,
                        }
                    }
                    other => HttpError::for_bad_request(
                        None,
                        format!("Rack setup request failed: {other}"),
                    ),
                }
            })?
            .into_inner();

        Ok(HttpResponseOk(reset_id))
    }

    async fn get_inventory(
        rqctx: RequestContext<ServerContext>,
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
            Err(GetMgsInventoryError::InvalidSpIdentifier) => {
                return Err(HttpError::for_unavail(
                    None,
                    "Invalid SP identifier in request".into(),
                ));
            }
            Err(GetMgsInventoryError::ShutdownInProgress) => {
                return Err(HttpError::for_unavail(
                    None,
                    "Server is shutting down".into(),
                ));
            }
        };

        // Fetch the transceiver information from the SP.
        let maybe_transceiver_inventory =
            match rqctx.context().transceiver_handle.get_transceivers() {
                GetTransceiversResponse::Response {
                    transceivers,
                    transceivers_last_seen,
                } => Some((transceivers, transceivers_last_seen)),
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
        rqctx: RequestContext<ServerContext>,
        body: StreamingBody,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let rqctx = rqctx.context();

        rqctx.update_tracker.put_repository(body.into_stream()).await?;

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn get_artifacts_and_event_reports(
        rqctx: RequestContext<ServerContext>,
    ) -> Result<HttpResponseOk<GetArtifactsAndEventReportsResponse>, HttpError>
    {
        let response =
            rqctx.context().update_tracker.artifacts_and_event_reports().await;
        Ok(HttpResponseOk(response))
    }

    async fn get_baseboard(
        rqctx: RequestContext<ServerContext>,
    ) -> Result<HttpResponseOk<GetBaseboardResponse>, HttpError> {
        let rqctx = rqctx.context();
        Ok(HttpResponseOk(GetBaseboardResponse {
            baseboard: rqctx.baseboard.clone(),
        }))
    }

    async fn get_location(
        rqctx: RequestContext<ServerContext>,
    ) -> Result<HttpResponseOk<GetLocationResponse>, HttpError> {
        let rqctx = rqctx.context();
        let inventory = mgs_inventory_or_unavail(
            &rqctx.mgs_handle,
            &rqctx.transceiver_handle,
        )
        .await?;

        let switch_id = rqctx.local_switch_id().await;
        let sled_baseboard = rqctx.baseboard.clone();

        let mut switch_baseboard = None;
        let mut sled_id = None;

        // Safety: `inventory_or_unavail` returns an error if there is no
        // MGS-derived inventory, so option is always `Some(_)`.
        for sp in &inventory
            .mgs
            .expect("checked by `inventory_or_unavail`")
            .inventory
            .sps
        {
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
        rqctx: RequestContext<ServerContext>,
        params: TypedBody<StartUpdateParams>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let log = &rqctx.log;
        let rqctx = rqctx.context();
        let params = params.into_inner();

        if params.targets.is_empty() {
            return Err(HttpError::for_bad_request(
                None,
                "No update targets specified".into(),
            ));
        }

        // Can we update the target SPs? We refuse to update if, for any target SP:
        //
        // 1. We haven't pulled its state in our inventory (most likely cause: the
        //    cubby is empty; less likely cause: the SP is misbehaving, which will
        //    make updating it very unlikely to work anyway)
        // 2. We have pulled its state but our hardware manager says we can't
        //    update it (most likely cause: the target is the sled we're currently
        //    running on; less likely cause: our hardware manager failed to get our
        //    local identifying information, and it refuses to update this target
        //    out of an abundance of caution).
        //
        // First, get our most-recently-cached inventory view. (Only wait 80% of
        // WICKETD_TIMEOUT for this: if even a cached inventory isn't available,
        // it's because we've never established contact with MGS. In that case, we
        // should produce a useful error message rather than timing out on the
        // client.)
        let inventory = match tokio::time::timeout(
            WICKETD_TIMEOUT.mul_f32(0.8),
            rqctx.mgs_handle.get_cached_inventory(),
        )
        .await
        {
            Ok(Ok(inventory)) => inventory,
            Ok(Err(ShutdownInProgress)) => {
                return Err(HttpError::for_unavail(
                    None,
                    "Server is shutting down".into(),
                ));
            }
            Err(_) => {
                // Have to construct an HttpError manually because
                // HttpError::for_unavail doesn't accept an external message.
                let message =
                    "Rack inventory not yet available (is MGS alive?)"
                        .to_owned();
                return Err(HttpError {
                    status_code: dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
                    error_code: None,
                    external_message: message.clone(),
                    internal_message: message,
                    headers: None,
                });
            }
        };

        // Error cases.
        let mut inventory_absent = BTreeSet::new();
        let mut self_update = None;
        let mut maybe_self_update = BTreeSet::new();

        // Next, do we have the states of the target SP?
        let sp_states = match inventory {
            GetMgsInventoryResponse::Response { inventory, .. } => inventory
                .sps
                .into_iter()
                .filter_map(|sp| {
                    if params.targets.contains(&sp.id) {
                        if let Some(sp_state) = sp.state {
                            Some((sp.id, sp_state))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect(),
            GetMgsInventoryResponse::Unavailable => BTreeMap::new(),
        };

        for target in &params.targets {
            let sp_state = match sp_states.get(target) {
                Some(sp_state) => sp_state,
                None => {
                    // The state isn't present, so add to inventory_absent.
                    inventory_absent.insert(*target);
                    continue;
                }
            };

            // If we have the state of the SP, are we allowed to update it? We
            // refuse to try to update our own sled.
            match &rqctx.baseboard {
                Some(baseboard) => {
                    if baseboard.identifier() == sp_state.serial_number
                        && baseboard.model() == sp_state.model
                        && baseboard.revision() == sp_state.revision
                    {
                        self_update = Some(*target);
                        continue;
                    }
                }
                None => {
                    // We don't know our own baseboard, which is a very questionable
                    // state to be in! For now, we will hard-code the possibly
                    // locations where we could be running: scrimlets can only be in
                    // cubbies 14 or 16, so we refuse to update either of those.
                    let target_is_scrimlet = matches!(
                        (target.type_, target.slot),
                        (SpType::Sled, 14 | 16)
                    );
                    if target_is_scrimlet {
                        maybe_self_update.insert(*target);
                        continue;
                    }
                }
            }
        }

        // Do we have any errors?
        let mut errors = Vec::new();
        if !inventory_absent.is_empty() {
            errors.push(format!(
                "cannot update sleds (no inventory state present for {})",
                sps_to_string(&inventory_absent)
            ));
        }
        if let Some(self_update) = self_update {
            errors.push(format!(
                "cannot update sled where wicketd is running ({})",
                SpIdentifierDisplay(self_update)
            ));
        }
        if !maybe_self_update.is_empty() {
            errors.push(format!(
                "wicketd does not know its own baseboard details: \
             refusing to update either scrimlet ({})",
                sps_to_string(&inventory_absent)
            ));
        }

        if let Some(test_error) = &params.options.test_error {
            errors.push(
                test_error.into_error_string(log, "starting update").await,
            );
        }

        let start_update_errors = if errors.is_empty() {
            // No errors: we can try and proceed with this update.
            match rqctx
                .update_tracker
                .start(params.targets, params.options)
                .await
            {
                Ok(()) => return Ok(HttpResponseUpdatedNoContent {}),
                Err(errors) => errors,
            }
        } else {
            // We've already found errors, so all we want to do is to check whether
            // the update tracker thinks there are any errors as well.
            match rqctx.update_tracker.update_pre_checks(params.targets).await {
                Ok(()) => Vec::new(),
                Err(errors) => errors,
            }
        };

        errors
            .extend(start_update_errors.iter().map(|error| error.to_string()));

        // If we get here, we have errors to report.

        match errors.len() {
            0 => {
                unreachable!(
                    "we already returned Ok(_) above if there were no errors"
                )
            }
            1 => {
                return Err(HttpError::for_bad_request(
                    None,
                    errors.pop().unwrap(),
                ));
            }
            _ => {
                return Err(HttpError::for_bad_request(
                    None,
                    format!(
                        "multiple errors encountered:\n - {}",
                        itertools::join(errors, "\n - ")
                    ),
                ));
            }
        }
    }

    async fn get_update_sp(
        rqctx: RequestContext<ServerContext>,
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
        rqctx: RequestContext<ServerContext>,
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
        rqctx: RequestContext<ServerContext>,
        params: TypedBody<ClearUpdateStateParams>,
    ) -> Result<HttpResponseOk<ClearUpdateStateResponse>, HttpError> {
        let log = &rqctx.log;
        let rqctx = rqctx.context();
        let params = params.into_inner();

        if params.targets.is_empty() {
            return Err(HttpError::for_bad_request(
                None,
                "No targets specified".into(),
            ));
        }

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
        rqctx: RequestContext<ServerContext>,
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
        rqctx: RequestContext<ServerContext>,
        body: TypedBody<PreflightUplinkCheckOptions>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let rqctx = rqctx.context();
        let options = body.into_inner();

        let our_switch_location = match rqctx.local_switch_id().await {
            Some(SpIdentifier { slot, type_: SpType::Switch }) => match slot {
                0 => SwitchLocation::Switch0,
                1 => SwitchLocation::Switch1,
                _ => {
                    return Err(HttpError::for_internal_error(format!(
                        "unexpected switch slot {slot}"
                    )));
                }
            },
            Some(other) => {
                return Err(HttpError::for_internal_error(format!(
                    "unexpected switch SP identifier {other:?}"
                )));
            }
            None => {
                return Err(HttpError::for_unavail(
                    Some("UnknownSwitchLocation".to_string()),
                    "local switch location not yet determined".to_string(),
                ));
            }
        };

        let (network_config, dns_servers, ntp_servers) = {
            let rss_config = rqctx.rss_config.lock().unwrap();

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
                our_switch_location,
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
        rqctx: RequestContext<ServerContext>,
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
        rqctx: RequestContext<ServerContext>,
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

// Get the current inventory or return a 503 Unavailable.
//
// Note that 503 is returned if we can't get the MGS-based inventory. If we fail
// to get the transceivers, that's not considered a fatal 503.
async fn mgs_inventory_or_unavail(
    mgs_handle: &MgsHandle,
    transceiver_handle: &TransceiverHandle,
) -> Result<RackV1Inventory, HttpError> {
    let mgs = match mgs_handle.get_cached_inventory().await {
        Ok(GetMgsInventoryResponse::Response { inventory, mgs_last_seen }) => {
            Some(MgsV1InventorySnapshot { inventory, last_seen: mgs_last_seen })
        }
        Ok(GetMgsInventoryResponse::Unavailable) => {
            return Err(HttpError::for_unavail(
                None,
                "Rack inventory not yet available".into(),
            ));
        }
        Err(ShutdownInProgress) => {
            return Err(HttpError::for_unavail(
                None,
                "Server is shutting down".into(),
            ));
        }
    };
    let transceivers = match transceiver_handle.get_transceivers() {
        GetTransceiversResponse::Response {
            transceivers,
            transceivers_last_seen,
        } => Some(TransceiverInventorySnapshot {
            inventory: transceivers,
            last_seen: transceivers_last_seen,
        }),
        GetTransceiversResponse::Unavailable => None,
    };
    Ok(RackV1Inventory { mgs, transceivers })
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
