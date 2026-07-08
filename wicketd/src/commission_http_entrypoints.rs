// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for the wicketd "commission" API.
//!
//! This API is a stable version of the wicketd API that is used by external
//! commissioning tools (such as `rkdeploy`). It is versioned independently of
//! the main wicketd API.

use crate::ServerContext;
use crate::context::CommonConfigContainer;
use crate::helpers::SpIdentifierDisplay;
use crate::helpers::sps_to_string;
use crate::mgs::GetInventoryError as GetMgsInventoryError;
use crate::mgs::GetInventoryResponse as GetMgsInventoryResponse;
use crate::mgs::MgsHandle;
use crate::mgs::ShutdownInProgress;
use crate::transceivers::GetTransceiversResponse;
use bootstrap_agent_lockstep_client::types::RackOperationStatus;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::RequestContext;
use dropshot::StreamingBody;
use dropshot::TypedBody;
use omicron_uuid_kinds::RackInitUuid;
use sled_hardware_types::Baseboard;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use wicket_common::WICKETD_TIMEOUT;
use wicket_common::inventory::MgsV1Inventory;
use wicket_common::inventory::MgsV1InventorySnapshot;
use wicket_common::inventory::RackV1Inventory;
use wicket_common::inventory::SpType;
use wicket_common::inventory::TransceiverInventorySnapshot;
use wicket_common::rack_setup::PutRssUserConfigInsensitive;
use wicket_common::rack_update::ClearUpdateStateResponse;
use wicketd_commission_api::WicketdCommissionApi;
use wicketd_commission_api::wicketd_commission_api_mod;
use wicketd_commission_types::artifacts::GetArtifactsAndEventReportsResponse;
use wicketd_commission_types::artifacts::InstallableArtifacts;
use wicketd_commission_types::bootstrap_sleds::BootstrapSledIp;
use wicketd_commission_types::bootstrap_sleds::BootstrapSledIps;
use wicketd_commission_types::inventory::GetInventoryParams;
use wicketd_commission_types::inventory::GetInventoryResponse;
use wicketd_commission_types::location::GetLocationResponse;
use wicketd_commission_types::rss_config::CertificateUploadResponse;
use wicketd_commission_types::rss_config::CurrentRssUserConfig;
use wicketd_commission_types::rss_config::CurrentRssUserConfigSensitive;
use wicketd_commission_types::rss_config::PutRssRecoveryUserPasswordHash;
use wicketd_commission_types::update::ClearUpdateStateParams;
use wicketd_commission_types::update::StartUpdateParams;

type CommissionApiDescription = ApiDescription<Arc<ServerContext>>;

/// Return a description of the wicketd commission API for use in generating
/// an OpenAPI spec.
pub fn api() -> CommissionApiDescription {
    wicketd_commission_api_mod::api_description::<WicketdCommissionApiImpl>()
        .expect("failed to register commission entrypoints")
}

pub enum WicketdCommissionApiImpl {}

impl WicketdCommissionApi for WicketdCommissionApiImpl {
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

        // We can't run RSS if we don't have an inventory from MGS yet; we
        // always need to fill in the bootstrap sleds first.
        let inventory = mgs_inventory_or_unavail(&ctx.mgs_handle).await?;

        let mut config = ctx.rss_or_multirack_join_config.lock().unwrap();
        let rss_config = config.rss_config_mut_or_conflict(
            "cannot get RSS config when not preparing for RSS",
        )?;

        let ddm_discovered_sleds = &ctx.bootstrap_peers.sleds();
        let config =
            rss_config.get_latest(&inventory, &ddm_discovered_sleds, &ctx.log);
        let converted: wicketd_api::CurrentRssUserConfig = config.into();

        Ok(HttpResponseOk(CurrentRssUserConfig {
            sensitive: CurrentRssUserConfigSensitive {
                num_external_certificates: converted
                    .sensitive
                    .num_external_certificates,
                recovery_silo_password_set: converted
                    .sensitive
                    .recovery_silo_password_set,
                bgp_auth_keys: converted.sensitive.bgp_auth_keys,
            },
            insensitive: converted.insensitive,
        }))
    }

    async fn put_rss_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<PutRssUserConfigInsensitive>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ctx = rqctx.context();

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

        Ok(HttpResponseOk(convert_cert_upload_response(response)))
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

        Ok(HttpResponseOk(convert_cert_upload_response(response)))
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

        let lockstep_addr =
            ctx.bootstrap_agent_lockstep_addr().map_err(|err| {
                HttpError::for_bad_request(None, format!("{err:#}"))
            })?;

        let client = bootstrap_agent_lockstep_client::Client::new(
            &format!("http://{}", lockstep_addr),
            ctx.log.new(slog::o!("component" => "bootstrap lockstep client")),
        );

        let op_status = client
            .rack_initialization_status()
            .await
            .map_err(|err| {
                use bootstrap_agent_lockstep_client::Error as BaError;
                match err {
                    BaError::CommunicationError(err) => {
                        let message = format!(
                            "Failed to send rack setup request: {}",
                            InlineErrorChain::new(&err)
                        );
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
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RackInitUuid>, HttpError> {
        let ctx = rqctx.context();
        let log = &rqctx.log;

        let lockstep_addr =
            ctx.bootstrap_agent_lockstep_addr().map_err(|err| {
                HttpError::for_bad_request(None, format!("{err:#}"))
            })?;

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
            lockstep_addr
        );
        let client = bootstrap_agent_lockstep_client::Client::new(
            &format!("http://{}", lockstep_addr),
            ctx.log.new(slog::o!("component" => "bootstrap lockstep client")),
        );

        let init_id = client
            .rack_initialize(&request)
            .await
            .map_err(|err| {
                use bootstrap_agent_lockstep_client::Error as BaError;
                match err {
                    BaError::CommunicationError(err) => {
                        let message = format!(
                            "Failed to send rack setup request: {}",
                            InlineErrorChain::new(&err)
                        );
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
        Ok(HttpResponseOk(GetArtifactsAndEventReportsResponse {
            system_version: response.system_version,
            artifacts: response
                .artifacts
                .into_iter()
                .map(|a| InstallableArtifacts {
                    artifact_id: a.artifact_id,
                    installable: a.installable,
                    sign: a.sign,
                })
                .collect(),
            event_reports: response.event_reports,
        }))
    }

    async fn get_location(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<GetLocationResponse>, HttpError> {
        let rqctx = rqctx.context();
        let inventory = mgs_inventory_or_unavail(&rqctx.mgs_handle).await?;

        let switch_id = rqctx.local_switch_id().await;
        let sled_baseboard = rqctx.baseboard.clone();

        let mut switch_baseboard = None;
        let mut sled_id = None;

        for sp in &inventory.sps {
            if Some(sp.id) == switch_id {
                switch_baseboard = sp.state.as_ref().map(|state| {
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

        if params.targets.is_empty() {
            return Err(HttpError::for_bad_request(
                None,
                "No update targets specified".into(),
            ));
        }

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

        let mut inventory_absent = BTreeSet::new();
        let mut self_update = None;
        let mut maybe_self_update = BTreeSet::new();

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
                    inventory_absent.insert(*target);
                    continue;
                }
            };

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
            match rqctx
                .update_tracker
                .start(params.targets, params.options)
                .await
            {
                Ok(()) => return Ok(HttpResponseUpdatedNoContent {}),
                Err(errors) => errors,
            }
        } else {
            match rqctx.update_tracker.update_pre_checks(params.targets).await {
                Ok(()) => Vec::new(),
                Err(errors) => errors,
            }
        };

        errors
            .extend(start_update_errors.iter().map(|error| error.to_string()));

        match errors.len() {
            0 => {
                unreachable!(
                    "we already returned Ok(_) above if there were no errors"
                )
            }
            1 => Err(HttpError::for_bad_request(None, errors.pop().unwrap())),
            _ => Err(HttpError::for_bad_request(
                None,
                format!(
                    "multiple errors encountered:\n - {}",
                    itertools::join(errors, "\n - ")
                ),
            )),
        }
    }

    async fn post_clear_update_state(
        rqctx: RequestContext<Self::Context>,
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
}

// Get the current inventory or return a 503 Unavailable.
async fn mgs_inventory_or_unavail(
    mgs_handle: &MgsHandle,
) -> Result<MgsV1Inventory, HttpError> {
    match mgs_handle.get_cached_inventory().await {
        Ok(GetMgsInventoryResponse::Response { inventory, .. }) => {
            Ok(inventory)
        }
        Ok(GetMgsInventoryResponse::Unavailable) => {
            Err(HttpError::for_unavail(
                None,
                "Rack inventory not yet available".into(),
            ))
        }
        Err(ShutdownInProgress) => {
            Err(HttpError::for_unavail(None, "Server is shutting down".into()))
        }
    }
}

fn convert_cert_upload_response(
    resp: wicketd_api::CertificateUploadResponse,
) -> CertificateUploadResponse {
    match resp {
        wicketd_api::CertificateUploadResponse::WaitingOnCert => {
            CertificateUploadResponse::WaitingOnCert
        }
        wicketd_api::CertificateUploadResponse::WaitingOnKey => {
            CertificateUploadResponse::WaitingOnKey
        }
        wicketd_api::CertificateUploadResponse::CertKeyAccepted => {
            CertificateUploadResponse::CertKeyAccepted
        }
        wicketd_api::CertificateUploadResponse::CertKeyDuplicateIgnored => {
            CertificateUploadResponse::CertKeyDuplicateIgnored
        }
    }
}
