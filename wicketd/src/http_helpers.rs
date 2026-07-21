// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper and utility code for the wicketd HTTP APIs.
//!
//! These helpers are shared by both the unstable wicketd API (defined in
//! `http_entrypoints.rs`) and the stable commission API (defined in the
//! `commission` module).

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use dropshot::ErrorStatusCode;
use dropshot::HttpError;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use wicket_common::WICKETD_TIMEOUT;
use wicket_common::inventory::MgsV1Inventory;
use wicket_common::inventory::SpType;
use wicket_common::rack_update::StartUpdateOptions;
use wicketd_commission_types::update::UpdateTargets;

use crate::ServerContext;
use crate::helpers::SpIdentifierDisplay;
use crate::helpers::sps_to_string;
use crate::mgs::GetInventoryError;
use crate::mgs::GetInventoryResponse;
use crate::mgs::MgsHandle;
use crate::mgs::ShutdownInProgress;

// Get the current inventory or return a 503 Unavailable.
//
// Note that 503 is returned if we can't get the MGS-based inventory. If we fail
// to get the transceivers, that's not considered a fatal 503.
pub(crate) async fn mgs_inventory_or_unavail(
    mgs_handle: &MgsHandle,
) -> Result<MgsV1Inventory, HttpError> {
    match mgs_handle.get_cached_inventory().await {
        Ok(GetInventoryResponse::Response { inventory, .. }) => Ok(inventory),
        Ok(GetInventoryResponse::Unavailable) => Err(inventory_unavailable()),
        Err(err @ ShutdownInProgress) => Err(shutdown_to_http(err)),
    }
}

pub(crate) fn inventory_unavailable() -> HttpError {
    http_error_with_message(
        ErrorStatusCode::SERVICE_UNAVAILABLE,
        None,
        "Rack inventory not yet available".to_owned(),
    )
}

pub(crate) fn shutdown_to_http(_err: ShutdownInProgress) -> HttpError {
    http_error_with_message(
        ErrorStatusCode::SERVICE_UNAVAILABLE,
        None,
        "Server is shutting down".to_owned(),
    )
}

pub(crate) fn inventory_err_to_http(err: GetInventoryError) -> HttpError {
    match err {
        GetInventoryError::ShutdownInProgress => {
            shutdown_to_http(ShutdownInProgress)
        }
        GetInventoryError::InvalidSpIdentifier => http_error_with_message(
            ErrorStatusCode::SERVICE_UNAVAILABLE,
            None,
            "Invalid SP identifier in request".to_owned(),
        ),
    }
}

pub(crate) fn ba_lockstep_client(
    ctx: &ServerContext,
) -> Result<bootstrap_agent_lockstep_client::Client, HttpError> {
    // The address should become known once BootstrapPeersFromDdm is populated,
    // so we treat errors as 503 Service Unavailable (which is retryable).
    let lockstep_addr = ctx.bootstrap_agent_lockstep_addr().map_err(|err| {
        http_error_with_message(
            dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
            None,
            format!("bootstrap agent address not yet known: {err:#}"),
        )
    })?;
    Ok(bootstrap_agent_lockstep_client::Client::new(
        &format!("http://{}", lockstep_addr),
        ctx.log.new(slog::o!("component" => "bootstrap lockstep client")),
    ))
}

/// Convert a [`bootstrap_agent_lockstep_client::Error`] to an [`HttpError`].
///
/// This is something we must do with some care -- clients mostly care about if
/// an error is retryable. Rather than classify statuses ourselves, we mirror
/// documented error responses verbatim: since the caller's progenitor
/// `is_retryable` is derived from the same status code, forwarding the status
/// unchanged means we don't have to do any kind of status code mapping here.
///
/// * A documented error response is forwarded mostly verbatim --
///   the status code and error code are the same, and the upstream message
///   has a small amount of context added to it. As of 2026-07, the only
///   application-level error returned by the bootstrap agent lockstep
///   calls is 409 Conflict.
/// * Communication errors are transient by nature, so they become 503 Service
///   Unavailable.
/// * All other client-side and protocol failures become 500 Internal Server
///   Error.
///
/// The full error is made available to clients as well, since this is an
/// operator-facing API and not the Nexus external API.
pub(crate) fn ba_lockstep_error_to_http(
    err: bootstrap_agent_lockstep_client::Error<
        bootstrap_agent_lockstep_client::types::Error,
    >,
    operation: &str,
) -> HttpError {
    use bootstrap_agent_lockstep_client::Error as BaError;

    let (status_code, error_code, message) = match &err {
        BaError::ErrorResponse(rv) => (
            rv.status()
                .try_into()
                .unwrap_or(ErrorStatusCode::INTERNAL_SERVER_ERROR),
            rv.error_code.clone(),
            format!(
                "{operation} request failed (upstream request id {}): {}",
                rv.request_id, rv.message
            ),
        ),
        BaError::CommunicationError(inner) => (
            dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
            None,
            format!(
                "Failed to send {operation} request: {}",
                InlineErrorChain::new(inner)
            ),
        ),
        BaError::InvalidRequest(_)
        | BaError::InvalidUpgrade(_)
        | BaError::ResponseBodyError(_)
        | BaError::InvalidResponsePayload(_, _)
        | BaError::UnexpectedResponse(_)
        | BaError::Custom(_) => (
            dropshot::ErrorStatusCode::INTERNAL_SERVER_ERROR,
            None,
            // The default error formatter for
            // `bootstrap_agent_lockstep_client::Error` (which is actually
            // `progenitor_client::Error`) does not print the error chain. Use
            // the alternate error formatter which does.
            //
            // The Progenitor alternate formatter is slightly better than
            // InlineErrorChain here, because the latter would double-print the
            // first cause due to the way Progenitor's error display works.
            format!("{operation} request failed: {err:#}"),
        ),
    };
    http_error_with_message(status_code, error_code, message)
}

/// Build an HttpError whose message is visible to the client.
///
/// This avoids using methods on `HttpError`, many of which don't expose the
/// full message to clients for security reasons.
pub(crate) fn http_error_with_message(
    status_code: dropshot::ErrorStatusCode,
    error_code: Option<String>,
    message: String,
) -> HttpError {
    HttpError {
        status_code,
        error_code,
        external_message: message.clone(),
        internal_message: message,
        headers: None,
    }
}

pub(crate) async fn start_update(
    ctx: &ServerContext,
    log: &Logger,
    targets: UpdateTargets,
    options: StartUpdateOptions,
) -> Result<(), HttpError> {
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
        ctx.mgs_handle.get_cached_inventory(),
    )
    .await
    {
        Ok(Ok(inventory)) => inventory,
        Ok(Err(err @ ShutdownInProgress)) => {
            return Err(shutdown_to_http(err));
        }
        Err(_) => {
            return Err(http_error_with_message(
                dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
                None,
                "Rack inventory not yet available (is MGS alive?)".to_owned(),
            ));
        }
    };

    // Error cases.
    let mut inventory_absent = BTreeSet::new();
    let mut self_update = None;
    let mut maybe_self_update = BTreeSet::new();

    // Next, do we have the states of the target SP?
    let sp_states: BTreeMap<_, _> = match inventory {
        GetInventoryResponse::Response { inventory, .. } => inventory
            .sps
            .into_iter()
            .filter_map(|sp| {
                if targets.contains(&sp.id) {
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
        GetInventoryResponse::Unavailable => BTreeMap::new(),
    };

    for target in &targets {
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
        match &ctx.baseboard {
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
                    (target.typ, target.slot),
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
            "wicketd does not know its own baseboard details: refusing to \
             update either scrimlet ({})",
            sps_to_string(&maybe_self_update)
        ));
    }

    if let Some(test_error) = &options.test_error {
        errors.push(test_error.into_error_string(log, "starting update").await);
    }

    let start_update_errors = if errors.is_empty() {
        // No errors: we can try and proceed with this update.
        match ctx.update_tracker.start(targets, options).await {
            Ok(()) => return Ok(()),
            Err(errors) => errors,
        }
    } else {
        // We've already found errors, so all we want to do is to check whether
        // the update tracker thinks there are any errors as well.
        match ctx.update_tracker.update_pre_checks(targets).await {
            Ok(()) => Vec::new(),
            Err(errors) => errors,
        }
    };

    errors.extend(start_update_errors.iter().map(|error| error.to_string()));

    // If we get here, we have errors to report.

    match errors.len() {
        0 => Ok(()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use bootstrap_agent_lockstep_client::Error as BaError;
    use bootstrap_agent_lockstep_client::ResponseValue;
    use bootstrap_agent_lockstep_client::types;
    use http::HeaderMap;
    use http::StatusCode;

    const DETAIL: &str = "the RSS is already initialized";
    const ERROR_CODE: &str = "TestErrorCode";
    const REQUEST_ID: &str = "test-request-id";

    fn error_response(status: StatusCode) -> BaError<types::Error> {
        BaError::ErrorResponse(ResponseValue::new(
            types::Error {
                error_code: Some(ERROR_CODE.to_string()),
                message: DETAIL.to_string(),
                request_id: REQUEST_ID.to_string(),
            },
            status,
            HeaderMap::new(),
        ))
    }

    #[test]
    fn error_responses_mirror_status_and_expose_detail() {
        // A list of upstream statuses, each of which should be mirrored
        // verbatim.
        for upstream in [
            StatusCode::BAD_REQUEST,
            StatusCode::CONFLICT,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::GATEWAY_TIMEOUT,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::NOT_IMPLEMENTED,
        ] {
            let http_error = ba_lockstep_error_to_http(
                error_response(upstream),
                "rack setup",
            );
            assert_eq!(
                http_error.status_code.as_u16(),
                upstream.as_u16(),
                "upstream {upstream} should be mirrored verbatim"
            );
            assert!(
                http_error.external_message.contains(DETAIL),
                "external message for upstream {upstream} should expose \
                 the inner detail, got: {}",
                http_error.external_message
            );
            assert!(
                http_error.external_message.contains(REQUEST_ID),
                "external message for upstream {upstream} should include \
                 the upstream request id, got: {}",
                http_error.external_message
            );
            assert_eq!(
                http_error.error_code.as_deref(),
                Some(ERROR_CODE),
                "upstream {upstream} should mirror the error code"
            );
            assert_eq!(
                http_error.internal_message, http_error.external_message,
                "upstream {upstream}: internal and external messages \
                 should match"
            );
        }
    }

    #[test]
    fn client_side_failures_become_500_with_detail() {
        for (err, detail) in [
            (
                BaError::InvalidRequest("malformed body".to_string()),
                "malformed body",
            ),
            (BaError::Custom("hook exploded".to_string()), "hook exploded"),
        ] {
            let http_error = ba_lockstep_error_to_http(err, "rack setup");
            assert_eq!(
                http_error.status_code.as_u16(),
                500,
                "{detail}: expected 500"
            );
            assert!(
                http_error.external_message.contains(detail),
                "external message should expose the detail, got: {}",
                http_error.external_message
            );
            assert_eq!(
                http_error.error_code, None,
                "{detail}: client-side failures carry no upstream error code"
            );
        }
    }
}
