// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper and utility code for the wicketd HTTP APIs.
//!
//! Currently this is only used for the main wicketd API implementation defined
//! in `http_entrypoints.rs`. In the future, the same code will be used for the
//! commission API (RFD 710).

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use dropshot::HttpError;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use wicket_common::WICKETD_TIMEOUT;
use wicket_common::inventory::MgsV1Inventory;
use wicket_common::inventory::SpIdentifier;
use wicket_common::inventory::SpType;
use wicket_common::rack_update::StartUpdateOptions;

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
    HttpError::for_unavail(None, "Rack inventory not yet available".into())
}

pub(crate) fn shutdown_to_http(_err: ShutdownInProgress) -> HttpError {
    HttpError::for_unavail(None, "Server is shutting down".into())
}

pub(crate) fn inventory_err_to_http(err: GetInventoryError) -> HttpError {
    match err {
        GetInventoryError::ShutdownInProgress => {
            shutdown_to_http(ShutdownInProgress)
        }
        GetInventoryError::InvalidSpIdentifier => HttpError::for_unavail(
            None,
            "Invalid SP identifier in request".into(),
        ),
    }
}

pub(crate) fn ba_lockstep_client(
    ctx: &ServerContext,
) -> Result<bootstrap_agent_lockstep_client::Client, HttpError> {
    let lockstep_addr = ctx
        .bootstrap_agent_lockstep_addr()
        .map_err(|err| HttpError::for_bad_request(None, format!("{err:#}")))?;
    Ok(bootstrap_agent_lockstep_client::Client::new(
        &format!("http://{}", lockstep_addr),
        ctx.log.new(slog::o!("component" => "bootstrap lockstep client")),
    ))
}

pub(crate) fn ba_lockstep_error_to_http(
    err: bootstrap_agent_lockstep_client::Error<
        bootstrap_agent_lockstep_client::types::Error,
    >,
    operation: &str,
) -> HttpError {
    use bootstrap_agent_lockstep_client::Error as BaError;
    // TODO: This isn't quite right -- errors other than communication errors
    // shouldn't be flattened down to 400s.
    match err {
        BaError::CommunicationError(err) => {
            let message = format!(
                "Failed to send {operation} request: {}",
                InlineErrorChain::new(&err)
            );
            HttpError {
                status_code: dropshot::ErrorStatusCode::SERVICE_UNAVAILABLE,
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
}

pub(crate) async fn start_update(
    ctx: &ServerContext,
    log: &Logger,
    targets: BTreeSet<SpIdentifier>,
    options: StartUpdateOptions,
) -> Result<(), HttpError> {
    if targets.is_empty() {
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
        ctx.mgs_handle.get_cached_inventory(),
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
                "Rack inventory not yet available (is MGS alive?)".to_owned();
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
            sps_to_string(&inventory_absent)
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
