// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use super::CursorExt;
use super::InnerCommand;
use super::Result;
use crate::communicator::ResponseKindExt;
use crate::error::UpdateError;
use crate::hubris_archive::HubrisArchive;
use gateway_messages::ComponentUpdatePrepare;
use gateway_messages::RequestKind;
use gateway_messages::SpComponent;
use gateway_messages::SpUpdatePrepare;
use gateway_messages::UpdateChunk;
use gateway_messages::UpdateId;
use gateway_messages::UpdateStatus;
use slog::debug;
use slog::error;
use slog::info;
use slog::Logger;
use std::convert::TryInto;
use std::io::Cursor;
use std::time::Duration;
use tlvc::TlvcReader;
use tokio::sync::mpsc;
use uuid::Uuid;

/// Start an update to the SP itself.
///
/// If the SP acks that the update can begin, spawns a task to deliver the
/// update.
pub(super) async fn start_sp_update(
    cmds_tx: &mpsc::Sender<InnerCommand>,
    update_id: Uuid,
    image: Vec<u8>,
    log: &Logger,
) -> Result<(), UpdateError> {
    let mut archive = HubrisArchive::new(image)?;

    let sp_image = archive.final_bin()?;
    let sp_image_size =
        sp_image.len().try_into().map_err(|_err| UpdateError::ImageTooLarge)?;

    let aux_image = match archive.aux_image() {
        Ok(aux_image) => Some(aux_image),
        Err(UpdateError::SpUpdateFileNotFound { .. }) => None,
        Err(err) => return Err(err),
    };

    let (aux_flash_size, aux_flash_chck) = match &aux_image {
        Some(data) => {
            let size = data
                .len()
                .try_into()
                .map_err(|_err| UpdateError::ImageTooLarge)?;
            let chck = read_auxi_check_from_tlvc(data)?;
            (size, chck)
        }
        None => (0, [0; 32]),
    };

    info!(
        log, "starting SP update";
        "id" => %update_id,
        "aux_flash_chck" => ?aux_flash_chck,
        "aux_flash_size" => aux_flash_size,
        "sp_image_size" => sp_image_size,
    );
    super::rpc(
        cmds_tx,
        RequestKind::SpUpdatePrepare(SpUpdatePrepare {
            id: update_id.into(),
            aux_flash_size,
            aux_flash_chck,
            sp_image_size,
        }),
        None,
    )
    .await
    .result
    .and_then(|(_peer, response)| {
        response.expect_sp_update_prepare_ack().map_err(Into::into)
    })?;

    tokio::spawn(drive_sp_update(
        cmds_tx.clone(),
        update_id,
        aux_image,
        sp_image,
        log.clone(),
    ));

    Ok(())
}

/// Function that should be `tokio::spawn`'d to drive an SP update to
/// completion.
async fn drive_sp_update(
    cmds_tx: mpsc::Sender<InnerCommand>,
    update_id: Uuid,
    aux_image: Option<Vec<u8>>,
    sp_image: Vec<u8>,
    log: Logger,
) {
    let id = update_id.into();

    // Wait until the SP has finished preparing for this update.
    let sp_matched_chck = match poll_until_update_prep_complete(
        &cmds_tx,
        SpComponent::SP_ITSELF,
        id,
        aux_image.is_some(),
        &log,
    )
    .await
    {
        Ok(sp_matched_chck) => {
            info!(
                log, "update preparation complete";
                "update_id" => %update_id,
            );
            sp_matched_chck
        }
        Err(message) => {
            error!(
                log, "update preparation failed";
                "err" => message,
                "update_id" => %update_id,
            );
            return;
        }
    };

    // Send the aux flash image, if necessary.
    if !sp_matched_chck {
        // `poll_until_update_prep_complete` can only return `Ok(false)` if we
        // told it we had an aux flash update (i.e., if `aux_image.is_some()`).
        // Therefore, we can safely unwrap here.
        let data = aux_image.unwrap();
        match send_update_in_chunks(
            &cmds_tx,
            SpComponent::SP_AUX_FLASH,
            update_id,
            data,
            &log,
        )
        .await
        {
            Ok(()) => {
                info!(log, "aux flash update complete"; "id" => %update_id);
            }
            Err(err) => {
                error!(
                    log, "aux flash update failed";
                    "id" => %update_id,
                    "err" => %err,
                );
                return;
            }
        }
    }

    // Deliver the SP image.
    match send_update_in_chunks(
        &cmds_tx,
        SpComponent::SP_ITSELF,
        update_id,
        sp_image,
        &log,
    )
    .await
    {
        Ok(()) => {
            info!(log, "update complete"; "id" => %update_id);
        }
        Err(err) => {
            error!(
                log, "update failed";
                "id" => %update_id,
                "err" => %err,
            );
        }
    }
}

fn read_auxi_check_from_tlvc(data: &[u8]) -> Result<[u8; 32], UpdateError> {
    let mut reader = TlvcReader::begin(data).map_err(UpdateError::TlvcError)?;
    let mut chck = None;

    while let Some(chunk) = reader.next().map_err(UpdateError::TlvcError)? {
        if chunk.header().tag != *b"CHCK" {
            // We could recompute the hash on AUXI and make sure it
            // matches, but the SP has to do that itself anyway. We don't expect
            // them to be mismatched more or less ever, so we won't bother
            // checking here and will just let the SP do it.
            continue;
        }
        if chunk.len() != 32 {
            return Err(UpdateError::CorruptTlvc(format!(
                "expected 32-long chck, got {}",
                chunk.len()
            )));
        }
        if chck.is_some() {
            return Err(UpdateError::CorruptTlvc(
                "multiple CHCK entries".to_string(),
            ));
        }

        let mut data = [0; 32];
        chunk.read_exact(0, &mut data[..]).map_err(UpdateError::TlvcError)?;
        chck = Some(data);
    }

    chck.ok_or_else(|| {
        UpdateError::CorruptTlvc("missing CHCK entry".to_string())
    })
}

/// Start an update to a component of the SP.
///
/// If the SP acks that the update can begin, spawns a task to deliver the
/// update.
pub(super) async fn start_component_update(
    cmds_tx: &mpsc::Sender<InnerCommand>,
    component: SpComponent,
    update_id: Uuid,
    slot: u16,
    image: Vec<u8>,
    log: &Logger,
) -> Result<(), UpdateError> {
    let total_size =
        image.len().try_into().map_err(|_err| UpdateError::ImageTooLarge)?;

    info!(
        log, "starting update";
        "component" => component.as_str(),
        "id" => %update_id,
        "total_size" => total_size,
    );
    super::rpc(
        cmds_tx,
        RequestKind::ComponentUpdatePrepare(ComponentUpdatePrepare {
            component,
            id: update_id.into(),
            slot,
            total_size,
        }),
        None,
    )
    .await
    .result
    .and_then(|(_peer, response)| {
        response.expect_component_update_prepare_ack().map_err(Into::into)
    })?;

    tokio::spawn(drive_component_update(
        cmds_tx.clone(),
        component,
        update_id,
        image,
        log.clone(),
    ));

    Ok(())
}

/// Function that should be `tokio::spawn`'d to drive a component update to
/// completion.
async fn drive_component_update(
    cmds_tx: mpsc::Sender<InnerCommand>,
    component: SpComponent,
    update_id: Uuid,
    image: Vec<u8>,
    log: Logger,
) {
    let id = update_id.into();

    // Wait until the SP has finished preparing for this update.
    match poll_until_update_prep_complete(&cmds_tx, component, id, false, &log)
        .await
    {
        Ok(_) => {
            info!(
                log, "update preparation complete";
                "update_id" => %update_id,
            );
        }
        Err(message) => {
            error!(
                log, "update preparation failed";
                "err" => message,
                "update_id" => %update_id,
            );
            return;
        }
    }

    // Deliver the update in chunks.
    match send_update_in_chunks(&cmds_tx, component, update_id, image, &log)
        .await
    {
        Ok(()) => {
            info!(log, "update complete"; "id" => %update_id);
        }
        Err(err) => {
            error!(
                log, "update failed";
                "id" => %update_id,
                "err" => %err,
            );
        }
    }
}

/// Poll an SP until it indicates that preparation for update identified by `id`
/// has completed.
///
/// If `update_has_aux_image` is `true` (i.e., the update we're waiting on has
/// is an SP update with an aux flash image), we poll until we see the
/// `SpUpdateAuxFlashChckScan` status from the SP, and then return `true` or
/// `false` indicating whether the SP found a matching CHCK (i.e., returning
/// `Ok(true)` means the SP found a matching CHCK, and we don't need to send the
/// aux flash image). Receiving an `InProgress` status will result in an error
/// being returned, as we don't expect to see that state until we start sending
/// data.
///
/// If `update_has_aux_image` is `false`, we poll until we see the `InProgress`
/// status from the SP. Receiving an `SpUpdateAuxFlashChckScan` status will
/// result in an error being returned. We always return `Ok(true)` upon seeing
/// `InProgress` (i.e., if `update_has_aux_image` is `false`, we will either
/// return `Ok(true)` or an error, never `Ok(false)`).
async fn poll_until_update_prep_complete(
    cmds_tx: &mpsc::Sender<InnerCommand>,
    component: SpComponent,
    id: UpdateId,
    update_has_aux_image: bool,
    log: &Logger,
) -> Result<bool, String> {
    // The choice of interval is relatively arbitrary; we expect update
    // preparation to generally fall in one of two cases:
    //
    // 1. No prep is necessary, and the update can happen immediately
    //    (we'll never sleep)
    // 2. Prep is relatively slow (e.g., erasing a flash part)
    //
    // We choose a few seconds assuming this polling interval is
    // primarily hit when the SP is doing something slow.
    const POLL_UPDATE_STATUS_INTERVAL: Duration = Duration::from_secs(2);

    // Poll SP until update preparation is complete.
    loop {
        // Get update status from the SP or give up.
        let status = match update_status(cmds_tx, component).await {
            Ok(status) => status,
            Err(err) => {
                return Err(format!("could not get status from SP: {err}"));
            }
        };

        // Either sleep and retry (if still preparing), break out of our
        // loop (if prep complete), or fail (anything else).
        match status {
            UpdateStatus::Preparing(sub_status) => {
                if sub_status.id == id {
                    debug!(
                        log,
                        "SP still preparing; sleeping for {:?}",
                        POLL_UPDATE_STATUS_INTERVAL
                    );
                    tokio::time::sleep(POLL_UPDATE_STATUS_INTERVAL).await;
                    continue;
                }
                // Else: fall through to returning an error.
            }
            UpdateStatus::InProgress(sub_status) => {
                if sub_status.id == id && !update_has_aux_image {
                    return Ok(true);
                }
                // Else: fall through to returning an error.
            }
            UpdateStatus::SpUpdateAuxFlashChckScan {
                id: sp_id,
                found_match,
                ..
            } => {
                if sp_id == id && update_has_aux_image {
                    return Ok(found_match);
                }
                // Else: fall through to returning an error.
            }
            UpdateStatus::None
            | UpdateStatus::Complete(_)
            | UpdateStatus::Failed { .. }
            | UpdateStatus::Aborted(_) => {
                // Fall through to returning an error below.
            }
        }

        return Err(format!("update preparation failed; status = {status:?}"));
    }
}

/// Get the status of any update being applied to the given component.
pub(super) async fn update_status(
    cmds_tx: &mpsc::Sender<InnerCommand>,
    component: SpComponent,
) -> Result<UpdateStatus> {
    super::rpc(cmds_tx, RequestKind::UpdateStatus(component), None)
        .await
        .result
        .and_then(|(_peer, response)| {
            response.expect_update_status().map_err(Into::into)
        })
}

/// Send an update image to the SP in chunks.
async fn send_update_in_chunks(
    cmds_tx: &mpsc::Sender<InnerCommand>,
    component: SpComponent,
    update_id: Uuid,
    data: Vec<u8>,
    log: &Logger,
) -> Result<()> {
    let mut image = Cursor::new(data);
    let mut offset = 0;
    let id = update_id.into();
    while !CursorExt::is_empty(&image) {
        let prior_pos = image.position();
        debug!(
            log, "sending update chunk";
            "id" => %update_id,
            "offset" => offset,
        );

        image =
            send_single_update_chunk(&cmds_tx, component, id, offset, image)
                .await?;

        // Update our offset according to how far our cursor advanced.
        offset += (image.position() - prior_pos) as u32;
    }
    Ok(())
}

/// Send a portion of an update to the SP.
///
/// `data` is moved into this function, updated based on the amount delivered in
/// this chunk, and returned.
async fn send_single_update_chunk(
    cmds_tx: &mpsc::Sender<InnerCommand>,
    component: SpComponent,
    id: UpdateId,
    offset: u32,
    data: Cursor<Vec<u8>>,
) -> Result<Cursor<Vec<u8>>> {
    let update_chunk = UpdateChunk { component, id, offset };
    let (result, data) = super::rpc_with_trailing_data(
        cmds_tx,
        RequestKind::UpdateChunk(update_chunk),
        data,
    )
    .await;

    result.and_then(|(_peer, response)| {
        response.expect_update_chunk_ack().map_err(Into::into)
    })?;

    Ok(data)
}
