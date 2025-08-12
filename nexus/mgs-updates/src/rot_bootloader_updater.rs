// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module containing types for updating RoT Bootloaders via MGS.

use super::MgsClients;
use crate::SpComponentUpdateHelperImpl;
use crate::common_sp_update::FoundVersion;
use crate::common_sp_update::PostUpdateError;
use crate::common_sp_update::PrecheckError;
use crate::common_sp_update::PrecheckStatus;
use crate::common_sp_update::error_means_caboose_is_invalid;
use crate::mgs_clients::GatewayClientError;
use futures::FutureExt;
use futures::future::BoxFuture;
use gateway_client::SpComponent;
use gateway_client::types::GetRotBootInfoParams;
use gateway_client::types::RotImageError;
use gateway_client::types::RotState;
use gateway_client::types::SpComponentFirmwareSlot;
use gateway_client::types::SpType;
use gateway_messages::RotBootInfo;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateDetails;
use slog::Logger;
use slog::{debug, error, info};
use slog_error_chain::InlineErrorChain;
use std::time::Duration;
use std::time::Instant;

const WAIT_FOR_BOOT_INFO_TIMEOUT: Duration = Duration::from_secs(120);

const WAIT_FOR_BOOT_INFO_INTERVAL: Duration = Duration::from_secs(10);

pub struct ReconfiguratorRotBootloaderUpdater;
impl SpComponentUpdateHelperImpl for ReconfiguratorRotBootloaderUpdater {
    /// Checks if the component is already updated or ready for update
    fn precheck<'a>(
        &'a self,
        log: &'a slog::Logger,
        mgs_clients: &'a mut MgsClients,
        update: &'a PendingMgsUpdate,
    ) -> BoxFuture<'a, Result<PrecheckStatus, PrecheckError>> {
        async move {
            // Verify that the device is the one we think it is.
            let state = mgs_clients
                .try_all_serially(log, move |mgs_client| async move {
                    mgs_client.sp_get(update.sp_type, update.slot_id).await
                })
                .await?
                .into_inner();
            debug!(log, "found SP state"; "state" => ?state);
            if state.model != update.baseboard_id.part_number
                || state.serial_number != update.baseboard_id.serial_number
            {
                return Err(PrecheckError::WrongDevice {
                    sp_type: update.sp_type,
                    slot_id: update.slot_id,
                    expected_part: update.baseboard_id.part_number.clone(),
                    expected_serial: update.baseboard_id.serial_number.clone(),
                    found_part: state.model,
                    found_serial: state.serial_number,
                });
            }

            // Fetch the caboose from the currently active slot (stage0).
            let caboose = mgs_clients
                .try_all_serially(log, move |mgs_client| async move {
                    mgs_client
                        .sp_component_caboose_get(
                            update.sp_type,
                            update.slot_id,
                            &SpComponent::STAGE0.to_string(),
                            0,
                        )
                        .await
                })
                .await?
                .into_inner();
            debug!(log, "found active slot caboose"; "caboose" => ?caboose);

            let found_stage0_version = caboose.version;

            // If the version in the currently active slot matches the one we're
            // trying to set, then there's nothing to do.
            if found_stage0_version == update.artifact_version.as_str() {
                return Ok(PrecheckStatus::UpdateComplete);
            }

            // Otherwise, if the version in the currently active slot does not
            // match what we expect to find, bail out.  It may be that somebody
            // else has come along and completed a subsequent update and we
            // don't want to roll that back.  (If for some reason we *do* want
            // to do this update, the planner will have to notice that what's
            // here is wrong and update the blueprint.)
            let PendingMgsUpdateDetails::RotBootloader {
                expected_stage0_version,
                expected_stage0_next_version,
            } = &update.details
            else {
                unreachable!(
                    "pending MGS update details within \
                    ReconfiguratorRotBootloaderUpdater will always be for the \
                    RoT bootloader"
                );
            };
            if found_stage0_version != expected_stage0_version.to_string() {
                return Err(PrecheckError::WrongActiveVersion {
                    expected: expected_stage0_version.clone(),
                    found: found_stage0_version,
                });
            }

            // For the same reason, check that the version in the inactive slot
            // matches what we expect to find.
            // TODO It's important for us to detect the condition that a caboose
            // is invalid because this can happen when devices are programmed
            // with a bad image.  Unfortunately, MGS currently reports this as a
            // 503.  Besides being annoying for us to look for, this causes
            // `try_all_serially()` to try the other MGS.  That's pointless
            // here, but not a big deal.
            let found_stage0_next_caboose_result = mgs_clients
                .try_all_serially(log, move |mgs_client| async move {
                    mgs_client
                        .sp_component_caboose_get(
                            update.sp_type,
                            update.slot_id,
                            // The naming here is a bit confusing because "stage0"
                            // sometimes refers to the component (RoT bootloader)
                            // and sometimes refers to the active slot for that
                            // component. Here, we're accessing the inactive slot
                            // for it. The component is still "stage0".
                            &SpComponent::STAGE0.to_string(),
                            1,
                        )
                        .await
                })
                .await;
            let found_stage0_next_version =
                match found_stage0_next_caboose_result {
                    Ok(version) => {
                        FoundVersion::Version(version.into_inner().version)
                    }
                    Err(error) => {
                        if error_means_caboose_is_invalid(&error) {
                            FoundVersion::MissingVersion
                        } else {
                            return Err(PrecheckError::from(error));
                        }
                    }
                };
            found_stage0_next_version
                .clone()
                .matches(&expected_stage0_next_version)?;

            Ok(PrecheckStatus::ReadyForUpdate)
        }
        .boxed()
    }

    /// Attempts once to perform any post-update actions (e.g., reset the
    /// device)
    fn post_update<'a>(
        &'a self,
        log: &'a slog::Logger,
        mgs_clients: &'a mut MgsClients,
        update: &'a PendingMgsUpdate,
    ) -> BoxFuture<'a, Result<(), PostUpdateError>> {
        async move {
            // To protect against bricking itself, the device will only activate
            // a new image after it's been verified. Images are only verified at
            // device boot time. Thus, we'll reset the device once to cause the
            // signature to be verified. Then we can activate the new image and
            // reset the device again.
            debug!(
                log,
                "attempting to reset device to do bootloader signature check"
            );
            mgs_clients
                .try_all_serially(log, move |mgs_client| async move {
                    mgs_client
                        .sp_component_reset(
                            update.sp_type,
                            update.slot_id,
                            &SpComponent::ROT.to_string(),
                        )
                        .await
                })
                .await?;

            // We now retrieve boot info from the RoT to verify the reset
            // has completed and signature checks done.
            debug!(
                log,
                "attempting to retrieve boot info to verify image validity"
            );
            let stage0next_error = wait_for_stage0_next_image_check(
                log,
                mgs_clients,
                update.sp_type,
                update.slot_id,
                WAIT_FOR_BOOT_INFO_TIMEOUT,
            )
            .await?;
            // If boot info contains any error with the image loaded onto
            // stage0_next, the device won't let us load this image onto stage0.
            // We return a fatal error.
            if let Some(e) = stage0next_error {
                return Err(PostUpdateError::FatalError {
                    error: InlineErrorChain::new(&e).to_string(),
                });
            }

            // This operation is very delicate. Here, we're overwriting the
            // device bootloader with the one that we've written to the
            // stage0next slot.
            // The hardware has no fallback slot for the bootloader. So if the
            // device resets or loses power while we're copying stage0next to
            // the stage0 slot, it could still become bricked.
            //
            // We've already done everything we can to mitigate this:
            //
            // - The data is already on the device, minimizing the time to copy
            //    it to where it needs to go.
            // - The image has already been verified by the device (and the
            //    device validates _that_ before starting this operation), so it
            //    won't fail at boot for that reason.
            // - The device can't be externally reset _during_ this operation
            //    because the same code responsible for processing the reset
            //    request will be busy doing the copy.
            // - We only ever update one RoT stage0 at a time in a rack, so if
            //    we brick one, only one sled would be affected (still bad).
            //
            // So we're ready to roll!
            debug!(log, "attempting to set RoT bootloader active slot");
            mgs_clients
                .try_all_serially(log, move |mgs_client| async move {
                    let persist = true;
                    mgs_client
                        .sp_component_active_slot_set(
                            update.sp_type,
                            update.slot_id,
                            &SpComponent::STAGE0.to_string(),
                            persist,
                            &SpComponentFirmwareSlot { slot: 1 },
                        )
                        .await?;
                    Ok::<_, GatewayClientError>(())
                })
                .await?;

            debug!(log, "attempting to reset device to set to new RoT bootloader version");
            mgs_clients
                .try_all_serially(log, move |mgs_client| async move {
                    mgs_client
                        .sp_component_reset(
                            update.sp_type,
                            update.slot_id,
                            &SpComponent::ROT.to_string(),
                        )
                        .await
                })
                .await?;

            Ok(())
        }
        .boxed()
    }
}

/// Poll the RoT asking for its boot information. This is used to check
/// state after RoT bootloader updates
async fn wait_for_stage0_next_image_check(
    log: &Logger,
    mgs_clients: &mut MgsClients,
    sp_type: SpType,
    sp_slot: u16,
    timeout: Duration,
) -> Result<Option<RotImageError>, PostUpdateError> {
    let before = Instant::now();
    loop {
        match mgs_clients
            .try_all_serially(log, |mgs_client| async move {
                mgs_client
                    .sp_rot_boot_info(
                        sp_type,
                        sp_slot,
                        SpComponent::ROT.const_as_str(),
                        &GetRotBootInfoParams {
                            version: RotBootInfo::HIGHEST_KNOWN_VERSION,
                        },
                    )
                    .await
            })
            .await
        {
            Ok(state) => match state.into_inner() {
                // The minimum we will ever return is v3.
                // Additionally, V2 does not report image errors, so we cannot
                // know with certainty if a signature check came back with errors
                RotState::V2 { .. } => {
                    let error = "unexpected RoT version: 2".to_string();
                    error!(
                        log,
                        "failed to get RoT boot info";
                        "error" => &error
                    );
                    return Err(PostUpdateError::FatalError { error });
                }
                RotState::V3 { stage0next_error, .. } => {
                    return Ok(stage0next_error);
                }
                // The RoT is probably still booting
                RotState::CommunicationFailed { message } => {
                    if before.elapsed() >= timeout {
                        error!(
                            log,
                            "failed to get RoT boot info";
                            "error" => %message
                        );
                        return Err(PostUpdateError::FatalError {
                            error: message,
                        });
                    }

                    info!(
                        log,
                        "failed getting RoT boot info (will retry)";
                        "error" => %message,
                    );
                    tokio::time::sleep(WAIT_FOR_BOOT_INFO_INTERVAL).await;
                }
            },
            // The RoT might still be booting
            Err(error) => {
                let e = InlineErrorChain::new(&error);
                if before.elapsed() >= timeout {
                    error!(
                        log,
                        "failed to get RoT boot info";
                        &e,
                    );
                    return Err(PostUpdateError::FatalError {
                        error: e.to_string(),
                    });
                }

                info!(
                    log,
                    "failed getting RoT boot info (will retry)";
                    e,
                );
                tokio::time::sleep(WAIT_FOR_BOOT_INFO_INTERVAL).await;
            }
        }
    }
}
