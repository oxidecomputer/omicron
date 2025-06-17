// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module containing types for updating RoT Bootloaders via MGS.

use super::MgsClients;
use crate::SpComponentUpdateHelper;
use crate::common_sp_update::FoundVersion;
use crate::common_sp_update::PostUpdateError;
use crate::common_sp_update::PrecheckError;
use crate::common_sp_update::PrecheckStatus;
use crate::common_sp_update::error_means_caboose_is_invalid;
use futures::FutureExt;
use futures::future::BoxFuture;
use gateway_client::SpComponent;
use gateway_client::types::GetRotBootInfoParams;
use gateway_client::types::RotImageError;
use gateway_client::types::RotState;
use gateway_client::types::SpComponentFirmwareSlot;
use gateway_client::types::SpType;
use gateway_messages::RotBootInfo;
use nexus_types::deployment::ExpectedVersion;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateDetails;
use slog::Logger;
use slog::{debug, error, info};
use std::time::Duration;
use std::time::Instant;

type GatewayClientError = gateway_client::Error<gateway_client::types::Error>;

pub struct ReconfiguratorRotBootloaderUpdater;
impl SpComponentUpdateHelper for ReconfiguratorRotBootloaderUpdater {
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

            // TODO-K: In the RoT bootloader update code in wicket, there is a set of
            // known bootloader FWIDs that don't have cabooses. Is this something we
            // should care about here?
            // https://github.com/oxidecomputer/omicron/blob/89ce370f0a96165c777e90a008257a6085897f2a/wicketd/src/update_tracker.rs#L1817-L1841

            // TODO-K: There are also older versions of the SP have a bug that prevents
            // setting the active slot for the RoT bootloader. Is this something we should
            // care about here?
            // https://github.com/oxidecomputer/omicron/blob/89ce370f0a96165c777e90a008257a6085897f2a/wicketd/src/update_tracker.rs#L1705-L1710

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

            // If the version in the currently active slot matches the one we're
            // trying to set, then there's nothing to do.
            if caboose.version == update.artifact_version.as_str() {
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
                    "pending MGS update details within ReconfiguratorSpUpdater \
                    will always be for the RoT bootloader"
                );
            };
            if caboose.version != expected_stage0_version.to_string() {
                return Err(PrecheckError::WrongActiveVersion {
                    expected: expected_stage0_version.clone(),
                    found: caboose.version,
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
                            // The name for the SP component here is STAGE0
                            // it's a little confusing because we're really
                            // trying to reach STAGE0NEXT, and there is no
                            // ROT_BOOTLOADER variant. We specify that we
                            // want STAGE0NEXT by setting the firmware slot
                            // to 1, which is where it will always be.
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
            match (&expected_stage0_next_version, &found_stage0_next_version) {
                // expected garbage, found garbage
                (
                    ExpectedVersion::NoValidVersion,
                    FoundVersion::MissingVersion,
                ) => (),
                // expected a specific version and found it
                (
                    ExpectedVersion::Version(artifact_version),
                    FoundVersion::Version(found_stage0_next_version),
                ) if artifact_version.to_string()
                    == *found_stage0_next_version =>
                {
                    ()
                }
                // anything else is a mismatch
                (ExpectedVersion::NoValidVersion, FoundVersion::Version(_))
                | (ExpectedVersion::Version(_), FoundVersion::MissingVersion)
                | (ExpectedVersion::Version(_), FoundVersion::Version(_)) => {
                    return Err(PrecheckError::WrongInactiveVersion {
                        expected: expected_stage0_next_version.clone(),
                        found: found_stage0_next_version,
                    });
                }
            };

            // TODO-K: In post_update we'll be restarting the RoT twice to do signature
            // checks, and to set stage0 to the new version. What happens if the RoT
            // itself is being updated (during the reset stage)? Should we check for that
            // here before setting the RoT bootloader as ready to update?

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
        const WAIT_FOR_BOOT_TIMEOUT: Duration = Duration::from_secs(30);

        // TODO-K: Again, we're resetting the ROT twice here, what happens
        // if an RoT update is happening at the same time?

        async move {
            // Before setting stage0 to the new version we want to ensure
            // the image is good and we're not going to brick the device.
            // The RoT will do a signature check when reset.
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
                WAIT_FOR_BOOT_TIMEOUT,
            )
            .await?;
            // If the image is not valid we bail
            if let Some(error) = stage0next_error {
                return Err(PostUpdateError::RotBootloaderImageError {
                    error,
                });
            }

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
                    Ok(())
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
                        .await?;
                    Ok(())
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
    sp_slot: u32,
    timeout: Duration,
    // TODO-K: Change to PostUpdateError?
) -> Result<Option<RotImageError>, GatewayClientError> {
    let mut ticker = tokio::time::interval(Duration::from_secs(1));

    let start = Instant::now();
    loop {
        ticker.tick().await;

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
                // The minimum we will ever return is 3.
                // Additionally, V2 does not report image errors, so we cannot
                // know with certainty if a signature check came back with errors
                RotState::V2 { .. } => unreachable!(),
                RotState::V3 { stage0next_error, .. } => {
                    return Ok(stage0next_error);
                }
                // The RoT is probably still booting
                RotState::CommunicationFailed { message } => {
                    if start.elapsed() < timeout {
                        info!(
                            log,
                            "failed getting RoT boot info (will retry)";
                            "error" => %message,
                        );
                    } else {
                        error!(
                            log,
                            "failed to get RoT boot info";
                            "error" => %message,
                        );
                        // TODO-K: change this one to RotCommunicationFailed?
                        return Ok(Some(RotImageError::Unchecked));
                    }
                }
            },
            Err(error) => {
                if start.elapsed() < timeout {
                    info!(
                        log,
                        "failed getting RoT boot info (will retry)";
                        "error" => %error,
                    );
                } else {
                    return Err(error);
                }
            }
        }
    }
}
