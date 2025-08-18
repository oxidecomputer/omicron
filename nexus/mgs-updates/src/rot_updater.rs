// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module containing types for updating RoTs via MGS.

use super::MgsClients;
use crate::SpComponentUpdateHelperImpl;
use crate::common_sp_update::FoundVersion;
use crate::common_sp_update::PostUpdateError;
use crate::common_sp_update::PrecheckError;
use crate::common_sp_update::PrecheckStatus;
use crate::common_sp_update::error_means_caboose_is_invalid;
use futures::FutureExt;
use futures::future::BoxFuture;
use gateway_client::SpComponent;
use gateway_client::types::RotState;
use gateway_client::types::SpComponentFirmwareSlot;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateRotDetails;
use slog::{debug, info};

type GatewayClientError = gateway_client::Error<gateway_client::types::Error>;

pub struct ReconfiguratorRotUpdater {
    details: PendingMgsUpdateRotDetails,
}

impl ReconfiguratorRotUpdater {
    pub fn new(details: PendingMgsUpdateRotDetails) -> Self {
        Self { details }
    }
}

impl SpComponentUpdateHelperImpl for ReconfiguratorRotUpdater {
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

            let PendingMgsUpdateRotDetails {
                expected_inactive_version,
                expected_active_slot,
                expected_persistent_boot_preference,
                ..
            } = &self.details;

            let (
                active, pending_persistent_boot_preference, transient_boot_preference
            ) = match &state.rot {
                RotState::V2 {
                    active,
                    pending_persistent_boot_preference,
                    transient_boot_preference,
                    ..
                }
                | RotState::V3 {
                    active,
                    pending_persistent_boot_preference,
                    transient_boot_preference,
                    ..
                } => (
                    active, pending_persistent_boot_preference, transient_boot_preference
                ),
                RotState::CommunicationFailed { message } => {
                    return Err(PrecheckError::RotCommunicationFailed { message: message.to_string() })
                },
            };

            // Fetch the caboose from the currently active slot.
            let caboose = mgs_clients
            .try_all_serially(log, move |mgs_client| async move {
                mgs_client
                    .sp_component_caboose_get(
                        update.sp_type,
                        update.slot_id,
                        SpComponent::ROT.const_as_str(),
                        active.to_u16(),
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

            // If the active slot does not match the expected active slot, it is possible
            // another update is happening. Bail out.
            if expected_active_slot.slot() != active {
                return Err(PrecheckError::WrongActiveRotSlot {
                    expected: expected_active_slot.slot, found: *active
                })
            }

            // Otherwise, if the version in the currently active slot does not
            // match what we expect to find, bail out.  It may be that somebody
            // else has come along and completed a subsequent update and we
            // don't want to roll that back.  (If for some reason we *do* want
            // to do this update, the planner will have to notice that what's
            // here is wrong and update the blueprint.)
            if caboose.version != expected_active_slot.version().to_string() {
                return Err(PrecheckError::WrongActiveVersion {
                    expected: expected_active_slot.version(),
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
            let found_inactive_caboose_result = mgs_clients
                .try_all_serially(log, move |mgs_client| async move {
                    mgs_client
                        .sp_component_caboose_get(
                            update.sp_type,
                            update.slot_id,
                            SpComponent::ROT.const_as_str(),
                            expected_active_slot.slot().toggled().to_u16(),
                        )
                        .await
                })
                .await;
            let found_version = match found_inactive_caboose_result {
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
            found_version.matches(expected_inactive_version)?;

            // If transient boot is being used, the persistent preference is not going to match 
            // the active slot. At the moment, this mismatch can also mean one of the partitions
            // had a bad signature check. We don't have a way to tell this appart yet.
            // https://github.com/oxidecomputer/hubris/issues/2066
            //
            // For now, this discrepancy will mean a bad signature check. That's ok, we can continue.
            // The logic here should change when transient boot preference is implemented.
            if expected_persistent_boot_preference != active {
                info!(log, "expected_persistent_boot_preference does not match active slot. \
                This could mean a previous broken update attempt.");
            };

            // If pending_persistent_boot_preference or transient_boot_preference is/are some,
            // then we need to wait, an update is happening.
            if transient_boot_preference.is_some() || pending_persistent_boot_preference.is_some() {
                return Err(PrecheckError::EphemeralRotBootPreferenceSet);
            }

            Ok(PrecheckStatus::ReadyForUpdate)
        }.boxed()
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
            // We want to set the slot we've just updated as the active one
            debug!(log, "attempting to set active slot");
            mgs_clients
                .try_all_serially(log, move |mgs_client| async move {
                    let inactive_slot = self
                        .details
                        .expected_active_slot
                        .slot()
                        .toggled()
                        .to_u16();
                    let persist = true;
                    mgs_client
                        .sp_component_active_slot_set(
                            update.sp_type,
                            update.slot_id,
                            SpComponent::ROT.const_as_str(),
                            persist,
                            &SpComponentFirmwareSlot { slot: inactive_slot },
                        )
                        .await?;
                    Ok::<_, GatewayClientError>(())
                })
                .await?;

            debug!(log, "attempting to reset device");
            mgs_clients
                .try_all_serially(log, move |mgs_client| async move {
                    mgs_client
                        .sp_component_reset(
                            update.sp_type,
                            update.slot_id,
                            SpComponent::ROT.const_as_str(),
                        )
                        .await
                })
                .await?;
            Ok(())
        }
        .boxed()
    }
}
