// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module containing types for updating SPs via MGS.

use crate::MgsClients;
use crate::SpComponentUpdateHelperImpl;
use crate::common_sp_update::FoundVersion;
use crate::common_sp_update::PostUpdateError;
use crate::common_sp_update::PrecheckError;
use crate::common_sp_update::PrecheckStatus;
use crate::common_sp_update::error_means_caboose_is_invalid;
use futures::FutureExt;
use futures::future::BoxFuture;
use gateway_client::SpComponent;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateSpDetails;
use slog::debug;

pub struct ReconfiguratorSpUpdater {
    details: PendingMgsUpdateSpDetails,
}

impl ReconfiguratorSpUpdater {
    pub fn new(details: PendingMgsUpdateSpDetails) -> Self {
        Self { details }
    }
}

impl SpComponentUpdateHelperImpl for ReconfiguratorSpUpdater {
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

            // Fetch the caboose from the currently active slot.
            let caboose = mgs_clients
                .try_all_serially(log, move |mgs_client| async move {
                    mgs_client
                        .sp_component_caboose_get(
                            update.sp_type,
                            update.slot_id,
                            SpComponent::SP_ITSELF.const_as_str(),
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
            let PendingMgsUpdateSpDetails {
                expected_active_version,
                expected_inactive_version,
            } = &self.details;
            if caboose.version != expected_active_version.to_string() {
                return Err(PrecheckError::WrongActiveVersion {
                    expected: expected_active_version.clone(),
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
                            SpComponent::SP_ITSELF.const_as_str(),
                            1,
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
            mgs_clients
                .try_all_serially(log, move |mgs_client| async move {
                    debug!(log, "attempting to reset device");
                    mgs_client
                        .sp_component_reset(
                            update.sp_type,
                            update.slot_id,
                            SpComponent::SP_ITSELF.const_as_str(),
                        )
                        .await
                })
                .await?;
            Ok(())
        }
        .boxed()
    }
}
