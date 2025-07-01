// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module containing types for updating RoTs via MGS.

use super::MgsClients;
use super::SpComponentUpdateError;
use super::UpdateProgress;
use super::common_sp_update::SpComponentUpdater;
use super::common_sp_update::deliver_update;
use crate::SpComponentUpdateHelper;
use crate::common_sp_update::FoundVersion;
use crate::common_sp_update::PrecheckError;
use crate::common_sp_update::PrecheckStatus;
use crate::common_sp_update::error_means_caboose_is_invalid;
use futures::FutureExt;
use futures::future::BoxFuture;
use gateway_client::SpComponent;
use gateway_client::types::RotState;
use gateway_client::types::SpComponentFirmwareSlot;
use gateway_client::types::SpType;
use gateway_types::rot::RotSlot;
use nexus_types::deployment::ExpectedVersion;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateDetails;
use slog::Logger;
use slog::{debug, info};
use tokio::sync::watch;
use uuid::Uuid;

type GatewayClientError = gateway_client::Error<gateway_client::types::Error>;

pub struct RotUpdater {
    log: Logger,
    progress: watch::Sender<Option<UpdateProgress>>,
    sp_type: SpType,
    sp_slot: u16,
    target_rot_slot: RotSlot,
    update_id: Uuid,
    // TODO-clarity maybe a newtype for this? TBD how we get this from
    // wherever it's stored, which might give us a stronger type already.
    rot_hubris_archive: Vec<u8>,
}

impl RotUpdater {
    pub fn new(
        sp_type: SpType,
        sp_slot: u16,
        target_rot_slot: RotSlot,
        update_id: Uuid,
        rot_hubris_archive: Vec<u8>,
        log: &Logger,
    ) -> Self {
        let log = log.new(slog::o!(
            "component" => "RotUpdater",
            "sp_type" => format!("{sp_type:?}"),
            "sp_slot" => sp_slot,
            "target_rot_slot" => format!("{target_rot_slot:?}"),
            "update_id" => format!("{update_id}"),
        ));
        let progress = watch::Sender::new(None);
        Self {
            log,
            progress,
            sp_type,
            sp_slot,
            target_rot_slot,
            update_id,
            rot_hubris_archive,
        }
    }

    pub fn progress_watcher(&self) -> watch::Receiver<Option<UpdateProgress>> {
        self.progress.subscribe()
    }

    /// Drive this RoT update to completion (or failure).
    ///
    /// Only one MGS instance is required to drive an update; however, if
    /// multiple MGS instances are available and passed to this method and an
    /// error occurs communicating with one instance, `RotUpdater` will try the
    /// remaining instances before failing.
    pub async fn update(
        mut self,
        mgs_clients: &mut MgsClients,
    ) -> Result<(), SpComponentUpdateError> {
        // Deliver and drive the update to "completion" (which isn't really
        // complete for the RoT, since we still have to do the steps below after
        // the delivery of the update completes).
        deliver_update(&mut self, mgs_clients).await?;

        // The async blocks below want `&self` references, but we take `self`
        // for API clarity (to start a new update, the caller should construct a
        // new updater). Create a `&self` ref that we use through the remainder
        // of this method.
        let me = &self;

        mgs_clients
            .try_all_serially(&self.log, |client| async move {
                me.mark_target_slot_active(&client).await
            })
            .await?;

        mgs_clients
            .try_all_serially(&self.log, |client| async move {
                me.finalize_update_via_reset(&client).await
            })
            .await?;

        // wait for any progress watchers to be dropped before we return;
        // otherwise, they'll get `RecvError`s when trying to check the current
        // status
        self.progress.closed().await;

        Ok(())
    }

    async fn mark_target_slot_active(
        &self,
        client: &gateway_client::Client,
    ) -> Result<(), GatewayClientError> {
        // RoT currently doesn't support non-persistent slot swapping, so always
        // tell it to persist our choice.
        let persist = true;

        let slot = self.firmware_slot();

        client
            .sp_component_active_slot_set(
                self.sp_type,
                self.sp_slot,
                self.component(),
                persist,
                &SpComponentFirmwareSlot { slot },
            )
            .await?;

        // TODO-correctness Should we send some kind of update to
        // `self.progress`? We already sent `InProgress(1.0)` when the update
        // finished delivering. Or perhaps we shouldn't even be doing this step
        // and the reset, and let our caller handle the finalization?

        info!(
            self.log, "RoT target slot marked active";
            "mgs_addr" => client.baseurl(),
        );

        Ok(())
    }

    async fn finalize_update_via_reset(
        &self,
        client: &gateway_client::Client,
    ) -> Result<(), GatewayClientError> {
        client
            .sp_component_reset(self.sp_type, self.sp_slot, self.component())
            .await?;

        self.progress.send_replace(Some(UpdateProgress::Complete));
        info!(
            self.log, "RoT update complete";
            "mgs_addr" => client.baseurl(),
        );

        Ok(())
    }
}

impl SpComponentUpdater for RotUpdater {
    fn component(&self) -> &'static str {
        SpComponent::ROT.const_as_str()
    }

    fn target_sp_type(&self) -> SpType {
        self.sp_type
    }

    fn target_sp_slot(&self) -> u16 {
        self.sp_slot
    }

    fn firmware_slot(&self) -> u16 {
        match self.target_rot_slot {
            RotSlot::A => 0,
            RotSlot::B => 1,
        }
    }

    fn update_id(&self) -> Uuid {
        self.update_id
    }

    fn update_data(&self) -> Vec<u8> {
        self.rot_hubris_archive.clone()
    }

    fn progress(&self) -> &watch::Sender<Option<UpdateProgress>> {
        &self.progress
    }

    fn logger(&self) -> &Logger {
        &self.log
    }
}

pub struct ReconfiguratorRotUpdater;
impl SpComponentUpdateHelper for ReconfiguratorRotUpdater {
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

            let PendingMgsUpdateDetails::Rot {
                expected_inactive_version,
                expected_active_slot,
                expected_persistent_boot_preference,
                ..
            } = &update.details
            else {
                unreachable!(
                    "pending MGS update details within ReconfiguratorRotUpdater \
                    will always be for the RoT"
                );
            };

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
                        &SpComponent::ROT.to_string(),
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
                return Err(PrecheckError::WrongActiveSlot {
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
                            &SpComponent::ROT.to_string(),
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
            match (&expected_inactive_version, &found_version) {
                // expected garbage, found garbage
                (
                    ExpectedVersion::NoValidVersion,
                    FoundVersion::MissingVersion,
                ) => (),
                // expected a specific version and found it
                (
                    ExpectedVersion::Version(artifact_version),
                    FoundVersion::Version(found_version),
                ) if artifact_version.to_string() == *found_version => (),
                // anything else is a mismatch
                (ExpectedVersion::NoValidVersion, FoundVersion::Version(_))
                | (ExpectedVersion::Version(_), FoundVersion::MissingVersion)
                | (ExpectedVersion::Version(_), FoundVersion::Version(_)) => {
                    return Err(PrecheckError::WrongInactiveVersion {
                        expected: expected_inactive_version.clone(),
                        found: found_version,
                    });
                }
            };

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
    ) -> BoxFuture<'a, Result<(), GatewayClientError>> {
        mgs_clients
            .try_all_serially(log, move |mgs_client| async move {
                // We want to set the slot we've just updated as the active one
                debug!(log, "attempting to set active slot");
                let inactive_slot = match &update.details {
                    PendingMgsUpdateDetails::Rot { expected_active_slot, .. } => {
                        expected_active_slot.slot().toggled().to_u16()
                    },
                    PendingMgsUpdateDetails::Sp { .. }
                    | PendingMgsUpdateDetails::RotBootloader { .. } => unreachable!(
                        "pending MGS update details within ReconfiguratorRotUpdater \
                        will always be for the RoT"
                    )
                };
                let persist = true;
                mgs_client
                    .sp_component_active_slot_set(
                        update.sp_type,
                        update.slot_id,
                        &SpComponent::ROT.to_string(),
                        persist,
                        &SpComponentFirmwareSlot { slot: inactive_slot }
                    )
                    .await?;

                debug!(log, "attempting to reset device");
                mgs_client
                    .sp_component_reset(
                        update.sp_type,
                        update.slot_id,
                        &SpComponent::ROT.to_string(),
                    )
                    .await?;
                Ok(())
            })
            .boxed()
    }
}
