// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module containing types for updating SPs via MGS.

use crate::MgsClients;
use crate::SpComponentUpdateError;
use crate::SpComponentUpdateHelper;
use crate::UpdateProgress;
use crate::common_sp_update::FoundVersion;
use crate::common_sp_update::PrecheckError;
use crate::common_sp_update::PrecheckStatus;
use crate::common_sp_update::SpComponentUpdater;
use crate::common_sp_update::deliver_update;
use crate::common_sp_update::error_means_caboose_is_invalid;
use futures::FutureExt;
use futures::future::BoxFuture;
use gateway_client::SpComponent;
use gateway_client::types::SpType;
use nexus_types::deployment::ExpectedVersion;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateDetails;
use slog::Logger;
use slog::{debug, info};
use tokio::sync::watch;
use uuid::Uuid;

type GatewayClientError = gateway_client::Error<gateway_client::types::Error>;

pub struct SpUpdater {
    log: Logger,
    progress: watch::Sender<Option<UpdateProgress>>,
    sp_type: SpType,
    sp_slot: u32,
    update_id: Uuid,
    // TODO-clarity maybe a newtype for this? TBD how we get this from
    // wherever it's stored, which might give us a stronger type already.
    sp_hubris_archive: Vec<u8>,
}

impl SpUpdater {
    pub fn new(
        sp_type: SpType,
        sp_slot: u32,
        update_id: Uuid,
        sp_hubris_archive: Vec<u8>,
        log: &Logger,
    ) -> Self {
        let log = log.new(slog::o!(
            "component" => "SpUpdater",
            "sp_type" => format!("{sp_type:?}"),
            "sp_slot" => sp_slot,
            "update_id" => format!("{update_id}"),
        ));
        let progress = watch::Sender::new(None);
        Self { log, progress, sp_type, sp_slot, update_id, sp_hubris_archive }
    }

    pub fn progress_watcher(&self) -> watch::Receiver<Option<UpdateProgress>> {
        self.progress.subscribe()
    }

    /// Drive this SP update to completion (or failure).
    ///
    /// Only one MGS instance is required to drive an update; however, if
    /// multiple MGS instances are available and passed to this method and an
    /// error occurs communicating with one instance, `SpUpdater` will try the
    /// remaining instances before failing.
    pub async fn update(
        mut self,
        mgs_clients: &mut MgsClients,
    ) -> Result<(), SpComponentUpdateError> {
        // Deliver and drive the update to "completion" (which isn't really
        // complete for the SP, since we still have to reset it after the
        // delivery of the update completes).
        deliver_update(&mut self, mgs_clients).await?;

        // The async block below wants a `&self` reference, but we take `self`
        // for API clarity (to start a new SP update, the caller should
        // construct a new `SpUpdater`). Create a `&self` ref that we use
        // through the remainder of this method.
        let me = &self;

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

    async fn finalize_update_via_reset(
        &self,
        client: &gateway_client::Client,
    ) -> Result<(), GatewayClientError> {
        client
            .sp_component_reset(self.sp_type, self.sp_slot, self.component())
            .await?;

        self.progress.send_replace(Some(UpdateProgress::Complete));
        info!(
            self.log, "SP update complete";
            "mgs_addr" => client.baseurl(),
        );

        Ok(())
    }
}

impl SpComponentUpdater for SpUpdater {
    fn component(&self) -> &'static str {
        SpComponent::SP_ITSELF.const_as_str()
    }

    fn target_sp_type(&self) -> SpType {
        self.sp_type
    }

    fn target_sp_slot(&self) -> u32 {
        self.sp_slot
    }

    fn firmware_slot(&self) -> u16 {
        // The SP has two firmware slots, but they're aren't individually
        // labled. We always request an update to slot 0, which means "the
        // inactive slot".
        0
    }

    fn update_id(&self) -> Uuid {
        self.update_id
    }

    fn update_data(&self) -> Vec<u8> {
        self.sp_hubris_archive.clone()
    }

    fn progress(&self) -> &watch::Sender<Option<UpdateProgress>> {
        &self.progress
    }

    fn logger(&self) -> &Logger {
        &self.log
    }
}

pub struct ReconfiguratorSpUpdater;
impl SpComponentUpdateHelper for ReconfiguratorSpUpdater {
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
                    mgs_client
                        .sp_get(update.sp_type, u32::from(update.slot_id))
                        .await
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
                            u32::from(update.slot_id),
                            &SpComponent::SP_ITSELF.to_string(),
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
            let PendingMgsUpdateDetails::Sp {
                expected_active_version,
                expected_inactive_version,
            } = &update.details
            else {
                unreachable!(
                    "pending MGS update details within ReconfiguratorSpUpdater \
                    will always be for the SP"
                );
            };
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
                            u32::from(update.slot_id),
                            &SpComponent::SP_ITSELF.to_string(),
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
    ) -> BoxFuture<'a, Result<(), GatewayClientError>> {
        mgs_clients
            .try_all_serially(log, move |mgs_client| async move {
                debug!(log, "attempting to reset device");
                mgs_client
                    .sp_component_reset(
                        update.sp_type,
                        u32::from(update.slot_id),
                        &SpComponent::SP_ITSELF.to_string(),
                    )
                    .await?;
                Ok(())
            })
            .boxed()
    }
}
