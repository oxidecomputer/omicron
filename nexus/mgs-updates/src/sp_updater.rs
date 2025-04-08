// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module containing types for updating SPs via MGS.

use super::MgsClients;
use super::SpComponentUpdateError;
use super::UpdateProgress;
use super::common_sp_update::SpComponentUpdater;
use super::common_sp_update::deliver_update;
use crate::ReconfiguratorSpComponentUpdater;
use crate::common_sp_update::VersionStatus;
use futures::FutureExt;
use futures::future::BoxFuture;
use gateway_client::SpComponent;
use gateway_client::types::SpType;
use nexus_types::deployment::PendingMgsUpdate;
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
impl ReconfiguratorSpComponentUpdater for ReconfiguratorSpUpdater {
    /// Checks if the component is already updated or ready for update
    fn version_status<'a>(
        &'a self,
        log: &'a slog::Logger,
        mgs_clients: &'a mut MgsClients,
        update: &'a PendingMgsUpdate,
    ) -> BoxFuture<'a, Result<VersionStatus, GatewayClientError>> {
        mgs_clients
            .try_all_serially(log, move |mgs_client| async move {
                let state = mgs_client
                    .sp_get(update.sp_type, update.slot_id)
                    .await?
                    .into_inner();
                debug!(log, "found SP state"; "state" => ?state);
                if state.model != update.baseboard_id.part_number
                    || state.serial_number != update.baseboard_id.serial_number
                {
                    // XXX-dap need to communicate the specific failure back
                    return Ok(VersionStatus::NotReadyForUpdate);
                }

                let caboose = mgs_client
                    .sp_component_caboose_get(
                        update.sp_type,
                        update.slot_id,
                        &SpComponent::SP_ITSELF.to_string(),
                        0,
                    )
                    .await?
                    .into_inner();
                debug!(log, "found caboose"; "caboose" => ?caboose);

                if caboose.version == update.artifact_version.as_str() {
                    // XXX-dap should we check if there's an update in
                    // progress?  If so it seems like we'll be saying "we're
                    // done" even though it might be about to get un-done or
                    // otherwise changed.
                    return Ok(VersionStatus::UpdateComplete);
                }

                // XXX-dap verify the precondition related to the *other*
                // slot.  there's an XXX-dap elsewhere about how this is
                // important so that an instance that's behind won't try to
                // undo a subsequent update.

                // XXX-dap verify the board from the caboose, too Do we
                // really want the caboose from the artifact that we're
                // trying to deploy available here?  That would obviate the
                // need to put artifact_version in PendingMgsUpdate, too.
                Ok(VersionStatus::ReadyForUpdate)
            })
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
                Ok(mgs_client
                    .sp_component_reset(
                        update.sp_type,
                        update.slot_id,
                        &SpComponent::SP_ITSELF.to_string(),
                    )
                    .await?
                    .into_inner())
            })
            .boxed()
    }
}
