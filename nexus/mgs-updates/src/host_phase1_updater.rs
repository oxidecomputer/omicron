// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module containing types for updating host OS phase1 images via MGS.

use super::MgsClients;
use super::SpComponentUpdateError;
use super::UpdateProgress;
use super::common_sp_update::SpComponentUpdater;
use super::common_sp_update::deliver_update;
use gateway_client::SpComponent;
use gateway_client::types::SpComponentFirmwareSlot;
use gateway_client::types::SpType;
use slog::Logger;
use slog::info;
use tokio::sync::watch;
use uuid::Uuid;

type GatewayClientError = gateway_client::Error<gateway_client::types::Error>;

pub struct HostPhase1Updater {
    log: Logger,
    progress: watch::Sender<Option<UpdateProgress>>,
    sp_type: SpType,
    sp_slot: u16,
    target_host_slot: u16,
    update_id: Uuid,
    // TODO-clarity maybe a newtype for this? TBD how we get this from
    // wherever it's stored, which might give us a stronger type already.
    phase1_data: Vec<u8>,
}

impl HostPhase1Updater {
    pub fn new(
        sp_type: SpType,
        sp_slot: u16,
        target_host_slot: u16,
        update_id: Uuid,
        phase1_data: Vec<u8>,
        log: &Logger,
    ) -> Self {
        let log = log.new(slog::o!(
            "component" => "HostPhase1Updater",
            "sp_type" => format!("{sp_type:?}"),
            "sp_slot" => sp_slot,
            "target_host_slot" => target_host_slot,
            "update_id" => format!("{update_id}"),
        ));
        let progress = watch::Sender::new(None);
        Self {
            log,
            progress,
            sp_type,
            sp_slot,
            target_host_slot,
            update_id,
            phase1_data,
        }
    }

    pub fn progress_watcher(&self) -> watch::Receiver<Option<UpdateProgress>> {
        self.progress.subscribe()
    }

    /// Drive this host phase 1 update to completion (or failure).
    ///
    /// Only one MGS instance is required to drive an update; however, if
    /// multiple MGS instances are available and passed to this method and an
    /// error occurs communicating with one instance, `HostPhase1Updater` will
    /// try the remaining instances before failing.
    pub async fn update(
        mut self,
        mgs_clients: &mut MgsClients,
    ) -> Result<(), SpComponentUpdateError> {
        // The async block below wants a `&self` reference, but we take `self`
        // for API clarity (to start a new update, the caller should construct a
        // new instance of the updater). Create a `&self` ref that we use
        // through the remainder of this method.
        let me = &self;

        // Prior to delivering the update, ensure the correct target slot is
        // activated.
        //
        // TODO-correctness Should we be doing this, or should a higher level
        // executor set this up before calling us?
        mgs_clients
            .try_all_serially(&self.log, |client| async move {
                me.mark_target_slot_active(&client).await
            })
            .await?;

        // Deliver and drive the update to completion
        deliver_update(&mut self, mgs_clients).await?;

        // Unlike SP and RoT updates, we have nothing to do after delivery of
        // the update completes; signal to any watchers that we're done.
        self.progress.send_replace(Some(UpdateProgress::Complete));

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
        // TODO-correctness Should we always persist this choice?
        let persist = true;

        let slot = self.firmware_slot();

        // TODO-correctness Until
        // https://github.com/oxidecomputer/hubris/issues/1172 is fixed, the
        // host must be in A2 for this operation to succeed. After it is fixed,
        // there will still be a window while a host is booting where this
        // operation can fail. How do we handle this?
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
        // `self.progress`? We haven't actually started delivering an update
        // yet, but it seems weird to give no indication that we have
        // successfully (potentially) modified the state of the target sled.

        info!(
            self.log, "host phase1 target slot marked active";
            "mgs_addr" => client.baseurl(),
        );

        Ok(())
    }
}

impl SpComponentUpdater for HostPhase1Updater {
    fn component(&self) -> &'static str {
        SpComponent::HOST_CPU_BOOT_FLASH.const_as_str()
    }

    fn target_sp_type(&self) -> SpType {
        self.sp_type
    }

    fn target_sp_slot(&self) -> u16 {
        self.sp_slot
    }

    fn firmware_slot(&self) -> u16 {
        self.target_host_slot
    }

    fn update_id(&self) -> Uuid {
        self.update_id
    }

    fn update_data(&self) -> Vec<u8> {
        self.phase1_data.clone()
    }

    fn progress(&self) -> &watch::Sender<Option<UpdateProgress>> {
        &self.progress
    }

    fn logger(&self) -> &Logger {
        &self.log
    }
}
