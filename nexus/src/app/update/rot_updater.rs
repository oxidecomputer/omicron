// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module containing types for updating RoTs via MGS.

use super::common_sp_update::deliver_update;
use super::common_sp_update::SpComponentUpdater;
use super::MgsClients;
use super::SpComponentUpdateError;
use super::UpdateProgress;
use gateway_client::types::RotSlot;
use gateway_client::types::SpComponentFirmwareSlot;
use gateway_client::types::SpType;
use gateway_client::SpComponent;
use slog::Logger;
use tokio::sync::watch;
use uuid::Uuid;

type GatewayClientError = gateway_client::Error<gateway_client::types::Error>;

pub struct RotUpdater {
    log: Logger,
    progress: watch::Sender<Option<UpdateProgress>>,
    sp_type: SpType,
    sp_slot: u32,
    target_rot_slot: RotSlot,
    update_id: Uuid,
    // TODO-clarity maybe a newtype for this? TBD how we get this from
    // wherever it's stored, which might give us a stronger type already.
    rot_hubris_archive: Vec<u8>,
}

impl RotUpdater {
    pub fn new(
        sp_type: SpType,
        sp_slot: u32,
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

    fn target_sp_slot(&self) -> u32 {
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
