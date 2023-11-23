// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module containing types for updating SPs via MGS.

use crate::app::update::mgs_clients::PollUpdateStatus;

use super::mgs_clients::PollUpdateStatusError;
use super::MgsClients;
use super::UpdateProgress;
use super::UpdateStatusError;
use gateway_client::types::SpType;
use gateway_client::SpComponent;
use slog::Logger;
use std::time::Duration;
use tokio::sync::watch;
use uuid::Uuid;

type GatewayClientError = gateway_client::Error<gateway_client::types::Error>;

#[derive(Debug, thiserror::Error)]
pub enum SpUpdateError {
    #[error("error communicating with MGS")]
    MgsCommunication(#[from] GatewayClientError),

    #[error("failed checking update status: {0}")]
    PollUpdateStatus(#[from] UpdateStatusError),
}

impl From<PollUpdateStatusError> for SpUpdateError {
    fn from(err: PollUpdateStatusError) -> Self {
        match err {
            PollUpdateStatusError::StatusError(err) => err.into(),
            PollUpdateStatusError::ClientError(err) => err.into(),
        }
    }
}

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
        self,
        mut mgs_clients: MgsClients,
    ) -> Result<(), SpUpdateError> {
        // The async blocks below want `&self` references, but we take `self`
        // for API clarity (to start a new SP update, the caller should
        // construct a new `SpUpdater`). Create a `&self` ref that we use
        // through the remainder of this method.
        let me = &self;

        mgs_clients
            .try_all_serially(&self.log, |client| async move {
                me.start_update_one_mgs(&client).await
            })
            .await?;

        // `wait_for_update_completion` uses `try_all_mgs_clients` internally,
        // so we don't wrap it here.
        me.wait_for_update_completion(&mut mgs_clients).await?;

        mgs_clients
            .try_all_serially(&self.log, |client| async move {
                me.finalize_update_via_reset_one_mgs(&client).await
            })
            .await?;

        // wait for any progress watchers to be dropped before we return;
        // otherwise, they'll get `RecvError`s when trying to check the current
        // status
        self.progress.closed().await;

        Ok(())
    }

    async fn start_update_one_mgs(
        &self,
        client: &gateway_client::Client,
    ) -> Result<(), GatewayClientError> {
        // The SP has two firmware slots, but they're aren't individually
        // labled. We always request an update to slot 0, which means "the
        // inactive slot".
        let firmware_slot = 0;

        // Start the update.
        client
            .sp_component_update(
                self.sp_type,
                self.sp_slot,
                SpComponent::SP_ITSELF.const_as_str(),
                firmware_slot,
                &self.update_id,
                reqwest::Body::from(self.sp_hubris_archive.clone()),
            )
            .await?;

        self.progress.send_replace(Some(UpdateProgress::Started));

        info!(
            self.log, "SP update started";
            "mgs_addr" => client.baseurl(),
        );

        Ok(())
    }

    async fn wait_for_update_completion(
        &self,
        mgs_clients: &mut MgsClients,
    ) -> Result<(), SpUpdateError> {
        // How frequently do we poll MGS for the update progress?
        const STATUS_POLL_INTERVAL: Duration = Duration::from_secs(3);

        loop {
            let status = mgs_clients
                .poll_update_status(
                    self.sp_type,
                    self.sp_slot,
                    SpComponent::SP_ITSELF.const_as_str(),
                    self.update_id,
                    &self.log,
                )
                .await?;

            // For `Preparing` and `InProgress`, we could check the progress
            // information returned by these steps and try to check that
            // we're still _making_ progress, but every Nexus instance needs
            // to do that anyway in case we (or the MGS instance delivering
            // the update) crash, so we'll omit that check here. Instead, we
            // just sleep and we'll poll again shortly.
            match status {
                PollUpdateStatus::Preparing { progress } => {
                    self.progress.send_replace(Some(
                        UpdateProgress::Preparing { progress },
                    ));
                }
                PollUpdateStatus::InProgress { progress } => {
                    self.progress.send_replace(Some(
                        UpdateProgress::InProgress { progress },
                    ));
                }
                PollUpdateStatus::Complete => {
                    self.progress.send_replace(Some(
                        UpdateProgress::InProgress { progress: Some(1.0) },
                    ));
                    return Ok(());
                }
            }

            tokio::time::sleep(STATUS_POLL_INTERVAL).await;
        }
    }

    async fn finalize_update_via_reset_one_mgs(
        &self,
        client: &gateway_client::Client,
    ) -> Result<(), GatewayClientError> {
        client
            .sp_component_reset(
                self.sp_type,
                self.sp_slot,
                SpComponent::SP_ITSELF.const_as_str(),
            )
            .await?;

        self.progress.send_replace(Some(UpdateProgress::Complete));
        info!(
            self.log, "SP update complete";
            "mgs_addr" => client.baseurl(),
        );

        Ok(())
    }
}
