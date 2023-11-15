// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module containing types for updating RoTs via MGS.

use std::time::Duration;

use super::MgsClients;
use super::UpdateProgress;
use gateway_client::types::RotSlot;
use gateway_client::types::SpComponentFirmwareSlot;
use gateway_client::types::SpType;
use gateway_client::types::SpUpdateStatus;
use gateway_client::SpComponent;
use slog::Logger;
use tokio::sync::watch;
use uuid::Uuid;

type GatewayClientError = gateway_client::Error<gateway_client::types::Error>;

#[derive(Debug, thiserror::Error)]
pub enum RotUpdateError {
    #[error("error communicating with MGS")]
    MgsCommunication(#[from] GatewayClientError),

    // Error returned when we successfully start an update but it fails to
    // complete successfully.
    #[error("RoT update failed to complete: {0}")]
    FailedToComplete(String),
}

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
        self,
        mut mgs_clients: MgsClients,
    ) -> Result<(), RotUpdateError> {
        // The async blocks below want `&self` references, but we take `self`
        // for API clarity (to start a new SP update, the caller should
        // construct a new `SpUpdater`). Create a `&self` ref that we use
        // through the remainder of this method.
        let me = &self;

        mgs_clients
            .try_all(&self.log, |client| async move {
                me.start_update_one_mgs(&client).await
            })
            .await?;

        // `wait_for_update_completion` uses `try_all_mgs_clients` internally,
        // so we don't wrap it here.
        me.wait_for_update_completion(&mut mgs_clients).await?;

        mgs_clients
            .try_all(&self.log, |client| async move {
                me.mark_target_slot_active_one_mgs(&client).await
            })
            .await?;

        mgs_clients
            .try_all(&self.log, |client| async move {
                me.finalize_update_via_reset_one_mgs(&client).await
            })
            .await?;

        Ok(())
    }

    async fn start_update_one_mgs(
        &self,
        client: &gateway_client::Client,
    ) -> Result<(), GatewayClientError> {
        let firmware_slot = self.target_rot_slot.as_u16();

        // Start the update.
        client
            .sp_component_update(
                self.sp_type,
                self.sp_slot,
                SpComponent::ROT.const_as_str(),
                firmware_slot,
                &self.update_id,
                reqwest::Body::from(self.rot_hubris_archive.clone()),
            )
            .await?;

        self.progress.send_replace(Some(UpdateProgress::Started));

        info!(
            self.log, "RoT update started";
            "mgs_addr" => client.baseurl(),
        );

        Ok(())
    }

    async fn wait_for_update_completion(
        &self,
        mgs_clients: &mut MgsClients,
    ) -> Result<(), RotUpdateError> {
        // How frequently do we poll MGS for the update progress?
        const STATUS_POLL_INTERVAL: Duration = Duration::from_secs(3);

        loop {
            let update_status = mgs_clients
                .try_all(&self.log, |client| async move {
                    let update_status = client
                        .sp_component_update_status(
                            self.sp_type,
                            self.sp_slot,
                            SpComponent::ROT.const_as_str(),
                        )
                        .await?;

                    info!(
                        self.log, "got SP update status";
                        "mgs_addr" => client.baseurl(),
                        "status" => ?update_status,
                    );

                    Ok(update_status)
                })
                .await?
                .into_inner();

            // The majority of possible update statuses indicate failure; we'll
            // handle the small number of non-failure cases by either
            // `continue`ing or `return`ing; all other branches will give us an
            // error string that we can report.
            let error_message = match update_status {
                // For `Preparing` and `InProgress`, we could check the progress
                // information returned by these steps and try to check that
                // we're still _making_ progress, but every Nexus instance needs
                // to do that anyway in case we (or the MGS instance delivering
                // the update) crash, so we'll omit that check here. Instead, we
                // just sleep and we'll poll again shortly.
                SpUpdateStatus::Preparing { id, progress } => {
                    if id == self.update_id {
                        let progress = progress.and_then(|progress| {
                            if progress.current > progress.total {
                                warn!(
                                    self.log, "nonsense preparing progress";
                                    "current" => progress.current,
                                    "total" => progress.total,
                                );
                                None
                            } else if progress.total == 0 {
                                None
                            } else {
                                Some(
                                    f64::from(progress.current)
                                        / f64::from(progress.total),
                                )
                            }
                        });
                        self.progress.send_replace(Some(
                            UpdateProgress::Preparing { progress },
                        ));
                        tokio::time::sleep(STATUS_POLL_INTERVAL).await;
                        continue;
                    } else {
                        format!("different update is now preparing ({id})")
                    }
                }
                SpUpdateStatus::InProgress {
                    id,
                    bytes_received,
                    total_bytes,
                } => {
                    if id == self.update_id {
                        let progress = if bytes_received > total_bytes {
                            warn!(
                                self.log, "nonsense progress";
                                "bytes_received" => bytes_received,
                                "total_bytes" => total_bytes,
                            );
                            None
                        } else if total_bytes == 0 {
                            None
                        } else {
                            Some(
                                f64::from(bytes_received)
                                    / f64::from(total_bytes),
                            )
                        };
                        self.progress.send_replace(Some(
                            UpdateProgress::InProgress { progress },
                        ));
                        tokio::time::sleep(STATUS_POLL_INTERVAL).await;
                        continue;
                    } else {
                        format!("different update is now in progress ({id})")
                    }
                }
                SpUpdateStatus::Complete { id } => {
                    if id == self.update_id {
                        self.progress.send_replace(Some(
                            UpdateProgress::InProgress { progress: Some(1.0) },
                        ));
                        return Ok(());
                    } else {
                        format!("different update is now in complete ({id})")
                    }
                }
                SpUpdateStatus::None => {
                    "update status lost (did the SP reset?)".to_string()
                }
                SpUpdateStatus::Aborted { id } => {
                    if id == self.update_id {
                        "update was aborted".to_string()
                    } else {
                        format!("different update is now in complete ({id})")
                    }
                }
                SpUpdateStatus::Failed { code, id } => {
                    if id == self.update_id {
                        format!("update failed (error code {code})")
                    } else {
                        format!("different update failed ({id})")
                    }
                }
                SpUpdateStatus::RotError { id, message } => {
                    if id == self.update_id {
                        format!("update failed (rot error: {message})")
                    } else {
                        format!("different update failed with rot error ({id})")
                    }
                }
            };

            self.progress.send_replace(Some(UpdateProgress::Failed(
                error_message.clone(),
            )));
            return Err(RotUpdateError::FailedToComplete(error_message));
        }
    }

    async fn mark_target_slot_active_one_mgs(
        &self,
        client: &gateway_client::Client,
    ) -> Result<(), GatewayClientError> {
        // RoT currently doesn't support non-persistent slot swapping, so always
        // tell it to persist our choice.
        let persist = true;

        let slot = self.target_rot_slot.as_u16();

        client
            .sp_component_active_slot_set(
                self.sp_type,
                self.sp_slot,
                SpComponent::ROT.const_as_str(),
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

    async fn finalize_update_via_reset_one_mgs(
        &self,
        client: &gateway_client::Client,
    ) -> Result<(), GatewayClientError> {
        client
            .sp_component_reset(
                self.sp_type,
                self.sp_slot,
                SpComponent::ROT.const_as_str(),
            )
            .await?;

        self.progress.send_replace(Some(UpdateProgress::Complete));
        info!(
            self.log, "RoT update complete";
            "mgs_addr" => client.baseurl(),
        );

        Ok(())
    }
}

trait RotSlotAsU16 {
    fn as_u16(&self) -> u16;
}

impl RotSlotAsU16 for RotSlot {
    fn as_u16(&self) -> u16 {
        match self {
            RotSlot::A => 0,
            RotSlot::B => 1,
        }
    }
}
