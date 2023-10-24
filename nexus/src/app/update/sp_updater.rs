// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module containing types for updating SPs via MGS.

use futures::Future;
use gateway_client::types::SpType;
use gateway_client::types::SpUpdateStatus;
use gateway_client::SpComponent;
use slog::Logger;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

type GatewayClientError = gateway_client::Error<gateway_client::types::Error>;

#[derive(Debug, thiserror::Error)]
pub enum SpUpdateError {
    #[error("error communicating with MGS")]
    MgsCommunication(#[from] GatewayClientError),

    // Error returned when we successfully start an update but it fails to
    // complete successfully.
    #[error("update failed to complete: {0}")]
    FailedToComplete(String),
}

pub struct SpUpdater {
    log: Logger,
    sp_type: SpType,
    sp_slot: u32,
    update_id: Uuid,
    // TODO-clarity maybe a newtype for this? TBD where we how we get this from
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
            "sp_type" => format!("{sp_type:?}"),
            "sp_slot" => sp_slot,
            "update_id" => format!("{update_id}"),
        ));
        Self { log, sp_type, sp_slot, update_id, sp_hubris_archive }
    }

    /// TODO
    ///
    /// # Panics
    ///
    /// If `mgs_clients` is empty.
    pub async fn update<T: Into<VecDeque<Arc<gateway_client::Client>>>>(
        self,
        mgs_clients: T,
    ) -> Result<(), SpUpdateError> {
        let mut mgs_clients = mgs_clients.into();
        assert!(!mgs_clients.is_empty());

        // The async blocks below want `&self` references, but we take `self`
        // for API clarity (to start a new SP update, the caller should
        // construct a new `SpUpdater`). Create a `&self` ref that we use
        // through the remainder of this method.
        let me = &self;

        me.try_all_mgs_clients(&mut mgs_clients, |client| async move {
            me.start_update_one_mgs(&client).await
        })
        .await?;

        me.wait_for_update_completion(&mut mgs_clients).await?;

        me.try_all_mgs_clients(&mut mgs_clients, |client| async move {
            me.finalize_update_via_reset_one_mgs(&client).await
        })
        .await?;

        Ok(())
    }

    async fn try_all_mgs_clients<T, F, Fut>(
        &self,
        mgs_clients: &mut VecDeque<Arc<gateway_client::Client>>,
        op: F,
    ) -> Result<T, GatewayClientError>
    where
        F: Fn(Arc<gateway_client::Client>) -> Fut,
        Fut: Future<Output = Result<T, GatewayClientError>>,
    {
        let mut last_err = None;
        for (i, client) in mgs_clients.iter().enumerate() {
            match op(Arc::clone(client)).await {
                Ok(val) => {
                    // Shift our list of MGS clients such that the one we just
                    // used is at the front for subsequent requests.
                    mgs_clients.rotate_left(i);
                    return Ok(val);
                }
                Err(GatewayClientError::CommunicationError(err)) => {
                    // Does this look like an error that means we should try the
                    // next MGS client?
                    if err.is_connect() || err.is_timeout() {
                        last_err = Some(err);
                        continue;
                    } else {
                        return Err(GatewayClientError::CommunicationError(
                            err,
                        ));
                    }
                }
                Err(err) => return Err(err),
            }
        }

        // We know we have at least one `mgs_client`, so the only way to get
        // here is if all clients failed with connection errors. Return the
        // error from the last MGS we tried.
        Err(GatewayClientError::CommunicationError(last_err.unwrap()))
    }

    async fn start_update_one_mgs(
        &self,
        client: &gateway_client::Client,
    ) -> Result<(), GatewayClientError> {
        // The SP has two firmware slots, but they're aren't invidually labled.
        // We always request an update to slot 0, which means "the inactive
        // slot".
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

        info!(
            self.log, "SP update started";
            "mgs_addr" => client.baseurl(),
        );

        Ok(())
    }

    async fn wait_for_update_completion(
        &self,
        mgs_clients: &mut VecDeque<Arc<gateway_client::Client>>,
    ) -> Result<(), SpUpdateError> {
        // How frequently do we poll MGS for the update progress?
        const STATUS_POLL_INTERVAL: Duration = Duration::from_secs(3);

        loop {
            let update_status = self
                .try_all_mgs_clients(mgs_clients, |client| async move {
                    let update_status = client
                        .sp_component_update_status(
                            self.sp_type,
                            self.sp_slot,
                            SpComponent::SP_ITSELF.const_as_str(),
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

            match update_status {
                SpUpdateStatus::Preparing { id, .. }
                | SpUpdateStatus::InProgress { id, .. } => {
                    if id == self.update_id {
                        // We could check the progress information returned by
                        // these steps (currently omitted via `..`) and try to
                        // check that we're still _making_ progress, but every
                        // Nexus instance needs to do that anyway in case we (or
                        // the MGS instance delivering the update) crash, so
                        // we'll omit that check here. Instead, we just sleep
                        // and we'll poll again shortly.
                        tokio::time::sleep(STATUS_POLL_INTERVAL).await;
                    } else {
                        return Err(SpUpdateError::FailedToComplete(format!(
                            "different update is now in progress ({id})"
                        )));
                    }
                }
                SpUpdateStatus::Complete { id } => {
                    if id == self.update_id {
                        return Ok(());
                    } else {
                        return Err(SpUpdateError::FailedToComplete(format!(
                            "different update is now in complete ({id})"
                        )));
                    }
                }
                SpUpdateStatus::None => {
                    return Err(SpUpdateError::FailedToComplete(
                        "update status lost (did the SP reset?)".to_string(),
                    ));
                }
                SpUpdateStatus::Aborted { id } => {
                    if id == self.update_id {
                        return Err(SpUpdateError::FailedToComplete(
                            "update was aborted".to_string(),
                        ));
                    } else {
                        return Err(SpUpdateError::FailedToComplete(format!(
                            "different update is now in complete ({id})"
                        )));
                    }
                }
                SpUpdateStatus::Failed { code, id } => {
                    if id == self.update_id {
                        return Err(SpUpdateError::FailedToComplete(format!(
                            "update failed (error code {code})"
                        )));
                    } else {
                        return Err(SpUpdateError::FailedToComplete(format!(
                            "different update failed ({id})"
                        )));
                    }
                }
                SpUpdateStatus::RotError { id, message } => {
                    if id == self.update_id {
                        return Err(SpUpdateError::FailedToComplete(format!(
                            "update failed (rot error: {message})"
                        )));
                    } else {
                        return Err(SpUpdateError::FailedToComplete(format!(
                            "different update failed with rot error ({id})"
                        )));
                    }
                }
            }
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

        info!(
            self.log, "SP update complete";
            "mgs_addr" => client.baseurl(),
        );

        Ok(())
    }
}
