// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module containing types for updating RoTs via MGS.

use super::UpdateProgress;
use futures::Future;
use gateway_client::types::SpType;
use gateway_client::types::SpUpdateStatus;
use gateway_client::SpComponent;
use slog::Logger;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
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
    rot_inactive_slot:
    update_id: Uuid,
    // TODO-clarity maybe a newtype for this? TBD how we get this from
    // wherever it's stored, which might give us a stronger type already.
    sp_hubris_archive: Vec<u8>,
}
