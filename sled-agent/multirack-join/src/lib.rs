// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Multirack Join Service
//!
//! This is the bootstrap service that provisions a new rack on an existing
//! underlay network such that it can be adopted by an existing Nexus in a
//! cluster on the same network. It's vastly simpler than RSS because only
//! sled-agent is started. No other control plane zones, including DNS and NTP,
//! are started. Reconfigurator on the existing Nexuses will setup the rack post
//! cluster join.
//!
//! See RFD 680 for further details.

#[macro_use]
extern crate slog;

use bootstrap_agent_lockstep_types::MultirackJoinRequest;
use sled_agent_bootstrap_common::RssContext;
use slog::{Logger, info};
use slog_error_chain::{InlineErrorChain, SlogInlineError};
use thiserror::Error;
use tokio::sync::watch;

/// Describes errors which may occur while operating the multirack join service.
#[derive(Error, Debug, SlogInlineError)]
pub enum MultirackJoinServiceError {}

/// The current state of the `MultirackJoinService` as retrieved from the `output`
/// watch channel.
#[derive(Debug, Clone)]
pub struct MultirackJoinServiceState {}

#[derive(Debug, Clone)]
pub enum MultiackJoinStep {}

/// The interface to the Multirack Join Service.
pub struct MultirackJoinServiceHandle {
    handle: tokio::task::JoinHandle<Result<(), MultirackJoinServiceError>>,
    input_tx: watch::Sender<MultirackJoinRequest>,
    output_rx: watch::Receiver<MultirackJoinServiceState>,
}

impl MultirackJoinServiceHandle {
    pub fn spawn(ctx: RssContext, request: MultirackJoinRequest) -> Self {
        let (input_tx, input_rx) = watch::channel(request);
        let state = MultirackJoinServiceState {};
        let (output_tx, output_rx) = watch::channel(state.clone());
        let handle = tokio::task::spawn(async move {
            let log =
                ctx.base_log.new(o!("component" => "MultirackJoinService"));
            info!(log, "Starting Multirack Join Service");
            let mut task =
                MultirackJoinServiceTask { log, ctx, input_rx, output_tx };
            task.run().await
        });

        Self { handle, input_tx, output_rx }
    }
}

/// The internal state of the main task running the join service
struct MultirackJoinServiceTask {
    log: Logger,
    ctx: RssContext,
    input_rx: watch::Receiver<MultirackJoinRequest>,
    output_tx: watch::Sender<MultirackJoinServiceState>,
}

impl MultirackJoinServiceTask {
    /// The main loop of the Multirack Join Service
    pub async fn run(&mut self) -> Result<(), MultirackJoinServiceError> {
        Ok(())
    }
}
