// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interaction with MGS

use slog::{o, Logger};
use std::sync::mpsc::Sender;

use crate::Event;

// Assume that these requests are periodic on the order of seconds or the
// result of human interaction. In either case, this buffer should be plenty
// large.
const CHANNEL_CAPACITY: usize = 1000;

pub enum MgsRequest {}

pub struct MgsHandle {
    tx: tokio::sync::mpsc::Sender<MgsRequest>,
}

/// Send requests to MGS
///
/// Forward replies to the [`Wizard`] as [`Event`]s
pub struct MgsManager {
    log: Logger,
    rx: tokio::sync::mpsc::Receiver<MgsRequest>,
    wizard_tx: Sender<Event>,
}

impl MgsManager {
    pub fn new(
        log: &Logger,
        wizard_tx: Sender<Event>,
    ) -> (MgsHandle, MgsManager) {
        let log = log.new(o!("component" => "MgsManager"));
        let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_CAPACITY);

        let handle = MgsHandle { tx };
        let manager = MgsManager { log, rx, wizard_tx };

        (handle, manager)
    }

    /// Manage interactions with local MGS
    ///
    /// * Send requests to MGS
    /// * Receive responses / errors
    /// * Translate any responses/errors into [`Event`]s
    /// * that can be utilized by the UI.
    ///
    /// TODO: Uh, um, make this not completely fake
    pub async fn run(mut self) {}
}
