// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interaction with MGS

use slog::{o, Logger};
use std::sync::mpsc::Sender;

use crate::inventory::{ComponentId, PowerState};
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
    pub async fn run(mut self) {
        for i in 0..32 {
            let state = {
                match i % 4 {
                    0 => PowerState::A0,
                    1 => PowerState::A2,
                    2 => PowerState::A3,
                    3 => PowerState::A4,
                    _ => unreachable!(),
                }
            };
            self.wizard_tx
                .send(Event::Power(ComponentId::Sled(i), state))
                .unwrap();
        }
        self.wizard_tx
            .send(Event::Power(ComponentId::Switch(0), PowerState::A0))
            .unwrap();
        self.wizard_tx
            .send(Event::Power(ComponentId::Switch(1), PowerState::A0))
            .unwrap();
        self.wizard_tx
            .send(Event::Power(ComponentId::Psc(0), PowerState::A0))
            .unwrap();
        self.wizard_tx
            .send(Event::Power(ComponentId::Psc(1), PowerState::A4))
            .unwrap();
    }
}
