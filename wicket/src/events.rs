// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::state::ComponentId;
use tokio::time::Instant;
use wicketd_client::types::RackV1Inventory;

use crossterm::event::Event as TermEvent;

/// An event that will update state in the wizard
///
/// This can be a keypress, mouse event, or response from a downstream service.
#[derive(Debug)]
pub enum Event {
    /// An input event from the terminal
    Term(TermEvent),

    /// An Inventory Update Event
    Inventory(InventoryEvent),

    /// The tick of a Timer
    /// This can be used to draw a frame to the terminal
    Tick,
}

/// Instructions for the [`Runner`]
///
/// Event's fed through the [`MainScreen::on`]  method return an [`Action`]
/// informing the wizard that it needs to do something. This allows separation
/// of the UI from the rest of the system and makes each more testable.
pub enum Action {
    Redraw,
    Update(ComponentId),
}

impl Action {
    /// Should the action result in a redraw
    ///
    /// For now, all actions result in a redraw.
    /// Some downstream operations will not trigger this in the future.
    pub fn should_redraw(&self) -> bool {
        true
    }
}

/// An inventory event occurred.
#[derive(Clone, Debug)]
pub enum InventoryEvent {
    /// Inventory was received.
    Inventory {
        /// This is Some if the inventory changed.
        changed_inventory: Option<RackV1Inventory>,

        /// The time at which at which information was received from wicketd.
        wicketd_received: Instant,

        /// The time at which information was received from MGS.
        mgs_received: libsw::Stopwatch,
    },
    /// The inventory is unavailable.
    Unavailable {
        /// The time at which at which information was received from wicketd.
        wicketd_received: Instant,
    },
}
