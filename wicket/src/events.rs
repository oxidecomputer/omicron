// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use crate::state::ComponentId;
use tokio::time::Instant;
use wicketd_client::types::{ArtifactId, RackV1Inventory, UpdateLogAll};

use crossterm::event::Event as TermEvent;

/// An event that will update state
///
/// This can be a keypress, mouse event, or response from a downstream service.
#[derive(Debug)]
pub enum Event {
    /// An input event from the terminal
    Term(TermEvent),

    /// An Inventory Update Event
    Inventory(InventoryEvent),

    /// Update Log Event
    UpdateLog(UpdateLogAll),

    /// TUF repo artifacts unpacked by wicketd
    UpdateArtifacts(Vec<ArtifactId>),

    /// The tick of a Timer
    /// This can be used to draw a frame to the terminal
    Tick,
}

/// Instructions for the [`crate::Runner`]
///
/// Event's fed through the [`crate::Control::on`] methods return an [`Action`]
/// informing the [`crate::Runner`] that it needs to do something. This allows
/// separation of the UI from the rest of the system and makes each more
/// testable.
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
        match self {
            Action::Redraw | Action::Update(_) => true,
        }
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
        mgs_received: libsw::TokioSw,
    },
    /// The inventory is unavailable.
    Unavailable {
        /// The time at which at which information was received from wicketd.
        wicketd_received: Instant,
    },
}
