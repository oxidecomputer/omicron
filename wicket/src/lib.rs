// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The library that is used via the technician port to initialize a rack
//! and perform disaster recovery.
//!
//! This interface is a text user interface (TUI) based wizard
//! that will guide the user through the steps the need to take
//! in an intuitive manner.

use std::collections::BTreeMap;
use std::sync::mpsc::Receiver;

/// The core type of this library is the `Wizard`.
///
/// A `Wizard` manages a set of [`Screen`]s, where each screen represents a
/// specific step in the user process. Each [`Screen`] is drawable, and the
/// active screen is rendered on every tick. The [`Wizard`] manages which
/// screen is active, issues the rendering operation to the terminal, and
/// communicates with other threads and async tasks to receive user input
/// and drive backend services.
pub struct Wizard {
    // All screens are stored in this map. However, not all screens are
    // necessarily accesible from the wizard UI at any point in time.
    screens: BTreeMap<ScreenId, Screen>,

    events_rx: Receiver<Event>,

    inventory: Inventory,

    /// The mechanism for sending requests to MGS
    ///
    /// Replies always come in as [`Event`]s.
    ///
    mgs: MgsManager,

    /// The mechanism for sending request to the RSS
    ///
    /// TODO: Should this really go through the bootstrap agent server or
    /// should it be it's own dropshot server?
    ///
    /// Replies always come in as events
    rss: RssManager,
}

/// Send requests to MGS
///
/// Replies come in as [`Event`]s
pub struct MgsManager {}

/// Send requests to RSS
///
/// Replies come in as [`Event`]s
pub struct RssManager {}

/// Inventory is the most recent information about rack composition as
/// received from MGS.
pub struct Inventory {}

/// An event that will update state in the wizard
///
/// This can be a keypress, mouse event, or response from a downstream service.
pub enum Event {
    /// An input event from the terminal
    Term(crossterm::event::Event),

    /// An Inventory Update Event
    ///
    /// TODO: This should be real information returned from MGS
    Inventory(FakeInventoryUpdate),

    /// The tick of a Timer
    /// This can be used to draw a frame to the terminal
    Tick,
    //... TODO: Replies from MGS & RSS
}

/// An identifier for a specific [`Screen`] in the [`Wizard`]
pub enum ScreenId {
    Inventory,
    Update,
    RackInit,
}

/// An individual screen in the [`Wizard`]
pub struct Screen {}

pub enum SwitchLocation {
    Top,
    Bottom,
}

/// TODO: Use real inventory received from MGS
pub enum FakeInventoryUpdate {
    Sled {
        slot: u16,
        serial_number: String,
        part_number: String,
        sp_version: String,
        rot_version: String,
        host_os_version: String,
        control_plane_version: Option<String>,
    },
    Switch {
        location: SwitchLocation,
        serial_number: String,
        part_number: String,
        sp_version: String,
        rot_version: String,
    },
    Psc {
        serial_number: String,
        part_number: String,
        sp_version: String,
        rot_version: String,
    },
}
