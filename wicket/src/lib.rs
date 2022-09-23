// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The library that is used via the technician port to initialize a rack
//! and perform disaster recovery.
//!
//! This interface is a text user interface (TUI) based wizard
//! that will guide the user through the steps the need to take
//! in an intuitive manner.

use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen,
    LeaveAlternateScreen,
};
use std::collections::BTreeMap;
use std::io::{stdout, Stdout};
use std::sync::mpsc::{channel, Receiver, Sender};
use tui::backend::{Backend, CrosstermBackend};
use tui::layout::{Constraint, Direction, Layout};
use tui::widgets::{Block, Borders};
use tui::{Frame, Terminal};

mod screens;

use screens::{InventoryScreen, Screen, ScreenId, Screens};

// We can avoid a bunch of unnecessary type parameters by picking them ahead of time.
pub type Term = Terminal<CrosstermBackend<Stdout>>;

/// The core type of this library is the `Wizard`.
///
/// A `Wizard` manages a set of [`Screen`]s, where each screen represents a
/// specific step in the user process. Each [`Screen`] is drawable, and the
/// active screen is rendered on every tick. The [`Wizard`] manages which
/// screen is active, issues the rendering operation to the terminal, and
/// communicates with other threads and async tasks to receive user input
/// and drive backend services.
pub struct Wizard {
    // The currently active screen
    active_screen: ScreenId,

    // All the screens managed by the [`Wizard`]
    screens: Screens,

    // The [`Wizard`] is purely single threaded. Every interaction with the
    // outside world is via channels.  All receiving from the outside world
    // comes in via an `Event` over a single channel.
    //
    // Doing this allows us to record and replay all received events, which
    // will deterministically draw the output of the UI, as long as we disable
    // any output to downstream services.
    //
    // This effectively acts as a way to mock real responses from servers
    // without ever having to run those servers or even send the requests that
    // triggered the incoming events!
    //
    // Note that for resize events or other terminal specific events we'll
    // likely have to "output" them to fake the same interaction.
    events_rx: Receiver<Event>,

    // We save a copy here so we can hand it out to event producers
    events_tx: Sender<Event>,

    // The internal state of the Wizard
    // This contains all updatable data
    state: State,

    // The mechanism for sending requests to MGS
    //
    // Replies always come in as [`Event`]s.
    mgs: MgsManager,

    // The mechanism for sending request to the RSS
    //
    // TODO: Should this really go through the bootstrap agent server or
    // should it be it's own dropshot server?
    //
    // Replies always come in as events
    rss: RssManager,

    // The terminal we are rendering to
    terminal: Term,
}

impl Wizard {
    pub fn new() -> Wizard {
        let screens = Screens::new();
        let (events_tx, events_rx) = channel();
        let state = State::default();
        let backend = CrosstermBackend::new(stdout());
        let terminal = Terminal::new(backend).unwrap();
        Wizard {
            screens,
            active_screen: ScreenId::Inventory,
            events_rx,
            events_tx,
            state,
            mgs: MgsManager {},
            rss: RssManager {},
            terminal,
        }
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        // TODO: Spawn off threads required for input and downstream service interaction
        enable_raw_mode()?;
        execute!(self.terminal.backend_mut(), EnterAlternateScreen)?;
        self.mainloop()?;
        disable_raw_mode()?;
        execute!(self.terminal.backend_mut(), LeaveAlternateScreen)?;
        Ok(())
    }

    fn mainloop(&mut self) -> anyhow::Result<()> {
        let screen = self.screens.get(self.active_screen);
        screen.draw(&self.state, &mut self.terminal)?;
        std::thread::sleep(std::time::Duration::from_secs(1));
        Ok(())
    }
}

/// The data state of the Wizard
///
/// Data is not tied to any specific screen and is updated upon event receipt.
#[derive(Debug, Default)]
pub struct State {
    inventory: Inventory,
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
#[derive(Debug, Default)]
pub struct Inventory {
    sleds: [Option<FakeSledInfo>; 32],
    switches: [Option<FakeSwitchInfo>; 2],
    psc: Option<FakePscInfo>,
}

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

/// An action for the system to take.
///
/// This can be something like a screen transition or calling a downstream
/// service. Screens never take actions directly, but they are the only ones
/// that know what visual content an input such as a key press or mouse event
/// is meant for and what action should be taken in that case.
pub enum Action {}

/// Events sent to a screen
///
/// These are a subset of [`Event`]
pub enum ScreenEvent {
    /// An input event from the terminal
    Term(crossterm::event::Event),

    /// The tick of a timer
    Tick,
}

#[derive(Debug)]
pub enum SwitchLocation {
    Top,
    Bottom,
}

#[derive(Debug)]
pub struct FakeSledInfo {
    slot: u16,
    serial_number: String,
    part_number: String,
    sp_version: String,
    rot_version: String,
    host_os_version: String,
    control_plane_version: Option<String>,
}

#[derive(Debug)]
pub struct FakeSwitchInfo {
    location: SwitchLocation,
    serial_number: String,
    part_number: String,
    sp_version: String,
    rot_version: String,
}

#[derive(Debug)]
pub struct FakePscInfo {
    serial_number: String,
    part_number: String,
    sp_version: String,
    rot_version: String,
}

/// TODO: Use real inventory received from MGS
pub enum FakeInventoryUpdate {
    Sled(FakeSledInfo),
    Switch(FakeSwitchInfo),
    Psc(FakePscInfo),
}
