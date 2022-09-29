// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The library that is used via the technician port to initialize a rack
//! and perform disaster recovery.
//!
//! This interface is a text user interface (TUI) based wizard
//! that will guide the user through the steps the need to take
//! in an intuitive manner.

use crossterm::event::Event as TermEvent;
use crossterm::event::EventStream;
use crossterm::event::{DisableMouseCapture, EnableMouseCapture};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen,
    LeaveAlternateScreen,
};
use futures::StreamExt;
use slog::{error, info, Drain};
use std::io::{stdout, Stdout};
use std::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{interval, Duration};
use tui::backend::CrosstermBackend;
use tui::Terminal;

pub(crate) mod inventory;
mod mgs;
mod screens;
mod widgets;

use inventory::{Component, ComponentId, Inventory, PowerState};
use mgs::{MgsHandle, MgsManager};
use screens::{ScreenId, Screens};

// We can avoid a bunch of unnecessary type parameters by picking them ahead of time.
pub type Term = Terminal<CrosstermBackend<Stdout>>;
pub type Frame<'a> = tui::Frame<'a, CrosstermBackend<Stdout>>;

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

    // A mechanism for interacting with MGS
    mgs: MgsHandle,

    // When the Wizard is run, this will be extracted and moved
    // into a tokio task.
    mgs_manager: Option<MgsManager>,

    // The terminal we are rendering to
    terminal: Term,

    // Our friendly neighborhood logger
    log: slog::Logger,

    // The tokio runtime for everything outside the main thread
    tokio_rt: tokio::runtime::Runtime,
}

impl Wizard {
    pub fn new() -> Wizard {
        let log = Self::setup_log("/tmp/wicket.log").unwrap();
        let screens = Screens::new(&log);
        let (events_tx, events_rx) = channel();
        let state = State::default();
        let backend = CrosstermBackend::new(stdout());
        let terminal = Terminal::new(backend).unwrap();
        let log = Self::setup_log("/tmp/wicket.log").unwrap();
        let tokio_rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let (mgs, mgs_manager) = MgsManager::new(&log, events_tx.clone());
        Wizard {
            screens,
            active_screen: ScreenId::Inventory,
            events_rx,
            events_tx,
            state,
            mgs,
            mgs_manager: Some(mgs_manager),
            terminal,
            log,
            tokio_rt,
        }
    }

    pub fn setup_log(path: &str) -> anyhow::Result<slog::Logger> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        let decorator = slog_term::PlainDecorator::new(file);
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();

        Ok(slog::Logger::root(drain, slog::o!()))
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        self.start_tokio_runtime();
        enable_raw_mode()?;
        execute!(
            self.terminal.backend_mut(),
            EnterAlternateScreen,
            EnableMouseCapture
        )?;
        self.mainloop()?;
        disable_raw_mode()?;
        execute!(
            self.terminal.backend_mut(),
            LeaveAlternateScreen,
            DisableMouseCapture
        )?;
        Ok(())
    }

    fn mainloop(&mut self) -> anyhow::Result<()> {
        // Draw the initial screen
        let screen = self.screens.get_mut(self.active_screen);
        screen.draw(&self.state, &mut self.terminal)?;

        loop {
            let screen = self.screens.get_mut(self.active_screen);
            // unwrap is safe because we always hold onto a Sender
            let event = self.events_rx.recv().unwrap();
            match event {
                Event::Tick => {
                    let actions = screen.on(&self.state, ScreenEvent::Tick);
                    self.handle_actions(actions)?;
                }
                Event::Term(TermEvent::Key(key_event)) => {
                    if is_control_c(&key_event) {
                        info!(self.log, "CTRL-C Pressed. Exiting.");
                        break;
                    }
                    let actions = screen.on(
                        &self.state,
                        ScreenEvent::Term(TermEvent::Key(key_event)),
                    );
                    self.handle_actions(actions)?;
                }
                Event::Term(TermEvent::Resize(_, _)) => {
                    screen.draw(&self.state, &mut self.terminal)?;
                }
                Event::Term(TermEvent::Mouse(mouse_event)) => {
                    let actions = screen.on(
                        &self.state,
                        ScreenEvent::Term(TermEvent::Mouse(mouse_event)),
                    );
                    self.handle_actions(actions)?;
                }
                Event::Power(component_id, power_state) => {
                    if let Err(e) = self
                        .state
                        .inventory
                        .update_power_state(component_id, power_state)
                    {
                        error!(
                            self.log,
                            "Failed to update power state for {}: {e}",
                            component_id.name()
                        );
                    }
                }
                Event::Inventory(component_id, component) => {
                    if let Err(e) = self
                        .state
                        .inventory
                        .update_inventory(component_id, component)
                    {
                        error!(
                            self.log,
                            "Failed to update inventory for {}: {e}",
                            component_id.name()
                        );
                    }
                }
                _ => info!(self.log, "{:?}", event),
            }
        }
        Ok(())
    }

    fn handle_actions(&mut self, actions: Vec<Action>) -> anyhow::Result<()> {
        for action in actions {
            match action {
                Action::Redraw => {
                    let screen = self.screens.get_mut(self.active_screen);
                    screen.draw(&self.state, &mut self.terminal)?;
                }
            }
        }
        Ok(())
    }

    fn start_tokio_runtime(&mut self) {
        let events_tx = self.events_tx.clone();
        let log = self.log.clone();
        let mgs_manager = self.mgs_manager.take().unwrap();
        self.tokio_rt.block_on(async {
            run_event_listener(log.clone(), events_tx).await;
            tokio::spawn(async move {
                mgs_manager.run().await;
            })
            .await
            .unwrap();
        });
    }
}

fn is_control_c(key_event: &KeyEvent) -> bool {
    key_event.code == KeyCode::Char('c')
        && key_event.modifiers == KeyModifiers::CONTROL
}

/// Listen for terminal related events
async fn run_event_listener(log: slog::Logger, events_tx: Sender<Event>) {
    info!(log, "Starting event listener");
    tokio::spawn(async move {
        let mut events = EventStream::new();
        let mut ticker = interval(Duration::from_millis(50));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                        if let Err(_) = events_tx.send(Event::Tick) {
                            info!(log, "Event listener completed");
                            // The receiver was dropped. Program is ending.
                            return;
                        }
                    }
                event = events.next() => {
                        let event = match event {
                            None => {
                            error!(log, "Event stream completed. Shutting down.");
                            return;
                           }
                            Some(Ok(event)) => event,
                            Some(Err(e)) => {
                              // TODO: Issue a shutdown
                              error!(log, "Failed to receive event: {:?}", e);
                              return;
                            }
                        };
                        if let Err(_) = events_tx.send(Event::Term(event)) {
                            info!(log, "Event listener completed");
                            // The receiver was dropped. Program is ending.
                            return;
                        }

                }
            }
        }
    });
}

/// The data state of the Wizard
///
/// Data is not tied to any specific screen and is updated upon event receipt.
#[derive(Debug, Default)]
pub struct State {
    inventory: Inventory,
}

/// Send requests to RSS
///
/// Replies come in as [`Event`]s
pub struct RssManager {}

/// An event that will update state in the wizard
///
/// This can be a keypress, mouse event, or response from a downstream service.
#[derive(Debug)]
pub enum Event {
    /// An input event from the terminal
    Term(TermEvent),

    /// An Inventory Update Event
    ///
    /// TODO: This should be real information returned from MGS
    Inventory(ComponentId, Component),

    /// PowerState changes
    Power(ComponentId, PowerState),

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
pub enum Action {
    Redraw,
}

/// Events sent to a screen
///
/// These are a subset of [`Event`]
pub enum ScreenEvent {
    /// An input event from the terminal
    Term(crossterm::event::Event),

    /// The tick of a timer
    Tick,
}
