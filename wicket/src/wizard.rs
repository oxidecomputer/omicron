// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crossterm::event::Event as TermEvent;
use crossterm::event::EventStream;
use crossterm::event::{DisableMouseCapture, EnableMouseCapture};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use crossterm::event::{MouseEvent, MouseEventKind};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen,
    LeaveAlternateScreen,
};
use futures::StreamExt;
use slog::{error, info};
use std::io::{stdout, Stdout};
use std::net::SocketAddrV6;
use std::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::Instant;
use tokio::time::{interval, Duration};
use tui::backend::CrosstermBackend;
use tui::Terminal;
use wicketd_client::types::RackV1Inventory;

use crate::inventory::Inventory;
use crate::screens::{Height, ScreenId, Screens};
use crate::wicketd::{WicketdHandle, WicketdManager};
use crate::widgets::RackState;
use crate::widgets::StatusBar;

pub const MARGIN: Height = Height(5);

// We can avoid a bunch of unnecessary type parameters by picking them ahead of time.
pub type Term = Terminal<CrosstermBackend<Stdout>>;
pub type Frame<'a> = tui::Frame<'a, CrosstermBackend<Stdout>>;

/// The core type of this library is the `Wizard`.
///
/// A `Wizard` manages a set of screens, where each screen represents a
/// specific step in the user process. Each screen is drawable, and the
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

    // A mechanism for interacting with `wicketd`
    #[allow(unused)]
    wicketd: WicketdHandle,

    // When the Wizard is run, this will be extracted and moved
    // into a tokio task.
    wicketd_manager: Option<WicketdManager>,

    // The terminal we are rendering to
    terminal: Term,

    // Our friendly neighborhood logger
    log: slog::Logger,

    // The tokio runtime for everything outside the main thread
    tokio_rt: tokio::runtime::Runtime,
}

#[allow(clippy::new_without_default)]
impl Wizard {
    pub fn new(log: slog::Logger, wicketd_addr: SocketAddrV6) -> Wizard {
        let screens = Screens::new(&log);
        let (events_tx, events_rx) = channel();
        let state = State::new();
        let backend = CrosstermBackend::new(stdout());
        let terminal = Terminal::new(backend).unwrap();
        let tokio_rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let (wicketd, wicketd_manager) =
            WicketdManager::new(&log, events_tx.clone(), wicketd_addr);
        Wizard {
            screens,
            active_screen: ScreenId::Splash,
            events_rx,
            events_tx,
            state,
            wicketd,
            wicketd_manager: Some(wicketd_manager),
            terminal,
            log,
            tokio_rt,
        }
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
        info!(self.log, "Starting main loop");
        let rect = self.terminal.get_frame().size();
        // Size the rack for the initial draw
        self.state.rack_state.resize(rect.width, rect.height, &MARGIN);

        // Draw the initial screen
        let screen = self.screens.get_mut(self.active_screen);
        screen.resize(&mut self.state, rect.width, rect.height);
        screen.draw(&self.state, &mut self.terminal)?;

        loop {
            let screen = self.screens.get_mut(self.active_screen);
            // unwrap is safe because we always hold onto a Sender
            let event = self.events_rx.recv().unwrap();
            match event {
                Event::Tick => {
                    let actions = screen.on(&mut self.state, ScreenEvent::Tick);
                    self.handle_actions(actions)?;
                }
                Event::Term(TermEvent::Key(key_event)) => {
                    if is_control_c(&key_event) {
                        info!(self.log, "CTRL-C Pressed. Exiting.");
                        break;
                    }
                    let actions = screen.on(
                        &mut self.state,
                        ScreenEvent::Term(TermEvent::Key(key_event)),
                    );
                    self.handle_actions(actions)?;
                }
                Event::Term(TermEvent::Resize(width, height)) => {
                    self.state.rack_state.resize(width, height, &MARGIN);
                    screen.resize(&mut self.state, width, height);
                    screen.draw(&self.state, &mut self.terminal)?;
                }
                Event::Term(TermEvent::Mouse(mouse_event)) => {
                    self.state.mouse =
                        Point { x: mouse_event.column, y: mouse_event.row };
                    let actions = screen.on(
                        &mut self.state,
                        ScreenEvent::Term(TermEvent::Mouse(mouse_event)),
                    );
                    self.handle_actions(actions)?;
                }
                Event::Inventory(event) => {
                    let mut redraw = false;

                    match event {
                        InventoryEvent::Inventory {
                            changed_inventory,
                            wicketd_received,
                            mgs_received,
                        } => {
                            if let Some(inventory) = changed_inventory {
                                if let Err(e) = self
                                    .state
                                    .inventory
                                    .update_inventory(inventory)
                                {
                                    error!(
                                        self.log,
                                        "Failed to update inventory: {e}",
                                    );
                                } else {
                                    redraw = true;
                                }
                            };

                            self.state
                                .status_bar
                                .reset_wicketd(wicketd_received.elapsed());
                            self.state
                                .status_bar
                                .reset_mgs(mgs_received.elapsed());
                        }
                        InventoryEvent::Unavailable { wicketd_received } => {
                            self.state
                                .status_bar
                                .reset_wicketd(wicketd_received.elapsed());
                        }
                    }

                    redraw |= self.state.status_bar.should_redraw();

                    if redraw {
                        // Inventory or status bar changed. Redraw the screen.
                        screen.draw(&self.state, &mut self.terminal)?;
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
                Action::SwitchScreen(id) => {
                    self.active_screen = id;
                    let screen = self.screens.get_mut(id);
                    let rect = self.terminal.get_frame().size();

                    screen.resize(&mut self.state, rect.width, rect.height);

                    // Simulate a mouse movement for the current position
                    // because the mouse may be in a different position when transitioning
                    // between screens.
                    let mouse_event = MouseEvent {
                        kind: MouseEventKind::Moved,
                        column: self.state.mouse.x,
                        row: self.state.mouse.y,
                        modifiers: KeyModifiers::NONE,
                    };
                    let event =
                        ScreenEvent::Term(TermEvent::Mouse(mouse_event));
                    // We ignore actions, as they can only be draw actions, and
                    // we are about to draw.
                    let _ = screen.on(&mut self.state, event);
                    screen.draw(&self.state, &mut self.terminal)?;
                }
            }
        }
        Ok(())
    }

    fn start_tokio_runtime(&mut self) {
        let events_tx = self.events_tx.clone();
        let log = self.log.clone();
        let wicketd_manager = self.wicketd_manager.take().unwrap();
        self.tokio_rt.block_on(async {
            run_event_listener(log.clone(), events_tx).await;
            tokio::spawn(async move {
                wicketd_manager.run().await;
            });
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
        let mut ticker = interval(Duration::from_millis(30));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                        if events_tx.send(Event::Tick).is_err() {
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
                        if events_tx.send(Event::Term(event)).is_err() {
                            info!(log, "Event listener completed");
                            // The receiver was dropped. Program is ending.
                            return;
                        }

                }
            }
        }
    });
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Point {
    pub x: u16,
    pub y: u16,
}

/// The data state of the Wizard
///
/// Data is not tied to any specific screen and is updated upon event receipt.
#[derive(Debug)]
pub struct State {
    pub inventory: Inventory,
    pub rack_state: RackState,
    pub status_bar: StatusBar,
    pub mouse: Point,
}

impl Default for State {
    fn default() -> Self {
        Self::new()
    }
}

impl State {
    pub fn new() -> State {
        State {
            inventory: Inventory::default(),
            rack_state: RackState::new(),
            status_bar: StatusBar::new(),
            mouse: Point::default(),
        }
    }
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
    Inventory(InventoryEvent),

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
    SwitchScreen(ScreenId),
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
