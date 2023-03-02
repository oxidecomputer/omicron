// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crossterm::event::Event as TermEvent;
use crossterm::event::EventStream;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
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
use tokio::time::{interval, Duration};
use tui::backend::CrosstermBackend;
use tui::Terminal;

use crate::ui::Screen;
use crate::wicketd::{WicketdHandle, WicketdManager};
use crate::{Action, Event, InventoryEvent, State};

// We can avoid a bunch of unnecessary type parameters by picking them ahead of time.
pub type Term = Terminal<CrosstermBackend<Stdout>>;
pub type Frame<'a> = tui::Frame<'a, CrosstermBackend<Stdout>>;

/// The `Runner` owns the main UI thread, and starts a tokio runtime
/// for interaction with downstream services.
pub struct Runner {
    /// The UI that handles input events and renders widgets to the screen
    screen: Screen,

    // The [`Runner`]'s main_loop is purely single threaded. Every interaction
    // with the outside world is via channels. All input from the outside world
    // comes in via an `Event` over a single channel.
    events_rx: Receiver<Event>,

    // We save a copy here so we can hand it out to event producers
    events_tx: Sender<Event>,

    // All global state managed by wicket.
    //
    // This state is updated from user input and events from downstream
    // services.
    state: State,

    // A mechanism for interacting with `wicketd`
    #[allow(unused)]
    wicketd: WicketdHandle,

    // When [`Runner::run`] is called, this is extracted and moved into a tokio
    // task.
    wicketd_manager: Option<WicketdManager>,

    // The terminal we are rendering to
    terminal: Term,

    // Our friendly neighborhood logger
    log: slog::Logger,

    // The tokio runtime for everything outside the main thread
    tokio_rt: tokio::runtime::Runtime,
}

#[allow(clippy::new_without_default)]
impl Runner {
    pub fn new(log: slog::Logger, wicketd_addr: SocketAddrV6) -> Runner {
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
        Runner {
            screen: Screen::new(),
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
        execute!(self.terminal.backend_mut(), EnterAlternateScreen,)?;
        self.main_loop()?;
        disable_raw_mode()?;
        execute!(self.terminal.backend_mut(), LeaveAlternateScreen,)?;
        Ok(())
    }

    fn main_loop(&mut self) -> anyhow::Result<()> {
        info!(self.log, "Starting main loop");

        // Size the initial screen
        let rect = self.terminal.get_frame().size();
        self.screen.resize(&mut self.state, rect.width, rect.height);

        // Draw the initial screen
        self.screen.draw(&self.state, &mut self.terminal)?;

        loop {
            // unwrap is safe because we always hold onto a Sender
            let event = self.events_rx.recv().unwrap();
            match event {
                Event::Tick => {
                    let action = self.screen.on(&mut self.state, Event::Tick);
                    self.handle_action(action)?;
                }
                Event::Term(TermEvent::Key(key_event)) => {
                    if is_control_c(&key_event) {
                        info!(self.log, "CTRL-C Pressed. Exiting.");
                        break;
                    }
                    let action = self.screen.on(
                        &mut self.state,
                        Event::Term(TermEvent::Key(key_event)),
                    );
                    self.handle_action(action)?;
                }
                Event::Term(TermEvent::Resize(width, height)) => {
                    self.screen.resize(&mut self.state, width, height);
                    self.screen.draw(&self.state, &mut self.terminal)?;
                }
                Event::Inventory(event) => {
                    self.handle_inventory_event(event)?;
                }
                _ => {
                    let action = self.screen.on(&mut self.state, event);
                    self.handle_action(action)?;
                }
            }
        }
        Ok(())
    }

    fn handle_inventory_event(
        &mut self,
        event: InventoryEvent,
    ) -> anyhow::Result<()> {
        let mut redraw = false;

        match event {
            InventoryEvent::Inventory {
                changed_inventory,
                wicketd_received,
                mgs_received,
            } => {
                if let Some(inventory) = changed_inventory {
                    if let Err(e) =
                        self.state.inventory.update_inventory(inventory)
                    {
                        error!(self.log, "Failed to update inventory: {e}",);
                    } else {
                        redraw = true;
                    }
                };

                self.state
                    .service_status
                    .reset_wicketd(wicketd_received.elapsed());
                self.state.service_status.reset_mgs(mgs_received.elapsed());
            }
            InventoryEvent::Unavailable { wicketd_received } => {
                self.state
                    .service_status
                    .reset_wicketd(wicketd_received.elapsed());
            }
        }

        redraw |= self.state.service_status.should_redraw();

        if redraw {
            // Inventory or status bar changed. Redraw the screen.
            self.screen.draw(&self.state, &mut self.terminal)?;
        }

        Ok(())
    }

    fn handle_action(&mut self, action: Option<Action>) -> anyhow::Result<()> {
        let Some(action) = action else {
         return Ok(());
        };

        match action {
            Action::Redraw => {
                self.screen.draw(&self.state, &mut self.terminal)?;
            }
            Action::Update(_component_id) => todo!(),
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
