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
use slog::{debug, error, info};
use std::io::{stdout, Stdout};
use std::net::SocketAddrV6;
use tokio::sync::mpsc::{
    unbounded_channel, UnboundedReceiver, UnboundedSender,
};
use tokio::time::{interval, Duration};
use tui::backend::CrosstermBackend;
use tui::Terminal;

use crate::ui::Screen;
use crate::wicketd::{self, WicketdHandle, WicketdManager};
use crate::{Action, Cmd, Event, KeyHandler, Recorder, State};

// We can avoid a bunch of unnecessary type parameters by picking them ahead of time.
pub type Term = Terminal<CrosstermBackend<Stdout>>;
pub type Frame<'a> = tui::Frame<'a, CrosstermBackend<Stdout>>;

const TICK_INTERVAL: Duration = Duration::from_millis(30);
const MAX_RECORDED_EVENTS: usize = 10000;

/// The `Runner` owns the main UI thread, and starts a tokio runtime
/// for interaction with downstream services.
pub struct Runner {
    /// The UI that handles input events and renders widgets to the screen
    screen: Screen,

    // The [`Runner`]'s main_loop is purely single threaded. Every interaction
    // with the outside world is via channels. All input from the outside world
    // comes in via an `Event` over a single channel.
    events_rx: UnboundedReceiver<Event>,

    // We save a copy here so we can hand it out to event producers
    events_tx: UnboundedSender<Event>,

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

    // A recorder for debugging the history of events for use in a debugger.
    recorder: Recorder,
}

#[allow(clippy::new_without_default)]
impl Runner {
    pub fn new(log: slog::Logger, wicketd_addr: SocketAddrV6) -> Runner {
        let (events_tx, events_rx) = unbounded_channel();
        let state = State::new(&log);
        let backend = CrosstermBackend::new(stdout());
        let terminal = Terminal::new(backend).unwrap();
        let tokio_rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let (wicketd, wicketd_manager) =
            WicketdManager::new(&log, events_tx.clone(), wicketd_addr);
        Runner {
            screen: Screen::new(&log),
            events_rx,
            events_tx,
            state,
            wicketd,
            wicketd_manager: Some(wicketd_manager),
            terminal,
            log,
            tokio_rt,
            recorder: Recorder::new(MAX_RECORDED_EVENTS),
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
            // unwrap is safe because we always hold onto a UnboundedSender
            let event = self.events_rx.blocking_recv().unwrap();
            self.recorder.push(&self.state, event.clone());
            match event {
                Event::Tick => {
                    // We want to periodically to update the status bar
                    // By default this is every 1s.
                    let redraw =
                        self.state.service_status.advance_all(TICK_INTERVAL);
                    let action = self.screen.on(&mut self.state, Cmd::Tick);
                    let already_drawn =
                        action.as_ref().map_or(false, |a| a.should_redraw());
                    self.handle_action(action)?;
                    if redraw && !already_drawn {
                        self.screen.draw(&self.state, &mut self.terminal)?;
                    }
                }
                Event::Term(cmd) => {
                    let action = self.screen.on(&mut self.state, cmd);
                    self.handle_action(action)?;
                }
                Event::Resize { width, height } => {
                    self.screen.resize(&mut self.state, width, height);
                    self.screen.draw(&self.state, &mut self.terminal)?;
                }
                Event::Inventory { inventory, mgs_last_seen } => {
                    self.state.service_status.reset_mgs(mgs_last_seen);
                    self.state.service_status.reset_wicketd(Duration::ZERO);
                    self.state.inventory.update_inventory(inventory)?;
                    self.screen.draw(&self.state, &mut self.terminal)?;
                }
                Event::UpdateArtifacts { system_version, artifacts } => {
                    self.state.service_status.reset_wicketd(Duration::ZERO);
                    self.state
                        .update_state
                        .update_artifacts(system_version, artifacts);
                    self.screen.draw(&self.state, &mut self.terminal)?;
                }
                Event::UpdateLog(logs) => {
                    self.state.service_status.reset_wicketd(Duration::ZERO);
                    debug!(self.log, "{:#?}", logs);
                    self.state.update_state.update_logs(logs);
                    self.screen.draw(&self.state, &mut self.terminal)?;
                }
                Event::Shutdown => break,
            }
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
            Action::Update(component_id) => {
                self.wicketd.tx.blocking_send(
                    wicketd::Request::StartUpdate(component_id),
                )?;
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
async fn run_event_listener(
    log: slog::Logger,
    events_tx: UnboundedSender<Event>,
) {
    info!(log, "Starting event listener");
    tokio::spawn(async move {
        let mut events = EventStream::new();
        let mut ticker = interval(TICK_INTERVAL);
        let mut key_handler = KeyHandler::default();
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

                    let event = match event {
                        TermEvent::Key(key_event) => {
                            if is_control_c(&key_event) {
                                info!(log, "CTRL-C Pressed. Exiting.");
                                Some(Event::Shutdown)
                            } else {
                                if let Some(cmd) = key_handler.on(key_event) {
                                    Some(Event::Term(cmd))
                                } else {
                                    None
                                }
                            }
                        }
                        TermEvent::Resize(width, height) => {
                            Some(Event::Resize{width, height})
                        }
                         _ => None
                    };

                    if let Some(event) = event {
                        if events_tx.send(event).is_err() {
                            info!(log, "Event listener completed");
                            // The receiver was dropped. Program is ending.
                            return;
                        }
                    }
                }
            }
        }
    });
}
