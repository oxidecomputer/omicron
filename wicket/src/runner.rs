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
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use slog::Logger;
use slog::{debug, error, info};
use std::io::{stdout, Stdout};
use std::net::SocketAddrV6;
use std::time::Instant;
use tokio::sync::mpsc::{
    unbounded_channel, UnboundedReceiver, UnboundedSender,
};
use tokio::time::{interval, Duration};
use wicket_common::rack_update::AbortUpdateOptions;

use crate::events::EventReportMap;
use crate::helpers::get_update_test_error;
use crate::state::CreateClearUpdateStateOptions;
use crate::state::CreateStartUpdateOptions;
use crate::ui::Screen;
use crate::wicketd::{self, WicketdHandle, WicketdManager};
use crate::{Action, Cmd, Event, KeyHandler, Recorder, State, TICK_INTERVAL};

// We can avoid a bunch of unnecessary type parameters by picking them ahead of time.
pub type Term = Terminal<CrosstermBackend<Stdout>>;

const MAX_RECORDED_EVENTS: usize = 10000;

/// The core of a runner that handles events and redraws the screen
///
/// The `RunnerCore` can be used by both the `Runner` and debugger
pub struct RunnerCore {
    /// The UI that handles input events and renders widgets to the screen
    pub screen: Screen,

    // All global state managed by wicket.
    //
    // This state is updated from user input and events from downstream
    // services.
    pub state: State,

    // The terminal we are rendering to
    pub terminal: Term,

    // Our friendly neighborhood logger
    pub log: slog::Logger,

    // Helper to limit our logging of event reports (which can be quite large)
    // to a slower cadence than their arrival.
    log_throttler: EventReportLogThrottler,
}

impl RunnerCore {
    pub fn new(log: Logger) -> Self {
        Self {
            screen: Screen::new(&log),
            state: State::new(),
            terminal: Terminal::new(CrosstermBackend::new(stdout())).unwrap(),
            log,
            log_throttler: EventReportLogThrottler::default(),
        }
    }

    /// Resize and draw the initial screen before handling `Event`s
    pub fn init_screen(&mut self) -> anyhow::Result<()> {
        // Size the initial screen
        let rect = self.terminal.get_frame().size();
        self.screen.resize(&mut self.state, rect.width, rect.height);

        // Draw the initial screen
        self.screen.draw(&self.state, &mut self.terminal)
    }

    /// Handle an individual `Event`
    ///
    /// Return true on `Event::Shutdown`, false otherwise.
    pub fn handle_event(
        &mut self,
        event: Event,
        recorder: Option<&mut Recorder>,
        wicketd: Option<&WicketdHandle>,
    ) -> anyhow::Result<bool> {
        match event {
            Event::Tick => {
                // We want to periodically to update the status bar
                // By default this is every 1s.
                let redraw =
                    self.state.service_status.advance_all(TICK_INTERVAL);
                let action = self.screen.on(&mut self.state, Cmd::Tick);
                let already_drawn =
                    action.as_ref().map_or(false, |a| a.should_redraw());
                self.handle_action(action, wicketd)?;
                if redraw && !already_drawn {
                    self.screen.draw(&self.state, &mut self.terminal)?;
                }
            }
            Event::Term(cmd) => {
                if cmd == Cmd::DumpSnapshot {
                    // TODO: Show a graphical indicator?
                    if let Some(recorder) = recorder {
                        if let Err(e) = recorder.dump() {
                            error!(self.log, "{}", e);
                        }
                    }
                } else {
                    let action = self.screen.on(&mut self.state, cmd);
                    self.handle_action(action, wicketd)?;
                }
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
            Event::ArtifactsAndEventReports {
                system_version,
                artifacts,
                event_reports,
            } => {
                self.state.service_status.reset_wicketd(Duration::ZERO);
                self.log_throttler.log_event_report(&event_reports, &self.log);
                self.state.update_state.update_artifacts_and_reports(
                    &self.log,
                    system_version,
                    artifacts,
                    event_reports,
                );
                self.screen.draw(&self.state, &mut self.terminal)?;
            }
            Event::RssConfig(config) => {
                self.state.rss_config = Some(config);
                self.screen.draw(&self.state, &mut self.terminal)?;
            }
            Event::RackSetupStatus(result) => {
                self.state.rack_setup_state = result;
                self.screen.draw(&self.state, &mut self.terminal)?;
            }
            Event::WicketdLocation(location) => {
                self.state.wicketd_location = location;
                self.screen.draw(&self.state, &mut self.terminal)?;
            }
            Event::Shutdown => return Ok(true),
        }
        Ok(false)
    }

    fn handle_action(
        &mut self,
        action: Option<Action>,
        wicketd: Option<&WicketdHandle>,
    ) -> anyhow::Result<()> {
        let Some(action) = action else {
            return Ok(());
        };

        match action {
            Action::Redraw => {
                self.screen.draw(&self.state, &mut self.terminal)?;
            }
            Action::StartUpdate(component_id) => {
                if let Some(wicketd) = wicketd {
                    let options = CreateStartUpdateOptions {
                        force_update_rot_bootloader: self
                            .state
                            .force_update_state
                            .force_update_rot_bootloader,
                        force_update_rot: self
                            .state
                            .force_update_state
                            .force_update_rot,
                        force_update_sp: self
                            .state
                            .force_update_state
                            .force_update_sp,
                    }
                    .to_start_update_options()?;

                    wicketd.tx.blocking_send(
                        wicketd::Request::StartUpdate { component_id, options },
                    )?;
                }
            }
            Action::AbortUpdate(component_id) => {
                if let Some(wicketd) = wicketd {
                    let test_error = get_update_test_error(
                        "WICKET_TEST_ABORT_UPDATE_ERROR",
                    )?;

                    let options = AbortUpdateOptions {
                        message: "Aborted by wicket user".to_owned(),
                        test_error,
                    };
                    wicketd.tx.blocking_send(
                        wicketd::Request::AbortUpdate { component_id, options },
                    )?;
                }
            }
            Action::ClearUpdateState(component_id) => {
                if let Some(wicketd) = wicketd {
                    let options = CreateClearUpdateStateOptions {};
                    let options = options.to_clear_update_state_options()?;
                    wicketd.tx.blocking_send(
                        wicketd::Request::ClearUpdateState {
                            component_id,
                            options,
                        },
                    )?;
                }
            }
            Action::Ignition(component_id, ignition_command) => {
                if let Some(wicketd) = wicketd {
                    wicketd.tx.blocking_send(
                        wicketd::Request::IgnitionCommand(
                            component_id,
                            ignition_command,
                        ),
                    )?;
                }
            }
            Action::StartRackSetup => {
                if let Some(wicketd) = wicketd {
                    wicketd
                        .tx
                        .blocking_send(wicketd::Request::StartRackSetup)?;
                }
            }
            Action::StartRackReset => {
                if let Some(wicketd) = wicketd {
                    wicketd
                        .tx
                        .blocking_send(wicketd::Request::StartRackReset)?;
                }
            }
        }
        Ok(())
    }
}

/// The `Runner` owns the main UI thread, and starts a tokio runtime
/// for interaction with downstream services.
pub struct Runner {
    core: RunnerCore,

    // The [`Runner`]'s main_loop is purely single threaded. Every interaction
    // with the outside world is via channels. All input from the outside world
    // comes in via an `Event` over a single channel.
    events_rx: UnboundedReceiver<Event>,

    // We save a copy here so we can hand it out to event producers
    events_tx: UnboundedSender<Event>,

    // A mechanism for interacting with `wicketd`
    #[allow(unused)]
    wicketd: WicketdHandle,

    // When [`Runner::run`] is called, this is extracted and moved into a tokio
    // task.
    wicketd_manager: Option<WicketdManager>,

    // The tokio runtime for everything outside the main thread
    tokio_rt: tokio::runtime::Runtime,

    // A recorder for debugging the history of events for use in a debugger.
    recorder: Recorder,
}

#[allow(clippy::new_without_default)]
impl Runner {
    pub fn new(log: slog::Logger, wicketd_addr: SocketAddrV6) -> Runner {
        let (events_tx, events_rx) = unbounded_channel();
        let tokio_rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let (wicketd, wicketd_manager) =
            WicketdManager::new(&log, events_tx.clone(), wicketd_addr);
        let core = RunnerCore::new(log);
        Runner {
            core,
            events_rx,
            events_tx,
            wicketd,
            wicketd_manager: Some(wicketd_manager),
            tokio_rt,
            recorder: Recorder::new(MAX_RECORDED_EVENTS),
        }
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        self.start_tokio_runtime();
        enable_raw_mode()?;
        execute!(self.core.terminal.backend_mut(), EnterAlternateScreen,)?;
        self.main_loop()?;
        disable_raw_mode()?;
        execute!(self.core.terminal.backend_mut(), LeaveAlternateScreen,)?;
        Ok(())
    }

    fn main_loop(&mut self) -> anyhow::Result<()> {
        info!(self.core.log, "Starting main loop");
        self.core.init_screen()?;

        loop {
            // unwrap is safe because we always hold onto a UnboundedSender
            let event = self.events_rx.blocking_recv().unwrap();
            self.recorder.push(&self.core.state, event.clone());
            if self.core.handle_event(
                event,
                Some(&mut self.recorder),
                Some(&mut self.wicketd),
            )? {
                // Event::Shutdown received
                break;
            }
        }
        Ok(())
    }

    fn start_tokio_runtime(&mut self) {
        let events_tx = self.events_tx.clone();
        let log = self.core.log.clone();
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

struct EventReportLogThrottler {
    last_log: Option<Instant>,
    min_time_between_logs: Duration,
}

impl Default for EventReportLogThrottler {
    fn default() -> Self {
        const DEFAULT_TIME_BETWEEN_LOGS: Duration = Duration::from_secs(15);
        Self::new(DEFAULT_TIME_BETWEEN_LOGS)
    }
}

impl EventReportLogThrottler {
    fn new(min_time_between_logs: Duration) -> Self {
        Self { last_log: None, min_time_between_logs }
    }

    fn log_event_report(
        &mut self,
        event_report: &EventReportMap,
        log: &slog::Logger,
    ) {
        let should_log_full_report = self
            .last_log
            .map(|last| last.elapsed() >= self.min_time_between_logs)
            .unwrap_or(true);

        if should_log_full_report {
            debug!(
                log,
                "received event reports for {} sleds",
                event_report.len();
                "details" => format!("{:#?}", event_report),
            );
            self.last_log = Some(Instant::now());
        } else {
            debug!(
                log,
                "received event reports for {} sleds",
                event_report.len();
                "details" => format!(
                    "(omitted; only logged every {:?})",
                    self.min_time_between_logs,
                ),
            );
        }
    }
}
