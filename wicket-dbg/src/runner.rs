// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An executor for [`wicket::Event`]s used by `wicket-dbg-server.rs`

use crate::{Cmd, DebugState, Rpy};
use camino::Utf8PathBuf;
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode,
    enable_raw_mode,
};
use slog::Logger;
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::fs::read;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot,
};
use tokio::time::sleep;
use wicket::{Event, RunnerCore, Screen, Snapshot, State, TICK_INTERVAL};

// Max speedup/slowdown factor for playback rate
const MAX_PLAYBACK_FACTOR: u32 = 5;

pub struct RunnerHandle {
    tx: Sender<(Cmd, oneshot::Sender<Rpy>)>,
}

impl RunnerHandle {
    /// Send a `Cmd` to the Runner, wait for a `Rpy`, and return the `Rpy` to
    /// the caller.
    pub async fn send(&self, cmd: Cmd) -> Rpy {
        let (tx, rx) = oneshot::channel();
        self.tx.send((cmd, tx)).await.unwrap();
        rx.await.unwrap()
    }
}

/// A parallel to [`wicket::Runner`] that allows stepping through individual
/// events, and doesn't interact with any external services. The only goal
/// is to replay the visuals of wicket, given a recording.
pub struct Runner {
    rx: Receiver<(Cmd, oneshot::Sender<Rpy>)>,
    core: RunnerCore,
    snapshot: Option<Snapshot>,
    path: Option<Utf8PathBuf>,
    current_event: usize,
    breakpoints: BTreeSet<u32>,
    is_playing: bool,
    event_rx: Receiver<Event>,
    event_tx: Sender<Event>,
    on_breakpoint: bool,

    /// The followin three fields are updated together
    playback_speedup: Option<u32>,
    playback_slowdown: Option<u32>,
    tick_interval: Duration,
}

impl Runner {
    pub fn new(log: Logger) -> (Runner, RunnerHandle) {
        let core = RunnerCore::new(log);
        // We only allow one request at a time
        let (tx, rx) = mpsc::channel(1);
        let (event_tx, event_rx) = mpsc::channel(1);
        (
            Runner {
                rx,
                core,
                snapshot: None,
                path: None,
                current_event: 0,
                breakpoints: BTreeSet::new(),
                is_playing: false,
                event_tx,
                event_rx,
                on_breakpoint: false,
                playback_speedup: None,
                playback_slowdown: None,
                tick_interval: TICK_INTERVAL,
            },
            RunnerHandle { tx },
        )
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some((cmd, tx)) = self.rx.recv() => {
                    let rpy = match cmd {
                        Cmd::Load(path) => self.load(path).await,
                        Cmd::Run { speedup, slowdown } => self.play(speedup, slowdown).await,
                        Cmd::Pause => {
                            self.is_playing = false;
                            Rpy::Ok
                        }
                        Cmd::Resume => {
                            let rpy = self.step_while_playing().await;
                            if !rpy.is_err() {
                                self.is_playing = true;
                            }
                            rpy
                        }

                        Cmd::Reset => self.restart(),
                        Cmd::GetState => Rpy::State(Box::new(self.core.state.clone())),
                        Cmd::Break{event} => {
                            self.breakpoints.insert(event);
                            Rpy::Ok
                        }
                        Cmd::GetEvent => {
                            Rpy::Event(self.current_event)
                        }
                        Cmd::Step => {
                            self.step().await
                        }
                        Cmd::Jump => {
                            self.jump().await
                        }
                        Cmd::DebugState => {
                            self.debug_state()
                        }
                    };
                    let _ = tx.send(rpy);
                }
                Some(event) = self.event_rx.recv() => {
                    if event.is_tick() {
                        sleep(self.tick_interval).await;
                    }
                    if let Err(_e) = self.core.handle_event(event, None, None) {
                        // TODO: Logging?
                    }
                    if self.is_playing {
                        self.step_while_playing().await;
                    }
                }
            }
        }
    }

    pub fn debug_state(&self) -> Rpy {
        Rpy::Debug(DebugState {
            snapshot_path: self.path.clone(),
            snapshot_start_event: self.snapshot.as_ref().map(|s| s.start),
            total_events: self.snapshot.as_ref().map(|s| s.history.len()),
            current_event: self.current_event,
            breakpoints: self.breakpoints.clone(),
            is_playing: self.is_playing,
            playback_speedup: self.playback_speedup,
            playback_slowdown: self.playback_slowdown,
            tick_interval: self.tick_interval,
        })
    }

    pub async fn step(&mut self) -> Rpy {
        let Some(snapshot) = &self.snapshot else {
            return Rpy::Err("Please load a wicket recording".to_string());
        };

        if self.is_playing {
            return Rpy::Err("Cannot step while running".to_string());
        }

        if snapshot.history.len() == self.current_event {
            Rpy::Event(self.current_event)
        } else {
            let event = snapshot.history[self.current_event].clone();
            if let Err(e) = self.core.handle_event(event, None, None) {
                return Rpy::Err(format!("Runner error: {e}"));
            }
            self.current_event += 1;
            Rpy::Event(self.current_event)
        }
    }

    pub async fn jump(&mut self) -> Rpy {
        let Some(snapshot) = &self.snapshot else {
            return Rpy::Err("Please load a wicket recording".to_string());
        };

        if self.is_playing {
            return Rpy::Err("Cannot jump while running".to_string());
        }

        loop {
            if snapshot.history.len() == self.current_event {
                return Rpy::Event(self.current_event);
            } else {
                let event = snapshot.history[self.current_event].clone();
                let is_tick = event.is_tick();
                if let Err(e) = self.core.handle_event(event, None, None) {
                    return Rpy::Err(format!("Runner error: {e}"));
                }
                self.current_event += 1;
                if !is_tick {
                    return Rpy::Event(self.current_event);
                }
            }
        }
    }

    pub async fn step_while_playing(&mut self) -> Rpy {
        let Some(snapshot) = &self.snapshot else {
            return Rpy::Err("Please load a wicket recording".to_string());
        };
        self.current_event += 1;

        if snapshot.history.len() == self.current_event {
            self.is_playing = false;
            let _ = self.restart();
        } else {
            if !self.on_breakpoint
                && self
                    .breakpoints
                    .contains(&self.current_event.try_into().unwrap())
            {
                self.is_playing = false;
                self.on_breakpoint = true;
                // We don't want to skip the event
                self.current_event = self.current_event.saturating_sub(1);
                return Rpy::Ok;
            }
            self.on_breakpoint = false;
            let event = snapshot.history[self.current_event].clone();
            // unwrap is safe, as self holds both sides of the channel
            self.event_tx.send(event).await.unwrap();
        }
        Rpy::Ok
    }

    pub async fn load(&mut self, path: Utf8PathBuf) -> Rpy {
        let contents = match read(&path).await {
            Ok(contents) => contents,
            Err(e) => return Rpy::Err(format!("{e}: {}", path)),
        };
        match ciborium::de::from_reader(&contents[..]) {
            Ok(snapshot) => {
                self.path = Some(path);
                self.snapshot = Some(snapshot);
                Rpy::Ok
            }
            Err(e) => Rpy::Err(format!("Failed to deserialize recording: {e}")),
        }
    }

    pub fn has_snapshot(&self) -> bool {
        self.snapshot.is_some()
    }

    /// Initialize the terminal for drawing
    pub fn init_terminal(&mut self) -> anyhow::Result<()> {
        enable_raw_mode()?;
        execute!(self.core.terminal.backend_mut(), EnterAlternateScreen,)?;
        Ok(())
    }

    /// Restore the terminal to its original state
    pub fn fini_terminal(&mut self) -> anyhow::Result<()> {
        disable_raw_mode()?;
        execute!(self.core.terminal.backend_mut(), LeaveAlternateScreen,)?;
        Ok(())
    }

    /// Load a new snapshot
    pub fn load_snapshot(&mut self, snapshot: Snapshot) {
        self.snapshot = Some(snapshot);
    }

    /// Restart the debugger
    pub fn restart(&mut self) -> Rpy {
        self.core.screen = Screen::new(&self.core.log);
        self.core.state = State::new();
        if let Err(e) = self.core.init_screen() {
            Rpy::Err(format!("Failed to init screen: {e}"))
        } else {
            Rpy::Ok
        }
    }

    /// Play the recording
    pub async fn play(
        &mut self,
        speedup: Option<u32>,
        slowdown: Option<u32>,
    ) -> Rpy {
        if self.is_playing {
            return Rpy::Err("Playback is already running".to_string());
        }

        let Some(snapshot) = &self.snapshot else {
            return Rpy::Err("Please load a wicket recording".to_string());
        };

        if let Err(e) = self.core.init_screen() {
            return Rpy::Err(format!("Failed to init screen: {e}"));
        }

        if let Some(s) = speedup {
            let s = u32::min(MAX_PLAYBACK_FACTOR, s);
            self.playback_speedup = Some(s);
            self.playback_slowdown = None;
            self.tick_interval = TICK_INTERVAL.checked_div(s).unwrap();
        } else if let Some(s) = slowdown {
            let s = u32::min(MAX_PLAYBACK_FACTOR, s);
            self.playback_slowdown = Some(s);
            self.playback_speedup = None;
            self.tick_interval = TICK_INTERVAL.saturating_mul(s);
        } else {
            self.playback_speedup = None;
            self.playback_slowdown = None;
            self.tick_interval = TICK_INTERVAL;
        }

        self.is_playing = true;
        self.current_event = 0;
        let event = snapshot.history[0].clone();
        // unwrap is safe, as self holds both sides of the channel
        self.event_tx.send(event).await.unwrap();
        Rpy::Ok
    }
}
