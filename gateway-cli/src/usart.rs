// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

use crate::picocom_map::RemapRules;
use anyhow::Context;
use anyhow::Result;
use futures::SinkExt;
use futures::StreamExt;
use reqwest::Upgraded;
use std::borrow::Cow;
use std::fs::File;
use std::io;
use std::io::Write;
use std::mem;
use std::os::unix::prelude::AsRawFd;
use std::path::PathBuf;
use std::time::Duration;
use termios::Termios;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const CTRL_A: u8 = b'\x01';
const CTRL_X: u8 = b'\x18';

pub(crate) async fn run(
    mut ws: WebSocketStream<Upgraded>,
    raw: bool,
    stdin_buffer_time: Duration,
    imap: Option<String>,
    omap: Option<String>,
    uart_logfile: Option<PathBuf>,
) -> Result<()> {
    // Put terminal in raw mode, if requested, with a guard to restore it.
    let _guard =
        if raw { Some(UnrawTermiosGuard::make_stdout_raw()?) } else { None };

    // Parse imap/omap strings.
    let imap = match imap {
        Some(s) => s.parse().context("invalid imap rules")?,
        None => RemapRules::default(),
    };
    let omap = match omap {
        Some(s) => s.parse().context("invalid omap rules")?,
        None => RemapRules::default(),
    };

    // Open uart logfile, if requested.
    let mut uart_logfile = match uart_logfile {
        Some(path) => {
            let f = File::options()
                .append(true)
                .create(true)
                .open(&path)
                .with_context(|| {
                    format!("failed to open {}", path.display())
                })?;
            Some(f)
        }
        None => None,
    };

    let mut stdin = tokio::io::stdin();
    let mut stdin_buf = Vec::with_capacity(64);
    let mut out_buf = StdinOutBuf::new(omap, raw);
    let mut flush_delay = FlushDelay::new(stdin_buffer_time);

    loop {
        tokio::select! {
            result = stdin.read_buf(&mut stdin_buf) => {
                let n = result.context("failed to read from stdin")?;

                let result = if n == 0 {
                    IngestResult::Exit
                } else {
                    out_buf.ingest(&mut stdin_buf)
                };

                match result {
                    IngestResult::Ok => (),
                    IngestResult::Exit => {
                        _ = ws.close(Some(CloseFrame {
                            code: CloseCode::Normal,
                            reason: Cow::Borrowed("client closed stdin"),
                        })).await;
                        return Ok(());
                    }
                }

                flush_delay.start_if_unstarted().await;
            }

            message = ws.next() => {
                let chunk = match message {
                    Some(Ok(Message::Binary(chunk))) => chunk,
                    Some(Ok(Message::Close(..))) | None => {
                        return Ok(());
                    }
                    _ => {
                        // TODO should we log an unexpected websocket message?
                        continue;
                    }
                };

                if let Some(uart_logfile) = uart_logfile.as_mut() {
                    uart_logfile
                        .write_all(&chunk)
                        .context("failed to write to logfile")?;
                }

                let data = imap.apply(chunk).collect::<Vec<_>>();

                let mut stdout = io::stdout().lock();
                stdout.write_all(&data).context("failed to write to stdout")?;
                stdout.flush().context("failed to flush stdout")?;
            }

            _ = flush_delay.ready() => {
                ws.send(Message::Binary(out_buf.steal_buf()))
                    .await
                    .context("failed to send data on websocket")?;
            }
        }
    }
}

struct UnrawTermiosGuard {
    stdout: i32,
    ios: Termios,
}

impl Drop for UnrawTermiosGuard {
    fn drop(&mut self) {
        termios::tcsetattr(self.stdout, termios::TCSAFLUSH, &self.ios).unwrap();
    }
}

impl UnrawTermiosGuard {
    fn make_stdout_raw() -> Result<Self> {
        let stdout = io::stdout().as_raw_fd();
        let mut ios = Termios::from_fd(stdout)
            .with_context(|| "could not get termios for stdout")?;
        let orig_ios = ios;
        termios::cfmakeraw(&mut ios);
        termios::tcsetattr(stdout, termios::TCSANOW, &ios)
            .with_context(|| "failed to set TCSANOW on stdout")?;
        termios::tcflush(stdout, termios::TCIOFLUSH)
            .with_context(|| "failed to set TCIOFLUSH on stdout")?;
        Ok(Self { stdout, ios: orig_ios })
    }
}

struct FlushDelay {
    started: bool,
    tx: mpsc::Sender<()>,
    rx: mpsc::Receiver<()>,
}

impl FlushDelay {
    fn new(duration: Duration) -> Self {
        let (tx0, mut rx0) = mpsc::channel(1);
        let (tx1, rx1) = mpsc::channel(1);
        tokio::spawn(async move {
            loop {
                match rx0.recv().await {
                    Some(()) => (),
                    None => return,
                }

                tokio::time::sleep(duration).await;

                let _ = tx1.send(()).await;
            }
        });
        Self { started: false, tx: tx0, rx: rx1 }
    }

    async fn start_if_unstarted(&mut self) {
        if !self.started {
            self.started = true;
            self.tx.send(()).await.unwrap();
        }
    }

    async fn ready(&mut self) {
        self.rx.recv().await.unwrap();
        self.started = false;
    }
}

struct StdinOutBuf {
    raw_mode: bool,
    in_prefix: bool,
    remap: RemapRules,
    buf: Vec<u8>,
}

enum IngestResult {
    Ok,
    Exit,
}

impl StdinOutBuf {
    fn new(remap: RemapRules, raw_mode: bool) -> Self {
        Self { raw_mode, in_prefix: false, remap, buf: Vec::new() }
    }

    fn ingest(&mut self, buf: &mut Vec<u8>) -> IngestResult {
        let buf = self.remap.apply(buf.drain(..));

        if !self.raw_mode {
            self.buf.extend(buf);
            return IngestResult::Ok;
        }

        for c in buf {
            match c {
                CTRL_A => {
                    if self.in_prefix {
                        // Ctrl-A Ctrl-A should be sent as Ctrl-A
                        self.buf.push(c);
                        self.in_prefix = false;
                    } else {
                        self.in_prefix = true;
                    }
                }
                CTRL_X => {
                    if self.in_prefix {
                        // Exit on Ctrl-A Ctrl-X
                        return IngestResult::Exit;
                    } else {
                        self.buf.push(c);
                    }
                }
                _ => {
                    self.buf.push(c);
                    self.in_prefix = false;
                }
            }
        }

        IngestResult::Ok
    }

    fn steal_buf(&mut self) -> Vec<u8> {
        let mut stolen = Vec::new();
        mem::swap(&mut stolen, &mut self.buf);
        stolen
    }
}
