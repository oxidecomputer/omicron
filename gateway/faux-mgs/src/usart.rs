// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use anyhow::Context;
use anyhow::Result;
use gateway_messages::SpComponent;
use gateway_sp_comms::AttachedSerialConsoleSend;
use gateway_sp_comms::SingleSp;
use slog::trace;
use slog::Logger;
use std::io;
use std::io::Write;
use std::mem;
use std::os::unix::prelude::AsRawFd;
use std::time::Duration;
use termios::Termios;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;

const CTRL_A: u8 = b'\x01';
const CTRL_C: u8 = b'\x03';

pub(crate) async fn run(
    sp: SingleSp,
    raw: bool,
    stdin_buffer_time: Duration,
    log: Logger,
) -> Result<()> {
    // Put terminal in raw mode, if requested, with a guard to restore it.
    let _guard =
        if raw { Some(UnrawTermiosGuard::make_stdout_raw()?) } else { None };

    let mut stdin = tokio::io::stdin();
    let mut stdin_buf = Vec::with_capacity(64);
    let mut out_buf = StdinOutBuf::new(raw);
    let mut flush_delay = FlushDelay::new(stdin_buffer_time);
    let console = sp
        .serial_console_attach(SpComponent::SP3_HOST_CPU)
        .await
        .with_context(|| "failed to attach to serial console")?;

    let (console_tx, mut console_rx) = console.split();
    let (send_tx, send_rx) = mpsc::channel(8);
    let tx_to_sp_handle = tokio::spawn(async move {
        relay_data_to_sp(console_tx, send_rx).await.unwrap();
    });

    loop {
        tokio::select! {
            result = stdin.read_buf(&mut stdin_buf) => {
                let n = result.with_context(|| "failed to read from stdin")?;
                if n == 0 {
                    mem::drop(send_tx);
                    tx_to_sp_handle.await.unwrap();
                    return Ok(());
                }

                match out_buf.ingest(&mut stdin_buf) {
                    IngestResult::Ok => (),
                    IngestResult::Exit => return Ok(()),
                }

                flush_delay.start_if_unstarted().await;
            }

            chunk = console_rx.recv() => {
                let chunk = chunk.unwrap();
                trace!(log, "writing {chunk:?} data to stdout");
                let mut stdout = io::stdout().lock();
                stdout.write_all(&chunk).unwrap();
                stdout.flush().unwrap();
            }

            _ = flush_delay.ready() => {
                send_tx
                    .send(out_buf.steal_buf())
                    .await
                    .with_context(|| "failed to send data (task shutdown?)")?;
            }
        }
    }
}

async fn relay_data_to_sp(
    mut console_tx: AttachedSerialConsoleSend,
    mut data_rx: mpsc::Receiver<Vec<u8>>,
) -> Result<()> {
    while let Some(data) = data_rx.recv().await {
        console_tx.write(data).await?;
    }
    console_tx.detach().await?;

    Ok(())
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
        termios::tcsetattr(stdout, termios::TCSAFLUSH, &ios)
            .with_context(|| "failed to set termios on stdout")?;
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
    next_raw: bool,
    buf: Vec<u8>,
}

enum IngestResult {
    Ok,
    Exit,
}

impl StdinOutBuf {
    fn new(raw_mode: bool) -> Self {
        Self { raw_mode, next_raw: false, buf: Vec::new() }
    }

    fn ingest(&mut self, buf: &mut Vec<u8>) -> IngestResult {
        if !self.raw_mode {
            self.buf.append(buf);
            return IngestResult::Ok;
        }

        for c in buf.drain(..) {
            match c {
                // Ctrl-A means send next one raw
                CTRL_A => {
                    if self.next_raw {
                        // Ctrl-A Ctrl-A should be sent as Ctrl-A
                        self.buf.push(c);
                        self.next_raw = false;
                    } else {
                        self.next_raw = true;
                    }
                }
                CTRL_C => {
                    if !self.next_raw {
                        // Exit on non-raw Ctrl-C
                        return IngestResult::Exit;
                    } else {
                        // Otherwise send Ctrl-C
                        self.buf.push(c);
                        self.next_raw = false;
                    }
                }
                _ => {
                    self.buf.push(c);
                    self.next_raw = false;
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
