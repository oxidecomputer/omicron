// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use anyhow::Context;
use anyhow::Result;
use crossbeam_channel::select;
use crossbeam_channel::Sender;
use gateway_messages::sp_impl::SerialConsolePacketizer;
use gateway_messages::RequestKind;
use gateway_messages::ResponseKind;
use gateway_messages::SpComponent;
use gateway_messages::SpMessage;
use gateway_messages::SpMessageKind;
use mio::unix::SourceFd;
use mio::Events;
use mio::Interest;
use mio::Poll;
use mio::Token;
use slog::debug;
use slog::error;
use slog::trace;
use slog::warn;
use slog::Logger;
use std::io;
use std::io::Write;
use std::net::SocketAddrV6;
use std::net::UdpSocket;
use std::os::unix::prelude::AsRawFd;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use termios::Termios;

use crate::RecvError;

const CTRL_A: u8 = b'\x01';
const CTRL_C: u8 = b'\x03';

pub(crate) fn run(
    log: Logger,
    socket: UdpSocket,
    sp: SocketAddrV6,
    raw: bool,
) -> Result<()> {
    // Put terminal in raw mode, if requested, with a guard to restore it.
    let _guard = if raw {
        let stdout = io::stdout().as_raw_fd();
        let mut ios = Termios::from_fd(stdout)
            .with_context(|| "could not get termios for stdout")?;
        let orig_ios = ios;
        termios::cfmakeraw(&mut ios);
        termios::tcsetattr(stdout, termios::TCSAFLUSH, &ios)
            .with_context(|| "failed to set termios on stdout")?;
        Some(UnrawTermiosGuard { stdout, ios: orig_ios })
    } else {
        None
    };

    let socket = Arc::new(socket);
    let rx_sp_msg = {
        let (tx, rx) = crossbeam_channel::bounded(16);
        let log = log.clone();
        let socket = Arc::clone(&socket);
        thread::spawn(move || recv_task(log, socket, tx));
        rx
    };
    let rx_stdin = {
        let (tx, rx) = crossbeam_channel::bounded(16);
        let log = log.clone();
        thread::spawn(move || stdin_task(log, raw, tx));
        rx
    };

    let mut packetizer =
        SerialConsolePacketizer::new(SpComponent::try_from("sp3").unwrap());
    loop {
        select! {
            recv(rx_sp_msg) -> msg => {
                // recv_task() runs as long as we're holding `rx_sp_msg`, so we
                // can unwrap here.
                handle_sp_message(&log, msg.unwrap(), None);
            }

            recv(rx_stdin) -> result => {
                let data = match result {
                    Ok(line) => line,
                    Err(_) => break,
                };

                for chunk in packetizer.packetize(&data) {
                    let request_id = crate::send_request(
                        &log,
                        &socket,
                        sp,
                        RequestKind::SerialConsoleWrite(chunk),
                    )?;

                    // Don't read any more from our stdin until we've gotten
                    // acks that this line as been received.
                    for msg in rx_sp_msg.iter() {
                        if handle_sp_message(&log, msg, Some(request_id)) {
                            break;
                        }
                    }
                }
            }
        }
    }

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

/// If `request_id` is `Some(n)`, returns `true` if `msg` contains a serial
/// console ack of that request id. Returns `false` in all other cases.
fn handle_sp_message(
    log: &Logger,
    msg: SpMessage,
    request_id: Option<u32>,
) -> bool {
    match msg.kind {
        SpMessageKind::Response { request_id: response_id, result } => {
            match result {
                Ok(ResponseKind::SerialConsoleWriteAck) => {
                    if Some(response_id) == request_id {
                        debug!(log, "received expected ack");
                        return true;
                    } else {
                        debug!(log, "received unexpected ack");
                    }
                }
                Ok(other) => {
                    debug!(
                        log, "ignoring unexpected message";
                        "message" => ?other,
                    );
                }
                Err(err) => {
                    warn!(log, "received error response"; "err" => %err);
                }
            }
        }
        SpMessageKind::SerialConsole(chunk) => {
            trace!(log, "writing {chunk:?} data to stdout");
            let mut stdout = io::stdout().lock();
            stdout.write_all(&chunk.data[..usize::from(chunk.len)]).unwrap();
            stdout.flush().unwrap();
        }
    }

    false
}

fn stdin_task(log: Logger, raw: bool, tx: Sender<Vec<u8>>) {
    const BUFFER_DELAY: Duration = Duration::from_millis(1000);

    let mut stdin = io::stdin().lock();
    let stdin_fd = stdin.as_raw_fd();

    let mut poll = Poll::new().expect("could not create mio Poll");
    let mut events = Events::with_capacity(16);
    let mut source = SourceFd(&stdin_fd);

    const STDIN: Token = Token(0);
    poll.registry()
        .register(&mut source, STDIN, Interest::READABLE)
        .expect("could not register readable interest for stdin");

    let mut flush_deadline: Option<Instant> = None;
    let mut buf = InOutBuf::new(raw);
    loop {
        let mut timeout =
            flush_deadline.map(|dl| dl.duration_since(Instant::now()));
        if timeout == Some(Duration::ZERO) {
            if tx.send(buf.outbuf.clone()).is_err() {
                return;
            }
            timeout = None;
            flush_deadline = None;
            buf.outbuf.clear();
        }
        poll.poll(&mut events, timeout).expect("poll failed");

        for event in events.iter() {
            match event.token() {
                STDIN => match buf.read(&log, &mut stdin) {
                    Ok(false) => return,
                    Ok(true) => {
                        if flush_deadline.is_none() {
                            flush_deadline =
                                Some(Instant::now() + BUFFER_DELAY);
                        }
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => (),
                    Err(err) => {
                        error!(log, "error reading from stdin"; "err" => %err);
                        return;
                    }
                },
                _ => unreachable!(),
            }
        }
    }
}

struct InOutBuf {
    inbuf: Vec<u8>,
    outbuf: Vec<u8>,
    term_raw: bool,
    next_raw: bool,
}

impl InOutBuf {
    fn new(term_raw: bool) -> Self {
        Self {
            inbuf: vec![0; 4096],
            outbuf: Vec::with_capacity(4096),
            term_raw,
            next_raw: false,
        }
    }

    // On success, returns `Ok(true)` if at least one byte was read, or
    // `Ok(false)` if `reader` returned `Ok(0)`. Read bytes will be stored into
    // `outbuf`, possibly modified if we're in raw mode (allowing for Ctrl-A to
    // be used as a prefix).
    fn read<R: io::Read>(
        &mut self,
        log: &Logger,
        reader: &mut R,
    ) -> io::Result<bool> {
        let n = reader.read(self.inbuf.as_mut_slice())?;
        trace!(log, "read {n} bytes from stdin");
        if n == 0 {
            return Ok(false);
        }

        if !self.term_raw {
            self.outbuf.extend_from_slice(&self.inbuf[..n]);
            return Ok(true);
        }

        // Put bytes from inbuf to outbuf, but don't send Ctrl-A unless
        // next_raw is true.
        for &c in &self.inbuf[..n] {
            match c {
                // Ctrl-A means send next one raw
                CTRL_A => {
                    if self.next_raw {
                        // Ctrl-A Ctrl-A should be sent as Ctrl-A
                        self.outbuf.push(c);
                        self.next_raw = false;
                    } else {
                        self.next_raw = true;
                    }
                }
                CTRL_C => {
                    if !self.next_raw {
                        // Exit on non-raw Ctrl-C
                        return Ok(false);
                    } else {
                        // Otherwise send Ctrl-C
                        self.outbuf.push(c);
                        self.next_raw = false;
                    }
                }
                _ => {
                    self.outbuf.push(c);
                    self.next_raw = false;
                }
            }
        }

        Ok(true)
    }
}

fn recv_task(log: Logger, socket: Arc<UdpSocket>, tx: Sender<SpMessage>) {
    loop {
        match crate::recv_sp_message(&log, &socket) {
            Ok(message) => {
                if tx.send(message).is_err() {
                    break;
                }
            }
            Err(err) => {
                match err {
                    // ignore "would block" errors, which are what we see when
                    // the recv times out. we're not necessarily expecting a
                    // timely response here, so that's not worth logging.
                    RecvError::Io(err)
                        if err.kind() == io::ErrorKind::WouldBlock =>
                    {
                        ()
                    }
                    other => {
                        error!(log, "recv error"; "err" => %other);
                    }
                }
            }
        }
    }
}
