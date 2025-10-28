// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An individual sprockets connection running in its own task

use crate::{ConnToMainMsg, ConnToMainMsgInner, MainToConnMsg, WireMsg};
use bytes::Buf;
use serde::Serialize;
use slog::{Logger, debug, error, o, warn};
use slog_error_chain::SlogInlineError;
use std::collections::VecDeque;
use std::io::Cursor;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf, split};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task;
use tokio::time::{Instant, MissedTickBehavior, interval};
use trust_quorum_protocol::BaseboardId;

/// Max buffer size of a connection
const CONN_BUF_SIZE: usize = 1024 * 1024;

/// Each message starts with a 4 bytes size header
const FRAME_HEADER_SIZE: usize = 4;

/// The number of serialized messages to queue for writing before closing the socket.
/// This means the remote side is very slow.
///
/// TODO: Alternatively we could drop the oldest message.
const MSG_WRITE_QUEUE_CAPACITY: usize = 5;

// Timing parameters for keeping the connection healthy
const PING_INTERVAL: Duration = Duration::from_secs(1);

/// The time limit for not receiving a complete message from a peer.
/// The connection is shutdown after this time.
const INACTIVITY_TIMEOUT: Duration = Duration::from_secs(10);

/// An error from within an `EstablishedConn` that triggers connection close
///
/// Also a great movie
#[derive(Debug, thiserror::Error, SlogInlineError)]
pub enum ConnErr {
    #[error("Failed to write")]
    FailedWrite(#[source] std::io::Error),
    #[error("Failed to read")]
    FailedRead(#[source] std::io::Error),
    #[error("Failed to deserialize wire message")]
    DeserializeWireMsg(#[from] ciborium::de::Error<std::io::Error>),
    #[error("Failed to serialize wire message")]
    SerializeWireMsg(#[from] ciborium::ser::Error<std::io::Error>),
    #[error("Write queue filled with serialized messages")]
    WriteQueueFull,
    #[error("Inactivity timeout")]
    InactivityTimeout,
}

/// Container for code running in its own task per sprockets connection
pub struct EstablishedConn {
    peer_id: BaseboardId,
    task_id: task::Id,
    reader: ReadHalf<sprockets_tls::Stream<TcpStream>>,
    writer: WriteHalf<sprockets_tls::Stream<TcpStream>>,
    main_tx: mpsc::Sender<ConnToMainMsg>,
    rx: mpsc::Receiver<MainToConnMsg>,
    log: Logger,

    // Buffer we read raw data into from a sprockets connection
    read_buf: Box<[u8]>,

    // The amount of data currently in `read_buf`
    total_read: usize,

    // Used for managing inactivity timeouts for the connection
    last_received_msg: Instant,

    // Keep a queue to write serialized messages into. We limit the queue
    // size, and if it gets exceeded it means the peer at the other
    // end isn't pulling data out fast enough. This should be basically
    // impossible to hit given the size and rate of message exchange
    // between peers. We go ahead and close the connection if the queue
    // fills.
    write_queue: VecDeque<Vec<u8>>,

    // The current serialized message being written if there is one
    current_write: Cursor<Vec<u8>>,
}

impl EstablishedConn {
    pub fn new(
        peer_id: BaseboardId,
        task_id: task::Id,
        stream: sprockets_tls::Stream<TcpStream>,
        main_tx: mpsc::Sender<ConnToMainMsg>,
        rx: mpsc::Receiver<MainToConnMsg>,
        log: &Logger,
    ) -> EstablishedConn {
        let log = log.new(o!("component" => "trust-quorum-established-conn"));
        let (reader, writer) = split(stream);
        EstablishedConn {
            peer_id,
            task_id,
            reader,
            writer,
            main_tx,
            rx,
            log,
            read_buf: vec![0u8; CONN_BUF_SIZE].into_boxed_slice(),
            total_read: 0,
            last_received_msg: Instant::now(),
            write_queue: VecDeque::with_capacity(MSG_WRITE_QUEUE_CAPACITY),
            current_write: Cursor::new(Vec::new()),
        }
    }

    pub async fn run(&mut self) {
        let mut interval = interval(PING_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        // This is the main loop of the connection
        //
        // Continuously process messages until the connection closes
        loop {
            if !self.current_write.has_remaining() {
                if let Some(buf) = self.write_queue.pop_front() {
                    self.current_write = Cursor::new(buf);
                }
            }

            let res = tokio::select! {
                _ = interval.tick() => {
                    self.ping().await
                }
                Some(msg) = self.rx.recv() => {
                    self.on_msg_from_main(msg).await
                }
                res = self.reader.read(&mut self.read_buf[self.total_read..]) => {
                    self.on_read(res).await
                }
                res = self.writer.write_buf(&mut self.current_write),
                   if self.current_write.has_remaining() =>
                {
                   self.check_write_result(res).await
                }
            };

            if let Err(err) = res {
                warn!(self.log, "Closing connection"; &err);
                self.close().await;
                return;
            }
        }
    }

    async fn close(&mut self) {
        if let Err(_) = self.main_tx.try_send(ConnToMainMsg {
            task_id: self.task_id,
            msg: ConnToMainMsgInner::Disconnected {
                peer_id: self.peer_id.clone(),
            },
        }) {
            warn!(self.log, "Failed to send to main task");
        }
        let _ = self.writer.shutdown().await;
    }

    async fn on_read(
        &mut self,
        res: Result<usize, std::io::Error>,
    ) -> Result<(), ConnErr> {
        let n = res.map_err(ConnErr::FailedRead)?;
        self.total_read += n;

        // We may have more than one message that has been read
        loop {
            if self.total_read < FRAME_HEADER_SIZE {
                return Ok(());
            }
            // Read frame size
            let size = read_frame_size(
                self.read_buf[..FRAME_HEADER_SIZE].try_into().unwrap(),
            );
            let end = size + FRAME_HEADER_SIZE;

            // If we haven't read the whole message yet, then return
            if end > self.total_read {
                return Ok(());
            }
            let msg: WireMsg =
                ciborium::from_reader(&self.read_buf[FRAME_HEADER_SIZE..end])?;
            // Move any remaining bytes to the beginning of the buffer.
            self.read_buf.copy_within(end..self.total_read, 0);
            self.total_read = self.total_read - end;

            self.last_received_msg = Instant::now();
            debug!(self.log, "Received {msg:?}");
            match msg {
                WireMsg::Tq(msg) => {
                    if let Err(_) = self.main_tx.try_send(ConnToMainMsg {
                        task_id: self.task_id,
                        msg: ConnToMainMsgInner::Received {
                            from: self.peer_id.clone(),
                            msg,
                        },
                    }) {
                        error!(
                            self.log,
                            "Failed to send received fsm msg to main task"
                        );
                        panic!("Connection to main task channel full");
                    }
                }
                WireMsg::Ping => {
                    // Nothing to do here, since Ping is just to keep us alive and
                    // we updated self.last_received_msg above.
                }
                WireMsg::NetworkConfig(config) => {
                    let generation = config.generation;
                    if let Err(_) = self.main_tx.try_send(ConnToMainMsg {
                        task_id: self.task_id,
                        msg: ConnToMainMsgInner::ReceivedNetworkConfig {
                            from: self.peer_id.clone(),
                            config,
                        },
                    }) {
                        warn!(
                            self.log,
                            "Failed to send received NetworkConfig with
                             generation {generation} to main task"
                        );
                        panic!("Connection to main task channnel full");
                    }
                }
            }
        }
    }

    async fn check_write_result(
        &mut self,
        res: Result<usize, std::io::Error>,
    ) -> Result<(), ConnErr> {
        match res {
            Ok(_) => {
                if !self.current_write.has_remaining() {
                    self.current_write = Cursor::new(Vec::new());
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.writer.shutdown().await;
                Err(ConnErr::FailedWrite(e))
            }
        }
    }

    async fn on_msg_from_main(
        &mut self,
        msg: MainToConnMsg,
    ) -> Result<(), ConnErr> {
        match msg {
            MainToConnMsg::Msg(msg) => self.write_framed_to_queue(msg).await,
        }
    }

    async fn write_framed_to_queue(
        &mut self,
        msg: WireMsg,
    ) -> Result<(), ConnErr> {
        if self.write_queue.len() == MSG_WRITE_QUEUE_CAPACITY {
            return Err(ConnErr::WriteQueueFull);
        } else {
            let msg = write_framed(&msg)?;
            self.write_queue.push_back(msg);
            Ok(())
        }
    }

    async fn ping(&mut self) -> Result<(), ConnErr> {
        if Instant::now() - self.last_received_msg > INACTIVITY_TIMEOUT {
            return Err(ConnErr::InactivityTimeout);
        }
        self.write_framed_to_queue(WireMsg::Ping).await
    }
}

// Decode the 4-byte big-endian frame size header
fn read_frame_size(buf: [u8; FRAME_HEADER_SIZE]) -> usize {
    u32::from_be_bytes(buf) as usize
}

/// Serialize and write `msg` into `buf`, prefixed by a 4-byte big-endian size
/// header
///
/// Return the total amount of data written into `buf` including the 4-byte
/// header.
fn write_framed<T: Serialize + ?Sized>(
    msg: &T,
) -> Result<Vec<u8>, ciborium::ser::Error<std::io::Error>> {
    let mut cursor = Cursor::new(vec![]);
    // Write a size placeholder
    std::io::Write::write(&mut cursor, &[0u8; FRAME_HEADER_SIZE])?;
    cursor.set_position(FRAME_HEADER_SIZE as u64);
    ciborium::into_writer(msg, &mut cursor)?;
    let size: u32 =
        (cursor.position() - FRAME_HEADER_SIZE as u64).try_into().unwrap();
    let mut buf = cursor.into_inner();
    buf[0..FRAME_HEADER_SIZE].copy_from_slice(&size.to_be_bytes());
    Ok(buf)
}
