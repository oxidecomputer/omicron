// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use crate::error::SpCommsError;
use dropshot::WebsocketChannelResult;
use dropshot::WebsocketConnection;
use futures::stream::SplitSink;
use futures::stream::SplitStream;
use futures::SinkExt;
use futures::StreamExt;
use gateway_sp_comms::AttachedSerialConsole;
use gateway_sp_comms::AttachedSerialConsoleSend;
use hyper::upgrade::Upgraded;
use slog::error;
use slog::info;
use slog::Logger;
use std::borrow::Cow;
use std::ops::Deref;
use std::ops::DerefMut;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

#[derive(Debug, thiserror::Error)]
enum SerialTaskError {
    #[error(transparent)]
    SpCommsError(#[from] SpCommsError),
    #[error(transparent)]
    TungsteniteError(#[from] tokio_tungstenite::tungstenite::Error),
}

pub(crate) async fn run(
    console: AttachedSerialConsole,
    conn: WebsocketConnection,
    log: Logger,
) -> WebsocketChannelResult {
    let upgraded = conn.into_inner();
    let config =
        WebSocketConfig { max_send_queue: Some(4096), ..Default::default() };
    let ws_stream =
        WebSocketStream::from_raw_socket(upgraded, Role::Server, Some(config))
            .await;

    let (ws_sink, ws_stream) = ws_stream.split();

    // Spawn a task to send any messages received from the SP to the client
    // websocket.
    //
    // TODO-cleanup We have no way to apply backpressure to the SP, and are
    // willing to buffer up an arbitray amount of data in memory. We should
    // apply some form of backpressure (which the SP could only handle by
    // discarding data).
    let (ws_sink_tx, ws_sink_rx) = mpsc::unbounded_channel();
    let mut ws_sink_handle = tokio::spawn(ws_sink_task(ws_sink, ws_sink_rx));

    // Spawn a task to send any messages received from the client websocket
    // to the SP.
    let (console_tx, mut console_rx) = console.split();
    let console_tx = DetachOnDrop::new(console_tx);
    let mut ws_recv_handle =
        tokio::spawn(ws_recv_task(ws_stream, console_tx, log.clone()));

    loop {
        tokio::select! {
            // Our ws_sink task completed; this is only possible if it
            // fails, since it loops until we drop ws_sink_tx (which doesn't
            // happen until we return!).
            join_result = &mut ws_sink_handle => {
                let result = join_result.expect("ws sink task panicked");
                return result.map_err(Into::into);
            }

            // Our ws_recv task completed; this is possible if the websocket
            // connection fails or is closed by the client. In either case,
            // we're also done.
            join_result = &mut ws_recv_handle => {
                let result = join_result.expect("ws recv task panicked");
                return result.map_err(Into::into);
            }

            // Receive a UDP packet from the SP.
            packet = console_rx.recv() => {
                match packet {
                    Some(data) => {
                        info!(
                            log, "received serial console data from SP";
                            "length" => data.len(),
                        );
                        let _ = ws_sink_tx.send(Message::Binary(data));
                    }
                    None => {
                        // Sender is closed; i.e., we've been detached.
                        // Close the websocket.
                        info!(log, "detaching from serial console");
                        let close = CloseFrame {
                            code: CloseCode::Policy,
                            reason: Cow::Borrowed("serial console was detached"),
                        };
                        let _ = ws_sink_tx.send(Message::Close(Some(close)));
                        return Ok(());
                    }
                }
            }
        }
    }
}

async fn ws_sink_task(
    mut ws_sink: SplitSink<WebSocketStream<Upgraded>, Message>,
    mut messages: mpsc::UnboundedReceiver<Message>,
) -> Result<(), SerialTaskError> {
    while let Some(message) = messages.recv().await {
        ws_sink.send(message).await?;
    }
    Ok(())
}

async fn ws_recv_task(
    mut ws_stream: SplitStream<WebSocketStream<Upgraded>>,
    mut console_tx: DetachOnDrop,
    log: Logger,
) -> Result<(), SerialTaskError> {
    while let Some(message) = ws_stream.next().await {
        match message {
            Ok(Message::Binary(data)) => {
                console_tx.write(data).await.map_err(SpCommsError::from)?;
            }
            Ok(Message::Close(_)) => {
                break;
            }
            Ok(other) => {
                error!(
                    log,
                    "bogus websocket message; terminating task";
                    "message" => ?other,
                );
                return Ok(());
            }
            Err(err) => return Err(err.into()),
        }
    }
    info!(log, "remote end closed websocket; terminating task",);
    Ok(())
}

struct DetachOnDrop(Option<AttachedSerialConsoleSend>);

impl DetachOnDrop {
    fn new(console: AttachedSerialConsoleSend) -> Self {
        Self(Some(console))
    }
}

impl Drop for DetachOnDrop {
    fn drop(&mut self) {
        // We can't `.await` within `drop()`, so we'll spawn a task to detach
        // the console. `detach()` only does anything if the current connection
        // is still attached, so it's fine if this runs after a new connection
        // has been attached (at which point it won't do anything).
        let console = self.0.take().unwrap();
        tokio::spawn(async move { console.detach().await });
    }
}

impl Deref for DetachOnDrop {
    type Target = AttachedSerialConsoleSend;

    fn deref(&self) -> &Self::Target {
        // We know from `new()` that we're created with `Some(console)`, and we
        // don't remove it until our `Drop` impl
        self.0.as_ref().unwrap()
    }
}

impl DerefMut for DetachOnDrop {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // We know from `new()` that we're created with `Some(console)`, and we
        // don't remove it until our `Drop` impl
        self.0.as_mut().unwrap()
    }
}
