// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use crate::error::Error;
use futures::stream::SplitSink;
use futures::stream::SplitStream;
use futures::SinkExt;
use futures::StreamExt;
use gateway_messages::SpComponent;
use gateway_sp_comms::AttachedSerialConsole;
use gateway_sp_comms::AttachedSerialConsoleSend;
use gateway_sp_comms::Communicator;
use gateway_sp_comms::SpIdentifier;
use http::header;
use hyper::upgrade;
use hyper::upgrade::Upgraded;
use hyper::Body;
use slog::debug;
use slog::error;
use slog::info;
use slog::Logger;
use std::borrow::Cow;
use std::ops::Deref;
use std::ops::DerefMut;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::handshake;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

pub(crate) async fn attach(
    sp_comms: &Communicator,
    sp: SpIdentifier,
    component: SpComponent,
    request: &mut http::Request<Body>,
    log: Logger,
) -> Result<http::Response<Body>, Error> {
    if !request
        .headers()
        .get(header::CONNECTION)
        .and_then(|hv| hv.to_str().ok())
        .map(|hv| {
            hv.split(|c| c == ',' || c == ' ')
                .any(|vs| vs.eq_ignore_ascii_case("upgrade"))
        })
        .unwrap_or(false)
    {
        return Err(Error::BadWebsocketConnection(
            "expected connection upgrade",
        ));
    }
    if !request
        .headers()
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.split(|c| c == ',' || c == ' ')
                .any(|v| v.eq_ignore_ascii_case("websocket"))
        })
        .unwrap_or(false)
    {
        return Err(Error::BadWebsocketConnection(
            "unexpected protocol for upgrade",
        ));
    }
    if request
        .headers()
        .get(header::SEC_WEBSOCKET_VERSION)
        .map(|v| v.as_bytes())
        != Some(b"13")
    {
        return Err(Error::BadWebsocketConnection(
            "missing or invalid websocket version",
        ));
    }
    let accept_key = request
        .headers()
        .get(header::SEC_WEBSOCKET_KEY)
        .map(|hv| hv.as_bytes())
        .map(|key| handshake::derive_accept_key(key))
        .ok_or(Error::BadWebsocketConnection("missing websocket key"))?;

    let console = sp_comms.serial_console_attach(sp, component).await?;
    let upgrade_fut = upgrade::on(request);
    tokio::spawn(async move {
        let upgraded = match upgrade_fut.await {
            Ok(u) => u,
            Err(e) => {
                error!(log, "serial task failed"; "err" => %e);
                return;
            }
        };
        let config = WebSocketConfig {
            max_send_queue: Some(4096),
            ..Default::default()
        };
        let ws_stream = WebSocketStream::from_raw_socket(
            upgraded,
            Role::Server,
            Some(config),
        )
        .await;

        let task = SerialConsoleTask { console, ws_stream };
        match task.run(&log).await {
            Ok(()) => debug!(log, "serial task complete"),
            Err(e) => {
                error!(log, "serial task failed"; "err" => %e)
            }
        }
    });

    // `.body()` only fails if our headers are bad, which they aren't
    // (unless `hyper::handshake` gives us a bogus accept key?), so we're
    // safe to unwrap this
    Ok(http::Response::builder()
        .status(http::StatusCode::SWITCHING_PROTOCOLS)
        .header(header::CONNECTION, "Upgrade")
        .header(header::UPGRADE, "websocket")
        .header(header::SEC_WEBSOCKET_ACCEPT, accept_key)
        .body(Body::empty())
        .unwrap())
}

#[derive(Debug, thiserror::Error)]
enum SerialTaskError {
    #[error(transparent)]
    Error(#[from] Error),
    #[error(transparent)]
    TungsteniteError(#[from] tokio_tungstenite::tungstenite::Error),
}

struct SerialConsoleTask {
    console: AttachedSerialConsole,
    ws_stream: WebSocketStream<Upgraded>,
}

impl SerialConsoleTask {
    async fn run(self, log: &Logger) -> Result<(), SerialTaskError> {
        let (ws_sink, ws_stream) = self.ws_stream.split();

        // Spawn a task to send any messages received from the SP to the client
        // websocket.
        //
        // TODO-cleanup We have no way to apply backpressure to the SP, and are
        // willing to buffer up an arbitray amount of data in memory. We should
        // apply some form of backpressure (which the SP could only handle by
        // discarding data).
        let (ws_sink_tx, ws_sink_rx) = mpsc::unbounded_channel();
        let mut ws_sink_handle =
            tokio::spawn(Self::ws_sink_task(ws_sink, ws_sink_rx));

        // Spawn a task to send any messages received from the client websocket
        // to the SP.
        let (console_tx, mut console_rx) = self.console.split();
        let console_tx = DetachOnDrop::new(console_tx);
        let mut ws_recv_handle = tokio::spawn(Self::ws_recv_task(
            ws_stream,
            console_tx,
            log.clone(),
        ));

        loop {
            tokio::select! {
                // Our ws_sink task completed; this is only possible if it
                // fails, since it loops until we drop ws_sink_tx (which doesn't
                // happen until we return!).
                join_result = &mut ws_sink_handle => {
                    let result = join_result.expect("ws sink task panicked");
                    return result;
                }

                // Our ws_recv task completed; this is possible if the websocket
                // connection fails or is closed by the client. In either case,
                // we're also done.
                join_result = &mut ws_recv_handle => {
                    let result = join_result.expect("ws recv task panicked");
                    return result;
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
                    console_tx
                        .write(data)
                        .await
                        .map_err(gateway_sp_comms::error::Error::from)
                        .map_err(Error::from)?;
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
