// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use crate::error::Error;
use futures::future::Fuse;
use futures::FutureExt;
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
use std::collections::VecDeque;
use std::mem;
use std::ops::Deref;
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
        let (mut ws_sink, mut ws_stream) = self.ws_stream.split();
        let (console_tx, mut console_rx) = self.console.split();
        let console_tx = DetachOnDrop::new(console_tx);

        // TODO Currently we do not apply any backpressure on the SP and are
        // willing to buffer up an arbitrary amount of data in memory. Is it
        // reasonable to apply backpressure to the SP over UDP? Should we have
        // caps on memory and start discarding data if we exceed them? We _do_
        // apply backpressure to the websocket, delaying reading from it if we
        // still have data waiting to be sent to the SP.
        let mut data_from_sp: VecDeque<Vec<u8>> = VecDeque::new();
        let mut data_to_sp: Vec<u8> = Vec::new();

        loop {
            let ws_send = if let Some(data) = data_from_sp.pop_front() {
                ws_sink.send(Message::Binary(data)).fuse()
            } else {
                Fuse::terminated()
            };

            let (ws_recv, sp_send) = if data_to_sp.is_empty() {
                (ws_stream.next().fuse(), Fuse::terminated())
            } else {
                // Steal `data_to_sp` and create a future to send it to the SP.
                let mut to_send = Vec::new();
                mem::swap(&mut to_send, &mut data_to_sp);
                (Fuse::terminated(), console_tx.write(to_send).fuse())
            };

            tokio::select! {
                // Finished (or failed) sending data to the SP.
                send_success = sp_send => {
                    send_success
                        .map_err(gateway_sp_comms::error::Error::from)
                        .map_err(Error::from)?;
                }

                // Receive a UDP packet from the SP.
                packet = console_rx.recv() => {
                    match packet {
                        Some(data) => {
                            info!(
                                log, "received serial console data from SP";
                                "length" => data.len(),
                            );
                            data_from_sp.push_back(data);
                        }
                        None => {
                            // Sender is closed; i.e., we've been detached.
                            // Close the websocket.
                            info!(log, "detaching from serial console");
                            let close = CloseFrame {
                                code: CloseCode::Policy,
                                reason: Cow::Borrowed("serial console was detached"),
                            };
                            ws_sink.send(Message::Close(Some(close))).await?;
                            return Ok(());
                        }
                    }
                }

                // Send a previously-received UDP packet of data to the websocket
                // client
                write_success = ws_send => {
                    write_success?;
                }

                // Receive from the websocket to send to the SP.
                msg = ws_recv => {
                    match msg {
                        Some(Ok(Message::Binary(mut data))) => {
                            // we only populate ws_recv when we have no data
                            // currently queued up; sanity check that here
                            assert!(data_to_sp.is_empty());
                            data_to_sp.append(&mut data);
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            info!(
                                log,
                                "remote end closed websocket; terminating task",
                            );
                            return Ok(());
                        }
                        Some(other) => {
                            let wrong_message = other?;
                            error!(
                                log,
                                "bogus websocket message; terminating task";
                                "message" => ?wrong_message,
                            );
                            return Ok(());
                        }
                    }
                }
            }
        }
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
