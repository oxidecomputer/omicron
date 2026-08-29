// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An authenticated byte tunnel to the rack's Support Shell proxy.
//!
//! Nexus copies bytes and nothing else. The client's platform TLS
//! terminates in the switch zone and every support action is signed
//! off-rack, so this endpoint decides who may reach the support
//! plane without being able to read or alter what crosses it.
//!
//! The mirror image of this proxy lives in wicketd's `nexus_proxy`,
//! which carries techport users the other way.

use std::net::{SocketAddr, SocketAddrV6};
use std::time::Duration;

use dropshot::{
    WebsocketConnectionRaw, WebsocketEndpointResult, WebsocketUpgrade,
};
use futures::{SinkExt, StreamExt};
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use omicron_common::address::SUSH_PROXY_PORT;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::RackUuid;
use slog::{Logger, info, o, warn};
use slog_error_chain::InlineErrorChain;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::{Role, WebSocketConfig};

use crate::app::switch_zone_address_mappings;

/// How long to wait for one switch before trying the other.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// The client's pipe sends 8 KiB frames; anything much bigger is not
/// our client, and the default cap is a 64 MiB allocation.
const MAX_WS_MESSAGE_SIZE: usize = 0x2_0000;

impl super::Nexus {
    /// Tunnel a support client's connection to a sush proxy.
    /// Errors are returned before the websocket upgrade so
    /// the client learns why the connection failed.
    pub(crate) async fn support_shell_tunnel(
        &self,
        opctx: &OpContext,
        rack_id: RackUuid,
        upgrade: WebsocketUpgrade,
    ) -> WebsocketEndpointResult {
        let log = opctx.log.new(o!(
            "component" => "SupportShellTunnel",
            "rack_id" => rack_id.to_string(),
            "actor" => format!("{:?}", opctx.authn.actor()),
        ));
        let proxy = self.support_shell_proxy(opctx, &rack_id, &log).await?;
        let log = match proxy.peer_addr() {
            Ok(addr) => log.new(o!("proxy_addr" => addr)),
            Err(_) => log,
        };
        upgrade.handle(move |conn| async move {
            let config = WebSocketConfig {
                max_message_size: Some(MAX_WS_MESSAGE_SIZE),
                max_frame_size: Some(MAX_WS_MESSAGE_SIZE),
                ..Default::default()
            };
            let client = WebSocketStream::from_raw_socket(
                conn.into_inner(),
                Role::Server,
                Some(config),
            )
            .await;
            info!(log, "tunnel opened");
            let PipeSummary { reason, to_proxy, to_client } =
                pipe(client, proxy).await;
            info!(
                log, "tunnel closed";
                "reason" => reason,
                "bytes_sent_to_proxy" => to_proxy,
                "bytes_sent_to_client" => to_client,
            );
            Ok(())
        })
    }

    /// Authorize the tunnel and connect to a sush proxy, on whichever
    /// switch answers first.
    ///
    /// The proxies have no directory entry of their own; like lldpd,
    /// they answer on the switch zone addresses dendrite advertises.
    async fn support_shell_proxy(
        &self,
        opctx: &OpContext,
        rack_id: &RackUuid,
        log: &Logger,
    ) -> Result<TcpStream, Error> {
        // The lookup comes first so an unauthorized caller gets the
        // same 404 as for a rack that does not exist. It otherwise
        // only validates existence: like lldpd_clients, we assume the
        // single rack is ours (omicron#1276).
        self.rack_lookup(opctx, rack_id).await?;
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let proxy_addrs =
            switch_zone_address_mappings(&self.internal_resolver, log)
                .await
                .map_err(|e| Error::unavail(&e))?
                .into_values()
                .map(|ip| {
                    SocketAddr::V6(SocketAddrV6::new(ip, SUSH_PROXY_PORT, 0, 0))
                })
                .collect::<Vec<_>>();
        for addr in &proxy_addrs {
            match timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await {
                Ok(Ok(stream)) => {
                    let _ = stream.set_nodelay(true);
                    return Ok(stream);
                }
                Ok(Err(error)) => warn!(
                    log, "sush proxy unreachable";
                    "addr" => %addr,
                    InlineErrorChain::new(&error),
                ),
                Err(_) => warn!(
                    log, "sush proxy connect timed out";
                    "addr" => %addr,
                ),
            }
        }
        Err(Error::unavail("no sush proxy reachable on either switch"))
    }
}

/// Why a tunnel ended and how much it carried.
struct PipeSummary {
    reason: &'static str,
    to_proxy: u64,
    to_client: u64,
}

/// Copy bytes both ways until either side finishes, returning why
/// and how much. The directions are independent pipes; when either
/// ends, both are torn down, since half-open service would only
/// delay the client's error.
async fn pipe(
    client: WebSocketStream<WebsocketConnectionRaw>,
    proxy: TcpStream,
) -> PipeSummary {
    let (mut ws_sink, mut ws_source) = client.split();
    let (mut proxy_read, mut proxy_write) = proxy.into_split();
    let mut to_proxy = 0;
    let mut to_client = 0;

    let inbound = async {
        while let Some(message) = ws_source.next().await {
            match message {
                Ok(Message::Binary(data)) => {
                    if proxy_write.write_all(&data).await.is_err() {
                        return "proxy write failed";
                    }
                    to_proxy += data.len() as u64;
                }
                Ok(Message::Close(_)) | Err(_) => break,
                // Tungstenite answers pings itself.
                Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
                // Anything else would truncate the byte stream
                // invisibly; fail loudly instead.
                Ok(_) => return "unexpected frame",
            }
        }
        let _ = proxy_write.shutdown().await;
        "client closed"
    };

    let outbound = async {
        let mut buf = [0; 0x2000];
        loop {
            match proxy_read.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let data = buf[..n].to_vec();
                    if ws_sink.send(Message::Binary(data)).await.is_err() {
                        return "client send failed";
                    }
                    to_client += n as u64;
                }
            }
        }
        let _ = ws_sink.send(Message::Close(None)).await;
        "proxy closed"
    };

    let reason = tokio::select! {
        reason = inbound => reason,
        reason = outbound => reason,
    };
    PipeSummary { reason, to_proxy, to_client }
}
