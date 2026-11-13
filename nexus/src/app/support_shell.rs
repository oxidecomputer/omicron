// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An authenticated tunnel to the Support Shell proxy.
//!
//! The tunnel endpoint controls who may reach the Support Shell,
//! without being able to read or alter what crosses it. The proxy
//! in the switch zone still terminates the platform-ID-backed TLS
//! connections from the client; this tunnel just wraps them in a
//! WebSocket.

use std::net::{SocketAddr, SocketAddrV6};
use std::time::Duration;

use dropshot::{
    WebsocketConnectionRaw, WebsocketEndpointResult, WebsocketUpgrade,
};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use internal_dns_types::names::ServiceName;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use omicron_common::address::SUSH_PROXY_PORT;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::RackUuid;
use slog::{Logger, info, o, warn};
use slog_error_chain::InlineErrorChain;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::spawn;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::{Role, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Bytes, Message};
use tokio_util::sync::CancellationToken;

/// How long to wait for a switch's proxy to answer a connect.
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
        let (proxy_addr, proxy) =
            self.support_shell_proxy(opctx, &rack_id, &log).await?;
        let log = log.new(o!("proxy_addr" => proxy_addr));
        upgrade.handle(move |conn| async move {
            let config = WebSocketConfig::default()
                .max_message_size(Some(MAX_WS_MESSAGE_SIZE))
                .max_frame_size(Some(MAX_WS_MESSAGE_SIZE));
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
    ) -> Result<(SocketAddr, TcpStream), Error> {
        // The lookup comes first so an unauthorized caller gets the
        // same 404 as for a rack that does not exist. It otherwise
        // only validates existence: like lldpd_clients, we assume the
        // single rack is ours (omicron#1276).
        self.rack_lookup(opctx, rack_id).await?;
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let ips = self
            .internal_resolver
            .lookup_all_ipv6(ServiceName::Dendrite)
            .await
            .map_err(|e| Error::unavail(&e.to_string()))?;
        let mut connects = JoinSet::new();
        for ip in ips {
            let addr =
                SocketAddr::V6(SocketAddrV6::new(ip, SUSH_PROXY_PORT, 0, 0));
            connects.spawn(async move {
                (addr, timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await)
            });
        }
        while let Some(joined) = connects.join_next().await {
            let (addr, connect) = joined.expect("connect panicked");
            match connect {
                Ok(Ok(stream)) => {
                    let _ = stream.set_nodelay(true);
                    return Ok((addr, stream));
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

/// Why a tunnel ended and how many bytes it carried each way.
struct PipeSummary {
    reason: String,
    to_proxy: u64,
    to_client: u64,
}

/// Copy bytes both ways until either side finishes. The directions
/// are independent pipes, but when either ends, both are torn down.
async fn pipe(
    client: WebSocketStream<WebsocketConnectionRaw>,
    proxy: TcpStream,
) -> PipeSummary {
    let (ws_sink, ws_source) = client.split();
    let (proxy_read, proxy_write) = proxy.into_split();
    let cancel = CancellationToken::new();
    let inbound = spawn(copy_to_proxy(ws_source, proxy_write, cancel.clone()));
    let outbound = spawn(copy_to_client(proxy_read, ws_sink, cancel));
    let (inbound_reason, to_proxy) = inbound.await.expect("pipe panicked");
    let (outbound_reason, to_client) = outbound.await.expect("pipe panicked");
    let reason = inbound_reason
        .or(outbound_reason)
        .unwrap_or_else(|| "cancelled".to_string());
    PipeSummary { reason, to_proxy, to_client }
}

async fn copy_to_proxy(
    mut source: SplitStream<WebSocketStream<WebsocketConnectionRaw>>,
    mut proxy: OwnedWriteHalf,
    cancel: CancellationToken,
) -> (Option<String>, u64) {
    let _cancel_other = cancel.clone().drop_guard();
    let mut copied = 0;
    let reason = loop {
        let message = tokio::select! {
            () = cancel.cancelled() => break None,
            message = source.next() => message,
        };
        match message {
            Some(Ok(Message::Binary(data))) => {
                if let Err(error) = proxy.write_all(&data).await {
                    break Some(format!(
                        "proxy write failed: {}",
                        InlineErrorChain::new(&error)
                    ));
                }
                copied += data.len() as u64;
            }
            None | Some(Ok(Message::Close(_))) => {
                break Some("client closed".to_string());
            }
            Some(Err(error)) => {
                break Some(format!(
                    "client read failed: {}",
                    InlineErrorChain::new(&error)
                ));
            }
            // Tungstenite answers pings itself.
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
            // Anything else would truncate the byte stream invisibly;
            // fail loudly instead.
            Some(Ok(_)) => break Some("unexpected frame".to_string()),
        }
    };
    let _ = proxy.shutdown().await;
    (reason, copied)
}

async fn copy_to_client(
    mut proxy: OwnedReadHalf,
    mut sink: SplitSink<WebSocketStream<WebsocketConnectionRaw>, Message>,
    cancel: CancellationToken,
) -> (Option<String>, u64) {
    let _cancel_other = cancel.clone().drop_guard();
    let mut copied = 0;
    let mut buf = [0; 0x2000];
    let reason = loop {
        let read = tokio::select! {
            () = cancel.cancelled() => break None,
            read = proxy.read(&mut buf) => read,
        };
        match read {
            Ok(0) => break Some("proxy closed".to_string()),
            Ok(n) => {
                let data = Bytes::copy_from_slice(&buf[..n]);
                if let Err(error) = sink.send(Message::Binary(data)).await {
                    break Some(format!(
                        "client send failed: {}",
                        InlineErrorChain::new(&error)
                    ));
                }
                copied += n as u64;
            }
            Err(error) => {
                break Some(format!(
                    "proxy read failed: {}",
                    InlineErrorChain::new(&error)
                ));
            }
        }
    };
    let _ = sink.close().await;
    (reason, copied)
}
