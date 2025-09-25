// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A runnable async trust quorum node that wraps the sans-io [`crate::Node`]

use crate::{Node, NodeCtx, PlatformId};
use camino::Utf8PathBuf;
use slog::{Logger, error, info, o, warn};
use sprockets_tls::Stream;
use sprockets_tls::keys::SprocketsConfig;
use sprockets_tls::server::Server;
use std::collections::BTreeSet;
use std::net::{SocketAddr, SocketAddrV6};

#[derive(Debug, Clone)]
pub struct Config {
    pub platform_id: PlatformId,
    pub listen_addr: SocketAddrV6,
    pub tq_state_ledger_paths: Vec<Utf8PathBuf>,
    pub network_config_ledger_paths: Vec<Utf8PathBuf>,
    pub sprockets: SprocketsConfig,
}

pub struct AsyncNode {
    log: Logger,
    config: Config,
    node: Node,
    ctx: NodeCtx,
    bootstrap_addrs: BTreeSet<SocketAddrV6>,
}

impl AsyncNode {
    pub fn new(config: Config, log: &Logger) -> AsyncNode {
        let log = log.new(o!(
            "component" => "trust-quorum",
            "platform_id" => config.platform_id.to_string()
        ));

        // TODO: Load persistent state from ledger
        let mut ctx = NodeCtx::new(config.platform_id.clone());
        let node = Node::new(&log, &mut ctx);
        AsyncNode { log, config, node, ctx, bootstrap_addrs: BTreeSet::new() }
    }

    pub async fn run(&mut self) {
        let listener = Server::new(
            self.config.sprockets.clone(),
            self.config.listen_addr,
            self.log.clone(),
        )
        .await
        .expect("sprockets server can listen");
        info!(self.log, "Started listening"; "local_addr" => %self.config.listen_addr);
        loop {
            // TODO: Plumb through corpus update
            let accept_fut = listener.accept(vec![]).await;
            let log = self.log.clone();
            tokio::spawn(async move {
                match accept_fut.await {
                    Ok((stream, addr)) => {
                        let platform_id = stream
                            .peer_platform_id()
                            .as_str()
                            .unwrap()
                            .to_owned();
                        let log =
                            log.new(o!("peer_platform_id" => platform_id));
                        let SocketAddr::V6(addr) = addr else {
                            warn!(
                                log,
                                "Got connection from IPv4 address";
                                "addr" => addr
                            );
                            return;
                        };
                        info!(log, "Accepted sprockets connection"; "addr" => %addr);
                    }
                    Err(err) => {
                        error!(log, "Failed to accept a connection: {err:?}");
                    }
                }
            });
        }
    }
}
