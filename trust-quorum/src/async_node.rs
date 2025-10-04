// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A runnable async trust quorum node that wraps the sans-io [`crate::Node`]

use crate::{BaseboardId, Node, NodeCtx};
use camino::Utf8PathBuf;
use slog::{Logger, error, info, o, warn};
use sprockets_tls::Stream;
use sprockets_tls::keys::SprocketsConfig;
use sprockets_tls::server::Server;
use std::collections::BTreeSet;
use std::net::{SocketAddr, SocketAddrV6};

#[derive(Debug, Clone)]
pub struct Config {
    pub baseboard_id: BaseboardId,
    pub listen_addr: SocketAddrV6,
    //    pub tq_state_ledger_paths: Vec<Utf8PathBuf>,
    //  pub network_config_ledger_paths: Vec<Utf8PathBuf>,
    pub sprockets: SprocketsConfig,
}

pub struct AsyncNode {
    log: Logger,
    config: Config,
    node: Node,
    ctx: NodeCtx,
    bootstrap_addrs: BTreeSet<SocketAddrV6>,
}

fn platform_id_to_baseboard_id(platform_id: &str) -> BaseboardId {
    let mut platform_id_iter = platform_id.split(":");
    let part_number = platform_id_iter.nth(1).unwrap().to_string();
    let serial_number = platform_id_iter.skip(1).next().unwrap().to_string();
    BaseboardId { part_number, serial_number }
}

impl AsyncNode {
    pub fn new(config: Config, log: &Logger) -> AsyncNode {
        let log = log.new(o!(
            "component" => "trust-quorum",
            "platform_id" => config.baseboard_id.to_string()
        ));

        // TODO: Load persistent state from ledger
        let mut ctx = NodeCtx::new(config.baseboard_id.clone());
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
            let acceptor = match listener.accept(vec![]).await {
                Ok(acceptor) => acceptor,
                Err(err) => {
                    error!(self.log, "Failed to accept connection: {err}");
                    continue;
                }
            };
            let log = self.log.clone();
            tokio::spawn(async move {
                match acceptor.handshake().await {
                    Ok((stream, addr)) => {
                        let platform_id =
                            stream.peer_platform_id().as_str().unwrap();
                        let baseboard_id =
                            platform_id_to_baseboard_id(platform_id);

                        // TODO: Conversion between `PlatformId` and `BaseboardId` should
                        // happen in `sled-agent-types`. This is waiting on an update
                        // to the `dice-mfg-msgs` crate.
                        let log = log.new(
                            o!("baseboard_id" => baseboard_id.to_string()),
                        );
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

#[cfg(test)]
mod tests {
    use super::*;
    use dropshot::test_util::log_prefix_for_test;
    use omicron_test_utils::dev::test_setup_log;
    use pki_playground::sprockets::{
        alias_prefix, cert_path, certlist_path, private_key_path, root_prefix,
        sprockets_auth_prefix,
    };
    use sprockets_tls::keys::ResolveSetting;

    fn pki_doc_to_node_configs(dir: Utf8PathBuf, n: usize) -> Vec<Config> {
        (1..=n)
            .map(|i| {
                let baseboard_id = platform_id_to_baseboard_id(
                    &pki_playground::sprockets::platform_id(i),
                );
                let listen_addr =
                    SocketAddrV6::new(std::net::Ipv6Addr::LOCALHOST, 0, 0, 0);
                let sprockets_auth_key_name = sprockets_auth_prefix(i);
                let alias_key_name = alias_prefix(i);
                let sprockets = SprocketsConfig {
                    resolve: ResolveSetting::Local {
                        priv_key: private_key_path(
                            dir.clone(),
                            &sprockets_auth_key_name,
                        ),
                        cert_chain: certlist_path(
                            dir.clone(),
                            &sprockets_auth_key_name,
                        ),
                    },
                    attest: sprockets_tls::keys::AttestConfig::Local {
                        priv_key: private_key_path(
                            dir.clone(),
                            &alias_key_name,
                        ),
                        cert_chain: certlist_path(dir.clone(), &alias_key_name),
                        // TODO: We need attest-mock to generate a real log
                        log: dir.join("log.bin"),
                    },
                    roots: vec![cert_path(dir.clone(), &root_prefix())],
                };
                Config { baseboard_id, listen_addr, sprockets }
            })
            .collect()
    }

    #[tokio::test]
    async fn connect() {
        let logctx = test_setup_log("connect");
        let (dir, _) = log_prefix_for_test("connect");
        println!("Writing keys and certs to {dir}");
        let num_nodes = 4;

        // Create `num_nodes` nodes worth of keys and certs
        let doc = pki_playground::sprockets::generate_config(num_nodes);
        doc.write_key_pairs(
            dir.clone(),
            pki_playground::OutputFileExistsBehavior::Overwrite,
        )
        .unwrap();
        doc.write_certificates(
            dir.clone(),
            pki_playground::OutputFileExistsBehavior::Overwrite,
        )
        .unwrap();
        doc.write_certificate_lists(
            dir.clone(),
            pki_playground::OutputFileExistsBehavior::Overwrite,
        )
        .unwrap();

        let configs = pki_doc_to_node_configs(dir, num_nodes);

        println!("{configs:#?}");
    }
}
