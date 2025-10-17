// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A runnable async trust quorum node that wraps the sans-io [`crate::Node`]

use crate::connection_manager::{ConnMgr, ConnToMainMsg, ConnToMainMsgInner};
use crate::{BaseboardId, Node, NodeCtx};
use camino::Utf8PathBuf;
use slog::{Logger, error, info, o, warn};
use sprockets_tls::Stream;
use sprockets_tls::client::Client;
use sprockets_tls::keys::SprocketsConfig;
use sprockets_tls::server::{Server, SprocketsAcceptor};
use std::collections::BTreeSet;
use std::net::{SocketAddr, SocketAddrV6};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone)]
pub struct Config {
    pub baseboard_id: BaseboardId,
    pub listen_addr: SocketAddrV6,
    //    pub tq_state_ledger_paths: Vec<Utf8PathBuf>,
    //  pub network_config_ledger_paths: Vec<Utf8PathBuf>,
    pub sprockets: SprocketsConfig,
}

/// A request sent to the `NodeTask` from the `NodeTaskHandle`
pub enum NodeApiRequest {
    /// Inform the `Node` of currently known IP addresses on the bootstrap network
    ///
    /// These are generated from DDM prefixes learned by the bootstrap agent.
    BootstrapAddresses(BTreeSet<SocketAddrV6>),
}

/// An error response from a `NodeApiRequest`
#[derive(Error, Debug, PartialEq)]
pub enum NodeApiError {
    #[error("failed to send request to node task")]
    Send,
}

#[derive(Debug, Clone)]
pub struct NodeTaskHandle {
    tx: mpsc::Sender<NodeApiRequest>,
}

impl NodeTaskHandle {
    /// Inform the node of currently known IP addresses on the bootstrap network
    ///
    /// These are generated from DDM prefixes learned by the bootstrap agent.
    pub async fn load_peer_addresses(
        &self,
        addrs: BTreeSet<SocketAddrV6>,
    ) -> Result<(), NodeApiError> {
        self.tx
            .send(NodeApiRequest::BootstrapAddresses(addrs))
            .await
            .map_err(|_| NodeApiError::Send)?;
        Ok(())
    }
}

pub struct NodeTask {
    log: Logger,
    config: Config,
    node: Node,
    ctx: NodeCtx,
    conn_mgr: ConnMgr,
    conn_mgr_rx: mpsc::Receiver<ConnToMainMsg>,

    // Handle requests received from `PeerHandle`
    rx: mpsc::Receiver<NodeApiRequest>,
}

impl NodeTask {
    pub async fn new(
        config: Config,
        log: &Logger,
    ) -> (NodeTask, NodeTaskHandle) {
        let log = log.new(o!(
            "component" => "trust-quorum",
            "platform_id" => config.baseboard_id.to_string()
        ));
        // We only expect one outstanding request at a time for `Init_` or
        // `LoadRackSecret` requests, We can have one of those requests in
        // flight while allowing `PeerAddresses` updates. We also allow status
        // requests in parallel. Just leave some room.
        let (tx, rx) = mpsc::channel(10);

        let (conn_mgr_tx, conn_mgr_rx) = mpsc::channel(100);

        // TODO: Load persistent state from ledger
        let mut ctx = NodeCtx::new(config.baseboard_id.clone());
        let node = Node::new(&log, &mut ctx);
        let conn_mgr = ConnMgr::new(
            &log,
            config.listen_addr,
            config.sprockets.clone(),
            conn_mgr_tx,
        )
        .await;
        (
            NodeTask { log, config, node, ctx, conn_mgr, conn_mgr_rx, rx },
            NodeTaskHandle { tx },
        )
    }

    /// Run the main loop of the node
    ///
    /// This should be spawned into its own tokio task
    pub async fn run(&mut self) {
        loop {
            // TODO: Real corpus
            let corpus = vec![];
            tokio::select! {
                Some(request) = self.rx.recv() => {
                    self.on_api_request(request).await;
                }
                res = self.conn_mgr.step(corpus.clone()) => {
                    if let Err(err) = res {
                        error!(self.log, "Failed to accept connection"; &err);
                        continue;
                    }
                }
                Some(msg) = self.conn_mgr_rx.recv() => {
                    self.on_conn_msg(msg).await
                }

            }
        }
    }

    async fn on_accept(&mut self, acceptor: SprocketsAcceptor) {}

    // Handle messages from connection management tasks
    async fn on_conn_msg(&mut self, msg: ConnToMainMsg) {
        let task_id = msg.task_id;
        match msg.msg {
            ConnToMainMsgInner::Accepted { addr, peer_id } => {
                self.conn_mgr
                    .server_handshake_completed(task_id, addr, peer_id)
                    .await;
            }
            ConnToMainMsgInner::Connected { addr, peer_id } => {
                self.conn_mgr
                    .client_handshake_completed(task_id, addr, peer_id)
                    .await;
            }
            ConnToMainMsgInner::Received { from, msg } => {
                todo!();
            }
            ConnToMainMsgInner::ReceivedNetworkConfig { from, config } => {
                todo!();
            }
        }
    }

    async fn on_api_request(&mut self, request: NodeApiRequest) {
        match request {
            NodeApiRequest::BootstrapAddresses(addrs) => {
                info!(self.log, "Updated Peer Addresses: {addrs:?}");
                // TODO: real corpus
                let corpus = vec![];
                self.conn_mgr.update_bootstrap_connections(addrs, corpus).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection_manager::platform_id_to_baseboard_id;
    use dropshot::test_util::log_prefix_for_test;
    use omicron_test_utils::dev::test_setup_log;
    use sprockets_tls::keys::ResolveSetting;
    use sprockets_tls_test_utils::{
        alias_prefix, cert_path, certlist_path, private_key_path, root_prefix,
        sprockets_auth_prefix,
    };

    fn pki_doc_to_node_configs(dir: Utf8PathBuf, n: usize) -> Vec<Config> {
        (1..=n)
            .map(|i| {
                let baseboard_id = platform_id_to_baseboard_id(
                    &sprockets_tls_test_utils::platform_id(i),
                );
                let listen_addr = SocketAddrV6::new(
                    std::net::Ipv6Addr::LOCALHOST,
                    11222,
                    0,
                    0,
                );
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

        let file_behavior =
            sprockets_tls_test_utils::OutputFileExistsBehavior::Overwrite;

        // Create `num_nodes` nodes worth of keys and certs
        let doc = sprockets_tls_test_utils::generate_config(num_nodes);
        doc.write_key_pairs(dir.clone(), file_behavior).unwrap();
        doc.write_certificates(dir.clone(), file_behavior).unwrap();
        doc.write_certificate_lists(dir.clone(), file_behavior).unwrap();

        // This is just a made up digest. We aren't currently using a corpus, so it
        // doesn't matter what the measurements are, just that there is at least
        // one in a file named "log.bin".
        let digest =
            "be4df4e085175f3de0c8ac4837e1c2c9a34e8983209dac6b549e94154f7cdd9c"
                .into();
        let attest_log_doc = attest_mock::log::Document {
            measurements: vec![attest_mock::log::Measurement {
                algorithm: "sha3-256".into(),
                digest,
            }],
        };
        // Write out the log document to the filesystem
        let out = attest_mock::log::mock(attest_log_doc).unwrap();
        std::fs::write(dir.join("log.bin"), &out).unwrap();

        let configs = pki_doc_to_node_configs(dir, num_nodes);

        let (mut task, handle) =
            NodeTask::new(configs[0].clone(), &logctx.log).await;
        tokio::spawn(async move { task.run().await });

        loop {
            if let Err(e) = Client::connect(
                configs[1].sprockets.clone(),
                configs[0].listen_addr,
                vec![],
                logctx.log.clone(),
            )
            .await
            {
                continue;
            }
        }
    }
}
