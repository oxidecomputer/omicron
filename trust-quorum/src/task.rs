// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A runnable async trust quorum node that wraps the sans-io
//! [`trust_quorum_protocol::Node`]

use crate::connection_manager::{
    ConnMgr, ConnMgrStatus, ConnToMainMsg, ConnToMainMsgInner,
};
use omicron_uuid_kinds::RackUuid;
use serde::{Deserialize, Serialize};
use slog::{Logger, debug, error, info, o};
use sprockets_tls::keys::SprocketsConfig;
use std::collections::BTreeSet;
use std::net::SocketAddrV6;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::{mpsc, oneshot};
use trust_quorum_protocol::{
    Alarm, BaseboardId, CommitError, Configuration, Epoch, ExpungedMetadata,
    LoadRackSecretError, LrtqUpgradeError, LrtqUpgradeMsg, Node, NodeCallerCtx,
    NodeCommonCtx, NodeCtx, PersistentState, PrepareAndCommitError,
    ReconfigurationError, ReconfigureMsg, ReconstructedRackSecret,
};

/// Whether or not a configuration has committed or is still underway.
pub enum CommitStatus {
    Committed,
    Pending,
}

/// We only expect a handful of messages at a time.
const API_CHANNEL_BOUND: usize = 32;

/// We size this bound large enough that it should never be hit. Up to 31
/// `EstablishedConn` tasks can send messages to the main task simultaneously when
/// messages are received.
///
/// We use `try_send.unwrap()` when sending to the main task to prevent deadlock
/// and inform us via panic that something has gone seriously wrong. This is
/// similar to using an unbounded channel but will not use all possible memory.
const CONN_TO_MAIN_CHANNEL_BOUND: usize = 1024;

#[derive(Debug, Clone)]
pub struct Config {
    pub baseboard_id: BaseboardId,
    pub listen_addr: SocketAddrV6,
    //    pub tq_state_ledger_paths: Vec<Utf8PathBuf>,
    //  pub network_config_ledger_paths: Vec<Utf8PathBuf>,
    pub sprockets: SprocketsConfig,
}

/// Status of the node coordinating the `Prepare` phase of a reconfiguration or
/// LRTQ upgrade.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorStatus {
    config: Configuration,
    acked_prepares: BTreeSet<BaseboardId>,
}

// Details about a given node's status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatus {
    connected_peers: BTreeSet<BaseboardId>,
    alarms: BTreeSet<Alarm>,
    persistent_state: NodePersistentStateSummary,
}

/// A summary of a node's persistent state, leaving out things like key shares
/// and hashes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodePersistentStateSummary {
    has_lrtq_share: bool,
    configs: BTreeSet<Epoch>,
    shares: BTreeSet<Epoch>,
    commits: BTreeSet<Epoch>,
    expunged: Option<ExpungedMetadata>,
}

impl From<&PersistentState> for NodePersistentStateSummary {
    fn from(value: &PersistentState) -> Self {
        Self {
            has_lrtq_share: value.lrtq.is_some(),
            configs: value.configs.iter().map(|c| c.epoch).collect(),
            shares: value.shares.keys().cloned().collect(),
            commits: value.commits.clone(),
            expunged: value.expunged.clone(),
        }
    }
}

/// A request sent to the `NodeTask` from the `NodeTaskHandle`
pub enum NodeApiRequest {
    /// Inform the `Node` of currently known IP addresses on the bootstrap network
    ///
    /// These are generated from DDM prefixes learned by the bootstrap agent.
    BootstrapAddresses(BTreeSet<SocketAddrV6>),

    /// Remove any secrets cached in memory at this node
    ClearSecrets,

    /// Retrieve connectivity status via the `ConnMgr`
    ConnMgrStatus { tx: oneshot::Sender<ConnMgrStatus> },

    /// Return the status of this node if it is a coordinator
    CoordinatorStatus { tx: oneshot::Sender<Option<CoordinatorStatus>> },

    /// Load a rack secret for the given epoch
    LoadRackSecret {
        epoch: Epoch,
        tx: oneshot::Sender<
            Result<Option<ReconstructedRackSecret>, LoadRackSecretError>,
        >,
    },

    /// Coordinate an upgrade from LRTQ at this node
    LrtqUpgrade {
        msg: LrtqUpgradeMsg,
        tx: oneshot::Sender<Result<(), LrtqUpgradeError>>,
    },

    /// Get the overall status of the node
    NodeStatus { tx: oneshot::Sender<NodeStatus> },

    /// `PrepareAndCommit` a configuration at this node
    PrepareAndCommit {
        config: Configuration,
        tx: oneshot::Sender<Result<CommitStatus, PrepareAndCommitError>>,
    },

    /// `Commit` a configuration at this node
    Commit {
        rack_id: RackUuid,
        epoch: Epoch,
        tx: oneshot::Sender<Result<CommitStatus, CommitError>>,
    },

    /// Coordinate a reconfiguration at this node
    Reconfigure {
        msg: ReconfigureMsg,
        tx: oneshot::Sender<Result<(), ReconfigurationError>>,
    },

    /// Shutdown the node's tokio tasks
    Shutdown,
}

/// An error response from a `NodeApiRequest`
#[derive(Error, Debug, PartialEq)]
pub enum NodeApiError {
    #[error("failed to send request to node task")]
    Send,
    #[error("failed to receive response from node task")]
    Recv,
    #[error("failed to reconfigure trust quorum")]
    Reconfigure(#[from] ReconfigurationError),
    #[error("failed to load rack secret")]
    LoadRackSecret(#[from] LoadRackSecretError),
    #[error("failed to upgrade from LRTQ")]
    LrtqUpgrade(#[from] LrtqUpgradeError),
    #[error("failed to prepare and commit")]
    PrepareAndCommit(#[from] PrepareAndCommitError),
    #[error("failed to commit")]
    Commit(#[from] CommitError),
}

impl<T> From<SendError<T>> for NodeApiError {
    fn from(_: SendError<T>) -> Self {
        NodeApiError::Send
    }
}

impl From<RecvError> for NodeApiError {
    fn from(_: RecvError) -> Self {
        NodeApiError::Recv
    }
}

#[derive(Debug, Clone)]
pub struct NodeTaskHandle {
    baseboard_id: BaseboardId,
    tx: mpsc::Sender<NodeApiRequest>,
    listen_addr: SocketAddrV6,
}

impl NodeTaskHandle {
    /// Return the actual ip and port being listened on
    ///
    /// This is useful when the port passed in was `0`.
    pub fn listen_addr(&self) -> SocketAddrV6 {
        self.listen_addr
    }

    pub fn baseboard_id(&self) -> &BaseboardId {
        &self.baseboard_id
    }

    /// Initiate a trust quorum reconfiguration at this node
    pub async fn reconfigure(
        &self,
        msg: ReconfigureMsg,
    ) -> Result<(), NodeApiError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(NodeApiRequest::Reconfigure { msg, tx: tx }).await?;
        rx.await??;
        Ok(())
    }

    /// Initiate an LRTQ upgrade at this node
    pub async fn upgrade_from_lrtq(
        &self,
        msg: LrtqUpgradeMsg,
    ) -> Result<(), NodeApiError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(NodeApiRequest::LrtqUpgrade { msg, tx: tx }).await?;
        rx.await??;
        Ok(())
    }

    /// Return the status of this node if it is coordinating the `Prepare` phase
    /// of a reconfiguration or LRTQ upgrade. Return `Ok(None)` or an error
    /// otherwise.
    pub async fn coordinator_status(
        &self,
    ) -> Result<Option<CoordinatorStatus>, NodeApiError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(NodeApiRequest::CoordinatorStatus { tx: tx }).await?;
        let res = rx.await?;
        Ok(res)
    }

    /// Load the rack secret for the given epoch
    ///
    /// This can block for an indefinite period of time before returning
    /// and depends on availability of the trust quorum.
    pub async fn load_rack_secret(
        &self,
        epoch: Epoch,
    ) -> Result<Option<ReconstructedRackSecret>, NodeApiError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(NodeApiRequest::LoadRackSecret { epoch, tx: tx }).await?;
        let rs = rx.await??;
        Ok(rs)
    }

    /// Return `Ok(true)` if the configuration has committed, `Ok(false)` if
    /// it hasn't committed yet, or an error otherwise.
    ///
    /// Nexus will retry this operation and so we should only try once here.
    /// This is in contrast to operations like `load_rack_secret` that are
    /// called directly from sled agent.
    pub async fn prepare_and_commit(
        &self,
        config: Configuration,
    ) -> Result<CommitStatus, NodeApiError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(NodeApiRequest::PrepareAndCommit { config, tx: tx })
            .await?;
        let res = rx.await??;
        Ok(res)
    }

    /// Return `Ok(true)` if the configuration has committed, `Ok(false)` if
    /// it hasn't committed yet, or an error otherwise.
    ///
    /// Nexus will retry this operation and so we should only try once here.
    /// This is in contrast to operations like `load_rack_secret` that are
    /// called directly from sled agent.
    pub async fn commit(
        &self,
        rack_id: RackUuid,
        epoch: Epoch,
    ) -> Result<CommitStatus, NodeApiError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(NodeApiRequest::Commit { rack_id, epoch, tx: tx }).await?;
        let res = rx.await??;
        Ok(res)
    }

    /// Clear all secrets loaded in memory at this node
    ///
    /// Rack secrets are cached after loading and must be manually cleared.
    pub async fn clear_secrets(&self) -> Result<(), NodeApiError> {
        self.tx.send(NodeApiRequest::ClearSecrets).await?;
        Ok(())
    }

    /// Inform the node of currently known IP addresses on the bootstrap network
    ///
    /// These are generated from DDM prefixes learned by the bootstrap agent.
    pub async fn load_peer_addresses(
        &self,
        addrs: BTreeSet<SocketAddrV6>,
    ) -> Result<(), NodeApiError> {
        self.tx.send(NodeApiRequest::BootstrapAddresses(addrs)).await?;
        Ok(())
    }

    /// Return information about connectivity to other peers
    pub async fn conn_mgr_status(&self) -> Result<ConnMgrStatus, NodeApiError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(NodeApiRequest::ConnMgrStatus { tx: tx }).await?;
        let res = rx.await?;
        Ok(res)
    }

    /// Return internal information for the [`Node`]
    pub async fn status(&self) -> Result<NodeStatus, NodeApiError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(NodeApiRequest::NodeStatus { tx: tx }).await?;
        let res = rx.await?;
        Ok(res)
    }

    /// Shutdown this [`NodeTask`] and all its child tasks
    pub async fn shutdown(&self) -> Result<(), NodeApiError> {
        self.tx.send(NodeApiRequest::Shutdown).await?;
        Ok(())
    }
}

pub struct NodeTask {
    shutdown: bool,
    log: Logger,
    #[expect(unused)]
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
            "baseboard_id" => config.baseboard_id.to_string()
        ));

        let (tx, rx) = mpsc::channel(API_CHANNEL_BOUND);

        let (conn_mgr_tx, conn_mgr_rx) =
            mpsc::channel(CONN_TO_MAIN_CHANNEL_BOUND);

        let baseboard_id = config.baseboard_id.clone();

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
        let listen_addr = conn_mgr.listen_addr();
        (
            NodeTask {
                shutdown: false,
                log,
                config,
                node,
                ctx,
                conn_mgr,
                conn_mgr_rx,
                rx,
            },
            NodeTaskHandle { baseboard_id, tx, listen_addr },
        )
    }

    /// Run the main loop of the node
    ///
    /// This should be spawned into its own tokio task
    pub async fn run(&mut self) {
        while !self.shutdown {
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

            for envelope in self.ctx.drain_envelopes() {
                self.conn_mgr.send(envelope).await;
            }
        }
    }

    // Handle messages from connection management tasks
    async fn on_conn_msg(&mut self, msg: ConnToMainMsg) {
        let task_id = msg.task_id;
        match msg.msg {
            ConnToMainMsgInner::Accepted { addr, peer_id } => {
                self.conn_mgr
                    .server_handshake_completed(task_id, addr, peer_id.clone())
                    .await;
                self.node.on_connect(&mut self.ctx, peer_id);
            }
            ConnToMainMsgInner::Connected { addr, peer_id } => {
                self.conn_mgr
                    .client_handshake_completed(task_id, addr, peer_id.clone())
                    .await;
                self.node.on_connect(&mut self.ctx, peer_id);
            }
            ConnToMainMsgInner::Disconnected { peer_id } => {
                self.conn_mgr.on_disconnected(task_id, peer_id.clone()).await;
                self.node.on_disconnect(&mut self.ctx, peer_id);
            }
            ConnToMainMsgInner::Received { from, msg } => {
                self.node.handle(&mut self.ctx, from, msg);
            }
            ConnToMainMsgInner::ReceivedNetworkConfig {
                from: _,
                config: _,
            } => {
                todo!();
            }
        }
    }

    // TODO: Process `ctx`: save persistent state
    async fn on_api_request(&mut self, request: NodeApiRequest) {
        match request {
            NodeApiRequest::BootstrapAddresses(addrs) => {
                info!(self.log, "Updated Peer Addresses: {addrs:?}");
                // TODO: real corpus
                let corpus = vec![];
                let disconnected = self
                    .conn_mgr
                    .update_bootstrap_connections(addrs, corpus)
                    .await;
                for peer_id in disconnected {
                    self.node.on_disconnect(&mut self.ctx, peer_id);
                }
            }
            NodeApiRequest::ClearSecrets => {
                self.node.clear_secrets();
            }
            NodeApiRequest::Commit { rack_id, epoch, tx } => {
                let res = self
                    .node
                    .commit_configuration(&mut self.ctx, rack_id, epoch)
                    .map(|_| {
                        if self.ctx.persistent_state().commits.contains(&epoch)
                        {
                            CommitStatus::Committed
                        } else {
                            CommitStatus::Pending
                        }
                    });
                let _ = tx.send(res);
            }
            NodeApiRequest::ConnMgrStatus { tx } => {
                debug!(self.log, "Received Request for ConnMgrStatus");
                let _ = tx.send(self.conn_mgr.status());
            }
            NodeApiRequest::CoordinatorStatus { tx } => {
                let status = self.node.get_coordinator_state().map(|cs| {
                    CoordinatorStatus {
                        config: cs.config().clone(),
                        acked_prepares: cs.op().acked_prepares(),
                    }
                });
                let _ = tx.send(status);
            }
            NodeApiRequest::LoadRackSecret { epoch, tx } => {
                let res = self.node.load_rack_secret(&mut self.ctx, epoch);
                let _ = tx.send(res);
            }
            NodeApiRequest::LrtqUpgrade { msg, tx } => {
                let res =
                    self.node.coordinate_upgrade_from_lrtq(&mut self.ctx, msg);
                let _ = tx.send(res);
            }
            NodeApiRequest::NodeStatus { tx } => {
                let _ = tx.send(NodeStatus {
                    connected_peers: self.ctx.connected().clone(),
                    alarms: self.ctx.alarms().clone(),
                    persistent_state: self.ctx.persistent_state().into(),
                });
            }
            NodeApiRequest::PrepareAndCommit { config, tx } => {
                let epoch = config.epoch;
                let res = self
                    .node
                    .prepare_and_commit(&mut self.ctx, config)
                    .map(|_| {
                        if self.ctx.persistent_state().commits.contains(&epoch)
                        {
                            CommitStatus::Committed
                        } else {
                            CommitStatus::Pending
                        }
                    });
                let _ = tx.send(res);
            }
            NodeApiRequest::Reconfigure { msg, tx } => {
                let res =
                    self.node.coordinate_reconfiguration(&mut self.ctx, msg);
                let _ = tx.send(res);
            }
            NodeApiRequest::Shutdown => {
                info!(self.log, "Shutting down Node tokio tasks");
                self.shutdown = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection_manager::{
        ConnState, RECONNECT_TIME, platform_id_to_baseboard_id,
    };
    use camino::Utf8PathBuf;
    use dropshot::test_util::{LogContext, log_prefix_for_test};
    use omicron_test_utils::dev::poll::{CondCheckError, wait_for_condition};
    use omicron_test_utils::dev::{self, test_setup_log};
    use omicron_uuid_kinds::GenericUuid;
    use secrecy::ExposeSecretMut;
    use sled_hardware_types::Baseboard;
    use sprockets_tls::keys::ResolveSetting;
    use sprockets_tls_test_utils::{
        alias_prefix, cert_path, certlist_path, private_key_path, root_prefix,
        sprockets_auth_prefix,
    };
    use std::time::Duration;
    use tokio::task::JoinHandle;
    use trust_quorum_protocol::NodeHandlerCtx;

    fn pki_doc_to_node_configs(dir: Utf8PathBuf, n: usize) -> Vec<Config> {
        (1..=n)
            .map(|i| {
                let baseboard_id = platform_id_to_baseboard_id(
                    &sprockets_tls_test_utils::platform_id(i),
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

    fn write_keys_and_measurements(dir: Utf8PathBuf, num_nodes: usize) {
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
    }

    struct TestSetup {
        pub logctx: LogContext,
        pub dir: Utf8PathBuf,
        pub configs: Vec<Config>,
        pub node_handles: Vec<NodeTaskHandle>,
        pub join_handles: Vec<JoinHandle<()>>,
        pub listen_addrs: Vec<SocketAddrV6>,
    }

    impl TestSetup {
        pub async fn spawn_nodes(
            name: &'static str,
            num_nodes: usize,
        ) -> TestSetup {
            let logctx = test_setup_log(name);
            let (mut dir, s) = log_prefix_for_test(name);
            dir.push(&s);
            std::fs::create_dir(&dir).unwrap();
            println!("Writing keys and certs to {dir}");
            write_keys_and_measurements(dir.clone(), num_nodes);
            let configs = pki_doc_to_node_configs(dir.clone(), num_nodes);

            let mut node_handles = vec![];
            let mut join_handles = vec![];
            for config in configs.clone() {
                let (mut task, handle) =
                    NodeTask::new(config, &logctx.log).await;
                node_handles.push(handle);
                join_handles
                    .push(tokio::spawn(async move { task.run().await }));
            }

            let listen_addrs: Vec<_> =
                node_handles.iter().map(|h| h.listen_addr()).collect();
            TestSetup {
                logctx,
                dir,
                configs,
                node_handles,
                join_handles,
                listen_addrs,
            }
        }

        pub async fn spawn_nodes_with_lrtq_shares(
            name: &'static str,
            num_nodes: usize,
        ) -> (TestSetup, RackUuid) {
            let logctx = test_setup_log(name);
            let (mut dir, s) = log_prefix_for_test(name);
            dir.push(&s);
            std::fs::create_dir(&dir).unwrap();
            println!("Writing keys and certs to {dir}");
            write_keys_and_measurements(dir.clone(), num_nodes);
            let configs = pki_doc_to_node_configs(dir.clone(), num_nodes);

            let rack_id = RackUuid::new_v4();

            // Translate `BaseboardId`s to `Baseboard`s for LRTQ membership
            let baseboards: BTreeSet<_> = configs
                .iter()
                .map(|c| {
                    Baseboard::new_pc(
                        c.baseboard_id.serial_number.clone(),
                        c.baseboard_id.part_number.clone(),
                    )
                })
                .collect();

            // Create the LRTQ key share packages and take only the common data,
            // which is what we use for trust quorum upgrade.
            let share_pkgs: Vec<_> = bootstore::schemes::v0::create_pkgs(
                rack_id.into_untyped_uuid(),
                baseboards.clone(),
            )
            .unwrap()
            .expose_secret_mut()
            .iter()
            .map(|pkg| pkg.common.clone())
            .collect();

            let mut node_handles = vec![];
            let mut join_handles = vec![];
            for (config, share_pkg) in
                configs.clone().into_iter().zip(share_pkgs)
            {
                let (mut task, handle) =
                    NodeTask::new(config, &logctx.log).await;
                task.ctx.update_persistent_state(|ps| {
                    ps.lrtq = Some(share_pkg);
                    // We are modifying the persistent state, but not in a way
                    // we want the test to recognize.
                    false
                });
                node_handles.push(handle);
                join_handles
                    .push(tokio::spawn(async move { task.run().await }));
            }

            let listen_addrs: Vec<_> =
                node_handles.iter().map(|h| h.listen_addr()).collect();
            (
                TestSetup {
                    logctx,
                    dir,
                    configs,
                    node_handles,
                    join_handles,
                    listen_addrs,
                },
                rack_id,
            )
        }

        pub fn members(&self) -> impl Iterator<Item = &BaseboardId> {
            self.configs.iter().map(|c| &c.baseboard_id)
        }

        pub fn cleanup_successful(self) {
            self.logctx.cleanup_successful();
            std::fs::remove_dir_all(self.dir).unwrap();
        }

        pub async fn wait_for_rack_secrets_and_assert_equality(
            &self,
            node_indexes: BTreeSet<usize>,
            epoch: Epoch,
        ) -> Result<(), dev::poll::Error<NodeApiError>> {
            let poll_interval = Duration::from_millis(10);
            let poll_max = Duration::from_secs(10);
            wait_for_condition(
                async || {
                    let mut secret = None;
                    for (i, h) in self.node_handles.iter().enumerate() {
                        if node_indexes.contains(&i) {
                            let Some(rs) = h.load_rack_secret(epoch).await?
                            else {
                                return Err(CondCheckError::NotYet);
                            };
                            if secret.is_none() {
                                secret = Some(rs.clone());
                            }
                            assert_eq!(&rs, secret.as_ref().unwrap());
                        }
                    }
                    Ok(())
                },
                &poll_interval,
                &poll_max,
            )
            .await
        }
    }

    /// Test that all nodes can connect to each other when given each the full
    /// set of "bootstrap addresses".
    #[tokio::test]
    async fn full_mesh_connectivity() {
        let num_nodes = 4;
        let mut setup =
            TestSetup::spawn_nodes("full_mesh_connectivity", num_nodes).await;

        for h in &setup.node_handles {
            h.load_peer_addresses(setup.listen_addrs.iter().cloned().collect())
                .await
                .unwrap();
        }

        let poll_interval = Duration::from_millis(1);
        let poll_max = Duration::from_secs(10);

        // Wait for all nodes have `num_nodes - 1` established connections
        wait_for_condition(
            async || {
                let mut count = 0;
                for h in &setup.node_handles {
                    let status = h.conn_mgr_status().await.unwrap();
                    if status
                        .connections
                        .iter()
                        .all(|c| matches!(c.state, ConnState::Established(_)))
                        && status.connections.len() == num_nodes - 1
                        && status.total_tasks_spawned == 3
                    {
                        count += 1;
                    }
                }
                if count == num_nodes {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &poll_interval,
            &poll_max,
        )
        .await
        .unwrap();

        // Pause time so we can jump it for reconnects
        tokio::time::pause();

        // Killing a single node should cause all other nodes to start
        // reconnecting. This should cause the task id counter to start
        // incrementing at all nodes and for their to be one fewer established
        // connection.
        let h = setup.node_handles.pop().unwrap();
        h.shutdown().await.unwrap();
        setup.join_handles.pop().unwrap();
        let stopped_addr = h.listen_addr;

        // Speed up reconnection in the test
        tokio::time::advance(RECONNECT_TIME).await;

        let poll_interval = Duration::from_millis(50);
        wait_for_condition(
            async || {
                let mut valid = 0;
                for h in &setup.node_handles {
                    let status = h.conn_mgr_status().await.unwrap();
                    let established_count = status
                        .connections
                        .iter()
                        .filter(|c| {
                            matches!(c.state, ConnState::Established(_))
                        })
                        .count();

                    // Nodes only connect to other nodes if their listening
                    // address sorts greater. The only node where a reconnect will be attempted
                    // is the stopped node.
                    let should_be_connecting = h.listen_addr > stopped_addr;
                    let valid_task_id = if should_be_connecting {
                        status.total_tasks_spawned > 3
                    } else {
                        true
                    };
                    if established_count == num_nodes - 2 && valid_task_id {
                        valid += 1;
                    }
                }
                if valid == num_nodes - 1 {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &poll_interval,
            &poll_max,
        )
        .await
        .unwrap();

        // Now let's bring back up the old node and ensure full connectivity again
        let (mut task, handle) = NodeTask::new(
            setup.configs.last().unwrap().clone(),
            &setup.logctx.log,
        )
        .await;
        setup.node_handles.push(handle.clone());
        setup.join_handles.push(tokio::spawn(async move { task.run().await }));

        // The port likely changed, so we must refresh everyone's set of addresses
        let listen_addrs: BTreeSet<_> =
            setup.node_handles.iter().map(|h| h.listen_addr()).collect();

        for h in &setup.node_handles {
            h.load_peer_addresses(listen_addrs.clone()).await.unwrap();
        }

        // Wait for all nodes have `num_nodes - 1` established connections
        wait_for_condition(
            async || {
                let mut count = 0;
                for h in &setup.node_handles {
                    let status = h.conn_mgr_status().await.unwrap();
                    if status
                        .connections
                        .iter()
                        .all(|c| matches!(c.state, ConnState::Established(_)))
                        && status.connections.len() == num_nodes - 1
                    {
                        count += 1;
                    }
                }
                if count == num_nodes {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &poll_interval,
            &poll_max,
        )
        .await
        .unwrap();

        setup.cleanup_successful();
    }

    /// Commit an initial configuration at all nodes
    #[tokio::test]
    pub async fn tq_initial_config() {
        let num_nodes = 4;
        let setup =
            TestSetup::spawn_nodes("tq_initial_config", num_nodes).await;
        let rack_id = RackUuid::new_v4();

        // Trigger an initial configuration by using the first node as a
        // coordinator. We're pretending to be the sled-agent with instruction from
        // Nexus here.
        let initial_config = ReconfigureMsg {
            rack_id,
            epoch: Epoch(1),
            last_committed_epoch: None,
            members: setup.members().cloned().collect(),
            threshold: trust_quorum_protocol::Threshold(3),
        };

        // Tell nodes how to reach each other
        for h in &setup.node_handles {
            h.load_peer_addresses(setup.listen_addrs.iter().cloned().collect())
                .await
                .unwrap();
        }

        let coordinator = setup.node_handles.first().unwrap();
        coordinator.reconfigure(initial_config).await.unwrap();

        let poll_interval = Duration::from_millis(10);
        let poll_max = Duration::from_secs(10);

        // Wait for the coordinator to see `PrepareAck`s from all nodes
        wait_for_condition(
            async || {
                let Ok(Some(s)) = coordinator.coordinator_status().await else {
                    return Err(CondCheckError::<()>::NotYet);
                };
                if s.acked_prepares.len() == num_nodes {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &poll_interval,
            &poll_max,
        )
        .await
        .unwrap();

        // Commit at each node
        //
        // Nexus retries this idempotent command until each node acks. So we
        // simulate that here.
        wait_for_condition(
            async || {
                let mut acked = 0;
                for h in &setup.node_handles {
                    if matches!(
                        h.commit(rack_id, Epoch(1)).await.unwrap(),
                        CommitStatus::Committed
                    ) {
                        acked += 1;
                    }
                }
                if acked == num_nodes {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &poll_interval,
            &poll_max,
        )
        .await
        .unwrap();

        // Now load the rack secret at all nodes
        setup
            .wait_for_rack_secrets_and_assert_equality(
                (0..num_nodes).collect(),
                Epoch(1),
            )
            .await
            .unwrap();

        setup.cleanup_successful();
    }

    /// Eventually Commit an initial configuration at all nodes
    ///
    /// We leave one node out of the bootstrap network info, trigger a commit
    /// at the first 3 nodes. Then we go and issue a `PrepareAndCommit` to the last
    /// node and ensure it commits.
    #[tokio::test]
    pub async fn tq_initial_config_prepare_and_commit() {
        let num_nodes = 4;
        let setup = TestSetup::spawn_nodes(
            "tq_initial_config_prepare_and_commit",
            num_nodes,
        )
        .await;
        let rack_id = RackUuid::new_v4();

        // Trigger an initial configuration by using the first node as a
        // coordinator. We're pretending to be the sled-agent with instruction from
        // Nexus here.
        let initial_config = ReconfigureMsg {
            rack_id,
            epoch: Epoch(1),
            last_committed_epoch: None,
            members: setup.members().cloned().collect(),
            threshold: trust_quorum_protocol::Threshold(3),
        };

        // Tell all but the last node how to reach each other
        for h in &setup.node_handles[0..num_nodes - 1] {
            h.load_peer_addresses(
                setup
                    .listen_addrs
                    .iter()
                    .take(num_nodes - 1)
                    .cloned()
                    .collect(),
            )
            .await
            .unwrap();
        }

        let coordinator = setup.node_handles.first().unwrap();
        coordinator.reconfigure(initial_config).await.unwrap();

        let poll_interval = Duration::from_millis(10);
        let poll_max = Duration::from_secs(10);

        // Wait for the coordinator to see `PrepareAck`s from all but the last
        // node
        wait_for_condition(
            async || {
                let Ok(Some(s)) = coordinator.coordinator_status().await else {
                    return Err(CondCheckError::<()>::NotYet);
                };
                if s.acked_prepares.len() == num_nodes - 1 {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &poll_interval,
            &poll_max,
        )
        .await
        .unwrap();

        // Save the configuration as if we were nexus
        let config =
            coordinator.coordinator_status().await.unwrap().unwrap().config;

        // Commit at each node
        //
        // Nexus retries this idempotent command until each node acks. So we
        // simulate that here.
        wait_for_condition(
            async || {
                let mut acked = 0;
                for h in &setup.node_handles[0..num_nodes - 1] {
                    if matches!(
                        h.commit(rack_id, Epoch(1)).await.unwrap(),
                        CommitStatus::Committed,
                    ) {
                        acked += 1;
                    }
                }
                if acked == num_nodes - 1 {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &poll_interval,
            &poll_max,
        )
        .await
        .unwrap();

        // Now ensure that the last node still hasn't prepared or committed for
        // epoch 1, and isn't connected to any other node.
        let status = setup.node_handles.last().unwrap().status().await.unwrap();
        assert!(status.connected_peers.is_empty());
        assert!(status.persistent_state.configs.is_empty());
        assert!(status.persistent_state.shares.is_empty());
        assert!(status.persistent_state.commits.is_empty());

        // Update connectivity at all nodes
        for h in &setup.node_handles {
            h.load_peer_addresses(setup.listen_addrs.iter().cloned().collect())
                .await
                .unwrap();
        }

        // Now issue a `PrepareAndCommit` to the last node and wait for it to
        // commit
        wait_for_condition(
            async || {
                let h = &setup.node_handles.last().unwrap();
                if matches!(
                    h.prepare_and_commit(config.clone()).await.unwrap(),
                    CommitStatus::Committed
                ) {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &poll_interval,
            &poll_max,
        )
        .await
        .unwrap();

        // The last node should now have all the info we expect
        let status = setup.node_handles.last().unwrap().status().await.unwrap();
        assert_eq!(status.connected_peers.len(), num_nodes - 1);
        assert!(status.persistent_state.configs.contains(&Epoch(1)));
        assert!(status.persistent_state.shares.contains(&Epoch(1)));
        assert!(status.persistent_state.commits.contains(&Epoch(1)));

        // Now load the rack secret at all nodes
        setup
            .wait_for_rack_secrets_and_assert_equality(
                (0..num_nodes).collect(),
                Epoch(1),
            )
            .await
            .unwrap();

        setup.cleanup_successful();
    }

    /// Perform an initial config, followed by a reconfiguration. Leave one
    /// node out of the reconfiguration, then connect it and attempt to load
    /// the configuration for the prior epoch. This should result in commit
    /// advancing to the latest epoch.
    #[tokio::test]
    pub async fn tq_reconfig_with_commit_advance() {
        let num_nodes = 4;
        let setup = TestSetup::spawn_nodes(
            "tq_recofnig_with_commit_advance",
            num_nodes,
        )
        .await;
        let rack_id = RackUuid::new_v4();

        // Trigger an initial configuration by using the first node as a
        // coordinator. We're pretending to be the sled-agent with instruction from
        // Nexus here.
        let initial_config = ReconfigureMsg {
            rack_id,
            epoch: Epoch(1),
            last_committed_epoch: None,
            members: setup.members().cloned().collect(),
            threshold: trust_quorum_protocol::Threshold(3),
        };

        // Tell all but the last node how to reach each other
        for h in &setup.node_handles {
            h.load_peer_addresses(setup.listen_addrs.iter().cloned().collect())
                .await
                .unwrap();
        }

        let coordinator = setup.node_handles.first().unwrap();
        coordinator.reconfigure(initial_config.clone()).await.unwrap();

        let poll_interval = Duration::from_millis(10);
        let poll_max = Duration::from_secs(10);

        // Wait for the coordinator to see `PrepareAck`s from all nodes
        wait_for_condition(
            async || {
                let Ok(Some(s)) = coordinator.coordinator_status().await else {
                    return Err(CondCheckError::<()>::NotYet);
                };
                if s.acked_prepares.len() == num_nodes {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &poll_interval,
            &poll_max,
        )
        .await
        .unwrap();

        // Commit at each node
        //
        // Nexus retries this idempotent command until each node acks. So we
        // simulate that here.
        wait_for_condition(
            async || {
                let mut acked = 0;
                for h in &setup.node_handles {
                    if matches!(
                        h.commit(rack_id, Epoch(1)).await.unwrap(),
                        CommitStatus::Committed
                    ) {
                        acked += 1;
                    }
                }
                if acked == num_nodes {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &poll_interval,
            &poll_max,
        )
        .await
        .unwrap();

        // Now load the rack secret at all nodes
        setup
            .wait_for_rack_secrets_and_assert_equality(
                (0..num_nodes).collect(),
                Epoch(1),
            )
            .await
            .unwrap();

        // Tell all but the last node how to reach each other
        // This should disconnect the last node from everybody
        for h in &setup.node_handles[0..num_nodes - 1] {
            h.load_peer_addresses(
                setup.listen_addrs.iter().take(3).cloned().collect(),
            )
            .await
            .unwrap();
        }
        setup
            .node_handles
            .last()
            .unwrap()
            .load_peer_addresses(BTreeSet::new())
            .await
            .unwrap();

        // Wait for peers to disconnect
        wait_for_condition(
            async || {
                let mut acked = 0;
                for h in &setup.node_handles[0..num_nodes - 1] {
                    let status = h.status().await.unwrap();
                    if status.connected_peers.len() == num_nodes - 2 {
                        acked += 1;
                    }
                }
                let status =
                    setup.node_handles.last().unwrap().status().await.unwrap();
                if status.connected_peers.is_empty() {
                    acked += 1;
                }

                if acked == num_nodes {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &poll_interval,
            &poll_max,
        )
        .await
        .unwrap();

        // Just stick to the same set of nodes for simplicity
        let mut new_config = initial_config;
        new_config.epoch = Epoch(2);
        new_config.last_committed_epoch = Some(Epoch(1));

        // Pick a different coordinator for the hell of it
        let coordinator = setup.node_handles.get(1).unwrap();
        coordinator.reconfigure(new_config).await.unwrap();

        // Wait for the coordinator to see `PrepareAck`s from all but the last
        // node
        wait_for_condition(
            async || {
                let Ok(Some(s)) = coordinator.coordinator_status().await else {
                    return Err(CondCheckError::<()>::NotYet);
                };
                if s.acked_prepares.len() == num_nodes - 1 {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &poll_interval,
            &poll_max,
        )
        .await
        .unwrap();

        // Commit at each node
        //
        // Nexus retries this idempotent command until each node acks. So we
        // simulate that here.
        wait_for_condition(
            async || {
                let mut acked = 0;
                for h in &setup.node_handles[0..num_nodes - 1] {
                    if matches!(
                        h.commit(rack_id, Epoch(2)).await.unwrap(),
                        CommitStatus::Committed
                    ) {
                        acked += 1;
                    }
                }
                if acked == num_nodes - 1 {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &poll_interval,
            &poll_max,
        )
        .await
        .unwrap();

        // Now ensure that the last node still hasn't prepared or committed for epoch 2,
        // and isn't connected to any other node.
        let status = setup.node_handles.last().unwrap().status().await.unwrap();
        assert!(status.connected_peers.is_empty());
        assert!(status.persistent_state.configs.contains(&Epoch(1)));
        assert!(status.persistent_state.shares.contains(&Epoch(1)));
        assert!(status.persistent_state.commits.contains(&Epoch(1)));
        assert!(!status.persistent_state.configs.contains(&Epoch(2)));
        assert!(!status.persistent_state.shares.contains(&Epoch(2)));
        assert!(!status.persistent_state.commits.contains(&Epoch(2)));

        // Now reconnect the last node.
        for h in &setup.node_handles {
            h.load_peer_addresses(setup.listen_addrs.iter().cloned().collect())
                .await
                .unwrap();
        }

        // Clear the rack secrets at the last node to force a request for shares.
        let last_node = setup.node_handles.last().unwrap();
        last_node.clear_secrets().await.unwrap();

        // Load the secret at epoch 1. This should trigger a `CommitAdvance`
        // response from nodes that committed at epoch 2.
        setup
            .wait_for_rack_secrets_and_assert_equality(
                BTreeSet::from([num_nodes - 1]),
                Epoch(1),
            )
            .await
            .unwrap();

        // Ensure the rack secret at epoch 2 is the same as at another node
        setup
            .wait_for_rack_secrets_and_assert_equality(
                BTreeSet::from([0, num_nodes - 1]),
                Epoch(2),
            )
            .await
            .unwrap();

        setup.cleanup_successful();
    }

    #[tokio::test]
    pub async fn tq_upgrade_from_lrtq() {
        let num_nodes = 4;
        let (setup, rack_id) = TestSetup::spawn_nodes_with_lrtq_shares(
            "tq_upgrade_from_lrtq",
            num_nodes,
        )
        .await;

        let msg = LrtqUpgradeMsg {
            rack_id,
            epoch: Epoch(2),
            members: setup.members().cloned().collect(),
            threshold: trust_quorum_protocol::Threshold(3),
        };

        // Tell nodes how to reach each other
        for h in &setup.node_handles {
            h.load_peer_addresses(setup.listen_addrs.iter().cloned().collect())
                .await
                .unwrap();
        }

        let coordinator = setup.node_handles.first().unwrap();
        coordinator.upgrade_from_lrtq(msg).await.unwrap();

        let poll_interval = Duration::from_millis(10);
        let poll_max = Duration::from_secs(10);

        // Wait for the coordinator to see `PrepareAck`s from all nodes
        wait_for_condition(
            async || {
                let Ok(Some(s)) = coordinator.coordinator_status().await else {
                    return Err(CondCheckError::<()>::NotYet);
                };
                if s.acked_prepares.len() == num_nodes {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &poll_interval,
            &poll_max,
        )
        .await
        .unwrap();

        // Commit at each node
        //
        // Nexus retries this idempotent command until each node acks. So we
        // simulate that here.
        wait_for_condition(
            async || {
                let mut acked = 0;
                for h in &setup.node_handles {
                    if matches!(
                        h.commit(rack_id, Epoch(2)).await.unwrap(),
                        CommitStatus::Committed
                    ) {
                        acked += 1;
                    }
                }
                if acked == num_nodes {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &poll_interval,
            &poll_max,
        )
        .await
        .unwrap();

        // Now load the rack secret at all nodes
        setup
            .wait_for_rack_secrets_and_assert_equality(
                (0..num_nodes).collect(),
                Epoch(1),
            )
            .await
            .unwrap();

        setup.cleanup_successful();
    }
}
