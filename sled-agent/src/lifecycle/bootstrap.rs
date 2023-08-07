// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TODO explanatory comment

use self::sprockets_server::SprocketsServer;

use super::StartError;
use crate::bootstrap::agent::BootstrapError;
use crate::bootstrap::agent::PersistentSledAgentRequest;
use crate::bootstrap::agent::RssAccess;
use crate::bootstrap::agent::RssAccessError;
use crate::bootstrap::config::BOOTSTORE_PORT;
use crate::bootstrap::config::BOOTSTRAP_AGENT_HTTP_PORT;
use crate::bootstrap::config::BOOTSTRAP_AGENT_RACK_INIT_PORT;
use crate::bootstrap::params::StartSledAgentRequest;
use crate::bootstrap::views::SledAgentResponse;
use crate::storage_manager::StorageResources;
use crate::updates::ConfigUpdates;
use crate::updates::UpdateManager;
use bootstore::schemes::v0 as bootstore;
use camino::Utf8PathBuf;
use cancel_safe_futures::TryStreamExt;
use ddm_admin_client::Client as DdmAdminClient;
use dropshot::HttpServer;
use futures::StreamExt;
use http::StatusCode;
use illumos_utils::zfs;
use illumos_utils::zone::Zones;
use omicron_common::ledger::Ledger;
use sled_hardware::underlay::BootstrapInterface;
use slog::Logger;
use std::collections::BTreeSet;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

mod sprockets_server;

const BOOTSTORE_FSM_STATE_FILE: &str = "bootstore-fsm-state.json";
const BOOTSTORE_NETWORK_CONFIG_FILE: &str = "bootstore-network-config.json";

pub(super) struct SledAgentBootstrap {
    pub(super) maybe_ledger:
        Option<Ledger<PersistentSledAgentRequest<'static>>>,
    pub(super) bootstrap_http_server: HttpServer<BootstrapServerContext>,
    pub(super) bootstore_node_handle: bootstore::NodeHandle,
    pub(super) sprockets_server_handle: JoinHandle<()>,
    pub(super) sled_reset_rx:
        mpsc::Receiver<oneshot::Sender<Result<(), BootstrapError>>>,
    pub(super) sled_init_rx: mpsc::Receiver<(
        StartSledAgentRequest,
        oneshot::Sender<Result<SledAgentResponse, String>>,
    )>,
}

impl SledAgentBootstrap {
    pub(super) async fn run(
        storage_resources: &StorageResources,
        baseboard: Baseboard,
        global_zone_bootstrap_ip: Ipv6Addr,
        updates: ConfigUpdates,
        ddm_admin_localhost_client: DdmAdminClient,
        base_log: &Logger,
    ) -> Result<Self, StartError> {
        let log = base_log.new(o!("component" => "SledAgentBootstrap"));

        // Wait for our boot M.2 to show up.
        wait_for_boot_m2(&storage_resources, &log).await;

        // Wait for the bootstore to start.
        let bootstore = spawn_bootstore_tasks(
            &storage_resources,
            ddm_admin_localhost_client,
            baseboard.clone(),
            global_zone_bootstrap_ip,
            base_log,
        )
        .await?;

        // Do we have a StartSledAgentRequest stored in the ledger?
        let maybe_ledger = read_persistent_sled_agent_request_from_ledger(
            &storage_resources,
            &log,
        )
        .await?;

        // We don't yet _act_ on the `StartSledAgentRequest` if we have one, but
        // if we have one we init our `RssAccess` noting that we're already
        // initialized. Our caller is responsible for actually starting the
        // sled-agent described by `maybe_ledger`.
        let rss_access = RssAccess::new(maybe_ledger.is_some());

        // Create a channel for requesting sled reset. We use a channel depth
        // one 1: if there's a pending sled reset request, there's no need to
        // enqueue another.
        let (sled_reset_tx, sled_reset_rx) = mpsc::channel(1);

        // Start the bootstrap dropshot server.
        let bootstrap_context = BootstrapServerContext {
            base_log: base_log.clone(),
            global_zone_bootstrap_ip,
            storage_resources: storage_resources.clone(),
            bootstore_node_handle: bootstore.node_handle.clone(),
            baseboard,
            rss_access,
            updates,
            sled_reset_tx,
        };
        let bootstrap_http_server = start_dropshot_server(bootstrap_context)?;

        // Start the currently-misnamed sprockets server, which listens for raw
        // TCP connections (which should ultimately be secured via sprockets).
        let (sled_init_tx, sled_init_rx) = mpsc::channel(1);
        let sprockets_server = SprocketsServer::bind(
            SocketAddrV6::new(
                global_zone_bootstrap_ip,
                BOOTSTRAP_AGENT_RACK_INIT_PORT,
                0,
                0,
            ),
            sled_init_tx,
            &base_log,
        )
        .await
        .map_err(StartError::BindSprocketsServer)?;
        let sprockets_server_handle = tokio::spawn(sprockets_server.run());

        Ok(Self {
            maybe_ledger,
            bootstore_node_handle: bootstore.node_handle,
            // TODO other bootstore.foo handles?
            bootstrap_http_server,
            sprockets_server_handle,
            sled_reset_rx,
            sled_init_rx,
        })
    }
}

/// Wait for at least the M.2 we booted from to show up.
///
/// TODO-correctness Subsequent steps may assume all M.2s that will ever be
/// present are present once we return from this function; see
/// https://github.com/oxidecomputer/omicron/issues/3815.
async fn wait_for_boot_m2(storage_resources: &StorageResources, log: &Logger) {
    // Wait for at least the M.2 we booted from to show up.
    loop {
        match storage_resources.boot_disk().await {
            Some(disk) => {
                info!(log, "Found boot disk M.2: {disk:?}");
                break;
            }
            None => {
                info!(log, "Waiting for boot disk M.2...");
                tokio::time::sleep(core::time::Duration::from_millis(250))
                    .await;
            }
        }
    }
}

struct BootstoreHandles {
    node_handle: bootstore::NodeHandle,
    join_handle: JoinHandle<()>,
    peer_update_handle: JoinHandle<()>,
}

async fn spawn_bootstore_tasks(
    storage_resources: &StorageResources,
    ddm_admin_client: DdmAdminClient,
    baseboard: Baseboard,
    global_zone_bootstrap_ip: Ipv6Addr,
    base_log: &Logger,
) -> Result<BootstoreHandles, StartError> {
    let config = bootstore::Config {
        id: baseboard,
        addr: SocketAddrV6::new(global_zone_bootstrap_ip, BOOTSTORE_PORT, 0, 0),
        time_per_tick: Duration::from_millis(250),
        learn_timeout: Duration::from_secs(5),
        rack_init_timeout: Duration::from_secs(300),
        rack_secret_request_timeout: Duration::from_secs(5),
        fsm_state_ledger_paths: bootstore_fsm_state_paths(&storage_resources)
            .await?,
        network_config_ledger_paths: bootstore_network_config_paths(
            &storage_resources,
        )
        .await?,
    };

    let (mut node, node_handle) = bootstore::Node::new(config, base_log).await;

    let join_handle = tokio::spawn(async move { node.run().await });

    // Spawn a task for polling DDMD and updating bootstore
    let peer_update_handle = tokio::spawn(poll_ddmd_for_bootstore_peer_update(
        base_log.new(o!("component" => "bootstore_ddmd_poller")),
        node_handle.clone(),
        ddm_admin_client,
    ));

    Ok(BootstoreHandles { node_handle, join_handle, peer_update_handle })
}

async fn bootstore_fsm_state_paths(
    storage: &StorageResources,
) -> Result<Vec<Utf8PathBuf>, StartError> {
    let paths: Vec<_> = storage
        .all_m2_mountpoints(sled_hardware::disk::CLUSTER_DATASET)
        .await
        .into_iter()
        .map(|p| p.join(BOOTSTORE_FSM_STATE_FILE))
        .collect();

    if paths.is_empty() {
        return Err(StartError::MissingM2Paths(
            sled_hardware::disk::CLUSTER_DATASET,
        ));
    }
    Ok(paths)
}

async fn bootstore_network_config_paths(
    storage: &StorageResources,
) -> Result<Vec<Utf8PathBuf>, StartError> {
    let paths: Vec<_> = storage
        .all_m2_mountpoints(sled_hardware::disk::CLUSTER_DATASET)
        .await
        .into_iter()
        .map(|p| p.join(BOOTSTORE_NETWORK_CONFIG_FILE))
        .collect();

    if paths.is_empty() {
        return Err(StartError::MissingM2Paths(
            sled_hardware::disk::CLUSTER_DATASET,
        ));
    }
    Ok(paths)
}

async fn poll_ddmd_for_bootstore_peer_update(
    log: Logger,
    bootstore_node_handle: bootstore::NodeHandle,
    ddmd_client: DdmAdminClient,
) {
    let mut current_peers: BTreeSet<SocketAddrV6> = BTreeSet::new();
    // We're talking to a service's admin interface on localhost and
    // we're only asking for its current state. We use a retry in a loop
    // instead of `backoff`.
    //
    // We also use this timeout in the case of spurious ddmd failures
    // that require a reconnection from the ddmd_client.
    const RETRY: tokio::time::Duration = tokio::time::Duration::from_secs(5);

    loop {
        match ddmd_client
            .derive_bootstrap_addrs_from_prefixes(&[
                BootstrapInterface::GlobalZone,
            ])
            .await
        {
            Ok(addrs) => {
                let peers: BTreeSet<_> = addrs
                    .map(|ip| SocketAddrV6::new(ip, BOOTSTORE_PORT, 0, 0))
                    .collect();
                if peers != current_peers {
                    current_peers = peers;
                    if let Err(e) = bootstore_node_handle
                        .load_peer_addresses(current_peers.clone())
                        .await
                    {
                        error!(
                            log,
                            concat!(
                                "Bootstore comms error: {}. ",
                                "bootstore::Node task must have paniced",
                            ),
                            e
                        );
                        return;
                    }
                }
            }
            Err(err) => {
                warn!(
                    log, "Failed to get prefixes from ddmd";
                    "err" => #%err,
                );
                break;
            }
        }
        tokio::time::sleep(RETRY).await;
    }
}

async fn read_persistent_sled_agent_request_from_ledger(
    storage_resources: &StorageResources,
    log: &Logger,
) -> Result<Option<Ledger<PersistentSledAgentRequest<'static>>>, StartError> {
    let paths = super::sled_config_paths(storage_resources).await?;
    let maybe_ledger =
        Ledger::<PersistentSledAgentRequest<'static>>::new(log, paths).await;
    Ok(maybe_ledger)
}

pub(super) fn start_dropshot_server(
    context: BootstrapServerContext,
) -> Result<HttpServer<BootstrapServerContext>, StartError> {
    let mut dropshot_config = dropshot::ConfigDropshot::default();
    dropshot_config.request_body_max_bytes = 1024 * 1024;
    dropshot_config.bind_address = SocketAddr::V6(SocketAddrV6::new(
        context.global_zone_bootstrap_ip,
        BOOTSTRAP_AGENT_HTTP_PORT,
        0,
        0,
    ));
    let dropshot_log =
        context.base_log.new(o!("component" => "dropshot (BootstrapAgent)"));
    let http_server = dropshot::HttpServerStarter::new(
        &dropshot_config,
        api(),
        context,
        &dropshot_log,
    )
    .map_err(|error| {
        StartError::InitBootstrapDropshotServer(error.to_string())
    })?
    .start();

    Ok(http_server)
}

pub(crate) struct BootstrapServerContext {
    base_log: Logger,
    global_zone_bootstrap_ip: Ipv6Addr,
    storage_resources: StorageResources,
    bootstore_node_handle: bootstore::NodeHandle,
    baseboard: Baseboard,
    rss_access: RssAccess,
    updates: ConfigUpdates,
    sled_reset_tx: mpsc::Sender<oneshot::Sender<Result<(), BootstrapError>>>,
}

impl BootstrapServerContext {
    pub(super) fn start_rack_initialize(
        &self,
        request: RackInitializeRequest,
    ) -> Result<RackInitId, RssAccessError> {
        self.rss_access.start_initializing_v2(
            &self.base_log,
            self.global_zone_bootstrap_ip,
            self.storage_resources.clone(),
            self.bootstore_node_handle.clone(),
            request,
        )
    }
}

// Uninstall all oxide zones (except the switch zone)
pub(super) async fn uninstall_zones() -> Result<(), BootstrapError> {
    const CONCURRENCY_CAP: usize = 32;
    futures::stream::iter(Zones::get().await?)
        .map(Ok::<_, anyhow::Error>)
        // Use for_each_concurrent_then_try to delete as much as possible.
        // We only return one error though -- hopefully that's enough to
        // signal to the caller that this failed.
        .for_each_concurrent_then_try(CONCURRENCY_CAP, |zone| async move {
            if zone.name() != "oxz_switch" {
                Zones::halt_and_remove(zone.name()).await?;
            }
            Ok(())
        })
        .await
        .map_err(BootstrapError::Cleanup)?;
    Ok(())
}

pub(super) async fn uninstall_sled_local_config(
    storage_resources: &StorageResources,
) -> Result<(), BootstrapError> {
    let config_dirs = storage_resources
        .all_m2_mountpoints(sled_hardware::disk::CONFIG_DATASET)
        .await
        .into_iter();

    for dir in config_dirs {
        for entry in dir.read_dir_utf8().map_err(|err| BootstrapError::Io {
            message: format!("Deleting {dir}"),
            err,
        })? {
            let entry = entry.map_err(|err| BootstrapError::Io {
                message: format!("Deleting {dir}"),
                err,
            })?;

            let path = entry.path();
            let file_type = entry.file_type().map_err(|err| {
                BootstrapError::Io { message: format!("Deleting {path}"), err }
            })?;

            if file_type.is_dir() {
                tokio::fs::remove_dir_all(path).await
            } else {
                tokio::fs::remove_file(path).await
            }
            .map_err(|err| BootstrapError::Io {
                message: format!("Deleting {path}"),
                err,
            })?;
        }
    }
    Ok(())
}

pub(super) async fn uninstall_networking(
    log: &Logger,
) -> Result<(), BootstrapError> {
    // NOTE: This is very similar to the invocations
    // in "sled_hardware::cleanup::cleanup_networking_resources",
    // with a few notable differences:
    //
    // - We can't remove bootstrap-related networking -- this operation
    // is performed via a request on the bootstrap network.
    // - We avoid deleting addresses using the chelsio link. Removing
    // these addresses would delete "cxgbe0/ll", and could render
    // the sled inaccessible via a local interface.

    sled_hardware::cleanup::delete_underlay_addresses(log)
        .map_err(BootstrapError::Cleanup)?;
    sled_hardware::cleanup::delete_omicron_vnics(log)
        .await
        .map_err(BootstrapError::Cleanup)?;
    illumos_utils::opte::delete_all_xde_devices(log)?;
    Ok(())
}

pub(super) async fn uninstall_storage(
    log: &Logger,
) -> Result<(), BootstrapError> {
    let datasets = zfs::get_all_omicron_datasets_for_delete()
        .map_err(BootstrapError::ZfsDatasetsList)?;
    for dataset in &datasets {
        info!(log, "Removing dataset: {dataset}");
        zfs::Zfs::destroy_dataset(dataset)?;
    }

    Ok(())
}

// --------------------------------------------
// TODO-john Move all of this back to bootstrap::http_entrypoints
// --------------------------------------------

use crate::bootstrap::agent::{RackInitId, RackResetId};
use crate::bootstrap::params::RackInitializeRequest;
use crate::bootstrap::RackOperationStatus;
use crate::updates::Component;
use dropshot::{
    endpoint, ApiDescription, HttpError, HttpResponseOk,
    HttpResponseUpdatedNoContent, RequestContext, TypedBody,
};
use omicron_common::api::external::Error;
use sled_hardware::Baseboard;

type BootstrapApiDescription = ApiDescription<BootstrapServerContext>;

/// Returns a description of the bootstrap agent API
pub(crate) fn api() -> BootstrapApiDescription {
    fn register_endpoints(
        api: &mut BootstrapApiDescription,
    ) -> Result<(), String> {
        api.register(baseboard_get)?;
        api.register(components_get)?;
        api.register(rack_initialization_status)?;
        api.register(rack_initialize)?;
        api.register(rack_reset)?;
        api.register(sled_reset)?;
        Ok(())
    }

    let mut api = BootstrapApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

/// Return the baseboard identity of this sled.
#[endpoint {
    method = GET,
    path = "/baseboard",
}]
async fn baseboard_get(
    rqctx: RequestContext<BootstrapServerContext>,
) -> Result<HttpResponseOk<Baseboard>, HttpError> {
    let ctx = rqctx.context();
    Ok(HttpResponseOk(ctx.baseboard.clone()))
}

/// Provides a list of components known to the bootstrap agent.
///
/// This API is intended to allow early boot services (such as Wicket)
/// to query the underlying component versions installed on a sled.
#[endpoint {
    method = GET,
    path = "/components",
}]
async fn components_get(
    rqctx: RequestContext<BootstrapServerContext>,
) -> Result<HttpResponseOk<Vec<Component>>, HttpError> {
    let ctx = rqctx.context();
    let updates = UpdateManager::new(ctx.updates.clone());
    let components = updates
        .components_get()
        .await
        .map_err(|err| HttpError::for_internal_error(err.to_string()))?;
    Ok(HttpResponseOk(components))
}

/// Get the current status of rack initialization or reset.
#[endpoint {
    method = GET,
    path = "/rack-initialize",
}]
async fn rack_initialization_status(
    rqctx: RequestContext<BootstrapServerContext>,
) -> Result<HttpResponseOk<RackOperationStatus>, HttpError> {
    let ctx = rqctx.context();
    let status = ctx.rss_access.operation_status();
    Ok(HttpResponseOk(status))
}

/// Initializes the rack with the provided configuration.
#[endpoint {
    method = POST,
    path = "/rack-initialize",
}]
async fn rack_initialize(
    rqctx: RequestContext<BootstrapServerContext>,
    body: TypedBody<RackInitializeRequest>,
) -> Result<HttpResponseOk<RackInitId>, HttpError> {
    let ctx = rqctx.context();
    let request = body.into_inner();
    let id = ctx
        .rss_access
        .start_initializing_v2(
            &ctx.base_log,
            ctx.global_zone_bootstrap_ip,
            ctx.storage_resources.clone(),
            ctx.bootstore_node_handle.clone(),
            request,
        )
        .map_err(|err| HttpError::for_bad_request(None, err.to_string()))?;
    Ok(HttpResponseOk(id))
}

/// Resets the rack to an unconfigured state.
#[endpoint {
    method = DELETE,
    path = "/rack-initialize",
}]
async fn rack_reset(
    rqctx: RequestContext<BootstrapServerContext>,
) -> Result<HttpResponseOk<RackResetId>, HttpError> {
    let ctx = rqctx.context();
    let id = ctx
        .rss_access
        .start_reset_v2(&ctx.base_log, ctx.global_zone_bootstrap_ip)
        .map_err(|err| HttpError::for_bad_request(None, err.to_string()))?;
    Ok(HttpResponseOk(id))
}

/// Resets this particular sled to an unconfigured state.
#[endpoint {
    method = DELETE,
    path = "/sled-initialize",
}]
async fn sled_reset(
    rqctx: RequestContext<BootstrapServerContext>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let ctx = rqctx.context();
    let (response_tx, response_rx) = oneshot::channel();

    let make_channel_closed_err = || {
        Err(HttpError::for_internal_error(
            "sled_reset channel closed: task panic?".to_string(),
        ))
    };

    match ctx.sled_reset_tx.try_send(response_tx) {
        Ok(()) => (),
        Err(TrySendError::Full(_)) => {
            return Err(HttpError::for_status(
                Some("ResetPending".to_string()),
                StatusCode::TOO_MANY_REQUESTS,
            ));
        }
        Err(TrySendError::Closed(_)) => {
            return make_channel_closed_err();
        }
    }

    match response_rx.await {
        Ok(result) => {
            () = result.map_err(Error::from)?;
            Ok(HttpResponseUpdatedNoContent())
        }
        Err(_) => make_channel_closed_err(),
    }
}
