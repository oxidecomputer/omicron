// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for controlling multiple instances on a sled.

use crate::instance::propolis_zone_name;
use crate::instance::Instance;
use crate::metrics::MetricsRequestQueue;
use crate::nexus::NexusClientWithResolver;
use crate::params::InstanceExternalIpBody;
use crate::params::InstanceMetadata;
use crate::params::ZoneBundleMetadata;
use crate::params::{
    InstanceHardware, InstancePutStateResponse, InstanceStateRequested,
    InstanceUnregisterResponse,
};
use crate::vmm_reservoir::VmmReservoirManagerHandle;
use crate::zone_bundle::BundleError;
use crate::zone_bundle::ZoneBundler;
use omicron_common::api::external::ByteCount;

use anyhow::anyhow;
use illumos_utils::dladm::Etherstub;
use illumos_utils::link::VnicAllocator;
use illumos_utils::opte::PortManager;
use illumos_utils::running_zone::ZoneBuilderFactory;
use omicron_common::api::external::Generation;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use omicron_common::api::internal::nexus::SledInstanceState;
use omicron_common::api::internal::nexus::VmmRuntimeState;
use omicron_common::api::internal::shared::SledIdentifiers;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::PropolisUuid;
use sled_storage::manager::StorageHandle;
use sled_storage::resources::AllDisks;
use slog::Logger;
use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

// The depth of the request queue for the instance manager.
const QUEUE_SIZE: usize = 256;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Instance error: {0}")]
    Instance(#[from] crate::instance::Error),

    #[error("No such instance ID: {0}")]
    NoSuchInstance(InstanceUuid),

    #[error("OPTE port management error: {0}")]
    Opte(#[from] illumos_utils::opte::Error),

    #[error("Cannot find data link: {0}")]
    Underlay(#[from] sled_hardware::underlay::Error),

    #[error("Zone bundle error")]
    ZoneBundle(#[from] BundleError),

    #[error("Failed to send request to Instance Manager: Channel closed")]
    FailedSendInstanceManagerClosed,

    #[error(
        "Failed to send request from Instance Manager: Client Channel closed"
    )]
    FailedSendClientClosed,

    #[error("Instance Manager dropped our request")]
    RequestDropped(#[from] oneshot::error::RecvError),
}

pub(crate) struct InstanceManagerServices {
    pub nexus_client: NexusClientWithResolver,
    pub vnic_allocator: VnicAllocator<Etherstub>,
    pub port_manager: PortManager,
    pub storage: StorageHandle,
    pub zone_bundler: ZoneBundler,
    pub zone_builder_factory: ZoneBuilderFactory,
    pub metrics_queue: MetricsRequestQueue,
}

// Describes the internals of the "InstanceManager", though most of the
// instance manager's state exists within the "InstanceManagerRunner" structure.
struct InstanceManagerInternal {
    tx: mpsc::Sender<InstanceManagerRequest>,
    vmm_reservoir_manager: VmmReservoirManagerHandle,

    #[allow(dead_code)]
    runner_handle: tokio::task::JoinHandle<()>,
}

/// All instances currently running on the sled.
pub struct InstanceManager {
    inner: Arc<InstanceManagerInternal>,
}

impl InstanceManager {
    /// Initializes a new [`InstanceManager`] object.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        log: Logger,
        nexus_client: NexusClientWithResolver,
        etherstub: Etherstub,
        port_manager: PortManager,
        storage: StorageHandle,
        zone_bundler: ZoneBundler,
        zone_builder_factory: ZoneBuilderFactory,
        vmm_reservoir_manager: VmmReservoirManagerHandle,
        metrics_queue: MetricsRequestQueue,
    ) -> Result<InstanceManager, Error> {
        let (tx, rx) = mpsc::channel(QUEUE_SIZE);
        let (terminate_tx, terminate_rx) = mpsc::unbounded_channel();

        let log = log.new(o!("component" => "InstanceManager"));
        let runner = InstanceManagerRunner {
            log: log.clone(),
            rx,
            terminate_tx,
            terminate_rx,
            nexus_client,
            instances: BTreeMap::new(),
            vnic_allocator: VnicAllocator::new("Instance", etherstub),
            port_manager,
            storage_generation: None,
            storage,
            zone_bundler,
            zone_builder_factory,
            metrics_queue,
        };

        let runner_handle =
            tokio::task::spawn(async move { runner.run().await });

        Ok(Self {
            inner: Arc::new(InstanceManagerInternal {
                tx,
                vmm_reservoir_manager,
                runner_handle,
            }),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn ensure_registered(
        &self,
        instance_id: InstanceUuid,
        propolis_id: PropolisUuid,
        hardware: InstanceHardware,
        instance_runtime: InstanceRuntimeState,
        vmm_runtime: VmmRuntimeState,
        propolis_addr: SocketAddr,
        sled_identifiers: SledIdentifiers,
        metadata: InstanceMetadata,
    ) -> Result<SledInstanceState, Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(InstanceManagerRequest::EnsureRegistered {
                instance_id,
                propolis_id,
                hardware,
                instance_runtime,
                vmm_runtime,
                propolis_addr,
                sled_identifiers,
                metadata,
                tx,
            })
            .await
            .map_err(|_| Error::FailedSendInstanceManagerClosed)?;
        rx.await?
    }

    pub async fn ensure_unregistered(
        &self,
        instance_id: InstanceUuid,
    ) -> Result<InstanceUnregisterResponse, Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(InstanceManagerRequest::EnsureUnregistered {
                instance_id,
                tx,
            })
            .await
            .map_err(|_| Error::FailedSendInstanceManagerClosed)?;
        rx.await?
    }

    pub async fn ensure_state(
        &self,
        instance_id: InstanceUuid,
        target: InstanceStateRequested,
    ) -> Result<InstancePutStateResponse, Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(InstanceManagerRequest::EnsureState {
                instance_id,
                target,
                tx,
            })
            .await
            .map_err(|_| Error::FailedSendInstanceManagerClosed)?;

        match target {
            // these may involve a long-running zone creation, so avoid HTTP
            // request timeouts by decoupling the response
            // (see InstanceRunner::put_state)
            InstanceStateRequested::MigrationTarget(_)
            | InstanceStateRequested::Running => {
                // We don't want the sending side of the channel to see an
                // error if we drop rx without awaiting it.
                // Since we don't care about the response here, we spawn rx
                // into a task which will await it for us in the background.
                tokio::spawn(rx);
                Ok(InstancePutStateResponse { updated_runtime: None })
            }
            InstanceStateRequested::Stopped
            | InstanceStateRequested::Reboot => rx.await?,
        }
    }

    pub async fn instance_issue_disk_snapshot_request(
        &self,
        instance_id: InstanceUuid,
        disk_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(InstanceManagerRequest::InstanceIssueDiskSnapshot {
                instance_id,
                disk_id,
                snapshot_id,
                tx,
            })
            .await
            .map_err(|_| Error::FailedSendInstanceManagerClosed)?;
        rx.await?
    }

    /// Create a zone bundle from a named instance zone, if it exists.
    pub async fn create_zone_bundle(
        &self,
        name: &str,
    ) -> Result<ZoneBundleMetadata, BundleError> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(InstanceManagerRequest::CreateZoneBundle {
                name: name.to_string(),
                tx,
            })
            .await
            .map_err(|err| BundleError::FailedSend(anyhow!(err)))?;
        rx.await.map_err(|err| BundleError::DroppedRequest(anyhow!(err)))?
    }

    pub async fn add_external_ip(
        &self,
        instance_id: InstanceUuid,
        ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(InstanceManagerRequest::InstanceAddExternalIp {
                instance_id,
                ip: *ip,
                tx,
            })
            .await
            .map_err(|_| Error::FailedSendInstanceManagerClosed)?;
        rx.await?
    }

    pub async fn delete_external_ip(
        &self,
        instance_id: InstanceUuid,
        ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(InstanceManagerRequest::InstanceDeleteExternalIp {
                instance_id,
                ip: *ip,
                tx,
            })
            .await
            .map_err(|_| Error::FailedSendInstanceManagerClosed)?;
        rx.await?
    }

    /// Returns the last-set size of the reservoir
    pub fn reservoir_size(&self) -> ByteCount {
        self.inner.vmm_reservoir_manager.reservoir_size()
    }

    pub async fn get_instance_state(
        &self,
        instance_id: InstanceUuid,
    ) -> Result<SledInstanceState, Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(InstanceManagerRequest::GetState { instance_id, tx })
            .await
            .map_err(|_| Error::FailedSendInstanceManagerClosed)?;
        rx.await?
    }

    /// Marks instances failed unless they're using storage from `disks`.
    ///
    /// This function looks for transient zone filesystem usage on expunged
    /// zpools.
    pub async fn use_only_these_disks(
        &self,
        disks: AllDisks,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(InstanceManagerRequest::OnlyUseDisks { disks, tx })
            .await
            .map_err(|_| Error::FailedSendInstanceManagerClosed)?;
        rx.await?
    }
}

// Most requests that can be sent to the "InstanceManagerRunner" task.
//
// These messages are sent by "InstanceManager"'s interface, and processed by
// the runner task.
//
// By convention, responses are sent on the "tx" oneshot.
#[derive(strum::Display)]
enum InstanceManagerRequest {
    EnsureRegistered {
        instance_id: InstanceUuid,
        propolis_id: PropolisUuid,
        hardware: InstanceHardware,
        instance_runtime: InstanceRuntimeState,
        vmm_runtime: VmmRuntimeState,
        propolis_addr: SocketAddr,
        sled_identifiers: SledIdentifiers,
        metadata: InstanceMetadata,
        tx: oneshot::Sender<Result<SledInstanceState, Error>>,
    },
    EnsureUnregistered {
        instance_id: InstanceUuid,
        tx: oneshot::Sender<Result<InstanceUnregisterResponse, Error>>,
    },
    EnsureState {
        instance_id: InstanceUuid,
        target: InstanceStateRequested,
        tx: oneshot::Sender<Result<InstancePutStateResponse, Error>>,
    },

    InstanceIssueDiskSnapshot {
        instance_id: InstanceUuid,
        disk_id: Uuid,
        snapshot_id: Uuid,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    CreateZoneBundle {
        name: String,
        tx: oneshot::Sender<Result<ZoneBundleMetadata, BundleError>>,
    },
    InstanceAddExternalIp {
        instance_id: InstanceUuid,
        ip: InstanceExternalIpBody,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    InstanceDeleteExternalIp {
        instance_id: InstanceUuid,
        ip: InstanceExternalIpBody,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    GetState {
        instance_id: InstanceUuid,
        tx: oneshot::Sender<Result<SledInstanceState, Error>>,
    },
    OnlyUseDisks {
        disks: AllDisks,
        tx: oneshot::Sender<Result<(), Error>>,
    },
}

// Requests that the instance manager stop processing information about a
// particular instance.
struct InstanceDeregisterRequest {
    id: InstanceUuid,
}

struct InstanceManagerRunner {
    log: Logger,

    // Request channel on which most instance manager requests are made.
    rx: mpsc::Receiver<InstanceManagerRequest>,

    // Request channel for "stop tracking instances", removing instances
    // from "self.instances".
    //
    // Although this is "unbounded", in practice, it cannot be larger than the
    // number of currently running instances, and it will be cleared before
    // new instances may be requested.
    //
    // We hold both sizes of this channel, and we give clones of the
    // sender to new instance objects that are created.
    terminate_tx: mpsc::UnboundedSender<InstanceDeregisterRequest>,
    terminate_rx: mpsc::UnboundedReceiver<InstanceDeregisterRequest>,

    nexus_client: NexusClientWithResolver,

    // TODO: If we held an object representing an enum of "Created OR Running"
    // instance, we could avoid the methods within "instance.rs" that panic
    // if the Propolis client hasn't been initialized.
    /// A mapping from a Sled Agent "Instance ID" to ("Propolis ID", [Instance]).
    instances: BTreeMap<InstanceUuid, (PropolisUuid, Instance)>,

    vnic_allocator: VnicAllocator<Etherstub>,
    port_manager: PortManager,
    storage_generation: Option<Generation>,
    storage: StorageHandle,
    zone_bundler: ZoneBundler,
    zone_builder_factory: ZoneBuilderFactory,
    metrics_queue: MetricsRequestQueue,
}

impl InstanceManagerRunner {
    /// Run the main loop of the InstanceManager.
    ///
    /// This should be spawned in a tokio task.
    pub async fn run(mut self) {
        use InstanceManagerRequest::*;
        loop {
            tokio::select! {
                biased;

                // If anyone has made a request to remove an instance from our
                // state tracking, we do so before processing subsequent
                // requests. This ensure that "add, remove, add" of the same
                // instance always works.
                request = self.terminate_rx.recv() => {
                    match request {
                        Some(request) => {
                            self.instances.remove(&request.id);
                        },
                        None => {
                            warn!(self.log, "InstanceManager's 'instance terminate' channel closed; shutting down");
                            break;
                        },
                    }
                },
                request = self.rx.recv() => {
                    let request_variant = request.as_ref().map(|r| r.to_string());
                    let result = match request {
                        Some(EnsureRegistered {
                            instance_id,
                            propolis_id,
                            hardware,
                            instance_runtime,
                            vmm_runtime,
                            propolis_addr,
                            sled_identifiers,
                            metadata,
                            tx,
                        }) => {
                            tx.send(self.ensure_registered(
                                instance_id,
                                propolis_id,
                                hardware,
                                instance_runtime,
                                vmm_runtime,
                                propolis_addr,
                                sled_identifiers,
                                metadata
                            ).await).map_err(|_| Error::FailedSendClientClosed)
                        },
                        Some(EnsureUnregistered { instance_id, tx }) => {
                            self.ensure_unregistered(tx, instance_id).await
                        },
                        Some(EnsureState { instance_id, target, tx }) => {
                            self.ensure_state(tx, instance_id, target).await
                        },
                        Some(InstanceIssueDiskSnapshot { instance_id, disk_id, snapshot_id, tx }) => {
                            self.instance_issue_disk_snapshot_request(tx, instance_id, disk_id, snapshot_id).await
                        },
                        Some(CreateZoneBundle { name, tx }) => {
                            self.create_zone_bundle(tx, &name).await.map_err(Error::from)
                        },
                        Some(InstanceAddExternalIp { instance_id, ip, tx }) => {
                            self.add_external_ip(tx, instance_id, &ip).await
                        },
                        Some(InstanceDeleteExternalIp { instance_id, ip, tx }) => {
                            self.delete_external_ip(tx, instance_id, &ip).await
                        },
                        Some(GetState { instance_id, tx }) => {
                            // TODO(eliza): it could potentially be nice to
                            // refactor this to use `tokio::sync::watch`, rather
                            // than having to force `GetState` requests to
                            // serialize with the requests that actually update
                            // the state...
                            self.get_instance_state(tx, instance_id).await
                        },
                        Some(OnlyUseDisks { disks, tx } ) => {
                            self.use_only_these_disks(disks).await;
                            tx.send(Ok(())).map_err(|_| Error::FailedSendClientClosed)
                        },
                        None => {
                            warn!(self.log, "InstanceManager's request channel closed; shutting down");
                            break;
                        },
                    };

                    if let Err(err) = result {
                        warn!(
                            self.log,
                            "Error handling request";
                            "request" => request_variant.unwrap(),
                            "err" => ?err
                        );
                    }
                }
            }
        }
    }

    fn get_instance(&self, instance_id: InstanceUuid) -> Option<&Instance> {
        self.instances.get(&instance_id).map(|(_id, v)| v)
    }

    /// Ensures that the instance manager contains a registered instance with
    /// the supplied instance ID and the Propolis ID specified in
    /// `initial_hardware`.
    ///
    /// # Arguments
    ///
    /// * instance_id: The ID of the instance to register.
    /// * initial_hardware: The initial hardware manifest and runtime state of
    ///   the instance, to be used if the instance does not already exist.
    ///
    /// # Return value
    ///
    /// `Ok` if the instance is registered with the supplied Propolis ID, `Err`
    /// otherwise. This routine is idempotent if called to register the same
    /// (instance ID, Propolis ID) pair multiple times, but will fail if the
    /// instance is registered with a Propolis ID different from the one the
    /// caller supplied.
    #[allow(clippy::too_many_arguments)]
    pub async fn ensure_registered(
        &mut self,
        instance_id: InstanceUuid,
        propolis_id: PropolisUuid,
        hardware: InstanceHardware,
        instance_runtime: InstanceRuntimeState,
        vmm_runtime: VmmRuntimeState,
        propolis_addr: SocketAddr,
        sled_identifiers: SledIdentifiers,
        metadata: InstanceMetadata,
    ) -> Result<SledInstanceState, Error> {
        info!(
            &self.log,
            "ensuring instance is registered";
            "instance_id" => %instance_id,
            "propolis_id" => %propolis_id,
            "hardware" => ?hardware,
            "instance_runtime" => ?instance_runtime,
            "vmm_runtime" => ?vmm_runtime,
            "propolis_addr" => ?propolis_addr,
            "metadata" => ?metadata,
        );

        let instance = {
            if let Some((existing_propolis_id, existing_instance)) =
                self.instances.get(&instance_id)
            {
                if propolis_id != *existing_propolis_id {
                    info!(&self.log,
                          "instance already registered with another Propolis ID";
                          "instance_id" => %instance_id,
                          "existing_propolis_id" => %*existing_propolis_id);
                    return Err(Error::Instance(
                        crate::instance::Error::InstanceAlreadyRegistered(
                            *existing_propolis_id,
                        ),
                    ));
                } else {
                    info!(
                        &self.log,
                        "instance already registered with requested Propolis ID"
                    );
                    existing_instance
                }
            } else {
                info!(&self.log,
                      "registering new instance";
                      "instance_id" => ?instance_id);
                let instance_log =
                    self.log.new(o!("instance_id" => format!("{instance_id}")));
                let ticket =
                    InstanceTicket::new(instance_id, self.terminate_tx.clone());

                let services = InstanceManagerServices {
                    nexus_client: self.nexus_client.clone(),
                    vnic_allocator: self.vnic_allocator.clone(),
                    port_manager: self.port_manager.clone(),
                    storage: self.storage.clone(),
                    zone_bundler: self.zone_bundler.clone(),
                    zone_builder_factory: self.zone_builder_factory.clone(),
                    metrics_queue: self.metrics_queue.clone(),
                };

                let state = crate::instance::InstanceInitialState {
                    hardware,
                    vmm_runtime,
                    propolis_addr,
                    migration_id: instance_runtime.migration_id,
                };

                let instance = Instance::new(
                    instance_log,
                    instance_id,
                    propolis_id,
                    ticket,
                    state,
                    services,
                    sled_identifiers,
                    metadata,
                )?;
                let _old =
                    self.instances.insert(instance_id, (propolis_id, instance));
                assert!(_old.is_none());
                &self.instances.get(&instance_id).unwrap().1
            }
        };

        Ok(instance.current_state().await?)
    }

    /// Idempotently ensures the instance is not registered with this instance
    /// manager. If the instance exists and has a running Propolis, that
    /// Propolis is rudely terminated.
    async fn ensure_unregistered(
        &mut self,
        tx: oneshot::Sender<Result<InstanceUnregisterResponse, Error>>,
        instance_id: InstanceUuid,
    ) -> Result<(), Error> {
        // If the instance does not exist, we response immediately.
        let Some(instance) = self.get_instance(instance_id) else {
            tx.send(Ok(InstanceUnregisterResponse { updated_runtime: None }))
                .map_err(|_| Error::FailedSendClientClosed)?;
            return Ok(());
        };

        // Otherwise, we pipeline the request, and send it to the instance,
        // where it can receive an appropriate response.
        let mark_failed = false;
        instance.terminate(tx, mark_failed).await?;
        Ok(())
    }

    /// Idempotently attempts to drive the supplied instance into the supplied
    /// runtime state.
    async fn ensure_state(
        &mut self,
        tx: oneshot::Sender<Result<InstancePutStateResponse, Error>>,
        instance_id: InstanceUuid,
        target: InstanceStateRequested,
    ) -> Result<(), Error> {
        let Some(instance) = self.get_instance(instance_id) else {
            match target {
                // If the instance isn't registered, then by definition it
                // isn't running here. Allow requests to stop or destroy the
                // instance to succeed to provide idempotency. This has to
                // be handled here (that is, on the "instance not found"
                // path) to handle the case where a stop request arrived,
                // Propolis handled it, sled agent unregistered the
                // instance, and only then did a second stop request
                // arrive.
                InstanceStateRequested::Stopped => {
                    tx.send(Ok(InstancePutStateResponse {
                        updated_runtime: None,
                    }))
                    .map_err(|_| Error::FailedSendClientClosed)?;
                }
                _ => {
                    tx.send(Err(Error::NoSuchInstance(instance_id)))
                        .map_err(|_| Error::FailedSendClientClosed)?;
                }
            }
            return Ok(());
        };
        instance.put_state(tx, target).await?;
        Ok(())
    }

    async fn instance_issue_disk_snapshot_request(
        &self,
        tx: oneshot::Sender<Result<(), Error>>,
        instance_id: InstanceUuid,
        disk_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        let instance = {
            let (_, instance) = self
                .instances
                .get(&instance_id)
                .ok_or(Error::NoSuchInstance(instance_id))?;
            instance
        };

        instance
            .issue_snapshot_request(tx, disk_id, snapshot_id)
            .await
            .map_err(Error::from)
    }

    /// Create a zone bundle from a named instance zone, if it exists.
    async fn create_zone_bundle(
        &self,
        tx: oneshot::Sender<Result<ZoneBundleMetadata, BundleError>>,
        name: &str,
    ) -> Result<(), BundleError> {
        let Some((_propolis_id, instance)) =
            self.instances.values().find(|(propolis_id, _instance)| {
                name == propolis_zone_name(propolis_id)
            })
        else {
            return Err(BundleError::NoSuchZone { name: name.to_string() });
        };
        instance.request_zone_bundle(tx).await
    }

    async fn add_external_ip(
        &self,
        tx: oneshot::Sender<Result<(), Error>>,
        instance_id: InstanceUuid,
        ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        let Some(instance) = self.get_instance(instance_id) else {
            return Err(Error::NoSuchInstance(instance_id));
        };
        instance.add_external_ip(tx, ip).await?;
        Ok(())
    }

    async fn delete_external_ip(
        &self,
        tx: oneshot::Sender<Result<(), Error>>,
        instance_id: InstanceUuid,
        ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        let Some(instance) = self.get_instance(instance_id) else {
            return Err(Error::NoSuchInstance(instance_id));
        };

        instance.delete_external_ip(tx, ip).await?;
        Ok(())
    }

    async fn get_instance_state(
        &self,
        tx: oneshot::Sender<Result<SledInstanceState, Error>>,
        instance_id: InstanceUuid,
    ) -> Result<(), Error> {
        let Some(instance) = self.get_instance(instance_id) else {
            return tx
                .send(Err(Error::NoSuchInstance(instance_id)))
                .map_err(|_| Error::FailedSendClientClosed);
        };

        let state = instance.current_state().await?;
        tx.send(Ok(state)).map_err(|_| Error::FailedSendClientClosed)?;
        Ok(())
    }

    async fn use_only_these_disks(&mut self, disks: AllDisks) {
        // Consider the generation number on the incoming request to avoid
        // applying old requests.
        let requested_generation = *disks.generation();
        if let Some(last_gen) = self.storage_generation {
            if last_gen >= requested_generation {
                // This request looks old, ignore it.
                info!(self.log, "use_only_these_disks: Ignoring request";
                    "last_gen" => ?last_gen, "requested_gen" => ?requested_generation);
                return;
            }
        }
        self.storage_generation = Some(requested_generation);
        info!(self.log, "use_only_these_disks: Processing new request";
            "gen" => ?requested_generation);

        let u2_set: HashSet<_> = disks.all_u2_zpools().into_iter().collect();

        let mut to_remove = vec![];
        for (id, (_, instance)) in self.instances.iter() {
            // If we can read the filesystem pool, consider it. Otherwise, move
            // on, to prevent blocking the cleanup of other instances.
            let Ok(Some(filesystem_pool)) =
                instance.get_filesystem_zpool().await
            else {
                info!(self.log, "use_only_these_disks: Cannot read filesystem pool"; "instance_id" => ?id);
                continue;
            };
            if !u2_set.contains(&filesystem_pool) {
                to_remove.push(*id);
            }
        }

        for id in to_remove {
            info!(self.log, "use_only_these_disks: Removing instance"; "instance_id" => ?id);
            if let Some((_, instance)) = self.instances.remove(&id) {
                let (tx, rx) = oneshot::channel();
                let mark_failed = true;
                if let Err(e) = instance.terminate(tx, mark_failed).await {
                    warn!(self.log, "use_only_these_disks: Failed to request instance removal"; "err" => ?e);
                    continue;
                }

                if let Err(e) = rx.await {
                    warn!(self.log, "use_only_these_disks: Failed while removing instance"; "err" => ?e);
                }
            }
        }
    }
}

/// Represents membership of an instance in the [`InstanceManager`].
pub struct InstanceTicket {
    id: InstanceUuid,
    terminate_tx: Option<mpsc::UnboundedSender<InstanceDeregisterRequest>>,
}

impl InstanceTicket {
    // Creates a new instance ticket for instance "id" to be removed
    // from the manger on destruction.
    fn new(
        id: InstanceUuid,
        terminate_tx: mpsc::UnboundedSender<InstanceDeregisterRequest>,
    ) -> Self {
        InstanceTicket { id, terminate_tx: Some(terminate_tx) }
    }

    #[cfg(all(test, target_os = "illumos"))]
    pub(crate) fn new_without_manager_for_test(id: InstanceUuid) -> Self {
        Self { id, terminate_tx: None }
    }

    /// Idempotently removes this instance from the tracked set of
    /// instances. This acts as an "upcall" for instances to remove
    /// themselves after stopping.
    pub fn deregister(&mut self) {
        if let Some(terminate_tx) = self.terminate_tx.take() {
            let _ =
                terminate_tx.send(InstanceDeregisterRequest { id: self.id });
        }
    }
}

impl Drop for InstanceTicket {
    fn drop(&mut self) {
        self.deregister();
    }
}
