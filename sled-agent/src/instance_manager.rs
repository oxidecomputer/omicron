// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for controlling multiple instances on a sled.

use crate::instance::Instance;
use crate::metrics::MetricsRequestQueue;
use crate::nexus::NexusClient;
use crate::vmm_reservoir::VmmReservoirManagerHandle;
use crate::zone_bundle::BundleError;
use crate::zone_bundle::ZoneBundler;
use illumos_utils::zone::PROPOLIS_ZONE_PREFIX;
use omicron_common::api::external::ByteCount;

use anyhow::anyhow;
use illumos_utils::dladm::Etherstub;
use illumos_utils::link::VnicAllocator;
use illumos_utils::opte::PortManager;
use illumos_utils::running_zone::ZoneBuilderFactory;
use omicron_common::api::external::Generation;
use omicron_common::api::internal::nexus::SledVmmState;
use omicron_common::api::internal::shared::SledIdentifiers;
use omicron_uuid_kinds::PropolisUuid;
use sled_agent_types::instance::*;
use sled_agent_types::zone_bundle::ZoneBundleMetadata;
use sled_storage::manager::StorageHandle;
use sled_storage::resources::AllDisks;
use slog::Logger;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

// The depth of the request queue for the instance manager.
const QUEUE_SIZE: usize = 256;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Instance error: {0}")]
    Instance(#[from] crate::instance::Error),

    #[error("VMM with ID {0} not found")]
    NoSuchVmm(PropolisUuid),

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
    pub nexus_client: NexusClient,
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
        nexus_client: NexusClient,
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
            jobs: BTreeMap::new(),
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

    pub async fn ensure_registered(
        &self,
        propolis_id: PropolisUuid,
        instance: InstanceEnsureBody,
        sled_identifiers: SledIdentifiers,
    ) -> Result<SledVmmState, Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(InstanceManagerRequest::EnsureRegistered {
                propolis_id,
                instance,
                sled_identifiers: Box::new(sled_identifiers),
                tx,
            })
            .await
            .map_err(|_| Error::FailedSendInstanceManagerClosed)?;
        rx.await?
    }

    pub async fn ensure_unregistered(
        &self,
        propolis_id: PropolisUuid,
    ) -> Result<VmmUnregisterResponse, Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(InstanceManagerRequest::EnsureUnregistered {
                propolis_id,
                tx,
            })
            .await
            .map_err(|_| Error::FailedSendInstanceManagerClosed)?;
        rx.await?
    }

    pub async fn ensure_state(
        &self,
        propolis_id: PropolisUuid,
        target: VmmStateRequested,
    ) -> Result<VmmPutStateResponse, Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(InstanceManagerRequest::EnsureState {
                propolis_id,
                target,
                tx,
            })
            .await
            .map_err(|_| Error::FailedSendInstanceManagerClosed)?;

        match target {
            // these may involve a long-running zone creation, so avoid HTTP
            // request timeouts by decoupling the response
            // (see InstanceRunner::put_state)
            VmmStateRequested::MigrationTarget(_)
            | VmmStateRequested::Running => {
                // We don't want the sending side of the channel to see an
                // error if we drop rx without awaiting it.
                // Since we don't care about the response here, we spawn rx
                // into a task which will await it for us in the background.
                tokio::spawn(rx);
                Ok(VmmPutStateResponse { updated_runtime: None })
            }
            VmmStateRequested::Stopped | VmmStateRequested::Reboot => {
                rx.await?
            }
        }
    }

    pub async fn issue_disk_snapshot_request(
        &self,
        propolis_id: PropolisUuid,
        disk_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(InstanceManagerRequest::IssueDiskSnapshot {
                propolis_id,
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
        propolis_id: PropolisUuid,
        ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(InstanceManagerRequest::AddExternalIp {
                propolis_id,
                ip: *ip,
                tx,
            })
            .await
            .map_err(|_| Error::FailedSendInstanceManagerClosed)?;
        rx.await?
    }

    pub async fn delete_external_ip(
        &self,
        propolis_id: PropolisUuid,
        ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(InstanceManagerRequest::DeleteExternalIp {
                propolis_id,
                ip: *ip,
                tx,
            })
            .await
            .map_err(|_| Error::FailedSendInstanceManagerClosed)?;
        rx.await?
    }

    pub async fn refresh_external_ips(&self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(InstanceManagerRequest::RefreshExternalIps { tx })
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
        propolis_id: PropolisUuid,
    ) -> Result<SledVmmState, Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(InstanceManagerRequest::GetState { propolis_id, tx })
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
        propolis_id: PropolisUuid,
        instance: InstanceEnsureBody,
        // These are boxed because they are, apparently, quite large, and Clippy
        // whinges about the overall size of this variant relative to the
        // others. Since we will generally send `EnsureRegistered` requests much
        // less frequently than most of the others, boxing this seems like a
        // reasonable choice...
        sled_identifiers: Box<SledIdentifiers>,
        tx: oneshot::Sender<Result<SledVmmState, Error>>,
    },
    EnsureUnregistered {
        propolis_id: PropolisUuid,
        tx: oneshot::Sender<Result<VmmUnregisterResponse, Error>>,
    },
    EnsureState {
        propolis_id: PropolisUuid,
        target: VmmStateRequested,
        tx: oneshot::Sender<Result<VmmPutStateResponse, Error>>,
    },

    IssueDiskSnapshot {
        propolis_id: PropolisUuid,
        disk_id: Uuid,
        snapshot_id: Uuid,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    CreateZoneBundle {
        name: String,
        tx: oneshot::Sender<Result<ZoneBundleMetadata, BundleError>>,
    },
    AddExternalIp {
        propolis_id: PropolisUuid,
        ip: InstanceExternalIpBody,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    DeleteExternalIp {
        propolis_id: PropolisUuid,
        ip: InstanceExternalIpBody,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    RefreshExternalIps {
        tx: oneshot::Sender<Result<(), Error>>,
    },
    GetState {
        propolis_id: PropolisUuid,
        tx: oneshot::Sender<Result<SledVmmState, Error>>,
    },
    OnlyUseDisks {
        disks: AllDisks,
        tx: oneshot::Sender<Result<(), Error>>,
    },
}

// Requests that the instance manager stop processing information about a
// particular instance.
struct InstanceDeregisterRequest {
    id: PropolisUuid,
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

    nexus_client: NexusClient,

    // TODO: If we held an object representing an enum of "Created OR Running"
    // instance, we could avoid the methods within "instance.rs" that panic
    // if the Propolis client hasn't been initialized.
    /// A mapping from a Propolis ID to the [Instance] that Propolis incarnates.
    jobs: BTreeMap<PropolisUuid, Instance>,

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
                            self.jobs.remove(&request.id);
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
                            propolis_id,
                            instance,
                            sled_identifiers,
                            tx,
                        }) => {
                            tx.send(self.ensure_registered(propolis_id, instance, *sled_identifiers).await).map_err(|_| Error::FailedSendClientClosed)
                        },
                        Some(EnsureUnregistered { propolis_id, tx }) => {
                            self.ensure_unregistered(tx, propolis_id).await
                        },
                        Some(EnsureState { propolis_id, target, tx }) => {
                            self.ensure_state(tx, propolis_id, target).await
                        },
                        Some(IssueDiskSnapshot { propolis_id, disk_id, snapshot_id, tx }) => {
                            self.issue_disk_snapshot_request(tx, propolis_id, disk_id, snapshot_id).await
                        },
                        Some(CreateZoneBundle { name, tx }) => {
                            self.create_zone_bundle(tx, &name).await.map_err(Error::from)
                        },
                        Some(AddExternalIp { propolis_id, ip, tx }) => {
                            self.add_external_ip(tx, propolis_id, &ip).await
                        },
                        Some(DeleteExternalIp { propolis_id, ip, tx }) => {
                            self.delete_external_ip(tx, propolis_id, &ip).await
                        },
                        Some(RefreshExternalIps { tx }) => {
                            self.refresh_external_ips(tx).await
                        }
                        Some(GetState { propolis_id, tx }) => {
                            // TODO(eliza): it could potentially be nice to
                            // refactor this to use `tokio::sync::watch`, rather
                            // than having to force `GetState` requests to
                            // serialize with the requests that actually update
                            // the state...
                            self.get_instance_state(tx, propolis_id).await
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

    fn get_propolis(&self, propolis_id: PropolisUuid) -> Option<&Instance> {
        self.jobs.get(&propolis_id)
    }

    /// Ensures that the instance manager contains a registered instance with
    /// the supplied Propolis ID and the instance spec provided in `instance`.
    ///
    /// # Arguments
    ///
    /// * `propolis_id`: The ID of the VMM to ensure exists.
    /// * `instance`: The initial hardware manifest, runtime state, and metadata
    ///   of the instance, to be used if the instance does not already exist.
    /// * `sled_identifiers`: This sled's [`SledIdentifiers`] --- you know, like
    ///   it says on the tin....
    ///
    /// # Return value
    ///
    /// `Ok` if the instance is registered with the supplied Propolis ID, `Err`
    /// otherwise. This routine is idempotent if called to register the same
    /// (instance ID, Propolis ID) pair multiple times, but will fail if the
    /// instance is registered with a Propolis ID different from the one the
    /// caller supplied.
    pub async fn ensure_registered(
        &mut self,
        propolis_id: PropolisUuid,
        instance: InstanceEnsureBody,
        sled_identifiers: SledIdentifiers,
    ) -> Result<SledVmmState, Error> {
        let InstanceEnsureBody {
            instance_id,
            migration_id,
            propolis_addr,
            hardware,
            vmm_runtime,
            metadata,
        } = instance;
        info!(
            &self.log,
            "ensuring instance is registered";
            "instance_id" => %instance_id,
            "propolis_id" => %propolis_id,
            "migration_id" => ?migration_id,
            "hardware" => ?hardware,
            "vmm_runtime" => ?vmm_runtime,
            "propolis_addr" => ?propolis_addr,
            "metadata" => ?metadata,
        );

        let instance = {
            if let Some(existing_instance) = self.jobs.get(&propolis_id) {
                if instance_id != existing_instance.id() {
                    info!(&self.log,
                          "Propolis ID already used by another instance";
                          "propolis_id" => %propolis_id,
                          "existing_instanceId" => %existing_instance.id());

                    return Err(Error::Instance(
                        crate::instance::Error::PropolisAlreadyRegistered(
                            propolis_id,
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
                      "instance_id" => %instance_id,
                      "propolis_id" => %propolis_id,
                    "migration_id" => ?migration_id);

                let instance_log = self.log.new(o!(
                    "instance_id" => instance_id.to_string(),
                    "propolis_id" => propolis_id.to_string(),
                ));

                let ticket =
                    InstanceTicket::new(propolis_id, self.terminate_tx.clone());

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
                    migration_id,
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
                let _old = self.jobs.insert(propolis_id, instance);
                assert!(_old.is_none());
                &self.jobs.get(&propolis_id).unwrap()
            }
        };

        Ok(instance.current_state().await?)
    }

    /// Idempotently ensures this VM is not registered with this instance
    /// manager. If this Propolis job is registered and has a running zone, the
    /// zone is rudely terminated.
    async fn ensure_unregistered(
        &mut self,
        tx: oneshot::Sender<Result<VmmUnregisterResponse, Error>>,
        propolis_id: PropolisUuid,
    ) -> Result<(), Error> {
        // If the instance does not exist, we response immediately.
        let Some(instance) = self.get_propolis(propolis_id) else {
            tx.send(Ok(VmmUnregisterResponse { updated_runtime: None }))
                .map_err(|_| Error::FailedSendClientClosed)?;
            return Ok(());
        };

        // Otherwise, we pipeline the request, and send it to the instance,
        // where it can receive an appropriate response.
        let mark_failed = false;
        instance.terminate(tx, mark_failed).await?;
        Ok(())
    }

    /// Idempotently attempts to drive the supplied Propolis into the supplied
    /// runtime state.
    async fn ensure_state(
        &mut self,
        tx: oneshot::Sender<Result<VmmPutStateResponse, Error>>,
        propolis_id: PropolisUuid,
        target: VmmStateRequested,
    ) -> Result<(), Error> {
        let Some(instance) = self.get_propolis(propolis_id) else {
            tx.send(Err(Error::NoSuchVmm(propolis_id)))
                .map_err(|_| Error::FailedSendClientClosed)?;
            return Ok(());
        };
        instance.put_state(tx, target).await?;
        Ok(())
    }

    async fn issue_disk_snapshot_request(
        &self,
        tx: oneshot::Sender<Result<(), Error>>,
        propolis_id: PropolisUuid,
        disk_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        let instance =
            self.jobs.get(&propolis_id).ok_or(Error::NoSuchVmm(propolis_id))?;

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
        // A well-formed Propolis zone name must consist of
        // `PROPOLIS_ZONE_PREFIX` and the Propolis ID. If the prefix is not
        // present or the Propolis ID portion of the supplied zone name isn't
        // parseable as a UUID, there is no Propolis zone with the specified
        // name to capture into a bundle, so return a `NoSuchZone` error.
        let vmm_id: PropolisUuid = name
            .strip_prefix(PROPOLIS_ZONE_PREFIX)
            .and_then(|uuid_str| uuid_str.parse::<PropolisUuid>().ok())
            .ok_or_else(|| BundleError::NoSuchZone {
                name: name.to_string(),
            })?;

        let Some(instance) = self.jobs.get(&vmm_id) else {
            return Err(BundleError::NoSuchZone { name: name.to_string() });
        };
        instance.request_zone_bundle(tx).await
    }

    async fn add_external_ip(
        &self,
        tx: oneshot::Sender<Result<(), Error>>,
        propolis_id: PropolisUuid,
        ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        let Some(instance) = self.get_propolis(propolis_id) else {
            return Err(Error::NoSuchVmm(propolis_id));
        };
        instance.add_external_ip(tx, ip).await?;
        Ok(())
    }

    async fn delete_external_ip(
        &self,
        tx: oneshot::Sender<Result<(), Error>>,
        propolis_id: PropolisUuid,
        ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        let Some(instance) = self.get_propolis(propolis_id) else {
            return Err(Error::NoSuchVmm(propolis_id));
        };

        instance.delete_external_ip(tx, ip).await?;
        Ok(())
    }

    async fn refresh_external_ips(
        &self,
        tx: oneshot::Sender<Result<(), Error>>,
    ) -> Result<(), Error> {
        let mut channels = vec![];
        for (_, instance) in &self.jobs {
            let (tx, rx_new) = oneshot::channel();
            instance.refresh_external_ips(tx).await?;
            channels.push(rx_new);
        }

        tokio::spawn(async move {
            for channel in channels {
                if let Err(e) = channel.await {
                    let _ = tx.send(Err(e.into()));
                    return;
                }
            }

            let _ = tx.send(Ok(()));
        });

        Ok(())
    }

    async fn get_instance_state(
        &self,
        tx: oneshot::Sender<Result<SledVmmState, Error>>,
        propolis_id: PropolisUuid,
    ) -> Result<(), Error> {
        let Some(instance) = self.get_propolis(propolis_id) else {
            return tx
                .send(Err(Error::NoSuchVmm(propolis_id)))
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
        for (id, instance) in self.jobs.iter() {
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
            if let Some(instance) = self.jobs.remove(&id) {
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
    id: PropolisUuid,
    terminate_tx: Option<mpsc::UnboundedSender<InstanceDeregisterRequest>>,
}

impl InstanceTicket {
    // Creates a new instance ticket for the Propolis job with the supplied `id`
    // to be removed from the manager on destruction.
    fn new(
        id: PropolisUuid,
        terminate_tx: mpsc::UnboundedSender<InstanceDeregisterRequest>,
    ) -> Self {
        InstanceTicket { id, terminate_tx: Some(terminate_tx) }
    }

    #[cfg(all(test, target_os = "illumos"))]
    pub(crate) fn new_without_manager_for_test(id: PropolisUuid) -> Self {
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
