// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for controlling multiple instances on a sled.

use crate::instance::propolis_zone_name;
use crate::instance::Instance;
use crate::nexus::NexusClientWithResolver;
use crate::params::InstanceExternalIpBody;
use crate::params::ZoneBundleMetadata;
use crate::params::{
    InstanceHardware, InstanceMigrationSourceParams, InstancePutStateResponse,
    InstanceStateRequested, InstanceUnregisterResponse,
};
use crate::zone_bundle::BundleError;
use crate::zone_bundle::ZoneBundler;

use anyhow::anyhow;
use illumos_utils::dladm::Etherstub;
use illumos_utils::link::VnicAllocator;
use illumos_utils::opte::PortManager;
use illumos_utils::running_zone::ZoneBuilderFactory;
use illumos_utils::vmm_reservoir;
use omicron_common::api::external::ByteCount;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use omicron_common::api::internal::nexus::SledInstanceState;
use omicron_common::api::internal::nexus::VmmRuntimeState;
use sled_storage::manager::StorageHandle;
use slog::Logger;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

// The depth of the request queue for the instance manager.
const QUEUE_SIZE: usize = 256;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Instance error: {0}")]
    Instance(#[from] crate::instance::Error),

    #[error("No such instance ID: {0}")]
    NoSuchInstance(Uuid),

    #[error("OPTE port management error: {0}")]
    Opte(#[from] illumos_utils::opte::Error),

    #[error("Failed to create reservoir: {0}")]
    Reservoir(#[from] vmm_reservoir::Error),

    #[error("Invalid reservoir configuration: {0}")]
    ReservoirConfig(String),

    #[error("Cannot find data link: {0}")]
    Underlay(#[from] sled_hardware::underlay::Error),

    #[error("Zone bundle error")]
    ZoneBundle(#[from] BundleError),

    #[error("Failed to send request to Instance Manager: Channel closed")]
    FailedSendChannelClosed,

    #[error("Instance Manager dropped our request")]
    RequestDropped(#[from] oneshot::error::RecvError),
}

pub enum ReservoirMode {
    None,
    Size(u32),
    Percentage(u8),
}

pub(crate) struct InstanceManagerServices {
    pub nexus_client: NexusClientWithResolver,
    pub vnic_allocator: VnicAllocator<Etherstub>,
    pub port_manager: PortManager,
    pub storage: StorageHandle,
    pub zone_bundler: ZoneBundler,
    pub zone_builder_factory: ZoneBuilderFactory,
}

// Describes the internals of the "InstanceManager", though most of the
// instance manager's state exists within the "InstanceManagerRunner" structure.
struct InstanceManagerInternal {
    log: Logger,
    tx: mpsc::Sender<InstanceManagerRequest>,
    // NOTE: Arguably, this field could be "owned" by the InstanceManagerRunner.
    // It was not moved there, and the reservoir functions were not converted to
    // use the message-passing interface (see: "InstanceManagerRequest") because
    // callers of "get/set reservoir size" are not async, and (in the case of
    // getting the size) they also do not expect a "Result" type.
    reservoir_size: Mutex<ByteCount>,

    #[allow(dead_code)]
    runner_handle: tokio::task::JoinHandle<()>,
}

/// All instances currently running on the sled.
pub struct InstanceManager {
    inner: Arc<InstanceManagerInternal>,
}

impl InstanceManager {
    /// Initializes a new [`InstanceManager`] object.
    pub fn new(
        log: Logger,
        nexus_client: NexusClientWithResolver,
        etherstub: Etherstub,
        port_manager: PortManager,
        storage: StorageHandle,
        zone_bundler: ZoneBundler,
        zone_builder_factory: ZoneBuilderFactory,
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
            storage,
            zone_bundler,
            zone_builder_factory,
        };

        let runner_handle =
            tokio::task::spawn(async move { runner.run().await });

        Ok(Self {
            inner: Arc::new(InstanceManagerInternal {
                log,
                tx,
                // no reservoir size set on startup
                reservoir_size: Mutex::new(ByteCount::from_kibibytes_u32(0)),
                runner_handle,
            }),
        })
    }

    /// Sets the VMM reservoir to the requested percentage of usable physical
    /// RAM or to a size in MiB. Either mode will round down to the nearest
    /// aligned size required by the control plane.
    pub fn set_reservoir_size(
        &self,
        hardware: &sled_hardware::HardwareManager,
        mode: ReservoirMode,
    ) -> Result<(), Error> {
        let hardware_physical_ram_bytes = hardware.usable_physical_ram_bytes();
        let req_bytes = match mode {
            ReservoirMode::None => return Ok(()),
            ReservoirMode::Size(mb) => {
                let bytes = ByteCount::from_mebibytes_u32(mb).to_bytes();
                if bytes > hardware_physical_ram_bytes {
                    return Err(Error::ReservoirConfig(format!(
                        "cannot specify a reservoir of {bytes} bytes when \
                        physical memory is {hardware_physical_ram_bytes} bytes",
                    )));
                }
                bytes
            }
            ReservoirMode::Percentage(percent) => {
                if !matches!(percent, 1..=99) {
                    return Err(Error::ReservoirConfig(format!(
                        "VMM reservoir percentage of {} must be between 0 and \
                        100",
                        percent
                    )));
                };
                (hardware_physical_ram_bytes as f64 * (percent as f64 / 100.0))
                    .floor() as u64
            }
        };

        let req_bytes_aligned = vmm_reservoir::align_reservoir_size(req_bytes);

        if req_bytes_aligned == 0 {
            warn!(
                self.inner.log,
                "Requested reservoir size of {} bytes < minimum aligned size \
                of {} bytes",
                req_bytes,
                vmm_reservoir::RESERVOIR_SZ_ALIGN
            );
            return Ok(());
        }

        // The max ByteCount value is i64::MAX, which is ~8 million TiB.
        // As this value is either a percentage of DRAM or a size in MiB
        // represented as a u32, constructing this should always work.
        let reservoir_size = ByteCount::try_from(req_bytes_aligned).unwrap();
        if let ReservoirMode::Percentage(percent) = mode {
            info!(
                self.inner.log,
                "{}% of {} physical ram = {} bytes)",
                percent,
                hardware_physical_ram_bytes,
                req_bytes,
            );
        }
        info!(
            self.inner.log,
            "Setting reservoir size to {reservoir_size} bytes"
        );
        vmm_reservoir::ReservoirControl::set(reservoir_size)?;

        *self.inner.reservoir_size.lock().unwrap() = reservoir_size;

        Ok(())
    }

    /// Returns the last-set size of the reservoir
    pub fn reservoir_size(&self) -> ByteCount {
        *self.inner.reservoir_size.lock().unwrap()
    }

    pub async fn ensure_registered(
        &self,
        instance_id: Uuid,
        propolis_id: Uuid,
        hardware: InstanceHardware,
        instance_runtime: InstanceRuntimeState,
        vmm_runtime: VmmRuntimeState,
        propolis_addr: SocketAddr,
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
                tx,
            })
            .await
            .map_err(|_| Error::FailedSendChannelClosed)?;
        rx.await?
    }

    pub async fn ensure_unregistered(
        &self,
        instance_id: Uuid,
    ) -> Result<InstanceUnregisterResponse, Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(InstanceManagerRequest::EnsureUnregistered {
                instance_id,
                tx,
            })
            .await
            .map_err(|_| Error::FailedSendChannelClosed)?;
        rx.await?
    }

    pub async fn ensure_state(
        &self,
        instance_id: Uuid,
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
            .map_err(|_| Error::FailedSendChannelClosed)?;
        rx.await?
    }

    pub async fn put_migration_ids(
        &self,
        instance_id: Uuid,
        old_runtime: &InstanceRuntimeState,
        migration_ids: &Option<InstanceMigrationSourceParams>,
    ) -> Result<SledInstanceState, Error> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .tx
            .send(InstanceManagerRequest::PutMigrationIds {
                instance_id,
                old_runtime: old_runtime.clone(),
                migration_ids: *migration_ids,
                tx,
            })
            .await
            .map_err(|_| Error::FailedSendChannelClosed)?;
        rx.await?
    }

    pub async fn instance_issue_disk_snapshot_request(
        &self,
        instance_id: Uuid,
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
            .map_err(|_| Error::FailedSendChannelClosed)?;
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
        instance_id: Uuid,
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
            .map_err(|_| Error::FailedSendChannelClosed)?;
        rx.await?
    }

    pub async fn delete_external_ip(
        &self,
        instance_id: Uuid,
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
            .map_err(|_| Error::FailedSendChannelClosed)?;
        rx.await?
    }
}

// Most requests that can be sent to the "InstanceManagerRunner" task.
//
// These messages are sent by "InstanceManager"'s interface, and processed by
// the runner task.
//
// By convention, responses are sent on the "tx" oneshot.
enum InstanceManagerRequest {
    EnsureRegistered {
        instance_id: Uuid,
        propolis_id: Uuid,
        hardware: InstanceHardware,
        instance_runtime: InstanceRuntimeState,
        vmm_runtime: VmmRuntimeState,
        propolis_addr: SocketAddr,
        tx: oneshot::Sender<Result<SledInstanceState, Error>>,
    },
    EnsureUnregistered {
        instance_id: Uuid,
        tx: oneshot::Sender<Result<InstanceUnregisterResponse, Error>>,
    },
    EnsureState {
        instance_id: Uuid,
        target: InstanceStateRequested,
        tx: oneshot::Sender<Result<InstancePutStateResponse, Error>>,
    },
    PutMigrationIds {
        instance_id: Uuid,
        old_runtime: InstanceRuntimeState,
        migration_ids: Option<InstanceMigrationSourceParams>,
        tx: oneshot::Sender<Result<SledInstanceState, Error>>,
    },
    InstanceIssueDiskSnapshot {
        instance_id: Uuid,
        disk_id: Uuid,
        snapshot_id: Uuid,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    CreateZoneBundle {
        name: String,
        tx: oneshot::Sender<Result<ZoneBundleMetadata, BundleError>>,
    },
    InstanceAddExternalIp {
        instance_id: Uuid,
        ip: InstanceExternalIpBody,
        tx: oneshot::Sender<Result<(), Error>>,
    },
    InstanceDeleteExternalIp {
        instance_id: Uuid,
        ip: InstanceExternalIpBody,
        tx: oneshot::Sender<Result<(), Error>>,
    },
}

// Requests that the instance manager stop processing information about a
// particular instance.
struct InstanceDeregisterRequest {
    id: Uuid,
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
    instances: BTreeMap<Uuid, (Uuid, Instance)>,

    vnic_allocator: VnicAllocator<Etherstub>,
    port_manager: PortManager,
    storage: StorageHandle,
    zone_bundler: ZoneBundler,
    zone_builder_factory: ZoneBuilderFactory,
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
                    match request {
                        Some(EnsureRegistered {
                            instance_id,
                            propolis_id,
                            hardware,
                            instance_runtime,
                            vmm_runtime,
                            propolis_addr,
                            tx,
                        }) => {
                            let _ = tx.send(self.ensure_registered(instance_id, propolis_id, hardware, instance_runtime, vmm_runtime, propolis_addr).await);
                        },
                        Some(EnsureUnregistered { instance_id, tx }) => {
                            let _ = self.ensure_unregistered(tx, instance_id).await;
                        },
                        Some(EnsureState { instance_id, target, tx }) => {
                            let _ = self.ensure_state(tx, instance_id, target).await;
                        },
                        Some(PutMigrationIds{ instance_id, old_runtime, migration_ids, tx }) => {
                            let _ = self.put_migration_ids(tx, instance_id, &old_runtime, &migration_ids).await;
                        },
                        Some(InstanceIssueDiskSnapshot{ instance_id, disk_id, snapshot_id, tx }) => {
                            let _ = self.instance_issue_disk_snapshot_request(tx, instance_id, disk_id, snapshot_id).await;
                        },
                        Some(CreateZoneBundle{ name, tx }) => {
                            let _ = self.create_zone_bundle(tx, &name).await.map_err(Error::from);
                        },
                        Some(InstanceAddExternalIp{ instance_id, ip, tx }) => {
                            let _ = self.add_external_ip(tx, instance_id, &ip).await;
                        },
                        Some(InstanceDeleteExternalIp{ instance_id, ip, tx }) => {
                            let _ = self.delete_external_ip(tx, instance_id, &ip).await;
                        },
                        None => {
                            warn!(self.log, "InstanceManager's request channel closed; shutting down");
                            break;
                        },
                    }
                }
            }
        }
    }

    fn get_instance(&self, instance_id: Uuid) -> Option<&Instance> {
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
    async fn ensure_registered(
        &mut self,
        instance_id: Uuid,
        propolis_id: Uuid,
        hardware: InstanceHardware,
        instance_runtime: InstanceRuntimeState,
        vmm_runtime: VmmRuntimeState,
        propolis_addr: SocketAddr,
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
                let instance_log = self.log.new(o!());
                let ticket =
                    InstanceTicket::new(instance_id, self.terminate_tx.clone());

                let services = InstanceManagerServices {
                    nexus_client: self.nexus_client.clone(),
                    vnic_allocator: self.vnic_allocator.clone(),
                    port_manager: self.port_manager.clone(),
                    storage: self.storage.clone(),
                    zone_bundler: self.zone_bundler.clone(),
                    zone_builder_factory: self.zone_builder_factory.clone(),
                };

                let state = crate::instance::InstanceInitialState {
                    hardware,
                    instance_runtime,
                    vmm_runtime,
                    propolis_addr,
                };

                let instance = Instance::new(
                    instance_log,
                    instance_id,
                    propolis_id,
                    ticket,
                    state,
                    services,
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
        instance_id: Uuid,
    ) -> Result<(), Error> {
        // If the instance does not exist, we response immediately.
        let Some(instance) = self.get_instance(instance_id) else {
            tx.send(Ok(InstanceUnregisterResponse { updated_runtime: None }))
                .map_err(|_| Error::FailedSendChannelClosed)?;
            return Ok(());
        };

        // Otherwise, we pipeline the request, and send it to the instance,
        // where it can receive an appropriate response.
        instance.terminate(tx).await?;
        Ok(())
    }

    /// Idempotently attempts to drive the supplied instance into the supplied
    /// runtime state.
    async fn ensure_state(
        &mut self,
        tx: oneshot::Sender<Result<InstancePutStateResponse, Error>>,
        instance_id: Uuid,
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
                    .map_err(|_| Error::FailedSendChannelClosed)?;
                }
                _ => {
                    tx.send(Err(Error::NoSuchInstance(instance_id)))
                        .map_err(|_| Error::FailedSendChannelClosed)?;
                }
            }
            return Ok(());
        };
        instance.put_state(tx, target).await?;
        Ok(())
    }

    /// Idempotently attempts to set the instance's migration IDs to the
    /// supplied IDs.
    async fn put_migration_ids(
        &mut self,
        tx: oneshot::Sender<Result<SledInstanceState, Error>>,
        instance_id: Uuid,
        old_runtime: &InstanceRuntimeState,
        migration_ids: &Option<InstanceMigrationSourceParams>,
    ) -> Result<(), Error> {
        let (_, instance) = self
            .instances
            .get(&instance_id)
            .ok_or_else(|| Error::NoSuchInstance(instance_id))?;
        instance
            .put_migration_ids(tx, old_runtime.clone(), *migration_ids)
            .await?;
        Ok(())
    }

    async fn instance_issue_disk_snapshot_request(
        &self,
        tx: oneshot::Sender<Result<(), Error>>,
        instance_id: Uuid,
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
        instance_id: Uuid,
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
        instance_id: Uuid,
        ip: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        let Some(instance) = self.get_instance(instance_id) else {
            return Err(Error::NoSuchInstance(instance_id));
        };

        instance.delete_external_ip(tx, ip).await?;
        Ok(())
    }
}

/// Represents membership of an instance in the [`InstanceManager`].
pub struct InstanceTicket {
    id: Uuid,
    terminate_tx: Option<mpsc::UnboundedSender<InstanceDeregisterRequest>>,
}

impl InstanceTicket {
    // Creates a new instance ticket for instance "id" to be removed
    // from the manger on destruction.
    fn new(
        id: Uuid,
        terminate_tx: mpsc::UnboundedSender<InstanceDeregisterRequest>,
    ) -> Self {
        InstanceTicket { id, terminate_tx: Some(terminate_tx) }
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
