// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for controlling multiple instances on a sled.

use crate::instance::propolis_zone_name;
use crate::instance::Instance;
use crate::nexus::NexusClientWithResolver;
use crate::params::ZoneBundleMetadata;
use crate::params::{
    InstanceHardware, InstanceMigrationSourceParams, InstancePutStateResponse,
    InstanceStateRequested, InstanceUnregisterResponse,
};
use crate::storage_manager::StorageResources;
use crate::zone_bundle::BundleError;
use crate::zone_bundle::ZoneBundler;
use illumos_utils::dladm::Etherstub;
use illumos_utils::link::VnicAllocator;
use illumos_utils::opte::PortManager;
use illumos_utils::vmm_reservoir;
use omicron_common::api::external::ByteCount;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use omicron_common::api::internal::nexus::SledInstanceState;
use omicron_common::api::internal::nexus::VmmRuntimeState;
use slog::Logger;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

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

    #[error("Cannot find data link: {0}")]
    Underlay(#[from] sled_hardware::underlay::Error),

    #[error("Zone bundle error")]
    ZoneBundle(#[from] BundleError),
}

struct InstanceManagerInternal {
    log: Logger,
    nexus_client: NexusClientWithResolver,

    /// Last set size of the VMM reservoir (in bytes)
    reservoir_size: Mutex<ByteCount>,

    // TODO: If we held an object representing an enum of "Created OR Running"
    // instance, we could avoid the methods within "instance.rs" that panic
    // if the Propolis client hasn't been initialized.
    /// A mapping from a Sled Agent "Instance ID" to ("Propolis ID", [Instance]).
    instances: Mutex<BTreeMap<Uuid, (Uuid, Instance)>>,

    vnic_allocator: VnicAllocator<Etherstub>,
    port_manager: PortManager,
    storage: StorageResources,
    zone_bundler: ZoneBundler,
}

pub(crate) struct InstanceManagerServices {
    pub nexus_client: NexusClientWithResolver,
    pub vnic_allocator: VnicAllocator<Etherstub>,
    pub port_manager: PortManager,
    pub storage: StorageResources,
    pub zone_bundler: ZoneBundler,
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
        storage: StorageResources,
        zone_bundler: ZoneBundler,
    ) -> Result<InstanceManager, Error> {
        Ok(InstanceManager {
            inner: Arc::new(InstanceManagerInternal {
                log: log.new(o!("component" => "InstanceManager")),
                nexus_client,

                // no reservoir size set on startup
                reservoir_size: Mutex::new(ByteCount::from_kibibytes_u32(0)),
                instances: Mutex::new(BTreeMap::new()),
                vnic_allocator: VnicAllocator::new("Instance", etherstub),
                port_manager,
                storage,
                zone_bundler,
            }),
        })
    }

    /// Sets the VMM reservoir size to the requested (nonzero) percentage of
    /// usable physical RAM, rounded down to nearest aligned size required by
    /// the control plane.
    pub fn set_reservoir_size(
        &self,
        hardware: &sled_hardware::HardwareManager,
        target_percent: u8,
    ) -> Result<(), Error> {
        assert!(
            target_percent > 0 && target_percent < 100,
            "target_percent {} must be nonzero and < 100",
            target_percent
        );

        let req_bytes = (hardware.usable_physical_ram_bytes() as f64
            * (target_percent as f64 / 100.0))
            .floor() as u64;
        let req_bytes_aligned = vmm_reservoir::align_reservoir_size(req_bytes);

        if req_bytes_aligned == 0 {
            warn!(
                self.inner.log,
                "Requested reservoir size of {} bytes < minimum aligned size of {} bytes",
                req_bytes, vmm_reservoir::RESERVOIR_SZ_ALIGN);
            return Ok(());
        }

        // The max ByteCount value is i64::MAX, which is ~8 million TiB. As this
        // value is a percentage of DRAM, constructing this should always work.
        let reservoir_size = ByteCount::try_from(req_bytes_aligned).unwrap();
        info!(
            self.inner.log,
            "Setting reservoir size to {} bytes \
            ({}% of {} total = {} bytes requested)",
            reservoir_size,
            target_percent,
            hardware.usable_physical_ram_bytes(),
            req_bytes,
        );
        vmm_reservoir::ReservoirControl::set(reservoir_size)?;

        *self.inner.reservoir_size.lock().unwrap() = reservoir_size;

        Ok(())
    }

    /// Returns the last-set size of the reservoir
    pub fn reservoir_size(&self) -> ByteCount {
        *self.inner.reservoir_size.lock().unwrap()
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
    pub async fn ensure_registered(
        &self,
        instance_id: Uuid,
        propolis_id: Uuid,
        hardware: InstanceHardware,
        instance_runtime: InstanceRuntimeState,
        vmm_runtime: VmmRuntimeState,
        propolis_addr: SocketAddr,
    ) -> Result<SledInstanceState, Error> {
        info!(
            &self.inner.log,
            "ensuring instance is registered";
            "instance_id" => %instance_id,
            "propolis_id" => %propolis_id,
            "hardware" => ?hardware,
            "instance_runtime" => ?instance_runtime,
            "vmm_runtime" => ?vmm_runtime,
            "propolis_addr" => ?propolis_addr,
        );

        let instance = {
            let mut instances = self.inner.instances.lock().unwrap();
            if let Some((existing_propolis_id, existing_instance)) =
                instances.get(&instance_id)
            {
                if propolis_id != *existing_propolis_id {
                    info!(&self.inner.log,
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
                        &self.inner.log,
                        "instance already registered with requested Propolis ID"
                    );
                    existing_instance.clone()
                }
            } else {
                info!(&self.inner.log,
                      "registering new instance";
                      "instance_id" => ?instance_id);
                let instance_log = self.inner.log.new(o!());
                let ticket =
                    InstanceTicket::new(instance_id, self.inner.clone());

                let services = InstanceManagerServices {
                    nexus_client: self.inner.nexus_client.clone(),
                    vnic_allocator: self.inner.vnic_allocator.clone(),
                    port_manager: self.inner.port_manager.clone(),
                    storage: self.inner.storage.clone(),
                    zone_bundler: self.inner.zone_bundler.clone(),
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
                let instance_clone = instance.clone();
                let _old =
                    instances.insert(instance_id, (propolis_id, instance));
                assert!(_old.is_none());
                instance_clone
            }
        };

        Ok(instance.current_state().await)
    }

    /// Idempotently ensures the instance is not registered with this instance
    /// manager. If the instance exists and has a running Propolis, that
    /// Propolis is rudely terminated.
    pub async fn ensure_unregistered(
        &self,
        instance_id: Uuid,
    ) -> Result<InstanceUnregisterResponse, Error> {
        let instance = {
            let instances = self.inner.instances.lock().unwrap();
            let instance = instances.get(&instance_id);
            if let Some((_, instance)) = instance {
                instance.clone()
            } else {
                return Ok(InstanceUnregisterResponse {
                    updated_runtime: None,
                });
            }
        };

        Ok(InstanceUnregisterResponse {
            updated_runtime: Some(instance.terminate().await?),
        })
    }

    /// Idempotently attempts to drive the supplied instance into the supplied
    /// runtime state.
    pub async fn ensure_state(
        &self,
        instance_id: Uuid,
        target: InstanceStateRequested,
    ) -> Result<InstancePutStateResponse, Error> {
        let instance = {
            let instances = self.inner.instances.lock().unwrap();
            let instance = instances.get(&instance_id);

            if let Some((_, instance)) = instance {
                instance.clone()
            } else {
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
                        return Ok(InstancePutStateResponse {
                            updated_runtime: None,
                        });
                    }
                    _ => {
                        return Err(Error::NoSuchInstance(instance_id));
                    }
                }
            }
        };

        let new_state = instance.put_state(target).await?;
        Ok(InstancePutStateResponse { updated_runtime: Some(new_state) })
    }

    /// Idempotently attempts to set the instance's migration IDs to the
    /// supplied IDs.
    pub async fn put_migration_ids(
        &self,
        instance_id: Uuid,
        old_runtime: &InstanceRuntimeState,
        migration_ids: &Option<InstanceMigrationSourceParams>,
    ) -> Result<SledInstanceState, Error> {
        let (_, instance) = self
            .inner
            .instances
            .lock()
            .unwrap()
            .get(&instance_id)
            .ok_or_else(|| Error::NoSuchInstance(instance_id))?
            .clone();

        Ok(instance.put_migration_ids(old_runtime, migration_ids).await?)
    }

    pub async fn instance_issue_disk_snapshot_request(
        &self,
        instance_id: Uuid,
        disk_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        let instance = {
            let instances = self.inner.instances.lock().unwrap();
            let (_, instance) = instances
                .get(&instance_id)
                .ok_or(Error::NoSuchInstance(instance_id))?;
            instance.clone()
        };

        instance
            .issue_snapshot_request(disk_id, snapshot_id)
            .await
            .map_err(Error::from)
    }

    /// Create a zone bundle from a named instance zone, if it exists.
    pub async fn create_zone_bundle(
        &self,
        name: &str,
    ) -> Result<ZoneBundleMetadata, BundleError> {
        // We need to find the instance and take its lock, but:
        //
        // 1. The instance-map lock is sync, and
        // 2. we don't want to hold the instance-map lock for the entire
        //    bundling duration.
        //
        // Instead, we cheaply clone the instance through its `Arc` around the
        // `InstanceInner`, which is ultimately what we want.
        let Some((_propolis_id, instance)) = self
            .inner
            .instances
            .lock()
            .unwrap()
            .values()
            .find(|(propolis_id, _instance)| {
                name == propolis_zone_name(propolis_id)
            })
            .cloned()
        else {
            return Err(BundleError::NoSuchZone { name: name.to_string() });
        };
        instance.request_zone_bundle().await
    }
}

/// Represents membership of an instance in the [`InstanceManager`].
pub struct InstanceTicket {
    id: Uuid,
    inner: Option<Arc<InstanceManagerInternal>>,
}

impl InstanceTicket {
    // Creates a new instance ticket for instance "id" to be removed
    // from "inner" on destruction.
    fn new(id: Uuid, inner: Arc<InstanceManagerInternal>) -> Self {
        InstanceTicket { id, inner: Some(inner) }
    }

    /// Idempotently removes this instance from the tracked set of
    /// instances. This acts as an "upcall" for instances to remove
    /// themselves after stopping.
    pub fn terminate(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.instances.lock().unwrap().remove(&self.id);
        }
    }
}

impl Drop for InstanceTicket {
    fn drop(&mut self) {
        self.terminate();
    }
}
