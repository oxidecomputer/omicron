//! API for controlling multiple instances on a sled.

use omicron_common::error::ApiError;
use omicron_common::model::{
    ApiInstanceRuntimeState, ApiInstanceRuntimeStateRequested,
};
use omicron_common::NexusClient;
use slog::Logger;
use std::collections::BTreeMap;
use std::sync::{atomic::{AtomicU64, Ordering}, Arc, Mutex};
use uuid::Uuid;

use crate::instance::Instance;
use crate::zone::{create_base_zone, ensure_zpool_exists, ZONE_ZFS_POOL};

struct InstanceManagerInternal {
    log: Logger,
    nexus_client: Arc<NexusClient>,

    // TODO: Could hold enum of "Created/Running instance"
    // would remove the "warning: might panic" documentation of the current API
    instances: Mutex<BTreeMap<Uuid, Instance>>,

    next_id: AtomicU64,
}

/// All instances currently running on the sled.
pub struct InstanceManager {
    inner: Arc<InstanceManagerInternal>,
}

impl InstanceManager {
    /// Initializes a new [`InstanceManager`] object.
    pub fn new(
        log: Logger,
        nexus_client: Arc<NexusClient>,
    ) -> Result<InstanceManager, ApiError> {
        // Before we start creating instances, we need to ensure that the
        // necessary ZFS and Zone resources are ready.
        ensure_zpool_exists(ZONE_ZFS_POOL)?;

        // Create a base zone, from which all running instance zones are cloned.
        create_base_zone(&log)?;

        // Identify all existing zones which should be managed by the Sled
        // Agent.
        //
        // NOTE: Currently, we're removing these zones. In the future, we should
        // re-establish contact (i.e., if the Sled Agent crashed, but we wanted
        // to leave the running Zones intact).
        let zones = crate::zone::get_zones()?;
        for z in zones {
            warn!(log, "Deleting zone: {}", z.name());
            zone::Adm::new(z.name()).halt().unwrap();
            zone::Adm::new(z.name()).uninstall(true).unwrap();
            zone::Config::new(z.name()).delete(true).run().unwrap();
        }

        // Identify all VNICs which should be managed by the Sled Agent.
        //
        // NOTE: Currently, we're removing these VNICs. In the future, we should
        // identify if they're being used by the aforementioned existing zones,
        // and track them once more.
        //
        // (dladm show-vnic -p -o ZONE,LINK) might help
        let vnics = crate::zone::get_vnics()?;
        for vnic in vnics {
            warn!(log, "Deleting VNIC: {}", vnic);
            crate::zone::delete_vnic(&vnic)?;
        }

        Ok(InstanceManager {
            inner: Arc::new(InstanceManagerInternal {
                log,
                nexus_client,
                instances: Mutex::new(BTreeMap::new()),
                next_id: AtomicU64::new(1),
            }),
        })
    }

    /// Idempotently ensures that the given Instance (described by
    /// `initial_runtime`) exists on this server in the given runtime state
    /// (described by `target`).
    pub async fn ensure(
        &self,
        instance_id: Uuid,
        initial_runtime: ApiInstanceRuntimeState,
        target: ApiInstanceRuntimeStateRequested,
    ) -> Result<ApiInstanceRuntimeState, ApiError> {
        info!(
            &self.inner.log,
            "instance_ensure {} -> {:?}", instance_id, target
        );

        let (instance, maybe_instance_ticket) = {
            let mut instances = self.inner.instances.lock().unwrap();
            if let Some(instance) = instances.get_mut(&instance_id) {
                // Instance already exists.
                info!(&self.inner.log, "instance already exists");
                (instance.clone(), None)
            } else {
                // Instance does not exist - create it.
                info!(&self.inner.log, "new instance");
                let instance_log = self
                    .inner
                    .log
                    .new(o!("instance" => instance_id.to_string()));
                instances.insert(
                    instance_id,
                    Instance::new(
                        instance_log,
                        instance_id,
                        self.inner.next_id.fetch_add(1, Ordering::SeqCst),
                        initial_runtime,
                        self.inner.nexus_client.clone(),
                    )?,
                );
                let instance = instances.get_mut(&instance_id).unwrap().clone();
                let ticket =
                    Some(InstanceTicket::new(instance_id, self.inner.clone()));
                (instance, ticket)
            }
        };

        // If we created a new instance, start it - but do so outside
        // the "instances" lock, since initialization may take a while.
        //
        // Additionally, this makes it possible to manage the "instance_ticket",
        // which might need to grab the lock to remove the instance during
        // teardown.
        if let Some(instance_ticket) = maybe_instance_ticket {
            instance.start(instance_ticket).await?;
        }

        instance.transition(target).await
    }
}

/// Represents membership of an instance in the [`InstanceManager`].
pub struct InstanceTicket {
    id: Uuid,
    inner: Arc<InstanceManagerInternal>,
    removed: bool,
}

impl InstanceTicket {
    fn new(id: Uuid, inner: Arc<InstanceManagerInternal>) -> Self {
        InstanceTicket { id, inner, removed: false }
    }

    /// Idempotently removes this instance from the tracked set of
    /// instances. This acts as an "upcall" for instances to remove
    /// themselves after stopping.
    pub fn terminate(&mut self) {
        if !self.removed {
            self.inner.instances.lock().unwrap().remove(&self.id);
            self.removed = true;
        }
    }
}

impl Drop for InstanceTicket {
    fn drop(&mut self) {
        self.terminate();
    }
}
