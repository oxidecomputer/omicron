//! API for controlling multiple instances on a sled.

use crate::illumos::zfs::ZONE_ZFS_DATASET;
use omicron_common::api::ApiError;
use omicron_common::api::{
    ApiInstanceRuntimeState, ApiInstanceRuntimeStateRequested,
};
use slog::Logger;
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use uuid::Uuid;

#[cfg(test)]
use crate::mocks::MockNexusClient as NexusClient;
#[cfg(not(test))]
use omicron_common::NexusClient;

#[cfg(not(test))]
use crate::{
    illumos::{dladm::Dladm, zfs::Zfs, zone::Zones},
    instance::Instance,
};
#[cfg(test)]
use crate::{
    illumos::{
        dladm::MockDladm as Dladm, zfs::MockZfs as Zfs,
        zone::MockZones as Zones,
    },
    instance::MockInstance as Instance,
};

struct InstanceManagerInternal {
    log: Logger,
    nexus_client: Arc<NexusClient>,

    // TODO: If we held an object representing an enum of "Created OR Running"
    // instance, we could avoid the methods within "instance.rs" that panic
    // if the Propolis client hasn't been initialized.
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
        Zfs::ensure_dataset(ZONE_ZFS_DATASET)?;

        // Create a base zone, from which all running instance zones are cloned.
        Zones::create_base(&log)?;

        // Identify all existing zones which should be managed by the Sled
        // Agent.
        //
        // NOTE: Currently, we're removing these zones. In the future, we should
        // re-establish contact (i.e., if the Sled Agent crashed, but we wanted
        // to leave the running Zones intact).
        let zones = Zones::get()?;
        for z in zones {
            warn!(log, "Deleting zone: {}", z.name());
            Zones::halt_and_remove(&log, z.name())?;
        }

        // Identify all VNICs which should be managed by the Sled Agent.
        //
        // NOTE: Currently, we're removing these VNICs. In the future, we should
        // identify if they're being used by the aforementioned existing zones,
        // and track them once more.
        //
        // (dladm show-vnic -p -o ZONE,LINK) might help
        let vnics = Dladm::get_vnics()?;
        for vnic in vnics {
            warn!(log, "Deleting VNIC: {}", vnic);
            Dladm::delete_vnic(&vnic)?;
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
    inner: Option<Arc<InstanceManagerInternal>>,
}

impl InstanceTicket {
    // Creates a new instance ticket for instance "id" to be removed
    // from "inner" on destruction.
    fn new(id: Uuid, inner: Arc<InstanceManagerInternal>) -> Self {
        InstanceTicket { id, inner: Some(inner) }
    }

    // (Test-only) Creates a null ticket that does nothing.
    //
    // Useful when testing instances without the an entire instance manager.
    #[cfg(test)]
    pub(crate) fn null(id: Uuid) -> Self {
        InstanceTicket { id, inner: None }
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::illumos::{dladm::MockDladm, zfs::MockZfs, zone::MockZones};
    use crate::instance::MockInstance;
    use crate::mocks::MockNexusClient;
    use chrono::Utc;
    use omicron_common::api::{
        ApiGeneration, ApiInstanceRuntimeState, ApiInstanceState,
        ApiInstanceStateRequested,
    };

    static INST_UUID_STR: &str = "e398c5d5-5059-4e55-beac-3a1071083aaa";

    fn test_uuid() -> Uuid {
        INST_UUID_STR.parse().unwrap()
    }

    fn logger() -> Logger {
        dropshot::ConfigLogging::StderrTerminal {
            level: dropshot::ConfigLoggingLevel::Info,
        }
        .to_logger("test-logger")
        .unwrap()
    }

    fn new_runtime_state() -> ApiInstanceRuntimeState {
        ApiInstanceRuntimeState {
            run_state: ApiInstanceState::Creating,
            sled_uuid: Uuid::new_v4(),
            gen: ApiGeneration::new(),
            time_updated: Utc::now(),
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn ensure_instance() {
        let log = logger();
        let nexus_client = Arc::new(MockNexusClient::default());

        // Creation of the instance manager incurs some "global" system
        // checks - creation of the base zone, and cleanup of existing
        // zones + vnics.

        let zfs_ensure_dataset_ctx = MockZfs::ensure_dataset_context();
        zfs_ensure_dataset_ctx.expect().return_once(|pool| {
            assert_eq!(pool, ZONE_ZFS_DATASET);
            Ok(())
        });

        let zones_create_base_ctx = MockZones::create_base_context();
        zones_create_base_ctx.expect().return_once(|_| Ok(()));

        let zones_get_ctx = MockZones::get_context();
        zones_get_ctx.expect().return_once(|| Ok(vec![]));

        let dladm_get_vnics_ctx = MockDladm::get_vnics_context();
        dladm_get_vnics_ctx.expect().return_once(|| Ok(vec![]));

        let im = InstanceManager::new(log, nexus_client).unwrap();

        // Verify that no instances exist.
        assert!(im.inner.instances.lock().unwrap().is_empty());

        // Insert a new instance, verify that it exists.
        //
        // In the process, we'll clone the instance reference out
        // of the manager, "start" and "transition" it to the desired state.
        //
        // Note that we need to perform some manual intervention to hold onto
        // the "InstanceTicket". Normally, the "Instance" object would drop
        // the ticket at the end of the instance lifetime to imply tracking
        // should stop.
        let ticket = Arc::new(std::sync::Mutex::new(None));
        let ticket_clone = ticket.clone();
        let instance_new_ctx = MockInstance::new_context();
        instance_new_ctx.expect().return_once(move |_, _, _, _, _| {
            let mut inst = MockInstance::default();
            inst.expect_clone().return_once(move || {
                let mut inst = MockInstance::default();
                inst.expect_start().return_once(move |t| {
                    // Grab hold of the ticket, so we don't try to remove the
                    // instance immediately after "start" completes.
                    let mut ticket_guard = ticket_clone.lock().unwrap();
                    *ticket_guard = Some(t);
                    Ok(())
                });
                inst.expect_transition().return_once(|_| {
                    let mut rt_state = new_runtime_state();
                    rt_state.run_state = ApiInstanceState::Running;
                    Ok(rt_state)
                });
                inst
            });
            Ok(inst)
        });
        let rt_state = im
            .ensure(
                test_uuid(),
                new_runtime_state(),
                ApiInstanceRuntimeStateRequested {
                    run_state: ApiInstanceStateRequested::Running,
                },
            )
            .await
            .unwrap();

        // At this point, we can observe the expected state of the instance
        // manager: contianing the created instance...
        assert_eq!(rt_state.run_state, ApiInstanceState::Running);
        assert_eq!(im.inner.instances.lock().unwrap().len(), 1);

        // ... however, when we drop the ticket of the corresponding instance,
        // the entry is automatically removed from the instance manager.
        ticket.lock().unwrap().take();
        assert_eq!(im.inner.instances.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn ensure_instance_repeatedly() {
        let log = logger();
        let nexus_client = Arc::new(MockNexusClient::default());

        // Instance Manager creation.

        let zfs_ensure_dataset_ctx = MockZfs::ensure_dataset_context();
        zfs_ensure_dataset_ctx.expect().return_once(|pool| {
            assert_eq!(pool, ZONE_ZFS_DATASET);
            Ok(())
        });

        let zones_create_base_ctx = MockZones::create_base_context();
        zones_create_base_ctx.expect().return_once(|_| Ok(()));

        let zones_get_ctx = MockZones::get_context();
        zones_get_ctx.expect().return_once(|| Ok(vec![]));

        let dladm_get_vnics_ctx = MockDladm::get_vnics_context();
        dladm_get_vnics_ctx.expect().return_once(|| Ok(vec![]));

        let im = InstanceManager::new(log, nexus_client).unwrap();

        let ticket = Arc::new(std::sync::Mutex::new(None));
        let ticket_clone = ticket.clone();
        let instance_new_ctx = MockInstance::new_context();
        let mut seq = mockall::Sequence::new();
        instance_new_ctx.expect().return_once(move |_, _, _, _, _| {
            let mut inst = MockInstance::default();
            // First call to ensure (start + transition).
            inst.expect_clone().times(1).in_sequence(&mut seq).return_once(
                move || {
                    let mut inst = MockInstance::default();
                    inst.expect_start().return_once(move |t| {
                        let mut ticket_guard = ticket_clone.lock().unwrap();
                        *ticket_guard = Some(t);
                        Ok(())
                    });
                    inst.expect_transition().return_once(|_| {
                        let mut rt_state = new_runtime_state();
                        rt_state.run_state = ApiInstanceState::Running;
                        Ok(rt_state)
                    });
                    inst
                },
            );
            // Next calls to ensure (transition only).
            inst.expect_clone().times(2).in_sequence(&mut seq).returning(
                move || {
                    let mut inst = MockInstance::default();
                    inst.expect_transition().returning(|_| {
                        let mut rt_state = new_runtime_state();
                        rt_state.run_state = ApiInstanceState::Running;
                        Ok(rt_state)
                    });
                    inst
                },
            );
            Ok(inst)
        });

        let id = test_uuid();
        let rt = new_runtime_state();
        let target = ApiInstanceRuntimeStateRequested {
            run_state: ApiInstanceStateRequested::Running,
        };

        // Creates instance, start + transition.
        im.ensure(id, rt.clone(), target.clone()).await.unwrap();
        // Transition only.
        im.ensure(id, rt.clone(), target.clone()).await.unwrap();
        // Transition only.
        im.ensure(id, rt, target).await.unwrap();

        assert_eq!(im.inner.instances.lock().unwrap().len(), 1);
        ticket.lock().unwrap().take();
        assert_eq!(im.inner.instances.lock().unwrap().len(), 0);
    }
}
