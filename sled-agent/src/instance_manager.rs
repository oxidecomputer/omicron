// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for controlling multiple instances on a sled.

use crate::illumos::dladm::Etherstub;
use crate::illumos::vnic::VnicAllocator;
use crate::nexus::NexusClient;
use crate::opte::PortManager;
use crate::params::{
    InstanceHardware, InstanceMigrateParams, InstanceRuntimeStateRequested,
    InstanceSerialConsoleData,
};
use crate::serial::ByteOffset;
use macaddr::MacAddr6;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use slog::Logger;
use std::collections::BTreeMap;
use std::net::Ipv6Addr;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[cfg(not(test))]
use crate::instance::Instance;
#[cfg(test)]
use crate::instance::MockInstance as Instance;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Instance error: {0}")]
    Instance(#[from] crate::instance::Error),

    #[error("No such instance ID: {0}")]
    NoSuchInstance(Uuid),

    #[error("OPTE port management error: {0}")]
    Opte(#[from] crate::opte::Error),
}

struct InstanceManagerInternal {
    log: Logger,
    nexus_client: Arc<NexusClient>,

    // TODO: If we held an object representing an enum of "Created OR Running"
    // instance, we could avoid the methods within "instance.rs" that panic
    // if the Propolis client hasn't been initialized.
    /// A mapping from a Sled Agent "Instance ID" to ("Propolis ID", [Instance]).
    instances: Mutex<BTreeMap<Uuid, (Uuid, Instance)>>,

    vnic_allocator: VnicAllocator<Etherstub>,
    port_manager: PortManager,
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
        etherstub: Etherstub,
        underlay_ip: Ipv6Addr,
        gateway_mac: MacAddr6,
    ) -> InstanceManager {
        InstanceManager {
            inner: Arc::new(InstanceManagerInternal {
                log: log.new(o!("component" => "InstanceManager")),
                nexus_client,
                instances: Mutex::new(BTreeMap::new()),
                vnic_allocator: VnicAllocator::new("Instance", etherstub),
                port_manager: PortManager::new(
                    log.new(o!("component" => "PortManager")),
                    underlay_ip,
                    gateway_mac,
                ),
            }),
        }
    }

    /// Idempotently ensures that the given Instance (described by
    /// `initial_hardware`) exists on this server in the given runtime state
    /// (described by `target`).
    pub async fn ensure(
        &self,
        instance_id: Uuid,
        initial_hardware: InstanceHardware,
        target: InstanceRuntimeStateRequested,
        migrate: Option<InstanceMigrateParams>,
    ) -> Result<InstanceRuntimeState, Error> {
        info!(
            &self.inner.log,
            "instance_ensure {} -> {:?}", instance_id, target
        );

        let target_propolis_id = initial_hardware.runtime.propolis_id;

        let (instance, maybe_instance_ticket) = {
            let mut instances = self.inner.instances.lock().unwrap();
            match (instances.get(&instance_id), &migrate) {
                (Some((_, instance)), None) => {
                    // Instance already exists and we're not performing a migration
                    info!(&self.inner.log, "instance already exists");
                    (instance.clone(), None)
                }
                (Some((propolis_id, instance)), Some(_))
                    if *propolis_id == target_propolis_id =>
                {
                    // A migration was requested but the given propolis id
                    // already seems to be the propolis backing this instance
                    // so just return the instance as is without triggering
                    // another migration.
                    info!(
                        &self.inner.log,
                        "instance already exists with given dst propolis"
                    );
                    (instance.clone(), None)
                }
                _ => {
                    // Instance does not exist or one does but we're performing
                    // a intra-sled migration. Either way - create an instance
                    info!(&self.inner.log, "new instance");
                    let instance_log = self.inner.log.new(o!());
                    let instance = Instance::new(
                        instance_log,
                        instance_id,
                        initial_hardware,
                        self.inner.vnic_allocator.clone(),
                        self.inner.port_manager.clone(),
                        self.inner.nexus_client.clone(),
                    )?;
                    let instance_clone = instance.clone();
                    let old_instance = instances
                        .insert(instance_id, (target_propolis_id, instance));
                    if let Some((_old_propolis_id, old_instance)) = old_instance
                    {
                        // If we had a previous instance, we must be migrating
                        assert!(migrate.is_some());
                        // TODO: assert that old_instance.inner.propolis_id() == migrate.src_uuid

                        // We forget the old instance because otherwise if it is the last
                        // handle, the `InstanceTicket` it holds will also be dropped.
                        // `InstanceTicket::drop` will try to remove the corresponding
                        // instance from the instance manager. It does this based off the
                        // instance's ID. Given that we just replaced said instance using
                        // the same ID, that would inadvertantly remove our newly created
                        // instance instead.
                        // TODO: cleanup source instance properly
                        std::mem::forget(old_instance);
                    }

                    let ticket = Some(InstanceTicket::new(
                        instance_id,
                        self.inner.clone(),
                    ));
                    (instance_clone, ticket)
                }
            }
        };

        // If we created a new instance, start or migrate it - but do so outside
        // the "instances" lock, since initialization may take a while.
        //
        // Additionally, this makes it possible to manage the "instance_ticket",
        // which might need to grab the lock to remove the instance during
        // teardown.
        if let Some(instance_ticket) = maybe_instance_ticket {
            instance.start(instance_ticket, migrate).await?;
        }

        instance.transition(target).await.map_err(|e| e.into())
    }

    pub async fn instance_serial_console_buffer_data(
        &self,
        instance_id: Uuid,
        byte_offset: ByteOffset,
        max_bytes: Option<usize>,
    ) -> Result<InstanceSerialConsoleData, Error> {
        let instance = {
            let instances = self.inner.instances.lock().unwrap();
            let (_, instance) = instances
                .get(&instance_id)
                .ok_or(Error::NoSuchInstance(instance_id))?;
            instance.clone()
        };
        instance
            .serial_console_buffer_data(byte_offset, max_bytes)
            .await
            .map_err(Error::from)
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::illumos::dladm::Etherstub;
    use crate::illumos::{dladm::MockDladm, zone::MockZones};
    use crate::instance::MockInstance;
    use crate::mocks::MockNexusClient;
    use crate::params::ExternalIp;
    use crate::params::InstanceStateRequested;
    use chrono::Utc;
    use macaddr::MacAddr6;
    use omicron_common::api::external::{
        ByteCount, Generation, InstanceCpuCount, InstanceState,
    };
    use omicron_common::api::internal::nexus::InstanceRuntimeState;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;

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

    fn new_initial_instance() -> InstanceHardware {
        InstanceHardware {
            runtime: InstanceRuntimeState {
                run_state: InstanceState::Creating,
                sled_id: Uuid::new_v4(),
                propolis_id: Uuid::new_v4(),
                dst_propolis_id: None,
                propolis_addr: None,
                migration_id: None,
                ncpus: InstanceCpuCount(2),
                memory: ByteCount::from_mebibytes_u32(512),
                hostname: "myvm".to_string(),
                gen: Generation::new(),
                time_updated: Utc::now(),
            },
            nics: vec![],
            external_ip: ExternalIp {
                ip: IpAddr::from(Ipv4Addr::new(10, 0, 0, 1)),
                first_port: 0,
                last_port: 1 << 14 - 1,
            },
            disks: vec![],
            cloud_init_bytes: None,
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn ensure_instance() {
        let log = logger();
        let nexus_client = Arc::new(MockNexusClient::default());

        // Creation of the instance manager incurs some "global" system
        // checks: cleanup of existing zones + vnics.

        let zones_get_ctx = MockZones::get_context();
        zones_get_ctx.expect().return_once(|| Ok(vec![]));

        let dladm_get_vnics_ctx = MockDladm::get_vnics_context();
        dladm_get_vnics_ctx.expect().return_once(|| Ok(vec![]));

        let im = InstanceManager::new(
            log,
            nexus_client,
            Etherstub("mylink".to_string()),
            std::net::Ipv6Addr::new(
                0xfd00, 0x1de, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
            ),
            MacAddr6::from([0u8; 6]),
        );

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
        instance_new_ctx.expect().return_once(move |_, _, _, _, _, _| {
            let mut inst = MockInstance::default();
            inst.expect_clone().return_once(move || {
                let mut inst = MockInstance::default();
                inst.expect_start().return_once(move |t, _| {
                    // Grab hold of the ticket, so we don't try to remove the
                    // instance immediately after "start" completes.
                    let mut ticket_guard = ticket_clone.lock().unwrap();
                    *ticket_guard = Some(t);
                    Ok(())
                });
                inst.expect_transition().return_once(|_| {
                    let mut rt_state = new_initial_instance();
                    rt_state.runtime.run_state = InstanceState::Running;
                    Ok(rt_state.runtime)
                });
                inst
            });
            Ok(inst)
        });
        let rt_state = im
            .ensure(
                test_uuid(),
                new_initial_instance(),
                InstanceRuntimeStateRequested {
                    run_state: InstanceStateRequested::Running,
                    migration_params: None,
                },
                None,
            )
            .await
            .unwrap();

        // At this point, we can observe the expected state of the instance
        // manager: contianing the created instance...
        assert_eq!(rt_state.run_state, InstanceState::Running);
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

        let zones_get_ctx = MockZones::get_context();
        zones_get_ctx.expect().return_once(|| Ok(vec![]));

        let dladm_get_vnics_ctx = MockDladm::get_vnics_context();
        dladm_get_vnics_ctx.expect().return_once(|| Ok(vec![]));

        let im = InstanceManager::new(
            log,
            nexus_client,
            Etherstub("mylink".to_string()),
            std::net::Ipv6Addr::new(
                0xfd00, 0x1de, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
            ),
            MacAddr6::from([0u8; 6]),
        );

        let ticket = Arc::new(std::sync::Mutex::new(None));
        let ticket_clone = ticket.clone();
        let instance_new_ctx = MockInstance::new_context();
        let mut seq = mockall::Sequence::new();
        instance_new_ctx.expect().return_once(move |_, _, _, _, _, _| {
            let mut inst = MockInstance::default();
            // First call to ensure (start + transition).
            inst.expect_clone().times(1).in_sequence(&mut seq).return_once(
                move || {
                    let mut inst = MockInstance::default();
                    inst.expect_start().return_once(move |t, _| {
                        let mut ticket_guard = ticket_clone.lock().unwrap();
                        *ticket_guard = Some(t);
                        Ok(())
                    });
                    inst.expect_transition().return_once(|_| {
                        let mut rt_state = new_initial_instance();
                        rt_state.runtime.run_state = InstanceState::Running;
                        Ok(rt_state.runtime)
                    });
                    inst
                },
            );
            // Next calls to ensure (transition only).
            inst.expect_clone().times(2).in_sequence(&mut seq).returning(
                move || {
                    let mut inst = MockInstance::default();
                    inst.expect_transition().returning(|_| {
                        let mut rt_state = new_initial_instance();
                        rt_state.runtime.run_state = InstanceState::Running;
                        Ok(rt_state.runtime)
                    });
                    inst
                },
            );
            Ok(inst)
        });

        let id = test_uuid();
        let rt = new_initial_instance();
        let target = InstanceRuntimeStateRequested {
            run_state: InstanceStateRequested::Running,
            migration_params: None,
        };

        // Creates instance, start + transition.
        im.ensure(id, rt.clone(), target.clone(), None).await.unwrap();
        // Transition only.
        im.ensure(id, rt.clone(), target.clone(), None).await.unwrap();
        // Transition only.
        im.ensure(id, rt, target, None).await.unwrap();

        assert_eq!(im.inner.instances.lock().unwrap().len(), 1);
        ticket.lock().unwrap().take();
        assert_eq!(im.inner.instances.lock().unwrap().len(), 0);
    }
}
