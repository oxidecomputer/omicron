// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! API for controlling multiple instances on a sled.

use crate::nexus::LazyNexusClient;
use crate::params::{
    InstanceHardware, InstancePutStateResponse, InstanceStateRequested,
    InstanceUnregisterResponse, VpcFirewallRule,
};
use illumos_utils::dladm::Etherstub;
use illumos_utils::link::VnicAllocator;
use illumos_utils::opte::params::SetVirtualNetworkInterfaceHost;
use illumos_utils::opte::PortManager;
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
    Opte(#[from] illumos_utils::opte::Error),

    #[error("Cannot find data link: {0}")]
    Underlay(#[from] sled_hardware::underlay::Error),

    #[error("No datalinks found")]
    NoDatalinks,
}

struct InstanceManagerInternal {
    log: Logger,
    lazy_nexus_client: LazyNexusClient,

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
        lazy_nexus_client: LazyNexusClient,
        etherstub: Etherstub,
        underlay_ip: Ipv6Addr,
        gateway_mac: MacAddr6,
    ) -> Result<InstanceManager, Error> {
        Ok(InstanceManager {
            inner: Arc::new(InstanceManagerInternal {
                log: log.new(o!("component" => "InstanceManager")),
                lazy_nexus_client,
                instances: Mutex::new(BTreeMap::new()),
                vnic_allocator: VnicAllocator::new("Instance", etherstub),
                port_manager: PortManager::new(
                    log.new(o!("component" => "PortManager")),
                    underlay_ip,
                    gateway_mac,
                ),
            }),
        })
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
        initial_hardware: InstanceHardware,
    ) -> Result<InstanceRuntimeState, Error> {
        let requested_propolis_id = initial_hardware.runtime.propolis_id;
        info!(
            &self.inner.log,
            "ensuring instance is registered";
            "instance_id" => %instance_id,
            "propolis_id" => %requested_propolis_id
        );

        let instance = {
            let mut instances = self.inner.instances.lock().unwrap();
            if let Some((existing_propolis_id, existing_instance)) =
                instances.get(&instance_id)
            {
                if requested_propolis_id != *existing_propolis_id {
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
                let instance = Instance::new(
                    instance_log,
                    instance_id,
                    ticket,
                    initial_hardware,
                    self.inner.vnic_allocator.clone(),
                    self.inner.port_manager.clone(),
                    self.inner.lazy_nexus_client.clone(),
                )?;
                let instance_clone = instance.clone();
                let _old = instances
                    .insert(instance_id, (requested_propolis_id, instance));
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

    pub async fn firewall_rules_ensure(
        &self,
        rules: &[VpcFirewallRule],
    ) -> Result<(), Error> {
        info!(
            &self.inner.log,
            "Ensuring VPC firewall rules";
            "rules" => ?&rules,
        );
        self.inner.port_manager.firewall_rules_ensure(rules)?;
        Ok(())
    }

    pub async fn set_virtual_nic_host(
        &self,
        mapping: &SetVirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        info!(
            &self.inner.log,
            "Mapping virtual NIC to physical host";
            "mapping" => ?&mapping,
        );
        self.inner.port_manager.set_virtual_nic_host(mapping)?;
        Ok(())
    }

    pub async fn unset_virtual_nic_host(
        &self,
        mapping: &SetVirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        info!(
            &self.inner.log,
            "Unmapping virtual NIC to physical host";
            "mapping" => ?&mapping,
        );
        self.inner.port_manager.unset_virtual_nic_host(mapping)?;
        Ok(())
    }

    /// Generates an instance ticket associated with this instance manager. This
    /// allows tests in other modules to create an Instance even though they
    /// lack visibility to `InstanceManagerInternal`.
    #[cfg(test)]
    pub fn test_instance_ticket(&self, instance_id: Uuid) -> InstanceTicket {
        InstanceTicket::new(instance_id, self.inner.clone())
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
    use crate::instance::MockInstance;
    use crate::nexus::LazyNexusClient;
    use crate::params::InstanceStateRequested;
    use crate::params::SourceNatConfig;
    use chrono::Utc;
    use illumos_utils::dladm::Etherstub;
    use illumos_utils::{dladm::MockDladm, zone::MockZones};
    use macaddr::MacAddr6;
    use omicron_common::api::external::{
        ByteCount, Generation, InstanceCpuCount, InstanceState,
    };
    use omicron_common::api::internal::nexus::InstanceRuntimeState;
    use omicron_test_utils::dev::test_setup_log;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;

    static INST_UUID_STR: &str = "e398c5d5-5059-4e55-beac-3a1071083aaa";

    fn test_uuid() -> Uuid {
        INST_UUID_STR.parse().unwrap()
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
            source_nat: SourceNatConfig {
                ip: IpAddr::from(Ipv4Addr::new(10, 0, 0, 1)),
                first_port: 0,
                last_port: 1 << (14 - 1),
            },
            external_ips: vec![],
            firewall_rules: vec![],
            disks: vec![],
            cloud_init_bytes: None,
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn ensure_instance() {
        let logctx = test_setup_log("ensure_instance");
        let log = &logctx.log;
        let lazy_nexus_client =
            LazyNexusClient::new(log.clone(), std::net::Ipv6Addr::LOCALHOST)
                .unwrap();

        // Creation of the instance manager incurs some "global" system
        // checks: cleanup of existing zones + vnics.

        let zones_get_ctx = MockZones::get_context();
        zones_get_ctx.expect().return_once(|| Ok(vec![]));

        let dladm_get_vnics_ctx = MockDladm::get_vnics_context();
        dladm_get_vnics_ctx.expect().return_once(|| Ok(vec![]));

        let im = InstanceManager::new(
            log.clone(),
            lazy_nexus_client,
            Etherstub("mylink".to_string()),
            std::net::Ipv6Addr::new(
                0xfd00, 0x1de, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
            ),
            MacAddr6::from([0u8; 6]),
        )
        .unwrap();

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
        let mut seq = mockall::Sequence::new();

        // Expect one call to new() that produces an instance that expects to be
        // cloned once. The clone should expect to ask to be put into the
        // Running state.
        instance_new_ctx.expect().return_once(move |_, _, t, _, _, _, _| {
            let mut inst = MockInstance::default();

            // Move the instance ticket out to the test, since the mock instance
            // won't hold onto it.
            let mut ticket_guard = ticket_clone.lock().unwrap();
            *ticket_guard = Some(t);

            // Expect to be cloned twice, once during registration (to fish the
            // current state out of the instance) and once during the state
            // transition (to hoist the instance reference out of the instance
            // manager lock).
            inst.expect_clone().times(1).in_sequence(&mut seq).return_once(
                move || {
                    let mut inst = MockInstance::default();
                    inst.expect_current_state()
                        .return_once(|| new_initial_instance().runtime);
                    inst
                },
            );

            inst.expect_clone().times(1).in_sequence(&mut seq).return_once(
                move || {
                    let mut inst = MockInstance::default();
                    inst.expect_put_state().return_once(|_| {
                        let mut rt_state = new_initial_instance();
                        rt_state.runtime.run_state = InstanceState::Running;
                        Ok(rt_state.runtime)
                    });
                    inst
                },
            );

            Ok(inst)
        });

        im.ensure_registered(test_uuid(), new_initial_instance())
            .await
            .unwrap();

        // The instance exists now.
        assert_eq!(im.inner.instances.lock().unwrap().len(), 1);

        let rt_state = im
            .ensure_state(test_uuid(), InstanceStateRequested::Running)
            .await
            .unwrap();

        // At this point, we can observe the expected state of the instance
        // manager: containing the created instance...
        assert_eq!(
            rt_state.updated_runtime.unwrap().run_state,
            InstanceState::Running
        );
        assert_eq!(im.inner.instances.lock().unwrap().len(), 1);

        // ... however, when we drop the ticket of the corresponding instance,
        // the entry is automatically removed from the instance manager.
        ticket.lock().unwrap().take();
        assert_eq!(im.inner.instances.lock().unwrap().len(), 0);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn ensure_instance_state_repeatedly() {
        let logctx = test_setup_log("ensure_instance_repeatedly");
        let log = &logctx.log;
        let lazy_nexus_client =
            LazyNexusClient::new(log.clone(), std::net::Ipv6Addr::LOCALHOST)
                .unwrap();

        // Instance Manager creation.

        let zones_get_ctx = MockZones::get_context();
        zones_get_ctx.expect().return_once(|| Ok(vec![]));

        let dladm_get_vnics_ctx = MockDladm::get_vnics_context();
        dladm_get_vnics_ctx.expect().return_once(|| Ok(vec![]));

        let im = InstanceManager::new(
            log.clone(),
            lazy_nexus_client,
            Etherstub("mylink".to_string()),
            std::net::Ipv6Addr::new(
                0xfd00, 0x1de, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
            ),
            MacAddr6::from([0u8; 6]),
        )
        .unwrap();

        let ticket = Arc::new(std::sync::Mutex::new(None));
        let ticket_clone = ticket.clone();
        let instance_new_ctx = MockInstance::new_context();
        let mut seq = mockall::Sequence::new();
        instance_new_ctx.expect().return_once(move |_, _, t, _, _, _, _| {
            let mut inst = MockInstance::default();
            let mut ticket_guard = ticket_clone.lock().unwrap();
            *ticket_guard = Some(t);

            // First call to ensure (start + transition).
            inst.expect_clone().times(1).in_sequence(&mut seq).return_once(
                move || {
                    let mut inst = MockInstance::default();

                    inst.expect_current_state()
                        .returning(|| new_initial_instance().runtime);

                    inst.expect_put_state().return_once(|_| {
                        let mut rt_state = new_initial_instance();
                        rt_state.runtime.run_state = InstanceState::Running;
                        Ok(rt_state.runtime)
                    });
                    inst
                },
            );

            // Next calls to ensure (transition only).
            inst.expect_clone().times(3).in_sequence(&mut seq).returning(
                move || {
                    let mut inst = MockInstance::default();
                    inst.expect_put_state().returning(|_| {
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

        // Register the instance, then issue all three state transitions.
        im.ensure_registered(id, rt).await.unwrap();
        im.ensure_state(id, InstanceStateRequested::Running).await.unwrap();
        im.ensure_state(id, InstanceStateRequested::Running).await.unwrap();
        im.ensure_state(id, InstanceStateRequested::Running).await.unwrap();

        assert_eq!(im.inner.instances.lock().unwrap().len(), 1);
        ticket.lock().unwrap().take();
        assert_eq!(im.inner.instances.lock().unwrap().len(), 0);

        logctx.cleanup_successful();
    }
}
