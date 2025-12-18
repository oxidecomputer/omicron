// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use async_trait::async_trait;
use nexus_db_lookup::LookupPath;
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{GenericUuid, InstanceUuid, PropolisUuid, SledUuid};
use sled_agent_client::Client as SledAgentClient;
use std::sync::Arc;
use uuid::Uuid;

/// The information needed to talk to a sled agent about an instance that is
/// active on that sled.
pub struct InstanceSledAgentInfo {
    /// The ID of the Propolis job to send to sled agent.
    pub propolis_id: PropolisUuid,

    /// The ID of the sled where the Propolis job is running.
    pub sled_id: SledUuid,

    /// A client for talking to the Propolis's host sled.
    pub sled_client: Arc<SledAgentClient>,

    /// The ID of the instance's migration target Propolis, if it has one.
    pub dst_propolis_id: Option<PropolisUuid>,
}

/// Exposes additional [`super::Nexus`] interfaces for use by the test suite
#[async_trait]
pub trait TestInterfaces {
    /// Access the Rack ID of the currently executing Nexus.
    fn rack_id(&self) -> Uuid;

    /// Attempts to obtain the Propolis ID and sled agent information for an
    /// instance.
    ///
    /// # Arguments
    ///
    /// - `id`: The ID of the instance of interest.
    /// - `opctx`: An optional operation context to use for authorization
    ///   checks. If `None`, this routine supplies the default test opctx.
    ///
    /// # Return value
    ///
    /// - `Ok(Some(info))` if the instance has an active Propolis.
    /// - `Ok(None)` if the instance has no active Propolis.
    /// - `Err` if an error occurred.
    async fn active_instance_info(
        &self,
        id: &InstanceUuid,
        opctx: Option<&OpContext>,
    ) -> Result<Option<InstanceSledAgentInfo>, Error>;

    /// Returns the SledAgentClient for the sled running an instance to which a
    /// disk is attached.
    async fn disk_sled_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Option<Arc<SledAgentClient>>, Error>;

    async fn set_disk_as_faulted(&self, disk_id: &Uuid) -> Result<bool, Error>;

    fn set_samael_max_issue_delay(&self, max_issue_delay: chrono::Duration);

    /// Manually invalidate multicast caches and activate reconciler.
    ///
    /// This simulates topology changes that would require cache invalidation,
    /// such as backplane configuration changes or sled movements.
    fn invalidate_multicast_caches(&self);
}

#[async_trait]
impl TestInterfaces for super::Nexus {
    fn rack_id(&self) -> Uuid {
        self.rack_id
    }

    async fn active_instance_info(
        &self,
        id: &InstanceUuid,
        opctx: Option<&OpContext>,
    ) -> Result<Option<InstanceSledAgentInfo>, Error> {
        let local_opctx;
        let opctx = match opctx {
            Some(o) => o,
            None => {
                local_opctx = OpContext::for_tests(
                    self.log.new(o!()),
                    Arc::clone(&self.db_datastore)
                        as Arc<dyn nexus_auth::storage::Storage>,
                );
                &local_opctx
            }
        };

        let (.., authz_instance) = LookupPath::new(&opctx, &self.db_datastore)
            .instance_id(id.into_untyped_uuid())
            .lookup_for(nexus_db_queries::authz::Action::Read)
            .await?;

        let state = self
            .datastore()
            .instance_fetch_with_vmm(opctx, &authz_instance)
            .await?;

        let Some(vmm) = state.vmm() else {
            return Ok(None);
        };

        let sled_id = vmm.sled_id();
        Ok(Some(InstanceSledAgentInfo {
            propolis_id: PropolisUuid::from_untyped_uuid(vmm.id),
            sled_id,
            sled_client: self.sled_client(&sled_id).await?,
            dst_propolis_id: state
                .instance()
                .runtime()
                .dst_propolis_id
                .map(PropolisUuid::from_untyped_uuid),
        }))
    }

    async fn disk_sled_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Option<Arc<SledAgentClient>>, Error> {
        let opctx = OpContext::for_tests(
            self.log.new(o!()),
            Arc::clone(&self.db_datastore)
                as Arc<dyn nexus_auth::storage::Storage>,
        );
        let (.., db_disk) = LookupPath::new(&opctx, &self.db_datastore)
            .disk_id(*id)
            .fetch()
            .await?;

        let instance_id = InstanceUuid::from_untyped_uuid(
            db_disk.runtime().attach_instance_id.unwrap(),
        );

        Ok(self
            .active_instance_info(&instance_id, Some(&opctx))
            .await?
            .map(|info| info.sled_client))
    }

    async fn set_disk_as_faulted(&self, disk_id: &Uuid) -> Result<bool, Error> {
        let opctx = OpContext::for_tests(
            self.log.new(o!()),
            Arc::clone(&self.db_datastore)
                as Arc<dyn nexus_auth::storage::Storage>,
        );

        let (.., authz_disk, db_disk) =
            LookupPath::new(&opctx, &self.db_datastore)
                .disk_id(*disk_id)
                .fetch()
                .await?;

        let new_runtime = db_disk.runtime_state.faulted();

        self.db_datastore
            .disk_update_runtime(&opctx, &authz_disk, &new_runtime)
            .await
    }

    fn set_samael_max_issue_delay(&self, max_issue_delay: chrono::Duration) {
        let mut mid = self.samael_max_issue_delay.lock().unwrap();
        *mid = Some(max_issue_delay);
    }

    fn invalidate_multicast_caches(&self) {
        if let Some(flag) =
            &self.background_tasks_internal.multicast_invalidate_cache
        {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
            self.background_tasks
                .activate(&self.background_tasks.task_multicast_reconciler);
        }
    }
}
