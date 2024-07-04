// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use async_trait::async_trait;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::lookup::LookupPath;
use omicron_common::api::external::Error;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::{InstanceUuid, SledUuid};
use sled_agent_client::Client as SledAgentClient;
use std::sync::Arc;
use uuid::Uuid;

pub use super::update::HostPhase1Updater;
pub use super::update::MgsClients;
pub use super::update::RotUpdater;
pub use super::update::SpUpdater;
pub use super::update::UpdateProgress;
pub use gateway_client::types::SpType;

/// Exposes additional [`super::Nexus`] interfaces for use by the test suite
#[async_trait]
pub trait TestInterfaces {
    /// Access the Rack ID of the currently executing Nexus.
    fn rack_id(&self) -> Uuid;

    /// Returns the SledAgentClient for an Instance from its id.  We may also
    /// want to split this up into instance_lookup_by_id() and instance_sled(),
    /// but after all it's a test suite special to begin with.
    async fn instance_sled_by_id(
        &self,
        id: &InstanceUuid,
    ) -> Result<Option<Arc<SledAgentClient>>, Error>;

    async fn instance_sled_by_id_with_opctx(
        &self,
        id: &InstanceUuid,
        opctx: &OpContext,
    ) -> Result<Option<Arc<SledAgentClient>>, Error>;

    /// Returns the SledAgentClient for the sled running an instance to which a
    /// disk is attached.
    async fn disk_sled_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Option<Arc<SledAgentClient>>, Error>;

    /// Returns the supplied instance's current active sled ID.
    async fn instance_sled_id(
        &self,
        instance_id: &InstanceUuid,
    ) -> Result<Option<SledUuid>, Error>;

    async fn instance_sled_id_with_opctx(
        &self,
        instance_id: &InstanceUuid,
        opctx: &OpContext,
    ) -> Result<Option<SledUuid>, Error>;

    async fn set_disk_as_faulted(&self, disk_id: &Uuid) -> Result<bool, Error>;

    fn set_samael_max_issue_delay(&self, max_issue_delay: chrono::Duration);
}

#[async_trait]
impl TestInterfaces for super::Nexus {
    fn rack_id(&self) -> Uuid {
        self.rack_id
    }

    async fn instance_sled_by_id(
        &self,
        id: &InstanceUuid,
    ) -> Result<Option<Arc<SledAgentClient>>, Error> {
        let opctx = OpContext::for_tests(
            self.log.new(o!()),
            Arc::clone(&self.db_datastore)
                as Arc<dyn nexus_auth::storage::Storage>,
        );

        self.instance_sled_by_id_with_opctx(id, &opctx).await
    }

    async fn instance_sled_by_id_with_opctx(
        &self,
        id: &InstanceUuid,
        opctx: &OpContext,
    ) -> Result<Option<Arc<SledAgentClient>>, Error> {
        let sled_id = self.instance_sled_id_with_opctx(id, opctx).await?;
        if let Some(sled_id) = sled_id {
            Ok(Some(self.sled_client(&sled_id).await?))
        } else {
            Ok(None)
        }
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
        self.instance_sled_by_id(&instance_id).await
    }

    async fn instance_sled_id(
        &self,
        id: &InstanceUuid,
    ) -> Result<Option<SledUuid>, Error> {
        let opctx = OpContext::for_tests(
            self.log.new(o!()),
            Arc::clone(&self.db_datastore)
                as Arc<dyn nexus_auth::storage::Storage>,
        );

        self.instance_sled_id_with_opctx(id, &opctx).await
    }

    async fn instance_sled_id_with_opctx(
        &self,
        id: &InstanceUuid,
        opctx: &OpContext,
    ) -> Result<Option<SledUuid>, Error> {
        let (.., authz_instance) = LookupPath::new(&opctx, &self.db_datastore)
            .instance_id(id.into_untyped_uuid())
            .lookup_for(nexus_db_queries::authz::Action::Read)
            .await?;

        Ok(self
            .datastore()
            .instance_fetch_with_vmm(opctx, &authz_instance)
            .await?
            .sled_id())
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
}
