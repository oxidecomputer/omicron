// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::db::lookup::LookupPath;
use async_trait::async_trait;
use nexus_db_queries::context::OpContext;
use omicron_common::api::external::Error;
use sled_agent_client::Client as SledAgentClient;
use std::sync::Arc;
use uuid::Uuid;

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
        id: &Uuid,
    ) -> Result<Arc<SledAgentClient>, Error>;

    /// Returns the SledAgentClient for a Disk from its id.
    async fn disk_sled_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Arc<SledAgentClient>, Error>;

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
        id: &Uuid,
    ) -> Result<Arc<SledAgentClient>, Error> {
        let opctx = OpContext::for_tests(
            self.log.new(o!()),
            Arc::clone(&self.db_datastore),
        );
        let (.., db_instance) = LookupPath::new(&opctx, &self.db_datastore)
            .instance_id(*id)
            .fetch()
            .await?;
        self.instance_sled(&db_instance).await
    }

    async fn disk_sled_by_id(
        &self,
        id: &Uuid,
    ) -> Result<Arc<SledAgentClient>, Error> {
        let opctx = OpContext::for_tests(
            self.log.new(o!()),
            Arc::clone(&self.db_datastore),
        );
        let (.., db_disk) = LookupPath::new(&opctx, &self.db_datastore)
            .disk_id(*id)
            .fetch()
            .await?;
        let (.., db_instance) = LookupPath::new(&opctx, &self.db_datastore)
            .instance_id(db_disk.runtime().attach_instance_id.unwrap())
            .fetch()
            .await?;
        self.instance_sled(&db_instance).await
    }

    async fn set_disk_as_faulted(&self, disk_id: &Uuid) -> Result<bool, Error> {
        let opctx = OpContext::for_tests(
            self.log.new(o!()),
            Arc::clone(&self.db_datastore),
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
