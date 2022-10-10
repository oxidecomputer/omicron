// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces available to saga actions and undo actions

use crate::context::OpContext;
use crate::Nexus;
use crate::{authz, db};
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::internal::nexus;
use slog::Logger;
use std::fmt;
use std::sync::Arc;
use uuid::Uuid;

/// Trait which exposes Nexus interfaces to Sagas.
///
/// In the production implementation of Nexus, this will almost certainly just
/// be implemented directly by Nexus. However, by defining these methods
/// explicitly, tests can mock out access saga actions that would modify more
/// state than simply the database.
#[async_trait::async_trait]
pub trait NexusForSagas: Send + Sync {
    /// Access the raw database under Nexus.
    fn datastore(&self) -> &Arc<db::DataStore>;

    /// Returns a random sled ID.
    async fn random_sled_id(&self) -> Result<Option<Uuid>, Error>;

    // TODO: Should this be here?
    //
    // On the one hand, we do want to let sagas call other sagas.
    // However, wouldn't it be nice if they called through this more limited
    // nexus interface, rather than the "true" (read: too large) Nexus inteface?
    //
    // Then we could actually validate the volume-deletion behavior when unit
    // testing disks / snapshots.
    //
    // TODO: I think... this might just involve isolating the "saga engine"
    // component of nexus from the rest of it. I'd like to re-use that bit!
    async fn volume_delete(self: Arc<Self>, volume_id: Uuid) -> DeleteResult;

    /// Makes a request for a specific sled to update its runtime.
    async fn instance_sled_agent_set_runtime(
        &self,
        sled_id: Uuid,
        body: &sled_agent_client::types::InstanceEnsureBody,
        instance_id: Uuid,
    ) -> Result<nexus::InstanceRuntimeState, Error>;

    async fn disk_snapshot_instance_sled_agent(
        &self,
        instance: &db::model::Instance,
        disk_id: Uuid,
        body: &sled_agent_client::types::InstanceIssueDiskSnapshotRequestBody,
    ) -> Result<(), Error>;

    /// Makes a request for a sled to take a snapshot of a disk.
    // TODO: Should just be 'snapshot'? already exposing 'random_sled_id'
    async fn disk_snapshot_random_sled_agent(
        &self,
        disk_id: Uuid,
        body: &sled_agent_client::types::DiskSnapshotRequestBody,
    ) -> Result<(), Error>;

    // TODO: This one could be implemented purely in the DB?
//    async fn instance_attach_disk(
//        &self,
//        opctx: &OpContext,
//        organization_name: &db::model::Name,
//        project_name: &db::model::Name,
//        instance_name: &db::model::Name,
//        disk_name: &db::model::Name,
//    ) -> UpdateResult<db::model::Disk>;

    // TODO: This one could be implemented purely in the DB?
    async fn instance_detach_disk(
        &self,
        opctx: &OpContext,
        organization_name: &db::model::Name,
        project_name: &db::model::Name,
        instance_name: &db::model::Name,
        disk_name: &db::model::Name,
    ) -> UpdateResult<db::model::Disk>;

    // TODO: This is half in the DB, half to the sled agent.
    async fn instance_set_runtime(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        db_instance: &db::model::Instance,
        requested: sled_agent_client::types::InstanceRuntimeStateRequested,
    ) -> Result<(), Error>;

    // TODO: This calls instance_set_runtime, so, all the problems
    // that one has too
    async fn instance_start_migrate(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
        migration_id: Uuid,
        dst_propolis_id: Uuid,
    ) -> UpdateResult<db::model::Instance>;
}

// When implementing this interface for Nexus, there's no reason to have
// more complexity than directly calling into one of Nexus' internal methods
// directly.
//
// If you're tempted to change that, consider modifying what interface is
// actually exposed to saga actions instead.
#[async_trait::async_trait]
impl NexusForSagas for Nexus {
    fn datastore(&self) -> &Arc<db::DataStore> {
        self.datastore()
    }

    async fn random_sled_id(&self) -> Result<Option<Uuid>, Error> {
        self.random_sled_id().await
    }

    async fn volume_delete(self: Arc<Self>, volume_id: Uuid) -> DeleteResult {
        self.volume_delete(volume_id).await
    }

    async fn instance_sled_agent_set_runtime(
        &self,
        sled_id: Uuid,
        body: &sled_agent_client::types::InstanceEnsureBody,
        instance_id: Uuid,
    ) -> Result<nexus::InstanceRuntimeState, Error> {
        self.instance_sled_agent_set_runtime(sled_id, body, instance_id).await
    }

    async fn disk_snapshot_instance_sled_agent(
        &self,
        instance: &db::model::Instance,
        disk_id: Uuid,
        body: &sled_agent_client::types::InstanceIssueDiskSnapshotRequestBody,
    ) -> Result<(), Error> {
        self.disk_snapshot_instance_sled_agent(instance, disk_id, body).await
    }

    async fn disk_snapshot_random_sled_agent(
        &self,
        disk_id: Uuid,
        body: &sled_agent_client::types::DiskSnapshotRequestBody,
    ) -> Result<(), Error> {
        self.disk_snapshot_random_sled_agent(disk_id, body).await
    }

//    async fn instance_attach_disk(
//        &self,
//        opctx: &OpContext,
//        organization_name: &db::model::Name,
//        project_name: &db::model::Name,
//        instance_name: &db::model::Name,
//        disk_name: &db::model::Name,
//    ) -> UpdateResult<db::model::Disk> {
//        self.instance_attach_disk(
//            opctx,
//            organization_name,
//            project_name,
//            instance_name,
//            disk_name,
//        )
//        .await
//    }

    async fn instance_detach_disk(
        &self,
        opctx: &OpContext,
        organization_name: &db::model::Name,
        project_name: &db::model::Name,
        instance_name: &db::model::Name,
        disk_name: &db::model::Name,
    ) -> UpdateResult<db::model::Disk> {
        self.instance_detach_disk(
            opctx,
            organization_name,
            project_name,
            instance_name,
            disk_name,
        )
        .await
    }

    async fn instance_set_runtime(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
        db_instance: &db::model::Instance,
        requested: sled_agent_client::types::InstanceRuntimeStateRequested,
    ) -> Result<(), Error> {
        self.instance_set_runtime(opctx, authz_instance, db_instance, requested)
            .await
    }

    async fn instance_start_migrate(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
        migration_id: Uuid,
        dst_propolis_id: Uuid,
    ) -> UpdateResult<db::model::Instance> {
        self.instance_start_migrate(
            opctx,
            instance_id,
            migration_id,
            dst_propolis_id,
        )
        .await
    }
}

// TODO-design Should this be the same thing as ServerContext?  It's
// very analogous, but maybe there's utility in having separate views for the
// HTTP server and sagas.
pub struct SagaContext {
    nexus: Arc<dyn NexusForSagas>,
    log: Logger,
    authz: Arc<authz::Authz>,
}

impl fmt::Debug for SagaContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SagaContext { (nexus) ... }")
    }
}

impl SagaContext {
    pub fn new(
        nexus: Arc<dyn NexusForSagas>,
        log: Logger,
        authz: Arc<authz::Authz>,
    ) -> SagaContext {
        SagaContext { authz, nexus, log }
    }

    pub fn log(&self) -> &Logger {
        &self.log
    }

    pub fn authz(&self) -> &Arc<authz::Authz> {
        &self.authz
    }

    pub fn nexus(&self) -> &Arc<dyn NexusForSagas> {
        &self.nexus
    }

    pub fn datastore(&self) -> &db::DataStore {
        self.nexus.datastore()
    }
}
