use std::sync::Arc;

use nexus_auth::{authz, context::OpContext, storage::Storage};
use nexus_db_lookup::LookupDataStore;
use omicron_common::api::external::{DeleteResult, Error};
use uuid::Uuid;

pub struct DataStoreContext {
    // These are references to the same datastore with different vtables. This
    // allows returning `Arc<dyn Trait>` objects in functions that require them.
    datastore: Arc<dyn DataStoreImpl>,
    storage: Arc<dyn Storage>,
}

impl DataStoreContext {
    pub fn new(datastore: Arc<dyn DataStoreImpl>) -> Self {
        let storage = datastore.clone();
        Self { datastore, storage }
    }

    #[inline]
    pub fn as_storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }

    /// Deletes the provided `authz_instance`, as long as it is eligible for
    /// deletion (in either the [`InstanceState::NoVmm`] or
    /// [`InstanceState::Failed`] state, or it has already started being
    /// deleted successfully).
    ///
    /// This function is idempotent, but not atomic.
    pub async fn project_delete_instance(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> DeleteResult {
        self.datastore.project_delete_instance(opctx, authz_instance).await
    }

    /// Delete all network interfaces attached to the given instance.
    pub async fn instance_delete_all_network_interfaces(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> DeleteResult {
        self.datastore
            .instance_delete_all_network_interfaces(opctx, authz_instance)
            .await
    }

    /// Delete all non-floating IP addresses associated with the provided instance
    /// ID.
    ///
    /// This method returns the number of records deleted, rather than the usual
    /// `DeleteResult`. That's mostly useful for tests, but could be important
    /// if callers have some invariants they'd like to check.
    pub async fn deallocate_external_ip_by_instance_id(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> Result<usize, Error> {
        self.datastore
            .deallocate_external_ip_by_instance_id(opctx, instance_id)
            .await
    }

    /// Detach an individual Floating IP address from their parent instance.
    ///
    /// As in `deallocate_external_ip_by_instance_id`, this method returns the
    /// number of records altered, rather than an `UpdateResult`.
    ///
    /// This method ignores ongoing state transitions, and is only safely
    /// usable from within the instance_delete saga.
    pub async fn detach_floating_ips_by_instance_id(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> Result<usize, Error> {
        self.datastore
            .detach_floating_ips_by_instance_id(opctx, instance_id)
            .await
    }
}

#[async_trait::async_trait]
pub trait DataStoreImpl: Storage + LookupDataStore {
    async fn project_delete_instance(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> DeleteResult;

    async fn instance_delete_all_network_interfaces(
        &self,
        opctx: &OpContext,
        authz_instance: &authz::Instance,
    ) -> DeleteResult;

    async fn deallocate_external_ip_by_instance_id(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> Result<usize, Error>;

    async fn detach_floating_ips_by_instance_id(
        &self,
        opctx: &OpContext,
        instance_id: Uuid,
    ) -> Result<usize, Error>;
}

impl<'a> From<&'a DataStoreContext> for &'a dyn LookupDataStore {
    fn from(datastore: &'a DataStoreContext) -> &'a dyn LookupDataStore {
        &*datastore.datastore
    }
}
