// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`VirtualResourceProvisioning`]s.

use super::DataStore;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::model::VirtualResourceProvisioning;
use crate::db::pool::DbConnection;
use crate::db::queries::virtual_resource_provisioning_update::VirtualResourceProvisioningUpdate;
use async_bb8_diesel::{AsyncRunQueryDsl, PoolError};
use diesel::prelude::*;
use omicron_common::api::external::{
    DeleteResult, Error, LookupType, ResourceType,
};
use oximeter::{types::Sample, Metric, MetricsError, Target};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// Describes a collection that holds other resources.
///
/// Example targets might include projects, organizations, silos or fleets.
#[derive(Debug, Clone, Target)]
struct CollectionTarget {
    id: Uuid,
}

#[derive(Debug, Clone, Metric)]
struct VirtualDiskSpaceProvisioned {
    #[datum]
    bytes_used: i64,
}

#[derive(Debug, Clone, Metric)]
struct CpusProvisioned {
    #[datum]
    cpus: i64,
}

#[derive(Debug, Clone, Metric)]
struct RamProvisioned {
    #[datum]
    bytes: i64,
}

/// An oximeter producer for reporting [`VirtualResourceProvisioning`] information to Clickhouse.
///
/// This producer collects samples whenever the database record for a collection
/// is created or updated. This implies that the CockroachDB record is always
/// kept up-to-date, and the Clickhouse historical records are batched and
/// transmitted once they are collected (as is the norm for Clickhouse metrics).
#[derive(Debug, Default, Clone)]
pub(crate) struct Producer {
    samples: Arc<Mutex<Vec<Sample>>>,
}

impl Producer {
    pub fn new() -> Self {
        Self { samples: Arc::new(Mutex::new(vec![])) }
    }

    fn append_disk_metrics(
        &self,
        provisions: &Vec<VirtualResourceProvisioning>,
    ) {
        let new_samples = provisions
            .iter()
            .map(|provision| {
                Sample::new(
                    &CollectionTarget { id: provision.id },
                    &VirtualDiskSpaceProvisioned {
                        bytes_used: provision.virtual_disk_bytes_provisioned,
                    },
                )
            })
            .collect::<Vec<_>>();

        self.append(new_samples);
    }

    fn append_cpu_metrics(
        &self,
        provisions: &Vec<VirtualResourceProvisioning>,
    ) {
        let new_samples = provisions
            .iter()
            .map(|provision| {
                Sample::new(
                    &CollectionTarget { id: provision.id },
                    &CpusProvisioned { cpus: provision.cpus_provisioned },
                )
            })
            .chain(provisions.iter().map(|provision| {
                Sample::new(
                    &CollectionTarget { id: provision.id },
                    &RamProvisioned { bytes: provision.ram_provisioned },
                )
            }))
            .collect::<Vec<_>>();

        self.append(new_samples);
    }

    fn append(&self, mut new_samples: Vec<Sample>) {
        let mut pending_samples = self.samples.lock().unwrap();
        pending_samples.append(&mut new_samples);
    }
}

impl oximeter::Producer for Producer {
    fn produce(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Sample> + 'static>, MetricsError> {
        let samples =
            std::mem::replace(&mut *self.samples.lock().unwrap(), vec![]);
        Ok(Box::new(samples.into_iter()))
    }
}

impl DataStore {
    /// Create a [`VirtualResourceProvisioning`] object.
    pub async fn virtual_resource_provisioning_create(
        &self,
        opctx: &OpContext,
        virtual_resource_provisioning: VirtualResourceProvisioning,
    ) -> Result<Vec<VirtualResourceProvisioning>, Error> {
        let pool = self.pool_authorized(opctx).await?;
        self.virtual_resource_provisioning_create_on_connection(
            pool,
            virtual_resource_provisioning,
        )
        .await
    }

    pub(crate) async fn virtual_resource_provisioning_create_on_connection<
        ConnErr,
    >(
        &self,
        conn: &(impl async_bb8_diesel::AsyncConnection<DbConnection, ConnErr>
              + Sync),
        virtual_resource_provisioning: VirtualResourceProvisioning,
    ) -> Result<Vec<VirtualResourceProvisioning>, Error>
    where
        ConnErr: From<diesel::result::Error> + Send + 'static,
        PoolError: From<ConnErr>,
    {
        use db::schema::virtual_resource_provisioning::dsl;

        let provisions: Vec<VirtualResourceProvisioning> =
            diesel::insert_into(dsl::virtual_resource_provisioning)
                .values(virtual_resource_provisioning)
                .on_conflict_do_nothing()
                .get_results_async(conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel_pool(
                        PoolError::from(e),
                        ErrorHandler::Server,
                    )
                })?;
        self.virtual_resource_provisioning_producer
            .append_disk_metrics(&provisions);
        self.virtual_resource_provisioning_producer
            .append_cpu_metrics(&provisions);
        Ok(provisions)
    }

    pub async fn virtual_resource_provisioning_get(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<VirtualResourceProvisioning, Error> {
        use db::schema::virtual_resource_provisioning::dsl;

        let virtual_resource_provisioning = dsl::virtual_resource_provisioning
            .find(id)
            .select(VirtualResourceProvisioning::as_select())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::VirtualResourceProvisioning,
                        LookupType::ById(id),
                    ),
                )
            })?;
        Ok(virtual_resource_provisioning)
    }

    /// Delete a [`VirtualResourceProvisioning`] object.
    pub async fn virtual_resource_provisioning_delete(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> DeleteResult {
        use db::schema::virtual_resource_provisioning::dsl;

        diesel::delete(dsl::virtual_resource_provisioning)
            .filter(dsl::id.eq(id))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        Ok(())
    }

    // TODO: These could 100% act on model types:
    // - Would help with identifying UUID
    // - Would help with project ID lookup
    // - Would help with calculating resource usage
    //
    // I think we just need to validate that the model exists when we make these
    // calls? Maybe it could be an optional helper?

    /// Transitively updates all provisioned disk provisions from project -> fleet.
    pub async fn virtual_resource_provisioning_insert_disk(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        disk_byte_diff: i64,
    ) -> Result<Vec<VirtualResourceProvisioning>, Error> {
        let provisions = VirtualResourceProvisioningUpdate::new_insert_disk(
            id,
            disk_byte_diff,
            project_id,
        )
        .get_results_async(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;
        self.virtual_resource_provisioning_producer
            .append_disk_metrics(&provisions);
        Ok(provisions)
    }

    /// Transitively updates all provisioned disk provisions from project -> fleet.
    pub async fn virtual_resource_provisioning_delete_disk(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        disk_byte_diff: i64,
    ) -> Result<Vec<VirtualResourceProvisioning>, Error> {
        let provisions = VirtualResourceProvisioningUpdate::new_delete_disk(
            id,
            disk_byte_diff,
            project_id,
        )
        .get_results_async(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;
        self.virtual_resource_provisioning_producer
            .append_disk_metrics(&provisions);
        Ok(provisions)
    }

    /// Transitively updates all CPU/RAM provisions from project -> fleet.
    pub async fn virtual_resource_provisioning_insert_instance(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        cpus_diff: i64,
        ram_diff: i64,
    ) -> Result<Vec<VirtualResourceProvisioning>, Error> {
        let provisions =
            VirtualResourceProvisioningUpdate::new_insert_instance(
                id, cpus_diff, ram_diff, project_id,
            )
            .get_results_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        self.virtual_resource_provisioning_producer
            .append_cpu_metrics(&provisions);
        Ok(provisions)
    }

    /// Transitively updates all CPU/RAM provisions from project -> fleet.
    pub async fn virtual_resource_provisioning_delete_instance(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        cpus_diff: i64,
        ram_diff: i64,
    ) -> Result<Vec<VirtualResourceProvisioning>, Error> {
        let provisions =
            VirtualResourceProvisioningUpdate::new_delete_instance(
                id, cpus_diff, ram_diff, project_id,
            )
            .get_results_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        self.virtual_resource_provisioning_producer
            .append_cpu_metrics(&provisions);
        Ok(provisions)
    }
}
