// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`ResourceUsage`]s.

use super::DataStore;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::model::ResourceUsage;
use crate::db::queries::resource_usage_update::ResourceUsageUpdate;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use oximeter::{types::Sample, Metric, MetricsError, Target};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// A collection which holds resources (such as a project, organization, or
/// silo).
#[derive(Debug, Clone, Target)]
struct CollectionTarget {
    id: Uuid,
}

#[derive(Debug, Clone, Metric)]
struct PhysicalDiskSpaceProvisioned {
    #[datum]
    bytes_used: i64,
}

#[derive(Debug, Clone, Metric)]
struct CpusProvisioned {
    #[datum]
    cpus: i64,
}

#[derive(Debug, Default, Clone)]
pub struct Producer {
    samples: Arc<Mutex<Vec<Sample>>>,
}

impl Producer {
    pub fn new() -> Self {
        Self { samples: Arc::new(Mutex::new(vec![])) }
    }

    fn append_disk_metrics(&self, usages: &Vec<ResourceUsage>) {
        let new_samples = usages
            .iter()
            .map(|usage| {
                Sample::new(
                    &CollectionTarget { id: usage.id },
                    &PhysicalDiskSpaceProvisioned {
                        bytes_used: usage.physical_disk_bytes_provisioned,
                    },
                )
            })
            .collect::<Vec<_>>();

        self.append(new_samples);
    }

    fn append_cpu_metrics(&self, usages: &Vec<ResourceUsage>) {
        let new_samples = usages
            .iter()
            .map(|usage| {
                Sample::new(
                    &CollectionTarget { id: usage.id },
                    &CpusProvisioned { cpus: usage.cpus_provisioned },
                )
            })
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
    /// Create a resource_usage
    pub async fn resource_usage_create(
        &self,
        opctx: &OpContext,
        resource_usage: ResourceUsage,
    ) -> Result<Vec<ResourceUsage>, Error> {
        use db::schema::resource_usage::dsl;

        let usages: Vec<ResourceUsage> =
            diesel::insert_into(dsl::resource_usage)
                .values(resource_usage)
                .on_conflict_do_nothing()
                .get_results_async(self.pool_authorized(opctx).await?)
                .await
                .map_err(|e| {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
                })?;
        self.resource_usage_producer.append_disk_metrics(&usages);
        self.resource_usage_producer.append_cpu_metrics(&usages);
        Ok(usages)
    }

    pub async fn resource_usage_get(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<ResourceUsage, Error> {
        use db::schema::resource_usage::dsl;

        let resource_usage = dsl::resource_usage
            .find(id)
            .select(ResourceUsage::as_select())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        Ok(resource_usage)
    }

    /// Delete a resource_usage
    pub async fn resource_usage_delete(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> DeleteResult {
        use db::schema::resource_usage::dsl;

        diesel::delete(dsl::resource_usage)
            .filter(dsl::id.eq(id))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        Ok(())
    }

    pub async fn resource_usage_update_disk(
        &self,
        opctx: &OpContext,
        project_id: Uuid,
        disk_byte_diff: i64,
    ) -> Result<Vec<ResourceUsage>, Error> {
        let usages =
            ResourceUsageUpdate::new_update_disk(project_id, disk_byte_diff)
                .get_results_async(self.pool_authorized(opctx).await?)
                .await
                .map_err(|e| {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
                })?;
        self.resource_usage_producer.append_disk_metrics(&usages);
        Ok(usages)
    }

    pub async fn resource_usage_update_cpus(
        &self,
        opctx: &OpContext,
        project_id: Uuid,
        cpus_diff: i64,
    ) -> Result<Vec<ResourceUsage>, Error> {
        let usages =
            ResourceUsageUpdate::new_update_cpus(project_id, cpus_diff)
                .get_results_async(self.pool_authorized(opctx).await?)
                .await
                .map_err(|e| {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
                })?;
        self.resource_usage_producer.append_cpu_metrics(&usages);
        Ok(usages)
    }
}
