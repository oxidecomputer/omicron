// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task that collects and emits silo quota metrics:
//!
//! * silo_quota:{memory,cpu,virtual_storage}_capacity: resource limits set on
//!   the silo.
//! * silo_quota:{memory,cpu,virtual_storage}_provisioned: resources charged
//!   against the silo.
//!
//! Note: because background tasks run on each Nexus instance, these metrics are
//! emitted once for each Nexus--as of this writing, in triplicate. Callers
//! should take the mean or maximum datum value over Nexus instance IDs to
//! deduplicate.
//!
//! Note: the `silo_quota` metrics are related to, but distinct from, the
//! `collection_target` metric. The `silo_quota` metrics are emitted on a regular
//! interval, and labeled by silo ID. Meanwhile, `collection_target` is currently
//! emitted only when silo utilization changes, resulting in a sparse timeseries,
//! and only covers utilization. Users who wish to show quota utilization ratios
//! (i.e. utilization divided by capacity), or who require regular timeseries,
//! should use the `silo_quota` metrics.

use crate::app::background::BackgroundTask;
use dropshot::PaginationOrder;
use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_auth::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::internal_api::background::CapacityMetricsStatus;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use oximeter::MetricsError;
use oximeter::Sample;
use oximeter::queue::BoundedQueue;
use oximeter::types::ProducerRegistry;
use std::sync::{Arc, Mutex};

oximeter::use_timeseries!("silo-quota.toml");

use silo_quota::SiloQuota;

const QUEUE_SIZE: usize = 8192;

pub(crate) struct CapacityMetrics {
    datastore: Arc<DataStore>,
    metrics: Arc<Mutex<BoundedQueue<Sample>>>,
    nexus_id: OmicronZoneUuid,
}

impl CapacityMetrics {
    pub(crate) fn new(
        datastore: Arc<DataStore>,
        producer_registry: &ProducerRegistry,
        nexus_id: OmicronZoneUuid,
    ) -> Self {
        let metrics = Arc::new(Mutex::new(BoundedQueue::new(QUEUE_SIZE)));
        producer_registry.register_producer(Producer(metrics.clone())).unwrap();
        Self { datastore, metrics, nexus_id }
    }

    async fn fetch_samples(
        &self,
        opctx: &OpContext,
        nexus_id: OmicronZoneUuid,
    ) -> anyhow::Result<usize> {
        let now = chrono::Utc::now();
        let nexus_id = nexus_id.into_untyped_uuid();

        let mut paginator =
            Paginator::new(SQL_BATCH_SIZE, PaginationOrder::Ascending);
        let mut samples = Vec::<Sample>::new();

        while let Some(p) = paginator.next() {
            let silo_utils = self
                .datastore
                .silo_utilization_list(
                    opctx,
                    &PaginatedBy::Id(p.current_pagparams()),
                )
                .await?;
            paginator = p.found_batch(&silo_utils, &|util| {
                util.silo_id().into_untyped_uuid()
            });

            for util in silo_utils {
                let target = SiloQuota {
                    silo_id: util.silo_id().into_untyped_uuid(),
                    nexus_id,
                };
                samples.extend([
                    Sample::new_with_timestamp(
                        now,
                        &target,
                        &silo_quota::MemoryCapacity {
                            datum: util.memory_allocated.to_bytes(),
                        },
                    )?,
                    Sample::new_with_timestamp(
                        now,
                        &target,
                        &silo_quota::MemoryProvisioned {
                            datum: util.memory_provisioned.to_bytes(),
                        },
                    )?,
                    Sample::new_with_timestamp(
                        now,
                        &target,
                        &silo_quota::CpuCapacity {
                            datum: u64::try_from(util.cpus_allocated)?,
                        },
                    )?,
                    Sample::new_with_timestamp(
                        now,
                        &target,
                        &silo_quota::CpuProvisioned {
                            datum: u64::try_from(util.cpus_provisioned)?,
                        },
                    )?,
                    Sample::new_with_timestamp(
                        now,
                        &target,
                        &silo_quota::VirtualStorageCapacity {
                            datum: util.storage_allocated.to_bytes(),
                        },
                    )?,
                    Sample::new_with_timestamp(
                        now,
                        &target,
                        &silo_quota::VirtualStorageProvisioned {
                            datum: util.storage_provisioned.to_bytes(),
                        },
                    )?,
                ]);
            }
        }

        let mut metrics = self.metrics.lock().unwrap();
        let n_new_samples = samples.len();
        let n_current_samples = metrics.len();
        let n_dropped_samples = metrics.extend(samples);
        if n_dropped_samples > 0 {
            slog::warn!(
                opctx.log,
                "sample queue is too full, dropping oldest samples";
                "n_new_samples" => n_new_samples,
                "n_current_samples" => n_current_samples,
                "n_dropped_samples" => n_dropped_samples,
            );
        }

        Ok(n_new_samples.min(QUEUE_SIZE))
    }
}

impl BackgroundTask for CapacityMetrics {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async move {
            match self.fetch_samples(opctx, self.nexus_id).await {
                Ok(sample_count) => {
                    serde_json::json!(CapacityMetricsStatus {
                        samples_queued: sample_count,
                    })
                }
                Err(error) => {
                    serde_json::json!({"error": format!("{error:#}")})
                }
            }
        }
        .boxed()
    }
}

#[derive(Debug)]
struct Producer(Arc<Mutex<BoundedQueue<Sample>>>);

impl oximeter::Producer for Producer {
    fn produce(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Sample>>, MetricsError> {
        let samples = self.0.lock().unwrap().drain();
        Ok(Box::new(samples.into_iter()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_auth::{authn, authz};
    use nexus_db_model::DnsGroup;
    use nexus_db_model::Project;
    use nexus_db_queries::db::datastore::DnsVersionUpdateBuilder;
    use nexus_test_utils::http_testing::AuthnMode;
    use nexus_test_utils::resource_helpers::{
        create_local_user, grant_iam, test_params,
    };
    use nexus_test_utils_macros::nexus_test;
    use nexus_types::external_api::policy::SiloRole;
    use nexus_types::external_api::project::ProjectCreate;
    use nexus_types::external_api::silo::{
        SiloCreate, SiloIdentityMode, SiloQuotasCreate,
    };
    use nexus_types::identity::Resource;
    use omicron_common::api::external::{
        ByteCount, IdentityMetadataCreateParams,
    };
    use omicron_uuid_kinds::InstanceUuid;
    use oximeter::types::{Datum, FieldValue, ProducerResultsItem};
    use std::collections::BTreeMap;
    use uuid::Uuid;

    type ControlPlaneTestContext =
        nexus_test_utils::ControlPlaneTestContext<crate::Server>;

    fn collect_samples(registry: &ProducerRegistry) -> Vec<Sample> {
        let mut results = registry.collect();
        assert_eq!(results.len(), 1);
        match results.pop().unwrap() {
            ProducerResultsItem::Ok(samples) => samples,
            ProducerResultsItem::Err(error) => {
                panic!("produce failed: {error}")
            }
        }
    }

    // Integration test for the CapacityMetrics task. Create silos, projects,
    // and utilization records; collect samples from the producer registry;
    // and assert the expected samples.
    #[nexus_test(server = crate::Server)]
    async fn test_capacity_metrics(cptestctx: &ControlPlaneTestContext) {
        let datastore = cptestctx.server.server_context().nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        let mut silos = Vec::new();
        for (name, cpus, memory, storage) in
            [("populated", 32, 128, 1024), ("empty", 8, 32, 256)]
        {
            let silo = datastore
                .silo_create(
                    &opctx,
                    &opctx,
                    SiloCreate {
                        identity: IdentityMetadataCreateParams {
                            name: name.parse().unwrap(),
                            description: "Capacity metrics fixture".into(),
                        },
                        quotas: SiloQuotasCreate {
                            cpus,
                            memory: ByteCount::from_gibibytes_u32(memory),
                            storage: ByteCount::from_gibibytes_u32(storage),
                        },
                        identity_mode: SiloIdentityMode::LocalOnly,
                        admin_group_name: None,
                        tls_certificates: vec![],
                        mapped_fleet_roles: Default::default(),
                    },
                    &[],
                    DnsVersionUpdateBuilder::new(
                        DnsGroup::External,
                        "capacity metrics fixture".into(),
                        "test_capacity_metrics".into(),
                    ),
                )
                .await
                .unwrap();
            silos.push(silo);
        }
        let populated_silo = silos[0].id();
        let empty_silo = silos[1].id();
        let client = &cptestctx.external_client;
        let admin_user = create_local_user(
            client,
            &silos[0].clone().try_into().unwrap(),
            &"admin".parse().unwrap(),
            test_params::UserPassword::LoginDisallowed,
        )
        .await;
        grant_iam(
            client,
            &format!("/v1/system/silos/{populated_silo}"),
            SiloRole::Admin,
            admin_user.id,
            AuthnMode::PrivilegedUser,
        )
        .await;
        let setup_opctx = OpContext::for_background(
            opctx.log.clone(),
            Arc::new(authz::Authz::new(&opctx.log)),
            authn::Context::for_test_user(
                admin_user.id,
                populated_silo,
                authn::SiloAuthnPolicy::try_from(&silos[0]).unwrap(),
            ),
            datastore.clone(),
        );
        let project = Project::new(
            populated_silo,
            ProjectCreate {
                identity: IdentityMetadataCreateParams {
                    name: "capacity-metrics-project".parse().unwrap(),
                    description: "Capacity metrics fixture".into(),
                },
                defaults: None,
            },
        );
        let (_, project) =
            datastore.project_create(&setup_opctx, project).await.unwrap();
        let project_id = project.id();

        datastore
            .virtual_provisioning_collection_insert_instance(
                &opctx,
                InstanceUuid::new_v4(),
                project_id,
                4,
                ByteCount::from_gibibytes_u32(16).into(),
            )
            .await
            .unwrap();

        let registry = ProducerRegistry::new();
        let nexus_id = OmicronZoneUuid::new_v4();
        let mut task =
            CapacityMetrics::new(datastore.clone(), &registry, nexus_id);
        assert!(collect_samples(&registry).is_empty());

        datastore
            .virtual_provisioning_collection_insert_disk(
                &opctx,
                Uuid::new_v4(),
                project_id,
                ByteCount::from_gibibytes_u32(100).into(),
            )
            .await
            .unwrap();

        assert_eq!(
            task.activate(&opctx).await,
            serde_json::json!({"samples_queued": 12}),
        );
        let samples = collect_samples(&registry);
        assert_eq!(samples.len(), 12);

        let mut expected = BTreeMap::new();
        for (silo_id, kind, cpu, memory, storage) in [
            (populated_silo, "capacity", 32, 128, 1024),
            (populated_silo, "provisioned", 4, 16, 100),
            (empty_silo, "capacity", 8, 32, 256),
            (empty_silo, "provisioned", 0, 0, 0),
        ] {
            for (metric, value) in [
                ("cpu", cpu),
                ("memory", ByteCount::from_gibibytes_u32(memory).to_bytes()),
                (
                    "virtual_storage",
                    ByteCount::from_gibibytes_u32(storage).to_bytes(),
                ),
            ] {
                expected.insert(
                    (
                        silo_id.into_untyped_uuid(),
                        format!("silo_quota:{metric}_{kind}"),
                    ),
                    value,
                );
            }
        }

        for sample in samples {
            let fields = sample.sorted_target_fields();
            let FieldValue::Uuid(silo_id) = fields["silo_id"].value else {
                panic!("expected silo_id be a uuid");
            };
            let key = (silo_id, sample.timeseries_name.to_string());
            let value = expected.remove(&key).unwrap_or_else(|| {
                panic!("unexpected or duplicate sample: {key:?}")
            });
            assert_eq!(sample.measurement.datum(), &Datum::U64(value));
        }
        assert!(expected.is_empty(), "missing samples: {expected:?}");
        assert!(
            collect_samples(&registry).is_empty(),
            "produce must drain the buffer"
        );
    }
}
