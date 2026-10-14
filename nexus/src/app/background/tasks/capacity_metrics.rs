// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task that collects and emits silo quota metrics:
//!
//! * silo_quota:{memory,cpu,disk}_capacity: resource limits set on the silo.
//! * silo_quota:{memory,cpu,disk}_provisioned: resources charged against the
//!   silo.
//!
//! Note: because background tasks run on each Nexus instance, these metrics are
//! emitted once for each Nexus--as of this writing, in triplicate. Callers
//! should take the mean or maximum datum value over Nexus instance IDs to
//! deduplicate.
//!
//! Note: the `silo_quota` metrics are related to, but distinct from, the
//! `collection_target` metric. The `silo_quota` metrics are emitted on a
//! regular interval, and labeled by silo ID. Meanwhile, `collection_target` is
//! emitted when silo utilization changes, resulting in a sparse timeseries, and
//! only covers utilization. Users who wish to show quota utilization ratios
//! (i.e. utilization divided by capacity), or who require regular timeseries,
//! should use the `silo_quota` metrics.

use crate::app::background::BackgroundTask;
use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_auth::context::OpContext;
use nexus_db_queries::db::DataStore;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use oximeter::Sample;
use oximeter::types::ProducerRegistry;
use std::sync::{Arc, Mutex};

oximeter::use_timeseries!("silo-quota.toml");

use silo_quota::SiloQuota;

pub(crate) struct CapacityMetrics {
    datastore: Arc<DataStore>,
    metrics: Arc<Mutex<metrics::Metrics>>,
    nexus_id: OmicronZoneUuid,
}

impl CapacityMetrics {
    pub(crate) fn new(
        datastore: Arc<DataStore>,
        producer_registry: &ProducerRegistry,
        nexus_id: OmicronZoneUuid,
    ) -> Self {
        let metrics = Arc::new(Mutex::new(metrics::Metrics::default()));
        producer_registry
            .register_producer(metrics::Producer(metrics.clone()))
            .unwrap();
        Self { datastore, metrics, nexus_id }
    }

    async fn fetch_samples(
        &self,
        opctx: &OpContext,
        nexus_id: OmicronZoneUuid,
    ) -> anyhow::Result<usize> {
        let now = chrono::Utc::now();
        let nexus_id = nexus_id.into_untyped_uuid();

        let silo_utils = self
            .datastore
            .silo_utilization_list(
                opctx,
                &PaginatedBy::Id(DataPageParams::max_page()),
            )
            .await?;

        let mut metrics = self.metrics.lock().unwrap();
        let mut sample_count = 0;
        for util in silo_utils {
            let target = SiloQuota { silo_id: util.silo_id, nexus_id };
            let samples = vec![
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
                        datum: util.cpus_allocated.cast_unsigned(),
                    },
                )?,
                Sample::new_with_timestamp(
                    now,
                    &target,
                    &silo_quota::CpuProvisioned {
                        datum: util.cpus_provisioned.cast_unsigned(),
                    },
                )?,
                Sample::new_with_timestamp(
                    now,
                    &target,
                    &silo_quota::DiskCapacity {
                        datum: util.storage_allocated.to_bytes(),
                    },
                )?,
                Sample::new_with_timestamp(
                    now,
                    &target,
                    &silo_quota::DiskProvisioned {
                        datum: util.storage_provisioned.to_bytes(),
                    },
                )?,
            ];
            sample_count += samples.len();
            metrics.push_samples(samples);
        }
        Ok(sample_count)
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
                    serde_json::json!({"samples_queued": sample_count})
                }
                Err(error) => {
                    serde_json::json!({"error": format!("{error:#}")})
                }
            }
        }
        .boxed()
    }
}

mod metrics {
    use std::sync::{Arc, Mutex};

    use oximeter::{MetricsError, Sample};

    #[derive(Debug)]
    pub(super) struct Producer(pub(super) Arc<Mutex<Metrics>>);

    #[derive(Debug, Default)]
    pub(super) struct Metrics {
        samples: Vec<Sample>,
    }

    impl Metrics {
        pub(crate) fn push_samples(&mut self, samples: Vec<Sample>) {
            self.samples.extend(samples);
        }
    }

    impl oximeter::Producer for Producer {
        fn produce(
            &mut self,
        ) -> Result<Box<dyn Iterator<Item = Sample>>, MetricsError> {
            let samples = std::mem::take(&mut self.0.lock().unwrap().samples);
            Ok(Box::new(samples.into_iter()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_auth::{authn, authz};
    use nexus_db_lookup::LookupPath;
    use nexus_db_model::DnsGroup;
    use nexus_db_model::Project;
    use nexus_db_queries::db::datastore::DnsVersionUpdateBuilder;
    use nexus_test_utils_macros::nexus_test;
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
        let setup_opctx = OpContext::for_background(
            opctx.log.clone(),
            Arc::new(authz::Authz::new(&opctx.log)),
            authn::Context::internal_db_init(),
            datastore.clone(),
        );
        let (authz_silo,) = LookupPath::new(&setup_opctx, datastore)
            .silo_id(populated_silo)
            .lookup_for(authz::Action::CreateChild)
            .await
            .unwrap();
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
        let (_, project) = datastore
            .project_create_in_silo(&setup_opctx, project, &authz_silo)
            .await
            .unwrap();
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
        for (silo_id, kind, cpu, memory, disk) in [
            (populated_silo, "capacity", 32, 128, 1024),
            (populated_silo, "provisioned", 4, 16, 100),
            (empty_silo, "capacity", 8, 32, 256),
            (empty_silo, "provisioned", 0, 0, 0),
        ] {
            for (metric, value) in [
                ("cpu", cpu),
                ("memory", ByteCount::from_gibibytes_u32(memory).to_bytes()),
                ("disk", ByteCount::from_gibibytes_u32(disk).to_bytes()),
            ] {
                expected.insert(
                    (silo_id, format!("silo_quota:{metric}_{kind}")),
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
