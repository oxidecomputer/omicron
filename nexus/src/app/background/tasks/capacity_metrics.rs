// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Background task that collects and emits capacity metrics, as described in
//! silo-quota.toml, project-quota.toml, sled-capacity.toml, and
//! sled-reservation.toml.
//!
//! Note: because background tasks run on each Nexus instance, these metrics are
//! emitted once for each Nexus--as of this writing, in triplicate. Callers
//! should take the mean or maximum datum value over Nexus instance IDs to
//! deduplicate.
//!
//! Note: the `silo_quota` and `project_quota` metrics are related to, but
//! distinct from, the `collection_target` metric. The quota metrics are emitted
//! on a regular interval, and labeled by silo or project ID. Meanwhile,
//! `collection_target` is currently emitted only when silo utilization changes,
//! resulting in a sparse timeseries, and only covers utilization. Users who
//! wish to show quota utilization ratios (i.e. utilization divided by
//! capacity), or who require regular timeseries, should use the quota metrics.

use crate::app::background::BackgroundTask;
use dropshot::PaginationOrder;
use futures::FutureExt;
use futures::future::BoxFuture;
use nexus_auth::context::OpContext;
use nexus_db_model::Resources;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::deployment::SledFilter;
use nexus_types::identity::Asset;
use nexus_types::internal_api::background::CapacityMetricsStatus;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use oximeter::MetricsError;
use oximeter::Sample;
use oximeter::queue::BoundedQueue;
use oximeter::types::ProducerRegistry;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

oximeter::use_timeseries!("silo-quota.toml");
oximeter::use_timeseries!("project-quota.toml");
oximeter::use_timeseries!("sled-capacity.toml");
oximeter::use_timeseries!("sled-reservation.toml");

use project_quota::ProjectQuota;
use silo_quota::SiloQuota;
use sled_capacity::SledCapacity;
use sled_reservation::SledReservation;

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
    ) -> CapacityMetricsStatus {
        let now = chrono::Utc::now();
        let nexus_id = nexus_id.into_untyped_uuid();

        let mut status = CapacityMetricsStatus {
            silo_samples: 0,
            project_samples: 0,
            sled_samples: 0,
        };

        match self.fetch_silo_samples(opctx, nexus_id, now).await {
            Ok(samples) => {
                status.silo_samples = samples.len();
                self.enqueue_samples(opctx, samples);
            }
            Err(error) => {
                slog::warn!(opctx.log, "failed to collect silo metrics"; "error" => format!("{error:#}"));
            }
        };
        match self.fetch_project_samples(opctx, nexus_id, now).await {
            Ok(samples) => {
                status.project_samples = samples.len();
                self.enqueue_samples(opctx, samples);
            }
            Err(error) => {
                slog::warn!(opctx.log, "failed to collect project metrics"; "error" => format!("{error:#}"));
            }
        };
        match self.fetch_sled_samples(opctx, nexus_id, now).await {
            Ok(samples) => {
                status.sled_samples = samples.len();
                self.enqueue_samples(opctx, samples);
            }
            Err(error) => {
                slog::warn!(opctx.log, "failed to collect sled capacity/reservation metrics"; "error" => format!("{error:#}"));
            }
        }

        status
    }

    async fn fetch_sled_samples(
        &self,
        opctx: &OpContext,
        nexus_id: Uuid,
        now: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<Vec<Sample>> {
        let mut samples = Vec::<Sample>::new();

        let sleds = self
            .datastore
            .sled_list_all_batched(opctx, SledFilter::InService)
            .await?;
        let sled_ids: Vec<SledUuid> =
            sleds.iter().map(|sled| sled.id()).collect();
        let reservations: HashMap<SledUuid, Resources> = self
            .datastore
            .sled_vmm_reservation_list(opctx, &sled_ids)
            .await?
            .into_iter()
            .map(|r| (r.sled_id.into(), r.resources))
            .collect();

        for sled in sleds {
            // Produce sled capacity metrics.
            let sled_target = SledCapacity {
                nexus_id,
                rack_id: sled.rack_id().into_untyped_uuid(),
                sled_id: sled.id().into_untyped_uuid(),
                sled_model: sled.part_number().to_string().into(),
                sled_revision: sled.revision(),
                sled_serial: sled.serial_number().to_string().into(),
                sled_cpu_family: sled.cpu_family.as_str().into(),
            };
            samples.extend([
                Sample::new_with_timestamp(
                    now,
                    &sled_target,
                    &sled_capacity::HardwareThreads {
                        datum: u64::from(*sled.usable_hardware_threads),
                    },
                )?,
                Sample::new_with_timestamp(
                    now,
                    &sled_target,
                    &sled_capacity::PhysicalRam {
                        datum: sled.usable_physical_ram.to_bytes(),
                    },
                )?,
                Sample::new_with_timestamp(
                    now,
                    &sled_target,
                    &sled_capacity::ReservoirRam {
                        datum: sled.reservoir_size.to_bytes(),
                    },
                )?,
            ]);

            // Produce sled vmm reservation metrics, emitting zeros if the sled
            // doesn't appear in the aggregated reservation list.
            let (hardware_threads, reservoir_ram) =
                match reservations.get(&sled.id()) {
                    Some(reservation) => (
                        u64::from(*reservation.hardware_threads),
                        reservation.reservoir_ram.to_bytes(),
                    ),
                    None => (0, 0),
                };
            let reservation_target = SledReservation {
                nexus_id,
                rack_id: sled.rack_id().into_untyped_uuid(),
                sled_id: sled.id().into_untyped_uuid(),
                sled_model: sled.part_number().to_string().into(),
                sled_revision: sled.revision(),
                sled_serial: sled.serial_number().to_string().into(),
                sled_cpu_family: sled.cpu_family.as_str().into(),
            };
            samples.extend([
                Sample::new_with_timestamp(
                    now,
                    &reservation_target,
                    &sled_reservation::HardwareThreads {
                        datum: hardware_threads,
                    },
                )?,
                Sample::new_with_timestamp(
                    now,
                    &reservation_target,
                    &sled_reservation::ReservoirRam { datum: reservoir_ram },
                )?,
            ]);
        }

        Ok(samples)
    }

    async fn fetch_silo_samples(
        &self,
        opctx: &OpContext,
        nexus_id: Uuid,
        now: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<Vec<Sample>> {
        let mut samples = Vec::<Sample>::new();

        let mut paginator =
            Paginator::new(SQL_BATCH_SIZE, PaginationOrder::Ascending);
        while let Some(p) = paginator.next() {
            let utils = self
                .datastore
                .silo_utilization_list(
                    opctx,
                    &PaginatedBy::Id(p.current_pagparams()),
                )
                .await?;
            paginator = p.found_batch(&utils, &|util| {
                util.silo_id().into_untyped_uuid()
            });

            for util in utils {
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

                match u64::try_from(util.cpus_allocated) {
                    Ok(datum) => samples.push(Sample::new_with_timestamp(
                        now,
                        &target,
                        &silo_quota::CpuCapacity { datum },
                    )?),
                    Err(error) => slog::warn!(
                        opctx.log,
                        "invalid CPU count; skipping metric sample";
                        "silo_id" => %util.silo_id,
                        "value" => util.cpus_allocated,
                        "error" => %error,
                    ),
                }

                match u64::try_from(util.cpus_provisioned) {
                    Ok(datum) => samples.push(Sample::new_with_timestamp(
                        now,
                        &target,
                        &silo_quota::CpuProvisioned { datum },
                    )?),
                    Err(error) => slog::warn!(
                        opctx.log,
                        "invalid CPU count; skipping metric sample";
                        "silo_id" => %util.silo_id,
                        "value" => util.cpus_provisioned,
                        "error" => %error,
                    ),
                }
            }
        }

        Ok(samples)
    }

    async fn fetch_project_samples(
        &self,
        opctx: &OpContext,
        nexus_id: Uuid,
        now: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<Vec<Sample>> {
        let mut samples = Vec::<Sample>::new();

        let mut paginator =
            Paginator::new(SQL_BATCH_SIZE, PaginationOrder::Ascending);
        while let Some(p) = paginator.next() {
            let utils = self
                .datastore
                .project_utilization_list(opctx, &p.current_pagparams())
                .await?;
            paginator = p.found_batch(&utils, &|util| util.project_id());

            for util in utils {
                let target = ProjectQuota {
                    project_id: util.project_id().into_untyped_uuid(),
                    silo_id: util.silo_id().into_untyped_uuid(),
                    nexus_id,
                };
                samples.extend([
                    Sample::new_with_timestamp(
                        now,
                        &target,
                        &project_quota::MemoryProvisioned {
                            datum: util.memory_provisioned.to_bytes(),
                        },
                    )?,
                    Sample::new_with_timestamp(
                        now,
                        &target,
                        &project_quota::VirtualStorageProvisioned {
                            datum: util.storage_provisioned.to_bytes(),
                        },
                    )?,
                ]);

                match u64::try_from(util.cpus_provisioned) {
                    Ok(datum) => samples.push(Sample::new_with_timestamp(
                        now,
                        &target,
                        &project_quota::CpuProvisioned { datum },
                    )?),
                    Err(error) => slog::warn!(
                        opctx.log,
                        "invalid CPU count; skipping metric sample";
                        "project_id" => %util.project_id,
                        "value" => util.cpus_provisioned,
                        "error" => %error,
                    ),
                }
            }
        }

        Ok(samples)
    }

    fn enqueue_samples(&self, opctx: &OpContext, samples: Vec<Sample>) {
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
    }
}

impl BackgroundTask for CapacityMetrics {
    fn activate<'a>(
        &'a mut self,
        opctx: &'a OpContext,
    ) -> BoxFuture<'a, serde_json::Value> {
        async move {
            let status = self.fetch_samples(opctx, self.nexus_id).await;
            serde_json::json!(status)
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
    use async_bb8_diesel::AsyncRunQueryDsl;
    use nexus_auth::{authn, authz};
    use nexus_db_model::DnsGroup;
    use nexus_db_model::Project;
    use nexus_db_model::SledCpuFamily;
    use nexus_db_model::SledResourceVmm;
    use nexus_db_model::SledResourceVmmState;
    use nexus_db_queries::db::datastore::DnsVersionUpdateBuilder;
    use nexus_db_queries::db::pub_test_utils::helpers::SledUpdateBuilder;
    use nexus_db_schema::schema::sled_resource_vmm;
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
    use omicron_uuid_kinds::PropolisUuid;
    use oximeter::types::{Datum, FieldValue, ProducerResultsItem};
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
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

    // Integration test for quota capacity metrics. Create silos, projects,
    // and utilization records; collect samples from the producer registry;
    // and assert the expected samples.
    #[nexus_test(server = crate::Server)]
    async fn test_quota_metrics(cptestctx: &ControlPlaneTestContext) {
        let datastore = cptestctx.server.server_context().nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Insert silo records.
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

        // Insert project records, first provisioning a local user for the
        // operation, since we don't have a public method for creating projects
        // in a specified silo.
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

        // Insert quota utilization records.
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
        datastore
            .virtual_provisioning_collection_insert_disk(
                &opctx,
                Uuid::new_v4(),
                project_id,
                ByteCount::from_gibibytes_u32(100).into(),
            )
            .await
            .unwrap();

        // Produce metrics.
        let registry = ProducerRegistry::new();
        let mut task = CapacityMetrics::new(
            datastore.clone(),
            &registry,
            OmicronZoneUuid::new_v4(),
        );
        assert!(collect_samples(&registry).is_empty());

        task.activate(&opctx).await;

        // Assert expected samples.
        let samples: Vec<_> = collect_samples(&registry)
            .into_iter()
            .filter(|sample| {
                matches!(sample.target_name(), "silo_quota" | "project_quota")
            })
            .collect();
        assert_eq!(samples.len(), 15);

        let mut expected = BTreeMap::new();
        for (silo_id, project_id, kind, cpu, memory, storage) in [
            (populated_silo, None, "capacity", 32, 128, 1024),
            (populated_silo, None, "provisioned", 4, 16, 100),
            (empty_silo, None, "capacity", 8, 32, 256),
            (empty_silo, None, "provisioned", 0, 0, 0),
            (
                populated_silo,
                Some(project_id.into_untyped_uuid()),
                "provisioned",
                4,
                16,
                100,
            ),
        ] {
            for (metric, value) in [
                ("cpu", cpu),
                ("memory", ByteCount::from_gibibytes_u32(memory).to_bytes()),
                (
                    "virtual_storage",
                    ByteCount::from_gibibytes_u32(storage).to_bytes(),
                ),
            ] {
                let target = if project_id.is_some() {
                    "project_quota"
                } else {
                    "silo_quota"
                };
                expected.insert(
                    (
                        silo_id.into_untyped_uuid(),
                        project_id,
                        format!("{target}:{metric}_{kind}"),
                    ),
                    Datum::U64(value),
                );
            }
        }

        let actual: BTreeMap<_, _> = samples
            .iter()
            .map(|sample| {
                let fields = sample.sorted_target_fields();
                let FieldValue::Uuid(silo_id) = fields["silo_id"].value else {
                    panic!("expected silo_id be a uuid");
                };
                let project_id = if sample.target_name() == "project_quota" {
                    let FieldValue::Uuid(project_id) =
                        fields["project_id"].value
                    else {
                        panic!("expected project_id to be a uuid");
                    };
                    Some(project_id)
                } else {
                    None
                };
                (
                    (silo_id, project_id, sample.timeseries_name.to_string()),
                    sample.measurement.datum().clone(),
                )
            })
            .collect();
        assert_eq!(actual, expected);
        assert!(
            collect_samples(&registry).is_empty(),
            "produce must drain the buffer"
        );
    }

    // Integration test for sled capacity metrics. Create sleds and vmms;
    // collect samples from the producer registry; and assert the expected
    // samples.
    #[nexus_test(server = crate::Server)]
    async fn test_sled_metrics(cptestctx: &ControlPlaneTestContext) {
        let datastore = cptestctx.server.server_context().nexus.datastore();
        let opctx = OpContext::for_tests(
            cptestctx.logctx.log.clone(),
            datastore.clone(),
        );

        // Insert sled and vmm records, including sleds with different
        // capacities, and a sled with no associated vmms.
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let mut expected = BTreeMap::new();
        for (cpu_family, threads, ram_gib, reservoir_gib, vmms) in [
            (
                SledCpuFamily::AmdMilan,
                32,
                256,
                128,
                vec![
                    (SledResourceVmmState::Active, 4, 1, 16),
                    (SledResourceVmmState::Tombstoned, 2, 1, 8),
                ],
            ),
            (
                SledCpuFamily::AmdTurin,
                64,
                512,
                256,
                vec![(SledResourceVmmState::Target, 2, 2, 8)],
            ),
            (SledCpuFamily::AmdTurinDense, 128, 1024, 512, vec![]),
        ] {
            let mut builder = SledUpdateBuilder::new();
            builder
                .hardware()
                .usable_hardware_threads(threads)
                .usable_physical_ram(i64::from(ram_gib) << 30)
                .reservoir_size(i64::from(reservoir_gib) << 30)
                .cpu_family(cpu_family);
            let sled_id =
                datastore.sled_upsert(builder.build()).await.unwrap().id();

            let (mut reserved_threads, mut reserved_reservoir_gib) = (0, 0);
            for (state, vmm_threads, rss_gib, vmm_reservoir_gib) in vmms {
                diesel::insert_into(sled_resource_vmm::table)
                    .values(SledResourceVmm::new(
                        PropolisUuid::new_v4(),
                        InstanceUuid::new_v4(),
                        sled_id,
                        Resources::new(
                            vmm_threads,
                            ByteCount::from_gibibytes_u32(rss_gib).into(),
                            ByteCount::from_gibibytes_u32(vmm_reservoir_gib)
                                .into(),
                        ),
                        state,
                    ))
                    .execute_async(&*conn)
                    .await
                    .unwrap();
                reserved_threads += vmm_threads;
                reserved_reservoir_gib += vmm_reservoir_gib;
            }

            for (metric, value) in [
                ("sled_capacity:hardware_threads", u64::from(threads)),
                (
                    "sled_capacity:physical_ram",
                    ByteCount::from_gibibytes_u32(ram_gib).to_bytes(),
                ),
                (
                    "sled_capacity:reservoir_ram",
                    ByteCount::from_gibibytes_u32(reservoir_gib).to_bytes(),
                ),
                (
                    "sled_reservation:hardware_threads",
                    u64::from(reserved_threads),
                ),
                (
                    "sled_reservation:reservoir_ram",
                    ByteCount::from_gibibytes_u32(reserved_reservoir_gib)
                        .to_bytes(),
                ),
            ] {
                expected.insert(
                    (sled_id.into_untyped_uuid(), metric.to_string()),
                    (
                        FieldValue::String(cpu_family.as_str().into()),
                        Datum::U64(value),
                    ),
                );
            }
        }

        // Produce metrics.
        let registry = ProducerRegistry::new();
        let mut task = CapacityMetrics::new(
            datastore.clone(),
            &registry,
            OmicronZoneUuid::new_v4(),
        );
        assert!(collect_samples(&registry).is_empty());

        task.activate(&opctx).await;

        // Assert expected samples.
        //
        // Note: the test harness includes a simulated sled that our test
        // doesn't know about. Filter samples to the sleds managed by this test
        // for explicitness.
        let sled_ids: BTreeSet<Uuid> =
            expected.keys().map(|(sled_id, _)| *sled_id).collect();
        let samples: Vec<_> = collect_samples(&registry)
            .into_iter()
            .filter(|sample| {
                matches!(
                    sample.target_name(),
                    "sled_capacity" | "sled_reservation"
                ) && matches!(
                    sample.sorted_target_fields()["sled_id"].value,
                    FieldValue::Uuid(sled_id) if sled_ids.contains(&sled_id)
                )
            })
            .collect();
        assert_eq!(samples.len(), 15);

        let actual: BTreeMap<_, _> = samples
            .iter()
            .map(|sample| {
                let fields = sample.sorted_target_fields();
                let FieldValue::Uuid(sled_id) = fields["sled_id"].value else {
                    panic!("expected sled_id to be a uuid");
                };
                (
                    (sled_id, sample.timeseries_name.to_string()),
                    (
                        fields["sled_cpu_family"].value.clone(),
                        sample.measurement.datum().clone(),
                    ),
                )
            })
            .collect();
        assert_eq!(actual, expected);
        assert!(
            collect_samples(&registry).is_empty(),
            "produce must drain the buffer"
        );
    }
}
