// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Task which ensures that expected Nexus services exist.

use super::interfaces::{
    DnsUpdaterInterface, NexusInterface, SledClientInterface,
};
use crate::context::OpContext;
use crate::db::datastore::DatasetRedundancy;
use crate::db::identity::Asset;
use crate::db::model::Dataset;
use crate::db::model::DatasetKind;
use crate::db::model::Service;
use crate::db::model::ServiceKind;
use crate::db::model::Sled;
use crate::db::model::Zpool;
use futures::stream::{self, StreamExt, TryStreamExt};
use omicron_common::address::{
    DnsSubnet, DNS_PORT, DNS_REDUNDANCY, DNS_SERVER_PORT,
};
use omicron_common::api::external::Error;
use sled_agent_client::types as SledAgentTypes;
use slog::Logger;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::net::{IpAddr, Ipv6Addr, SocketAddrV6};
use std::sync::Arc;

// Policy for the number of services to be provisioned.
#[derive(Debug)]
enum ServiceRedundancy {
    // This service must exist on at least this many sleds
    // within the rack.
    PerRack(u32),

    // This service must exist on all Scrimlets within the rack.
    AllScrimlets,

    // This service must exist on at least this many sleds
    // within the availability zone. Note that this is specific
    // for the DNS service, as some expectations surrounding
    // addressing are specific to that service.
    DnsPerAz(u32),
}

#[derive(Debug)]
struct ExpectedService {
    kind: ServiceKind,
    redundancy: ServiceRedundancy,
}

// TODO(https://github.com/oxidecomputer/omicron/issues/1276):
// Longer-term, when we integrate multi-rack support,
// it is expected that Nexus will manage multiple racks
// within the fleet, rather than simply per-rack services.
//
// When that happens, it is likely that many of the "per-rack"
// services will become "per-fleet", such as Nexus and CRDB.
const EXPECTED_SERVICES: [ExpectedService; 4] = [
    ExpectedService {
        kind: ServiceKind::InternalDNS,
        redundancy: ServiceRedundancy::DnsPerAz(DNS_REDUNDANCY),
    },
    ExpectedService {
        kind: ServiceKind::Nexus,
        redundancy: ServiceRedundancy::PerRack(1),
    },
    ExpectedService {
        kind: ServiceKind::Oximeter,
        redundancy: ServiceRedundancy::PerRack(1),
    },
    ExpectedService {
        kind: ServiceKind::Dendrite,
        redundancy: ServiceRedundancy::AllScrimlets,
    },
];

#[derive(Debug)]
struct ExpectedDataset {
    kind: DatasetKind,
    redundancy: DatasetRedundancy,
}

const EXPECTED_DATASETS: [ExpectedDataset; 3] = [
    ExpectedDataset {
        kind: DatasetKind::Crucible,
        redundancy: DatasetRedundancy::OnAll,
    },
    ExpectedDataset {
        kind: DatasetKind::Cockroach,
        // TODO(https://github.com/oxidecomputer/omicron/issues/727):
        // Update this to more than one.
        redundancy: DatasetRedundancy::PerRack(1),
    },
    ExpectedDataset {
        kind: DatasetKind::Clickhouse,
        redundancy: DatasetRedundancy::PerRack(1),
    },
];

/// Contains logic for balancing services within a fleet.
///
/// This struct operates on generic parameters to easily permit
/// dependency injection via testing, but in production, practically
/// operates on the same concrete types.
pub struct ServiceBalancer<D, N, S>
where
    D: DnsUpdaterInterface,
    N: NexusInterface<S>,
    S: SledClientInterface,
{
    log: Logger,
    nexus: Arc<N>,
    dns_updater: D,
    phantom: PhantomData<S>,
}

impl<D, N, S> ServiceBalancer<D, N, S>
where
    D: DnsUpdaterInterface,
    N: NexusInterface<S>,
    S: SledClientInterface,
{
    pub fn new(log: Logger, nexus: Arc<N>, dns_updater: D) -> Self {
        Self { log, nexus, dns_updater, phantom: PhantomData }
    }

    // Reaches out to all sled agents implied in "services", and
    // requests that the desired services are executing.
    async fn instantiate_services(
        &self,
        opctx: &OpContext,
        services: Vec<Service>,
    ) -> Result<(), Error> {
        let mut sled_ids = HashSet::new();
        for svc in &services {
            sled_ids.insert(svc.sled_id);
        }

        // For all sleds requiring an update, request all services be
        // instantiated.
        stream::iter(&sled_ids)
            .map(Ok::<_, Error>)
            .try_for_each_concurrent(None, |sled_id| async {
                // Query for all services that should be running on a Sled,
                // and notify Sled Agent about all of them.
                let services = self
                    .nexus
                    .datastore()
                    .service_list(opctx, *sled_id)
                    .await?;
                let sled_client = self.nexus.sled_client(sled_id).await?;

                info!(self.log, "instantiate_services: {:?}", services);

                sled_client
                    .services_put(&SledAgentTypes::ServiceEnsureBody {
                        services: services
                            .iter()
                            .map(|s| {
                                let address = Ipv6Addr::from(s.ip);
                                let (name, service_type) =
                                    Self::get_service_name_and_type(
                                        address, s.kind,
                                    );
                                let gz_addresses = match &s.kind {
                                    ServiceKind::InternalDNS => {
                                        vec![DnsSubnet::from_dns_address(
                                            address,
                                        )
                                        .gz_address()
                                        .ip()]
                                    }
                                    _ => vec![],
                                };

                                SledAgentTypes::ServiceRequest {
                                    id: s.id(),
                                    name,
                                    addresses: vec![address],
                                    gz_addresses,
                                    service_type,
                                }
                            })
                            .collect(),
                    })
                    .await?;
                Ok(())
            })
            .await?;

        let mut records = HashMap::new();
        for service in &services {
            records
                .entry(service.srv())
                .or_insert_with(Vec::new)
                .push((service.aaaa(), service.address()));
        }
        self.dns_updater
            .insert_dns_records(&records)
            .await
            .map_err(|e| Error::internal_error(&e.to_string()))?;

        Ok(())
    }

    // Translates (address, db kind) to Sled Agent client types.
    fn get_service_name_and_type(
        address: Ipv6Addr,
        kind: ServiceKind,
    ) -> (String, SledAgentTypes::ServiceType) {
        match kind {
            ServiceKind::Nexus => (
                "nexus".to_string(),
                SledAgentTypes::ServiceType::Nexus {
                    internal_ip: address,
                    // TODO: This is wrong! needs a separate address for Nexus
                    external_ip: IpAddr::V6(address),
                },
            ),
            ServiceKind::InternalDNS => (
                "internal-dns".to_string(),
                SledAgentTypes::ServiceType::InternalDns {
                    server_address: SocketAddrV6::new(
                        address,
                        DNS_SERVER_PORT,
                        0,
                        0,
                    )
                    .to_string(),
                    dns_address: SocketAddrV6::new(address, DNS_PORT, 0, 0)
                        .to_string(),
                },
            ),
            ServiceKind::Oximeter => {
                ("oximeter".to_string(), SledAgentTypes::ServiceType::Oximeter)
            }
            ServiceKind::Dendrite => (
                "dendrite".to_string(),
                SledAgentTypes::ServiceType::Dendrite {
                    asic: SledAgentTypes::DendriteAsic::TofinoStub,
                },
            ),
        }
    }

    async fn ensure_services_provisioned(
        &self,
        opctx: &OpContext,
        expected_services: &[ExpectedService],
    ) -> Result<(), Error> {
        // Provision services within the database.
        let mut svcs = vec![];
        for expected_svc in expected_services {
            info!(self.log, "Ensuring service {:?} exists", expected_svc);
            match expected_svc.redundancy {
                ServiceRedundancy::PerRack(desired_count) => {
                    svcs.extend_from_slice(
                        &self
                            .nexus
                            .datastore()
                            .ensure_rack_service(
                                opctx,
                                self.nexus.rack_id(),
                                expected_svc.kind,
                                desired_count,
                            )
                            .await?,
                    );
                }
                ServiceRedundancy::DnsPerAz(desired_count) => {
                    svcs.extend_from_slice(
                        &self
                            .nexus
                            .datastore()
                            .ensure_dns_service(
                                opctx,
                                self.nexus.rack_subnet(),
                                desired_count,
                            )
                            .await?,
                    );
                }
                ServiceRedundancy::AllScrimlets => {
                    svcs.extend_from_slice(
                        &self
                            .nexus
                            .datastore()
                            .ensure_scrimlet_service(
                                opctx,
                                self.nexus.rack_id(),
                                expected_svc.kind,
                            )
                            .await?,
                    );
                }
            }
        }

        // Ensure services exist on the target sleds.
        self.instantiate_services(opctx, svcs).await?;
        Ok(())
    }

    async fn ensure_rack_dataset(
        &self,
        opctx: &OpContext,
        kind: DatasetKind,
        redundancy: DatasetRedundancy,
    ) -> Result<(), Error> {
        // Provision the datasets within the database.
        let new_datasets = self
            .nexus
            .datastore()
            .ensure_rack_dataset(opctx, self.nexus.rack_id(), kind, redundancy)
            .await?;

        // Actually instantiate those datasets.
        self.instantiate_datasets(new_datasets, kind).await
    }

    // Reaches out to all sled agents implied in "services", and
    // requests that the desired services are executing.
    async fn instantiate_datasets(
        &self,
        datasets: Vec<(Sled, Zpool, Dataset)>,
        kind: DatasetKind,
    ) -> Result<(), Error> {
        if datasets.is_empty() {
            return Ok(());
        }

        // Ensure that there is one connection per sled.
        let mut sled_clients = HashMap::new();
        for (sled, _, _) in &datasets {
            if sled_clients.get(&sled.id()).is_none() {
                let sled_client = self.nexus.sled_client(&sled.id()).await?;
                sled_clients.insert(sled.id(), sled_client);
            }
        }

        // Issue all dataset instantiation requests concurrently.
        stream::iter(&datasets)
            .map(Ok::<_, Error>)
            .try_for_each_concurrent(None, |(sled, zpool, dataset)| async {
                let sled_client = sled_clients.get(&sled.id()).unwrap();

                let dataset_kind = match kind {
                    // TODO(https://github.com/oxidecomputer/omicron/issues/727):
                    // This set of "all addresses" isn't right. We'll need to
                    // deal with that before supporting multi-node CRDB.
                    DatasetKind::Cockroach => {
                        SledAgentTypes::DatasetKind::CockroachDb(vec![])
                    }
                    DatasetKind::Crucible => {
                        SledAgentTypes::DatasetKind::Crucible
                    }
                    DatasetKind::Clickhouse => {
                        SledAgentTypes::DatasetKind::Clickhouse
                    }
                };

                // Instantiate each dataset.
                sled_client
                    .filesystem_put(&SledAgentTypes::DatasetEnsureBody {
                        id: dataset.id(),
                        zpool_id: zpool.id(),
                        dataset_kind,
                        address: dataset.address().to_string(),
                    })
                    .await?;
                Ok(())
            })
            .await?;

        // Ensure all DNS records are updated for the created datasets.
        let mut records = HashMap::new();
        for (_, _, dataset) in &datasets {
            records
                .entry(dataset.srv())
                .or_insert_with(Vec::new)
                .push((dataset.aaaa(), dataset.address()));
        }
        self.dns_updater
            .insert_dns_records(&records)
            .await
            .map_err(|e| Error::internal_error(&e.to_string()))?;

        Ok(())
    }

    async fn ensure_datasets_provisioned(
        &self,
        opctx: &OpContext,
        expected_datasets: &[ExpectedDataset],
    ) -> Result<(), Error> {
        // Provision all dataset types concurrently.
        stream::iter(expected_datasets)
            .map(Ok::<_, Error>)
            .try_for_each_concurrent(None, |expected_dataset| async move {
                info!(
                    self.log,
                    "Ensuring dataset {:?} exists", expected_dataset
                );
                self.ensure_rack_dataset(
                    opctx,
                    expected_dataset.kind,
                    expected_dataset.redundancy,
                )
                .await?;
                Ok(())
            })
            .await
    }

    /// Provides a single point-in-time evaluation and adjustment of
    /// the services provisioned within the rack.
    ///
    /// May adjust the provisioned services to meet the redundancy of the
    /// rack, if necessary.
    // TODO: Consider using sagas to ensure the rollout of services.
    //
    // Not using sagas *happens* to be fine because these operations are
    // re-tried periodically, but that's kind forcing a dependency on the
    // caller.
    pub async fn balance_services(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        self.ensure_datasets_provisioned(opctx, &EXPECTED_DATASETS).await?;
        self.ensure_services_provisioned(opctx, &EXPECTED_SERVICES).await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::app::background::fakes::{FakeDnsUpdater, FakeNexus};
    use crate::db::datastore::DataStore;
    use crate::{authn, authz};
    use dropshot::test_util::LogContext;
    use internal_dns_client::names::{BackendName, AAAA, SRV};
    use nexus_test_utils::db::test_setup_database;
    use omicron_common::address::Ipv6Subnet;
    use omicron_common::api::external::ByteCount;
    use omicron_test_utils::dev;
    use std::sync::Arc;
    use uuid::Uuid;

    struct ProvisionTest {
        logctx: LogContext,
        opctx: OpContext,
        db: dev::db::CockroachInstance,
        datastore: Arc<DataStore>,
    }

    impl ProvisionTest {
        // Create the logger and setup the database.
        async fn new(name: &str) -> Self {
            let logctx = dev::test_setup_log(name);
            let db = test_setup_database(&logctx.log).await;
            let (_, datastore) =
                crate::db::datastore::datastore_test(&logctx, &db).await;
            let opctx = OpContext::for_background(
                logctx.log.new(o!()),
                Arc::new(authz::Authz::new(&logctx.log)),
                authn::Context::internal_service_balancer(),
                datastore.clone(),
            );
            Self { logctx, opctx, db, datastore }
        }

        async fn cleanup(mut self) {
            self.db.cleanup().await.unwrap();
            self.logctx.cleanup_successful();
        }
    }

    async fn create_test_sled(rack_id: Uuid, datastore: &DataStore) -> Uuid {
        let bogus_addr = SocketAddrV6::new(
            Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1),
            8080,
            0,
            0,
        );
        let sled_id = Uuid::new_v4();
        let is_scrimlet = false;
        let sled = Sled::new(sled_id, bogus_addr.clone(), is_scrimlet, rack_id);
        datastore.sled_upsert(sled).await.unwrap();
        sled_id
    }

    async fn create_test_zpool(datastore: &DataStore, sled_id: Uuid) -> Uuid {
        let zpool_id = Uuid::new_v4();
        let zpool = Zpool::new(
            zpool_id,
            sled_id,
            &crate::internal_api::params::ZpoolPutRequest {
                size: ByteCount::from_gibibytes_u32(10),
            },
        );
        datastore.zpool_upsert(zpool).await.unwrap();
        zpool_id
    }

    #[tokio::test]
    async fn test_provision_dataset_on_all_no_zpools() {
        let test =
            ProvisionTest::new("test_provision_dataset_on_all_no_zpools").await;

        let rack_subnet = Ipv6Subnet::new(Ipv6Addr::LOCALHOST);
        let nexus = FakeNexus::new(test.datastore.clone(), rack_subnet);
        let dns_updater = FakeDnsUpdater::new();
        let service_balancer = ServiceBalancer::new(
            test.logctx.log.clone(),
            nexus.clone(),
            dns_updater.clone(),
        );

        // Setup: One sled, no zpools.
        let sled_id = create_test_sled(nexus.rack_id(), &test.datastore).await;

        // Make the request to the service balancer for Crucibles on all Zpools.
        //
        // However, with no zpools, this is a no-op.
        let expected_datasets = [ExpectedDataset {
            kind: DatasetKind::Crucible,
            redundancy: DatasetRedundancy::OnAll,
        }];
        service_balancer
            .ensure_datasets_provisioned(&test.opctx, &expected_datasets)
            .await
            .unwrap();

        // Observe that nothing was requested at the sled.
        let sled = nexus.sled_client(&sled_id).await.unwrap();
        assert!(sled.service_requests().is_empty());
        assert!(sled.dataset_requests().is_empty());

        // Observe that no DNS records were updated.
        let records = dns_updater.records();
        assert!(records.is_empty());

        test.cleanup().await;
    }

    #[tokio::test]
    async fn test_provision_dataset_on_all_zpools() {
        let test =
            ProvisionTest::new("test_provision_dataset_on_all_zpools").await;

        let rack_subnet = Ipv6Subnet::new(Ipv6Addr::LOCALHOST);
        let nexus = FakeNexus::new(test.datastore.clone(), rack_subnet);
        let dns_updater = FakeDnsUpdater::new();
        let service_balancer = ServiceBalancer::new(
            test.logctx.log.clone(),
            nexus.clone(),
            dns_updater.clone(),
        );

        // Setup: One sled, multiple zpools
        let sled_id = create_test_sled(nexus.rack_id(), &test.datastore).await;
        const ZPOOL_COUNT: usize = 3;
        let mut zpools = vec![];
        for _ in 0..ZPOOL_COUNT {
            zpools.push(create_test_zpool(&test.datastore, sled_id).await);
        }

        // Make the request to the service balancer for Crucibles on all Zpools.
        let expected_datasets = [ExpectedDataset {
            kind: DatasetKind::Crucible,
            redundancy: DatasetRedundancy::OnAll,
        }];
        service_balancer
            .ensure_datasets_provisioned(&test.opctx, &expected_datasets)
            .await
            .unwrap();

        // Observe that datasets were requested on each zpool.
        let sled = nexus.sled_client(&sled_id).await.unwrap();
        assert!(sled.service_requests().is_empty());
        let dataset_requests = sled.dataset_requests();
        assert_eq!(ZPOOL_COUNT, dataset_requests.len());
        for request in &dataset_requests {
            assert!(
                zpools.contains(&request.zpool_id),
                "Dataset request for unexpected zpool"
            );
            assert!(matches!(
                request.dataset_kind,
                SledAgentTypes::DatasetKind::Crucible
            ));
        }

        // Observe that DNS records for each Crucible exist.
        let records = dns_updater.records();
        assert_eq!(ZPOOL_COUNT, records.len());
        for (srv, aaaas) in records {
            match srv {
                SRV::Backend(BackendName::Crucible, dataset_id) => {
                    let expected_address = dataset_requests
                        .iter()
                        .find_map(|request| {
                            if request.id == dataset_id {
                                Some(request.address.clone())
                            } else {
                                None
                            }
                        })
                        .unwrap();

                    assert_eq!(1, aaaas.len());
                    let (aaaa_name, dns_addr) = &aaaas[0];
                    assert_eq!(dns_addr.to_string(), expected_address);
                    if let AAAA::Zone(zone_id) = aaaa_name {
                        assert_eq!(
                            *zone_id, dataset_id,
                            "Expected AAAA UUID to match SRV record",
                        );
                    } else {
                        panic!(
                            "Expected AAAA record for Zone from {aaaa_name}"
                        );
                    }
                }
                _ => panic!("Unexpected SRV record"),
            }
        }

        test.cleanup().await;
    }

    // Observe that "per-rack" dataset provisions can be completed.
    //
    // This test uses multiple racks, and verifies that a provision occurs
    // on each one.
    #[tokio::test]
    async fn test_provision_dataset_per_rack() {
        let test = ProvisionTest::new("test_provision_dataset_per_rack").await;

        let rack_subnet = Ipv6Subnet::new(Ipv6Addr::LOCALHOST);
        let nexus = FakeNexus::new(test.datastore.clone(), rack_subnet);
        let dns_updater = FakeDnsUpdater::new();
        let service_balancer = ServiceBalancer::new(
            test.logctx.log.clone(),
            nexus.clone(),
            dns_updater.clone(),
        );

        // Setup: Create a couple sleds on the first rack, and create a third
        // sled on a "different rack".
        //
        // Each sled gets a single zpool.
        let mut zpools = vec![];

        let sled1_id = create_test_sled(nexus.rack_id(), &test.datastore).await;
        zpools.push(create_test_zpool(&test.datastore, sled1_id).await);

        let sled2_id = create_test_sled(nexus.rack_id(), &test.datastore).await;
        zpools.push(create_test_zpool(&test.datastore, sled2_id).await);

        let other_rack_id = Uuid::new_v4();
        let other_rack_sled_id =
            create_test_sled(other_rack_id, &test.datastore).await;
        zpools
            .push(create_test_zpool(&test.datastore, other_rack_sled_id).await);

        // Ask for one dataset per rack.
        let expected_datasets = [ExpectedDataset {
            kind: DatasetKind::Cockroach,
            redundancy: DatasetRedundancy::PerRack(1),
        }];
        service_balancer
            .ensure_datasets_provisioned(&test.opctx, &expected_datasets)
            .await
            .unwrap();

        // Observe that the datasets were requested on each rack.

        // Rack 1: One of the two sleds should have a dataset.
        let sled = nexus.sled_client(&sled1_id).await.unwrap();
        let requests1 = sled.dataset_requests();
        if !requests1.is_empty() {
            assert_eq!(1, requests1.len());
            assert_eq!(zpools[0], requests1[0].zpool_id);
        }
        let sled = nexus.sled_client(&sled2_id).await.unwrap();
        let requests2 = sled.dataset_requests();
        if !requests2.is_empty() {
            assert_eq!(1, requests2.len());
            assert_eq!(zpools[1], requests2[0].zpool_id);
        }
        assert!(
            requests1.is_empty() ^ requests2.is_empty(),
            "One of the sleds should have a dataset, the other should not"
        );

        // Rack 2: The sled should have a dataset.
        let sled = nexus.sled_client(&other_rack_sled_id).await.unwrap();
        let requests = sled.dataset_requests();
        // TODO(https://github.com/oxidecomputer/omicron/issues/1276):
        // We should see a request to the "other rack" when multi-rack
        // is supported.
        //
        // At the moment, however, all requests for service-balancing are
        // "rack-local".
        assert_eq!(0, requests.len());

        // We should be able to assert this when multi-rack is supported.
        // assert_eq!(zpools[2], requests[0].zpool_id);

        test.cleanup().await;
    }

    #[tokio::test]
    async fn test_provision_oximeter_service_per_rack() {
        let test =
            ProvisionTest::new("test_provision_oximeter_service_per_rack")
                .await;

        let rack_subnet = Ipv6Subnet::new(Ipv6Addr::LOCALHOST);
        let nexus = FakeNexus::new(test.datastore.clone(), rack_subnet);
        let dns_updater = FakeDnsUpdater::new();
        let service_balancer = ServiceBalancer::new(
            test.logctx.log.clone(),
            nexus.clone(),
            dns_updater.clone(),
        );

        // Setup: Create three sleds, with the goal of putting services on two
        // of them.
        const SLED_COUNT: u32 = 3;
        const SERVICE_COUNT: u32 = 2;
        let mut sleds = vec![];
        for _ in 0..SLED_COUNT {
            sleds
                .push(create_test_sled(nexus.rack_id(), &test.datastore).await);
        }
        let expected_services = [ExpectedService {
            kind: ServiceKind::Oximeter,
            redundancy: ServiceRedundancy::PerRack(SERVICE_COUNT),
        }];

        // Request the services
        service_balancer
            .ensure_services_provisioned(&test.opctx, &expected_services)
            .await
            .unwrap();

        // Observe the service on SERVICE_COUNT of the SLED_COUNT sleds.
        let mut observed_service_count = 0;
        for sled_id in &sleds {
            let sled = nexus.sled_client(&sled_id).await.unwrap();
            let requests = sled.service_requests();

            match requests.len() {
                0 => (), // Ignore the sleds where nothing was provisioned
                1 => {
                    assert_eq!(requests[0].name, "oximeter");
                    assert!(matches!(
                        requests[0].service_type,
                        SledAgentTypes::ServiceType::Oximeter
                    ));
                    assert!(requests[0].gz_addresses.is_empty());
                    observed_service_count += 1;
                }
                _ => {
                    panic!("Unexpected requests (should only see one per sled): {:#?}", requests);
                }
            }
        }
        assert_eq!(observed_service_count, SERVICE_COUNT);

        test.cleanup().await;
    }

    #[tokio::test]
    async fn test_provision_nexus_service_per_rack() {
        let test =
            ProvisionTest::new("test_provision_nexus_service_per_rack").await;

        let rack_subnet = Ipv6Subnet::new(Ipv6Addr::LOCALHOST);
        let nexus = FakeNexus::new(test.datastore.clone(), rack_subnet);
        let dns_updater = FakeDnsUpdater::new();
        let service_balancer = ServiceBalancer::new(
            test.logctx.log.clone(),
            nexus.clone(),
            dns_updater.clone(),
        );

        // Setup: Create three sleds, with the goal of putting services on two
        // of them.
        const SLED_COUNT: u32 = 3;
        const SERVICE_COUNT: u32 = 2;
        let mut sleds = vec![];
        for _ in 0..SLED_COUNT {
            sleds
                .push(create_test_sled(nexus.rack_id(), &test.datastore).await);
        }
        let expected_services = [ExpectedService {
            kind: ServiceKind::Nexus,
            redundancy: ServiceRedundancy::PerRack(SERVICE_COUNT),
        }];

        // Request the services
        service_balancer
            .ensure_services_provisioned(&test.opctx, &expected_services)
            .await
            .unwrap();

        // Observe the service on SERVICE_COUNT of the SLED_COUNT sleds.
        let mut observed_service_count = 0;
        for sled_id in &sleds {
            let sled = nexus.sled_client(&sled_id).await.unwrap();
            let requests = sled.service_requests();
            match requests.len() {
                0 => (), // Ignore the sleds where nothing was provisioned
                1 => {
                    assert_eq!(requests[0].name, "nexus");
                    match &requests[0].service_type {
                        SledAgentTypes::ServiceType::Nexus {
                            internal_ip,
                            external_ip,
                        } => {
                            // TODO: This is currently failing! We need to make
                            // the Nexus external IP come from an IP pool for
                            // external addresses.
                            assert_ne!(
                                internal_ip,
                                external_ip,
                            );

                            // TODO: check ports too, maybe?
                        }
                        _ => panic!(
                            "unexpected service type: {:?}",
                            requests[0].service_type
                        ),
                    }
                    assert!(requests[0].gz_addresses.is_empty());
                    observed_service_count += 1;
                }
                _ => {
                    panic!("Unexpected requests (should only see one per sled): {:#?}", requests);
                }
            }
        }
        assert_eq!(observed_service_count, SERVICE_COUNT);

        test.cleanup().await;
    }

    /*

    // TODO: Check for GZ?
    #[tokio::test]
    async fn test_provision_dns_service_per_az() {
        todo!();
    }

    // TODO: Check for value of 'asic'?
    #[tokio::test]
    async fn test_provision_scrimlet_service() {
        todo!();
    }

    */
}
