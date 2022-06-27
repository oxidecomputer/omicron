// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Task which ensures that expected Nexus services exist.

use async_trait::async_trait;
use crate::context::OpContext;
use crate::db::datastore::{DataStore, DatasetRedundancy};
use crate::db::identity::Asset;
use crate::db::model::Dataset;
use crate::db::model::DatasetKind;
use crate::db::model::Service;
use crate::db::model::ServiceKind;
use crate::db::model::Sled;
use crate::db::model::Zpool;
use crate::Nexus;
use futures::stream::{self, StreamExt, TryStreamExt};
use internal_dns_client::{
    multiclient::{
        AAAARecord,
        DnsError,
        Updater as DnsUpdater
    },
    names::SRV,
};
use omicron_common::address::{
    DNS_PORT, DNS_REDUNDANCY, DNS_SERVER_PORT, NEXUS_EXTERNAL_PORT,
    NEXUS_INTERNAL_PORT,
};
use omicron_common::api::external::Error;
use sled_agent_client::types as SledAgentTypes;
use slog::Logger;
use std::collections::{HashMap, HashSet};
use std::net::{Ipv6Addr, SocketAddrV6};
use std::sync::Arc;
use uuid::Uuid;

// Policy for the number of services to be provisioned.
#[derive(Debug)]
enum ServiceRedundancy {
    // This service must exist on at least this many sleds
    // within the rack.
    PerRack(u32),

    // This service must exist on at least this many sleds
    // within the availability zone.
    DnsPerAz(u32),
}

#[derive(Debug)]
struct ExpectedService {
    kind: ServiceKind,
    redundancy: ServiceRedundancy,
}

// NOTE: longer-term, when we integrate multi-rack support,
// it is expected that Nexus will manage multiple racks
// within the fleet, rather than simply per-rack services.
//
// When that happens, it is likely that many of the "per-rack"
// services will become "per-fleet", such as Nexus and CRDB.
const EXPECTED_SERVICES: [ExpectedService; 3] = [
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
        redundancy: DatasetRedundancy::PerRack(1),
    },
    ExpectedDataset {
        kind: DatasetKind::Clickhouse,
        redundancy: DatasetRedundancy::PerRack(1),
    },
];

// A trait intended to aid testing.
//
// The non-test implementation should be as simple as possible.
#[async_trait]
trait SledClientInterface {
    async fn services_put(&self, body: &SledAgentTypes::ServiceEnsureBody) -> Result<(), Error>;
}

#[async_trait]
impl SledClientInterface for sled_agent_client::Client {
    async fn services_put(&self, body: &SledAgentTypes::ServiceEnsureBody) -> Result<(), Error> {
        self.services_put(body).await?;
        Ok(())
    }
}

// A trait intended to aid testing.
//
// The non-test implementation should be as simple as possible.
#[async_trait]
trait NexusInterface<SledClient: SledClientInterface> {
    fn rack_id(&self) -> Uuid;
    fn datastore(&self) -> &Arc<DataStore>;
    async fn sled_client(&self, id: &Uuid) -> Result<Arc<SledClient>, Error>;
}

#[async_trait]
impl NexusInterface<sled_agent_client::Client> for Nexus {
    fn rack_id(&self) -> Uuid {
        self.rack_id
    }

    fn datastore(&self) -> &Arc<DataStore> {
        self.datastore()
    }

    async fn sled_client(&self, id: &Uuid) -> Result<Arc<sled_agent_client::Client>, Error> {
        self.sled_client(id).await
    }
}

// A trait intended to aid testing.
//
// The non-test implementation should be as simple as possible.
#[async_trait]
trait DnsUpdaterInterface {
    async fn insert_dns_records(&self, records: &HashMap<SRV, Vec<AAAARecord>>) -> Result<(), DnsError>;
}

#[async_trait]
impl DnsUpdaterInterface for DnsUpdater {
    async fn insert_dns_records(&self, records: &HashMap<SRV, Vec<AAAARecord>>) -> Result<(), DnsError> {
        self.insert_dns_records(records).await
    }
}

pub struct ServiceBalancer {
    log: Logger,
    nexus: Arc<Nexus>,
    dns_updater: DnsUpdater,
}

impl ServiceBalancer {
    pub fn new(log: Logger, nexus: Arc<Nexus>, dns_updater: DnsUpdater) -> Self {
        Self { log, nexus, dns_updater }
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
                // TODO: This interface kinda sucks; ideally we would
                // only insert the *new* services.
                //
                // Inserting the old ones too is costing us an extra query.
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

                                // TODO: This is hacky, specifically to inject
                                // global zone addresses in the DNS service.
                                let gz_addresses = match &s.kind {
                                    ServiceKind::InternalDNS => {
                                        let mut octets = address.octets();
                                        octets[15] = octets[15] + 1;
                                        vec![Ipv6Addr::from(octets)]
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
                    internal_address: SocketAddrV6::new(
                        address,
                        NEXUS_INTERNAL_PORT,
                        0,
                        0,
                    )
                    .to_string(),
                    external_address: SocketAddrV6::new(
                        address,
                        NEXUS_EXTERNAL_PORT,
                        0,
                        0,
                    )
                    .to_string(),
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
        }
    }

    // Provision the services within the database.
    async fn provision_rack_service(
        &self,
        opctx: &OpContext,
        kind: ServiceKind,
        desired_count: u32,
    ) -> Result<Vec<Service>, Error> {
        self.nexus
            .datastore()
            .ensure_rack_service(opctx, self.nexus.rack_id, kind, desired_count)
            .await
    }

    // Provision the services within the database.
    async fn provision_dns_service(
        &self,
        opctx: &OpContext,
        desired_count: u32,
    ) -> Result<Vec<Service>, Error> {
        self.nexus
            .datastore()
            .ensure_dns_service(opctx, self.nexus.rack_subnet, desired_count)
            .await
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
                            .provision_rack_service(
                                opctx,
                                expected_svc.kind,
                                desired_count,
                            )
                            .await?,
                    );
                }
                ServiceRedundancy::DnsPerAz(desired_count) => {
                    svcs.extend_from_slice(
                        &self
                            .provision_dns_service(opctx, desired_count)
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
            .ensure_rack_dataset(opctx, self.nexus.rack_id, kind, redundancy)
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
                    // TODO: This set of "all addresses" isn't right.
                    // TODO: ... should we even be using "all addresses" to contact CRDB?
                    // Can it just rely on DNS, somehow?
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
        expected_datasets: &[ExpectedDataset]
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

    use crate::{authn, authz};
    use dropshot::test_util::LogContext;
    use nexus_test_utils::db::test_setup_database;
    use omicron_test_utils::dev;
    use std::sync::Arc;

    // TODO: maybe figure out what you *want* to test?
    // I suspect we'll need to refactor this API for testability.
    //
    // - Dataset init:
    //   - Call to DB
    //  - For each new dataset...
    //     - Call to Sled (filesystem put)
    //     - Update DNS record
    //
    // - Service init:
    //   - Call to DB
    //   - For each sled...
    //     - List svcs
    //     - Put svcs
    //  - For each new service...
    //     - Update DNS record
    //
    // TODO: Also, idempotency check

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
            Self {
                logctx,
                opctx,
                db,
                datastore,
            }
        }

        async fn cleanup(mut self) {
            self.db.cleanup().await.unwrap();
            self.logctx.cleanup_successful();
        }
    }

    #[derive(Clone)]
    struct FakeNexus {
        datastore: Arc<DataStore>,
    }

    #[derive(Clone)]
    struct FakeDnsUpdater {

    }

    // TODO: interfaces:
    //
    // - Nexus
    //   - Datastore: ✔
    //   - Sled Client:
    //   - Rack ID: easy
    //   - Rack Subnet: easy
    //
    //
    // - DNS service
    //   - insert dns records

    #[tokio::test]
    async fn test_provision_dataset_on_all() {
        let test = ProvisionTest::new("test_provision_dataset_on_all").await;

        // TODO: move into "test"?
        let nexus = Arc::new(FakeNexus {
            datastore: test.datastore.clone(),
        });
        let dns_updater = FakeDnsUpdater {};

        let service_balancer = ServiceBalancer::new(
            test.logctx.log.clone(),
            nexus.clone(),
            dns_updater.clone(),
        );

        // TODO: Upsert zpools?
        // TODO: Also, maybe add a test when invoking this fn on "no zpools".


        // Make the request to the service balancer for Crucibles on all Zpools.
        let expected_datasets = [
            ExpectedDataset {
                kind: DatasetKind::Crucible,
                redundancy: DatasetRedundancy::OnAll,
            }
        ];
        service_balancer.ensure_datasets_provisioned(
            &test.opctx,
            &expected_datasets,
        ).await.unwrap();

        // TODO: Validate that:
        // - That "filesystem_put" was invoked ->  Store the calls?
        // - That the DNS record was updated -> Store the records?

        test.cleanup().await;
    }

    #[tokio::test]
    async fn test_provision_dataset_per_rack() {
        let expected_datasets = [
            ExpectedDataset {
                kind: DatasetKind::Crucible,
                redundancy: DatasetRedundancy::PerRack(2),
            }
        ];
        todo!();
    }

    #[tokio::test]
    async fn test_provision_service_per_rack() {
        todo!();
    }

    #[tokio::test]
    async fn test_provision_service_dns_per_az() {
        todo!();
    }
}
