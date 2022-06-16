// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Task which ensures that expected Nexus services exist.

use crate::context::OpContext;
use crate::db::datastore::DatasetRedundancy;
use crate::db::identity::Asset;
use crate::db::model::Dataset;
use crate::db::model::DatasetKind;
use crate::db::model::Service;
use crate::db::model::ServiceKind;
use crate::db::model::Sled;
use crate::db::model::Zpool;
use crate::Nexus;
use internal_dns_client::multiclient::{
    Service as DnsService, Updater as DnsUpdater,
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

// Policy for the number of services to be provisioned.
#[derive(Debug)]
enum ServiceRedundancy {
    // This service must exist on at least this many sleds
    // within the racki.
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

pub struct ServiceBalancer {
    log: Logger,
    nexus: Arc<Nexus>,
    dns_updater: DnsUpdater,
}

impl ServiceBalancer {
    pub fn new(log: Logger, nexus: Arc<Nexus>) -> Self {
        let dns_updater = DnsUpdater::new(
            nexus.az_subnet(),
            log.new(o!("component" => "DNS Updater")),
        );

        Self { log, nexus, dns_updater }
    }

    // Reaches out to all sled agents implied in "services", and
    // requests that the desired services are executing.
    async fn instantiate_services(
        &self,
        opctx: &OpContext,
        mut services: Vec<Service>,
    ) -> Result<(), Error> {
        let mut sled_ids = HashSet::new();
        for svc in &services {
            sled_ids.insert(svc.sled_id);
        }

        // For all sleds requiring an update, request all services be
        // instantiated.
        for sled_id in &sled_ids {
            // TODO: This interface kinda sucks; ideally we would
            // only insert the *new* services.
            //
            // Inserting the old ones too is costing us an extra query.
            let services =
                self.nexus.datastore().service_list(opctx, *sled_id).await?;
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
        }

        // Putting records of the same SRV right next to each other isn't
        // strictly necessary, but doing so makes the record insertion more
        // efficient.
        services.sort_by(|a, b| a.srv().partial_cmp(&b.srv()).unwrap());
        self.dns_updater
            .insert_dns_records(&services)
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

    async fn ensure_rack_service(
        &self,
        opctx: &OpContext,
        kind: ServiceKind,
        desired_count: u32,
    ) -> Result<(), Error> {
        // Provision the services within the database.
        let services = self
            .nexus
            .datastore()
            .ensure_rack_service(opctx, self.nexus.rack_id, kind, desired_count)
            .await?;

        // Actually instantiate those services.
        self.instantiate_services(opctx, services).await
    }

    async fn ensure_dns_service(
        &self,
        opctx: &OpContext,
        desired_count: u32,
    ) -> Result<(), Error> {
        // Provision the services within the database.
        let services = self
            .nexus
            .datastore()
            .ensure_dns_service(opctx, self.nexus.rack_subnet, desired_count)
            .await?;

        // Actually instantiate those services.
        self.instantiate_services(opctx, services).await
    }

    // TODO: Consider using sagas to ensure the rollout of services happens.
    // Not using sagas *happens* to be fine because these operations are
    // re-tried periodically, but that's kind forcing a dependency on the
    // caller.
    async fn ensure_services_provisioned(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        // NOTE: If any sleds host DNS + other redudant services, we send
        // redundant requests. We could propagate the service list up to a
        // higher level, and do instantiation after all services complete?
        for expected_svc in &EXPECTED_SERVICES {
            info!(self.log, "Ensuring service {:?} exists", expected_svc);
            match expected_svc.redundancy {
                ServiceRedundancy::PerRack(desired_count) => {
                    self.ensure_rack_service(
                        opctx,
                        expected_svc.kind,
                        desired_count,
                    )
                    .await?;
                }
                ServiceRedundancy::DnsPerAz(desired_count) => {
                    self.ensure_dns_service(opctx, desired_count).await?;
                }
            }
        }
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

        let mut sled_clients = HashMap::new();

        // TODO: We could issue these requests concurrently
        for (sled, zpool, dataset) in &datasets {
            let sled_client = {
                match sled_clients.get(&sled.id()) {
                    Some(client) => client,
                    None => {
                        let sled_client =
                            self.nexus.sled_client(&sled.id()).await?;
                        sled_clients.insert(sled.id(), sled_client);
                        sled_clients.get(&sled.id()).unwrap()
                    }
                }
            };

            let dataset_kind = match kind {
                // TODO: This set of "all addresses" isn't right.
                // TODO: ... should we even be using "all addresses" to contact CRDB?
                // Can it just rely on DNS, somehow?
                DatasetKind::Cockroach => {
                    SledAgentTypes::DatasetKind::CockroachDb(vec![])
                }
                DatasetKind::Crucible => SledAgentTypes::DatasetKind::Crucible,
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
        }

        self.dns_updater
            .insert_dns_records(
                &datasets.into_iter().map(|(_, _, dataset)| dataset).collect(),
            )
            .await
            .map_err(|e| Error::internal_error(&e.to_string()))?;

        Ok(())
    }

    async fn ensure_datasets_provisioned(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        for expected_dataset in &EXPECTED_DATASETS {
            info!(self.log, "Ensuring dataset {:?} exists", expected_dataset);
            self.ensure_rack_dataset(
                opctx,
                expected_dataset.kind,
                expected_dataset.redundancy,
            )
            .await?
        }
        Ok(())
    }

    // Provides a single point-in-time evaluation and adjustment of
    // the services provisioned within the rack.
    //
    // May adjust the provisioned services to meet the redundancy of the
    // rack, if necessary.
    pub async fn balance_services(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        self.ensure_datasets_provisioned(opctx).await?;
        self.ensure_services_provisioned(opctx).await?;
        Ok(())
    }
}
