// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Task which ensures that expected Nexus services exist.

use crate::Nexus;
use crate::context::OpContext;
use crate::db::identity::Asset;
use crate::db::model::DatasetKind;
use crate::db::model::ServiceKind;
use omicron_common::api::external::Error;
use omicron_common::address::{DNS_REDUNDANCY, ReservedRackSubnet};
use slog::Logger;
use std::sync::Arc;
use std::net::Ipv6Addr;
use uuid::Uuid;

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

pub struct ServiceWorker {
    log: Logger,
    nexus: Arc<Nexus>,
}

impl ServiceWorker {
    async fn ensure_rack_svc(
        &self,
        opctx: &OpContext,
        expected_svc: &ExpectedService,
        desired_count: u32,
    ) -> Result<(), Error> {
        // Look up all the sleds, both with and without the service.
        let sleds_and_maybe_svcs = self.nexus
            .datastore()
            .sled_and_service_list(
                opctx,
                expected_svc.kind.clone(),
                self.nexus.rack_id,
            )
            .await?;
        let (sleds_with_svc, sleds_without_svc): (Vec<_>, Vec<_>) =
            sleds_and_maybe_svcs
            .iter()
            .partition(|(_, maybe_svc)| {
                maybe_svc.is_some()
            });
        let mut sleds_without_svc = sleds_without_svc.into_iter()
            .map(|(sled, _)| sled);
        let mut actual_count = sleds_with_svc.len() as u32;

        // Add services to sleds, in-order, until we've met a
        // number sufficient for our redundancy.
        while desired_count < actual_count {
            let sled = sleds_without_svc.next().ok_or_else(|| {
                Error::internal_error("Not enough sleds to deploy service")
            })?;
            let svc_id = Uuid::new_v4();
            let address = self.nexus.datastore()
                .next_ipv6_address(&opctx, sled.id())
                .await?;

            self.nexus.upsert_service(
                    &opctx,
                    svc_id,
                    sled.id(),
                    address,
                    expected_svc.kind.clone()
                )
                .await?;

            actual_count += 1;
        }

        // TODO: Actually deploy service

        Ok(())
    }

    async fn ensure_dns_svc(
        &self,
        opctx: &OpContext,
        expected_svc: &ExpectedService,
        desired_count: u32,
    ) -> Result<(), Error> {
        if !matches!(expected_svc.kind, ServiceKind::InternalDNS) {
            // NOTE: This is a constraint on how we allocate IP addresses
            // within the AZ - however, as DNS is the only existing
            // AZ-wide service, support for this has been punted.
            return Err(Error::internal_error(
                &format!("DNS is the only suppoted svc ({:?} is not supported)", expected_svc),
            ));
        }

        // Look up all existing DNS services.
        //
        // Note that we should not look up "all services" - as internal DNS servers
        // are rack-wide, this would be too expensive of an operation.
        let existing_services = self.nexus
            .datastore()
            .dns_service_list(opctx)
            .await?;

        let mut actual_count = existing_services.len() as u32;

        // Get all subnets not allocated to existing services.
        let mut usable_dns_subnets = ReservedRackSubnet(self.nexus.rack_subnet)
            .get_dns_subnets()
            .into_iter()
            .filter(|subnet| {
                // This address is only usable if none of the existing
                // DNS services are using it.
                existing_services.iter()
                    .all(|svc| Ipv6Addr::from(svc.ip) != subnet.dns_address().ip())
            });

        // Get all sleds which aren't already running DNS services.
        let mut target_sleds = self.nexus
            .datastore()
            .sled_list_with_limit(opctx, desired_count)
            .await?
            .into_iter()
            .filter(|sled| {
                // The target sleds are only considered if they aren't already
                // running a DNS service.
                existing_services.iter()
                    .all(|svc| svc.sled_id != sled.id())
            });

        while desired_count < actual_count {
            let sled = target_sleds.next().ok_or_else(|| {
                    Error::internal_error("Not enough sleds to deploy service")
                })?;
            let svc_id = Uuid::new_v4();
            let dns_subnet = usable_dns_subnets.next().ok_or_else(|| {
                    Error::internal_error("Not enough IPs to deploy service")
                })?;
            let address = dns_subnet
                .dns_address()
                .ip();

            self.nexus.upsert_service(
                    &opctx,
                    svc_id,
                    sled.id(),
                    address,
                    expected_svc.kind.clone()
                )
                .await?;

            actual_count += 1;
        }

        // TODO: actually deploy service

        Ok(())
    }

    // Provides a single point-in-time evaluation and adjustment of
    // the services provisioned within the rack.
    //
    // May adjust the provisioned services to meet the redundancy of the
    // rack, if necessary.
    //
    // TODO: Can we:
    //  - [ ] Put these steps in a saga, to ensure they happen
    //  - [ ] Use a state variable on the rack to ensure mutual exclusion
    //        of service re-balancing. It's an involved operation; it would
    //        be nice to not be conflicting with anyone else while operating -
    //        and also helps us avoid using transactions.
    pub async fn ensure_services_provisioned(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        for expected_svc in &EXPECTED_SERVICES {
            info!(
                self.log,
                "Ensuring service {:?} exists according to redundancy {:?}",
                expected_svc.kind,
                expected_svc.redundancy,
            );
            match expected_svc.redundancy {
                ServiceRedundancy::PerRack(desired_count) => {
                    self.ensure_rack_svc(opctx, expected_svc, desired_count).await?;
                },
                ServiceRedundancy::DnsPerAz(desired_count) => {
                    self.ensure_dns_svc(opctx, expected_svc, desired_count).await?;
                }
            }
        }

        // Strategy:
        //
        // TODO Step 1. In a transaction:
        // - Look up all sleds within the Rack
        // - Look up all the services of a particular kind (e.g., Oximeter)
        // - IF enough exist, exit early.
        // - ELSE assign services to sleds. Write to Db.
        //
        // Step 2. As follow-up: request those svcs execute on sleds.

        Ok(())

    }
}

// Redundancy for the number of datasets to be provisioned.
enum DatasetRedundancy {
    // The dataset should exist on all zpools.
    OnAll,
    // The dataset should exist on at least this many zpools.
    PerRack(u32),
}

struct ExpectedDataset {
    kind: DatasetKind,
    redundancy: DatasetRedundancy,
}

const EXPECTED_DATASERT: [ExpectedDataset; 3] = [
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

fn ensure_datasets_provisioned() {
    // TODO:
    // - [ ] Each zpool has Crucible
    // - [ ] Clickhouse exists on N zpools
    // - [ ] CRDB exists on N zpools

    // Strategy:
    //
    // Step 1. In a transaction:
    // - Look up all sleds within the Rack
    // - Look up all zpools within those sleds
    //
    // - Look up all the services of a particular kind (e.g., Oximeter)
    // - IF enough exist, exit early.
    // - ELSE assign services to sleds. Write to Db.
    //
    // Step 2. As follow-up: request those datasets exist on sleds.


}
