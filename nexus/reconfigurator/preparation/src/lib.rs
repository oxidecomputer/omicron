// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common facilities for assembling inputs to the planner

use anyhow::Context;
use futures::StreamExt;
use nexus_db_model::DnsGroup;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::DataStoreDnsTest;
use nexus_db_queries::db::datastore::DataStoreInventoryTest;
use nexus_db_queries::db::datastore::Discoverability;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintMetadata;
use nexus_types::deployment::CockroachDbClusterVersion;
use nexus_types::deployment::CockroachDbSettings;
use nexus_types::deployment::OmicronZoneExternalIp;
use nexus_types::deployment::OmicronZoneNic;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::PlanningInputBuilder;
use nexus_types::deployment::Policy;
use nexus_types::deployment::SledDetails;
use nexus_types::deployment::SledDisk;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::SledResources;
use nexus_types::deployment::UnstableReconfiguratorState;
use nexus_types::identity::Asset;
use nexus_types::identity::Resource;
use nexus_types::inventory::Collection;
use omicron_common::address::IpRange;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_common::disk::DatasetConfig;
use omicron_common::disk::DiskIdentity;
use omicron_common::policy::BOUNDARY_NTP_REDUNDANCY;
use omicron_common::policy::COCKROACHDB_REDUNDANCY;
use omicron_common::policy::NEXUS_REDUNDANCY;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use slog::error;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// Given various pieces of database state that go into the blueprint planning
/// process, produce a `PlanningInput` object encapsulating what the planner
/// needs to generate a blueprint
pub struct PlanningInputFromDb<'a> {
    pub sled_rows: &'a [nexus_db_model::Sled],
    pub zpool_rows:
        &'a [(nexus_db_model::Zpool, nexus_db_model::PhysicalDisk)],
    pub dataset_rows: &'a [nexus_db_model::Dataset],
    pub ip_pool_range_rows: &'a [nexus_db_model::IpPoolRange],
    pub external_ip_rows: &'a [nexus_db_model::ExternalIp],
    pub service_nic_rows: &'a [nexus_db_model::ServiceNetworkInterface],
    pub target_boundary_ntp_zone_count: usize,
    pub target_nexus_zone_count: usize,
    pub target_cockroachdb_zone_count: usize,
    pub target_cockroachdb_cluster_version: CockroachDbClusterVersion,
    pub internal_dns_version: nexus_db_model::Generation,
    pub external_dns_version: nexus_db_model::Generation,
    pub cockroachdb_settings: &'a CockroachDbSettings,
    pub log: &'a Logger,
}

impl PlanningInputFromDb<'_> {
    pub fn build(&self) -> Result<PlanningInput, Error> {
        let service_ip_pool_ranges =
            self.ip_pool_range_rows.iter().map(IpRange::from).collect();
        let policy = Policy {
            service_ip_pool_ranges,
            target_boundary_ntp_zone_count: self.target_boundary_ntp_zone_count,
            target_nexus_zone_count: self.target_nexus_zone_count,
            target_cockroachdb_zone_count: self.target_cockroachdb_zone_count,
            target_cockroachdb_cluster_version: self
                .target_cockroachdb_cluster_version,
            clickhouse_policy: None,
        };
        let mut builder = PlanningInputBuilder::new(
            policy,
            self.internal_dns_version.into(),
            self.external_dns_version.into(),
            self.cockroachdb_settings.clone(),
        );

        let mut zpools_by_sled_id = {
            // Gather all the datasets first, by Zpool ID
            let mut datasets: Vec<_> = self
                .dataset_rows
                .iter()
                .map(|dataset| {
                    (
                        ZpoolUuid::from_untyped_uuid(dataset.pool_id),
                        dataset.clone(),
                    )
                })
                .collect();
            datasets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            let mut datasets_by_zpool: BTreeMap<_, Vec<_>> = BTreeMap::new();
            for (zpool_id, dataset) in datasets {
                datasets_by_zpool
                    .entry(zpool_id)
                    .or_default()
                    .push(DatasetConfig::try_from(dataset)?);
            }

            // Iterate over all Zpools, identifying their disks and datasets
            let mut zpools = BTreeMap::new();
            for (zpool, disk) in self.zpool_rows {
                let sled_zpool_names =
                    zpools.entry(zpool.sled_id).or_insert_with(BTreeMap::new);
                let zpool_id = ZpoolUuid::from_untyped_uuid(zpool.id());
                let disk = SledDisk {
                    disk_identity: DiskIdentity {
                        vendor: disk.vendor.clone(),
                        serial: disk.serial.clone(),
                        model: disk.model.clone(),
                    },
                    disk_id: PhysicalDiskUuid::from_untyped_uuid(disk.id()),
                    policy: disk.disk_policy.into(),
                    state: disk.disk_state.into(),
                };

                let datasets = datasets_by_zpool
                    .remove(&zpool_id)
                    .unwrap_or_else(|| vec![]);
                sled_zpool_names.insert(zpool_id, (disk, datasets));
            }
            zpools
        };

        for sled_row in self.sled_rows {
            let sled_id = sled_row.id();
            let subnet = Ipv6Subnet::<SLED_PREFIX>::new(sled_row.ip());
            let zpools = zpools_by_sled_id
                .remove(&sled_id)
                .unwrap_or_else(BTreeMap::new);
            let sled_details = SledDetails {
                policy: sled_row.policy(),
                state: sled_row.state().into(),
                resources: SledResources { subnet, zpools },
            };
            // TODO-cleanup use `TypedUuid` everywhere
            let sled_id = SledUuid::from_untyped_uuid(sled_id);
            builder.add_sled(sled_id, sled_details).map_err(|e| {
                Error::internal_error(&format!(
                    "unexpectedly failed to add sled to planning input: {e}"
                ))
            })?;
        }

        for external_ip_row in
            self.external_ip_rows.iter().filter(|r| r.is_service)
        {
            let Some(zone_id) = external_ip_row.parent_id else {
                error!(
                    self.log,
                    "internal database consistency error: service external IP \
                     is missing parent_id (should be the Omicron zone ID)";
                    "ip_row" => ?external_ip_row,
                );
                continue;
            };

            let zone_id = OmicronZoneUuid::from_untyped_uuid(zone_id);

            let external_ip = OmicronZoneExternalIp::try_from(external_ip_row)
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "invalid database IP record for \
                         Omicron zone {zone_id}: {}",
                        InlineErrorChain::new(&e)
                    ))
                })?;

            builder
                .add_omicron_zone_external_ip(zone_id, external_ip)
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "unexpectedly failed to add external IP \
                         to planning input: {e}"
                    ))
                })?;
        }

        for nic_row in self.service_nic_rows {
            let zone_id =
                OmicronZoneUuid::from_untyped_uuid(nic_row.service_id);
            let nic = OmicronZoneNic::try_from(nic_row).map_err(|e| {
                Error::internal_error(&format!(
                    "invalid Omicron zone NIC read from database: {e}"
                ))
            })?;
            builder.add_omicron_zone_nic(zone_id, nic).map_err(|e| {
                Error::internal_error(&format!(
                    "unexpectedly failed to add Omicron zone NIC \
                     to planning input: {e}"
                ))
            })?;
        }

        Ok(builder.build())
    }
}

/// Loads state for import into `reconfigurator-cli`
///
/// This is only to be used in omdb or tests.
pub async fn reconfigurator_state_load(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<UnstableReconfiguratorState, anyhow::Error> {
    opctx.check_complex_operations_allowed()?;
    let sled_rows = datastore
        .sled_list_all_batched(opctx, SledFilter::Commissioned)
        .await
        .context("listing sleds")?;
    let zpool_rows = datastore
        .zpool_list_all_external_batched(opctx)
        .await
        .context("listing zpools")?;
    let dataset_rows = datastore
        .dataset_list_all_batched(opctx, None)
        .await
        .context("listing datasets")?;
    let ip_pool_range_rows = {
        let (authz_service_ip_pool, _) = datastore
            .ip_pools_service_lookup(opctx)
            .await
            .context("fetching IP services pool")?;
        datastore
            .ip_pool_list_ranges_batched(opctx, &authz_service_ip_pool)
            .await
            .context("listing services IP pool ranges")?
    };
    let external_ip_rows = datastore
        .external_ip_list_service_all_batched(opctx)
        .await
        .context("fetching service external IPs")?;
    let service_nic_rows = datastore
        .service_network_interfaces_all_list_batched(opctx)
        .await
        .context("fetching service NICs")?;
    let internal_dns_version = datastore
        .dns_group_latest_version(opctx, DnsGroup::Internal)
        .await
        .context("fetching internal DNS version")?
        .version;
    let external_dns_version = datastore
        .dns_group_latest_version(opctx, DnsGroup::External)
        .await
        .context("fetching external DNS version")?
        .version;
    let cockroachdb_settings = datastore
        .cockroachdb_settings(opctx)
        .await
        .context("fetching cockroachdb settings")?;

    let planning_input = PlanningInputFromDb {
        sled_rows: &sled_rows,
        zpool_rows: &zpool_rows,
        dataset_rows: &dataset_rows,
        ip_pool_range_rows: &ip_pool_range_rows,
        target_boundary_ntp_zone_count: BOUNDARY_NTP_REDUNDANCY,
        target_nexus_zone_count: NEXUS_REDUNDANCY,
        target_cockroachdb_zone_count: COCKROACHDB_REDUNDANCY,
        target_cockroachdb_cluster_version: CockroachDbClusterVersion::POLICY,
        external_ip_rows: &external_ip_rows,
        service_nic_rows: &service_nic_rows,
        log: &opctx.log,
        internal_dns_version,
        external_dns_version,
        cockroachdb_settings: &cockroachdb_settings,
    }
    .build()
    .context("assembling planning_input")?;

    let collection_ids = datastore
        .inventory_collections()
        .await
        .context("listing collections")?
        .into_iter()
        .map(|c| c.id());
    let collections = futures::stream::iter(collection_ids)
        .filter_map(|id| async move {
            let read = datastore
                .inventory_collection_read(opctx, id)
                .await
                .with_context(|| format!("reading collection {}", id));
            // It's not necessarily a problem if we failed to read a collection.
            // They can be removed since we fetched the list.
            read.ok()
        })
        .collect::<Vec<Collection>>()
        .await;

    let mut blueprint_ids = Vec::new();
    let mut paginator = Paginator::new(SQL_BATCH_SIZE);
    while let Some(p) = paginator.next() {
        let batch = datastore
            .blueprints_list(opctx, &p.current_pagparams())
            .await
            .context("listing blueprints")?;
        paginator =
            p.found_batch(&blueprint_ids, &|b: &BlueprintMetadata| b.id);
        blueprint_ids.extend(batch.into_iter());
    }

    let blueprints = futures::stream::iter(blueprint_ids)
        .filter_map(|bpm| async move {
            let blueprint_id = bpm.id;
            let read = datastore
                .blueprint_read(
                    opctx,
                    &nexus_db_queries::authz::Blueprint::new(
                        nexus_db_queries::authz::FLEET,
                        blueprint_id,
                        LookupType::ById(blueprint_id),
                    ),
                )
                .await
                .with_context(|| format!("reading blueprint {}", blueprint_id));
            // It's not necessarily a problem if we failed to read a blueprint.
            // They can be removed since we fetched the list.
            read.ok()
        })
        .collect::<Vec<Blueprint>>()
        .await;

    // It's also useful to include information about any DNS generations
    // mentioned in any blueprints.
    let blueprints_list = &blueprints;
    let fetch_dns_group = |dns_group: DnsGroup| async move {
        let latest_version = datastore
            .dns_group_latest_version(&opctx, dns_group)
            .await
            .with_context(|| {
                format!("reading latest {:?} version", dns_group)
            })?;
        let dns_generations_needed: BTreeSet<_> = blueprints_list
            .iter()
            .map(|blueprint| match dns_group {
                DnsGroup::Internal => blueprint.internal_dns_version,
                DnsGroup::External => blueprint.external_dns_version,
            })
            .chain(std::iter::once(*latest_version.version))
            .collect();
        let mut rv = BTreeMap::new();
        for gen in dns_generations_needed {
            let config = datastore
                .dns_config_read_version(&opctx, dns_group, gen)
                .await
                .with_context(|| {
                    format!("reading {:?} DNS version {}", dns_group, gen)
                })?;
            rv.insert(gen, config);
        }

        Ok::<BTreeMap<_, _>, anyhow::Error>(rv)
    };

    let internal_dns = fetch_dns_group(DnsGroup::Internal).await?;
    let external_dns = fetch_dns_group(DnsGroup::External).await?;
    let silo_names = datastore
        .silo_list_all_batched(&opctx, Discoverability::All)
        .await
        .context("listing all Silos")?
        .into_iter()
        .map(|s| s.name().clone())
        .collect();
    let external_dns_zone_names = datastore
        .dns_zones_list_all(&opctx, DnsGroup::External)
        .await
        .context("listing external DNS zone names")?
        .into_iter()
        .map(|dns_zone| dns_zone.zone_name)
        .collect();
    Ok(UnstableReconfiguratorState {
        planning_input,
        collections,
        blueprints,
        internal_dns,
        external_dns,
        silo_names,
        external_dns_zone_names,
    })
}
