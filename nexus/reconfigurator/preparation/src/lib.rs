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
use nexus_types::deployment::Policy;
use nexus_types::deployment::SledResources;
use nexus_types::deployment::UnstableReconfiguratorState;
use nexus_types::deployment::ZpoolName;
use nexus_types::identity::Asset;
use nexus_types::identity::Resource;
use nexus_types::inventory::Collection;
use omicron_common::address::IpRange;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::NEXUS_REDUNDANCY;
use omicron_common::address::SLED_PREFIX;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::str::FromStr;

/// Given various pieces of database state that go into the blueprint planning
/// process, produce a `Policy` object encapsulating what the planner needs to
/// generate a blueprint
pub fn policy_from_db(
    sled_rows: &[nexus_db_model::Sled],
    zpool_rows: &[nexus_db_model::Zpool],
    ip_pool_range_rows: &[nexus_db_model::IpPoolRange],
    target_nexus_zone_count: usize,
) -> Result<Policy, Error> {
    let mut zpools_by_sled_id = {
        let mut zpools = BTreeMap::new();
        for z in zpool_rows {
            let sled_zpool_names =
                zpools.entry(z.sled_id).or_insert_with(BTreeSet::new);
            // It's unfortunate that Nexus knows how Sled Agent
            // constructs zpool names, but there's not currently an
            // alternative.
            let zpool_name_generated =
                illumos_utils::zpool::ZpoolName::new_external(z.id())
                    .to_string();
            let zpool_name = ZpoolName::from_str(&zpool_name_generated)
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "unexpectedly failed to parse generated \
                                zpool name: {}: {}",
                        zpool_name_generated, e
                    ))
                })?;
            sled_zpool_names.insert(zpool_name);
        }
        zpools
    };

    let sleds = sled_rows
        .into_iter()
        .map(|sled_row| {
            let sled_id = sled_row.id();
            let subnet = Ipv6Subnet::<SLED_PREFIX>::new(sled_row.ip());
            let zpools = zpools_by_sled_id
                .remove(&sled_id)
                .unwrap_or_else(BTreeSet::new);
            let sled_info = SledResources {
                policy: sled_row.policy(),
                state: sled_row.state().into(),
                subnet,
                zpools,
            };
            (sled_id, sled_info)
        })
        .collect();

    let service_ip_pool_ranges =
        ip_pool_range_rows.iter().map(IpRange::from).collect();

    Ok(Policy { sleds, service_ip_pool_ranges, target_nexus_zone_count })
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
        .sled_list_all_batched(opctx)
        .await
        .context("listing sleds")?;
    let zpool_rows = datastore
        .zpool_list_all_external_batched(opctx)
        .await
        .context("listing zpools")?;
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

    let policy = policy_from_db(
        &sled_rows,
        &zpool_rows,
        &ip_pool_range_rows,
        NEXUS_REDUNDANCY,
    )
    .context("assembling policy")?;

    let collection_ids = datastore
        .inventory_collections()
        .await
        .context("listing collections")?;
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
        policy,
        collections,
        blueprints,
        internal_dns,
        external_dns,
        silo_names,
        external_dns_zone_names,
    })
}
