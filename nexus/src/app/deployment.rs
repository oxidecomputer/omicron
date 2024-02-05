// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration of the deployment system

use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pagination::Paginator;
use nexus_deployment::blueprint_builder::BlueprintBuilder;
use nexus_deployment::default_service_count;
use nexus_deployment::planner::Planner;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintMetadata;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::BlueprintTargetSet;
use nexus_types::deployment::Policy;
use nexus_types::deployment::SledResources;
use nexus_types::deployment::ZpoolName;
use nexus_types::identity::Asset;
use nexus_types::inventory::Collection;
use omicron_common::address::IpRange;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::str::FromStr;
use uuid::Uuid;

/// "limit" used in SQL queries that paginate through all sleds, zpools, etc.
// unsafe: `new_unchecked` is only unsound if the argument is 0.
const SQL_BATCH_SIZE: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(1000) };

/// Common structure for collecting information that the planner needs
struct PlanningContext {
    policy: Policy,
    creator: String,
    inventory: Option<Collection>,
}

impl super::Nexus {
    pub async fn blueprint_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<BlueprintMetadata> {
        self.db_datastore.blueprints_list(opctx, pagparams).await
    }

    pub async fn blueprint_view(
        &self,
        opctx: &OpContext,
        blueprint_id: Uuid,
    ) -> LookupResult<Blueprint> {
        let blueprint = authz::Blueprint::new(
            authz::FLEET,
            blueprint_id,
            LookupType::ById(blueprint_id),
        );
        self.db_datastore.blueprint_read(opctx, &blueprint).await
    }

    pub async fn blueprint_delete(
        &self,
        opctx: &OpContext,
        blueprint_id: Uuid,
    ) -> DeleteResult {
        let blueprint = authz::Blueprint::new(
            authz::FLEET,
            blueprint_id,
            LookupType::ById(blueprint_id),
        );
        self.db_datastore.blueprint_delete(opctx, &blueprint).await
    }

    pub async fn blueprint_target_view(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<BlueprintTarget>, Error> {
        self.db_datastore.blueprint_target_get_current(opctx).await
    }

    pub async fn blueprint_target_set(
        &self,
        opctx: &OpContext,
        params: BlueprintTargetSet,
    ) -> Result<BlueprintTarget, Error> {
        let new_target = BlueprintTarget {
            target_id: params.target_id,
            enabled: params.enabled,
            time_made_target: chrono::Utc::now(),
        };

        self.db_datastore
            .blueprint_target_set_current(opctx, new_target)
            .await?;

        // When we add a background task executing the target blueprint,
        // this is the point where we'd signal it to update its target.

        Ok(new_target)
    }

    async fn blueprint_planning_context(
        &self,
        opctx: &OpContext,
    ) -> Result<PlanningContext, Error> {
        let creator = self.id.to_string();
        let datastore = self.datastore();

        let sled_rows = {
            let mut all_sleds = Vec::new();
            let mut paginator = Paginator::new(SQL_BATCH_SIZE);
            while let Some(p) = paginator.next() {
                let batch =
                    datastore.sled_list(opctx, &p.current_pagparams()).await?;
                paginator =
                    p.found_batch(&batch, &|s: &nexus_db_model::Sled| s.id());
                all_sleds.extend(batch);
            }
            all_sleds
        };

        let mut zpools_by_sled_id = {
            let mut zpools = BTreeMap::new();
            let mut paginator = Paginator::new(SQL_BATCH_SIZE);
            while let Some(p) = paginator.next() {
                let batch = datastore
                    .zpool_list_all_external(opctx, &p.current_pagparams())
                    .await?;
                paginator =
                    p.found_batch(&batch, &|z: &nexus_db_model::Zpool| z.id());
                for z in batch {
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
                let sled_info = SledResources { subnet, zpools };
                (sled_id, sled_info)
            })
            .collect();

        let service_ip_pool_ranges = {
            let (authz_service_ip_pool, _) =
                datastore.ip_pools_service_lookup(opctx).await?;

            let mut ip_ranges = Vec::new();
            let mut paginator = Paginator::new(SQL_BATCH_SIZE);
            while let Some(p) = paginator.next() {
                let batch = datastore
                    .ip_pool_list_ranges(
                        opctx,
                        &authz_service_ip_pool,
                        &p.current_pagparams(),
                    )
                    .await?;
                // The use of `last_address` here assumes `paginator` is sorting
                // in Ascending order (which it does - see the implementation of
                // `current_pagparams()`).
                paginator = p.found_batch(&batch, &|r| r.last_address);
                ip_ranges.extend(batch.iter().map(IpRange::from));
            }

            ip_ranges
        };

        // The choice of which inventory collection to use here is not
        // necessarily trivial.  Inventory collections may be incomplete due to
        // transient (or even persistent) errors.  It's not yet clear what
        // general criteria we'll want to use in picking a collection here.  But
        // as of this writing, this is only used for one specific case, which is
        // to implement a gate that prevents the planner from provisioning
        // non-NTP zones on a sled unless we know there's an NTP zone already on
        // that sled.  For that purpose, it's okay if this collection is
        // incomplete due to a transient error -- that would just prevent
        // forward progress in the planner until the next time we try this.
        // (Critically, it won't cause the planner to do anything wrong.)
        let inventory = datastore
            .inventory_get_latest_collection(opctx)
            .await
            .internal_context(
                "fetching latest inventory collection for blueprint planner",
            )?;

        Ok(PlanningContext {
            creator,
            policy: Policy {
                sleds,
                service_ip_pool_ranges,
                target_nexus_zone_count: default_service_count::NEXUS,
            },
            inventory,
        })
    }

    async fn blueprint_add(
        &self,
        opctx: &OpContext,
        blueprint: &Blueprint,
    ) -> Result<(), Error> {
        self.db_datastore.blueprint_insert(opctx, blueprint).await
    }

    pub async fn blueprint_generate_from_collection(
        &self,
        opctx: &OpContext,
        collection_id: Uuid,
    ) -> CreateResult<Blueprint> {
        let collection = self
            .datastore()
            .inventory_collection_read(opctx, collection_id)
            .await?;
        let planning_context = self.blueprint_planning_context(opctx).await?;
        let blueprint = BlueprintBuilder::build_initial_from_collection(
            &collection,
            &planning_context.policy,
            &planning_context.creator,
        )
        .map_err(|error| {
            Error::internal_error(&format!(
                "error generating initial blueprint from collection {}: {}",
                collection_id,
                InlineErrorChain::new(&error)
            ))
        })?;

        self.blueprint_add(&opctx, &blueprint).await?;
        Ok(blueprint)
    }

    pub async fn blueprint_create_regenerate(
        &self,
        opctx: &OpContext,
    ) -> CreateResult<Blueprint> {
        let maybe_target =
            self.db_datastore.blueprint_target_get_current_full(opctx).await?;
        let Some((_, parent_blueprint)) = maybe_target else {
            return Err(Error::conflict(
                "cannot regenerate blueprint without existing target",
            ));
        };

        let planning_context = self.blueprint_planning_context(opctx).await?;
        let inventory = planning_context.inventory.ok_or_else(|| {
            Error::internal_error("no recent inventory collection found")
        })?;
        let planner = Planner::new_based_on(
            opctx.log.clone(),
            &parent_blueprint,
            &planning_context.policy,
            &planning_context.creator,
            &inventory,
        )
        .map_err(|error| {
            Error::internal_error(&format!(
                "error creating blueprint planner: {error:#}",
            ))
        })?;
        let blueprint = planner.plan().map_err(|error| {
            Error::internal_error(&format!(
                "error generating blueprint: {}",
                InlineErrorChain::new(&error)
            ))
        })?;

        self.blueprint_add(&opctx, &blueprint).await?;
        Ok(blueprint)
    }
}
