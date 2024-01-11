// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Configuration of the deployment system

use nexus_db_queries::authz;
use nexus_db_queries::authz::Action;
use nexus_db_queries::authz::ApiResource;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::pagination::Paginator;
use nexus_deployment::blueprint_builder::BlueprintBuilder;
use nexus_deployment::planner::Planner;
use nexus_types::deployment::params;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::Policy;
use nexus_types::deployment::SledResources;
use nexus_types::deployment::ZpoolName;
use nexus_types::identity::Asset;
use nexus_types::inventory::Collection;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::SLED_PREFIX;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::str::FromStr;
use uuid::Uuid;

/// "limit" used in SQL queries that paginate through all sleds, zpools, etc.
// unsafe: `new_unchecked` is only unsound if the argument is 0.
const SQL_BATCH_SIZE: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(1000) };

/// "limit" used in SQL queries that fetch inventory data.  Unlike the batch
/// size above, this is a limit on the *total* number of records returned.  If
/// it's too small, the whole operation will fail.  See
/// oxidecomputer/omicron#4629.
// unsafe: `new_unchecked` is only unsound if the argument is 0.
const SQL_LIMIT_INVENTORY: NonZeroU32 =
    unsafe { NonZeroU32::new_unchecked(1000) };

/// Temporary in-memory store of blueprints
///
/// Blueprints eventually need to be stored in the database.  That will obviate
/// the need for this structure.
pub struct Blueprints {
    all_blueprints: BTreeMap<Uuid, Blueprint>,
    target: BlueprintTarget,
}

impl Blueprints {
    pub fn new() -> Blueprints {
        Blueprints {
            all_blueprints: BTreeMap::new(),
            target: BlueprintTarget {
                target_id: None,
                enabled: false,
                time_set: chrono::Utc::now(),
            },
        }
    }
}

/// Common structure for collecting information that the planner needs
struct PlanningContext {
    collection: Collection,
    policy: Policy,
    creator: String,
}

impl super::Nexus {
    pub async fn blueprint_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Blueprint> {
        opctx.authorize(Action::ListChildren, &authz::BLUEPRINT_CONFIG).await?;
        Ok(self
            .blueprints
            .lock()
            .unwrap()
            .all_blueprints
            .values()
            .filter_map(|f| match pagparams.marker {
                None => Some(f.clone()),
                Some(marker) if f.id > *marker => Some(f.clone()),
                _ => None,
            })
            .collect())
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
        opctx.authorize(Action::Read, &blueprint).await?;
        self.blueprints
            .lock()
            .unwrap()
            .all_blueprints
            .get(&blueprint_id)
            .cloned()
            .ok_or_else(|| blueprint.not_found())
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
        opctx.authorize(Action::Delete, &blueprint).await?;

        let mut blueprints = self.blueprints.lock().unwrap();
        if let Some(target_id) = blueprints.target.target_id {
            if target_id == blueprint_id {
                return Err(Error::conflict(format!(
                    "blueprint {} is the current target and cannot be deleted",
                    blueprint_id
                )));
            }
        }

        if blueprints.all_blueprints.remove(&blueprint_id).is_none() {
            return Err(blueprint.not_found());
        }

        Ok(())
    }

    pub async fn blueprint_target_view(
        &self,
        opctx: &OpContext,
    ) -> Result<BlueprintTarget, Error> {
        opctx.authorize(Action::Read, &authz::BLUEPRINT_CONFIG).await?;
        let blueprints = self.blueprints.lock().unwrap();
        Ok(blueprints.target.clone())
    }

    pub async fn blueprint_target_set(
        &self,
        opctx: &OpContext,
        params: params::BlueprintTargetSet,
    ) -> Result<BlueprintTarget, Error> {
        opctx.authorize(Action::Modify, &authz::BLUEPRINT_CONFIG).await?;
        let new_target_id = params.target_id;
        let enabled = params.enabled;
        let mut blueprints = self.blueprints.lock().unwrap();
        if let Some(blueprint) = blueprints.all_blueprints.get(&new_target_id) {
            if blueprint.parent_blueprint_id != blueprints.target.target_id {
                return Err(Error::conflict(&format!(
                    "blueprint {:?}: parent is {:?}, which is not the current \
                    target {:?}",
                    new_target_id,
                    blueprint
                        .parent_blueprint_id
                        .map(|p| p.to_string())
                        .unwrap_or_else(|| String::from("<none>")),
                    blueprints
                        .target
                        .target_id
                        .map(|p| p.to_string())
                        .unwrap_or_else(|| String::from("<none>")),
                )));
            }
            blueprints.target = BlueprintTarget {
                target_id: Some(new_target_id),
                enabled,
                time_set: chrono::Utc::now(),
            };

            // When we add a background task executing the target blueprint,
            // this is the point where we'd signal it to update its target.
            Ok(blueprints.target.clone())
        } else {
            Err(Error::not_found_by_id(ResourceType::Blueprint, &new_target_id))
        }
    }

    async fn blueprint_planning_context(
        &self,
        opctx: &OpContext,
    ) -> Result<PlanningContext, Error> {
        let creator = self.id.to_string();
        let datastore = self.datastore();
        let collection = datastore
            .inventory_get_latest_collection(opctx, SQL_LIMIT_INVENTORY)
            .await?
            .ok_or_else(|| {
                Error::unavail("no recent inventory collection available")
            })?;

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

        Ok(PlanningContext { collection, creator, policy: Policy { sleds } })
    }

    async fn blueprint_add(
        &self,
        opctx: &OpContext,
        blueprint: Blueprint,
    ) -> Result<(), Error> {
        opctx.authorize(Action::Modify, &authz::BLUEPRINT_CONFIG).await?;
        let mut blueprints = self.blueprints.lock().unwrap();
        assert!(blueprints
            .all_blueprints
            .insert(blueprint.id, blueprint)
            .is_none());
        Ok(())
    }

    pub async fn blueprint_create_current(
        &self,
        opctx: &OpContext,
    ) -> CreateResult<Blueprint> {
        opctx.authorize(Action::Read, &authz::BLUEPRINT_CONFIG).await?;
        let planning_context = self.blueprint_planning_context(opctx).await?;
        let blueprint = BlueprintBuilder::build_initial_from_collection(
            &planning_context.collection,
            &planning_context.policy,
            &planning_context.creator,
        )
        .map_err(|error| {
            Error::internal_error(&format!(
                "error generating initial blueprint from current state: {}",
                InlineErrorChain::new(&error)
            ))
        })?;

        self.blueprint_add(&opctx, blueprint.clone()).await?;
        Ok(blueprint)
    }

    pub async fn blueprint_create_regenerate(
        &self,
        opctx: &OpContext,
    ) -> CreateResult<Blueprint> {
        opctx.authorize(Action::Read, &authz::BLUEPRINT_CONFIG).await?;
        let parent_blueprint = {
            let blueprints = self.blueprints.lock().unwrap();
            let Some(target_id) = blueprints.target.target_id else {
                return Err(Error::conflict(
                    "cannot regenerate blueprint without existing target",
                ));
            };
            blueprints
                .all_blueprints
                .get(&target_id)
                .cloned()
                .expect("expected target_id to correspond to a blueprint")
        };
        let planning_context = self.blueprint_planning_context(opctx).await?;
        let planner = Planner::new_based_on(
            opctx.log.clone(),
            &parent_blueprint,
            &planning_context.policy,
            &planning_context.creator,
        );
        let blueprint = planner.plan().map_err(|error| {
            Error::internal_error(&format!(
                "error generating blueprint: {}",
                InlineErrorChain::new(&error)
            ))
        })?;

        self.blueprint_add(&opctx, blueprint.clone()).await?;
        Ok(blueprint)
    }
}
