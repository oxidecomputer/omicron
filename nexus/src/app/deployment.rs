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
use nexus_deployment::blueprint_builder::SledInfo;
use nexus_deployment::planner::Planner;
use nexus_types::deployment::params;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintTarget;
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

// XXX-dap temporary in-memory store of blueprints, for testing.  This will move
// to the database.
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
        // XXX-dap replace with an authz resource + lookup resource etc.
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
            // XXX-dap this is the point where we'd poke a background task
            Ok(blueprints.target.clone())
        } else {
            Err(Error::not_found_by_id(ResourceType::Blueprint, &new_target_id))
        }
    }

    async fn blueprint_planning_context(
        &self,
        opctx: &OpContext,
    ) -> Result<PlanningContext, Error> {
        let limit = NonZeroU32::new(1000).unwrap(); // XXX-dap
        let creator = self.id.to_string();
        let datastore = self.datastore();
        let collection = datastore
            .inventory_get_latest_collection(opctx, limit)
            .await?
            .ok_or_else(|| {
                Error::unavail("no recent inventory collection available")
            })?;

        let sled_rows = {
            let mut all_sleds = Vec::new();
            let mut paginator = Paginator::new(limit);
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
            let mut paginator = Paginator::new(limit);
            while let Some(p) = paginator.next() {
                let batch = datastore
                    .zpool_list_all_external(opctx, &p.current_pagparams())
                    .await?;
                paginator =
                    p.found_batch(&batch, &|z: &nexus_db_model::Zpool| z.id());
                for z in batch {
                    let sled_zpool_names =
                        zpools.entry(z.sled_id).or_insert_with(BTreeSet::new);
                    // XXX-dap this seems like slightly gross knowledge.  But
                    // as far as I can tell: sled agent accepts zpool *names*
                    // with the list of zones, but it only provides zpool *ids*
                    // when publishing information about its zpools.  This is
                    // the same logic that RSS currently uses.
                    let zpool_name =
                        illumos_utils::zpool::ZpoolName::new_external(z.id())
                            .to_string();
                    // XXX-dap unwrap
                    sled_zpool_names
                        .insert(ZpoolName::from_str(&zpool_name).unwrap());
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
                let sled_info = SledInfo { subnet, zpools };
                (sled_id, sled_info)
            })
            .collect();

        Ok(PlanningContext { collection, creator, sleds })
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
            &planning_context.sleds,
            &planning_context.creator,
            "initial blueprint",
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
                return Err(Error::conflict(&format!(
                    "cannot regenerate blueprint without existing target"
                )));
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
            &planning_context.sleds,
            &planning_context.creator,
            // XXX-dap this "reason" was intended for the case where we know why
            // we're doing this.  Right now such a case doesn't exist.
            "on-demand regenerate",
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

struct PlanningContext {
    collection: Collection,
    sleds: BTreeMap<Uuid, SledInfo>,
    creator: String,
}
