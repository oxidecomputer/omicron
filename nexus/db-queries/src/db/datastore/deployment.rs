// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::authz;
use crate::authz::ApiResource;
use crate::context::OpContext;
use crate::db;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::pagination::paginated;
use crate::db::pagination::Paginator;
use crate::db::DbConnection;
use crate::db::TransactionError;
use anyhow::Context;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::DateTime;
use chrono::Utc;
use diesel::expression::SelectableHelper;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use diesel::sql_types;
use diesel::Column;
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::RunQueryDsl;
use nexus_db_model::Blueprint as DbBlueprint;
use nexus_db_model::BpOmicronZone;
use nexus_db_model::BpOmicronZoneNic;
use nexus_db_model::BpOmicronZoneNotInService;
use nexus_db_model::BpSledOmicronZones;
use nexus_db_model::BpTarget;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintMetadata;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::OmicronZonesConfig;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::bail_unless;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use uuid::Uuid;

impl DataStore {
    /// List blueprints
    pub async fn blueprints_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<BlueprintMetadata> {
        use db::schema::blueprint;

        opctx
            .authorize(authz::Action::ListChildren, &authz::BLUEPRINT_CONFIG)
            .await?;

        let blueprints = paginated(blueprint::table, blueprint::id, pagparams)
            .select(DbBlueprint::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(blueprints.into_iter().map(BlueprintMetadata::from).collect())
    }

    /// Store a complete blueprint into the database
    pub async fn blueprint_insert(
        &self,
        opctx: &OpContext,
        blueprint: &Blueprint,
    ) -> Result<(), Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        Self::blueprint_insert_on_connection(&conn, opctx, blueprint).await
    }

    /// Variant of [Self::blueprint_insert] which may be called from a
    /// transaction context.
    pub(crate) async fn blueprint_insert_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        opctx: &OpContext,
        blueprint: &Blueprint,
    ) -> Result<(), Error> {
        opctx
            .authorize(authz::Action::Modify, &authz::BLUEPRINT_CONFIG)
            .await?;

        // In the database, the blueprint is represented essentially as a tree
        // rooted at a `blueprint` row.  Other nodes in the tree point
        // back at the `blueprint` via `blueprint_id`.
        //
        // It's helpful to assemble some values before entering the transaction
        // so that we can produce the `Error` type that we want here.
        let row_blueprint = DbBlueprint::from(blueprint);
        let blueprint_id = row_blueprint.id;
        let sled_omicron_zones = blueprint
            .omicron_zones
            .iter()
            .map(|(sled_id, zones_config)| {
                BpSledOmicronZones::new(blueprint_id, *sled_id, zones_config)
            })
            .collect::<Vec<_>>();
        let omicron_zones = blueprint
            .omicron_zones
            .iter()
            .flat_map(|(sled_id, zones_config)| {
                zones_config.zones.iter().map(|zone| {
                    BpOmicronZone::new(blueprint_id, *sled_id, zone)
                        .map_err(|e| Error::internal_error(&format!("{:#}", e)))
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let omicron_zone_nics = blueprint
            .omicron_zones
            .values()
            .flat_map(|zones_config| {
                zones_config.zones.iter().filter_map(|zone| {
                    BpOmicronZoneNic::new(blueprint_id, zone)
                        .with_context(|| format!("zone {:?}", zone.id))
                        .map_err(|e| Error::internal_error(&format!("{:#}", e)))
                        .transpose()
                })
            })
            .collect::<Result<Vec<BpOmicronZoneNic>, _>>()?;

        // `Blueprint` stores a set of zones in service, but in the database we
        // store the set of zones NOT in service (which we expect to be much
        // smaller, often empty). Build that inverted set here.
        let omicron_zones_not_in_service = {
            let mut zones_not_in_service = Vec::new();
            for zone in &omicron_zones {
                if !blueprint.zones_in_service.contains(&zone.id) {
                    zones_not_in_service.push(BpOmicronZoneNotInService {
                        blueprint_id,
                        bp_omicron_zone_id: zone.id,
                    });
                }
            }
            zones_not_in_service
        };

        // This implementation inserts all records associated with the
        // blueprint in one transaction.  This is required: we don't want
        // any planner or executor to see a half-inserted blueprint, nor do we
        // want to leave a partial blueprint around if we crash. However, it
        // does mean this is likely to be a big transaction and if that becomes
        // a problem we could break this up as long as we address those
        // problems.
        //
        // The SQL here is written so that it doesn't have to be an
        // *interactive* transaction.  That is, it should in principle be
        // possible to generate all this SQL up front and send it as one big
        // batch rather than making a bunch of round-trips to the database.
        // We'd do that if we had an interface for doing that with bound
        // parameters, etc.  See oxidecomputer/omicron#973.
        conn.transaction_async(|conn| async move {
            // Insert the row for the blueprint.
            {
                use db::schema::blueprint::dsl;
                let _: usize = diesel::insert_into(dsl::blueprint)
                    .values(row_blueprint)
                    .execute_async(&conn)
                    .await?;
            }

            // Insert all the Omicron zones for this blueprint.
            {
                use db::schema::bp_sled_omicron_zones::dsl as sled_zones;
                let _ = diesel::insert_into(sled_zones::bp_sled_omicron_zones)
                    .values(sled_omicron_zones)
                    .execute_async(&conn)
                    .await?;
            }

            {
                use db::schema::bp_omicron_zone::dsl as omicron_zone;
                let _ = diesel::insert_into(omicron_zone::bp_omicron_zone)
                    .values(omicron_zones)
                    .execute_async(&conn)
                    .await?;
            }

            {
                use db::schema::bp_omicron_zone_nic::dsl as omicron_zone_nic;
                let _ =
                    diesel::insert_into(omicron_zone_nic::bp_omicron_zone_nic)
                        .values(omicron_zone_nics)
                        .execute_async(&conn)
                        .await?;
            }

            {
                use db::schema::bp_omicron_zones_not_in_service::dsl;
                let _ =
                    diesel::insert_into(dsl::bp_omicron_zones_not_in_service)
                        .values(omicron_zones_not_in_service)
                        .execute_async(&conn)
                        .await?;
            }

            Ok(())
        })
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        info!(
            &opctx.log,
            "inserted blueprint";
            "blueprint_id" => %blueprint.id,
        );

        Ok(())
    }

    /// Read a complete blueprint from the database
    pub async fn blueprint_read(
        &self,
        opctx: &OpContext,
        authz_blueprint: &authz::Blueprint,
    ) -> Result<Blueprint, Error> {
        opctx.authorize(authz::Action::Read, authz_blueprint).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let blueprint_id = authz_blueprint.id();

        // Read the metadata from the primary blueprint row, and ensure that it
        // exists.
        let (
            parent_blueprint_id,
            internal_dns_version,
            time_created,
            creator,
            comment,
        ) = {
            use db::schema::blueprint::dsl;

            let Some(blueprint) = dsl::blueprint
                .filter(dsl::id.eq(blueprint_id))
                .select(DbBlueprint::as_select())
                .get_result_async(&*conn)
                .await
                .optional()
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?
            else {
                return Err(authz_blueprint.not_found());
            };

            (
                blueprint.parent_blueprint_id,
                *blueprint.internal_dns_version,
                blueprint.time_created,
                blueprint.creator,
                blueprint.comment,
            )
        };

        // Read this blueprint's `bp_sled_omicron_zones` rows, which describes
        // the `OmicronZonesConfig` generation number for each sled that is a
        // part of this blueprint. Construct the BTreeMap we ultimately need,
        // but all the `zones` vecs will be empty until our next query below.
        let mut omicron_zones: BTreeMap<Uuid, OmicronZonesConfig> = {
            use db::schema::bp_sled_omicron_zones::dsl;

            let mut omicron_zones = BTreeMap::new();
            let mut paginator = Paginator::new(SQL_BATCH_SIZE);
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::bp_sled_omicron_zones,
                    dsl::sled_id,
                    &p.current_pagparams(),
                )
                .filter(dsl::blueprint_id.eq(blueprint_id))
                .select(BpSledOmicronZones::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

                paginator = p.found_batch(&batch, &|s| s.sled_id);

                for s in batch {
                    let old = omicron_zones.insert(
                        s.sled_id,
                        OmicronZonesConfig {
                            generation: *s.generation,
                            zones: Vec::new(),
                        },
                    );
                    bail_unless!(
                        old.is_none(),
                        "found duplicate sled ID in bp_sled_omicron_zones: {}",
                        s.sled_id
                    );
                }
            }

            omicron_zones
        };

        // Assemble a mutable map of all the NICs found, by NIC id.  As we
        // match these up with the corresponding zone below, we'll remove items
        // from this set.  That way we can tell if the same NIC was used twice
        // or not used at all.
        let mut omicron_zone_nics = {
            use db::schema::bp_omicron_zone_nic::dsl;

            let mut omicron_zone_nics = BTreeMap::new();
            let mut paginator = Paginator::new(SQL_BATCH_SIZE);
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::bp_omicron_zone_nic,
                    dsl::id,
                    &p.current_pagparams(),
                )
                .filter(dsl::blueprint_id.eq(blueprint_id))
                .select(BpOmicronZoneNic::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

                paginator = p.found_batch(&batch, &|n| n.id);

                for n in batch {
                    let nic_id = n.id;
                    let old = omicron_zone_nics.insert(nic_id, n);
                    bail_unless!(
                        old.is_none(),
                        "found duplicate NIC ID in bp_omicron_zone_nic: {}",
                        nic_id,
                    );
                }
            }

            omicron_zone_nics
        };

        // Load the list of not-in-service zones. Similar to NICs, we'll use a
        // mutable set of zone IDs so we can tell if a zone we expected to be
        // inactive wasn't present in the blueprint at all.
        let mut omicron_zones_not_in_service = {
            use db::schema::bp_omicron_zones_not_in_service::dsl;

            let mut omicron_zones_not_in_service = BTreeSet::new();
            let mut paginator = Paginator::new(SQL_BATCH_SIZE);
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::bp_omicron_zones_not_in_service,
                    dsl::bp_omicron_zone_id,
                    &p.current_pagparams(),
                )
                .filter(dsl::blueprint_id.eq(blueprint_id))
                .select(BpOmicronZoneNotInService::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

                paginator = p.found_batch(&batch, &|z| z.bp_omicron_zone_id);

                for z in batch {
                    let inserted = omicron_zones_not_in_service
                        .insert(z.bp_omicron_zone_id);
                    bail_unless!(
                        inserted,
                        "found duplicate zone ID in \
                         bp_omicron_zones_not_in_service: {}",
                        z.bp_omicron_zone_id,
                    );
                }
            }

            omicron_zones_not_in_service
        };

        // Create the in-memory list of zones _in_ service, which we'll
        // calculate below as we load zones. (Any zone that isn't present in
        // `omicron_zones_not_in_service` is considered in service.)
        let mut zones_in_service = BTreeSet::new();

        // Load all the zones for each sled.
        {
            use db::schema::bp_omicron_zone::dsl;

            let mut paginator = Paginator::new(SQL_BATCH_SIZE);
            while let Some(p) = paginator.next() {
                // `paginated` implicitly orders by our `id`, which is also
                // handy for testing: the zones are always consistently ordered
                let batch = paginated(
                    dsl::bp_omicron_zone,
                    dsl::id,
                    &p.current_pagparams(),
                )
                .filter(dsl::blueprint_id.eq(blueprint_id))
                .select(BpOmicronZone::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

                paginator = p.found_batch(&batch, &|z| z.id);

                for z in batch {
                    let nic_row = z
                        .bp_nic_id
                        .map(|id| {
                            // This error means that we found a row in
                            // bp_omicron_zone that references a NIC by id but
                            // there's no corresponding row in
                            // bp_omicron_zone_nic with that id.  This should be
                            // impossible and reflects either a bug or database
                            // corruption.
                            omicron_zone_nics.remove(&id).ok_or_else(|| {
                                Error::internal_error(&format!(
                                    "zone {:?}: expected to find NIC {:?}, \
                                     but didn't",
                                    z.id, z.bp_nic_id
                                ))
                            })
                        })
                        .transpose()?;
                    let sled_zones =
                        omicron_zones.get_mut(&z.sled_id).ok_or_else(|| {
                            // This error means that we found a row in
                            // bp_omicron_zone with no associated record in
                            // bp_sled_omicron_zones.  This should be
                            // impossible and reflects either a bug or database
                            // corruption.
                            Error::internal_error(&format!(
                                "zone {:?}: unknown sled: {:?}",
                                z.id, z.sled_id
                            ))
                        })?;
                    let zone_id = z.id;
                    let zone = z
                        .into_omicron_zone_config(nic_row)
                        .with_context(|| {
                            format!("zone {:?}: parse from database", zone_id)
                        })
                        .map_err(|e| {
                            Error::internal_error(&format!(
                                "{:#}",
                                e.to_string()
                            ))
                        })?;
                    sled_zones.zones.push(zone);

                    // If we can remove `zone_id` from
                    // `omicron_zones_not_in_service`, then the zone is not in
                    // service. Otherwise, add it to the list of in-service
                    // zones.
                    if !omicron_zones_not_in_service.remove(&zone_id) {
                        zones_in_service.insert(zone_id);
                    }
                }
            }
        }

        bail_unless!(
            omicron_zone_nics.is_empty(),
            "found extra Omicron zone NICs: {:?}",
            omicron_zone_nics.keys()
        );
        bail_unless!(
            omicron_zones_not_in_service.is_empty(),
            "found extra Omicron zones not in service: {:?}",
            omicron_zones_not_in_service,
        );

        Ok(Blueprint {
            id: blueprint_id,
            omicron_zones,
            zones_in_service,
            parent_blueprint_id,
            internal_dns_version,
            time_created,
            creator,
            comment,
        })
    }

    /// Delete a blueprint from the database
    pub async fn blueprint_delete(
        &self,
        opctx: &OpContext,
        authz_blueprint: &authz::Blueprint,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Delete, authz_blueprint).await?;
        let blueprint_id = authz_blueprint.id();

        // As with inserting a whole blueprint, we remove it in one big
        // transaction.  Similar considerations apply.  We could
        // break it up if these transactions become too big.  But we'd need a
        // way to stop other clients from discovering a collection after we
        // start removing it and we'd also need to make sure we didn't leak a
        // collection if we crash while deleting it.
        let conn = self.pool_connection_authorized(opctx).await?;

        let (
            nblueprints,
            nsled_agent_zones,
            nzones,
            nnics,
            nzones_not_in_service,
        ) = conn
            .transaction_async(|conn| async move {
                // Ensure that blueprint we're about to delete is not the
                // current target.
                let current_target =
                    self.blueprint_current_target_only(&conn).await?;
                if let Some(current_target) = current_target {
                    if current_target.target_id == blueprint_id {
                        return Err(TransactionError::CustomError(
                            Error::conflict(format!(
                                "blueprint {blueprint_id} is the \
                                 current target and cannot be deleted",
                            )),
                        ));
                    }
                }

                // Remove the record describing the blueprint itself.
                let nblueprints = {
                    use db::schema::blueprint::dsl;
                    diesel::delete(
                        dsl::blueprint.filter(dsl::id.eq(blueprint_id)),
                    )
                    .execute_async(&conn)
                    .await?
                };

                // Bail out if this blueprint didn't exist; there won't be
                // references to it in any of the remaining tables either, since
                // deletion always goes through this transaction.
                if nblueprints == 0 {
                    return Err(TransactionError::CustomError(
                        authz_blueprint.not_found(),
                    ));
                }

                // Remove rows associated with Omicron zones
                let nsled_agent_zones = {
                    use db::schema::bp_sled_omicron_zones::dsl;
                    diesel::delete(
                        dsl::bp_sled_omicron_zones
                            .filter(dsl::blueprint_id.eq(blueprint_id)),
                    )
                    .execute_async(&conn)
                    .await?
                };

                let nzones = {
                    use db::schema::bp_omicron_zone::dsl;
                    diesel::delete(
                        dsl::bp_omicron_zone
                            .filter(dsl::blueprint_id.eq(blueprint_id)),
                    )
                    .execute_async(&conn)
                    .await?
                };

                let nnics = {
                    use db::schema::bp_omicron_zone_nic::dsl;
                    diesel::delete(
                        dsl::bp_omicron_zone_nic
                            .filter(dsl::blueprint_id.eq(blueprint_id)),
                    )
                    .execute_async(&conn)
                    .await?
                };

                let nzones_not_in_service = {
                    use db::schema::bp_omicron_zones_not_in_service::dsl;
                    diesel::delete(
                        dsl::bp_omicron_zones_not_in_service
                            .filter(dsl::blueprint_id.eq(blueprint_id)),
                    )
                    .execute_async(&conn)
                    .await?
                };

                Ok((
                    nblueprints,
                    nsled_agent_zones,
                    nzones,
                    nnics,
                    nzones_not_in_service,
                ))
            })
            .await
            .map_err(|error| match error {
                TransactionError::CustomError(e) => e,
                TransactionError::Database(e) => {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })?;

        info!(&opctx.log, "removed blueprint";
            "blueprint_id" => blueprint_id.to_string(),
            "nblueprints" => nblueprints,
            "nsled_agent_zones" => nsled_agent_zones,
            "nzones" => nzones,
            "nnics" => nnics,
            "nzones_not_in_service" => nzones_not_in_service,
        );

        Ok(())
    }

    /// Set the current target blueprint
    ///
    /// In order to become the target blueprint, `target`'s parent blueprint
    /// must be the current target
    pub async fn blueprint_target_set_current(
        &self,
        opctx: &OpContext,
        target: BlueprintTarget,
    ) -> Result<(), Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        Self::blueprint_target_set_current_on_connection(&conn, opctx, target)
            .await
    }

    /// Variant of [Self::blueprint_target_set_current] which may be called from
    /// a transaction context.
    pub(crate) async fn blueprint_target_set_current_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        opctx: &OpContext,
        target: BlueprintTarget,
    ) -> Result<(), Error> {
        opctx
            .authorize(authz::Action::Modify, &authz::BLUEPRINT_CONFIG)
            .await?;

        let query = InsertTargetQuery {
            target_id: target.target_id,
            enabled: target.enabled,
            time_made_target: target.time_made_target,
        };

        query
            .execute_async(conn)
            .await
            .map_err(|e| Error::from(query.decode_error(e)))?;

        Ok(())
    }

    /// Get the current target blueprint, if one exists
    ///
    /// Returns both the metadata about the target and the full blueprint
    /// contents. If you only need the target metadata, use
    /// `blueprint_target_get_current` instead.
    pub async fn blueprint_target_get_current_full(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<(BlueprintTarget, Blueprint)>, Error> {
        opctx.authorize(authz::Action::Read, &authz::BLUEPRINT_CONFIG).await?;

        let conn = self.pool_connection_authorized(opctx).await?;
        let Some(target) = self.blueprint_current_target_only(&conn).await?
        else {
            return Ok(None);
        };

        // The blueprint for the current target cannot be deleted while it is
        // the current target, but it's possible someone else (a) made a new
        // blueprint the target and (b) deleted the blueprint pointed to by our
        // `target` between the above query and the below query. In such a case,
        // this query will fail with an "unknown blueprint ID" error. This
        // should be rare in practice.
        let authz_blueprint = authz_blueprint_from_id(target.target_id);
        let blueprint = self.blueprint_read(opctx, &authz_blueprint).await?;

        Ok(Some((target, blueprint)))
    }

    /// Get the current target blueprint, if one exists
    pub async fn blueprint_target_get_current(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<BlueprintTarget>, Error> {
        opctx.authorize(authz::Action::Read, &authz::BLUEPRINT_CONFIG).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        self.blueprint_current_target_only(&conn).await
    }

    // Helper to fetch the current blueprint target (without fetching the entire
    // blueprint for that target).
    //
    // Caller is responsible for checking authz for this operation.
    async fn blueprint_current_target_only(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<Option<BlueprintTarget>, Error> {
        use db::schema::bp_target::dsl;

        let current_target = dsl::bp_target
            .order_by(dsl::version.desc())
            .first_async::<BpTarget>(conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(current_target.map(BlueprintTarget::from))
    }
}

// Helper to create an `authz::Blueprint` for a specific blueprint ID
fn authz_blueprint_from_id(blueprint_id: Uuid) -> authz::Blueprint {
    authz::Blueprint::new(
        authz::FLEET,
        blueprint_id,
        LookupType::ById(blueprint_id),
    )
}

/// Errors related to inserting a target blueprint
#[derive(Debug)]
enum InsertTargetError {
    /// The requested target blueprint ID does not exist in the blueprint table.
    NoSuchBlueprint(Uuid),
    /// The requested target blueprint's parent does not match the current
    /// target.
    ParentNotTarget(Uuid),
    /// Any other error
    Other(DieselError),
}

impl From<InsertTargetError> for Error {
    fn from(value: InsertTargetError) -> Self {
        match value {
            InsertTargetError::NoSuchBlueprint(id) => {
                Error::not_found_by_id(ResourceType::Blueprint, &id)
            }
            InsertTargetError::ParentNotTarget(id) => {
                Error::invalid_request(format!(
                    "Blueprint {id}'s parent blueprint is not the current \
                     target blueprint"
                ))
            }
            InsertTargetError::Other(e) => {
                public_error_from_diesel(e, ErrorHandler::Server)
            }
        }
    }
}

/// Query to insert a new current target blueprint.
///
/// The `bp_target` table's primary key is the `version` field, and we enforce
/// the following invariants:
///
/// * The first "current target" blueprint is assigned version 1.
/// * In order to be inserted as the first current target blueprint, a
///   blueprint must have a parent_blueprint_id of NULL.
/// * After the first, any subsequent blueprint can only be assigned as the
///   current target if its parent_blueprint_id is the current target blueprint.
/// * When inserting a new child blueprint as the current target, it is assigned
///   a version of 1 + its parent's version.
///
/// The result of this is a linear history of blueprints, where each target is a
/// direct child of the previous current target. Enforcing the above has some
/// subtleties (particularly around handling the "first blueprint with no
/// parent" case). These are expanded on below through inline comments on the
/// query we generate:
///
/// ```sql
/// WITH
///   -- Subquery to fetch the current target (i.e., the row with the max
///   -- veresion in `bp_target`).
///   current_target AS (
///     SELECT
///       "version" AS version,
///       "blueprint_id" AS blueprint_id
///     FROM "bp_target"
///     ORDER BY "version" DESC
///     LIMIT 1
///   ),
///
///   -- Error checking subquery: This uses similar tricks as elsewhere in
///   -- this crate to `CAST(... AS UUID)` with non-UUID values that result
///   -- in runtime errors in specific cases, allowing us to give accurate
///   -- error messages.
///   --
///   -- These checks are not required for correct behavior by the insert
///   -- below. If we removed them, the insert would insert 0 rows if
///   -- these checks would have failed. But they make it easier to report
///   -- specific problems to our caller.
///   --
///   -- The specific cases we check here are noted below.
///   check_validity AS MATERIALIZED (
///     SELECT CAST(IF(
///       -- Return `no-such-blueprint` if the ID we're being told to
///       -- set as the target doesn't exist in the blueprint table.
///       (SELECT "id" FROM "blueprint" WHERE "id" = <new_target_id>) IS NULL,
///       'no-such-blueprint',
///       IF(
///         -- Check for whether our new target's parent matches our current
///         -- target. There are two cases here: The first is the common case
///         -- (i.e., the new target has a parent: does it match the current
///         -- target ID?). The second is the bootstrapping check: if we're
///         -- trying to insert a new target that does not have a parent,
///         -- we should not have a current target at all.
///         --
///         -- If either of these cases fails, we return `parent-not-target`.
///         (
///            SELECT "parent_blueprint_id" FROM "blueprint", current_target
///            WHERE
///              "id" = <new_target_id>
///              AND current_target.blueprint_id = "parent_blueprint_id"
///         ) IS NOT NULL
///         OR
///         (
///            SELECT 1 FROM "blueprint"
///            WHERE
///              "id" = <new_target_id>
///              AND "parent_blueprint_id" IS NULL
///              AND NOT EXISTS (SELECT version FROM current_target)
///         ) = 1,
///         <new_target_id>,
///         'parent-not-target'
///       )
///     ) AS UUID)
///   ),
///
///   -- Determine the new version number to use: either 1 if this is the
///   -- first blueprint being made the current target, or 1 higher than
///   -- the previous target's version.
///   --
///   -- The final clauses of each of these WHERE clauses repeat the
///   -- checks performed above in `check_validity`, and will cause this
///   -- subquery to return no rows if we should not allow the new
///   -- target to be set.
///   new_target AS (
///     SELECT 1 AS new_version FROM "blueprint"
///       WHERE
///         "id" = <new_target_id>
///         AND "parent_blueprint_id" IS NULL
///         AND NOT EXISTS (SELECT version FROM current_target)
///     UNION
///     SELECT current_target.version + 1 FROM current_target, "blueprint"
///       WHERE
///         "id" = <new_target_id>
///         AND "parent_blueprint_id" IS NOT NULL
///         AND "parent_blueprint_id" = current_target.blueprint_id
///   )
///
///   -- Perform the actual insertion.
///   INSERT INTO "bp_target"(
///     "version","blueprint_id","enabled","time_made_target"
///   )
///   SELECT
///     new_target.new_version,
///     <new_target_id>,
///     <new_target_enabled>,
///     <new_target_time_made_target>
///     FROM new_target
/// ```
#[derive(Debug, Clone, Copy)]
struct InsertTargetQuery {
    target_id: Uuid,
    enabled: bool,
    time_made_target: DateTime<Utc>,
}

// Uncastable sentinel used to detect we attempt to make a blueprint the target
// when it does not exist in the blueprint table.
const NO_SUCH_BLUEPRINT_SENTINEL: &str = "no-such-blueprint";

// Uncastable sentinel used to detect we attempt to make a blueprint the target
// when its parent_blueprint_id is not the current target.
const PARENT_NOT_TARGET_SENTINEL: &str = "parent-not-target";

// Error messages generated from the above sentinel values.
const NO_SUCH_BLUEPRINT_ERROR_MESSAGE: &str =
    "could not parse \"no-such-blueprint\" as type uuid: \
     uuid: incorrect UUID length: no-such-blueprint";
const PARENT_NOT_TARGET_ERROR_MESSAGE: &str =
    "could not parse \"parent-not-target\" as type uuid: \
     uuid: incorrect UUID length: parent-not-target";

impl InsertTargetQuery {
    fn decode_error(&self, err: DieselError) -> InsertTargetError {
        match err {
            DieselError::DatabaseError(DatabaseErrorKind::Unknown, info)
                if info.message() == NO_SUCH_BLUEPRINT_ERROR_MESSAGE =>
            {
                InsertTargetError::NoSuchBlueprint(self.target_id)
            }
            DieselError::DatabaseError(DatabaseErrorKind::Unknown, info)
                if info.message() == PARENT_NOT_TARGET_ERROR_MESSAGE =>
            {
                InsertTargetError::ParentNotTarget(self.target_id)
            }
            other => InsertTargetError::Other(other),
        }
    }
}

impl QueryId for InsertTargetQuery {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for InsertTargetQuery {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        use crate::db::schema::blueprint::dsl as bp_dsl;
        use crate::db::schema::bp_target::dsl;

        type FromClause<T> =
            diesel::internal::table_macro::StaticQueryFragmentInstance<T>;
        type BpTargetFromClause = FromClause<db::schema::bp_target::table>;
        type BlueprintFromClause = FromClause<db::schema::blueprint::table>;
        const BP_TARGET_FROM_CLAUSE: BpTargetFromClause =
            BpTargetFromClause::new();
        const BLUEPRINT_FROM_CLAUSE: BlueprintFromClause =
            BlueprintFromClause::new();

        out.push_sql("WITH ");

        out.push_sql("current_target AS (SELECT ");
        out.push_identifier(dsl::version::NAME)?;
        out.push_sql(" AS version,");
        out.push_identifier(dsl::blueprint_id::NAME)?;
        out.push_sql(" AS blueprint_id FROM ");
        BP_TARGET_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" ORDER BY ");
        out.push_identifier(dsl::version::NAME)?;
        out.push_sql(" DESC LIMIT 1),");

        out.push_sql(
            "check_validity AS MATERIALIZED ( \
               SELECT \
                 CAST( \
                   IF( \
                     (SELECT ",
        );
        out.push_identifier(bp_dsl::id::NAME)?;
        out.push_sql(" FROM ");
        BLUEPRINT_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(bp_dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.target_id)?;
        out.push_sql(") IS NULL, ");
        out.push_bind_param::<sql_types::Text, &'static str>(
            &NO_SUCH_BLUEPRINT_SENTINEL,
        )?;
        out.push_sql(
            ", \
                     IF( \
                       (SELECT ",
        );
        out.push_identifier(bp_dsl::parent_blueprint_id::NAME)?;
        out.push_sql(" FROM ");
        BLUEPRINT_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(", current_target WHERE ");
        out.push_identifier(bp_dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.target_id)?;
        out.push_sql(" AND current_target.blueprint_id = ");
        out.push_identifier(bp_dsl::parent_blueprint_id::NAME)?;
        out.push_sql(
            "          ) IS NOT NULL \
                       OR \
                       (SELECT 1 FROM ",
        );
        BLUEPRINT_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(bp_dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.target_id)?;
        out.push_sql(" AND ");
        out.push_identifier(bp_dsl::parent_blueprint_id::NAME)?;
        out.push_sql(
            "  IS NULL \
                        AND NOT EXISTS ( \
                          SELECT version FROM current_target) \
                        ) = 1, ",
        );
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.target_id)?;
        out.push_sql(", ");
        out.push_bind_param::<sql_types::Text, &'static str>(
            &PARENT_NOT_TARGET_SENTINEL,
        )?;
        out.push_sql(
            "   ) \
              ) \
            AS UUID) \
          ), ",
        );

        out.push_sql("new_target AS (SELECT 1 AS new_version FROM ");
        BLUEPRINT_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(bp_dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.target_id)?;
        out.push_sql(" AND ");
        out.push_identifier(bp_dsl::parent_blueprint_id::NAME)?;
        out.push_sql(
            " IS NULL \
            AND NOT EXISTS \
            (SELECT version FROM current_target) \
             UNION \
            SELECT current_target.version + 1 FROM \
              current_target, ",
        );
        BLUEPRINT_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql(" WHERE ");
        out.push_identifier(bp_dsl::id::NAME)?;
        out.push_sql(" = ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.target_id)?;
        out.push_sql(" AND ");
        out.push_identifier(bp_dsl::parent_blueprint_id::NAME)?;
        out.push_sql(" IS NOT NULL AND ");
        out.push_identifier(bp_dsl::parent_blueprint_id::NAME)?;
        out.push_sql(" = current_target.blueprint_id) ");

        out.push_sql("INSERT INTO ");
        BP_TARGET_FROM_CLAUSE.walk_ast(out.reborrow())?;
        out.push_sql("(");
        out.push_identifier(dsl::version::NAME)?;
        out.push_sql(",");
        out.push_identifier(dsl::blueprint_id::NAME)?;
        out.push_sql(",");
        out.push_identifier(dsl::enabled::NAME)?;
        out.push_sql(",");
        out.push_identifier(dsl::time_made_target::NAME)?;
        out.push_sql(") SELECT new_target.new_version, ");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.target_id)?;
        out.push_sql(",");
        out.push_bind_param::<sql_types::Bool, bool>(&self.enabled)?;
        out.push_sql(",");
        out.push_bind_param::<sql_types::Timestamptz, DateTime<Utc>>(
            &self.time_made_target,
        )?;
        out.push_sql(" FROM new_target");

        Ok(())
    }
}

impl RunQueryDsl<DbConnection> for InsertTargetQuery {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::datastore::test_utils::datastore_test;
    use nexus_inventory::now_db_precision;
    use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
    use nexus_reconfigurator_planning::blueprint_builder::Ensure;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::deployment::Policy;
    use nexus_types::deployment::SledResources;
    use nexus_types::external_api::views::SledPolicy;
    use nexus_types::external_api::views::SledState;
    use nexus_types::inventory::Collection;
    use omicron_common::address::Ipv6Subnet;
    use omicron_common::api::external::Generation;
    use omicron_test_utils::dev;
    use rand::thread_rng;
    use rand::Rng;
    use std::mem;
    use std::net::Ipv6Addr;

    static EMPTY_POLICY: Policy = Policy {
        sleds: BTreeMap::new(),
        service_ip_pool_ranges: Vec::new(),
        target_nexus_zone_count: 0,
    };

    // This is a not-super-future-maintainer-friendly helper to check that all
    // the subtables related to blueprints have been pruned of a specific
    // blueprint ID. If additional blueprint tables are added in the future,
    // this function will silently ignore them unless they're manually added.
    async fn ensure_blueprint_fully_deleted(
        datastore: &DataStore,
        blueprint_id: Uuid,
    ) {
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        macro_rules! query_count {
            ($table:ident, $blueprint_id_col:ident) => {{
                use db::schema::$table::dsl;
                let result = dsl::$table
                    .filter(dsl::$blueprint_id_col.eq(blueprint_id))
                    .count()
                    .get_result_async(&*conn)
                    .await;
                (stringify!($table), result)
            }};
        }

        for (table_name, result) in [
            query_count!(blueprint, id),
            query_count!(bp_omicron_zone, blueprint_id),
            query_count!(bp_omicron_zone_nic, blueprint_id),
            query_count!(bp_omicron_zones_not_in_service, blueprint_id),
        ] {
            let count: i64 = result.unwrap();
            assert_eq!(
                count, 0,
                "nonzero row count for blueprint \
                 {blueprint_id} in table {table_name}"
            );
        }
    }

    // Create a fake set of `SledResources`, either with a subnet matching
    // `ip` or with an arbitrary one.
    fn fake_sled_resources(ip: Option<Ipv6Addr>) -> SledResources {
        use illumos_utils::zpool::ZpoolName;
        let zpools = (0..4)
            .map(|_| {
                let name = ZpoolName::new_external(Uuid::new_v4()).to_string();
                name.parse().unwrap()
            })
            .collect();
        let ip = ip.unwrap_or_else(|| thread_rng().gen::<u128>().into());
        SledResources {
            policy: SledPolicy::provisionable(),
            state: SledState::Active,
            zpools,
            subnet: Ipv6Subnet::new(ip),
        }
    }

    // Create a `Policy` that contains all the sleds found in `collection`
    fn policy_from_collection(collection: &Collection) -> Policy {
        Policy {
            sleds: collection
                .sled_agents
                .iter()
                .map(|(sled_id, agent)| {
                    // `Collection` doesn't currently hold zpool names, so
                    // we'll construct fake resources for each sled.
                    (
                        *sled_id,
                        fake_sled_resources(Some(
                            *agent.sled_agent_address.ip(),
                        )),
                    )
                })
                .collect(),
            service_ip_pool_ranges: Vec::new(),
            target_nexus_zone_count: collection
                .all_omicron_zones()
                .filter(|z| z.zone_type.is_nexus())
                .count(),
        }
    }

    fn representative() -> (Collection, Policy, Blueprint) {
        // We'll start with a representative collection...
        let mut collection =
            nexus_inventory::examples::representative().builder.build();

        // ...and then mutate it such that the omicron zones it reports match
        // the sled agent IDs it reports. Steal the sled agent info and drop the
        // fake sled-agent IDs:
        let mut empty_map = BTreeMap::new();
        mem::swap(&mut empty_map, &mut collection.sled_agents);
        let mut sled_agents = empty_map.into_values().collect::<Vec<_>>();

        // Now reinsert them with IDs pulled from the omicron zones. This
        // assumes we have more fake sled agents than omicron zones, which is
        // currently true for the representative collection.
        for &sled_id in collection.omicron_zones.keys() {
            let some_sled_agent = sled_agents.pop().expect(
                "fewer representative sled agents than \
                 representative omicron zones sleds",
            );
            collection.sled_agents.insert(sled_id, some_sled_agent);
        }

        let policy = policy_from_collection(&collection);
        let blueprint = BlueprintBuilder::build_initial_from_collection(
            &collection,
            Generation::new(),
            &policy,
            "test",
        )
        .unwrap();

        (collection, policy, blueprint)
    }

    async fn blueprint_list_all_ids(
        opctx: &OpContext,
        datastore: &DataStore,
    ) -> Vec<Uuid> {
        datastore
            .blueprints_list(opctx, &DataPageParams::max_page())
            .await
            .unwrap()
            .into_iter()
            .map(|bp| bp.id)
            .collect()
    }

    #[tokio::test]
    async fn test_empty_blueprint() {
        // Setup
        let logctx = dev::test_setup_log("test_empty_blueprint");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Create an empty collection and a blueprint from it
        let collection =
            nexus_inventory::CollectionBuilder::new("test").build();
        let blueprint1 = BlueprintBuilder::build_initial_from_collection(
            &collection,
            Generation::new(),
            &EMPTY_POLICY,
            "test",
        )
        .unwrap();
        let authz_blueprint = authz_blueprint_from_id(blueprint1.id);

        // Trying to read it from the database should fail with the relevant
        // "not found" error.
        let err = datastore
            .blueprint_read(&opctx, &authz_blueprint)
            .await
            .unwrap_err();
        assert_eq!(err, authz_blueprint.not_found());

        // Write it to the database and read it back.
        datastore
            .blueprint_insert(&opctx, &blueprint1)
            .await
            .expect("failed to insert blueprint");
        let blueprint_read = datastore
            .blueprint_read(&opctx, &authz_blueprint)
            .await
            .expect("failed to read collection back");
        assert_eq!(blueprint1, blueprint_read);
        assert_eq!(
            blueprint_list_all_ids(&opctx, &datastore).await,
            [blueprint1.id]
        );

        // There ought to be no sleds or zones in service, and no parent
        // blueprint.
        assert_eq!(blueprint1.omicron_zones.len(), 0);
        assert_eq!(blueprint1.zones_in_service.len(), 0);
        assert_eq!(blueprint1.parent_blueprint_id, None);

        // Trying to insert the same blueprint again should fail.
        let err =
            datastore.blueprint_insert(&opctx, &blueprint1).await.unwrap_err();
        assert!(err.to_string().contains("duplicate key"));

        // Delete the blueprint and ensure it's really gone.
        datastore.blueprint_delete(&opctx, &authz_blueprint).await.unwrap();
        ensure_blueprint_fully_deleted(&datastore, blueprint1.id).await;
        assert_eq!(blueprint_list_all_ids(&opctx, &datastore).await, []);

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_representative_blueprint() {
        // Setup
        let logctx = dev::test_setup_log("test_representative_blueprint");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Create a cohesive representative collection/policy/blueprint
        let (collection, mut policy, blueprint1) = representative();
        let authz_blueprint1 = authz_blueprint_from_id(blueprint1.id);

        // Write it to the database and read it back.
        datastore
            .blueprint_insert(&opctx, &blueprint1)
            .await
            .expect("failed to insert blueprint");
        let blueprint_read = datastore
            .blueprint_read(&opctx, &authz_blueprint1)
            .await
            .expect("failed to read collection back");
        assert_eq!(blueprint1, blueprint_read);
        assert_eq!(
            blueprint_list_all_ids(&opctx, &datastore).await,
            [blueprint1.id]
        );

        // Check the number of blueprint elements against our collection.
        assert_eq!(blueprint1.omicron_zones.len(), policy.sleds.len());
        assert_eq!(
            blueprint1.omicron_zones.len(),
            collection.omicron_zones.len()
        );
        assert_eq!(
            blueprint1.all_omicron_zones().count(),
            collection.all_omicron_zones().count()
        );
        // All zones should be in service.
        assert_eq!(
            blueprint1.zones_in_service.len(),
            blueprint1.all_omicron_zones().count()
        );
        assert_eq!(blueprint1.parent_blueprint_id, None);

        // Set blueprint1 as the current target, and ensure that we cannot
        // delete it (as the current target cannot be deleted).
        let bp1_target = BlueprintTarget {
            target_id: blueprint1.id,
            enabled: true,
            time_made_target: now_db_precision(),
        };
        datastore
            .blueprint_target_set_current(&opctx, bp1_target)
            .await
            .unwrap();
        assert_eq!(
            datastore.blueprint_target_get_current_full(&opctx).await.unwrap(),
            Some((bp1_target, blueprint1.clone()))
        );
        let err = datastore
            .blueprint_delete(&opctx, &authz_blueprint1)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains(&format!(
                "blueprint {} is the current target and cannot be deleted",
                blueprint1.id
            )),
            "unexpected error: {err}"
        );

        // Add a new sled to `policy`.
        let new_sled_id = Uuid::new_v4();
        policy.sleds.insert(new_sled_id, fake_sled_resources(None));
        let new_sled_zpools = &policy.sleds.get(&new_sled_id).unwrap().zpools;

        // Create a builder for a child blueprint.  While we're at it, use a
        // different DNS version to test that that works.
        let new_dns_version = blueprint1.internal_dns_version.next();
        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint1,
            new_dns_version,
            &policy,
            "test",
        )
        .expect("failed to create builder");

        // Add zones to our new sled.
        assert_eq!(
            builder.sled_ensure_zone_ntp(new_sled_id).unwrap(),
            Ensure::Added
        );
        for zpool_name in new_sled_zpools {
            assert_eq!(
                builder
                    .sled_ensure_zone_crucible(new_sled_id, zpool_name.clone())
                    .unwrap(),
                Ensure::Added
            );
        }
        let num_new_sled_zones = 1 + new_sled_zpools.len();

        let blueprint2 = builder.build();
        let authz_blueprint2 = authz_blueprint_from_id(blueprint2.id);

        // Check that we added the new sled and its zones.
        assert_eq!(
            blueprint1.omicron_zones.len() + 1,
            blueprint2.omicron_zones.len()
        );
        assert_eq!(
            blueprint1.all_omicron_zones().count() + num_new_sled_zones,
            blueprint2.all_omicron_zones().count()
        );

        // All zones should be in service.
        assert_eq!(
            blueprint2.zones_in_service.len(),
            blueprint2.all_omicron_zones().count()
        );
        assert_eq!(blueprint2.parent_blueprint_id, Some(blueprint1.id));

        // Check that we can write it to the DB and read it back.
        datastore
            .blueprint_insert(&opctx, &blueprint2)
            .await
            .expect("failed to insert blueprint");
        let blueprint_read = datastore
            .blueprint_read(&opctx, &authz_blueprint2)
            .await
            .expect("failed to read collection back");
        println!("diff: {}", blueprint2.diff_sleds(&blueprint_read));
        assert_eq!(blueprint2, blueprint_read);
        assert_eq!(blueprint2.internal_dns_version, new_dns_version);
        {
            let mut expected_ids = [blueprint1.id, blueprint2.id];
            expected_ids.sort();
            assert_eq!(
                blueprint_list_all_ids(&opctx, &datastore).await,
                expected_ids
            );
        }

        // Set blueprint2 as the current target and ensure that means we can not
        // delete it.
        let bp2_target = BlueprintTarget {
            target_id: blueprint2.id,
            enabled: true,
            time_made_target: now_db_precision(),
        };
        datastore
            .blueprint_target_set_current(&opctx, bp2_target)
            .await
            .unwrap();
        assert_eq!(
            datastore.blueprint_target_get_current_full(&opctx).await.unwrap(),
            Some((bp2_target, blueprint2.clone()))
        );
        let err = datastore
            .blueprint_delete(&opctx, &authz_blueprint2)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains(&format!(
                "blueprint {} is the current target and cannot be deleted",
                blueprint2.id
            )),
            "unexpected error: {err}"
        );

        // Now that blueprint2 is the target, we should be able to delete
        // blueprint1.
        datastore.blueprint_delete(&opctx, &authz_blueprint1).await.unwrap();
        ensure_blueprint_fully_deleted(&datastore, blueprint1.id).await;
        assert_eq!(
            blueprint_list_all_ids(&opctx, &datastore).await,
            [blueprint2.id]
        );

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_set_target() {
        // Setup
        let logctx = dev::test_setup_log("test_set_target");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Trying to insert a target that doesn't reference a blueprint should
        // fail with a relevant error message.
        let nonexistent_blueprint_id = Uuid::new_v4();
        let err = datastore
            .blueprint_target_set_current(
                &opctx,
                BlueprintTarget {
                    target_id: nonexistent_blueprint_id,
                    enabled: true,
                    time_made_target: now_db_precision(),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(
            err,
            Error::from(InsertTargetError::NoSuchBlueprint(
                nonexistent_blueprint_id
            ))
        );

        // There should be no current target still.
        assert_eq!(
            datastore.blueprint_target_get_current_full(&opctx).await.unwrap(),
            None
        );

        // Create three blueprints:
        // * `blueprint1` has no parent
        // * `blueprint2` and `blueprint3` both have `blueprint1` as parent
        let collection =
            nexus_inventory::CollectionBuilder::new("test").build();
        let blueprint1 = BlueprintBuilder::build_initial_from_collection(
            &collection,
            Generation::new(),
            &EMPTY_POLICY,
            "test1",
        )
        .unwrap();
        let blueprint2 = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint1,
            Generation::new(),
            &EMPTY_POLICY,
            "test2",
        )
        .expect("failed to create builder")
        .build();
        let blueprint3 = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint1,
            Generation::new(),
            &EMPTY_POLICY,
            "test3",
        )
        .expect("failed to create builder")
        .build();
        assert_eq!(blueprint1.parent_blueprint_id, None);
        assert_eq!(blueprint2.parent_blueprint_id, Some(blueprint1.id));
        assert_eq!(blueprint3.parent_blueprint_id, Some(blueprint1.id));

        // Insert all three into the blueprint table.
        datastore.blueprint_insert(&opctx, &blueprint1).await.unwrap();
        datastore.blueprint_insert(&opctx, &blueprint2).await.unwrap();
        datastore.blueprint_insert(&opctx, &blueprint3).await.unwrap();

        let bp1_target = BlueprintTarget {
            target_id: blueprint1.id,
            enabled: true,
            time_made_target: now_db_precision(),
        };
        let bp2_target = BlueprintTarget {
            target_id: blueprint2.id,
            enabled: true,
            time_made_target: now_db_precision(),
        };
        let bp3_target = BlueprintTarget {
            target_id: blueprint3.id,
            enabled: true,
            time_made_target: now_db_precision(),
        };

        // Attempting to make blueprint2 the current target should fail because
        // it has a non-NULL parent_blueprint_id, but there is no current target
        // (i.e., only a blueprint with no parent can be made the current
        // target).
        let err = datastore
            .blueprint_target_set_current(&opctx, bp2_target)
            .await
            .unwrap_err();
        assert_eq!(
            err,
            Error::from(InsertTargetError::ParentNotTarget(blueprint2.id))
        );

        // There should be no current target still.
        assert_eq!(
            datastore.blueprint_target_get_current_full(&opctx).await.unwrap(),
            None
        );

        // We should be able to insert blueprint1, which has no parent (matching
        // the currently-empty `bp_target` table's lack of a target).
        datastore
            .blueprint_target_set_current(&opctx, bp1_target)
            .await
            .unwrap();
        assert_eq!(
            datastore.blueprint_target_get_current_full(&opctx).await.unwrap(),
            Some((bp1_target, blueprint1.clone()))
        );

        // Now that blueprint1 is the current target, we should be able to
        // insert blueprint2 or blueprint3. WLOG, pick blueprint3.
        datastore
            .blueprint_target_set_current(&opctx, bp3_target)
            .await
            .unwrap();
        assert_eq!(
            datastore.blueprint_target_get_current_full(&opctx).await.unwrap(),
            Some((bp3_target, blueprint3.clone()))
        );

        // Now that blueprint3 is the target, trying to insert blueprint1 or
        // blueprint2 should fail, because neither of their parents (NULL and
        // blueprint1, respectively) match the current target.
        let err = datastore
            .blueprint_target_set_current(&opctx, bp1_target)
            .await
            .unwrap_err();
        assert_eq!(
            err,
            Error::from(InsertTargetError::ParentNotTarget(blueprint1.id))
        );
        let err = datastore
            .blueprint_target_set_current(&opctx, bp2_target)
            .await
            .unwrap_err();
        assert_eq!(
            err,
            Error::from(InsertTargetError::ParentNotTarget(blueprint2.id))
        );

        // Create a child of blueprint3, and ensure when we set it as the target
        // with enabled=false, that status is serialized.
        let blueprint4 = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint3,
            Generation::new(),
            &EMPTY_POLICY,
            "test3",
        )
        .expect("failed to create builder")
        .build();
        assert_eq!(blueprint4.parent_blueprint_id, Some(blueprint3.id));
        datastore.blueprint_insert(&opctx, &blueprint4).await.unwrap();
        let bp4_target = BlueprintTarget {
            target_id: blueprint4.id,
            enabled: false,
            time_made_target: now_db_precision(),
        };
        datastore
            .blueprint_target_set_current(&opctx, bp4_target)
            .await
            .unwrap();
        assert_eq!(
            datastore.blueprint_target_get_current_full(&opctx).await.unwrap(),
            Some((bp4_target, blueprint4))
        );

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
