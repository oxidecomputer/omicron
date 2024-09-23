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
use crate::transaction_retry::OptionalError;
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
use diesel::Insertable;
use diesel::IntoSql;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::RunQueryDsl;
use nexus_db_model::Blueprint as DbBlueprint;
use nexus_db_model::BpOmicronDataset;
use nexus_db_model::BpOmicronPhysicalDisk;
use nexus_db_model::BpOmicronZone;
use nexus_db_model::BpOmicronZoneNic;
use nexus_db_model::BpSledOmicronDatasets;
use nexus_db_model::BpSledOmicronPhysicalDisks;
use nexus_db_model::BpSledOmicronZones;
use nexus_db_model::BpSledState;
use nexus_db_model::BpTarget;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintDatasetsConfig;
use nexus_types::deployment::BlueprintMetadata;
use nexus_types::deployment::BlueprintPhysicalDisksConfig;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::BlueprintZonesConfig;
use nexus_types::deployment::CockroachDbPreserveDowngrade;
use nexus_types::external_api::views::SledState;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::bail_unless;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use std::collections::BTreeMap;
use uuid::Uuid;

mod external_networking;

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

        let sled_states = blueprint
            .sled_state
            .iter()
            .map(|(&sled_id, &state)| BpSledState {
                blueprint_id,
                sled_id: sled_id.into(),
                sled_state: state.into(),
            })
            .collect::<Vec<_>>();

        let sled_omicron_physical_disks = blueprint
            .blueprint_disks
            .iter()
            .map(|(sled_id, disks_config)| {
                BpSledOmicronPhysicalDisks::new(
                    blueprint_id,
                    sled_id.into_untyped_uuid(),
                    disks_config,
                )
            })
            .collect::<Vec<_>>();
        let omicron_physical_disks = blueprint
            .blueprint_disks
            .iter()
            .flat_map(|(sled_id, disks_config)| {
                disks_config.disks.iter().map(move |disk| {
                    BpOmicronPhysicalDisk::new(
                        blueprint_id,
                        sled_id.into_untyped_uuid(),
                        disk,
                    )
                })
            })
            .collect::<Vec<_>>();

        let sled_omicron_datasets = blueprint
            .blueprint_datasets
            .iter()
            .map(|(sled_id, datasets_config)| {
                BpSledOmicronDatasets::new(
                    blueprint_id,
                    *sled_id,
                    datasets_config,
                )
            })
            .collect::<Vec<_>>();
        let omicron_datasets = blueprint
            .blueprint_datasets
            .iter()
            .flat_map(|(sled_id, datasets_config)| {
                datasets_config.datasets.values().map(move |dataset| {
                    BpOmicronDataset::new(blueprint_id, *sled_id, dataset)
                })
            })
            .collect::<Vec<_>>();

        let sled_omicron_zones = blueprint
            .blueprint_zones
            .iter()
            .map(|(sled_id, zones_config)| {
                BpSledOmicronZones::new(blueprint_id, *sled_id, zones_config)
            })
            .collect::<Vec<_>>();
        let omicron_zones = blueprint
            .blueprint_zones
            .iter()
            .flat_map(|(sled_id, zones_config)| {
                zones_config.zones.iter().map(move |zone| {
                    BpOmicronZone::new(blueprint_id, *sled_id, zone)
                        .map_err(|e| Error::internal_error(&format!("{:#}", e)))
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let omicron_zone_nics = blueprint
            .blueprint_zones
            .values()
            .flat_map(|zones_config| {
                zones_config.zones.iter().filter_map(|zone| {
                    BpOmicronZoneNic::new(blueprint_id, zone)
                        .with_context(|| format!("zone {}", zone.id))
                        .map_err(|e| Error::internal_error(&format!("{:#}", e)))
                        .transpose()
                })
            })
            .collect::<Result<Vec<BpOmicronZoneNic>, _>>()?;

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

            // Insert all the sled states for this blueprint.
            {
                use db::schema::bp_sled_state::dsl as sled_state;

                let _ = diesel::insert_into(sled_state::bp_sled_state)
                    .values(sled_states)
                    .execute_async(&conn)
                    .await?;
            }

            // Insert all physical disks for this blueprint.

            {
                use db::schema::bp_sled_omicron_physical_disks::dsl as sled_disks;
                let _ = diesel::insert_into(sled_disks::bp_sled_omicron_physical_disks)
                    .values(sled_omicron_physical_disks)
                    .execute_async(&conn)
                    .await?;
            }

            {
                use db::schema::bp_omicron_physical_disk::dsl as omicron_disk;
                let _ = diesel::insert_into(omicron_disk::bp_omicron_physical_disk)
                    .values(omicron_physical_disks)
                    .execute_async(&conn)
                    .await?;
            }

            // Insert all datasets for this blueprint.

            {
                use db::schema::bp_sled_omicron_datasets::dsl as sled_datasets;
                let _ = diesel::insert_into(sled_datasets::bp_sled_omicron_datasets)
                    .values(sled_omicron_datasets)
                    .execute_async(&conn)
                    .await?;
            }

            {
                use db::schema::bp_omicron_dataset::dsl as omicron_dataset;
                let _ = diesel::insert_into(omicron_dataset::bp_omicron_dataset)
                    .values(omicron_datasets)
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
            external_dns_version,
            cockroachdb_fingerprint,
            cockroachdb_setting_preserve_downgrade,
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
                *blueprint.external_dns_version,
                blueprint.cockroachdb_fingerprint,
                blueprint.cockroachdb_setting_preserve_downgrade,
                blueprint.time_created,
                blueprint.creator,
                blueprint.comment,
            )
        };
        let cockroachdb_setting_preserve_downgrade =
            CockroachDbPreserveDowngrade::from_optional_string(
                &cockroachdb_setting_preserve_downgrade,
            )
            .map_err(|_| {
                Error::internal_error(&format!(
                    "unrecognized cluster version {:?}",
                    cockroachdb_setting_preserve_downgrade
                ))
            })?;

        // Load the sled states for this blueprint.
        let sled_state: BTreeMap<SledUuid, SledState> = {
            use db::schema::bp_sled_state::dsl;

            let mut sled_state = BTreeMap::new();
            let mut paginator = Paginator::new(SQL_BATCH_SIZE);
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::bp_sled_state,
                    dsl::sled_id,
                    &p.current_pagparams(),
                )
                .filter(dsl::blueprint_id.eq(blueprint_id))
                .select(BpSledState::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

                paginator = p.found_batch(&batch, &|s| s.sled_id);

                for s in batch {
                    let old = sled_state
                        .insert(s.sled_id.into(), s.sled_state.into());
                    bail_unless!(
                        old.is_none(),
                        "found duplicate sled ID in bp_sled_state: {}",
                        s.sled_id
                    );
                }
            }
            sled_state
        };

        // Read this blueprint's `bp_sled_omicron_zones` rows, which describes
        // the `OmicronZonesConfig` generation number for each sled that is a
        // part of this blueprint. Construct the BTreeMap we ultimately need,
        // but all the `zones` vecs will be empty until our next query below.
        let mut blueprint_zones: BTreeMap<SledUuid, BlueprintZonesConfig> = {
            use db::schema::bp_sled_omicron_zones::dsl;

            let mut blueprint_zones = BTreeMap::new();
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
                    let old = blueprint_zones.insert(
                        s.sled_id.into(),
                        BlueprintZonesConfig {
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

            blueprint_zones
        };

        // Do the same thing we just did for zones, but for physical disks too.
        let mut blueprint_disks: BTreeMap<
            SledUuid,
            BlueprintPhysicalDisksConfig,
        > = {
            use db::schema::bp_sled_omicron_physical_disks::dsl;

            let mut blueprint_physical_disks = BTreeMap::new();
            let mut paginator = Paginator::new(SQL_BATCH_SIZE);
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::bp_sled_omicron_physical_disks,
                    dsl::sled_id,
                    &p.current_pagparams(),
                )
                .filter(dsl::blueprint_id.eq(blueprint_id))
                .select(BpSledOmicronPhysicalDisks::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

                paginator = p.found_batch(&batch, &|s| s.sled_id);

                for s in batch {
                    let old = blueprint_physical_disks.insert(
                        SledUuid::from_untyped_uuid(s.sled_id),
                        BlueprintPhysicalDisksConfig {
                            generation: *s.generation,
                            disks: Vec::new(),
                        },
                    );
                    bail_unless!(
                        old.is_none(),
                        "found duplicate sled ID in bp_sled_omicron_physical_disks: {}",
                        s.sled_id
                    );
                }
            }

            blueprint_physical_disks
        };

        // Do the same thing we just did for zones, but for datasets too.
        let mut blueprint_datasets: BTreeMap<
            SledUuid,
            BlueprintDatasetsConfig,
        > = {
            use db::schema::bp_sled_omicron_datasets::dsl;

            let mut blueprint_datasets = BTreeMap::new();
            let mut paginator = Paginator::new(SQL_BATCH_SIZE);
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::bp_sled_omicron_datasets,
                    dsl::sled_id,
                    &p.current_pagparams(),
                )
                .filter(dsl::blueprint_id.eq(blueprint_id))
                .select(BpSledOmicronDatasets::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

                paginator = p.found_batch(&batch, &|s| s.sled_id);

                for s in batch {
                    let old = blueprint_datasets.insert(
                        s.sled_id.into(),
                        BlueprintDatasetsConfig {
                            generation: *s.generation,
                            datasets: BTreeMap::new(),
                        },
                    );
                    bail_unless!(
                        old.is_none(),
                        "found duplicate sled ID in bp_sled_omicron_datasets: {}",
                        s.sled_id
                    );
                }
            }

            blueprint_datasets
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
                    let sled_id = SledUuid::from(z.sled_id);
                    let zone_id = z.id;
                    let sled_zones =
                        blueprint_zones.get_mut(&sled_id).ok_or_else(|| {
                            // This error means that we found a row in
                            // bp_omicron_zone with no associated record in
                            // bp_sled_omicron_zones.  This should be
                            // impossible and reflects either a bug or database
                            // corruption.
                            Error::internal_error(&format!(
                                "zone {zone_id}: unknown sled: {sled_id}",
                            ))
                        })?;
                    let zone = z
                        .into_blueprint_zone_config(nic_row)
                        .with_context(|| {
                            format!("zone {zone_id}: parse from database")
                        })
                        .map_err(|e| {
                            Error::internal_error(&format!(
                                "{:#}",
                                e.to_string()
                            ))
                        })?;
                    sled_zones.zones.push(zone);
                }
            }
        }

        // Sort all zones to match what blueprint builders do.
        for (_, zones_config) in blueprint_zones.iter_mut() {
            zones_config.sort();
        }

        bail_unless!(
            omicron_zone_nics.is_empty(),
            "found extra Omicron zone NICs: {:?}",
            omicron_zone_nics.keys()
        );

        // Load all the physical disks for each sled.
        {
            use db::schema::bp_omicron_physical_disk::dsl;

            let mut paginator = Paginator::new(SQL_BATCH_SIZE);
            while let Some(p) = paginator.next() {
                // `paginated` implicitly orders by our `id`, which is also
                // handy for testing: the physical disks are always consistently ordered
                let batch = paginated(
                    dsl::bp_omicron_physical_disk,
                    dsl::id,
                    &p.current_pagparams(),
                )
                .filter(dsl::blueprint_id.eq(blueprint_id))
                .select(BpOmicronPhysicalDisk::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

                paginator = p.found_batch(&batch, &|d| d.id);

                for d in batch {
                    let sled_disks = blueprint_disks
                        .get_mut(&SledUuid::from_untyped_uuid(d.sled_id))
                        .ok_or_else(|| {
                            // This error means that we found a row in
                            // bp_omicron_physical_disk with no associated record in
                            // bp_sled_omicron_physical_disks.  This should be
                            // impossible and reflects either a bug or database
                            // corruption.
                            Error::internal_error(&format!(
                                "disk {}: unknown sled: {}",
                                d.id, d.sled_id
                            ))
                        })?;
                    sled_disks.disks.push(d.into());
                }
            }
        }

        // Load all the datasets for each sled
        {
            use db::schema::bp_omicron_dataset::dsl;

            let mut paginator = Paginator::new(SQL_BATCH_SIZE);
            while let Some(p) = paginator.next() {
                // `paginated` implicitly orders by our `id`, which is also
                // handy for testing: the datasets are always consistently ordered
                let batch = paginated(
                    dsl::bp_omicron_dataset,
                    dsl::id,
                    &p.current_pagparams(),
                )
                .filter(dsl::blueprint_id.eq(blueprint_id))
                .select(BpOmicronDataset::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

                paginator = p.found_batch(&batch, &|d| d.id);

                for d in batch {
                    let sled_datasets = blueprint_datasets
                        .get_mut(&d.sled_id.into())
                        .ok_or_else(|| {
                            // This error means that we found a row in
                            // bp_omicron_dataset with no associated record in
                            // bp_sled_omicron_datasets.  This should be
                            // impossible and reflects either a bug or database
                            // corruption.
                            Error::internal_error(&format!(
                                "dataset {}: unknown sled: {}",
                                d.id, d.sled_id
                            ))
                        })?;

                    let dataset_id = d.id;
                    sled_datasets.datasets.insert(
                        dataset_id.into(),
                        d.try_into().map_err(|e| {
                            Error::internal_error(&format!(
                                "Cannot parse dataset {}: {e}",
                                dataset_id
                            ))
                        })?,
                    );
                }
            }
        }

        // Sort all disks to match what blueprint builders do.
        for (_, disks_config) in blueprint_disks.iter_mut() {
            disks_config.disks.sort_unstable_by_key(|d| d.id);
        }

        Ok(Blueprint {
            id: blueprint_id,
            blueprint_zones,
            blueprint_disks,
            blueprint_datasets,
            sled_state,
            parent_blueprint_id,
            internal_dns_version,
            external_dns_version,
            cockroachdb_fingerprint,
            cockroachdb_setting_preserve_downgrade,
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
            nsled_states,
            nsled_physical_disks,
            nphysical_disks,
            nsled_datasets,
            ndatasets,
            nsled_agent_zones,
            nzones,
            nnics,
        ) = conn
            .transaction_async(|conn| async move {
                // Ensure that blueprint we're about to delete is not the
                // current target.
                let current_target = self
                    .blueprint_current_target_only(
                        &conn,
                        SelectFlavor::Standard,
                    )
                    .await?;
                if current_target.target_id == blueprint_id {
                    return Err(TransactionError::CustomError(
                        Error::conflict(format!(
                            "blueprint {blueprint_id} is the \
                             current target and cannot be deleted",
                        )),
                    ));
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

                // Remove rows associated with sled states.
                let nsled_states = {
                    use db::schema::bp_sled_state::dsl;
                    diesel::delete(
                        dsl::bp_sled_state
                            .filter(dsl::blueprint_id.eq(blueprint_id)),
                    )
                    .execute_async(&conn)
                    .await?
                };

                // Remove rows associated with Omicron physical disks
                let nsled_physical_disks = {
                    use db::schema::bp_sled_omicron_physical_disks::dsl;
                    diesel::delete(
                        dsl::bp_sled_omicron_physical_disks
                            .filter(dsl::blueprint_id.eq(blueprint_id)),
                    )
                    .execute_async(&conn)
                    .await?
                };
                let nphysical_disks = {
                    use db::schema::bp_omicron_physical_disk::dsl;
                    diesel::delete(
                        dsl::bp_omicron_physical_disk
                            .filter(dsl::blueprint_id.eq(blueprint_id)),
                    )
                    .execute_async(&conn)
                    .await?
                };

                // Remove rows associated with Omicron datasets
                let nsled_datasets = {
                    use db::schema::bp_sled_omicron_datasets::dsl;
                    diesel::delete(
                        dsl::bp_sled_omicron_datasets
                            .filter(dsl::blueprint_id.eq(blueprint_id)),
                    )
                    .execute_async(&conn)
                    .await?
                };
                let ndatasets = {
                    use db::schema::bp_omicron_dataset::dsl;
                    diesel::delete(
                        dsl::bp_omicron_dataset
                            .filter(dsl::blueprint_id.eq(blueprint_id)),
                    )
                    .execute_async(&conn)
                    .await?
                };

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

                Ok((
                    nblueprints,
                    nsled_states,
                    nsled_physical_disks,
                    nphysical_disks,
                    nsled_datasets,
                    ndatasets,
                    nsled_agent_zones,
                    nzones,
                    nnics,
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
            "nsled_states" => nsled_states,
            "nsled_physical_disks" => nsled_physical_disks,
            "nphysical_disks" => nphysical_disks,
            "nsled_datasets" => nsled_datasets,
            "ndatasets" => ndatasets,
            "nsled_agent_zones" => nsled_agent_zones,
            "nzones" => nzones,
            "nnics" => nnics,
        );

        Ok(())
    }

    /// Ensure all external networking IPs and service vNICs described by
    /// `blueprint` are allocated (for in-service zones) or deallocated
    /// (otherwise), conditional on `blueprint` being the current target
    /// blueprint.
    ///
    /// This method may be safely executed from the blueprint executor RPW; the
    /// condition on the current target blueprint ensures a Nexus attempting to
    /// realize an out of date blueprint can't overwrite changes made by a Nexus
    /// that realized the current target.
    pub async fn blueprint_ensure_external_networking_resources(
        &self,
        opctx: &OpContext,
        blueprint: &Blueprint,
    ) -> Result<(), Error> {
        self.blueprint_ensure_external_networking_resources_impl(
            opctx,
            blueprint,
            #[cfg(test)]
            None,
            #[cfg(test)]
            None,
        )
        .await
    }

    // The third and fourth arguments to this function only exist when run under
    // test, and allows the calling test to control the general timing of the
    // transaction executed by this method:
    //
    // 1. Check that `blueprint` is the current target blueprint
    // 2. Set `target_check_done` is set to true (the test can wait on this)
    // 3. Run remainder of transaction to allocate/deallocate resources
    // 4. Wait until `return_on_completion` is set to true
    // 5. Return
    //
    // If either of these arguments are `None`, steps 2 or 4 will be skipped.
    async fn blueprint_ensure_external_networking_resources_impl(
        &self,
        opctx: &OpContext,
        blueprint: &Blueprint,
        #[cfg(test)] target_check_done: Option<
            std::sync::Arc<std::sync::atomic::AtomicBool>,
        >,
        #[cfg(test)] return_on_completion: Option<
            std::sync::Arc<std::sync::atomic::AtomicBool>,
        >,
    ) -> Result<(), Error> {
        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper(
            "blueprint_ensure_external_networking_resources",
        )
        .transaction(&conn, |conn| {
            let err = err.clone();
            #[cfg(test)]
            let target_check_done = target_check_done.clone();
            #[cfg(test)]
            let return_on_completion = return_on_completion.clone();

            async move {
                // Bail out if `blueprint` isn't the current target.
                let current_target = self
                    .blueprint_current_target_only(
                        &conn,
                        SelectFlavor::ForUpdate,
                    )
                    .await
                    .map_err(|e| err.bail(e))?;
                if current_target.target_id != blueprint.id {
                    return Err(err.bail(Error::invalid_request(format!(
                        "blueprint {} is not the current target blueprint ({})",
                        blueprint.id, current_target.target_id
                    ))));
                }

                // See the comment on this method; this lets us notify our test
                // caller that we've performed our target blueprint check.
                #[cfg(test)]
                {
                    use std::sync::atomic::Ordering;
                    if let Some(gate) = target_check_done {
                        gate.store(true, Ordering::SeqCst);
                    }
                }

                // Deallocate external networking resources for
                // non-externally-reachable zones before allocating resources
                // for reachable zones. This will allow allocation to succeed if
                // we are swapping an external IP between two zones (e.g.,
                // moving a specific external IP from an old external DNS zone
                // to a new one).
                self.ensure_zone_external_networking_deallocated_on_connection(
                    &conn,
                    &opctx.log,
                    blueprint
                        .all_omicron_zones_not_in(
                            BlueprintZoneFilter::ShouldBeExternallyReachable,
                        )
                        .map(|(_sled_id, zone)| zone),
                )
                .await
                .map_err(|e| err.bail(e))?;
                self.ensure_zone_external_networking_allocated_on_connection(
                    &conn,
                    opctx,
                    blueprint
                        .all_omicron_zones(
                            BlueprintZoneFilter::ShouldBeExternallyReachable,
                        )
                        .map(|(_sled_id, zone)| zone),
                )
                .await
                .map_err(|e| err.bail(e))?;

                // See the comment on this method; this lets us wait until our
                // test caller is ready for us to return.
                #[cfg(test)]
                {
                    use std::sync::atomic::Ordering;
                    use std::time::Duration;
                    if let Some(gate) = return_on_completion {
                        while !gate.load(Ordering::SeqCst) {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    }
                }

                Ok(())
            }
        })
        .await
        .map_err(|e| {
            if let Some(err) = err.take() {
                err
            } else {
                public_error_from_diesel(e, ErrorHandler::Server)
            }
        })
    }

    /// Set the current target blueprint
    ///
    /// In order to become the target blueprint, `target`'s parent blueprint
    /// must be the current target. To instead change the current target's
    /// properties (particularly whether it's enabled), use
    /// [`DataStore::blueprint_target_set_current_enabled`].
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

    /// Set the current target blueprint's `enabled` field
    ///
    /// In order to change the enabled field, `target` must already be the
    /// current target blueprint. To instead set a new blueprint target, use
    /// [`DataStore::blueprint_target_set_current`].
    // Although this function is like `blueprint_target_set_current()` in that
    // both store the given `BlueprintTarget` into the table, the functions are
    // distinct because the preconditions and error cases are different. We
    // could reconsider this and make `blueprint_target_set_current` accept
    // blueprints where either their own or their parent is the current
    // blueprint, although this would require some rework in the nontrivial
    // `InsertTargetQuery` CTE.
    pub async fn blueprint_target_set_current_enabled(
        &self,
        opctx: &OpContext,
        target: BlueprintTarget,
    ) -> Result<(), Error> {
        use db::schema::bp_target::dsl;

        opctx
            .authorize(authz::Action::Modify, &authz::BLUEPRINT_CONFIG)
            .await?;

        // Diesel requires us to use an alias in order to refer to the
        // `bp_target` table twice in the same query.
        let bp_target2 = diesel::alias!(db::schema::bp_target as bp_target1);

        // The following diesel produces this query:
        //
        // ```sql
        // INSERT INTO bp_target
        //   (SELECT
        //        version + 1,
        //        blueprint_id,
        //        <target.enabled>,
        //        <target.time_made_target>
        //    FROM bp_target
        //    WHERE
        //        -- This part of the subquery restricts us to only the
        //        -- current target (i.e., the bp_target with maximal version)
        //        version IN (SELECT version FROM bp_target
        //                    ORDER BY version DESC LIMIT 1)
        //
        //        -- ... and that current target must exactly equal the target
        //        -- blueprint on which we're trying to set `enabled`
        //        AND blueprint_id = <target.blueprint_id>
        //   );
        // ```
        //
        // This will either insert one new row (if the filters were satisified)
        // or no new rows (if the filters were not satisfied).
        let query = dsl::bp_target
            .select((
                dsl::version + 1,
                dsl::blueprint_id,
                target.enabled.into_sql::<sql_types::Bool>(),
                target.time_made_target.into_sql::<sql_types::Timestamptz>(),
            ))
            .filter(
                dsl::version.eq_any(
                    bp_target2
                        .select(bp_target2.field(dsl::version))
                        .order_by(bp_target2.field(dsl::version).desc())
                        .limit(1),
                ),
            )
            .filter(dsl::blueprint_id.eq(target.target_id))
            .insert_into(dsl::bp_target);

        let conn = self.pool_connection_authorized(opctx).await?;

        let num_inserted = query
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        match num_inserted {
            0 => Err(Error::invalid_request(format!(
                "Blueprint {} is not the current target blueprint",
                target.target_id
            ))),
            1 => Ok(()),
            // This is impossible, not only due to the `.limit(1)` in the
            // subquery above, but also because we're inserting `version + 1`
            // which would fail with pkey conflicts if we matched more than one
            // existing row in the subquery.
            _ => unreachable!("query inserted more than one row"),
        }
    }

    /// Get the current target blueprint, if one exists
    ///
    /// Returns both the metadata about the target and the full blueprint
    /// contents. If you only need the target metadata, use
    /// `blueprint_target_get_current` instead.
    pub async fn blueprint_target_get_current_full(
        &self,
        opctx: &OpContext,
    ) -> Result<(BlueprintTarget, Blueprint), Error> {
        opctx.authorize(authz::Action::Read, &authz::BLUEPRINT_CONFIG).await?;

        let conn = self.pool_connection_authorized(opctx).await?;
        let target = self
            .blueprint_current_target_only(&conn, SelectFlavor::Standard)
            .await?;

        // The blueprint for the current target cannot be deleted while it is
        // the current target, but it's possible someone else (a) made a new
        // blueprint the target and (b) deleted the blueprint pointed to by our
        // `target` between the above query and the below query. In such a case,
        // this query will fail with an "unknown blueprint ID" error. This
        // should be rare in practice.
        let authz_blueprint = authz_blueprint_from_id(target.target_id);
        let blueprint = self.blueprint_read(opctx, &authz_blueprint).await?;

        Ok((target, blueprint))
    }

    /// Get the current target blueprint, if one exists
    pub async fn blueprint_target_get_current(
        &self,
        opctx: &OpContext,
    ) -> Result<BlueprintTarget, Error> {
        opctx.authorize(authz::Action::Read, &authz::BLUEPRINT_CONFIG).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        self.blueprint_current_target_only(&conn, SelectFlavor::Standard).await
    }

    // Helper to fetch the current blueprint target (without fetching the entire
    // blueprint for that target).
    //
    // Caller is responsible for checking authz for this operation.
    async fn blueprint_current_target_only(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        select_flavor: SelectFlavor,
    ) -> Result<BlueprintTarget, Error> {
        use db::schema::bp_target::dsl;

        let query_result = match select_flavor {
            SelectFlavor::ForUpdate => {
                dsl::bp_target
                    .order_by(dsl::version.desc())
                    .for_update()
                    .first_async::<BpTarget>(conn)
                    .await
            }
            SelectFlavor::Standard => {
                dsl::bp_target
                    .order_by(dsl::version.desc())
                    .first_async::<BpTarget>(conn)
                    .await
            }
        };
        let current_target = query_result
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // We expect a target blueprint to be set on all systems. RSS sets an
        // initial blueprint, but we shipped systems before it did so. We added
        // target blueprints to those systems via support operations, but let's
        // be careful here and return a specific error for this case.
        let current_target =
            current_target.ok_or_else(|| Error::InternalError {
                internal_message: "no target blueprint set".to_string(),
            })?;

        Ok(current_target.into())
    }
}

#[derive(Debug, Clone, Copy)]
enum SelectFlavor {
    /// A normal `SELECT`.
    Standard,
    /// Acquire a database-level write lock via `SELECT ... FOR UPDATE`.
    ForUpdate,
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
///         -- Sometime between v22.1.9 and v22.2.19, Cockroach's type checker
///         -- became too smart for our `CAST(... as UUID)` error checking
///         -- gadget: it can infer that `<new_target_id>` must be a UUID, so
///         -- then tries to parse 'parent-not-target' and 'no-such-blueprint'
///         -- as UUIDs _during typechecking_, which causes the query to always
///         -- fail. We can defeat this by casting the UUID to text here, which
///         -- will allow the 'parent-not-target' and 'no-such-blueprint'
///         -- sentinels to survive type checking, making it to query execution
///         -- where they will only be cast to UUIDs at runtime in the failure
///         -- cases they're supposed to catch.
///         CAST(<new_target_id> AS text),
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
        out.push_sql("  CAST(");
        out.push_bind_param::<sql_types::Uuid, Uuid>(&self.target_id)?;
        out.push_sql("  AS text), ");
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
    use nexus_inventory::CollectionBuilder;
    use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
    use nexus_reconfigurator_planning::blueprint_builder::Ensure;
    use nexus_reconfigurator_planning::blueprint_builder::EnsureMultiple;
    use nexus_reconfigurator_planning::example::example;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::deployment::BlueprintZoneFilter;
    use nexus_types::deployment::PlanningInput;
    use nexus_types::deployment::PlanningInputBuilder;
    use nexus_types::deployment::SledDetails;
    use nexus_types::deployment::SledDisk;
    use nexus_types::deployment::SledFilter;
    use nexus_types::deployment::SledResources;
    use nexus_types::external_api::views::PhysicalDiskPolicy;
    use nexus_types::external_api::views::PhysicalDiskState;
    use nexus_types::external_api::views::SledPolicy;
    use nexus_types::inventory::Collection;
    use omicron_common::address::Ipv6Subnet;
    use omicron_common::disk::DiskIdentity;
    use omicron_test_utils::dev;
    use omicron_test_utils::dev::poll::wait_for_condition;
    use omicron_test_utils::dev::poll::CondCheckError;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::SledUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use once_cell::sync::Lazy;
    use pretty_assertions::assert_eq;
    use rand::thread_rng;
    use rand::Rng;
    use slog::Logger;
    use std::mem;
    use std::net::Ipv6Addr;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    static EMPTY_PLANNING_INPUT: Lazy<PlanningInput> =
        Lazy::new(|| PlanningInputBuilder::empty_input());

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
            query_count!(bp_sled_state, blueprint_id),
            query_count!(bp_sled_omicron_datasets, blueprint_id),
            query_count!(bp_sled_omicron_physical_disks, blueprint_id),
            query_count!(bp_sled_omicron_zones, blueprint_id),
            query_count!(bp_omicron_dataset, blueprint_id),
            query_count!(bp_omicron_physical_disk, blueprint_id),
            query_count!(bp_omicron_zone, blueprint_id),
            query_count!(bp_omicron_zone_nic, blueprint_id),
        ] {
            let count: i64 = result.unwrap();
            assert_eq!(
                count, 0,
                "nonzero row count for blueprint \
                 {blueprint_id} in table {table_name}"
            );
        }
    }

    // Create a fake set of `SledDetails`, either with a subnet matching
    // `ip` or with an arbitrary one.
    fn fake_sled_details(ip: Option<Ipv6Addr>) -> SledDetails {
        let zpools = (0..4)
            .map(|i| {
                (
                    ZpoolUuid::new_v4(),
                    (
                        SledDisk {
                            disk_identity: DiskIdentity {
                                vendor: String::from("v"),
                                serial: format!("s-{i}"),
                                model: String::from("m"),
                            },
                            disk_id: PhysicalDiskUuid::new_v4(),
                            policy: PhysicalDiskPolicy::InService,
                            state: PhysicalDiskState::Active,
                        },
                        // Datasets
                        vec![],
                    ),
                )
            })
            .collect();
        let ip = ip.unwrap_or_else(|| thread_rng().gen::<u128>().into());
        let resources = SledResources { zpools, subnet: Ipv6Subnet::new(ip) };
        SledDetails {
            policy: SledPolicy::provisionable(),
            state: SledState::Active,
            resources,
        }
    }

    fn representative(
        log: &Logger,
        test_name: &str,
    ) -> (Collection, PlanningInput, Blueprint) {
        // We'll start with an example system.
        let (mut base_collection, planning_input, mut blueprint) =
            example(log, test_name, 3);

        // Take a more thorough collection representative (includes SPs,
        // etc.)...
        let mut collection =
            nexus_inventory::examples::representative().builder.build();

        // ... and replace its sled agent and Omicron zones with those from our
        // example system.
        mem::swap(
            &mut collection.sled_agents,
            &mut base_collection.sled_agents,
        );
        mem::swap(
            &mut collection.omicron_zones,
            &mut base_collection.omicron_zones,
        );

        // Treat this blueprint as the initial blueprint for the system.
        blueprint.parent_blueprint_id = None;

        (collection, planning_input, blueprint)
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

        // Create an empty blueprint from it
        let blueprint1 = BlueprintBuilder::build_empty_with_sleds(
            std::iter::empty(),
            "test",
        );
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
            .expect("failed to read blueprint back");
        assert_eq!(blueprint1, blueprint_read);
        assert_eq!(
            blueprint_list_all_ids(&opctx, &datastore).await,
            [blueprint1.id]
        );

        // There ought to be no sleds or zones, and no parent blueprint.
        assert_eq!(blueprint1.blueprint_zones.len(), 0);
        assert_eq!(blueprint1.parent_blueprint_id, None);

        // Trying to insert the same blueprint again should fail.
        let err =
            datastore.blueprint_insert(&opctx, &blueprint1).await.unwrap_err();
        assert!(err.to_string().contains("duplicate key"));

        // We could try to test deleting this blueprint, but deletion checks
        // that the blueprint being deleted isn't the current target, and we
        // haven't set a current target at all as part of this test. Instead of
        // going through the motions of creating another blueprint and making it
        // the target just to test deletion, we'll end this test here, and rely
        // on other tests to check blueprint deletion.

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_representative_blueprint() {
        const TEST_NAME: &str = "test_representative_blueprint";
        // Setup
        let logctx = dev::test_setup_log(TEST_NAME);
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Create a cohesive representative collection/policy/blueprint
        let (collection, planning_input, blueprint1) =
            representative(&logctx.log, TEST_NAME);
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
        assert_eq!(
            blueprint1.blueprint_zones.len(),
            planning_input.all_sled_ids(SledFilter::Commissioned).count(),
        );
        assert_eq!(
            blueprint1.blueprint_zones.len(),
            collection.omicron_zones.len()
        );
        assert_eq!(
            blueprint1.all_omicron_zones(BlueprintZoneFilter::All).count(),
            collection.all_omicron_zones().count()
        );
        // All zones should be in service.
        assert_all_zones_in_service(&blueprint1);
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
            (bp1_target, blueprint1.clone())
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

        // Add a new sled.
        let new_sled_id = SledUuid::new_v4();

        // While we're at it, use a different DNS version to test that that
        // works.
        let new_internal_dns_version = blueprint1.internal_dns_version.next();
        let new_external_dns_version = new_internal_dns_version.next();
        let planning_input = {
            let mut builder = planning_input.into_builder();
            builder
                .add_sled(new_sled_id, fake_sled_details(None))
                .expect("failed to add sled");
            builder.set_internal_dns_version(new_internal_dns_version);
            builder.set_external_dns_version(new_external_dns_version);
            builder.build()
        };
        let new_sled_zpools =
            &planning_input.sled_resources(&new_sled_id).unwrap().zpools;

        // Create a builder for a child blueprint.
        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint1,
            &planning_input,
            &collection,
            "test",
        )
        .expect("failed to create builder");

        // Ensure disks on our sled
        assert_eq!(
            builder
                .sled_ensure_disks(
                    new_sled_id,
                    &planning_input
                        .sled_resources(&new_sled_id)
                        .unwrap()
                        .clone(),
                )
                .unwrap(),
            EnsureMultiple::Changed {
                added: 4,
                updated: 0,
                expunged: 0,
                removed: 0
            }
        );

        // Add zones to our new sled.
        assert_eq!(
            builder.sled_ensure_zone_ntp(new_sled_id).unwrap(),
            Ensure::Added
        );
        for zpool_id in new_sled_zpools.keys() {
            assert_eq!(
                builder
                    .sled_ensure_zone_crucible(new_sled_id, *zpool_id)
                    .unwrap(),
                Ensure::Added
            );
        }

        let num_new_ntp_zones = 1;
        let num_new_crucible_zones = new_sled_zpools.len();
        let num_new_sled_zones = num_new_ntp_zones + num_new_crucible_zones;

        let blueprint2 = builder.build();
        let authz_blueprint2 = authz_blueprint_from_id(blueprint2.id);

        let diff = blueprint2.diff_since_blueprint(&blueprint1);
        println!("b1 -> b2: {}", diff.display());
        println!("b1 disks: {:?}", blueprint1.blueprint_disks);
        println!("b2 disks: {:?}", blueprint2.blueprint_disks);
        // Check that we added the new sled, as well as its disks and zones.
        assert_eq!(
            blueprint1
                .blueprint_disks
                .values()
                .map(|c| c.disks.len())
                .sum::<usize>()
                + new_sled_zpools.len(),
            blueprint2
                .blueprint_disks
                .values()
                .map(|c| c.disks.len())
                .sum::<usize>()
        );
        assert_eq!(
            blueprint1.blueprint_zones.len() + 1,
            blueprint2.blueprint_zones.len()
        );
        assert_eq!(
            blueprint1.all_omicron_zones(BlueprintZoneFilter::All).count()
                + num_new_sled_zones,
            blueprint2.all_omicron_zones(BlueprintZoneFilter::All).count()
        );

        // All zones should be in service.
        assert_all_zones_in_service(&blueprint2);
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
        let diff = blueprint_read.diff_since_blueprint(&blueprint2);
        println!("diff: {}", diff.display());
        assert_eq!(blueprint2, blueprint_read);
        assert_eq!(blueprint2.internal_dns_version, new_internal_dns_version);
        assert_eq!(blueprint2.external_dns_version, new_external_dns_version);
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
            (bp2_target, blueprint2.clone())
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

        // There should be no current target; this is never expected in a real
        // system, since RSS sets an initial target blueprint, so we should get
        // an error.
        let err = datastore
            .blueprint_target_get_current_full(&opctx)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no target blueprint set"));

        // Create an initial empty collection
        let collection = CollectionBuilder::new("test").build();

        // Create three blueprints:
        // * `blueprint1` has no parent
        // * `blueprint2` and `blueprint3` both have `blueprint1` as parent
        let blueprint1 = BlueprintBuilder::build_empty_with_sleds(
            std::iter::empty(),
            "test1",
        );
        let blueprint2 = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint1,
            &EMPTY_PLANNING_INPUT,
            &collection,
            "test2",
        )
        .expect("failed to create builder")
        .build();
        let blueprint3 = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint1,
            &EMPTY_PLANNING_INPUT,
            &collection,
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

        // There should be no current target; this is never expected in a real
        // system, since RSS sets an initial target blueprint, so we should get
        // an error.
        let err = datastore
            .blueprint_target_get_current_full(&opctx)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no target blueprint set"));

        // We should be able to insert blueprint1, which has no parent (matching
        // the currently-empty `bp_target` table's lack of a target).
        datastore
            .blueprint_target_set_current(&opctx, bp1_target)
            .await
            .unwrap();
        assert_eq!(
            datastore.blueprint_target_get_current_full(&opctx).await.unwrap(),
            (bp1_target, blueprint1.clone())
        );

        // Now that blueprint1 is the current target, we should be able to
        // insert blueprint2 or blueprint3. WLOG, pick blueprint3.
        datastore
            .blueprint_target_set_current(&opctx, bp3_target)
            .await
            .unwrap();
        assert_eq!(
            datastore.blueprint_target_get_current_full(&opctx).await.unwrap(),
            (bp3_target, blueprint3.clone())
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
            &EMPTY_PLANNING_INPUT,
            &collection,
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
            (bp4_target, blueprint4)
        );

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_set_target_enabled() {
        // Setup
        let logctx = dev::test_setup_log("test_set_target_enabled");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Create an initial empty collection
        let collection = CollectionBuilder::new("test").build();

        // Create an initial blueprint and a child.
        let blueprint1 = BlueprintBuilder::build_empty_with_sleds(
            std::iter::empty(),
            "test1",
        );
        let blueprint2 = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint1,
            &EMPTY_PLANNING_INPUT,
            &collection,
            "test2",
        )
        .expect("failed to create builder")
        .build();
        assert_eq!(blueprint1.parent_blueprint_id, None);
        assert_eq!(blueprint2.parent_blueprint_id, Some(blueprint1.id));

        // Insert both into the blueprint table.
        datastore.blueprint_insert(&opctx, &blueprint1).await.unwrap();
        datastore.blueprint_insert(&opctx, &blueprint2).await.unwrap();

        let mut bp1_target = BlueprintTarget {
            target_id: blueprint1.id,
            enabled: true,
            time_made_target: now_db_precision(),
        };
        let mut bp2_target = BlueprintTarget {
            target_id: blueprint2.id,
            enabled: true,
            time_made_target: now_db_precision(),
        };

        // Set bp1_target as the current target.
        datastore
            .blueprint_target_set_current(&opctx, bp1_target)
            .await
            .unwrap();
        assert_eq!(
            datastore.blueprint_target_get_current(&opctx).await.unwrap(),
            bp1_target,
        );

        // We should be able to toggle its enabled status an arbitrary number of
        // times.
        for _ in 0..10 {
            bp1_target.enabled = !bp1_target.enabled;
            datastore
                .blueprint_target_set_current_enabled(&opctx, bp1_target)
                .await
                .unwrap();
            assert_eq!(
                datastore.blueprint_target_get_current(&opctx).await.unwrap(),
                bp1_target,
            );
        }

        // We cannot use `blueprint_target_set_current_enabled` to make
        // bp2_target the target...
        let err = datastore
            .blueprint_target_set_current_enabled(&opctx, bp2_target)
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("is not the current target blueprint"));

        // ...but we can make it the target via `blueprint_target_set_current`.
        datastore
            .blueprint_target_set_current(&opctx, bp2_target)
            .await
            .unwrap();
        assert_eq!(
            datastore.blueprint_target_get_current(&opctx).await.unwrap(),
            bp2_target,
        );

        // We can no longer toggle the enabled bit of bp1_target.
        let err = datastore
            .blueprint_target_set_current_enabled(&opctx, bp1_target)
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("is not the current target blueprint"));

        // We can toggle bp2_target.enabled an arbitrary number of times.
        for _ in 0..10 {
            bp2_target.enabled = !bp2_target.enabled;
            datastore
                .blueprint_target_set_current_enabled(&opctx, bp2_target)
                .await
                .unwrap();
            assert_eq!(
                datastore.blueprint_target_get_current(&opctx).await.unwrap(),
                bp2_target,
            );
        }

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_ensure_external_networking_bails_on_bad_target() {
        // Setup
        let logctx = dev::test_setup_log(
            "test_ensure_external_networking_bails_on_bad_target",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        // Create an initial empty collection
        let collection = CollectionBuilder::new("test").build();

        // Create an initial blueprint and a child.
        let blueprint1 = BlueprintBuilder::build_empty_with_sleds(
            std::iter::empty(),
            "test1",
        );
        let blueprint2 = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint1,
            &EMPTY_PLANNING_INPUT,
            &collection,
            "test2",
        )
        .expect("failed to create builder")
        .build();

        // Insert both into the blueprint table.
        datastore.blueprint_insert(&opctx, &blueprint1).await.unwrap();
        datastore.blueprint_insert(&opctx, &blueprint2).await.unwrap();

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

        // Set bp1_target as the current target.
        datastore
            .blueprint_target_set_current(&opctx, bp1_target)
            .await
            .unwrap();

        // Attempting to ensure the (empty) resources for bp1 should succeed.
        datastore
            .blueprint_ensure_external_networking_resources(&opctx, &blueprint1)
            .await
            .expect("ensured networking resources for empty blueprint 1");

        // Attempting to ensure the (empty) resources for bp2 should fail,
        // because it isn't the target blueprint.
        let err = datastore
            .blueprint_ensure_external_networking_resources(&opctx, &blueprint2)
            .await
            .expect_err("failed because blueprint 2 isn't the target");
        assert!(
            err.to_string().contains("is not the current target blueprint"),
            "unexpected error: {err}"
        );

        // Create flags to control method execution.
        let target_check_done = Arc::new(AtomicBool::new(false));
        let return_on_completion = Arc::new(AtomicBool::new(false));

        // Spawn a task to execute our method.
        let mut ensure_resources_task = tokio::spawn({
            let datastore = datastore.clone();
            let opctx =
                OpContext::for_tests(logctx.log.clone(), datastore.clone());
            let target_check_done = target_check_done.clone();
            let return_on_completion = return_on_completion.clone();
            async move {
                datastore
                    .blueprint_ensure_external_networking_resources_impl(
                        &opctx,
                        &blueprint1,
                        Some(target_check_done),
                        Some(return_on_completion),
                    )
                    .await
            }
        });

        // Wait until `task` has proceeded past the point at which it's checked
        // the target blueprint.
        wait_for_condition(
            || async {
                if target_check_done.load(Ordering::SeqCst) {
                    Ok(())
                } else {
                    Err(CondCheckError::<()>::NotYet)
                }
            },
            &Duration::from_millis(50),
            &Duration::from_secs(10),
        )
        .await
        .expect("`target_check_done` not set to true");

        // Spawn another task that tries to read the current target. This should
        // block at the database level due to the `SELECT ... FOR UPDATE` inside
        // `blueprint_ensure_external_networking_resources`.
        let mut current_target_task = tokio::spawn({
            let datastore = datastore.clone();
            let opctx =
                OpContext::for_tests(logctx.log.clone(), datastore.clone());
            async move {
                datastore
                    .blueprint_target_get_current(&opctx)
                    .await
                    .expect("read current target")
            }
        });

        // Spawn another task that tries to set the current target. This should
        // block at the database level due to the `SELECT ... FOR UPDATE` inside
        // `blueprint_ensure_external_networking_resources`.
        let mut update_target_task = tokio::spawn({
            let datastore = datastore.clone();
            let opctx =
                OpContext::for_tests(logctx.log.clone(), datastore.clone());
            async move {
                datastore.blueprint_target_set_current(&opctx, bp2_target).await
            }
        });

        // None of our spawned tasks should be able to make progress:
        // `ensure_resources_task` is waiting for us to set
        // `return_on_completion` to true, and the other two should be
        // queued by Cockroach, because
        // `blueprint_ensure_external_networking_resources` should have
        // performed a `SELECT ... FOR UPDATE` on the current target, forcing
        // the query that wants to change it to wait until the transaction
        // completes.
        //
        // We'll somewhat haphazardly test this by trying to wait for any
        // task to finish, and succeeding on a timeout of a few seconds. This
        // could spuriously succeed if we're executing on a very overloaded
        // system where we hit the timeout even though one of the tasks is
        // actually making progress, but hopefully will fail often enough if
        // we've gotten this wrong.
        tokio::select! {
            result = &mut ensure_resources_task => {
                panic!(
                    "unexpected completion of \
                     `blueprint_ensure_external_networking_resources`: \
                     {result:?}",
                );
            }
            result = &mut update_target_task => {
                panic!(
                    "unexpected completion of \
                     `blueprint_target_set_current`: {result:?}",
                );
            }
            result = &mut current_target_task => {
                panic!(
                    "unexpected completion of \
                     `blueprint_target_get_current`: {result:?}",
                );
            }
            _ = tokio::time::sleep(Duration::from_secs(5)) => (),
        }

        // Release `ensure_resources_task` to finish.
        return_on_completion.store(true, Ordering::SeqCst);

        tokio::time::timeout(Duration::from_secs(10), ensure_resources_task)
            .await
            .expect(
                "time out waiting for \
                `blueprint_ensure_external_networking_resources`",
            )
            .expect("panic in `blueprint_ensure_external_networking_resources")
            .expect("ensured networking resources for empty blueprint 2");

        // Our other tasks should now also complete.
        tokio::time::timeout(Duration::from_secs(10), update_target_task)
            .await
            .expect("time out waiting for `blueprint_target_set_current`")
            .expect("panic in `blueprint_target_set_current")
            .expect("updated target to blueprint 2");
        tokio::time::timeout(Duration::from_secs(10), current_target_task)
            .await
            .expect("time out waiting for `blueprint_target_get_current`")
            .expect("panic in `blueprint_target_get_current");

        // Clean up.
        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    fn assert_all_zones_in_service(blueprint: &Blueprint) {
        let not_in_service = blueprint
            .all_omicron_zones(BlueprintZoneFilter::All)
            .filter(|(_, z)| {
                z.disposition != BlueprintZoneDisposition::InService
            })
            .collect::<Vec<_>>();
        assert!(
            not_in_service.is_empty(),
            "expected all zones to be in service, \
             found these zones not in service: {not_in_service:?}"
        );
    }
}
