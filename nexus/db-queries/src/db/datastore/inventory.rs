// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::pagination::{Paginator, paginated, paginated_multicolumn};
use crate::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
use anyhow::Context;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::AsyncSimpleConnection;
use clickhouse_admin_types::ClickhouseKeeperClusterMembership;
use diesel::BoolExpressionMethods;
use diesel::ExpressionMethods;
use diesel::IntoSql;
use diesel::JoinOnDsl;
use diesel::NullableExpressionMethods;
use diesel::OptionalExtension;
use diesel::PgExpressionMethods;
use diesel::QueryDsl;
use diesel::Table;
use diesel::expression::SelectableHelper;
use diesel::sql_types::Nullable;
use futures::FutureExt;
use futures::future::BoxFuture;
use id_map::{IdMap, IdMappable};
use iddqd::IdOrdMap;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_errors::public_error_from_diesel_lookup;
use nexus_db_model::ArtifactHash;
use nexus_db_model::HwM2Slot;
use nexus_db_model::InvClickhouseKeeperMembership;
use nexus_db_model::InvCockroachStatus;
use nexus_db_model::InvCollection;
use nexus_db_model::InvCollectionError;
use nexus_db_model::InvConfigReconcilerStatus;
use nexus_db_model::InvConfigReconcilerStatusKind;
use nexus_db_model::InvDataset;
use nexus_db_model::InvHostPhase1FlashHash;
use nexus_db_model::InvLastReconciliationDatasetResult;
use nexus_db_model::InvLastReconciliationDiskResult;
use nexus_db_model::InvLastReconciliationOrphanedDataset;
use nexus_db_model::InvLastReconciliationZoneResult;
use nexus_db_model::InvNtpTimesync;
use nexus_db_model::InvNvmeDiskFirmware;
use nexus_db_model::InvOmicronSledConfig;
use nexus_db_model::InvOmicronSledConfigDataset;
use nexus_db_model::InvOmicronSledConfigDisk;
use nexus_db_model::InvOmicronSledConfigZone;
use nexus_db_model::InvOmicronSledConfigZoneNic;
use nexus_db_model::InvPhysicalDisk;
use nexus_db_model::InvRootOfTrust;
use nexus_db_model::InvRotPage;
use nexus_db_model::InvServiceProcessor;
use nexus_db_model::InvSledAgent;
use nexus_db_model::InvSledBootPartition;
use nexus_db_model::InvSledConfigReconciler;
use nexus_db_model::InvZpool;
use nexus_db_model::RotImageError;
use nexus_db_model::SledRole;
use nexus_db_model::SpType;
use nexus_db_model::SqlU16;
use nexus_db_model::SqlU32;
use nexus_db_model::SwCaboose;
use nexus_db_model::SwRotPage;
use nexus_db_model::to_db_typed_uuid;
use nexus_db_model::{
    HwBaseboardId, InvZoneImageResolver, InvZoneManifestZone,
};
use nexus_db_model::{HwPowerState, InvZoneManifestNonBoot};
use nexus_db_model::{HwRotSlot, InvMupdateOverrideNonBoot};
use nexus_db_model::{InvCaboose, InvClearMupdateOverride};
use nexus_db_schema::enums::HwM2SlotEnum;
use nexus_db_schema::enums::HwRotSlotEnum;
use nexus_db_schema::enums::RotImageErrorEnum;
use nexus_db_schema::enums::RotPageWhichEnum;
use nexus_db_schema::enums::SledRoleEnum;
use nexus_db_schema::enums::SpTypeEnum;
use nexus_db_schema::enums::{
    CabooseWhichEnum, InvConfigReconcilerStatusKindEnum,
};
use nexus_db_schema::enums::{HwPowerStateEnum, InvZoneManifestSourceEnum};
use nexus_sled_agent_shared::inventory::BootPartitionContents;
use nexus_sled_agent_shared::inventory::BootPartitionDetails;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventory;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryResult;
use nexus_sled_agent_shared::inventory::ConfigReconcilerInventoryStatus;
use nexus_sled_agent_shared::inventory::MupdateOverrideNonBootInventory;
use nexus_sled_agent_shared::inventory::OmicronSledConfig;
use nexus_sled_agent_shared::inventory::OrphanedDataset;
use nexus_sled_agent_shared::inventory::ZoneArtifactInventory;
use nexus_sled_agent_shared::inventory::ZoneManifestNonBootInventory;
use nexus_types::inventory::BaseboardId;
use nexus_types::inventory::CockroachStatus;
use nexus_types::inventory::Collection;
use nexus_types::inventory::PhysicalDiskFirmware;
use nexus_types::inventory::SledAgent;
use nexus_types::inventory::TimeSync;
use omicron_cockroach_metrics::NodeId as CockroachNodeId;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::bail_unless;
use omicron_common::disk::M2Slot;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronSledConfigUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::sync::Arc;
use uuid::Uuid;

/// "limit" used in SQL queries that paginate through all SPs, RoTs, sleds,
/// omicron zones, etc.
///
/// We use a [`Paginator`] to guard against single queries returning an
/// unchecked number of rows.
// unsafe: `new_unchecked` is only unsound if the argument is 0.
const SQL_BATCH_SIZE: NonZeroU32 = NonZeroU32::new(1000).unwrap();

impl DataStore {
    /// Store a complete inventory collection into the database
    pub async fn inventory_insert_collection(
        &self,
        opctx: &OpContext,
        collection: &Collection,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::INVENTORY).await?;

        // In the database, the collection is represented essentially as a tree
        // rooted at an `inv_collection` row.  Other nodes in the tree point
        // back at the `inv_collection` via `inv_collection_id`.
        //
        // It's helpful to assemble some values before entering the transaction
        // so that we can produce the `Error` type that we want here.
        let row_collection = InvCollection::from(collection);
        let collection_id = row_collection.id();
        let db_collection_id = to_db_typed_uuid(collection_id);
        let baseboards = collection
            .baseboards
            .iter()
            .map(|b| HwBaseboardId::from((**b).clone()))
            .collect::<Vec<_>>();
        let cabooses = collection
            .cabooses
            .iter()
            .map(|s| SwCaboose::from((**s).clone()))
            .collect::<Vec<_>>();
        let rot_pages = collection
            .rot_pages
            .iter()
            .map(|p| SwRotPage::from((**p).clone()))
            .collect::<Vec<_>>();
        let error_values = collection
            .errors
            .iter()
            .enumerate()
            .map(|(i, message)| {
                let index = u16::try_from(i).map_err(|e| {
                    Error::internal_error(&format!(
                        "failed to convert error index to u16 (too \
                        many errors in inventory collection?): {}",
                        e
                    ))
                })?;
                Ok(InvCollectionError::new(
                    collection_id,
                    index,
                    message.clone(),
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;

        // Pull disk firmware out of sled agents
        let mut nvme_disk_firmware = Vec::new();
        for sled_agent in &collection.sled_agents {
            for disk in &sled_agent.disks {
                match &disk.firmware {
                    PhysicalDiskFirmware::Unknown => (),
                    PhysicalDiskFirmware::Nvme(firmware) => nvme_disk_firmware
                        .push(
                            InvNvmeDiskFirmware::new(
                                collection_id,
                                sled_agent.sled_id,
                                disk.slot,
                                firmware,
                            )
                            .map_err(|e| {
                                Error::internal_error(&e.to_string())
                            })?,
                        ),
                }
            }
        }

        // Pull disks out of all sled agents
        let disks: Vec<_> = collection
            .sled_agents
            .iter()
            .flat_map(|sled_agent| {
                sled_agent.disks.iter().map(|disk| {
                    InvPhysicalDisk::new(
                        collection_id,
                        sled_agent.sled_id,
                        disk.clone(),
                    )
                })
            })
            .collect();

        // Pull zpools out of all sled agents
        let zpools: Vec<_> = collection
            .sled_agents
            .iter()
            .flat_map(|sled_agent| {
                sled_agent.zpools.iter().map(|pool| {
                    InvZpool::new(collection_id, sled_agent.sled_id, pool)
                })
            })
            .collect();

        // Pull datasets out of all sled agents
        let datasets: Vec<_> = collection
            .sled_agents
            .iter()
            .flat_map(|sled_agent| {
                sled_agent.datasets.iter().map(|dataset| {
                    InvDataset::new(collection_id, sled_agent.sled_id, dataset)
                })
            })
            .collect();

        // Pull zone manifest zones out of all sled agents.
        let zone_manifest_zones: Vec<_> = collection
            .sled_agents
            .iter()
            .filter_map(|sled_agent| {
                sled_agent
                    .zone_image_resolver
                    .zone_manifest
                    .boot_inventory
                    .as_ref()
                    .ok()
                    .map(|artifacts| {
                        artifacts.artifacts.iter().map(|artifact| {
                            InvZoneManifestZone::new(
                                collection_id,
                                sled_agent.sled_id,
                                artifact,
                            )
                        })
                    })
            })
            .flatten()
            .collect();

        // Pull zone manifest non-boot info out of all sled agents.
        let zone_manifest_non_boot: Vec<_> = collection
            .sled_agents
            .iter()
            .flat_map(|sled_agent| {
                sled_agent
                    .zone_image_resolver
                    .zone_manifest
                    .non_boot_status
                    .iter()
                    .map(|non_boot| {
                        InvZoneManifestNonBoot::new(
                            collection_id,
                            sled_agent.sled_id,
                            non_boot,
                        )
                    })
            })
            .collect();

        // Pull mupdate override non-boot info out of all sled agents.
        let mupdate_override_non_boot: Vec<_> = collection
            .sled_agents
            .iter()
            .flat_map(|sled_agent| {
                sled_agent
                    .zone_image_resolver
                    .mupdate_override
                    .non_boot_status
                    .iter()
                    .map(|non_boot| {
                        InvMupdateOverrideNonBoot::new(
                            collection_id,
                            sled_agent.sled_id,
                            non_boot,
                        )
                    })
            })
            .collect();

        // Build up a list of `OmicronSledConfig`s we need to insert. Each sled
        // has 0-3:
        //
        // * The ledgered sled config (if the sled has gotten a config from RSS
        //   or Nexus)
        // * The most-recently-reconciled config (if the sled-agent's config
        //   reconciler has run since the last time it started)
        // * The currently-being-reconciled config (if the sled-agent's config
        //   reconciler was actively running when inventory was collected)
        //
        // If more than one of these are present, they may be equal or distinct;
        // for any that are equal, we'll only insert one copy and reuse the
        // foreign key ID for subsequent instances.
        //
        // For each of these configs, we need to insert the top-level scalar
        // data (held in an `InvOmicronSledConfig`), plus all of their zones,
        // zone NICs, datasets, and disks, and we need collect the config
        // reconciler properties for each sled. We need all of this to construct
        // `InvSledAgent` instances below.
        let ConfigReconcilerRows {
            config_reconcilers,
            sled_configs: omicron_sled_configs,
            disks: omicron_sled_config_disks,
            datasets: omicron_sled_config_datasets,
            zones: omicron_sled_config_zones,
            zone_nics: omicron_sled_config_zone_nics,
            disk_results: reconciler_disk_results,
            dataset_results: reconciler_dataset_results,
            orphaned_datasets: reconciler_orphaned_datasets,
            zone_results: reconciler_zone_results,
            boot_partitions: reconciler_boot_partitions,
            mut config_reconciler_fields_by_sled,
        } = ConfigReconcilerRows::new(collection_id, collection)
            .map_err(|e| Error::internal_error(&format!("{e:#}")))?;

        // Partition the sled agents into those with an associated baseboard id
        // and those without one.  We handle these pretty differently.
        let (sled_agents_baseboards, sled_agents_no_baseboards): (
            Vec<_>,
            Vec<_>,
        ) = collection
            .sled_agents
            .iter()
            .partition(|sled_agent| sled_agent.baseboard_id.is_some());
        let sled_agents_no_baseboards = sled_agents_no_baseboards
            .into_iter()
            .map(|sled_agent| {
                assert!(sled_agent.baseboard_id.is_none());
                let ConfigReconcilerFields {
                    ledgered_sled_config,
                    reconciler_status,
                } = config_reconciler_fields_by_sled
                    .remove(&sled_agent.sled_id)
                    .expect("all sled IDs should exist");
                let zone_image_resolver =
                    InvZoneImageResolver::new(&sled_agent.zone_image_resolver);
                InvSledAgent::new_without_baseboard(
                    collection_id,
                    sled_agent,
                    ledgered_sled_config,
                    reconciler_status,
                    zone_image_resolver,
                )
                .map_err(|e| Error::internal_error(&e.to_string()))
            })
            .collect::<Result<Vec<_>, Error>>()?;

        let mut inv_clickhouse_keeper_memberships = Vec::new();
        for membership in &collection.clickhouse_keeper_cluster_membership {
            inv_clickhouse_keeper_memberships.push(
                InvClickhouseKeeperMembership::new(
                    collection_id,
                    membership.clone(),
                )
                .map_err(|e| Error::internal_error(&e.to_string()))?,
            );
        }

        let inv_cockroach_status_records: Vec<InvCockroachStatus> = collection
            .cockroach_status
            .iter()
            .map(|(node_id, status)| {
                InvCockroachStatus::new(collection_id, node_id.clone(), status)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| Error::internal_error(&e.to_string()))?;

        let inv_ntp_timesync_records: Vec<InvNtpTimesync> = collection
            .ntp_timesync
            .iter()
            .map(|timesync| InvNtpTimesync::new(collection_id, timesync))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| Error::internal_error(&e.to_string()))?;

        // This implementation inserts all records associated with the
        // collection in one transaction.  This is primarily for simplicity.  It
        // means we don't have to worry about other readers seeing a
        // half-inserted collection, nor leaving detritus around if we start
        // inserting records and then crash.  However, it does mean this is
        // likely to be a big transaction and if that becomes a problem we could
        // break this up as long as we address those problems.
        //
        // The SQL here is written so that it doesn't have to be an
        // *interactive* transaction.  That is, it should in principle be
        // possible to generate all this SQL up front and send it as one big
        // batch rather than making a bunch of round-trips to the database.
        // We'd do that if we had an interface for doing that with bound
        // parameters, etc.  See oxidecomputer/omicron#973.
        let conn = self.pool_connection_authorized(opctx).await?;

        // The risk of a serialization error is possible here, but low,
        // as most of the operations should be insertions rather than in-place
        // modifications of existing tables.
        self.transaction_non_retry_wrapper("inventory_insert_collection")
            .transaction(&conn, |conn| async move {
            // Insert records (and generate ids) for any baseboards that do not
            // already exist in the database.  These rows are not scoped to a
            // particular collection.  They contain only immutable data --
            // they're just a mapping between hardware-provided baseboard
            // identifiers (part number and model number) and an
            // Omicron-specific primary key (a UUID).
            {
                use nexus_db_schema::schema::hw_baseboard_id::dsl;
                let _ = diesel::insert_into(dsl::hw_baseboard_id)
                    .values(baseboards)
                    .on_conflict_do_nothing()
                    .execute_async(&conn)
                    .await?;
            }

            // Insert records (and generate ids) for each distinct caboose that
            // we've found.  Like baseboards, these might already be present and
            // rows in this table are not scoped to a particular collection
            // because they only map (immutable) identifiers to UUIDs.
            {
                use nexus_db_schema::schema::sw_caboose::dsl;
                let _ = diesel::insert_into(dsl::sw_caboose)
                    .values(cabooses)
                    .on_conflict_do_nothing()
                    .execute_async(&conn)
                    .await?;
            }

            // Insert records (and generate ids) for each distinct RoT page that
            // we've found.  Like baseboards, these might already be present and
            // rows in this table are not scoped to a particular collection
            // because they only map (immutable) identifiers to UUIDs.
            {
                use nexus_db_schema::schema::sw_root_of_trust_page::dsl;
                let _ = diesel::insert_into(dsl::sw_root_of_trust_page)
                    .values(rot_pages)
                    .on_conflict_do_nothing()
                    .execute_async(&conn)
                    .await?;
            }

            // Insert a record describing the collection itself.
            {
                use nexus_db_schema::schema::inv_collection::dsl;
                let _ = diesel::insert_into(dsl::inv_collection)
                    .values(row_collection)
                    .execute_async(&conn)
                    .await?;
            }

            // Insert rows for the service processors we found.  These have a
            // foreign key into the hw_baseboard_id table.  We don't have those
            // id values, though.  We may have just inserted them, or maybe not
            // (if they previously existed).  To avoid dozens of unnecessary
            // round-trips, we use INSERT INTO ... SELECT, which looks like
            // this:
            //
            //   INSERT INTO inv_service_processor
            //       SELECT
            //           id
            //           [other service_processor column values as literals]
            //         FROM hw_baseboard_id
            //         WHERE part_number = ... AND serial_number = ...;
            //
            // This way, we don't need to know the id.  The database looks it up
            // for us as it does the INSERT.
            {
                use nexus_db_schema::schema::hw_baseboard_id::dsl as baseboard_dsl;
                use nexus_db_schema::schema::inv_service_processor::dsl as sp_dsl;

                for (baseboard_id, sp) in &collection.sps {
                    let selection = nexus_db_schema::schema::hw_baseboard_id::table
                        .select((
                            db_collection_id
                                .into_sql::<diesel::sql_types::Uuid>(),
                            baseboard_dsl::id,
                            sp.time_collected
                                .into_sql::<diesel::sql_types::Timestamptz>(),
                            sp.source
                                .clone()
                                .into_sql::<diesel::sql_types::Text>(),
                            SpType::from(sp.sp_type).into_sql::<SpTypeEnum>(),
                            i32::from(sp.sp_slot)
                                .into_sql::<diesel::sql_types::Int4>(),
                            i64::from(sp.baseboard_revision)
                                .into_sql::<diesel::sql_types::Int8>(),
                            sp.hubris_archive
                                .clone()
                                .into_sql::<diesel::sql_types::Text>(),
                            HwPowerState::from(sp.power_state)
                                .into_sql::<HwPowerStateEnum>(),
                        ))
                        .filter(
                            baseboard_dsl::part_number
                                .eq(baseboard_id.part_number.clone()),
                        )
                        .filter(
                            baseboard_dsl::serial_number
                                .eq(baseboard_id.serial_number.clone()),
                        );

                    let _ = diesel::insert_into(
                        nexus_db_schema::schema::inv_service_processor::table,
                    )
                    .values(selection)
                    .into_columns((
                        sp_dsl::inv_collection_id,
                        sp_dsl::hw_baseboard_id,
                        sp_dsl::time_collected,
                        sp_dsl::source,
                        sp_dsl::sp_type,
                        sp_dsl::sp_slot,
                        sp_dsl::baseboard_revision,
                        sp_dsl::hubris_archive_id,
                        sp_dsl::power_state,
                    ))
                    .execute_async(&conn)
                    .await?;

                    // This statement is just here to force a compilation error
                    // if the set of columns in `inv_service_processor` changes.
                    // The code above attempts to insert a row into
                    // `inv_service_processor` using an explicit list of columns
                    // and values.  Without the following statement, If a new
                    // required column were added, this would only fail at
                    // runtime.
                    //
                    // If you're here because of a compile error, you might be
                    // changing the `inv_service_processor` table.  Update the
                    // statement below and be sure to update the code above,
                    // too!
                    //
                    // See also similar comments in blocks below, near other
                    // uses of `all_columns().
                    let (
                        _inv_collection_id,
                        _hw_baseboard_id,
                        _time_collected,
                        _source,
                        _sp_type,
                        _sp_slot,
                        _baseboard_revision,
                        _hubris_archive_id,
                        _power_state,
                    ) = sp_dsl::inv_service_processor::all_columns();
                }
            }

            // Insert rows for the roots of trust that we found.  Like service
            // processors, we do this using INSERT INTO ... SELECT.
            {
                use nexus_db_schema::schema::hw_baseboard_id::dsl as baseboard_dsl;
                use nexus_db_schema::schema::inv_root_of_trust::dsl as rot_dsl;

                for (baseboard_id, rot) in &collection.rots {
                    let selection = nexus_db_schema::schema::hw_baseboard_id::table
                        .select((
                            db_collection_id
                                .into_sql::<diesel::sql_types::Uuid>(),
                            baseboard_dsl::id,
                            rot.time_collected
                                .into_sql::<diesel::sql_types::Timestamptz>(),
                            rot.source
                                .clone()
                                .into_sql::<diesel::sql_types::Text>(),
                            HwRotSlot::from(rot.active_slot)
                                .into_sql::<HwRotSlotEnum>(),
                            HwRotSlot::from(rot.persistent_boot_preference)
                                .into_sql::<HwRotSlotEnum>(),
                            rot.pending_persistent_boot_preference
                                .map(HwRotSlot::from)
                                .into_sql::<Nullable<HwRotSlotEnum>>(),
                            rot.transient_boot_preference
                                .map(HwRotSlot::from)
                                .into_sql::<Nullable<HwRotSlotEnum>>(),
                            rot.slot_a_sha3_256_digest
                                .clone()
                                .into_sql::<Nullable<diesel::sql_types::Text>>(
                                ),
                            rot.slot_b_sha3_256_digest
                                .clone()
                                .into_sql::<Nullable<diesel::sql_types::Text>>(
                                ),
                            rot.stage0_digest
                                .clone()
                                .into_sql::<Nullable<diesel::sql_types::Text>>(
                                ),
                            rot.stage0next_digest
                                .clone()
                                .into_sql::<Nullable<diesel::sql_types::Text>>(
                                ),
                            rot.slot_a_error
                                .map(RotImageError::from)
                                .into_sql::<Nullable<RotImageErrorEnum>>(),
                            rot.slot_b_error
                                .map(RotImageError::from)
                                .into_sql::<Nullable<RotImageErrorEnum>>(),
                            rot.stage0_error
                                .map(RotImageError::from)
                                .into_sql::<Nullable<RotImageErrorEnum>>(),
                            rot.stage0next_error
                                .map(RotImageError::from)
                                .into_sql::<Nullable<RotImageErrorEnum>>(),
                        ))
                        .filter(
                            baseboard_dsl::part_number
                                .eq(baseboard_id.part_number.clone()),
                        )
                        .filter(
                            baseboard_dsl::serial_number
                                .eq(baseboard_id.serial_number.clone()),
                        );

                    let _ = diesel::insert_into(
                        nexus_db_schema::schema::inv_root_of_trust::table,
                    )
                    .values(selection)
                    .into_columns((
                        rot_dsl::inv_collection_id,
                        rot_dsl::hw_baseboard_id,
                        rot_dsl::time_collected,
                        rot_dsl::source,
                        rot_dsl::slot_active,
                        rot_dsl::slot_boot_pref_persistent,
                        rot_dsl::slot_boot_pref_persistent_pending,
                        rot_dsl::slot_boot_pref_transient,
                        rot_dsl::slot_a_sha3_256,
                        rot_dsl::slot_b_sha3_256,
                        rot_dsl::stage0_fwid,
                        rot_dsl::stage0next_fwid,
                        rot_dsl::slot_a_error,
                        rot_dsl::slot_b_error,
                        rot_dsl::stage0_error,
                        rot_dsl::stage0next_error,
                    ))
                    .execute_async(&conn)
                    .await?;

                    // See the comment in the previous block (where we use
                    // `inv_service_processor::all_columns()`).  The same
                    // applies here.
                    let (
                        _inv_collection_id,
                        _hw_baseboard_id,
                        _time_collected,
                        _source,
                        _slot_active,
                        _slot_boot_pref_persistent,
                        _slot_boot_pref_persistent_pending,
                        _slot_boot_pref_transient,
                        _slot_a_sha3_256,
                        _slot_b_sha3_256,
                        _stage0_fwid,
                        _stage0next_fwid,
                        _slot_a_error,
                        _slot_b_error,
                        _stage0_error,
                        _stage0next_error,
                    ) = rot_dsl::inv_root_of_trust::all_columns();
                }
            }

            // Insert rows for the host phase 1 flash hashes that we found.
            // Like service processors, we do this using INSERT INTO ... SELECT.
            {
                use nexus_db_schema::schema::hw_baseboard_id::dsl as baseboard_dsl;
                use nexus_db_schema::schema::inv_host_phase_1_flash_hash::dsl as phase1_dsl;

                // Squish our map-of-maps down to a flat iterator.
                //
                // We can throw away the `_slot` key because the `phase1`
                // structures also contain their own slot. (Maybe we could use
                // `iddqd` here instead?)
                let phase1_hashes = collection
                    .host_phase_1_flash_hashes
                    .iter()
                    .flat_map(|(_slot, by_baseboard)| by_baseboard.iter());

                for (baseboard_id, phase1) in phase1_hashes {
                    let selection = nexus_db_schema::schema::hw_baseboard_id::table
                        .select((
                            db_collection_id
                                .into_sql::<diesel::sql_types::Uuid>(),
                            baseboard_dsl::id,
                            phase1.time_collected
                                .into_sql::<diesel::sql_types::Timestamptz>(),
                            phase1.source
                                .clone()
                                .into_sql::<diesel::sql_types::Text>(),
                            HwM2Slot::from(phase1.slot)
                                .into_sql::<HwM2SlotEnum>(),
                            ArtifactHash(phase1.hash)
                                .into_sql::<diesel::sql_types::Text>(),
                        ))
                        .filter(
                            baseboard_dsl::part_number
                                .eq(baseboard_id.part_number.clone()),
                        )
                        .filter(
                            baseboard_dsl::serial_number
                                .eq(baseboard_id.serial_number.clone()),
                        );

                    let _ = diesel::insert_into(
                        nexus_db_schema::schema::inv_host_phase_1_flash_hash::table,
                    )
                    .values(selection)
                    .into_columns((
                        phase1_dsl::inv_collection_id,
                        phase1_dsl::hw_baseboard_id,
                        phase1_dsl::time_collected,
                        phase1_dsl::source,
                        phase1_dsl::slot,
                        phase1_dsl::hash,
                    ))
                    .execute_async(&conn)
                    .await?;

                    // See the comment in the above block (where we use
                    // `inv_service_processor::all_columns()`).  The same
                    // applies here.
                    let (
                        _inv_collection_id,
                        _hw_baseboard_id,
                        _time_collected,
                        _source,
                        _slot,
                        _hash,
                    ) = phase1_dsl::inv_host_phase_1_flash_hash::all_columns();
                }
            }

            // Insert rows for the cabooses that we found.  Like service
            // processors and roots of trust, we do this using INSERT INTO ...
            // SELECT.  This one's a little more complicated because there are
            // two foreign keys.  Concretely, we have these three tables:
            //
            // - `hw_baseboard` with an "id" primary key and lookup columns
            //   "part_number" and "serial_number"
            // - `sw_caboose` with an "id" primary key and lookup columns
            //   "board", "git_commit", "name", "version", and "sign"
            // - `inv_caboose` with foreign keys "hw_baseboard_id",
            //   "sw_caboose_id", and various other columns
            //
            // We want to INSERT INTO `inv_caboose` a row with:
            //
            // - hw_baseboard_id (foreign key) the result of looking up an
            //   hw_baseboard row by a specific part number and serial number
            //
            // - sw_caboose_id (foreign key) the result of looking up a
            //   specific sw_caboose row by board, git_commit, name, and version
            //
            // - the other columns being literals
            //
            // To achieve this, we're going to generate something like:
            //
            //     INSERT INTO
            //         inv_caboose (
            //             hw_baseboard_id,
            //             sw_caboose_id,
            //             inv_collection_id,
            //             time_collected,
            //             source,
            //             which,
            //         )
            //         SELECT (
            //             hw_baseboard_id.id,
            //             sw_caboose.id,
            //             ...              /* literal collection id */
            //             ...              /* literal time collected */
            //             ...              /* literal source */
            //             ...              /* literal 'which' */
            //         )
            //         FROM
            //             hw_baseboard
            //         INNER JOIN
            //             sw_caboose
            //         ON  hw_baseboard.part_number = ...
            //         AND hw_baseboard.serial_number = ...
            //         AND sw_caboose.board = ...
            //         AND sw_caboose.git_commit = ...
            //         AND sw_caboose.name = ...
            //         AND sw_caboose.version = ...
            //         AND sw_caboose.sign IS NOT DISTINCT FROM ...;
            //
            // Again, the whole point is to avoid back-and-forth between the
            // client and the database.  Those back-and-forth interactions can
            // significantly increase latency and the probability of transaction
            // conflicts.  See RFD 192 for details.  (Unfortunately, we still
            // _are_ going back and forth here to issue each of these queries.
            // But that's an artifact of the interface we currently have for
            // sending queries.  It should be possible to send all of these in
            // one batch.
            for (which, tree) in &collection.cabooses_found {
                let db_which = nexus_db_model::CabooseWhich::from(*which);
                for (baseboard_id, found_caboose) in tree {
                    use nexus_db_schema::schema::hw_baseboard_id::dsl as dsl_baseboard_id;
                    use nexus_db_schema::schema::inv_caboose::dsl as dsl_inv_caboose;
                    use nexus_db_schema::schema::sw_caboose::dsl as dsl_sw_caboose;

                    let selection = nexus_db_schema::schema::hw_baseboard_id::table
                        .inner_join(
                            nexus_db_schema::schema::sw_caboose::table.on(
                                dsl_baseboard_id::part_number
                                    .eq(baseboard_id.part_number.clone())
                                    .and(
                                        dsl_baseboard_id::serial_number.eq(
                                            baseboard_id.serial_number.clone(),
                                        ),
                                    )
                                    .and(dsl_sw_caboose::board.eq(
                                        found_caboose.caboose.board.clone(),
                                    ))
                                    .and(
                                        dsl_sw_caboose::git_commit.eq(
                                            found_caboose
                                                .caboose
                                                .git_commit
                                                .clone(),
                                        ),
                                    )
                                    .and(
                                        dsl_sw_caboose::name.eq(found_caboose
                                            .caboose
                                            .name
                                            .clone()),
                                    )
                                    .and(dsl_sw_caboose::version.eq(
                                        found_caboose.caboose.version.clone(),
                                    ))
                                    .and(dsl_sw_caboose::sign.is_not_distinct_from(
                                        found_caboose.caboose.sign.clone(),
                                    )),
                            ),
                        )
                        .select((
                            dsl_baseboard_id::id,
                            dsl_sw_caboose::id,
                            db_collection_id
                                .into_sql::<diesel::sql_types::Uuid>(),
                            found_caboose
                                .time_collected
                                .into_sql::<diesel::sql_types::Timestamptz>(),
                            found_caboose
                                .source
                                .clone()
                                .into_sql::<diesel::sql_types::Text>(),
                            db_which.into_sql::<CabooseWhichEnum>(),
                        ));

                    let _ = diesel::insert_into(nexus_db_schema::schema::inv_caboose::table)
                        .values(selection)
                        .into_columns((
                            dsl_inv_caboose::hw_baseboard_id,
                            dsl_inv_caboose::sw_caboose_id,
                            dsl_inv_caboose::inv_collection_id,
                            dsl_inv_caboose::time_collected,
                            dsl_inv_caboose::source,
                            dsl_inv_caboose::which,
                        ))
                        .execute_async(&conn)
                        .await?;

                    // See the comments above.  The same applies here.  If you
                    // update the statement below because the schema for
                    // `inv_caboose` has changed, be sure to update the code
                    // above, too!
                    let (
                        _hw_baseboard_id,
                        _sw_caboose_id,
                        _inv_collection_id,
                        _time_collected,
                        _source,
                        _which,
                    ) = dsl_inv_caboose::inv_caboose::all_columns();
                }
            }

            // Insert rows for the root of trust pages that we found. This is
            // almost identical to inserting cabooses above, and just like for
            // cabooses, we do this using INSERT INTO ... SELECT. We have these
            // three tables:
            //
            // - `hw_baseboard` with an "id" primary key and lookup columns
            //   "part_number" and "serial_number"
            // - `sw_root_of_trust_page` with an "id" primary key and lookup
            //   column "data_base64"
            // - `inv_root_of_trust_page` with foreign keys "hw_baseboard_id",
            //   "sw_root_of_trust_page_id", and various other columns
            //
            // and generate an INSERT INTO query that is structurally the same
            // as the caboose query described above.
            for (which, tree) in &collection.rot_pages_found {
                use nexus_db_schema::schema::hw_baseboard_id::dsl as dsl_baseboard_id;
                use nexus_db_schema::schema::inv_root_of_trust_page::dsl as dsl_inv_rot_page;
                use nexus_db_schema::schema::sw_root_of_trust_page::dsl as dsl_sw_rot_page;
                let db_which = nexus_db_model::RotPageWhich::from(*which);
                for (baseboard_id, found_rot_page) in tree {
                    let selection = nexus_db_schema::schema::hw_baseboard_id::table
                        .inner_join(
                            nexus_db_schema::schema::sw_root_of_trust_page::table.on(
                                dsl_baseboard_id::part_number
                                    .eq(baseboard_id.part_number.clone())
                                    .and(
                                        dsl_baseboard_id::serial_number.eq(
                                            baseboard_id.serial_number.clone(),
                                        ),
                                    )
                                    .and(dsl_sw_rot_page::data_base64.eq(
                                        found_rot_page.page.data_base64.clone(),
                                    )),
                            ),
                        )
                        .select((
                            dsl_baseboard_id::id,
                            dsl_sw_rot_page::id,
                            db_collection_id
                                .into_sql::<diesel::sql_types::Uuid>(),
                            found_rot_page
                                .time_collected
                                .into_sql::<diesel::sql_types::Timestamptz>(),
                            found_rot_page
                                .source
                                .clone()
                                .into_sql::<diesel::sql_types::Text>(),
                            db_which.into_sql::<RotPageWhichEnum>(),
                        ));

                    let _ = diesel::insert_into(
                        nexus_db_schema::schema::inv_root_of_trust_page::table,
                    )
                    .values(selection)
                    .into_columns((
                        dsl_inv_rot_page::hw_baseboard_id,
                        dsl_inv_rot_page::sw_root_of_trust_page_id,
                        dsl_inv_rot_page::inv_collection_id,
                        dsl_inv_rot_page::time_collected,
                        dsl_inv_rot_page::source,
                        dsl_inv_rot_page::which,
                    ))
                    .execute_async(&conn)
                    .await?;

                    // See the comments above.  The same applies here.  If you
                    // update the statement below because the schema for
                    // `inv_root_of_trust_page` has changed, be sure to update
                    // the code above, too!
                    let (
                        _hw_baseboard_id,
                        _sw_root_of_trust_page_id,
                        _inv_collection_id,
                        _time_collected,
                        _source,
                        _which,
                    ) = dsl_inv_rot_page::inv_root_of_trust_page::all_columns();
                }
            }

            // Insert rows for all the physical disks we found.
            {
                use nexus_db_schema::schema::inv_physical_disk::dsl;

                let batch_size = SQL_BATCH_SIZE.get().try_into().unwrap();
                let mut disks = disks.into_iter();
                loop {
                    let some_disks =
                        disks.by_ref().take(batch_size).collect::<Vec<_>>();
                    if some_disks.is_empty() {
                        break;
                    }
                    let _ = diesel::insert_into(dsl::inv_physical_disk)
                        .values(some_disks)
                        .execute_async(&conn)
                        .await?;
                }
            }

            // Insert rows for all the physical disk firmware we found.
            {
                use nexus_db_schema::schema::inv_nvme_disk_firmware::dsl;

                let batch_size = SQL_BATCH_SIZE.get().try_into().unwrap();
                let mut nvme_disk_firmware = nvme_disk_firmware.into_iter();
                loop {
                    let some_disk_firmware = nvme_disk_firmware
                        .by_ref()
                        .take(batch_size)
                        .collect::<Vec<_>>();
                    if some_disk_firmware.is_empty() {
                        break;
                    }
                    let _ = diesel::insert_into(dsl::inv_nvme_disk_firmware)
                        .values(some_disk_firmware)
                        .execute_async(&conn)
                        .await?;
                }
            }

            // Insert rows for all the zpools we found.
            {
                use nexus_db_schema::schema::inv_zpool::dsl;

                let batch_size = SQL_BATCH_SIZE.get().try_into().unwrap();
                let mut zpools = zpools.into_iter();
                loop {
                    let some_zpools =
                        zpools.by_ref().take(batch_size).collect::<Vec<_>>();
                    if some_zpools.is_empty() {
                        break;
                    }
                    let _ = diesel::insert_into(dsl::inv_zpool)
                        .values(some_zpools)
                        .execute_async(&conn)
                        .await?;
                }
            }

            // Insert rows for all the datasets we found.
            {
                use nexus_db_schema::schema::inv_dataset::dsl;

                let batch_size = SQL_BATCH_SIZE.get().try_into().unwrap();
                let mut datasets = datasets.into_iter();
                loop {
                    let some_datasets =
                        datasets.by_ref().take(batch_size).collect::<Vec<_>>();
                    if some_datasets.is_empty() {
                        break;
                    }
                    let _ = diesel::insert_into(dsl::inv_dataset)
                        .values(some_datasets)
                        .execute_async(&conn)
                        .await?;
                }
            }

            // Insert rows for all the config reconcilers we found.
            {
                use nexus_db_schema::schema::inv_sled_config_reconciler::dsl;

                let batch_size = SQL_BATCH_SIZE.get().try_into().unwrap();
                let mut reconcilers = config_reconcilers.into_iter();
                loop {
                    let some_reconcilers = reconcilers
                        .by_ref()
                        .take(batch_size)
                        .collect::<Vec<_>>();
                    if some_reconcilers.is_empty() {
                        break;
                    }
                    let _ = diesel::insert_into(dsl::inv_sled_config_reconciler)
                        .values(some_reconcilers)
                        .execute_async(&conn)
                        .await?;
                }
            }

            // Insert rows for all the boot partition details we found.
            {
                use nexus_db_schema::schema::inv_sled_boot_partition::dsl;

                let batch_size = SQL_BATCH_SIZE.get().try_into().unwrap();
                let mut boot_partitions = reconciler_boot_partitions
                    .into_iter();
                loop {
                    let some_boot_partitions = boot_partitions
                        .by_ref()
                        .take(batch_size)
                        .collect::<Vec<_>>();
                    if some_boot_partitions.is_empty() {
                        break;
                    }
                    let _ = diesel::insert_into(dsl::inv_sled_boot_partition)
                        .values(some_boot_partitions)
                        .execute_async(&conn)
                        .await?;
                }
            }

            // Insert rows for the all the sled configs we found.
            {
                use nexus_db_schema::schema::inv_omicron_sled_config::dsl;

                let batch_size = SQL_BATCH_SIZE.get().try_into().unwrap();
                let mut configs = omicron_sled_configs.into_iter();
                loop {
                    let some_configs =
                        configs.by_ref().take(batch_size).collect::<Vec<_>>();
                    if some_configs.is_empty() {
                        break;
                    }
                    let _ = diesel::insert_into(dsl::inv_omicron_sled_config)
                        .values(some_configs)
                        .execute_async(&conn)
                        .await?;
                }
            }

            // Insert rows for the all the sled configs' disks we found.
            {
                use nexus_db_schema::schema::inv_omicron_sled_config_disk::dsl;

                let batch_size = SQL_BATCH_SIZE.get().try_into().unwrap();
                let mut disks = omicron_sled_config_disks.into_iter();
                loop {
                    let some_disks =
                        disks.by_ref().take(batch_size).collect::<Vec<_>>();
                    if some_disks.is_empty() {
                        break;
                    }
                    let _ = diesel::insert_into(dsl::inv_omicron_sled_config_disk)
                        .values(some_disks)
                        .execute_async(&conn)
                        .await?;
                }
            }

            // Insert rows for the all the sled configs' datasets we found.
            {
                use nexus_db_schema::schema::inv_omicron_sled_config_dataset::dsl;

                let batch_size = SQL_BATCH_SIZE.get().try_into().unwrap();
                let mut datasets = omicron_sled_config_datasets.into_iter();
                loop {
                    let some_datasets =
                        datasets.by_ref().take(batch_size).collect::<Vec<_>>();
                    if some_datasets.is_empty() {
                        break;
                    }
                    let _ = diesel::insert_into(dsl::inv_omicron_sled_config_dataset)
                        .values(some_datasets)
                        .execute_async(&conn)
                        .await?;
                }
            }

            // Insert rows for the all the sled configs' zones we found.
            {
                use nexus_db_schema::schema::inv_omicron_sled_config_zone::dsl;

                let batch_size = SQL_BATCH_SIZE.get().try_into().unwrap();
                let mut zones = omicron_sled_config_zones.into_iter();
                loop {
                    let some_zones =
                        zones.by_ref().take(batch_size).collect::<Vec<_>>();
                    if some_zones.is_empty() {
                        break;
                    }
                    let _ = diesel::insert_into(dsl::inv_omicron_sled_config_zone)
                        .values(some_zones)
                        .execute_async(&conn)
                        .await?;
                }
            }

            // Insert rows for the all the sled configs' zones' nics we found.
            {
                use nexus_db_schema::schema::inv_omicron_sled_config_zone_nic::dsl;

                let batch_size = SQL_BATCH_SIZE.get().try_into().unwrap();
                let mut zone_nics = omicron_sled_config_zone_nics.into_iter();
                loop {
                    let some_zone_nics =
                        zone_nics.by_ref().take(batch_size).collect::<Vec<_>>();
                    if some_zone_nics.is_empty() {
                        break;
                    }
                    let _ = diesel::insert_into(dsl::inv_omicron_sled_config_zone_nic)
                        .values(some_zone_nics)
                        .execute_async(&conn)
                        .await?;
                }
            }

            // Insert rows for all the sled config reconciler disk results
            {
                use nexus_db_schema::schema::inv_last_reconciliation_disk_result::dsl;

                let batch_size = SQL_BATCH_SIZE.get().try_into().unwrap();
                let mut disk_results = reconciler_disk_results.into_iter();
                loop {
                    let some_disk_results =
                        disk_results.by_ref().take(batch_size).collect::<Vec<_>>();
                    if some_disk_results.is_empty() {
                        break;
                    }
                    let _ = diesel::insert_into(dsl::inv_last_reconciliation_disk_result)
                        .values(some_disk_results)
                        .execute_async(&conn)
                        .await?;
                }
            }

            // Insert rows for all the sled config reconciler dataset results
            {
                use nexus_db_schema::schema::inv_last_reconciliation_dataset_result::dsl;

                let batch_size = SQL_BATCH_SIZE.get().try_into().unwrap();
                let mut dataset_results = reconciler_dataset_results.into_iter();
                loop {
                    let some_dataset_results =
                        dataset_results.by_ref().take(batch_size).collect::<Vec<_>>();
                    if some_dataset_results.is_empty() {
                        break;
                    }
                    let _ = diesel::insert_into(dsl::inv_last_reconciliation_dataset_result)
                        .values(some_dataset_results)
                        .execute_async(&conn)
                        .await?;
                }
            }

            // Insert rows for all the sled config reconciler orphaned datasets
            {
                use nexus_db_schema::schema::inv_last_reconciliation_orphaned_dataset::dsl;

                let batch_size = SQL_BATCH_SIZE.get().try_into().unwrap();
                let mut orphaned_datasets = reconciler_orphaned_datasets.into_iter();
                loop {
                    let some_orphaned_datasets =
                        orphaned_datasets.by_ref().take(batch_size).collect::<Vec<_>>();
                    if some_orphaned_datasets.is_empty() {
                        break;
                    }
                    let _ = diesel::insert_into(dsl::inv_last_reconciliation_orphaned_dataset)
                        .values(some_orphaned_datasets)
                        .execute_async(&conn)
                        .await?;
                }
            }

            // Insert rows for all the sled config reconciler zone results
            {
                use nexus_db_schema::schema::inv_last_reconciliation_zone_result::dsl;

                let batch_size = SQL_BATCH_SIZE.get().try_into().unwrap();
                let mut zone_results = reconciler_zone_results.into_iter();
                loop {
                    let some_zone_results =
                        zone_results.by_ref().take(batch_size).collect::<Vec<_>>();
                    if some_zone_results.is_empty() {
                        break;
                    }
                    let _ = diesel::insert_into(dsl::inv_last_reconciliation_zone_result)
                        .values(some_zone_results)
                        .execute_async(&conn)
                        .await?;
                }
            }

            // Insert rows for all the zones found in the zone manifest on the
            // boot disk.
            {
                use nexus_db_schema::schema::inv_zone_manifest_zone::dsl;

                let batch_size = SQL_BATCH_SIZE.get().try_into().unwrap();
                let mut zones = zone_manifest_zones.into_iter();
                loop {
                    let some_zones =
                        zones.by_ref().take(batch_size).collect::<Vec<_>>();
                    if some_zones.is_empty() {
                        break;
                    }
                    let _ = diesel::insert_into(dsl::inv_zone_manifest_zone)
                        .values(some_zones)
                        .execute_async(&conn)
                        .await?;
                }
            }

            // Insert rows for non-boot zone manifests.
            {
                use nexus_db_schema::schema::inv_zone_manifest_non_boot::dsl;

                let batch_size = SQL_BATCH_SIZE.get().try_into().unwrap();
                let mut non_boot = zone_manifest_non_boot.into_iter();
                loop {
                    let some_non_boot =
                        non_boot.by_ref().take(batch_size).collect::<Vec<_>>();
                    if some_non_boot.is_empty() {
                        break;
                    }
                    let _ = diesel::insert_into(dsl::inv_zone_manifest_non_boot)
                        .values(some_non_boot)
                        .execute_async(&conn)
                        .await?;
                }
            }

            // Insert rows for non-boot mupdate overrides.
            {
                use nexus_db_schema::schema::inv_mupdate_override_non_boot::dsl;

                let batch_size = SQL_BATCH_SIZE.get().try_into().unwrap();
                let mut non_boot = mupdate_override_non_boot.into_iter();
                loop {
                    let some_non_boot =
                        non_boot.by_ref().take(batch_size).collect::<Vec<_>>();
                    if some_non_boot.is_empty() {
                        break;
                    }
                    let _ = diesel::insert_into(dsl::inv_mupdate_override_non_boot)
                        .values(some_non_boot)
                        .execute_async(&conn)
                        .await?;
                }
            }

            // Insert rows for the sled agents that we found.  In practice, we'd
            // expect these to all have baseboards (if using Oxide hardware) or
            // none have baseboards (if not).
            {
                use nexus_db_schema::schema::hw_baseboard_id::dsl as baseboard_dsl;
                use nexus_db_schema::schema::inv_sled_agent::dsl as sa_dsl;

                // For sleds with a real baseboard id, we have to use the
                // `INSERT INTO ... SELECT` pattern that we used for other types
                // of rows above to pull in the baseboard id's uuid.
                for sled_agent in &sled_agents_baseboards {
                    let baseboard_id = sled_agent.baseboard_id.as_ref().expect(
                        "already selected only sled agents with baseboards",
                    );
                    let ConfigReconcilerFields {
                        ledgered_sled_config,
                        reconciler_status,
                    } = config_reconciler_fields_by_sled
                        .remove(&sled_agent.sled_id)
                        .expect("all sled IDs should exist");
                    let zone_image_resolver = InvZoneImageResolver::new(&sled_agent.zone_image_resolver);
                    let selection = nexus_db_schema::schema::hw_baseboard_id::table
                        .select((
                            db_collection_id
                                .into_sql::<diesel::sql_types::Uuid>(),
                            sled_agent
                                .time_collected
                                .into_sql::<diesel::sql_types::Timestamptz>(),
                            sled_agent
                                .source
                                .clone()
                                .into_sql::<diesel::sql_types::Text>(),
                            (sled_agent.sled_id.into_untyped_uuid())
                                .into_sql::<diesel::sql_types::Uuid>(),
                            baseboard_dsl::id.nullable(),
                            nexus_db_model::ipv6::Ipv6Addr::from(
                                sled_agent.sled_agent_address.ip(),
                            )
                            .into_sql::<diesel::sql_types::Inet>(),
                            SqlU16(sled_agent.sled_agent_address.port())
                                .into_sql::<diesel::sql_types::Int4>(),
                            SledRole::from(sled_agent.sled_role)
                                .into_sql::<SledRoleEnum>(),
                            SqlU32(sled_agent.usable_hardware_threads)
                                .into_sql::<diesel::sql_types::Int8>(),
                            nexus_db_model::ByteCount::from(
                                sled_agent.usable_physical_ram,
                            )
                            .into_sql::<diesel::sql_types::Int8>(),
                            nexus_db_model::ByteCount::from(
                                sled_agent.reservoir_size,
                            )
                            .into_sql::<diesel::sql_types::Int8>(),
                            ledgered_sled_config
                                .map(|id| id.into_untyped_uuid())
                                .into_sql::<Nullable<diesel::sql_types::Uuid>>(),
                            reconciler_status.reconciler_status_kind
                                .into_sql::<InvConfigReconcilerStatusKindEnum>(),
                            reconciler_status.reconciler_status_sled_config
                                .map(|id| id.into_untyped_uuid())
                                .into_sql::<Nullable<diesel::sql_types::Uuid>>(),
                            reconciler_status.reconciler_status_timestamp
                                .into_sql::<Nullable<diesel::sql_types::Timestamptz>>(),
                            reconciler_status.reconciler_status_duration_secs
                                .into_sql::<Nullable<diesel::sql_types::Double>>(),
                            zone_image_resolver.zone_manifest_boot_disk_path
                                .into_sql::<diesel::sql_types::Text>(),
                            zone_image_resolver.zone_manifest_source
                                .into_sql::<Nullable<InvZoneManifestSourceEnum>>(),
                            zone_image_resolver.zone_manifest_mupdate_id
                                .into_sql::<Nullable<diesel::sql_types::Uuid>>(),
                            zone_image_resolver.zone_manifest_boot_disk_error
                                .into_sql::<Nullable<diesel::sql_types::Text>>(),
                            zone_image_resolver.mupdate_override_boot_disk_path
                                .into_sql::<diesel::sql_types::Text>(),
                            zone_image_resolver.mupdate_override_id
                                .into_sql::<Nullable<diesel::sql_types::Uuid>>(),
                            zone_image_resolver.mupdate_override_boot_disk_error
                                .into_sql::<Nullable<diesel::sql_types::Text>>(),
                        ))
                        .filter(
                            baseboard_dsl::part_number
                                .eq(baseboard_id.part_number.clone()),
                        )
                        .filter(
                            baseboard_dsl::serial_number
                                .eq(baseboard_id.serial_number.clone()),
                        );

                    let _ =
                        diesel::insert_into(nexus_db_schema::schema::inv_sled_agent::table)
                            .values(selection)
                            .into_columns((
                                sa_dsl::inv_collection_id,
                                sa_dsl::time_collected,
                                sa_dsl::source,
                                sa_dsl::sled_id,
                                sa_dsl::hw_baseboard_id,
                                sa_dsl::sled_agent_ip,
                                sa_dsl::sled_agent_port,
                                sa_dsl::sled_role,
                                sa_dsl::usable_hardware_threads,
                                sa_dsl::usable_physical_ram,
                                sa_dsl::reservoir_size,
                                sa_dsl::ledgered_sled_config,
                                sa_dsl::reconciler_status_kind,
                                sa_dsl::reconciler_status_sled_config,
                                sa_dsl::reconciler_status_timestamp,
                                sa_dsl::reconciler_status_duration_secs,
                                sa_dsl::zone_manifest_boot_disk_path,
                                sa_dsl::zone_manifest_source,
                                sa_dsl::zone_manifest_mupdate_id,
                                sa_dsl::zone_manifest_boot_disk_error,
                                sa_dsl::mupdate_override_boot_disk_path,
                                sa_dsl::mupdate_override_id,
                                sa_dsl::mupdate_override_boot_disk_error,
                            ))
                            .execute_async(&conn)
                            .await?;

                    // See the comment in the earlier block (where we use
                    // `inv_service_processor::all_columns()`).  The same
                    // applies here.
                    let (
                        _inv_collection_id,
                        _time_collected,
                        _source,
                        _sled_id,
                        _hw_baseboard_id,
                        _sled_agent_ip,
                        _sled_agent_port,
                        _sled_role,
                        _usable_hardware_threads,
                        _usable_physical_ram,
                        _reservoir_size,
                        _ledgered_sled_config,
                        _reconciler_status_kind,
                        _reconciler_status_sled_config,
                        _reconciler_status_timestamp,
                        _reconciler_status_duration_secs,
                        _zone_manifest_boot_disk_path,
                        _zone_manifest_source,
                        _zone_manifest_mupdate_id,
                        _zone_manifest_boot_disk_error,
                        _mupdate_override_boot_disk_path,
                        _mupdate_override_boot_disk_id,
                        _mupdate_override_boot_disk_error,
                    ) = sa_dsl::inv_sled_agent::all_columns();
                }

                // For sleds with no baseboard information, we can't use
                // the same INSERT INTO ... SELECT pattern because we
                // won't find anything in the hw_baseboard_id table.  It
                // sucks that these are bifurcated code paths, but on
                // the plus side, this is a much simpler INSERT, and we
                // can insert all of them in one statement.
                let _ = diesel::insert_into(nexus_db_schema::schema::inv_sled_agent::table)
                    .values(sled_agents_no_baseboards)
                    .execute_async(&conn)
                    .await?;
            }

            // Insert the clickhouse keeper memberships we've received
            {
                use nexus_db_schema::schema::inv_clickhouse_keeper_membership::dsl;
                diesel::insert_into(dsl::inv_clickhouse_keeper_membership)
                    .values(inv_clickhouse_keeper_memberships)
                    .execute_async(&conn)
                    .await?;
            }

            // Insert the cockroach status information we've observed
            if !inv_cockroach_status_records.is_empty() {
                use nexus_db_schema::schema::inv_cockroachdb_status::dsl;
                diesel::insert_into(dsl::inv_cockroachdb_status)
                    .values(inv_cockroach_status_records)
                    .execute_async(&conn)
                    .await?;
            }

            // Insert the NTP info we've observed
            if !inv_ntp_timesync_records.is_empty() {
                use nexus_db_schema::schema::inv_ntp_timesync::dsl;
                diesel::insert_into(dsl::inv_ntp_timesync)
                    .values(inv_ntp_timesync_records)
                    .execute_async(&conn)
                    .await?;
            }

            // Finally, insert the list of errors.
            {
                use nexus_db_schema::schema::inv_collection_error::dsl as errors_dsl;
                let _ = diesel::insert_into(errors_dsl::inv_collection_error)
                    .values(error_values)
                    .execute_async(&conn)
                    .await?;
            }

            Ok(())
        })
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        info!(
            &opctx.log,
            "inserted inventory collection";
            "collection_id" => collection.id.to_string(),
        );

        Ok(())
    }

    /// Prune inventory collections stored in the database, keeping at least
    /// `nkeep`.
    ///
    /// This function removes as many collections as possible while preserving
    /// the last `nkeep`.  This will also preserve at least one "complete"
    /// collection (i.e., one having zero errors).
    // It might seem surprising that such a high-level application policy is
    // embedded in the DataStore.  The reason is that we want to push a bunch of
    // the logic into the SQL to avoid interactive queries.
    pub async fn inventory_prune_collections(
        &self,
        opctx: &OpContext,
        nkeep: u32,
    ) -> Result<(), Error> {
        // Assumptions:
        //
        // - Most of the time, there will be about `nkeep + 1` collections in
        //   the database.  That's because the normal expected case is: we had
        //   `nkeep`, we created another one, and now we're pruning the oldest
        //   one.
        //
        // - There could be fewer collections in the database, early in the
        //   system's lifetime (before we've accumulated `nkeep` of them).
        //
        // - There could be many more collections in the database, if something
        //   has gone wrong and we've fallen behind in our cleanup.
        //
        // - Due to transient errors during the collection process, it's
        //   possible that a collection is known to be potentially incomplete.
        //   We can tell this because it has rows in `inv_collection_errors`.
        //   (It's possible that a collection can be incomplete with zero
        //   errors, but we can't know that here and so we can't do anything
        //   about it.)
        //
        // Goals:
        //
        // - When this function returns without error, there were at most
        //   `nkeep` collections in the database.
        //
        // - If we have to remove any collections, we want to start from the
        //   oldest ones.  (We want to maintain a window of the last `nkeep`,
        //   not the first `nkeep - 1` from the beginning of time plus the most
        //   recent one.)
        //
        // - We want to avoid removing the last collection that had zero errors.
        //   (If we weren't careful, we might do this if there were `nkeep`
        //   collections with errors that were newer than the last complete
        //   collection.)
        //
        // Here's the plan:
        //
        // - Select from the database the `nkeep + 1` oldest collections and the
        //   number of errors associated with each one.
        //
        // - If we got fewer than `nkeep + 1` back, we're done.  We shouldn't
        //   prune anything.
        //
        // - Otherwise, if the oldest collection is the only complete one,
        //   remove the next-oldest collection and go back to the top (repeat).
        //
        // - Otherwise, remove the oldest collection and go back to the top
        //   (repeat).
        //
        // This seems surprisingly complicated.  It's designed to meet the above
        // goals.
        //
        // Is this going to work if multiple Nexuses are doing it concurrently?
        // This cannot remove the last complete collection because a given Nexus
        // will only remove a complete collection if it has seen a newer
        // complete one.  This cannot result in keeping fewer than "nkeep"
        // collections because any Nexus will only remove a collection if there
        // are "nkeep" newer ones.  In both of these cases, another Nexus might
        // remove one of the ones that the first Nexus was counting on keeping,
        // but only if there was a newer one to replace it.

        opctx.authorize(authz::Action::Modify, &authz::INVENTORY).await?;

        loop {
            match self.inventory_find_pruneable(opctx, nkeep).await? {
                None => break,
                Some(collection_id) => {
                    self.inventory_delete_collection(opctx, collection_id)
                        .await?
                }
            }
        }

        Ok(())
    }

    /// Return the oldest inventory collection that's eligible for pruning,
    /// if any
    ///
    /// The caller of this (non-pub) function is responsible for authz.
    async fn inventory_find_pruneable(
        &self,
        opctx: &OpContext,
        nkeep: u32,
    ) -> Result<Option<CollectionUuid>, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        // Diesel requires us to use aliases in order to refer to the
        // `inv_collection` table twice in the same query.
        let (inv_collection1, inv_collection2) = diesel::alias!(
            nexus_db_schema::schema::inv_collection as inv_collection1,
            nexus_db_schema::schema::inv_collection as inv_collection2
        );

        // This subquery essentially generates:
        //
        //    SELECT id FROM inv_collection ORDER BY time_started" ASC LIMIT $1
        //
        // where $1 becomes `nkeep + 1`.  This just lists the `nkeep + 1` oldest
        // collections.
        let subquery = inv_collection1
            .select(
                inv_collection1
                    .field(nexus_db_schema::schema::inv_collection::id),
            )
            .order_by(
                inv_collection1
                    .field(
                        nexus_db_schema::schema::inv_collection::time_started,
                    )
                    .asc(),
            )
            .limit(i64::from(nkeep) + 1);

        // This essentially generates:
        //
        //     SELECT
        //         inv_collection.id,
        //         count(inv_collection_error.inv_collection_id)
        //     FROM (
        //             inv_collection
        //         LEFT OUTER JOIN
        //             inv_collection_error
        //         ON (
        //             inv_collection_error.inv_collection_id = inv_collection.id
        //         )
        //     ) WHERE (
        //         inv_collection.id = ANY( <<subquery above>> )
        //     )
        //     GROUP BY inv_collection.id
        //     ORDER BY inv_collection.time_started ASC
        //
        // This looks a lot scarier than it is.  The goal is to produce a
        // two-column table that looks like this:
        //
        //     collection_id1     count of errors from collection_id1
        //     collection_id2     count of errors from collection_id2
        //     collection_id3     count of errors from collection_id3
        //     ...
        //
        let candidates: Vec<(Uuid, i64)> = inv_collection2
            .left_outer_join(nexus_db_schema::schema::inv_collection_error::table)
            .filter(
                inv_collection2
                    .field(nexus_db_schema::schema::inv_collection::id)
                    .eq_any(subquery),
            )
            .group_by(inv_collection2.field(nexus_db_schema::schema::inv_collection::id))
            .select((
                inv_collection2.field(nexus_db_schema::schema::inv_collection::id),
                diesel::dsl::count(
                    nexus_db_schema::schema::inv_collection_error::inv_collection_id
                        .nullable(),
                ),
            ))
            .order_by(
                inv_collection2
                    .field(nexus_db_schema::schema::inv_collection::time_started)
                    .asc(),
            )
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
            .internal_context("listing oldest collections")?;

        if u32::try_from(candidates.len()).unwrap() <= nkeep {
            debug!(
                &opctx.log,
                "inventory_prune_one: nothing eligible for removal (too few)";
                "candidates" => ?candidates,
            );
            return Ok(None);
        }

        // We've now got up to "nkeep + 1" oldest collections, starting with the
        // very oldest.  We can get rid of the oldest one unless it's the only
        // complete one.  Another way to think about it: find the _last_
        // complete one.  Remove it from the list of candidates.  Now mark the
        // first item in the remaining list for deletion.
        let last_completed_idx = candidates
            .iter()
            .enumerate()
            .rev()
            .find(|(_i, (_collection_id, nerrors))| *nerrors == 0);
        let candidate = match last_completed_idx {
            Some((0, _)) => candidates.get(1),
            _ => candidates.first(),
        }
        .map(|(collection_id, _nerrors)| *collection_id);
        if let Some(c) = candidate {
            debug!(
                &opctx.log,
                "inventory_prune_one: eligible for removal";
                "collection_id" => c.to_string(),
                "candidates" => ?candidates,
            );
        } else {
            debug!(
                &opctx.log,
                "inventory_prune_one: nothing eligible for removal";
                "candidates" => ?candidates,
            );
        }
        Ok(candidate.map(CollectionUuid::from_untyped_uuid))
    }

    /// Removes an inventory collection from the database
    ///
    /// The caller of this (non-pub) function is responsible for authz.
    async fn inventory_delete_collection(
        &self,
        opctx: &OpContext,
        collection_id: CollectionUuid,
    ) -> Result<(), Error> {
        // As with inserting a whole collection, we remove it in one big
        // transaction for simplicity.  Similar considerations apply.  We could
        // break it up if these transactions become too big.  But we'd need a
        // way to stop other clients from discovering a collection after we
        // start removing it and we'd also need to make sure we didn't leak a
        // collection if we crash while deleting it.
        let conn = self.pool_connection_authorized(opctx).await?;
        let db_collection_id = to_db_typed_uuid(collection_id);

        // Helper to pack and unpack all the counts of rows we delete in the
        // transaction below (exclusively used for logging).
        struct NumRowsDeleted {
            ncollections: usize,
            nsps: usize,
            nhost_phase1_flash_hashes: usize,
            nrots: usize,
            ncabooses: usize,
            nrot_pages: usize,
            nsled_agents: usize,
            ndatasets: usize,
            nphysical_disks: usize,
            nnvme_disk_firmware: usize,
            nlast_reconciliation_disk_results: usize,
            nlast_reconciliation_dataset_results: usize,
            nlast_reconciliation_orphaned_datasets: usize,
            nlast_reconciliation_zone_results: usize,
            nzone_manifest_zones: usize,
            nzone_manifest_non_boot: usize,
            nmupdate_override_non_boot: usize,
            nconfig_reconcilers: usize,
            nboot_partitions: usize,
            nomicron_sled_configs: usize,
            nomicron_sled_config_disks: usize,
            nomicron_sled_config_datasets: usize,
            nomicron_sled_config_zones: usize,
            nomicron_sled_config_zone_nics: usize,
            nzpools: usize,
            nerrors: usize,
            nclickhouse_keeper_membership: usize,
            ncockroach_status: usize,
            nntp_timesync: usize,
        }

        let NumRowsDeleted {
            ncollections,
            nsps,
            nhost_phase1_flash_hashes,
            nrots,
            ncabooses,
            nrot_pages,
            nsled_agents,
            ndatasets,
            nphysical_disks,
            nnvme_disk_firmware,
            nlast_reconciliation_disk_results,
            nlast_reconciliation_dataset_results,
            nlast_reconciliation_orphaned_datasets,
            nlast_reconciliation_zone_results,
            nzone_manifest_zones,
            nzone_manifest_non_boot,
            nmupdate_override_non_boot,
            nconfig_reconcilers,
            nboot_partitions,
            nomicron_sled_configs,
            nomicron_sled_config_disks,
            nomicron_sled_config_datasets,
            nomicron_sled_config_zones,
            nomicron_sled_config_zone_nics,
            nzpools,
            nerrors,
            nclickhouse_keeper_membership,
            ncockroach_status,
            nntp_timesync,
        } =
            self.transaction_retry_wrapper("inventory_delete_collection")
                .transaction(&conn, |conn| async move {
                    // Remove the record describing the collection itself.
                    let ncollections = {
                        use nexus_db_schema::schema::inv_collection::dsl;
                        diesel::delete(
                            dsl::inv_collection
                                .filter(dsl::id.eq(db_collection_id)),
                        )
                        .execute_async(&conn)
                        .await?
                    };

                    // Remove rows for service processors.
                    let nsps = {
                        use nexus_db_schema::schema::inv_service_processor::dsl;
                        diesel::delete(dsl::inv_service_processor.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };

                    // Remove rows for host phase 1 flash hashes.
                    let nhost_phase1_flash_hashes = {
                        use nexus_db_schema::schema::inv_host_phase_1_flash_hash::dsl;
                        diesel::delete(dsl::inv_host_phase_1_flash_hash.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };

                    // Remove rows for roots of trust.
                    let nrots = {
                        use nexus_db_schema::schema::inv_root_of_trust::dsl;
                        diesel::delete(dsl::inv_root_of_trust.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };

                    // Remove rows for cabooses found.
                    let ncabooses = {
                        use nexus_db_schema::schema::inv_caboose::dsl;
                        diesel::delete(dsl::inv_caboose.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };

                    // Remove rows for root of trust pages found.
                    let nrot_pages = {
                        use nexus_db_schema::schema::inv_root_of_trust_page::dsl;
                        diesel::delete(dsl::inv_root_of_trust_page.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };

                    // Remove rows for sled agents found.
                    let nsled_agents = {
                        use nexus_db_schema::schema::inv_sled_agent::dsl;
                        diesel::delete(dsl::inv_sled_agent.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };

                    // Remove rows for datasets
                    let ndatasets = {
                        use nexus_db_schema::schema::inv_dataset::dsl;
                        diesel::delete(dsl::inv_dataset.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };

                    // Remove rows for physical disks found.
                    let nphysical_disks = {
                        use nexus_db_schema::schema::inv_physical_disk::dsl;
                        diesel::delete(dsl::inv_physical_disk.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };

                    // Remove rows for NVMe physical disk firmware found.
                    let nnvme_disk_firmware = {
                        use nexus_db_schema::schema::inv_nvme_disk_firmware::dsl;
                        diesel::delete(dsl::inv_nvme_disk_firmware.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };

                    // Remove rows associated with the last reconciliation
                    // result (disks, datasets, and zones).
                    let nlast_reconciliation_disk_results = {
                        use nexus_db_schema::schema::inv_last_reconciliation_disk_result::dsl;
                        diesel::delete(dsl::inv_last_reconciliation_disk_result.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };
                    let nlast_reconciliation_dataset_results = {
                        use nexus_db_schema::schema::inv_last_reconciliation_dataset_result::dsl;
                        diesel::delete(dsl::inv_last_reconciliation_dataset_result.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };
                    let nlast_reconciliation_orphaned_datasets = {
                        use nexus_db_schema::schema::inv_last_reconciliation_orphaned_dataset::dsl;
                        diesel::delete(dsl::inv_last_reconciliation_orphaned_dataset.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };
                    let nlast_reconciliation_zone_results = {
                        use nexus_db_schema::schema::inv_last_reconciliation_zone_result::dsl;
                        diesel::delete(dsl::inv_last_reconciliation_zone_result.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };

                    // Remove rows associated with zone resolver inventory.
                    let nzone_manifest_zones = {
                        use nexus_db_schema::schema::inv_zone_manifest_zone::dsl;
                        diesel::delete(dsl::inv_zone_manifest_zone.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };
                    let nzone_manifest_non_boot = {
                        use nexus_db_schema::schema::inv_zone_manifest_non_boot::dsl;
                        diesel::delete(dsl::inv_zone_manifest_non_boot.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };
                    let nmupdate_override_non_boot = {
                        use nexus_db_schema::schema::inv_mupdate_override_non_boot::dsl;
                        diesel::delete(dsl::inv_mupdate_override_non_boot.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };

                    // Remove rows associated with sled-agent config reconcilers
                    let nconfig_reconcilers = {
                        use nexus_db_schema::schema::inv_sled_config_reconciler::dsl;
                        diesel::delete(dsl::inv_sled_config_reconciler.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };
                    let nboot_partitions = {
                        use nexus_db_schema::schema::inv_sled_boot_partition::dsl;
                        diesel::delete(dsl::inv_sled_boot_partition.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };

                    // Remove rows associated with `OmicronSledConfig`s.
                    let nomicron_sled_configs = {
                        use nexus_db_schema::schema::inv_omicron_sled_config::dsl;
                        diesel::delete(dsl::inv_omicron_sled_config.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };
                    let nomicron_sled_config_disks = {
                        use nexus_db_schema::schema::inv_omicron_sled_config_disk::dsl;
                        diesel::delete(dsl::inv_omicron_sled_config_disk.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };
                    let nomicron_sled_config_datasets = {
                        use nexus_db_schema::schema::inv_omicron_sled_config_dataset::dsl;
                        diesel::delete(dsl::inv_omicron_sled_config_dataset.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };
                    let nomicron_sled_config_zones = {
                        use nexus_db_schema::schema::inv_omicron_sled_config_zone::dsl;
                        diesel::delete(dsl::inv_omicron_sled_config_zone.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };
                    let nomicron_sled_config_zone_nics = {
                        use nexus_db_schema::schema::inv_omicron_sled_config_zone_nic::dsl;
                        diesel::delete(dsl::inv_omicron_sled_config_zone_nic.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };

                    let nzpools = {
                        use nexus_db_schema::schema::inv_zpool::dsl;
                        diesel::delete(dsl::inv_zpool.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };

                    // Remove rows for errors encountered.
                    let nerrors = {
                        use nexus_db_schema::schema::inv_collection_error::dsl;
                        diesel::delete(dsl::inv_collection_error.filter(
                            dsl::inv_collection_id.eq(db_collection_id),
                        ))
                        .execute_async(&conn)
                        .await?
                    };

                    // Remove rows for clickhouse keeper membership
                    let nclickhouse_keeper_membership = {
                        use nexus_db_schema::schema::inv_clickhouse_keeper_membership::dsl;
                        diesel::delete(
                            dsl::inv_clickhouse_keeper_membership.filter(
                                dsl::inv_collection_id.eq(db_collection_id),
                            ),
                        )
                        .execute_async(&conn)
                        .await?
                    };
                    // Remove rows for cockroach status
                    let ncockroach_status = {
                        use nexus_db_schema::schema::inv_cockroachdb_status::dsl;
                        diesel::delete(
                            dsl::inv_cockroachdb_status.filter(
                                dsl::inv_collection_id.eq(db_collection_id),
                            ),
                        )
                        .execute_async(&conn)
                        .await?
                    };
                    // Remove rows for NTP timesync
                    let nntp_timesync = {
                        use nexus_db_schema::schema::inv_ntp_timesync::dsl;
                        diesel::delete(
                            dsl::inv_ntp_timesync.filter(
                                dsl::inv_collection_id.eq(db_collection_id),
                            ),
                        )
                        .execute_async(&conn)
                        .await?
                    };

                    Ok(NumRowsDeleted {
                        ncollections,
                        nsps,
                        nhost_phase1_flash_hashes,
                        nrots,
                        ncabooses,
                        nrot_pages,
                        nsled_agents,
                        ndatasets,
                        nphysical_disks,
                        nnvme_disk_firmware,
                        nlast_reconciliation_disk_results,
                        nlast_reconciliation_dataset_results,
                        nlast_reconciliation_orphaned_datasets,
                        nlast_reconciliation_zone_results,
                        nzone_manifest_zones,
                        nzone_manifest_non_boot,
                        nmupdate_override_non_boot,
                        nconfig_reconcilers,
                        nboot_partitions,
                        nomicron_sled_configs,
                        nomicron_sled_config_disks,
                        nomicron_sled_config_datasets,
                        nomicron_sled_config_zones,
                        nomicron_sled_config_zone_nics,
                        nzpools,
                        nerrors,
                        nclickhouse_keeper_membership,
                        ncockroach_status,
                        nntp_timesync,
                    })
                })
                .await
                .map_err(|error| {
                    public_error_from_diesel(error, ErrorHandler::Server)
                })?;

        info!(&opctx.log, "removed inventory collection";
            "collection_id" => collection_id.to_string(),
            "ncollections" => ncollections,
            "nsps" => nsps,
            "nhost_phase1_flash_hashes" => nhost_phase1_flash_hashes,
            "nrots" => nrots,
            "ncabooses" => ncabooses,
            "nrot_pages" => nrot_pages,
            "nsled_agents" => nsled_agents,
            "ndatasets" => ndatasets,
            "nphysical_disks" => nphysical_disks,
            "nnvme_disk_firmware" => nnvme_disk_firmware,
            "nlast_reconciliation_disk_results" =>
                nlast_reconciliation_disk_results,
            "nlast_reconciliation_dataset_results" =>
                nlast_reconciliation_dataset_results,
            "nlast_reconciliation_orphaned_datasets" =>
                nlast_reconciliation_orphaned_datasets,
            "nlast_reconciliation_zone_results" =>
                nlast_reconciliation_zone_results,
            "nzone_manifest_zones" => nzone_manifest_zones,
            "nzone_manifest_non_boot" => nzone_manifest_non_boot,
            "nmupdate_override_non_boot" => nmupdate_override_non_boot,
            "nconfig_reconcilers" => nconfig_reconcilers,
            "nboot_partitions" => nboot_partitions,
            "nomicron_sled_configs" => nomicron_sled_configs,
            "nomicron_sled_config_disks" => nomicron_sled_config_disks,
            "nomicron_sled_config_datasets" => nomicron_sled_config_datasets,
            "nomicron_sled_config_zones" => nomicron_sled_config_zones,
            "nomicron_sled_config_zone_nics" => nomicron_sled_config_zone_nics,
            "nzpools" => nzpools,
            "nerrors" => nerrors,
            "nclickhouse_keeper_membership" => nclickhouse_keeper_membership,
            "ncockroach_status" => ncockroach_status,
            "nntp_timesync" => nntp_timesync,
        );

        Ok(())
    }

    // Find the primary key for `hw_baseboard_id` given a `BaseboardId`
    pub async fn find_hw_baseboard_id(
        &self,
        opctx: &OpContext,
        baseboard_id: &BaseboardId,
    ) -> Result<Uuid, Error> {
        opctx.authorize(authz::Action::Read, &authz::INVENTORY).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        use nexus_db_schema::schema::hw_baseboard_id::dsl;
        dsl::hw_baseboard_id
            .filter(dsl::serial_number.eq(baseboard_id.serial_number.clone()))
            .filter(dsl::part_number.eq(baseboard_id.part_number.clone()))
            .select(dsl::id)
            .first_async::<Uuid>(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel_lookup(
                    e,
                    ResourceType::Sled,
                    &LookupType::ByCompositeId(format!("{baseboard_id:?}")),
                )
            })
    }

    /// Attempt to read the latest collection.
    ///
    /// If there aren't any collections, return `Ok(None)`.
    pub async fn inventory_get_latest_collection(
        &self,
        opctx: &OpContext,
    ) -> Result<Option<Collection>, Error> {
        opctx.authorize(authz::Action::Read, &authz::INVENTORY).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        use nexus_db_schema::schema::inv_collection::dsl;
        let collection_id = dsl::inv_collection
            .select(dsl::id)
            .order_by(dsl::time_started.desc())
            .first_async::<Uuid>(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        let Some(collection_id) = collection_id else {
            return Ok(None);
        };

        Ok(Some(
            self.inventory_collection_read(
                opctx,
                CollectionUuid::from_untyped_uuid(collection_id),
            )
            .await?,
        ))
    }

    /// Attempt to read the current collection
    pub async fn inventory_collection_read(
        &self,
        opctx: &OpContext,
        id: CollectionUuid,
    ) -> Result<Collection, Error> {
        self.inventory_collection_read_batched(opctx, id, SQL_BATCH_SIZE).await
    }

    /// Attempt to read the current collection with the provided batch size.
    ///
    /// Queries are limited to `batch_size` records at a time, performing
    /// multiple queries if more than `batch_size` records exist.
    ///
    /// In general, we don't want to permit downstream code to determine the
    /// batch size; instead, we would like to always use `SQL_BATCH_SIZE`.
    /// However, in order to facilitate testing of the batching logic itself,
    /// this private method is separated from the public APIs
    /// [`Self::inventory_get_latest_collection`] and
    /// [`Self::inventory_collection_read`], so that we can test with smaller
    /// batch sizes.
    async fn inventory_collection_read_batched(
        &self,
        opctx: &OpContext,
        id: CollectionUuid,
        batch_size: NonZeroU32,
    ) -> Result<Collection, Error> {
        let conn = self.pool_connection_authorized(opctx).await?;
        let db_id = to_db_typed_uuid(id);
        let (time_started, time_done, collector) = {
            use nexus_db_schema::schema::inv_collection::dsl;

            let collections = dsl::inv_collection
                .filter(dsl::id.eq(db_id))
                .limit(2)
                .select(InvCollection::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
            bail_unless!(collections.len() == 1);
            let collection = collections.into_iter().next().unwrap();
            (
                collection.time_started,
                collection.time_done,
                collection.collector,
            )
        };

        let errors: Vec<String> = {
            use nexus_db_schema::schema::inv_collection_error::dsl;
            let mut errors = Vec::new();
            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::inv_collection_error,
                    dsl::idx,
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .order_by(dsl::idx)
                .select(InvCollectionError::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator =
                    p.found_batch(&batch, &|row: &InvCollectionError| row.idx);
                errors.extend(batch.into_iter().map(|e| e.message));
            }
            errors
        };

        let sps: BTreeMap<_, _> = {
            use nexus_db_schema::schema::inv_service_processor::dsl;

            let mut sps = BTreeMap::new();

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::inv_service_processor,
                    dsl::hw_baseboard_id,
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvServiceProcessor::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| row.hw_baseboard_id);
                sps.extend(batch.into_iter().map(|row| {
                    let baseboard_id = row.hw_baseboard_id;
                    (
                        baseboard_id,
                        nexus_types::inventory::ServiceProcessor::from(row),
                    )
                }));
            }
            sps
        };

        let rots: BTreeMap<_, _> = {
            use nexus_db_schema::schema::inv_root_of_trust::dsl;

            let mut rots = BTreeMap::new();

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::inv_root_of_trust,
                    dsl::hw_baseboard_id,
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvRootOfTrust::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| row.hw_baseboard_id);
                rots.extend(batch.into_iter().map(|rot_row| {
                    let baseboard_id = rot_row.hw_baseboard_id;
                    (
                        baseboard_id,
                        nexus_types::inventory::RotState::from(rot_row),
                    )
                }));
            }
            rots
        };

        let sled_agent_rows: Vec<_> = {
            use nexus_db_schema::schema::inv_sled_agent::dsl;

            let mut rows = Vec::new();

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let mut batch = paginated(
                    dsl::inv_sled_agent,
                    dsl::sled_id,
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvSledAgent::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| row.sled_id);
                rows.append(&mut batch);
            }

            rows
        };

        // Mapping of "Sled ID" -> "Mapping of physical slot -> disk firmware"
        let disk_firmware: BTreeMap<
            SledUuid,
            BTreeMap<i64, nexus_types::inventory::PhysicalDiskFirmware>,
        > = {
            use nexus_db_schema::schema::inv_nvme_disk_firmware::dsl;

            let mut disk_firmware = BTreeMap::<
                SledUuid,
                BTreeMap<i64, nexus_types::inventory::PhysicalDiskFirmware>,
            >::new();
            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated_multicolumn(
                    dsl::inv_nvme_disk_firmware,
                    (dsl::sled_id, dsl::slot),
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvNvmeDiskFirmware::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator =
                    p.found_batch(&batch, &|row| (row.sled_id(), row.slot()));
                for firmware in batch {
                    disk_firmware
                        .entry(firmware.sled_id().into())
                        .or_default()
                        .insert(
                            firmware.slot(),
                            nexus_types::inventory::PhysicalDiskFirmware::from(
                                firmware,
                            ),
                        );
                }
            }
            disk_firmware
        };

        // Mapping of "Sled ID" -> "All disks reported by that sled"
        let physical_disks: BTreeMap<
            SledUuid,
            Vec<nexus_types::inventory::PhysicalDisk>,
        > = {
            use nexus_db_schema::schema::inv_physical_disk::dsl;

            let mut disks = BTreeMap::<
                SledUuid,
                Vec<nexus_types::inventory::PhysicalDisk>,
            >::new();
            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated_multicolumn(
                    dsl::inv_physical_disk,
                    (dsl::sled_id, dsl::slot),
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvPhysicalDisk::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator =
                    p.found_batch(&batch, &|row| (row.sled_id, row.slot));
                for disk in batch {
                    let sled_id = disk.sled_id.into();
                    let firmware = disk_firmware
                        .get(&sled_id)
                        .and_then(|lookup| lookup.get(&disk.slot))
                        .unwrap_or(&nexus_types::inventory::PhysicalDiskFirmware::Unknown);

                    disks.entry(sled_id).or_default().push(
                        nexus_types::inventory::PhysicalDisk {
                            identity: omicron_common::disk::DiskIdentity {
                                vendor: disk.vendor,
                                model: disk.model,
                                serial: disk.serial,
                            },
                            variant: disk.variant.into(),
                            slot: disk.slot,
                            firmware: firmware.clone(),
                        },
                    );
                }
            }
            disks
        };

        // Mapping of "Sled ID" -> "All zpools reported by that sled"
        let zpools: BTreeMap<Uuid, Vec<nexus_types::inventory::Zpool>> = {
            use nexus_db_schema::schema::inv_zpool::dsl;

            let mut zpools =
                BTreeMap::<Uuid, Vec<nexus_types::inventory::Zpool>>::new();
            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated_multicolumn(
                    dsl::inv_zpool,
                    (dsl::sled_id, dsl::id),
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvZpool::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| (row.sled_id, row.id));
                for zpool in batch {
                    zpools
                        .entry(zpool.sled_id.into_untyped_uuid())
                        .or_default()
                        .push(zpool.into());
                }
            }
            zpools
        };

        // Mapping of "Sled ID" -> "All datasets reported by that sled"
        let datasets: BTreeMap<Uuid, Vec<nexus_types::inventory::Dataset>> = {
            use nexus_db_schema::schema::inv_dataset::dsl;

            let mut datasets =
                BTreeMap::<Uuid, Vec<nexus_types::inventory::Dataset>>::new();
            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated_multicolumn(
                    dsl::inv_dataset,
                    (dsl::sled_id, dsl::name),
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvDataset::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| {
                    (row.sled_id, row.name.clone())
                });
                for dataset in batch {
                    datasets
                        .entry(dataset.sled_id.into_untyped_uuid())
                        .or_default()
                        .push(dataset.into());
                }
            }
            datasets
        };

        // Collect the unique baseboard ids referenced by SPs, RoTs, and Sled
        // Agents.
        let baseboard_id_ids: BTreeSet<_> = sps
            .keys()
            .chain(rots.keys())
            .cloned()
            .chain(sled_agent_rows.iter().filter_map(|s| s.hw_baseboard_id))
            .collect();
        // Fetch the corresponding baseboard records.
        let baseboards_by_id: BTreeMap<_, _> = {
            use nexus_db_schema::schema::hw_baseboard_id::dsl;

            let mut bbs = BTreeMap::new();

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::hw_baseboard_id,
                    dsl::id,
                    &p.current_pagparams(),
                )
                .filter(dsl::id.eq_any(baseboard_id_ids.clone()))
                .select(HwBaseboardId::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| row.id);
                bbs.extend(batch.into_iter().map(|bb| {
                    (
                        bb.id,
                        Arc::new(nexus_types::inventory::BaseboardId::from(bb)),
                    )
                }));
            }

            bbs
        };

        // Having those, we can replace the keys in the maps above with
        // references to the actual baseboard rather than the uuid.
        let sps = sps
            .into_iter()
            .map(|(id, sp)| {
                baseboards_by_id.get(&id).map(|bb| (bb.clone(), sp)).ok_or_else(
                    || {
                        Error::internal_error(
                            "missing baseboard that we should have fetched",
                        )
                    },
                )
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let rots = rots
            .into_iter()
            .map(|(id, rot)| {
                baseboards_by_id
                    .get(&id)
                    .map(|bb| (bb.clone(), rot))
                    .ok_or_else(|| {
                        Error::internal_error(
                            "missing baseboard that we should have fetched",
                        )
                    })
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;

        // Fetch records of host phase 1 flash hashes found.
        let inv_host_phase_1_flash_hash_rows = {
            use nexus_db_schema::schema::inv_host_phase_1_flash_hash::dsl;

            let mut phase_1s = Vec::new();

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let mut batch = paginated_multicolumn(
                    dsl::inv_host_phase_1_flash_hash,
                    (dsl::hw_baseboard_id, dsl::slot),
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvHostPhase1FlashHash::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| {
                    (row.hw_baseboard_id, row.slot)
                });
                phase_1s.append(&mut batch);
            }

            phase_1s
        };
        // Assemble the lists of host phase 1 flash hashes found.
        let mut host_phase_1_flash_hashes = BTreeMap::new();
        for p in inv_host_phase_1_flash_hash_rows {
            let slot = M2Slot::from(p.slot);
            let by_baseboard = host_phase_1_flash_hashes
                .entry(slot)
                .or_insert_with(BTreeMap::new);
            let Some(bb) = baseboards_by_id.get(&p.hw_baseboard_id) else {
                let msg = format!(
                    "unknown baseboard found in \
                     inv_host_phase_1_flash_hash: {}",
                    p.hw_baseboard_id
                );
                return Err(Error::internal_error(&msg));
            };

            let previous = by_baseboard.insert(
                bb.clone(),
                nexus_types::inventory::HostPhase1FlashHash {
                    time_collected: p.time_collected,
                    source: p.source,
                    slot,
                    hash: *p.hash,
                },
            );
            bail_unless!(
                previous.is_none(),
                "duplicate host phase 1 flash hash found: {:?} baseboard {:?}",
                p.slot,
                p.hw_baseboard_id
            );
        }

        // Fetch records of cabooses found.
        let inv_caboose_rows = {
            use nexus_db_schema::schema::inv_caboose::dsl;

            let mut cabooses = Vec::new();

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let mut batch = paginated_multicolumn(
                    dsl::inv_caboose,
                    (dsl::hw_baseboard_id, dsl::which),
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvCaboose::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| {
                    (row.hw_baseboard_id, row.which)
                });
                cabooses.append(&mut batch);
            }

            cabooses
        };

        // Collect the unique sw_caboose_ids for those cabooses.
        let sw_caboose_ids: BTreeSet<_> = inv_caboose_rows
            .iter()
            .map(|inv_caboose| inv_caboose.sw_caboose_id)
            .collect();
        // Fetch the corresponing records.
        let cabooses_by_id: BTreeMap<_, _> = {
            use nexus_db_schema::schema::sw_caboose::dsl;

            let mut cabooses = BTreeMap::new();

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch =
                    paginated(dsl::sw_caboose, dsl::id, &p.current_pagparams())
                        .filter(dsl::id.eq_any(sw_caboose_ids.clone()))
                        .select(SwCaboose::as_select())
                        .load_async(&*conn)
                        .await
                        .map_err(|e| {
                            public_error_from_diesel(e, ErrorHandler::Server)
                        })?;
                paginator = p.found_batch(&batch, &|row| row.id);
                cabooses.extend(batch.into_iter().map(|sw_caboose_row| {
                    (
                        sw_caboose_row.id,
                        Arc::new(nexus_types::inventory::Caboose::from(
                            sw_caboose_row,
                        )),
                    )
                }));
            }

            cabooses
        };

        // Assemble the lists of cabooses found.
        let mut cabooses_found = BTreeMap::new();
        for c in inv_caboose_rows {
            let by_baseboard = cabooses_found
                .entry(nexus_types::inventory::CabooseWhich::from(c.which))
                .or_insert_with(BTreeMap::new);
            let Some(bb) = baseboards_by_id.get(&c.hw_baseboard_id) else {
                let msg = format!(
                    "unknown baseboard found in inv_caboose: {}",
                    c.hw_baseboard_id
                );
                return Err(Error::internal_error(&msg));
            };
            let Some(sw_caboose) = cabooses_by_id.get(&c.sw_caboose_id) else {
                let msg = format!(
                    "unknown caboose found in inv_caboose: {}",
                    c.sw_caboose_id
                );
                return Err(Error::internal_error(&msg));
            };

            let previous = by_baseboard.insert(
                bb.clone(),
                nexus_types::inventory::CabooseFound {
                    time_collected: c.time_collected,
                    source: c.source,
                    caboose: sw_caboose.clone(),
                },
            );
            bail_unless!(
                previous.is_none(),
                "duplicate caboose found: {:?} baseboard {:?}",
                c.which,
                c.hw_baseboard_id
            );
        }

        // Fetch records of RoT pages found.
        let inv_rot_page_rows = {
            use nexus_db_schema::schema::inv_root_of_trust_page::dsl;

            let mut rot_pages = Vec::new();

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let mut batch = paginated_multicolumn(
                    dsl::inv_root_of_trust_page,
                    (dsl::hw_baseboard_id, dsl::which),
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvRotPage::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| {
                    (row.hw_baseboard_id, row.which)
                });
                rot_pages.append(&mut batch);
            }

            rot_pages
        };

        // Collect the unique sw_rot_page_ids for those pages.
        let sw_rot_page_ids: BTreeSet<_> = inv_rot_page_rows
            .iter()
            .map(|inv_rot_page| inv_rot_page.sw_root_of_trust_page_id)
            .collect();
        // Fetch the corresponding records.
        let rot_pages_by_id: BTreeMap<_, _> = {
            use nexus_db_schema::schema::sw_root_of_trust_page::dsl;

            let mut rot_pages = BTreeMap::new();

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::sw_root_of_trust_page,
                    dsl::id,
                    &p.current_pagparams(),
                )
                .filter(dsl::id.eq_any(sw_rot_page_ids.clone()))
                .select(SwRotPage::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| row.id);
                rot_pages.extend(batch.into_iter().map(|sw_rot_page_row| {
                    (
                        sw_rot_page_row.id,
                        Arc::new(nexus_types::inventory::RotPage::from(
                            sw_rot_page_row,
                        )),
                    )
                }))
            }

            rot_pages
        };

        // Assemble the lists of rot pages found.
        let mut rot_pages_found = BTreeMap::new();
        for p in inv_rot_page_rows {
            let by_baseboard = rot_pages_found
                .entry(nexus_types::inventory::RotPageWhich::from(p.which))
                .or_insert_with(BTreeMap::new);
            let Some(bb) = baseboards_by_id.get(&p.hw_baseboard_id) else {
                let msg = format!(
                    "unknown baseboard found in inv_root_of_trust_page: {}",
                    p.hw_baseboard_id
                );
                return Err(Error::internal_error(&msg));
            };
            let Some(sw_rot_page) =
                rot_pages_by_id.get(&p.sw_root_of_trust_page_id)
            else {
                let msg = format!(
                    "unknown rot page found in inv_root_of_trust_page: {}",
                    p.sw_root_of_trust_page_id
                );
                return Err(Error::internal_error(&msg));
            };

            let previous = by_baseboard.insert(
                bb.clone(),
                nexus_types::inventory::RotPageFound {
                    time_collected: p.time_collected,
                    source: p.source,
                    page: sw_rot_page.clone(),
                },
            );
            bail_unless!(
                previous.is_none(),
                "duplicate rot page found: {:?} baseboard {:?}",
                p.which,
                p.hw_baseboard_id
            );
        }

        // Now read the `OmicronSledConfig`s.
        //
        // There are 0-3 sled configs per sled agent (ledgered sled
        // config, last reconciled config, and actively-being-reconciled
        // config). Each of these is keyed by a database-only ID; these IDs are
        // referenced by `inv_sled_agent`. Before we read `inv_sled_agent`,
        // build up the map of all the sled configs it might reference.
        //
        // In this first pass, we'll load the `inv_omicron_sled_config` records.
        // It does not contain the actual disk/dataset/zone configs; we'll start
        // with empty sets and build those up by reading additional tables
        // below.
        let mut omicron_sled_configs = {
            use nexus_db_schema::schema::inv_omicron_sled_config::dsl;

            let mut configs = IdMap::new();

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::inv_omicron_sled_config,
                    dsl::id,
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvOmicronSledConfig::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| row.id);
                for sled_config in batch {
                    configs.insert(OmicronSledConfigWithId {
                        id: sled_config.id.into(),
                        config: OmicronSledConfig {
                            generation: sled_config.generation.into(),
                            remove_mupdate_override: sled_config
                                .remove_mupdate_override
                                .map(From::from),
                            disks: IdMap::default(),
                            datasets: IdMap::default(),
                            zones: IdMap::default(),
                            host_phase_2: sled_config.host_phase_2.into(),
                        },
                    });
                }
            }

            configs
        };

        // Assemble a mutable map of all the NICs found, by NIC id.  As we
        // match these up with the corresponding zone below, we'll remove items
        // from this set.  That way we can tell if the same NIC was used twice
        // or not used at all.
        let mut omicron_zone_nics: BTreeMap<_, _> = {
            use nexus_db_schema::schema::inv_omicron_sled_config_zone_nic::dsl;

            let mut nics = BTreeMap::new();

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::inv_omicron_sled_config_zone_nic,
                    dsl::id,
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvOmicronSledConfigZoneNic::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| row.id);
                nics.extend(batch.into_iter().map(|found_zone_nic| {
                    (
                        (found_zone_nic.sled_config_id, found_zone_nic.id),
                        found_zone_nic,
                    )
                }));
            }

            nics
        };

        // Now load the actual list of zones from all configs.
        let omicron_zones_list = {
            use nexus_db_schema::schema::inv_omicron_sled_config_zone::dsl;

            let mut zones = Vec::new();

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let mut batch = paginated(
                    dsl::inv_omicron_sled_config_zone,
                    dsl::id,
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvOmicronSledConfigZone::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| row.id);
                zones.append(&mut batch);
            }

            zones
        };
        for z in omicron_zones_list {
            let nic_row = z
                .nic_id
                .map(|id| {
                    // This error means that we found a row in
                    // inv_omicron_sled_config_zone that references a NIC by id
                    // but there's no corresponding row in
                    // inv_omicron_sled_config_zone with that id.  This should
                    // be impossible and reflects either a bug or database
                    // corruption.
                    omicron_zone_nics.remove(&(z.sled_config_id, id)).ok_or_else(|| {
                        Error::internal_error(&format!(
                            "zone {:?}: expected to find NIC {:?}, but didn't",
                            z.id, z.nic_id
                        ))
                    })
                })
                .transpose()?;
            let mut config_with_id = omicron_sled_configs
                .get_mut(&z.sled_config_id.into())
                .ok_or_else(|| {
                    // This error means that we found a row in
                    // inv_omicron_sled_config_zone with no associated record in
                    // inv_sled_omicron_config. This should be impossible and
                    // reflects either a bug or database corruption.
                    Error::internal_error(&format!(
                        "zone {:?}: unknown config ID: {:?}",
                        z.id, z.sled_config_id
                    ))
                })?;
            let zone_id = z.id;
            let zone = z
                .into_omicron_zone_config(nic_row)
                .with_context(|| {
                    format!("zone {:?}: parse from database", zone_id)
                })
                .map_err(|e| {
                    Error::internal_error(&format!("{:#}", e.to_string()))
                })?;
            config_with_id.config.zones.insert(zone);
        }

        bail_unless!(
            omicron_zone_nics.is_empty(),
            "found extra Omicron zone NICs: {:?}",
            omicron_zone_nics.keys()
        );

        // Now load the datasets from all configs.
        {
            use nexus_db_schema::schema::inv_omicron_sled_config_dataset::dsl;

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::inv_omicron_sled_config_dataset,
                    dsl::id,
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvOmicronSledConfigDataset::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| row.id);

                for row in batch {
                    let mut config_with_id = omicron_sled_configs
                        .get_mut(&row.sled_config_id.into())
                        .ok_or_else(|| {
                            Error::internal_error(&format!(
                                "dataset config {:?}: unknown config ID: {:?}",
                                row.id, row.sled_config_id
                            ))
                        })?;
                    config_with_id.config.datasets.insert(
                        row.try_into().map_err(|e| {
                            Error::internal_error(&format!("{e:#}"))
                        })?,
                    );
                }
            }
        }

        // Now load the disks from all configs.
        {
            use nexus_db_schema::schema::inv_omicron_sled_config_disk::dsl;

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::inv_omicron_sled_config_disk,
                    dsl::id,
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvOmicronSledConfigDisk::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| row.id);

                for row in batch {
                    let mut config_with_id = omicron_sled_configs
                        .get_mut(&row.sled_config_id.into())
                        .ok_or_else(|| {
                            Error::internal_error(&format!(
                                "disk config {:?}: unknown config ID: {:?}",
                                row.id, row.sled_config_id
                            ))
                        })?;
                    config_with_id.config.disks.insert(row.into());
                }
            }
        }

        // Load all the config reconciler top-level rows; build a map keyed by
        // sled ID.
        let mut sled_config_reconcilers = {
            use nexus_db_schema::schema::inv_sled_config_reconciler::dsl;

            let mut results: BTreeMap<SledUuid, _> = BTreeMap::new();

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::inv_sled_config_reconciler,
                    dsl::sled_id,
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvSledConfigReconciler::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| row.sled_id);

                for row in batch {
                    results.insert(row.sled_id.into(), row);
                }
            }

            results
        };

        // Load all the sled boot partition details; build a map of maps keyed
        // by sled ID -> M2Slot.
        let mut sled_boot_partition_details = {
            use nexus_db_schema::schema::inv_sled_boot_partition::dsl;

            let mut results: BTreeMap<SledUuid, BTreeMap<M2Slot, _>> =
                BTreeMap::new();

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated_multicolumn(
                    dsl::inv_sled_boot_partition,
                    (dsl::sled_id, dsl::boot_disk_slot),
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvSledBootPartition::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| {
                    (row.sled_id, row.boot_disk_slot)
                });

                for row in batch {
                    let sled_map =
                        results.entry(row.sled_id.into()).or_default();
                    let slot = row.slot().map_err(|err| {
                        Error::internal_error(&format!("{err:#}"))
                    })?;
                    sled_map.insert(slot, row);
                }
            }

            results
        };

        // Load all the config reconciler disk results; build a map of maps
        // keyed by sled ID.
        let mut last_reconciliation_disk_results = {
            use nexus_db_schema::schema::inv_last_reconciliation_disk_result::dsl;

            let mut results: BTreeMap<
                SledUuid,
                BTreeMap<PhysicalDiskUuid, ConfigReconcilerInventoryResult>,
            > = BTreeMap::new();

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::inv_last_reconciliation_disk_result,
                    dsl::disk_id,
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvLastReconciliationDiskResult::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| row.disk_id);

                for row in batch {
                    let sled_map =
                        results.entry(row.sled_id.into()).or_default();
                    sled_map.insert(row.disk_id.into(), row.into());
                }
            }

            results
        };

        // Load all the config reconciler dataset results; build a map of maps
        // keyed by sled ID.
        let mut last_reconciliation_dataset_results = {
            use nexus_db_schema::schema::inv_last_reconciliation_dataset_result::dsl;

            let mut results: BTreeMap<
                SledUuid,
                BTreeMap<DatasetUuid, ConfigReconcilerInventoryResult>,
            > = BTreeMap::new();

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::inv_last_reconciliation_dataset_result,
                    dsl::dataset_id,
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvLastReconciliationDatasetResult::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| row.dataset_id);

                for row in batch {
                    let sled_map =
                        results.entry(row.sled_id.into()).or_default();
                    sled_map.insert(row.dataset_id.into(), row.into());
                }
            }

            results
        };

        // Load all the config reconciler orphaned dataset rows; built a map
        // of sets keyed by sled ID.
        let mut last_reconciliation_orphaned_datasets = {
            use nexus_db_schema::schema::inv_last_reconciliation_orphaned_dataset::dsl;

            let mut orphaned: BTreeMap<SledUuid, IdOrdMap<OrphanedDataset>> =
                BTreeMap::new();

            // TODO-performance This ought to be paginated like the other
            // queries in this method, but
            //
            // (a) this table's primary key is 5 columns, and we don't have
            //     `paginated` support that wide
            // (b) we expect a very small number of orphaned datasets, generally
            //
            // so we just do the lazy thing and load all the rows at once.
            let rows = dsl::inv_last_reconciliation_orphaned_dataset
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvLastReconciliationOrphanedDataset::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

            for row in rows {
                orphaned
                    .entry(row.sled_id.into())
                    .or_default()
                    .insert_unique(row.try_into()?)
                    .map_err(|err| {
                        // We should never get duplicates: the table's primary
                        // key is the dataset name (same as the IdOrdMap)
                        Error::internal_error(&format!(
                            "unexpected duplicate orphaned dataset: {}",
                            InlineErrorChain::new(&err)
                        ))
                    })?;
            }

            orphaned
        };

        // Load all the config reconciler zone results; build a map of maps
        // keyed by sled ID.
        let mut last_reconciliation_zone_results = {
            use nexus_db_schema::schema::inv_last_reconciliation_zone_result::dsl;

            let mut results: BTreeMap<
                SledUuid,
                BTreeMap<OmicronZoneUuid, ConfigReconcilerInventoryResult>,
            > = BTreeMap::new();

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::inv_last_reconciliation_zone_result,
                    dsl::zone_id,
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvLastReconciliationZoneResult::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| row.zone_id);

                for row in batch {
                    let sled_map =
                        results.entry(row.sled_id.into()).or_default();
                    sled_map.insert(row.zone_id.into(), row.into());
                }
            }

            results
        };

        // Load zone_manifest_zone rows.
        let mut zone_manifest_artifacts_by_sled_id = {
            use nexus_db_schema::schema::inv_zone_manifest_zone::dsl;

            let mut by_sled_id: BTreeMap<
                SledUuid,
                IdOrdMap<ZoneArtifactInventory>,
            > = BTreeMap::new();

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated_multicolumn(
                    dsl::inv_zone_manifest_zone,
                    (dsl::sled_id, dsl::zone_file_name),
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvZoneManifestZone::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| {
                    (row.sled_id, row.zone_file_name.clone())
                });

                for row in batch {
                    by_sled_id
                        .entry(row.sled_id.into())
                        .or_default()
                        .insert_unique(row.into())
                        .expect("database ensures the row is unique");
                }
            }

            by_sled_id
        };

        // Load zone-manifest non-boot rows.
        let mut zone_manifest_non_boot_by_sled_id = {
            use nexus_db_schema::schema::inv_zone_manifest_non_boot::dsl;

            let mut by_sled_id: BTreeMap<
                SledUuid,
                IdOrdMap<ZoneManifestNonBootInventory>,
            > = BTreeMap::new();

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated_multicolumn(
                    dsl::inv_zone_manifest_non_boot,
                    (dsl::sled_id, dsl::non_boot_zpool_id),
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvZoneManifestNonBoot::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| {
                    (row.sled_id, row.non_boot_zpool_id)
                });

                for row in batch {
                    by_sled_id
                        .entry(row.sled_id.into())
                        .or_default()
                        .insert_unique(row.into())
                        .expect("database ensures the row is unique");
                }
            }

            by_sled_id
        };

        // Load mupdate-override non-boot rows.
        let mut mupdate_override_non_boot_by_sled_id = {
            use nexus_db_schema::schema::inv_mupdate_override_non_boot::dsl;

            let mut by_sled_id: BTreeMap<
                SledUuid,
                IdOrdMap<MupdateOverrideNonBootInventory>,
            > = BTreeMap::new();

            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated_multicolumn(
                    dsl::inv_mupdate_override_non_boot,
                    (dsl::sled_id, dsl::non_boot_zpool_id),
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvMupdateOverrideNonBoot::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| {
                    (row.sled_id, row.non_boot_zpool_id)
                });
                for row in batch {
                    by_sled_id
                        .entry(row.sled_id.into())
                        .or_default()
                        .insert_unique(row.into())
                        .expect("database ensures the row is unique");
                }
            }

            by_sled_id
        };

        // Now load the clickhouse keeper cluster memberships
        let clickhouse_keeper_cluster_membership = {
            use nexus_db_schema::schema::inv_clickhouse_keeper_membership::dsl;
            let mut memberships = BTreeSet::new();
            let mut paginator = Paginator::new(
                batch_size,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::inv_clickhouse_keeper_membership,
                    dsl::queried_keeper_id,
                    &p.current_pagparams(),
                )
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvClickhouseKeeperMembership::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;
                paginator = p.found_batch(&batch, &|row| row.queried_keeper_id);
                for membership in batch.into_iter() {
                    memberships.insert(
                        ClickhouseKeeperClusterMembership::try_from(membership)
                            .map_err(|e| {
                                Error::internal_error(&format!("{e:#}",))
                            })?,
                    );
                }
            }
            memberships
        };

        // Load the cockroach status records for all nodes.
        let cockroach_status: BTreeMap<CockroachNodeId, CockroachStatus> = {
            use nexus_db_schema::schema::inv_cockroachdb_status::dsl;

            let status_records: Vec<InvCockroachStatus> =
                dsl::inv_cockroachdb_status
                    .filter(dsl::inv_collection_id.eq(db_id))
                    .select(InvCockroachStatus::as_select())
                    .load_async(&*conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel(e, ErrorHandler::Server)
                    })?;

            status_records
                .into_iter()
                .map(|record| {
                    let node_id = CockroachNodeId::new(record.node_id.clone());
                    let status: nexus_types::inventory::CockroachStatus =
                        record.try_into().map_err(|e| {
                            Error::internal_error(&format!("{e:#}"))
                        })?;
                    Ok((node_id, status))
                })
                .collect::<Result<BTreeMap<_, _>, Error>>()?
        };

        // Load TimeSync statuses
        let ntp_timesync = {
            use nexus_db_schema::schema::inv_ntp_timesync::dsl;

            let records: Vec<InvNtpTimesync> = dsl::inv_ntp_timesync
                .filter(dsl::inv_collection_id.eq(db_id))
                .select(InvNtpTimesync::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

            records
                .into_iter()
                .map(|record| TimeSync::from(record))
                .collect::<IdOrdMap<_>>()
        };

        // Finally, build up the sled-agent map using the sled agent and
        // omicron zone rows. A for loop is easier to understand than into_iter
        // + filter_map + return Result + collect.
        let mut sled_agents = IdOrdMap::new();
        for s in sled_agent_rows {
            let sled_id = SledUuid::from(s.sled_id);
            let baseboard_id = s
                .hw_baseboard_id
                .map(|id| {
                    baseboards_by_id.get(&id).cloned().ok_or_else(|| {
                        Error::internal_error(
                            "missing baseboard that we should have fetched",
                        )
                    })
                })
                .transpose()?;

            // Convert all the sled config foreign keys back into fully realized
            // `OmicronSledConfig`s. We have to clone these: the same
            // `OmicronSledConfig` may be referenced up to three times by this
            // sled.
            let ledgered_sled_config = s
                .ledgered_sled_config
                .map(|id| {
                    omicron_sled_configs.get(&id.into()).as_ref()
                    .map(|c| c.config.clone())
                        .ok_or_else(|| {
                        Error::internal_error(
                            "missing sled config that we should have fetched",
                        )
                    })
                })
                .transpose()?;
            let reconciler_status = s
                .reconciler_status
                .to_status(|config_id| {
                    omicron_sled_configs
                        .get(config_id)
                        .as_ref()
                        .map(|c| c.config.clone())
                })
                .map_err(|e| Error::internal_error(&format!("{e:#}")))?;
            let last_reconciliation = sled_config_reconcilers
                .remove(&sled_id)
                .map(|reconciler| {
                    let last_reconciled_config = omicron_sled_configs
                        .get(&reconciler.last_reconciled_config.into())
                        .as_ref()
                        .ok_or_else(|| {
                            Error::internal_error(
                                "missing sled config that we \
                                 should have fetched",
                            )
                        })?
                        .config
                        .clone();

                    let boot_partitions = {
                        let boot_disk =
                            reconciler.boot_disk().map_err(|err| {
                                Error::internal_error(&format!("{err:#}"))
                            })?;

                        // Helper to convert our nullable error column into
                        // either the error message (if present) or, if NULL,
                        // looking up the `BootPartitionDetails` from the rows
                        // we loaded above. We have to do this for both slots.
                        let mut slot_details = |maybe_err, slot| match maybe_err
                        {
                            Some(err) => Ok(Err(err)),
                            None => sled_boot_partition_details
                                .get_mut(&sled_id)
                                .and_then(|by_slot| by_slot.remove(&slot))
                                .map(|details| {
                                    Ok(BootPartitionDetails::from(details))
                                })
                                .ok_or_else(|| {
                                    Error::internal_error(
                                        "missing boot partition details that \
                                         we should have fetched",
                                    )
                                }),
                        };

                        let slot_a = slot_details(
                            reconciler.boot_partition_a_error,
                            M2Slot::A,
                        )?;
                        let slot_b = slot_details(
                            reconciler.boot_partition_b_error,
                            M2Slot::B,
                        )?;

                        BootPartitionContents { boot_disk, slot_a, slot_b }
                    };

                    let clear_mupdate_override = reconciler
                        .clear_mupdate_override
                        .into_inventory()
                        .map_err(|err| {
                            Error::internal_error(&format!("{err:#}"))
                        })?;

                    Ok::<_, Error>(ConfigReconcilerInventory {
                        last_reconciled_config,
                        external_disks: last_reconciliation_disk_results
                            .remove(&sled_id)
                            .unwrap_or_default(),
                        datasets: last_reconciliation_dataset_results
                            .remove(&sled_id)
                            .unwrap_or_default(),
                        orphaned_datasets:
                            last_reconciliation_orphaned_datasets
                                .remove(&sled_id)
                                .unwrap_or_default(),
                        zones: last_reconciliation_zone_results
                            .remove(&sled_id)
                            .unwrap_or_default(),
                        boot_partitions,
                        clear_mupdate_override,
                    })
                })
                .transpose()?;

            let zone_image_resolver = s
                .zone_image_resolver
                .into_inventory(
                    zone_manifest_artifacts_by_sled_id.remove(&sled_id),
                    zone_manifest_non_boot_by_sled_id.remove(&sled_id),
                    mupdate_override_non_boot_by_sled_id.remove(&sled_id),
                )
                .map_err(|e| {
                    Error::internal_error(&format!(
                        "failed to create zone image resolver inventory \
                         for sled {sled_id}: {}",
                        InlineErrorChain::new(e.as_ref()),
                    ))
                })?;

            let sled_agent = nexus_types::inventory::SledAgent {
                time_collected: s.time_collected,
                source: s.source,
                sled_id,
                baseboard_id,
                sled_agent_address: std::net::SocketAddrV6::new(
                    std::net::Ipv6Addr::from(s.sled_agent_ip),
                    u16::from(s.sled_agent_port),
                    0,
                    0,
                ),
                sled_role: s.sled_role.into(),
                usable_hardware_threads: u32::from(s.usable_hardware_threads),
                usable_physical_ram: s.usable_physical_ram.into(),
                reservoir_size: s.reservoir_size.into(),
                // For disks, zpools, and datasets, the map for a sled ID is
                // only populated if there is at least one disk/zpool/dataset
                // for that sled. The `unwrap_or_default` calls cover the case
                // where there are no disks/zpools/datasets for a sled.
                disks: physical_disks
                    .get(&sled_id)
                    .map(|disks| disks.to_vec())
                    .unwrap_or_default(),
                zpools: zpools
                    .get(sled_id.as_untyped_uuid())
                    .map(|zpools| zpools.to_vec())
                    .unwrap_or_default(),
                datasets: datasets
                    .get(sled_id.as_untyped_uuid())
                    .map(|datasets| datasets.to_vec())
                    .unwrap_or_default(),
                ledgered_sled_config,
                reconciler_status,
                last_reconciliation,
                zone_image_resolver,
            };
            sled_agents
                .insert_unique(sled_agent)
                .expect("database ensures sled_agent_rows has unique sled_id");
        }

        // Check that we consumed all the reconciliation results we found in
        // this collection.
        bail_unless!(
            sled_config_reconcilers.is_empty(),
            "found extra sled config reconcilers: {:?}",
            sled_config_reconcilers.keys(),
        );
        {
            // `sled_boot_partition_details` is a map of maps; we don't prune
            // the outermost map, but they should all be empty.
            let sleds_with_leftover_boot_partitions =
                sled_boot_partition_details
                    .iter()
                    .filter_map(|(sled_id, boot_partitions)| {
                        if boot_partitions.is_empty() {
                            None
                        } else {
                            Some(sled_id)
                        }
                    })
                    .collect::<Vec<_>>();
            bail_unless!(
                sleds_with_leftover_boot_partitions.is_empty(),
                "found extra sled boot partition details: {:?}",
                sleds_with_leftover_boot_partitions,
            );
        }
        bail_unless!(
            last_reconciliation_disk_results.is_empty(),
            "found extra config reconciliation disk results: {:?}",
            last_reconciliation_disk_results.keys()
        );
        bail_unless!(
            last_reconciliation_dataset_results.is_empty(),
            "found extra config reconciliation dataset results: {:?}",
            last_reconciliation_dataset_results.keys()
        );
        bail_unless!(
            last_reconciliation_zone_results.is_empty(),
            "found extra config reconciliation zone results: {:?}",
            last_reconciliation_zone_results.keys()
        );
        bail_unless!(
            zone_manifest_artifacts_by_sled_id.is_empty(),
            "found extra zone manifest artifacts: {:?}",
            zone_manifest_artifacts_by_sled_id.keys()
        );
        bail_unless!(
            zone_manifest_non_boot_by_sled_id.is_empty(),
            "found extra zone manifest non-boot entries: {:?}",
            zone_manifest_non_boot_by_sled_id.keys()
        );
        bail_unless!(
            mupdate_override_non_boot_by_sled_id.is_empty(),
            "found extra mupdate override non-boot entries: {:?}",
            mupdate_override_non_boot_by_sled_id.keys()
        );

        Ok(Collection {
            id,
            errors,
            time_started,
            time_done,
            collector,
            baseboards: baseboards_by_id.values().cloned().collect(),
            cabooses: cabooses_by_id.values().cloned().collect(),
            rot_pages: rot_pages_by_id.values().cloned().collect(),
            sps,
            host_phase_1_flash_hashes,
            rots,
            cabooses_found,
            rot_pages_found,
            sled_agents,
            clickhouse_keeper_cluster_membership,
            cockroach_status,
            ntp_timesync,
        })
    }
}

#[derive(Debug)]
struct OmicronSledConfigWithId {
    id: OmicronSledConfigUuid,
    config: OmicronSledConfig,
}

impl IdMappable for OmicronSledConfigWithId {
    type Id = OmicronSledConfigUuid;

    fn id(&self) -> Self::Id {
        self.id
    }
}

#[derive(Debug)]
struct ConfigReconcilerFields {
    ledgered_sled_config: Option<OmicronSledConfigUuid>,
    reconciler_status: InvConfigReconcilerStatus,
}

// Helper to build the sets of rows for all the `OmicronSledConfig`s and
// per-sled config reconciler status rows for an inventory collection.
#[derive(Debug, Default)]
struct ConfigReconcilerRows {
    config_reconcilers: Vec<InvSledConfigReconciler>,
    sled_configs: Vec<InvOmicronSledConfig>,
    disks: Vec<InvOmicronSledConfigDisk>,
    datasets: Vec<InvOmicronSledConfigDataset>,
    zones: Vec<InvOmicronSledConfigZone>,
    zone_nics: Vec<InvOmicronSledConfigZoneNic>,
    disk_results: Vec<InvLastReconciliationDiskResult>,
    dataset_results: Vec<InvLastReconciliationDatasetResult>,
    orphaned_datasets: Vec<InvLastReconciliationOrphanedDataset>,
    zone_results: Vec<InvLastReconciliationZoneResult>,
    boot_partitions: Vec<InvSledBootPartition>,
    config_reconciler_fields_by_sled:
        BTreeMap<SledUuid, ConfigReconcilerFields>,
}

impl ConfigReconcilerRows {
    fn new(
        collection_id: CollectionUuid,
        collection: &Collection,
    ) -> anyhow::Result<Self> {
        let mut this = Self::default();
        for sled_agent in &collection.sled_agents {
            this.accumulate(collection_id, sled_agent)?;
        }
        Ok(this)
    }

    fn accumulate(
        &mut self,
        collection_id: CollectionUuid,
        sled_agent: &SledAgent,
    ) -> anyhow::Result<()> {
        let sled_id = sled_agent.sled_id;
        let mut ledgered_sled_config = None;
        if let Some(config) = &sled_agent.ledgered_sled_config {
            ledgered_sled_config =
                Some(self.accumulate_config(collection_id, config)?);
        }

        let mut last_reconciliation_config_id = None;
        if let Some(last_reconciliation) = &sled_agent.last_reconciliation {
            // If this config exactly matches the ledgered sled config, we can
            // reuse the foreign key; otherwise, accumulate the new one.
            let last_reconciled_config =
                if Some(&last_reconciliation.last_reconciled_config)
                    == sled_agent.ledgered_sled_config.as_ref()
                {
                    // We always set this to `Some(_)` above if we have a
                    // ledgered sled config, which we must to pass this check.
                    ledgered_sled_config
                        .expect("always Some(_) if we have a ledgered config")
                } else {
                    self.accumulate_config(
                        collection_id,
                        &last_reconciliation.last_reconciled_config,
                    )?
                };
            last_reconciliation_config_id = Some(last_reconciled_config);
            let clear_mupdate_override = InvClearMupdateOverride::new(
                last_reconciliation.clear_mupdate_override.as_ref(),
            );

            self.config_reconcilers.push(InvSledConfigReconciler::new(
                collection_id,
                sled_id,
                last_reconciled_config,
                last_reconciliation.boot_partitions.boot_disk.clone(),
                last_reconciliation
                    .boot_partitions
                    .slot_a
                    .as_ref()
                    .err()
                    .cloned(),
                last_reconciliation
                    .boot_partitions
                    .slot_b
                    .as_ref()
                    .err()
                    .cloned(),
                clear_mupdate_override,
            ));

            // Boot partition _errors_ are kept in `InvSledConfigReconciler`
            // above, but non-errors get their own rows; handle those here.
            //
            // `.into_iter().flatten()` strips out `None`s (i.e., slots that had
            // an error, after the conversions we do here).
            for (slot, details) in [
                last_reconciliation
                    .boot_partitions
                    .slot_a
                    .as_ref()
                    .ok()
                    .map(|details| (M2Slot::A, details.clone())),
                last_reconciliation
                    .boot_partitions
                    .slot_b
                    .as_ref()
                    .ok()
                    .map(|details| (M2Slot::B, details.clone())),
            ]
            .into_iter()
            .flatten()
            {
                self.boot_partitions.push(InvSledBootPartition::new(
                    collection_id,
                    sled_id,
                    slot,
                    details,
                ));
            }

            self.disk_results.extend(
                last_reconciliation.external_disks.iter().map(
                    |(disk_id, result)| {
                        InvLastReconciliationDiskResult::new(
                            collection_id,
                            sled_id,
                            *disk_id,
                            result.clone(),
                        )
                    },
                ),
            );
            self.dataset_results.extend(
                last_reconciliation.datasets.iter().map(
                    |(dataset_id, result)| {
                        InvLastReconciliationDatasetResult::new(
                            collection_id,
                            sled_id,
                            *dataset_id,
                            result.clone(),
                        )
                    },
                ),
            );
            self.orphaned_datasets.extend(
                last_reconciliation.orphaned_datasets.iter().map(|dataset| {
                    InvLastReconciliationOrphanedDataset::new(
                        collection_id,
                        sled_id,
                        dataset.clone(),
                    )
                }),
            );
            self.zone_results.extend(last_reconciliation.zones.iter().map(
                |(zone_id, result)| {
                    InvLastReconciliationZoneResult::new(
                        collection_id,
                        sled_id,
                        *zone_id,
                        result.clone(),
                    )
                },
            ));
        }

        let reconciler_status = match &sled_agent.reconciler_status {
            ConfigReconcilerInventoryStatus::NotYetRun => {
                InvConfigReconcilerStatus {
                    reconciler_status_kind:
                        InvConfigReconcilerStatusKind::NotYetRun,
                    reconciler_status_sled_config: None,
                    reconciler_status_timestamp: None,
                    reconciler_status_duration_secs: None,
                }
            }
            ConfigReconcilerInventoryStatus::Running {
                config,
                started_at,
                running_for,
            } => {
                // If this config exactly matches the ledgered or
                // most-recently-reconciled configs, we can reuse those IDs.
                // Otherwise, accumulate a new one.
                let reconciler_status_sled_config = if Some(config)
                    == sled_agent.ledgered_sled_config.as_ref()
                {
                    ledgered_sled_config
                } else if Some(config)
                    == sled_agent
                        .last_reconciliation
                        .as_ref()
                        .map(|lr| &lr.last_reconciled_config)
                {
                    last_reconciliation_config_id
                } else {
                    Some(self.accumulate_config(collection_id, config)?)
                };
                InvConfigReconcilerStatus {
                    reconciler_status_kind:
                        InvConfigReconcilerStatusKind::Running,
                    reconciler_status_sled_config:
                        reconciler_status_sled_config.map(to_db_typed_uuid),
                    reconciler_status_timestamp: Some(*started_at),
                    reconciler_status_duration_secs: Some(
                        running_for.as_secs_f64(),
                    ),
                }
            }
            ConfigReconcilerInventoryStatus::Idle { completed_at, ran_for } => {
                InvConfigReconcilerStatus {
                    reconciler_status_kind: InvConfigReconcilerStatusKind::Idle,
                    reconciler_status_sled_config: None,
                    reconciler_status_timestamp: Some(*completed_at),
                    reconciler_status_duration_secs: Some(
                        ran_for.as_secs_f64(),
                    ),
                }
            }
        };

        self.config_reconciler_fields_by_sled.insert(
            sled_id,
            ConfigReconcilerFields { ledgered_sled_config, reconciler_status },
        );

        Ok(())
    }

    fn accumulate_config(
        &mut self,
        collection_id: CollectionUuid,
        config: &OmicronSledConfig,
    ) -> anyhow::Result<OmicronSledConfigUuid> {
        let sled_config_id = OmicronSledConfigUuid::new_v4();

        self.sled_configs.push(InvOmicronSledConfig::new(
            collection_id,
            sled_config_id,
            config.generation,
            config.remove_mupdate_override,
            config.host_phase_2.clone(),
        ));
        self.disks.extend(config.disks.iter().map(|disk| {
            InvOmicronSledConfigDisk::new(
                collection_id,
                sled_config_id,
                disk.clone(),
            )
        }));
        self.datasets.extend(config.datasets.iter().map(|dataset| {
            InvOmicronSledConfigDataset::new(
                collection_id,
                sled_config_id,
                dataset,
            )
        }));
        for zone in &config.zones {
            self.zones.push(InvOmicronSledConfigZone::new(
                collection_id,
                sled_config_id,
                zone,
            )?);
            if let Some(nic) = InvOmicronSledConfigZoneNic::new(
                collection_id,
                sled_config_id,
                zone,
            )? {
                self.zone_nics.push(nic);
            }
        }

        Ok(sled_config_id)
    }
}

/// Extra interfaces that are not intended (and potentially unsafe) for use in
/// Nexus, but useful for testing and `omdb`
pub trait DataStoreInventoryTest: Send + Sync {
    /// List all collections
    ///
    /// This does not paginate.
    fn inventory_collections(
        &self,
    ) -> BoxFuture<anyhow::Result<Vec<InvCollection>>>;
}

impl DataStoreInventoryTest for DataStore {
    fn inventory_collections(
        &self,
    ) -> BoxFuture<anyhow::Result<Vec<InvCollection>>> {
        async {
            let conn = self
                .pool_connection_for_tests()
                .await
                .context("getting connection")?;

            // This transaction is used by tests, and does not need to retry.
            #[allow(clippy::disallowed_methods)]
            conn.transaction_async(|conn| async move {
                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL)
                    .await
                    .context("failed to allow table scan")?;

                use nexus_db_schema::schema::inv_collection::dsl;
                let collections = dsl::inv_collection
                    .select(InvCollection::as_select())
                    .order_by(dsl::time_started)
                    .load_async(&conn)
                    .await
                    .context("failed to list collections")?;

                Ok(collections)
            })
            .await
        }
        .boxed()
    }
}

#[cfg(test)]
mod test {
    use crate::db::DataStore;
    use crate::db::datastore::DataStoreConnection;
    use crate::db::datastore::inventory::DataStoreInventoryTest;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::{QueryBuilder, TrustedStr};
    use anyhow::{Context, bail};
    use async_bb8_diesel::AsyncConnection;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use async_bb8_diesel::AsyncSimpleConnection;
    use diesel::QueryDsl;
    use gateway_client::types::SpType;
    use nexus_db_schema::schema;
    use nexus_inventory::examples::Representative;
    use nexus_inventory::examples::representative;
    use nexus_inventory::now_db_precision;
    use nexus_sled_agent_shared::inventory::BootPartitionContents;
    use nexus_sled_agent_shared::inventory::BootPartitionDetails;
    use nexus_sled_agent_shared::inventory::OrphanedDataset;
    use nexus_sled_agent_shared::inventory::{
        BootImageHeader, ClearMupdateOverrideBootSuccessInventory,
        ClearMupdateOverrideInventory,
    };
    use nexus_sled_agent_shared::inventory::{
        ConfigReconcilerInventory, ConfigReconcilerInventoryResult,
        ConfigReconcilerInventoryStatus, OmicronZoneImageSource,
    };
    use nexus_test_utils::db::ALLOW_FULL_TABLE_SCAN_SQL;
    use nexus_types::inventory::BaseboardId;
    use nexus_types::inventory::CabooseWhich;
    use nexus_types::inventory::RotPageWhich;
    use omicron_common::api::external::Error;
    use omicron_common::disk::DatasetKind;
    use omicron_common::disk::DatasetName;
    use omicron_common::disk::M2Slot;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::{
        CollectionUuid, DatasetUuid, OmicronZoneUuid, PhysicalDiskUuid,
        ZpoolUuid,
    };
    use pretty_assertions::assert_eq;
    use std::num::NonZeroU32;
    use std::time::Duration;
    use tufaceous_artifact::ArtifactHash;

    struct CollectionCounts {
        baseboards: usize,
        cabooses: usize,
        rot_pages: usize,
    }

    impl CollectionCounts {
        async fn new(conn: &DataStoreConnection) -> anyhow::Result<Self> {
            // This transaction is used by tests, and does not need to retry.
            #[allow(clippy::disallowed_methods)]
            conn.transaction_async(|conn| async move {
                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL)
                    .await
                    .unwrap();
                let bb_count = schema::hw_baseboard_id::dsl::hw_baseboard_id
                    .select(diesel::dsl::count_star())
                    .first_async::<i64>(&conn)
                    .await
                    .context("failed to count baseboards")?;
                let caboose_count = schema::sw_caboose::dsl::sw_caboose
                    .select(diesel::dsl::count_star())
                    .first_async::<i64>(&conn)
                    .await
                    .context("failed to count cabooses")?;
                let rot_page_count =
                    schema::sw_root_of_trust_page::dsl::sw_root_of_trust_page
                        .select(diesel::dsl::count_star())
                        .first_async::<i64>(&conn)
                        .await
                        .context("failed to count rot pages")?;
                let baseboards = usize::try_from(bb_count)
                    .context("failed to convert baseboard count to usize")?;
                let cabooses = usize::try_from(caboose_count)
                    .context("failed to convert caboose count to usize")?;
                let rot_pages = usize::try_from(rot_page_count)
                    .context("failed to convert rot page count to usize")?;
                Ok(Self { baseboards, cabooses, rot_pages })
            })
            .await
        }
    }

    #[tokio::test]
    async fn test_find_hw_baseboard_id_missing_returns_not_found() {
        let logctx = dev::test_setup_log("inventory_insert");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let baseboard_id = BaseboardId {
            serial_number: "some-serial".into(),
            part_number: "some-part".into(),
        };
        let err = datastore
            .find_hw_baseboard_id(&opctx, &baseboard_id)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ObjectNotFound { .. }));
        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Tests inserting several collections, reading them back, and making sure
    /// they look the same.
    #[tokio::test]
    async fn test_inventory_insert() {
        // Setup
        let logctx = dev::test_setup_log("inventory_insert");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create an empty collection and write it to the database.
        let builder = nexus_inventory::CollectionBuilder::new("test");
        let collection1 = builder.build();
        datastore
            .inventory_insert_collection(&opctx, &collection1)
            .await
            .expect("failed to insert collection");

        // Read it back.
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let collection_read = datastore
            .inventory_collection_read(&opctx, collection1.id)
            .await
            .expect("failed to read collection back");
        assert_eq!(collection1, collection_read);

        // There ought to be no baseboards, cabooses, or RoT pages in the
        // database from that collection.
        assert_eq!(collection1.baseboards.len(), 0);
        assert_eq!(collection1.cabooses.len(), 0);
        assert_eq!(collection1.rot_pages.len(), 0);
        let coll_counts = CollectionCounts::new(&conn).await.unwrap();
        assert_eq!(collection1.baseboards.len(), coll_counts.baseboards);
        assert_eq!(collection1.cabooses.len(), coll_counts.cabooses);
        assert_eq!(collection1.rot_pages.len(), coll_counts.rot_pages);

        // Now insert a more complex collection, write it to the database, and
        // read it back.
        let Representative { builder, .. } = representative();
        let collection2 = builder.build();
        datastore
            .inventory_insert_collection(&opctx, &collection2)
            .await
            .expect("failed to insert collection");
        let collection_read = datastore
            .inventory_collection_read(&opctx, collection2.id)
            .await
            .expect("failed to read collection back");
        assert_eq!(collection2, collection_read);
        // Verify that we have exactly the set of cabooses, baseboards, and RoT
        // pages in the databases that came from this first non-empty
        // collection.
        assert_ne!(collection2.baseboards.len(), collection1.baseboards.len());
        assert_ne!(collection2.cabooses.len(), collection1.cabooses.len());
        assert_ne!(collection2.rot_pages.len(), collection1.rot_pages.len());
        let coll_counts = CollectionCounts::new(&conn).await.unwrap();
        assert_eq!(collection2.baseboards.len(), coll_counts.baseboards);
        assert_eq!(collection2.cabooses.len(), coll_counts.cabooses);
        assert_eq!(collection2.rot_pages.len(), coll_counts.rot_pages);

        // Try another read with a batch size of 1, and assert we got all the
        // same data as the previous read with the default batch size. This
        // ensures that we correctly handle queries over the batch size, without
        // having to actually read 1000s of records.
        let batched_read = datastore
            .inventory_collection_read_batched(
                &opctx,
                collection2.id,
                NonZeroU32::new(1).unwrap(),
            )
            .await
            .expect("failed to read back with batch size 1");
        assert_eq!(
            collection_read, batched_read,
            "read with default batch size and read with batch size 1 must \
            return the same results"
        );

        // Now insert an equivalent collection again.  Verify the distinct
        // baseboards, cabooses, and RoT pages again.  This is important: the
        // insertion process should re-use the baseboards, cabooses, and RoT
        // pages from the previous collection.
        let Representative { builder, .. } = representative();
        let collection3 = builder.build();
        datastore
            .inventory_insert_collection(&opctx, &collection3)
            .await
            .expect("failed to insert collection");
        let collection_read = datastore
            .inventory_collection_read(&opctx, collection3.id)
            .await
            .expect("failed to read collection back");
        assert_eq!(collection3, collection_read);
        // Verify that we have the same number of cabooses, baseboards, and RoT
        // pages, since those didn't change.
        assert_eq!(collection3.baseboards.len(), collection2.baseboards.len());
        assert_eq!(collection3.cabooses.len(), collection2.cabooses.len());
        assert_eq!(collection3.rot_pages.len(), collection2.rot_pages.len());
        let coll_counts = CollectionCounts::new(&conn).await.unwrap();
        assert_eq!(collection3.baseboards.len(), coll_counts.baseboards);
        assert_eq!(collection3.cabooses.len(), coll_counts.cabooses);
        assert_eq!(collection3.rot_pages.len(), coll_counts.rot_pages);

        // Now insert a collection that's almost equivalent, but has an extra
        // couple of baseboards, one caboose, and one RoT page.  Verify that we
        // re-use the existing ones, but still insert the new ones.
        let Representative { mut builder, .. } = representative();
        builder.found_sp_state(
            "test suite",
            SpType::Switch,
            1,
            nexus_inventory::examples::sp_state("2"),
        );
        let bb = builder
            .found_sp_state(
                "test suite",
                SpType::Power,
                1,
                nexus_inventory::examples::sp_state("3"),
            )
            .unwrap();
        builder
            .found_caboose(
                &bb,
                CabooseWhich::SpSlot0,
                "dummy",
                nexus_inventory::examples::caboose("dummy"),
            )
            .unwrap();
        builder
            .found_rot_page(
                &bb,
                RotPageWhich::Cmpa,
                "dummy",
                nexus_inventory::examples::rot_page("dummy"),
            )
            .unwrap();
        let collection4 = builder.build();
        datastore
            .inventory_insert_collection(&opctx, &collection4)
            .await
            .expect("failed to insert collection");
        let collection_read = datastore
            .inventory_collection_read(&opctx, collection4.id)
            .await
            .expect("failed to read collection back");
        assert_eq!(collection4, collection_read);
        // Verify the number of baseboards and collections again.
        assert_eq!(
            collection4.baseboards.len(),
            collection3.baseboards.len() + 2
        );
        assert_eq!(collection4.cabooses.len(), collection3.cabooses.len() + 1);
        assert_eq!(
            collection4.rot_pages.len(),
            collection3.rot_pages.len() + 1
        );
        let coll_counts = CollectionCounts::new(&conn).await.unwrap();
        assert_eq!(collection4.baseboards.len(), coll_counts.baseboards);
        assert_eq!(collection4.cabooses.len(), coll_counts.cabooses);
        assert_eq!(collection4.rot_pages.len(), coll_counts.rot_pages);

        // This time, go back to our earlier collection.  This logically removes
        // some baseboards.  They should still be present in the database, but
        // not in the collection.
        let Representative { builder, .. } = representative();
        let collection5 = builder.build();
        datastore
            .inventory_insert_collection(&opctx, &collection5)
            .await
            .expect("failed to insert collection");
        let collection_read = datastore
            .inventory_collection_read(&opctx, collection5.id)
            .await
            .expect("failed to read collection back");
        assert_eq!(collection5, collection_read);
        assert_eq!(collection5.baseboards.len(), collection3.baseboards.len());
        assert_eq!(collection5.cabooses.len(), collection3.cabooses.len());
        assert_eq!(collection5.rot_pages.len(), collection3.rot_pages.len());
        assert_ne!(collection5.baseboards.len(), collection4.baseboards.len());
        assert_ne!(collection5.cabooses.len(), collection4.cabooses.len());
        assert_ne!(collection5.rot_pages.len(), collection4.rot_pages.len());
        let coll_counts = CollectionCounts::new(&conn).await.unwrap();
        assert_eq!(collection4.baseboards.len(), coll_counts.baseboards);
        assert_eq!(collection4.cabooses.len(), coll_counts.cabooses);
        assert_eq!(collection4.rot_pages.len(), coll_counts.rot_pages);

        // Try to insert the same collection again and make sure it fails.
        let error = datastore
            .inventory_insert_collection(&opctx, &collection5)
            .await
            .expect_err("unexpectedly succeeded in inserting collection");
        assert!(
            format!("{:#}", error)
                .contains("duplicate key value violates unique constraint")
        );

        // Now that we've inserted a bunch of collections, we can test pruning.
        //
        // The datastore should start by pruning the oldest collection, unless
        // it's the only collection with no errors.  The oldest one is
        // `collection1`, which _is_ the only one with no errors.  So we should
        // get back `collection2`.
        assert_eq!(
            &datastore
                .inventory_collections()
                .await
                .unwrap()
                .iter()
                .map(|c| c.id.into())
                .collect::<Vec<CollectionUuid>>(),
            &[
                collection1.id,
                collection2.id,
                collection3.id,
                collection4.id,
                collection5.id,
            ]
        );
        println!(
            "all collections: {:?}\n",
            &[
                collection1.id,
                collection2.id,
                collection3.id,
                collection4.id,
                collection5.id,
            ]
        );
        datastore
            .inventory_prune_collections(&opctx, 4)
            .await
            .expect("failed to prune collections");
        assert_eq!(
            datastore
                .inventory_collections()
                .await
                .unwrap()
                .iter()
                .map(|c| c.id.into())
                .collect::<Vec<CollectionUuid>>(),
            &[collection1.id, collection3.id, collection4.id, collection5.id,]
        );
        // Again, we should skip over collection1 and delete the next oldest:
        // collection3.
        datastore
            .inventory_prune_collections(&opctx, 3)
            .await
            .expect("failed to prune collections");
        assert_eq!(
            datastore
                .inventory_collections()
                .await
                .unwrap()
                .iter()
                .map(|c| c.id.into())
                .collect::<Vec<CollectionUuid>>(),
            &[collection1.id, collection4.id, collection5.id,]
        );
        // At this point, if we're keeping 3, we don't need to prune anything.
        datastore
            .inventory_prune_collections(&opctx, 3)
            .await
            .expect("failed to prune collections");
        assert_eq!(
            datastore
                .inventory_collections()
                .await
                .unwrap()
                .iter()
                .map(|c| c.id.into())
                .collect::<Vec<CollectionUuid>>(),
            &[collection1.id, collection4.id, collection5.id,]
        );

        // If we then insert an empty collection (which has no errors),
        // collection1 becomes pruneable.
        let builder = nexus_inventory::CollectionBuilder::new("test");
        let collection6 = builder.build();
        println!(
            "collection 6: {} ({:?})",
            collection6.id, collection6.time_started
        );
        datastore
            .inventory_insert_collection(&opctx, &collection6)
            .await
            .expect("failed to insert collection");
        assert_eq!(
            datastore
                .inventory_collections()
                .await
                .unwrap()
                .iter()
                .map(|c| c.id.into())
                .collect::<Vec<CollectionUuid>>(),
            &[collection1.id, collection4.id, collection5.id, collection6.id,]
        );
        datastore
            .inventory_prune_collections(&opctx, 3)
            .await
            .expect("failed to prune collections");
        assert_eq!(
            datastore
                .inventory_collections()
                .await
                .unwrap()
                .iter()
                .map(|c| c.id.into())
                .collect::<Vec<CollectionUuid>>(),
            &[collection4.id, collection5.id, collection6.id,]
        );
        // Again, at this point, we should not prune anything.
        datastore
            .inventory_prune_collections(&opctx, 3)
            .await
            .expect("failed to prune collections");
        assert_eq!(
            datastore
                .inventory_collections()
                .await
                .unwrap()
                .iter()
                .map(|c| c.id.into())
                .collect::<Vec<CollectionUuid>>(),
            &[collection4.id, collection5.id, collection6.id,]
        );

        // If we insert another collection with errors, then prune, we should
        // end up pruning collection 4.
        let Representative { builder, .. } = representative();
        let collection7 = builder.build();
        println!(
            "collection 7: {} ({:?})",
            collection7.id, collection7.time_started
        );
        datastore
            .inventory_insert_collection(&opctx, &collection7)
            .await
            .expect("failed to insert collection");
        datastore
            .inventory_prune_collections(&opctx, 3)
            .await
            .expect("failed to prune collections");
        assert_eq!(
            datastore
                .inventory_collections()
                .await
                .unwrap()
                .iter()
                .map(|c| c.id.into())
                .collect::<Vec<CollectionUuid>>(),
            &[collection5.id, collection6.id, collection7.id,]
        );

        // If we try to fetch a pruned collection, we should get nothing.
        let _ = datastore
            .inventory_collection_read(&opctx, collection4.id)
            .await
            .expect_err("unexpectedly read pruned collection");

        // But we should still be able to fetch the collections that do exist.
        let collection_read = datastore
            .inventory_collection_read(&opctx, collection5.id)
            .await
            .unwrap();
        assert_eq!(collection5, collection_read);
        let collection_read = datastore
            .inventory_collection_read(&opctx, collection6.id)
            .await
            .unwrap();
        assert_eq!(collection6, collection_read);
        let collection_read = datastore
            .inventory_collection_read(&opctx, collection7.id)
            .await
            .unwrap();
        assert_eq!(collection7, collection_read);

        // We should prune more than one collection, if needed.  We'll wind up
        // with just collection6 because that's the latest one with no errors.
        datastore
            .inventory_prune_collections(&opctx, 1)
            .await
            .expect("failed to prune collections");
        assert_eq!(
            datastore
                .inventory_collections()
                .await
                .unwrap()
                .iter()
                .map(|c| c.id.into())
                .collect::<Vec<CollectionUuid>>(),
            &[collection6.id,]
        );

        // Remove the remaining collection and make sure the inventory tables
        // are empty (i.e., we got everything).
        datastore
            .inventory_delete_collection(&opctx, collection6.id)
            .await
            .expect("failed to delete collection");
        assert!(datastore.inventory_collections().await.unwrap().is_empty());

        // This transaction is used by tests, and does not need to retry.
        #[allow(clippy::disallowed_methods)]
        conn.transaction_async(|conn| async move {
            conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await.unwrap();
            let count = schema::inv_collection::dsl::inv_collection
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .unwrap();
            assert_eq!(0, count);
            let count = schema::inv_collection_error::dsl::inv_collection_error
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .unwrap();
            assert_eq!(0, count);
            let count =
                schema::inv_service_processor::dsl::inv_service_processor
                    .select(diesel::dsl::count_star())
                    .first_async::<i64>(&conn)
                    .await
                    .unwrap();
            assert_eq!(0, count);
            let count = schema::inv_root_of_trust::dsl::inv_root_of_trust
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .unwrap();
            assert_eq!(0, count);
            let count = schema::inv_caboose::dsl::inv_caboose
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .unwrap();
            assert_eq!(0, count);
            let count =
                schema::inv_root_of_trust_page::dsl::inv_root_of_trust_page
                    .select(diesel::dsl::count_star())
                    .first_async::<i64>(&conn)
                    .await
                    .unwrap();
            assert_eq!(0, count);
            let count = schema::inv_sled_agent::dsl::inv_sled_agent
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .unwrap();
            assert_eq!(0, count);
            let count = schema::inv_dataset::dsl::inv_dataset
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .unwrap();
            assert_eq!(0, count);
            let count = schema::inv_physical_disk::dsl::inv_physical_disk
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .unwrap();
            assert_eq!(0, count);
            let count =
                schema::inv_omicron_sled_config::dsl::inv_omicron_sled_config
                    .select(diesel::dsl::count_star())
                    .first_async::<i64>(&conn)
                    .await
                    .unwrap();
            assert_eq!(0, count);
            let count = schema::inv_last_reconciliation_disk_result::dsl::inv_last_reconciliation_disk_result
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .unwrap();
            assert_eq!(0, count);
            let count = schema::inv_last_reconciliation_dataset_result::dsl::inv_last_reconciliation_dataset_result
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .unwrap();
            assert_eq!(0, count);
            let count = schema::inv_last_reconciliation_zone_result::dsl::inv_last_reconciliation_zone_result
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .unwrap();
            assert_eq!(0, count);
            let count = schema::inv_omicron_sled_config::dsl::inv_omicron_sled_config
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .unwrap();
            assert_eq!(0, count);
            let count = schema::inv_omicron_sled_config_disk::dsl::inv_omicron_sled_config_disk
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .unwrap();
            assert_eq!(0, count);
            let count = schema::inv_omicron_sled_config_dataset::dsl::inv_omicron_sled_config_dataset
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .unwrap();
            assert_eq!(0, count);
            let count = schema::inv_omicron_sled_config_zone::dsl::inv_omicron_sled_config_zone
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .unwrap();
            assert_eq!(0, count);
            let count = schema::inv_omicron_sled_config_zone_nic::dsl::inv_omicron_sled_config_zone_nic
                .select(diesel::dsl::count_star())
                .first_async::<i64>(&conn)
                .await
                .unwrap();
            assert_eq!(0, count);

            Ok::<(), anyhow::Error>(())
        })
        .await
        .expect("failed to check that tables were empty");

        // We currently keep the baseboard ids and sw_cabooses around.
        let coll_counts = CollectionCounts::new(&conn).await.unwrap();
        assert_ne!(coll_counts.baseboards, 0);
        assert_ne!(coll_counts.cabooses, 0);
        assert_ne!(coll_counts.rot_pages, 0);

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    enum AllInvTables {
        AreEmpty,
        ArePopulated,
    }

    async fn check_all_inv_tables(
        datastore: &DataStore,
        status: AllInvTables,
    ) -> anyhow::Result<()> {
        let conn = datastore
            .pool_connection_for_tests()
            .await
            .context("Failed to get datastore connection")?;
        let mut query = QueryBuilder::new();
        query.sql(
            "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'inv\\_%'"
        );
        let tables: Vec<String> = query
            .query::<diesel::sql_types::Text>()
            .load_async(&*conn)
            .await
            .context("Failed to query information_schema for tables")?;

        // Sanity-check, if this breaks, break loudly.
        // We expect to see all the "inv_..." tables here, even ones that
        // haven't been written yet.
        if tables.is_empty() {
            bail!("Tables missing from information_schema query");
        }

        // This transaction is used by tests, and does not need to retry.
        #[allow(clippy::disallowed_methods)]
        conn.transaction_async(|conn| async move {
            // We need this to call "COUNT(*)" below.
            conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL)
                .await
                .context("Failed to allow full table scans")?;

            for table in tables {
                let mut query = QueryBuilder::new();
                query.sql(
                    // We're scraping the table names dynamically here, so we
                    // don't know them ahead of time. However, this is also a
                    // test, so this usage is pretty benign.
                    TrustedStr::i_take_responsibility_for_validating_this_string(
                        format!("SELECT COUNT(*) FROM {table}")
                    )
                );
                let count: i64 = query
                    .query::<diesel::sql_types::Int8>()
                    .get_result_async(&conn)
                    .await
                    .with_context(|| format!("Couldn't SELECT COUNT(*) from table {table}"))?;

                match status {
                    AllInvTables::AreEmpty => {
                        if count != 0 {
                            bail!("Found deleted row(s) from table: {table}");
                        }
                    },
                    AllInvTables::ArePopulated => {
                        if count == 0 {
                            bail!("Found table without entries: {table}");
                        }
                    },

                }
            }
            Ok::<(), anyhow::Error>(())
        })
        .await?;

        Ok(())
    }

    /// Creates a representative collection, deletes it, and walks through
    /// tables to ensure that the subcomponents of the inventory have been
    /// deleted.
    ///
    /// NOTE: This test depends on the naming convention "inv_" prefix name
    /// to identify pieces of the inventory.
    #[tokio::test]
    async fn test_inventory_deletion() {
        // Setup
        let logctx = dev::test_setup_log("inventory_deletion");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a representative collection and write it to the database.
        let Representative { builder, .. } = representative();
        let collection = builder.build();
        datastore
            .inventory_insert_collection(&opctx, &collection)
            .await
            .expect("failed to insert collection");

        // Read all "inv_" tables and ensure that they are populated.
        check_all_inv_tables(&datastore, AllInvTables::ArePopulated)
            .await
            .expect("All inv_... tables should be populated by representative collection");

        // Delete that collection we just added
        datastore
            .inventory_delete_collection(&opctx, collection.id)
            .await
            .expect("failed to prune collections");
        assert_eq!(
            datastore
                .inventory_collections()
                .await
                .unwrap()
                .iter()
                .map(|c| c.id.into())
                .collect::<Vec<CollectionUuid>>(),
            &[]
        );

        // Read all "inv_" tables and ensure that they're empty
        check_all_inv_tables(&datastore, AllInvTables::AreEmpty).await.expect(
            "All inv_... tables should be deleted alongside collection",
        );

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_representative_collection_populates_database() {
        // Setup
        let logctx = dev::test_setup_log("inventory_deletion");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create a representative collection and write it to the database.
        let Representative { builder, .. } = representative();
        let collection = builder.build();
        datastore
            .inventory_insert_collection(&opctx, &collection)
            .await
            .expect("failed to insert collection");

        // Read all "inv_" tables and ensure that they are populated.
        check_all_inv_tables(&datastore, AllInvTables::ArePopulated)
            .await
            .expect("All inv_... tables should be populated by representative collection");

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_reconciler_status_fields() {
        // Setup
        let logctx = dev::test_setup_log("reconciler_status_fields");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Start with a representative collection.
        let Representative { builder, .. } = representative();
        let mut collection = builder.build();

        // Mutate the sled-agent contents to test variants of the reconciler
        // fields:
        //
        // * try all three `ConfigReconcilerInventoryStatus` variants
        // * add a `last_reconciliation` with a mix of success/error results
        {
            let mut sled_agents = collection.sled_agents.iter_mut();
            let mut sa1 = sled_agents.next().expect("at least 1 sled agent");
            let mut sa2 = sled_agents.next().expect("at least 2 sled agents");
            let mut sa3 = sled_agents.next().expect("at least 3 sled agents");

            sa1.reconciler_status = ConfigReconcilerInventoryStatus::NotYetRun;
            sa1.last_reconciliation = None;

            sa2.reconciler_status = ConfigReconcilerInventoryStatus::Running {
                config: sa2.ledgered_sled_config.clone().unwrap(),
                started_at: now_db_precision(),
                running_for: Duration::from_secs(1),
            };
            sa2.last_reconciliation = Some({
                let make_result = |kind, i| {
                    if i % 2 == 0 {
                        ConfigReconcilerInventoryResult::Ok
                    } else {
                        ConfigReconcilerInventoryResult::Err {
                            message: format!("fake {kind} error {i}"),
                        }
                    }
                };
                ConfigReconcilerInventory {
                    last_reconciled_config: sa2
                        .ledgered_sled_config
                        .clone()
                        .unwrap(),
                    external_disks: (0..10)
                        .map(|i| {
                            (PhysicalDiskUuid::new_v4(), make_result("disk", i))
                        })
                        .collect(),
                    datasets: (0..10)
                        .map(|i| {
                            (DatasetUuid::new_v4(), make_result("dataset", i))
                        })
                        .collect(),
                    orphaned_datasets: (0..5)
                        .map(|i| OrphanedDataset {
                            name: DatasetName::new(
                                ZpoolName::new_external(ZpoolUuid::new_v4()),
                                DatasetKind::Cockroach,
                            ),
                            reason: format!("test orphan {i}"),
                            id: if i % 2 == 0 {
                                Some(DatasetUuid::new_v4())
                            } else {
                                None
                            },
                            mounted: i % 2 == 1,
                            available: (10 * i).into(),
                            used: i.into(),
                        })
                        .collect(),
                    zones: (0..10)
                        .map(|i| {
                            (OmicronZoneUuid::new_v4(), make_result("zone", i))
                        })
                        .collect(),
                    boot_partitions: BootPartitionContents {
                        boot_disk: Ok(M2Slot::B),
                        slot_a: Err("some error".to_string()),
                        slot_b: Ok(BootPartitionDetails {
                            header: BootImageHeader {
                                flags: u64::MAX,
                                data_size: 123456,
                                image_size: 234567,
                                target_size: 345678,
                                sha256: [1; 32],
                                image_name: "test image".to_string(),
                            },
                            artifact_hash: ArtifactHash([2; 32]),
                            artifact_size: 456789,
                        }),
                    },
                    clear_mupdate_override: Some(
                        ClearMupdateOverrideInventory {
                            boot_disk_result: Ok(
                                ClearMupdateOverrideBootSuccessInventory::Cleared,
                            ),
                            non_boot_message: "simulated non-boot message"
                                .to_owned(),
                        },
                    ),
                }
            });

            sa3.reconciler_status = ConfigReconcilerInventoryStatus::Idle {
                completed_at: now_db_precision(),
                ran_for: Duration::from_secs(5),
            };
        }

        // Write it to the db; read it back and check it survived.
        datastore
            .inventory_insert_collection(&opctx, &collection)
            .await
            .expect("failed to insert collection");
        let collection_read = datastore
            .inventory_collection_read(&opctx, collection.id)
            .await
            .expect("failed to read collection back");
        assert_eq!(collection, collection_read);

        // Now delete it and ensure we remove everything.
        datastore
            .inventory_delete_collection(&opctx, collection.id)
            .await
            .expect("failed to prune collections");

        // Read all "inv_" tables and ensure that they're empty
        check_all_inv_tables(&datastore, AllInvTables::AreEmpty).await.expect(
            "All inv_... tables should be deleted alongside collection",
        );

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_zone_image_source() {
        // Setup
        let logctx = dev::test_setup_log("zone_image_source");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Start with a representative collection.
        let Representative { builder, .. } = representative();
        let mut collection = builder.build();

        // Mutate some zones on one of the sleds to have various image sources.
        {
            // Find a sled that has a few zones in one of its sled configs.
            let mut sa = collection
                .sled_agents
                .iter_mut()
                .find(|sa| {
                    sa.ledgered_sled_config
                        .as_ref()
                        .map_or(false, |config| config.zones.len() >= 3)
                })
                .expect("at least one sled has 3 or more zones");
            let mut zones_iter =
                sa.ledgered_sled_config.as_mut().unwrap().zones.iter_mut();

            let mut z1 = zones_iter.next().expect("at least 1 zone");
            let mut z2 = zones_iter.next().expect("at least 2 zones");
            let mut z3 = zones_iter.next().expect("at least 3 zones");
            z1.image_source = OmicronZoneImageSource::InstallDataset;
            z2.image_source = OmicronZoneImageSource::Artifact {
                hash: ArtifactHash([0; 32]),
            };
            z3.image_source = OmicronZoneImageSource::Artifact {
                hash: ArtifactHash([1; 32]),
            };
            eprintln!("changed image sources: {z1:?} {z2:?} {z3:?}");
        }

        // Write it to the db; read it back and check it survived.
        datastore
            .inventory_insert_collection(&opctx, &collection)
            .await
            .expect("failed to insert collection");
        let collection_read = datastore
            .inventory_collection_read(&opctx, collection.id)
            .await
            .expect("failed to read collection back");
        assert_eq!(collection, collection_read);

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }
}
