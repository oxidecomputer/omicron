// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::DataStore;
use crate::authz;
use crate::authz::ApiResource;
use crate::context::OpContext;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use crate::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
use anyhow::Context;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::AsyncSimpleConnection;
use chrono::DateTime;
use chrono::Utc;
use clickhouse_admin_types::{KeeperId, ServerId};
use core::future::Future;
use core::pin::Pin;
use diesel::BoolExpressionMethods;
use diesel::Column;
use diesel::ExpressionMethods;
use diesel::Insertable;
use diesel::IntoSql;
use diesel::JoinOnDsl;
use diesel::NullableExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::RunQueryDsl;
use diesel::Table;
use diesel::expression::SelectableHelper;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use diesel::sql_types;
use diesel::sql_types::Nullable;
use futures::FutureExt;
use id_map::IdMap;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::TransactionError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_model::ArtifactHash;
use nexus_db_model::Blueprint as DbBlueprint;
use nexus_db_model::BpClickhouseClusterConfig;
use nexus_db_model::BpClickhouseKeeperZoneIdToNodeId;
use nexus_db_model::BpClickhouseServerZoneIdToNodeId;
use nexus_db_model::BpOmicronDataset;
use nexus_db_model::BpOmicronPhysicalDisk;
use nexus_db_model::BpOmicronZone;
use nexus_db_model::BpOmicronZoneNic;
use nexus_db_model::BpOximeterReadPolicy;
use nexus_db_model::BpPendingMgsUpdateComponent;
use nexus_db_model::BpPendingMgsUpdateHostPhase1;
use nexus_db_model::BpPendingMgsUpdateRot;
use nexus_db_model::BpPendingMgsUpdateRotBootloader;
use nexus_db_model::BpPendingMgsUpdateSp;
use nexus_db_model::BpSledMetadata;
use nexus_db_model::BpTarget;
use nexus_db_model::DbArtifactVersion;
use nexus_db_model::DbTypedUuid;
use nexus_db_model::DebugLogBlueprintPlanning;
use nexus_db_model::HwBaseboardId;
use nexus_db_model::HwM2Slot;
use nexus_db_model::HwRotSlot;
use nexus_db_model::Ipv6Addr;
use nexus_db_model::SpMgsSlot;
use nexus_db_model::SpType;
use nexus_db_model::SqlU16;
use nexus_db_model::TufArtifact;
use nexus_db_model::to_db_typed_uuid;
use nexus_db_schema::enums::HwM2SlotEnum;
use nexus_db_schema::enums::HwRotSlotEnum;
use nexus_db_schema::enums::SpTypeEnum;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintMetadata;
use nexus_types::deployment::BlueprintSledConfig;
use nexus_types::deployment::BlueprintSource;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::ClickhouseClusterConfig;
use nexus_types::deployment::CockroachDbPreserveDowngrade;
use nexus_types::deployment::ExpectedVersion;
use nexus_types::deployment::OximeterReadMode;
use nexus_types::deployment::PendingMgsUpdate;
use nexus_types::deployment::PendingMgsUpdateDetails;
use nexus_types::deployment::PendingMgsUpdateHostPhase1Details;
use nexus_types::deployment::PendingMgsUpdateRotBootloaderDetails;
use nexus_types::deployment::PendingMgsUpdateRotDetails;
use nexus_types::deployment::PendingMgsUpdateSpDetails;
use nexus_types::deployment::PendingMgsUpdates;
use nexus_types::inventory::BaseboardId;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::bail_unless;
use omicron_uuid_kinds::BlueprintKind;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::TypedUuid;
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use thiserror::Error;
use tufaceous_artifact::ArtifactKind;
use tufaceous_artifact::KnownArtifactKind;
use uuid::Uuid;

mod external_networking;

impl DataStore {
    /// List blueprints
    pub async fn blueprints_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<BlueprintMetadata> {
        use nexus_db_schema::schema::blueprint;

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
        self.blueprint_insert_on_connection(&conn, opctx, blueprint).await
    }

    /// Creates a transaction iff the current blueprint is "bp_id".
    ///
    /// - The transaction is retryable and named "name"
    /// - The "bp_id" value is checked as the first operation within the
    /// transaction.
    /// - If "bp_id" is still the current target, then "f" is called,
    /// within a transactional context.
    pub async fn transaction_if_current_blueprint_is<Func, R>(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        name: &'static str,
        opctx: &OpContext,
        bp_id: BlueprintUuid,
        f: Func,
    ) -> Result<R, Error>
    where
        Func: for<'t> Fn(
                &'t async_bb8_diesel::Connection<DbConnection>,
            ) -> Pin<
                Box<
                    dyn Future<Output = Result<R, TransactionError<Error>>>
                        + Send
                        + 't,
                >,
            > + Send
            + Sync
            + Clone,
        R: Send + 'static,
    {
        let err = OptionalError::new();
        let r = self
            .transaction_retry_wrapper(name)
            .transaction(&conn, |conn| {
                let err = err.clone();
                let f = f.clone();
                async move {
                    // Bail if `bp_id` is no longer the target
                    let target =
                        Self::blueprint_target_get_current_on_connection(
                            &conn, opctx,
                        )
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))?;
                    if target.target_id != bp_id {
                        return Err(err.bail(
                            Error::invalid_request(format!(
                                "blueprint target has changed from {} -> {}",
                                bp_id, target.target_id
                            ))
                            .into(),
                        ));
                    }

                    // Otherwise, perform our actual operation
                    f(&conn)
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))
                }
                .boxed()
            })
            .await
            .map_err(|e| match err.take() {
                Some(txn_error) => txn_error.into(),
                None => public_error_from_diesel(e, ErrorHandler::Server),
            })?;
        Ok(r)
    }

    /// Variant of [Self::blueprint_insert] which may be called from a
    /// transaction context.
    pub(crate) async fn blueprint_insert_on_connection(
        &self,
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
        let blueprint_id = BlueprintUuid::from(row_blueprint.id);

        let sled_metadatas = blueprint
            .sleds
            .iter()
            .map(|(&sled_id, sled)| BpSledMetadata {
                blueprint_id: blueprint_id.into(),
                sled_id: sled_id.into(),
                sled_state: sled.state.into(),
                sled_agent_generation: sled.sled_agent_generation.into(),
                remove_mupdate_override: sled
                    .remove_mupdate_override
                    .map(|id| id.into()),
                host_phase_2_desired_slot_a: sled
                    .host_phase_2
                    .slot_a
                    .artifact_hash()
                    .map(ArtifactHash),
                host_phase_2_desired_slot_b: sled
                    .host_phase_2
                    .slot_b
                    .artifact_hash()
                    .map(ArtifactHash),
            })
            .collect::<Vec<_>>();

        let omicron_physical_disks = blueprint
            .sleds
            .iter()
            .flat_map(|(sled_id, sled)| {
                sled.disks.iter().map(move |disk| {
                    BpOmicronPhysicalDisk::new(blueprint_id, *sled_id, disk)
                })
            })
            .collect::<Vec<_>>();

        let omicron_datasets = blueprint
            .sleds
            .iter()
            .flat_map(|(sled_id, sled)| {
                sled.datasets.iter().map(move |dataset| {
                    BpOmicronDataset::new(blueprint_id, *sled_id, dataset)
                })
            })
            .collect::<Vec<_>>();

        let omicron_zones = blueprint
            .sleds
            .iter()
            .flat_map(|(sled_id, sled)| {
                sled.zones.iter().map(move |zone| {
                    BpOmicronZone::new(blueprint_id, *sled_id, zone)
                        .map_err(|e| Error::internal_error(&format!("{:#}", e)))
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let omicron_zone_nics = blueprint
            .sleds
            .values()
            .flat_map(|sled| {
                sled.zones.iter().filter_map(|zone| {
                    BpOmicronZoneNic::new(blueprint_id, zone)
                        .with_context(|| format!("zone {}", zone.id))
                        .map_err(|e| Error::internal_error(&format!("{:#}", e)))
                        .transpose()
                })
            })
            .collect::<Result<Vec<BpOmicronZoneNic>, _>>()?;

        let clickhouse_tables: Option<(_, _, _)> = if let Some(config) =
            &blueprint.clickhouse_cluster_config
        {
            let mut keepers = vec![];
            for (zone_id, keeper_id) in &config.keepers {
                let keeper = BpClickhouseKeeperZoneIdToNodeId::new(
                    blueprint_id,
                    *zone_id,
                    *keeper_id,
                )
                .with_context(|| format!("zone {zone_id}, keeper {keeper_id}"))
                .map_err(|e| Error::internal_error(&format!("{:#}", e)))?;
                keepers.push(keeper)
            }

            let mut servers = vec![];
            for (zone_id, server_id) in &config.servers {
                let server = BpClickhouseServerZoneIdToNodeId::new(
                    blueprint_id,
                    *zone_id,
                    *server_id,
                )
                .with_context(|| format!("zone {zone_id}, server {server_id}"))
                .map_err(|e| Error::internal_error(&format!("{:#}", e)))?;
                servers.push(server);
            }

            let cluster_config =
                BpClickhouseClusterConfig::new(blueprint_id, config)
                    .map_err(|e| Error::internal_error(&format!("{:#}", e)))?;

            Some((cluster_config, keepers, servers))
        } else {
            None
        };

        let oximeter_read_policy = BpOximeterReadPolicy::new(
            blueprint_id,
            nexus_db_model::Generation(blueprint.oximeter_read_version),
            &blueprint.oximeter_read_mode,
        );

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

        // The risk of a serialization error is possible here, but low,
        // as most of the operations should be insertions rather than in-place
        // modifications of existing tables.
        #[allow(clippy::disallowed_methods)]
        self.transaction_non_retry_wrapper("blueprint_insert")
            .transaction(&conn, |conn| async move {
                // Insert the row for the blueprint.
                {
                    use nexus_db_schema::schema::blueprint::dsl;
                    let _: usize = diesel::insert_into(dsl::blueprint)
                        .values(row_blueprint)
                        .execute_async(&conn)
                        .await?;
                }

                // Insert all the sled states for this blueprint.
                {
                    // Skip formatting this line to prevent rustfmt bailing out.
                    #[rustfmt::skip]
                    use nexus_db_schema::schema::bp_sled_metadata::dsl
                        as sled_metadata;

                    let _ =
                        diesel::insert_into(sled_metadata::bp_sled_metadata)
                            .values(sled_metadatas)
                            .execute_async(&conn)
                            .await?;
                }

                // Insert all physical disks for this blueprint.
                {
                    // Skip formatting this line to prevent rustfmt bailing out.
                    #[rustfmt::skip]
                    use nexus_db_schema::schema::bp_omicron_physical_disk::dsl
                        as omicron_disk;
                    let _ = diesel::insert_into(
                        omicron_disk::bp_omicron_physical_disk,
                    )
                    .values(omicron_physical_disks)
                    .execute_async(&conn)
                    .await?;
                }

                // Insert all datasets for this blueprint.
                {
                    // Skip formatting this line to prevent rustfmt bailing out.
                    #[rustfmt::skip]
                    use nexus_db_schema::schema::bp_omicron_dataset::dsl
                        as omicron_dataset;
                    let _ = diesel::insert_into(
                        omicron_dataset::bp_omicron_dataset,
                    )
                    .values(omicron_datasets)
                    .execute_async(&conn)
                    .await?;
                }

                // Insert all the Omicron zones for this blueprint.
                {
                    // Skip formatting this line to prevent rustfmt bailing out.
                    #[rustfmt::skip]
                    use nexus_db_schema::schema::bp_omicron_zone::dsl
                        as omicron_zone;
                    let _ = diesel::insert_into(omicron_zone::bp_omicron_zone)
                        .values(omicron_zones)
                        .execute_async(&conn)
                        .await?;
                }

                {
                    // Skip formatting this line to prevent rustfmt bailing out.
                    #[rustfmt::skip]
                    use nexus_db_schema::schema::bp_omicron_zone_nic::dsl
                        as omicron_zone_nic;
                    let _ = diesel::insert_into(
                        omicron_zone_nic::bp_omicron_zone_nic,
                    )
                    .values(omicron_zone_nics)
                    .execute_async(&conn)
                    .await?;
                }

                // Insert all clickhouse cluster related tables if necessary
                if let Some((clickhouse_cluster_config, keepers, servers)) =
                    clickhouse_tables
                {
                    {
                        // Skip formatting this line to prevent rustfmt bailing
                        // out.
                        #[rustfmt::skip]
                        use nexus_db_schema::schema::
                            bp_clickhouse_cluster_config::dsl;
                        let _ = diesel::insert_into(
                            dsl::bp_clickhouse_cluster_config,
                        )
                        .values(clickhouse_cluster_config)
                        .execute_async(&conn)
                        .await?;
                    }
                    {
                        // Skip formatting this line to prevent rustfmt bailing
                        // out.
                        #[rustfmt::skip]
                        use nexus_db_schema::schema::
                            bp_clickhouse_keeper_zone_id_to_node_id::dsl;
                        let _ = diesel::insert_into(
                            dsl::bp_clickhouse_keeper_zone_id_to_node_id,
                        )
                        .values(keepers)
                        .execute_async(&conn)
                        .await?;
                    }
                    {
                        // Skip formatting this line to prevent rustfmt bailing
                        // out.
                        #[rustfmt::skip]
                        use nexus_db_schema::schema::
                            bp_clickhouse_server_zone_id_to_node_id::dsl;
                        let _ = diesel::insert_into(
                            dsl::bp_clickhouse_server_zone_id_to_node_id,
                        )
                        .values(servers)
                        .execute_async(&conn)
                        .await?;
                    }
                }

                // Insert oximeter read policy for this blueprint
                {
                    use nexus_db_schema::schema::bp_oximeter_read_policy::dsl;
                    let _ = diesel::insert_into(dsl::bp_oximeter_read_policy)
                        .values(oximeter_read_policy)
                        .execute_async(&conn)
                        .await?;
                }

                // Insert pending MGS updates for this blueprint.
                for update in &blueprint.pending_mgs_updates {
                    insert_pending_mgs_update(
                        &conn,
                        update,
                        blueprint_id,
                        &opctx.log,
                    )
                    .await?;
                }

                // Serialize and insert a debug log for the planning report
                // created with this blueprint, if we have one.
                if let BlueprintSource::Planner(report) = &blueprint.source {
                    match DebugLogBlueprintPlanning::new(
                        blueprint_id,
                        report.clone(),
                    ) {
                        Ok(debug_log) => {
                            use nexus_db_schema::schema::debug_log_blueprint_planning::dsl;
                            let _ = diesel::insert_into(
                                dsl::debug_log_blueprint_planning
                            )
                                .values(debug_log)
                                .execute_async(&conn)
                                .await?;
                        }
                        Err(err) => {
                            // This isn't a fatal error - we've already inserted
                            // the production-meaningful blueprint content. Not
                            // being able to log the debug version of the report
                            // isn't great, but blocking real blueprint
                            // insertion on debug logging issues seems worse.
                            error!(
                                self.log,
                                "could not serialize blueprint planning report";
                                InlineErrorChain::new(&err),
                            );
                        }

                    }
                }

                Ok(())
            })
            .await
            .map_err(|e| match e {
                InsertTxnError::Diesel(e) => {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
                e @ InsertTxnError::BadInsertCount { .. } => {
                    // This variant is always an internal error and has no
                    // causes so we don't need to use InlineErrorChain here.
                    Error::internal_error(&e.to_string())
                }
            })?;

        info!(
            &opctx.log,
            "inserted blueprint";
            "blueprint_id" => %blueprint.id,
        );

        Ok(())
    }

    /// Get a count of the number of blueprints in the database.
    ///
    /// This (necessarily) does a full table scan on the blueprint table, so it
    /// must only be used in places that care about the count, such as the
    /// blueprint_planner background task.
    pub async fn blueprint_count(
        &self,
        opctx: &OpContext,
    ) -> Result<u64, Error> {
        opctx.authorize(authz::Action::Read, &authz::BLUEPRINT_CONFIG).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        let count = self
            .transaction_retry_wrapper("blueprint_count")
            .transaction(&conn, |conn| {
                async move {
                    // We need this to call "COUNT(*)" below.
                    use nexus_db_schema::schema::blueprint::dsl;

                    conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;
                    dsl::blueprint
                        .select(diesel::dsl::count_star())
                        .first_async::<i64>(&conn)
                        .await
                }
            })
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        let count = u64::try_from(count).map_err(|_| {
            Error::internal_error(&format!(
                "error converting blueprint count {} into \
                 u64 (how is it negative?)",
                count
            ))
        })?;

        Ok(count)
    }

    /// Read a complete blueprint from the database
    pub async fn blueprint_read(
        &self,
        opctx: &OpContext,
        authz_blueprint: &authz::Blueprint,
    ) -> Result<Blueprint, Error> {
        opctx.authorize(authz::Action::Read, authz_blueprint).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let blueprint_id =
            BlueprintUuid::from_untyped_uuid(authz_blueprint.id());

        // Read the metadata from the primary blueprint row, and ensure that it
        // exists.
        let (
            parent_blueprint_id,
            internal_dns_version,
            external_dns_version,
            target_release_minimum_generation,
            nexus_generation,
            cockroachdb_fingerprint,
            cockroachdb_setting_preserve_downgrade,
            time_created,
            creator,
            comment,
            source,
        ) = {
            use nexus_db_schema::schema::blueprint::dsl;

            let Some(blueprint) = dsl::blueprint
                .filter(dsl::id.eq(to_db_typed_uuid(blueprint_id)))
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
                blueprint.parent_blueprint_id.map(From::from),
                *blueprint.internal_dns_version,
                *blueprint.external_dns_version,
                *blueprint.target_release_minimum_generation,
                *blueprint.nexus_generation,
                blueprint.cockroachdb_fingerprint,
                blueprint.cockroachdb_setting_preserve_downgrade,
                blueprint.time_created,
                blueprint.creator,
                blueprint.comment,
                BlueprintSource::from(blueprint.source),
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

        // Load the sled metadata for this blueprint. We use this to prime our
        // primary map of sled configs, but we leave the zones / disks /
        // datasets maps empty (to be filled in when we query those tables
        // below).
        let mut sled_configs: BTreeMap<SledUuid, BlueprintSledConfig> = {
            use nexus_db_schema::schema::bp_sled_metadata::dsl;
            use nexus_db_schema::schema::tuf_artifact::dsl as tuf_artifact_dsl;

            let (tuf1, tuf2) = diesel::alias!(
                nexus_db_schema::schema::tuf_artifact as tuf_artifact_1,
                nexus_db_schema::schema::tuf_artifact as tuf_artifact_2,
            );

            let mut sled_configs = BTreeMap::new();
            let mut paginator = Paginator::new(
                SQL_BATCH_SIZE,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::bp_sled_metadata,
                    dsl::sled_id,
                    &p.current_pagparams(),
                )
                .filter(dsl::blueprint_id.eq(to_db_typed_uuid(blueprint_id)))
                // Left join against the tuf_artifact table twice (once for each
                // host slot) in case the artifact is missing from the table,
                // which is non-fatal.
                .left_join(
                    tuf1.on(tuf1
                        .field(tuf_artifact_dsl::kind)
                        .eq(ArtifactKind::HOST_PHASE_2.to_string())
                        .and(
                            tuf1.field(tuf_artifact_dsl::sha256)
                                .nullable()
                                .eq(dsl::host_phase_2_desired_slot_a),
                        )),
                )
                .left_join(
                    tuf2.on(tuf2
                        .field(tuf_artifact_dsl::kind)
                        .eq(ArtifactKind::HOST_PHASE_2.to_string())
                        .and(
                            tuf2.field(tuf_artifact_dsl::sha256)
                                .nullable()
                                .eq(dsl::host_phase_2_desired_slot_b),
                        )),
                )
                .select((
                    BpSledMetadata::as_select(),
                    tuf1.fields(tuf_artifact_dsl::version).nullable(),
                    tuf2.fields(tuf_artifact_dsl::version).nullable(),
                ))
                .load_async::<(
                    BpSledMetadata,
                    Option<DbArtifactVersion>,
                    Option<DbArtifactVersion>,
                )>(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

                paginator = p.found_batch(&batch, &|(s, _, _)| s.sled_id);

                for (s, slot_a_version, slot_b_version) in batch {
                    let config = BlueprintSledConfig {
                        state: s.sled_state.into(),
                        sled_agent_generation: *s.sled_agent_generation,
                        disks: IdMap::new(),
                        datasets: IdMap::new(),
                        zones: IdMap::new(),
                        remove_mupdate_override: s
                            .remove_mupdate_override
                            .map(|id| id.into()),
                        host_phase_2: s
                            .host_phase_2(slot_a_version, slot_b_version),
                    };
                    let old = sled_configs.insert(s.sled_id.into(), config);
                    bail_unless!(
                        old.is_none(),
                        "found duplicate sled ID in bp_sled_metadata: {}",
                        s.sled_id
                    );
                }
            }
            sled_configs
        };

        // Assemble a mutable map of all the NICs found, by NIC id.  As we
        // match these up with the corresponding zone below, we'll remove items
        // from this set.  That way we can tell if the same NIC was used twice
        // or not used at all.
        let mut omicron_zone_nics = {
            use nexus_db_schema::schema::bp_omicron_zone_nic::dsl;

            let mut omicron_zone_nics = BTreeMap::new();
            let mut paginator = Paginator::new(
                SQL_BATCH_SIZE,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::bp_omicron_zone_nic,
                    dsl::id,
                    &p.current_pagparams(),
                )
                .filter(dsl::blueprint_id.eq(to_db_typed_uuid(blueprint_id)))
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
            use nexus_db_schema::schema::bp_omicron_zone::dsl;
            use nexus_db_schema::schema::tuf_artifact::dsl as tuf_artifact_dsl;

            let mut paginator = Paginator::new(
                SQL_BATCH_SIZE,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                // `paginated` implicitly orders by our `id`, which is also
                // handy for testing: the zones are always consistently ordered
                let batch = paginated(
                    dsl::bp_omicron_zone,
                    dsl::id,
                    &p.current_pagparams(),
                )
                .filter(dsl::blueprint_id.eq(to_db_typed_uuid(blueprint_id)))
                // Left join in case the artifact is missing from the
                // tuf_artifact table, which is non-fatal.
                .left_join(
                    tuf_artifact_dsl::tuf_artifact.on(tuf_artifact_dsl::kind
                        .eq(KnownArtifactKind::Zone.to_string())
                        .and(
                            tuf_artifact_dsl::sha256
                                .nullable()
                                .eq(dsl::image_artifact_sha256),
                        )),
                )
                .select((
                    BpOmicronZone::as_select(),
                    Option::<TufArtifact>::as_select(),
                ))
                .load_async::<(BpOmicronZone, Option<TufArtifact>)>(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

                paginator = p.found_batch(&batch, &|(z, _)| z.id);

                for (z, artifact) in batch {
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
                    let sled_config =
                        sled_configs.get_mut(&sled_id).ok_or_else(|| {
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
                        .into_blueprint_zone_config(nic_row, artifact)
                        .with_context(|| {
                            format!("zone {zone_id}: parse from database")
                        })
                        .map_err(|e| {
                            Error::internal_error(&format!(
                                "{:#}",
                                e.to_string()
                            ))
                        })?;
                    sled_config.zones.insert(zone);
                }
            }
        }

        bail_unless!(
            omicron_zone_nics.is_empty(),
            "found extra Omicron zone NICs: {:?}",
            omicron_zone_nics.keys()
        );

        // Load all the physical disks for each sled.
        {
            use nexus_db_schema::schema::bp_omicron_physical_disk::dsl;

            let mut paginator = Paginator::new(
                SQL_BATCH_SIZE,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                // `paginated` implicitly orders by our `id`, which is also
                // handy for testing: the physical disks are always consistently ordered
                let batch = paginated(
                    dsl::bp_omicron_physical_disk,
                    dsl::id,
                    &p.current_pagparams(),
                )
                .filter(dsl::blueprint_id.eq(to_db_typed_uuid(blueprint_id)))
                .select(BpOmicronPhysicalDisk::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

                paginator = p.found_batch(&batch, &|d| d.id);

                for d in batch {
                    let sled_config = sled_configs
                        .get_mut(&d.sled_id.into())
                        .ok_or_else(|| {
                        // This error means that we found a row in
                        // bp_omicron_physical_disk with no associated
                        // record in bp_sled_omicron_physical_disks.  This
                        // should be impossible and reflects either a bug or
                        // database corruption.
                        Error::internal_error(&format!(
                            "disk {}: unknown sled: {}",
                            d.id, d.sled_id
                        ))
                    })?;
                    let disk_id = d.id;
                    sled_config.disks.insert(d.try_into().map_err(|e| {
                        Error::internal_error(&format!(
                            "Cannot convert BpOmicronPhysicalDisk {}: {e}",
                            disk_id
                        ))
                    })?);
                }
            }
        }

        // Load all the datasets for each sled
        {
            use nexus_db_schema::schema::bp_omicron_dataset::dsl;

            let mut paginator = Paginator::new(
                SQL_BATCH_SIZE,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                // `paginated` implicitly orders by our `id`, which is also
                // handy for testing: the datasets are always consistently ordered
                let batch = paginated(
                    dsl::bp_omicron_dataset,
                    dsl::id,
                    &p.current_pagparams(),
                )
                .filter(dsl::blueprint_id.eq(to_db_typed_uuid(blueprint_id)))
                .select(BpOmicronDataset::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

                paginator = p.found_batch(&batch, &|d| d.id);

                for d in batch {
                    let sled_config = sled_configs
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
                    sled_config.datasets.insert(d.try_into().map_err(|e| {
                        Error::internal_error(&format!(
                            "Cannot parse dataset {}: {e}",
                            dataset_id
                        ))
                    })?);
                }
            }
        }

        // Load our `ClickhouseClusterConfig` if it exists
        let clickhouse_cluster_config: Option<ClickhouseClusterConfig> = {
            use nexus_db_schema::schema::bp_clickhouse_cluster_config::dsl;

            let res = dsl::bp_clickhouse_cluster_config
                .filter(dsl::blueprint_id.eq(to_db_typed_uuid(blueprint_id)))
                .select(BpClickhouseClusterConfig::as_select())
                .get_result_async(&*conn)
                .await
                .optional()
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

            match res {
                None => None,
                Some(bp_config) => {
                    // Load our clickhouse keeper configs for the given blueprint
                    let keepers: BTreeMap<OmicronZoneUuid, KeeperId> = {
                        use nexus_db_schema::schema::bp_clickhouse_keeper_zone_id_to_node_id::dsl;
                        let mut keepers = BTreeMap::new();
                        let mut paginator = Paginator::new(
                            SQL_BATCH_SIZE,
                            dropshot::PaginationOrder::Ascending,
                        );
                        while let Some(p) = paginator.next() {
                            let batch = paginated(
                                dsl::bp_clickhouse_keeper_zone_id_to_node_id,
                                dsl::omicron_zone_id,
                                &p.current_pagparams(),
                            )
                            .filter(
                                dsl::blueprint_id
                                    .eq(to_db_typed_uuid(blueprint_id)),
                            )
                            .select(
                                BpClickhouseKeeperZoneIdToNodeId::as_select(),
                            )
                            .load_async(&*conn)
                            .await
                            .map_err(|e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::Server,
                                )
                            })?;

                            paginator =
                                p.found_batch(&batch, &|k| k.omicron_zone_id);

                            for k in batch {
                                let keeper_id = KeeperId(
                                    u64::try_from(k.keeper_id).map_err(
                                        |_| {
                                            Error::internal_error(&format!(
                                                "keeper id is negative: {}",
                                                k.keeper_id
                                            ))
                                        },
                                    )?,
                                );
                                keepers.insert(
                                    k.omicron_zone_id.into(),
                                    keeper_id,
                                );
                            }
                        }
                        keepers
                    };

                    // Load our clickhouse server configs for the given blueprint
                    let servers: BTreeMap<OmicronZoneUuid, ServerId> = {
                        use nexus_db_schema::schema::bp_clickhouse_server_zone_id_to_node_id::dsl;
                        let mut servers = BTreeMap::new();
                        let mut paginator = Paginator::new(
                            SQL_BATCH_SIZE,
                            dropshot::PaginationOrder::Ascending,
                        );
                        while let Some(p) = paginator.next() {
                            let batch = paginated(
                                dsl::bp_clickhouse_server_zone_id_to_node_id,
                                dsl::omicron_zone_id,
                                &p.current_pagparams(),
                            )
                            .filter(
                                dsl::blueprint_id
                                    .eq(to_db_typed_uuid(blueprint_id)),
                            )
                            .select(
                                BpClickhouseServerZoneIdToNodeId::as_select(),
                            )
                            .load_async(&*conn)
                            .await
                            .map_err(|e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::Server,
                                )
                            })?;

                            paginator =
                                p.found_batch(&batch, &|s| s.omicron_zone_id);

                            for s in batch {
                                let server_id = ServerId(
                                    u64::try_from(s.server_id).map_err(
                                        |_| {
                                            Error::internal_error(&format!(
                                                "server id is negative: {}",
                                                s.server_id
                                            ))
                                        },
                                    )?,
                                );
                                servers.insert(
                                    s.omicron_zone_id.into(),
                                    server_id,
                                );
                            }
                        }
                        servers
                    };

                    Some(ClickhouseClusterConfig {
                        generation: bp_config.generation.into(),
                        max_used_server_id: ServerId(
                            u64::try_from(bp_config.max_used_server_id)
                                .map_err(|_| {
                                    Error::internal_error(&format!(
                                        "max server id is negative: {}",
                                        bp_config.max_used_server_id
                                    ))
                                })?,
                        ),
                        max_used_keeper_id: KeeperId(
                            u64::try_from(bp_config.max_used_keeper_id)
                                .map_err(|_| {
                                    Error::internal_error(&format!(
                                        "max keeper id is negative: {}",
                                        bp_config.max_used_keeper_id
                                    ))
                                })?,
                        ),
                        cluster_name: bp_config.cluster_name,
                        cluster_secret: bp_config.cluster_secret,
                        highest_seen_keeper_leader_committed_log_index:
                            u64::try_from(
                                bp_config.highest_seen_keeper_leader_committed_log_index,
                            )
                            .map_err(|_| {
                                Error::internal_error(&format!(
                                    "max server id is negative: {}",
                                    bp_config.highest_seen_keeper_leader_committed_log_index
                                ))
                            })?,
                        keepers,
                        servers,
                    })
                }
            }
        };

        let (oximeter_read_version, oximeter_read_mode) = {
            use nexus_db_schema::schema::bp_oximeter_read_policy::dsl;

            let res = dsl::bp_oximeter_read_policy
                .filter(dsl::blueprint_id.eq(to_db_typed_uuid(blueprint_id)))
                .select(BpOximeterReadPolicy::as_select())
                .get_result_async(&*conn)
                .await
                .optional()
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

            match res {
                // If policy is empty, we can safely assume we are at version 1 which defaults
                // to reading from a single node installation
                None => (Generation::new(), OximeterReadMode::SingleNode),
                Some(p) => (
                    Generation::from(p.version),
                    OximeterReadMode::from(p.oximeter_read_mode),
                ),
            }
        };

        // Load all pending RoT bootloader updates.
        //
        // Pagination is a little silly here because we will only allow one at a
        // time in practice for a while, but it's easy enough to do.
        let mut pending_updates_rot_bootloader = Vec::new();
        {
            use nexus_db_schema::schema::bp_pending_mgs_update_rot_bootloader::dsl;

            let mut paginator = Paginator::new(
                SQL_BATCH_SIZE,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::bp_pending_mgs_update_rot_bootloader,
                    dsl::hw_baseboard_id,
                    &p.current_pagparams(),
                )
                .filter(dsl::blueprint_id.eq(to_db_typed_uuid(blueprint_id)))
                .select(BpPendingMgsUpdateRotBootloader::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

                paginator = p.found_batch(&batch, &|d| d.hw_baseboard_id);
                for row in batch {
                    pending_updates_rot_bootloader.push(row);
                }
            }
        }

        // Load all pending RoT updates.
        let mut pending_updates_rot = Vec::new();
        {
            use nexus_db_schema::schema::bp_pending_mgs_update_rot::dsl;

            let mut paginator = Paginator::new(
                SQL_BATCH_SIZE,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::bp_pending_mgs_update_rot,
                    dsl::hw_baseboard_id,
                    &p.current_pagparams(),
                )
                .filter(dsl::blueprint_id.eq(to_db_typed_uuid(blueprint_id)))
                .select(BpPendingMgsUpdateRot::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

                paginator = p.found_batch(&batch, &|d| d.hw_baseboard_id);
                for row in batch {
                    pending_updates_rot.push(row);
                }
            }
        }

        // Load all pending SP updates.
        let mut pending_updates_sp = Vec::new();
        {
            use nexus_db_schema::schema::bp_pending_mgs_update_sp::dsl;

            let mut paginator = Paginator::new(
                SQL_BATCH_SIZE,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::bp_pending_mgs_update_sp,
                    dsl::hw_baseboard_id,
                    &p.current_pagparams(),
                )
                .filter(dsl::blueprint_id.eq(to_db_typed_uuid(blueprint_id)))
                .select(BpPendingMgsUpdateSp::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

                paginator = p.found_batch(&batch, &|d| d.hw_baseboard_id);
                for row in batch {
                    pending_updates_sp.push(row);
                }
            }
        }

        // Load all pending host_phase_1 updates.
        let mut pending_updates_host_phase_1 = Vec::new();
        {
            #[rustfmt::skip]
            use nexus_db_schema::schema::bp_pending_mgs_update_host_phase_1::dsl;

            let mut paginator = Paginator::new(
                SQL_BATCH_SIZE,
                dropshot::PaginationOrder::Ascending,
            );
            while let Some(p) = paginator.next() {
                let batch = paginated(
                    dsl::bp_pending_mgs_update_host_phase_1,
                    dsl::hw_baseboard_id,
                    &p.current_pagparams(),
                )
                .filter(dsl::blueprint_id.eq(to_db_typed_uuid(blueprint_id)))
                .select(BpPendingMgsUpdateHostPhase1::as_select())
                .load_async(&*conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

                paginator = p.found_batch(&batch, &|d| d.hw_baseboard_id);
                for row in batch {
                    pending_updates_host_phase_1.push(row);
                }
            }
        }

        // Collect the unique baseboard ids referenced by pending updates.
        let baseboard_id_ids: BTreeSet<_> = pending_updates_sp
            .iter()
            .map(|s| s.hw_baseboard_id)
            .chain(pending_updates_rot.iter().map(|s| s.hw_baseboard_id))
            .chain(
                pending_updates_rot_bootloader
                    .iter()
                    .map(|s| s.hw_baseboard_id),
            )
            .chain(
                pending_updates_host_phase_1.iter().map(|s| s.hw_baseboard_id),
            )
            .collect();
        // Fetch the corresponding baseboard records.
        let baseboards_by_id: BTreeMap<_, _> = {
            use nexus_db_schema::schema::hw_baseboard_id::dsl;

            let mut bbs = BTreeMap::new();

            let mut paginator = Paginator::new(
                SQL_BATCH_SIZE,
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
                bbs.extend(
                    batch
                        .into_iter()
                        .map(|bb| (bb.id, Arc::new(BaseboardId::from(bb)))),
                );
            }

            bbs
        };

        // Combine this information to assemble the set of pending MGS updates.
        let mut pending_mgs_updates = PendingMgsUpdates::new();
        for row in pending_updates_rot_bootloader {
            process_update_row(
                row,
                &baseboards_by_id,
                &mut pending_mgs_updates,
                &blueprint_id,
            )?;
        }
        for row in pending_updates_rot {
            process_update_row(
                row,
                &baseboards_by_id,
                &mut pending_mgs_updates,
                &blueprint_id,
            )?;
        }
        for row in pending_updates_sp {
            process_update_row(
                row,
                &baseboards_by_id,
                &mut pending_mgs_updates,
                &blueprint_id,
            )?;
        }
        for row in pending_updates_host_phase_1 {
            process_update_row(
                row,
                &baseboards_by_id,
                &mut pending_mgs_updates,
                &blueprint_id,
            )?;
        }

        Ok(Blueprint {
            id: blueprint_id,
            pending_mgs_updates,
            sleds: sled_configs,
            parent_blueprint_id,
            internal_dns_version,
            external_dns_version,
            target_release_minimum_generation,
            nexus_generation,
            cockroachdb_fingerprint,
            cockroachdb_setting_preserve_downgrade,
            clickhouse_cluster_config,
            oximeter_read_mode,
            oximeter_read_version,
            time_created,
            creator,
            comment,
            source,
        })
    }

    /// Delete a blueprint from the database
    pub async fn blueprint_delete(
        &self,
        opctx: &OpContext,
        authz_blueprint: &authz::Blueprint,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Delete, authz_blueprint).await?;
        let blueprint_id =
            BlueprintUuid::from_untyped_uuid(authz_blueprint.id());

        // As with inserting a whole blueprint, we remove it in one big
        // transaction.  Similar considerations apply.  We could
        // break it up if these transactions become too big.  But we'd need a
        // way to stop other clients from discovering a collection after we
        // start removing it and we'd also need to make sure we didn't leak a
        // collection if we crash while deleting it.
        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        // Helper to pack and unpack all the counts of rows we delete in the
        // transaction below (exclusively used for logging).
        struct NumRowsDeleted {
            nblueprints: usize,
            nsled_metadata: usize,
            nphysical_disks: usize,
            ndatasets: usize,
            nzones: usize,
            nnics: usize,
            nclickhouse_cluster_configs: usize,
            nclickhouse_keepers: usize,
            nclickhouse_servers: usize,
            noximeter_policy: usize,
            npending_mgs_updates_sp: usize,
            npending_mgs_updates_rot: usize,
            npending_mgs_updates_rot_bootloader: usize,
            npending_mgs_updates_host_phase_1: usize,
            ndebug_log_planning_report: usize,
        }

        let NumRowsDeleted {
            nblueprints,
            nsled_metadata,
            nphysical_disks,
            ndatasets,
            nzones,
            nnics,
            nclickhouse_cluster_configs,
            nclickhouse_keepers,
            nclickhouse_servers,
            noximeter_policy,
            npending_mgs_updates_sp,
            npending_mgs_updates_rot,
            npending_mgs_updates_rot_bootloader,
            npending_mgs_updates_host_phase_1,
            ndebug_log_planning_report,
        } = self
            .transaction_retry_wrapper("blueprint_delete")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    // Ensure that blueprint we're about to delete is not the
                    // current target.
                    let current_target =
                        Self::blueprint_current_target_only(&conn)
                            .await
                            .map_err(|txn_err| txn_err.into_diesel(&err))?;

                    if current_target.target_id == blueprint_id {
                        return Err(err.bail(TransactionError::CustomError(
                            Error::conflict(format!(
                                "blueprint {blueprint_id} is the \
                             current target and cannot be deleted",
                            )),
                        )));
                    }

                    // Remove the record describing the blueprint itself.
                    let nblueprints =
                        {
                            use nexus_db_schema::schema::blueprint::dsl;
                            diesel::delete(dsl::blueprint.filter(
                                dsl::id.eq(to_db_typed_uuid(blueprint_id)),
                            ))
                            .execute_async(&conn)
                            .await?
                        };

                    // Bail out if this blueprint didn't exist; there won't be
                    // references to it in any of the remaining tables either,
                    // since deletion always goes through this transaction.
                    if nblueprints == 0 {
                        return Err(err.bail(TransactionError::CustomError(
                            authz_blueprint.not_found(),
                        )));
                    }

                    // Remove rows associated with sled metadata.
                    let nsled_metadata = {
                        // Skip rustfmt because it bails out on this long line.
                        #[rustfmt::skip]
                        use nexus_db_schema::schema::
                            bp_sled_metadata::dsl;
                        diesel::delete(
                            dsl::bp_sled_metadata.filter(
                                dsl::blueprint_id
                                    .eq(to_db_typed_uuid(blueprint_id)),
                            ),
                        )
                        .execute_async(&conn)
                        .await?
                    };

                    // Remove rows associated with Omicron physical disks
                    let nphysical_disks = {
                        // Skip rustfmt because it bails out on this long line.
                        #[rustfmt::skip]
                        use nexus_db_schema::schema::
                            bp_omicron_physical_disk::dsl;
                        diesel::delete(
                            dsl::bp_omicron_physical_disk.filter(
                                dsl::blueprint_id
                                    .eq(to_db_typed_uuid(blueprint_id)),
                            ),
                        )
                        .execute_async(&conn)
                        .await?
                    };

                    // Remove rows associated with Omicron datasets
                    let ndatasets = {
                        // Skip rustfmt because it bails out on this long line.
                        #[rustfmt::skip]
                        use nexus_db_schema::schema::
                            bp_omicron_dataset::dsl;
                        diesel::delete(
                            dsl::bp_omicron_dataset.filter(
                                dsl::blueprint_id
                                    .eq(to_db_typed_uuid(blueprint_id)),
                            ),
                        )
                        .execute_async(&conn)
                        .await?
                    };

                    // Remove rows associated with Omicron zones
                    let nzones = {
                        // Skip rustfmt because it bails out on this long line.
                        #[rustfmt::skip]
                        use nexus_db_schema::schema::
                            bp_omicron_zone::dsl;
                        diesel::delete(
                            dsl::bp_omicron_zone.filter(
                                dsl::blueprint_id
                                    .eq(to_db_typed_uuid(blueprint_id)),
                            ),
                        )
                        .execute_async(&conn)
                        .await?
                    };

                    let nnics = {
                        // Skip rustfmt because it bails out on this long line.
                        #[rustfmt::skip]
                        use nexus_db_schema::schema::
                            bp_omicron_zone_nic::dsl;
                        diesel::delete(
                            dsl::bp_omicron_zone_nic.filter(
                                dsl::blueprint_id
                                    .eq(to_db_typed_uuid(blueprint_id)),
                            ),
                        )
                        .execute_async(&conn)
                        .await?
                    };

                    let nclickhouse_cluster_configs = {
                        // Skip rustfmt because it bails out on this long line.
                        #[rustfmt::skip]
                        use nexus_db_schema::schema::
                            bp_clickhouse_cluster_config::dsl;
                        diesel::delete(
                            dsl::bp_clickhouse_cluster_config.filter(
                                dsl::blueprint_id
                                    .eq(to_db_typed_uuid(blueprint_id)),
                            ),
                        )
                        .execute_async(&conn)
                        .await?
                    };

                    let nclickhouse_keepers = {
                        // Skip rustfmt because it bails out on this long line.
                        #[rustfmt::skip]
                        use nexus_db_schema::schema::
                            bp_clickhouse_keeper_zone_id_to_node_id::dsl;
                        diesel::delete(
                            dsl::bp_clickhouse_keeper_zone_id_to_node_id
                                .filter(
                                    dsl::blueprint_id
                                        .eq(to_db_typed_uuid(blueprint_id)),
                                ),
                        )
                        .execute_async(&conn)
                        .await?
                    };

                    let nclickhouse_servers = {
                        // Skip rustfmt because it bails out on this long line.
                        #[rustfmt::skip]
                        use nexus_db_schema::schema::
                            bp_clickhouse_server_zone_id_to_node_id::dsl;
                        diesel::delete(
                            dsl::bp_clickhouse_server_zone_id_to_node_id
                                .filter(
                                    dsl::blueprint_id
                                        .eq(to_db_typed_uuid(blueprint_id)),
                                ),
                        )
                        .execute_async(&conn)
                        .await?
                    };

                    let noximeter_policy = {
                        // Skip rustfmt because it bails out on this long line.
                        #[rustfmt::skip]
                        use nexus_db_schema::schema::
                            bp_oximeter_read_policy::dsl;
                        diesel::delete(
                            dsl::bp_oximeter_read_policy.filter(
                                dsl::blueprint_id
                                    .eq(to_db_typed_uuid(blueprint_id)),
                            ),
                        )
                        .execute_async(&conn)
                        .await?
                    };

                    let npending_mgs_updates_sp = {
                        // Skip rustfmt because it bails out on this long line.
                        #[rustfmt::skip]
                        use nexus_db_schema::schema::
                            bp_pending_mgs_update_sp::dsl;
                        diesel::delete(
                            dsl::bp_pending_mgs_update_sp.filter(
                                dsl::blueprint_id
                                    .eq(to_db_typed_uuid(blueprint_id)),
                            ),
                        )
                        .execute_async(&conn)
                        .await?
                    };

                    let npending_mgs_updates_rot = {
                        // Skip rustfmt because it bails out on this long line.
                        #[rustfmt::skip]
                        use nexus_db_schema::schema::
                            bp_pending_mgs_update_rot::dsl;
                        diesel::delete(
                            dsl::bp_pending_mgs_update_rot.filter(
                                dsl::blueprint_id
                                    .eq(to_db_typed_uuid(blueprint_id)),
                            ),
                        )
                        .execute_async(&conn)
                        .await?
                    };

                    let npending_mgs_updates_rot_bootloader = {
                        // Skip rustfmt because it bails out on this long line.
                        #[rustfmt::skip]
                        use nexus_db_schema::schema::
                            bp_pending_mgs_update_rot_bootloader::dsl;
                        diesel::delete(
                            dsl::bp_pending_mgs_update_rot_bootloader.filter(
                                dsl::blueprint_id
                                    .eq(to_db_typed_uuid(blueprint_id)),
                            ),
                        )
                        .execute_async(&conn)
                        .await?
                    };

                    let npending_mgs_updates_host_phase_1 = {
                        // Skip rustfmt because it bails out on this long line.
                        #[rustfmt::skip]
                        use nexus_db_schema::schema::
                            bp_pending_mgs_update_host_phase_1::dsl;
                        diesel::delete(
                            dsl::bp_pending_mgs_update_host_phase_1.filter(
                                dsl::blueprint_id
                                    .eq(to_db_typed_uuid(blueprint_id)),
                            ),
                        )
                        .execute_async(&conn)
                        .await?
                    };

                    let ndebug_log_planning_report = {
                        // Skip rustfmt because it bails out on this long line.
                        #[rustfmt::skip]
                        use nexus_db_schema::schema::
                            debug_log_blueprint_planning::dsl;
                        diesel::delete(
                            dsl::debug_log_blueprint_planning.filter(
                                dsl::blueprint_id
                                    .eq(to_db_typed_uuid(blueprint_id)),
                            ),
                        )
                        .execute_async(&conn)
                        .await?
                    };

                    Ok(NumRowsDeleted {
                        nblueprints,
                        nsled_metadata,
                        nphysical_disks,
                        ndatasets,
                        nzones,
                        nnics,
                        nclickhouse_cluster_configs,
                        nclickhouse_keepers,
                        nclickhouse_servers,
                        noximeter_policy,
                        npending_mgs_updates_sp,
                        npending_mgs_updates_rot,
                        npending_mgs_updates_rot_bootloader,
                        npending_mgs_updates_host_phase_1,
                        ndebug_log_planning_report,
                    })
                }
            })
            .await
            .map_err(|e| match err.take() {
                Some(err) => err.into(),
                None => public_error_from_diesel(e, ErrorHandler::Server),
            })?;

        info!(&opctx.log, "removed blueprint";
            "blueprint_id" => blueprint_id.to_string(),
            "nblueprints" => nblueprints,
            "nsled_metadata" => nsled_metadata,
            "nphysical_disks" => nphysical_disks,
            "ndatasets" => ndatasets,
            "nzones" => nzones,
            "nnics" => nnics,
            "nclickhouse_cluster_configs" => nclickhouse_cluster_configs,
            "nclickhouse_keepers" => nclickhouse_keepers,
            "nclickhouse_servers" => nclickhouse_servers,
            "noximeter_policy" => noximeter_policy,
            "npending_mgs_updates_sp" => npending_mgs_updates_sp,
            "npending_mgs_updates_rot" => npending_mgs_updates_rot,
            "npending_mgs_updates_rot_bootloader" =>
            npending_mgs_updates_rot_bootloader,
            "npending_mgs_updates_host_phase_1" =>
            npending_mgs_updates_host_phase_1,
            "ndebug_log_planning_report" => ndebug_log_planning_report,
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
            tests::NetworkResourceControlFlow::default(),
        )
        .await
    }

    // The third and fourth arguments to this function only exist when run under
    // test, and allows the calling test to control the general timing of the
    // transaction executed by this method:
    //
    // 1. Check that `blueprint` is the current target blueprint
    // 2. Set `target_check_done` is set to true (the test can wait on this)
    // 3. Wait until `should_write_data` is set to true (the test can wait on this).
    // 4. Run remainder of transaction to allocate/deallocate resources
    // 5. Return
    //
    // If any of the test-only control flow parameters are "None", they are skipped.
    async fn blueprint_ensure_external_networking_resources_impl(
        &self,
        opctx: &OpContext,
        blueprint: &Blueprint,
        #[cfg(test)] test_control_flow: tests::NetworkResourceControlFlow,
    ) -> Result<(), Error> {
        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper(
            "blueprint_ensure_external_networking_resources",
        )
        .transaction(&conn, |conn| {
            let err = err.clone();
            #[cfg(test)]
            let target_check_done = test_control_flow.target_check_done.clone();
            #[cfg(test)]
            let should_write_data = test_control_flow.should_write_data.clone();

            async move {
                // Bail out if `blueprint` isn't the current target.
                let current_target = Self::blueprint_current_target_only(&conn)
                    .await
                    .map_err(|e| err.bail(e))?;
                if current_target.target_id != blueprint.id {
                    return Err(err.bail(
                        Error::invalid_request(format!(
                        "blueprint {} is not the current target blueprint ({})",
                        blueprint.id, current_target.target_id
                    ))
                        .into(),
                    ));
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
                // See the comment on this method; this lets us wait until our
                // test caller is ready for us to write data.
                #[cfg(test)]
                {
                    use std::sync::atomic::Ordering;
                    use std::time::Duration;
                    if let Some(gate) = should_write_data {
                        while !gate.load(Ordering::SeqCst) {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
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
                        .all_omicron_zones(|disposition| {
                            !disposition.is_in_service()
                        })
                        .map(|(_sled_id, zone)| zone),
                )
                .await
                .map_err(|e| err.bail(e.into()))?;
                self.ensure_zone_external_networking_allocated_on_connection(
                    &conn,
                    opctx,
                    blueprint
                        .all_omicron_zones(|disposition| {
                            disposition.is_in_service()
                        })
                        .map(|(_sled_id, zone)| zone),
                )
                .await
                .map_err(|e| err.bail(e.into()))?;

                Ok(())
            }
        })
        .await
        .map_err(|e| {
            if let Some(err) = err.take() {
                err.into()
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
        use nexus_db_schema::schema::bp_target::dsl;

        opctx
            .authorize(authz::Action::Modify, &authz::BLUEPRINT_CONFIG)
            .await?;

        // Diesel requires us to use an alias in order to refer to the
        // `bp_target` table twice in the same query.
        let bp_target2 =
            diesel::alias!(nexus_db_schema::schema::bp_target as bp_target1);

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
            .filter(dsl::blueprint_id.eq(to_db_typed_uuid(target.target_id)))
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
        let target = Self::blueprint_current_target_only(&conn).await?;

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
    pub async fn blueprint_target_get_current_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        opctx: &OpContext,
    ) -> Result<BlueprintTarget, TransactionError<Error>> {
        opctx.authorize(authz::Action::Read, &authz::BLUEPRINT_CONFIG).await?;
        Self::blueprint_current_target_only(&conn).await
    }

    /// Get the current target blueprint, if one exists
    pub async fn blueprint_target_get_current(
        &self,
        opctx: &OpContext,
    ) -> Result<BlueprintTarget, Error> {
        opctx.authorize(authz::Action::Read, &authz::BLUEPRINT_CONFIG).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        Self::blueprint_current_target_only(&conn).await.map_err(|e| e.into())
    }

    // Helper to fetch the current blueprint target (without fetching the entire
    // blueprint for that target).
    //
    // Caller is responsible for checking authz for this operation.
    async fn blueprint_current_target_only(
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<BlueprintTarget, TransactionError<Error>> {
        use nexus_db_schema::schema::bp_target::dsl;

        let current_target = dsl::bp_target
            .order_by(dsl::version.desc())
            .first_async::<BpTarget>(conn)
            .await
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

// Helper for reporting "should never happen" errors while inserting blueprints.
#[derive(Debug, Error)]
enum InsertTxnError {
    #[error("database error")]
    Diesel(#[from] DieselError),

    #[error(
        "aborting transaction after unexpectedly inserting {count} rows \
         for baseboard {baseboard_id:?} into {table_name}"
    )]
    BadInsertCount {
        table_name: &'static str,
        baseboard_id: Arc<BaseboardId>,
        count: usize,
    },
}

// Insert pending MGS updates for service processors for this blueprint.  These
// include foreign keys into the hw_baseboard_id table that we don't have handy.
// To achieve this, we use the same pattern used during inventory insertion:
//
//   INSERT INTO bp_pending_mgs_update_sp
//       SELECT
//           id
//           [other column values as literals]
//         FROM hw_baseboard_id
//         WHERE part_number = ... AND serial_number = ...;
//
// This way, we don't need to know the id.  The database looks it up for us as
// it does the INSERT.
//
// For each SP component (SP, RoT, RoT bootloader) we will be inserting to a
// different table: bp_pending_mgs_update_sp, bp_pending_mgs_update_rot, or
// bp_pending_mgs_update_rot_bootloader.
async fn insert_pending_mgs_update(
    conn: &async_bb8_diesel::Connection<DbConnection>,
    update: &PendingMgsUpdate,
    blueprint_id: BlueprintUuid,
    log: &Logger,
) -> Result<(), InsertTxnError> {
    match &update.details {
        PendingMgsUpdateDetails::Sp(PendingMgsUpdateSpDetails {
            expected_active_version,
            expected_inactive_version,
        }) => {
            let db_blueprint_id = DbTypedUuid::from(blueprint_id)
                .into_sql::<diesel::sql_types::Uuid>();
            let db_sp_type =
                SpType::from(update.sp_type).into_sql::<SpTypeEnum>();
            let db_slot_id = SpMgsSlot::from(SqlU16::from(update.slot_id))
                .into_sql::<diesel::sql_types::Int4>();
            let db_artifact_hash = ArtifactHash::from(update.artifact_hash)
                .into_sql::<diesel::sql_types::Text>();
            let db_artifact_version =
                DbArtifactVersion::from(update.artifact_version.clone())
                    .into_sql::<diesel::sql_types::Text>();
            let db_expected_version =
                DbArtifactVersion::from(expected_active_version.clone())
                    .into_sql::<diesel::sql_types::Text>();
            let db_expected_inactive_version =
                match expected_inactive_version {
                    ExpectedVersion::NoValidVersion => None,
                    ExpectedVersion::Version(v) => {
                        Some(DbArtifactVersion::from(v.clone()))
                    }
                }
                .into_sql::<Nullable<diesel::sql_types::Text>>();

            use nexus_db_schema::schema::hw_baseboard_id::dsl as baseboard_dsl;
            // Skip formatting to prevent rustfmt bailing out.
            #[rustfmt::skip]
            use nexus_db_schema::schema::bp_pending_mgs_update_sp::dsl
                as update_dsl;
            let selection = nexus_db_schema::schema::hw_baseboard_id::table
                .select((
                    db_blueprint_id,
                    baseboard_dsl::id,
                    db_sp_type,
                    db_slot_id,
                    db_artifact_hash,
                    db_artifact_version,
                    db_expected_version,
                    db_expected_inactive_version,
                ))
                .filter(
                    baseboard_dsl::part_number
                        .eq(update.baseboard_id.part_number.clone()),
                )
                .filter(
                    baseboard_dsl::serial_number
                        .eq(update.baseboard_id.serial_number.clone()),
                );
            let count =
                diesel::insert_into(update_dsl::bp_pending_mgs_update_sp)
                    .values(selection)
                    .into_columns((
                        update_dsl::blueprint_id,
                        update_dsl::hw_baseboard_id,
                        update_dsl::sp_type,
                        update_dsl::sp_slot,
                        update_dsl::artifact_sha256,
                        update_dsl::artifact_version,
                        update_dsl::expected_active_version,
                        update_dsl::expected_inactive_version,
                    ))
                    .execute_async(conn)
                    .await?;
            if count != 1 {
                // This should be impossible in practice. We will insert however
                // many rows matched the `baseboard_id` parts of the query
                // above. It can't be more than one 1 because we've filtered on
                // a pair of columns that are unique together. It could only be
                // 0 if the baseboard id had never been seen before in an
                // inventory collection.  But in that case, how did we manage to
                // construct a blueprint with it?
                //
                // This could happen in the test suite or with
                // `reconfigurator-cli`, which both let you create any blueprint
                // you like. In the test suite, the test just has to deal with
                // this behaviour (e.g., by inserting an inventory collection
                // containing this SP). With `reconfigurator-cli`, this amounts
                // to user error.
                error!(log,
                    "blueprint insertion: unexpectedly tried to insert wrong \
                     number of rows into bp_pending_mgs_update_sp \
                     (aborting transaction)";
                    "count" => count,
                    &update.baseboard_id,
                );
                return Err(InsertTxnError::BadInsertCount {
                    table_name: "bp_pending_mgs_update_sp",
                    count,
                    baseboard_id: update.baseboard_id.clone(),
                });
            }

            // This statement is just here to force a compilation error if the
            // set of columns in `bp_pending_mgs_update_sp` changes because that
            // will affect the correctness of the above statement.
            //
            // If you're here because of a compile error, you might be changing
            // the `bp_pending_mgs_update_sp` table. Update the statement below
            // and be sure to update the code above, too!
            let (
                _blueprint_id,
                _hw_baseboard_id,
                _sp_type,
                _sp_slot,
                _artifact_sha256,
                _artifact_version,
                _expected_active_version,
                _expected_inactive_version,
            ) = update_dsl::bp_pending_mgs_update_sp::all_columns();
        }
        PendingMgsUpdateDetails::Rot(PendingMgsUpdateRotDetails {
            expected_active_slot,
            expected_inactive_version,
            expected_persistent_boot_preference,
            expected_pending_persistent_boot_preference,
            expected_transient_boot_preference,
        }) => {
            let db_blueprint_id = DbTypedUuid::from(blueprint_id)
                .into_sql::<diesel::sql_types::Uuid>();
            let db_sp_type =
                SpType::from(update.sp_type).into_sql::<SpTypeEnum>();
            let db_slot_id = SpMgsSlot::from(SqlU16::from(update.slot_id))
                .into_sql::<diesel::sql_types::Int4>();
            let db_artifact_hash = ArtifactHash::from(update.artifact_hash)
                .into_sql::<diesel::sql_types::Text>();
            let db_artifact_version =
                DbArtifactVersion::from(update.artifact_version.clone())
                    .into_sql::<diesel::sql_types::Text>();
            let db_expected_active_slot =
                HwRotSlot::from(*expected_active_slot.slot())
                    .into_sql::<HwRotSlotEnum>();
            let db_expected_active_version =
                DbArtifactVersion::from(expected_active_slot.version())
                    .into_sql::<diesel::sql_types::Text>();
            let db_expected_inactive_version =
                match expected_inactive_version {
                    ExpectedVersion::NoValidVersion => None,
                    ExpectedVersion::Version(v) => {
                        Some(DbArtifactVersion::from(v.clone()))
                    }
                }
                .into_sql::<Nullable<diesel::sql_types::Text>>();
            let db_expected_persistent_boot_preference =
                HwRotSlot::from(*expected_persistent_boot_preference)
                    .into_sql::<HwRotSlotEnum>();
            let db_expected_pending_persistent_boot_preference =
                expected_pending_persistent_boot_preference
                    .map(|p| HwRotSlot::from(p))
                    .into_sql::<Nullable<HwRotSlotEnum>>();
            let db_expected_transient_boot_preference =
                expected_transient_boot_preference
                    .map(|p| HwRotSlot::from(p))
                    .into_sql::<Nullable<HwRotSlotEnum>>();

            use nexus_db_schema::schema::hw_baseboard_id::dsl as baseboard_dsl;
            // Skip formatting to prevent rustfmt bailing out.
            #[rustfmt::skip]
            use nexus_db_schema::schema::bp_pending_mgs_update_rot::dsl
                as update_dsl;
            let selection = nexus_db_schema::schema::hw_baseboard_id::table
                .select((
                    db_blueprint_id,
                    baseboard_dsl::id,
                    db_sp_type,
                    db_slot_id,
                    db_artifact_hash,
                    db_artifact_version,
                    db_expected_active_slot,
                    db_expected_active_version,
                    db_expected_inactive_version,
                    db_expected_persistent_boot_preference,
                    db_expected_pending_persistent_boot_preference,
                    db_expected_transient_boot_preference,
                ))
                .filter(
                    baseboard_dsl::part_number
                        .eq(update.baseboard_id.part_number.clone()),
                )
                .filter(
                    baseboard_dsl::serial_number
                        .eq(update.baseboard_id.serial_number.clone()),
                );
            let count =
                diesel::insert_into(update_dsl::bp_pending_mgs_update_rot)
                    .values(selection)
                    .into_columns((
                        update_dsl::blueprint_id,
                        update_dsl::hw_baseboard_id,
                        update_dsl::sp_type,
                        update_dsl::sp_slot,
                        update_dsl::artifact_sha256,
                        update_dsl::artifact_version,
                        update_dsl::expected_active_slot,
                        update_dsl::expected_active_version,
                        update_dsl::expected_inactive_version,
                        update_dsl::expected_persistent_boot_preference,
                        update_dsl::expected_pending_persistent_boot_preference,
                        update_dsl::expected_transient_boot_preference,
                    ))
                    .execute_async(conn)
                    .await?;
            if count != 1 {
                // As with `PendingMgsUpdateDetails::Sp`, this should be
                // impossible in practice.
                error!(log,
                    "blueprint insertion: unexpectedly tried to insert wrong \
                     number of rows into bp_pending_mgs_update_rot \
                     (aborting transaction)";
                    "count" => count,
                    &update.baseboard_id,
                );
                return Err(InsertTxnError::BadInsertCount {
                    table_name: "bp_pending_mgs_update_rot",
                    count,
                    baseboard_id: update.baseboard_id.clone(),
                });
            }

            // This statement is just here to force a compilation error if the
            // set of columns in `bp_pending_mgs_update_rot` changes because
            // that will affect the correctness of the above statement.
            //
            // If you're here because of a compile error, you might be changing
            // the `bp_pending_mgs_update_rot` table. Update the statement below
            // and be sure to update the code above, too!
            let (
                _blueprint_id,
                _hw_baseboard_id,
                _sp_type,
                _sp_slot,
                _artifact_sha256,
                _artifact_version,
                _expected_active_slot,
                _expected_active_version,
                _expected_inactive_version,
                _expected_persistent_boot_preference,
                _expected_pending_persistent_boot_preference,
                _expected_transient_boot_preference,
            ) = update_dsl::bp_pending_mgs_update_rot::all_columns();
        }
        PendingMgsUpdateDetails::RotBootloader(
            PendingMgsUpdateRotBootloaderDetails {
                expected_stage0_version,
                expected_stage0_next_version,
            },
        ) => {
            let db_blueprint_id = DbTypedUuid::from(blueprint_id)
                .into_sql::<diesel::sql_types::Uuid>();
            let db_sp_type =
                SpType::from(update.sp_type).into_sql::<SpTypeEnum>();
            let db_slot_id = SpMgsSlot::from(SqlU16::from(update.slot_id))
                .into_sql::<diesel::sql_types::Int4>();
            let db_artifact_hash = ArtifactHash::from(update.artifact_hash)
                .into_sql::<diesel::sql_types::Text>();
            let db_artifact_version =
                DbArtifactVersion::from(update.artifact_version.clone())
                    .into_sql::<diesel::sql_types::Text>();
            let db_expected_stage0_version =
                DbArtifactVersion::from(expected_stage0_version.clone())
                    .into_sql::<diesel::sql_types::Text>();
            let db_expected_stage0_next_version =
                match expected_stage0_next_version {
                    ExpectedVersion::NoValidVersion => None,
                    ExpectedVersion::Version(v) => {
                        Some(DbArtifactVersion::from(v.clone()))
                    }
                }
                .into_sql::<Nullable<diesel::sql_types::Text>>();

            use nexus_db_schema::schema::hw_baseboard_id::dsl as baseboard_dsl;
            // Skip formatting to prevent rustfmt bailing out.
            #[rustfmt::skip]
            use nexus_db_schema::schema::bp_pending_mgs_update_rot_bootloader::dsl
                as update_dsl;
            let selection = nexus_db_schema::schema::hw_baseboard_id::table
                .select((
                    db_blueprint_id,
                    baseboard_dsl::id,
                    db_sp_type,
                    db_slot_id,
                    db_artifact_hash,
                    db_artifact_version,
                    db_expected_stage0_version,
                    db_expected_stage0_next_version,
                ))
                .filter(
                    baseboard_dsl::part_number
                        .eq(update.baseboard_id.part_number.clone()),
                )
                .filter(
                    baseboard_dsl::serial_number
                        .eq(update.baseboard_id.serial_number.clone()),
                );
            let count = diesel::insert_into(
                update_dsl::bp_pending_mgs_update_rot_bootloader,
            )
            .values(selection)
            .into_columns((
                update_dsl::blueprint_id,
                update_dsl::hw_baseboard_id,
                update_dsl::sp_type,
                update_dsl::sp_slot,
                update_dsl::artifact_sha256,
                update_dsl::artifact_version,
                update_dsl::expected_stage0_version,
                update_dsl::expected_stage0_next_version,
            ))
            .execute_async(conn)
            .await?;
            if count != 1 {
                // As with `PendingMgsUpdateDetails::Sp`, this should be
                // impossible in practice.
                error!(log,
                    "blueprint insertion: unexpectedly tried to insert wrong \
                     number of rows into bp_pending_mgs_update_rot_bootloader \
                     (aborting transaction)";
                    "count" => count,
                    &update.baseboard_id,
                );
                return Err(InsertTxnError::BadInsertCount {
                    table_name: "bp_pending_mgs_update_rot_bootloader",
                    count,
                    baseboard_id: update.baseboard_id.clone(),
                });
            }

            // This statement is just here to force a compilation error if the
            // set of columns in `bp_pending_mgs_update_rot_bootloader` changes
            // because that will affect the correctness of the above statement.
            //
            // If you're here because of a compile error, you might be changing
            // the `bp_pending_mgs_update_rot_bootloader` table. Update the
            // statement below and be sure to update the code above, too!
            let (
                _blueprint_id,
                _hw_baseboard_id,
                _sp_type,
                _sp_slot,
                _artifact_sha256,
                _artifact_version,
                _expected_stage0_version,
                _expected_stage0_next_version,
            ) = update_dsl::bp_pending_mgs_update_rot_bootloader::all_columns();
        }
        PendingMgsUpdateDetails::HostPhase1(
            PendingMgsUpdateHostPhase1Details {
                expected_active_phase_1_slot,
                expected_boot_disk,
                expected_active_phase_1_hash,
                expected_active_phase_2_hash,
                expected_inactive_phase_1_hash,
                expected_inactive_phase_2_hash,
                sled_agent_address,
            },
        ) => {
            let db_blueprint_id = DbTypedUuid::from(blueprint_id)
                .into_sql::<diesel::sql_types::Uuid>();
            let db_sp_type =
                SpType::from(update.sp_type).into_sql::<SpTypeEnum>();
            let db_slot_id = SpMgsSlot::from(SqlU16::from(update.slot_id))
                .into_sql::<diesel::sql_types::Int4>();
            let db_artifact_hash = ArtifactHash::from(update.artifact_hash)
                .into_sql::<diesel::sql_types::Text>();
            let db_artifact_version =
                DbArtifactVersion::from(update.artifact_version.clone())
                    .into_sql::<diesel::sql_types::Text>();
            let db_expected_active_phase_1_slot =
                HwM2Slot::from(*expected_active_phase_1_slot)
                    .into_sql::<HwM2SlotEnum>();
            let db_expected_boot_disk =
                HwM2Slot::from(*expected_boot_disk).into_sql::<HwM2SlotEnum>();
            let db_expected_active_phase_1_hash =
                ArtifactHash(*expected_active_phase_1_hash)
                    .into_sql::<diesel::sql_types::Text>();
            let db_expected_active_phase_2_hash =
                ArtifactHash(*expected_active_phase_2_hash)
                    .into_sql::<diesel::sql_types::Text>();
            let db_expected_inactive_phase_1_hash =
                ArtifactHash(*expected_inactive_phase_1_hash)
                    .into_sql::<diesel::sql_types::Text>();
            let db_expected_inactive_phase_2_hash =
                ArtifactHash(*expected_inactive_phase_2_hash)
                    .into_sql::<diesel::sql_types::Text>();
            let db_sled_agent_ip = Ipv6Addr::from(sled_agent_address.ip())
                .into_sql::<diesel::sql_types::Inet>();
            let db_sled_agent_port =
                SqlU16(sled_agent_address.port()).into_sql::<sql_types::Int4>();

            use nexus_db_schema::schema::hw_baseboard_id::dsl as baseboard_dsl;
            // Skip formatting to prevent rustfmt bailing out.
            #[rustfmt::skip]
            use nexus_db_schema::schema::bp_pending_mgs_update_host_phase_1::dsl
                as update_dsl;
            let selection = nexus_db_schema::schema::hw_baseboard_id::table
                .select((
                    db_blueprint_id,
                    baseboard_dsl::id,
                    db_sp_type,
                    db_slot_id,
                    db_artifact_hash,
                    db_artifact_version,
                    db_expected_active_phase_1_slot,
                    db_expected_boot_disk,
                    db_expected_active_phase_1_hash,
                    db_expected_active_phase_2_hash,
                    db_expected_inactive_phase_1_hash,
                    db_expected_inactive_phase_2_hash,
                    db_sled_agent_ip,
                    db_sled_agent_port,
                ))
                .filter(
                    baseboard_dsl::part_number
                        .eq(update.baseboard_id.part_number.clone()),
                )
                .filter(
                    baseboard_dsl::serial_number
                        .eq(update.baseboard_id.serial_number.clone()),
                );
            let count = diesel::insert_into(
                update_dsl::bp_pending_mgs_update_host_phase_1,
            )
            .values(selection)
            .into_columns((
                update_dsl::blueprint_id,
                update_dsl::hw_baseboard_id,
                update_dsl::sp_type,
                update_dsl::sp_slot,
                update_dsl::artifact_sha256,
                update_dsl::artifact_version,
                update_dsl::expected_active_phase_1_slot,
                update_dsl::expected_boot_disk,
                update_dsl::expected_active_phase_1_hash,
                update_dsl::expected_active_phase_2_hash,
                update_dsl::expected_inactive_phase_1_hash,
                update_dsl::expected_inactive_phase_2_hash,
                update_dsl::sled_agent_ip,
                update_dsl::sled_agent_port,
            ))
            .execute_async(conn)
            .await?;
            if count != 1 {
                // As with `PendingMgsUpdateDetails::Sp`, this should be
                // impossible in practice.
                error!(
                    log,
                    "blueprint insertion: unexpectedly tried to \
                     insert wrong number of rows into \
                     bp_pending_mgs_update_host_phase_1 (aborting transaction)";
                    "count" => count,
                    &update.baseboard_id,
                );
                return Err(InsertTxnError::BadInsertCount {
                    table_name: "bp_pending_mgs_update_host_phase_1",
                    count,
                    baseboard_id: update.baseboard_id.clone(),
                });
            }

            // This statement is just here to force a compilation error if the
            // set of columns in `bp_pending_mgs_update_host_phase_1` changes
            // because that will affect the correctness of the above statement.
            //
            // If you're here because of a compile error, you might be changing
            // the `bp_pending_mgs_update_host_phase_1` table. Update the
            // statement below and be sure to update the code above, too!
            let (
                _blueprint_id,
                _hw_baseboard_id,
                _sp_type,
                _sp_slot,
                _artifact_sha256,
                _artifact_version,
                _expected_active_phase_1_slot,
                _expected_boot_disk,
                _expected_active_phase_1_hash,
                _expected_active_phase_2_hash,
                _expected_inactive_phase_1_hash,
                _expected_inactive_phase_2_hash,
                _sled_agent_ip,
                _sled_agent_port,
            ) = update_dsl::bp_pending_mgs_update_host_phase_1::all_columns();
        }
    }
    Ok(())
}

// Helper to process BpPendingMgsUpdateComponent rows
fn process_update_row<T>(
    row: T,
    baseboards_by_id: &BTreeMap<Uuid, Arc<BaseboardId>>,
    pending_mgs_updates: &mut PendingMgsUpdates,
    blueprint_id: &TypedUuid<BlueprintKind>,
) -> Result<(), Error>
where
    T: BpPendingMgsUpdateComponent,
{
    let Some(baseboard) = baseboards_by_id.get(row.hw_baseboard_id()) else {
        // This should be impossible.
        return Err(Error::internal_error(&format!(
            "loading blueprint {}: missing baseboard that we should \
             have fetched: {}",
            blueprint_id,
            row.hw_baseboard_id()
        )));
    };

    let update = row.into_generic(baseboard.clone());
    if let Some(previous) = pending_mgs_updates.insert(update) {
        // This should be impossible.
        return Err(Error::internal_error(&format!(
            "blueprint {}: found multiple pending updates for \
             baseboard {:?}",
            blueprint_id, previous.baseboard_id
        )));
    }
    Ok(())
}

// Helper to create an `authz::Blueprint` for a specific blueprint ID
fn authz_blueprint_from_id(blueprint_id: BlueprintUuid) -> authz::Blueprint {
    let blueprint_id = blueprint_id.into_untyped_uuid();
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
    NoSuchBlueprint(BlueprintUuid),
    /// The requested target blueprint's parent does not match the current
    /// target.
    ParentNotTarget(BlueprintUuid),
    /// Any other error
    Other(DieselError),
}

impl From<InsertTargetError> for Error {
    fn from(value: InsertTargetError) -> Self {
        match value {
            InsertTargetError::NoSuchBlueprint(id) => Error::not_found_by_id(
                ResourceType::Blueprint,
                id.as_untyped_uuid(),
            ),
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
    target_id: BlueprintUuid,
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
const NO_SUCH_BLUEPRINT_ERROR_MESSAGE: &str = "could not parse \"no-such-blueprint\" as type uuid: \
     uuid: incorrect UUID length: no-such-blueprint";
const PARENT_NOT_TARGET_ERROR_MESSAGE: &str = "could not parse \"parent-not-target\" as type uuid: \
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
        use nexus_db_schema::schema::blueprint::dsl as bp_dsl;
        use nexus_db_schema::schema::bp_target::dsl;

        type FromClause<T> =
            diesel::internal::table_macro::StaticQueryFragmentInstance<T>;
        type BpTargetFromClause =
            FromClause<nexus_db_schema::schema::bp_target::table>;
        type BlueprintFromClause =
            FromClause<nexus_db_schema::schema::blueprint::table>;
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
        out.push_bind_param::<sql_types::Uuid, Uuid>(
            self.target_id.as_untyped_uuid(),
        )?;
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
        out.push_bind_param::<sql_types::Uuid, Uuid>(
            self.target_id.as_untyped_uuid(),
        )?;
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
        out.push_bind_param::<sql_types::Uuid, Uuid>(
            self.target_id.as_untyped_uuid(),
        )?;
        out.push_sql(" AND ");
        out.push_identifier(bp_dsl::parent_blueprint_id::NAME)?;
        out.push_sql(
            "  IS NULL \
                        AND NOT EXISTS ( \
                          SELECT version FROM current_target) \
                        ) = 1, ",
        );
        out.push_sql("  CAST(");
        out.push_bind_param::<sql_types::Uuid, Uuid>(
            self.target_id.as_untyped_uuid(),
        )?;
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
        out.push_bind_param::<sql_types::Uuid, Uuid>(
            self.target_id.as_untyped_uuid(),
        )?;
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
        out.push_bind_param::<sql_types::Uuid, Uuid>(
            self.target_id.as_untyped_uuid(),
        )?;
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
        out.push_bind_param::<sql_types::Uuid, Uuid>(
            self.target_id.as_untyped_uuid(),
        )?;
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

    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::raw_query_builder::QueryBuilder;
    use gateway_types::rot::RotSlot;
    use nexus_db_model::IpVersion;
    use nexus_inventory::CollectionBuilder;
    use nexus_inventory::now_db_precision;
    use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
    use nexus_reconfigurator_planning::blueprint_builder::Ensure;
    use nexus_reconfigurator_planning::blueprint_builder::EnsureMultiple;
    use nexus_reconfigurator_planning::example::ExampleSystemBuilder;
    use nexus_reconfigurator_planning::example::example;
    use nexus_reconfigurator_planning::planner::PlannerRng;
    use nexus_types::deployment::BlueprintArtifactVersion;
    use nexus_types::deployment::BlueprintHostPhase2DesiredContents;
    use nexus_types::deployment::BlueprintHostPhase2DesiredSlots;
    use nexus_types::deployment::BlueprintPhysicalDiskDisposition;
    use nexus_types::deployment::BlueprintZoneConfig;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::deployment::BlueprintZoneImageSource;
    use nexus_types::deployment::BlueprintZoneType;
    use nexus_types::deployment::ExpectedActiveRotSlot;
    use nexus_types::deployment::OmicronZoneExternalFloatingIp;
    use nexus_types::deployment::PendingMgsUpdate;
    use nexus_types::deployment::PlanningInput;
    use nexus_types::deployment::PlanningInputBuilder;
    use nexus_types::deployment::SledDetails;
    use nexus_types::deployment::SledDisk;
    use nexus_types::deployment::SledFilter;
    use nexus_types::deployment::SledResources;
    use nexus_types::deployment::blueprint_zone_type;
    use nexus_types::external_api::views::PhysicalDiskPolicy;
    use nexus_types::external_api::views::PhysicalDiskState;
    use nexus_types::external_api::views::SledPolicy;
    use nexus_types::external_api::views::SledState;
    use nexus_types::inventory::BaseboardId;
    use nexus_types::inventory::Collection;
    use omicron_common::address::IpRange;
    use omicron_common::address::Ipv6Subnet;
    use omicron_common::api::external::MacAddr;
    use omicron_common::api::external::Name;
    use omicron_common::api::external::TufArtifactMeta;
    use omicron_common::api::external::TufRepoDescription;
    use omicron_common::api::external::TufRepoMeta;
    use omicron_common::api::external::Vni;
    use omicron_common::api::internal::shared::NetworkInterface;
    use omicron_common::api::internal::shared::NetworkInterfaceKind;
    use omicron_common::disk::DiskIdentity;
    use omicron_common::disk::M2Slot;
    use omicron_common::update::ArtifactId;
    use omicron_common::zpool_name::ZpoolName;
    use omicron_test_utils::dev;
    use omicron_test_utils::dev::poll::CondCheckError;
    use omicron_test_utils::dev::poll::wait_for_condition;
    use omicron_uuid_kinds::ExternalIpUuid;
    use omicron_uuid_kinds::OmicronZoneUuid;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::SledUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use oxnet::IpNet;
    use pretty_assertions::assert_eq;
    use rand::Rng;
    use std::collections::BTreeSet;
    use std::mem;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;
    use std::net::SocketAddrV6;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::LazyLock;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use tufaceous_artifact::ArtifactHash;
    use tufaceous_artifact::ArtifactVersion;

    static EMPTY_PLANNING_INPUT: LazyLock<PlanningInput> =
        LazyLock::new(|| PlanningInputBuilder::empty_input());

    #[derive(Default)]
    pub struct NetworkResourceControlFlow {
        pub target_check_done: Option<Arc<AtomicBool>>,
        pub should_write_data: Option<Arc<AtomicBool>>,
    }

    // Check that all the subtables related to blueprints have been pruned of a specific
    // blueprint ID. Uses the shared BlueprintTableCounts struct.
    async fn ensure_blueprint_fully_deleted(
        datastore: &DataStore,
        blueprint_id: BlueprintUuid,
    ) {
        let counts = BlueprintTableCounts::new(datastore, blueprint_id).await;

        // All tables should be empty (no exceptions for deleted blueprints)
        assert!(
            counts.all_empty(),
            "Blueprint {blueprint_id} not fully deleted. Non-empty tables: {:?}",
            counts.non_empty_tables()
        );
    }

    // Create a fake set of `SledDetails`, either with a subnet matching
    // `ip` or with an arbitrary one.
    fn fake_sled_details(ip: Option<Ipv6Addr>) -> SledDetails {
        let zpools = (0..4)
            .map(|i| {
                (
                    ZpoolUuid::new_v4(),
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
                )
            })
            .collect();
        let ip = ip.unwrap_or_else(|| rand::rng().random::<u128>().into());
        let resources = SledResources { zpools, subnet: Ipv6Subnet::new(ip) };
        SledDetails {
            policy: SledPolicy::provisionable(),
            state: SledState::Active,
            resources,
            baseboard_id: BaseboardId {
                part_number: String::from("unused"),
                serial_number: String::from("unused"),
            },
        }
    }

    fn representative(
        log: &Logger,
        test_name: &str,
    ) -> (Collection, PlanningInput, Blueprint) {
        // We'll start with an example system.
        let (mut base_collection, planning_input, mut blueprint) =
            example(log, test_name);

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

        // Treat this blueprint as the initial blueprint for the system.
        blueprint.parent_blueprint_id = None;

        (collection, planning_input, blueprint)
    }

    async fn blueprint_list_all_ids(
        opctx: &OpContext,
        datastore: &DataStore,
    ) -> Vec<BlueprintUuid> {
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
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

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

        // There ought to be no sleds and no parent blueprint.
        assert_eq!(blueprint1.sleds.len(), 0);
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
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_representative_blueprint() {
        const TEST_NAME: &str = "test_representative_blueprint";
        // Setup
        let logctx = dev::test_setup_log(TEST_NAME);
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

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

        // Ensure every bp_* table received at least one row for this blueprint (issue #8455).
        ensure_blueprint_fully_populated(&datastore, blueprint1.id).await;

        // Check the number of blueprint elements against our collection.
        assert_eq!(
            blueprint1.sleds.len(),
            planning_input.all_sled_ids(SledFilter::Commissioned).count(),
        );
        assert_eq!(blueprint1.sleds.len(), collection.sled_agents.len());
        assert_eq!(
            blueprint1.all_omicron_zones(BlueprintZoneDisposition::any).count(),
            collection.all_ledgered_omicron_zones().count()
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
        let new_sled_zpools = &planning_input
            .sled_lookup(SledFilter::Commissioned, new_sled_id)
            .unwrap()
            .resources
            .zpools;

        // Create a builder for a child blueprint.
        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint1,
            &planning_input,
            &collection,
            "test",
            PlannerRng::from_entropy(),
        )
        .expect("failed to create builder");

        // Ensure disks on our sled
        assert_eq!(
            EnsureMultiple::from(
                builder
                    .sled_add_disks(
                        new_sled_id,
                        &planning_input
                            .sled_lookup(SledFilter::Commissioned, new_sled_id)
                            .unwrap()
                            .resources
                    )
                    .unwrap()
                    .disks
            ),
            EnsureMultiple::Changed {
                added: 4,
                updated: 0,
                expunged: 0,
                removed: 0
            }
        );

        // Add zones to our new sled.
        assert_eq!(
            builder
                .sled_ensure_zone_ntp(
                    new_sled_id,
                    BlueprintZoneImageSource::InstallDataset
                )
                .unwrap(),
            Ensure::Added
        );
        for zpool_id in new_sled_zpools.keys() {
            assert_eq!(
                builder
                    .sled_ensure_zone_crucible(
                        new_sled_id,
                        *zpool_id,
                        BlueprintZoneImageSource::InstallDataset
                    )
                    .unwrap(),
                Ensure::Added
            );
        }

        const ARTIFACT_VERSION_1: ArtifactVersion =
            ArtifactVersion::new_const("1.0.0");
        const ARTIFACT_VERSION_2: ArtifactVersion =
            ArtifactVersion::new_const("2.0.0");
        const ARTIFACT_VERSION_3: ArtifactVersion =
            ArtifactVersion::new_const("2.0.0");
        const ZONE_ARTIFACT_HASH_1: ArtifactHash = ArtifactHash([1; 32]);
        const ZONE_ARTIFACT_HASH_2: ArtifactHash = ArtifactHash([2; 32]);
        const HOST_ARTIFACT_HASH_1: ArtifactHash = ArtifactHash([3; 32]);
        const HOST_ARTIFACT_HASH_2: ArtifactHash = ArtifactHash([4; 32]);
        const HOST_ARTIFACT_HASH_3: ArtifactHash = ArtifactHash([5; 32]);

        // Add rows to the tuf_artifact table to test version lookups.
        {
            const SYSTEM_VERSION: semver::Version =
                semver::Version::new(0, 0, 1);
            const SYSTEM_HASH: ArtifactHash = ArtifactHash([3; 32]);

            // Add a zone artifact and two host phase 2 artifacts.
            datastore
                .tuf_repo_insert(
                    opctx,
                    &TufRepoDescription {
                        repo: TufRepoMeta {
                            hash: SYSTEM_HASH,
                            targets_role_version: 0,
                            valid_until: Utc::now(),
                            system_version: SYSTEM_VERSION,
                            file_name: String::new(),
                        },
                        artifacts: vec![
                            TufArtifactMeta {
                                id: ArtifactId {
                                    name: String::new(),
                                    version: ARTIFACT_VERSION_1,
                                    kind: KnownArtifactKind::Zone.into(),
                                },
                                hash: ZONE_ARTIFACT_HASH_1,
                                size: 0,
                                board: None,
                                sign: None,
                            },
                            TufArtifactMeta {
                                id: ArtifactId {
                                    name: "host-1".into(),
                                    version: ARTIFACT_VERSION_2,
                                    kind: ArtifactKind::HOST_PHASE_2,
                                },
                                hash: HOST_ARTIFACT_HASH_1,
                                size: 0,
                                board: None,
                                sign: None,
                            },
                            TufArtifactMeta {
                                id: ArtifactId {
                                    name: "host-2".into(),
                                    version: ARTIFACT_VERSION_3,
                                    kind: ArtifactKind::HOST_PHASE_2,
                                },
                                hash: HOST_ARTIFACT_HASH_2,
                                size: 0,
                                board: None,
                                sign: None,
                            },
                        ],
                    },
                )
                .await
                .expect("inserted TUF repo");
        }

        // Take the first two zones and set their image sources.
        {
            let zone_ids: Vec<OmicronZoneUuid> = builder
                .current_sled_zones(
                    new_sled_id,
                    BlueprintZoneDisposition::is_in_service,
                )
                .map(|zone| zone.id)
                .take(2)
                .collect();
            if zone_ids.len() < 2 {
                panic!(
                    "expected new sled to have at least 2 zones, got {}",
                    zone_ids.len()
                );
            }
            builder
                .sled_set_zone_source(
                    new_sled_id,
                    zone_ids[0],
                    BlueprintZoneImageSource::Artifact {
                        version: BlueprintArtifactVersion::Available {
                            version: ARTIFACT_VERSION_1,
                        },
                        hash: ZONE_ARTIFACT_HASH_1,
                    },
                )
                .unwrap();
            builder
                .sled_set_zone_source(
                    new_sled_id,
                    zone_ids[1],
                    BlueprintZoneImageSource::Artifact {
                        version: BlueprintArtifactVersion::Unknown,
                        hash: ZONE_ARTIFACT_HASH_2,
                    },
                )
                .unwrap();
        }

        // Try a few different combinations of desired host phase 2 contents on
        // four sleds:
        //
        // 1. slot_a set to a known version; slot_b left at current contents
        // 2. slot_a left at current contents; slot_b set to a known version
        // 3. both slots set to a known version
        // 4. slot_a set to a known version; slot b set to an unknown version
        {
            let sled_ids = builder.sled_ids_with_zones().collect::<Vec<_>>();
            assert!(sled_ids.len() >= 4, "at least 4 sleds");

            let host_phase_2_samples = [
                BlueprintHostPhase2DesiredSlots {
                    slot_a: BlueprintHostPhase2DesiredContents::Artifact {
                        version: BlueprintArtifactVersion::Available {
                            version: ARTIFACT_VERSION_2,
                        },
                        hash: HOST_ARTIFACT_HASH_1,
                    },
                    slot_b: BlueprintHostPhase2DesiredContents::CurrentContents,
                },
                BlueprintHostPhase2DesiredSlots {
                    slot_a: BlueprintHostPhase2DesiredContents::CurrentContents,
                    slot_b: BlueprintHostPhase2DesiredContents::Artifact {
                        version: BlueprintArtifactVersion::Available {
                            version: ARTIFACT_VERSION_2,
                        },
                        hash: HOST_ARTIFACT_HASH_1,
                    },
                },
                BlueprintHostPhase2DesiredSlots {
                    slot_a: BlueprintHostPhase2DesiredContents::Artifact {
                        version: BlueprintArtifactVersion::Available {
                            version: ARTIFACT_VERSION_2,
                        },
                        hash: HOST_ARTIFACT_HASH_1,
                    },
                    slot_b: BlueprintHostPhase2DesiredContents::Artifact {
                        version: BlueprintArtifactVersion::Available {
                            version: ARTIFACT_VERSION_3,
                        },
                        hash: HOST_ARTIFACT_HASH_2,
                    },
                },
                BlueprintHostPhase2DesiredSlots {
                    slot_a: BlueprintHostPhase2DesiredContents::Artifact {
                        version: BlueprintArtifactVersion::Available {
                            version: ARTIFACT_VERSION_2,
                        },
                        hash: HOST_ARTIFACT_HASH_1,
                    },
                    slot_b: BlueprintHostPhase2DesiredContents::Artifact {
                        version: BlueprintArtifactVersion::Unknown,
                        hash: HOST_ARTIFACT_HASH_3,
                    },
                },
            ];

            for (sled_id, host_phase_2) in
                sled_ids.into_iter().zip(host_phase_2_samples.into_iter())
            {
                builder.sled_set_host_phase_2(sled_id, host_phase_2).unwrap();
            }
        }

        // Configure an SP update.
        let (baseboard_id, sp) =
            collection.sps.iter().next().expect("at least one SP");
        builder.pending_mgs_update_insert(PendingMgsUpdate {
            baseboard_id: baseboard_id.clone(),
            sp_type: sp.sp_type,
            slot_id: sp.sp_slot,
            details: PendingMgsUpdateDetails::Sp(PendingMgsUpdateSpDetails {
                expected_active_version: "1.0.0".parse().unwrap(),
                expected_inactive_version: ExpectedVersion::NoValidVersion,
            }),
            artifact_hash: ArtifactHash([72; 32]),
            artifact_version: "2.0.0".parse().unwrap(),
        });

        let num_new_ntp_zones = 1;
        let num_new_crucible_zones = new_sled_zpools.len();
        let num_new_sled_zones = num_new_ntp_zones + num_new_crucible_zones;

        let blueprint2 = builder.build(BlueprintSource::Test);
        let authz_blueprint2 = authz_blueprint_from_id(blueprint2.id);

        let diff = blueprint2.diff_since_blueprint(&blueprint1);
        println!("b1 -> b2: {}", diff.display());
        println!("b1 sleds: {:?}", blueprint1.sleds);
        println!("b2 sleds: {:?}", blueprint2.sleds);
        // Check that we added the new sled, as well as its disks and zones.
        assert_eq!(
            blueprint1
                .all_omicron_disks(BlueprintPhysicalDiskDisposition::any)
                .count()
                + new_sled_zpools.len(),
            blueprint2
                .all_omicron_disks(BlueprintPhysicalDiskDisposition::any)
                .count()
        );
        assert_eq!(blueprint1.sleds.len() + 1, blueprint2.sleds.len());
        assert_eq!(
            blueprint1.all_omicron_zones(BlueprintZoneDisposition::any).count()
                + num_new_sled_zones,
            blueprint2.all_omicron_zones(BlueprintZoneDisposition::any).count()
        );

        // All zones should be in service.
        assert_all_zones_in_service(&blueprint2);
        assert_eq!(blueprint2.parent_blueprint_id, Some(blueprint1.id));

        // This blueprint contains a PendingMgsUpdate that references an SP from
        // `collection`.  This must already be present in the database for
        // blueprint insertion to work.
        datastore
            .inventory_insert_collection(&opctx, &collection)
            .await
            .expect("failed to insert inventory collection");

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

        // blueprint2 is more interesting in terms of containing a variety of
        // different blueprint structures.  We want to try deleting that.  To do
        // that, we have to create a new blueprint and make that one the target.
        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint2,
            &planning_input,
            &collection,
            "dummy",
            PlannerRng::from_entropy(),
        )
        .expect("failed to create builder");

        // Configure an RoT update
        let (baseboard_id, sp) =
            collection.sps.iter().next().expect("at least one SP");
        builder.pending_mgs_update_insert(PendingMgsUpdate {
            baseboard_id: baseboard_id.clone(),
            sp_type: sp.sp_type,
            slot_id: sp.sp_slot,
            details: PendingMgsUpdateDetails::Rot(PendingMgsUpdateRotDetails {
                expected_active_slot: ExpectedActiveRotSlot {
                    slot: RotSlot::A,
                    version: "1.0.0".parse().unwrap(),
                },
                expected_inactive_version: ExpectedVersion::NoValidVersion,
                expected_persistent_boot_preference: RotSlot::A,
                expected_pending_persistent_boot_preference: None,
                expected_transient_boot_preference: None,
            }),
            artifact_hash: ArtifactHash([72; 32]),
            artifact_version: "2.0.0".parse().unwrap(),
        });
        let blueprint3 = builder.build(BlueprintSource::Test);
        let authz_blueprint3 = authz_blueprint_from_id(blueprint3.id);
        datastore
            .blueprint_insert(&opctx, &blueprint3)
            .await
            .expect("failed to insert blueprint");
        assert_eq!(
            blueprint3,
            datastore
                .blueprint_read(&opctx, &authz_blueprint3)
                .await
                .expect("failed to read collection back")
        );
        let bp3_target = BlueprintTarget {
            target_id: blueprint3.id,
            enabled: true,
            time_made_target: now_db_precision(),
        };
        datastore
            .blueprint_target_set_current(&opctx, bp3_target)
            .await
            .unwrap();
        datastore.blueprint_delete(&opctx, &authz_blueprint2).await.unwrap();
        ensure_blueprint_fully_deleted(&datastore, blueprint2.id).await;

        // We now make sure we can build and insert a blueprint containing an
        // RoT bootloader Pending MGS update
        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint3,
            &planning_input,
            &collection,
            "dummy",
            PlannerRng::from_entropy(),
        )
        .expect("failed to create builder");

        // Configure an RoT bootloader update
        let (baseboard_id, sp) =
            collection.sps.iter().next().expect("at least one SP");
        builder.pending_mgs_update_insert(PendingMgsUpdate {
            baseboard_id: baseboard_id.clone(),
            sp_type: sp.sp_type,
            slot_id: sp.sp_slot,
            details: PendingMgsUpdateDetails::RotBootloader(
                PendingMgsUpdateRotBootloaderDetails {
                    expected_stage0_version: "1.0.0".parse().unwrap(),
                    expected_stage0_next_version:
                        ExpectedVersion::NoValidVersion,
                },
            ),
            artifact_hash: ArtifactHash([72; 32]),
            artifact_version: "2.0.0".parse().unwrap(),
        });
        let blueprint4 = builder.build(BlueprintSource::Test);
        let authz_blueprint4 = authz_blueprint_from_id(blueprint4.id);
        datastore
            .blueprint_insert(&opctx, &blueprint4)
            .await
            .expect("failed to insert blueprint");
        assert_eq!(
            blueprint4,
            datastore
                .blueprint_read(&opctx, &authz_blueprint4)
                .await
                .expect("failed to read collection back")
        );
        let bp4_target = BlueprintTarget {
            target_id: blueprint4.id,
            enabled: true,
            time_made_target: now_db_precision(),
        };
        datastore
            .blueprint_target_set_current(&opctx, bp4_target)
            .await
            .unwrap();
        datastore.blueprint_delete(&opctx, &authz_blueprint3).await.unwrap();
        ensure_blueprint_fully_deleted(&datastore, blueprint3.id).await;

        // We now make sure we can build and insert a blueprint containing a
        // host phase 1 MGS update
        let mut builder = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint4,
            &planning_input,
            &collection,
            "dummy",
            PlannerRng::from_entropy(),
        )
        .expect("failed to create builder");

        // Configure a host phase 1 update
        let (baseboard_id, sp) =
            collection.sps.iter().next().expect("at least one SP");
        builder.pending_mgs_update_insert(PendingMgsUpdate {
            baseboard_id: baseboard_id.clone(),
            sp_type: sp.sp_type,
            slot_id: sp.sp_slot,
            details: PendingMgsUpdateDetails::HostPhase1(
                PendingMgsUpdateHostPhase1Details {
                    expected_active_phase_1_slot: M2Slot::A,
                    expected_boot_disk: M2Slot::B,
                    expected_active_phase_1_hash: ArtifactHash([1; 32]),
                    expected_active_phase_2_hash: ArtifactHash([2; 32]),
                    expected_inactive_phase_1_hash: ArtifactHash([3; 32]),
                    expected_inactive_phase_2_hash: ArtifactHash([4; 32]),
                    sled_agent_address: "[::1]:12345".parse().unwrap(),
                },
            ),
            artifact_hash: ArtifactHash([72; 32]),
            artifact_version: "2.0.0".parse().unwrap(),
        });
        let blueprint5 = builder.build(BlueprintSource::Test);
        let authz_blueprint5 = authz_blueprint_from_id(blueprint5.id);
        datastore
            .blueprint_insert(&opctx, &blueprint5)
            .await
            .expect("failed to insert blueprint");
        assert_eq!(
            blueprint5,
            datastore
                .blueprint_read(&opctx, &authz_blueprint5)
                .await
                .expect("failed to read collection back")
        );
        let bp5_target = BlueprintTarget {
            target_id: blueprint5.id,
            enabled: true,
            time_made_target: now_db_precision(),
        };
        datastore
            .blueprint_target_set_current(&opctx, bp5_target)
            .await
            .unwrap();
        datastore.blueprint_delete(&opctx, &authz_blueprint4).await.unwrap();
        ensure_blueprint_fully_deleted(&datastore, blueprint4.id).await;

        // Now make a new blueprint (with no meaningful changes) to ensure we
        // can delete the last test blueprint we generated above.
        let blueprint6 = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint5,
            &planning_input,
            &collection,
            "dummy",
            PlannerRng::from_entropy(),
        )
        .expect("failed to create builder")
        .build(BlueprintSource::Test);
        datastore
            .blueprint_insert(&opctx, &blueprint6)
            .await
            .expect("failed to insert blueprint");
        let bp6_target = BlueprintTarget {
            target_id: blueprint6.id,
            enabled: true,
            time_made_target: now_db_precision(),
        };
        datastore
            .blueprint_target_set_current(&opctx, bp6_target)
            .await
            .unwrap();
        datastore.blueprint_delete(&opctx, &authz_blueprint5).await.unwrap();
        ensure_blueprint_fully_deleted(&datastore, blueprint5.id).await;

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_set_target() {
        // Setup
        let logctx = dev::test_setup_log("test_set_target");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Trying to insert a target that doesn't reference a blueprint should
        // fail with a relevant error message.
        let nonexistent_blueprint_id = BlueprintUuid::new_v4();
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
            PlannerRng::from_entropy(),
        )
        .expect("failed to create builder")
        .build(BlueprintSource::Test);
        let blueprint3 = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint1,
            &EMPTY_PLANNING_INPUT,
            &collection,
            "test3",
            PlannerRng::from_entropy(),
        )
        .expect("failed to create builder")
        .build(BlueprintSource::Test);
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
            PlannerRng::from_entropy(),
        )
        .expect("failed to create builder")
        .build(BlueprintSource::Test);
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
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_set_target_enabled() {
        // Setup
        let logctx = dev::test_setup_log("test_set_target_enabled");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

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
            PlannerRng::from_entropy(),
        )
        .expect("failed to create builder")
        .build(BlueprintSource::Test);
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
        assert!(
            err.to_string().contains("is not the current target blueprint")
        );

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
        assert!(
            err.to_string().contains("is not the current target blueprint")
        );

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
        db.terminate().await;
        logctx.cleanup_successful();
    }

    async fn create_blueprint_with_external_ip(
        datastore: &DataStore,
        opctx: &OpContext,
    ) -> Blueprint {
        // Create an initial blueprint and a child.
        let sled_id = SledUuid::new_v4();
        let mut blueprint = BlueprintBuilder::build_empty_with_sleds(
            [sled_id].into_iter(),
            "test1",
        );

        // To observe realistic database behavior, we need the invocation of
        // "blueprint_ensure_external_networking_resources" to actually write something
        // back to the database.
        //
        // While this is *mostly* made-up blueprint contents, the part that matters
        // is that it's provisioning a zone (Nexus) which does have resources
        // to be allocated.
        let ip_range = IpRange::try_from((
            Ipv4Addr::new(10, 0, 0, 1),
            Ipv4Addr::new(10, 0, 0, 10),
        ))
        .unwrap();
        let (service_authz_ip_pool, service_ip_pool) = datastore
            .ip_pools_service_lookup(&opctx, IpVersion::V4)
            .await
            .expect("lookup service ip pool");
        datastore
            .ip_pool_add_range(
                &opctx,
                &service_authz_ip_pool,
                &service_ip_pool,
                &ip_range,
            )
            .await
            .expect("add range to service ip pool");
        let zone_id = OmicronZoneUuid::new_v4();
        blueprint.sleds.get_mut(&sled_id).unwrap().zones.insert(
            BlueprintZoneConfig {
                disposition: BlueprintZoneDisposition::InService,
                id: zone_id,
                filesystem_pool: ZpoolName::new_external(ZpoolUuid::new_v4()),
                zone_type: BlueprintZoneType::Nexus(
                    blueprint_zone_type::Nexus {
                        internal_address: SocketAddrV6::new(
                            Ipv6Addr::LOCALHOST,
                            0,
                            0,
                            0,
                        ),
                        lockstep_port: 0,
                        external_ip: OmicronZoneExternalFloatingIp {
                            id: ExternalIpUuid::new_v4(),
                            ip: "10.0.0.1".parse().unwrap(),
                        },
                        nic: NetworkInterface {
                            id: Uuid::new_v4(),
                            kind: NetworkInterfaceKind::Service {
                                id: *zone_id.as_untyped_uuid(),
                            },
                            name: Name::from_str("mynic").unwrap(),
                            ip: "fd77:e9d2:9cd9:2::8".parse().unwrap(),
                            mac: MacAddr::random_system(),
                            subnet: IpNet::host_net(IpAddr::V6(
                                Ipv6Addr::LOCALHOST,
                            )),
                            vni: Vni::random(),
                            primary: true,
                            slot: 1,
                            transit_ips: vec![],
                        },
                        external_tls: false,
                        external_dns_servers: vec![],
                        nexus_generation: Generation::new(),
                    },
                ),
                image_source: BlueprintZoneImageSource::InstallDataset,
            },
        );

        blueprint
    }

    #[tokio::test]
    async fn test_ensure_external_networking_works_with_good_target() {
        // Setup
        let logctx = dev::test_setup_log(
            "test_ensure_external_networking_works_with_good_target",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let blueprint =
            create_blueprint_with_external_ip(&datastore, &opctx).await;
        datastore.blueprint_insert(&opctx, &blueprint).await.unwrap();

        let bp_target = BlueprintTarget {
            target_id: blueprint.id,
            enabled: true,
            time_made_target: now_db_precision(),
        };

        datastore
            .blueprint_target_set_current(&opctx, bp_target)
            .await
            .unwrap();
        datastore
            .blueprint_ensure_external_networking_resources_impl(
                &opctx,
                &blueprint,
                NetworkResourceControlFlow::default(),
            )
            .await
            .expect("Should be able to allocate external network resources");

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_ensure_external_networking_bails_on_bad_target() {
        let test_name = "test_ensure_external_networking_bails_on_bad_target";

        // Setup
        let logctx = dev::test_setup_log(test_name);
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Create two blueprints, both of which have external networking (via 1
        // Nexus zone).
        let (example_system, blueprint1) =
            ExampleSystemBuilder::new(&opctx.log, test_name)
                .nsleds(1)
                .nexus_count(1)
                .internal_dns_count(0)
                .expect("internal DNS count can be 0")
                .external_dns_count(0)
                .expect("external DNS count can be 0")
                .crucible_pantry_count(0)
                .build();
        let blueprint2 = BlueprintBuilder::new_based_on(
            &logctx.log,
            &blueprint1,
            &example_system.input,
            &example_system.collection,
            &format!("{test_name}-2"),
            PlannerRng::from_entropy(),
        )
        .expect("failed to create builder")
        .build(BlueprintSource::Test);

        // Insert an IP pool range covering the one Nexus IP.
        let nexus_ip = blueprint1
            .all_omicron_zones(BlueprintZoneDisposition::is_in_service)
            .find_map(|(_, zone_config)| {
                zone_config
                    .zone_type
                    .external_networking()
                    .map(|(ip, _nic)| ip.ip())
            })
            .expect("found external IP");
        let (service_authz_ip_pool, service_ip_pool) = datastore
            .ip_pools_service_lookup(&opctx, IpVersion::V4)
            .await
            .expect("lookup service ip pool");
        datastore
            .ip_pool_add_range(
                &opctx,
                &service_authz_ip_pool,
                &service_ip_pool,
                &IpRange::try_from((nexus_ip, nexus_ip))
                    .expect("valid IP range"),
            )
            .await
            .expect("add range to service IP pool");

        // Insert both (plus the original parent of blueprint1, internal to
        // `ExampleSystemBuilder`) into the blueprint table.
        let blueprint0 = example_system.initial_blueprint;
        datastore.blueprint_insert(&opctx, &blueprint0).await.unwrap();
        datastore.blueprint_insert(&opctx, &blueprint1).await.unwrap();
        datastore.blueprint_insert(&opctx, &blueprint2).await.unwrap();

        let bp0_target = BlueprintTarget {
            target_id: blueprint0.id,
            enabled: true,
            time_made_target: now_db_precision(),
        };
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

        // Set bp1_target as the current target (which requires making bp0 the
        // target first).
        datastore
            .blueprint_target_set_current(&opctx, bp0_target)
            .await
            .unwrap();
        datastore
            .blueprint_target_set_current(&opctx, bp1_target)
            .await
            .unwrap();

        // Create flags to control method execution.
        let target_check_done = Arc::new(AtomicBool::new(false));
        let should_write_data = Arc::new(AtomicBool::new(false));

        // Spawn a task to execute our method.
        let ensure_resources_task = tokio::spawn({
            let datastore = datastore.clone();
            let opctx =
                OpContext::for_tests(logctx.log.clone(), datastore.clone());
            let target_check_done = target_check_done.clone();
            let should_write_data = should_write_data.clone();
            async move {
                datastore
                    .blueprint_ensure_external_networking_resources_impl(
                        &opctx,
                        &blueprint1,
                        NetworkResourceControlFlow {
                            target_check_done: Some(target_check_done),
                            should_write_data: Some(should_write_data),
                        },
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

        // While the "Ensure resources" task is still mid-transaction:
        //
        // - Update the target
        // - Read the data which "Ensure resources" is attempting to write
        datastore
            .blueprint_target_set_current(&opctx, bp2_target)
            .await
            .unwrap();

        let conn = datastore
            .pool_connection_authorized(&opctx)
            .await
            .expect("failed to get connection");

        // NOTE: Performing this "SELECT" is a necessary step for our test, even
        // though we don't actually care about the result.
        //
        // If we don't perform this read, it is possible for CockroachDB to
        // logically order the "Ensure Resources" task before the blueprint
        // target changes.
        //
        // More on this in the block comment below.
        let mut query = QueryBuilder::new();
        query.sql("SELECT id FROM omicron.public.external_ip WHERE time_deleted IS NULL");
        let _external_ips = query
            .query::<diesel::sql_types::Uuid>()
            .load_async::<uuid::Uuid>(&*conn)
            .await
            .expect("SELECT external IPs");

        // == WHAT ORDERING DO WE EXPECT?
        //
        // We expect to have the following reads/writes:
        //
        // | Ensure Resources | Unit test |
        // | -----------------|-----------|
        // | BEGIN            |           |
        // | R(target)        |           |
        // |                  | W(target) |
        // |                  | R(data)   |
        // | W(data)          |           |
        // | COMMIT           |           |
        //
        // With this ordering, and an eye on "Read-Write", "Write-Read", and
        // "Write-Write" conflicts:
        //
        // - (R->W) "Ensure Resources" must be ordered before "Unit test", because of access to
        // "target".
        // - (R->W) "Unit test" must be ordered before "Ensure Resources", because of
        // access to "data".
        //
        // This creates a circular dependency, and therefore means "Ensure Resources"
        // cannot commit. We expect that this ordering will force CockroachDB
        // to retry the "Ensure Resources" transaction, which will cause it to
        // see the new target:
        //
        // | Ensure Resources | Unit Test |
        // | -----------------|-----------|
        // |                  | W(target) |
        // |                  | R(data)   |
        // | BEGIN            |           |
        // | R(target)        |           |
        //
        // This should cause it to abort the current transaction, as the target no longer matches.
        //
        // == WHY ARE WE DOING THIS?
        //
        // Although CockroachDB's transactions provide serializability, they
        // do not preserve "strict serializability". This means that, as long as we don't violate
        // transaction conflict ordering (aka, the "Read-Write", "Write-Read", and "Write-Write"
        // relationships used earlier), transactions can be re-ordered independently of when
        // they completed in "real time".
        //
        // Although we may want to test the following application-level invariant:
        //
        // > If the target blueprint changes while we're updating network resources, the
        // transaction should abort.
        //
        // We actually need to reframe this statement in terms of concurrency control:
        //
        // > If a transaction attempts to read the current blueprint and update network resources,
        // and concurrently the blueprint changes, the transaction should fail if any other
        // operations have concurrently attempted to read or write network resources.
        //
        // This statement is a bit more elaborate, but it more accurately describes what Cockroach
        // is doing: if the "Ensure Resources" operation COULD have completed before another
        // transaction (e.g., one updating the blueprint target), it is acceptable for the
        // transactions to be logically re-ordered in a different way than they completed in
        // real-time.

        should_write_data.store(true, Ordering::SeqCst);

        // After setting `should_write_data`, `ensure_resources_task` will finish.
        //
        // We expect that it will keep running, but before COMMIT-ing successfully,
        // it should be forced to retry.

        let err = tokio::time::timeout(
            Duration::from_secs(10),
            ensure_resources_task,
        )
        .await
        .expect(
            "time out waiting for \
                `blueprint_ensure_external_networking_resources`",
        )
        .expect("panic in `blueprint_ensure_external_networking_resources")
        .expect_err("Should have failed to ensure resources")
        .to_string();

        assert!(
            err.contains("is not the current target blueprint"),
            "Error: {err}",
        );

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    fn assert_all_zones_in_service(blueprint: &Blueprint) {
        let not_in_service = blueprint
            .all_omicron_zones(|disposition| !disposition.is_in_service())
            .collect::<Vec<_>>();
        assert!(
            not_in_service.is_empty(),
            "expected all zones to be in service, \
             found these zones not in service: {not_in_service:?}"
        );
    }

    /// Counts rows in blueprint-related tables for a specific blueprint ID.
    /// Used by both `ensure_blueprint_fully_populated` and `ensure_blueprint_fully_deleted`.
    struct BlueprintTableCounts {
        counts: BTreeMap<String, i64>,
    }

    impl BlueprintTableCounts {
        /// Create a new BlueprintTableCounts by querying all blueprint tables.
        async fn new(
            datastore: &DataStore,
            blueprint_id: BlueprintUuid,
        ) -> BlueprintTableCounts {
            let conn = datastore.pool_connection_for_tests().await.unwrap();

            macro_rules! query_count {
                ($table:ident, $blueprint_id_col:ident) => {{
                    use nexus_db_schema::schema::$table::dsl;
                    let result = dsl::$table
                        .filter(
                            dsl::$blueprint_id_col
                                .eq(to_db_typed_uuid(blueprint_id)),
                        )
                        .count()
                        .get_result_async(&*conn)
                        .await;
                    (stringify!($table), result)
                }};
            }

            let mut counts = BTreeMap::new();
            for (table_name, result) in [
                query_count!(blueprint, id),
                query_count!(bp_sled_metadata, blueprint_id),
                query_count!(bp_omicron_dataset, blueprint_id),
                query_count!(bp_omicron_physical_disk, blueprint_id),
                query_count!(bp_omicron_zone, blueprint_id),
                query_count!(bp_omicron_zone_nic, blueprint_id),
                query_count!(bp_clickhouse_cluster_config, blueprint_id),
                query_count!(
                    bp_clickhouse_keeper_zone_id_to_node_id,
                    blueprint_id
                ),
                query_count!(
                    bp_clickhouse_server_zone_id_to_node_id,
                    blueprint_id
                ),
                query_count!(bp_oximeter_read_policy, blueprint_id),
                query_count!(bp_pending_mgs_update_sp, blueprint_id),
                query_count!(bp_pending_mgs_update_rot, blueprint_id),
                query_count!(
                    bp_pending_mgs_update_rot_bootloader,
                    blueprint_id
                ),
                query_count!(bp_pending_mgs_update_host_phase_1, blueprint_id),
                query_count!(debug_log_blueprint_planning, blueprint_id),
            ] {
                let count: i64 = result.unwrap();
                counts.insert(table_name.to_string(), count);
            }

            let table_counts = BlueprintTableCounts { counts };

            // Verify no new blueprint tables were added without updating this function
            if let Err(msg) =
                table_counts.verify_all_tables_covered(datastore).await
            {
                panic!("{}", msg);
            }

            table_counts
        }

        /// Returns true if all tables are empty (0 rows).
        fn all_empty(&self) -> bool {
            self.counts.values().all(|&count| count == 0)
        }

        /// Returns a list of table names that are empty.
        fn empty_tables(&self) -> Vec<String> {
            self.counts
                .iter()
                .filter_map(
                    |(table, &count)| {
                        if count == 0 { Some(table.clone()) } else { None }
                    },
                )
                .collect()
        }

        /// Returns a list of table names that are non-empty.
        fn non_empty_tables(&self) -> Vec<String> {
            self.counts
                .iter()
                .filter_map(
                    |(table, &count)| {
                        if count > 0 { Some(table.clone()) } else { None }
                    },
                )
                .collect()
        }

        /// Get all table names that were checked.
        fn tables_checked(&self) -> BTreeSet<&str> {
            self.counts.keys().map(|s| s.as_str()).collect()
        }

        /// Verify no new blueprint tables were added without updating this function.
        async fn verify_all_tables_covered(
            &self,
            datastore: &DataStore,
        ) -> Result<(), String> {
            let conn = datastore.pool_connection_for_tests().await.unwrap();

            // Tables prefixed with `bp_` that are *not* specific to a single blueprint
            // and therefore intentionally ignored.  There is only one of these right now.
            let tables_ignored: BTreeSet<_> =
                ["bp_target"].into_iter().collect();
            let tables_checked = self.tables_checked();

            let mut query = QueryBuilder::new();
            query.sql(
                "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'bp\\_%'",
            );
            let tables_unchecked: Vec<String> = query
                .query::<diesel::sql_types::Text>()
                .load_async(&*conn)
                .await
                .expect("Failed to query information_schema for tables")
                .into_iter()
                .filter(|f: &String| {
                    let t = f.as_str();
                    !tables_ignored.contains(t) && !tables_checked.contains(t)
                })
                .collect();

            if !tables_unchecked.is_empty() {
                Err(format!(
                    "found blueprint-related table(s) not covered by BlueprintTableCounts: {}\n\n\
                    If you see this message, you probably added a blueprint table whose name started with `bp_*`. \
                    Add it to the query list in BlueprintTableCounts::new() so that this function checks the table. \
                    You may also need to update blueprint deletion/insertion code to handle rows in that table.",
                    tables_unchecked.join(", ")
                ))
            } else {
                Ok(())
            }
        }
    }

    // Verify that every blueprint-related table contains ≥1 row for `blueprint_id`.
    // Complements `ensure_blueprint_fully_deleted`.
    async fn ensure_blueprint_fully_populated(
        datastore: &DataStore,
        blueprint_id: BlueprintUuid,
    ) {
        let counts = BlueprintTableCounts::new(datastore, blueprint_id).await;

        // Exception tables that may be empty in the representative blueprint:
        // - MGS update tables: only populated when blueprint includes firmware
        //   updates
        // - ClickHouse tables: only populated when blueprint includes
        //   ClickHouse configuration
        // - debug log for planner reports: only populated when the blueprint
        //   was produced by the planner (test blueprints generally aren't)
        let exception_tables = [
            "bp_pending_mgs_update_sp",
            "bp_pending_mgs_update_rot",
            "bp_pending_mgs_update_rot_bootloader",
            "bp_pending_mgs_update_host_phase_1",
            "bp_clickhouse_cluster_config",
            "bp_clickhouse_keeper_zone_id_to_node_id",
            "bp_clickhouse_server_zone_id_to_node_id",
            "debug_log_blueprint_planning",
        ];

        // Check that all non-exception tables have at least one row
        let empty_tables = counts.empty_tables();
        let problematic_tables: Vec<_> = empty_tables
            .into_iter()
            .filter(|table| !exception_tables.contains(&table.as_str()))
            .collect();

        if !problematic_tables.is_empty() {
            panic!(
                "Expected tables to be populated for blueprint {blueprint_id}: {:?}\n\n\
                If every blueprint should be expected to have a value in this table, then this is a bug. \
                Otherwise, you may need to add a table to the exception list in `ensure_blueprint_fully_populated()`. \
                If you do this, please ensure that you add a test to `test_representative_blueprint()` that creates a \
                blueprint that _does_ populate this table and verifies it.",
                problematic_tables
            );
        }
    }
}
