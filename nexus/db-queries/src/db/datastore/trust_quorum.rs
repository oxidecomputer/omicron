// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Trust quorum related queries

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::TransactionError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_model::DbTrustQuorumConfigurationState;
use nexus_db_model::DbTrustQuorumMemberState;
use nexus_db_model::DbTypedUuid;
use nexus_db_model::HwBaseboardId;
use nexus_db_model::TrustQuorumConfiguration as DbTrustQuorumConfiguration;
use nexus_db_model::TrustQuorumMember as DbTrustQuorumMember;
use nexus_types::trust_quorum::IsLrtqUpgrade;
use nexus_types::trust_quorum::ProposedTrustQuorumConfig;
use nexus_types::trust_quorum::{
    TrustQuorumConfig, TrustQuorumConfigState, TrustQuorumMemberData,
    TrustQuorumMemberState,
};
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::OptionalLookupResult;
use omicron_common::bail_unless;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::RackKind;
use omicron_uuid_kinds::RackUuid;
use rand::rng;
use rand::seq::IteratorRandom;
use sled_hardware_types::BaseboardId;
use std::collections::{BTreeMap, BTreeSet};
use trust_quorum_types::{
    crypto::{EncryptedRackSecrets, Salt, Sha3_256Digest},
    types::{Epoch, Threshold},
};

macro_rules! bail_txn {
    ($err:ident, $($arg:tt),*) => {
        return Err($err.bail(
            omicron_common::api::external::Error::internal_error(&format!(
                $($arg),*
            ))
            .into()
        ));
    }
}

fn i64_to_epoch(val: i64) -> Result<Epoch, Error> {
    let Ok(epoch) = val.try_into() else {
        return Err(Error::internal_error(&format!(
            "Failed to convert i64 from database: {val} \
                into trust quroum epoch",
        )));
    };
    Ok(Epoch(epoch))
}

fn epoch_to_i64(epoch: Epoch) -> Result<i64, Error> {
    epoch.0.try_into().map_err(|_| {
        Error::internal_error(&format!(
            "Failed to convert trust quorum epoch to i64 in attempt to insert \
            into database: {epoch}"
        ))
    })
}

// A configuration returned from `validate_new_config_conn` that is safe to
// be inserted into the database inside a transaction.
struct ValidatedConfig {
    config: TrustQuorumConfig,
    // The last committed configuration should have its state changed from
    // committing to committed-partially.
    commit_partially: bool,
}

impl DataStore {
    /// Return any active `RackUuid` for each trust quorum along with its latest
    /// `Epoch`.
    ///
    /// An active trust quorum configuration is one that has not `Committed`
    /// or `Aborted`, which means that nexus has more work to do on that
    /// configuration.
    ///
    /// For now, since we do not have multirack, and we aren't sure how big
    /// those clusters are going to be we return all values and don't bother
    /// paginating. The current `SQL_BATCH_SIZE` is also 1000, and it's unlikely
    /// that there will ever be more than 1000 racks in a single fleet, sharing
    /// a single CRDB cluster.
    pub async fn tq_get_all_active_rack_id_and_latest_epoch(
        &self,
        opctx: &OpContext,
    ) -> Result<BTreeMap<RackUuid, Epoch>, Error> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        let values: Vec<(DbTypedUuid<RackKind>, i64)> =
            dsl::trust_quorum_configuration
                .filter(dsl::state.eq_any(vec![
                    DbTrustQuorumConfigurationState::Preparing,
                    DbTrustQuorumConfigurationState::PreparingLrtqUpgrade,
                    DbTrustQuorumConfigurationState::Committing,
                ]))
                .select((dsl::rack_id, dsl::epoch))
                .order_by((dsl::rack_id, dsl::epoch.desc()))
                .load_async(conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

        let mut output = BTreeMap::new();
        for (rack_id, epoch) in values {
            output.insert(rack_id.into(), i64_to_epoch(epoch)?);
        }

        Ok(output)
    }

    /// This is a special method called during `rack_initialize` that inserts a
    /// configuration with all nodes acked. It specifically does no validation
    /// and is only expected to be called once during the lifetime of a rack.
    ///
    /// For reconfiguration and lrtq upgrade we always call
    /// `tq_insert_latest_config`.
    pub async fn tq_insert_rss_config_after_handoff(
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
        initial_members: BTreeSet<BaseboardId>,
        coordinator: BaseboardId,
    ) -> Result<(), Error> {
        let authz_rack = authz::Rack::new(
            authz::FLEET,
            rack_id.into_untyped_uuid(),
            LookupType::ById(rack_id.into_untyped_uuid()),
        );
        opctx.authorize(authz::Action::Modify, &authz_rack).await?;

        let initial_config = TrustQuorumConfig::new_rss_committed_config(
            rack_id,
            initial_members,
            coordinator,
        );

        Self::insert_tq_config_conn(opctx, conn, initial_config)
            .await
            .map_err(|err| err.into_public_ignore_retries())
    }

    /// Get the latest trust quorum configuration from the database
    pub async fn tq_get_latest_config(
        &self,
        opctx: &OpContext,
        rack_id: RackUuid,
    ) -> OptionalLookupResult<TrustQuorumConfig> {
        let authz_rack = authz::Rack::new(
            authz::FLEET,
            rack_id.into_untyped_uuid(),
            LookupType::ById(rack_id.into_untyped_uuid()),
        );
        opctx.authorize(authz::Action::Read, &authz_rack).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        Self::tq_get_latest_config_with_members_conn(conn, rack_id)
            .await
            .map_err(|err| err.into_public_ignore_retries())
    }

    pub async fn tq_list_config(
        &self,
        opctx: &OpContext,
        rack_id: RackUuid,
        pagparams: &DataPageParams<'_, i64>,
    ) -> ListResultVec<DbTrustQuorumConfiguration> {
        let authz_rack = authz::Rack::new(
            authz::FLEET,
            rack_id.into_untyped_uuid(),
            LookupType::ById(rack_id.into_untyped_uuid()),
        );
        opctx.authorize(authz::Action::Read, &authz_rack).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::trust_quorum_configuration::dsl;
        paginated(dsl::trust_quorum_configuration, dsl::epoch, pagparams)
            .filter(dsl::rack_id.eq(DbTypedUuid::<RackKind>::from(rack_id)))
            .select(DbTrustQuorumConfiguration::as_select())
            .load_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Get the trust quorum configuration from the database for the given Epoch
    pub async fn tq_get_config(
        &self,
        opctx: &OpContext,
        rack_id: RackUuid,
        epoch: Epoch,
    ) -> OptionalLookupResult<TrustQuorumConfig> {
        let authz_rack = authz::Rack::new(
            authz::FLEET,
            rack_id.into_untyped_uuid(),
            LookupType::ById(rack_id.into_untyped_uuid()),
        );
        opctx.authorize(authz::Action::Read, &authz_rack).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        Self::tq_get_config_with_members_from_epoch_conn(conn, rack_id, epoch)
            .await
            .map_err(|err| err.into_public_ignore_retries())
    }

    async fn tq_get_latest_config_with_members_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
    ) -> Result<Option<TrustQuorumConfig>, TransactionError<Error>> {
        // First, retrieve our configuration if there is one.
        let Some(latest) =
            Self::tq_get_latest_config_conn(conn, rack_id).await?
        else {
            return Ok(None);
        };

        // Then fill in our members
        Self::tq_get_config_with_members_conn(conn, rack_id, latest).await
    }

    async fn tq_get_config_with_members_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
        config: DbTrustQuorumConfiguration,
    ) -> Result<Option<TrustQuorumConfig>, TransactionError<Error>> {
        // Then get any members associated with the configuration
        let members =
            Self::tq_get_members_conn(conn, rack_id, config.epoch).await?;

        let mut tq_members: BTreeMap<BaseboardId, TrustQuorumMemberData> =
            BTreeMap::new();
        let mut coordinator: Option<BaseboardId> = None;
        for (member, hw_baseboard_id) in members {
            let digest = if let Some(digest_str) = member.share_digest {
                let mut data = [0u8; 32];
                hex::decode_to_slice(&digest_str, &mut data).map_err(|e| {
                    Error::internal_error(&format!(
                        "Failed to decode share digest for trust quorum member \
                        {}:{} : {e}",
                        hw_baseboard_id.part_number,
                        hw_baseboard_id.serial_number
                    ))
                })?;
                Some(Sha3_256Digest(data))
            } else {
                None
            };

            // The coordinator is always a member of the group
            // We pull out its `BaseboardId` here.
            if config.coordinator == hw_baseboard_id.id {
                coordinator = Some(hw_baseboard_id.clone().into());
            }
            tq_members.insert(
                hw_baseboard_id.into(),
                TrustQuorumMemberData {
                    state: member.state.into(),
                    share_digest: digest,
                    time_prepared: member.time_prepared,
                    time_committed: member.time_committed,
                },
            );
        }

        let salt = if let Some(salt_str) = config.encrypted_rack_secrets_salt {
            let mut data = [0u8; 32];
            hex::decode_to_slice(&salt_str, &mut data).map_err(|e| {
                Error::internal_error(&format!(
                    "Failed to decode salt for trust quorum config: \
                    rack_id: {}, epoch: {}: {e}",
                    config.rack_id, config.epoch
                ))
            })?;
            Some(Salt(data))
        } else {
            None
        };

        let encrypted_rack_secrets = if let Some(salt) = salt {
            let Some(secrets) = config.encrypted_rack_secrets else {
                // This should never happend due to constraint checks
                return Err(Error::internal_error(&format!(
                    "Salt exists, but secrets do not for trust quorum config: \
                    rack_id: {}, epoch: {}",
                    config.rack_id, config.epoch
                ))
                .into());
            };
            Some(EncryptedRackSecrets::new(salt, secrets.into_boxed_slice()))
        } else {
            None
        };

        let Some(coordinator) = coordinator else {
            return Err(Error::internal_error(&format!(
                "Failed to find coordinator for hw_baseboard_id: {} \
                in trust quorum config.",
                config.coordinator
            ))
            .into());
        };

        let last_committed_epoch =
            if let Some(epoch) = config.last_committed_epoch {
                Some(i64_to_epoch(epoch)?)
            } else {
                None
            };

        Ok(Some(TrustQuorumConfig {
            rack_id: config.rack_id.into(),
            epoch: i64_to_epoch(config.epoch)?,
            last_committed_epoch,
            state: config.state.into(),
            threshold: Threshold(config.threshold.into()),
            commit_crash_tolerance: config.commit_crash_tolerance.into(),
            coordinator,
            encrypted_rack_secrets,
            members: tq_members,
            time_created: config.time_created,
            time_committing: config.time_committing,
            time_committed: config.time_committed,
            time_aborted: config.time_aborted,
            abort_reason: config.abort_reason,
        }))
    }

    async fn tq_get_config_with_members_from_epoch_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
        epoch: Epoch,
    ) -> Result<Option<TrustQuorumConfig>, TransactionError<Error>> {
        // First, retrieve our configuration if there is one.
        let Some(latest) =
            Self::tq_get_config_conn(conn, rack_id, epoch).await?
        else {
            return Ok(None);
        };

        // Then fill in our members
        Self::tq_get_config_with_members_conn(conn, rack_id, latest).await
    }

    /// Insert a new trust quorum configuration, if the proposed configuration
    /// validates.
    pub async fn tq_insert_latest_config(
        &self,
        opctx: &OpContext,
        proposed: ProposedTrustQuorumConfig,
    ) -> Result<(), Error> {
        let authz_rack = authz::Rack::new(
            authz::FLEET,
            proposed.rack_id.into_untyped_uuid(),
            LookupType::ById(proposed.rack_id.into_untyped_uuid()),
        );
        opctx.authorize(authz::Action::Modify, &authz_rack).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        let err = OptionalError::new();

        self.transaction_retry_wrapper("tq_insert_latest_config")
            .transaction(&conn, |c| {
                let err = err.clone();
                let proposed = proposed.clone();

                async move {
                    let validated =
                        Self::validate_new_config_conn(&c, proposed)
                            .await
                            .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    // Mark the old configuration as committed-partially
                    //
                    // Safe to unwrap because we validated that there is a last
                    // committed configuration and that it needs to be committed
                    // partially.
                    if validated.commit_partially {
                        Self::updated_tq_state_committed_partially_conn(
                            &c,
                            validated.config.rack_id,
                            validated.config.last_committed_epoch.unwrap(),
                        )
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))?;
                    }

                    // Save the new config
                    Self::insert_tq_config_conn(opctx, &c, validated.config)
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))
                }
            })
            .await
            .map_err(|e| match err.take() {
                Some(err) => err.into_public_ignore_retries(),
                None => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    async fn validate_new_config_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        proposed: ProposedTrustQuorumConfig,
    ) -> Result<ValidatedConfig, TransactionError<Error>> {
        //
        // Check validity of members in proposed config
        //

        // Check membership size
        bail_unless!(
            proposed.members.len() >= 3 && proposed.members.len() <= 32,
            "Invalid Trust Quorum membership size ({}): \
            must be between 3 and 32 nodes",
            proposed.members.len()
        );

        // Ensure all baseboards actually exist
        let hw_baseboard_ids = Self::lookup_hw_baseboard_ids_conn(
            conn,
            proposed.members.iter().cloned(),
        )
        .await?;

        bail_unless!(
            hw_baseboard_ids.len() == proposed.members.len(),
            "Trust Quorum proposed configuration missing hw_baseboard_ids \
            in database. Do all physical sleds exist in this rack?"
        );

        // Check that if a sled is commissioned with a matching baseboard that
        // it's for this `rack_id`.
        for member in &proposed.members {
            if let Some(db_rack_id) =
                Self::sled_get_rack_id_if_commissioned_conn(conn, member)
                    .await?
            {
                bail_unless!(
                    proposed.rack_id == db_rack_id,
                    "Sled {} already commissioned for another rack. \
                    Expected rack_id {}, Got {}",
                    member,
                    proposed.rack_id,
                    db_rack_id
                );
            }
        }

        //
        // Check that there is not an in progress reconfiguration:
        //
        let latest_config = Self::tq_get_latest_config_with_members_conn(
            conn,
            proposed.rack_id,
        )
        .await?;

        // Ensure that epochs are sequential or this is the inital attempt at an
        // LRTQ upgrade.
        //
        // In the latter case the proposed epoch will be 2, as LRTQ has an epoch
        // of 1 that is encoded as a ZFS dataset property.
        let latest_epoch = latest_config.as_ref().map(|c| c.epoch);
        bail_unless!(
            latest_epoch == proposed.epoch.previous()
                || (latest_epoch.is_none()
                    && proposed.is_lrtq_upgrade == IsLrtqUpgrade::Yes
                    && proposed.epoch == Epoch(2)),
            "Epochs for trust quorum configurations must be sequential. \
            Current epoch = {:?}, Proposed Epoch = {:?}",
            latest_epoch,
            proposed.epoch
        );

        // Helper variable to operate on the last committed config, no matter
        // how we find it.
        let mut last_committed_config: Option<TrustQuorumConfig> = None;

        // Is there actually a latest configuration?
        if let Some(latest_config) = latest_config {
            // If the configuration is being prepared then it must be aborted
            // before a reconfiguration can occur.
            bail_unless!(
                !latest_config.state.is_preparing(),
                "Trust Quorum reconfiguration in progress: still preparing \
                in epoch {}. Configuration must be aborted to reconfigure.",
                latest_config.epoch
            );

            // If the latest configuration is at least committing, then the
            // proposed configuration must have the last committed configuration
            // set to match this.
            if latest_config.state.is_committed()
                || latest_config.state.is_committing()
            {
                bail_unless!(
                    Some(latest_config.epoch)
                        == proposed.last_committed_epoch(),
                    "Trust Quorum proposed configuration does not match \
                    the last committed epoch. Committed configurations must \
                    build upon each other sequentially. Expected epoch \
                    {}, Got {:?}",
                    latest_config.epoch,
                    proposed.last_committed_epoch()
                );

                last_committed_config = Some(latest_config);
            } else if latest_config.state.is_aborted() {
                // Inductively,the latest configuration should not have
                // been written to the database unless its last committed
                // configuration was valid.
                bail_unless!(
                    latest_config.last_committed_epoch
                        == proposed.last_committed_epoch(),
                    "Trust quorum proposed configuration has a last committed \
                    epoch that does not match the last committed epoch of the \
                    latest (aborted) configuration. \
                    Proposed = {:?}, Actual = {:?}",
                    proposed.last_committed_epoch(),
                    latest_config.last_committed_epoch
                );
            }
        }

        // The latest config was aborted or there wasn't one
        if last_committed_config.is_none() {
            // Do we need to load the last committed config?
            if let Some(epoch) = proposed.last_committed_epoch() {
                let config = Self::tq_get_config_with_members_from_epoch_conn(
                    conn,
                    proposed.rack_id,
                    epoch,
                )
                .await?;

                bail_unless!(
                    config.is_some(),
                    "Trust Quorum proposed configuration contains a last \
                    committed epoch ({}) for a configuration that does \
                    not exist.",
                    epoch
                );

                last_committed_config = config;
            }
        }

        // The last committed epoch may not have received acked commits from
        // all nodes, but it may have received enough nodes to allow a new
        // configuration to be installed. If the latter is true then we must
        // mark the old configuration `CommittedPartially` in the DB.
        let mut should_commit_partially = false;

        // At this point we have either loaded the proposed last committed
        // configuration or there is not one.
        let possible_coordinators: BTreeSet<BaseboardId> =
            if let Some(last_committed_config) = last_committed_config {
                bail_unless!(
                    last_committed_config.state.is_committed()
                        || last_committed_config.state.is_committing(),
                    "Trust Quorum proposed configuration contains a last \
                committed epoch ({}) for a configuration that is not \
                committed",
                    last_committed_config.epoch
                );

                if last_committed_config.state.is_committing() {
                    let acked = last_committed_config.acked_commits();
                    // N - Z
                    let min = last_committed_config.members.len()
                        - last_committed_config.commit_crash_tolerance as usize;
                    bail_unless!(
                        acked >= min,
                        "Trust quorum reconfiguration in progress: not enough \
                         nodes have acked commit in epoch {}. Expected at \
                         least {} acks, Got {} acks. If this persists please \
                         contact Oxide support.",
                        last_committed_config.epoch,
                        min,
                        acked
                    );
                    should_commit_partially = true;
                }

                // A coordinator must have acked a commit in the prior configuration
                // if it's to be a candidate.
                let committed_members: BTreeSet<BaseboardId> =
                    last_committed_config
                        .members
                        .iter()
                        .filter_map(|(id, data)| {
                            if data.state == TrustQuorumMemberState::Committed {
                                Some(id.clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                proposed
                    .members
                    .intersection(&committed_members)
                    .cloned()
                    .collect()
            } else {
                proposed.members.clone()
            };

        bail_unless!(
            !possible_coordinators.is_empty(),
            "Trust Quorum proposed configuration has no possible coordinators \
            for epoch {}",
            proposed.epoch
        );

        // Pick a random coordinator from our set of possibilities.
        //
        // Safe to unwrap, as we check for emptiness above
        let coordinator =
            possible_coordinators.into_iter().choose(&mut rng()).unwrap();

        // Convert proposal to a new `TrustQuorumConfig`
        Ok(ValidatedConfig {
            config: TrustQuorumConfig::new(proposed, coordinator),
            commit_partially: should_commit_partially,
        })
    }

    /// If this configuration is in the `Preparing` state, then update any
    /// members to acknowledge the prepare.
    ///
    /// Also, update any digests or encrypted rack secrets if necessary.
    /// Lastly, if enough members have acked prepares then commit the configuration.
    pub async fn tq_update_prepare_status(
        &self,
        opctx: &OpContext,
        config: trust_quorum_types::configuration::Configuration,
        acked_prepares: BTreeSet<BaseboardId>,
    ) -> Result<TrustQuorumConfigState, Error> {
        let authz_rack = authz::Rack::new(
            authz::FLEET,
            config.rack_id.into_untyped_uuid(),
            LookupType::ById(config.rack_id.into_untyped_uuid()),
        );
        opctx.authorize(authz::Action::Modify, &authz_rack).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        let epoch = epoch_to_i64(config.epoch)?;
        let rack_id = config.rack_id;

        let err = OptionalError::new();

        self.transaction_retry_wrapper("tq_update_prepare_status")
            .transaction(&conn, |c| {
                let err = err.clone();
                let config = config.clone();
                let acked_prepares = acked_prepares.clone();
                async move {
                    // First, retrieve our configuration if there is one.
                    let latest = Self::tq_get_latest_config_conn(&c, rack_id)
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    let Some(db_config) = latest else {
                        bail_txn!(
                            err,
                            "No trust quorum config for rack_id {} at epoch {}",
                            rack_id,
                            epoch
                        );
                    };

                    if db_config.epoch != epoch {
                        let actual = db_config.epoch;
                        bail_txn!(
                            err,
                            "Cannot update trust quorum config. \
                             Latest epoch does not match. Proposed {}, Got {}",
                            epoch,
                            actual
                        );
                    }

                    // If we aren't preparing, then ignore this call. Multiple
                    // Nexuses race to completion and we don't want to worry
                    // about overwriting commits with prepares in the `state`
                    // field of each member.
                    if db_config.state !=
                        DbTrustQuorumConfigurationState::Preparing
                      && db_config.state
                        != DbTrustQuorumConfigurationState::PreparingLrtqUpgrade
                  {
                        let state = db_config.state;
                        bail_txn!(
                            err,
                            "Ignoring stale update of trust quorum prepare \
                            status. Expected state = preparing || \
                            preparing-lrtq-upgrade, Got {:?}",
                            state
                        );
                    }

                    // Then get any members associated with the configuration
                    let db_members =
                        Self::tq_get_members_conn(&c, rack_id, db_config.epoch)
                            .await
                            .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    let mut total_acks = 0;
                    for (mut member, hw_id) in db_members {
                        let baseboard_id: BaseboardId = hw_id.into();

                        // Set the share digest for the member if we just learned it
                        if member.share_digest.is_none() {
                            let Some(digest) =
                                config.members.get(&baseboard_id)
                            else {
                                bail_txn!(
                                    err,
                                    "Cannot update share digest for {}. Not a \
                                    member of the trust quorum configuration.",
                                    baseboard_id
                                );
                            };
                            member.share_digest = Some(hex::encode(digest.0));
                            Self::update_tq_member_share_digest_conn(
                                &c,
                                member.clone(),
                            )
                            .await
                            .map_err(|txn_error| txn_error.into_diesel(&err))?;
                        }

                        // Set the state of this member
                        if acked_prepares.contains(&baseboard_id)
                            && member.state == DbTrustQuorumMemberState::Unacked
                        {
                            total_acks += 1;
                            Self::update_tq_member_state_prepared_conn(
                                &c,
                                member.clone(),
                            )
                            .await
                            .map_err(|txn_error| txn_error.into_diesel(&err))?;
                        }

                        if member.state == DbTrustQuorumMemberState::Prepared {
                            total_acks += 1;
                        }
                    }

                    // We only update the configuration in the database if:
                    //  1. This is the first time we have seen encrypted rack secrets
                    //  2. We are transitioning from preparing to committed state.

                    // Should we write secrets?
                    if db_config.encrypted_rack_secrets_salt.is_none()
                        && config.encrypted_rack_secrets.is_some()
                    {
                        Self::update_tq_encrypted_rack_secrets_conn(
                            &c,
                            db_config.rack_id,
                            db_config.epoch,
                            config.encrypted_rack_secrets.unwrap(),
                        )
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))?;
                    }

                    // Do we have enough acks to commit?
                    if total_acks
                        >= (db_config.threshold.0
                            + db_config.commit_crash_tolerance.0)
                            as usize
                    {
                        Self::update_tq_state_committing_conn(
                            &c,
                            db_config.rack_id,
                            db_config.epoch,
                        )
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))?;

                        return Ok(TrustQuorumConfigState::Committing);
                    }

                    Ok(db_config.state.into())
                }
            })
            .await
            .map_err(|e| match err.take() {
                Some(err) => err.into_public_ignore_retries(),
                None => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    /// If this configuration is in the `Committing` state, then update any
    /// members to acknowledge their commit acknowledgements.
    pub async fn tq_update_commit_status(
        &self,
        opctx: &OpContext,
        rack_id: RackUuid,
        epoch: Epoch,
        acked_commits: BTreeSet<BaseboardId>,
    ) -> Result<TrustQuorumConfigState, Error> {
        let authz_rack = authz::Rack::new(
            authz::FLEET,
            rack_id.into_untyped_uuid(),
            LookupType::ById(rack_id.into_untyped_uuid()),
        );
        opctx.authorize(authz::Action::Modify, &authz_rack).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        let epoch = epoch_to_i64(epoch)?;

        let err = OptionalError::new();

        self.transaction_retry_wrapper("tq_update_commit_status")
            .transaction(&conn, |c| {
                let err = err.clone();
                let acked_commits = acked_commits.clone();
                async move {
                    // First, retrieve our configuration if there is one.
                    let latest = Self::tq_get_latest_config_conn(&c, rack_id)
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    let Some(db_config) = latest else {
                        bail_txn!(
                            err,
                            "No trust quorum config for rack_id {} at epoch {}",
                            rack_id,
                            epoch
                        );
                    };

                    if db_config.epoch != epoch {
                        let actual = db_config.epoch;
                        bail_txn!(
                            err,
                            "Cannot update trust quorum config. \
                             Latest epoch does not match. Expected {}, Got {}",
                            epoch,
                            actual
                        );
                    }

                    // Nexus should not be retrieving committed acks if the
                    // configuration is not `Committing`.
                    if db_config.state
                        != DbTrustQuorumConfigurationState::Committing
                    {
                        let state = db_config.state;
                        bail_txn!(
                            err,
                            "Invalid update of trust quorum commit status. \
                            Expected `Committing`, got {:?}",
                            state
                        );
                    }

                    Self::update_tq_members_state_commit_conn(
                        &c,
                        rack_id.into(),
                        epoch,
                        acked_commits,
                    )
                    .await
                    .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    // Then get any members associated with the configuration
                    let db_members =
                        Self::tq_get_members_conn(&c, rack_id, db_config.epoch)
                            .await
                            .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    // If all members have acked their commits then mark the
                    // configuration as committed.
                    if db_members.iter().all(|(m, _)| {
                        m.state == DbTrustQuorumMemberState::Committed
                    }) {
                        Self::update_tq_state_committed_conn(
                            &c,
                            db_config.rack_id,
                            db_config.epoch,
                        )
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))?;

                        return Ok(TrustQuorumConfigState::Committed);
                    }

                    Ok(TrustQuorumConfigState::Committing)
                }
            })
            .await
            .map_err(|e| match err.take() {
                Some(err) => err.into_public_ignore_retries(),
                None => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    /// Abort the configuration for a trust quorum if `epoch` is the latest per `rack_id`
    /// and the configuration has not been committed.
    ///
    /// This operation returns `Ok(())` if the configuration has already been
    /// aborted and it is still the latest configuration.
    pub async fn tq_abort_config(
        &self,
        opctx: &OpContext,
        rack_id: RackUuid,
        epoch: Epoch,
        abort_reason: String,
    ) -> Result<(), Error> {
        let authz_rack = authz::Rack::new(
            authz::FLEET,
            rack_id.into_untyped_uuid(),
            LookupType::ById(rack_id.into_untyped_uuid()),
        );
        opctx.authorize(authz::Action::Modify, &authz_rack).await?;
        let conn = &*self.pool_connection_authorized(opctx).await?;

        let epoch = epoch_to_i64(epoch)?;

        let err = OptionalError::new();

        self.transaction_retry_wrapper("tq_abort_config")
            .transaction(&conn, |c| {
                let err = err.clone();
                let abort_reason = abort_reason.clone();
                async move {
                    // First, retrieve our configuration if there is one.
                    let latest = Self::tq_get_latest_config_conn(&c, rack_id)
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    let Some(db_config) = latest else {
                        bail_txn!(
                            err,
                            "No trust quorum config for rack_id {} at epoch {}",
                            rack_id,
                            epoch
                        );
                    };

                    if db_config.epoch != epoch {
                        let actual = db_config.epoch;
                        bail_txn!(
                            err,
                            "Cannot abort trust quorum config. \
                             Latest epoch does not match. Expected {}, Got {}",
                            epoch,
                            actual
                        );
                    }

                    if db_config.state
                        == DbTrustQuorumConfigurationState::Aborted
                    {
                        // Abort is idempotent
                        return Ok(());
                    }

                    // If we've already started committing , we can't abort
                    if db_config.state
                        == DbTrustQuorumConfigurationState::Committing
                        || db_config.state
                            == DbTrustQuorumConfigurationState::Committed
                    {
                        let state = db_config.state;
                        bail_txn!(
                            err,
                            "Invalid update of trust quorum abort status. \
                            Expected `Preparing`, got `{:?}`",
                            state
                        );
                    }

                    Self::update_tq_abort_state_conn(
                        &c,
                        db_config.rack_id,
                        db_config.epoch,
                        abort_reason,
                    )
                    .await
                    .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    Ok(())
                }
            })
            .await
            .map_err(|e| match err.take() {
                Some(err) => err.into_public_ignore_retries(),
                None => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    // Unconditional insert
    async fn insert_tq_config_conn(
        opctx: &OpContext,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        config: TrustQuorumConfig,
    ) -> Result<(), TransactionError<Error>> {
        let authz_rack = authz::Rack::new(
            authz::FLEET,
            config.rack_id.into_untyped_uuid(),
            LookupType::ById(config.rack_id.into_untyped_uuid()),
        );
        opctx.authorize(authz::Action::Modify, &authz_rack).await?;

        let hw_baseboard_ids = Self::lookup_hw_baseboard_ids_conn(
            conn,
            config.members.keys().cloned(),
        )
        .await?;

        bail_unless!(
            hw_baseboard_ids.len() == config.members.len(),
            "Failed to find all hw_baseboard_ids for trust quorum members \
            of rack_id {}, epoch = {}",
            config.rack_id,
            config.epoch
        );

        let (salt, secrets) =
            config.encrypted_rack_secrets.map_or((None, None), |s| {
                (Some(hex::encode(s.salt.0)), Some(s.data.into()))
            });

        // Max of 32 members to search. We could use binary search if we sorted
        // the output with an `order_by` in the DB query, or speed up search
        // if converted to a map. Neither seems necessary for such a rare
        // operation.
        let coordinator_id = hw_baseboard_ids.iter().find(|m| {
            m.part_number == config.coordinator.part_number
                && m.serial_number == config.coordinator.serial_number
        });
        bail_unless!(
            coordinator_id.is_some(),
            "Coordinator: {} is not a member of the trust quorum",
            config.coordinator
        );
        let coordinator_id = coordinator_id.unwrap().id;

        let epoch = epoch_to_i64(config.epoch)
            .map_err(|e| TransactionError::from(e))?;

        let last_committed_epoch =
            if let Some(lc_epoch) = config.last_committed_epoch {
                Some(
                    epoch_to_i64(lc_epoch)
                        .map_err(|e| TransactionError::from(e))?,
                )
            } else {
                None
            };

        // Insert the configuration
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;
        let rows_updated = diesel::insert_into(dsl::trust_quorum_configuration)
            .values(DbTrustQuorumConfiguration {
                rack_id: config.rack_id.into(),
                epoch,
                last_committed_epoch,
                state: config.state.into(),
                threshold: config.threshold.0.into(),
                commit_crash_tolerance: config.commit_crash_tolerance.into(),
                coordinator: coordinator_id,
                encrypted_rack_secrets_salt: salt,
                encrypted_rack_secrets: secrets,
                time_created: config.time_created,
                time_committing: config.time_committing,
                time_committed: config.time_committed,
                time_aborted: config.time_aborted,
                abort_reason: config.abort_reason.clone(),
            })
            .execute_async(conn)
            .await?;

        bail_unless!(
            rows_updated == 1,
            "Failed to insert trust quorum config for rack_id {}, epoch {}",
            config.rack_id,
            epoch
        );

        // Insert the members
        let members: Vec<_> = hw_baseboard_ids
            .iter()
            .zip(config.members.values())
            .map(|(hw_baseboard_id, m)| DbTrustQuorumMember {
                rack_id: config.rack_id.into(),
                epoch,
                hw_baseboard_id: hw_baseboard_id.id,
                state: m.state.into(),
                share_digest: m.share_digest.map(|d| hex::encode(d.0)),
                time_prepared: m.time_prepared,
                time_committed: m.time_committed,
            })
            .collect();

        let num_members = members.len();

        use nexus_db_schema::schema::trust_quorum_member::dsl as members_dsl;
        let rows_updated =
            diesel::insert_into(members_dsl::trust_quorum_member)
                .values(members)
                .execute_async(conn)
                .await?;

        bail_unless!(
            rows_updated == num_members,
            "Failed to insert all trust quorum members for config for \
            rack_id {}, epoch {}",
            config.rack_id,
            epoch
        );

        Ok(())
    }

    async fn update_tq_members_state_commit_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: DbTypedUuid<RackKind>,
        epoch: i64,
        acked_commits: BTreeSet<BaseboardId>,
    ) -> Result<(), TransactionError<Error>> {
        use nexus_db_schema::schema::trust_quorum_member::dsl;

        let hw_baseboard_ids: Vec<_> =
            Self::lookup_hw_baseboard_ids_conn(conn, acked_commits.into_iter())
                .await?
                .into_iter()
                .map(|hw| hw.id)
                .collect();

        let num_members = hw_baseboard_ids.len();

        let rows_updated = diesel::update(dsl::trust_quorum_member)
            .filter(dsl::rack_id.eq(rack_id))
            .filter(dsl::epoch.eq(epoch))
            .filter(dsl::hw_baseboard_id.eq_any(hw_baseboard_ids))
            .filter(dsl::share_digest.is_not_null())
            .filter(dsl::state.eq_any(vec![
                DbTrustQuorumMemberState::Unacked,
                DbTrustQuorumMemberState::Prepared,
            ]))
            .set((
                dsl::state.eq(DbTrustQuorumMemberState::Committed),
                dsl::time_committed.eq(Some(Utc::now())),
            ))
            .execute_async(conn)
            .await?;

        bail_unless!(
            rows_updated == num_members,
            "Failed to set commit state for all acked trust quorum members for \
            config for rack_id {}, epoch {}",
            rack_id,
            epoch
        );

        Ok(())
    }

    async fn update_tq_member_share_digest_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        member: DbTrustQuorumMember,
    ) -> Result<(), TransactionError<Error>> {
        use nexus_db_schema::schema::trust_quorum_member::dsl;

        let rows_updated = diesel::update(dsl::trust_quorum_member)
            .filter(dsl::rack_id.eq(member.rack_id))
            .filter(dsl::epoch.eq(member.epoch))
            .filter(dsl::hw_baseboard_id.eq(member.hw_baseboard_id))
            .filter(dsl::share_digest.is_null())
            .filter(dsl::state.eq(DbTrustQuorumMemberState::Unacked))
            .set(dsl::share_digest.eq(member.share_digest))
            .execute_async(conn)
            .await?;

        bail_unless!(
            rows_updated == 1,
            "Failed to set share digest for trust quorum member {} for \
            config for rack_id {}, epoch {}",
            member.hw_baseboard_id,
            member.rack_id,
            member.epoch
        );

        Ok(())
    }

    async fn update_tq_member_state_prepared_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        member: DbTrustQuorumMember,
    ) -> Result<(), TransactionError<Error>> {
        use nexus_db_schema::schema::trust_quorum_member::dsl;

        let rows_updated = diesel::update(dsl::trust_quorum_member)
            .filter(dsl::rack_id.eq(member.rack_id))
            .filter(dsl::epoch.eq(member.epoch))
            .filter(dsl::hw_baseboard_id.eq(member.hw_baseboard_id))
            .filter(dsl::share_digest.is_not_null())
            .filter(dsl::state.eq(DbTrustQuorumMemberState::Unacked))
            .set((
                dsl::state.eq(DbTrustQuorumMemberState::Prepared),
                dsl::time_prepared.eq(Some(Utc::now())),
            ))
            .execute_async(conn)
            .await?;

        bail_unless!(
            rows_updated == 1,
            "Failed to update trust quorum member state to prepared for \
            member {}, rack_id {}, epoch {}",
            member.hw_baseboard_id,
            member.rack_id,
            member.epoch
        );

        Ok(())
    }

    async fn update_tq_encrypted_rack_secrets_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: DbTypedUuid<RackKind>,
        epoch: i64,
        encrypted_rack_secrets: EncryptedRackSecrets,
    ) -> Result<(), TransactionError<Error>> {
        let salt = Some(hex::encode(encrypted_rack_secrets.salt.0));
        let secrets: Option<Vec<u8>> = Some(encrypted_rack_secrets.data.into());

        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        let rows_updated = diesel::update(dsl::trust_quorum_configuration)
            .filter(dsl::rack_id.eq(rack_id))
            .filter(dsl::epoch.eq(epoch))
            .filter(dsl::encrypted_rack_secrets_salt.is_null())
            .filter(dsl::encrypted_rack_secrets.is_null())
            .set((
                dsl::encrypted_rack_secrets_salt.eq(salt),
                dsl::encrypted_rack_secrets.eq(secrets),
            ))
            .execute_async(conn)
            .await?;

        bail_unless!(
            rows_updated == 1,
            "Failed to set encrypted rack secrets in trust quorum config for \
            rack_id {}, epoch {}",
            rack_id,
            epoch
        );

        Ok(())
    }

    /// Returns the number of rows update
    async fn update_tq_state_committing_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: DbTypedUuid<RackKind>,
        epoch: i64,
    ) -> Result<(), TransactionError<Error>> {
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        let rows_updated = diesel::update(dsl::trust_quorum_configuration)
            .filter(dsl::rack_id.eq(rack_id))
            .filter(dsl::epoch.eq(epoch))
            .filter(dsl::state.eq_any(vec![
                DbTrustQuorumConfigurationState::Preparing,
                DbTrustQuorumConfigurationState::PreparingLrtqUpgrade,
            ]))
            .set((
                dsl::state.eq(DbTrustQuorumConfigurationState::Committing),
                dsl::time_committing.eq(Some(Utc::now())),
            ))
            .execute_async(conn)
            .await?;

        bail_unless!(
            rows_updated == 1,
            "Failed to update trust quorum config state to committing for \
            rack_id {}, epoch {}",
            rack_id,
            epoch
        );

        Ok(())
    }

    /// Returns the number of rows update
    async fn update_tq_state_committed_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: DbTypedUuid<RackKind>,
        epoch: i64,
    ) -> Result<(), TransactionError<Error>> {
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        let rows_updated = diesel::update(dsl::trust_quorum_configuration)
            .filter(dsl::rack_id.eq(rack_id))
            .filter(dsl::epoch.eq(epoch))
            .filter(dsl::state.eq(DbTrustQuorumConfigurationState::Committing))
            .set((
                dsl::state.eq(DbTrustQuorumConfigurationState::Committed),
                dsl::time_committed.eq(Some(Utc::now())),
            ))
            .execute_async(conn)
            .await?;

        bail_unless!(
            rows_updated == 1,
            "Failed to update trust quorum config state to committed for \
            rack_id {}, epoch {}",
            rack_id,
            epoch
        );

        Ok(())
    }

    async fn updated_tq_state_committed_partially_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
        epoch: Epoch,
    ) -> Result<(), TransactionError<Error>> {
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        let epoch =
            epoch_to_i64(epoch).map_err(|e| TransactionError::from(e))?;

        let rows_updated = diesel::update(dsl::trust_quorum_configuration)
            .filter(dsl::rack_id.eq(DbTypedUuid::<RackKind>::from(rack_id)))
            .filter(dsl::epoch.eq(epoch))
            .filter(dsl::state.eq(DbTrustQuorumConfigurationState::Committing))
            .set((
                dsl::state
                    .eq(DbTrustQuorumConfigurationState::CommittedPartially),
                dsl::time_committed.eq(Some(Utc::now())),
            ))
            .execute_async(conn)
            .await?;

        bail_unless!(
            rows_updated == 1,
            "Failed to update trust quorum config state to committed-partially \
            for rack_id {}, epoch {}",
            rack_id,
            epoch
        );

        Ok(())
    }

    async fn update_tq_abort_state_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: DbTypedUuid<RackKind>,
        epoch: i64,
        abort_reason: String,
    ) -> Result<(), TransactionError<Error>> {
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        let rows_updated = diesel::update(dsl::trust_quorum_configuration)
            .filter(dsl::rack_id.eq(rack_id))
            .filter(dsl::epoch.eq(epoch))
            .filter(dsl::state.eq_any([
                DbTrustQuorumConfigurationState::Preparing,
                DbTrustQuorumConfigurationState::PreparingLrtqUpgrade,
            ]))
            .set((
                dsl::state.eq(DbTrustQuorumConfigurationState::Aborted),
                dsl::time_aborted.eq(Some(Utc::now())),
                dsl::abort_reason.eq(Some(abort_reason)),
            ))
            .execute_async(conn)
            .await?;

        bail_unless!(
            rows_updated == 1,
            "Failed to update trust quorum config state to aborted \
            for rack_id {}, epoch {}",
            rack_id,
            epoch
        );

        Ok(())
    }

    async fn lookup_hw_baseboard_ids_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        members: impl Iterator<Item = BaseboardId>,
    ) -> Result<Vec<HwBaseboardId>, TransactionError<Error>> {
        use nexus_db_schema::schema::hw_baseboard_id::dsl;

        let (parts, serials): (Vec<_>, Vec<_>) = members
            .into_iter()
            .map(|m| (m.part_number, m.serial_number))
            .collect();

        dsl::hw_baseboard_id
            .filter(dsl::part_number.eq_any(parts))
            .filter(dsl::serial_number.eq_any(serials))
            .select(HwBaseboardId::as_select())
            .load_async(conn)
            .await
            .map_err(TransactionError::Database)
    }

    async fn tq_get_latest_config_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
    ) -> Result<Option<DbTrustQuorumConfiguration>, TransactionError<Error>>
    {
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        let latest = dsl::trust_quorum_configuration
            .filter(dsl::rack_id.eq(DbTypedUuid::<RackKind>::from(rack_id)))
            .order_by(dsl::epoch.desc())
            .first_async::<DbTrustQuorumConfiguration>(conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(latest)
    }

    async fn tq_get_config_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
        epoch: Epoch,
    ) -> Result<Option<DbTrustQuorumConfiguration>, TransactionError<Error>>
    {
        use nexus_db_schema::schema::trust_quorum_configuration::dsl;

        let epoch =
            epoch_to_i64(epoch).map_err(|e| TransactionError::from(e))?;

        let latest = dsl::trust_quorum_configuration
            .filter(dsl::rack_id.eq(DbTypedUuid::<RackKind>::from(rack_id)))
            .filter(dsl::epoch.eq(epoch))
            .get_result_async::<DbTrustQuorumConfiguration>(conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(latest)
    }

    async fn tq_get_members_conn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        rack_id: RackUuid,
        epoch: i64,
    ) -> Result<
        Vec<(DbTrustQuorumMember, HwBaseboardId)>,
        TransactionError<Error>,
    > {
        use nexus_db_schema::schema::hw_baseboard_id::dsl as hw_baseboard_id_dsl;
        use nexus_db_schema::schema::trust_quorum_member::dsl;

        let members = dsl::trust_quorum_member
            .filter(dsl::rack_id.eq(DbTypedUuid::<RackKind>::from(rack_id)))
            .filter(dsl::epoch.eq(epoch))
            .inner_join(
                hw_baseboard_id_dsl::hw_baseboard_id
                    .on(hw_baseboard_id_dsl::id.eq(dsl::hw_baseboard_id)),
            )
            .select((
                DbTrustQuorumMember::as_select(),
                HwBaseboardId::as_select(),
            ))
            .load_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(members)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::pub_test_utils::TestDatabase;
    use nexus_db_model::HwBaseboardId;
    use nexus_types::trust_quorum::{
        IsLrtqUpgrade, TrustQuorumConfigState, TrustQuorumMemberState,
    };
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::RackUuid;
    use uuid::Uuid;

    async fn insert_hw_baseboard_ids(db: &TestDatabase) -> Vec<HwBaseboardId> {
        let (_, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        use nexus_db_schema::schema::hw_baseboard_id::dsl;
        let mut hw_baseboard_ids: Vec<_> = (0..10)
            .map(|_| HwBaseboardId {
                id: Uuid::new_v4(),
                part_number: "test-part".to_string(),
                serial_number: Uuid::new_v4().to_string(),
            })
            .collect();

        // Sort like a `BaseboardId` would be sorted in a `BTreeSet`. This
        // allows us to use the coordinator as first entry in the set for our
        // tests.
        hw_baseboard_ids.sort_by(|a, b| {
            (&a.part_number, &a.serial_number)
                .cmp(&(&b.part_number, &b.serial_number))
        });

        diesel::insert_into(dsl::hw_baseboard_id)
            .values(hw_baseboard_ids.clone())
            .execute_async(&*conn)
            .await
            .unwrap();

        hw_baseboard_ids
    }

    #[tokio::test]
    async fn test_tq_insert_latest_errors() {
        let logctx = test_setup_log("test_tq_insert_latest_errors");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let hw_ids = insert_hw_baseboard_ids(&db).await;
        let rack_id = RackUuid::new_v4();
        let members: BTreeSet<_> =
            hw_ids.iter().cloned().map(BaseboardId::from).collect();

        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let coordinator = members.first().unwrap().clone();

        // Insert an initial config
        DataStore::tq_insert_rss_config_after_handoff(
            opctx,
            &conn,
            rack_id,
            members.clone(),
            coordinator,
        )
        .await
        .unwrap();

        // Last committed epoch is incorrect (should be 1)
        let bad_config = ProposedTrustQuorumConfig {
            rack_id,
            epoch: Epoch(2),
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: Epoch(4),
            },
            members: members.clone(),
        };
        let e1 = datastore
            .tq_insert_latest_config(opctx, bad_config)
            .await
            .unwrap_err();
        println!("{e1}");

        // Missing baseboard
        let mut bad_members = members.clone();
        bad_members.insert(BaseboardId {
            part_number: "bad".into(),
            serial_number: "value".into(),
        });
        let bad_config = ProposedTrustQuorumConfig {
            rack_id,
            epoch: Epoch(2),
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: Epoch(1),
            },
            members: bad_members,
        };
        let e2 = datastore
            .tq_insert_latest_config(opctx, bad_config)
            .await
            .unwrap_err();
        println!("{e2}");

        // Non-sequential epoch (should be 2)
        let bad_config = ProposedTrustQuorumConfig {
            rack_id,
            epoch: Epoch(3),
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: Epoch(1),
            },
            members: members.clone(),
        };
        let e3 = datastore
            .tq_insert_latest_config(opctx, bad_config)
            .await
            .unwrap_err();
        println!("{e3}");

        assert_ne!(e1, e2);
        assert_ne!(e1, e3);
        assert_ne!(e2, e3);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_tq_insert_initial_lrtq_upgrade() {
        let logctx = test_setup_log("test_tq_update_prepare_and_commit");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let hw_ids = insert_hw_baseboard_ids(&db).await;
        let rack_id = RackUuid::new_v4();
        let members: BTreeSet<_> =
            hw_ids.iter().cloned().map(BaseboardId::from).collect();

        // Propse a an LRTQ upgrade and successfully insert it
        let config = ProposedTrustQuorumConfig {
            rack_id,
            epoch: Epoch(2),
            is_lrtq_upgrade: IsLrtqUpgrade::Yes,
            members: members.clone(),
        };

        // Insert should succeed
        datastore.tq_insert_latest_config(opctx, config.clone()).await.unwrap();

        // Read the config back and check that it's preparing for LRTQ upgrade
        // with no acks.
        let read_config = datastore
            .tq_get_latest_config(opctx, rack_id)
            .await
            .expect("no error")
            .expect("returned config");

        // The read config should be preparing
        assert_eq!(read_config.epoch, config.epoch);
        assert_eq!(
            read_config.state,
            TrustQuorumConfigState::PreparingLrtqUpgrade
        );
        assert!(read_config.encrypted_rack_secrets.is_none());
        assert!(read_config.members.iter().all(|(_, info)| {
            info.state == TrustQuorumMemberState::Unacked
        }));

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_tq_update_prepare_and_commit() {
        let logctx = test_setup_log("test_tq_update_prepare_and_commit");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let hw_ids = insert_hw_baseboard_ids(&db).await;
        let rack_id = RackUuid::new_v4();
        let members: BTreeSet<_> =
            hw_ids.iter().cloned().map(BaseboardId::from).collect();

        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let coordinator = members.first().unwrap().clone();

        // Insert an initial config
        DataStore::tq_insert_rss_config_after_handoff(
            opctx,
            &conn,
            rack_id,
            members.clone(),
            coordinator,
        )
        .await
        .unwrap();

        // Propse a second configuration and successfully insert it
        let config = ProposedTrustQuorumConfig {
            rack_id,
            epoch: Epoch(2),
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: Epoch(1),
            },
            members: members.clone(),
        };
        datastore.tq_insert_latest_config(opctx, config.clone()).await.unwrap();

        // Read the config back and check that it's preparing with no acks
        let read_config = datastore
            .tq_get_latest_config(opctx, rack_id)
            .await
            .expect("no error")
            .expect("returned config");

        // The read config should be preparing
        assert_eq!(read_config.epoch, config.epoch);
        assert_eq!(read_config.state, TrustQuorumConfigState::Preparing);
        assert!(read_config.encrypted_rack_secrets.is_none());
        assert!(read_config.members.iter().all(|(_, info)| {
            info.state == TrustQuorumMemberState::Unacked
        }));

        // Trying to insert a new config should fail since we are currently
        // preparing
        let bad_config = ProposedTrustQuorumConfig {
            rack_id,
            epoch: Epoch(3),
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: Epoch(1),
            },
            members: members.clone(),
        };
        let e = datastore
            .tq_insert_latest_config(opctx, bad_config)
            .await
            .unwrap_err();
        println!("{e}");

        // Trying to ack commits should fail since we are currently preparing
        let acked_commits = members.clone();
        let e = datastore
            .tq_update_commit_status(
                opctx,
                rack_id,
                config.epoch,
                acked_commits,
            )
            .await
            .unwrap_err();
        println!("{e}");

        // A configuration returned from a coordinator is part of the trust
        // quorum protocol
        let coordinator_config =
            trust_quorum_types::configuration::Configuration {
                rack_id: config.rack_id,
                epoch: config.epoch,
                coordinator: hw_ids.first().unwrap().clone().into(),
                members: members
                    .clone()
                    .into_iter()
                    .map(|id| (id, Sha3_256Digest([0u8; 32])))
                    .collect(),
                threshold: TrustQuorumConfig::threshold(members.len() as u8),
                encrypted_rack_secrets: None,
            };

        // Ack only the coordinator
        datastore
            .tq_update_prepare_status(
                opctx,
                coordinator_config.clone(),
                [coordinator_config.coordinator.clone()].into_iter().collect(),
            )
            .await
            .unwrap();

        let read_config = datastore
            .tq_get_latest_config(opctx, rack_id)
            .await
            .expect("no error")
            .expect("returned config");

        // Ensure that Nexus has only seen the coordinator ack and that it has
        // not yet committed. There should also be no encrypted rack secrets,
        // and all members should now have share digests.
        assert_eq!(read_config.epoch, config.epoch);
        assert_eq!(read_config.state, TrustQuorumConfigState::Preparing);
        assert!(read_config.encrypted_rack_secrets.is_none());
        for (id, info) in &read_config.members {
            assert!(info.share_digest.is_some());
            if *id == coordinator_config.coordinator {
                assert_eq!(info.state, TrustQuorumMemberState::Prepared);
            } else {
                assert_eq!(info.state, TrustQuorumMemberState::Unacked);
            }
        }

        // Ack a threshold of peers.
        datastore
            .tq_update_prepare_status(
                opctx,
                coordinator_config.clone(),
                coordinator_config
                    .members
                    .keys()
                    .take(coordinator_config.threshold.0 as usize)
                    .cloned()
                    .collect(),
            )
            .await
            .unwrap();

        let read_config = datastore
            .tq_get_latest_config(opctx, rack_id)
            .await
            .expect("no error")
            .expect("returned config");

        // We've acked a threshold of nodes, but still should not have committed
        // because we haven't yet acked the `commit_crash_tolerance` number of
        // nodes in addition.
        assert_eq!(read_config.epoch, config.epoch);
        assert_eq!(read_config.state, TrustQuorumConfigState::Preparing);
        assert!(read_config.encrypted_rack_secrets.is_none());
        assert_eq!(
            coordinator_config.threshold.0 as usize,
            read_config
                .members
                .iter()
                .filter(
                    |(_, info)| info.state == TrustQuorumMemberState::Prepared
                )
                .count()
        );

        // Ack an additional `commit_crash_tolerance` of nodes. This should
        // trigger a commit.
        let acked_prepares = coordinator_config.threshold.0 as usize
            + read_config.commit_crash_tolerance as usize;

        datastore
            .tq_update_prepare_status(
                opctx,
                coordinator_config.clone(),
                coordinator_config
                    .members
                    .keys()
                    .take(acked_prepares)
                    .cloned()
                    .collect(),
            )
            .await
            .unwrap();

        let read_config = datastore
            .tq_get_latest_config(opctx, rack_id)
            .await
            .expect("no error")
            .expect("returned config");

        // We've acked enough nodes and should have written our status to the DB
        assert_eq!(read_config.epoch, config.epoch);
        assert_eq!(read_config.state, TrustQuorumConfigState::Committing);
        assert!(read_config.encrypted_rack_secrets.is_none());
        assert_eq!(
            acked_prepares,
            read_config
                .members
                .iter()
                .filter(
                    |(_, info)| info.state == TrustQuorumMemberState::Prepared
                )
                .count()
        );

        // Future prepare acks should fail because we have already started
        // committing.
        datastore
            .tq_update_prepare_status(
                opctx,
                coordinator_config.clone(),
                coordinator_config
                    .members
                    .keys()
                    .take(acked_prepares)
                    .cloned()
                    .collect(),
            )
            .await
            .unwrap_err();

        // Commit at all nodes
        datastore
            .tq_update_commit_status(
                opctx,
                rack_id,
                config.epoch,
                coordinator_config.members.keys().cloned().collect(),
            )
            .await
            .unwrap();

        let read_config = datastore
            .tq_get_latest_config(opctx, rack_id)
            .await
            .expect("no error")
            .expect("returned config");

        assert_eq!(read_config.epoch, config.epoch);
        // Now that all nodes have committed, we should see the config state
        // change from `Committing` to Committed.
        assert_eq!(read_config.state, TrustQuorumConfigState::Committed);
        assert!(read_config.encrypted_rack_secrets.is_none());
        assert!(
            read_config.members.iter().all(
                |(_, info)| info.state == TrustQuorumMemberState::Committed
            )
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_tq_abort() {
        let logctx = test_setup_log("test_tq_abort");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let hw_ids = insert_hw_baseboard_ids(&db).await;
        let rack_id = RackUuid::new_v4();
        let members: BTreeSet<_> =
            hw_ids.iter().cloned().map(BaseboardId::from).collect();

        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let coordinator = members.first().unwrap().clone();

        // Insert an initial config
        DataStore::tq_insert_rss_config_after_handoff(
            opctx,
            &conn,
            rack_id,
            members.clone(),
            coordinator,
        )
        .await
        .unwrap();

        // Propse a second configuration and successfully insert it
        let config = ProposedTrustQuorumConfig {
            rack_id,
            epoch: Epoch(2),
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: Epoch(1),
            },
            members: members.clone(),
        };
        datastore.tq_insert_latest_config(opctx, config.clone()).await.unwrap();

        // Aborting should succeed, since we haven't committed
        datastore
            .tq_abort_config(opctx, config.rack_id, config.epoch, "test".into())
            .await
            .unwrap();

        // Aborting is idempotent
        datastore
            .tq_abort_config(opctx, config.rack_id, config.epoch, "test".into())
            .await
            .unwrap();

        // Committing will fail to update any rows
        // (This is not directly callable from a public API).
        {
            let conn = datastore.pool_connection_for_tests().await.unwrap();
            DataStore::update_tq_state_committing_conn(
                &conn,
                config.rack_id.into(),
                config.epoch.0 as i64,
            )
            .await
            .unwrap_err();
        }

        // Create a config to simulate retrieving one from a coordinator
        let coordinator_config =
            trust_quorum_types::configuration::Configuration {
                rack_id: config.rack_id,
                epoch: config.epoch,
                coordinator: hw_ids.first().unwrap().clone().into(),
                members: members
                    .clone()
                    .into_iter()
                    .map(|id| (id, Sha3_256Digest([0u8; 32])))
                    .collect(),
                threshold: TrustQuorumConfig::threshold(members.len() as u8),
                encrypted_rack_secrets: None,
            };

        // This is how we actually try to trigger commit operations. This should
        // fail outright.
        let acked_prepares = (coordinator_config.threshold.0
            + TrustQuorumConfig::commit_crash_tolerance(members.len() as u8))
            as usize;
        datastore
            .tq_update_prepare_status(
                opctx,
                coordinator_config.clone(),
                coordinator_config
                    .members
                    .keys()
                    .take(acked_prepares)
                    .cloned()
                    .collect(),
            )
            .await
            .unwrap_err();

        // Retrieve the configuration and ensure it is actually aborted
        let read_config = datastore
            .tq_get_latest_config(opctx, rack_id)
            .await
            .expect("no error")
            .expect("returned config");
        assert_eq!(read_config.state, TrustQuorumConfigState::Aborted);

        // Create and insert another config
        let config2 = ProposedTrustQuorumConfig {
            rack_id,
            epoch: Epoch(3),
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: Epoch(1),
            },
            members: members.clone(),
        };
        datastore
            .tq_insert_latest_config(opctx, config2.clone())
            .await
            .unwrap();

        // Trying to abort the old config will fail because it's stale
        datastore
            .tq_abort_config(opctx, config.rack_id, config.epoch, "test".into())
            .await
            .unwrap_err();

        // Commit the new config
        let coordinator_config2 =
            trust_quorum_types::configuration::Configuration {
                epoch: config2.epoch,
                ..coordinator_config
            };
        let acked_prepares = (coordinator_config.threshold.0
            + TrustQuorumConfig::commit_crash_tolerance(members.len() as u8))
            as usize;
        datastore
            .tq_update_prepare_status(
                opctx,
                coordinator_config2.clone(),
                coordinator_config2
                    .members
                    .keys()
                    .take(acked_prepares)
                    .cloned()
                    .collect(),
            )
            .await
            .unwrap();

        // Abort of latest config should fail because it has already committed
        datastore
            .tq_abort_config(
                opctx,
                config2.rack_id,
                config2.epoch,
                "test".into(),
            )
            .await
            .unwrap_err();

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_tq_get_all_active_rack_id_and_latest_epoch() {
        let logctx =
            test_setup_log("test_get_all_active_rack_id_and_latest_epoch");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let rack_id1 = RackUuid::new_v4();
        let rack_id2 = RackUuid::new_v4();
        let rack_id3 = RackUuid::new_v4();

        let conn = datastore.pool_connection_for_tests().await.unwrap();

        // Save the membership for rack_id 2
        let mut rack2_members: BTreeSet<BaseboardId> = BTreeSet::new();

        // Create an initial config for 3 diff racks
        for rack_id in [rack_id1, rack_id2, rack_id3] {
            let hw_ids = insert_hw_baseboard_ids(&db).await;
            let members: BTreeSet<_> =
                hw_ids.iter().cloned().map(BaseboardId::from).collect();
            if rack_id == rack_id2 {
                rack2_members = members.clone();
            }
            let coordinator = members.first().unwrap().clone();
            DataStore::tq_insert_rss_config_after_handoff(
                opctx,
                &conn,
                rack_id,
                members.clone(),
                coordinator,
            )
            .await
            .unwrap();
        }

        // Create a second rack config for rack 2
        let config = ProposedTrustQuorumConfig {
            rack_id: rack_id2,
            epoch: Epoch(2),
            is_lrtq_upgrade: IsLrtqUpgrade::No {
                last_committed_epoch: Epoch(1),
            },
            members: rack2_members,
        };

        datastore.tq_insert_latest_config(opctx, config.clone()).await.unwrap();

        // Retreive the latest epochs per rack_id
        let values = datastore
            .tq_get_all_active_rack_id_and_latest_epoch(opctx)
            .await
            .unwrap();

        // We should only have retrieved the configuration for rack 2 that did
        // not commit or abort yet.
        assert_eq!(values.len(), 1);

        // The epoch should be the latest that exists
        assert_eq!(values[&rack_id2], Epoch(2));

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
